//! One leaf column of a row group, encoded on its own task.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, compute_leaves,
};
use parquet::errors::{ParquetError, Result as ParquetResult};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

/// The rows of one leaf column handed to its encoder in one go. A flat field
/// ships its array, so the parquet levels are computed on the encoder's task; a
/// nested field fans out to several leaves, so its levels are computed upstream
/// and shipped per leaf.
pub(super) enum Slice {
    Column(FieldRef, ArrayRef),
    Leaf(ArrowLeafColumn),
}

impl Slice {
    fn encode(self, writer: &mut ArrowColumnWriter) -> ParquetResult<()> {
        match self {
            Slice::Leaf(leaf) => writer.write(&leaf),
            Slice::Column(field, array) => {
                let mut leaves = compute_leaves(&field, &array)?;
                let leaf = leaves.pop().ok_or_else(|| {
                    ParquetError::General(format!("field {} has no leaf column", field.name()))
                })?;
                writer.write(&leaf)
            }
        }
    }
}

/// A queued slice and its share of the encode budget, which the task releases
/// once the slice is encoded.
type Queued = (Slice, OwnedSemaphorePermit);

/// Encodes the slices of one leaf column on its own task, as they arrive.
pub(super) struct ColumnEncoder {
    /// `None` once closed: dropping the sender ends the task's receive loop,
    /// which closes the column and returns its chunk.
    sender: Option<UnboundedSender<Queued>>,
    handle: JoinHandle<ParquetResult<ArrowColumnChunk>>,
    /// Anticipated encoded size, published by the task after each slice.
    encoded_size: Arc<AtomicUsize>,
    /// Memory the column writer holds, published by the task after each slice.
    memory_size: Arc<AtomicUsize>,
    /// Why the task failed, set before it exits. Its error only comes back from
    /// `finish`, but a hand-off notices a stopped task sooner.
    failure: Arc<OnceLock<String>>,
}

impl ColumnEncoder {
    pub(super) fn spawn(writer: ArrowColumnWriter) -> Self {
        let (sender, receiver) = unbounded_channel();
        let encoded_size = Arc::new(AtomicUsize::new(0));
        let memory_size = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(OnceLock::new());
        let handle = tokio::spawn(encode_column(
            writer,
            receiver,
            encoded_size.clone(),
            memory_size.clone(),
            failure.clone(),
        ));
        Self {
            sender: Some(sender),
            handle,
            encoded_size,
            memory_size,
            failure,
        }
    }

    /// Queue `slice`, with `permit` as its share of the encode budget.
    pub(super) fn send(&self, slice: Slice, permit: OwnedSemaphorePermit) -> ParquetResult<()> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| ParquetError::General("column encoder closed".to_string()))?;
        // The task only goes away when an encode failed.
        sender.send((slice, permit)).map_err(|_| self.stopped())
    }

    /// Take no more slices. The task encodes its backlog, then closes the column.
    pub(super) fn close(&mut self) {
        self.sender = None;
    }

    pub(super) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// The finished column chunk, once the task is done.
    pub(super) async fn finish(self) -> ParquetResult<ArrowColumnChunk> {
        self.handle
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?
    }

    pub(super) fn encoded_size(&self) -> usize {
        self.encoded_size.load(Ordering::Relaxed)
    }

    pub(super) fn memory_size(&self) -> usize {
        self.memory_size.load(Ordering::Relaxed)
    }

    fn stopped(&self) -> ParquetError {
        match self.failure.get() {
            Some(reason) => ParquetError::General(format!("column encoder stopped: {reason}")),
            None => ParquetError::General("column encoder stopped".to_string()),
        }
    }
}

/// Encoding time after which the task yields its runtime worker.
const YIELD_AFTER: Duration = Duration::from_millis(1);

/// The task behind a [`ColumnEncoder`]: encodes each slice as it arrives and
/// returns the finished column chunk once the channel closes.
async fn encode_column(
    mut writer: ArrowColumnWriter,
    mut receiver: UnboundedReceiver<Queued>,
    encoded_size: Arc<AtomicUsize>,
    memory_size: Arc<AtomicUsize>,
    failure: Arc<OnceLock<String>>,
) -> ParquetResult<ArrowColumnChunk> {
    let mut since_yield = Instant::now();
    while let Some((slice, budget)) = receiver.recv().await {
        if let Err(e) = slice.encode(&mut writer) {
            let _ = failure.set(e.to_string());
            return Err(e);
        }
        // Encoded: the arrow slice can go, and its bytes return to the budget.
        drop(budget);
        encoded_size.store(writer.get_estimated_total_bytes(), Ordering::Relaxed);
        memory_size.store(writer.memory_size(), Ordering::Relaxed);

        // Encoding never awaits, so yield every 1 ms for the other tasks on this worker.
        if since_yield.elapsed() >= YIELD_AFTER {
            tokio::task::yield_now().await;
            since_yield = Instant::now();
        }
    }
    writer.close()
}

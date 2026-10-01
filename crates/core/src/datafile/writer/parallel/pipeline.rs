//! The row groups of a file in flight: the open one being fed, and closed ones
//! still encoding their backlog, handed over in order as they finish.

use std::collections::VecDeque;
use std::sync::OnceLock;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_writer::ArrowRowGroupWriterFactory;
use parquet::errors::Result as ParquetResult;

use super::budget::EncodeBudget;
use super::row_group::{EncodedRowGroup, RowGroupEncoder};

/// Row groups a file may have in flight: the open one plus closed ones still
/// encoding their backlog. Each extra group runs one more encoder per column and
/// holds its encoded pages until it is appended. `1` finishes every row group
/// before the next one is fed. Override with `DELTARS_ENCODE_ROW_GROUPS_IN_FLIGHT`.
fn row_groups_in_flight() -> usize {
    static IN_FLIGHT: OnceLock<usize> = OnceLock::new();
    *IN_FLIGHT.get_or_init(|| {
        std::env::var("DELTARS_ENCODE_ROW_GROUPS_IN_FLIGHT")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(8)
            .clamp(1, 32)
    })
}

/// Feeds batches into row groups of `max_rows` rows, several of which encode
/// at once.
pub(super) struct RowGroupPipeline {
    factory: ArrowRowGroupWriterFactory,
    schema: SchemaRef,
    budget: EncodeBudget,
    max_rows: usize,
    /// The row group being fed; none until the first write.
    open: Option<RowGroupEncoder>,
    /// Closed row groups still encoding, oldest first.
    closing: VecDeque<RowGroupEncoder>,
    /// Index of the next row group, for the factory.
    next_index: usize,
}

impl RowGroupPipeline {
    pub(super) fn new(
        factory: ArrowRowGroupWriterFactory,
        schema: SchemaRef,
        budget: EncodeBudget,
        max_rows: usize,
    ) -> Self {
        Self {
            factory,
            schema,
            budget,
            max_rows,
            open: None,
            closing: VecDeque::new(),
            next_index: 0,
        }
    }

    /// Feed `batch`, closing the open row group at `max_rows`. Returns the row
    /// groups ready to append, in file order: those whose encoders finished in
    /// the meantime, and, past the in-flight limit, the oldest still encoding,
    /// waited for.
    pub(super) async fn write(
        &mut self,
        batch: &RecordBatch,
    ) -> ParquetResult<Vec<EncodedRowGroup>> {
        let mut done = Vec::new();
        while self
            .closing
            .front()
            .is_some_and(RowGroupEncoder::is_finished)
        {
            done.extend(self.finish_oldest().await?);
        }

        let max_rows = self.max_rows;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let open = self.open_row_group()?;
            let length = usize::min(max_rows - open.rows(), batch.num_rows() - offset);
            if length == batch.num_rows() {
                open.write(batch).await?;
            } else {
                open.write(&batch.slice(offset, length)).await?;
            }
            offset += length;

            if open.rows() >= max_rows {
                self.close_row_group();
                while self.closing.len() >= row_groups_in_flight() {
                    done.extend(self.finish_oldest().await?);
                }
            }
        }
        Ok(done)
    }

    /// Close the open row group and wait for every row group in flight.
    pub(super) async fn finish(&mut self) -> ParquetResult<Vec<EncodedRowGroup>> {
        self.close_row_group();
        let mut done = Vec::with_capacity(self.closing.len());
        while !self.closing.is_empty() {
            done.extend(self.finish_oldest().await?);
        }
        Ok(done)
    }

    /// Rows of the open row group.
    pub(super) fn open_rows(&self) -> usize {
        self.open.as_ref().map_or(0, RowGroupEncoder::rows)
    }

    /// Rows of every row group in flight.
    pub(super) fn rows(&self) -> usize {
        self.in_flight().map(RowGroupEncoder::rows).sum()
    }

    /// Arrow bytes of every row group in flight.
    pub(super) fn arrow_bytes(&self) -> usize {
        self.in_flight().map(RowGroupEncoder::arrow_bytes).sum()
    }

    /// Anticipated encoded size of the rows the encoders got through so far.
    pub(super) fn encoded_size(&self) -> usize {
        self.in_flight().map(RowGroupEncoder::encoded_size).sum()
    }

    /// Memory the encoders of every row group in flight hold.
    pub(super) fn memory_size(&self) -> usize {
        self.in_flight().map(RowGroupEncoder::memory_size).sum()
    }

    fn in_flight(&self) -> impl Iterator<Item = &RowGroupEncoder> {
        self.closing.iter().chain(self.open.iter())
    }

    /// The open row group, started if there is none.
    fn open_row_group(&mut self) -> ParquetResult<&mut RowGroupEncoder> {
        if self.open.is_none() {
            let column_writers = self.factory.create_column_writers(self.next_index)?;
            self.next_index += 1;
            self.open = Some(RowGroupEncoder::spawn(
                column_writers,
                self.schema.clone(),
                self.budget.clone(),
            ));
        }
        Ok(self.open.as_mut().expect("a row group was just opened"))
    }

    /// Move the open row group, if any, to the closed ones; its tasks keep
    /// encoding their backlog.
    fn close_row_group(&mut self) {
        if let Some(mut group) = self.open.take() {
            group.close();
            self.closing.push_back(group);
        }
    }

    /// Wait for the oldest closed row group, if any.
    async fn finish_oldest(&mut self) -> ParquetResult<Option<EncodedRowGroup>> {
        match self.closing.pop_front() {
            Some(group) => group.finish().await.map(Some),
            None => Ok(None),
        }
    }
}

//! Reads the active files of a snapshot from a base and a tail.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use bytes::Bytes;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use futures::{StreamExt, TryStreamExt};
use tracing::warn;

use super::super::SendableRBStream;
use super::ReadContext;
use super::entry::{Manifest, Tail, decode_batches};
use super::file_key::{FileKeySet, RowKeys};
use super::key::TableKey;
use super::store::CacheStore;
use crate::DeltaResult;

/// Returns the rows of `batch` whose key is not in `keys`, or `None` when no row is left.
pub(super) fn drop_rows(
    batch: &RecordBatch,
    keys: &FileKeySet,
    snapshot: &KernelSnapshot,
) -> DeltaResult<Option<RecordBatch>> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    if keys.is_empty() {
        return Ok(Some(batch.clone()));
    }

    let rows = RowKeys::try_new(batch, snapshot)?;
    let mut dropped_any = false;
    let keep: BooleanArray = (0..rows.len())
        .map(|row| {
            let keep = !keys.contains(rows.path(row), rows.dv(row));
            dropped_any |= !keep;
            Some(keep)
        })
        .collect();
    if !dropped_any {
        return Ok(Some(batch.clone()));
    }
    let batch = filter_record_batch(batch, &keep)?;
    Ok((batch.num_rows() > 0).then_some(batch))
}

/// Streams the files the tail added and then the base rows that are still active.
///
/// This is the order of a log replay: newest first.
///
/// The store can lose a base chunk at any time. When that occurs, the stream reads the
/// log for the rows it has not returned yet.
pub(super) fn merged_files(
    ctx: Arc<ReadContext>,
    store: Arc<dyn CacheStore>,
    table: TableKey,
    manifest: Manifest,
    tail: Tail,
    first_chunk: Option<Bytes>,
) -> SendableRBStream {
    let state = MergeState {
        ctx,
        store,
        table,
        manifest,
        ready: tail.adds.into(),
        dropped: tail.dropped,
        first_chunk,
        next_chunk: 0,
        returned: Vec::new(),
        replay: None,
    };
    futures::stream::try_unfold(state, |mut state| async move {
        Ok(state.next().await?.map(|batch| (batch, state)))
    })
    .boxed()
}

struct MergeState {
    ctx: Arc<ReadContext>,
    store: Arc<dyn CacheStore>,
    table: TableKey,
    manifest: Manifest,
    /// Batches to return before the next chunk is read.
    ready: VecDeque<RecordBatch>,
    dropped: FileKeySet,
    /// The first chunk is read before the stream starts, to find a lost base early.
    first_chunk: Option<Bytes>,
    next_chunk: u32,
    /// The keys of all returned rows. Only a lost chunk makes them necessary.
    returned: Vec<RowKeys>,
    /// The log replay that takes over after a lost chunk.
    replay: Option<SendableRBStream>,
}

impl MergeState {
    async fn next(&mut self) -> DeltaResult<Option<RecordBatch>> {
        loop {
            if let Some(replay) = &mut self.replay {
                return replay.try_next().await;
            }
            if let Some(batch) = self.ready.pop_front() {
                let keys = RowKeys::try_new(&batch, &self.ctx.snapshot.inner)?;
                self.returned.push(keys.detached());
                return Ok(Some(batch));
            }
            if self.next_chunk == self.manifest.chunks {
                return Ok(None);
            }
            match self.read_chunk().await {
                Some(batches) => {
                    self.next_chunk += 1;
                    for batch in &batches {
                        self.ready.extend(drop_rows(
                            batch,
                            &self.dropped,
                            &self.ctx.snapshot.inner,
                        )?);
                    }
                }
                None => self.replay_remaining_rows().await,
            }
        }
    }

    async fn read_chunk(&mut self) -> Option<Vec<RecordBatch>> {
        let data = match self.first_chunk.take() {
            Some(data) => data,
            None => {
                let key = self.table.chunk(&self.manifest.base, self.next_chunk);
                self.store.get(&key).await.ok().flatten()?
            }
        };
        decode_batches(data).ok()
    }

    async fn replay_remaining_rows(&mut self) {
        warn!(
            chunk = self.next_chunk,
            "the file list cache lost a part of a base during a read, reading the log instead"
        );
        // The base is not complete. Remove it, so the next read writes a new one.
        let manifest_key = self.table.manifest();
        if let Ok(Some(data)) = self.store.get(&manifest_key).await
            && Manifest::decode(&data).is_ok_and(|current| current.base == self.manifest.base)
        {
            let _ = self.store.remove(&manifest_key).await;
        }

        let returned: FileKeySet = self
            .returned
            .drain(..)
            .flat_map(|keys| keys.keys().collect::<Vec<_>>())
            .collect();
        let kernel = self.ctx.snapshot.inner.clone();
        self.replay = Some(
            self.ctx
                .replay()
                .try_filter_map(move |batch| {
                    std::future::ready(drop_rows(&batch, &returned, &kernel))
                })
                .boxed(),
        );
    }
}

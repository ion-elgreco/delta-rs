//! Writes the active files of one table version to the cache as a new base.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take_record_batch};
use delta_kernel::Version;

use super::super::Snapshot;
use super::Limits;
use super::entry::{Manifest, encode_batches};
use super::key::{BaseId, TableKey};
use super::store::CacheStore;
use crate::{DeltaResult, DeltaTableError};

/// Splits a file list into chunks and stores them. The manifest is stored last, so a base
/// that was not written to the end does not exist.
pub(super) struct BaseWriter {
    store: Arc<dyn CacheStore>,
    table: TableKey,
    base: BaseId,
    limits: Limits,
    pending: VecDeque<RecordBatch>,
    pending_rows: usize,
    rows_per_chunk: usize,
    chunks: u32,
    rows: u64,
}

impl BaseWriter {
    pub(super) fn new(
        store: Arc<dyn CacheStore>,
        table: TableKey,
        version: Version,
        limits: Limits,
    ) -> Self {
        Self {
            store,
            table,
            base: BaseId::new(version),
            limits,
            pending: VecDeque::new(),
            pending_rows: 0,
            rows_per_chunk: limits.first_chunk_rows.max(1),
            chunks: 0,
            rows: 0,
        }
    }

    pub(super) async fn push(&mut self, batch: &RecordBatch) -> DeltaResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.pending_rows += batch.num_rows();
        self.pending.push_back(batch.clone());
        while self.pending_rows >= self.rows_per_chunk {
            let chunk = self.take_rows(self.rows_per_chunk)?;
            self.write(chunk).await?;
        }
        Ok(())
    }

    /// Stores the remaining rows and then the manifest.
    pub(super) async fn finish(mut self, snapshot: &Snapshot) -> DeltaResult<Manifest> {
        if self.pending_rows > 0 {
            let chunk = self.take_rows(self.pending_rows)?;
            self.write(chunk).await?;
        }
        let manifest = Manifest::new(self.base, self.chunks, self.rows, snapshot);
        self.store
            .put(self.table.manifest(), manifest.encode()?.into())
            .await?;
        Ok(manifest)
    }

    /// Removes the first `rows` pending rows and returns them as one batch.
    fn take_rows(&mut self, rows: usize) -> DeltaResult<RecordBatch> {
        let mut parts = Vec::new();
        let mut missing = rows;
        while missing > 0 {
            let batch = self.pending.pop_front().expect("enough pending rows");
            if batch.num_rows() <= missing {
                missing -= batch.num_rows();
                parts.push(batch);
            } else {
                parts.push(batch.slice(0, missing));
                self.pending
                    .push_front(batch.slice(missing, batch.num_rows() - missing));
                missing = 0;
            }
        }
        self.pending_rows -= rows;
        Ok(concat_batches(&parts[0].schema(), &parts)?)
    }

    async fn write(&mut self, chunk: RecordBatch) -> DeltaResult<()> {
        let mut todo = vec![chunk];
        while let Some(batch) = todo.pop() {
            let rows = batch.num_rows();
            let data = encode_batches(std::slice::from_ref(&batch))?;
            if data.len() > self.limits.max_entry_bytes {
                if rows == 1 {
                    return Err(DeltaTableError::Generic(
                        "one row of the file list is larger than a cache entry".into(),
                    ));
                }
                // The second half goes first on the stack, so the first half is written first.
                let half = rows / 2;
                todo.push(rows_of(&batch, half..rows)?);
                todo.push(rows_of(&batch, 0..half)?);
                continue;
            }

            self.rows_per_chunk = (self.limits.target_chunk_bytes * rows / data.len()).max(1);
            self.store
                .put(self.table.chunk(&self.base, self.chunks), data.into())
                .await?;
            self.chunks += 1;
            self.rows += rows as u64;
        }
        Ok(())
    }
}

/// Copies a row range into a new batch that shares no buffers with `batch`.
fn rows_of(batch: &RecordBatch, rows: std::ops::Range<usize>) -> DeltaResult<RecordBatch> {
    let indices = UInt32Array::from_iter_values(rows.map(|row| row as u32));
    Ok(take_record_batch(batch, &indices)?)
}

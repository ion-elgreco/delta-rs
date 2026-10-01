//! One row group: a column encoder per leaf column, fed the slices of each batch.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowColumnWriter, compute_leaves};
use parquet::errors::{ParquetError, Result as ParquetResult};

use super::budget::EncodeBudget;
use super::column::ColumnEncoder;

/// Arrow bytes `array` keeps alive, honouring slices: `get_array_memory_size`
/// reports the full backing buffers, which overstates a slice many times over.
fn array_bytes(array: &ArrayRef) -> usize {
    array
        .to_data()
        .get_slice_memory_size()
        .unwrap_or_else(|_| array.get_array_memory_size())
}

/// A row group every column encoder finished, ready to append to a file.
pub(super) struct EncodedRowGroup {
    pub(super) chunks: Vec<ArrowColumnChunk>,
    pub(super) rows: usize,
    pub(super) arrow_bytes: usize,
}

/// Encodes batches into one row group, one task per leaf column.
pub(super) struct RowGroupEncoder {
    columns: Vec<ColumnEncoder>,
    /// Arrow schema of the batches, which splits them into leaf columns.
    schema: SchemaRef,
    budget: EncodeBudget,
    /// Rows fed so far.
    rows: usize,
    /// Arrow bytes fed so far.
    arrow_bytes: usize,
}

impl RowGroupEncoder {
    pub(super) fn spawn(
        column_writers: Vec<ArrowColumnWriter>,
        schema: SchemaRef,
        budget: EncodeBudget,
    ) -> Self {
        Self {
            columns: column_writers
                .into_iter()
                .map(ColumnEncoder::spawn)
                .collect(),
            schema,
            budget,
            rows: 0,
            arrow_bytes: 0,
        }
    }

    /// Hand every leaf column of `batch` to its encoder, each slice charged to
    /// the budget first. The leaves of a nested field share the field's
    /// buffers, so only its first leaf is charged.
    pub(super) async fn write(&mut self, batch: &RecordBatch) -> ParquetResult<()> {
        let mut leaf = 0;
        for (field, array) in self.schema.fields().iter().zip(batch.columns()) {
            let mut bytes = array_bytes(array);
            for slice in compute_leaves(field, array)? {
                let permit = self.budget.acquire(bytes).await?;
                let column = self.columns.get(leaf).ok_or_else(|| {
                    ParquetError::General(format!("no column encoder for leaf {leaf}"))
                })?;
                column.send(slice, permit)?;
                self.arrow_bytes += bytes;
                bytes = 0;
                leaf += 1;
            }
        }
        self.rows += batch.num_rows();
        Ok(())
    }

    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    pub(super) fn arrow_bytes(&self) -> usize {
        self.arrow_bytes
    }

    /// Anticipated encoded size of the rows encoded so far.
    pub(super) fn encoded_size(&self) -> usize {
        self.columns.iter().map(ColumnEncoder::encoded_size).sum()
    }

    /// Memory the column writers hold.
    pub(super) fn memory_size(&self) -> usize {
        self.columns.iter().map(ColumnEncoder::memory_size).sum()
    }

    /// Take no more rows. The column tasks encode their backlog, then close.
    pub(super) fn close(&mut self) {
        for column in &mut self.columns {
            column.close();
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.columns.iter().all(ColumnEncoder::is_finished)
    }

    /// The finished row group, once every column task is done.
    pub(super) async fn finish(self) -> ParquetResult<EncodedRowGroup> {
        let mut chunks = Vec::with_capacity(self.columns.len());
        for column in self.columns {
            chunks.push(column.finish().await?);
        }
        Ok(EncodedRowGroup {
            chunks,
            rows: self.rows,
            arrow_bytes: self.arrow_bytes,
        })
    }
}

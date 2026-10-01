//! One row group: a column encoder per leaf column, fed the slices of each batch.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_writer::{ArrowColumnChunk, ArrowColumnWriter, compute_leaves};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::schema::types::SchemaDescriptor;

use super::budget::EncodeBudget;
use super::column::{ColumnEncoder, Slice};

/// How the fields of the arrow schema map to parquet leaf columns, which
/// decides whether a field's rows ship whole or as computed leaves.
pub(super) struct LeafLayout {
    schema: SchemaRef,
    /// Parquet leaf columns under each top-level field, in schema order.
    leaves_per_field: Vec<usize>,
}

impl LeafLayout {
    pub(super) fn new(schema: SchemaRef, parquet_schema: &SchemaDescriptor) -> Self {
        let leaves_per_field = schema
            .fields()
            .iter()
            .map(|field| {
                parquet_schema
                    .columns()
                    .iter()
                    .filter(|leaf| leaf.path().parts().first() == Some(field.name()))
                    .count()
            })
            .collect();
        Self {
            schema,
            leaves_per_field,
        }
    }

    /// Split `batch` into the work of each leaf column, in leaf order, with the
    /// arrow bytes to charge for it. A flat field ships its array whole. A
    /// nested field's leaves are computed here; they share the field's buffers,
    /// so only its first leaf is charged.
    pub(super) fn split(&self, batch: &RecordBatch) -> ParquetResult<Vec<(Slice, usize)>> {
        let mut slices = Vec::with_capacity(self.leaves_per_field.iter().sum());
        let fields = self.schema.fields().iter().zip(batch.columns());
        for ((field, array), &leaves) in fields.zip(&self.leaves_per_field) {
            let bytes = array_bytes(array);
            if leaves == 1 {
                slices.push((Slice::Column(field.clone(), array.clone()), bytes));
            } else {
                for (index, leaf) in compute_leaves(field, array)?.into_iter().enumerate() {
                    let charged = if index == 0 { bytes } else { 0 };
                    slices.push((Slice::Leaf(leaf), charged));
                }
            }
        }
        Ok(slices)
    }
}

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
    layout: Arc<LeafLayout>,
    budget: EncodeBudget,
    /// Rows fed so far.
    rows: usize,
    /// Arrow bytes fed so far.
    arrow_bytes: usize,
}

impl RowGroupEncoder {
    pub(super) fn spawn(
        column_writers: Vec<ArrowColumnWriter>,
        layout: Arc<LeafLayout>,
        budget: EncodeBudget,
    ) -> Self {
        Self {
            columns: column_writers
                .into_iter()
                .map(ColumnEncoder::spawn)
                .collect(),
            layout,
            budget,
            rows: 0,
            arrow_bytes: 0,
        }
    }

    /// Hand every leaf column of `batch` to its encoder, each slice charged to
    /// the budget first.
    pub(super) async fn write(&mut self, batch: &RecordBatch) -> ParquetResult<()> {
        for (leaf, (slice, bytes)) in self.layout.split(batch)?.into_iter().enumerate() {
            let permit = self.budget.acquire(bytes).await?;
            let column = self.columns.get(leaf).ok_or_else(|| {
                ParquetError::General(format!("no column encoder for leaf {leaf}"))
            })?;
            column.send(slice, permit)?;
            self.arrow_bytes += bytes;
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

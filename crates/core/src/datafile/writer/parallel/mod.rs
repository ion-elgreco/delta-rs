//! Parallel column encoding for a single parquet file.
//!
//! Each leaf column of a row group encodes on its own task, so encoding and compression, the
//! expensive part, run on as many cores as the table has leaf columns. A closed row group keeps
//! encoding while the next one is fed, so the slowest column of several row groups encodes at
//! once.
//!
//! ```text
//!       RecordBatch
//!            │
//!            ▼
//! ┌─────────────────────┐
//! │ ParallelArrowWriter │  one per file: feeds the rows to the pipeline below, and
//! └──────────┬──────────┘  appends the row groups it hands back to the file
//!            │
//!            ▼
//! ┌─────────────────────┐  cuts the rows into row groups of max_row_group_row_count rows,
//! │  RowGroupPipeline   │  up to 8 in flight: the open one takes new rows, the closed ones
//! └──────────┬──────────┘  finish encoding theirs, and the oldest leaves first
//!            │
//!            ▼ open           closed, newest              closed, oldest
//! ┌─────────────────────┐ ┌─────────────────────┐     ┌─────────────────────┐
//! │   RowGroupEncoder   │ │   RowGroupEncoder   │ ... │   RowGroupEncoder   │
//! └───┬──────┬──────┬───┘ └───┬──────┬──────┬───┘     └───┬──────┬──────┬───┘
//!     ▼      ▼      ▼         ▼      ▼      ▼             ▼      ▼      ▼
//!   col 0  col 1  col n     col 0  col 1  col n         col 0  col 1  col n
//!                                                         │      │      │
//!   one ColumnEncoder task per leaf column of             └──────┼──────┘
//!   each row group in flight, all running at once                │ EncodedRowGroup, once
//!                                                                ▼ all its columns are done
//!                                                     ┌──────────────────────┐
//!                                                     │ SerializedFileWriter │ ──► sink
//!                                                     └──────────────────────┘
//! ```
//!
//! Every file of a partition shares one `EncodeShared`, since a rolled file keeps encoding in
//! the background while the next one is fed:
//!
//! - `EncodeBudget` bounds the arrow bytes queued ahead of the column tasks. A slice takes its
//!   bytes before it is queued, and its task gives them back once the slice is encoded.
//! - `SizeModel` keeps the bytes per row of the appended row groups. It sizes the rows still in
//!   flight, so the `PartitionWriter` can roll a file at its target without waiting for the
//!   encoders. Before the first row group is appended, or when the rows change shape, a write
//!   that could carry the file past its target waits for the encoders instead.
//!
//! `ParallelArrowWriter` and `EncodeShared` live here, `RowGroupPipeline` in `pipeline.rs`,
//! `RowGroupEncoder` in `row_group.rs`, `ColumnEncoder` in `column.rs`, `EncodeBudget` in
//! `budget.rs`, and `SizeModel` in `size_model.rs`.

mod budget;
mod column;
mod pipeline;
mod row_group;
mod size_model;

use std::mem;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use parquet::arrow::arrow_writer::ArrowRowGroupWriterFactory;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::{ArrowSchemaConverter, add_encoded_arrow_schema_to_metadata};
use parquet::column::page_store::PageStoreFactory;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;

use self::budget::EncodeBudget;
use self::pipeline::RowGroupPipeline;
use self::row_group::EncodedRowGroup;
use self::size_model::SizeModel;

/// Arrow-specific settings for writing parquet data files.
#[derive(Debug, Clone)]
pub struct ArrowWriterOptions {
    skip_arrow_metadata_hint: bool,
    page_store_factory: Option<Arc<dyn PageStoreFactory>>,
    enable_parallel_encoding: bool,
}

impl Default for ArrowWriterOptions {
    fn default() -> Self {
        Self {
            skip_arrow_metadata_hint: false,
            page_store_factory: None,
            enable_parallel_encoding: true,
        }
    }
}

impl ArrowWriterOptions {
    /// Creates [`ArrowWriterOptions`] with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Skip writing the serialized arrow schema into the parquet footer (defaults to `false`).
    pub fn with_skip_arrow_metadata(mut self, skip_arrow_metadata: bool) -> Self {
        self.skip_arrow_metadata_hint = skip_arrow_metadata;
        self
    }

    /// Sets the [`PageStoreFactory`] that buffers completed pages while a row group is open.
    ///
    /// By default pages are held on the heap until the row group is flushed.
    pub fn with_page_store_factory(
        mut self,
        page_store_factory: Arc<dyn PageStoreFactory>,
    ) -> Self {
        self.page_store_factory = Some(page_store_factory);
        self
    }

    /// Encode each column of a row group in its own task (defaults to `true`). When `false`,
    /// arrow-rs's `AsyncArrowWriter` encodes the columns one after another.
    pub fn with_enable_parallel_encoding(mut self, enable_parallel_encoding: bool) -> Self {
        self.enable_parallel_encoding = enable_parallel_encoding;
        self
    }

    pub(crate) fn enable_parallel_encoding(&self) -> bool {
        self.enable_parallel_encoding
    }

    /// The same settings as parquet's own options, for `AsyncArrowWriter`.
    pub(crate) fn to_parquet_options(
        &self,
        props: WriterProperties,
    ) -> parquet::arrow::arrow_writer::ArrowWriterOptions {
        let options = parquet::arrow::arrow_writer::ArrowWriterOptions::new()
            .with_properties(props)
            .with_skip_arrow_metadata(self.skip_arrow_metadata_hint);
        match &self.page_store_factory {
            Some(page_store_factory) => {
                options.with_page_store_factory(Arc::clone(page_store_factory))
            }
            None => options,
        }
    }
}

/// What the files of one partition share while they encode. A rolled file keeps
/// encoding and appending in the background, so both span every file.
#[derive(Clone, Debug)]
pub(crate) struct EncodeShared {
    budget: EncodeBudget,
    sizes: SizeModel,
}

impl EncodeShared {
    pub(crate) fn new() -> Self {
        Self {
            budget: EncodeBudget::new(),
            sizes: SizeModel::default(),
        }
    }
}

/// Encodes [`RecordBatch`]es to one parquet file, one column per task.
pub(crate) struct ParallelArrowWriter<W: AsyncFileWriter> {
    /// Underlying parquet writer that writes into buffer
    file_writer: SerializedFileWriter<Vec<u8>>,

    /// Writer that sinks to storage
    sink_writer: W,

    /// The row groups in flight
    pipeline: RowGroupPipeline,

    shared: EncodeShared,

    /// Size at which the caller rolls the file, see [`Self::settle_queued_rows`].
    target_file_size: Option<u64>,
}

impl<W: AsyncFileWriter> ParallelArrowWriter<W> {
    pub(crate) fn try_new(
        sink_writer: W,
        arrow_schema: SchemaRef,
        props: WriterProperties,
        options: Option<ArrowWriterOptions>,
        shared: EncodeShared,
        target_file_size: Option<u64>,
    ) -> ParquetResult<Self> {
        let mut props = props;
        let options = options.unwrap_or_default();

        if !options.skip_arrow_metadata_hint {
            add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);
        }

        let props = Arc::new(props);

        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&arrow_schema)?;
        let file_writer =
            SerializedFileWriter::new(Vec::new(), parquet_schema.root_schema_ptr(), props.clone())?;

        let mut factory = ArrowRowGroupWriterFactory::new(&file_writer, arrow_schema.clone());
        if let Some(page_store_factory) = options.page_store_factory {
            factory = factory.with_page_store_factory(page_store_factory);
        }
        let pipeline = RowGroupPipeline::new(
            factory,
            arrow_schema,
            shared.budget.clone(),
            props.max_row_group_row_count().unwrap_or(usize::MAX),
        );

        Ok(Self {
            file_writer,
            sink_writer,
            pipeline,
            shared,
            target_file_size,
        })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> ParquetResult<()> {
        for row_group in self.pipeline.write(batch).await? {
            self.append(row_group).await?;
        }
        self.settle_queued_rows().await
    }

    /// Close the file and flush everything to the sink.
    pub(crate) async fn finish(&mut self) -> ParquetResult<ParquetMetaData> {
        for row_group in self.pipeline.finish().await? {
            self.append(row_group).await?;
        }
        let metadata = self.file_writer.finish()?;
        self.flush_buffer().await?;
        self.sink_writer.complete().await?;
        Ok(metadata)
    }

    pub(crate) fn into_inner(self) -> W {
        // Callers only take this route to abort, so the row groups in flight are dropped.
        self.sink_writer
    }

    pub(crate) fn bytes_written(&self) -> usize {
        self.file_writer.bytes_written()
    }

    /// Anticipated encoded size of the row groups not yet appended to the file.
    ///
    /// Once the partition has appended a row group, and the rows in flight are
    /// shaped like the appended ones, every row in flight counts at the appended
    /// bytes per row, queued or not. Before that, it is the encoders' own figure
    /// for the rows they got through, which leaves out the rows still queued;
    /// [`Self::settle_queued_rows`] keeps that from hiding a file past its target.
    pub(crate) fn in_progress_size(&self) -> usize {
        match self.bytes_per_row() {
            Some(bytes_per_row) => (self.pipeline.rows() as f64 * bytes_per_row) as usize,
            None => self.pipeline.encoded_size(),
        }
    }

    /// Memory the encoders of the row groups in flight hold.
    pub(crate) fn memory_size(&self) -> usize {
        self.pipeline.memory_size()
    }

    pub(crate) fn in_progress_rows(&self) -> usize {
        self.pipeline.open_rows()
    }

    pub(crate) fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
        self.file_writer.flushed_row_groups()
    }

    /// Bytes per row to size the rows in flight with, see [`SizeModel::bytes_per_row_for`].
    fn bytes_per_row(&self) -> Option<f64> {
        self.shared
            .sizes
            .bytes_per_row_for(self.pipeline.rows(), self.pipeline.arrow_bytes())
    }

    /// Until the partition can size the rows in flight, `in_progress_size`
    /// leaves out the rows still queued. Once those, at arrow size, could carry
    /// the file to its target, wait for the encoders to catch up, so the roll
    /// decision the caller makes next rests on encoded bytes alone.
    async fn settle_queued_rows(&mut self) -> ParquetResult<()> {
        let Some(target) = self.target_file_size else {
            return Ok(());
        };
        if self.bytes_per_row().is_some() {
            return Ok(());
        }
        let queued = self.shared.budget.queued_bytes();
        if queued == 0
            || ((self.bytes_written() + self.in_progress_size() + queued) as u64) < target
        {
            return Ok(());
        }
        self.shared.budget.drain().await
    }

    /// Append a finished row group to the file and send its bytes on.
    async fn append(&mut self, row_group: EncodedRowGroup) -> ParquetResult<()> {
        let bytes: u64 = row_group
            .chunks
            .iter()
            .map(|chunk| chunk.close().bytes_written)
            .sum();
        let mut writer = self.file_writer.next_row_group()?;
        for chunk in row_group.chunks {
            chunk.append_to_row_group(&mut writer)?;
        }
        writer.close()?;
        self.shared
            .sizes
            .record(bytes, row_group.rows, row_group.arrow_bytes);
        self.flush_buffer().await
    }

    /// Move the bytes the file writer buffered into the sink.
    async fn flush_buffer(&mut self) -> ParquetResult<()> {
        let buffer = mem::take(self.file_writer.inner_mut());
        if buffer.is_empty() {
            return Ok(());
        }
        self.sink_writer.write(Bytes::from(buffer)).await
    }
}

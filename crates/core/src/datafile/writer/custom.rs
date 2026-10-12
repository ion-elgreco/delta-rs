//! A parquet writer of the caller's own: a [`CustomArrowWriter`] takes the place
//! of the built-in writers for every data file of a write, created per file by its
//! [`CustomArrowWriterFactory`].

use std::fmt::Debug;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::properties::WriterProperties;

use super::ArrowWriterOptions;
use super::file::ParquetObjectWriter;

/// The parquet writer of one data file, supplied by the caller in place of the built-in
/// writers. See [`ArrowWriterOptions::with_custom_writer`].
///
/// The writer owns its sink, a [`ParquetObjectWriter`], and hands it back through
/// [`into_inner`](Self::into_inner) after a failure so the upload can be aborted, the same
/// contract as arrow-rs's `AsyncArrowWriter::into_inner`.
///
/// The size figures drive the partition writer: a file rolls once `bytes_written` plus
/// `in_progress_size` reaches the target file size, the upload budget reserves `memory_size`
/// plus the last of `flushed_row_groups` when a file closes, and the row-group-aligned roll
/// slices on `in_progress_rows`. A writer without a figure returns `0` (or `&[]`); the
/// partition writer then only reacts to flushed row groups.
#[async_trait::async_trait]
pub trait CustomArrowWriter: Send {
    /// Encode `batch` into the file, closing row groups as the writer's own limits dictate.
    async fn write(&mut self, batch: &RecordBatch) -> ParquetResult<()>;

    /// Encode the last row group and the footer, flush everything to the sink and complete
    /// it. Returns the metadata the file's Add action is built from.
    async fn finish(&mut self) -> ParquetResult<ParquetMetaData>;

    /// Give the sink back so its upload can be aborted; the open row group is dropped.
    fn into_inner(self: Box<Self>) -> ParquetObjectWriter;

    /// Bytes of complete row groups, and after `finish` the footer, handed to the sink.
    fn bytes_written(&self) -> usize;

    /// Anticipated encoded size of the open row group.
    fn in_progress_size(&self) -> usize;

    /// Memory the open row group's encoders hold.
    fn memory_size(&self) -> usize;

    /// Rows buffered in the open row group.
    fn in_progress_rows(&self) -> usize;

    /// Metadata of the row groups written so far.
    fn flushed_row_groups(&self) -> &[RowGroupMetaData];
}

/// Creates the [`CustomArrowWriter`] of each data file of a write.
pub trait CustomArrowWriterFactory: Send + Sync + Debug {
    /// A writer for one file that sends its bytes to `sink`. `schema` is the file schema,
    /// without the partition columns; `properties` and `options` are the settings of the
    /// write, which [`ArrowWriterOptions::to_parquet_options`] turns into arrow-rs's options.
    fn create(
        &self,
        sink: ParquetObjectWriter,
        schema: SchemaRef,
        properties: WriterProperties,
        options: &ArrowWriterOptions,
    ) -> ParquetResult<Box<dyn CustomArrowWriter>>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use delta_kernel::table_properties::DataSkippingNumIndexedCols;
    use indexmap::IndexMap;
    use object_store::ObjectStoreExt as _;
    use object_store::path::Path;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::DeltaTableBuilder;
    use crate::datafile::writer::{PartitionWriter, PartitionWriterConfig};
    use crate::table::config::DEFAULT_NUM_INDEX_COLS;
    use crate::writer::test_utils::get_record_batch;

    /// arrow-rs's writer behind the trait, counting the batches it receives.
    struct CountingWriter {
        inner: AsyncArrowWriter<ParquetObjectWriter>,
        batches: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl CustomArrowWriter for CountingWriter {
        async fn write(&mut self, batch: &RecordBatch) -> ParquetResult<()> {
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.inner.write(batch).await
        }

        async fn finish(&mut self) -> ParquetResult<ParquetMetaData> {
            self.inner.finish().await
        }

        fn into_inner(self: Box<Self>) -> ParquetObjectWriter {
            self.inner.into_inner()
        }

        fn bytes_written(&self) -> usize {
            self.inner.bytes_written()
        }

        fn in_progress_size(&self) -> usize {
            self.inner.in_progress_size()
        }

        fn memory_size(&self) -> usize {
            self.inner.memory_size()
        }

        fn in_progress_rows(&self) -> usize {
            self.inner.in_progress_rows()
        }

        fn flushed_row_groups(&self) -> &[RowGroupMetaData] {
            self.inner.flushed_row_groups()
        }
    }

    #[derive(Debug, Default)]
    struct CountingFactory {
        files: AtomicUsize,
        batches: Arc<AtomicUsize>,
    }

    impl CustomArrowWriterFactory for CountingFactory {
        fn create(
            &self,
            sink: ParquetObjectWriter,
            schema: SchemaRef,
            properties: WriterProperties,
            options: &ArrowWriterOptions,
        ) -> ParquetResult<Box<dyn CustomArrowWriter>> {
            self.files.fetch_add(1, Ordering::Relaxed);
            let inner = AsyncArrowWriter::try_new_with_options(
                sink,
                schema,
                options.to_parquet_options(properties),
            )?;
            Ok(Box::new(CountingWriter {
                inner,
                batches: Arc::clone(&self.batches),
            }))
        }
    }

    #[tokio::test]
    async fn test_custom_writer_writes_the_file() {
        let batch = get_record_batch(None, false);
        let object_store = DeltaTableBuilder::from_url(url::Url::parse("memory:///").unwrap())
            .unwrap()
            .build_storage()
            .unwrap()
            .object_store();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let factory = Arc::new(CountingFactory::default());
        let config = PartitionWriterConfig::try_new(
            batch.schema(),
            IndexMap::new(),
            Some(properties),
            Some(ArrowWriterOptions::new().with_custom_writer(factory.clone())),
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let mut writer = PartitionWriter::try_with_config(
            object_store.clone(),
            config,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            None,
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        let adds = writer.close().await.unwrap();
        assert_eq!(adds.len(), 1);

        // Without a target file size the partition writer slices at row group boundaries.
        assert_eq!(factory.files.load(Ordering::Relaxed), 1);
        assert_eq!(
            factory.batches.load(Ordering::Relaxed),
            batch.num_rows().div_ceil(3)
        );

        let bytes = object_store
            .get(&Path::from(adds[0].path.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert!(reader.metadata().num_row_groups() > 1);
        let read: Vec<RecordBatch> = reader.build().unwrap().map(Result::unwrap).collect();
        let read = arrow::compute::concat_batches(&batch.schema(), &read).unwrap();
        assert_eq!(read, batch);
    }
}

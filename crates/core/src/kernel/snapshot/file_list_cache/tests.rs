use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use arrow::compute::{concat_batches, sort_to_indices, take_record_batch};
use delta_kernel::Version;
use futures::TryStreamExt;
use tempfile::TempDir;

use super::entry::{Manifest, Tail};
use super::file_key::{FileKey, FileKeySet};
use super::foyer_store::FoyerStore;
use super::store::memory::MemoryStore;
use super::*;
use crate::checkpoints::create_checkpoint;
use crate::kernel::transaction::{CommitBuilder, TableReference};
use crate::kernel::{Action, DataType, PrimitiveType, Remove, StructField};
use crate::operations::create::CreateBuilder;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::test_utils::{TestResult, make_test_add, open_fs_path};
use crate::{DeltaTable, DeltaTableBuilder};

/// Entries small enough that a table with some hundred files needs many chunks.
const SMALL: Limits = Limits {
    first_chunk_rows: 16,
    target_chunk_bytes: 16 * 1024,
    max_entry_bytes: 64 * 1024,
};

fn cache(store: &Arc<MemoryStore>, limits: Limits) -> FileListCache {
    FileListCache {
        limits,
        ..FileListCache::new(Arc::clone(store) as Arc<dyn CacheStore>)
    }
}

async fn new_table() -> TestResult<(TempDir, DeltaTable)> {
    let dir = tempfile::tempdir()?;
    let table = CreateBuilder::new()
        .with_location(dir.path().to_string_lossy())
        .with_columns([StructField::new(
            "id",
            DataType::Primitive(PrimitiveType::Integer),
            true,
        )])
        .await?;
    Ok((dir, table))
}

fn add(path: &str) -> Action {
    add_with_rows(path, 10)
}

fn add_with_rows(path: &str, rows: i64) -> Action {
    let mut add = make_test_add(path, &[], 0);
    add.size = 100;
    add.stats = Some(format!(
        r#"{{"numRecords":{rows},"minValues":{{"id":1}},"maxValues":{{"id":{rows}}},"nullCount":{{"id":0}}}}"#
    ));
    Action::Add(add)
}

fn remove(path: &str) -> Action {
    Action::Remove(Remove {
        path: path.to_string(),
        data_change: true,
        deletion_timestamp: Some(1),
        extended_file_metadata: None,
        partition_values: None,
        size: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
    })
}

async fn commit(table: &mut DeltaTable, actions: Vec<Action>) -> TestResult<Version> {
    let version = CommitBuilder::default()
        .with_actions(actions)
        .build(
            table
                .state
                .as_ref()
                .map(|state| state as &dyn TableReference),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await?
        .version();
    table.update_state().await?;
    Ok(version)
}

/// Commits `count` files with the names `{prefix}-{index}`, some in each of `commits` commits.
async fn commit_files(
    table: &mut DeltaTable,
    prefix: &str,
    count: usize,
    commits: usize,
) -> TestResult {
    let names: Vec<String> = (0..count)
        .map(|i| format!("{prefix}-{i}.parquet"))
        .collect();
    for names in names.chunks(count.div_ceil(commits)) {
        commit(table, names.iter().map(|name| add(name)).collect()).await?;
    }
    Ok(())
}

async fn snapshot_at(table: &DeltaTable, version: Option<Version>) -> TestResult<Snapshot> {
    Ok(Snapshot::try_new(table.log_store().as_ref(), version).await?)
}

async fn replayed(table: &DeltaTable, snapshot: &Snapshot) -> TestResult<Vec<RecordBatch>> {
    Ok(snapshot
        .files_with_engine_preserving_raw(table.log_store().engine(), None)
        .try_collect()
        .await?)
}

async fn cached(
    cache: &FileListCache,
    table: &DeltaTable,
    snapshot: &Snapshot,
) -> TestResult<Vec<RecordBatch>> {
    Ok(cache
        .files(snapshot, table.log_store().as_ref())
        .try_collect()
        .await?)
}

fn one_batch(batches: &[RecordBatch]) -> Option<RecordBatch> {
    let batches: Vec<_> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    let first = batches.first()?;
    Some(concat_batches(&first.schema(), batches.iter().copied()).unwrap())
}

fn sorted_by_path(batches: &[RecordBatch]) -> Option<RecordBatch> {
    let batch = one_batch(batches)?;
    let order = sort_to_indices(batch.column_by_name("path").unwrap(), None, None).unwrap();
    Some(take_record_batch(&batch, &order).unwrap())
}

fn paths(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let paths = batch.column_by_name("path").unwrap().as_string::<i32>();
            paths
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Reads the newest version through the cache and compares all columns of all rows, and
/// the row order, with a log replay.
async fn assert_read_equals_replay(cache: &FileListCache, table: &DeltaTable) -> TestResult {
    let snapshot = snapshot_at(table, None).await?;
    assert_snapshot_read_equals_replay(cache, table, &snapshot).await
}

async fn assert_snapshot_read_equals_replay(
    cache: &FileListCache,
    table: &DeltaTable,
    snapshot: &Snapshot,
) -> TestResult {
    let expected = replayed(table, snapshot).await?;
    let actual = cached(cache, table, snapshot).await?;
    assert_eq!(paths(&actual), paths(&expected), "paths and their order");
    assert_eq!(one_batch(&actual), one_batch(&expected), "all columns");
    Ok(())
}

async fn manifest(store: &MemoryStore, table: &DeltaTable) -> TestResult<Option<Manifest>> {
    let snapshot = snapshot_at(table, None).await?;
    let key = TableKey::new(&snapshot).manifest();
    Ok(match store.get(&key).await? {
        Some(data) => Some(Manifest::decode(&data)?),
        None => None,
    })
}

async fn base_version(store: &MemoryStore, table: &DeltaTable) -> TestResult<Option<Version>> {
    Ok(manifest(store, table)
        .await?
        .map(|manifest| manifest.base.version))
}

async fn tail_version(store: &MemoryStore, table: &DeltaTable) -> TestResult<Option<Version>> {
    let snapshot = snapshot_at(table, None).await?;
    let Some(manifest) = manifest(store, table).await? else {
        return Ok(None);
    };
    let key = TableKey::new(&snapshot).tail(&manifest.base);
    Ok(match store.get(&key).await? {
        Some(data) => Some(Tail::decode(data)?.through_version),
        None => None,
    })
}

#[test]
fn file_key_set_keeps_keys_with_and_without_deletion_vector_apart() {
    let key = |path: &str, dv: Option<&str>| FileKey {
        path: path.to_string(),
        dv: dv.map(str::to_string),
    };
    let mut keys = FileKeySet::default();
    keys.insert(key("a", None));
    keys.insert(key("a", Some("dv1")));
    keys.insert(key("a", Some("dv1")));

    assert_eq!(keys.iter().count(), 2);
    assert!(keys.contains("a", None));
    assert!(keys.contains("a", Some("dv1")));
    assert!(!keys.contains("a", Some("dv2")));
    assert!(!keys.contains("b", None));
}

#[tokio::test]
async fn tail_encoding_round_trip() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 3, 1).await?;
    let snapshot = snapshot_at(&table, None).await?;
    let tail = Tail {
        through_version: 7,
        adds: replayed(&table, &snapshot).await?,
        dropped: [FileKey {
            path: "gone.parquet".into(),
            dv: Some("dv".into()),
        }]
        .into_iter()
        .collect(),
    };

    let decoded = Tail::decode(tail.encode()?.into())?;

    assert_eq!(decoded.through_version, 7);
    assert_eq!(decoded.adds, tail.adds);
    assert!(decoded.dropped.contains("gone.parquet", Some("dv")));
    assert!(Tail::decode(bytes::Bytes::from_static(b"abc")).is_err());
    Ok(())
}

#[tokio::test]
async fn first_read_writes_a_base_and_the_next_read_uses_it() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 300, 3).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);

    assert_read_equals_replay(&cache, &table).await?;

    let manifest = manifest(&store, &table).await?.expect("a base");
    assert_eq!(manifest.base.version, table.version().unwrap());
    assert_eq!(manifest.rows, 300);
    assert!(manifest.chunks > 3, "chunks: {}", manifest.chunks);
    // The manifest and the chunks.
    assert_eq!(store.keys().len(), manifest.chunks as usize + 1);

    let puts = store.puts();
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(
        store.puts(),
        puts,
        "a read at the base version writes nothing"
    );
    Ok(())
}

#[tokio::test]
async fn empty_table() -> TestResult {
    let (_dir, table) = new_table().await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);

    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(manifest(&store, &table).await?.expect("a base").chunks, 0);
    assert_read_equals_replay(&cache, &table).await?;
    Ok(())
}

#[tokio::test]
async fn read_at_a_newer_version_extends_the_tail() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "base", 50, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;
    let base = table.version().unwrap();

    // New files, a removed base file, and a new file that a later commit removes.
    commit(&mut table, vec![add("new-1.parquet"), add("new-2.parquet")]).await?;
    commit(
        &mut table,
        vec![remove("base-7.parquet"), add("new-3.parquet")],
    )
    .await?;
    commit(&mut table, vec![remove("new-1.parquet")]).await?;
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(base_version(&store, &table).await?, Some(base));
    assert_eq!(tail_version(&store, &table).await?, table.version());

    // The next read starts from the stored tail.
    commit(
        &mut table,
        vec![remove("new-3.parquet"), add("new-4.parquet")],
    )
    .await?;
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(base_version(&store, &table).await?, Some(base));
    assert_eq!(tail_version(&store, &table).await?, table.version());

    let puts = store.puts();
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(
        store.puts(),
        puts,
        "a read at the tail version writes nothing"
    );
    Ok(())
}

#[tokio::test]
async fn a_path_that_is_added_again_replaces_the_base_row() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit(&mut table, vec![add("a.parquet"), add("b.parquet")]).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;

    commit(&mut table, vec![add_with_rows("a.parquet", 99)]).await?;

    assert_read_equals_replay(&cache, &table).await?;
    let snapshot = snapshot_at(&table, None).await?;
    assert_eq!(paths(&cached(&cache, &table, &snapshot).await?).len(), 2);
    Ok(())
}

#[tokio::test]
async fn paths_with_special_characters() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    let odd = ["x y/a b.parquet", "x%20y/a%25b.parquet", "ä=ö/ü+1.parquet"];
    commit(&mut table, odd.iter().map(|path| add(path)).collect()).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;

    commit(&mut table, vec![remove(odd[0]), remove(odd[1])]).await?;

    assert_read_equals_replay(&cache, &table).await?;
    let snapshot = snapshot_at(&table, None).await?;
    assert_eq!(paths(&cached(&cache, &table, &snapshot).await?).len(), 1);
    Ok(())
}

#[tokio::test]
async fn deletion_vector_update_replaces_the_base_row() -> TestResult {
    // Version 0 adds one file. Version 1 removes it and adds it with a deletion vector.
    let table = open_fs_path("../test/tests/data/table-with-dv-small");
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);

    let first = snapshot_at(&table, Some(0)).await?;
    assert_snapshot_read_equals_replay(&cache, &table, &first).await?;
    let second = snapshot_at(&table, Some(1)).await?;
    assert_snapshot_read_equals_replay(&cache, &table, &second).await?;

    let files = cached(&cache, &table, &second).await?;
    assert_eq!(paths(&files).len(), 1);
    let dv = one_batch(&files).unwrap();
    assert_eq!(dv.column_by_name("deletionVector").unwrap().null_count(), 0);
    assert_eq!(base_version(&store, &table).await?, Some(0));
    Ok(())
}

#[tokio::test]
async fn read_of_an_older_version_does_not_change_the_cache() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 20, 4).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);

    // Base at version 2, tail to version 4.
    let base = snapshot_at(&table, Some(2)).await?;
    assert_snapshot_read_equals_replay(&cache, &table, &base).await?;
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(base_version(&store, &table).await?, Some(2));
    assert_eq!(tail_version(&store, &table).await?, Some(4));
    let puts = store.puts();

    // Older than the base.
    let older = snapshot_at(&table, Some(1)).await?;
    assert_snapshot_read_equals_replay(&cache, &table, &older).await?;
    // Between the base and the tail.
    let between = snapshot_at(&table, Some(3)).await?;
    assert_snapshot_read_equals_replay(&cache, &table, &between).await?;

    assert_eq!(store.puts(), puts);
    assert_eq!(base_version(&store, &table).await?, Some(2));
    assert_eq!(tail_version(&store, &table).await?, Some(4));
    Ok(())
}

#[tokio::test]
async fn metadata_change_writes_a_new_base() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 20, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;

    let mut table = table
        .set_tbl_properties()
        .with_properties(HashMap::from([(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "0".to_string(),
        )]))
        .await?;
    commit(&mut table, vec![add("after.parquet")]).await?;

    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(base_version(&store, &table).await?, table.version());
    assert_read_equals_replay(&cache, &table).await?;
    Ok(())
}

#[tokio::test]
async fn checkpoint_after_the_base() -> TestResult {
    let (dir, mut table) = new_table().await?;
    commit_files(&mut table, "old", 20, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;
    let base = table.version().unwrap();

    commit(
        &mut table,
        vec![remove("old-3.parquet"), add("mid.parquet")],
    )
    .await?;
    create_checkpoint(&table).await?;
    commit(
        &mut table,
        vec![remove("old-4.parquet"), add("new.parquet")],
    )
    .await?;

    // A new table instance starts from the checkpoint. It cannot read the commits between
    // the base and the checkpoint, so the read replays the log and writes a new base.
    let url = url::Url::from_directory_path(dir.path()).unwrap();
    let mut reopened = DeltaTableBuilder::from_url(url)?.load().await?;
    let snapshot = snapshot_at(&reopened, None).await?;
    assert!(snapshot.checkpoint_version().is_some_and(|v| v > base));
    assert_read_equals_replay(&cache, &reopened).await?;
    assert_eq!(base_version(&store, &reopened).await?, reopened.version());

    commit(&mut reopened, vec![add("newest.parquet")]).await?;
    assert_read_equals_replay(&cache, &reopened).await?;
    assert_eq!(tail_version(&store, &reopened).await?, reopened.version());
    Ok(())
}

#[tokio::test]
async fn a_tail_that_is_too_large_becomes_a_new_base() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "base", 100, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;
    let first_base = manifest(&store, &table).await?.expect("a base");

    commit(&mut table, vec![remove("base-1.parquet")]).await?;
    commit_files(&mut table, "tail", 400, 4).await?;

    assert_read_equals_replay(&cache, &table).await?;
    let second_base = manifest(&store, &table).await?.expect("a base");
    assert_ne!(second_base.base, first_base.base);
    assert_eq!(second_base.base.version, table.version().unwrap());
    assert_eq!(second_base.rows, 499);

    let puts = store.puts();
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(store.puts(), puts);
    Ok(())
}

#[tokio::test]
async fn lost_first_chunk_writes_a_new_base() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 200, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;
    let lost = manifest(&store, &table).await?.expect("a base");
    let snapshot = snapshot_at(&table, None).await?;
    store.forget(&TableKey::new(&snapshot).chunk(&lost.base, 0));

    assert_read_equals_replay(&cache, &table).await?;

    let new = manifest(&store, &table).await?.expect("a base");
    assert_ne!(new.base, lost.base);
    assert_read_equals_replay(&cache, &table).await?;
    Ok(())
}

#[tokio::test]
async fn lost_later_chunk_reads_the_other_rows_from_the_log() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "base", 200, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    assert_read_equals_replay(&cache, &table).await?;
    commit(
        &mut table,
        vec![remove("base-150.parquet"), add("new.parquet")],
    )
    .await?;
    assert_read_equals_replay(&cache, &table).await?;

    let lost = manifest(&store, &table).await?.expect("a base");
    assert!(lost.chunks > 4);
    let snapshot = snapshot_at(&table, None).await?;
    store.forget(&TableKey::new(&snapshot).chunk(&lost.base, lost.chunks / 2));

    // The read has returned the rows of the first chunks when it finds the lost chunk. It
    // must return each other row one time.
    let expected = replayed(&table, &snapshot).await?;
    let actual = cached(&cache, &table, &snapshot).await?;
    assert_eq!(paths(&actual).len(), 200);
    assert_eq!(sorted_by_path(&actual), sorted_by_path(&expected));
    assert_eq!(manifest(&store, &table).await?, None);

    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(base_version(&store, &table).await?, table.version());
    Ok(())
}

#[tokio::test]
async fn a_read_that_stops_early_writes_no_base() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 200, 2).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = cache(&store, SMALL);
    let snapshot = snapshot_at(&table, None).await?;

    let mut files = cache.files(&snapshot, table.log_store().as_ref());
    assert!(files.try_next().await?.is_some());
    drop(files);

    assert_eq!(manifest(&store, &table).await?, None);
    // The stopped read does not keep later reads from writing the base.
    assert_read_equals_replay(&cache, &table).await?;
    assert_eq!(base_version(&store, &table).await?, table.version());
    Ok(())
}

#[tokio::test]
async fn snapshot_reads_without_a_predicate_use_the_cache() -> TestResult {
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 30, 3).await?;
    let store = Arc::new(MemoryStore::default());
    let cache = Arc::new(cache(&store, SMALL));
    let expected_uris: Vec<String> = table.get_file_uris().await?.collect();
    let expected_files = table.eager_snapshot().await?;
    assert_eq!(store.puts(), 0);

    SCOPED
        .scope(Arc::clone(&cache), async {
            let uris: Vec<String> = table.get_file_uris().await?.collect();
            assert_eq!(uris, expected_uris);
            assert!(store.puts() > 0, "the first read writes the base");
            let puts = store.puts();

            let eager = table.eager_snapshot().await?;
            let files = |eager: &crate::kernel::EagerSnapshot| {
                one_batch(&eager.snapshot().materialized_files().unwrap().batches)
            };
            assert_eq!(files(&eager), files(&expected_files));

            let snapshot = snapshot_at(&table, None).await?;
            let views: Vec<_> = snapshot
                .file_views(table.log_store().as_ref(), None)
                .try_collect()
                .await?;
            assert_eq!(views.len(), 30);
            assert_eq!(views[0].num_records(), Some(10));
            assert_eq!(store.puts(), puts, "later reads only read");
            TestResult::Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn foyer_store_keeps_entries_on_disk() -> TestResult {
    let dir = tempfile::tempdir()?;
    let config = FileListCacheConfig::new(1024 * 1024)
        .with_directory(dir.path())
        .with_disk_bytes(256 * 1024 * 1024);
    let value = bytes::Bytes::from(vec![7u8; 4 * 1024 * 1024]);

    let store = FoyerStore::try_new(&config).await?;
    // More than the memory tier can hold.
    for index in 0..8 {
        store.put(format!("key-{index}"), value.clone()).await?;
    }
    for index in 0..8 {
        assert_eq!(
            store.get(&format!("key-{index}")).await?,
            Some(value.clone())
        );
    }
    store.remove("key-0").await?;
    assert_eq!(store.get("key-0").await?, None);
    assert_eq!(store.get("other").await?, None);
    store.close().await;

    // The entries are still there for a new store on the same directory.
    let store = FoyerStore::try_new(&config).await?;
    assert_eq!(store.get("key-1").await?, Some(value));
    store.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cache_on_a_foyer_store() -> TestResult {
    let dir = tempfile::tempdir()?;
    let config = FileListCacheConfig::new(1024 * 1024)
        .with_directory(dir.path())
        .with_disk_bytes(256 * 1024 * 1024);
    let cache = FileListCache::new(Arc::new(FoyerStore::try_new(&config).await?));
    let (_dir, mut table) = new_table().await?;
    commit_files(&mut table, "f", 300, 3).await?;

    assert_read_equals_replay(&cache, &table).await?;
    commit(&mut table, vec![remove("f-1.parquet"), add("new.parquet")]).await?;
    assert_read_equals_replay(&cache, &table).await?;
    assert_read_equals_replay(&cache, &table).await?;
    cache.store.close().await;
    Ok(())
}

#[test]
fn config_from_env_needs_the_memory_size() {
    // Other tests do not set these variables.
    assert_eq!(FileListCacheConfig::from_env().unwrap(), None);
}

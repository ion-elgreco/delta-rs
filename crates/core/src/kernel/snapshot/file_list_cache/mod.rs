//! A cache of the active file list of a table.
//!
//! A [`Snapshot`] does not keep the active files in memory. Each read of the file list
//! replays the log. This cache keeps the result of a replay outside the snapshot, so a later
//! read, also from a new snapshot or a new table instance, does not replay the log again.
//!
//! The cache has three kinds of entries for each table:
//!
//! - The base: the active files at one table version, in chunks. A base never changes.
//! - The tail: the files that the commits after the base added, and the keys of the files
//!   these commits added or removed. A read at a newer version reads only the commits the
//!   tail does not have yet.
//! - The manifest: it names the current base.
//!
//! A read returns the files the tail added, and then the base rows whose key is not in the
//! tail. When the tail becomes too large for one entry, the read also writes its result as
//! the new base.
//!
//! The cache serves reads of the complete file list. A read with a predicate replays the log.
//!
//! The cache is off until [`install_file_list_cache`] is called.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use delta_kernel::Engine;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use tracing::warn;

use self::base_writer::BaseWriter;
use self::entry::{Manifest, Tail};
use self::key::TableKey;
use self::merge::merged_files;
use self::store::CacheStore;
use self::tail::extend_tail;
use super::{SendableRBStream, Snapshot};
use crate::DeltaResult;
use crate::logstore::LogStore;

pub use self::foyer_store::FileListCacheConfig;

mod base_writer;
mod entry;
mod file_key;
mod foyer_store;
mod key;
mod merge;
mod store;
mod tail;
#[cfg(test)]
mod tests;

static INSTALLED: RwLock<Option<Arc<FileListCache>>> = RwLock::new(None);

#[cfg(test)]
tokio::task_local! {
    /// A cache for the reads of one test, so tests do not share a cache.
    static SCOPED: Arc<FileListCache>;
}

/// Turns the file list cache on for all tables of this process.
///
/// A cache with a directory needs a multi-thread Tokio runtime. One directory can serve one
/// process at a time.
///
/// The directory holds file paths and column statistics (minimum and maximum values) of the
/// tables in plain form.
pub async fn install_file_list_cache(config: FileListCacheConfig) -> DeltaResult<()> {
    let store = foyer_store::FoyerStore::try_new(&config).await?;
    let cache = Arc::new(FileListCache::new(Arc::new(store)));
    replace_installed(Some(cache)).await;
    Ok(())
}

/// Turns the file list cache off. Entries in the cache directory stay there.
pub async fn uninstall_file_list_cache() {
    replace_installed(None).await;
}

async fn replace_installed(cache: Option<Arc<FileListCache>>) {
    let previous = std::mem::replace(
        &mut *INSTALLED.write().expect("file list cache lock"),
        cache,
    );
    // A read that still uses the closed store gets errors from it and reads the log.
    if let Some(previous) = previous {
        previous.store.close().await;
    }
}

fn installed() -> Option<Arc<FileListCache>> {
    #[cfg(test)]
    if let Ok(cache) = SCOPED.try_with(Arc::clone) {
        return Some(cache);
    }
    INSTALLED.read().expect("file list cache lock").clone()
}

/// Streams the complete active file list of `snapshot` through the cache.
///
/// Returns `None` when the cache is off or cannot serve this table.
pub(super) fn cached_files(
    snapshot: &Snapshot,
    log_store: &dyn LogStore,
) -> Option<SendableRBStream> {
    let cache = installed()?;
    // The commits of a catalog-managed table are not always files in the log, and the tail
    // reads commit files.
    let catalog_managed = snapshot.table_configuration().is_catalog_managed();
    (!catalog_managed).then(|| cache.files(snapshot, log_store))
}

/// Size limits of cache entries.
#[derive(Debug, Clone, Copy)]
struct Limits {
    /// The row count of the first chunk of a base. Later chunks use the measured row size.
    first_chunk_rows: usize,
    /// The size the base writer aims at for a chunk.
    target_chunk_bytes: usize,
    /// The largest entry the store keeps. A chunk is never larger. A tail that is larger
    /// is not stored, and the read writes a new base.
    max_entry_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            first_chunk_rows: 4096,
            target_chunk_bytes: 4 * 1024 * 1024,
            max_entry_bytes: 8 * 1024 * 1024,
        }
    }
}

/// What a read needs from the snapshot and the log store, without their lifetimes.
struct ReadContext {
    snapshot: Snapshot,
    engine: Arc<dyn Engine>,
    object_store: Arc<dyn ObjectStore>,
}

impl ReadContext {
    /// The file list from a log replay, with the columns the cache stores.
    fn replay(&self) -> SendableRBStream {
        self.snapshot
            .files_with_engine_preserving_raw(self.engine.clone(), None)
    }
}

#[derive(Debug)]
struct FileListCache {
    store: Arc<dyn CacheStore>,
    limits: Limits,
    /// The tables for which a read writes a new base at this time.
    writing: Arc<Mutex<HashSet<TableKey>>>,
}

impl FileListCache {
    fn new(store: Arc<dyn CacheStore>) -> Self {
        Self {
            store,
            limits: Limits::default(),
            writing: Arc::default(),
        }
    }

    fn files(&self, snapshot: &Snapshot, log_store: &dyn LogStore) -> SendableRBStream {
        let read = Read {
            store: Arc::clone(&self.store),
            limits: self.limits,
            writing: Arc::clone(&self.writing),
            table: TableKey::new(snapshot),
            ctx: Arc::new(ReadContext {
                snapshot: snapshot.with_materialized_files(None),
                engine: log_store.engine(),
                object_store: log_store.object_store(),
            }),
        };
        futures::stream::once(read.open()).try_flatten().boxed()
    }
}

/// How a read gets the file list.
enum Plan {
    /// From the base and the tail. With `write_base`, the result becomes the new base.
    Cached {
        manifest: Box<Manifest>,
        tail: Tail,
        first_chunk: Option<Bytes>,
        write_base: bool,
    },
    /// From a log replay. With `write_base`, the result becomes the new base.
    Replay { write_base: bool },
}

/// One read of the file list of one snapshot.
struct Read {
    store: Arc<dyn CacheStore>,
    limits: Limits,
    writing: Arc<Mutex<HashSet<TableKey>>>,
    table: TableKey,
    ctx: Arc<ReadContext>,
}

impl Read {
    async fn open(self) -> DeltaResult<SendableRBStream> {
        let plan = match self.plan().await {
            Ok(plan) => plan,
            Err(err) => {
                warn!(error = %err, "the file list cache failed, reading the log instead");
                Plan::Replay { write_base: false }
            }
        };

        let (files, write_base) = match plan {
            Plan::Cached {
                manifest,
                tail,
                first_chunk,
                write_base,
            } => {
                let files = merged_files(
                    Arc::clone(&self.ctx),
                    Arc::clone(&self.store),
                    self.table.clone(),
                    *manifest,
                    tail,
                    first_chunk,
                );
                (files, write_base)
            }
            Plan::Replay { write_base } => (self.ctx.replay(), write_base),
        };
        if !write_base {
            return Ok(files);
        }
        // One read at a time writes the base of a table. The other reads only read.
        let Some(guard) = WriteGuard::try_new(&self.writing, &self.table) else {
            return Ok(files);
        };
        let writer = BaseWriter::new(
            self.store,
            self.table,
            self.ctx.snapshot.version(),
            self.limits,
        );
        Ok(write_base_from(files, writer, guard, self.ctx))
    }

    async fn plan(&self) -> DeltaResult<Plan> {
        let snapshot = &self.ctx.snapshot;
        let Some(data) = self.store.get(&self.table.manifest()).await? else {
            return Ok(Plan::Replay { write_base: true });
        };
        let Ok(manifest) = Manifest::decode(&data) else {
            return Ok(Plan::Replay { write_base: true });
        };
        if manifest.base.version > snapshot.version() {
            // A read of an older version must not replace the base of a newer version.
            return Ok(Plan::Replay { write_base: false });
        }
        if !manifest.has_layout_of(snapshot) {
            return Ok(Plan::Replay { write_base: true });
        }

        let first_chunk = if manifest.chunks > 0 {
            let key = self.table.chunk(&manifest.base, 0);
            match self.store.get(&key).await? {
                Some(data) => Some(data),
                None => return Ok(Plan::Replay { write_base: true }),
            }
        } else {
            None
        };
        let Some((tail, fits)) = self.tail(&manifest).await? else {
            return Ok(Plan::Replay { write_base: true });
        };
        Ok(Plan::Cached {
            manifest: Box::new(manifest),
            tail,
            first_chunk,
            write_base: !fits,
        })
    }

    /// Returns the tail for this snapshot, and if the tail fits in one cache entry.
    ///
    /// Returns `None` when the snapshot cannot read all commits after the base.
    async fn tail(&self, manifest: &Manifest) -> DeltaResult<Option<(Tail, bool)>> {
        let version = self.ctx.snapshot.version();
        if manifest.base.version == version {
            return Ok(Some((Tail::empty(version), true)));
        }

        let key = self.table.tail(&manifest.base);
        let cached = match self.store.get(&key).await? {
            Some(data) => Tail::decode(data).ok(),
            None => None,
        };
        let (start, is_newest) = match cached {
            Some(tail) if tail.through_version == version => return Ok(Some((tail, true))),
            Some(tail) if tail.through_version < version => (tail, true),
            // The cached tail is for a newer version than this snapshot. Build a tail only
            // for this read.
            Some(_) => (Tail::empty(manifest.base.version), false),
            None => (Tail::empty(manifest.base.version), true),
        };
        let Some(tail) = extend_tail(&self.ctx, start).await? else {
            return Ok(None);
        };
        if !is_newest {
            return Ok(Some((tail, true)));
        }
        let data = tail.encode()?;
        let fits = data.len() <= self.limits.max_entry_bytes;
        if fits {
            self.store.put(key, data.into()).await?;
        }
        Ok(Some((tail, fits)))
    }
}

/// Marks a table as "a read writes its base". The mark goes away when the guard drops.
struct WriteGuard {
    writing: Arc<Mutex<HashSet<TableKey>>>,
    table: TableKey,
}

impl WriteGuard {
    fn try_new(writing: &Arc<Mutex<HashSet<TableKey>>>, table: &TableKey) -> Option<Self> {
        writing
            .lock()
            .expect("file list cache lock")
            .insert(table.clone())
            .then(|| Self {
                writing: Arc::clone(writing),
                table: table.clone(),
            })
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        self.writing
            .lock()
            .expect("file list cache lock")
            .remove(&self.table);
    }
}

/// Passes `files` through and writes them to the cache as a new base.
///
/// The base exists after the caller has read the stream to its end. A failure to write
/// stops the write and does not fail the read.
fn write_base_from(
    files: SendableRBStream,
    writer: BaseWriter,
    guard: WriteGuard,
    ctx: Arc<ReadContext>,
) -> SendableRBStream {
    futures::stream::try_unfold(
        (files, Some(writer), guard, ctx),
        |(mut files, mut writer, guard, ctx)| async move {
            match files.try_next().await? {
                Some(batch) => {
                    if let Some(active) = &mut writer
                        && let Err(err) = active.push(&batch).await
                    {
                        warn!(error = %err, "failed to write the file list cache");
                        writer = None;
                    }
                    Ok(Some((batch, (files, writer, guard, ctx))))
                }
                None => {
                    if let Some(writer) = writer
                        && let Err(err) = writer.finish(&ctx.snapshot).await
                    {
                        warn!(error = %err, "failed to write the file list cache");
                    }
                    Ok(None)
                }
            }
        },
    )
    .boxed()
}

//! A [`CacheStore`] on a foyer cache: a memory tier, and a disk tier when a directory is set.

use std::path::PathBuf;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy,
};

use super::store::CacheStore;
use crate::{DeltaResult, DeltaTableError};

const ENV_MEMORY_BYTES: &str = "DELTA_FILE_LIST_CACHE_CAPACITY_BYTES";
const ENV_DIRECTORY: &str = "DELTA_CACHE_DIR";
const ENV_DISK_BYTES: &str = "DELTA_FILE_LIST_CACHE_DISK_CAPACITY_BYTES";
/// The directory below `DELTA_CACHE_DIR` that this cache uses.
const ENV_SUBDIRECTORY: &str = "file-list";

/// The disk space the cache uses when the configuration gives none.
///
/// foyer's own default is 80% of the free space.
const DEFAULT_DISK_BYTES: usize = 8 * 1024 * 1024 * 1024;
/// foyer keeps one open file for each block of disk space. Blocks of this size keep the
/// number of open files low: 16 files for each GiB.
const DISK_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Configuration of the file list cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileListCacheConfig {
    memory_bytes: usize,
    directory: Option<PathBuf>,
    disk_bytes: Option<usize>,
}

impl FileListCacheConfig {
    /// A cache that keeps up to `memory_bytes` of entries in memory.
    ///
    /// Without a directory, the cache keeps a table only when its file list fits in
    /// `memory_bytes`. A file list needs about 600 bytes for each file.
    pub fn new(memory_bytes: usize) -> Self {
        Self {
            memory_bytes,
            directory: None,
            disk_bytes: None,
        }
    }

    /// Also keep entries in `directory`. Entries there stay available after a restart.
    pub fn with_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.directory = Some(directory.into());
        self
    }

    /// The disk space the cache can use in the directory. The default is 8 GiB.
    ///
    /// The cache keeps 16 files open for each GiB.
    pub fn with_disk_bytes(mut self, disk_bytes: usize) -> Self {
        self.disk_bytes = Some(disk_bytes);
        self
    }

    /// Reads the configuration from the environment.
    ///
    /// - `DELTA_FILE_LIST_CACHE_CAPACITY_BYTES`: memory for the cache. The cache is on only
    ///   when this is set to a value larger than zero.
    /// - `DELTA_CACHE_DIR`: the cache keeps entries in the `file-list` directory below it.
    /// - `DELTA_FILE_LIST_CACHE_DISK_CAPACITY_BYTES`: the disk space the cache can use.
    pub fn from_env() -> DeltaResult<Option<Self>> {
        let Some(memory_bytes) = env_bytes(ENV_MEMORY_BYTES)?.filter(|bytes| *bytes > 0) else {
            return Ok(None);
        };
        let mut config = Self::new(memory_bytes);
        if let Some(directory) = std::env::var_os(ENV_DIRECTORY) {
            config = config.with_directory(PathBuf::from(directory).join(ENV_SUBDIRECTORY));
        }
        if let Some(disk_bytes) = env_bytes(ENV_DISK_BYTES)? {
            config = config.with_disk_bytes(disk_bytes);
        }
        Ok(Some(config))
    }
}

fn env_bytes(name: &str) -> DeltaResult<Option<usize>> {
    match std::env::var(name) {
        Ok(value) => value.trim().parse().map(Some).map_err(|_| {
            DeltaTableError::Generic(format!("{name} must be a number of bytes, got `{value}`"))
        }),
        Err(_) => Ok(None),
    }
}

fn cache_error(err: impl std::fmt::Display) -> DeltaTableError {
    DeltaTableError::Generic(format!("file list cache: {err}"))
}

/// A cache value. foyer stores it with serde, and this type writes the bytes as one block.
#[derive(Debug, Clone)]
struct Value(Bytes);

impl serde::Serialize for Value {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl serde::de::Visitor<'_> for BytesVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Value, E> {
                Ok(Value(Bytes::copy_from_slice(value)))
            }

            fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<Value, E> {
                Ok(Value(value.into()))
            }
        }

        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

pub(super) struct FoyerStore {
    cache: HybridCache<String, Value>,
}

impl std::fmt::Debug for FoyerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerStore").finish_non_exhaustive()
    }
}

impl FoyerStore {
    pub(super) async fn try_new(config: &FileListCacheConfig) -> DeltaResult<Self> {
        let builder = HybridCacheBuilder::new()
            .with_name("delta-file-list")
            // Write to disk at once, so an entry does not depend on the memory tier.
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(config.memory_bytes)
            .with_weighter(|key: &String, value: &Value| key.len() + value.0.len())
            .storage();
        let builder = match &config.directory {
            Some(directory) => {
                if tokio::runtime::Handle::current().runtime_flavor()
                    == tokio::runtime::RuntimeFlavor::CurrentThread
                {
                    return Err(cache_error(
                        "a cache directory needs a multi-thread Tokio runtime",
                    ));
                }
                std::fs::create_dir_all(directory).map_err(cache_error)?;
                let device = FsDeviceBuilder::new(directory)
                    .with_capacity(config.disk_bytes.unwrap_or(DEFAULT_DISK_BYTES))
                    .build()
                    .map_err(cache_error)?;
                builder.with_engine_config(
                    BlockEngineConfig::new(device).with_block_size(DISK_BLOCK_BYTES),
                )
            }
            None => builder,
        };
        let cache = builder.build().await.map_err(cache_error)?;
        Ok(Self { cache })
    }
}

#[async_trait::async_trait]
impl CacheStore for FoyerStore {
    async fn get(&self, key: &str) -> DeltaResult<Option<Bytes>> {
        let entry = self.cache.get(key).await.map_err(cache_error)?;
        Ok(entry.map(|entry| entry.value().0.clone()))
    }

    async fn put(&self, key: String, value: Bytes) -> DeltaResult<()> {
        self.cache.insert(key, Value(value));
        // foyer drops entries when writes come faster than it can flush them to disk.
        // Wait for the flush, so the next write does not push this one out.
        self.cache.storage().wait().await;
        Ok(())
    }

    async fn remove(&self, key: &str) -> DeltaResult<()> {
        self.cache.remove(key);
        Ok(())
    }

    async fn close(&self) {
        if let Err(err) = self.cache.close().await {
            tracing::warn!(error = %err, "failed to close the file list cache");
        }
    }
}

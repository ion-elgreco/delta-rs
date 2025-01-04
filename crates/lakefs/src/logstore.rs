//! Default implementation of [`LakeFSLogStore`] for LakeFS

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use deltalake_core::logstore::*;
use deltalake_core::storage::{
    commit_uri_from_version, DefaultObjectStoreRegistry, ObjectStoreRegistry,
};
use deltalake_core::{
    operations::transaction::TransactionError,
    storage::{ObjectStoreRef, StorageOptions},
    DeltaResult,
};
use object_store::{Attributes, Error as ObjectStoreError, ObjectStore, PutOptions, TagSet};
use url::Url;

/// Slim LakeFS client for lakefs branch operations.
// pub struct LakeFSClient {
//     /// DynamoDb client
//     lakefs_client: Client,
//     /// configuration of the
//     config: LakeFSConfig,
// }

/// Return the [LakeFSLogStore] implementation with the provided configuration options
pub fn lakefs_logstore(
    store: ObjectStoreRef,
    location: &Url,
    options: &StorageOptions,
) -> Arc<dyn LogStore> {
    Arc::new(LakeFSLogStore::new(
        store,
        LogStoreConfig {
            location: location.clone(),
            options: options.clone(),
        },
    ))
}

/// Default [`LogStore`] implementation
#[derive(Debug, Clone)]
pub struct LakeFSLogStore {
    pub(crate) storage: DefaultObjectStoreRegistry,
    config: LogStoreConfig,
    // client: LakeFSClient,
}

impl LakeFSLogStore {
    /// Create a new instance of [`LakeFSLogStore`]
    ///
    /// # Arguments
    ///
    /// * `storage` - A shared reference to an [`object_store::ObjectStore`] with "/" pointing at delta table root (i.e. where `_delta_log` is located).
    /// * `location` - A url corresponding to the storage location of `storage`.
    pub fn new(
        storage: ObjectStoreRef,
        config: LogStoreConfig,
        // client: LakeFSClient
    ) -> Self {
        let registry = DefaultObjectStoreRegistry::new();
        registry.register_store(&config.location, storage);
        Self {
            storage: registry,
            config,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for LakeFSLogStore {
    fn name(&self) -> String {
        "LakeFSLogStore".into()
    }

    fn register_object_store(&self, url: &Url, store: ObjectStoreRef) {
        self.storage.register_store(url, store);
    }

    async fn read_commit_entry(&self, version: i64) -> DeltaResult<Option<Bytes>> {
        read_commit_entry(&self.storage.get_store(&self.config.location)?, version).await
    }

    /// Tries to commit a prepared commit file. Returns [`TransactionError`]
    /// if the given `version` already exists. The caller should handle the retry logic itself.
    /// This is low-level transaction API. If user does not want to maintain the commit loop then
    /// the `DeltaTransaction.commit` is desired to be used as it handles `try_commit_transaction`
    /// with retry logic.
    async fn write_commit_entry(
        &self,
        version: i64,
        commit_or_bytes: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        match commit_or_bytes {
            CommitOrBytes::LogBytes(log_bytes) => self
                .object_store()
                .put_opts(
                    &commit_uri_from_version(version),
                    log_bytes.into(),
                    put_options().clone(),
                )
                .await
                .map_err(|err| -> TransactionError {
                    match err {
                        ObjectStoreError::AlreadyExists { .. } => {
                            TransactionError::VersionAlreadyExists(version)
                        }
                        _ => TransactionError::from(err),
                    }
                })?,
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        };
        Ok(())
    }

    async fn abort_commit_entry(
        &self,
        _version: i64,
        commit_or_bytes: CommitOrBytes,
    ) -> Result<(), TransactionError> {
        match &commit_or_bytes {
            CommitOrBytes::LogBytes(_) => Ok(()),
            _ => unreachable!(), // Default log store should never get a tmp_commit, since this is for conditional put stores
        }
    }

    async fn get_latest_version(&self, current_version: i64) -> DeltaResult<i64> {
        get_latest_version(self, current_version).await
    }

    async fn get_earliest_version(&self, current_version: i64) -> DeltaResult<i64> {
        get_earliest_version(self, current_version).await
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.storage
            .get_store(&Url::parse("transaction").unwrap())
            .expect("LakeFS Operation middleware not executed. Transaction store is missing.")
    }

    fn config(&self) -> &LogStoreConfig {
        &self.config
    }
}

fn put_options() -> &'static PutOptions {
    static PUT_OPTS: OnceLock<PutOptions> = OnceLock::new();
    PUT_OPTS.get_or_init(|| PutOptions {
        mode: object_store::PutMode::Create, // Creates if file doesn't exists yet
        tags: TagSet::default(),
        attributes: Attributes::default(),
    })
}

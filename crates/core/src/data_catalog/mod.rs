//! Catalog abstraction for Delta Table

use std::fmt::Debug;

use crate::DeltaResult;
use crate::logstore::{LogStoreRef, StorageConfig};

#[cfg(feature = "datafusion")]
pub mod storage;

/// A result type for data catalog implementations
pub type DataCatalogResult<T> = Result<T, DataCatalogError>;

/// Error enum that represents a CatalogError.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DataCatalogError {
    /// A generic error qualified in the message
    #[error("Error in {catalog} catalog: {source}")]
    Generic {
        /// Name of the catalog
        catalog: &'static str,
        /// Error message
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// Error representing an invalid Data Catalog.
    #[error("This data catalog doesn't exist: {data_catalog}")]
    InvalidDataCatalog {
        /// The catalog input
        data_catalog: String,
    },

    /// Unknown configuration key
    #[error("Unknown configuration key '{catalog}' in '{key}' catalog.")]
    UnknownConfigKey {
        /// Name of the catalog
        catalog: &'static str,

        /// configuration key
        key: String,
    },

    /// A request to the underlying catalog service failed.
    #[error("Error in request: {source}")]
    RequestError {
        /// The underlying transport or service error that caused the request to fail.
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

/// A catalog that opens the Delta tables that it knows by name.
///
/// The catalog gives the [`LogStore`](crate::logstore::LogStore) of a table: the location of the
/// table, an object store with the access that the catalog grants, and the commit rules of the
/// catalog. A table opens from that log store:
///
/// ```ignore
/// let log_store = catalog.log_store("main.sales.orders", StorageConfig::default()).await?;
/// let mut table = DeltaTable::new(log_store);
/// table.load().await?;
/// ```
#[async_trait::async_trait]
pub trait TableCatalog: Send + Sync + Debug {
    /// The log store of the table `name`, a dotted name such as `catalog.schema.table`.
    ///
    /// `storage_config` has the settings of the object store that the catalog does not give,
    /// such as timeouts and the IO runtime.
    async fn log_store(
        &self,
        name: &str,
        storage_config: StorageConfig,
    ) -> DeltaResult<LogStoreRef>;
}

//! LakFS storage backend (internally S3).

use deltalake_core::storage::object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use deltalake_core::storage::{
    limit_store_handler, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use std::fmt::Debug;
use std::str::FromStr;
use tracing::log::*;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct LakeFSObjectStoreFactory {}

impl LakeFSObjectStoreFactory {
    fn with_env_s3(&self, options: &StorageOptions) -> StorageOptions {
        let mut options = StorageOptions(
            options
                .0
                .clone()
                .into_iter()
                .map(|(k, v)| {
                    if let Ok(config_key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                        (config_key.as_ref().to_string(), v)
                    } else {
                        (k, v)
                    }
                })
                .collect(),
        );

        for (os_key, os_value) in std::env::vars_os() {
            if let (Some(key), Some(value)) = (os_key.to_str(), os_value.to_str()) {
                if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                    if !options.0.contains_key(config_key.as_ref()) {
                        options
                            .0
                            .insert(config_key.as_ref().to_string(), value.to_string());
                    }
                }
            }
        }

        // Conditional put is supported in LakeFS since v1.47
        if !options.0.keys().any(|key| {
            let key = key.to_ascii_lowercase();
            [
                AmazonS3ConfigKey::ConditionalPut.as_ref(),
                "conditional_put",
            ]
            .contains(&key.as_str())
        }) {
            options.0.insert("conditional_put".into(), "etag".into());
        }
        options
    }
}

impl ObjectStoreFactory for LakeFSObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        storage_options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let options = self.with_env_s3(storage_options);

        // Convert LakeFS URI to equivalent S3 URI.
        let s3_url = Url::parse(&format!("s3://{}", url.path()))
            .map_err(|_| DeltaTableError::InvalidTableLocation(url.clone().into()))?;

        // All S3-likes should start their builder the same way
        let mut builder = AmazonS3Builder::new().with_url(s3_url.to_string());

        for (key, value) in options.0.iter() {
            if let Ok(key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                builder = builder.with_config(key, value.clone());
            }
        }

        let inner = builder.build()?;

        let store = limit_store_handler(inner, &options);
        debug!("Initialized the object store: {store:?}");

        Ok((store, s3_url.path().into()))
    }
}

// TODO: ADD LakeFSObjectStore which can also work with unsafe_put for backwards compatible, this will be
// Just a bare put where we don't check if hte object exists. In theory this will be safe, since deltalake
// writer will never use the same transaction branch.

//! LakeFS storage backend (internally S3).

use deltalake_core::storage::object_store::aws::AmazonS3ConfigKey;
use deltalake_core::storage::{
    limit_store_handler, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, DeltaTableError, Path};
use object_store::parse_url_opts;
use std::collections::HashMap;
use std::fmt::Debug;
use std::str::FromStr;
use tracing::log::*;
use url::Url;

#[derive(Clone, Default, Debug)]
pub struct LakeFSObjectStoreFactory {}

pub(crate) trait S3StorageOptionsConversion {
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

impl S3StorageOptionsConversion for LakeFSObjectStoreFactory {}

impl ObjectStoreFactory for LakeFSObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        storage_options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let options = self.with_env_s3(storage_options);

        // Convert LakeFS URI to equivalent S3 URI.
        let s3_url = url.to_string().replace("lakefs://", "s3://");

        let s3_url = Url::parse(&s3_url)
            .map_err(|_| DeltaTableError::InvalidTableLocation(url.clone().into()))?;

        // All S3-likes should start their builder the same way
        let config = options
            .clone()
            .0
            .into_iter()
            .filter_map(|(k, v)| {
                if let Ok(key) = AmazonS3ConfigKey::from_str(&k.to_ascii_lowercase()) {
                    Some((key, v))
                } else {
                    None
                }
            })
            .collect::<HashMap<AmazonS3ConfigKey, String>>();
        let (inner, prefix) = parse_url_opts(&s3_url, config)?;
        let store = limit_store_handler(inner, &options);
        debug!("Initialized the object store: {store:?}");
        Ok((store, prefix))
    }
}

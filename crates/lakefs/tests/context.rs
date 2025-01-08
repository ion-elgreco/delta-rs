// #![cfg(feature = "integration_test")]
use deltalake_lakefs::register_handlers;
use deltalake_test::utils::*;
use std::{
    collections::HashSet,
    process::{Command, ExitStatus},
};

use which::which;

pub struct LakeFSIntegration {}

impl Default for LakeFSIntegration {
    fn default() -> Self {
        register_handlers(None);
        Self {}
    }
}

impl StorageIntegration for LakeFSIntegration {
    fn prepare_env(&self) {
        println!("Preparing env");

        set_env_if_not_set("endpoint", "http://127.0.0.1:8000");
        set_env_if_not_set("access_key_id", "LAKEFSID");
        set_env_if_not_set("secret_access_key", "LAKEFSKEY");
        set_env_if_not_set("allow_http", "true");
    }

    fn create_bucket(&self) -> std::io::Result<ExitStatus> {
        Ok(())
    }

    fn bucket_name(&self) -> String {
        "bronze"
    }

    fn root_uri(&self) -> String {
        format!("lakefs://{}/main", self.bucket_name())
    }

    fn copy_directory(&self, source: &str, destination: &str) -> std::io::Result<ExitStatus> {
        
    }
}
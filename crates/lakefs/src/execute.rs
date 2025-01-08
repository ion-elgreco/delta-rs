use async_trait::async_trait;
use deltalake_core::{
    logstore::LogStoreRef, operations::CustomExecuteHandler, DeltaResult, DeltaTableError,
};
use tracing::debug;
use uuid::Uuid;

use crate::logstore::LakeFSLogStore;

pub struct LakeFSCustomExecuteHandler {}

#[async_trait]
impl CustomExecuteHandler for LakeFSCustomExecuteHandler {
    // LakeFS Log store pre execution of delta operation (create branch, logs object store and transaction)
    async fn pre_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()> {
        debug!("Running LakeFS pre execution inside delta operation");
        if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>() {
            lakefs_store.pre_execute(operation_id).await
        } else {
            Err(DeltaTableError::generic(
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
            ))
        }
    }
    // Not required for LakeFS
    async fn post_execute(&self, log_store: &LogStoreRef, operation_id: Uuid) -> DeltaResult<()> {
        debug!("Running LakeFS post execution inside delta operation");
        if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>() {
            let (repo, _, _) = lakefs_store
                .client
                .decompose_url(lakefs_store.config.location.to_string());
            let result = lakefs_store
                .client
                .delete_branch(repo, lakefs_store.client.get_transaction(operation_id)?)
                .await
                .map_err(|e| DeltaTableError::Transaction { source: e });
            lakefs_store.client.clear_transaction(operation_id);
            result
        } else {
            Err(DeltaTableError::generic(
                "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
            ))
        }
    }

    // Execute arbitrary code at the start of the post commit hook
    async fn before_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operations: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()> {
        if file_operations {
            debug!("Running LakeFS pre execution inside post_commit_hook");
            if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>()
            {
                lakefs_store.pre_execute(operation_id).await
            } else {
                Err(DeltaTableError::generic(
                    "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
                ))
            }?;
        }
        Ok(())
    }

    // Execute arbitrary code at the end of the post commit hook
    async fn after_post_commit_hook(
        &self,
        log_store: &LogStoreRef,
        file_operations: bool,
        operation_id: Uuid,
    ) -> DeltaResult<()> {
        if file_operations {
            debug!("Running LakeFS post execution inside post_commit_hook");
            if let Some(lakefs_store) = log_store.clone().as_any().downcast_ref::<LakeFSLogStore>()
            {
                lakefs_store.commit_merge(operation_id).await
            } else {
                Err(DeltaTableError::generic(
                    "LakeFSPreEcuteHandler is used, but no LakeFSLogStore has been found",
                ))
            }?;
        }
        Ok(())
    }
}

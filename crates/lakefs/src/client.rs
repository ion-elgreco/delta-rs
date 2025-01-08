use dashmap::DashMap;
use deltalake_core::operations::transaction::TransactionError;
use deltalake_core::DeltaResult;
use reqwest::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::errors::LakeFSOperationError;

#[derive(Debug, Clone)]
pub struct LakeFSConfig {
    host: String,
    username: String,
    password: String,
}

impl LakeFSConfig {
    pub fn new(host: String, username: String, password: String) -> Self {
        LakeFSConfig {
            host,
            username,
            password,
        }
    }
}

/// Slim LakeFS client for lakefs branch operations.
#[derive(Debug, Clone)]
pub struct LakeFSClient {
    /// configuration of the lakefs client
    config: LakeFSConfig,
    http_client: Client,
    /// Holds the running delta lake operations, each operation propogates the operation ID into execution handler.
    transactions: DashMap<Uuid, String>,
}

impl LakeFSClient {
    pub fn with_config(config: LakeFSConfig) -> Self {
        let http_client = Client::new();
        Self {
            config,
            http_client,
            transactions: DashMap::new(),
        }
    }

    pub async fn create_branch(
        &self,
        source_url: &Url,
        operation_id: Uuid,
    ) -> DeltaResult<(Url, String)> {
        let (repo, source_branch, table) = self.decompose_url(source_url.to_string());

        let request_url = format!("{}/api/v1/repositories/{}/branches", self.config.host, repo);

        let transaction_branch = format!("delta-tx-{}", operation_id);
        let body = json!({
            "name": transaction_branch,
            "source": source_branch,
            "force": false,
            "hidden": true,
        });

        let response = self
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        // Handle the response
        match response.status() {
            StatusCode::CREATED => {
                // Branch created successfully
                let new_url = Url::parse(&format!(
                    "lakefs://{}/{}/{}",
                    repo, transaction_branch, table
                ))
                .unwrap();
                Ok((new_url, transaction_branch))
            }
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                Err(LakeFSOperationError::CreateBranchFailed(error.message).into())
            }
        }
    }

    pub async fn delete_branch(
        &self,
        repo: String,
        branch: String,
    ) -> Result<(), TransactionError> {
        let request_url = format!(
            "{}/api/v1/repositories/{}/branches/{}",
            self.config.host, repo, branch
        );
        let response = self
            .http_client
            .delete(&request_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        debug!("Deleting LakeFS Branch.");
        // Handle the response
        match response.status() {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                Err(LakeFSOperationError::DeleteBranchFailed(error.message).into())
            }
        }
    }

    pub async fn commit(
        &self,
        repo: String,
        branch: String,
        commit_message: String,
        allow_empty: bool,
    ) -> DeltaResult<()> {
        let request_url = format!(
            "{}/api/v1/repositories/{}/branches/{}/commits",
            self.config.host, repo, branch
        );

        let body = json!({
            "message": commit_message,
            "allow_empty": allow_empty,
        });

        debug!(
            "Committing to LakeFS Branch: '{}' in repo: '{}'",
            branch, repo
        );
        let response = self
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        // Handle the response
        match response.status() {
            StatusCode::NO_CONTENT | StatusCode::CREATED => Ok(()),
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                Err(LakeFSOperationError::CommitFailed(error.message).into())
            }
        }
    }

    pub async fn merge(
        &self,
        repo: String,
        target_branch: String,
        transaction_branch: String,
        commit_version: i64,
        commit_message: String,
        allow_empty: bool,
    ) -> Result<(), TransactionError> {
        let request_url = format!(
            "{}/api/v1/repositories/{}/refs/{}/merge/{}",
            self.config.host, repo, transaction_branch, target_branch
        );

        let body = json!({
            "message": commit_message,
            "allow_empty": allow_empty,
            "squash_merge": true,
        });

        debug!(
            "Merging LakeFS, source `{}` into target `{}` in repo: {}",
            transaction_branch, transaction_branch, repo
        );
        let response = self
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| LakeFSOperationError::HttpRequestFailed { source: e })?;

        // Handle the response;
        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::CONFLICT => Err(TransactionError::VersionAlreadyExists(commit_version)),
            StatusCode::UNAUTHORIZED => Err(LakeFSOperationError::UnauthorizedAction.into()),
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                Err(LakeFSOperationError::MergeFailed(error.message).into())
            }
        }
    }

    pub fn set_transaction(&self, id: Uuid, branch: String) {
        self.transactions.insert(id, branch);
        debug!("{}", format!("LakeFS Transaction `{}` has been set.", id));
    }

    pub fn get_transaction(&self, id: Uuid) -> Result<String, TransactionError> {
        let transaction_branch = self
            .transactions
            .get(&id)
            .map(|v| v.to_string())
            .ok_or(LakeFSOperationError::TransactionIdNotFound(id.to_string()))?;
        debug!(
            "{}",
            format!("LakeFS Transaction `{}` has been grabbed.", id)
        );
        Ok(transaction_branch)
    }

    pub fn clear_transaction(&self, id: Uuid) {
        self.transactions.remove(&id);
        debug!(
            "{}",
            format!("LakeFS Transaction `{}` has been removed.", id)
        );
    }

    pub fn decompose_url(&self, url: String) -> (String, String, String) {
        let url_path = url
            .strip_prefix("lakefs://")
            .unwrap()
            .split("/")
            .collect::<Vec<&str>>();
        let repo = url_path[0].to_owned();
        let branch = url_path[1].to_owned();
        let table = url_path[2..].join("/");

        (repo, branch, table)
    }
}

#[derive(Deserialize, Debug)]
struct LakeFSErrorResponse {
    message: String,
}

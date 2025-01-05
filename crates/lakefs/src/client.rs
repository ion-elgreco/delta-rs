use std::sync::{Arc, Mutex};

use deltalake_core::operations::transaction::TransactionError;
use deltalake_core::{DeltaResult, DeltaTableError};
use reqwest::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use tracing::debug;
use url::Url;
use uuid::Uuid;

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

// Slim LakeFS client for lakefs branch operations.
#[derive(Debug, Clone)]
pub struct LakeFSClient {
    /// configuration of the
    config: LakeFSConfig,
    http_client: Client,
    transaction: Arc<Mutex<TransactionId>>,
}

impl LakeFSClient {
    pub fn with_config(config: LakeFSConfig) -> Self {
        let http_client = Client::new();
        Self {
            config,
            http_client,
            transaction: Arc::new(Mutex::new(TransactionId::default())),
        }
    }

    pub async fn create_txn_branch(&self, source_url: &Url) -> DeltaResult<(Url, String)> {
        let (repo, source_branch, table) = self.decompose_url(source_url.to_string());

        let request_url = format!("{}/api/v1/repositories/{}/branches", self.config.host, repo);

        let transaction_branch = format!("tx-{}", Uuid::new_v4());
        let body = json!({
            "name": transaction_branch,
            "source": source_branch,
            "force": false,
            "hidden": false, // Set to true later
        });

        let response = self
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to send request: {}", e)))?;

        // Handle the response
        match response.status() {
            StatusCode::CREATED => {
                // Branch created successfully
                let new_url = Url::parse(&format!(
                    "lakefs://{}/{}/{}",
                    repo, transaction_branch, table
                ))
                .map_err(|_| {
                    DeltaTableError::InvalidTableLocation(format!(
                        "lakefs://{}/{}/{}",
                        repo, transaction_branch, table
                    ))
                })?;
                Ok((new_url, transaction_branch))
            }
            StatusCode::UNAUTHORIZED => Err(DeltaTableError::generic(
                "Unauthorized request, please check credentials/access.",
            )),
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                Err(DeltaTableError::generic(format!(
                    "LakeFS: {}",
                    error.message
                )))
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
            .map_err(|e| TransactionError::LogStoreError {
                msg: format!("Failed to send request: {}", e),
                source: Box::new(DeltaTableError::generic(format!(
                    "Failed to send request: {}",
                    e
                ))),
            })?;

        debug!("Deleting LakeFS Branch.");
        // Handle the response
        match response.status() {
            StatusCode::NO_CONTENT => return Ok(()),
            StatusCode::UNAUTHORIZED => {
                return Err(TransactionError::LogStoreError {
                    msg: "Unauthorized request, please check credentials/access.".to_string(),
                    source: Box::new(DeltaTableError::generic(
                        "Unauthorized request, please check credentials/access.",
                    )),
                })
            }
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                Err(TransactionError::LogStoreError {
                    msg: format!("LakeFS: {}", error.message),
                    source: Box::new(DeltaTableError::generic(format!(
                        "LakeFS: {}",
                        error.message
                    ))),
                })
            }
        }
    }

    pub async fn commit(&self, url: String, commit_version: i64) -> DeltaResult<()> {
        let (repo, branch, table) = self.decompose_url(url);

        let request_url = format!(
            "{}/api/v1/repositories/{}/branches/{}/commits",
            self.config.host, repo, branch
        );

        let body = json!({
            "message": format!("commit: deltalake transaction for table: {}, version: {}", table, commit_version),
        });

        debug!("Committing to LakeFS Branch: {}.", branch);
        let response = self
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| DeltaTableError::generic(format!("Failed to send request: {}", e)))?;

        // Handle the response
        match response.status() {
            StatusCode::NO_CONTENT | StatusCode::CREATED => return Ok(()),
            StatusCode::UNAUTHORIZED => {
                return Err(DeltaTableError::generic(
                    "Unauthorized request, please check credentials/access.",
                ))
            }
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                return Err(DeltaTableError::generic(format!(
                    "LakeFS: {}",
                    error.message
                )));
            }
        };
    }

    pub async fn merge(
        &self,
        target: &Url,
        transaction_branch: String,
        commit_version: i64,
    ) -> Result<(), TransactionError> {
        let (repo, target, table) = self.decompose_url(target.to_string());

        let request_url = format!(
            "{}/api/v1/repositories/{}/refs/{}/merge/{}",
            self.config.host, repo, transaction_branch, target
        );

        let body = json!({
            "message": format!("completed deltalake transaction for table: {}, version: {}", table, commit_version),
        });

        debug!(
            "Merging LakeFS, source `{}` into target `{}`",
            transaction_branch, transaction_branch
        );
        let response = self
            .http_client
            .post(&request_url)
            .json(&body)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .send()
            .await
            .map_err(|e| TransactionError::LogStoreError {
                msg: format!("Failed to send request: {}", e),
                source: Box::new(DeltaTableError::generic(format!(
                    "Failed to send request: {}",
                    e
                ))),
            })?;

        // Handle the response;
        match response.status() {
            StatusCode::OK => return Ok(()),
            StatusCode::CONFLICT => {
                return Err(TransactionError::VersionAlreadyExists(commit_version))
            }
            StatusCode::UNAUTHORIZED => {
                return Err(TransactionError::LogStoreError {
                    msg: "Unauthorized request, please check credentials/access.".to_string(),
                    source: Box::new(DeltaTableError::generic(
                        "Unauthorized request, please check credentials/access.",
                    )),
                })
            }
            _ => {
                let error: LakeFSErrorResponse =
                    response
                        .json()
                        .await
                        .unwrap_or_else(|_| LakeFSErrorResponse {
                            message: "Unknown error occurred.".to_string(),
                        });
                return Err(TransactionError::LogStoreError {
                    msg: format!("LakeFS: {}", error.message),
                    source: Box::new(DeltaTableError::generic(format!(
                        "LakeFS: {}",
                        error.message
                    ))),
                });
            }
        };
    }

    pub fn set_transaction(&self, id: String) -> DeltaResult<()> {
        self.transaction.lock().unwrap().insert(id.clone())?;
        debug!("{}", format!("LakeFS Transaction `{}` has been set.", id));
        Ok(())
    }

    pub fn get_transaction(&self) -> String {
        self.transaction.lock().unwrap().get().clone().unwrap()
    }

    pub fn clear_transaction(&self) {
        self.transaction.lock().unwrap().clear();
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

#[derive(Default, Debug, Clone)]
struct TransactionId {
    value: Option<String>,
}

impl TransactionId {
    fn insert(&mut self, value: String) -> Result<(), TransactionError> {
        if self.value.is_some() {
            // Replace with LakeFS errors.
            return Err(TransactionError::LogStoreError {
                msg: "Transaction branch ID is already set.".to_string(),
                source: Box::new(DeltaTableError::generic(
                    "Transaction branch ID is already set.",
                )),
            });
        }
        self.value = Some(value);
        Ok(())
    }

    fn get(&self) -> &Option<String> {
        &self.value
    }

    fn clear(&mut self) {
        self.value = None;
    }
}

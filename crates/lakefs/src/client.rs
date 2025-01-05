use deltalake_core::operations::transaction::TransactionError;
use deltalake_core::{DeltaResult, DeltaTableError};
use reqwest::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
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
}

impl LakeFSClient {
    pub fn with_config(config: LakeFSConfig) -> Self {
        let http_client = Client::new();
        Self {
            config,
            http_client,
        }
    }

    pub async fn create_txn_branch(&self, source_url: &Url) -> DeltaResult<Url> {
        let (repo, source_branch, table) = self.decompose_url(source_url);

        let request_url = format!("{}/repositories/{}/branches", self.config.host, repo);

        let transaction_branch = format!("tx-{}", Uuid::new_v4());
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
                Ok(new_url)
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

    pub async fn commit(&self, url: &Url) -> DeltaResult<()> {
        let (repo, branch, table) = self.decompose_url(url);

        let request_url = format!(
            "{}/repositories/{}/branches/{}/commits",
            self.config.host, repo, branch
        );

        let body = json!({
            "message": format!("commiting transaction for table: {}", table),
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
            StatusCode::CREATED => return Ok(()),
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
        source_url: &Url,
        target_url: &Url,
        commit_version: i64,
    ) -> Result<(), TransactionError> {
        let (repo, source_branch, _) = self.decompose_url(source_url);
        let (repo, target_branch, table) = self.decompose_url(target_url);

        let request_url = format!(
            "{}//repositories/{}/refs/{}/merge/{}",
            self.config.host, repo, source_branch, target_branch
        );

        let body = json!({
            "message": format!("completed deltalake transaction for table: {}", table),
        });

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

        // Handle the response
        match response.status() {
            StatusCode::CREATED => return Ok(()),
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

    fn decompose_url(&self, url: &Url) -> (String, String, String) {
        let url_path = url.path().split("/").collect::<Vec<&str>>();
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

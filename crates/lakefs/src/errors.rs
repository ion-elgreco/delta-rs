//! Errors for LakeFS log store

use deltalake_core::DeltaTableError;

#[derive(thiserror::Error, Debug)]
pub enum LakeFSConfigError {
    /// Missing endpoint
    #[error("LakeFS endpoint is missing in storage options. Set `endpoint`.")]
    EndpointMissing,

    /// Missing username
    #[error("LakeFS username is missing in storage options. Set `access_key_id`.")]
    UsernameCredentialMissing,

    /// Missing password
    #[error("LakeFS password is missing in storage options. Set `secret_access_key`.")]
    PasswordCredentialMissing,
}

impl From<LakeFSConfigError> for DeltaTableError {
    fn from(err: LakeFSConfigError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

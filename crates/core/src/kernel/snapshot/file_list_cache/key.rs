//! Names of the cache entries of one table.

use delta_kernel::Version;
use serde::{Deserialize, Serialize};

use super::super::Snapshot;

/// A new release can change the layout of the file list, so each release has its own entries.
const FORMAT: &str = concat!("file-list/", env!("CARGO_PKG_VERSION"));

/// One written base. The write id keeps the chunks of two writes of the same version apart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BaseId {
    pub(super) version: Version,
    pub(super) write_id: String,
}

impl BaseId {
    pub(super) fn new(version: Version) -> Self {
        Self {
            version,
            write_id: uuid::Uuid::new_v4().simple().to_string(),
        }
    }
}

/// Identifies one table: the table root and the table id.
///
/// A table that is created again at the same location has a new id and gets new entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct TableKey(String);

impl TableKey {
    pub(super) fn new(snapshot: &Snapshot) -> Self {
        Self(format!(
            "{FORMAT}/{}#{}",
            snapshot.inner.table_root(),
            snapshot.metadata().id()
        ))
    }

    pub(super) fn manifest(&self) -> String {
        format!("{}/manifest", self.0)
    }

    pub(super) fn chunk(&self, base: &BaseId, index: u32) -> String {
        format!("{}/base/{}/{}/{index}", self.0, base.version, base.write_id)
    }

    pub(super) fn tail(&self, base: &BaseId) -> String {
        format!("{}/tail/{}/{}", self.0, base.version, base.write_id)
    }
}

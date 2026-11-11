//! The values the cache stores: the manifest, the base chunks, and the tail.

use arrow::array::RecordBatch;
use bytes::Bytes;
use delta_kernel::Version;
use serde::{Deserialize, Serialize};

use super::super::serde::{deserialize_batches, serialize_batches};
use super::super::{Metadata, Protocol, Snapshot};
use super::file_key::{FileKey, FileKeySet};
use super::key::BaseId;
use crate::{DeltaResult, DeltaTableError};

fn invalid(what: &str, err: impl std::fmt::Display) -> DeltaTableError {
    DeltaTableError::Generic(format!("invalid file list cache {what}: {err}"))
}

pub(super) fn encode_batches(batches: &[RecordBatch]) -> DeltaResult<Vec<u8>> {
    serialize_batches(batches).map_err(DeltaTableError::Generic)
}

pub(super) fn decode_batches(data: Bytes) -> DeltaResult<Vec<RecordBatch>> {
    deserialize_batches(data).map_err(|err| invalid("batches", err))
}

/// Describes the base of a table. A base exists only when its manifest exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub(super) base: BaseId,
    pub(super) chunks: u32,
    pub(super) rows: u64,
    protocol: Protocol,
    metadata: Metadata,
}

impl Manifest {
    pub(super) fn new(base: BaseId, chunks: u32, rows: u64, snapshot: &Snapshot) -> Self {
        Self {
            base,
            chunks,
            rows,
            protocol: snapshot.protocol().clone(),
            metadata: snapshot.metadata().clone(),
        }
    }

    /// The columns of the file list come from the protocol and the metadata. A base is
    /// usable for a snapshot only when both are the same.
    pub(super) fn has_layout_of(&self, snapshot: &Snapshot) -> bool {
        &self.protocol == snapshot.protocol() && &self.metadata == snapshot.metadata()
    }

    pub(super) fn encode(&self) -> DeltaResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|err| invalid("manifest", err))
    }

    pub(super) fn decode(data: &[u8]) -> DeltaResult<Self> {
        serde_json::from_slice(data).map_err(|err| invalid("manifest", err))
    }
}

/// The changes of the commits after the base, up to `through_version`.
#[derive(Debug, Clone)]
pub(super) struct Tail {
    pub(super) through_version: Version,
    /// The files these commits added that are still active, newest first.
    pub(super) adds: Vec<RecordBatch>,
    /// The keys of all files these commits added or removed. A base row with one of these
    /// keys is not active any more.
    pub(super) dropped: FileKeySet,
}

#[derive(Serialize, Deserialize)]
struct TailHeader {
    through_version: Version,
    dropped: Vec<FileKey>,
}

impl Tail {
    pub(super) fn empty(through_version: Version) -> Self {
        Self {
            through_version,
            adds: vec![],
            dropped: FileKeySet::default(),
        }
    }

    /// Layout: header length (8 bytes, little endian), JSON header, Arrow IPC file.
    pub(super) fn encode(&self) -> DeltaResult<Vec<u8>> {
        let header = serde_json::to_vec(&TailHeader {
            through_version: self.through_version,
            dropped: self.dropped.iter().collect(),
        })
        .map_err(|err| invalid("tail", err))?;
        let adds = encode_batches(&self.adds)?;

        let mut data = Vec::with_capacity(8 + header.len() + adds.len());
        data.extend_from_slice(&(header.len() as u64).to_le_bytes());
        data.extend_from_slice(&header);
        data.extend_from_slice(&adds);
        Ok(data)
    }

    pub(super) fn decode(data: Bytes) -> DeltaResult<Self> {
        let header_len = data
            .get(..8)
            .map(|len| u64::from_le_bytes(len.try_into().expect("8 bytes")) as usize)
            .filter(|len| data.len() - 8 >= *len)
            .ok_or_else(|| invalid("tail", "truncated"))?;
        let header: TailHeader =
            serde_json::from_slice(&data[8..8 + header_len]).map_err(|err| invalid("tail", err))?;
        Ok(Self {
            through_version: header.through_version,
            adds: decode_batches(data.slice(8 + header_len..))?,
            dropped: header.dropped.into_iter().collect(),
        })
    }
}

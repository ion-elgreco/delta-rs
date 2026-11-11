//! The identity of a file action in log replay: the path and the deletion vector id.

use std::collections::HashMap;

use arrow::array::{Array, AsArray, RecordBatch, StringArray};
use delta_kernel::actions::deletion_vector::DeletionVectorDescriptor;
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use serde::{Deserialize, Serialize};

use crate::kernel::LogicalFileView;
use crate::{DeltaResult, DeltaTableError};

const FIELD_PATH: &str = "path";
const FIELD_DELETION_VECTOR: &str = "deletionVector";

/// A file action key. Log replay keeps the newest action for each key.
///
/// `path` is the path as the log has it, with no decoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(super) struct FileKey {
    pub(super) path: String,
    pub(super) dv: Option<String>,
}

/// A set of [`FileKey`]s with a lookup that needs no allocation.
#[derive(Debug, Clone, Default)]
pub(super) struct FileKeySet {
    // Almost all paths have one key, so a `Vec` holds the deletion vector ids.
    paths: HashMap<String, Vec<Option<String>>, foldhash::fast::RandomState>,
    len: usize,
}

impl FileKeySet {
    pub(super) fn insert(&mut self, key: FileKey) {
        let dvs = self.paths.entry(key.path).or_default();
        if !dvs.contains(&key.dv) {
            dvs.push(key.dv);
            self.len += 1;
        }
    }

    pub(super) fn contains(&self, path: &str, dv: Option<&str>) -> bool {
        self.paths
            .get(path)
            .is_some_and(|dvs| dvs.iter().any(|known| known.as_deref() == dv))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = FileKey> + '_ {
        self.paths.iter().flat_map(|(path, dvs)| {
            dvs.iter().map(move |dv| FileKey {
                path: path.clone(),
                dv: dv.clone(),
            })
        })
    }
}

impl Extend<FileKey> for FileKeySet {
    fn extend<T: IntoIterator<Item = FileKey>>(&mut self, iter: T) {
        for key in iter {
            self.insert(key);
        }
    }
}

impl FromIterator<FileKey> for FileKeySet {
    fn from_iter<T: IntoIterator<Item = FileKey>>(iter: T) -> Self {
        let mut set = Self::default();
        set.extend(iter);
        set
    }
}

/// The keys of the rows of one file-list batch.
pub(super) struct RowKeys {
    paths: StringArray,
    /// `None` when no row has a deletion vector.
    dvs: Option<Vec<Option<String>>>,
}

impl RowKeys {
    pub(super) fn try_new(batch: &RecordBatch, snapshot: &KernelSnapshot) -> DeltaResult<Self> {
        let paths = batch
            .column_by_name(FIELD_PATH)
            .and_then(|column| column.as_string_opt::<i32>())
            .ok_or_else(|| {
                DeltaTableError::Generic("file list batch has no string `path` column".into())
            })?
            .clone();

        let has_dv = batch
            .column_by_name(FIELD_DELETION_VECTOR)
            .is_some_and(|column| column.null_count() < column.len());
        let dvs = if has_dv {
            let mut dvs = Vec::with_capacity(batch.num_rows());
            for index in 0..batch.num_rows() {
                let descriptor =
                    LogicalFileView::new(batch.clone(), index).deletion_vector_descriptor();
                dvs.push(
                    descriptor
                        .map(|descriptor| {
                            let descriptor = DeletionVectorDescriptor::try_from(descriptor)?;
                            Ok::<_, DeltaTableError>(descriptor.unique_id_for(snapshot)?)
                        })
                        .transpose()?,
                );
            }
            Some(dvs)
        } else {
            None
        };

        Ok(Self { paths, dvs })
    }

    /// Copies the paths, so the keys do not keep the buffers of their batch in memory.
    pub(super) fn detached(self) -> Self {
        Self {
            paths: self.paths.iter().collect(),
            dvs: self.dvs,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.paths.len()
    }

    pub(super) fn path(&self, index: usize) -> &str {
        self.paths.value(index)
    }

    pub(super) fn dv(&self, index: usize) -> Option<&str> {
        self.dvs.as_ref().and_then(|dvs| dvs[index].as_deref())
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = FileKey> + '_ {
        (0..self.len()).map(|index| FileKey {
            path: self.path(index).to_string(),
            dv: self.dv(index).map(str::to_string),
        })
    }
}

#[derive(Deserialize)]
struct CommitLine {
    add: Option<FileAction>,
    remove: Option<FileAction>,
}

/// The fields of an `add` or `remove` action that make its key.
#[derive(Deserialize)]
struct FileAction {
    path: String,
    #[serde(default, rename = "deletionVector")]
    deletion_vector: Option<DeletionVectorDescriptor>,
}

/// Adds the keys of all `add` and `remove` actions of one commit file to `keys`.
pub(super) fn commit_file_keys(
    commit: &[u8],
    snapshot: &KernelSnapshot,
    keys: &mut FileKeySet,
) -> DeltaResult<()> {
    for line in serde_json::Deserializer::from_slice(commit).into_iter::<CommitLine>() {
        let line = line.map_err(|err| {
            DeltaTableError::Generic(format!("failed to read a commit for the file list: {err}"))
        })?;
        for action in [line.add, line.remove].into_iter().flatten() {
            let dv = action
                .deletion_vector
                .map(|descriptor| descriptor.unique_id_for(snapshot))
                .transpose()?;
            keys.insert(FileKey {
                path: action.path,
                dv,
            });
        }
    }
    Ok(())
}

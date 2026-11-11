//! Builds the tail: the file changes of the commits after the base.

use arrow::array::RecordBatch;
use futures::{StreamExt, TryStreamExt};

use super::ReadContext;
use super::entry::Tail;
use super::file_key::{FileKeySet, commit_file_keys};
use super::merge::drop_rows;
use crate::DeltaResult;
use crate::logstore::read_commit_entry;

/// The number of commit files to read at the same time.
const COMMIT_READS: usize = 16;

/// Extends `tail` to the version of the snapshot.
///
/// Returns `None` when this snapshot cannot read all commits after the tail: a checkpoint
/// replaced them, or a commit is not a file in the log.
pub(super) async fn extend_tail(ctx: &ReadContext, tail: Tail) -> DeltaResult<Option<Tail>> {
    let from = tail.through_version;
    let to = ctx.snapshot.version();
    if ctx
        .snapshot
        .checkpoint_version()
        .is_some_and(|checkpoint| checkpoint > from)
    {
        return Ok(None);
    }

    let commits: Vec<_> = futures::stream::iter(from + 1..=to)
        .map(|version| read_commit_entry(ctx.object_store.as_ref(), version))
        .buffered(COMMIT_READS)
        .try_collect()
        .await?;
    let kernel = ctx.snapshot.inner.as_ref();
    let mut changed = FileKeySet::default();
    for commit in commits {
        let Some(commit) = commit else {
            return Ok(None);
        };
        commit_file_keys(&commit, kernel, &mut changed)?;
    }

    // With an empty list of known files at `from`, kernel replays only the commits after
    // `from`. The result is the files these commits added that are still active.
    let mut adds: Vec<RecordBatch> = ctx
        .snapshot
        .files_from_preserving_raw(
            ctx.engine.clone(),
            None,
            from,
            Box::new(std::iter::empty::<RecordBatch>()),
            None,
        )
        .try_filter(|batch| std::future::ready(batch.num_rows() > 0))
        .try_collect()
        .await?;
    for batch in &tail.adds {
        adds.extend(drop_rows(batch, &changed, kernel)?);
    }

    let mut dropped = tail.dropped;
    dropped.extend(changed.iter());
    Ok(Some(Tail {
        through_version: to,
        adds,
        dropped,
    }))
}

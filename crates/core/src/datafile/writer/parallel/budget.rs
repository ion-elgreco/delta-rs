//! The arrow bytes a partition's feeder may queue ahead of its column encoders.

use std::sync::{Arc, OnceLock};

use parquet::errors::{ParquetError, Result as ParquetResult};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// 256 MiB by default. Override with `DELTARS_ENCODE_BUDGET_MB`, at most 4095 so
/// the whole budget fits one acquire.
fn encode_budget_bytes() -> usize {
    static BUDGET: OnceLock<usize> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("DELTARS_ENCODE_BUDGET_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(256)
            .clamp(1, 4095)
            * 1024
            * 1024
    })
}

/// Bounds the arrow bytes queued ahead of the encoders, summed over every
/// column, in-flight row group and file of a partition still encoding. The
/// feeder can only run ahead into the next row group once a whole row group is
/// queued, so this is what lets narrow tables keep several row groups encoding
/// at once, while wide rows stay close to a sequential writer's memory. Clones
/// share one bound.
#[derive(Clone, Debug)]
pub(super) struct EncodeBudget {
    bytes: usize,
    permits: Arc<Semaphore>,
}

impl EncodeBudget {
    pub(super) fn new() -> Self {
        let bytes = encode_budget_bytes();
        Self {
            bytes,
            permits: Arc::new(Semaphore::new(bytes)),
        }
    }

    /// Take `bytes` of the budget, waiting for the encoders to release some
    /// when it is spent. A slice larger than the whole budget takes all of it.
    pub(super) async fn acquire(&self, bytes: usize) -> ParquetResult<OwnedSemaphorePermit> {
        let permits = bytes.min(self.bytes) as u32;
        // Try first: an async acquire spends the task's cooperative budget even
        // when permits are free, and a task that runs out is parked until its
        // runtime worker has nothing else to run. With every worker busy
        // encoding, that stalls the feeder, which the encoders all wait on.
        if let Ok(permit) = self.permits.clone().try_acquire_many_owned(permits) {
            return Ok(permit);
        }
        self.permits
            .clone()
            .acquire_many_owned(permits)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))
    }

    /// Arrow bytes queued and not yet encoded.
    pub(super) fn queued_bytes(&self) -> usize {
        self.bytes.saturating_sub(self.permits.available_permits())
    }

    /// Wait until every queued slice is encoded.
    pub(super) async fn drain(&self) -> ParquetResult<()> {
        // Every permit is back once nothing is queued.
        let all = self
            .permits
            .acquire_many(self.bytes as u32)
            .await
            .map_err(|e| ParquetError::External(Box::new(e)))?;
        drop(all);
        Ok(())
    }
}

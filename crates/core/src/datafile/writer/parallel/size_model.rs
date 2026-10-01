//! Sizes the rows in flight from the row groups the partition has appended so far.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Encoded bytes, rows and arrow bytes of the row groups appended so far. Each
/// row group halves the weight of the ones before it, so the figures follow the
/// data, while a short final row group, weighted by its few rows, barely moves
/// them.
#[derive(Clone, Copy, Debug, Default)]
struct RowGroupSizes {
    bytes: f64,
    rows: f64,
    arrow_bytes: f64,
}

impl RowGroupSizes {
    fn add(&mut self, bytes: f64, rows: f64, arrow_bytes: f64) {
        self.bytes = self.bytes / 2.0 + bytes;
        self.rows = self.rows / 2.0 + rows;
        self.arrow_bytes = self.arrow_bytes / 2.0 + arrow_bytes;
    }

    /// Encoded bytes per row to size `rows` rows of `arrow_bytes` with, when
    /// they are shaped like the appended row groups: arrow bytes per row within
    /// half again of theirs. `None` before the first row group is appended, or
    /// for rows that changed shape, say strings that doubled in length: those
    /// encode unlike the appended rows, and a projection from them would miss.
    fn bytes_per_row_for(&self, rows: usize, arrow_bytes: usize) -> Option<f64> {
        if self.rows == 0.0 {
            return None;
        }
        if rows > 0 && arrow_bytes > 0 && self.arrow_bytes > 0.0 {
            let ratio = (arrow_bytes as f64 / rows as f64) / (self.arrow_bytes / self.rows);
            if !(2.0 / 3.0..=1.5).contains(&ratio) {
                return None;
            }
        }
        Some(self.bytes / self.rows)
    }
}

/// The sizes of a partition's appended row groups, shared by its files: a
/// rolled file keeps appending row groups in the background.
#[derive(Clone, Debug, Default)]
pub(super) struct SizeModel(Arc<Mutex<RowGroupSizes>>);

impl SizeModel {
    /// Record a row group just appended to a file.
    pub(super) fn record(&self, bytes: u64, rows: usize, arrow_bytes: usize) {
        self.sizes()
            .add(bytes as f64, rows as f64, arrow_bytes as f64);
    }

    /// Encoded bytes per row to size `rows` rows of `arrow_bytes` with; `None`
    /// before the first row group is appended, or for rows shaped unlike the
    /// appended ones.
    pub(super) fn bytes_per_row_for(&self, rows: usize, arrow_bytes: usize) -> Option<f64> {
        self.sizes().bytes_per_row_for(rows, arrow_bytes)
    }

    fn sizes(&self) -> MutexGuard<'_, RowGroupSizes> {
        // The figures stay consistent even if a holder panicked.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_per_row_follows_the_appended_row_groups() {
        let mut sizes = RowGroupSizes::default();
        assert_eq!(sizes.bytes_per_row_for(100, 800), None);

        sizes.add(1000.0, 100.0, 800.0);
        assert_eq!(sizes.bytes_per_row_for(100, 800), Some(10.0));

        // Later row groups outweigh earlier ones:
        // (1000 / 4 + 2000 / 2 + 2000) / (100 / 4 + 100 / 2 + 100)
        sizes.add(2000.0, 100.0, 800.0);
        sizes.add(2000.0, 100.0, 800.0);
        let bytes_per_row = sizes.bytes_per_row_for(100, 800).unwrap();
        assert!((bytes_per_row - 3250.0 / 175.0).abs() < 1e-9);
    }

    #[test]
    fn a_short_row_group_barely_moves_the_figures() {
        let mut sizes = RowGroupSizes::default();
        sizes.add(1000.0, 100.0, 800.0);
        // A one-row group, like the tail of a file, at three times the rate.
        sizes.add(30.0, 1.0, 8.0);
        // (1000 / 2 + 30) / (100 / 2 + 1)
        let bytes_per_row = sizes.bytes_per_row_for(100, 800).unwrap();
        assert!((bytes_per_row - 530.0 / 51.0).abs() < 1e-9);
    }

    #[test]
    fn rows_shaped_unlike_the_appended_ones_are_not_sized() {
        let mut sizes = RowGroupSizes::default();
        sizes.add(1000.0, 100.0, 800.0);
        // Twice the appended arrow bytes per row, say strings that doubled.
        assert_eq!(sizes.bytes_per_row_for(100, 1600), None);
        // Within half again of theirs, the appended row groups still apply.
        assert_eq!(sizes.bytes_per_row_for(100, 1000), Some(10.0));
        // Nothing in flight tells nothing about the shape.
        assert_eq!(sizes.bytes_per_row_for(0, 0), Some(10.0));
    }
}

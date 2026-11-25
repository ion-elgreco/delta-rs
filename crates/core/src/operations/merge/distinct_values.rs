//! The distinct values of a MERGE source column, for an IN list in the early filter.
//!
//! The early filter skips target files with the minimum and maximum of each source key. When the
//! source has few distinct keys, an IN list of the keys skips more: two keys at the ends of the
//! target range keep only the files that can hold them, where the range keeps every file between
//! them. Kernel compares each value of an IN list with the statistics of every target file, so the
//! early filter uses an IN list only for a small number of values. The limits are those of the IN
//! list pushdown of DataFusion hash joins.

use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, ListArray, RecordBatch, UInt64Array, new_null_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::{SortColumn, and, cast, filter, is_not_null, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema, UInt64Type};
use datafusion::catalog::Session;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion::physical_plan::aggregates::order::GroupOrdering;
use parking_lot::Mutex;

/// The number of rows added to the distinct values before they are compared with the limits. The
/// memory above the limits is at most the values of this many rows.
const CHUNK_ROWS: usize = 1024;

/// When the distinct values are too many for an IN list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct InListLimits {
    /// The maximum number of distinct values, of all groups together
    max_values: usize,
    /// The maximum memory of the distinct values, in bytes
    max_bytes: usize,
}

impl InListLimits {
    /// The limits of the IN list pushdown of DataFusion hash joins, from the session options
    /// `datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values` and
    /// `datafusion.optimizer.hash_join_inlist_pushdown_max_size`. Returns `None` when one of them
    /// is 0, which disables IN lists.
    pub(crate) fn from_session(session: &dyn Session) -> Option<Self> {
        let options = &session.config_options().optimizer;
        let limits = Self {
            max_values: options.hash_join_inlist_pushdown_max_distinct_values,
            max_bytes: options.hash_join_inlist_pushdown_max_size,
        };
        (limits.max_values > 0 && limits.max_bytes > 0).then_some(limits)
    }

    /// The aggregate function that collects the values of an IN list, see [`DistinctValues`].
    pub(crate) fn distinct_values(self) -> AggregateUDF {
        AggregateUDF::new_from_impl(DistinctValues::new(self))
    }

    /// Replace the lists of a column of `items` with NULL, when one list is NULL or all lists
    /// together have more values than the limits.
    ///
    /// An aggregation in partitions applies the limits to the groups of each partition. This
    /// applies them to the groups of all partitions: the early filter has the IN lists of all
    /// groups.
    pub(crate) fn limit_lists(self, items: &RecordBatch) -> Result<RecordBatch> {
        let columns = items
            .columns()
            .iter()
            .map(|column| match column.as_list_opt::<i32>() {
                Some(lists) if self.exceeded_by(lists) => {
                    new_null_array(column.data_type(), column.len())
                }
                _ => Arc::clone(column),
            })
            .collect();
        Ok(RecordBatch::try_new(items.schema(), columns)?)
    }

    fn exceeded_by(self, lists: &ListArray) -> bool {
        let offsets = lists.offsets();
        let values = (offsets[lists.len()] - offsets[0]) as usize;
        lists.null_count() > 0
            || values > self.max_values
            || lists.values().get_array_memory_size() > self.max_bytes
    }
}

/// Aggregates the distinct non-null values of a column into a list.
///
/// The list is NULL when the values are more than the limits. With a GROUP BY, the limits apply to
/// the values of all groups of the accumulator together, and all lists are NULL when the values
/// are more. Each value adds the same cost to the early filter, whichever its group. See
/// [`InListLimits::limit_lists`] for the groups of all accumulators.
#[derive(Debug, PartialEq, Eq, Hash)]
struct DistinctValues {
    signature: Signature,
    limits: InListLimits,
}

impl DistinctValues {
    fn new(limits: InListLimits) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            limits,
        }
    }
}

impl AggregateUDFImpl for DistinctValues {
    fn name(&self) -> &str {
        "distinct_values"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::new_list(value_type(&arg_types[0]).clone(), true))
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let set = DistinctSet::new(args.expr_fields[0].data_type(), self.limits, false);
        Ok(Box::new(DistinctValuesAccumulator(Mutex::new(set))))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let set = DistinctSet::new(args.expr_fields[0].data_type(), self.limits, true);
        Ok(Box::new(set))
    }
}

/// The type of the list values: the values of a dictionary.
fn value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => value_type,
        other => other,
    }
}

/// The distinct values of each group, until they are more than the limits.
///
/// One [`GroupValues`] holds the values of all groups, as (group index, value) pairs. Without a
/// GROUP BY, it holds only the values.
struct DistinctSet {
    /// `None` after the values were more than the limits
    values: Option<Box<dyn GroupValues>>,
    limits: InListLimits,
    value_type: DataType,
    /// The number of groups of the GROUP BY
    num_groups: usize,
    /// The group ids that [`GroupValues::intern`] returns. Not used, kept to reuse the memory.
    interned: Vec<usize>,
}

impl DistinctSet {
    fn new(data_type: &DataType, limits: InListLimits, grouped: bool) -> Self {
        let value_type = value_type(data_type).clone();
        let mut fields = Vec::with_capacity(2);
        if grouped {
            fields.push(Field::new("group", DataType::UInt64, false));
        }
        fields.push(Field::new("value", value_type.clone(), true));
        // A type that `GroupValues` does not support gets no IN list, only the range.
        let values = new_group_values(Arc::new(Schema::new(fields)), &GroupOrdering::None).ok();
        Self {
            values,
            limits,
            value_type,
            num_groups: 0,
            interned: Vec::new(),
        }
    }

    /// Add the non-null `values` of the rows that `keep` does not remove, each to its group in
    /// `groups`.
    fn insert(
        &mut self,
        groups: Option<&[usize]>,
        values: &ArrayRef,
        keep: Option<&BooleanArray>,
    ) -> Result<()> {
        if self.values.is_none() {
            return Ok(());
        }
        let (values, keep) = prepare(values, &self.value_type, keep)?;
        let groups = groups.map(|groups| {
            Arc::new(UInt64Array::from_iter_values(
                groups.iter().map(|group| *group as u64),
            )) as ArrayRef
        });
        let (groups, values) = match keep {
            Some(keep) => (
                groups.map(|groups| filter(&groups, &keep)).transpose()?,
                filter(&values, &keep)?,
            ),
            None => (groups, values),
        };
        self.intern(groups.as_ref(), &values)
    }

    /// Add the values of the lists of a state, each list to its group in `groups`.
    fn merge(&mut self, groups: Option<&[usize]>, lists: &ArrayRef) -> Result<()> {
        if self.values.is_none() {
            return Ok(());
        }
        let lists = lists.as_list::<i32>();
        if lists.null_count() > 0 {
            // The values of another accumulator were more than the limits, so all values are.
            self.values = None;
            return Ok(());
        }
        let offsets = lists.offsets();
        let start = offsets[0] as usize;
        let values = lists
            .values()
            .slice(start, offsets[lists.len()] as usize - start);
        let groups = groups.map(|groups| {
            groups
                .iter()
                .zip(offsets.lengths())
                .flat_map(|(group, len)| std::iter::repeat_n(*group, len))
                .collect::<Vec<_>>()
        });
        self.insert(groups.as_deref(), &values, None)
    }

    /// Intern the values [`CHUNK_ROWS`] rows at a time, until they are more than the limits.
    fn intern(&mut self, groups: Option<&ArrayRef>, values: &ArrayRef) -> Result<()> {
        let Some(set) = self.values.as_mut() else {
            return Ok(());
        };
        for offset in (0..values.len()).step_by(CHUNK_ROWS) {
            let len = CHUNK_ROWS.min(values.len() - offset);
            let mut columns = Vec::with_capacity(2);
            columns.extend(groups.map(|groups| groups.slice(offset, len)));
            columns.push(values.slice(offset, len));
            set.intern(&columns, &mut self.interned)?;
            if set.len() > self.limits.max_values || set.size() > self.limits.max_bytes {
                // An IN list of the values costs more than it saves. Free the memory.
                self.values = None;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Remove the values of the groups `0..n`, and return them as one sorted list per group. The
    /// other groups move down by `n`.
    fn take_lists(&mut self, n: usize) -> Result<ArrayRef> {
        let field = Arc::new(Field::new_list_field(self.value_type.clone(), true));
        let Some(set) = self.values.as_mut() else {
            return Ok(new_null_array(&DataType::List(field), n));
        };
        let columns = set.emit(EmitTo::All)?;
        // Sorted values give the same filter for the same source.
        let order = lexsort_to_indices(
            &columns
                .iter()
                .map(|column| SortColumn {
                    values: Arc::clone(column),
                    options: None,
                })
                .collect::<Vec<_>>(),
            None,
        )?;
        let values = &columns[columns.len() - 1];
        let mut lengths = vec![0; n];
        let taken = match columns.len() {
            // Without a GROUP BY, all values are in the one group.
            1 => {
                lengths[0] = values.len();
                values.len()
            }
            _ => {
                let groups = columns[0].as_primitive::<UInt64Type>();
                let group = |row: &u32| groups.value(*row as usize) as usize;
                let taken = order.values().partition_point(|row| group(row) < n);
                for row in &order.values()[..taken] {
                    lengths[group(row)] += 1;
                }
                if taken < order.len() {
                    let rest = order.slice(taken, order.len() - taken);
                    let rest_groups = UInt64Array::from_iter_values(
                        rest.values().iter().map(|row| (group(row) - n) as u64),
                    );
                    set.intern(
                        &[Arc::new(rest_groups), take(values, &rest, None)?],
                        &mut self.interned,
                    )?;
                }
                taken
            }
        };
        let lists = ListArray::try_new(
            field,
            OffsetBuffer::from_lengths(lengths),
            take(values, &order.slice(0, taken), None)?,
            None,
        )?;
        Ok(Arc::new(lists))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.as_ref().map_or(0, |values| values.size())
            + self.interned.capacity() * size_of::<usize>()
    }
}

/// Unpack a dictionary, and keep only the non-null values of the rows that `keep` keeps.
fn prepare(
    values: &ArrayRef,
    value_type: &DataType,
    keep: Option<&BooleanArray>,
) -> Result<(ArrayRef, Option<BooleanArray>)> {
    let values = if values.data_type() == value_type {
        Arc::clone(values)
    } else {
        cast(values, value_type)?
    };
    let not_null = (values.logical_null_count() > 0)
        .then(|| is_not_null(&values))
        .transpose()?;
    let keep = match (keep, not_null) {
        (Some(keep), Some(not_null)) => Some(and(keep, &not_null)?),
        (Some(keep), None) => Some(keep.clone()),
        (None, not_null) => not_null,
    };
    Ok((values, keep))
}

impl fmt::Debug for DistinctSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistinctSet")
            .field("values", &self.values.as_ref().map(|values| values.len()))
            .field("limits", &self.limits)
            .field("num_groups", &self.num_groups)
            .finish_non_exhaustive()
    }
}

impl GroupsAccumulator for DistinctSet {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.num_groups = self.num_groups.max(total_num_groups);
        self.insert(Some(group_indices), &values[0], opt_filter)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let n = match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n,
        };
        let lists = self.take_lists(n)?;
        self.num_groups = self.num_groups.saturating_sub(n);
        Ok(lists)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.num_groups = self.num_groups.max(total_num_groups);
        self.merge(Some(group_indices), &values[0])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // One list per row, with the value of the row
        let (values, keep) = prepare(&values[0], &self.value_type, opt_filter)?;
        let field = Arc::new(Field::new_list_field(self.value_type.clone(), true));
        let lists = match keep {
            None => ListArray::try_new(
                field,
                OffsetBuffer::from_lengths(std::iter::repeat_n(1, values.len())),
                values,
                None,
            )?,
            Some(keep) => ListArray::try_new(
                field,
                OffsetBuffer::from_lengths(keep.iter().map(|keep| usize::from(keep == Some(true)))),
                filter(&values, &keep)?,
                None,
            )?,
        };
        Ok(vec![Arc::new(lists)])
    }

    fn size(&self) -> usize {
        DistinctSet::size(self)
    }
}

/// The distinct values of a column without a GROUP BY.
///
/// [`GroupValues`] is not `Sync`, which [`Accumulator`] requires. The methods with `&mut self` use
/// [`Mutex::get_mut`], which does not lock.
#[derive(Debug)]
struct DistinctValuesAccumulator(Mutex<DistinctSet>);

impl Accumulator for DistinctValuesAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.get_mut().insert(None, &values[0], None)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let lists = self.0.get_mut().take_lists(1)?;
        ScalarValue::try_from_array(&lists, 0)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.0.lock().size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.0.get_mut().merge(None, &states[0])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, AsArray, BooleanArray, DictionaryArray, Int64Array, ListArray,
        RecordBatch, StringArray,
    };
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::{AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator};
    use datafusion::prelude::{SessionConfig, SessionContext, col};

    use super::{DistinctSet, DistinctValues, InListLimits};

    const LIMITS: InListLimits = InListLimits {
        max_values: 150,
        max_bytes: 128 * 1024,
    };

    /// The values of each list, `None` for a NULL list.
    fn lists(lists: &ArrayRef) -> Vec<Option<Vec<i64>>> {
        let lists = lists.as_list::<i32>();
        (0..lists.len())
            .map(|i| {
                (!lists.is_null(i))
                    .then(|| lists.value(i).as_primitive::<Int64Type>().values().to_vec())
            })
            .collect()
    }

    /// `SELECT distinct_values(v) FROM t GROUP BY g ORDER BY g`, without the GROUP BY when
    /// `grouped` is false. Each item of `partitions` is one partition of `t`, of (g, v) rows.
    async fn distinct_values(
        limits: InListLimits,
        partitions: Vec<Vec<(i64, Option<i64>)>>,
        grouped: bool,
    ) -> Vec<Option<Vec<i64>>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let partitions = partitions
            .into_iter()
            .map(|rows| {
                let (groups, values): (Vec<_>, Vec<_>) = rows.into_iter().unzip();
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(Int64Array::from(groups)),
                    Arc::new(Int64Array::from(values)),
                ];
                vec![RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()]
            })
            .collect();
        let table = MemTable::try_new(Arc::clone(&schema), partitions).unwrap();
        // More partitions than the input, so the aggregation has a partial and a final step.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let udaf = AggregateUDF::new_from_impl(DistinctValues::new(limits));
        let values = vec![udaf.call(vec![col("v")]).alias("values")];
        let df = ctx.read_table(Arc::new(table)).unwrap();
        let df = if grouped {
            df.aggregate(vec![col("g")], values)
                .unwrap()
                .sort(vec![col("g").sort(true, false)])
        } else {
            df.aggregate(vec![], values)
        };
        let batches = df.unwrap().collect().await.unwrap();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        lists(batch.column_by_name("values").unwrap())
    }

    #[tokio::test]
    async fn test_distinct_values_sorted_without_nulls() {
        let partitions = vec![
            vec![(0, Some(3)), (0, Some(1)), (0, None), (0, Some(3))],
            vec![(0, Some(2)), (0, Some(1))],
        ];
        let lists = distinct_values(LIMITS, partitions, false).await;
        assert_eq!(lists, vec![Some(vec![1, 2, 3])]);
    }

    #[tokio::test]
    async fn test_distinct_values_null_over_max_values() {
        let limits = |max_values| InListLimits {
            max_values,
            ..LIMITS
        };
        // Each partition has fewer values than the limit, all together have more.
        let partitions = vec![vec![(0, Some(1)), (0, Some(2))], vec![(0, Some(3))]];
        let lists = distinct_values(limits(3), partitions.clone(), false).await;
        assert_eq!(lists, vec![Some(vec![1, 2, 3])]);
        let lists = distinct_values(limits(2), partitions, false).await;
        assert_eq!(lists, vec![None]);
    }

    #[tokio::test]
    async fn test_distinct_values_of_groups() {
        let partitions = vec![
            vec![(1, Some(11)), (1, Some(10)), (2, None)],
            vec![(2, Some(10)), (1, Some(10)), (3, None)],
        ];
        let lists = distinct_values(LIMITS, partitions, true).await;
        assert_eq!(
            lists,
            vec![Some(vec![10, 11]), Some(vec![10]), Some(vec![])]
        );
    }

    #[test]
    fn test_limit_lists_of_all_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("values", DataType::new_list(DataType::Int64, true), true),
        ]));
        let items = |lists: Vec<Option<Vec<Option<i64>>>>| {
            let groups = Int64Array::from_iter_values(0..lists.len() as i64);
            let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(lists);
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(groups), Arc::new(lists)])
                .unwrap()
        };
        let limits = InListLimits {
            max_values: 3,
            ..LIMITS
        };

        let within = items(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(1)])]);
        assert_eq!(limits.limit_lists(&within).unwrap(), within);

        // Each list has fewer values than the limit, the lists together have more.
        let more_values = items(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ]);
        let limited = limits.limit_lists(&more_values).unwrap();
        assert_eq!(limited.column(0), more_values.column(0));
        assert_eq!(lists(limited.column(1)), vec![None, None]);

        // The values of one group were more than the limits.
        let one_null = items(vec![Some(vec![Some(1)]), None]);
        let limited = limits.limit_lists(&one_null).unwrap();
        assert_eq!(lists(limited.column(1)), vec![None, None]);
    }

    #[test]
    fn test_distinct_values_null_over_max_bytes() {
        let limits = InListLimits {
            max_values: 1000,
            ..LIMITS
        };
        // Values of 1000 bytes
        let values = |n: usize| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(
                (0..n).map(|i| format!("{i:01000}")),
            ))
        };
        let mut set = DistinctSet::new(&DataType::Utf8, limits, false);
        set.insert(None, &values(10), None).unwrap();
        assert!(!set.take_lists(1).unwrap().is_null(0));

        let mut set = DistinctSet::new(&DataType::Utf8, limits, false);
        set.insert(None, &values(200), None).unwrap();
        assert!(set.values.is_none(), "the memory is freed");
        assert!(set.take_lists(1).unwrap().is_null(0));
    }

    #[test]
    fn test_distinct_values_emit_first_groups() {
        let mut set = DistinctSet::new(&DataType::Int64, LIMITS, true);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![12, 10, 11, 10, 13]));
        set.update_batch(&[values], &[2, 0, 1, 0, 2], None, 3)
            .unwrap();

        let first = set.evaluate(EmitTo::First(2)).unwrap();
        assert_eq!(lists(&first), vec![Some(vec![10]), Some(vec![11])]);
        // Group 2 moved down to group 0
        let rest = set.evaluate(EmitTo::All).unwrap();
        assert_eq!(lists(&rest), vec![Some(vec![12, 13])]);
    }

    #[test]
    fn test_distinct_values_convert_to_state_and_merge() {
        let set = DistinctSet::new(&DataType::Int64, LIMITS, true);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(2), Some(3)]));
        let keep = BooleanArray::from(vec![true, true, false, true]);
        let state = set.convert_to_state(&[values], Some(&keep)).unwrap();
        // One list per row, empty for a NULL value or a removed row
        assert_eq!(
            lists(&state[0]),
            vec![Some(vec![1]), Some(vec![]), Some(vec![]), Some(vec![3])]
        );

        let mut merged = DistinctSet::new(&DataType::Int64, LIMITS, true);
        merged.merge_batch(&state, &[1, 0, 0, 1], 2).unwrap();
        let lists_of_groups = merged.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            lists(&lists_of_groups),
            vec![Some(vec![]), Some(vec![1, 3])]
        );
    }

    #[test]
    fn test_distinct_values_unpack_dictionary() {
        let keys: DictionaryArray<Int32Type> = vec!["b", "a", "b"].into_iter().collect();
        let udaf = DistinctValues::new(LIMITS);
        assert_eq!(
            udaf.return_type(&[keys.data_type().clone()]).unwrap(),
            DataType::new_list(DataType::Utf8, true)
        );

        let mut set = DistinctSet::new(keys.data_type(), LIMITS, false);
        set.insert(None, &(Arc::new(keys) as ArrayRef), None)
            .unwrap();
        let lists = set.take_lists(1).unwrap();
        let values = lists.as_list::<i32>().value(0);
        assert_eq!(
            values.as_string::<i32>(),
            &StringArray::from(vec!["a", "b"])
        );
    }
}

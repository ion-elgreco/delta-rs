use std::sync::Arc;
use std::vec;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::prelude::DataFrame;
use datafusion_common::cse::NormalizeEq;
use datafusion_expr::{lit, when, Expr, LogicalPlanBuilder};
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::prefix::PrefixStore;
use parquet::file::properties::WriterProperties;
use tracing::log::*;

use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{find_files, DeltaScanConfigBuilder, DeltaTableProvider};
use crate::delta_datafusion::{DataFusionMixins, DeltaDataChecker};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, AddCDCFile, Remove, StructType, StructTypeExt};
use crate::logstore::LogStoreRef;
use crate::operations::writer::{DeltaWriter, WriterConfig};
use crate::storage::ObjectStoreRef;
use crate::table::state::DeltaTableState;
use crate::table::Constraint as DeltaConstraint;

use arrow::compute::concat_batches;
use arrow_schema::Schema;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_expr::col;

use crate::operations::cdc::CDC_COLUMN_NAME;

use crate::operations::write::{WriteError, WriterStatsConfig};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_cdc(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    let cdc_store = Arc::new(PrefixStore::new(object_store, "_change_data"));

    Ok(write_execution_plan(
        snapshot,
        state,
        plan,
        partition_columns,
        cdc_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
    )
    .await?
    .into_iter()
    .map(|add| {
        // Modify add actions into CDC actions
        match add {
            Action::Add(add) => {
                Action::Cdc(AddCDCFile {
                    // This is a gnarly hack, but the action needs the nested path, not the
                    // path isnide the prefixed store
                    path: format!("_change_data/{}", add.path),
                    size: add.size,
                    partition_values: add.partition_values,
                    data_change: false,
                    tags: add.tags,
                })
            }
            _ => panic!("Expected Add action"),
        }
    })
    .collect::<Vec<_>>())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
) -> DeltaResult<Vec<Action>> {
    write_execution_plan_v2(
        snapshot,
        state,
        plan,
        partition_columns,
        object_store,
        target_file_size,
        write_batch_size,
        writer_properties,
        writer_stats_config,
        false,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: SessionState,
    rewrite: &[Add],
    partition_scan: bool,
) -> DeltaResult<Option<DataFrame>> {
    if !partition_scan {
        // For each identified file perform a parquet scan + filter + limit (1) + count.
        // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.

        // Take the insert plan schema since it might have been schema evolved, if its not
        // it is simply the table schema
        let scan_config = DeltaScanConfigBuilder::new()
            .with_schema(snapshot.input_schema()?)
            .build(snapshot)?;

        let target_provider = Arc::new(
            DeltaTableProvider::try_new(snapshot.clone(), log_store.clone(), scan_config.clone())?
                .with_files(rewrite.to_vec()),
        );

        let target_provider = provider_as_source(target_provider);
        let source = LogicalPlanBuilder::scan("target", target_provider.clone(), None)?.build()?;
        // We don't want to verify the predicate against existing data

        let df = DataFrame::new(state.clone(), source);

        return Ok(Some(df));
    } else {
        return Ok(None);
    }
}

// This should only be called with a valid predicate
#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_predicate_actions(
    predicate: Expr,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    deletion_timestamp: i64,
) -> DeltaResult<(Vec<Action>, Option<DataFrame>)> {
    let candidates =
        find_files(snapshot, log_store.clone(), &state, Some(predicate.clone())).await?;

    let scan_df = execute_non_empty_expr(
        snapshot,
        log_store,
        state,
        &candidates.candidates,
        candidates.partition_scan,
    )
    .await?;

    let remove = candidates.candidates;
    let mut actions = Vec::new();
    for action in remove {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: action.base_row_id,
            default_row_commit_version: action.default_row_commit_version,
        }))
    }
    Ok((actions, scan_df))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_execution_plan_v2(
    snapshot: Option<&DeltaTableState>,
    state: SessionState,
    plan: Arc<dyn ExecutionPlan>,
    partition_columns: Vec<String>,
    object_store: ObjectStoreRef,
    target_file_size: Option<usize>,
    write_batch_size: Option<usize>,
    writer_properties: Option<WriterProperties>,
    writer_stats_config: WriterStatsConfig,
    contains_cdc: bool,
    predicate: Option<Expr>,
) -> DeltaResult<Vec<Action>> {
    // We always take the plan Schema since the data may contain Large/View arrow types,
    // the schema and batches were prior constructed with this in mind.
    let schema: ArrowSchemaRef = plan.schema();
    let mut checker = if let Some(snapshot) = snapshot {
        DeltaDataChecker::new(snapshot)
    } else {
        debug!("Using plan schema to derive generated columns, since no snapshot was provided. Implies first write.");
        let delta_schema: StructType = schema.as_ref().try_into()?;
        DeltaDataChecker::new_with_generated_columns(
            delta_schema.get_generated_columns().unwrap_or_default(),
        )
    };

    // Write data to disk
    let mut tasks = vec![];
    if !contains_cdc {
        if let Some(predicate) = &predicate {
            let chk = DeltaConstraint::new("*", &fmt_expr_to_sql(predicate)?);
            checker = checker.with_extra_constraints(vec![chk]);
        };

        for i in 0..plan.properties().output_partitioning().partition_count() {
            let inner_plan = plan.clone();
            let inner_schema = schema.clone();
            let task_ctx = Arc::new(TaskContext::from(&state));
            let config = WriterConfig::new(
                inner_schema.clone(),
                partition_columns.clone(),
                writer_properties.clone(),
                target_file_size,
                write_batch_size,
                writer_stats_config.num_indexed_cols,
                writer_stats_config.stats_columns.clone(),
            );
            let mut writer = DeltaWriter::new(object_store.clone(), config);
            let checker_stream = checker.clone();
            let mut stream = inner_plan.execute(i, task_ctx)?;

            let handle: tokio::task::JoinHandle<DeltaResult<Vec<Action>>> =
                tokio::task::spawn(async move {
                    while let Some(maybe_batch) = stream.next().await {
                        let batch = maybe_batch?;
                        checker_stream.check_batch(&batch).await?;
                        writer.write(&batch).await?;
                    }
                    let add_actions = writer.close().await;
                    match add_actions {
                        Ok(actions) => Ok(actions.into_iter().map(Action::Add).collect::<Vec<_>>()),
                        Err(err) => Err(err),
                    }
                });
            tasks.push(handle);
        }
    } else {
        // Incoming plan contains the normal write_plan unioned with the cdf plan
        // we split these batches during the write
        let cdf_store = Arc::new(PrefixStore::new(object_store.clone(), "_change_data"));
        for i in 0..plan.properties().output_partitioning().partition_count() {
            let inner_plan = plan.clone();
            let write_schema = Arc::new(Schema::new(
                schema
                    .clone()
                    .fields()
                    .into_iter()
                    .filter_map(|f| {
                        if f.name() != CDC_COLUMN_NAME {
                            Some(f.as_ref().clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            ));
            let cdf_schema = schema.clone();
            let task_ctx = Arc::new(TaskContext::from(&state));
            let normal_config = WriterConfig::new(
                write_schema.clone(),
                partition_columns.clone(),
                writer_properties.clone(),
                target_file_size,
                write_batch_size,
                writer_stats_config.num_indexed_cols,
                writer_stats_config.stats_columns.clone(),
            );

            let cdf_config = WriterConfig::new(
                cdf_schema.clone(),
                partition_columns.clone(),
                writer_properties.clone(),
                target_file_size,
                write_batch_size,
                writer_stats_config.num_indexed_cols,
                writer_stats_config.stats_columns.clone(),
            );

            let mut writer = DeltaWriter::new(object_store.clone(), normal_config);

            let mut cdf_writer = DeltaWriter::new(cdf_store.clone(), cdf_config);

            let mut normal_checker_stream = checker.clone();
            let cdf_checker_stream = checker.clone();

            // We check the normal batches with the predicate, cdf checker_stream doesn't require this.
            if let Some(predicate) = &predicate {
                let chk = DeltaConstraint::new(
                    "*",
                    &fmt_expr_to_sql(
                        &when(col(CDC_COLUMN_NAME).eq(lit("insert")), predicate.clone())
                            .otherwise(lit(true))?,
                    )?,
                );
                normal_checker_stream = normal_checker_stream.with_extra_constraints(vec![chk]);
            };
            let mut stream = inner_plan.execute(i, task_ctx)?;

            let session_context = SessionContext::new();

            let handle: tokio::task::JoinHandle<DeltaResult<Vec<Action>>> =
                tokio::task::spawn(async move {
                    while let Some(maybe_batch) = stream.next().await {
                        let batch = maybe_batch?;

                        // split batch since we unioned upstream the operation write and cdf plan
                        let table_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                            batch.schema(),
                            vec![vec![batch.clone()]],
                        )?);
                        let batch_df = session_context.read_table(table_provider).unwrap();

                        let normal_df = batch_df.clone().filter(col(CDC_COLUMN_NAME).in_list(
                            vec![lit("delete"), lit("source_delete"), lit("update_preimage")],
                            true,
                        ))?;

                        let cdf_df = batch_df.filter(col(CDC_COLUMN_NAME).in_list(
                            vec![
                                lit("delete"),
                                lit("insert"),
                                lit("update_preimage"),
                                lit("update_postimage"),
                            ],
                            false,
                        ))?;

                        let normal_batch =
                            concat_batches(&cdf_schema, &normal_df.collect().await?)?;
                        normal_checker_stream.check_batch(&normal_batch).await?;

                        // Drop cdc column in normal batch
                        let table_provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
                            normal_batch.schema(),
                            vec![vec![normal_batch.clone()]],
                        )?);
                        let normal_batch = concat_batches(
                            &write_schema,
                            &session_context
                                .read_table(table_provider)
                                .unwrap()
                                .drop_columns(&[CDC_COLUMN_NAME])?
                                .collect()
                                .await?,
                        )?;

                        let cdf_batch = concat_batches(&cdf_schema, &cdf_df.collect().await?)?;
                        cdf_checker_stream.check_batch(&cdf_batch).await?;
                        writer.write(&normal_batch).await?;
                        cdf_writer.write(&cdf_batch).await?;
                    }
                    let mut add_actions = writer
                        .close()
                        .await?
                        .into_iter()
                        .map(Action::Add)
                        .collect::<Vec<_>>();
                    let cdf_actions = cdf_writer.close().await.map(|v| {
                        v.into_iter()
                            .map(|add| {
                                {
                                    Action::Cdc(AddCDCFile {
                                        // This is a gnarly hack, but the action needs the nested path, not the
                                        // path isnide the prefixed store
                                        path: format!("_change_data/{}", add.path),
                                        size: add.size,
                                        partition_values: add.partition_values,
                                        data_change: false,
                                        tags: add.tags,
                                    })
                                }
                            })
                            .collect::<Vec<_>>()
                    })?;
                    add_actions.extend(cdf_actions);
                    Ok(add_actions)
                });
            tasks.push(handle);
        }
    }
    let actions = futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| WriteError::WriteTask { source: err })?
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .concat()
        .into_iter()
        .collect::<Vec<_>>();
    // Collect add actions to add to commit
    Ok(actions)
}

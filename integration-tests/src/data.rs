use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::{RecordBatch, RecordBatchOptions},
    },
    catalog::Session,
    common::Result as DFResult,
    datasource::{MemTable, TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{ColumnarValue, Volatility},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::{Expr, SessionConfig, SessionContext, create_udf},
};
use futures::stream;

pub fn build_session_context() -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(12).set_bool(
        "datafusion.optimizer.enable_join_dynamic_filter_pushdown",
        false,
    );
    let ctx = SessionContext::new_with_config(config);
    register_tables(&ctx);
    register_udfs(&ctx);
    ctx
}

pub fn register_tables(ctx: &SessionContext) {
    register_simple_table(ctx);
    register_file_grid_original_44691_table(ctx);
    register_zero_col_tables(ctx);
}

pub fn register_udfs(ctx: &SessionContext) {
    register_panic_udf(ctx);
    register_cpu_intensive_udf(ctx);
}

pub fn register_simple_table(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));

    let names = StringArray::from(vec!["Alice"]);
    let ages = Int32Array::from(vec![25]);
    let batch0 = RecordBatch::try_new(schema.clone(), vec![Arc::new(names), Arc::new(ages)])
        .expect("Failed to create record batch");

    let names = StringArray::from(vec!["Bob"]);
    let ages = Int32Array::from(vec![30]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(names), Arc::new(ages)])
        .expect("Failed to create record batch");

    let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]])
        .expect("Failed to create MemTable");

    ctx.register_table("simple", Arc::new(table))
        .expect("Failed to register simple table");
}

pub fn register_file_grid_original_44691_table(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("view_updated", DataType::Int64, true),
    ]));

    let ids = Int32Array::from(vec![1, 1, 1]);
    let file_names = StringArray::from(vec!["older", "latest", "missing"]);
    let view_updated = Int64Array::from(vec![Some(100), Some(200), None]);
    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(file_names), Arc::new(view_updated)],
    )
    .expect("Failed to create file grid record batch 0");

    let ids = Int32Array::from(vec![2, 3, 3]);
    let file_names = StringArray::from(vec!["only_null", "latest3", "older3"]);
    let view_updated = Int64Array::from(vec![None, Some(50), Some(40)]);
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(file_names), Arc::new(view_updated)],
    )
    .expect("Failed to create file grid record batch 1");

    let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]])
        .expect("Failed to create file grid MemTable");

    ctx.register_table(
        "file_grid_original_44691_20260313152925290",
        Arc::new(table),
    )
    .expect("Failed to register file grid table");
}

pub fn register_panic_udf(ctx: &SessionContext) {
    let panic_udf = create_udf(
        "panic",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(panic_udf_impl),
    );
    ctx.register_udf(panic_udf);
}

fn panic_udf_impl(_args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    panic!("udf panicked");
}

/// Register a CPU-intensive UDF that simulates heavy computation
/// Usage: SELECT cpu_intensive(iterations) FROM table
/// The function computes fibonacci-like operations to burn CPU cycles
pub fn register_cpu_intensive_udf(ctx: &SessionContext) {
    let cpu_intensive_udf = create_udf(
        "cpu_intensive",
        vec![DataType::Int64],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(cpu_intensive_udf_impl),
    );
    ctx.register_udf(cpu_intensive_udf);
}

fn cpu_intensive_udf_impl(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    let arg0_arr = args[0].clone().into_array(1)?;
    let arg0_arr = arg0_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");

    let results: Vec<i64> = arg0_arr
        .values()
        .iter()
        .map(|&n| {
            // CPU-intensive computation: iterative calculation
            let mut a: i64 = 0;
            let mut b: i64 = 1;
            for _ in 0..n.abs() {
                let temp = a.wrapping_add(b);
                a = b;
                b = temp;
            }
            a
        })
        .collect();

    match &args[0] {
        ColumnarValue::Scalar(_) => {
            let arr = Int64Array::from(results);
            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Int64(Some(arr.value(0))),
            ))
        }
        ColumnarValue::Array(_) => {
            let arr = Int64Array::from(results);
            Ok(ColumnarValue::Array(Arc::new(arr)))
        }
    }
}

// ============================================================
// Zero-column RecordBatch regression fixture
// ------------------------------------------------------------
// `ZeroColTable` is a custom [`TableProvider`] that **intentionally does NOT
// normalize** `projection = Some(empty)` to `Some(vec![0])`. This mirrors the
// pre-fix shape of the custom TableProviders in `data-fabric` which paniced
// in the dist path when `COUNT(*)` lowered the projection to an empty vec
// and the resulting 0-column / row_count > 0 RecordBatch flowed through
// dist's task driver (small-table) and arrow-select coalesce (repartition).
//
// The fix in data-fabric (`normalize_projection()`, commit `6e7b30a3`) is
// purely on the TableProvider side. This fixture exists so that the dist
// integration suite can act as a regression guard for the dist-side
// behavior under 0-column batches with `row_count > 0` — i.e. so that any
// future change to dist that re-introduces a panic on this shape is caught
// here, regardless of whether upstream TableProvider authors remember to
// normalize their projections.
//
// The two registered variants exist to cover both code paths that have
// historically paniced:
//
//   * `zero_col_small` — 1 partition, 2 rows. Targets the small-table /
//     single-task path (e.g. `dist/src/util.rs` `assert_eq!` on column
//     counts when consolidating partial aggregates).
//   * `zero_col_large` — 4 partitions, 2 rows each (8 rows total). Forces
//     repartition through `RepartitionExec` / `CoalescePartitionsExec`,
//     which has historically paniced at `arrow-select coalesce.rs:462`
//     when feeding 0-column batches into the coalesce buffer.
//
// Both tables are registered unconditionally — they are inert unless
// queried. Queries that exercise them live in
// `tests/zero_col_regression.slt`, run only by the `#[ignore]` /
// env-gated test in `tests/sqllogictest.rs` so the default `cargo test`
// run does not turn main CI red.
// ============================================================

#[derive(Debug, Clone)]
pub struct ZeroColTable {
    schema: SchemaRef,
    rows_per_partition: usize,
    num_partitions: usize,
}

impl ZeroColTable {
    pub fn new(rows_per_partition: usize, num_partitions: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        Self {
            schema,
            rows_per_partition,
            num_partitions,
        }
    }
}

#[async_trait]
impl TableProvider for ZeroColTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // INTENTIONAL: do NOT normalize `Some(empty)` → `Some(vec![0])`.
        // This reproduces the pre-fix behavior of data-fabric task #7/#10.
        let projected_schema: SchemaRef = match projection {
            None => self.schema.clone(),
            Some(p) if p.is_empty() => Arc::new(Schema::empty()),
            Some(p) => Arc::new(self.schema.project(p)?),
        };
        let cache = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(self.num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Arc::new(ZeroColExec {
            projected_schema,
            rows_per_partition: self.rows_per_partition,
            num_partitions: self.num_partitions,
            cache,
        }))
    }
}

#[derive(Debug)]
struct ZeroColExec {
    projected_schema: SchemaRef,
    rows_per_partition: usize,
    num_partitions: usize,
    cache: PlanProperties,
}

impl ExecutionPlan for ZeroColExec {
    fn name(&self) -> &str {
        "ZeroColExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let schema = self.projected_schema.clone();
        let rows = self.rows_per_partition;
        let batch = if schema.fields().is_empty() {
            // 0-column RecordBatch with `row_count > 0` — the exact shape
            // that paniced dist's util.rs:113 and arrow-select coalesce.
            RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )?
        } else {
            // Normal id batch when the planner asked for the `id` column.
            let ids: Vec<i32> = (0..rows as i32).collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids))])?
        };
        let stream = stream::iter(std::iter::once(Ok(batch)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for ZeroColExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ZeroColExec: schema_fields={}, rows_per_partition={}, partitions={}",
            self.projected_schema.fields().len(),
            self.rows_per_partition,
            self.num_partitions,
        )
    }
}

pub fn register_zero_col_tables(ctx: &SessionContext) {
    // Single-partition / small-table path.
    let small = ZeroColTable::new(2, 1);
    ctx.register_table("zero_col_small", Arc::new(small))
        .expect("Failed to register zero_col_small table");

    // Multi-partition path — drives repartition / coalesce stages.
    let large = ZeroColTable::new(2, 4);
    ctx.register_table("zero_col_large", Arc::new(large))
        .expect("Failed to register zero_col_large table");
}

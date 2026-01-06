use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    common::Result as DFResult,
    datasource::MemTable,
    logical_expr::{ColumnarValue, Volatility},
    prelude::{SessionConfig, SessionContext, create_udf},
};

pub fn build_session_context() -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(12);
    let ctx = SessionContext::new_with_config(config);
    register_tables(&ctx);
    register_udfs(&ctx);
    ctx
}

pub fn register_tables(ctx: &SessionContext) {
    register_simple_table(ctx);
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

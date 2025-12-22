use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    prelude::SessionContext,
};

pub fn build_session_context() -> SessionContext {
    let ctx = SessionContext::new();
    register_tables(&ctx);
    ctx
}

pub fn register_tables(ctx: &SessionContext) {
    register_simple_table(ctx);
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

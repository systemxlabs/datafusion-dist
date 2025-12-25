use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{RecordBatch, StringBuilder, UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::Session,
    common::Result as DFResult,
    datasource::{MemTable, TableProvider, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_dist::runtime::DistRuntime;

#[derive(Debug)]
pub struct RunningJobsTable {
    runtime: DistRuntime,
    schema: SchemaRef,
}

impl RunningJobsTable {
    pub fn new(runtime: DistRuntime) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("job_id", DataType::Utf8, false),
            Field::new("stage", DataType::UInt32, false),
            Field::new("num_running_tasks", DataType::UInt64, false),
            Field::new("num_dropped_tasks", DataType::UInt64, false),
        ]));
        Self { runtime, schema }
    }

    async fn build_batch(&self) -> DFResult<RecordBatch> {
        let job_stats = self.runtime.get_all_jobs().await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to get all jobs: {e}"))
        })?;

        let mut job_id_builder = StringBuilder::new();
        let mut stage_builder = UInt32Builder::new();
        let mut num_running_tasks_builder = UInt64Builder::new();
        let mut num_dropped_tasks_builder = UInt64Builder::new();

        for (job_id, stages) in job_stats {
            for (stage_id, stage_info) in stages {
                let running: usize = stage_info
                    .task_set_infos
                    .iter()
                    .map(|ts| ts.running_partitions.len())
                    .sum();
                let dropped: usize = stage_info
                    .task_set_infos
                    .iter()
                    .map(|ts| ts.dropped_partitions.len())
                    .sum();

                job_id_builder.append_value(job_id.to_string());
                stage_builder.append_value(stage_id.stage);
                num_running_tasks_builder.append_value(running as u64);
                num_dropped_tasks_builder.append_value(dropped as u64);
            }
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(job_id_builder.finish()),
                Arc::new(stage_builder.finish()),
                Arc::new(num_running_tasks_builder.finish()),
                Arc::new(num_dropped_tasks_builder.finish()),
            ],
        )?;

        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for RunningJobsTable {
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let batch = self.build_batch().await?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

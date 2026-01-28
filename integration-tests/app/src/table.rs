use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{RecordBatch, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::Session,
    common::Result as DFResult,
    datasource::{MemTable, TableProvider, TableType},
    error::DataFusionError,
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
            Field::new("stages", DataType::Utf8, false),
        ]));
        Self { runtime, schema }
    }

    async fn build_batch(&self) -> DFResult<RecordBatch> {
        let job_stats = self.runtime.get_all_jobs().await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to get all jobs: {e}"))
        })?;

        let mut job_id_builder = StringBuilder::new();
        let mut stages_builder = StringBuilder::new();

        for (job_id, stages) in job_stats {
            job_id_builder.append_value(&job_id);

            let stages = stages
                .into_iter()
                .map(|(stage_id, stage_info)| (stage_id.stage.to_string(), stage_info))
                .collect::<HashMap<_, _>>();
            let stages_json = serde_json::to_string(&stages).map_err(|e| {
                DataFusionError::Execution(format!("Failed to serialize job stages: {e}"))
            })?;
            stages_builder.append_value(stages_json);
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(job_id_builder.finish()),
                Arc::new(stages_builder.finish()),
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

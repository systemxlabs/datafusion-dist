use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::Result as DFResult,
    datasource::{MemTable, TableProvider, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_dist::{runtime::DistRuntime, util::JobsArrowConverter};

#[derive(Debug)]
pub struct RunningJobsTable {
    runtime: DistRuntime,
    converter: JobsArrowConverter,
}

impl RunningJobsTable {
    pub fn new(runtime: DistRuntime) -> Self {
        Self {
            runtime,
            converter: JobsArrowConverter::new(),
        }
    }
}

#[async_trait]
impl TableProvider for RunningJobsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.converter.schema().clone()
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
        let jobs = self.runtime.get_all_jobs().await.map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to get all jobs: {e}"))
        })?;
        let batch = self.converter.convert(&jobs).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to convert jobs to record batch: {e}"
            ))
        })?;
        let mem_table = MemTable::try_new(self.converter.schema().clone(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

use std::sync::Arc;

use datafusion::{
    common::Statistics,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
        execution_plan::CardinalityEffect,
    },
};

use crate::network::StageId;

#[derive(Debug)]
pub struct UnresolvedExec {
    pub stage_id: StageId,
    pub delegated_stage_id: StageId,
    pub delegated_plan: Arc<dyn ExecutionPlan>,
}

impl UnresolvedExec {
    pub fn new(
        stage_id: StageId,
        delegated_stage_id: StageId,
        delegated_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            stage_id,
            delegated_stage_id,
            delegated_plan,
        }
    }
}

impl ExecutionPlan for UnresolvedExec {
    fn name(&self) -> &str {
        "UnresolvedExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.delegated_plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.delegated_plan.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(
            "UnresolvedExec with_new_children should not be called".to_string(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Err(DataFusionError::Internal(
            "UnresolvedExec execute should not be called".to_string(),
        ))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Statistics, DataFusionError> {
        self.delegated_plan.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.delegated_plan.cardinality_effect()
    }
}

impl DisplayAs for UnresolvedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "UnresolvedExec: stage={}, delegated_stage={}",
            self.stage_id.stage, self.delegated_stage_id.stage
        )
    }
}

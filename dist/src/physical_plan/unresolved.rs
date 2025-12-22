use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

use crate::planner::StageId;

#[derive(Debug)]
pub struct UnresolvedExec {
    pub delegated_stage_id: StageId,
    pub delegated_plan: Arc<dyn ExecutionPlan>,
}

impl UnresolvedExec {
    pub fn new(delegated_stage_id: StageId, delegated_plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
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
        vec![]
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
}

impl DisplayAs for UnresolvedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "UnresolvedExec: delegated_plan={}, delegated_stage={}",
            self.delegated_plan.name(),
            self.delegated_stage_id.stage
        )
    }
}

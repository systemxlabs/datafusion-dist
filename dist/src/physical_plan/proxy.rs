use std::{collections::HashMap, sync::Arc};

use datafusion::{
    common::Statistics,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execution_plan::CardinalityEffect,
    },
};

use crate::{
    DistError, DistResult,
    cluster::NodeId,
    network::{DistNetwork, StageId, TaskId},
    physical_plan::UnresolvedExec,
};

#[derive(Debug)]
pub struct ProxyExec {
    pub network: Arc<dyn DistNetwork>,
    pub stage_id: StageId,
    pub delegated_stage_id: StageId,
    pub delegated_plan: Arc<dyn ExecutionPlan>,
    pub delegated_task_distribution: HashMap<TaskId, NodeId>,
}

impl ProxyExec {
    pub fn try_from_unresolved(
        unresolved: &UnresolvedExec,
        network: Arc<dyn DistNetwork>,
        task_distribution: &HashMap<TaskId, NodeId>,
    ) -> DistResult<Self> {
        let partition_count = unresolved
            .delegated_plan
            .output_partitioning()
            .partition_count();
        let mut delegated_task_distribution = HashMap::new();
        for partition in 0..partition_count {
            let task_id = unresolved.delegated_stage_id.task_id(partition as u32);
            let Some(node_id) = task_distribution.get(&task_id) else {
                return Err(DistError::internal(format!(
                    "Not found task id {task_id} in task distribution: {task_distribution:?}"
                )));
            };
            delegated_task_distribution.insert(task_id, node_id.clone());
        }
        Ok(ProxyExec {
            network,
            stage_id: unresolved.stage_id,
            delegated_stage_id: unresolved.delegated_stage_id,
            delegated_plan: unresolved.delegated_plan.clone(),
            delegated_task_distribution,
        })
    }
}

impl ExecutionPlan for ProxyExec {
    fn name(&self) -> &str {
        "ProxyExec"
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
        todo!()
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

impl DisplayAs for ProxyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let task_distribution_display = self
            .delegated_task_distribution
            .iter()
            .map(|(task_id, node_id)| format!("{}->{}", task_id.partition, node_id))
            .collect::<Vec<String>>()
            .join(", ");
        write!(
            f,
            "ProxyExec: stage={}, delegated_stage={}, delegated_task_distribution=[{}]",
            self.stage_id.stage, self.delegated_stage_id.stage, task_distribution_display
        )
    }
}

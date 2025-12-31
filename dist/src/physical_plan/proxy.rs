use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    stream::RecordBatchStreamAdapter,
};
use futures::{StreamExt, TryStreamExt};

use crate::{
    DistError, DistResult,
    cluster::NodeId,
    physical_plan::UnresolvedExec,
    planner::{StageId, TaskId},
    runtime::DistRuntime,
};

#[derive(Debug)]
pub struct ProxyExec {
    pub delegated_stage_id: StageId,
    pub delegated_plan_name: String,
    pub delegated_plan_properties: PlanProperties,
    pub delegated_task_distribution: HashMap<TaskId, NodeId>,
    pub runtime: DistRuntime,
}

impl ProxyExec {
    pub fn try_from_unresolved(
        unresolved: &UnresolvedExec,
        task_distribution: &HashMap<TaskId, NodeId>,
        runtime: DistRuntime,
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
            delegated_stage_id: unresolved.delegated_stage_id,
            delegated_plan_name: unresolved.delegated_plan.name().to_string(),
            delegated_plan_properties: unresolved.delegated_plan.properties().clone(),
            delegated_task_distribution,
            runtime,
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
        &self.delegated_plan_properties
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_id = self.delegated_stage_id.task_id(partition as u32);
        let node_id = self
            .delegated_task_distribution
            .get(&task_id)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Not found node id for task id {task_id} in task distribution: {:?}",
                    self.delegated_task_distribution
                ))
            })?;

        let fut = get_df_batch_stream(
            self.runtime.clone(),
            node_id.clone(),
            task_id,
            self.delegated_plan_properties
                .eq_properties
                .schema()
                .clone(),
        );
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.delegated_plan_properties
                .eq_properties
                .schema()
                .clone(),
            stream,
        )))
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
            "ProxyExec: delegated_plan={}, delegated_stage={}, delegated_task_distribution=[{}]",
            self.delegated_plan_name, self.delegated_stage_id.stage, task_distribution_display
        )
    }
}

async fn get_df_batch_stream(
    runtime: DistRuntime,
    node_id: NodeId,
    task_id: TaskId,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let dist_stream = if node_id == runtime.node_id {
        runtime.execute_local(task_id).await?
    } else {
        runtime.execute_remote(node_id, task_id).await?
    };
    let df_stream = dist_stream.map_err(DataFusionError::from).boxed();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, df_stream)))
}

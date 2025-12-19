use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    physical_plan::{
        ExecutionPlan, coalesce_partitions::CoalescePartitionsExec,
        display::DisplayableExecutionPlan, repartition::RepartitionExec,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    DistResult,
    cluster::NodeId,
    network::DistNetwork,
    physical_plan::{ProxyExec, UnresolvedExec},
};

pub trait DistPlanner: Debug + Send + Sync {
    fn plan_stages(
        &self,
        job_id: Uuid,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StageId {
    pub job_id: Uuid,
    pub stage: u32,
}

impl StageId {
    pub fn task_id(&self, partition: u32) -> TaskId {
        TaskId {
            job_id: self.job_id,
            stage: self.stage,
            partition,
        }
    }
}

impl Display for StageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.job_id, self.stage)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub job_id: Uuid,
    pub stage: u32,
    pub partition: u32,
}

impl TaskId {
    pub fn stage_id(&self) -> StageId {
        StageId {
            job_id: self.job_id,
            stage: self.stage,
        }
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.job_id, self.stage, self.partition)
    }
}

#[derive(Debug)]
pub struct DefaultPlanner;

impl DistPlanner for DefaultPlanner {
    fn plan_stages(
        &self,
        job_id: Uuid,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>> {
        let mut stage_count = 0u32;
        let plan = plan
            .transform_up(|node| {
                if node.as_any().is::<RepartitionExec>()
                    || node.as_any().is::<CoalescePartitionsExec>()
                {
                    stage_count += node.children().len() as u32;
                }
                Ok(Transformed::no(node))
            })?
            .data;

        let mut stage_plans = HashMap::new();
        let final_plan = plan
            .transform_up(|node| {
                if node.as_any().is::<RepartitionExec>()
                    || node.as_any().is::<CoalescePartitionsExec>()
                {
                    let mut new_children = Vec::with_capacity(node.children().len());

                    for child in node.children() {
                        let stage_id = StageId {
                            job_id,
                            stage: stage_count,
                        };
                        stage_plans.insert(stage_id, child.clone());
                        stage_count -= 1;

                        let new_child = UnresolvedExec::new(stage_id, child.clone());
                        new_children.push(Arc::new(new_child) as Arc<dyn ExecutionPlan>);
                    }
                    let new_plan = node.with_new_children(new_children)?;
                    Ok(Transformed::yes(new_plan))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;

        let final_stage_id = StageId {
            job_id,
            stage: stage_count,
        };
        stage_plans.insert(final_stage_id, final_plan);

        Ok(stage_plans)
    }
}

pub fn resolve_stage_plan(
    stage_plan: Arc<dyn ExecutionPlan>,
    task_distribution: &HashMap<TaskId, NodeId>,
    network: &Arc<dyn DistNetwork>,
) -> DistResult<Arc<dyn ExecutionPlan>> {
    let transformed = stage_plan.transform(|node| {
        if let Some(unresolved) = node.as_any().downcast_ref::<UnresolvedExec>() {
            let proxy =
                ProxyExec::try_from_unresolved(unresolved, network.clone(), task_distribution)?;
            Ok(Transformed::yes(Arc::new(proxy)))
        } else {
            Ok(Transformed::no(node))
        }
    })?;
    Ok(transformed.data)
}

pub struct DisplayableStagePlans<'a>(pub &'a HashMap<StageId, Arc<dyn ExecutionPlan>>);

impl Display for DisplayableStagePlans<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (stage_id, plan) in self.0.iter() {
            writeln!(f, "===============Stage {}===============", stage_id.stage)?;
            writeln!(
                f,
                "{}",
                DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
            )?;
        }
        Ok(())
    }
}

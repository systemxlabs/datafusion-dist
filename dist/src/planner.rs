use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties,
    aggregates::{AggregateExec, AggregateMode},
    display::DisplayableExecutionPlan,
    joins::{HashJoinExec, PartitionMode},
    sorts::sort::SortExec,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    DistError, DistResult, JobId,
    cluster::NodeId,
    physical_plan::{ProxyExec, UnresolvedExec},
    runtime::DistRuntime,
};

pub trait DistPlanner: Debug + Send + Sync {
    fn plan_stages(
        &self,
        job_id: JobId,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>>;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StageId {
    pub job_id: JobId,
    pub stage: u32,
}

impl StageId {
    pub fn task_id(&self, partition: u32) -> TaskId {
        TaskId {
            job_id: self.job_id.clone(),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId {
    pub job_id: JobId,
    pub stage: u32,
    pub partition: u32,
}

impl TaskId {
    pub fn stage_id(&self) -> StageId {
        StageId {
            job_id: self.job_id.clone(),
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
        job_id: JobId,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>> {
        let mut stage_count = 0u32;
        let plan = plan
            .transform_up(|node| {
                if is_plan_children_can_be_stages(node.as_ref()) {
                    stage_count += node.children().len() as u32;
                }
                Ok(Transformed::no(node))
            })?
            .data;

        let mut stage_plans = HashMap::new();
        let final_plan = plan
            .transform_up(|node| {
                if is_plan_children_can_be_stages(node.as_ref()) {
                    let mut new_children = Vec::with_capacity(node.children().len());

                    for child in node.children() {
                        let stage_id = StageId {
                            job_id: job_id.clone(),
                            stage: stage_count,
                        };
                        stage_plans.insert(stage_id.clone(), child.clone());
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

pub fn is_plan_children_can_be_stages(plan: &dyn ExecutionPlan) -> bool {
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        matches!(hash_join.partition_mode(), PartitionMode::Partitioned)
    } else if plan.children().len() == 1 {
        if let Some(agg) = plan.children()[0].as_any().downcast_ref::<AggregateExec>() {
            matches!(agg.mode(), AggregateMode::Partial)
        } else if let Some(sort) = plan.children()[0].as_any().downcast_ref::<SortExec>() {
            sort.preserve_partitioning()
        } else {
            false
        }
    } else {
        false
    }
}

pub fn check_initial_stage_plans(
    job_id: JobId,
    stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
) -> DistResult<()> {
    if stage_plans.is_empty() {
        return Err(DistError::internal("Stage plans cannot be empty"));
    }

    let stage0 = StageId {
        job_id: job_id.clone(),
        stage: 0,
    };
    if !stage_plans.contains_key(&stage0) {
        return Err(DistError::internal("Stage 0 must exist in stage plans"));
    }

    let mut depended_stages: HashSet<StageId> = HashSet::new();

    for (_, plan) in stage_plans.iter() {
        plan.apply(|node| {
            if let Some(unresolved) = node.as_any().downcast_ref::<UnresolvedExec>() {
                depended_stages.insert(unresolved.delegated_stage_id.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }

    for stage_id in stage_plans.keys() {
        if stage_id.stage != 0 && !depended_stages.contains(stage_id) {
            return Err(DistError::internal(format!(
                "Stage {} is not depended upon by any other stage",
                stage_id.stage
            )));
        }
    }

    Ok(())
}

pub fn resolve_stage_plan(
    stage_plan: Arc<dyn ExecutionPlan>,
    task_distribution: &HashMap<TaskId, NodeId>,
    runtime: DistRuntime,
) -> DistResult<Arc<dyn ExecutionPlan>> {
    let transformed = stage_plan.transform(|node| {
        if let Some(unresolved) = node.as_any().downcast_ref::<UnresolvedExec>() {
            let proxy =
                ProxyExec::try_from_unresolved(unresolved, task_distribution, runtime.clone())?;
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
        for (stage_id, plan) in self.0.iter().sorted_by_key(|(stage_id, _)| *stage_id) {
            writeln!(
                f,
                "===============Stage {} (partitions={})===============",
                stage_id.stage,
                plan.output_partitioning().partition_count()
            )?;
            write!(
                f,
                "{}",
                DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
            )?;
        }
        Ok(())
    }
}

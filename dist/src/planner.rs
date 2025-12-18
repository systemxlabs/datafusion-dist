use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    physical_plan::{ExecutionPlan, repartition::RepartitionExec},
};
use uuid::Uuid;

use crate::{DistResult, network::StageId, physical_plan::UnresolvedExec};

pub trait DistPlanner: Debug + Send + Sync {
    fn plan_stages(
        &self,
        job_id: Uuid,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>>;
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
                if let Some(exec) = node.as_any().downcast_ref::<RepartitionExec>() {
                    stage_count += exec.children().len() as u32;
                }
                Ok(Transformed::no(node))
            })?
            .data;
        // final stage
        stage_count += 1;

        let mut stage_plans = HashMap::new();
        let final_plan = plan
            .transform_up(|node| {
                if node.as_any().is::<RepartitionExec>() {
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

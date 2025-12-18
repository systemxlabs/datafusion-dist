use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    DistError, DistResult, RecordBatchStream,
    cluster::{DistCluster, NodeId},
    network::{DistNetwork, StageId, TaskId},
    physical_plan::{ProxyExec, UnresolvedExec},
    planner::{DefaultPlanner, DistPlanner},
    schedule::{DistSchedule, RoundRobinScheduler},
};

pub struct DistRuntime {
    pub node_id: NodeId,
    pub ctx: SessionContext,
    pub cluster: Arc<dyn DistCluster>,
    pub network: Arc<dyn DistNetwork>,
    pub planner: Arc<dyn DistPlanner>,
    pub scheduler: Arc<dyn DistSchedule>,
    pub stage_plans: Arc<Mutex<HashMap<StageId, Arc<dyn ExecutionPlan>>>>,
}

impl DistRuntime {
    pub fn try_new(
        ctx: SessionContext,
        cluster: Arc<dyn DistCluster>,
        network: Arc<dyn DistNetwork>,
    ) -> DistResult<Self> {
        Ok(Self {
            node_id: network.local_node(),
            ctx,
            cluster,
            network,
            planner: Arc::new(DefaultPlanner),
            scheduler: Arc::new(RoundRobinScheduler),
            stage_plans: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn with_planner(self, planner: Arc<dyn DistPlanner>) -> Self {
        Self { planner, ..self }
    }

    pub fn with_scheduler(self, scheduler: Arc<dyn DistSchedule>) -> Self {
        Self { scheduler, ..self }
    }

    pub async fn submit(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<(Uuid, HashMap<TaskId, NodeId>)> {
        let job_id = Uuid::new_v4();
        let mut stage_plans = self.planner.plan_stages(job_id, plan)?;

        let node_states = self.cluster.alive_nodes().await?;

        let task_distribution = self.scheduler.schedule(node_states, &stage_plans).await?;

        // Resolve stage plans based on task distribution
        for (_, stage_plan) in stage_plans.iter_mut() {
            *stage_plan =
                resolve_stage_plan(stage_plan.clone(), &task_distribution, &self.network)?;
        }

        let mut node_stages = HashMap::new();
        for (task_id, node_id) in task_distribution.iter() {
            node_stages
                .entry(node_id.clone())
                .or_insert_with(HashSet::new)
                .insert(task_id.stage_id());
        }

        // Send stage plans to cluster nodes
        let mut handles = Vec::with_capacity(node_stages.len());
        for (node_id, stage_ids) in node_stages {
            let node_stage_plans = stage_ids
                .iter()
                .map(|stage_id| {
                    (
                        *stage_id,
                        stage_plans
                            .get(stage_id)
                            .cloned()
                            .expect("stage id should be valid"),
                    )
                })
                .collect();

            if node_id == self.node_id {
                let mut guard = self.stage_plans.lock().await;
                for (stage_id, stage_plan) in node_stage_plans {
                    guard.insert(stage_id, stage_plan);
                }
            } else {
                let network = self.network.clone();
                let handle = tokio::spawn(async move {
                    network
                        .send_plans(node_id.clone(), node_stage_plans)
                        .await?;
                    Ok::<_, DistError>(())
                });
                handles.push(handle);
            }
        }

        for handle in handles {
            handle.await??;
        }

        let stage0_id = StageId { job_id, stage: 0 };
        let stage0_task_distribution: HashMap<TaskId, NodeId> = task_distribution
            .iter()
            .filter(|(task_id, _)| task_id.stage_id() == stage0_id)
            .map(|(task_id, node_id)| (*task_id, node_id.clone()))
            .collect();
        Ok((job_id, stage0_task_distribution))
    }

    pub async fn execute_local(&self, task_id: TaskId) -> DistResult<RecordBatchStream> {
        let stage_id = task_id.stage_id();

        let guard = self.stage_plans.lock().await;
        let plan = guard
            .get(&stage_id)
            .ok_or_else(|| DistError::internal(format!("Plan not found for stage {stage_id}")))?
            .clone();
        drop(guard);

        let df_stream = plan.execute(task_id.partition as usize, self.ctx.task_ctx())?;
        Ok(df_stream.map_err(DistError::from).boxed())
    }

    pub async fn execute_remote(
        &self,
        node_id: NodeId,
        task_id: TaskId,
    ) -> DistResult<RecordBatchStream> {
        if node_id == self.node_id {
            return Err(DistError::internal(format!(
                "remote node id {node_id} is actually self"
            )));
        }
        self.network.execute_task(node_id, task_id).await
    }

    pub async fn receive_plans(
        &self,
        stage_plans: HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<()> {
        let mut guard = self.stage_plans.lock().await;
        guard.extend(stage_plans);
        Ok(())
    }
}

fn resolve_stage_plan(
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

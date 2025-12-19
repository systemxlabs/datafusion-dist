use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::array::RecordBatch,
    common::tree_node::{Transformed, TreeNode},
    physical_plan::ExecutionPlan,
    prelude::SessionContext,
};

use futures::{Stream, StreamExt, TryStreamExt};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    DistError, DistResult, RecordBatchStream,
    cluster::{DistCluster, NodeId},
    heartbeater::Heartbeater,
    network::{DistNetwork, ScheduledTasks, StageId, TaskId},
    physical_plan::{ProxyExec, UnresolvedExec},
    planner::{DefaultPlanner, DistPlanner},
    schedule::{DistSchedule, RoundRobinScheduler},
    util::timestamp_ms,
};

pub struct DistRuntime {
    pub node_id: NodeId,
    pub ctx: SessionContext,
    pub cluster: Arc<dyn DistCluster>,
    pub network: Arc<dyn DistNetwork>,
    pub planner: Arc<dyn DistPlanner>,
    pub scheduler: Arc<dyn DistSchedule>,
    pub heartbeater: Arc<Heartbeater>,
    pub stage_plans: Arc<Mutex<HashMap<StageId, Arc<dyn ExecutionPlan>>>>,
    pub tasks: Arc<Mutex<HashMap<TaskId, TaskState>>>,
}

impl DistRuntime {
    pub fn try_new(
        ctx: SessionContext,
        cluster: Arc<dyn DistCluster>,
        network: Arc<dyn DistNetwork>,
    ) -> DistResult<Self> {
        let node_id = network.local_node();
        let tasks = Arc::new(Mutex::new(HashMap::new()));
        let heartbeater = Heartbeater::new(node_id.clone(), cluster.clone(), tasks.clone());
        Ok(Self {
            node_id: network.local_node(),
            ctx,
            cluster,
            network,
            planner: Arc::new(DefaultPlanner),
            scheduler: Arc::new(RoundRobinScheduler),
            heartbeater: Arc::new(heartbeater),
            stage_plans: Arc::new(Mutex::new(HashMap::new())),
            tasks,
        })
    }

    pub fn with_planner(self, planner: Arc<dyn DistPlanner>) -> Self {
        Self { planner, ..self }
    }

    pub fn with_scheduler(self, scheduler: Arc<dyn DistSchedule>) -> Self {
        Self { scheduler, ..self }
    }

    pub async fn start(&self) {
        self.heartbeater.start();
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
        let mut node_tasks = HashMap::new();
        for (task_id, node_id) in task_distribution.iter() {
            node_stages
                .entry(node_id.clone())
                .or_insert_with(HashSet::new)
                .insert(task_id.stage_id());
            node_tasks
                .entry(node_id.clone())
                .or_insert_with(Vec::new)
                .push(*task_id);
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
                .collect::<HashMap<_, _>>();

            let tasks = node_tasks.get(&node_id).cloned().unwrap_or_default();

            if node_id == self.node_id {
                let mut guard = self.stage_plans.lock().await;
                guard.extend(node_stage_plans);
                let mut guard = self.tasks.lock().await;
                let task_state = TaskState {
                    running: false,
                    create_at: timestamp_ms(),
                    start_at: 0,
                };
                for task_id in tasks {
                    guard.insert(task_id, task_state.clone());
                }
            } else {
                let network = self.network.clone();
                let handle = tokio::spawn(async move {
                    let scheduled_tasks = ScheduledTasks::new(node_stage_plans, tasks);
                    network.send_tasks(node_id.clone(), scheduled_tasks).await?;
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

        let mut guard = self.tasks.lock().await;
        if let Some(task_stage) = guard.get_mut(&task_id) {
            task_stage.running = true;
            task_stage.start_at = timestamp_ms();
        } else {
            return Err(DistError::internal(format!(
                "Task {task_id} not found in this node"
            )));
        }
        drop(guard);

        let df_stream = plan.execute(task_id.partition as usize, self.ctx.task_ctx())?;
        let stream = df_stream.map_err(DistError::from).boxed();

        let task_stream = TaskStream {
            task_id,
            tasks: self.tasks.clone(),
            stage_plans: self.stage_plans.clone(),
            stream,
        };

        Ok(task_stream.boxed())
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

    pub async fn receive_tasks(&self, scheduled_tasks: ScheduledTasks) {
        let mut guard = self.stage_plans.lock().await;
        guard.extend(scheduled_tasks.stage_plans);
        let mut guard = self.tasks.lock().await;
        let task_state = TaskState {
            running: false,
            create_at: timestamp_ms(),
            start_at: 0,
        };
        for task_id in scheduled_tasks.task_ids {
            guard.insert(task_id, task_state.clone());
        }
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

#[derive(Debug, Clone)]
pub struct TaskState {
    pub running: bool,
    pub create_at: i64,
    pub start_at: i64,
}

pub struct TaskStream {
    pub task_id: TaskId,
    pub tasks: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    pub stage_plans: Arc<Mutex<HashMap<StageId, Arc<dyn ExecutionPlan>>>>,
    pub stream: RecordBatchStream,
}

impl Stream for TaskStream {
    type Item = DistResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

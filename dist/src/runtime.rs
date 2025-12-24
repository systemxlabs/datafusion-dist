use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use uuid::Uuid;

use datafusion::{
    arrow::array::RecordBatch,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::ExecutionPlan,
};

use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, error};
use tokio::sync::{Mutex, mpsc::Sender};

use crate::{
    DistError, DistResult, RecordBatchStream,
    cluster::{DistCluster, NodeId},
    config::DistConfig,
    event::{Event, EventHandler, check_job_completed, local_job_status, start_event_handler},
    heartbeat::Heartbeater,
    network::{DistNetwork, ScheduledTasks, StageInfo},
    planner::{
        DefaultPlanner, DisplayableStagePlans, DistPlanner, StageId, TaskId, resolve_stage_plan,
    },
    schedule::{DisplayableTaskDistribution, DistSchedule, RoundRobinScheduler},
};

#[derive(Debug, Clone)]
pub struct DistRuntime {
    pub node_id: NodeId,
    pub task_ctx: Arc<TaskContext>,
    pub config: Arc<DistConfig>,
    pub cluster: Arc<dyn DistCluster>,
    pub network: Arc<dyn DistNetwork>,
    pub planner: Arc<dyn DistPlanner>,
    pub scheduler: Arc<dyn DistSchedule>,
    pub heartbeater: Arc<Heartbeater>,
    pub stages: Arc<Mutex<HashMap<StageId, Arc<StageState>>>>,
    pub event_sender: Sender<Event>,
}

impl DistRuntime {
    pub fn new(
        task_ctx: Arc<TaskContext>,
        config: Arc<DistConfig>,
        cluster: Arc<dyn DistCluster>,
        network: Arc<dyn DistNetwork>,
    ) -> Self {
        let node_id = network.local_node();
        let stages = Arc::new(Mutex::new(HashMap::new()));
        let heartbeater = Heartbeater::new(
            node_id.clone(),
            cluster.clone(),
            stages.clone(),
            config.heartbeat_interval,
        );

        let (sender, receiver) = tokio::sync::mpsc::channel::<Event>(1024);
        let event_handler = EventHandler {
            local_node: node_id.clone(),
            cluster: cluster.clone(),
            network: network.clone(),
            local_stages: stages.clone(),
            receiver,
        };
        start_event_handler(event_handler);

        Self {
            node_id: network.local_node(),
            task_ctx,
            config,
            cluster,
            network,
            planner: Arc::new(DefaultPlanner),
            scheduler: Arc::new(RoundRobinScheduler),
            heartbeater: Arc::new(heartbeater),
            stages,
            event_sender: sender,
        }
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
        debug!(
            "job {job_id} initial stage plans:\n{}",
            DisplayableStagePlans(&stage_plans)
        );

        let node_states = self.cluster.alive_nodes().await?;
        debug!(
            "alive nodes: {}",
            node_states
                .keys()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let task_distribution = self.scheduler.schedule(node_states, &stage_plans).await?;
        debug!(
            "job {job_id} task distribution: {}",
            DisplayableTaskDistribution(&task_distribution)
        );

        // Resolve stage plans based on task distribution
        for (_, stage_plan) in stage_plans.iter_mut() {
            *stage_plan = resolve_stage_plan(stage_plan.clone(), &task_distribution, self.clone())?;
        }
        debug!(
            "job {job_id} final stage plans:\n{}",
            DisplayableStagePlans(&stage_plans)
        );

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

            let scheduled_tasks = ScheduledTasks::new(node_stage_plans, tasks);

            if node_id == self.node_id {
                self.receive_tasks(scheduled_tasks).await?;
            } else {
                debug!(
                    "Sending tasks [{}] to {node_id}",
                    scheduled_tasks
                        .task_ids
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                );
                let network = self.network.clone();
                let handle = tokio::spawn(async move {
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

        let guard = self.stages.lock().await;
        let stage_state = guard
            .get(&stage_id)
            .ok_or_else(|| DistError::internal(format!("Stage {stage_id} not found")))?
            .clone();
        drop(guard);

        let (task_set_id, df_stream) = stage_state
            .execute(task_id.partition as usize, self.task_ctx.clone())
            .await?;
        let stream = df_stream.map_err(DistError::from).boxed();

        let task_stream = TaskStream {
            task_id,
            task_set_id,
            stage_state,
            event_sender: self.event_sender.clone(),
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

        debug!("Executing remote task {task_id} on node {node_id}");
        self.network.execute_task(node_id, task_id).await
    }

    pub async fn receive_tasks(&self, scheduled_tasks: ScheduledTasks) -> DistResult<()> {
        debug!(
            "Received tasks: [{}] and plans of stages: [{}]",
            scheduled_tasks
                .task_ids
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            scheduled_tasks
                .stage_plans
                .keys()
                .map(|k| k.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        let stage_states = StageState::from_scheduled_tasks(scheduled_tasks)?;
        let mut guard = self.stages.lock().await;
        guard.extend(stage_states);
        Ok(())
    }

    pub async fn is_job_completed(&self, job_id: Uuid) -> DistResult<Option<bool>> {
        check_job_completed(&self.cluster, &self.network, &self.stages, job_id).await
    }

    pub async fn get_local_job_status(&self, job_id: Uuid) -> HashMap<StageId, StageInfo> {
        local_job_status(&self.stages, job_id).await
    }

    pub async fn cleanup_local_job(&self, job_id: Uuid) {
        debug!("Cleaning up local Job {job_id}");
        let mut guard = self.stages.lock().await;
        guard.retain(|stage_id, _| stage_id.job_id != job_id);
    }
}

#[derive(Debug)]
pub struct StageState {
    pub stage_id: StageId,
    pub stage_plan: Arc<dyn ExecutionPlan>,
    pub assigned_partitions: HashSet<usize>,
    pub task_sets: Mutex<Vec<TaskSet>>,
}

impl StageState {
    pub fn from_scheduled_tasks(
        tasks: ScheduledTasks,
    ) -> DistResult<HashMap<StageId, Arc<StageState>>> {
        let mut stage_tasks: HashMap<StageId, HashSet<TaskId>> = HashMap::new();
        for task_id in tasks.task_ids {
            let stage_id = task_id.stage_id();
            stage_tasks.entry(stage_id).or_default().insert(task_id);
        }

        let mut stage_states = HashMap::new();
        for (stage_id, assigned_task_ids) in stage_tasks {
            let stage_state = StageState {
                stage_id,
                stage_plan: tasks
                    .stage_plans
                    .get(&stage_id)
                    .ok_or_else(|| {
                        DistError::internal(format!("Not found plan of stage {stage_id}"))
                    })?
                    .clone(),
                assigned_partitions: assigned_task_ids
                    .iter()
                    .map(|task_id| task_id.partition as usize)
                    .collect(),
                task_sets: Mutex::new(Vec::new()),
            };
            stage_states.insert(stage_id, Arc::new(stage_state));
        }
        Ok(stage_states)
    }

    pub async fn num_running_tasks(&self) -> usize {
        let guard = self.task_sets.lock().await;
        guard
            .iter()
            .map(|task_set| task_set.running_partitions.len())
            .sum()
    }

    pub async fn execute(
        &self,
        partition: usize,
        task_ctx: Arc<TaskContext>,
    ) -> DistResult<(Uuid, SendableRecordBatchStream)> {
        if !self.assigned_partitions.contains(&partition) {
            let task_id = self.stage_id.task_id(partition as u32);
            return Err(DistError::internal(format!(
                "Task {task_id} not found in this node"
            )));
        }

        let mut guard = self.task_sets.lock().await;
        for task_set in guard.iter_mut() {
            if !task_set.never_executed(&partition) {
                task_set.running_partitions.insert(partition);

                let df_stream = task_set.shared_plan.execute(partition, task_ctx)?;
                return Ok((task_set.id, df_stream));
            }
        }

        let task_set_id = Uuid::new_v4();
        let mut new_task_set = TaskSet {
            id: task_set_id,
            shared_plan: self.stage_plan.clone().reset_state()?,
            running_partitions: HashSet::new(),
            completed_partitions: HashSet::new(),
        };
        new_task_set.running_partitions.insert(partition);
        let shared_plan = new_task_set.shared_plan.clone();
        guard.push(new_task_set);
        drop(guard);

        let df_stream = shared_plan.execute(partition, task_ctx)?;

        Ok((task_set_id, df_stream))
    }

    pub async fn complete_task(&self, task_id: TaskId, task_set_id: Uuid) {
        let mut guard = self.task_sets.lock().await;
        if let Some(task_set) = guard.iter_mut().find(|task_set| task_set.id == task_set_id) {
            task_set
                .running_partitions
                .remove(&(task_id.partition as usize));
            task_set
                .completed_partitions
                .insert(task_id.partition as usize);
        }
    }

    pub async fn assigned_partitions_executed_at_least_once(&self) -> bool {
        let guard = self.task_sets.lock().await;
        let executed_partitions = guard
            .iter()
            .flat_map(|task_set| {
                let mut executed = task_set.running_partitions.clone();
                executed.extend(task_set.completed_partitions.iter());
                executed
            })
            .collect::<HashSet<_>>();
        drop(guard);

        for partition in self.assigned_partitions.iter() {
            if !executed_partitions.contains(partition) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
pub struct TaskSet {
    pub id: Uuid,
    pub shared_plan: Arc<dyn ExecutionPlan>,
    pub running_partitions: HashSet<usize>,
    pub completed_partitions: HashSet<usize>,
}

impl TaskSet {
    pub fn never_executed(&self, partition: &usize) -> bool {
        !self.running_partitions.contains(partition)
            && !self.completed_partitions.contains(partition)
    }
}

pub struct TaskStream {
    pub task_id: TaskId,
    pub task_set_id: Uuid,
    pub stage_state: Arc<StageState>,
    pub event_sender: Sender<Event>,
    pub stream: RecordBatchStream,
}

impl Stream for TaskStream {
    type Item = DistResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl Drop for TaskStream {
    fn drop(&mut self) {
        let task_id = self.task_id;
        let task_set_id = self.task_set_id;
        let stage_state = self.stage_state.clone();
        let sender = self.event_sender.clone();
        tokio::spawn(async move {
            stage_state.complete_task(task_id, task_set_id).await;
            if stage_state.stage_id.stage == 0
                && stage_state
                    .assigned_partitions_executed_at_least_once()
                    .await
                && let Err(e) = sender.send(Event::TryCleanupJob(task_id.job_id)).await
            {
                error!(
                    "Failed to send TryCleanupJob event after task {task_id} stream dropped: {e}"
                );
            }
        });
    }
}

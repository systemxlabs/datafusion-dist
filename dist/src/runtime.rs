use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use arrow::array::RecordBatch;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::ExecutionPlan;

use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, error};
use tokio::sync::{Mutex, mpsc::Sender};

use crate::{
    DistError, DistResult, RecordBatchStream,
    cluster::{DistCluster, NodeId, NodeStatus},
    config::DistConfig,
    event::{Event, EventHandler, cleanup_job, local_job_status, start_event_handler},
    heartbeat::Heartbeater,
    network::{DistNetwork, ScheduledTasks, StageInfo},
    planner::{
        DefaultPlanner, DisplayableStagePlans, DistPlanner, StageId, TaskId,
        check_initial_stage_plans, resolve_stage_plan,
    },
    schedule::{DefaultScheduler, DisplayableTaskDistribution, DistSchedule},
    util::timestamp_ms,
};

#[derive(Debug, Clone)]
pub struct DistRuntime {
    pub node_id: NodeId,
    pub status: Arc<Mutex<NodeStatus>>,
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
        let status = Arc::new(Mutex::new(NodeStatus::Available));
        let stages = Arc::new(Mutex::new(HashMap::new()));
        let heartbeater = Heartbeater {
            node_id: node_id.clone(),
            cluster: cluster.clone(),
            stages: stages.clone(),
            heartbeat_interval: config.heartbeat_interval,
            status: status.clone(),
        };

        let (sender, receiver) = tokio::sync::mpsc::channel::<Event>(1024);

        let event_handler = EventHandler {
            local_node: node_id.clone(),
            config: config.clone(),
            cluster: cluster.clone(),
            network: network.clone(),
            local_stages: stages.clone(),
            sender: sender.clone(),
            receiver,
        };
        start_event_handler(event_handler);

        Self {
            node_id: network.local_node(),
            status,
            task_ctx,
            config,
            cluster,
            network,
            planner: Arc::new(DefaultPlanner),
            scheduler: Arc::new(DefaultScheduler::new()),
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

    pub async fn shutdown(&self) {
        // Set status to Terminating
        *self.status.lock().await = NodeStatus::Terminating;
        debug!("Set node status to Terminating, no new tasks will be assigned");

        self.heartbeater.send_heartbeat().await;
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
        check_initial_stage_plans(job_id, &stage_plans)?;

        let node_states = self.cluster.alive_nodes().await?;
        debug!(
            "alive nodes: {}",
            node_states
                .keys()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let task_distribution = self
            .scheduler
            .schedule(&self.node_id, &node_states, &stage_plans)
            .await?;
        debug!(
            "job {job_id} task distribution: {}",
            DisplayableTaskDistribution(&task_distribution)
        );
        let stage0_task_distribution: HashMap<TaskId, NodeId> = task_distribution
            .iter()
            .filter(|(task_id, _)| task_id.stage == 0)
            .map(|(task_id, node_id)| (*task_id, node_id.clone()))
            .collect();
        if stage0_task_distribution.is_empty() {
            return Err(DistError::internal(format!(
                "Not found stage0 task distribution in {task_distribution:?} for job {job_id}"
            )));
        }

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
                    "Sending job {job_id} tasks [{}] to {node_id}",
                    scheduled_tasks
                        .task_ids
                        .iter()
                        .map(|t| format!("{}/{}", t.stage, t.partition))
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

        let task_stream = TaskStream::new(
            task_id,
            task_set_id,
            stage_state,
            self.event_sender.clone(),
            stream,
        );

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
        let stage_ids = stage_states.keys().cloned().collect::<Vec<StageId>>();
        let mut guard = self.stages.lock().await;
        guard.extend(stage_states);

        let stage0_ids = stage_ids
            .iter()
            .filter(|id| id.stage == 0)
            .cloned()
            .collect::<Vec<StageId>>();
        if !stage0_ids.is_empty() {
            self.event_sender
                .send(Event::ReceivedStage0Tasks(stage0_ids))
                .await
                .map_err(|e| {
                    DistError::internal(format!("Failed to send ReceivedStage0Tasks event: {e}"))
                })?;
        }

        Ok(())
    }

    pub async fn cleanup_local_job(&self, job_id: Uuid) {
        debug!("Cleaning up local Job {job_id}");
        let mut guard = self.stages.lock().await;
        guard.retain(|stage_id, _| stage_id.job_id != job_id);
    }

    pub async fn cleanup_job(&self, job_id: Uuid) -> DistResult<()> {
        cleanup_job(
            &self.node_id,
            &self.cluster,
            &self.network,
            &self.stages,
            job_id,
        )
        .await
    }

    pub async fn get_local_job(&self, job_id: Uuid) -> HashMap<StageId, StageInfo> {
        local_job_status(&self.stages, Some(job_id)).await
    }

    pub async fn get_local_jobs(&self) -> HashMap<StageId, StageInfo> {
        local_job_status(&self.stages, None).await
    }

    pub async fn get_all_jobs(&self) -> DistResult<HashMap<Uuid, HashMap<StageId, StageInfo>>> {
        // First, get local status for all jobs
        let mut combined_status = self.get_local_jobs().await;

        // Then, get status from all other alive nodes
        let node_states = self.cluster.alive_nodes().await?;

        let mut handles = Vec::new();
        for node_id in node_states.keys() {
            if *node_id != self.node_id {
                let network = self.network.clone();
                let node_id = node_id.clone();
                let handle =
                    tokio::spawn(async move { network.get_job_status(node_id, None).await });
                handles.push(handle);
            }
        }

        for handle in handles {
            let remote_status = handle.await??;
            for (stage_id, remote_stage_info) in remote_status {
                combined_status
                    .entry(stage_id)
                    .and_modify(|existing| {
                        existing
                            .assigned_partitions
                            .extend(&remote_stage_info.assigned_partitions);
                        existing
                            .task_set_infos
                            .extend(remote_stage_info.task_set_infos.clone());
                    })
                    .or_insert(remote_stage_info);
            }
        }

        // Aggregate stats by job_id
        let mut job_stats: HashMap<Uuid, HashMap<StageId, StageInfo>> = HashMap::new();
        for (stage_id, stage_info) in combined_status {
            job_stats
                .entry(stage_id.job_id)
                .or_default()
                .insert(stage_id, stage_info);
        }

        Ok(job_stats)
    }
}

#[derive(Debug)]
pub struct StageState {
    pub stage_id: StageId,
    pub create_at_ms: i64,
    pub stage_plan: Arc<dyn ExecutionPlan>,
    pub assigned_partitions: HashSet<usize>,
    pub task_sets: Mutex<Vec<TaskSet>>,
}

impl StageState {
    pub fn from_scheduled_tasks(
        scheduled_tasks: ScheduledTasks,
    ) -> DistResult<HashMap<StageId, Arc<StageState>>> {
        let mut stage_tasks: HashMap<StageId, HashSet<TaskId>> = HashMap::new();
        for task_id in scheduled_tasks.task_ids {
            let stage_id = task_id.stage_id();
            stage_tasks.entry(stage_id).or_default().insert(task_id);
        }

        let mut stage_states = HashMap::new();
        for (stage_id, assigned_task_ids) in stage_tasks {
            let stage_state = StageState {
                stage_id,
                create_at_ms: timestamp_ms(),
                stage_plan: scheduled_tasks
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
            dropped_partitions: HashMap::new(),
        };
        new_task_set.running_partitions.insert(partition);
        let shared_plan = new_task_set.shared_plan.clone();
        guard.push(new_task_set);
        drop(guard);

        let df_stream = shared_plan.execute(partition, task_ctx)?;

        Ok((task_set_id, df_stream))
    }

    pub async fn complete_task(
        &self,
        task_id: TaskId,
        task_set_id: Uuid,
        task_metrics: TaskMetrics,
    ) {
        let mut guard = self.task_sets.lock().await;
        if let Some(task_set) = guard.iter_mut().find(|task_set| task_set.id == task_set_id) {
            task_set
                .running_partitions
                .remove(&(task_id.partition as usize));
            task_set
                .dropped_partitions
                .insert(task_id.partition as usize, task_metrics);
        }
    }

    pub async fn assigned_partitions_executed_at_least_once(&self) -> bool {
        let guard = self.task_sets.lock().await;
        let executed_partitions = guard
            .iter()
            .flat_map(|task_set| {
                let mut executed = task_set.running_partitions.clone();
                executed.extend(task_set.dropped_partitions.keys());
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

    pub async fn never_executed(&self) -> bool {
        let guard = self.task_sets.lock().await;
        let never_executed = guard
            .iter()
            .all(|set| set.running_partitions.is_empty() && set.dropped_partitions.is_empty());
        drop(guard);

        never_executed
    }
}

#[derive(Debug)]
pub struct TaskSet {
    pub id: Uuid,
    pub shared_plan: Arc<dyn ExecutionPlan>,
    pub running_partitions: HashSet<usize>,
    pub dropped_partitions: HashMap<usize, TaskMetrics>,
}

impl TaskSet {
    pub fn never_executed(&self, partition: &usize) -> bool {
        !self.running_partitions.contains(partition)
            && !self.dropped_partitions.contains_key(partition)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetrics {
    pub output_rows: usize,
    pub output_bytes: usize,
    pub completed: bool,
}

pub struct TaskStream {
    pub task_id: TaskId,
    pub task_set_id: Uuid,
    pub stage_state: Arc<StageState>,
    pub event_sender: Sender<Event>,
    pub stream: RecordBatchStream,
    pub output_rows: usize,
    pub output_bytes: usize,
    pub completed: bool,
}

impl TaskStream {
    pub fn new(
        task_id: TaskId,
        task_set_id: Uuid,
        stage_state: Arc<StageState>,
        event_sender: Sender<Event>,
        stream: RecordBatchStream,
    ) -> Self {
        Self {
            task_id,
            task_set_id,
            stage_state,
            event_sender,
            stream,
            output_rows: 0,
            output_bytes: 0,
            completed: false,
        }
    }
}

impl Stream for TaskStream {
    type Item = DistResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.output_rows += batch.num_rows();
                self.output_bytes += batch.get_array_memory_size();
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.completed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for TaskStream {
    fn drop(&mut self) {
        let task_id = self.task_id;
        let task_set_id = self.task_set_id;
        let task_metrics = TaskMetrics {
            output_bytes: self.output_bytes,
            output_rows: self.output_rows,
            completed: self.completed,
        };
        debug!("Task {task_id} dropped with metrics: {task_metrics:?}");

        let stage_state = self.stage_state.clone();
        let sender = self.event_sender.clone();
        tokio::spawn(async move {
            stage_state
                .complete_task(task_id, task_set_id, task_metrics)
                .await;
            if stage_state.stage_id.stage == 0
                && stage_state
                    .assigned_partitions_executed_at_least_once()
                    .await
                && let Err(e) = sender.send(Event::CheckJobCompleted(task_id.job_id)).await
            {
                error!(
                    "Failed to send CheckJobCompleted event after task {task_id} stream dropped: {e}"
                );
            }
        });
    }
}

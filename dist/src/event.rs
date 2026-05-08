use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use backon::{ExponentialBuilder, Retryable};
use futures::future::join_all;
use log::{debug, error, warn};
use parking_lot::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    DistError, DistResult, JobId,
    cluster::{DistCluster, NodeId},
    config::DistConfig,
    network::{DistNetwork, StageInfo},
    planner::StageId,
    runtime::StageState,
};

#[derive(Debug, Clone)]
pub enum Event {
    CheckJobCompleted(JobId),
    CleanupJob(JobId),
    ReceivedStage0Tasks(Vec<StageId>),
}

const MAX_BATCH_SIZE: usize = 1024;
const EVENT_SEND_TIMEOUT: Duration = Duration::from_secs(300);
const CHECK_JOB_RETRY_MAX_DELAY: Duration = Duration::from_secs(10);
const CHECK_JOB_RETRY_MAX_TIMES: usize = 3;

fn job_check_retry_strategy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_max_delay(CHECK_JOB_RETRY_MAX_DELAY)
        .with_max_times(CHECK_JOB_RETRY_MAX_TIMES)
        .with_jitter()
}

pub async fn send_event_with_timeout(sender: &Sender<Event>, event: Event) -> DistResult<()> {
    tokio::time::timeout(EVENT_SEND_TIMEOUT, sender.send(event))
        .await
        .map_err(|_| {
            DistError::internal(format!(
                "Timed out sending event after {}s",
                EVENT_SEND_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|e| DistError::internal(format!("Failed to send event: {e}")))
}

/// Merge duplicate events by `(job_id, event_kind)`.
///
/// - `CheckJobCompleted(JobId)` and `CleanupJob(JobId)` are deduplicated:
///   only the first occurrence for a given `(job_id, event_kind)` is kept.
/// - `ReceivedStage0Tasks(Vec<StageId>)` for the same job are merged into
///   a single event whose `stage0_ids` preserves first-occurrence order
///   (IDs from the first event keep their relative order; new IDs from
///   subsequent events are appended in the order they first appear).
/// - Empty `ReceivedStage0Tasks` vectors are silently skipped.
fn merge_events(events: &mut Vec<Event>) -> Vec<Event> {
    let mut merged: Vec<Event> = Vec::with_capacity(events.len());
    let mut index_map: HashMap<(JobId, u8), usize> = HashMap::with_capacity(events.len());
    for event in events.drain(..) {
        let key = match &event {
            Event::CheckJobCompleted(job_id) => (job_id.clone(), 0u8),
            Event::CleanupJob(job_id) => (job_id.clone(), 1u8),
            Event::ReceivedStage0Tasks(stage0_ids) => {
                if stage0_ids.is_empty() {
                    continue;
                }
                let job_id = stage0_ids[0].job_id.clone();
                (job_id, 2u8)
            }
        };
        if let Some(&idx) = index_map.get(&key) {
            if let Event::ReceivedStage0Tasks(existing_ids) = &mut merged[idx]
                && let Event::ReceivedStage0Tasks(new_ids) = &event
            {
                let existing_set: HashSet<StageId> = existing_ids.iter().cloned().collect();
                for id in new_ids {
                    if !existing_set.contains(id) {
                        existing_ids.push(id.clone());
                    }
                }
            }
            // For CheckJobCompleted and CleanupJob, keep the first occurrence
            // (already stored in merged[idx]).
        } else {
            index_map.insert(key, merged.len());
            merged.push(event);
        }
    }
    merged
}

pub fn start_event_handler(mut handler: EventHandler) {
    tokio::spawn(async move {
        handler.start().await;
    });
}

pub struct EventHandler {
    pub config: Arc<DistConfig>,
    pub cluster: Arc<dyn DistCluster>,
    pub network: Arc<dyn DistNetwork>,
    pub local_stages: Arc<Mutex<HashMap<StageId, StageState>>>,
    pub sender: Sender<Event>,
    pub receiver: Receiver<Event>,
}

impl EventHandler {
    pub async fn start(&mut self) {
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        loop {
            batch.clear();
            let received = self.receiver.recv_many(&mut batch, MAX_BATCH_SIZE).await;
            if received == 0 {
                break;
            }
            debug!("Received batch of {received} events, merging duplicates");
            let merged = merge_events(&mut batch);
            debug!("Merged into {} events", merged.len());
            self.handle_events(merged).await;
        }
    }

    async fn handle_events(&self, events: Vec<Event>) {
        let mut check_job_ids = Vec::new();
        let mut cleanup_job_ids = Vec::new();
        let mut stage0_batches = Vec::new();

        for event in events {
            debug!("Handling event: {event:?}");
            match event {
                Event::CheckJobCompleted(job_id) => check_job_ids.push(job_id),
                Event::CleanupJob(job_id) => cleanup_job_ids.push(job_id),
                Event::ReceivedStage0Tasks(stage0_ids) => stage0_batches.push(stage0_ids),
            }
        }

        if !check_job_ids.is_empty() {
            let cluster = self.cluster.clone();
            let network = self.network.clone();
            let local_stages = self.local_stages.clone();
            let sender = self.sender.clone();
            tokio::spawn(async move {
                handle_check_jobs_completed(
                    &cluster,
                    &network,
                    &local_stages,
                    &sender,
                    check_job_ids.clone(),
                )
                .await;
            });
        }

        if !cleanup_job_ids.is_empty() {
            let cluster = self.cluster.clone();
            let network = self.network.clone();
            let local_stages = self.local_stages.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    cleanup_jobs(&cluster, &network, &local_stages, cleanup_job_ids.clone()).await
                {
                    error!("Failed to cleanup jobs {cleanup_job_ids:?}: {e}");
                }
            });
        }

        for stage0_ids in stage0_batches {
            self.handle_received_stage0_tasks(stage0_ids).await;
        }
    }

    async fn handle_received_stage0_tasks(&self, stage0_ids: Vec<StageId>) {
        let stage0_task_poll_timeout = self.config.stage0_task_poll_timeout;
        let local_stages = self.local_stages.clone();
        let sender = self.sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(stage0_task_poll_timeout).await;

            let mut timeout_stage0_id = None;
            {
                let stages_guard = local_stages.lock();
                for stage_id in stage0_ids {
                    if let Some(stage) = stages_guard.get(&stage_id)
                        && stage.never_executed()
                    {
                        debug!("Found stage0 {stage_id} never polled until timeout");
                        timeout_stage0_id = Some(stage_id);
                        break;
                    }
                }
                drop(stages_guard);
            }

            if let Some(stage_id) = timeout_stage0_id
                && let Err(e) =
                    send_event_with_timeout(&sender, Event::CleanupJob(stage_id.job_id.clone()))
                        .await
            {
                error!(
                    "Failed to send CleanupJob event for job {}: {e}",
                    stage_id.job_id
                );
            }
        });
    }
}

async fn handle_check_jobs_completed(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    sender: &Sender<Event>,
    job_ids: Vec<JobId>,
) {
    match (|| async { check_jobs_completed(cluster, network, local_stages, job_ids.clone()).await })
        .retry(job_check_retry_strategy())
        .await
    {
        Ok(completed_map) => {
            for (job_id, completed) in completed_map {
                if completed {
                    debug!("Job {job_id} completed, remove it from cluster");
                    if let Err(e) =
                        send_event_with_timeout(sender, Event::CleanupJob(job_id.clone())).await
                    {
                        error!("Failed to send cleanup job event for job {job_id}: {e}");
                    }
                }
            }
        }
        Err(err) => {
            error!("Failed to check jobs {job_ids:?} completed: {err}");
        }
    }
}

pub async fn check_jobs_completed(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_ids: Vec<JobId>,
) -> DistResult<HashMap<JobId, bool>> {
    if job_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Get alive nodes for validation
    let alive_nodes = cluster
        .alive_nodes()
        .await?
        .keys()
        .cloned()
        .collect::<HashSet<_>>();

    // Determine target nodes from job_task_distribution if available
    let target_nodes_by_job = {
        let guard = local_stages.lock();
        job_ids
            .iter()
            .cloned()
            .map(|job_id| {
                let target_nodes = guard
                    .values()
                    .find(|stage| stage.stage_id.job_id == job_id)
                    .map(|stage| {
                        stage
                            .job_task_distribution
                            .values()
                            .cloned()
                            .collect::<HashSet<_>>()
                    });
                (job_id, target_nodes)
            })
            .collect::<Vec<_>>()
    };

    let mut completed_map = HashMap::with_capacity(job_ids.len());

    let mut jobs_by_node: HashMap<NodeId, Vec<JobId>> = HashMap::new();
    for (job_id, target_nodes) in target_nodes_by_job {
        match target_nodes {
            Some(nodes) if nodes.is_subset(&alive_nodes) => {
                for node_id in nodes {
                    jobs_by_node
                        .entry(node_id)
                        .or_default()
                        .push(job_id.clone());
                }
            }
            Some(nodes) => {
                let missing: Vec<_> = nodes.difference(&alive_nodes).collect();
                warn!(
                    "Job {job_id} is polluted: task nodes {missing:?} are not alive, treat as completed"
                );
                completed_map.insert(job_id, true);
            }
            None => {
                warn!(
                    "No job_task_distribution found for job {job_id}, skipping remote status check"
                );
            }
        }
    }

    let mut all_job_statuses = HashMap::new();

    if let Some(local_job_ids) = jobs_by_node.remove(&network.local_node()) {
        let local_job_statuses = local_stage_stats(local_stages, Some(&local_job_ids));
        all_job_statuses.extend(local_job_statuses);
    }

    let mut futures = Vec::new();
    for (node_id, job_ids) in jobs_by_node {
        let network = network.clone();
        futures.push(async move {
            network
                .get_job_statuses(node_id.clone(), Some(job_ids.clone()))
                .await
        });
    }

    for remote_status in join_all(futures).await {
        let remote_status = remote_status?;
        for (stage_id, remote_stage_info) in remote_status {
            all_job_statuses
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

    for job_id in job_ids {
        if completed_map.contains_key(&job_id) {
            continue;
        }

        let stage0 = StageId {
            job_id: job_id.clone(),
            stage: 0,
        };

        let job_completed = match all_job_statuses.get(&stage0) {
            Some(stage0_info) => stage0_info.assigned_partitions.iter().all(|partition| {
                stage0_info
                    .task_set_infos
                    .iter()
                    .any(|ts| ts.dropped_partitions.contains_key(partition))
            }),
            None => true,
        };
        completed_map.insert(job_id, job_completed);
    }

    Ok(completed_map)
}

pub fn local_stage_stats(
    stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_ids: Option<&Vec<JobId>>,
) -> HashMap<StageId, StageInfo> {
    let guard = stages.lock();

    let mut result = HashMap::new();
    for (stage_id, stage_state) in guard.iter() {
        if job_ids.is_none_or(|job_ids| job_ids.contains(&stage_id.job_id)) {
            let stage_info = StageInfo::from_stage_state(stage_state);
            result.insert(stage_id.clone(), stage_info);
        }
    }

    result
}

pub async fn cleanup_jobs(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_ids: Vec<JobId>,
) -> DistResult<()> {
    let alive_nodes: HashSet<NodeId> = cluster.alive_nodes().await?.keys().cloned().collect();

    let target_nodes_by_job = {
        let guard = local_stages.lock();
        job_ids
            .iter()
            .cloned()
            .map(|job_id| {
                let target_nodes = guard
                    .values()
                    .find(|stage| stage.stage_id.job_id == job_id)
                    .map(|stage| {
                        stage
                            .job_task_distribution
                            .values()
                            .cloned()
                            .collect::<HashSet<_>>()
                    });
                (job_id, target_nodes)
            })
            .collect::<Vec<_>>()
    };

    let mut jobs_by_node: HashMap<NodeId, Vec<JobId>> = HashMap::new();
    for (job_id, target_nodes) in target_nodes_by_job {
        let nodes_to_clean: HashSet<NodeId> = match target_nodes {
            Some(nodes) if nodes.is_subset(&alive_nodes) => nodes,
            Some(nodes) => {
                let missing: Vec<_> = nodes.difference(&alive_nodes).collect();
                warn!("Job {job_id} is polluted: task nodes {missing:?} are not alive");
                nodes
                    .into_iter()
                    .filter(|n| alive_nodes.contains(n))
                    .collect()
            }
            None => alive_nodes.clone(),
        };

        for node_id in nodes_to_clean {
            jobs_by_node
                .entry(node_id)
                .or_default()
                .push(job_id.clone());
        }
    }

    if let Some(local_job_ids) = jobs_by_node.remove(&network.local_node()) {
        let local_job_ids: HashSet<JobId> = local_job_ids.into_iter().collect();
        let mut guard = local_stages.lock();
        guard.retain(|stage_id, _| !local_job_ids.contains(&stage_id.job_id));
    }

    let mut futures = Vec::new();
    for (node_id, job_ids) in jobs_by_node {
        if !job_ids.is_empty() {
            let network = network.clone();
            futures
                .push(async move { network.cleanup_jobs(node_id.clone(), job_ids.clone()).await });
        }
    }

    for res in join_all(futures).await {
        res?;
    }
    Ok(())
}

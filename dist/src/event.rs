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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EventType {
    CheckJobCompleted,
    CleanupJob,
    ReceivedStage0Tasks,
}

const MAX_BATCH_SIZE: usize = 64;
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

/// Merge duplicate events by `(job_id, event_type)`.
///
/// - `CheckJobCompleted(JobId)` and `CleanupJob(JobId)` are deduplicated:
///   only the first occurrence for a given `(job_id, event_type)` is kept.
/// - `ReceivedStage0Tasks(Vec<StageId>)` for the same job are merged into
///   a single event whose `stage0_ids` preserves first-occurrence order
///   (IDs from the first event keep their relative order; new IDs from
///   subsequent events are appended in the order they first appear).
/// - Empty `ReceivedStage0Tasks` vectors are silently skipped.
fn merge_events(events: &mut Vec<Event>) -> Vec<Event> {
    let mut merged: Vec<Event> = Vec::with_capacity(events.len());
    let mut index_map: HashMap<(JobId, EventType), usize> = HashMap::with_capacity(events.len());
    for event in events.drain(..) {
        let key = match &event {
            Event::CheckJobCompleted(job_id) => (job_id.clone(), EventType::CheckJobCompleted),
            Event::CleanupJob(job_id) => (job_id.clone(), EventType::CleanupJob),
            Event::ReceivedStage0Tasks(stage0_ids) => {
                if stage0_ids.is_empty() {
                    continue;
                }
                let job_id = stage0_ids[0].job_id.clone();
                (job_id, EventType::ReceivedStage0Tasks)
            }
        };
        if let Some(&idx) = index_map.get(&key) {
            if let Event::ReceivedStage0Tasks(existing_ids) = &mut merged[idx] {
                if let Event::ReceivedStage0Tasks(new_ids) = &event {
                    let existing_set: HashSet<StageId> =
                        existing_ids.iter().cloned().collect();
                    for id in new_ids {
                        if !existing_set.contains(id) {
                            existing_ids.push(id.clone());
                        }
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
    pub local_node: NodeId,
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
            for event in merged {
                self.handle_event(event).await;
            }
        }
    }

    async fn handle_event(&self, event: Event) {
        debug!("Handling event: {event:?}");
        match event {
            Event::CheckJobCompleted(job_id) => {
                let cluster = self.cluster.clone();
                let network = self.network.clone();
                let local_stages = self.local_stages.clone();
                let sender = self.sender.clone();
                tokio::spawn(async move {
                    handle_check_job_completed(
                        &cluster,
                        &network,
                        &local_stages,
                        &sender,
                        job_id,
                    )
                    .await;
                });
            }
            Event::CleanupJob(job_id) => {
                let local_node = self.local_node.clone();
                let cluster = self.cluster.clone();
                let network = self.network.clone();
                let local_stages = self.local_stages.clone();
                tokio::spawn(async move {
                    if let Err(e) = cleanup_job(
                        &local_node,
                        &cluster,
                        &network,
                        &local_stages,
                        job_id.clone(),
                    )
                    .await
                    {
                        error!("Failed to cleanup job {job_id}: {e}");
                    }
                });
            }
            Event::ReceivedStage0Tasks(stage0_ids) => {
                self.handle_received_stage0_tasks(stage0_ids).await;
            }
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

async fn handle_check_job_completed(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    sender: &Sender<Event>,
    job_id: JobId,
) {
    match (|| async { check_job_completed(cluster, network, local_stages, job_id.clone()).await })
        .retry(job_check_retry_strategy())
        .await
    {
        Ok(Some(true)) => {
            debug!("Job {job_id} completed, remove it from cluster");
            if let Err(e) = send_event_with_timeout(sender, Event::CleanupJob(job_id.clone())).await
            {
                error!("Failed to send cleanup job event for job {job_id}: {e}");
            }
        }
        Ok(_) => {}
        Err(err) => {
            error!("Failed to check job {job_id} completed: {err}");
        }
    }
}

pub async fn check_job_completed(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_id: JobId,
) -> DistResult<Option<bool>> {
    let job_ids = HashSet::from([job_id.clone()]);

    // First, get local status
    let mut combined_status = local_stage_stats(local_stages, Some(&job_ids));

    let local_node_id = network.local_node();

    // Get alive nodes for validation
    let alive_nodes = cluster.alive_nodes().await?;
    let alive_set: HashSet<NodeId> = alive_nodes.keys().cloned().collect();

    // Determine target nodes from job_task_distribution if available
    let target_nodes = {
        let guard = local_stages.lock();
        guard
            .values()
            .find(|stage| stage.stage_id.job_id == job_id)
            .map(|stage| {
                let mut nodes: HashSet<NodeId> =
                    stage.job_task_distribution.values().cloned().collect();
                nodes.remove(&local_node_id);
                nodes
            })
    };

    let mut handles = Vec::new();
    match target_nodes {
        Some(nodes) if nodes.is_subset(&alive_set) => {
            for node_id in nodes {
                let network = network.clone();
                let node_id = node_id.clone();
                let job_id = job_id.clone();
                let handle = tokio::spawn(async move {
                    network.get_job_statuses(node_id, Some(vec![job_id])).await
                });
                handles.push(handle);
            }
        }
        Some(nodes) => {
            let missing: Vec<_> = nodes.difference(&alive_set).collect();
            warn!(
                "Job {job_id} is polluted: task nodes {missing:?} are not alive, treat as completed"
            );
            return Ok(Some(true));
        }
        None => {
            warn!("No job_task_distribution found for job {job_id}, skipping remote status check");
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

    let stage0 = StageId {
        job_id: job_id.clone(),
        stage: 0,
    };

    let Some(stage0_info) = combined_status.get(&stage0) else {
        return Ok(None);
    };

    // Check if all assigned partitions are completed
    for partition in &stage0_info.assigned_partitions {
        let is_completed = stage0_info
            .task_set_infos
            .iter()
            .any(|ts| ts.dropped_partitions.contains_key(partition));
        if !is_completed {
            return Ok(Some(false));
        }
    }

    Ok(Some(true))
}

pub fn local_stage_stats(
    stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_ids: Option<&HashSet<JobId>>,
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

pub async fn cleanup_job(
    local_node: &NodeId,
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_id: JobId,
) -> DistResult<()> {
    let alive_nodes = cluster.alive_nodes().await?;
    let alive_set: HashSet<NodeId> = alive_nodes.keys().cloned().collect();

    let target_nodes = {
        let guard = local_stages.lock();
        guard
            .values()
            .find(|stage| stage.stage_id.job_id == job_id)
            .map(|stage| {
                let mut nodes: HashSet<NodeId> =
                    stage.job_task_distribution.values().cloned().collect();
                nodes.insert(local_node.clone());
                nodes
            })
    };

    let nodes_to_clean: HashSet<NodeId> = match target_nodes {
        Some(nodes) if nodes.is_subset(&alive_set) => nodes,
        Some(nodes) => {
            let missing: Vec<_> = nodes.difference(&alive_set).collect();
            warn!("Job {job_id} is polluted: task nodes {missing:?} are not alive");
            nodes
                .into_iter()
                .filter(|n| n == local_node || alive_set.contains(n))
                .collect()
        }
        None => {
            let mut nodes = alive_set;
            nodes.insert(local_node.clone());
            nodes
        }
    };

    let mut futures = Vec::new();
    for node_id in nodes_to_clean {
        if &node_id == local_node {
            let mut guard = local_stages.lock();
            guard.retain(|stage_id, _| stage_id.job_id != job_id);
        } else {
            let network = network.clone();
            let node_id = node_id.clone();
            let job_id = job_id.clone();
            futures.push(async move { network.cleanup_jobs(node_id, vec![job_id]).await });
        }
    }

    for res in join_all(futures).await {
        res?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job_id(id: &str) -> JobId {
        Arc::from(id)
    }

    fn stage_id(job_id: JobId, stage: u32) -> StageId {
        StageId { job_id, stage }
    }

    #[test]
    fn test_merge_check_job_completed_dedup() {
        let j1 = job_id("job1");
        let mut events = vec![
            Event::CheckJobCompleted(j1.clone()),
            Event::CheckJobCompleted(j1.clone()),
            Event::CheckJobCompleted(j1.clone()),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 1);
        assert!(matches!(merged[0], Event::CheckJobCompleted(ref id) if id == &j1));
    }

    #[test]
    fn test_merge_cleanup_job_dedup() {
        let j1 = job_id("job1");
        let mut events = vec![
            Event::CleanupJob(j1.clone()),
            Event::CleanupJob(j1.clone()),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 1);
        assert!(matches!(merged[0], Event::CleanupJob(ref id) if id == &j1));
    }

    #[test]
    fn test_merge_received_stage0_tasks_union_order_stable() {
        let j1 = job_id("job1");
        let mut events = vec![
            Event::ReceivedStage0Tasks(vec![stage_id(j1.clone(), 0)]),
            Event::ReceivedStage0Tasks(vec![stage_id(j1.clone(), 1)]),
            Event::ReceivedStage0Tasks(vec![stage_id(j1.clone(), 0), stage_id(j1.clone(), 2)]),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 1);
        if let Event::ReceivedStage0Tasks(ids) = &merged[0] {
            assert_eq!(ids.len(), 3);
            assert_eq!(ids[0].stage, 0);
            assert_eq!(ids[1].stage, 1);
            assert_eq!(ids[2].stage, 2);
        } else {
            panic!("Expected ReceivedStage0Tasks");
        }
    }

    #[test]
    fn test_merge_received_stage0_tasks_empty_skipped() {
        let j1 = job_id("job1");
        let mut events = vec![
            Event::ReceivedStage0Tasks(vec![]),
            Event::ReceivedStage0Tasks(vec![stage_id(j1.clone(), 0)]),
            Event::ReceivedStage0Tasks(vec![]),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 1);
        if let Event::ReceivedStage0Tasks(ids) = &merged[0] {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0].stage, 0);
        } else {
            panic!("Expected ReceivedStage0Tasks");
        }
    }

    #[test]
    fn test_merge_received_stage0_tasks_all_empty() {
        let mut events = vec![
            Event::ReceivedStage0Tasks(vec![]),
            Event::ReceivedStage0Tasks(vec![]),
        ];
        let merged = merge_events(&mut events);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_merge_no_cross_job() {
        let j1 = job_id("job1");
        let j2 = job_id("job2");
        let mut events = vec![
            Event::CheckJobCompleted(j1.clone()),
            Event::CheckJobCompleted(j2.clone()),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_merge_no_cross_type() {
        let j1 = job_id("job1");
        let mut events = vec![
            Event::CheckJobCompleted(j1.clone()),
            Event::CleanupJob(j1.clone()),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_merge_mixed_events() {
        let j1 = job_id("job1");
        let j2 = job_id("job2");
        let mut events = vec![
            Event::CheckJobCompleted(j1.clone()),
            Event::CleanupJob(j1.clone()),
            Event::CheckJobCompleted(j2.clone()),
            Event::CheckJobCompleted(j1.clone()),
            Event::CleanupJob(j2.clone()),
            Event::CleanupJob(j2.clone()),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 4);
    }

    #[test]
    fn test_merge_empty_vec() {
        let mut events: Vec<Event> = vec![];
        let merged = merge_events(&mut events);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_merge_preserves_first_occurrence_order() {
        let j1 = job_id("job1");
        let j2 = job_id("job2");
        let j3 = job_id("job3");
        let mut events = vec![
            Event::CheckJobCompleted(j2.clone()),
            Event::CleanupJob(j1.clone()),
            Event::CheckJobCompleted(j3.clone()),
        ];
        let merged = merge_events(&mut events);
        assert_eq!(merged.len(), 3);
        assert!(matches!(merged[0], Event::CheckJobCompleted(ref id) if id == &j2));
        assert!(matches!(merged[1], Event::CleanupJob(ref id) if id == &j1));
        assert!(matches!(merged[2], Event::CheckJobCompleted(ref id) if id == &j3));
    }

    #[tokio::test]
    async fn test_recv_many_batch() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(16);
        let j1 = job_id("job1");
        let j2 = job_id("job2");

        tx.send(Event::CheckJobCompleted(j1.clone())).await.unwrap();
        tx.send(Event::CheckJobCompleted(j1.clone())).await.unwrap();
        tx.send(Event::CheckJobCompleted(j2.clone())).await.unwrap();
        drop(tx);

        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        let received = rx.recv_many(&mut batch, MAX_BATCH_SIZE).await;
        assert_eq!(received, 3);

        let merged = merge_events(&mut batch);
        assert_eq!(merged.len(), 2);
    }
}

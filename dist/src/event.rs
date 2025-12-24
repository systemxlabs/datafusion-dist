use std::{collections::HashMap, sync::Arc};

use log::{debug, error};
use tokio::sync::{Mutex, mpsc::Receiver};
use uuid::Uuid;

use crate::{
    DistResult,
    cluster::{DistCluster, NodeId},
    network::{DistNetwork, StageInfo},
    planner::StageId,
    runtime::StageState,
};

#[derive(Debug)]
pub enum Event {
    TryCleanupJob(Uuid),
}

pub fn start_event_handler(mut handler: EventHandler) {
    tokio::spawn(async move {
        handler.start().await;
    });
}

pub struct EventHandler {
    pub local_node: NodeId,
    pub cluster: Arc<dyn DistCluster>,
    pub network: Arc<dyn DistNetwork>,
    pub local_stages: Arc<Mutex<HashMap<StageId, Arc<StageState>>>>,
    pub receiver: Receiver<Event>,
}

impl EventHandler {
    pub async fn start(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            debug!("Received event: {event:?}");
            match event {
                Event::TryCleanupJob(job_id) => {
                    self.handle_try_cleanup_job(job_id).await;
                }
            }
        }
    }

    async fn handle_try_cleanup_job(&mut self, job_id: Uuid) {
        match check_job_completed(&self.cluster, &self.network, &self.local_stages, job_id).await {
            Ok(Some(true)) => {
                debug!("Job {job_id} completed, removeing it from cluster");
                let alive_nodes = match self.cluster.alive_nodes().await {
                    Ok(nodes) => nodes,
                    Err(err) => {
                        error!("Failed to get alive nodes: {err}");
                        return;
                    }
                };

                for node_id in alive_nodes.keys() {
                    if node_id == &self.local_node {
                        let mut guard = self.local_stages.lock().await;
                        guard.retain(|stage_id, _| stage_id.job_id != job_id);
                        drop(guard);
                    } else {
                        // Send cleanup request to remote node
                        if let Err(err) = self.network.cleanup_job(node_id.clone(), job_id).await {
                            error!(
                                "Failed to send cleanup job {job_id} request to node {node_id}: {err}"
                            );
                        }
                    }
                }
            }
            Ok(_) => {}
            Err(err) => {
                error!("Failed to check job {job_id} completed: {err}");
            }
        }
    }
}

pub async fn check_job_completed(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, Arc<StageState>>>>,
    job_id: Uuid,
) -> DistResult<Option<bool>> {
    // First, get local status
    let mut combined_status = local_job_status(local_stages, Some(job_id)).await;

    // Then, get status from all other alive nodes
    let node_states = cluster.alive_nodes().await?;

    let local_node_id = network.local_node();

    let mut handles = Vec::new();
    for node_id in node_states.keys() {
        if *node_id != local_node_id {
            let network = network.clone();
            let node_id = node_id.clone();
            let handle =
                tokio::spawn(async move { network.get_job_status(node_id, Some(job_id)).await });
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

    let stage0 = StageId { job_id, stage: 0 };

    let Some(stage0_info) = combined_status.get(&stage0) else {
        return Ok(None);
    };

    // Check if all assigned partitions are completed
    for partition in &stage0_info.assigned_partitions {
        let is_completed = stage0_info
            .task_set_infos
            .iter()
            .any(|ts| ts.completed_partitions.contains(partition));
        if !is_completed {
            return Ok(Some(false));
        }
    }

    Ok(Some(true))
}

pub async fn local_job_status(
    stages: &Arc<Mutex<HashMap<StageId, Arc<StageState>>>>,
    job_id: Option<Uuid>,
) -> HashMap<StageId, StageInfo> {
    let guard = stages.lock().await;

    let mut result = HashMap::new();
    for (stage_id, stage_state) in guard.iter() {
        if job_id.is_none() || stage_id.job_id == job_id.unwrap() {
            let stage_info = StageInfo::from_stage_state(stage_state).await;
            result.insert(*stage_id, stage_info);
        }
    }

    result
}

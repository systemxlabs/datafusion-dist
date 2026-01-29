use std::{collections::HashMap, sync::Arc};

use log::{debug, error};
use parking_lot::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    DistResult, JobId,
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
        while let Some(event) = self.receiver.recv().await {
            debug!("Received event: {event:?}");
            match event {
                Event::CheckJobCompleted(job_id) => {
                    self.handle_check_job_completed(job_id).await;
                }
                Event::CleanupJob(job_id) => {
                    self.handle_cleanup_job(job_id).await;
                }
                Event::ReceivedStage0Tasks(stage0_ids) => {
                    self.handle_received_stage0_tasks(stage0_ids).await;
                }
            }
        }
    }

    async fn handle_check_job_completed(&mut self, job_id: JobId) {
        match check_job_completed(
            &self.cluster,
            &self.network,
            &self.local_stages,
            job_id.clone(),
        )
        .await
        {
            Ok(Some(true)) => {
                debug!("Job {job_id} completed, remove it from cluster");

                if let Err(e) = self.sender.send(Event::CleanupJob(job_id.clone())).await {
                    error!("Failed to send cleanup job event for job {job_id}: {e}");
                }
            }
            Ok(_) => {}
            Err(err) => {
                error!("Failed to check job {job_id} completed: {err}");
            }
        }
    }

    async fn handle_cleanup_job(&mut self, job_id: JobId) {
        if let Err(e) = cleanup_job(
            &self.local_node,
            &self.cluster,
            &self.network,
            &self.local_stages,
            job_id.clone(),
        )
        .await
        {
            error!("Failed to cleanup job {job_id}: {e}");
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
                && let Err(e) = sender
                    .send(Event::CleanupJob(stage_id.job_id.clone()))
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

pub async fn check_job_completed(
    cluster: &Arc<dyn DistCluster>,
    network: &Arc<dyn DistNetwork>,
    local_stages: &Arc<Mutex<HashMap<StageId, StageState>>>,
    job_id: JobId,
) -> DistResult<Option<bool>> {
    let mut combined_status = local_stage_stats(local_stages, Some(job_id.clone()));

    let node_states = cluster.alive_nodes().await?;

    let local_node_id = network.local_node();

    let mut handles = Vec::new();
    for node_id in node_states.keys() {
        if *node_id != local_node_id {
            let network = network.clone();
            let node_id = node_id.clone();
            let job_id = job_id.clone();
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

    let stage0 = StageId {
        job_id: job_id.clone(),
        stage: 0,
    };

    let Some(stage0_info) = combined_status.get(&stage0) else {
        return Ok(None);
    };

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
    job_id: Option<JobId>,
) -> HashMap<StageId, StageInfo> {
    let guard = stages.lock();

    let mut result = HashMap::new();
    for (stage_id, stage_state) in guard.iter() {
        if job_id.is_none() || stage_id.job_id.as_ref() == job_id.as_ref().unwrap().as_ref() {
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

    for node_id in alive_nodes.keys() {
        if node_id == local_node {
            let mut guard = local_stages.lock();
            guard.retain(|stage_id, _| stage_id.job_id != job_id);
            drop(guard);
        } else {
            network.cleanup_job(node_id.clone(), job_id.clone()).await?
        }
    }
    Ok(())
}

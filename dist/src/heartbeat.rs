use std::{collections::HashMap, sync::Arc, time::Duration};

use log::error;
use tokio::sync::Mutex;

use crate::{
    cluster::{DistCluster, NodeId, NodeState, NodeStatus},
    planner::StageId,
    runtime::StageState,
};

#[derive(Debug, Clone)]
pub struct Heartbeater {
    pub node_id: NodeId,
    pub cluster: Arc<dyn DistCluster>,
    pub stages: Arc<Mutex<HashMap<StageId, Arc<StageState>>>>,
    pub heartbeat_interval: Duration,
    pub status: Arc<Mutex<NodeStatus>>,
}

impl Heartbeater {
    pub fn new(
        node_id: NodeId,
        cluster: Arc<dyn DistCluster>,
        stages: Arc<Mutex<HashMap<StageId, Arc<StageState>>>>,
        heartbeat_interval: Duration,
    ) -> Self {
        Heartbeater {
            node_id,
            cluster,
            stages,
            heartbeat_interval,
            status: Arc::new(Mutex::new(NodeStatus::Available)),
        }
    }

    pub async fn set_status(&self, status: NodeStatus) {
        *self.status.lock().await = status;
    }

    pub async fn get_status(&self) -> NodeStatus {
        *self.status.lock().await
    }

    pub fn start(&self) {
        let node_id = self.node_id.clone();
        let cluster = self.cluster.clone();
        let stages = self.stages.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let status = self.status.clone();

        tokio::spawn(async move {
            let mut sys = sysinfo::System::new();

            loop {
                let guard = stages.lock().await;
                let mut num_running_tasks = 0;
                for (_, state) in guard.iter() {
                    num_running_tasks += state.num_running_tasks().await;
                }
                drop(guard);

                sys.refresh_memory();
                sys.refresh_cpu_usage();

                let node_state = NodeState {
                    status: *status.lock().await,
                    total_memory: sys.total_memory(),
                    used_memory: sys.used_memory(),
                    free_memory: sys.free_memory(),
                    available_memory: sys.available_memory(),
                    global_cpu_usage: sys.global_cpu_usage(),
                    num_running_tasks: num_running_tasks as u32,
                };
                if let Err(e) = cluster.heartbeat(node_id.clone(), node_state).await {
                    error!("Failed to send heartbeat: {e}");
                }

                tokio::time::sleep(heartbeat_interval).await;
            }
        });
    }
}

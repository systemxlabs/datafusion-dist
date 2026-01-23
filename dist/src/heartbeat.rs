use std::{collections::HashMap, sync::Arc, time::Duration};

use log::{debug, error};
use parking_lot::Mutex;

use crate::{
    cluster::{DistCluster, NodeId, NodeState, NodeStatus},
    planner::StageId,
    runtime::StageState,
};

#[derive(Debug, Clone)]
pub struct Heartbeater {
    pub node_id: NodeId,
    pub cluster: Arc<dyn DistCluster>,
    pub stages: Arc<Mutex<HashMap<StageId, StageState>>>,
    pub heartbeat_interval: Duration,
    pub status: Arc<Mutex<NodeStatus>>,
}

impl Heartbeater {
    pub async fn send_heartbeat(&self) {
        let mut num_running_tasks = 0;
        {
            let guard = self.stages.lock();
            for (_, state) in guard.iter() {
                num_running_tasks += state.num_running_tasks();
            }
            drop(guard);
        }

        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        sys.refresh_cpu_usage();

        let node_state = NodeState {
            status: *self.status.lock(),
            total_memory: sys.total_memory(),
            used_memory: sys.used_memory(),
            free_memory: sys.free_memory(),
            available_memory: sys.available_memory(),
            global_cpu_usage: sys.global_cpu_usage(),
            num_running_tasks: num_running_tasks as u32,
        };
        match self
            .cluster
            .heartbeat(self.node_id.clone(), node_state)
            .await
        {
            Ok(_) => {
                debug!("Heartbeat sent successfully");
            }
            Err(e) => {
                error!("Failed to send heartbeat: {e:?}");
            }
        }
    }

    pub fn start(&self) {
        let heartbeater = self.clone();
        let heartbeat_interval = self.heartbeat_interval;

        tokio::spawn(async move {
            loop {
                heartbeater.send_heartbeat().await;
                tokio::time::sleep(heartbeat_interval).await;
            }
        });
    }
}

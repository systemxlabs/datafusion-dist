use std::{collections::HashMap, sync::Arc, time::Duration};

use log::error;
use tokio::sync::Mutex;

use crate::{
    cluster::{DistCluster, NodeId, NodeState},
    planner::TaskId,
    runtime::TaskState,
};

pub struct Heartbeater {
    pub node_id: NodeId,
    pub cluster: Arc<dyn DistCluster>,
    pub tasks: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    pub heartbeat_interval: Duration,
}

impl Heartbeater {
    pub fn new(
        node_id: NodeId,
        cluster: Arc<dyn DistCluster>,
        tasks: Arc<Mutex<HashMap<TaskId, TaskState>>>,
        heartbeat_interval: Duration,
    ) -> Self {
        Heartbeater {
            node_id,
            cluster,
            tasks,
            heartbeat_interval,
        }
    }

    pub fn start(&self) {
        let node_id = self.node_id.clone();
        let cluster = self.cluster.clone();
        let tasks = self.tasks.clone();
        let heartbeat_interval = self.heartbeat_interval;
        tokio::spawn(async move {
            loop {
                let guard = tasks.lock().await;
                let (num_ready_tasks, num_running_tasks) =
                    guard
                        .iter()
                        .fold((0u32, 0u32), |(ready, running), (_, state)| {
                            if state.running {
                                (ready, running + 1)
                            } else {
                                (ready + 1, running)
                            }
                        });
                drop(guard);

                let mut sys = sysinfo::System::new();
                sys.refresh_memory();
                sys.refresh_cpu_usage();

                let node_state = NodeState {
                    total_memory: sys.total_memory(),
                    used_memory: sys.used_memory(),
                    free_memory: sys.free_memory(),
                    available_memory: sys.available_memory(),
                    global_cpu_usage: sys.global_cpu_usage(),
                    num_ready_tasks,
                    num_running_tasks,
                };
                if let Err(e) = cluster.heartbeat(node_id.clone(), node_state).await {
                    error!("Failed to send heartbeat: {e}");
                }

                tokio::time::sleep(heartbeat_interval).await;
            }
        });
    }
}

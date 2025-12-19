use std::{collections::HashMap, sync::Arc, time::Duration};

use log::error;
use tokio::sync::Mutex;

use crate::{
    cluster::{DistCluster, NodeId, NodeState},
    network::TaskId,
    runtime::TaskState,
};

pub struct Heartbeater {
    pub node_id: NodeId,
    pub cluster: Arc<dyn DistCluster>,
    pub tasks: Arc<Mutex<HashMap<TaskId, TaskState>>>,
}

impl Heartbeater {
    pub fn new(
        node_id: NodeId,
        cluster: Arc<dyn DistCluster>,
        tasks: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    ) -> Self {
        Heartbeater {
            node_id,
            cluster,
            tasks,
        }
    }

    pub fn start(&self) {
        let cluster = self.cluster.clone();
        let node_id = self.node_id.clone();
        tokio::spawn(async move {
            loop {
                let node_state = NodeState::default();
                if let Err(e) = cluster.heartbeat(node_id.clone(), node_state).await {
                    error!("Failed to send heartbeat: {e}");
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }
}

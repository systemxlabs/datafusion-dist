use datafusion_dist::{
    DistError, DistResult, RecordBatchStream,
    cluster::NodeId,
    network::{DistNetwork, ScheduledTasks, StageId, TaskId},
    util::get_local_ip,
};
use tonic::transport::Endpoint;

use crate::protobuf::{self, SendTasksReq, dist_tonic_service_client::DistTonicServiceClient};

#[derive(Debug)]
pub struct DistTonicNetwork {
    port: u16,
}

#[async_trait::async_trait]
impl DistNetwork for DistTonicNetwork {
    fn local_node(&self) -> NodeId {
        NodeId {
            host: get_local_ip(),
            port: self.port,
        }
    }

    async fn send_tasks(&self, node_id: NodeId, scheduled_tasks: ScheduledTasks) -> DistResult<()> {
        let addr = format!("http://{}:{}", node_id.host, node_id.port);
        let endpoint = Endpoint::from_shared(addr).map_err(|e| DistError::network(Box::new(e)))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| DistError::network(Box::new(e)))?;
        let mut tonic_client =
            DistTonicServiceClient::new(channel).max_encoding_message_size(usize::MAX);
        let send_tasks_req = serialize_scheduled_tasks(scheduled_tasks)?;
        tonic_client
            .send_tasks(send_tasks_req)
            .await
            .map_err(|e| DistError::network(Box::new(e)))?;
        Ok(())
    }

    async fn execute_task(
        &self,
        _node_id: NodeId,
        _task_id: TaskId,
    ) -> DistResult<RecordBatchStream> {
        todo!()
    }
}

fn serialize_scheduled_tasks(_scheduled_tasks: ScheduledTasks) -> DistResult<SendTasksReq> {
    todo!()
}

fn _serialize_stage_id(stage_id: StageId) -> protobuf::StageId {
    protobuf::StageId {
        job_id: stage_id.job_id.to_string(),
        stage: stage_id.stage,
    }
}

fn _serialize_task_id(task_id: TaskId) -> protobuf::TaskId {
    protobuf::TaskId {
        job_id: task_id.job_id.to_string(),
        stage: task_id.stage,
        partition: task_id.partition,
    }
}

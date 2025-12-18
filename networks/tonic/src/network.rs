use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion_dist::{
    DistError, DistResult, RecordBatchStream,
    cluster::NodeId,
    network::{DistNetwork, ScheduledTasks, StageId, TaskId},
    util::get_local_ip,
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use tonic::transport::Endpoint;

use crate::protobuf::{
    self, SendTasksReq, StagePlan, dist_tonic_service_client::DistTonicServiceClient,
};

#[derive(Debug)]
pub struct DistTonicNetwork {
    pub port: u16,
    pub extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DistTonicNetwork {
    fn serialize_scheduled_tasks(
        &self,
        scheduled_tasks: ScheduledTasks,
    ) -> DistResult<SendTasksReq> {
        let mut proto_stage_plans = Vec::new();
        for (stage_id, plan) in scheduled_tasks.stage_plans {
            proto_stage_plans.push(self.serialize_stage_plan(stage_id, plan)?);
        }
        let proto_task_ids = scheduled_tasks
            .task_ids
            .into_iter()
            .map(serialize_task_id)
            .collect::<Vec<_>>();

        Ok(SendTasksReq {
            stage_plans: proto_stage_plans,
            tasks: proto_task_ids,
        })
    }

    fn serialize_stage_plan(
        &self,
        stage_id: StageId,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<StagePlan> {
        let proto_stage_id = serialize_stage_id(stage_id);
        let mut plan_buf: Vec<u8> = vec![];
        let plan_proto =
            PhysicalPlanNode::try_from_physical_plan(plan, self.extension_codec.as_ref())?;
        plan_proto.try_encode(&mut plan_buf)?;
        Ok(StagePlan {
            stage_id: Some(proto_stage_id),
            plan: plan_buf,
        })
    }
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
        let send_tasks_req = self.serialize_scheduled_tasks(scheduled_tasks)?;
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

fn serialize_stage_id(stage_id: StageId) -> protobuf::StageId {
    protobuf::StageId {
        job_id: stage_id.job_id.to_string(),
        stage: stage_id.stage,
    }
}

fn serialize_task_id(task_id: TaskId) -> protobuf::TaskId {
    protobuf::TaskId {
        job_id: task_id.job_id.to_string(),
        stage: task_id.stage,
        partition: task_id.partition,
    }
}

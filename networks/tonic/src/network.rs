use std::sync::Arc;

use datafusion::{
    arrow::{self, array::RecordBatch, error::ArrowError, ipc::reader::StreamReader},
    physical_plan::ExecutionPlan,
};
use datafusion_dist::{
    DistError, DistResult, RecordBatchStream,
    cluster::NodeId,
    network::{DistNetwork, ScheduledTasks},
    planner::{StageId, TaskId},
    util::get_local_ip,
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, ComposedPhysicalExtensionCodec, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use futures::StreamExt;
use tonic::{
    Status,
    transport::{Channel, Endpoint},
};

use crate::{
    codec::DistPhysicalExtensionEncoder,
    protobuf::{self, SendTasksReq, StagePlan, dist_tonic_service_client::DistTonicServiceClient},
};

#[derive(Debug)]
pub struct DistTonicNetwork {
    pub port: u16,
    pub composed_extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DistTonicNetwork {
    pub fn new(port: u16, app_extension_codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        let composed_extension_codec = Arc::new(ComposedPhysicalExtensionCodec::new(vec![
            app_extension_codec.clone(),
            Arc::new(DistPhysicalExtensionEncoder {
                app_extension_codec,
            }),
        ]));
        Self {
            port,
            composed_extension_codec,
        }
    }
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
            PhysicalPlanNode::try_from_physical_plan(plan, self.composed_extension_codec.as_ref())?;
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
        let channel = build_tonic_channel(node_id).await?;
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
        node_id: NodeId,
        task_id: TaskId,
    ) -> DistResult<RecordBatchStream> {
        let channel = build_tonic_channel(node_id).await?;
        let mut tonic_client =
            DistTonicServiceClient::new(channel).max_decoding_message_size(usize::MAX);
        let stream = tonic_client
            .execute_task(serialize_task_id(task_id))
            .await
            .map_err(|e| DistError::network(Box::new(e)))?;
        Ok(stream.into_inner().map(parse_record_batch_res).boxed())
    }
}

async fn build_tonic_channel(node_id: NodeId) -> DistResult<Channel> {
    let addr = format!("http://{}:{}", node_id.host, node_id.port);
    let endpoint = Endpoint::from_shared(addr).map_err(|e| DistError::network(Box::new(e)))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| DistError::network(Box::new(e)))?;
    Ok(channel)
}

pub fn serialize_stage_id(stage_id: StageId) -> protobuf::StageId {
    protobuf::StageId {
        job_id: stage_id.job_id.to_string(),
        stage: stage_id.stage,
    }
}

pub fn serialize_task_id(task_id: TaskId) -> protobuf::TaskId {
    protobuf::TaskId {
        job_id: task_id.job_id.to_string(),
        stage: task_id.stage,
        partition: task_id.partition,
    }
}

fn parse_record_batch_res(
    proto_res: Result<protobuf::RecordBatch, Status>,
) -> DistResult<RecordBatch> {
    let proto_batch = proto_res.map_err(|e| DistError::network(Box::new(e)))?;
    let reader = StreamReader::try_new(proto_batch.data.as_slice(), None)?;
    let mut batches = reader.into_iter().collect::<Result<Vec<_>, ArrowError>>()?;
    if batches.len() == 1 {
        return Ok(batches.remove(0));
    }
    let first_batch = batches
        .first()
        .ok_or_else(|| DistError::internal("No batch found in stream reader"))?;
    let batch = arrow::compute::concat_batches(first_batch.schema_ref(), &batches)?;
    Ok(batch)
}

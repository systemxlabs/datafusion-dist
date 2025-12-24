use std::{collections::HashMap, sync::Arc};

use datafusion::physical_plan::ExecutionPlan;
use datafusion_dist::{
    DistError, DistResult, RecordBatchStream,
    cluster::NodeId,
    network::{DistNetwork, ScheduledTasks, StageInfo},
    planner::{StageId, TaskId},
    util::get_local_ip,
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, ComposedPhysicalExtensionCodec, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use futures::StreamExt;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use crate::{
    codec::DistPhysicalExtensionEncoder,
    protobuf::{self, SendTasksReq, StagePlan, dist_tonic_service_client::DistTonicServiceClient},
    serde::{
        parse_record_batch_res, parse_stage_id, parse_stage_info, serialize_stage_id,
        serialize_task_id,
    },
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

    async fn get_job_status(
        &self,
        node_id: NodeId,
        job_id: Uuid,
    ) -> DistResult<HashMap<StageId, StageInfo>> {
        let channel = build_tonic_channel(node_id).await?;
        let mut tonic_client = DistTonicServiceClient::new(channel);

        let req = protobuf::GetJobStatusReq {
            job_id: job_id.to_string(),
        };

        let resp = tonic_client
            .get_job_status(req)
            .await
            .map_err(|e| DistError::network(Box::new(e)))?
            .into_inner();

        let mut result = HashMap::new();
        for proto_stage_info in resp.stage_infos {
            let stage_id = parse_stage_id(
                proto_stage_info
                    .stage_id
                    .clone()
                    .ok_or_else(|| DistError::internal("Missing stage_id in StageInfo"))?,
            );
            let stage_info = parse_stage_info(proto_stage_info);
            result.insert(stage_id, stage_info);
        }

        Ok(result)
    }

    async fn cleanup_job(&self, node_id: NodeId, job_id: Uuid) -> DistResult<()> {
        let channel = build_tonic_channel(node_id).await?;
        let mut tonic_client = DistTonicServiceClient::new(channel);

        let req = protobuf::CleanupJobReq {
            job_id: job_id.to_string(),
        };

        tonic_client
            .cleanup_job(req)
            .await
            .map_err(|e| DistError::network(Box::new(e)))?;

        Ok(())
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

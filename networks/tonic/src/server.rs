use std::{collections::HashMap, sync::Arc};

use datafusion::{physical_plan::ExecutionPlan, prelude::SessionContext};
use datafusion_dist::{
    DistResult,
    network::{ScheduledTasks, StageId, TaskId},
    runtime::DistRuntime,
};
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::protobuf::{
    self, SendTasksReq, SendTasksResp, StagePlan, dist_tonic_service_server::DistTonicService,
};

pub struct DistTonicServer {
    pub runtime: Arc<DistRuntime>,
    pub ctx: SessionContext,
    pub extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DistTonicServer {
    fn parse_send_tasks_req(&self, req: SendTasksReq) -> DistResult<ScheduledTasks> {
        let stage_plans = req
            .stage_plans
            .into_iter()
            .map(|p| self.parse_stage_plan(p))
            .collect::<DistResult<HashMap<_, _>>>()?;
        let task_ids = req.tasks.into_iter().map(parse_task_id).collect::<Vec<_>>();
        Ok(ScheduledTasks::new(stage_plans, task_ids))
    }

    fn parse_stage_plan(&self, proto: StagePlan) -> DistResult<(StageId, Arc<dyn ExecutionPlan>)> {
        let stage_id = parse_stage_id(proto.stage_id.expect("stage_id should not be null"));
        let plan: Arc<dyn ExecutionPlan> =
            PhysicalPlanNode::try_decode(&proto.plan).and_then(|proto| {
                proto.try_into_physical_plan(
                    &self.ctx,
                    &self.ctx.runtime_env(),
                    self.extension_codec.as_ref(),
                )
            })?;
        Ok((stage_id, plan))
    }
}

#[async_trait::async_trait]
impl DistTonicService for DistTonicServer {
    async fn send_tasks(
        &self,
        request: Request<SendTasksReq>,
    ) -> Result<Response<SendTasksResp>, Status> {
        let scheduled_tasks = self
            .parse_send_tasks_req(request.into_inner())
            .map_err(|e| Status::internal(format!("Failed to parse SendTasksReq: {e}")))?;
        self.runtime.receive_tasks(scheduled_tasks).await;
        Ok(Response::new(SendTasksResp {}))
    }
}

fn parse_stage_id(proto: protobuf::StageId) -> StageId {
    let job_id = Uuid::parse_str(&proto.job_id)
        .unwrap_or_else(|_| panic!("Failed to parse job id {} as uuid", proto.job_id));
    StageId {
        job_id,
        stage: proto.stage,
    }
}

fn parse_task_id(proto: protobuf::TaskId) -> TaskId {
    let job_id = Uuid::parse_str(&proto.job_id)
        .unwrap_or_else(|_| panic!("Failed to parse job id {} as uuid", proto.job_id));
    TaskId {
        job_id,
        stage: proto.stage,
        partition: proto.partition,
    }
}

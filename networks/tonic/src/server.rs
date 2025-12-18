use datafusion_dist::{
    network::{ScheduledTasks, StageId, TaskId},
    runtime::DistRuntime,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::protobuf::{
    self, SendTasksReq, SendTasksResp, dist_tonic_service_server::DistTonicService,
};

#[async_trait::async_trait]
impl DistTonicService for DistRuntime {
    async fn send_tasks(
        &self,
        request: Request<SendTasksReq>,
    ) -> Result<Response<SendTasksResp>, Status> {
        let scheduled_tasks = parse_send_tasks_req(request.into_inner())?;
        self.receive_tasks(scheduled_tasks).await;
        Ok(Response::new(SendTasksResp {}))
    }
}

#[allow(clippy::result_large_err)]
fn parse_send_tasks_req(_req: SendTasksReq) -> Result<ScheduledTasks, Status> {
    todo!()
}

fn _parse_stage_id(proto: protobuf::StageId) -> StageId {
    let job_id = Uuid::parse_str(&proto.job_id)
        .unwrap_or_else(|_| panic!("Failed to parse job id {} as uuid", proto.job_id));
    StageId {
        job_id,
        stage: proto.stage,
    }
}

fn _parse_task_id(proto: protobuf::TaskId) -> TaskId {
    let job_id = Uuid::parse_str(&proto.job_id)
        .unwrap_or_else(|_| panic!("Failed to parse job id {} as uuid", proto.job_id));
    TaskId {
        job_id,
        stage: proto.stage,
        partition: proto.partition,
    }
}

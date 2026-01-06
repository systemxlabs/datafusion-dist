use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightData, encode::FlightDataEncoderBuilder};
use datafusion::prelude::SessionContext;
use datafusion_dist::{
    DistResult,
    network::{ScheduledTasks, StageInfo},
    planner::StageId,
    runtime::DistRuntime,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, ComposedPhysicalExtensionCodec, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::{
    codec::DistPhysicalExtensionDecoder,
    protobuf::{
        self, CleanupJobReq, CleanupJobResp, GetJobStatusReq, GetJobStatusResp, SendTasksReq,
        SendTasksResp, StagePlan, dist_tonic_service_server::DistTonicService,
    },
    serde::{parse_stage_id, parse_task_distribution, parse_task_id, serialize_stage_info},
};

pub struct DistTonicServer {
    pub runtime: DistRuntime,
    pub ctx: SessionContext,
    pub composed_extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DistTonicServer {
    pub fn new(
        runtime: DistRuntime,
        ctx: SessionContext,
        app_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        let composed_extension_codec = Arc::new(ComposedPhysicalExtensionCodec::new(vec![
            app_extension_codec.clone(),
            Arc::new(DistPhysicalExtensionDecoder {
                runtime: runtime.clone(),
                ctx: ctx.clone(),
                app_extension_codec,
            }),
        ]));

        Self {
            runtime,
            ctx,
            composed_extension_codec,
        }
    }
}

impl DistTonicServer {
    fn parse_send_tasks_req(&self, req: SendTasksReq) -> DistResult<ScheduledTasks> {
        let stage_plans = req
            .stage_plans
            .into_iter()
            .map(|p| self.parse_stage_plan(p))
            .collect::<DistResult<HashMap<_, _>>>()?;
        let task_ids = req.tasks.into_iter().map(parse_task_id).collect::<Vec<_>>();
        let job_task_distribution = parse_task_distribution(
            req.job_task_distribution
                .expect("job task distribution is none"),
        );
        Ok(ScheduledTasks::new(
            stage_plans,
            task_ids,
            Arc::new(job_task_distribution),
        ))
    }

    fn parse_stage_plan(&self, proto: StagePlan) -> DistResult<(StageId, Arc<dyn ExecutionPlan>)> {
        let stage_id = parse_stage_id(proto.stage_id.expect("stage_id should not be null"));
        let plan: Arc<dyn ExecutionPlan> =
            PhysicalPlanNode::try_decode(&proto.plan).and_then(|proto| {
                proto.try_into_physical_plan(
                    &self.ctx,
                    &self.ctx.runtime_env(),
                    self.composed_extension_codec.as_ref(),
                )
            })?;
        Ok((stage_id, plan))
    }
}

type BoxedDistStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[async_trait::async_trait]
impl DistTonicService for DistTonicServer {
    type ExecuteTaskStream = BoxedDistStream<FlightData>;

    async fn send_tasks(
        &self,
        request: Request<SendTasksReq>,
    ) -> Result<Response<SendTasksResp>, Status> {
        let scheduled_tasks = self
            .parse_send_tasks_req(request.into_inner())
            .map_err(|e| Status::internal(format!("Failed to parse SendTasksReq: {e}")))?;
        self.runtime
            .receive_tasks(scheduled_tasks)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        Ok(Response::new(SendTasksResp {}))
    }

    async fn execute_task(
        &self,
        request: Request<protobuf::TaskId>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let task_id = parse_task_id(request.into_inner());
        let record_batch_stream = self.runtime.execute_local(task_id).await.map_err(|e| {
            Status::internal(format!("Failed to execute local task {task_id}: {e}"))
        })?;

        // Configure IPC write options with LZ4 compression
        let write_options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .map_err(|e| Status::internal(format!("Failed to configure IPC write options: {e}")))?;

        // Create flight data encoder directly from the record batch stream
        let flight_encoder = FlightDataEncoderBuilder::new()
            .with_options(write_options)
            .build(record_batch_stream.map_err(|e| FlightError::ExternalError(Box::new(e))));

        // Map FlightData to our protobuf FlightData
        let flight_data_stream = flight_encoder
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {e}")))
            .boxed();

        Ok(Response::new(flight_data_stream))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusReq>,
    ) -> Result<Response<GetJobStatusResp>, Status> {
        let status: HashMap<StageId, StageInfo> = match request.into_inner().job_id {
            Some(id) => {
                let job_id = Uuid::parse_str(&id)
                    .map_err(|e| Status::invalid_argument(format!("Invalid job_id: {e}")))?;
                self.runtime.get_local_job(job_id)
            }
            None => self
                .runtime
                .get_local_jobs()
                .into_values()
                .flatten()
                .collect(),
        };

        let stage_infos = status
            .into_iter()
            .map(|(stage_id, stage_info)| serialize_stage_info(stage_id, stage_info))
            .collect();

        Ok(Response::new(GetJobStatusResp { stage_infos }))
    }

    async fn cleanup_job(
        &self,
        request: Request<CleanupJobReq>,
    ) -> Result<Response<CleanupJobResp>, Status> {
        let job_id = Uuid::parse_str(&request.into_inner().job_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid job_id: {e}")))?;

        self.runtime.cleanup_local_job(job_id);

        Ok(Response::new(CleanupJobResp {}))
    }
}

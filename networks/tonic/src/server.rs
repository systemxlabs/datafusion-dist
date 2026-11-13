use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightData, encode::FlightDataEncoderBuilder};
use datafusion_dist::{
    DistResult,
    network::{ScheduledTasks, StageInfo},
    planner::StageId,
    runtime::DistRuntime,
};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::{
    physical_plan::{AsExecutionPlan, ComposedPhysicalExtensionCodec, PhysicalExtensionCodec},
    protobuf::PhysicalPlanNode,
};
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status};

use crate::{
    codec::DistPhysicalExtensionDecoder,
    protobuf::{
        self, CleanupJobsReq, CleanupJobsResp, GetJobsReq, GetJobsResp, SendTasksReq,
        SendTasksResp, StagePlan, dist_tonic_service_server::DistTonicService,
    },
    serde::{parse_stage_id, parse_task_distribution, parse_task_id, serialize_stage_info},
};

pub struct DistTonicServer {
    pub runtime: DistRuntime,
    pub task_ctx: Arc<TaskContext>,
    pub composed_extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DistTonicServer {
    pub fn new(
        runtime: DistRuntime,
        task_ctx: Arc<TaskContext>,
        app_extension_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        let composed_extension_codec = Arc::new(ComposedPhysicalExtensionCodec::new(vec![
            app_extension_codec.clone(),
            Arc::new(DistPhysicalExtensionDecoder {
                runtime: runtime.clone(),
                app_extension_codec,
            }),
        ]));

        Self {
            runtime,
            task_ctx,
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
        let job_meta = Arc::new(req.job_meta);
        Ok(ScheduledTasks::new(
            stage_plans,
            task_ids,
            Arc::new(job_task_distribution),
            job_meta,
        ))
    }

    fn parse_stage_plan(&self, proto: StagePlan) -> DistResult<(StageId, Arc<dyn ExecutionPlan>)> {
        let stage_id = parse_stage_id(proto.stage_id.expect("stage_id should not be null"));
        let plan: Arc<dyn ExecutionPlan> =
            PhysicalPlanNode::try_decode(&proto.plan).and_then(|proto| {
                proto.try_into_physical_plan(&self.task_ctx, self.composed_extension_codec.as_ref())
            })?;
        Ok((stage_id, plan))
    }
}

type BoxedDistStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// The trace of the call, taken from its metadata.
///
/// A header that does not parse reads as no trace rather than as an error: a
/// call with a broken `traceparent` still has tasks to run, it just runs them
/// untraced — the same thing that happens when the caller sends none.
fn parse_traceparent(
    metadata: &tonic::metadata::MetadataMap,
) -> datafusion_dist::trace::TraceContext {
    metadata
        .get(datafusion_dist::trace::TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(datafusion_dist::trace::TraceContext::parse)
        .unwrap_or_default()
}

#[async_trait::async_trait]
impl DistTonicService for DistTonicServer {
    type ExecuteTaskStream = BoxedDistStream<FlightData>;

    async fn send_tasks(
        &self,
        request: Request<SendTasksReq>,
    ) -> Result<Response<SendTasksResp>, Status> {
        // The trace arrives out of band, in the metadata of the call that is
        // delivering the tasks: it names this hop, and the stages it creates
        // derive their own span ids from it.
        let trace = parse_traceparent(request.metadata());
        let scheduled_tasks = self
            .parse_send_tasks_req(request.into_inner())
            .map_err(|e| Status::internal(format!("Failed to parse SendTasksReq: {e}")))?
            .with_trace(trace);
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
        // A caller that names a span gets it as the parent of this task, which is
        // how the fetch of a task on one node is joined to its execution on
        // another. This client does not send one yet — `DistNetwork::execute_task`
        // has no parameter to carry it — so the stored context of the job is what
        // the span continues, which is the same trace either way.
        let parent = request
            .metadata()
            .get(datafusion_dist::trace::TRACEPARENT_HEADER)
            .map(|_| parse_traceparent(request.metadata()));
        let task_id = parse_task_id(request.into_inner());
        let record_batch_stream = self
            .runtime
            .execute_local_with_parent(task_id.clone(), parent)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to execute local task {task_id}: {e}"))
            })?;

        let schema = record_batch_stream.schema();

        // Configure IPC write options with LZ4 compression
        let write_options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .map_err(|e| Status::internal(format!("Failed to configure IPC write options: {e}")))?;

        // Create flight data encoder directly from the record batch stream
        let flight_encoder = FlightDataEncoderBuilder::new()
            .with_options(write_options)
            .with_schema(schema)
            .build(record_batch_stream.map_err(|e| FlightError::ExternalError(Box::new(e))));

        // Map FlightData to our protobuf FlightData
        let flight_data_stream = flight_encoder
            .map_err(|e| Status::internal(format!("Failed to encode flight data: {e}")))
            .boxed();

        Ok(Response::new(flight_data_stream))
    }

    async fn get_jobs(
        &self,
        request: Request<GetJobsReq>,
    ) -> Result<Response<GetJobsResp>, Status> {
        let req = request.into_inner();
        let status: HashMap<StageId, StageInfo> = if let Some(job_ids) = req.job_ids {
            self.runtime
                .get_local_jobs(Some(&job_ids.job_ids.into_iter().map(Into::into).collect()))
        } else {
            self.runtime.get_local_jobs(None)
        };

        let stage_infos = status
            .into_iter()
            .map(|(stage_id, stage_info)| serialize_stage_info(stage_id, stage_info))
            .collect();

        Ok(Response::new(GetJobsResp { stage_infos }))
    }

    async fn cleanup_jobs(
        &self,
        request: Request<CleanupJobsReq>,
    ) -> Result<Response<CleanupJobsResp>, Status> {
        let job_ids = request.into_inner().job_ids;
        self.runtime
            .cleanup_local_jobs(job_ids.into_iter().map(Into::into).collect());

        Ok(Response::new(CleanupJobsResp {}))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_dist::trace::{TRACEPARENT_HEADER, TraceContext};
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        network::with_traceparent, protobuf::dist_tonic_service_client::DistTonicServiceClient,
        protobuf::dist_tonic_service_server::DistTonicServiceServer,
    };

    const SAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    /// Stands in for the runtime: it only reports what the metadata of the call
    /// it received contained.
    #[derive(Debug, Clone, Default)]
    struct MetadataRecorder {
        seen: Arc<Mutex<Option<String>>>,
    }

    #[async_trait::async_trait]
    impl DistTonicService for MetadataRecorder {
        type ExecuteTaskStream = BoxedDistStream<FlightData>;

        async fn send_tasks(
            &self,
            request: Request<SendTasksReq>,
        ) -> Result<Response<SendTasksResp>, Status> {
            let seen = request
                .metadata()
                .get(TRACEPARENT_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            *self.seen.lock().await = seen;
            Ok(Response::new(SendTasksResp {}))
        }

        async fn execute_task(
            &self,
            _request: Request<protobuf::TaskId>,
        ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
            unimplemented!("this test only sends tasks")
        }

        async fn get_jobs(
            &self,
            _request: Request<GetJobsReq>,
        ) -> Result<Response<GetJobsResp>, Status> {
            unimplemented!("this test only sends tasks")
        }

        async fn cleanup_jobs(
            &self,
            _request: Request<CleanupJobsReq>,
        ) -> Result<Response<CleanupJobsResp>, Status> {
            unimplemented!("this test only sends tasks")
        }
    }

    /// A header on the wire is what a receiver reads back: the trace survives a
    /// real gRPC round trip, not just the formatting of the value.
    #[tokio::test]
    async fn the_trace_crosses_the_wire_in_the_metadata() {
        let recorder = MetadataRecorder::default();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let incoming = tonic::transport::server::TcpIncoming::from(listener);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(DistTonicServiceServer::new(recorder.clone()))
                .serve_with_incoming(incoming),
        );

        let mut client = None;
        for _ in 0..100 {
            match DistTonicServiceClient::connect(format!("http://{addr}")).await {
                Ok(connected) => {
                    client = Some(connected);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }
        let mut client = client.expect("the stub server is up");

        let caller = TraceContext::from_traceparent(SAMPLE).expect("valid");
        let (hop, _) = caller.child();
        let request = with_traceparent(
            tonic::Request::new(SendTasksReq::default()),
            hop.traceparent().as_deref(),
        )
        .expect("inject");
        client.send_tasks(request).await.expect("send");

        let seen = recorder
            .seen
            .lock()
            .await
            .clone()
            .expect("the header arrived");
        let received = TraceContext::from_traceparent(&seen).expect("parse");
        assert_eq!(received.trace_id_hex(), caller.trace_id_hex());
        assert_eq!(received.parent_span_id_hex(), hop.parent_span_id_hex());
    }

    /// A header that does not parse is "no trace", not a failed call: the same
    /// thing a caller that sends no header gets.
    #[test]
    fn a_malformed_header_reads_as_untraced() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(TRACEPARENT_HEADER, "not-a-traceparent".parse().unwrap());
        assert!(parse_traceparent(&metadata).is_none());

        metadata.insert(TRACEPARENT_HEADER, SAMPLE.parse().unwrap());
        let parsed = parse_traceparent(&metadata);
        assert_eq!(parsed.trace_id_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
    }

    /// An untraced job sends no header at all rather than an empty one.
    #[test]
    fn an_untraced_delivery_sends_no_header() {
        let request =
            with_traceparent::<SendTasksReq>(tonic::Request::new(SendTasksReq::default()), None)
                .expect("inject");
        assert!(
            request.metadata().get(TRACEPARENT_HEADER).is_none(),
            "no header for an untraced job"
        );
    }
}

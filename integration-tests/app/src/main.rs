pub mod table;

use std::{collections::HashMap, error::Error, pin::Pin, sync::Arc, time::Duration};

use table::RunningJobsTable;

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{Any, CommandStatementQuery, ProstMessageExt, SqlInfo, server::FlightSqlService},
};
use datafusion::{
    arrow::ipc::writer::IpcWriteOptions,
    physical_plan::{
        ExecutionPlan, display::DisplayableExecutionPlan, placeholder_row::PlaceholderRowExec,
        projection::ProjectionExec,
    },
    prelude::SessionContext,
};
use datafusion_dist::{
    cluster::NodeId, config::DistConfig, planner::TaskId, runtime::DistRuntime,
    scheduler::DefaultScheduler,
};
use datafusion_dist_cluster_postgres::PostgresClusterBuilder;
use datafusion_dist_integration_tests::data::build_session_context;
use datafusion_dist_network_tonic::{
    network::DistTonicNetwork, protobuf::dist_tonic_service_server::DistTonicServiceServer,
    server::DistTonicServer,
};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use futures::{Stream, StreamExt, TryStreamExt};
use log::info;
use prost::Message;
use tonic::{Request, Response, Status, Streaming, metadata::MetadataValue, transport::Server};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let port = 50050u16;

    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});

    let network = DistTonicNetwork::new(port, app_extension_codec.clone());

    let ctx = build_session_context();

    let config = DistConfig::default()
        .with_job_ttl(Duration::from_secs(60))
        .with_job_ttl_check_interval(Duration::from_secs(3));

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    )
    .with_scheduler(Arc::new(build_dist_scheduler()));
    runtime.start().await;

    // Register the jobs table provider under information_schema
    ctx.register_table(
        "running_jobs",
        Arc::new(RunningJobsTable::new(runtime.clone())),
    )?;

    let dist_tonic_service =
        DistTonicServer::new(runtime.clone(), ctx.task_ctx(), app_extension_codec.clone());
    let dist_tonic_server = Server::builder()
        .add_service(DistTonicServiceServer::new(dist_tonic_service))
        .serve("[::]:50050".parse()?);

    let test_flight_sql_service = TestFlightSqlService {
        ctx: ctx.clone(),
        runtime: runtime.clone(),
    };
    let test_flight_server = Server::builder()
        .add_service(FlightServiceServer::new(test_flight_sql_service))
        .serve("[::]:50051".parse()?);

    tokio::select! {
        result = dist_tonic_server => {
            if let Err(e) = result {
                eprintln!("Dist tonic server error: {}", e);
            }
        }
        result = test_flight_server => {
            if let Err(e) = result {
                eprintln!("Test flight server error: {}", e);
            }
        }
    }
    Ok(())
}

fn build_dist_scheduler() -> DefaultScheduler {
    let assign_self = |plan: &Arc<dyn ExecutionPlan>| -> bool { is_plan_select_1(plan) };
    DefaultScheduler::new().with_assign_self(Some(Box::new(assign_self)))
}

fn is_plan_select_1(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() else {
        return false;
    };
    if !proj.input().as_any().is::<PlaceholderRowExec>() {
        return false;
    }
    if proj.expr().len() != 1 {
        return false;
    }
    let expr = &proj.expr()[0];
    let Some(literal) = expr
        .expr
        .as_any()
        .downcast_ref::<datafusion::physical_expr::expressions::Literal>()
    else {
        return false;
    };
    matches!(
        literal.value(),
        datafusion::scalar::ScalarValue::Int32(Some(1))
            | datafusion::scalar::ScalarValue::Int64(Some(1))
    )
}

struct TestFlightSqlService {
    ctx: SessionContext,
    runtime: DistRuntime,
}

#[derive(::prost::Message)]
pub struct NodeTask {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub port: u32,
    #[prost(string, tag = "3")]
    pub job_id: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub stage: u32,
    #[prost(uint32, tag = "5")]
    pub partition: u32,
}

impl arrow_flight::sql::ProstMessageExt for NodeTask {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.NodeTask"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: NodeTask::type_url().to_string(),
            value: self.encode_to_vec().into(),
        }
    }
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightSqlService for TestFlightSqlService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<BoxedFlightStream<HandshakeResponse>>, Status> {
        info!("do_handshake");
        let token = Uuid::new_v4();

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, Status>> + Send + 'static>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        info!("do_get_fallback type_url: {}", message.type_url);
        if !message.is::<NodeTask>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let node_task: NodeTask = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected NodeTask but got None!"))?;

        let node_id = NodeId {
            host: node_task.host,
            port: node_task.port as u16,
        };
        let task_id = TaskId {
            job_id: Uuid::parse_str(&node_task.job_id).expect("job id is not uuid"),
            stage: node_task.stage,
            partition: node_task.partition,
        };
        info!("Fetching data for task {task_id} and node {node_id}");

        let stream = if self.runtime.node_id == node_id {
            self.runtime
                .execute_local(task_id)
                .await
                .map_err(|e| Status::from_error(Box::new(e)))?
        } else {
            self.runtime
                .execute_remote(node_id, task_id)
                .await
                .map_err(|e| Status::from_error(Box::new(e)))?
        };

        let schema = stream.schema();

        let stream = stream
            .map_err(|e| FlightError::ExternalError(Box::new(e)))
            .boxed();

        let write_options = IpcWriteOptions::default();
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_options(write_options)
            .with_schema(schema)
            .build(stream)
            .map_err(|err| Status::from_error(Box::new(err)));
        Ok(Response::new(
            Box::pin(flight_data_stream) as <Self as FlightService>::DoGetStream
        ))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        info!("get_flight_info_statement query: {}", query.query);
        let df = self
            .ctx
            .sql(&query.query)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        info!(
            "Create Physical Plan: {}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let schema = plan.schema();

        let (_job_id, stage0_task_distribution) = self
            .runtime
            .submit(plan)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        info!("Stage0 task distribution: {:?}", stage0_task_distribution);

        let endpoints = build_flight_endpoints(stage0_task_distribution);

        let mut info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::from_error(Box::new(e)))?;
        for endpoint in endpoints {
            info = info.with_endpoint(endpoint);
        }

        Ok(Response::new(info))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

fn build_flight_endpoints(task_distribution: HashMap<TaskId, NodeId>) -> Vec<FlightEndpoint> {
    let mut endpoints = Vec::new();
    for (task_id, node_id) in task_distribution {
        let node_task = NodeTask {
            host: node_id.host,
            port: node_id.port as u32,
            job_id: task_id.job_id.to_string(),
            stage: task_id.stage,
            partition: task_id.partition,
        };
        let buf = node_task.as_any().encode_to_vec();
        let ticket = Ticket { ticket: buf.into() };

        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        endpoints.push(endpoint);
    }
    endpoints
}

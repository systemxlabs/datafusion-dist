use std::{collections::HashMap, error::Error, pin::Pin, sync::Arc};

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaAsIpc,
    Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{Any, CommandStatementQuery, ProstMessageExt, SqlInfo, server::FlightSqlService},
};
use datafusion::{
    arrow::{
        datatypes::SchemaRef,
        ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    },
    physical_plan::display::DisplayableExecutionPlan,
    prelude::SessionContext,
};
use datafusion_dist::{cluster::NodeId, config::DistConfig, planner::TaskId, runtime::DistRuntime};
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

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    let dist_tonic_service =
        DistTonicServer::new(runtime.clone(), ctx.clone(), app_extension_codec.clone());
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

        let stream = stream
            .map_err(|e| FlightError::ExternalError(Box::new(e)))
            .boxed();

        let write_options = IpcWriteOptions::default();
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_options(write_options)
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
        let schema_bytes = schema_to_ipc(schema)?;
        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Vec::new().into(),
            path: vec![],
        };

        let info = FlightInfo {
            schema: schema_bytes.into(),
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Default::default(),
        };
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

#[allow(clippy::result_large_err)]
fn schema_to_ipc(schema: SchemaRef) -> Result<Vec<u8>, Status> {
    let options = IpcWriteOptions::default();
    let pair = SchemaAsIpc::new(&schema, &options);
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let encoded_data =
        data_gen.schema_to_bytes_with_dictionary_tracker(pair.0, &mut dictionary_tracker, pair.1);
    let mut schema_bytes = vec![];
    datafusion::arrow::ipc::writer::write_message(&mut schema_bytes, encoded_data, pair.1)
        .map_err(|e| Status::internal(format!("Error encoding schema: {e}")))?;
    Ok(schema_bytes)
}

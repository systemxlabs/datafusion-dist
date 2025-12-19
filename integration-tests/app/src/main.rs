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
use datafusion_dist::{cluster::NodeId, network::TaskId, runtime::DistRuntime};
use datafusion_dist_cluster_postgres::PostgresClusterBuilder;
use datafusion_dist_network_tonic::{
    network::{DistTonicNetwork, serialize_task_id},
    protobuf::{self, dist_tonic_service_server::DistTonicServiceServer},
    server::{DistTonicServer, parse_task_id},
};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use futures::{Stream, StreamExt, TryStreamExt};
use log::debug;
use prost::Message;
use tonic::{Request, Response, Status, Streaming, metadata::MetadataValue, transport::Server};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    unsafe {
        std::env::set_var(
            "RUST_LOG",
            "debug,datafusion_dist=debug,datafusion_dist_network_tonic=debug,datafusion_dist_cluster_postgres=debug",
        );
    }
    env_logger::init();

    let port = 50050u16;

    let cluster = PostgresClusterBuilder::new("localhost", 5432, "postgres", "password")
        .build()
        .await?;

    let network = DistTonicNetwork {
        port,
        extension_codec: Arc::new(DefaultPhysicalExtensionCodec {}),
    };

    let ctx = SessionContext::new();

    let runtime = Arc::new(DistRuntime::try_new(
        ctx.clone(),
        Arc::new(cluster),
        Arc::new(network),
    )?);
    runtime.start().await;

    let dist_tonic_service = DistTonicServer {
        runtime: runtime.clone(),
        ctx: ctx.clone(),
        extension_codec: Arc::new(DefaultPhysicalExtensionCodec {}),
    };
    let dist_tonic_server = Server::builder()
        .add_service(DistTonicServiceServer::new(dist_tonic_service))
        .serve("[::1]:50050".parse()?);

    let test_flight_sql_service = TestFlightSqlService {
        ctx: ctx.clone(),
        runtime: runtime.clone(),
    };
    let test_flight_server = Server::builder()
        .add_service(FlightServiceServer::new(test_flight_sql_service))
        .serve("[::1]:50051".parse()?);

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
    runtime: Arc<DistRuntime>,
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightSqlService for TestFlightSqlService {
    type FlightService = Self;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<BoxedFlightStream<HandshakeResponse>>, Status> {
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
        debug!("do_get_fallback type_url: {}", message.type_url);
        if !message.is::<protobuf::TaskId>() {
            Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )))?
        }

        let proto_task_id: protobuf::TaskId = message
            .unpack()
            .map_err(|e| Status::internal(format!("{e:?}")))?
            .ok_or_else(|| Status::internal("Expected an TaskId but got None!"))?;

        let task_id = parse_task_id(proto_task_id);

        let stream = self
            .runtime
            .execute_local(task_id)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
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
        let df = self
            .ctx
            .sql(&query.query)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let plan = df
            .create_physical_plan()
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        debug!(
            "Create Physical Plan: {}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let schema = plan.schema();

        let (_job_id, stage0_task_distribution) = self
            .runtime
            .submit(plan)
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;

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
        let proto_task_id = serialize_task_id(task_id);
        let buf = proto_task_id.as_any().encode_to_vec();
        let ticket = Ticket { ticket: buf.into() };

        let location = format!("grpc://{}:{}", node_id.host, node_id.port);

        let endpoint = FlightEndpoint::new()
            .with_ticket(ticket)
            .with_location(location);
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

use std::{error::Error, io::Cursor, pin::Pin, sync::Arc};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
};
use datafusion::{arrow::ipc::writer::IpcWriteOptions, prelude::SessionContext};
use datafusion_dist::runtime::DistRuntime;
use datafusion_dist_cluster_postgres::PostgresClusterBuilder;
use datafusion_dist_network_tonic::{
    network::DistTonicNetwork,
    protobuf::{self, dist_tonic_service_server::DistTonicServiceServer},
    server::{DistTonicServer, parse_task_id},
};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming, metadata::MetadataValue, transport::Server};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let port = 50050u16;

    let cluster = PostgresClusterBuilder::new("", 5432, "postgres", "password")
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

    let dist_tonic_service = DistTonicServer {
        runtime: runtime.clone(),
        ctx: ctx.clone(),
        extension_codec: Arc::new(DefaultPhysicalExtensionCodec {}),
    };
    let dist_tonic_server = Server::builder()
        .add_service(DistTonicServiceServer::new(dist_tonic_service))
        .serve("[::1]:50050".parse()?);

    let test_flight_service = TestFlightService {
        runtime: runtime.clone(),
    };
    let test_flight_server = Server::builder()
        .add_service(FlightServiceServer::new(test_flight_service))
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

struct TestFlightService {
    runtime: Arc<DistRuntime>,
}

type BoxedFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for TestFlightService {
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let mut buf = Cursor::new(ticket.ticket);
        let proto_task_id = protobuf::TaskId::decode(&mut buf).unwrap();
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
            Box::pin(flight_data_stream) as Self::DoGetStream
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
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

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }
}

pub mod network;
pub mod protobuf;
pub mod server;

use prost::Message;

impl arrow_flight::sql::ProstMessageExt for protobuf::TaskId {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.TaskId"
    }

    fn as_any(&self) -> arrow_flight::sql::Any {
        arrow_flight::sql::Any {
            type_url: protobuf::TaskId::type_url().to_string(),
            value: self.encode_to_vec().into(),
        }
    }
}

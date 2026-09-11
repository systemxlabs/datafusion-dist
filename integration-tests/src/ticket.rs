//! Flight ticket identifying the task served by a flight endpoint.

use arrow_flight::{
    Ticket,
    sql::{Any, ProstMessageExt},
};
use datafusion::arrow::error::ArrowError;
use datafusion_dist::{cluster::NodeId, planner::TaskId};
use prost::Message;

/// Identifies a task and the node that runs it.
///
/// The application returns one flight endpoint per task of the stage the
/// client reads from, and this is the ticket of those endpoints.
#[derive(Clone, PartialEq, ::prost::Message)]
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

impl NodeTask {
    pub fn new(node_id: &NodeId, task_id: &TaskId) -> Self {
        NodeTask {
            host: node_id.host.clone(),
            port: node_id.port as u32,
            job_id: task_id.job_id.to_string(),
            stage: task_id.stage,
            partition: task_id.partition,
        }
    }

    /// Node that runs the task.
    pub fn node_id(&self) -> NodeId {
        NodeId {
            host: self.host.clone(),
            port: self.port as u16,
        }
    }

    pub fn to_ticket(&self) -> Ticket {
        Ticket {
            ticket: self.as_any().encode_to_vec().into(),
        }
    }

    pub fn from_ticket(ticket: &Ticket) -> Result<Self, ArrowError> {
        let message = Any::decode(ticket.ticket.as_ref())
            .map_err(|e| ArrowError::ParseError(format!("Failed to decode ticket: {e}")))?;
        message.unpack::<NodeTask>()?.ok_or_else(|| {
            ArrowError::ParseError(format!("Unexpected ticket type: {}", message.type_url))
        })
    }
}

impl ProstMessageExt for NodeTask {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.NodeTask"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: Self::type_url().to_string(),
            value: self.encode_to_vec().into(),
        }
    }
}

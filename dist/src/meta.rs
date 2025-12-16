use std::{fmt::Debug};

use crate::{error::DistResult, network::NodeId};

#[async_trait::async_trait]
pub trait DistMeta: Debug + Send + Sync {
    async fn heartbeat(&self) -> DistResult<()>;
    async fn alive_nodes(&self) -> DistResult<Vec<NodeId>>;
}
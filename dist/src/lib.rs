pub mod cluster;
pub mod config;
mod error;
pub mod heartbeater;
pub mod network;
pub mod physical_plan;
pub mod planner;
pub mod runtime;
pub mod schedule;
pub mod util;

use std::pin::Pin;

pub use error::{DistError, DistResult};

use datafusion::arrow::array::RecordBatch;
use futures::Stream;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = DistResult<RecordBatch>> + Send>>;

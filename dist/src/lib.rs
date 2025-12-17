pub mod cluster;
mod error;
pub mod network;
pub mod planner;
pub mod runtime;
pub mod schedule;

use std::pin::Pin;

pub use error::{DistError, DistResult};

use datafusion::arrow::array::RecordBatch;
use futures::Stream;

pub type RecordBatchStream = Pin<Box<dyn Stream<Item = DistResult<RecordBatch>> + Send>>;

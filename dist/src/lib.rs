pub mod cluster;
pub mod error;
pub mod network;
pub mod schedule;

use datafusion::arrow::array::RecordBatch;
use futures::Stream;

use crate::error::DistResult;

pub type RecordBatchStream = Box<dyn Stream<Item = DistResult<RecordBatch>> + Send + Sync>;

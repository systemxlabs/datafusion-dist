pub mod error;
pub mod network;
pub mod meta;

use datafusion::arrow::array::RecordBatch;
use futures::Stream;

use crate::error::DistResult;

pub type RecordBatchStream = Box<dyn Stream<Item = DistResult<RecordBatch>> + Send + Sync>;
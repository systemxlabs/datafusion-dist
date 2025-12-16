use std::{error::Error, panic::Location};

use datafusion::error::DataFusionError;

pub type DistResult<T> = Result<T, DistError>;

pub enum DistError {
    DataFusion(DataFusionError, &'static Location<'static>),
    Network(Box<dyn Error + Send + Sync>, &'static Location<'static>),
}

impl DistError {
    #[track_caller]
    pub fn network(err: Box<dyn Error + Send + Sync>) -> Self {
        DistError::Network(err, Location::caller())
    }
}

impl From<DataFusionError> for DistError {
    #[track_caller]
    fn from(err: DataFusionError) -> Self {
        DistError::DataFusion(err, Location::caller())
    }
}

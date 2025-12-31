use std::{error::Error, fmt::Display, panic::Location};

use arrow::error::ArrowError;
use datafusion_common::DataFusionError;
use tokio::task::JoinError;

pub type DistResult<T> = Result<T, DistError>;

#[derive(Debug)]
pub enum DistError {
    Arrow(ArrowError, &'static Location<'static>),
    DataFusion(DataFusionError, &'static Location<'static>),
    Tokio(JoinError, &'static Location<'static>),
    Network(Box<dyn Error + Send + Sync>, &'static Location<'static>),
    Plan(String, &'static Location<'static>),
    Schedule(String, &'static Location<'static>),
    Internal(String, &'static Location<'static>),
    Cluster(Box<dyn Error + Send + Sync>, &'static Location<'static>),
}

impl DistError {
    #[track_caller]
    pub fn network(err: Box<dyn Error + Send + Sync>) -> Self {
        DistError::Network(err, Location::caller())
    }

    #[track_caller]
    pub fn schedule(msg: impl Into<String>) -> Self {
        DistError::Schedule(msg.into(), Location::caller())
    }

    #[track_caller]
    pub fn internal(msg: impl Into<String>) -> Self {
        DistError::Internal(msg.into(), Location::caller())
    }

    #[track_caller]
    pub fn cluster(err: Box<dyn Error + Send + Sync>) -> Self {
        DistError::Cluster(err, Location::caller())
    }
}

impl Display for DistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistError::Arrow(err, loc) => write!(f, "Arrow error: {err} at {loc}"),
            DistError::DataFusion(err, loc) => write!(f, "DataFusion error: {err} at {loc}"),
            DistError::Tokio(err, loc) => write!(f, "Tokio error: {err} at {loc}"),
            DistError::Network(err, loc) => write!(f, "Network error: {err} at {loc}"),
            DistError::Plan(msg, loc) => write!(f, "Plan error: {msg} at {loc}"),
            DistError::Schedule(msg, loc) => write!(f, "Schedule error: {msg} at {loc}"),
            DistError::Internal(msg, loc) => write!(f, "Internal error: {msg} at {loc}"),
            DistError::Cluster(err, _) => write!(f, "Cluster error: {}", err),
        }
    }
}

impl Error for DistError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DistError::Arrow(err, _) => Some(err),
            DistError::DataFusion(err, _) => Some(err),
            DistError::Tokio(err, _) => Some(err),
            DistError::Network(err, _) => Some(err.as_ref()),
            DistError::Plan(_, _) => None,
            DistError::Schedule(_, _) => None,
            DistError::Internal(_, _) => None,
            DistError::Cluster(err, _) => Some(err.as_ref()),
        }
    }
}

impl From<ArrowError> for DistError {
    #[track_caller]
    fn from(value: ArrowError) -> Self {
        DistError::Arrow(value, Location::caller())
    }
}

impl From<DataFusionError> for DistError {
    #[track_caller]
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::External(external) if external.is::<DistError>() => {
                let dist = external
                    .downcast::<DistError>()
                    .expect("Datafusion external error can not be downcasted to DistError");
                *dist
            }
            e => DistError::DataFusion(e, Location::caller()),
        }
    }
}

impl From<JoinError> for DistError {
    #[track_caller]
    fn from(err: JoinError) -> Self {
        DistError::Tokio(err, Location::caller())
    }
}

impl From<DistError> for DataFusionError {
    fn from(value: DistError) -> Self {
        match value {
            DistError::DataFusion(err, _) => err,
            v => DataFusionError::External(Box::new(v)),
        }
    }
}

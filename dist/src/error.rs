use std::{error::Error, fmt::Display, panic::Location};

use datafusion::error::DataFusionError;
use tokio::task::JoinError;

pub type DistResult<T> = Result<T, DistError>;

#[derive(Debug)]
pub enum DistError {
    DataFusion(DataFusionError, &'static Location<'static>),
    Tokio(JoinError, &'static Location<'static>),
    Network(Box<dyn Error + Send + Sync>, &'static Location<'static>),
    Plan(String, &'static Location<'static>),
    Schedule(String, &'static Location<'static>),
    Internal(String, &'static Location<'static>),
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
}

impl Display for DistError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DistError::DataFusion(err, _) => write!(f, "DataFusion error: {}", err),
            DistError::Tokio(err, _) => write!(f, "Tokio error: {}", err),
            DistError::Network(err, _) => write!(f, "Network error: {}", err),
            DistError::Plan(msg, _) => write!(f, "Plan error: {}", msg),
            DistError::Schedule(msg, _) => write!(f, "Schedule error: {}", msg),
            DistError::Internal(msg, _) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl Error for DistError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DistError::DataFusion(err, _) => Some(err),
            DistError::Tokio(err, _) => Some(err),
            DistError::Network(err, _) => Some(err.as_ref()),
            DistError::Plan(_, _) => None,
            DistError::Schedule(_, _) => None,
            DistError::Internal(_, _) => None,
        }
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

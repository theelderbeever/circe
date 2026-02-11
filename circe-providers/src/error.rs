use std::fmt;

use datafusion::error::DataFusionError;

#[derive(Debug)]
pub enum ScyllaProviderError {
    Prepare(scylla::errors::PrepareError),
    PagerExecution(scylla::errors::PagerExecutionError),
    TypeCheck(scylla::deserialize::TypeCheckError),
    NextRow(scylla::client::pager::NextRowError),
    Arrow(datafusion::arrow::error::ArrowError),
    Internal(String),
}

impl fmt::Display for ScyllaProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prepare(e) => write!(f, "Scylla prepare error: {e}"),
            Self::PagerExecution(e) => write!(f, "Scylla pager execution error: {e}"),
            Self::TypeCheck(e) => write!(f, "Scylla type check error: {e}"),
            Self::NextRow(e) => write!(f, "Scylla next row error: {e}"),
            Self::Arrow(e) => write!(f, "Arrow error: {e}"),
            Self::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl std::error::Error for ScyllaProviderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Prepare(e) => Some(e),
            Self::PagerExecution(e) => Some(e),
            Self::TypeCheck(e) => Some(e),
            Self::NextRow(e) => Some(e),
            Self::Arrow(e) => Some(e),
            Self::Internal(_) => None,
        }
    }
}

impl From<scylla::errors::PrepareError> for ScyllaProviderError {
    fn from(e: scylla::errors::PrepareError) -> Self {
        Self::Prepare(e)
    }
}

impl From<scylla::errors::PagerExecutionError> for ScyllaProviderError {
    fn from(e: scylla::errors::PagerExecutionError) -> Self {
        Self::PagerExecution(e)
    }
}

impl From<scylla::deserialize::TypeCheckError> for ScyllaProviderError {
    fn from(e: scylla::deserialize::TypeCheckError) -> Self {
        Self::TypeCheck(e)
    }
}

impl From<scylla::client::pager::NextRowError> for ScyllaProviderError {
    fn from(e: scylla::client::pager::NextRowError) -> Self {
        Self::NextRow(e)
    }
}

impl From<datafusion::arrow::error::ArrowError> for ScyllaProviderError {
    fn from(e: datafusion::arrow::error::ArrowError) -> Self {
        Self::Arrow(e)
    }
}

impl From<ScyllaProviderError> for DataFusionError {
    fn from(e: ScyllaProviderError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}

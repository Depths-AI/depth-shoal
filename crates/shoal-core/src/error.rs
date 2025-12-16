use arrow::error::ArrowError;
use thiserror::Error;

/// Core error type for Depth Shoal v1.
#[derive(Debug, Error)]
pub enum ShoalError {
    #[error("Invalid identifier: {0}")]
    InvalidName(String),

    #[error("Duplicate field name: {0}")]
    DuplicateField(String),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Unknown field: {0}")]
    UnknownField(String),

    #[error("Missing non-nullable field: {0}")]
    MissingNonNullableField(String),

    #[error("Type mismatch for field '{field}': expected {expected}, got {got}")]
    TypeMismatch {
        expected: String,
        got: String,
        field: String,
    },

    #[error("Ingestion type failure: {0}")]
    IngestTypeFailure(String),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

pub type Result<T> = std::result::Result<T, ShoalError>;

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

    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

pub type Result<T> = std::result::Result<T, ShoalError>;

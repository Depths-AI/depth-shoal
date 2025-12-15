//! Depth Shoal core library.

pub mod error;
pub mod ndjson;
pub mod spec;

pub use error::{Result, ShoalError};
pub use ndjson::{NdjsonDecoder, NdjsonOptions};
pub use spec::{FieldName, Ident, ShoalDataType, ShoalField, ShoalSchema, TableName};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_non_empty() {
        assert!(!crate::version().is_empty());
    }
}

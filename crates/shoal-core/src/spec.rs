use crate::error::{Result, ShoalError};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

/// A validated identifier, used for table/schema/field names in v1.
///
/// Rules (v1): `[A-Za-z_][A-Za-z0-9_]*`
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ident(String);

impl Ident {
    pub fn new(s: &str) -> Result<Self> {
        validate_ident(s)?;
        Ok(Self(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for Ident {
    type Err = ShoalError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl std::fmt::Display for Ident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// v1 table name (validated identifier).
pub type TableName = Ident;

/// v1 field name (validated identifier).
pub type FieldName = Ident;

/// Fully qualified table reference for DataFusion catalog.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShoalTableRef {
    pub catalog: Ident,
    pub schema: Ident,
    pub table: Ident,
}

impl ShoalTableRef {
    pub fn new(catalog: &str, schema: &str, table: &str) -> Result<Self> {
        Ok(Self {
            catalog: Ident::new(catalog)?,
            schema: Ident::new(schema)?,
            table: Ident::new(table)?,
        })
    }
}

/// Configuration for the Shoal Runtime.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShoalRuntimeConfig {
    pub default_catalog: String,
    pub default_schema: String,
}

impl Default for ShoalRuntimeConfig {
    fn default() -> Self {
        Self {
            default_catalog: "datafusion".to_string(),
            default_schema: "public".to_string(),
        }
    }
}

/// Configuration for a specific Shoal Memory Table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShoalTableConfig {
    /// Flush head to sealed when rows exceed this count.
    pub head_max_rows: usize,
    /// Flush head to sealed when estimated bytes exceed this count.
    pub head_max_bytes: usize,
    /// Max total bytes (sealed + head) before eviction kicks in.
    pub max_total_bytes: usize,
    /// Max sealed batches before eviction kicks in.
    pub max_sealed_batches: usize,
    /// If true, reject rows with unknown fields.
    pub strict_mode: bool,
    /// Trigger compaction when sealed batch count exceeds this.
    pub compact_trigger_batches: usize,
    /// Target rows per batch during compaction.
    pub compact_target_rows: usize,
}

impl Default for ShoalTableConfig {
    fn default() -> Self {
        Self {
            head_max_rows: 10_000,
            head_max_bytes: 10 * 1024 * 1024,    // 10 MiB
            max_total_bytes: 1024 * 1024 * 1024, // 1 GiB
            max_sealed_batches: 10_000,
            strict_mode: false,
            compact_trigger_batches: 50,
            compact_target_rows: 100_000,
        }
    }
}

/// Shoal table schema data types (v1).
///
/// v1 guarantees:
/// - primitives
/// - nested list
/// - nested struct
/// - arbitrary binary blobs
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ShoalDataType {
    Bool,
    Int32,
    Int64,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,

    /// Arrow List<T>
    List {
        item: Box<ShoalDataType>,
        item_nullable: bool,
    },

    /// Arrow Struct<{...}>
    Struct {
        fields: Vec<ShoalField>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShoalField {
    pub name: FieldName,
    pub dtype: ShoalDataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShoalSchema {
    pub fields: Vec<ShoalField>,
}

impl ShoalSchema {
    pub fn new(fields: Vec<ShoalField>) -> Result<Self> {
        validate_unique_field_names(&fields)?;
        Ok(Self { fields })
    }
}

impl ShoalDataType {
    pub fn to_arrow(&self) -> Result<DataType> {
        Ok(match self {
            ShoalDataType::Bool => DataType::Boolean,
            ShoalDataType::Int32 => DataType::Int32,
            ShoalDataType::Int64 => DataType::Int64,
            ShoalDataType::UInt32 => DataType::UInt32,
            ShoalDataType::UInt64 => DataType::UInt64,
            ShoalDataType::Float32 => DataType::Float32,
            ShoalDataType::Float64 => DataType::Float64,
            ShoalDataType::Utf8 => DataType::Utf8,
            ShoalDataType::Binary => DataType::Binary,
            ShoalDataType::List {
                item,
                item_nullable,
            } => {
                let item_dt = item.to_arrow()?;
                // Arrow's default name for list members is "item".
                let item_field =
                    Field::new(Field::LIST_FIELD_DEFAULT_NAME, item_dt, *item_nullable);
                DataType::List(Arc::new(item_field))
            }
            ShoalDataType::Struct { fields } => {
                validate_unique_field_names(fields)?;
                let arrow_fields: Vec<_> = fields
                    .iter()
                    .map(|f| Ok(Arc::new(f.to_arrow()?)))
                    .collect::<Result<Vec<_>>>()?;
                DataType::Struct(Fields::from(arrow_fields))
            }
        })
    }
}

impl ShoalField {
    pub fn to_arrow(&self) -> Result<Field> {
        Ok(Field::new(
            self.name.as_str(),
            self.dtype.to_arrow()?,
            self.nullable,
        ))
    }
}

impl TryFrom<ShoalSchema> for Schema {
    type Error = ShoalError;

    fn try_from(value: ShoalSchema) -> std::result::Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&ShoalSchema> for Schema {
    type Error = ShoalError;

    fn try_from(value: &ShoalSchema) -> std::result::Result<Self, Self::Error> {
        validate_unique_field_names(&value.fields)?;
        let fields = value
            .fields
            .iter()
            .map(|f| f.to_arrow())
            .collect::<Result<Vec<_>>>()?;
        Ok(Schema::new(fields))
    }
}

fn validate_ident(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(ShoalError::InvalidName("empty".to_string()));
    }
    if !s.is_ascii() {
        return Err(ShoalError::InvalidName(format!(
            "non-ascii identifier: {s}"
        )));
    }

    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(ShoalError::InvalidName(format!(
            "must start with [A-Za-z_]: {s}"
        )));
    }
    for c in chars {
        if !(c == '_' || c.is_ascii_alphanumeric()) {
            return Err(ShoalError::InvalidName(format!(
                "invalid character '{c}' in identifier: {s}"
            )));
        }
    }
    Ok(())
}

fn validate_unique_field_names(fields: &[ShoalField]) -> Result<()> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(fields.len());
    for f in fields {
        let name = f.name.as_str();
        if !seen.insert(name) {
            return Err(ShoalError::DuplicateField(name.to_string()));
        }
    }
    Ok(())
}

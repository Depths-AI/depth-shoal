use crate::error::{Result, ShoalError};
use crate::spec::{ShoalSchema, ShoalTableConfig};
use arrow::array::{
    make_builder, ArrayBuilder, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, RwLock};

/// Inner state of the memory table.
///
/// Holds the mutable head (builders) and the sealed tail (RecordBatches).
struct TableState {
    schema: SchemaRef,
    config: ShoalTableConfig,

    /// Mutable head: one builder per field in the schema.
    head_builders: Vec<Box<dyn ArrayBuilder>>,
    /// Number of rows currently in the head.
    head_rows: usize,
    /// Estimated size of the head in bytes.
    head_bytes_estimate: usize,

    /// Sealed tail: immutable batches.
    sealed: VecDeque<RecordBatch>,
    /// Estimated size of the sealed tail in bytes.
    sealed_bytes_estimate: usize,
}

impl fmt::Debug for TableState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableState")
            .field("schema", &self.schema)
            .field("config", &self.config)
            .field("head_rows", &self.head_rows)
            .field("head_bytes_estimate", &self.head_bytes_estimate)
            .field("sealed_batches_count", &self.sealed.len())
            .field("sealed_bytes_estimate", &self.sealed_bytes_estimate)
            .finish()
    }
}

impl TableState {
    fn new(schema: SchemaRef, config: ShoalTableConfig) -> Self {
        let head_builders = schema
            .fields()
            .iter()
            .map(|f| make_builder(f.data_type(), config.head_max_rows))
            .collect();

        Self {
            schema,
            config,
            head_builders,
            head_rows: 0,
            head_bytes_estimate: 0,
            sealed: VecDeque::new(),
            sealed_bytes_estimate: 0,
        }
    }

    /// Appends a single JSON row to the head.
    ///
    /// If strict_mode is on, errors on unknown fields.
    fn append_row(&mut self, row: &serde_json::Map<String, Value>) -> Result<()> {
        // 1. Strict mode check: ensure no unknown fields exist in the input row
        if self.config.strict_mode {
            for key in row.keys() {
                if self.schema.field_with_name(key).is_err() {
                    return Err(ShoalError::UnknownField(key.clone()));
                }
            }
        }

        // 2. Append values for each column in schema order
        for i in 0..self.schema.fields().len() {
            let field = self.schema.field(i);
            let builder = &mut self.head_builders[i];
            let val = row.get(field.name());

            match val {
                Some(v) if !v.is_null() => {
                    append_value_recursive(builder, field.data_type(), v, field.name())?;
                }
                _ => {
                    if field.is_nullable() {
                        append_null_recursive(builder, field.data_type())?;
                    } else {
                        return Err(ShoalError::MissingNonNullableField(field.name().clone()));
                    }
                }
            }
        }

        self.head_rows += 1;
        Ok(())
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.head_rows >= self.config.head_max_rows {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.head_rows == 0 {
            return Ok(());
        }

        // finish() resets the builders but usually keeps capacity
        let arrays = self
            .head_builders
            .iter_mut()
            .map(|b| b.finish())
            .collect::<Vec<_>>();

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.sealed_bytes_estimate += batch.get_array_memory_size();
        self.sealed.push_back(batch);

        self.head_rows = 0;
        self.head_bytes_estimate = 0;
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<()> {
        if self.sealed.len() < self.config.compact_trigger_batches {
            return Ok(());
        }

        // Compaction strategy: Merge the first K batches
        let k = self.config.compact_trigger_batches;
        let mut batches_to_merge = Vec::with_capacity(k);
        let mut rows_to_merge = 0;

        for _ in 0..k {
            if let Some(b) = self.sealed.pop_front() {
                rows_to_merge += b.num_rows();
                self.sealed_bytes_estimate = self
                    .sealed_bytes_estimate
                    .saturating_sub(b.get_array_memory_size());
                batches_to_merge.push(b);

                if rows_to_merge >= self.config.compact_target_rows {
                    break;
                }
            }
        }

        if batches_to_merge.is_empty() {
            return Ok(());
        }

        // Fix: Pass reference to Vec, which acts as slice of RecordBatch
        let merged_batch = concat_batches(&self.schema, &batches_to_merge)?;

        self.sealed_bytes_estimate += merged_batch.get_array_memory_size();
        self.sealed.push_front(merged_batch);

        Ok(())
    }

    fn maybe_evict(&mut self) {
        while (self.sealed_bytes_estimate > self.config.max_total_bytes
            || self.sealed.len() > self.config.max_sealed_batches)
            && !self.sealed.is_empty()
        {
            if let Some(batch) = self.sealed.pop_front() {
                self.sealed_bytes_estimate = self
                    .sealed_bytes_estimate
                    .saturating_sub(batch.get_array_memory_size());
            }
        }
    }

    fn snapshot(&mut self) -> Result<(Vec<RecordBatch>, Option<RecordBatch>)> {
        // 1. Clone sealed batches (cheap Arc clone)
        let sealed = self.sealed.iter().cloned().collect();

        // 2. Snapshot head using finish_cloned (requires mutable access to builders)
        let head = if self.head_rows > 0 {
            let arrays = self
                .head_builders
                .iter_mut()
                .map(|b| b.finish_cloned())
                .collect::<Vec<_>>();
            Some(RecordBatch::try_new(self.schema.clone(), arrays)?)
        } else {
            None
        };

        Ok((sealed, head))
    }
}

/// A handle to a memory table. Thread-safe.
///
/// Wraps the internal state in an RwLock.
#[derive(Clone, Debug)]
pub struct ShoalTable {
    inner: Arc<RwLock<TableState>>,
}

impl ShoalTable {
    pub fn new(shoal_schema: ShoalSchema, config: ShoalTableConfig) -> Result<Self> {
        let arrow_schema: SchemaRef = Arc::new((&shoal_schema).try_into()?);
        let state = TableState::new(arrow_schema, config);
        Ok(Self {
            inner: Arc::new(RwLock::new(state)),
        })
    }

    /// Returns a copy of the Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.inner.read().unwrap().schema.clone()
    }

    /// Append a single JSON row, potentially flushing, compacting, or evicting.
    ///
    /// The caller (e.g., ingestion worker) is responsible for framing bytes into JSON objects.
    pub fn append_row(&self, row: serde_json::Map<String, Value>) -> Result<()> {
        let mut state = self.inner.write().unwrap();
        state.append_row(&row)?;
        state.maybe_flush()?;
        state.maybe_compact()?;
        state.maybe_evict();
        Ok(())
    }

    /// Create a consistent snapshot of the table (sealed + head).
    ///
    /// This requires a brief write lock to snapshot the head builders.
    pub fn snapshot(&self) -> Result<(Vec<RecordBatch>, Option<RecordBatch>)> {
        let mut state = self.inner.write().unwrap();
        state.snapshot()
    }
}

// --- Recursive Append Helpers ---

fn append_value_recursive(
    builder: &mut Box<dyn ArrayBuilder>,
    dt: &DataType,
    val: &Value,
    field_name: &str,
) -> Result<()> {
    match dt {
        DataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            if let Value::Bool(v) = val {
                b.append_value(*v);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "bool".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Int32 => {
            let b = builder.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            if let Some(n) = val.as_i64() {
                if n > i32::MAX as i64 || n < i32::MIN as i64 {
                    return Err(ShoalError::IngestTypeFailure(format!(
                        "int32 overflow: {n}"
                    )));
                }
                b.append_value(n as i32);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "int32".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            if let Some(n) = val.as_i64() {
                b.append_value(n);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "int64".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::UInt32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap();
            if let Some(n) = val.as_u64() {
                if n > u32::MAX as u64 {
                    return Err(ShoalError::IngestTypeFailure(format!(
                        "uint32 overflow: {n}"
                    )));
                }
                b.append_value(n as u32);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "uint32".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::UInt64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap();
            if let Some(n) = val.as_u64() {
                b.append_value(n);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "uint64".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Float32 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            if let Some(n) = val.as_f64() {
                b.append_value(n as f32);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "float32".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            if let Some(n) = val.as_f64() {
                b.append_value(n);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "float64".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            if let Value::String(s) = val {
                b.append_value(s);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "string".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Binary => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BinaryBuilder>()
                .unwrap();
            if let Value::String(s) = val {
                // Decode hex string to bytes
                let bytes = (0..s.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|_| ShoalError::IngestTypeFailure(format!("invalid hex: {s}")))?;
                b.append_value(bytes);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "hex_string".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::List(field) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();

            if let Value::Array(arr) = val {
                let values_builder = b.values();
                for item in arr {
                    if item.is_null() {
                        if !field.is_nullable() {
                            return Err(ShoalError::MissingNonNullableField(format!(
                                "{}.item",
                                field_name
                            )));
                        }
                        append_null_recursive(values_builder, field.data_type())?;
                    } else {
                        append_value_recursive(
                            values_builder,
                            field.data_type(),
                            item,
                            field_name,
                        )?;
                    }
                }
                b.append(true);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "array".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        DataType::Struct(fields) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();

            if let Value::Object(map) = val {
                for (i, field) in fields.iter().enumerate() {
                    let child_builder = b
                        .field_builder(i)
                        .expect("struct field builder index out of bounds");
                    match map.get(field.name()) {
                        Some(v) if !v.is_null() => {
                            append_value_recursive(
                                child_builder,
                                field.data_type(),
                                v,
                                field.name(),
                            )?;
                        }
                        _ => {
                            if !field.is_nullable() {
                                return Err(ShoalError::MissingNonNullableField(format!(
                                    "{}.{}",
                                    field_name,
                                    field.name()
                                )));
                            }
                            append_null_recursive(child_builder, field.data_type())?;
                        }
                    }
                }
                b.append(true);
            } else {
                return Err(ShoalError::TypeMismatch {
                    expected: "object".into(),
                    got: val.to_string(),
                    field: field_name.into(),
                });
            }
        }
        other => {
            return Err(ShoalError::IngestTypeFailure(format!(
                "unsupported type: {other:?}"
            )));
        }
    }
    Ok(())
}

fn append_null_recursive(builder: &mut Box<dyn ArrayBuilder>, dt: &DataType) -> Result<()> {
    match dt {
        DataType::Boolean => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_null(),
        DataType::Int32 => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_null(),
        DataType::Int64 => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_null(),
        DataType::UInt32 => builder
            .as_any_mut()
            .downcast_mut::<UInt32Builder>()
            .unwrap()
            .append_null(),
        DataType::UInt64 => builder
            .as_any_mut()
            .downcast_mut::<UInt64Builder>()
            .unwrap()
            .append_null(),
        DataType::Float32 => builder
            .as_any_mut()
            .downcast_mut::<Float32Builder>()
            .unwrap()
            .append_null(),
        DataType::Float64 => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_null(),
        DataType::Utf8 => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_null(),
        DataType::Binary => builder
            .as_any_mut()
            .downcast_mut::<BinaryBuilder>()
            .unwrap()
            .append_null(),
        DataType::List(_) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .unwrap();
            b.append_null();
        }
        DataType::Struct(fields) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            // For StructBuilder, we must append to children to keep lengths in sync,
            // even if the parent struct slot is null.
            for i in 0..fields.len() {
                let child = b
                    .field_builder(i)
                    .expect("struct field builder index out of bounds");
                // Recurse using child field type
                append_null_recursive(child, fields[i].data_type())?;
            }
            b.append_null();
        }
        _ => {
            return Err(ShoalError::IngestTypeFailure(format!(
                "null not supported for {dt:?}"
            )))
        }
    }
    Ok(())
}

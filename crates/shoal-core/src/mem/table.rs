use crate::error::{Result, ShoalError};
use crate::spec::{ShoalSchema, ShoalTableConfig};
use arc_swap::ArcSwap;
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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

// --- Public API ---

/// A handle to a memory table.
///
/// This is the public interface for interacting with a table.
/// - Writes are asynchronous and serialized via an MPSC channel to the [`IngestionWorker`].
/// - Reads (snapshots) are synchronous and access [`SharedTableState`].
/// - Readers and Writers are decoupled: writers append to a private active head,
///   while readers read from shared state. Rotation happens periodically.
#[derive(Clone, Debug)]
pub struct TableHandle {
    tx: mpsc::Sender<IngestMsg>,
    // Readers access this atomic pointer. No locking required.
    snapshot: Arc<ArcSwap<TableSnapshot>>,
}

impl TableHandle {
    /// Create a new standalone table with the given schema and configuration.
    ///
    /// This spawns the background ingestion worker and returns a handle.
    /// Use this if you don't need the full `ShoalRuntime` / SQL integration.
    pub fn create(schema: ShoalSchema, config: ShoalTableConfig) -> Result<Self> {
        let arrow_schema: SchemaRef = Arc::new((&schema).try_into()?);

        // Initial empty snapshot
        let initial_snapshot = TableSnapshot {
            schema: arrow_schema.clone(),
            batches: Vec::new(),
            bytes_estimate: 0,
        };
        let snapshot = Arc::new(ArcSwap::from_pointee(initial_snapshot));

        // Create MPSC channel
        let (tx, rx) = mpsc::channel(1024);

        // Spawn Worker
        let worker = IngestionWorker::new(rx, arrow_schema, config, snapshot.clone());
        tokio::spawn(worker.run());

        Ok(Self { tx, snapshot })
    }

    /// Internal constructor used by the runtime if it needs to share state setup.
    /// (Ideally runtime should just use create() too, but runtime registers with DF).
    pub(crate) fn new(tx: mpsc::Sender<IngestMsg>, snapshot: Arc<ArcSwap<TableSnapshot>>) -> Self {
        Self { tx, snapshot }
    }

    /// Append a single JSON row asynchronously.
    pub async fn append_row(&self, row: serde_json::Map<String, Value>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let msg = IngestMsg::Append { row, resp: resp_tx };

        self.tx
            .send(msg)
            .await
            .map_err(|_| ShoalError::IngestTypeFailure("Ingestion worker channel closed".into()))?;

        resp_rx.await.map_err(|_| {
            ShoalError::IngestTypeFailure("Ingestion worker dropped response".into())
        })?
    }

    /// Returns a copy of the Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.snapshot.load().schema.clone()
    }

    /// Get a consistent snapshot of the sealed state.
    ///
    /// This is wait-free. It loads the current pointer from ArcSwap.
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        self.snapshot.load().batches.to_vec()
    }
}

// --- Internal Data Structures ---

/// Immutable snapshot of the table state visible to readers.
#[derive(Debug)]
pub struct TableSnapshot {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>, // Flattened view
    pub bytes_estimate: usize,
}

pub enum IngestMsg {
    Append {
        row: serde_json::Map<String, Value>,
        resp: oneshot::Sender<Result<()>>,
    },
}

/// The background worker that owns the write path.
pub struct IngestionWorker {
    rx: mpsc::Receiver<IngestMsg>,
    active_head: ActiveHead,
    // The mutable "Source of Truth" for the tail
    write_state: WriteSideTail,
    // The atomic pointer to update
    snapshot: Arc<ArcSwap<TableSnapshot>>,
}

impl IngestionWorker {
    pub fn new(
        rx: mpsc::Receiver<IngestMsg>,
        schema: SchemaRef,
        config: ShoalTableConfig,
        snapshot: Arc<ArcSwap<TableSnapshot>>,
    ) -> Self {
        Self {
            rx,
            active_head: ActiveHead::new(schema.clone(), config.clone()),
            write_state: WriteSideTail::new(schema, config),
            snapshot,
        }
    }

    pub async fn run(mut self) {
        let check_interval =
            Duration::from_millis((self.active_head.config.active_head_max_latency_ms / 2).max(1));
        let mut ticker = tokio::time::interval(check_interval);

        loop {
            tokio::select! {
                maybe_msg = self.rx.recv() => {
                    match maybe_msg {
                        Some(IngestMsg::Append { row, resp }) => {
                            let res = self.active_head.append_row(&row);
                            if res.is_ok() {
                                self.check_and_rotate();
                            }
                            let _ = resp.send(res);
                        }
                        None => {
                            self.force_rotate();
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    self.check_and_rotate();
                }
            }
        }
    }

    fn check_and_rotate(&mut self) {
        if self.active_head.should_rotate() {
            self.force_rotate();
        }
    }

    fn force_rotate(&mut self) {
        match self.active_head.rotate() {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    // 1. Mutate private state (cheap, no lock)
                    self.write_state.push_batch(batch);

                    // 2. Publish new snapshot (ArcSwap)
                    self.publish_snapshot();
                }
            }
            Err(e) => {
                eprintln!(
                    "CRITICAL: ActiveHead rotation failed: {}. Data loss occurred.",
                    e
                );
            }
        }
    }

    fn publish_snapshot(&self) {
        // Construct the immutable view
        let view = TableSnapshot {
            schema: self.write_state.schema.clone(),
            batches: self.write_state.batches.iter().cloned().collect(),
            bytes_estimate: self.write_state.bytes_estimate,
        };
        // Atomic swap
        self.snapshot.store(Arc::new(view));
    }
}

// --- Write-Side State (Private to Worker) ---

struct ActiveHead {
    schema: SchemaRef,
    config: ShoalTableConfig,
    builders: Vec<Box<dyn ArrayBuilder>>,
    current_rows: usize,
    last_flush: Instant,
}

impl ActiveHead {
    fn new(schema: SchemaRef, config: ShoalTableConfig) -> Self {
        let builders = make_builders(&schema, config.active_head_max_rows);
        Self {
            schema,
            config,
            builders,
            current_rows: 0,
            last_flush: Instant::now(),
        }
    }

    fn append_row(&mut self, row: &serde_json::Map<String, Value>) -> Result<()> {
        if self.config.strict_mode {
            for key in row.keys() {
                if self.schema.field_with_name(key).is_err() {
                    return Err(ShoalError::UnknownField(key.clone()));
                }
            }
        }

        for i in 0..self.schema.fields().len() {
            let field = self.schema.field(i);
            let builder = &mut self.builders[i];
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
        self.current_rows += 1;
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        if self.current_rows == 0 {
            return false;
        }
        if self.current_rows >= self.config.active_head_max_rows {
            return true;
        }
        if self.last_flush.elapsed().as_millis() as u64 >= self.config.active_head_max_latency_ms {
            return true;
        }
        false
    }

    fn rotate(&mut self) -> Result<RecordBatch> {
        if self.current_rows == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let arrays = self
            .builders
            .iter_mut()
            .map(|b| b.finish())
            .collect::<Vec<_>>();

        let batch = RecordBatch::try_new(self.schema.clone(), arrays)?;
        self.current_rows = 0;
        self.last_flush = Instant::now();

        Ok(batch)
    }
}

// --- Write-Side Tail (Worker's Private Mutable Tail) ---

struct WriteSideTail {
    schema: SchemaRef,
    config: ShoalTableConfig,
    batches: VecDeque<RecordBatch>,
    bytes_estimate: usize,
}

impl WriteSideTail {
    fn new(schema: SchemaRef, config: ShoalTableConfig) -> Self {
        Self {
            schema,
            config,
            batches: VecDeque::new(),
            bytes_estimate: 0,
        }
    }

    fn push_batch(&mut self, batch: RecordBatch) {
        self.bytes_estimate += batch.get_array_memory_size();
        self.batches.push_back(batch);

        self.maybe_compact();
        self.maybe_evict();
    }

    fn maybe_compact(&mut self) {
        if self.batches.len() < self.config.compact_trigger_batches {
            return;
        }

        let k = self.config.compact_trigger_batches;

        let mut batches_to_merge = Vec::with_capacity(k);
        let mut local_bytes_accum = 0;
        let mut rows_to_merge = 0;

        for _ in 0..k {
            if let Some(b) = self.batches.pop_front() {
                rows_to_merge += b.num_rows();
                local_bytes_accum += b.get_array_memory_size();
                batches_to_merge.push(b);

                if rows_to_merge >= self.config.compact_target_rows {
                    break;
                }
            } else {
                break;
            }
        }

        if batches_to_merge.is_empty() {
            return;
        }

        if let Ok(merged) = concat_batches(&self.schema, batches_to_merge.iter()) {
            self.bytes_estimate = self.bytes_estimate.saturating_sub(local_bytes_accum);
            self.bytes_estimate += merged.get_array_memory_size();
            self.batches.push_front(merged);
        } else {
            for b in batches_to_merge.into_iter().rev() {
                self.batches.push_front(b);
            }
            eprintln!("WARNING: Compaction failed via concat_batches. Retrying later.");
        }
    }

    fn maybe_evict(&mut self) {
        while (self.bytes_estimate > self.config.max_total_bytes
            || self.batches.len() > self.config.max_sealed_batches)
            && !self.batches.is_empty()
        {
            if let Some(batch) = self.batches.pop_front() {
                self.bytes_estimate = self
                    .bytes_estimate
                    .saturating_sub(batch.get_array_memory_size());
            }
        }
    }
}

// --- Helpers (Unchanged) ---

fn make_builders(schema: &SchemaRef, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|f| make_builder(f.data_type(), capacity))
        .collect()
}

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
            for i in 0..fields.len() {
                let child = b
                    .field_builder(i)
                    .expect("struct field builder index out of bounds");
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

use crate::error::{Result, ShoalError};
use crate::ndjson::{NdjsonDecoder, NdjsonOptions};
use crate::spec::ShoalTableConfig;
use arc_swap::ArcSwap;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

// --- Public API ---

#[derive(Clone, Debug)]
pub struct TableHandle {
    tx: mpsc::Sender<IngestMsg>,
    snapshot: Arc<ArcSwap<TableSnapshot>>,
}

impl TableHandle {
    pub fn create(schema: crate::spec::ShoalSchema, config: ShoalTableConfig) -> Result<Self> {
        let arrow_schema: SchemaRef = Arc::new((&schema).try_into()?);

        let initial_snapshot = TableSnapshot {
            schema: arrow_schema.clone(),
            batches: Vec::new(),
            bytes_estimate: 0,
        };
        let snapshot = Arc::new(ArcSwap::from_pointee(initial_snapshot));

        let (tx, rx) = mpsc::channel(1024);

        let worker = IngestionWorker::new(rx, arrow_schema, config, snapshot.clone())?;
        tokio::spawn(worker.run());

        Ok(Self { tx, snapshot })
    }

    pub(crate) fn new(tx: mpsc::Sender<IngestMsg>, snapshot: Arc<ArcSwap<TableSnapshot>>) -> Self {
        Self { tx, snapshot }
    }

    pub async fn append_row(&self, row: serde_json::Map<String, Value>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let msg = IngestMsg::AppendRow { row, resp: resp_tx };

        self.tx
            .send(msg)
            .await
            .map_err(|_| ShoalError::IngestTypeFailure("Ingestion worker channel closed".into()))?;

        resp_rx.await.map_err(|_| {
            ShoalError::IngestTypeFailure("Ingestion worker dropped response".into())
        })?
    }

    pub async fn append_bytes(&self, chunk: bytes::Bytes) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let msg = IngestMsg::AppendBytes {
            chunk,
            resp: resp_tx,
        };

        self.tx
            .send(msg)
            .await
            .map_err(|_| ShoalError::IngestTypeFailure("Ingestion worker channel closed".into()))?;

        resp_rx.await.map_err(|_| {
            ShoalError::IngestTypeFailure("Ingestion worker dropped response".into())
        })?
    }

    pub fn schema(&self) -> SchemaRef {
        self.snapshot.load().schema.clone()
    }

    pub fn snapshot(&self) -> Vec<RecordBatch> {
        self.snapshot.load().batches.to_vec()
    }
}

// --- Internal Data Structures ---

#[derive(Debug)]
pub struct TableSnapshot {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub bytes_estimate: usize,
}

pub enum IngestMsg {
    AppendRow {
        row: serde_json::Map<String, Value>,
        resp: oneshot::Sender<Result<()>>,
    },
    AppendBytes {
        chunk: bytes::Bytes,
        resp: oneshot::Sender<Result<()>>,
    },
}

pub struct IngestionWorker {
    rx: mpsc::Receiver<IngestMsg>,
    decoder: NdjsonDecoder,
    write_state: WriteSideTail,
    snapshot: Arc<ArcSwap<TableSnapshot>>,
    config: ShoalTableConfig,
    last_flush: Instant,
}

impl IngestionWorker {
    pub fn new(
        rx: mpsc::Receiver<IngestMsg>,
        schema: SchemaRef,
        config: ShoalTableConfig,
        snapshot: Arc<ArcSwap<TableSnapshot>>,
    ) -> Result<Self> {
        let decoder_options = NdjsonOptions {
            batch_size: config.active_head_max_rows,
            strict_mode: config.strict_mode,
        };
        let decoder = NdjsonDecoder::with_options(schema.clone(), decoder_options)?;

        Ok(Self {
            rx,
            decoder,
            write_state: WriteSideTail::new(schema, config.clone()),
            snapshot,
            config,
            last_flush: Instant::now(),
        })
    }

    pub async fn run(mut self) {
        let check_interval =
            Duration::from_millis((self.config.active_head_max_latency_ms / 2).max(1));
        let mut ticker = tokio::time::interval(check_interval);

        loop {
            tokio::select! {
                maybe_msg = self.rx.recv() => {
                    match maybe_msg {
                        Some(IngestMsg::AppendBytes { chunk, resp }) => {
                            let res = self.handle_bytes(&chunk);
                            let _ = resp.send(res);
                        }
                        Some(IngestMsg::AppendRow { row, resp }) => {
                            // Unified Path: Serialize row to bytes -> Decoder
                            let res = match serde_json::to_vec(&row) {
                                Ok(mut bytes) => {
                                    bytes.push(b'\n'); // Ensure delimiter
                                    self.handle_bytes(&bytes)
                                }
                                Err(e) => Err(ShoalError::JsonParse(e)),
                            };
                            let _ = resp.send(res);
                        }
                        None => {
                            self.force_flush();
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if self.should_rotate() {
                        self.force_flush();
                    }
                }
            }
        }
    }

    fn handle_bytes(&mut self, chunk: &[u8]) -> Result<()> {
        let batches = self.decoder.push_bytes(chunk)?;
        let mut published = false;

        for batch in batches {
            if batch.num_rows() > 0 {
                self.write_state.push_batch(batch);
                published = true;
            }
        }

        if published {
            self.publish_snapshot();
            self.last_flush = Instant::now();
        }
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        self.last_flush.elapsed().as_millis() as u64 >= self.config.active_head_max_latency_ms
    }

    fn force_flush(&mut self) {
        // decoder.finish() flushes any buffered rows that haven't reached batch_size
        match self.decoder.finish() {
            Ok(Some(batch)) => {
                if batch.num_rows() > 0 {
                    self.write_state.push_batch(batch);
                    self.publish_snapshot();
                    self.last_flush = Instant::now();
                }
            }
            Ok(None) => {} // Nothing to flush
            Err(e) => {
                eprintln!("CRITICAL: Decoder flush failed: {}. Data loss occurred.", e);
            }
        }
    }

    fn publish_snapshot(&self) {
        let view = TableSnapshot {
            schema: self.write_state.schema.clone(),
            batches: self.write_state.batches.iter().cloned().collect(),
            bytes_estimate: self.write_state.bytes_estimate,
        };
        self.snapshot.store(Arc::new(view));
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

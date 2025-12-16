use crate::error::{Result, ShoalError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_json::reader::{Decoder, ReaderBuilder};
use serde_json::Value;

/// Configuration for [`NdjsonDecoder`].
#[derive(Clone, Debug)]
pub struct NdjsonOptions {
    /// Target number of rows per decoded RecordBatch.
    pub batch_size: usize,
    /// If true, error on any JSON key not present in the schema.
    pub strict_mode: bool,
}

impl Default for NdjsonOptions {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            strict_mode: false,
        }
    }
}

/// Incrementally decodes newline-delimited JSON into Arrow [`RecordBatch`]es.
///
/// Internally wraps `arrow_json::reader::Decoder`, which supports arbitrary chunk boundaries
/// (i.e. chunks can split in the middle of a JSON record).
pub struct NdjsonDecoder {
    decoder: Decoder,
    batch_size: usize,
}

impl NdjsonDecoder {
    /// Create a decoder using `options`.
    pub fn with_options(schema: SchemaRef, options: NdjsonOptions) -> Result<Self> {
        if options.batch_size == 0 {
            return Err(ShoalError::InvalidSchema(
                "ndjson batch_size must be > 0".to_string(),
            ));
        }

        let decoder = ReaderBuilder::new(schema)
            .with_batch_size(options.batch_size)
            .with_strict_mode(options.strict_mode)
            .build_decoder()?;

        Ok(Self {
            decoder,
            batch_size: options.batch_size,
        })
    }

    /// Convenience constructor with defaults (batch_size=1024, strict_mode=false).
    pub fn new(schema: SchemaRef) -> Result<Self> {
        Self::with_options(schema, NdjsonOptions::default())
    }

    /// Push a byte chunk and return any fully decoded [`RecordBatch`]es.
    ///
    /// This will yield a batch whenever `batch_size` rows have been buffered. Remaining
    /// rows can be retrieved using [`Self::finish`].
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        let mut offset = 0usize;

        while offset < chunk.len() {
            let buf = &chunk[offset..];
            let consumed = self.decoder.decode(buf)?;
            offset += consumed;

            // If `decode` returned early, it hit batch_size and left bytes unconsumed.
            if consumed < buf.len() {
                // `flush` errors if we're part-way through a record.
                if self.decoder.has_partial_record() {
                    return Err(ShoalError::InvalidSchema(
                        "decoder reported partial record at batch boundary".to_string(),
                    ));
                }
                if let Some(batch) = self.decoder.flush()? {
                    out.push(batch);
                }
                continue;
            }

            // Handle the corner case where we end *exactly* on a batch boundary.
            if !self.decoder.has_partial_record() && self.decoder.len() >= self.batch_size {
                if let Some(batch) = self.decoder.flush()? {
                    out.push(batch);
                }
            }
        }

        Ok(out)
    }

    /// Flush remaining buffered rows into a final [`RecordBatch`].
    ///
    /// Returns `Ok(None)` if there are no buffered rows. Errors if called while a record is
    /// only partially decoded.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.decoder.has_partial_record() {
            return Err(ShoalError::InvalidSchema(
                "cannot finish: partial JSON record buffered".to_string(),
            ));
        }
        Ok(self.decoder.flush()?)
    }
}

/// A robust framer for NDJSON streams.
///
/// Buffers partial lines across chunk boundaries and extracts complete lines
/// split by `\n`. Trims `\r` and ignores empty lines.
#[derive(Default, Debug)]
pub struct NdjsonLineFramer {
    buffer: Vec<u8>,
}

impl NdjsonLineFramer {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Appends bytes to the internal buffer and returns any complete lines found.
    pub fn push_bytes(&mut self, chunk: &[u8]) -> Vec<Vec<u8>> {
        self.buffer.extend_from_slice(chunk);
        let mut lines = Vec::new();

        let mut start = 0;
        // Search for newlines starting from the beginning of the buffer
        while let Some(mut pos) = self.buffer[start..].iter().position(|&b| b == b'\n') {
            // .position() is relative to the slice `start..`, so adjust absolute pos
            pos += start;

            // Extract the line content, excluding the newline
            let end_payload = if pos > start && self.buffer[pos - 1] == b'\r' {
                pos - 1
            } else {
                pos
            };

            // Ignore empty lines (e.g., "\n" or "\r\n")
            if end_payload > start {
                lines.push(self.buffer[start..end_payload].to_vec());
            }

            // Advance start to the byte after '\n'
            start = pos + 1;
        }

        // Drop processed bytes from the buffer
        if start > 0 {
            // Using split_off is a simple way to keep the tail.
            // Optimized implementations might use a ring buffer or indices, but Vec is fine here.
            self.buffer = self.buffer.split_off(start);
        }

        lines
    }

    /// Returns any remaining bytes in the buffer as a final line, if not empty.
    pub fn finish(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            return None;
        }
        let mut line = std::mem::take(&mut self.buffer);
        // Trim trailing \r if present (e.g. stream ended with "...\r")
        if line.last() == Some(&b'\r') {
            line.pop();
        }

        if line.is_empty() {
            None
        } else {
            Some(line)
        }
    }
}

/// Parse a raw JSON line into a `serde_json::Map` object.
///
/// Errors if the JSON is invalid or if the root element is not an Object.
pub fn parse_ndjson_row(line: &[u8]) -> Result<serde_json::Map<String, Value>> {
    let val: Value = serde_json::from_slice(line).map_err(ShoalError::JsonParse)?;
    match val {
        Value::Object(map) => Ok(map),
        _ => Err(ShoalError::SchemaMismatch(
            "NDJSON row must be a JSON object".to_string(),
        )),
    }
}

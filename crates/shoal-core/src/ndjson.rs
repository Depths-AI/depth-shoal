use crate::error::{Result, ShoalError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_json::reader::{Decoder, ReaderBuilder};

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::array::{BinaryArray, Int64Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn chunked_input_yields_batches_and_finish_flushes_tail() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let mut dec = NdjsonDecoder::with_options(
            schema,
            NdjsonOptions {
                batch_size: 2,
                strict_mode: false,
            },
        )
        .unwrap();

        let ndjson =
            b"{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n{\"id\":3,\"name\":\"c\"}\n";

        // Split in the middle of the first record to validate partial record buffering.
        let (c1, c2) = ndjson.split_at(10);

        let mut batches = Vec::new();
        batches.extend(dec.push_bytes(c1).unwrap());
        batches.extend(dec.push_bytes(c2).unwrap());

        // Expect one full batch of 2 rows, plus 1-row tail from finish()
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        let tail = dec.finish().unwrap().unwrap();
        assert_eq!(tail.num_rows(), 1);

        let ids = tail
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
    }

    #[test]
    fn strict_mode_rejects_extra_fields() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));

        // strict
        let mut strict = NdjsonDecoder::with_options(
            schema.clone(),
            NdjsonOptions {
                batch_size: 8,
                strict_mode: true,
            },
        )
        .unwrap();
        strict.push_bytes(b"{\"a\":1,\"b\":2}\n").unwrap();
        let err = strict.finish().unwrap_err();
        match err {
            ShoalError::Arrow(_) => {}
            other => panic!("expected Arrow error, got {other:?}"),
        }

        // non-strict
        let mut non_strict = NdjsonDecoder::with_options(
            schema,
            NdjsonOptions {
                batch_size: 8,
                strict_mode: false,
            },
        )
        .unwrap();

        assert!(non_strict
            .push_bytes(b"{\"a\":1,\"b\":2}\n")
            .unwrap()
            .is_empty());
        let batch = non_strict.finish().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a.value(0), 1);
    }

    #[test]
    fn binary_base16_decodes() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "blob",
            DataType::Binary,
            false,
        )]));
        let mut dec = NdjsonDecoder::with_options(
            schema,
            NdjsonOptions {
                batch_size: 16,
                strict_mode: true,
            },
        )
        .unwrap();

        dec.push_bytes(b"{\"blob\":\"68656c6c6f\"}\n").unwrap();
        let batch = dec.finish().unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(col.value(0), b"hello");
    }

    #[test]
    fn nested_list_of_struct_decodes() {
        // points: List<Struct{x:int64,y:utf8}>
        let point_struct = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("x", DataType::Int64, false)),
            Arc::new(Field::new("y", DataType::Utf8, false)),
        ]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "points",
            DataType::List(Arc::new(Field::new(
                Field::LIST_FIELD_DEFAULT_NAME,
                point_struct,
                true,
            ))),
            true,
        )]));

        let mut dec = NdjsonDecoder::with_options(
            schema,
            NdjsonOptions {
                batch_size: 8,
                strict_mode: true,
            },
        )
        .unwrap();

        let data = b"{\"points\":[{\"x\":1,\"y\":\"a\"},{\"x\":2,\"y\":\"b\"}]}\n";
        dec.push_bytes(data).unwrap();
        let batch = dec.finish().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.len(), 1);

        let values = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.num_columns(), 2);

        let xs = values
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ys = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(xs.value(0), 1);
        assert_eq!(ys.value(0), "a");
        assert_eq!(xs.value(1), 2);
        assert_eq!(ys.value(1), "b");
    }
}

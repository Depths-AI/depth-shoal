use arrow::array::{Array, BinaryArray, Int64Array, ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use shoal_core::error::ShoalError;
use shoal_core::ndjson::{NdjsonDecoder, NdjsonOptions};
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

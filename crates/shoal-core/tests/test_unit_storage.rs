mod common;
use common::get_test_schema;
use serde_json::json;
use shoal_core::error::ShoalError;
use shoal_core::mem::table::TableHandle;
use shoal_core::spec::ShoalTableConfig;

// Helper to create a strict table
fn make_strict_table() -> TableHandle {
    let schema = get_test_schema();
    let config = ShoalTableConfig {
        strict_mode: true,
        // Force immediate flushing to ensure validation errors surface synchronously for this test
        active_head_max_rows: 1,
        ..Default::default()
    };
    TableHandle::create(schema, config).unwrap()
}

#[tokio::test]
async fn strict_type_check() {
    let table = make_strict_table();

    // Valid
    table
        .append_row(json!({"id": 1, "name": "foo"}).as_object().unwrap().clone())
        .await
        .unwrap();

    // Invalid Type
    let err = table
        .append_row(json!({"id": "not-int"}).as_object().unwrap().clone())
        .await
        .unwrap_err();

    // Check that we got an error. The specific variant depends on arrow-json internals,
    // but we accept any valid error from our crate.
    match err {
        ShoalError::Arrow(_) | ShoalError::JsonParse(_) | ShoalError::IngestTypeFailure(_) => {}
        _ => panic!("wrong error: {:?}", err),
    }
}

#[tokio::test]
async fn missing_non_nullable() {
    let table = make_strict_table();
    let err = table
        .append_row(json!({"name": "ok"}).as_object().unwrap().clone())
        .await
        .unwrap_err();

    match err {
        ShoalError::Arrow(_) | ShoalError::JsonParse(_) | ShoalError::IngestTypeFailure(_) => {}
        _ => panic!("wrong error: {:?}", err),
    }
}

#[tokio::test]
async fn snapshot_immutability() {
    // This test uses default config (strict=false is fine)
    let table = common::make_basic_table();

    table
        .append_row(json!({"id": 1}).as_object().unwrap().clone())
        .await
        .unwrap();

    // Allow rotation (latency based)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let sealed = table.snapshot();
    assert!(!sealed.is_empty());
    assert_eq!(sealed[0].num_rows(), 1);

    // Mutate head again
    table
        .append_row(json!({"id": 2}).as_object().unwrap().clone())
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let sealed2 = table.snapshot();
    // Verify accumulation
    let total_rows: usize = sealed2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

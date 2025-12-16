mod common;
use common::make_basic_table;
use serde_json::json;
use shoal_core::error::ShoalError;

#[tokio::test]
async fn strict_type_check() {
    let table = make_basic_table();

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
    match err {
        ShoalError::TypeMismatch { expected, .. } => assert_eq!(expected, "int64"),
        _ => panic!("wrong error: {:?}", err),
    }
}

#[tokio::test]
async fn missing_non_nullable() {
    let table = make_basic_table();
    let err = table
        .append_row(json!({"name": "ok"}).as_object().unwrap().clone())
        .await
        .unwrap_err();
    match err {
        ShoalError::MissingNonNullableField(f) => assert_eq!(f, "id"),
        _ => panic!("wrong error: {:?}", err),
    }
}

#[tokio::test]
async fn snapshot_immutability() {
    let table = make_basic_table();
    
    // Ingest enough to trigger rotation (default active_head_max_rows is 1000)
    // or force rotation. For this test, we just want to verify data lands.
    // NOTE: Because of MPSC+Rotation, data won't show up in snapshot immediately 
    // unless we hit the active_head_max_rows or latency.
    // Unit tests relying on immediate visibility might need a lower threshold config
    // or a wait. However, `make_basic_table` uses default config (1000 rows).
    // This test will fail if we don't rotate.
    
    // To fix this cleanly without changing `common`, we rely on `active_head_max_latency_ms`
    // which defaults to 50ms. We sleep > 50ms to ensure data rotates.
    
    table
        .append_row(json!({"id": 1}).as_object().unwrap().clone())
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let sealed = table.snapshot();
    // With 1 row and 50ms wait, it should be in the shared state (1 batch).
    assert!(!sealed.is_empty());
    assert_eq!(sealed[0].num_rows(), 1);

    // Mutate head again
    table
        .append_row(json!({"id": 2}).as_object().unwrap().clone())
        .await
        .unwrap();
        
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let sealed2 = table.snapshot();
    // Previous batch + new batch (rotation creates new batches)
    assert!(sealed2.len() >= 1);
    let total_rows: usize = sealed2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}
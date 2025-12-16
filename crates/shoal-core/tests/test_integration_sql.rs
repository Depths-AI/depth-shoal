mod common; // Import the common module

use serde_json::json;
use shoal_core::mem::ShoalRuntime;
use shoal_core::spec::{ShoalRuntimeConfig, ShoalTableConfig, ShoalTableRef};

#[tokio::test]
async fn test_end_to_end_ingest_and_query() {
    let runtime = ShoalRuntime::new(ShoalRuntimeConfig::default()).unwrap();

    // REUSE: Use the schema from common/mod.rs
    let schema = common::get_test_schema();

    let table_ref = ShoalTableRef::new("datafusion", "public", "users").unwrap();

    // Pass the shared schema to the runtime
    let table = runtime
        .create_table(table_ref, schema, ShoalTableConfig::default())
        .unwrap();

    table
        .append_row(
            json!({"id": 1, "name": "Alice"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .await
        .unwrap();
    table
        .append_row(json!({"id": 2, "name": "Bob"}).as_object().unwrap().clone())
        .await
        .unwrap();

    // WAIT: Allow time for the worker to rotate the active head into shared state.
    // Default config has 50ms latency.
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let batches = runtime
        .sql("SELECT * FROM users ORDER BY id")
        .await
        .unwrap();

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

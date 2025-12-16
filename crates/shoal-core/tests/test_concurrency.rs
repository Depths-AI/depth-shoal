mod common;
use serde_json::json;
use shoal_core::mem::ShoalRuntime;
use shoal_core::spec::{ShoalRuntimeConfig, ShoalTableConfig, ShoalTableRef};
use std::sync::Arc;
use tokio::task;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_ingest_and_query() {
    let runtime = Arc::new(ShoalRuntime::new(ShoalRuntimeConfig::default()).unwrap());
    let schema = common::get_test_schema(); // {id, name}
    let table_ref = ShoalTableRef::new("datafusion", "public", "concurrent").unwrap();

    // Configure frequent flushing (rotation)
    let config = ShoalTableConfig {
        active_head_max_rows: 100,      // Flush often
        active_head_max_latency_ms: 10, // Low latency
        ..Default::default()
    };

    let table = runtime.create_table(table_ref, schema, config).unwrap();
    let table_clone = table.clone();
    let runtime_clone = runtime.clone();

    // 1. Writer Task: Ingest 10,000 rows
    let writer_handle = task::spawn(async move {
        for i in 0..10_000 {
            table_clone
                .append_row(
                    json!({"id": i, "name": "stress"})
                        .as_object()
                        .unwrap()
                        .clone(),
                )
                .await
                .unwrap();
            // Yield occasionally
            if i % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // 2. Reader Task: Query constantly
    let reader_handle = task::spawn(async move {
        let mut max_seen = 0;
        for _ in 0..50 {
            let batches = runtime_clone
                .sql("SELECT count(*) as c FROM concurrent")
                .await
                .unwrap();

            // Handle case where query returns empty if no batches rotated yet
            if !batches.is_empty() {
                let count = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(0);

                assert!(count >= max_seen, "Count decreased! Eviction not enabled.");
                max_seen = count;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }
    });

    // Wait for writer
    writer_handle.await.unwrap();

    // Allow final rotation (latency < 100ms)
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Final verify
    let batches = runtime
        .sql("SELECT count(*) FROM concurrent")
        .await
        .unwrap();

    let final_count = if batches.is_empty() {
        0
    } else {
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0)
    };

    assert_eq!(final_count, 10_000);

    reader_handle.await.unwrap();
}

mod common;
use shoal_core::spec::{ShoalRuntimeConfig, ShoalTableConfig, ShoalTableRef};
use shoal_core::mem::ShoalRuntime;
use serde_json::json;
use std::sync::Arc;
use tokio::task;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_concurrent_writers() {
    let runtime = Arc::new(ShoalRuntime::new(ShoalRuntimeConfig::default()).unwrap());
    let schema = common::get_test_schema();
    let table_ref = ShoalTableRef::new("datafusion", "public", "multi_writer").unwrap();
    
    // Create table
    let table = runtime.create_table(table_ref, schema, ShoalTableConfig::default()).unwrap();
    
    let mut handles = Vec::new();
    let num_writers = 4;
    let rows_per_writer = 2_500;

    // Spawn 4 writers
    for writer_id in 0..num_writers {
        let table_clone = table.clone();
        
        let handle = task::spawn(async move {
            for i in 0..rows_per_writer {
                // Ensure unique IDs across writers to avoid logic confusion, 
                // though strictly speaking ID uniqueness isn't enforced by the table yet.
                let id = (writer_id * rows_per_writer) + i;
                
                table_clone
                    .append_row(json!({
                        "id": id, 
                        "name": format!("writer-{}", writer_id)
                    }).as_object().unwrap().clone())
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Await all writers
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify data integrity
    // 1. Count must be exactly 10,000
    let batches = runtime.sql("SELECT count(*) FROM multi_writer").await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    
    assert_eq!(count, (num_writers * rows_per_writer) as i64);

    // 2. Verify we have data from ALL writers
    // Check distinct names (should be 4: writer-0, writer-1...)
    let distinct_batches = runtime.sql("SELECT count(DISTINCT name) FROM multi_writer").await.unwrap();
    let distinct_cnt = distinct_batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
        
    assert_eq!(distinct_cnt, 4);
}
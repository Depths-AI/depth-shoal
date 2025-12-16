mod common;
use arrow::datatypes::Schema as ArrowSchema;
use serde_json::json;
use shoal_core::mem::table::{IngestionWorker, SharedTableState, TableHandle};
use shoal_core::spec::ShoalTableConfig;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

// Helper to create a handle with custom config
fn make_table_with_config(config: ShoalTableConfig) -> TableHandle {
    let schema = common::get_test_schema();
    // Fix: Explicitly type the arrow schema so Arc knows what it wraps
    let arrow_schema: ArrowSchema = (&schema).try_into().unwrap();
    let arrow_schema_ref = Arc::new(arrow_schema);
    
    let shared_state = SharedTableState::new(arrow_schema_ref.clone(), config.clone());
    let inner = Arc::new(RwLock::new(shared_state));
    
    let (tx, rx) = mpsc::channel(1024);
    let worker = IngestionWorker::new(rx, arrow_schema_ref, config, inner.clone());

    tokio::spawn(worker.run());

    TableHandle::new(tx, inner)
}

#[tokio::test]
async fn test_eviction_drops_old_data() {
    // Config: Allow very little memory (~ enough for 2 batches)
    let config = ShoalTableConfig {
        active_head_max_rows: 10, // Rotate often
        max_total_bytes: 500,  // Very tight limit (approx)
        max_sealed_batches: 5, // Secondary limit
        ..Default::default()
    };

    let table = make_table_with_config(config);

    // Ingest 20 batches (200 rows total)
    for i in 0..200 {
        table
            .append_row(
                json!({"id": i, "name": "fill"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .await
            .unwrap();
    }
    
    // Allow rotation to catch up
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let sealed = table.snapshot();
    
    // Verify we don't have all 200 rows
    assert!(sealed.len() <= 5);
    
    let total_rows: usize = sealed.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows < 200);
    
    // Verify oldest data is gone (id 0 should be evicted)
    if !sealed.is_empty() {
        let first_batch = &sealed[0];
        let ids = first_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        // If strict FIFO eviction worked, the first ID > 0
        assert!(ids.value(0) > 0);
    }
}

#[tokio::test]
async fn test_compaction_merges_batches() {
    let config = ShoalTableConfig {
        active_head_max_rows: 2,    // Tiny batches -> rotate fast
        compact_trigger_batches: 4, // Merge when we hit 4 batches
        compact_target_rows: 100,   // Merge all of them
        ..Default::default()
    };

    let table = make_table_with_config(config);

    // Ingest 4 batches (2 rows each) -> 8 rows total
    for i in 0..8 {
        table
            .append_row(
                json!({"id": i, "name": "compact"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .await
            .unwrap();
    }
    
    // Allow processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let sealed = table.snapshot();
    
    // If compaction worked, we shouldn't have 4 batches (they merged).
    // Note: Depends on timing, but trigger is 4.
    assert!(sealed.len() < 4);
    
    let total_rows: usize = sealed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 8);
}
mod common;
use serde_json::json;
use shoal_core::mem::table::ShoalTable;
use shoal_core::spec::ShoalTableConfig;

#[test]
fn test_eviction_drops_old_data() {
    let schema = common::get_test_schema();

    // Config: Allow very little memory (~ enough for 2 batches)
    let config = ShoalTableConfig {
        head_max_rows: 10,     // Small batches
        max_total_bytes: 500,  // Very tight limit (approx)
        max_sealed_batches: 5, // Secondary limit
        ..Default::default()
    };

    let table = ShoalTable::new(schema, config).unwrap();

    // Ingest 20 batches (200 rows total)
    // Should trigger eviction multiple times
    for i in 0..200 {
        table
            .append_row(
                json!({"id": i, "name": "fill"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }

    let (sealed, _head) = table.snapshot().unwrap();

    // Verify we don't have all 200 rows (20 batches)
    // Sealed should be capped at max_sealed_batches (5) or by bytes
    assert!(sealed.len() <= 5);

    let total_rows: usize = sealed.iter().map(|b| b.num_rows()).sum();
    // Head has some rows (0-9), sealed has max 50 rows. Total << 200.
    assert!(total_rows < 200);

    // Verify oldest data is gone (id 0 should be evicted)
    let first_batch = &sealed[0];
    let ids = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert!(ids.value(0) > 0);
}

#[test]
fn test_compaction_merges_batches() {
    let schema = common::get_test_schema();

    let config = ShoalTableConfig {
        head_max_rows: 2,           // Tiny batches
        compact_trigger_batches: 4, // Merge when we hit 4 batches
        compact_target_rows: 100,   // Merge all of them
        ..Default::default()
    };

    let table = ShoalTable::new(schema, config).unwrap();

    // Ingest 4 batches (2 rows each) -> 8 rows total
    // Batches: [2], [2], [2], [2] -> Trigger Compaction -> [8]
    for i in 0..8 {
        table
            .append_row(
                json!({"id": i, "name": "compact"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }

    let (sealed, _) = table.snapshot().unwrap();

    // If compaction worked, we shouldn't have 4 batches.
    // We should have fewer (ideally 1, but depends on exact timing of trigger vs flush).
    assert!(sealed.len() < 4);

    let total_rows: usize = sealed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 8);
}

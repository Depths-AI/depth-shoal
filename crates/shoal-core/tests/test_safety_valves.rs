mod common;
use serde_json::json;
use shoal_core::mem::table::TableHandle;
use shoal_core::spec::ShoalTableConfig;
use tokio::time::{sleep, Duration};

fn make_table_with_config(config: ShoalTableConfig) -> TableHandle {
    let schema = common::get_test_schema();
    // Clean public API usage
    TableHandle::create(schema, config).unwrap()
}

#[tokio::test]
async fn test_eviction_drops_old_data() {
    let config = ShoalTableConfig {
        active_head_max_rows: 10,
        max_total_bytes: 500,
        max_sealed_batches: 5,
        ..Default::default()
    };

    let table = make_table_with_config(config);

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

    sleep(Duration::from_millis(200)).await;

    let sealed = table.snapshot();

    assert!(sealed.len() <= 5);

    let total_rows: usize = sealed.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows < 200);

    if !sealed.is_empty() {
        let first_batch = &sealed[0];
        let ids = first_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(ids.value(0) > 0);
    }
}

#[tokio::test]
async fn test_compaction_merges_batches() {
    let config = ShoalTableConfig {
        active_head_max_rows: 2,
        compact_trigger_batches: 4,
        compact_target_rows: 100,
        ..Default::default()
    };

    let table = make_table_with_config(config);

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

    sleep(Duration::from_millis(200)).await;

    let sealed = table.snapshot();

    assert!(sealed.len() < 4);

    let total_rows: usize = sealed.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 8);
}

mod common;
use serde_json::json;
use shoal_core::mem::ShoalRuntime;
use shoal_core::spec::{
    ShoalDataType, ShoalField, ShoalRuntimeConfig, ShoalSchema, ShoalTableConfig, ShoalTableRef,
};

#[tokio::test]
async fn test_table_isolation() {
    let runtime = ShoalRuntime::new(ShoalRuntimeConfig::default()).unwrap();

    // Table A: Users
    let schema_a = common::get_test_schema();
    let ref_a = ShoalTableRef::new("datafusion", "public", "users").unwrap();
    let table_a = runtime
        .create_table(ref_a, schema_a, ShoalTableConfig::default())
        .unwrap();

    // Table B: Logs (Different schema)
    let schema_b = ShoalSchema::new(vec![ShoalField {
        name: "msg".parse().unwrap(),
        dtype: ShoalDataType::Utf8,
        nullable: false,
    }])
    .unwrap();
    let ref_b = ShoalTableRef::new("datafusion", "public", "logs").unwrap();
    let table_b = runtime
        .create_table(ref_b, schema_b, ShoalTableConfig::default())
        .unwrap();

    // Ingest
    table_a
        .append_row(json!({"id": 1, "name": "A"}).as_object().unwrap().clone())
        .await
        .unwrap();
    table_b
        .append_row(json!({"msg": "log1"}).as_object().unwrap().clone())
        .await
        .unwrap();

    // Allow worker rotation (default latency is 50ms)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Query A
    let batches_a = runtime.sql("SELECT * FROM users").await.unwrap();
    assert!(!batches_a.is_empty(), "Table A should have data");
    assert_eq!(batches_a[0].num_rows(), 1);

    // Query B
    let batches_b = runtime.sql("SELECT * FROM logs").await.unwrap();
    assert!(!batches_b.is_empty(), "Table B should have data");
    assert_eq!(batches_b[0].num_rows(), 1);

    // Ensure we can't query cross-table by mistake (names are scoped)
    // DataFusion handles this, but good to verify our registration logic
    let err = runtime.sql("SELECT msg FROM users").await;
    assert!(err.is_err());
}

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
    table
        .append_row(json!({"id": 1}).as_object().unwrap().clone())
        .await
        .unwrap();

    let (sealed, head) = table.snapshot().unwrap();
    assert!(sealed.is_empty());
    assert_eq!(head.unwrap().num_rows(), 1);

    // Mutate head again
    table
        .append_row(json!({"id": 2}).as_object().unwrap().clone())
        .await
        .unwrap();

    let (sealed2, head2) = table.snapshot().unwrap();
    assert!(sealed2.is_empty());
    assert_eq!(head2.unwrap().num_rows(), 2);
}

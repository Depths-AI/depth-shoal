#![allow(dead_code)]

use shoal_core::mem::table::{IngestionWorker, TableHandle, TableState};
use shoal_core::spec::{ShoalDataType, ShoalField, ShoalSchema, ShoalTableConfig};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Shared schema used across unit and integration tests.
pub fn get_test_schema() -> ShoalSchema {
    ShoalSchema::new(vec![
        ShoalField {
            name: "id".parse().unwrap(),
            dtype: ShoalDataType::Int64,
            nullable: false,
        },
        ShoalField {
            name: "name".parse().unwrap(),
            dtype: ShoalDataType::Utf8,
            nullable: true,
        },
    ])
    .unwrap()
}

/// Helper for unit tests that need a standalone table handle.
/// Spawns the worker task automatically.
pub fn make_basic_table() -> TableHandle {
    let schema = get_test_schema();
    let config = ShoalTableConfig::default();

    // Manual setup similar to ShoalRuntime::create_table
    let table_state = TableState::new(schema, config).unwrap();
    let inner = Arc::new(RwLock::new(table_state));
    let (tx, rx) = mpsc::channel(1024);
    let worker = IngestionWorker::new(rx, inner.clone());

    tokio::spawn(worker.run());

    TableHandle::new(tx, inner)
}

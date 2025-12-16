#![allow(dead_code)]

use arrow::datatypes::Schema as ArrowSchema;
use shoal_core::mem::table::{IngestionWorker, SharedTableState, TableHandle};
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
    
    // Explicitly type the arrow schema
    let arrow_schema: ArrowSchema = (&schema).try_into().unwrap();
    let arrow_schema_ref = Arc::new(arrow_schema);

    // Setup Shared State
    let shared_state = SharedTableState::new(arrow_schema_ref.clone(), config.clone());
    let inner = Arc::new(RwLock::new(shared_state));
    
    // Setup Channel & Worker
    let (tx, rx) = mpsc::channel(1024);
    let worker = IngestionWorker::new(rx, arrow_schema_ref, config, inner.clone());
    
    tokio::spawn(worker.run());
    
    TableHandle::new(tx, inner)
}
#![allow(dead_code)]

use shoal_core::mem::table::TableHandle;
use shoal_core::spec::{ShoalDataType, ShoalField, ShoalSchema, ShoalTableConfig};

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

pub fn make_basic_table() -> TableHandle {
    let schema = get_test_schema();
    let config = ShoalTableConfig::default();

    // Clean public API usage
    TableHandle::create(schema, config).unwrap()
}

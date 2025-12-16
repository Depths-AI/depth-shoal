use shoal_core::mem::table::ShoalTable;
use shoal_core::spec::{ShoalDataType, ShoalField, ShoalSchema, ShoalTableConfig};

/// Shared schema used across unit and integration tests.
/// Schema: { id: Int64 (non-null), name: Utf8 (nullable) }
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

/// Helper for unit tests that need a standalone table without a runtime.
pub fn make_basic_table() -> ShoalTable {
    ShoalTable::new(get_test_schema(), ShoalTableConfig::default()).unwrap()
}

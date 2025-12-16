use arrow::datatypes::DataType;
use arrow::datatypes::Schema as ArrowSchema;
use shoal_core::spec::{Ident, ShoalDataType, ShoalField, ShoalSchema};

#[test]
fn ident_validation() {
    assert!(Ident::new("_ok").is_ok());
    assert!(Ident::new("ok_123").is_ok());

    assert!(Ident::new("").is_err());
    assert!(Ident::new("9no").is_err());
    assert!(Ident::new("has-dash").is_err());
    assert!(Ident::new("has space").is_err());
}

#[test]
fn schema_to_arrow_primitives() {
    let schema = ShoalSchema::new(vec![
        ShoalField {
            name: "id".parse().unwrap(),
            dtype: ShoalDataType::Int64,
            nullable: false,
        },
        ShoalField {
            name: "ok".parse().unwrap(),
            dtype: ShoalDataType::Bool,
            nullable: true,
        },
        ShoalField {
            name: "name".parse().unwrap(),
            dtype: ShoalDataType::Utf8,
            nullable: true,
        },
        ShoalField {
            name: "blob".parse().unwrap(),
            dtype: ShoalDataType::Binary,
            nullable: true,
        },
    ])
    .unwrap();

    let arrow_schema: ArrowSchema = (&schema).try_into().unwrap();
    assert_eq!(arrow_schema.fields.len(), 4);
    assert_eq!(arrow_schema.field(0).name(), "id");
    assert_eq!(arrow_schema.field(0).data_type(), &DataType::Int64);
    assert!(!arrow_schema.field(0).is_nullable());
}

#[test]
fn schema_to_arrow_nested_list_of_struct() {
    let point_struct = ShoalDataType::Struct {
        fields: vec![
            ShoalField {
                name: "x".parse().unwrap(),
                dtype: ShoalDataType::Int32,
                nullable: false,
            },
            ShoalField {
                name: "y".parse().unwrap(),
                dtype: ShoalDataType::Int32,
                nullable: false,
            },
        ],
    };

    let schema = ShoalSchema::new(vec![ShoalField {
        name: "points".parse().unwrap(),
        dtype: ShoalDataType::List {
            item: Box::new(point_struct),
            item_nullable: false,
        },
        nullable: true,
    }])
    .unwrap();

    let arrow_schema: ArrowSchema = (&schema).try_into().unwrap();
    let dt = arrow_schema.field(0).data_type();
    match dt {
        DataType::List(item) => match item.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "x");
                assert_eq!(fields[1].name(), "y");
            }
            other => panic!("expected struct, got {other:?}"),
        },
        other => panic!("expected list, got {other:?}"),
    }
}

#[test]
fn schema_to_arrow_struct_with_list_field() {
    let inner = ShoalDataType::Struct {
        fields: vec![
            ShoalField {
                name: "values".parse().unwrap(),
                dtype: ShoalDataType::List {
                    item: Box::new(ShoalDataType::Int64),
                    item_nullable: false,
                },
                nullable: false,
            },
            ShoalField {
                name: "label".parse().unwrap(),
                dtype: ShoalDataType::Utf8,
                nullable: true,
            },
        ],
    };

    let schema = ShoalSchema::new(vec![ShoalField {
        name: "payload".parse().unwrap(),
        dtype: inner,
        nullable: false,
    }])
    .unwrap();

    let arrow_schema: ArrowSchema = (&schema).try_into().unwrap();
    match arrow_schema.field(0).data_type() {
        DataType::Struct(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "values");
            match fields[0].data_type() {
                DataType::List(item) => assert_eq!(item.data_type(), &DataType::Int64),
                other => panic!("expected list, got {other:?}"),
            }
        }
        other => panic!("expected struct, got {other:?}"),
    }
}

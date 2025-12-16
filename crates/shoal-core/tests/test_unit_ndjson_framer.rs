use serde_json::json;
use shoal_core::error::ShoalError;
use shoal_core::ndjson::{parse_ndjson_row, NdjsonLineFramer};

#[test]
fn framer_handles_chunks_and_newlines() {
    let mut framer = NdjsonLineFramer::new();

    // 1. Full line in one chunk
    let lines = framer.push_bytes(b"{\"a\":1}\n");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0], b"{\"a\":1}");

    // 2. Split line across chunks
    let lines = framer.push_bytes(b"{\"b\":");
    assert!(lines.is_empty());
    let lines = framer.push_bytes(b"2}\n");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0], b"{\"b\":2}");

    // 3. Multiple lines, CRLF, empty lines ignored
    let chunk = b"{\"c\":3}\r\n\n{\"d\":4}\n";
    let lines = framer.push_bytes(chunk);
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0], b"{\"c\":3}");
    assert_eq!(lines[1], b"{\"d\":4}");

    // 4. Finish with partial line
    framer.push_bytes(b"{\"e\":5}");
    let last = framer.finish().unwrap();
    assert_eq!(last, b"{\"e\":5}");
}

#[test]
fn parser_validates_objects() {
    let map = parse_ndjson_row(b"{\"key\": 123}").unwrap();
    assert_eq!(map["key"], json!(123));

    let err = parse_ndjson_row(b"[1, 2]").unwrap_err();
    match err {
        ShoalError::SchemaMismatch(msg) => assert!(msg.contains("must be a JSON object")),
        _ => panic!("Wrong error type"),
    }

    let err = parse_ndjson_row(b"{bad").unwrap_err();
    match err {
        ShoalError::JsonParse(_) => {}
        _ => panic!("Expected JsonParse error"),
    }
}

#[test]
fn version_is_non_empty() {
    assert!(!shoal_core::version().is_empty());
}

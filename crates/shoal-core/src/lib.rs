//! `shoal-core` is the single library crate that holds all core Depth Shoal logic.

pub mod prelude {
    //! Re-export common types once we start defining them.
}

/// Returns the crate version at compile time.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_non_empty() {
        assert!(!version().is_empty());
    }
}

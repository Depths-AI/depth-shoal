# Depth Shoal: A unified buffer and cache for tabular data

Depth Shoal (v0.1.0) is a small, DataFusion-native ingestion + query service with two modes:

- **cache**: in-memory NDJSON → Arrow → SQL queries
- **buffer**: crash-durable NDJSON WAL + group commit + flush to Parquet + upload via object_store

This repo is intentionally split into **two crates**:

- `shoal-core` (library): all the real logic (spec/ndjson/table/query/wal/flush/policy)
- `shoal-server` (binary): CLI + HTTP server (wires config + core)

## Quick start

```bash
cargo test -q
cargo run -q -p shoal-server -- --help
```

## Workspace notes

- Dependency versions are pinned centrally in the root `Cargo.toml` under `[workspace.dependencies]`.
- Toolchain is pinned via `rust-toolchain.toml` for reproducible builds.

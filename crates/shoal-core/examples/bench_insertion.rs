use serde_json::{json, Value};
use shoal_core::mem::table::{IngestionWorker, TableHandle, TableState};
use shoal_core::spec::{ShoalDataType, ShoalField, ShoalSchema, ShoalTableConfig};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::mpsc;

/// Benchmark insertion speed for different payload sizes.
/// Usage: cargo run -p shoal-core --example bench_insertion --release
#[tokio::main]
async fn main() {
    println!("Running Insertion Benchmark (Single Thread)...");

    let sizes = [128, 256, 512, 1024];

    for size in sizes {
        run_benchmark(size).await;
    }
}

async fn run_benchmark(payload_size: usize) {
    // 1. Setup Table
    let schema = ShoalSchema::new(vec![
        ShoalField {
            name: "id".parse().unwrap(),
            dtype: ShoalDataType::Int64,
            nullable: false,
        },
        ShoalField {
            name: "payload".parse().unwrap(),
            dtype: ShoalDataType::Utf8,
            nullable: false,
        },
    ])
    .unwrap();

    // Configure generous flush thresholds to test raw append speed, not flush speed
    let config = ShoalTableConfig {
        head_max_rows: 100_000,
        ..Default::default()
    };

    // Construct Handle + Worker manually (since we don't need full runtime for this bench)
    let table_state = TableState::new(schema, config).unwrap();
    let inner = Arc::new(RwLock::new(table_state));
    let (tx, rx) = mpsc::channel(1024);
    let worker = IngestionWorker::new(rx, inner.clone());

    tokio::spawn(worker.run());
    let table = TableHandle::new(tx, inner);

    // 2. Pre-generate data (avoid measuring JSON creation time)
    // Create a string of `payload_size` length
    let payload_str = "x".repeat(payload_size);
    let row_count = 100_000;
    let mut rows: Vec<serde_json::Map<String, Value>> = Vec::with_capacity(row_count);

    for i in 0..row_count {
        let row = json!({
            "id": i as i64,
            "payload": payload_str
        });
        rows.push(row.as_object().unwrap().clone());
    }

    // 3. Run Loop
    let start = Instant::now();
    let mut inserted = 0;

    // Insert the batch repeatedly until 3 seconds have passed
    while start.elapsed().as_secs() < 3 {
        for row in &rows {
            table.append_row(row.clone()).await.unwrap();
            inserted += 1;
        }
    }

    let duration = start.elapsed();
    let secs = duration.as_secs_f64();
    let rps = inserted as f64 / secs;
    let mbps = (inserted as f64 * payload_size as f64) / (1024.0 * 1024.0) / secs;

    println!(
        "Payload: {:4} bytes | {:10.0} rows/sec | {:8.2} MB/sec",
        payload_size, rps, mbps
    );
}

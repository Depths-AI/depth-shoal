use serde_json::{json, Value};
use shoal_core::mem::table::ShoalTable;
use shoal_core::spec::{ShoalDataType, ShoalField, ShoalSchema, ShoalTableConfig};
use std::time::Instant;

/// Benchmark insertion speed for different payload sizes.
/// Usage: cargo run -p shoal-core --example bench_insertion --release
fn main() {
    println!("Running Insertion Benchmark (Single Thread)...");

    let sizes = [128, 256, 512, 1024];

    for size in sizes {
        run_benchmark(size);
    }
}

fn run_benchmark(payload_size: usize) {
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

    let table = ShoalTable::new(schema, config).unwrap();

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
            table.append_row(row.clone()).unwrap();
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

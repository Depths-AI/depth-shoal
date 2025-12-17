use serde_json::{json, Value};
use shoal_core::mem::table::TableHandle;
use shoal_core::spec::{ShoalDataType, ShoalField, ShoalSchema, ShoalTableConfig};
use std::time::Instant;

#[tokio::main]
async fn main() {
    println!("Running Insertion Benchmark (Single Thread)...");

    let sizes = [128, 256, 512, 1024];

    for size in sizes {
        run_benchmark(size).await;
    }
}

async fn run_benchmark(payload_size: usize) {
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

    let config = ShoalTableConfig {
        active_head_max_rows: 100_000,
        ..Default::default()
    };

    // Clean public API usage
    let table = TableHandle::create(schema, config).unwrap();

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

    let start = Instant::now();
    let mut inserted = 0;

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

// Standalone probe: call the hudi-rs 0.5.0 kernel directly (same compiled
// crate + runtime as the extension) to isolate kernel-vs-datapath for a fixture.
// Run: cargo run --example kernel_probe -- <fixture_path>
use std::process::exit;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/Users/sa/ws/hudi-rs/crates/core/tests/data/timeline/commits_load_schema_from_base_file_cow".into());

    let table = hudi::Table::new(&path).await?;
    println!("[probe] table opened: {path}");

    let stats = table.compute_table_stats().await?;
    println!(
        "[probe] table_stats: rows={:?} size_bytes={:?}",
        stats.record_count,
        stats.file_size_bytes
    );

    let batches = table.read(&hudi::ReadOptions::new()).await?;
    println!("[probe] read() returned {} batches", batches.len());
    let total: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    println!("[probe] total rows across batches = {total}");
    for (i, b) in batches.iter().enumerate() {
        let names: Vec<_> = b.schema().field_names().iter().map(|s| s.as_str()).collect();
        println!("  batch[{i}] rows={} cols={:?}", b.num_rows(), names);
    }

    // Also try read_stream to confirm the snapshot path.
    let mut stream = table.read_stream(&hudi::ReadOptions::new()).await?;
    let mut stream_rows: u64 = 0;
    while let Some(rb) = stream.next().await {
        let rb = rb?;
        stream_rows += rb.num_rows() as u64;
    }
    println!("[probe] read_stream() total rows = {stream_rows}");

    if total == 0 {
        eprintln!("[probe] KERNEL RETURNED 0 ROWS — kernel/fixture issue, not data path");
        exit(2);
    }
    Ok(())
}

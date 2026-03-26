//! Creates the bench.events table and populates it with ~7 GiB of data.
//!
//! Run with:
//!   cargo run --example bench_setup --features examples
//!   cargo run --example bench_setup --features examples -- --rows 40000000 --workers 32
//!
//! Text column cardinality:
//!   event     — 20 distinct values  (low:  Parquet dict-encodes to almost nothing)
//!   device_id — 10 000 distinct values (medium)
//!   user_id   — 100 000 distinct values (medium)
//!
//! At ~200 bytes/row (raw, before LZ4) the default 40 M rows produces roughly
//! 7–8 GiB of logical data.  Actual on-disk size will be lower due to compression.

#![allow(clippy::print_stdout)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use scylla::{client::session_builder::SessionBuilder, value::CqlTimestamp};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Text column pools
// ---------------------------------------------------------------------------

const EVENTS: &[&str] = &[
    "page_view",
    "click",
    "purchase",
    "login",
    "logout",
    "signup",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "checkout",
    "payment_success",
    "payment_failed",
    "refund",
    "share",
    "like",
    "comment",
    "view_product",
    "download",
    "install",
    "uninstall",
];

const DEVICE_COUNT: u64 = 10_000;
const USER_COUNT: u64 = 100_000;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(about = "Populate bench.events with bulk data for benchmarking")]
struct Args {
    /// ScyllaDB node address
    #[arg(long, default_value = "127.0.0.1:9042")]
    node: String,

    /// Total rows to insert (~200 bytes each; 40 M ≈ 7 GiB logical)
    #[arg(long, default_value_t = 40_000_000u64)]
    rows: u64,

    /// Concurrent writer tasks
    #[arg(long, default_value_t = 32u64)]
    workers: u64,
}

// ---------------------------------------------------------------------------
// Fast PRNG (xorshift64) — no external dependency, good distribution
// ---------------------------------------------------------------------------

#[inline]
fn xorshift64(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("Connecting to {}...", args.node);
    let session = Arc::new(SessionBuilder::new().known_node(&args.node).build().await?);

    // -- keyspace -----------------------------------------------------------
    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS bench \
             WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[],
        )
        .await?;

    // -- table (drop + recreate for a clean slate) --------------------------
    println!("Recreating bench.events...");
    session
        .query_unpaged("DROP TABLE IF EXISTS bench.events", &[])
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    session
        .query_unpaged(
            "CREATE TABLE bench.events (
                id        uuid,
                ts        timestamp,
                device_id text,
                user_id   text,
                event     text,
                value     double,
                count     int,
                active    boolean,
                PRIMARY KEY (id)
            ) WITH compression = {'sstable_compression': 'LZ4Compressor'}",
            &[],
        )
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // -- prepared statement -------------------------------------------------
    let insert = Arc::new(
        session
            .prepare(
                "INSERT INTO bench.events \
                 (id, ts, device_id, user_id, event, value, count, active) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .await?,
    );

    // -- pre-generate string pools ------------------------------------------
    let device_ids: Arc<Vec<String>> =
        Arc::new((0..DEVICE_COUNT).map(|i| format!("dev-{i:05}")).collect());
    let user_ids: Arc<Vec<String>> =
        Arc::new((0..USER_COUNT).map(|i| format!("user-{i:06}")).collect());

    // -- progress bar -------------------------------------------------------
    let pb = Arc::new(ProgressBar::new(args.rows));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
                 {pos}/{len} rows  ({per_sec}, eta {eta})",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );

    // Timestamp range: 2023-01-01 → 2025-01-01 (milliseconds since epoch)
    let ts_start: i64 = 1_672_531_200_000;
    let ts_range: u64 = 63_072_000_000; // 2 years in ms

    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // -- spawn worker tasks -------------------------------------------------
    let rows_per_worker = args.rows / args.workers;
    let remainder = args.rows % args.workers; // absorbed into worker 0

    let mut handles = Vec::with_capacity(args.workers as usize);
    for worker_id in 0..args.workers {
        let session = Arc::clone(&session);
        let insert = Arc::clone(&insert);
        let device_ids = Arc::clone(&device_ids);
        let user_ids = Arc::clone(&user_ids);
        let counter = Arc::clone(&counter);
        let pb: Arc<ProgressBar> = Arc::clone(&pb);

        let my_rows = rows_per_worker + if worker_id == 0 { remainder } else { 0 };

        handles.push(tokio::spawn(async move {
            // Each worker gets a unique seed via Knuth multiplicative hashing.
            let mut rng: u64 =
                (worker_id + 1).wrapping_mul(6364136223846793005) ^ 1442695040888963407;

            for _ in 0..my_rows {
                let r1 = xorshift64(&mut rng);
                let r2 = xorshift64(&mut rng);

                let id = Uuid::from_u128((r1 as u128) << 64 | r2 as u128);
                let ts = CqlTimestamp(ts_start + (r1 % ts_range) as i64);
                let device_id = device_ids[(r1 % DEVICE_COUNT) as usize].as_str();
                let user_id = user_ids[(r2 % USER_COUNT) as usize].as_str();
                let event = EVENTS[(r1 % EVENTS.len() as u64) as usize];
                let value: f64 = (r1 % 1_000_000) as f64 / 100.0;
                let count: i32 = (r2 % 10_000) as i32;
                let active: bool = r1 % 2 == 0;

                session
                    .execute_unpaged(
                        &insert,
                        (id, ts, device_id, user_id, event, value, count, active),
                    )
                    .await?;

                let done = counter.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 5_000 == 0 {
                    pb.set_position(done);
                }
            }

            Ok::<_, anyhow::Error>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }

    pb.finish_with_message("done");

    let elapsed = start.elapsed();
    let total = counter.load(Ordering::Relaxed);
    let rows_per_sec = total as f64 / elapsed.as_secs_f64();
    let logical_gib = total as f64 * 200.0 / (1024.0_f64.powi(3));

    println!(
        "\nInserted {total} rows in {:.1}s  ({rows_per_sec:.0} rows/sec)",
        elapsed.as_secs_f64(),
    );
    println!("Estimated logical size: ~{logical_gib:.1} GiB  (before LZ4 compression)");
    println!("\nText column cardinality written:");
    println!("  event     — {} distinct values (low)", EVENTS.len());
    println!("  device_id — {DEVICE_COUNT} distinct values (medium)");
    println!("  user_id   — {USER_COUNT} distinct values (medium)");
    println!("\nSample query:");
    println!(
        "  circe token-range bench.events --partition-keys id \
         --output file:///tmp/bench_events.parquet"
    );

    Ok(())
}

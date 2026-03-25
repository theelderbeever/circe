//! Populates a local ScyllaDB instance with sample data for manual testing.
//!
//! Run with: cargo run --example populate_test_data
//!
//! This creates:
//! - Keyspace: test_ks
//! - Table: events with timestamp clustering for time-series queries
//! - Table: users with simple partition key for basic testing

use scylla::{client::session_builder::SessionBuilder, value::CqlTimestamp};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to ScyllaDB at 127.0.0.1:9042...");
    let session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await?;

    println!("Creating keyspace test_ks...");
    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS test_ks \
             WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[],
        )
        .await?;

    // Drop existing tables for clean slate
    println!("Dropping existing tables...");
    let _ = session
        .query_unpaged("DROP TABLE IF EXISTS test_ks.events", &[])
        .await;
    let _ = session
        .query_unpaged("DROP TABLE IF EXISTS test_ks.users", &[])
        .await;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Create events table with timestamp clustering
    println!("Creating test_ks.events table...");
    session
        .query_unpaged(
            "CREATE TABLE test_ks.events (
                user_id bigint,
                event_time timestamp,
                event_type text,
                region text,
                value int,
                PRIMARY KEY ((user_id), event_time)
            ) WITH CLUSTERING ORDER BY (event_time DESC)",
            &[],
        )
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Insert events data spanning multiple days
    println!("Inserting events data...");
    let insert_event = session
        .prepare(
            "INSERT INTO test_ks.events (user_id, event_time, event_type, region, value) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .await?;

    // Generate events for 10 users over 30 days
    let base_time = 1704067200000i64; // 2024-01-01 00:00:00 UTC in milliseconds
    let one_day_ms = 86400000i64;
    let mut count = 0;

    for user_id in 1..=10 {
        let region = match user_id % 4 {
            0 => "us-east",
            1 => "us-west",
            2 => "eu-west",
            _ => "ap-south",
        };

        // 100 events per user over 30 days
        for day in 0..30 {
            for event in 0..3 {
                let event_time_ms = base_time + (day * one_day_ms) + (event * 3600000);
                let event_time = CqlTimestamp(event_time_ms);

                let event_type = match event % 3 {
                    0 => "login",
                    1 => "purchase",
                    _ => "logout",
                };

                let value = ((user_id * day + event) % 100) as i32;

                session
                    .execute_unpaged(
                        &insert_event,
                        (user_id, event_time, event_type, region, value),
                    )
                    .await?;

                count += 1;
            }
        }
    }

    println!("Inserted {} events", count);

    // Create users table for basic testing
    println!("Creating test_ks.users table...");
    session
        .query_unpaged(
            "CREATE TABLE test_ks.users (
                region text,
                user_id bigint,
                name text,
                age int,
                score double,
                active boolean,
                PRIMARY KEY ((region), user_id)
            )",
            &[],
        )
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    println!("Inserting users data...");
    let insert_user = session
        .prepare(
            "INSERT INTO test_ks.users (region, user_id, name, age, score, active) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .await?;

    let users: Vec<(&str, i64, &str, i32, f64, bool)> = vec![
        ("us-east", 1, "Alice", 30, 95.5, true),
        ("us-east", 2, "Bob", 25, 88.0, true),
        ("us-east", 3, "Charlie", 35, 72.3, false),
        ("us-west", 4, "Diana", 28, 91.2, true),
        ("us-west", 5, "Eve", 22, 67.8, false),
        ("eu-west", 6, "Frank", 40, 85.1, true),
        ("eu-west", 7, "Grace", 33, 90.0, true),
        ("eu-west", 8, "Hank", 45, 78.5, false),
        ("ap-south", 9, "Ivy", 27, 93.4, true),
        ("ap-south", 10, "Jack", 31, 82.7, true),
    ];

    for (region, user_id, name, age, score, active) in users {
        session
            .execute_unpaged(&insert_user, (region, user_id, name, age, score, active))
            .await?;
    }

    println!("Inserted {} users", 10);

    println!("\nDatabase populated successfully!");
    println!("\nTables created:");
    println!(
        "  - test_ks.events ({} rows with timestamp clustering)",
        count
    );
    println!("  - test_ks.users (10 rows with region partition)");
    println!("\nExample queries:");
    println!("\nToken range (full table scan):");
    println!(
        "  circe token-range test_ks.events --partition-keys user_id --output file:///tmp/all_events.parquet"
    );
    println!(
        "  circe token-range test_ks.users --partition-keys region --output file:///tmp/all_users.parquet"
    );
    println!("\nFrom query (filtered by time range):");
    println!(
        "  circe from-query \"SELECT * FROM test_ks.events WHERE user_id = ? AND event_time >= ? AND event_time < ?\" \\"
    );
    println!("    --params \"1,2024-01-15T00:00:00Z,2024-01-20T00:00:00Z\" \\");
    println!("    --output file:///tmp/user1_jan15_20.parquet");
    println!("\nFrom query (multiple users, concurrent):");
    println!("  circe from-query \"SELECT * FROM test_ks.events WHERE user_id = ?\" \\");
    println!("    --params 1 --params 2 --params 3 \\");
    println!("    --output file:///tmp/users_1_2_3.parquet");

    Ok(())
}

#![allow(clippy::expect_used)]
use std::path::PathBuf;
use std::sync::Arc;

use circe_providers::token_range::ScyllaTokenRangeProvider;
use circe_providers::writer::write_scylla_to_parquet;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

async fn setup_scylla() -> Arc<Session> {
    let session = SessionBuilder::new()
        .known_node("127.0.0.1:9042")
        .build()
        .await
        .expect("Failed to connect to ScyllaDB");

    let session = Arc::new(session);

    // Create keyspace
    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS test_ks \
             WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[],
        )
        .await
        .expect("Failed to create keyspace");

    // Drop and recreate to ensure clean schema
    let _ = session
        .query_unpaged("DROP TABLE IF EXISTS test_ks.test_export", &[])
        .await;

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    session
        .query_unpaged(
            "CREATE TABLE test_ks.test_export (
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
        .await
        .expect("Failed to create table");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Insert test data across multiple partitions
    let insert = session
        .prepare(
            "INSERT INTO test_ks.test_export (region, user_id, name, age, score, active) \
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .await
        .expect("Failed to prepare insert");

    let rows: Vec<(&str, i64, &str, i32, f64, bool)> = vec![
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

    for (region, user_id, name, age, score, active) in rows {
        session
            .execute_unpaged(&insert, (region, user_id, name, age, score, active))
            .await
            .expect("Failed to insert row");
    }

    session
}

fn output_dir() -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("no parent")
        .join("data");
    // Clean up from previous runs
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("Failed to clean data dir");
    }
    std::fs::create_dir_all(&dir).expect("Failed to create data dir");
    dir
}

#[tokio::test]
async fn test_token_range_scan_to_hive_parquet() {
    let session = setup_scylla().await;

    // Build the provider — use small concurrency for a local test cluster
    let provider =
        ScyllaTokenRangeProvider::builder(session, "test_ks".into(), "test_export".into())
            .partition_key_columns(vec!["region".into()])
            .nodes(1)
            .cores_per_node(1)
            .build()
            .await
            .expect("Failed to build provider");

    let provider = Arc::new(provider);

    // Verify schema was derived correctly
    let schema = provider.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"region"),
        "Schema missing 'region': {field_names:?}"
    );
    assert!(
        field_names.contains(&"user_id"),
        "Schema missing 'user_id': {field_names:?}"
    );
    assert!(
        field_names.contains(&"name"),
        "Schema missing 'name': {field_names:?}"
    );

    // Write hive-partitioned parquet to local filesystem
    let dir = output_dir();
    let output_url = dir.to_str().expect("invalid path");

    let ctx = SessionContext::new();

    write_scylla_to_parquet(&ctx, provider, output_url)
        .await
        .expect("Failed to write parquet");

    // Verify output: read back the parquet files with hive partitioning enabled
    let read_ctx = SessionContext::new();
    let listing_options = datafusion::datasource::listing::ListingOptions::new(Arc::new(
        datafusion::datasource::file_format::parquet::ParquetFormat::new(),
    ))
    .with_file_extension(".parquet")
    .with_table_partition_cols(vec![(
        "region".to_string(),
        datafusion::arrow::datatypes::DataType::Utf8,
    )]);

    let table_path = datafusion::datasource::listing::ListingTableUrl::parse(output_url)
        .expect("Failed to parse table path");
    let resolved_schema = listing_options
        .infer_schema(&read_ctx.state(), &table_path)
        .await
        .expect("Failed to infer schema");

    let listing_config = datafusion::datasource::listing::ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let listing_table = datafusion::datasource::listing::ListingTable::try_new(listing_config)
        .expect("Failed to create listing table");

    read_ctx
        .register_table("exported", Arc::new(listing_table))
        .expect("Failed to register listing table");

    let df = read_ctx
        .sql("SELECT COUNT(*) AS cnt FROM exported")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Expected Int64Array for count")
        .value(0);

    assert_eq!(count, 10, "Expected 10 rows, got {count}");

    // Verify hive partitioning structure: should have region=* directories
    let mut found_partitions: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("Failed to read output dir") {
        let entry = entry.expect("Failed to read entry");
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("region=") {
            found_partitions.push(name);
        }
    }
    found_partitions.sort();

    assert!(
        !found_partitions.is_empty(),
        "Expected hive partition directories (region=*), found none in {dir:?}"
    );

    // Verify we can query by partition column
    let df = read_ctx
        .sql("SELECT * FROM exported WHERE region = 'us-east' ORDER BY user_id")
        .await
        .expect("Failed to query by region");
    let batches = df.collect().await.expect("Failed to collect");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Expected 3 rows for us-east, got {total_rows}"
    );
}

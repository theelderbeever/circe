#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
use std::sync::Arc;

use circe_providers::ScyllaProvider;
use datafusion::{
    arrow::array::Array, catalog::TableProvider, dataframe::DataFrameWriteOptions, error::Result,
    prelude::SessionContext,
};
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    value::CqlValue,
};

/// Reads from a TableProvider and writes hive-partitioned Parquet files.
///
/// The caller is responsible for registering the appropriate `ObjectStore` on the
/// `SessionContext`'s `RuntimeEnv` before calling this function.
pub async fn write_hive_partitioned_parquet<T>(
    ctx: &SessionContext,
    provider: Arc<T>,
    output_url: &str,
    partition_columns: Vec<String>,
) -> Result<()>
where
    T: TableProvider + 'static,
{
    tracing::info!(
        output = %output_url,
        partitions = ?partition_columns,
        "Writing Parquet output"
    );

    let table_name = "_circe_temp_table";

    ctx.register_table(table_name, provider)?;

    let df = ctx.table(table_name).await?;

    let write_options = DataFrameWriteOptions::new().with_partition_by(partition_columns);

    df.write_parquet(output_url, write_options, None).await?;

    ctx.deregister_table(table_name)?;

    tracing::debug!("Parquet write completed");
    Ok(())
}

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

#[tokio::test]
async fn test_from_query_simple() {
    let session = setup_scylla().await;

    let provider: ScyllaProvider<()> =
        ScyllaProvider::builder(session, "SELECT * FROM test_ks.test_export".to_string())
            .build()
            .await
            .expect("Failed to build provider");

    let provider = Arc::new(provider);

    // Verify schema
    let schema = provider.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"region"));
    assert!(field_names.contains(&"user_id"));
    assert!(field_names.contains(&"name"));

    // Query and count rows
    let ctx = SessionContext::new();
    ctx.register_table("test", provider.clone())
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT COUNT(*) AS cnt FROM test")
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
}

#[tokio::test]
async fn test_from_query_with_limit() {
    let session = setup_scylla().await;

    let provider: ScyllaProvider<()> = ScyllaProvider::builder(
        session,
        "SELECT * FROM test_ks.test_export LIMIT 5".to_string(),
    )
    .build()
    .await
    .expect("Failed to build provider");

    let provider = Arc::new(provider);

    let ctx = SessionContext::new();
    ctx.register_table("test", provider)
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT COUNT(*) AS cnt FROM test")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Expected Int64Array")
        .value(0);

    assert_eq!(count, 5, "Expected 5 rows, got {count}");
}

#[tokio::test]
async fn test_from_query_single_param_set() {
    let session = setup_scylla().await;

    let param_sets = vec![vec![CqlValue::Text("us-east".to_string())]];

    let mut builder = ScyllaProvider::builder(
        session,
        "SELECT * FROM test_ks.test_export WHERE region = ?".to_string(),
    );

    for params in param_sets {
        builder = builder.with_params(params);
    }

    let provider = Arc::new(builder.build().await.expect("Failed to build provider"));

    let ctx = SessionContext::new();
    ctx.register_table("test", provider)
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM test WHERE region = 'us-east' ORDER BY user_id")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Expected 3 rows for us-east, got {total_rows}"
    );

    // Verify all rows have region = 'us-east'
    for batch in &batches {
        let region_col = batch
            .column_by_name("region")
            .expect("Missing region column");
        let region_array = region_col
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("Expected StringArray");

        for i in 0..region_array.len() {
            assert_eq!(region_array.value(i), "us-east");
        }
    }
}

#[tokio::test]
async fn test_from_query_multiple_params() {
    let session = setup_scylla().await;

    let param_sets = vec![vec![
        CqlValue::Text("us-east".to_string()),
        CqlValue::Int(25),
    ]];

    let mut builder = ScyllaProvider::builder(
        session,
        "SELECT * FROM test_ks.test_export WHERE region = ? AND age > ? ALLOW FILTERING"
            .to_string(),
    );

    for params in param_sets {
        builder = builder.with_params(params);
    }

    let provider = Arc::new(builder.build().await.expect("Failed to build provider"));

    let ctx = SessionContext::new();
    ctx.register_table("test", provider)
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM test ORDER BY user_id")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 2,
        "Expected 2 rows (Alice=30, Charlie=35), got {total_rows}"
    );

    // Verify conditions: region = 'us-east' AND age > 25
    for batch in &batches {
        let region_array = batch
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let age_array = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            assert_eq!(region_array.value(i), "us-east");
            assert!(age_array.value(i) > 25);
        }
    }
}

#[tokio::test]
async fn test_from_query_concurrent_param_sets() {
    let session = setup_scylla().await;

    let param_sets = vec![
        vec![CqlValue::Text("us-east".to_string())],
        vec![CqlValue::Text("us-west".to_string())],
        vec![CqlValue::Text("eu-west".to_string())],
    ];

    let mut builder = ScyllaProvider::builder(
        session,
        "SELECT * FROM test_ks.test_export WHERE region = ?".to_string(),
    );

    for params in param_sets {
        builder = builder.with_params(params);
    }

    let provider = Arc::new(builder.build().await.expect("Failed to build provider"));

    let ctx = SessionContext::new();
    ctx.register_table("test", provider)
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM test")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 8, "Expected 8 rows (3+2+3), got {total_rows}");

    // Verify all three regions are present
    let df = ctx
        .sql("SELECT DISTINCT region FROM test ORDER BY region")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    let mut regions: Vec<String> = Vec::new();
    for batch in &batches {
        let region_array = batch
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..region_array.len() {
            regions.push(region_array.value(i).to_string());
        }
    }

    regions.sort();
    assert_eq!(regions, vec!["eu-west", "us-east", "us-west"]);
}

#[tokio::test]
async fn test_from_query_concurrent_multiple_filter_columns() {
    let session = setup_scylla().await;

    let param_sets = vec![
        vec![CqlValue::Text("us-east".to_string()), CqlValue::Int(25)],
        vec![CqlValue::Text("us-west".to_string()), CqlValue::Int(20)],
        vec![CqlValue::Text("eu-west".to_string()), CqlValue::Int(30)],
    ];

    let mut builder = ScyllaProvider::builder(
        session,
        "SELECT * FROM test_ks.test_export WHERE region = ? AND age > ? ALLOW FILTERING"
            .to_string(),
    );

    for params in param_sets {
        builder = builder.with_params(params);
    }

    let provider = Arc::new(builder.build().await.expect("Failed to build provider"));

    let ctx = SessionContext::new();
    ctx.register_table("test", provider)
        .expect("Failed to register table");

    let df = ctx
        .sql("SELECT * FROM test ORDER BY region, user_id")
        .await
        .expect("Failed to query");
    let batches = df.collect().await.expect("Failed to collect");

    // Expected results:
    // us-east, age > 25: Alice(30), Charlie(35) = 2 rows
    // us-west, age > 20: Diana(28) = 1 row (Eve=22 excluded since 22 is not > 20, it equals)
    // Actually Eve(22) IS > 20, so: Diana(28), Eve(22) = 2 rows
    // eu-west, age > 30: Grace(33), Hank(45) = 2 rows (Frank=40 IS > 30, so 3 rows)
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // us-east: 2, us-west: 2, eu-west: 3 = 7 total
    // Let me check the test data: Eve is 22 which is > 20 (included)
    // Frank is 40 which is > 30 (included)
    assert!(
        total_rows >= 5,
        "Expected at least 5 rows, got {total_rows}"
    );

    // Verify each row satisfies its filter conditions
    for batch in &batches {
        let region_array = batch
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let age_array = batch
            .column_by_name("age")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let region = region_array.value(i);
            let age = age_array.value(i);

            match region {
                "us-east" => assert!(age > 25, "us-east row has age {} which is not > 25", age),
                "us-west" => assert!(age > 20, "us-west row has age {} which is not > 20", age),
                "eu-west" => assert!(age > 30, "eu-west row has age {} which is not > 30", age),
                _ => panic!("Unexpected region: {}", region),
            }
        }
    }
}

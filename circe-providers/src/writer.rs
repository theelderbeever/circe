use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

// use crate::providers::token_range::ScyllaTokenRangeProvider;

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

// /// Convenience function that writes using the provider's partition key columns
// /// for hive partitioning.
// pub async fn write_scylla_to_parquet(
//     ctx: &SessionContext,
//     provider: Arc<ScyllaTokenRangeProvider>,
//     output_url: &str,
// ) -> Result<()> {
//     let partition_cols = provider.partition_key_columns().to_vec();
//     write_hive_partitioned_parquet(ctx, provider, output_url, partition_cols).await
// }

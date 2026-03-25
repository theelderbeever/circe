use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, dataframe::DataFrameWriteOptions, error::Result,
    prelude::SessionContext,
};

use crate::OutputArgs;

/// Reads from a TableProvider and writes hive-partitioned Parquet files.
///
/// If `output.transform` is set, the DataFusion SQL is run against the provider data
/// (referenced as "source") before writing. The caller is responsible for registering
/// the appropriate `ObjectStore` on the `SessionContext`'s `RuntimeEnv`.
pub async fn write_hive_partitioned_parquet<T>(
    ctx: &SessionContext,
    provider: Arc<T>,
    output: &OutputArgs,
) -> Result<()>
where
    T: TableProvider + 'static,
{
    let partition_columns = output.partition_by.clone().unwrap_or_default();

    tracing::info!(
        output = %output.output,
        partitions = ?partition_columns,
        "Writing Parquet output"
    );

    ctx.register_table("source", provider)?;

    let df = match &output.transform {
        Some(sql) => ctx.sql(sql).await?,
        None => ctx.table("source").await?,
    };

    let write_options = DataFrameWriteOptions::new().with_partition_by(partition_columns);

    df.write_parquet(&output.output, write_options, None)
        .await?;

    ctx.deregister_table("source")?;

    tracing::debug!("Parquet write completed");
    Ok(())
}

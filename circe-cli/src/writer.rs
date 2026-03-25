use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, dataframe::DataFrameWriteOptions, error::Result,
    prelude::SessionContext,
};

use crate::{OutputArgs, OutputFormat};

/// Reads from a TableProvider and writes output in the configured format.
///
/// If the output path has a recognized extension (.parquet, .csv, .ndjson, .jsonl) it
/// takes precedence over `--format`. If `output.transform` is set, the DataFusion SQL
/// is run against the provider data (referenced as "source") before writing. The caller
/// is responsible for registering the appropriate `ObjectStore` on the
/// `SessionContext`'s `RuntimeEnv`.
pub async fn write<T>(ctx: &SessionContext, provider: Arc<T>, output: &OutputArgs) -> Result<()>
where
    T: TableProvider + 'static,
{
    let format = output
        .output_format()
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;
    let partition_columns = output.partition_by.clone().unwrap_or_default();

    tracing::debug!(
        output = %output.output,
        format = ?format,
        single_file = output.single_file,
        partitions = ?partition_columns,
        "Writing output"
    );

    ctx.register_table("source", provider)?;

    let df = match &output.transform {
        Some(sql) => ctx.sql(sql).await?,
        None => ctx.table("source").await?,
    };

    let write_options = DataFrameWriteOptions::new()
        .with_single_file_output(output.single_file)
        .with_partition_by(partition_columns);

    match format {
        OutputFormat::Parquet => {
            df.write_parquet(&output.output, write_options, None)
                .await?;
        }
        OutputFormat::Csv => {
            df.write_csv(&output.output, write_options, None).await?;
        }
        OutputFormat::Ndjson => {
            df.write_json(&output.output, write_options, None).await?;
        }
    }

    ctx.deregister_table("source")?;

    tracing::debug!("Write completed");
    Ok(())
}

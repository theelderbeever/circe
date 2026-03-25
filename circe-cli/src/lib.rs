use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use datafusion::object_store::aws::AmazonS3Builder;
use datafusion::prelude::SessionContext;
use indicatif::ProgressStyle;
use itertools::Itertools;
use scylla::client::session_builder::SessionBuilder;
use scylla::frame::Compression;
use scylla::value::CqlValue;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

use circe_providers::ScyllaProvider;

use crate::writer::write_hive_partitioned_parquet;

pub(crate) mod params;
pub(crate) mod writer;

#[derive(Debug, Parser)]
#[command(name = "circe", about = "Export ScyllaDB tables to columnar formats")]
pub struct Cli {
    /// Enable interactive progress display
    #[arg(short = 'i', long, global = true)]
    pub interactive: bool,

    #[command(flatten)]
    pub scylla: ScyllaArgs,

    #[command(flatten)]
    pub output: OutputArgs,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Args)]
pub struct ScyllaArgs {
    /// ScyllaDB contact points (comma-separated)
    #[arg(
        env = "SCYLLA_KNOWN_NODES",
        default_value = "127.0.0.1:9042",
        value_delimiter = ','
    )]
    pub known_nodes: Vec<String>,

    /// ScyllaDB username
    #[arg(long, env = "SCYLLA_USER")]
    pub scylla_user: Option<String>,

    /// ScyllaDB password
    #[arg(long, env = "SCYLLA_PASSWORD")]
    pub scylla_password: Option<String>,
}

#[derive(Debug, Args)]
pub struct OutputArgs {
    /// Output URL (file:///path or s3://bucket/prefix)
    #[arg(short, long, env = "CIRCE_OUTPUT")]
    pub output: String,

    /// Hive partition columns (comma-separated)
    #[arg(long, value_delimiter = ',')]
    pub partition_by: Option<Vec<String>>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Full table scan using token range queries
    TokenRange(TokenRangeArgs),

    /// Execute a custom query (not yet implemented)
    FromQuery(FromQueryArgs),
}

#[derive(Debug, Args)]
pub struct TokenRangeArgs {
    /// Fully qualified table name in the form {keyspace}.{table}
    #[arg()]
    pub table: String,

    /// Partition key columns for token range scanning (comma-separated). Must be in order of table partition key.
    #[arg(long, value_delimiter = ',')]
    pub partition_keys: Vec<String>,

    /// Columns to select (comma-separated, defaults to all)
    #[arg(long, value_delimiter = ',', default_value = "*")]
    pub columns: Vec<String>,

    /// Number of token range splits and max concurrent queries (defaults to 2 * num_cpus)
    #[arg(short = 'C', long, default_value_t = num_cpus::get() * 2)]
    pub concurrency: usize,
}

impl TokenRangeArgs {
    /// Splits the full Murmur3 token space into `num_ranges` sub-ranges.
    ///
    /// Returns a lazy iterator of [`TokenRange`] values covering
    /// `i64::MIN..=i64::MAX`.
    pub fn token_ranges(&self) -> impl Iterator<Item = (i64, i64, usize)> {
        const MIN_TOKEN: i128 = i64::MIN as i128;
        const MAX_TOKEN: i128 = i64::MAX as i128;

        let total_range = (MAX_TOKEN - MIN_TOKEN) as usize;
        let step = total_range / self.concurrency;

        (MIN_TOKEN..=MAX_TOKEN)
            .step_by(step)
            .tuple_windows()
            .enumerate()
            .map(|(i, (start, end))| (start.max(MIN_TOKEN) as i64, end.min(MAX_TOKEN) as i64, i))
    }
}

#[derive(Debug, Args)]
pub struct FromQueryArgs {
    /// The CQL query to execute
    pub query: String,

    /// CSV-delimited parameters for ? placeholders in query.
    /// Can be repeated multiple times for concurrent execution.
    /// Example: --params "value1,value2" --params "value3,value4"
    /// This creates 2 partitions executing the query concurrently.
    #[arg(long, conflicts_with = "params_file")]
    pub params: Option<Vec<String>>,

    /// File containing parameter sets, one set per line (CSV format).
    /// Enables concurrent execution with many parameter sets.
    /// Example file content:
    ///   us-east,25
    ///   us-west,30
    ///   eu-west,35
    #[arg(long, conflicts_with = "params")]
    pub params_file: Option<PathBuf>,

    /// Max concurrent queries to Scylla (defaults to num_cpus * 10)
    #[arg(short = 'C', long)]
    pub max_concurrency: Option<usize>,
}

impl Cli {
    /// Sets up a progress bar for interactive mode and returns a callback and span.
    /// Returns (optional_span, builder) with the builder already configured with the callback.
    fn setup_progress_bar<B, F>(
        interactive: bool,
        builder: B,
        attach_callback: F,
    ) -> anyhow::Result<(Option<tracing::Span>, B)>
    where
        F: FnOnce(B, Arc<dyn Fn(usize) + Send + Sync>) -> B,
    {
        if interactive {
            let current_span = tracing::Span::current();
            current_span.pb_set_style(
                &ProgressStyle::with_template(
                    "[{elapsed_precise}] {bar:100.cyan/blue} {pos:>5}/{len:5} {per_sec:.1} [{eta_precise}]",
                )
                .context("invalid progress bar template")?,
            );

            let span_clone = current_span.clone();
            let callback = Arc::new(move |_: usize| {
                span_clone.pb_inc(1);
            });

            let builder = attach_callback(builder, callback);
            Ok((Some(current_span), builder))
        } else {
            Ok((None, builder))
        }
    }

    #[tracing::instrument]
    pub async fn execute(self) -> anyhow::Result<()> {
        // --- Tracing setup ---
        setup_tracing(self.interactive);

        // --- ScyllaDB session ---
        let mut session_builder = SessionBuilder::new();
        for node in &self.scylla.known_nodes {
            session_builder = session_builder
                .known_node(node)
                .compression(Some(Compression::Lz4));
        }
        if let (Some(user), Some(password)) =
            (&self.scylla.scylla_user, &self.scylla.scylla_password)
        {
            session_builder = session_builder.user(user, password);
        }
        let session = Arc::new(session_builder.build().await?);

        // --- Normalize output path ---
        let normalized_output = normalize_output_path(&self.output.output)?;

        // --- SessionContext + object store ---
        let ctx = SessionContext::new();
        register_object_store(&normalized_output, &ctx)?;

        // --- Dispatch command ---
        let interactive = self.interactive;
        let mut output = self.output;
        output.output = normalized_output;

        match self.command {
            Command::TokenRange(args) => {
                run_token_range(interactive, &output, args, session, &ctx).await?;
            }
            Command::FromQuery(args) => {
                run_from_query(interactive, &output, args, session, &ctx).await?;
            }
        }

        Ok(())
    }
}

#[tracing::instrument(skip_all, name = "export")]
async fn run_token_range(
    interactive: bool,
    output: &OutputArgs,
    args: TokenRangeArgs,
    session: Arc<scylla::client::session::Session>,
    ctx: &SessionContext,
) -> anyhow::Result<()> {
    tracing::info!(table = %args.table, output = %output.output, "Starting token range scan");

    let partition_keys = args.partition_keys.clone();

    let (keyspace, table) = args
        .table
        .split_once('.')
        .context("--table must be in the form {keyspace}.{table}")?;

    let pk_cols_csv = args.partition_keys.join(", ");
    let select_cols = args.columns.join(", ");
    let scan_query = format!(
        "SELECT {select_cols} FROM {keyspace}.{table} WHERE TOKEN({pk_cols_csv}) >= ? AND TOKEN({pk_cols_csv}) < ? BYPASS CACHE"
    );

    let param_sets: Vec<Vec<CqlValue>> = args
        .token_ranges()
        .map(|(start, end, ..)| vec![CqlValue::BigInt(start), CqlValue::BigInt(end)])
        .collect();
    let num_ranges = param_sets.len();

    let mut builder =
        ScyllaProvider::builder(session, scan_query).with_max_concurrency(args.concurrency);

    for params in param_sets {
        builder = builder.with_params(params);
    }

    // Set up progress bar styling if interactive
    let (progress_span, builder) = Cli::setup_progress_bar(interactive, builder, |b, callback| {
        b.on_query_complete(callback)
    })?;

    let provider = Arc::new(builder.build().await?);

    let partition_cols = output.partition_by.clone().unwrap_or(partition_keys);

    // Set progress bar length after we know the count
    if interactive {
        tracing::Span::current().pb_set_length(num_ranges as u64);
    }

    write_hive_partitioned_parquet(ctx, provider, &output.output, partition_cols).await?;

    if let Some(span) = &progress_span {
        // Force final progress bar update to 100%
        span.pb_set_position(num_ranges as u64);
        span.pb_tick();

        // Give indicatif time to render the update
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    tracing::info!("Token range scan completed successfully");
    Ok(())
}

fn setup_tracing(interactive: bool) {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if interactive {
        let indicatif_layer = tracing_indicatif::IndicatifLayer::new();
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
            .with(indicatif_layer)
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }
}

#[tracing::instrument(skip_all, name = "export")]
async fn run_from_query(
    _interactive: bool,
    output: &OutputArgs,
    args: FromQueryArgs,
    session: Arc<scylla::client::session::Session>,
    ctx: &SessionContext,
) -> anyhow::Result<()> {
    tracing::info!(query = %args.query, output = %output.output, "Starting from-query");

    // Parse parameter sets from either --params or --params-file
    let raw_param_sets = if let Some(params_file) = args.params_file {
        tracing::debug!(file = %params_file.display(), "Loading parameters from file");
        params::parse_params_from_file(&params_file)?
    } else if let Some(params) = args.params {
        params::parse_params_from_args(Some(params))
    } else {
        vec![] // No parameters
    };

    // Prepare the statement first to get metadata for parameter conversion
    let prepared = session
        .prepare(args.query.as_str())
        .await
        .context("Failed to prepare CQL query")?;

    // Convert parameter sets if provided
    let param_sets = if !raw_param_sets.is_empty() {
        tracing::debug!(count = raw_param_sets.len(), "Converting parameter sets");
        params::convert_all_param_sets(raw_param_sets, &prepared)?
    } else {
        vec![]
    };

    // Build provider with converted parameters
    let mut builder = ScyllaProvider::builder(session, args.query);
    if !param_sets.is_empty() {
        for params in param_sets {
            builder = builder.with_params(params);
        }
    }
    if let Some(max_concurrency) = args.max_concurrency {
        builder = builder.with_max_concurrency(max_concurrency);
    }

    // Set up progress bar styling if interactive
    let (progress_span, builder) =
        Cli::setup_progress_bar(_interactive, builder, |b, callback| {
            b.on_query_complete(callback)
        })?;

    let provider = Arc::new(builder.build().await?);

    let partition_cols = output.partition_by.clone().unwrap_or_default();
    let num_queries = provider.num_queries();

    // Set progress bar length after we know the count
    if _interactive {
        tracing::Span::current().pb_set_length(num_queries as u64);
    }

    write_hive_partitioned_parquet(ctx, provider, &output.output, partition_cols).await?;

    if let Some(span) = &progress_span {
        // Force final progress bar update to 100%
        span.pb_set_position(num_queries as u64);
        span.pb_tick();

        // Give indicatif time to render the update
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    tracing::info!("From-query completed successfully");
    Ok(())
}

/// Normalizes an output path to a proper URL.
/// If the path doesn't have a scheme, treats it as a file path and converts to absolute file:// URL.
fn normalize_output_path(output: &str) -> anyhow::Result<String> {
    // Try parsing as URL first
    match Url::parse(output) {
        Ok(url) => {
            // Valid URL with scheme
            Ok(url.to_string())
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            // No scheme, treat as file path
            let path = std::path::Path::new(output);
            let absolute_path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            };

            // Convert to file:// URL
            let file_url = Url::from_file_path(&absolute_path).map_err(|_| {
                anyhow::anyhow!(
                    "Failed to convert path to file:// URL: {}",
                    absolute_path.display()
                )
            })?;

            Ok(file_url.to_string())
        }
        Err(e) => {
            // Other parse errors
            Err(anyhow::anyhow!("Invalid output path: {}", e))
        }
    }
}

fn register_object_store(output: &str, ctx: &SessionContext) -> anyhow::Result<()> {
    let url = Url::parse(output)?;
    match url.scheme() {
        "s3" => {
            let bucket = url
                .host_str()
                .context("s3:// URL must have a bucket name")?;
            let s3 = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()?;
            let store_url = Url::parse(&format!("s3://{bucket}"))?;
            ctx.runtime_env()
                .register_object_store(&store_url, Arc::new(s3));
        }
        "file" | "" => { /* local filesystem — default store works */ }
        scheme => anyhow::bail!("Unsupported output scheme: {scheme}"),
    }
    Ok(())
}

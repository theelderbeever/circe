use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use datafusion::object_store::aws::AmazonS3Builder;
use datafusion::prelude::SessionContext;
use indicatif::ProgressStyle;
use scylla::client::session_builder::SessionBuilder;

use scylla::frame::Compression;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

use circe_providers::writer::write_hive_partitioned_parquet;
use circe_providers::{ScyllaFromQueryProvider, ScyllaTokenRangeProvider};

mod params;

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

    /// Max concurrent queries to Scylla (overrides --node-count / -k calculation)
    #[arg(short = 'C', long)]
    pub max_concurrency: Option<usize>,

    /// Number of nodes in the ScyllaDB cluster. Max concurrency is calculated based on node count and cores per node.
    #[arg(short = 'N', long, default_value = "1")]
    pub node_count: usize,

    /// CPU cores per node. Max concurrency is calculated based on node count and cores per node.
    #[arg(short = 'k', long, default_value_t = num_cpus::get())]
    pub cores_per_node: usize,
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

    /// Spawns a background task to monitor and display throughput metrics.
    /// Returns a span for the throughput indicator and a task handle.
    fn spawn_throughput_monitor(
        row_counter: Arc<AtomicU64>,
    ) -> (tracing::Span, tokio::task::JoinHandle<()>) {
        let throughput_span = tracing::info_span!(parent: &tracing::Span::current(), "throughput");
        throughput_span.pb_set_style(
            &ProgressStyle::with_template("Throughput: {msg} rows/sec (total: {human_pos} rows)")
                .expect("valid progress bar template"),
        );

        // Initialize the display
        throughput_span.pb_set_message("0");
        throughput_span.pb_set_position(0);

        let span_clone = throughput_span.clone();
        let handle = tokio::spawn(async move {
            let _entered = span_clone.enter();
            let mut last_count = 0u64;
            let mut last_instant = tokio::time::Instant::now();

            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;

                let current_count = row_counter.load(Ordering::Relaxed);
                let now = tokio::time::Instant::now();
                let elapsed = now.duration_since(last_instant).as_secs_f64();

                if elapsed > 0.0 {
                    let delta = current_count.saturating_sub(last_count);
                    let rate = delta as f64 / elapsed;

                    span_clone.pb_set_position(current_count);
                    span_clone.pb_set_message(&format!("{:.0}", rate));
                    span_clone.pb_tick();
                }

                last_count = current_count;
                last_instant = now;
            }
        });

        (throughput_span, handle)
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

    let mut builder =
        ScyllaTokenRangeProvider::builder(session, keyspace.to_owned(), table.to_owned())
            .partition_key_columns(args.partition_keys)
            .columns(args.columns);

    if let Some(c) = args.max_concurrency {
        builder = builder.concurrency(c);
    } else {
        builder = builder
            .nodes(args.node_count)
            .cores_per_node(args.cores_per_node);
    }

    // Set up progress bar styling if interactive
    let (progress_span, builder) = Cli::setup_progress_bar(interactive, builder, |b, callback| {
        b.on_range_complete(move |idx| callback(idx))
    })?;

    let provider = Arc::new(builder.build().await?);

    let partition_cols = output.partition_by.clone().unwrap_or(partition_keys);
    let num_ranges = provider.num_ranges();

    // Spawn throughput monitor FIRST so it appears above the progress bar
    let (throughput_span, throughput_handle) = if interactive {
        let row_counter = provider.row_counter();
        let (span, handle) = Cli::spawn_throughput_monitor(row_counter);
        (Some(span), Some(handle))
    } else {
        (None, None)
    };

    // Set progress bar length after we know the count
    if interactive {
        tracing::Span::current().pb_set_length(num_ranges as u64);
    }

    write_hive_partitioned_parquet(ctx, provider, &output.output, partition_cols).await?;

    // Stop throughput monitor
    if let Some(handle) = throughput_handle {
        handle.abort();
    }

    // Keep throughput_span alive until here
    drop(throughput_span);

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
    let mut builder = ScyllaFromQueryProvider::builder(session, args.query);
    if !param_sets.is_empty() {
        builder = builder.param_sets(param_sets);
    }
    if let Some(max_concurrency) = args.max_concurrency {
        builder = builder.max_concurrency(max_concurrency);
    }

    // Set up progress bar styling if interactive
    let (progress_span, builder) =
        Cli::setup_progress_bar(_interactive, builder, |b, callback| {
            b.on_query_complete(callback)
        })?;

    let provider = Arc::new(builder.build().await?);

    let partition_cols = output.partition_by.clone().unwrap_or_default();
    let num_queries = provider.num_queries();

    // Spawn throughput monitor FIRST so it appears above the progress bar
    let (throughput_span, throughput_handle) = if _interactive {
        let row_counter = provider.row_counter();
        let (span, handle) = Cli::spawn_throughput_monitor(row_counter);
        (Some(span), Some(handle))
    } else {
        (None, None)
    };

    // Set progress bar length after we know the count
    if _interactive {
        tracing::Span::current().pb_set_length(num_queries as u64);
    }

    write_hive_partitioned_parquet(ctx, provider, &output.output, partition_cols).await?;

    // Stop throughput monitor
    if let Some(handle) = throughput_handle {
        handle.abort();
    }

    // Keep throughput_span alive until here
    drop(throughput_span);

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

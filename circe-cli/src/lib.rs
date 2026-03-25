use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use datafusion::{object_store::aws::AmazonS3Builder, prelude::SessionContext};
use indicatif::ProgressStyle;
use itertools::Itertools;
use scylla::{client::session_builder::SessionBuilder, frame::Compression, value::CqlValue};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

use circe_providers::ScyllaProvider;

use crate::writer::write;

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
        env = "CIRCE_KNOWN_NODES",
        default_value = "127.0.0.1:9042",
        value_delimiter = ','
    )]
    pub known_nodes: Vec<String>,

    /// ScyllaDB username
    #[arg(short = 'U', long, env = "CIRCE_USER")]
    pub user: Option<String>,

    /// ScyllaDB password
    #[arg(short = 'P', long, env = "CIRCE_PASSWORD")]
    pub password: Option<String>,
}

#[derive(Debug, Clone, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Parquet,
    Csv,
    #[value(name = "ndjson", alias = "jsonl")]
    Ndjson,
}

#[derive(Debug, Args)]
pub struct OutputArgs {
    /// Output URL (file:///path or s3://bucket/prefix).
    #[arg(
        short,
        long,
        env = "CIRCE_OUTPUT",
        long_help = "Output URL (file:///path or s3://bucket/prefix).\nS3 configuration use the environment (https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.from_env)"
    )]
    pub output: String,

    /// Output format
    #[arg(short = 'F', long, default_value = "parquet")]
    pub format: OutputFormat,

    /// Write output to a single file instead of multiple part files
    #[arg(short, long, conflicts_with = "partition_by")]
    pub single_file: bool,

    /// Hive partition columns (comma-separated)
    #[arg(long, value_delimiter = ',', conflicts_with = "single_file")]
    pub partition_by: Option<Vec<String>>,

    /// Optional DataFusion SQL to run on the fetched data before writing.
    /// Reference the source data as "source".
    /// Example: "SELECT region, COUNT(*) as cnt FROM source GROUP BY region"
    #[arg(short = 'T', long, global = true)]
    pub transform: Option<String>,
}

impl OutputArgs {
    /// Returns the effective output format, preferring a recognized file extension
    /// over the `--format` flag.
    pub fn output_format(&self) -> anyhow::Result<OutputFormat> {
        let path = self.output.trim_end_matches('/');
        match std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
        {
            Some("parquet") => Ok(OutputFormat::Parquet),
            Some("csv") => Ok(OutputFormat::Csv),
            Some("ndjson" | "jsonl" | "json") => Ok(OutputFormat::Ndjson),
            None => Ok(self.format.clone()),
            Some(ext) => anyhow::bail!("Unrecognized output extension: .{ext}"),
        }
    }

    /// Normalizes an output path to a proper URL.
    /// If the path doesn't have a scheme, treats it as a file path and converts to absolute file:// URL.
    fn normalize_output_path(&self) -> anyhow::Result<String> {
        // Try parsing as URL first
        match Url::parse(&self.output) {
            Ok(url) => {
                // Valid URL with scheme
                Ok(url.to_string())
            }
            Err(url::ParseError::RelativeUrlWithoutBase) => {
                // No scheme, treat as file path
                let path = std::path::Path::new(&self.output);
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
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Full table scan using token range queries
    TokenRange(TokenRangeArgs),

    /// Execute a custom query (not yet implemented)
    Query(QueryArgs),
}

#[derive(Debug, Args)]
pub struct TokenRangeArgs {
    /// Fully qualified table name in the form {keyspace}.{table}
    #[arg()]
    pub table: String,

    /// Partition key columns for token range scanning (comma-separated). Must be in order of table partition key.
    #[arg(short = 'K', long, value_delimiter = ',', required = true)]
    pub partition_keys: Vec<String>,

    /// Columns to select (comma-separated, defaults to all)
    #[arg(short, long, value_delimiter = ',', default_value = "*")]
    pub columns: Vec<String>,

    /// Number of token range splits and max concurrent queries (defaults to 2 * num_cpus)
    #[arg(short = 'C', long, default_value_t = num_cpus::get() * 2)]
    pub concurrency: usize,
}

impl TokenRangeArgs {
    pub fn prepare(&self) -> anyhow::Result<(String, Vec<HashMap<String, CqlValue>>)> {
        let (keyspace, table) = self
            .table
            .split_once('.')
            .context("--table must be in the form {keyspace}.{table}")?;

        let pk_cols = self.partition_keys.join(", ");
        let selection = self.columns.join(", ");
        let query = format!(
            "SELECT {selection} FROM {keyspace}.{table} WHERE TOKEN({pk_cols}) >= :token_start AND TOKEN({pk_cols}) < :token_end BYPASS CACHE"
        );

        let params = self
            .token_ranges()
            .map(|(start, end, ..)| {
                HashMap::from([
                    ("token_start".to_string(), CqlValue::BigInt(start)),
                    ("token_end".to_string(), CqlValue::BigInt(end)),
                ])
            })
            .collect();

        Ok((query, params))
    }

    fn token_ranges(&self) -> impl Iterator<Item = (i64, i64, usize)> {
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
pub struct QueryArgs {
    /// The CQL query to execute
    pub query: String,

    /// NDJson file containing an object per line for params.
    /// Only named params are supported. ie: value = :name
    /// Enables concurrent execution with many parameter sets.
    #[arg(long)]
    pub params: Option<PathBuf>,

    /// Max concurrent queries to Scylla (defaults to num_cpus * 2)
    #[arg(short = 'C', long, default_value_t = num_cpus::get() * 2)]
    pub max_concurrency: usize,
}

impl QueryArgs {
    pub async fn prepare(
        &self,
        session: &scylla::client::session::Session,
    ) -> anyhow::Result<(String, Vec<HashMap<String, CqlValue>>)> {
        let params = if let Some(params_file) = &self.params {
            let prepared = session
                .prepare(self.query.as_str())
                .await
                .context("Failed to prepare CQL query")?;
            let raw = params::parse_params_from_file(params_file)?;
            tracing::debug!(count = raw.len(), "Converting parameter sets");
            params::convert_param_sets(raw, &prepared)?
        } else {
            vec![]
        };

        Ok((self.query.clone(), params))
    }
}

impl Cli {
    #[tracing::instrument]
    pub async fn execute(&self) -> anyhow::Result<()> {
        // --- Tracing setup ---
        setup_tracing(self.interactive);

        // --- ScyllaDB session ---
        let mut session_builder = SessionBuilder::new();
        for node in &self.scylla.known_nodes {
            session_builder = session_builder
                .known_node(node)
                .compression(Some(Compression::Lz4));
        }
        if let (Some(user), Some(password)) = (&self.scylla.user, &self.scylla.password) {
            session_builder = session_builder.user(user, password);
        }
        let session = Arc::new(session_builder.build().await?);

        // --- SessionContext + object store ---
        let ctx = SessionContext::new();
        register_object_store(&self.output.normalize_output_path()?, &ctx)?;

        match &self.command {
            Command::TokenRange(args) => {
                let (query, params) = args.prepare()?;
                tracing::debug!("{query}");
                self.execute_query(ctx, session, query, params, args.concurrency)
                    .await?;
            }
            Command::Query(args) => {
                let (query, params) = args.prepare(&session).await?;
                tracing::debug!("{query}");
                self.execute_query(ctx, session, query, params, args.max_concurrency)
                    .await?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn execute_query(
        &self,
        ctx: SessionContext,
        session: Arc<scylla::client::session::Session>,
        query: String,
        params: Vec<HashMap<String, CqlValue>>,
        max_concurrency: usize,
    ) -> anyhow::Result<()> {
        let mut builder =
            ScyllaProvider::builder(session, query).with_max_concurrency(max_concurrency);
        for param_set in params {
            builder = builder.with_params(param_set);
        }

        let (progress_span, builder) = if self.interactive {
            let current_span = tracing::Span::current();
            current_span.pb_set_style(
                &ProgressStyle::with_template(
                    "[{elapsed_precise}] {bar:100.cyan/blue} {pos:>5}/{len:5} {per_sec:.1} [{eta_precise}]",
                )
                .context("invalid progress bar template")?,
            );
            let span_clone = current_span.clone();
            let callback = Arc::new(move |_: usize| span_clone.pb_inc(1));
            (Some(current_span), builder.on_query_complete(callback))
        } else {
            (None, builder)
        };

        let provider = Arc::new(builder.build().await?);
        let num_queries = provider.num_queries() as u64;

        if let Some(span) = &progress_span {
            span.pb_set_length(num_queries);
        }

        write(&ctx, provider, &self.output).await?;

        if let Some(span) = &progress_span {
            span.pb_set_position(num_queries);
            span.pb_tick();
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(())
    }
}

fn setup_tracing(interactive: bool) {
    use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

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

use std::sync::Arc;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use datafusion::object_store::aws::AmazonS3Builder;
use datafusion::prelude::SessionContext;
use indicatif::ProgressStyle;
use scylla::client::session_builder::SessionBuilder;
use tracing::info_span;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use url::Url;

use circe_providers::token_range::ScyllaTokenRangeProvider;
use circe_providers::writer::write_hive_partitioned_parquet;

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
    #[arg(long, env = "CIRCE_OUTPUT")]
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

    /// Max concurrent queries (overrides --node-count / -k calculation)
    #[arg(short = 'C', long)]
    pub concurrency: Option<usize>,

    /// Number of nodes in the ScyllaDB cluster. Concurrency is calculated based on node count and cores per node.
    #[arg(short = 'N', long, default_value = "1")]
    pub node_count: usize,

    /// CPU cores per node. Concurrency is calculated based on node count and cores per node.
    #[arg(short = 'k', long, default_value_t = num_cpus::get())]
    pub cores_per_node: usize,
}

#[derive(Debug, Args)]
pub struct FromQueryArgs {
    /// The CQL query to execute
    pub query: String,
}

impl Cli {
    pub async fn execute(self) -> anyhow::Result<()> {
        // --- Tracing setup ---
        setup_tracing(self.interactive);

        // --- ScyllaDB session ---
        let mut session_builder = SessionBuilder::new();
        for node in &self.scylla.known_nodes {
            session_builder = session_builder.known_node(node);
        }
        if let (Some(user), Some(password)) =
            (&self.scylla.scylla_user, &self.scylla.scylla_password)
        {
            session_builder = session_builder.user(user, password);
        }
        let session = Arc::new(session_builder.build().await?);

        // --- SessionContext + object store ---
        let ctx = SessionContext::new();
        register_object_store(&self.output.output, &ctx)?;

        // --- Dispatch command ---
        let interactive = self.interactive;
        let output = self.output;

        match self.command {
            Command::TokenRange(args) => {
                run_token_range(interactive, &output, args, session, &ctx).await?;
            }
            Command::FromQuery(_) => {
                anyhow::bail!("from-query is not yet implemented");
            }
        }

        Ok(())
    }
}

async fn run_token_range(
    interactive: bool,
    output: &OutputArgs,
    args: TokenRangeArgs,
    session: Arc<scylla::client::session::Session>,
    ctx: &SessionContext,
) -> anyhow::Result<()> {
    let partition_keys = args.partition_keys.clone();

    let (keyspace, table) = args
        .table
        .split_once('.')
        .context("--table must be in the form {keyspace}.{table}")?;

    let mut builder =
        ScyllaTokenRangeProvider::builder(session, keyspace.to_owned(), table.to_owned())
            .partition_key_columns(args.partition_keys)
            .columns(args.columns);

    if let Some(c) = args.concurrency {
        builder = builder.concurrency(c);
    } else {
        builder = builder
            .nodes(args.node_count)
            .cores_per_node(args.cores_per_node);
    }

    if interactive {
        builder = builder.on_range_complete(move |_| {
            tracing::Span::current().pb_inc(1);
        });
    }

    let provider = Arc::new(builder.build().await?);

    let partition_cols = output.partition_by.clone().unwrap_or(partition_keys);

    // Set up progress bar if interactive
    let _guard = if interactive {
        let progress_span = info_span!("export");
        progress_span.pb_set_style(
            &ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:100.cyan/blue} {pos:>5}/{len:5} {per_sec}",
            )
            .context("invalid progress bar template")?,
        );
        progress_span.pb_set_length(provider.num_ranges() as u64);
        Some(progress_span.entered())
    } else {
        None
    };

    write_hive_partitioned_parquet(ctx, provider, &output.output, partition_cols).await?;

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

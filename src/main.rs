use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    circe_cli::Cli::parse().execute().await
}

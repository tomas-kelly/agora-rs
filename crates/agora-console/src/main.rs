use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "agora-console", about = "TUI for the agora swarm")]
struct Cli {
    #[command(flatten)]
    args: agora_console::ConsoleArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    agora_console::run(Cli::parse().args).await
}

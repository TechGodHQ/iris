//! Iris CLI — command-line interface for the unified messaging system.

mod commands;
mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../generated/cli.rs"
    ));
}

use clap::{Parser, Subcommand};

mod watch;

#[derive(Parser)]
#[command(name = "iris")]
#[command(version, about = "LLM-first, source-agnostic messaging system")]
#[command(
    long_about = "Iris normalizes messages from multiple sources (Telegram, SMS, Email, etc.) into a unified API."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a generated API operation.
    #[command(flatten)]
    Generated(generated::GeneratedCommand),
    /// List registered providers.
    Providers,
    /// Serve the HTTP API.
    Serve(commands::ServeArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Generated(command) => commands::execute_generated(command).await,
        Commands::Providers => commands::list_providers(),
        Commands::Serve(args) => commands::serve(args).await,
    }
}

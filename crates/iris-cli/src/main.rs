//! Iris CLI — command-line interface for the unified messaging system.
//!
//! Subcommands are auto-generated from the core API definition by
//! iris-codegen in a future iteration. For now, hand-wired.

mod commands;

use clap::{Parser, Subcommand};

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
    /// List threads across all providers.
    Threads(commands::ListArgs),
    /// List messages in a thread.
    Messages(commands::MessagesArgs),
    /// List contacts.
    Contacts(commands::ListArgs),
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
        Commands::Threads(args) => commands::list_threads(args).await,
        Commands::Messages(args) => commands::list_messages(args).await,
        Commands::Contacts(args) => commands::list_contacts(args).await,
        Commands::Providers => {
            commands::list_providers();
            Ok(())
        }
        Commands::Serve(args) => commands::serve(args).await,
    }
}

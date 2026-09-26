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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, error::ErrorKind};

    #[test]
    fn generated_watch_preserves_cursor_and_keeps_output_flag_out_of_request() {
        let cli = Cli::try_parse_from(["iris", "watch", "--cursor", "inc.7"]).unwrap();
        let Commands::Generated(generated::GeneratedCommand::Watch(args)) = cli.command else {
            panic!("expected generated watch command");
        };
        assert_eq!(args.cursor.as_deref(), Some("inc.7"));
        assert!(!args.include_cursor);

        let command = generated::GeneratedCommand::Watch(args);
        let parameters = command.parameters_json();
        assert_eq!(parameters["cursor"], "inc.7");
        assert!(
            parameters.get("include_cursor").is_none(),
            "CLI-only output flags must not enter the request schema"
        );

        let cli = Cli::try_parse_from(["iris", "watch", "--include-cursor"]).unwrap();
        let Commands::Generated(generated::GeneratedCommand::Watch(args)) = cli.command else {
            panic!("expected generated watch command");
        };
        assert!(args.include_cursor);
        assert_eq!(args.cursor, None);
    }

    #[test]
    fn generated_watch_rejects_a_value_for_boolean_output_flag() {
        let Err(error) = Cli::try_parse_from(["iris", "watch", "--include-cursor=true"]) else {
            panic!("set-true flags reject supplied values");
        };
        assert_eq!(error.kind(), ErrorKind::TooManyValues);
    }

    #[test]
    fn generated_watch_help_describes_replay_options() {
        let Err(error) = Cli::try_parse_from(["iris", "watch", "--help"]) else {
            panic!("--help should render clap help");
        };
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        let help = error.to_string();
        assert!(help.contains("--cursor <CURSOR>"), "help: {help}");
        assert!(help.contains("--include-cursor"), "help: {help}");
        assert!(
            help.contains("Include the opaque replay cursor alongside each streamed message"),
            "help: {help}"
        );
    }
}

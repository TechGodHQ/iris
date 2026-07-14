//! Command-line entrypoint for committed Iris code generation.

use anyhow::{Context, Result};
use iris_codegen::{
    API_DEFINITION_PATH, GENERATED_DIR, generate_all, load_api_definition, verify_generated,
    write_generated,
};

fn main() -> Result<()> {
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "check".to_string());
    match command.as_str() {
        "write" => {
            let definition = load_api_definition(API_DEFINITION_PATH)?;
            let artifacts = generate_all(&definition);
            write_generated(GENERATED_DIR, &artifacts)?;
        }
        "check" => {
            verify_generated(API_DEFINITION_PATH, GENERATED_DIR)
                .context("generated artifacts are stale")?;
        }
        other => anyhow::bail!("unknown command {other:?}; expected `check` or `write`"),
    }
    Ok(())
}

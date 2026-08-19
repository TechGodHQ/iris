//! Command-line entrypoint for committed Iris code generation.

use anyhow::{Context, Result};
use iris_codegen::{
    generate_all, iris_generate_config, load_api_definition, verify_generated, write_generated,
};
// Hydra defaults: definition at api/operations.yaml, artifacts in generated/.
const API_DEFINITION_PATH: &str = hydra_core::DEFAULT_DEFINITION_PATH;
const GENERATED_DIR: &str = hydra_core::DEFAULT_GENERATED_DIR;

fn main() -> Result<()> {
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "check".to_string());
    let config = iris_generate_config();
    match command.as_str() {
        "write" => {
            let definition = load_api_definition(API_DEFINITION_PATH)?;
            let artifacts = generate_all(&definition, &config);
            write_generated(GENERATED_DIR, &artifacts)?;
        }
        "check" => {
            verify_generated(API_DEFINITION_PATH, GENERATED_DIR, &config)
                .context("generated artifacts are stale")?;
        }
        other => anyhow::bail!("unknown command {other:?}; expected `check` or `write`"),
    }
    Ok(())
}

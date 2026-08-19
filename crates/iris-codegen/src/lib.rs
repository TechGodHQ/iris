//! Iris code generation — now a thin configuration layer over
//! [hydra](https://github.com/TechGodHQ/hydra), `TechGodHQ`'s shared
//! projection layer.
//!
//! Iris keeps its `api/operations.yaml` contract and its committed
//! `generated/` artifacts. The definition model, validation, and surface
//! emitters moved to hydra after the server-less spike
//! (`docs/spikes/server-less.md`); this crate pins iris's per-project
//! generation knobs and re-exports the hydra API so existing imports keep
//! working.

pub use hydra_codegen::{
    GenerateConfig, GeneratedArtifacts, generate_all, verify_generated, write_generated,
};
pub use hydra_core::{
    ApiDefinition, Delivery, HttpMethod, Operation, Parameter, ParameterLocation, Surface,
    load_api_definition,
};

/// Hydra generation knobs for iris: generated HTTP handlers dispatch to the
/// server's shared operation executor with iris's application state.
#[must_use]
pub fn iris_generate_config() -> GenerateConfig {
    GenerateConfig {
        http_dispatch_fn: "super::execute_generated_operation".to_string(),
        http_state_type: "crate::app::AppState".to_string(),
        sse_binding_prefix: "super::".to_string(),
        generator_name: "hydra (iris)".to_string(),
    }
}

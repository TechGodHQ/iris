//! Iris code generator.
//!
//! Reads a core API definition (the single source of truth for operations)
//! and generates the client-facing surfaces:
//!
//! - CLI subcommands (clap structs)
//! - HTTP route handlers (axum)
//! - MCP tool definitions
//!
//! This ensures all three surfaces stay in sync. Adding a new operation
//! to the API definition automatically generates the corresponding code
//! in all three targets.

// Placeholder — code generation logic will be implemented following
// the first OpenSpec change that exercises this.

/// Path to the API operations definition file (single source of truth).
pub const API_DEFINITION_PATH: &str = "api/operations.yaml";

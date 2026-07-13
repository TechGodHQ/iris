//! Iris MCP server — exposes Iris operations as MCP tools.
//!
//! Tool definitions are auto-generated from the core API definition by
//! iris-codegen. The MCP surface will be kept in sync with CLI and HTTP
//! routes automatically.

// This crate is a placeholder — MCP protocol implementation will follow
// the core API definition stabilization.

/// Constant identifying the MCP server name.
pub const SERVER_NAME: &str = "iris";
/// Constant identifying the MCP server version.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

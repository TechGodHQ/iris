//! Iris MCP server — exposes Iris operations as MCP tools.
//!
//! Tool definitions are generated from the core API definition by iris-codegen.

/// Constant identifying the MCP server name.
pub const SERVER_NAME: &str = "iris";
/// Constant identifying the MCP server version.
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Generated MCP tool definitions as JSON.
pub const GENERATED_TOOLS_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../generated/mcp.json"
));

/// Return generated MCP tool definitions.
pub fn generated_tools() -> serde_json::Result<serde_json::Value> {
    serde_json::from_str(GENERATED_TOOLS_JSON)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_tools_include_core_operations() {
        let tools = generated_tools().expect("generated tools parse");
        let names: Vec<_> = tools["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name"))
            .collect();

        assert!(names.contains(&"list_messages"));
        assert!(names.contains(&"send_message"));
    }
}

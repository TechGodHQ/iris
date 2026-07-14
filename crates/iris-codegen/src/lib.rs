//! Iris code generator.
//!
//! Reads a core API definition (the single source of truth for operations)
//! and generates the client-facing surfaces:
//!
//! - CLI subcommands (clap structs)
//! - HTTP route metadata (axum route wiring source)
//! - MCP tool schemas
//!
//! This ensures all three surfaces stay in sync. Adding a new operation
//! to the API definition automatically generates the corresponding code
//! in all three targets.

use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Path to the API operations definition file (single source of truth).
pub const API_DEFINITION_PATH: &str = "api/operations.yaml";

/// Directory containing committed generated artifacts.
pub const GENERATED_DIR: &str = "generated";

/// The API definition file root.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ApiDefinition {
    /// Operations exposed by Iris.
    pub operations: Vec<Operation>,
}

/// A single Iris operation exposed through CLI, HTTP, and MCP.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Operation {
    /// Stable `snake_case` operation name.
    pub name: String,
    /// Human-readable operation description.
    pub description: String,
    /// HTTP method used by the REST surface.
    pub method: HttpMethod,
    /// HTTP path used by the REST surface.
    pub path: String,
    /// Whether this is a read-only operation.
    pub read: bool,
    /// Rust-ish output type documentation for generated surfaces.
    pub output_type: String,
    /// Input parameters for the operation.
    #[serde(default)]
    pub parameters: Vec<Parameter>,
}

/// HTTP method for a generated REST operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    /// HTTP GET.
    Get,
    /// HTTP POST.
    Post,
}

/// A typed operation parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Parameter {
    /// Stable `snake_case` parameter name.
    pub name: String,
    /// Human-readable parameter description.
    pub description: String,
    /// Logical type name.
    #[serde(rename = "type")]
    pub ty: ParameterType,
    /// Whether callers must provide this parameter.
    pub required: bool,
    /// Where this parameter appears in HTTP requests.
    pub location: ParameterLocation,
}

/// Parameter type supported by the first-generation codegen contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterType {
    /// UTF-8 string.
    String,
    /// Unsigned 32-bit integer.
    U32,
    /// Boolean flag.
    Bool,
}

/// Parameter location for HTTP and generated surface mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterLocation {
    /// Path parameter.
    Path,
    /// Query-string parameter.
    Query,
    /// JSON body parameter.
    Body,
}

/// Load an API definition from YAML.
pub fn load_api_definition(path: impl AsRef<Path>) -> Result<ApiDefinition> {
    let path = path.as_ref();
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read API definition from {}", path.display()))?;
    let definition: ApiDefinition = serde_yaml::from_str(&raw)
        .with_context(|| format!("parse API definition from {}", path.display()))?;
    validate_definition(&definition)?;
    Ok(definition)
}

/// Validate semantic constraints that YAML parsing alone cannot enforce.
pub fn validate_definition(definition: &ApiDefinition) -> Result<()> {
    anyhow::ensure!(
        !definition.operations.is_empty(),
        "API definition must contain at least one operation"
    );

    let mut names = std::collections::BTreeSet::new();
    for operation in &definition.operations {
        anyhow::ensure!(
            is_valid_identifier(&operation.name),
            "operation name must be a Rust-safe snake_case identifier: {}",
            operation.name
        );
        anyhow::ensure!(
            names.insert(operation.name.as_str()),
            "duplicate operation name: {}",
            operation.name
        );
        anyhow::ensure!(
            !GENERATED_HTTP_RESERVED_NAMES.contains(&operation.name.as_str()),
            "operation name is reserved by generated HTTP module: {}",
            operation.name
        );
        anyhow::ensure!(
            operation.path.starts_with('/'),
            "operation {} path must start with /",
            operation.name
        );
        anyhow::ensure!(
            matches!(
                (operation.read, operation.method),
                (true, HttpMethod::Get) | (false, HttpMethod::Post)
            ),
            "operation {} read/method mismatch: reads must be GET, writes must be POST",
            operation.name
        );

        let mut parameter_names = std::collections::BTreeSet::new();
        for parameter in &operation.parameters {
            anyhow::ensure!(
                is_valid_identifier(&parameter.name),
                "parameter name must be a Rust-safe snake_case identifier: {}.{}",
                operation.name,
                parameter.name
            );
            anyhow::ensure!(
                parameter_names.insert(parameter.name.as_str()),
                "duplicate parameter name: {}.{}",
                operation.name,
                parameter.name
            );
        }

        let path_parameters = path_parameters(&operation.path)?;
        for path_parameter in &path_parameters {
            anyhow::ensure!(
                operation
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == *path_parameter
                        && parameter.location == ParameterLocation::Path),
                "operation {} path placeholder {{{}}} has no matching path parameter",
                operation.name,
                path_parameter
            );
        }
        for parameter in operation
            .parameters
            .iter()
            .filter(|parameter| parameter.location == ParameterLocation::Path)
        {
            anyhow::ensure!(
                path_parameters
                    .iter()
                    .any(|path_parameter| path_parameter == &parameter.name),
                "operation {} path parameter {} is not present in path {}",
                operation.name,
                parameter.name,
                operation.path
            );
        }
    }

    Ok(())
}

/// Generate all committed artifacts for a definition.
pub fn generate_all(definition: &ApiDefinition) -> GeneratedArtifacts {
    GeneratedArtifacts {
        cli_rs: generate_cli(definition),
        http_rs: generate_http(definition),
        mcp_json: generate_mcp(definition),
    }
}

/// Generated artifact bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedArtifacts {
    /// Generated Clap command metadata/structs.
    pub cli_rs: String,
    /// Generated Axum route metadata.
    pub http_rs: String,
    /// Generated MCP tool schema JSON.
    pub mcp_json: String,
}

/// Write generated artifacts under a directory.
pub fn write_generated(dir: impl AsRef<Path>, artifacts: &GeneratedArtifacts) -> Result<()> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    write_if_changed(dir.join("cli.rs"), &artifacts.cli_rs)?;
    write_if_changed(dir.join("http.rs"), &artifacts.http_rs)?;
    write_if_changed(dir.join("mcp.json"), &artifacts.mcp_json)?;
    Ok(())
}

/// Verify committed generated artifacts match the API definition.
pub fn verify_generated(
    definition_path: impl AsRef<Path>,
    generated_dir: impl AsRef<Path>,
) -> Result<()> {
    let definition = load_api_definition(definition_path)?;
    let expected = generate_all(&definition);
    let dir = generated_dir.as_ref();

    compare_file(dir.join("cli.rs"), &expected.cli_rs)?;
    compare_file(dir.join("http.rs"), &expected.http_rs)?;
    compare_file(dir.join("mcp.json"), &expected.mcp_json)?;
    Ok(())
}

fn generate_cli(definition: &ApiDefinition) -> String {
    let mut out = generated_header("CLI command structs generated from api/operations.yaml");
    out.push_str("use clap::{Args, Subcommand};\n");
    out.push_str("use serde::{Deserialize, Serialize};\n\n");
    out.push_str("#[derive(Debug, Clone, Subcommand)]\n");
    out.push_str("pub enum GeneratedCommand {\n");
    for operation in &definition.operations {
        push_doc_comment(&mut out, "    ", &operation.description);
        out.push_str("    ");
        out.push_str(&pascal_case(&operation.name));
        out.push('(');
        out.push_str(&pascal_case(&operation.name));
        out.push_str("Args),\n");
    }
    out.push_str("}\n\n");

    out.push_str("impl GeneratedCommand {\n");
    out.push_str("    pub const fn operation_name(&self) -> &'static str {\n");
    out.push_str("        match self {\n");
    for operation in &definition.operations {
        out.push_str("            Self::");
        out.push_str(&pascal_case(&operation.name));
        out.push_str("(_) => ");
        out.push_str(&rust_string_literal(&operation.name));
        out.push_str(",\n");
    }
    out.push_str("        }\n");
    out.push_str("    }\n\n");
    out.push_str("    pub fn parameters_json(&self) -> serde_json::Value {\n");
    out.push_str("        match self {\n");
    for operation in &definition.operations {
        out.push_str("            Self::");
        out.push_str(&pascal_case(&operation.name));
        out.push_str("(args) => serde_json::json!({");
        for (index, parameter) in operation.parameters.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(&rust_string_literal(&parameter.name));
            out.push_str(": args.");
            out.push_str(&parameter.name);
            if matches!(parameter.ty, ParameterType::String) || !parameter.required {
                out.push_str(".clone()");
            }
        }
        out.push_str("}),\n");
    }
    out.push_str("        }\n");
    out.push_str("    }\n");
    out.push_str("}\n\n");

    for operation in &definition.operations {
        out.push_str("#[derive(Debug, Clone, Serialize, Deserialize, Args)]\n");
        out.push_str("pub struct ");
        out.push_str(&pascal_case(&operation.name));
        out.push_str("Args {\n");
        for parameter in &operation.parameters {
            push_doc_comment(&mut out, "    ", &parameter.description);
            if parameter.location != ParameterLocation::Path {
                out.push_str("    #[arg(long)]\n");
            }
            out.push_str("    pub ");
            out.push_str(&parameter.name);
            out.push_str(": ");
            if parameter.required {
                out.push_str(parameter.rust_type());
            } else {
                out.push_str("Option<");
                out.push_str(parameter.rust_type());
                out.push('>');
            }
            out.push_str(",\n");
        }
        out.push_str("}\n\n");
    }

    out
}

fn generate_http(definition: &ApiDefinition) -> String {
    let mut out = generated_header("HTTP route handlers generated from api/operations.yaml");
    out.push_str("use std::collections::BTreeMap;\n\n");
    out.push_str("use axum::{extract::{Path, Query, State}, response::Response, routing::{get, post}, Json, Router};\n");
    out.push_str("use serde_json::Value;\n\n");
    out.push_str("use crate::app::AppState;\n\n");
    out.push_str("#[derive(Debug, Clone, Copy, PartialEq, Eq)]\n");
    out.push_str("pub struct GeneratedRoute {\n");
    out.push_str("    pub name: &'static str,\n");
    out.push_str("    pub method: &'static str,\n");
    out.push_str("    pub path: &'static str,\n");
    out.push_str("}\n\n");
    out.push_str("#[derive(Debug, Clone, Default, PartialEq, Eq)]\n");
    out.push_str("pub struct GeneratedOperationInput {\n");
    out.push_str("    pub path: BTreeMap<String, String>,\n");
    out.push_str("    pub query: BTreeMap<String, String>,\n");
    out.push_str("    pub body: Value,\n");
    out.push_str("}\n\n");
    out.push_str("pub const GENERATED_ROUTES: &[GeneratedRoute] = &[\n");
    for operation in &definition.operations {
        out.push_str("    GeneratedRoute { name: ");
        out.push_str(&rust_string_literal(&operation.name));
        out.push_str(", method: ");
        out.push_str(&rust_string_literal(operation.method.as_str()));
        out.push_str(", path: ");
        out.push_str(&rust_string_literal(&axum_path(&operation.path)));
        out.push_str(" },\n");
    }
    out.push_str("];\n\n");
    out.push_str("pub fn generated_router() -> Router<AppState> {\n");
    out.push_str("    Router::new()\n");
    for operation in &definition.operations {
        out.push_str("        .route(");
        out.push_str(&rust_string_literal(&axum_path(&operation.path)));
        out.push_str(", ");
        match operation.method {
            HttpMethod::Get => out.push_str("get("),
            HttpMethod::Post => out.push_str("post("),
        }
        out.push_str(&operation.name);
        out.push_str("))\n");
    }
    out.push_str("}\n\n");

    for (operation_index, operation) in definition.operations.iter().enumerate() {
        let has_path = operation
            .parameters
            .iter()
            .any(|parameter| parameter.location == ParameterLocation::Path);
        let has_query = operation
            .parameters
            .iter()
            .any(|parameter| parameter.location == ParameterLocation::Query);
        let has_body = operation
            .parameters
            .iter()
            .any(|parameter| parameter.location == ParameterLocation::Body);

        out.push_str("async fn ");
        out.push_str(&operation.name);
        out.push_str("(\n");
        out.push_str("    State(state): State<AppState>,\n");
        if has_path {
            out.push_str("    Path(path): Path<BTreeMap<String, String>>,\n");
        }
        if has_query {
            out.push_str("    Query(query): Query<BTreeMap<String, String>>,\n");
        }
        if has_body {
            out.push_str("    Json(body): Json<Value>,\n");
        }
        out.push_str(") -> Response {\n");
        out.push_str("    super::execute_generated_operation(\n");
        out.push_str("        &state,\n");
        out.push_str("        ");
        out.push_str(&rust_string_literal(&operation.name));
        out.push_str(",\n");
        out.push_str("        GeneratedOperationInput {\n");
        if has_path {
            out.push_str("            path,\n");
        } else {
            out.push_str("            path: BTreeMap::new(),\n");
        }
        if has_query {
            out.push_str("            query,\n");
        } else {
            out.push_str("            query: BTreeMap::new(),\n");
        }
        if has_body {
            out.push_str("            body,\n");
        } else {
            out.push_str("            body: Value::Null,\n");
        }
        out.push_str("        },\n");
        out.push_str("    )\n");
        out.push_str("    .await\n");
        out.push_str("}\n");
        if operation_index + 1 < definition.operations.len() {
            out.push('\n');
        }
    }

    out
}

fn generate_mcp(definition: &ApiDefinition) -> String {
    let tools: Vec<_> = definition
        .operations
        .iter()
        .map(|operation| {
            let mut required = Vec::new();
            let mut properties = serde_json::Map::new();
            for parameter in &operation.parameters {
                if parameter.required {
                    required.push(parameter.name.clone());
                }
                properties.insert(
                    parameter.name.clone(),
                    json!({
                        "type": parameter.json_schema_type(),
                        "description": parameter.description,
                    }),
                );
            }
            json!({
                "name": operation.name,
                "description": operation.description,
                "inputSchema": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                    "additionalProperties": false,
                },
            })
        })
        .collect();

    let value = json!({
        "generated_from": API_DEFINITION_PATH,
        "tools": tools,
    });
    let mut out = serde_json::to_string_pretty(&value).expect("serializing MCP JSON cannot fail");
    out.push('\n');
    out
}

fn generated_header(purpose: &str) -> String {
    format!("// Code generated by iris-codegen. DO NOT EDIT.\n// {purpose}\n\n")
}

fn push_doc_comment(out: &mut String, indent: &str, text: &str) {
    for line in text.lines() {
        out.push_str(indent);
        out.push_str("/// ");
        out.push_str(line.trim());
        out.push('\n');
    }
}

fn write_if_changed(path: impl AsRef<Path>, content: &str) -> Result<()> {
    let path = path.as_ref();
    if fs::read_to_string(path).is_ok_and(|current| current == content) {
        return Ok(());
    }
    fs::write(path, content).with_context(|| format!("write {}", path.display()))
}

fn compare_file(path: impl AsRef<Path>, expected: &str) -> Result<()> {
    let path = path.as_ref();
    let actual = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    anyhow::ensure!(
        actual == expected,
        "generated artifact is stale: {} (run `cargo run -p iris-codegen --bin iris-codegen -- write`)",
        path.display()
    );
    Ok(())
}

fn rust_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("string literal serialization cannot fail")
}

fn axum_path(path: &str) -> String {
    path.to_string()
}

fn path_parameters(path: &str) -> Result<Vec<String>> {
    let mut parameters = Vec::new();
    let mut chars = path.char_indices();
    while let Some((_, ch)) = chars.next() {
        if ch != '{' {
            anyhow::ensure!(ch != '}', "unmatched closing brace in path: {path}");
            continue;
        }

        let mut name = String::new();
        let mut closed = false;
        for (_, inner) in chars.by_ref() {
            if inner == '}' {
                closed = true;
                break;
            }
            anyhow::ensure!(inner != '{', "nested opening brace in path: {path}");
            name.push(inner);
        }
        anyhow::ensure!(closed, "unmatched opening brace in path: {path}");
        anyhow::ensure!(!name.is_empty(), "empty path parameter in path: {path}");
        anyhow::ensure!(
            is_valid_identifier(&name),
            "invalid path parameter {{{name}}} in path: {path}"
        );
        parameters.push(name);
    }
    Ok(parameters)
}

fn is_valid_identifier(value: &str) -> bool {
    is_snake_case(value) && !value.as_bytes()[0].is_ascii_digit() && !RUST_KEYWORDS.contains(&value)
}

const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for",
    "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return",
    "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe", "use", "where",
    "while", "async", "await", "dyn",
];

const GENERATED_HTTP_RESERVED_NAMES: &[&str] = &[
    "generated_router",
    "generated_route",
    "generated_operation_input",
    "state",
    "path",
    "query",
    "body",
    "get",
    "post",
    "router",
    "value",
    "response",
];

fn is_snake_case(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !value.starts_with('_')
        && !value.ends_with('_')
        && !value.contains("__")
}

fn pascal_case(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect::<String>()
            })
        })
        .collect()
}

impl HttpMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

impl Parameter {
    const fn rust_type(&self) -> &'static str {
        match self.ty {
            ParameterType::String => "String",
            ParameterType::U32 => "u32",
            ParameterType::Bool => "bool",
        }
    }

    const fn json_schema_type(&self) -> &'static str {
        match self.ty {
            ParameterType::String => "string",
            ParameterType::U32 => "integer",
            ParameterType::Bool => "boolean",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_generates_all_surfaces_from_definition() {
        let definition =
            load_api_definition("../../api/operations.yaml").expect("definition loads");
        let artifacts = generate_all(&definition);

        assert!(artifacts.cli_rs.contains("pub enum GeneratedCommand"));
        assert!(artifacts.cli_rs.contains("ListMessages(ListMessagesArgs)"));
        assert!(
            artifacts
                .http_rs
                .contains("name: \"send_message\", method: \"POST\"")
        );
        assert!(artifacts.mcp_json.contains("\"name\": \"list_contacts\""));
    }

    #[test]
    fn committed_generated_artifacts_are_current() {
        verify_generated("../../api/operations.yaml", "../../generated")
            .expect("generated artifacts are current");
    }

    #[test]
    fn adding_operation_changes_every_surface() {
        let mut definition =
            load_api_definition("../../api/operations.yaml").expect("definition loads");
        definition.operations.push(Operation {
            name: "mark_read".to_string(),
            description: "Mark a thread as read.".to_string(),
            method: HttpMethod::Post,
            path: "/threads/{thread_id}/read".to_string(),
            read: false,
            output_type: "Thread".to_string(),
            parameters: vec![Parameter {
                name: "thread_id".to_string(),
                description: "Thread ID to mark read.".to_string(),
                ty: ParameterType::String,
                required: true,
                location: ParameterLocation::Path,
            }],
        });

        let artifacts = generate_all(&definition);
        assert!(artifacts.cli_rs.contains("MarkRead(MarkReadArgs)"));
        assert!(artifacts.http_rs.contains("/threads/{thread_id}/read"));
        assert!(artifacts.mcp_json.contains("\"name\": \"mark_read\""));
    }

    #[test]
    fn rejects_duplicate_operations() {
        let definition = ApiDefinition {
            operations: vec![
                Operation {
                    name: "list_threads".to_string(),
                    description: "first".to_string(),
                    method: HttpMethod::Get,
                    path: "/threads".to_string(),
                    read: true,
                    output_type: "Vec<Thread>".to_string(),
                    parameters: Vec::new(),
                },
                Operation {
                    name: "list_threads".to_string(),
                    description: "second".to_string(),
                    method: HttpMethod::Get,
                    path: "/threads2".to_string(),
                    read: true,
                    output_type: "Vec<Thread>".to_string(),
                    parameters: Vec::new(),
                },
            ],
        };

        let err = validate_definition(&definition).expect_err("duplicates rejected");
        assert!(err.to_string().contains("duplicate operation"));
    }

    #[test]
    fn rejects_reserved_generated_http_operation_names() {
        let definition = ApiDefinition {
            operations: vec![Operation {
                name: "get".to_string(),
                description: "Reserved generated helper name.".to_string(),
                method: HttpMethod::Get,
                path: "/get".to_string(),
                read: true,
                output_type: "Vec<Thread>".to_string(),
                parameters: Vec::new(),
            }],
        };

        let err = validate_definition(&definition).expect_err("reserved names rejected");
        assert!(
            err.to_string()
                .contains("reserved by generated HTTP module")
        );
    }
}

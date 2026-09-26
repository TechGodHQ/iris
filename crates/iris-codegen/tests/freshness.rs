//! Codegen freshness: the committed `generated/` artifacts must match
//! `api/operations.yaml` exactly. This is the same check CI runs via
//! `iris-codegen check`; as a test it fails locally before a stale commit
//! can land.

use iris_codegen::{
    ParameterLocation, Surface, generate_all, iris_generate_config, load_api_definition,
};

#[test]
fn committed_generated_artifacts_are_fresh() {
    let definition = load_api_definition("../../api/operations.yaml")
        .expect("api/operations.yaml parses and validates");
    let config = iris_generate_config();
    let artifacts = generate_all(&definition, &config);

    let cli = std::fs::read_to_string("../../generated/cli.rs").expect("generated/cli.rs");
    let http = std::fs::read_to_string("../../generated/http.rs").expect("generated/http.rs");
    let mcp = std::fs::read_to_string("../../generated/mcp.json").expect("generated/mcp.json");
    let ts_client = std::fs::read_to_string("../../generated/ts-client/index.ts")
        .expect("generated/ts-client/index.ts");

    assert_eq!(
        cli, artifacts.cli_rs,
        "generated/cli.rs is stale — run `cargo run -p iris-codegen --bin iris-codegen -- write`"
    );
    assert_eq!(
        http, artifacts.http_rs,
        "generated/http.rs is stale — run `cargo run -p iris-codegen --bin iris-codegen -- write`"
    );
    assert_eq!(
        mcp, artifacts.mcp_json,
        "generated/mcp.json is stale — run `cargo run -p iris-codegen --bin iris-codegen -- write`"
    );
    assert_eq!(
        ts_client, artifacts.ts_client_ts,
        "generated/ts-client/index.ts is stale — run `cargo run -p iris-codegen --bin iris-codegen -- write`"
    );
}

#[test]
fn sse_replay_projection_is_explicit_and_four_artifacts_are_deterministic() {
    let definition = load_api_definition("../../api/operations.yaml")
        .expect("api/operations.yaml parses and validates");
    let operation = definition
        .operations
        .iter()
        .find(|operation| operation.name == "subscribe_events")
        .expect("subscribe_events operation");

    assert_eq!(
        operation.surfaces.as_ref(),
        Some(&vec![Surface::Http, Surface::Cli])
    );
    let cursor = operation
        .parameters
        .iter()
        .find(|parameter| parameter.name == "cursor")
        .expect("cursor parameter");
    assert!(!cursor.required);
    assert_eq!(cursor.location, ParameterLocation::Query);

    assert_eq!(operation.cli_output_flags.len(), 1);
    assert_eq!(operation.cli_output_flags[0].flag, "include-cursor");
    assert_eq!(operation.cli_output_flags[0].field, "include_cursor");
    assert_eq!(operation.http_error_responses.len(), 2);
    assert_eq!(operation.http_error_responses[0].status, 400);
    assert_eq!(
        operation.http_error_responses[0].fields[0]
            .constant
            .as_deref(),
        Some("invalid_replay_cursor")
    );
    assert_eq!(operation.http_error_responses[1].status, 409);
    assert!(!operation.http_error_responses[1].fields[1].required);
    assert!(
        operation.http_error_responses[1].fields[1]
            .constant
            .is_none()
    );

    let config = iris_generate_config();
    let first = generate_all(&definition, &config);
    let second = generate_all(&definition, &config);
    assert_eq!(first.cli_rs, second.cli_rs);
    assert_eq!(first.http_rs, second.http_rs);
    assert_eq!(first.mcp_json, second.mcp_json);
    assert_eq!(first.ts_client_ts, second.ts_client_ts);
    assert!(first.cli_rs.contains("pub include_cursor: bool"));
    assert!(
        first
            .http_rs
            .contains("pub mod subscribe_events_http_errors")
    );
    assert!(!first.mcp_json.contains("subscribe_events"));
    assert!(!first.ts_client_ts.contains("subscribe_events"));
}

#[test]
fn public_ingest_surfaces_do_not_name_a_provider() {
    for path in [
        "../../api/operations.yaml",
        "../../generated/cli.rs",
        "../../generated/http.rs",
        "../../generated/mcp.json",
        "../../generated/ts-client",
        "../../crates/iris-server/src",
        "../../crates/iris-mcp/src",
        "../../crates/iris-cli/src",
    ] {
        let output = std::process::Command::new("grep")
            .args(["-R", "-i", "herdr", path])
            .output()
            .expect("grep provider leak guard paths");
        assert!(
            output.stdout.is_empty(),
            "provider name leaked into public ingest surface {path}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
}

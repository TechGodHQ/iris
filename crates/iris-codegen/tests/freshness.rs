//! Codegen freshness: the committed `generated/` artifacts must match
//! `api/operations.yaml` exactly. This is the same check CI runs via
//! `iris-codegen check`; as a test it fails locally before a stale commit
//! can land.

use iris_codegen::{generate_all, iris_generate_config, load_api_definition};

#[test]
fn committed_generated_artifacts_are_fresh() {
    let definition = load_api_definition("../../api/operations.yaml")
        .expect("api/operations.yaml parses and validates");
    let config = iris_generate_config();
    let artifacts = generate_all(&definition, &config);

    let cli = std::fs::read_to_string("../../generated/cli.rs").expect("generated/cli.rs");
    let http = std::fs::read_to_string("../../generated/http.rs").expect("generated/http.rs");
    let mcp = std::fs::read_to_string("../../generated/mcp.json").expect("generated/mcp.json");

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
}

#[test]
fn public_ingest_surfaces_do_not_name_a_provider() {
    for path in [
        "../../api/operations.yaml",
        "../../generated/cli.rs",
        "../../generated/http.rs",
        "../../generated/mcp.json",
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

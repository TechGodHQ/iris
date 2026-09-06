use std::process::{Command, Output};

fn iris_command(config: &std::path::Path, audit: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_iris"));
    command
        .env("IRIS_CONFIG", config)
        .env("IRIS_AUDIT_DIR", audit)
        .env("IRIS_ATTACHMENT_DIR", audit.join("attachments"));
    command
}

fn run(command: &mut Command) -> Output {
    command.output().expect("iris CLI process starts")
}

#[test]
fn generated_cli_discovers_and_routes_to_named_mock_instances() {
    let temp = tempfile::tempdir().expect("temporary test directory");
    let config = temp.path().join("iris.toml");
    let audit = temp.path().join("audit");
    std::fs::write(
        &config,
        "[providers.mock.instances.ops]\n[providers.mock.instances.support]\n",
    )
    .expect("write named mock config");

    let providers = run(iris_command(&config, &audit).arg("providers"));
    assert!(
        providers.status.success(),
        "providers failed: {}",
        String::from_utf8_lossy(&providers.stderr)
    );
    let providers = String::from_utf8(providers.stdout).expect("providers output is UTF-8");
    assert!(providers.contains("mock.ops (mock)"), "{providers}");
    assert!(providers.contains("mock.support (mock)"), "{providers}");
    let discovered_ops = providers
        .lines()
        .filter_map(|line| line.trim().split_once(" ("))
        .map(|(instance_id, _)| instance_id)
        .find(|instance_id| *instance_id == "mock.ops")
        .expect("providers output exposes mock.ops")
        .to_owned();

    let sent = run(iris_command(&config, &audit).args([
        "send-message",
        "--body",
        "to ops",
        "--provider",
        &discovered_ops,
        "thread-1",
    ]));
    assert!(
        sent.status.success(),
        "explicit send failed: {}",
        String::from_utf8_lossy(&sent.stderr)
    );

    let unknown = run(iris_command(&config, &audit).args([
        "send-message",
        "--body",
        "must not dispatch",
        "--provider",
        "mock.unknown",
        "thread-1",
    ]));
    assert!(
        !unknown.status.success(),
        "unknown instance unexpectedly sent"
    );
    assert!(
        String::from_utf8_lossy(&unknown.stderr).contains("provider not available: mock.unknown"),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );

    let audit = run(iris_command(&config, &audit).args(["audit-query", "--action", "send"]));
    assert!(
        audit.status.success(),
        "audit query failed: {}",
        String::from_utf8_lossy(&audit.stderr)
    );
    let entries: serde_json::Value =
        serde_json::from_slice(&audit.stdout).expect("audit-query emits JSON");
    let entries = entries.as_array().expect("audit entries array");
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0]["event"]["provider"], "mock.ops");
    assert_eq!(entries[0]["event"]["source_id"], "thread-1");
}

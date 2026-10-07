//! Compiled-CLI acceptance for replay checkpoints and reconnects.

use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::Request;
use axum::middleware::Next;
use iris_core::{
    AttachmentStore, AuditEntry, AuditFilter, AuditLog, Contact, IrisError, Message, MessageKind,
    MessageProvider, MessageStream, OutboundMessage, ProviderCapability, ProviderMetadata,
    RecordOutcome, Result, Thread,
};
use iris_server::{SseSettings, create_app_with_sse};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;
use tokio_stream::StreamExt;

const THREAD_ID: &str = "00000000-0000-0000-0000-000000000042";
const OTHER_THREAD_ID: &str = "00000000-0000-0000-0000-000000000043";
const FIXTURE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
struct Probe {
    sender: Arc<Mutex<Option<mpsc::Sender<Result<Message>>>>>,
    subscribed: Arc<Notify>,
    progress: Arc<Notify>,
    delivered: Arc<AtomicUsize>,
    drained: Arc<AtomicUsize>,
}

impl Probe {
    async fn wait_until_subscribed(&self) {
        timeout(FIXTURE_TIMEOUT, async {
            loop {
                let notified = self.subscribed.notified();
                if self.sender.lock().expect("probe sender mutex").is_some() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("realtime subscription timeout");
    }

    async fn send(&self, message: Message) {
        timeout(FIXTURE_TIMEOUT, async {
            self.wait_until_subscribed().await;
            let target = self.delivered.load(Ordering::SeqCst) + 1;
            let sender = self
                .sender
                .lock()
                .expect("probe sender mutex")
                .clone()
                .expect("realtime sender");
            sender
                .send(Ok(message))
                .await
                .expect("broker keeps the provider stream open");
            loop {
                let notified = self.progress.notified();
                if self.drained.load(Ordering::SeqCst) >= target {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("realtime delivery drain timeout");
    }

    fn close(&self) {
        self.sender.lock().expect("probe sender mutex").take();
    }
}

struct ProbeStream {
    rx: mpsc::Receiver<Result<Message>>,
    probe: Probe,
}

impl tokio_stream::Stream for ProbeStream {
    type Item = Result<Message>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = self.rx.poll_recv(cx);
        match &result {
            Poll::Ready(Some(_)) => {
                self.probe.delivered.fetch_add(1, Ordering::SeqCst);
                self.probe.progress.notify_waiters();
            }
            Poll::Pending => {
                let delivered = self.probe.delivered.load(Ordering::SeqCst);
                self.probe.drained.store(delivered, Ordering::SeqCst);
                self.probe.progress.notify_waiters();
            }
            Poll::Ready(None) => {}
        }
        result
    }
}

struct MockRealtimeProvider {
    metadata: ProviderMetadata,
    probe: Probe,
}

#[async_trait]
impl MessageProvider for MockRealtimeProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &self.metadata
    }

    async fn list_threads(&self, _limit: Option<u32>) -> Result<Vec<Thread>> {
        Ok(Vec::new())
    }

    async fn list_messages(
        &self,
        _thread_id: &str,
        _before: Option<chrono::DateTime<chrono::Utc>>,
        _limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        Ok(Vec::new())
    }

    async fn list_contacts(&self, _limit: Option<u32>) -> Result<Vec<Contact>> {
        Ok(Vec::new())
    }

    async fn send_message(&self, _thread_id: &str, _message: &OutboundMessage) -> Result<Message> {
        Err(IrisError::UnsupportedCapability {
            provider: self.metadata.id.to_string(),
            capability: "SendMessages".to_string(),
        })
    }

    async fn subscribe_realtime(&self) -> Result<MessageStream> {
        let (sender, receiver) = mpsc::channel(32);
        *self.probe.sender.lock().expect("probe sender mutex") = Some(sender);
        self.probe.subscribed.notify_waiters();
        Ok(Box::pin(ProbeStream {
            rx: receiver,
            probe: self.probe.clone(),
        }))
    }
}

#[derive(Debug)]
struct NullStore;

#[async_trait]
impl AttachmentStore for NullStore {
    async fn store(
        &self,
        _content: iris_core::AttachmentContent,
    ) -> Result<iris_core::AttachmentRef> {
        Err(IrisError::Storage("null store".into()))
    }

    async fn get(&self, _id: &uuid::Uuid) -> Result<iris_core::AttachmentContent> {
        Err(IrisError::NotFound("null store".into()))
    }

    async fn delete(&self, _id: &uuid::Uuid) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct NullAudit;

#[async_trait]
impl AuditLog for NullAudit {
    async fn record(&self, _event: iris_core::AuditEvent) -> Result<AuditEntry> {
        unimplemented!("NullAudit is a selection-time placeholder")
    }

    async fn query(&self, _filter: &AuditFilter) -> Result<Vec<AuditEntry>> {
        Ok(Vec::new())
    }

    async fn verify_chain(&self) -> Result<bool> {
        Ok(true)
    }

    async fn record_once(
        &self,
        _provider: &str,
        _source_id: &str,
        _event: iris_core::AuditEvent,
    ) -> Result<RecordOutcome> {
        unimplemented!("NullAudit is a selection-time placeholder")
    }
}

fn message(id: &str, body: &str, source_id: &str) -> Message {
    message_for("fake", THREAD_ID, id, body, source_id)
}

fn message_for(source: &str, thread_id: &str, id: &str, body: &str, source_id: &str) -> Message {
    Message {
        id: uuid::Uuid::parse_str(id).expect("message UUID"),
        thread_id: uuid::Uuid::parse_str(thread_id).expect("thread UUID"),
        source: source.into(),
        source_id: source_id.into(),
        sender: Contact {
            id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000099").expect("sender UUID"),
            source: source.into(),
            provider_instance: Some(source.into()),
            source_id: "sender".into(),
            display_name: Some("Synthetic sender".into()),
            avatar_url: None,
            metadata: serde_json::json!({}),
        },
        kind: MessageKind::Text,
        body: body.into(),
        attachments: Vec::new(),
        timestamp: chrono::Utc::now(),
        is_outbound: false,
        metadata: serde_json::json!({}),
    }
}

#[derive(Debug)]
struct RunningWatch {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
}

fn launch_watch(base_url: &str, args: &[&str]) -> RunningWatch {
    let mut command = Command::new(env!("CARGO_BIN_EXE_iris"));
    command
        .arg("watch")
        .args(args)
        .env("IRIS_SERVER_URL", base_url)
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn compiled iris binary");
    let stdout = child.stdout.take().expect("watch stdout");
    RunningWatch {
        child,
        lines: BufReader::new(stdout).lines(),
    }
}

async fn next_line(lines: &mut Lines<BufReader<ChildStdout>>) -> String {
    timeout(FIXTURE_TIMEOUT, lines.next_line())
        .await
        .expect("watch output timeout")
        .expect("watch output read")
        .expect("watch emitted a line")
}

async fn stop_watch(mut watch: RunningWatch) {
    timeout(FIXTURE_TIMEOUT, async move {
        let _ = watch.child.kill().await;
        let _ = watch.child.wait().await;
    })
    .await
    .expect("watch process shutdown timeout");
}

async fn finish_watch(mut watch: RunningWatch) {
    let status = timeout(FIXTURE_TIMEOUT, watch.child.wait())
        .await
        .expect("watch clean-end timeout")
        .expect("watch clean-end wait");
    assert!(status.success(), "watch exited unsuccessfully: {status}");
    let extra = timeout(FIXTURE_TIMEOUT, watch.lines.next_line())
        .await
        .expect("watch output drain timeout")
        .expect("watch output drain read");
    assert!(
        extra.is_none(),
        "watch emitted an unexpected extra line: {extra:?}"
    );
}

async fn next_request(rx: &mut mpsc::UnboundedReceiver<String>, label: &str) -> String {
    timeout(FIXTURE_TIMEOUT, rx.recv())
        .await
        .unwrap_or_else(|error| panic!("SSE request timeout ({label}): {error}"))
        .expect("SSE request signal")
}

async fn run_watch_error(base_url: &str, cursor: &str) -> (String, String, i32) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_iris"));
    command
        .args(["watch", "--provider", "fake", "--cursor", cursor])
        .env("IRIS_SERVER_URL", base_url)
        .kill_on_drop(true);
    let output = timeout(FIXTURE_TIMEOUT, command.output())
        .await
        .expect("compiled iris error-path timeout")
        .expect("run compiled iris error path");
    (
        String::from_utf8(output.stdout).expect("stdout UTF-8"),
        String::from_utf8(output.stderr).expect("stderr UTF-8"),
        output.status.code().expect("process exit code"),
    )
}

#[derive(Debug)]
struct KeeperObservation {
    id: String,
    message: serde_json::Value,
}

/// Parse the small SSE subset emitted by the real replay router. This is kept
/// independent from the CLI parser so the keeper is an authoritative wire
/// observer rather than another invocation of the code under test.
fn parse_keeper_frames(buffer: &mut Vec<u8>) -> Vec<KeeperObservation> {
    let mut observations = Vec::new();
    while let Some(position) = buffer.windows(2).position(|window| window == b"\n\n") {
        let frame: Vec<u8> = buffer.drain(..position + 2).collect();
        let text = String::from_utf8(frame).expect("keeper SSE UTF-8");
        let mut event = None;
        let mut id = None;
        let mut data = Vec::new();
        for line in text[..text.len() - 2].split('\n') {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if let Some(value) = line.strip_prefix("event: ") {
                event = Some(value.to_owned());
            } else if let Some(value) = line.strip_prefix("id: ") {
                id = Some(value.to_owned());
            } else if let Some(value) = line.strip_prefix("data: ") {
                data.push(value);
            }
        }
        if event.as_deref().unwrap_or("message") == "message" && !data.is_empty() {
            let id = id.expect("keeper message SSE ID");
            let data = data.join("\n");
            observations.push(KeeperObservation {
                id,
                message: serde_json::from_str(&data).expect("keeper message JSON"),
            });
        }
    }
    observations
}

fn spawn_keeper(
    base_url: &str,
) -> (
    tokio::task::JoinHandle<()>,
    mpsc::Receiver<KeeperObservation>,
) {
    let (observation_tx, observation_rx) = mpsc::channel(32);
    let url = format!("{base_url}/v1/events");
    let handle = tokio::spawn(async move {
        let response = reqwest::Client::new()
            .get(url)
            .send()
            .await
            .expect("keeper response");
        assert!(response.status().is_success());
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("keeper SSE bytes");
            buffer.extend_from_slice(&chunk);
            for observation in parse_keeper_frames(&mut buffer) {
                observation_tx
                    .send(observation)
                    .await
                    .expect("keeper observation receiver");
            }
        }
        assert!(buffer.is_empty(), "keeper ended with a partial SSE frame");
    });
    (handle, observation_rx)
}

async fn next_keeper_observation(
    observations: &mut mpsc::Receiver<KeeperObservation>,
) -> KeeperObservation {
    timeout(FIXTURE_TIMEOUT, observations.recv())
        .await
        .expect("keeper observation timeout")
        .expect("keeper observation channel closed")
}

async fn watch_help() -> String {
    let mut command = Command::new(env!("CARGO_BIN_EXE_iris"));
    command.arg("watch").arg("--help").kill_on_drop(true);
    let output = timeout(FIXTURE_TIMEOUT, command.output())
        .await
        .expect("watch --help timeout")
        .expect("run compiled watch --help");
    assert!(output.status.success());
    String::from_utf8(output.stdout).expect("watch help UTF-8")
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compiled_cli_saves_checkpoint_and_reconnects_through_real_router() {
    let help = watch_help().await;
    assert!(help.contains("--cursor"), "watch help: {help}");
    assert!(help.contains("--include-cursor"), "watch help: {help}");

    let fake_probe = Probe::default();
    let other_probe = Probe::default();
    let fake_provider = Arc::new(MockRealtimeProvider {
        metadata: ProviderMetadata {
            id: "fake",
            name: "Synthetic fake",
            capabilities: &[ProviderCapability::ReceiveRealtime],
        },
        probe: fake_probe.clone(),
    });
    let other_provider = Arc::new(MockRealtimeProvider {
        metadata: ProviderMetadata {
            id: "other",
            name: "Synthetic other",
            capabilities: &[ProviderCapability::ReceiveRealtime],
        },
        probe: other_probe.clone(),
    });
    let (request_tx, mut request_rx) = mpsc::unbounded_channel::<String>();
    let resume_gate = Arc::new(Notify::new());
    let hold_resume = Arc::new(AtomicBool::new(false));
    let app = create_app_with_sse(
        vec![fake_provider, other_provider],
        Arc::new(NullStore),
        Arc::new(NullAudit),
        SseSettings {
            heartbeat_interval: Duration::from_secs(15),
        },
    );
    let resume_gate_for_middleware = resume_gate.clone();
    let hold_resume_for_middleware = hold_resume.clone();
    let app = app.layer(axum::middleware::from_fn(
        move |request: Request, next: Next| {
            let request_tx = request_tx.clone();
            let resume_gate = resume_gate_for_middleware.clone();
            let hold_resume = hold_resume_for_middleware.clone();
            async move {
                let is_events = request.uri().path() == "/v1/events";
                let query = request.uri().query().unwrap_or_default().to_owned();
                let should_hold_resume =
                    is_events && query.contains("cursor=") && hold_resume.load(Ordering::SeqCst);
                if is_events {
                    let _ = request_tx.send(query);
                }
                if should_hold_resume {
                    resume_gate.notified().await;
                }
                next.run(request).await
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("loopback Iris router");
    });
    let base_url = format!("http://{address}");

    // Synthetic discovery -> checkpoint -> reconnect transcript. The keeper
    // observes authoritative SSE IDs and complete payloads independently of
    // the compiled CLI under test, while its aggregate filter exposes events
    // that the exact fake/thread-filtered CLI must reject.
    let (mut keeper, mut keeper_observations) = spawn_keeper(&base_url);
    let keeper_request = next_request(&mut request_rx, "keeper").await;
    assert!(
        keeper_request.is_empty(),
        "aggregate keeper query: {keeper_request}"
    );
    fake_probe.wait_until_subscribed().await;
    other_probe.wait_until_subscribed().await;

    let mut first = launch_watch(
        &base_url,
        &[
            "--provider",
            "fake",
            "--thread-id",
            THREAD_ID,
            "--include-cursor",
        ],
    );
    let first_request = next_request(&mut request_rx, "first").await;
    assert!(
        first_request.contains("provider=fake") && first_request.contains("thread_id="),
        "query: {first_request}"
    );
    assert!(
        !first_request.contains("include_cursor"),
        "CLI-only output flag leaked into HTTP query: {first_request}"
    );

    let wrong_thread = message_for(
        "fake",
        OTHER_THREAD_ID,
        "00000000-0000-0000-0000-000000000201",
        "excluded wrong thread",
        "source-wrong-thread",
    );
    fake_probe.send(wrong_thread.clone()).await;
    let wrong_thread_observation = next_keeper_observation(&mut keeper_observations).await;
    assert_eq!(
        wrong_thread_observation.message,
        serde_json::to_value(&wrong_thread).unwrap()
    );

    let wrong_provider = message_for(
        "other",
        THREAD_ID,
        "00000000-0000-0000-0000-000000000202",
        "excluded wrong provider instance",
        "source-other-instance",
    );
    other_probe.send(wrong_provider.clone()).await;
    let wrong_provider_observation = next_keeper_observation(&mut keeper_observations).await;
    assert_eq!(
        wrong_provider_observation.message,
        serde_json::to_value(&wrong_provider).unwrap()
    );

    let first_message = message(
        "00000000-0000-0000-0000-000000000101",
        "same body — first\nline",
        "source-1",
    );
    fake_probe.send(first_message.clone()).await;
    let first_observation = next_keeper_observation(&mut keeper_observations).await;
    let first_line = next_line(&mut first.lines).await;
    let first_value: serde_json::Value =
        serde_json::from_str(&first_line).expect("first checkpoint JSON");
    let expected_first = serde_json::to_value(&first_message).unwrap();
    assert_eq!(first_observation.message, expected_first);
    let saved_cursor = first_value["cursor"]
        .as_str()
        .expect("first cursor")
        .to_owned();
    assert_eq!(first_value["message"], expected_first);
    assert_eq!(first_value["cursor"], first_observation.id);

    // Keep the original compiled consumer alive while the resumed consumer
    // registers. This avoids racing broker teardown with the reconnect path;
    // its next two matching messages are drained explicitly below. Hold the
    // reconnect handler after query arrival so events below are unambiguously
    // part of the replay snapshot.
    hold_resume.store(true, Ordering::SeqCst);
    let mut resumed = launch_watch(
        &base_url,
        &[
            "--provider",
            "fake",
            "--thread-id",
            THREAD_ID,
            "--cursor",
            &saved_cursor,
            "--include-cursor",
        ],
    );
    let resumed_request = next_request(&mut request_rx, "resumed").await;
    assert!(
        resumed_request.contains("provider=fake")
            && resumed_request.contains("thread_id=")
            && resumed_request.contains("cursor="),
        "query: {resumed_request}"
    );
    assert!(
        !resumed_request.contains("include_cursor"),
        "CLI-only output flag leaked into HTTP query: {resumed_request}"
    );

    let replay_message = message(
        "00000000-0000-0000-0000-000000000102",
        "same body — replay/live identity stays distinct",
        "source-2",
    );
    fake_probe.send(replay_message.clone()).await;
    let replay_observation = next_keeper_observation(&mut keeper_observations).await;
    assert_eq!(
        replay_observation.message,
        serde_json::to_value(&replay_message).unwrap()
    );

    // These mismatches are emitted after the saved cursor but before the
    // reconnect handler is released, so they are genuinely in the replay
    // snapshot and prove the resumed provider/thread filters rather than only
    // the live path.
    let replay_wrong_provider = message_for(
        "other",
        THREAD_ID,
        "00000000-0000-0000-0000-000000000204",
        "excluded during replay provider",
        "source-other-replay",
    );
    other_probe.send(replay_wrong_provider.clone()).await;
    let replay_wrong_provider_observation = next_keeper_observation(&mut keeper_observations).await;
    assert_eq!(
        replay_wrong_provider_observation.message,
        serde_json::to_value(&replay_wrong_provider).unwrap()
    );

    let replay_wrong_thread = message_for(
        "fake",
        OTHER_THREAD_ID,
        "00000000-0000-0000-0000-000000000205",
        "excluded during replay thread",
        "source-fake-replay-wrong-thread",
    );
    fake_probe.send(replay_wrong_thread.clone()).await;
    let replay_wrong_thread_observation = next_keeper_observation(&mut keeper_observations).await;
    assert_eq!(
        replay_wrong_thread_observation.message,
        serde_json::to_value(&replay_wrong_thread).unwrap()
    );

    hold_resume.store(false, Ordering::SeqCst);
    resume_gate.notify_one();
    let replay_line = next_line(&mut resumed.lines).await;
    let replay_value: serde_json::Value =
        serde_json::from_str(&replay_line).expect("replay checkpoint JSON");
    assert_eq!(
        replay_value["message"],
        serde_json::to_value(&replay_message).unwrap()
    );
    assert_eq!(replay_value["cursor"], replay_observation.id);

    let live_message = message(
        "00000000-0000-0000-0000-000000000103",
        "same body — replay/live identity stays distinct",
        "source-3",
    );
    fake_probe.send(live_message.clone()).await;
    let live_observation = next_keeper_observation(&mut keeper_observations).await;
    assert_eq!(
        live_observation.message,
        serde_json::to_value(&live_message).unwrap()
    );

    let live_line = next_line(&mut resumed.lines).await;
    let live_value: serde_json::Value =
        serde_json::from_str(&live_line).expect("live checkpoint JSON");
    assert_eq!(
        live_value["message"],
        serde_json::to_value(&live_message).unwrap()
    );
    assert_eq!(live_value["cursor"], live_observation.id);
    assert_eq!(
        replay_value["message"]["body"],
        live_value["message"]["body"]
    );
    assert_ne!(replay_value["message"]["id"], live_value["message"]["id"]);
    assert_ne!(replay_value["cursor"], live_value["cursor"]);

    let first_replay_line = next_line(&mut first.lines).await;
    let first_live_line = next_line(&mut first.lines).await;
    let first_replay_value: serde_json::Value =
        serde_json::from_str(&first_replay_line).expect("original replay JSON");
    let first_live_value: serde_json::Value =
        serde_json::from_str(&first_live_line).expect("original live JSON");
    assert_eq!(
        first_replay_value["message"]["id"],
        replay_message.id.to_string()
    );
    assert_eq!(
        first_live_value["message"]["id"],
        live_message.id.to_string()
    );
    stop_watch(first).await;

    let (stdout, stderr, code) = run_watch_error(&base_url, "bad cursor/%").await;
    let invalid_request = next_request(&mut request_rx, "invalid").await;
    assert!(
        invalid_request.contains("cursor=bad+cursor%2F%25"),
        "cursor was not safely query-encoded: {invalid_request}"
    );
    assert_eq!(code, 1);
    assert!(stdout.is_empty(), "invalid cursor stdout: {stdout}");
    assert!(stderr.contains("400"), "invalid cursor stderr: {stderr}");
    assert!(
        stderr.contains("invalid_replay_cursor"),
        "invalid cursor stderr: {stderr}"
    );

    let (stdout, stderr, code) = run_watch_error(&base_url, "AAAAAAAAAAAAAAAAAAAAAA.1").await;
    let expired_request = next_request(&mut request_rx, "expired").await;
    assert!(expired_request.contains("cursor=AAAAAAAAAAAAAAAAAAAAAA.1"));
    assert_eq!(code, 1);
    assert!(stdout.is_empty(), "expired cursor stdout: {stdout}");
    assert!(stderr.contains("409"), "expired cursor stderr: {stderr}");
    assert!(
        stderr.contains("replay_cursor_expired"),
        "expired cursor stderr: {stderr}"
    );

    // Drop both synthetic provider senders: the real router closes its SSE
    // subscriptions, giving the resumed CLI and independent keeper a bounded,
    // controlled end rather than killing them after reading expected lines.
    fake_probe.close();
    other_probe.close();
    finish_watch(resumed).await;
    timeout(FIXTURE_TIMEOUT, &mut keeper)
        .await
        .expect("keeper clean-end timeout")
        .expect("keeper task failed");

    server.abort();
    let _ = server.await;
}

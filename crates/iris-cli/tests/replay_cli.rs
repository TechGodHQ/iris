//! Compiled-CLI acceptance for replay checkpoints and reconnects.

use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
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

const THREAD_ID: &str = "00000000-0000-0000-0000-000000000042";

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
        loop {
            let notified = self.subscribed.notified();
            if self.sender.lock().expect("probe sender mutex").is_some() {
                return;
            }
            notified.await;
        }
    }

    async fn send(&self, message: Message) {
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
    Message {
        id: uuid::Uuid::parse_str(id).expect("message UUID"),
        thread_id: uuid::Uuid::parse_str(THREAD_ID).expect("thread UUID"),
        source: "fake".into(),
        source_id: source_id.into(),
        sender: Contact {
            id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000099").expect("sender UUID"),
            source: "fake".into(),
            provider_instance: None,
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
    timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("watch output timeout")
        .expect("watch output read")
        .expect("watch emitted a line")
}

async fn stop_watch(mut watch: RunningWatch) {
    let _ = watch.child.kill().await;
    let _ = watch.child.wait().await;
}

async fn next_request(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
    timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("SSE request timeout")
        .expect("SSE request signal")
}

async fn run_watch_error(base_url: &str, cursor: &str) -> (String, String, i32) {
    let output = Command::new(env!("CARGO_BIN_EXE_iris"))
        .args(["watch", "--provider", "fake", "--cursor", cursor])
        .env("IRIS_SERVER_URL", base_url)
        .output()
        .await
        .expect("run compiled iris error path");
    (
        String::from_utf8(output.stdout).expect("stdout UTF-8"),
        String::from_utf8(output.stderr).expect("stderr UTF-8"),
        output.status.code().expect("process exit code"),
    )
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compiled_cli_saves_checkpoint_and_reconnects_through_real_router() {
    let probe = Probe::default();
    let provider = Arc::new(MockRealtimeProvider {
        metadata: ProviderMetadata {
            id: "fake",
            name: "Synthetic fake",
            capabilities: &[ProviderCapability::ReceiveRealtime],
        },
        probe: probe.clone(),
    });
    let (request_tx, mut request_rx) = mpsc::unbounded_channel::<String>();
    let app = create_app_with_sse(
        vec![provider],
        Arc::new(NullStore),
        Arc::new(NullAudit),
        SseSettings {
            heartbeat_interval: Duration::from_secs(15),
        },
    );
    let app = app.layer(axum::middleware::from_fn(
        move |request: Request, next: Next| {
            let request_tx = request_tx.clone();
            async move {
                let is_events = request.uri().path() == "/v1/events";
                let query = request.uri().query().unwrap_or_default().to_owned();
                let response = next.run(request).await;
                if is_events {
                    let _ = request_tx.send(query);
                }
                response
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

    let mut first = launch_watch(&base_url, &["--provider", "fake", "--include-cursor"]);
    let first_request = next_request(&mut request_rx).await;
    assert!(
        first_request.contains("provider=fake"),
        "query: {first_request}"
    );
    assert!(
        !first_request.contains("include_cursor"),
        "CLI-only output flag leaked into HTTP query: {first_request}"
    );
    probe.wait_until_subscribed().await;

    // A separate HTTP subscriber keeps provider demand alive while the CLI
    // disconnects and reconnects.
    let keeper_client = reqwest::Client::new();
    let keeper_url = format!("{base_url}/v1/events?provider=fake");
    let keeper = tokio::spawn(async move {
        let response = keeper_client
            .get(keeper_url)
            .send()
            .await
            .expect("keeper response");
        assert!(response.status().is_success());
        std::future::pending::<()>().await;
        drop(response);
    });
    let keeper_request = next_request(&mut request_rx).await;
    assert!(keeper_request.contains("provider=fake"));

    let first_message = message(
        "00000000-0000-0000-0000-000000000101",
        "same body — first\nline",
        "source-1",
    );
    probe.send(first_message).await;
    let first_line = next_line(&mut first.lines).await;
    let first_value: serde_json::Value =
        serde_json::from_str(&first_line).expect("first checkpoint JSON");
    let saved_cursor = first_value["cursor"]
        .as_str()
        .expect("first cursor")
        .to_owned();
    assert_eq!(first_value["message"]["body"], "same body — first\nline");
    assert_eq!(
        first_value["message"]["id"],
        "00000000-0000-0000-0000-000000000101"
    );
    stop_watch(first).await;

    let replay_message = message(
        "00000000-0000-0000-0000-000000000102",
        "same body — replay/live identity stays distinct",
        "source-2",
    );
    probe.send(replay_message).await;

    let mut resumed = launch_watch(
        &base_url,
        &[
            "--provider",
            "fake",
            "--cursor",
            &saved_cursor,
            "--include-cursor",
        ],
    );
    let resumed_request = next_request(&mut request_rx).await;
    assert!(
        resumed_request.contains("provider=fake") && resumed_request.contains("cursor="),
        "query: {resumed_request}"
    );
    assert!(
        !resumed_request.contains("include_cursor"),
        "CLI-only output flag leaked into reconnect query: {resumed_request}"
    );

    let live_message = message(
        "00000000-0000-0000-0000-000000000103",
        "same body — replay/live identity stays distinct",
        "source-3",
    );
    probe.send(live_message).await;
    let replay_line = next_line(&mut resumed.lines).await;
    let live_line = next_line(&mut resumed.lines).await;
    let replay_value: serde_json::Value =
        serde_json::from_str(&replay_line).expect("replay checkpoint JSON");
    let live_value: serde_json::Value =
        serde_json::from_str(&live_line).expect("live checkpoint JSON");
    assert_eq!(
        replay_value["message"]["id"],
        "00000000-0000-0000-0000-000000000102"
    );
    assert_eq!(
        live_value["message"]["id"],
        "00000000-0000-0000-0000-000000000103"
    );
    assert_eq!(
        replay_value["message"]["body"],
        live_value["message"]["body"]
    );
    assert_ne!(replay_value["message"]["id"], live_value["message"]["id"]);
    assert_ne!(replay_value["cursor"], live_value["cursor"]);
    stop_watch(resumed).await;

    let (stdout, stderr, code) = run_watch_error(&base_url, "bad cursor/%").await;
    let invalid_request = next_request(&mut request_rx).await;
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
    let expired_request = next_request(&mut request_rx).await;
    assert!(expired_request.contains("cursor=AAAAAAAAAAAAAAAAAAAAAA.1"));
    assert_eq!(code, 1);
    assert!(stdout.is_empty(), "expired cursor stdout: {stdout}");
    assert!(stderr.contains("409"), "expired cursor stderr: {stderr}");
    assert!(
        stderr.contains("replay_cursor_expired"),
        "expired cursor stderr: {stderr}"
    );

    keeper.abort();
    let _ = keeper.await;
    server.abort();
    let _ = server.await;
}

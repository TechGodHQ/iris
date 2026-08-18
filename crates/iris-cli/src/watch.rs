//! `iris watch` — the SSE client runtime for `subscribe_events`.
//!
//! Connects to `IRIS_SERVER_URL` (default `http://127.0.0.1:3000`), parses
//! SSE frames from `GET /v1/events`, writes every `message` JSON unchanged
//! as one stdout line (JSONL), writes `error` diagnostics to stderr, and
//! applies the exit policy: non-zero when the selected (provider-filtered)
//! stream terminates in error, or when the unfiltered aggregate's last
//! branch terminates in error.

use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

use crate::generated::WatchArgs;

/// Default Iris server base URL.
pub const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:3000";

/// Environment variable overriding the server base URL.
pub const SERVER_URL_ENV: &str = "IRIS_SERVER_URL";

/// Exit code reported when the watch stream terminates in error.
pub const WATCH_EXIT_ERROR: u8 = 1;

/// Run `iris watch` against `IRIS_SERVER_URL`.
///
/// Diagnostics stream to stderr as they arrive. When the exit policy
/// fires, the returned error makes the process exit non-zero.
///
/// # Errors
/// Returns an error when the request cannot be established, the
/// connection fails mid-stream, or the stream terminates in error (the
/// exit policy).
pub async fn watch(args: WatchArgs) -> anyhow::Result<()> {
    let url = server_url_from_env(std::env::var(SERVER_URL_ENV).ok().as_deref());
    let client = reqwest::Client::builder().build()?;
    let stdout = tokio::io::stdout();
    let stderr = tokio::io::stderr();
    let code = watch_with_io(&args, &url, &client, stdout, stderr).await?;
    if code == std::process::ExitCode::SUCCESS {
        Ok(())
    } else {
        Err(anyhow::anyhow!("watch stream terminated in error"))
    }
}

/// Resolve the server base URL from `IRIS_SERVER_URL` or the default.
///
/// Empty or whitespace-only values fall back to the default.
#[must_use]
pub fn server_url_from_env(env: Option<&str>) -> String {
    match env {
        Some(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => DEFAULT_SERVER_URL.to_string(),
    }
}

/// One parsed SSE frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseFrame {
    /// `event: message` with raw JSON data.
    Message(String),
    /// `event: error` with raw JSON data.
    Error(String),
    /// A comment line (`: …`), e.g. heartbeats.
    Comment,
}

/// Incremental SSE frame parser.
///
/// Feeds bytes; yields complete frames. A frame is a block of `field:
/// value` lines closed by a blank line. `event:` names the event
/// (defaulting to `message` per the SSE spec); `data:` lines accumulate
/// and join with `\n`. Lines starting with `:` are comments.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: String,
    event: Option<String>,
    data_lines: Vec<String>,
    /// Whether a comment line arrived since the last yielded frame.
    pending_comment: bool,
}

impl SseParser {
    /// Create an empty parser.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed raw bytes; return every complete frame they closed.
    ///
    /// Lines may end with LF, CRLF, or a lone CR (SSE spec). A trailing
    /// lone CR at the buffer edge waits for the next byte in case it is
    /// the first half of a CRLF pair split across chunks.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        self.buffer.push_str(&String::from_utf8_lossy(chunk));
        let mut frames = Vec::new();
        while let Some(pos) = self.buffer.find(['\r', '\n']) {
            let bytes = self.buffer.as_bytes();
            let is_cr = bytes[pos] == b'\r';
            let crlf = is_cr && bytes.get(pos + 1) == Some(&b'\n');
            if is_cr && !crlf && pos + 1 == bytes.len() {
                // Lone CR at the buffer edge: might be a split CRLF — wait.
                break;
            }
            let terminator_len = usize::from(crlf) + 1;
            let line: String = self.buffer.drain(..pos + terminator_len).collect();
            let line = line.trim_end_matches(['\r', '\n']);
            self.feed_line(line, &mut frames);
        }
        frames
    }

    /// Process one complete line.
    fn feed_line(&mut self, line: &str, frames: &mut Vec<SseFrame>) {
        if line.is_empty() {
            if let Some(frame) = self.take_frame() {
                frames.push(frame);
            } else if self.pending_comment {
                frames.push(SseFrame::Comment);
                self.pending_comment = false;
            }
            return;
        }
        if let Some(comment) = line.strip_prefix(':') {
            let _ = comment;
            self.pending_comment = true;
            return;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line, ""),
        };
        match field {
            "event" => self.event = Some(value.to_string()),
            "data" => self.data_lines.push(value.to_string()),
            _ => {}
        }
    }

    /// Flush a partially-buffered frame at end-of-stream, if any.
    pub fn finish(&mut self) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            self.feed_line(line.trim_end_matches(['\r', '\n']), &mut frames);
        }
        if let Some(frame) = self.take_frame() {
            frames.push(frame);
        } else if self.pending_comment {
            frames.push(SseFrame::Comment);
            self.pending_comment = false;
        }
        frames
    }

    /// Consume accumulated fields into one frame, if there is data.
    fn take_frame(&mut self) -> Option<SseFrame> {
        let event = self.event.take().unwrap_or_else(|| "message".into());
        if self.data_lines.is_empty() {
            return None;
        }
        let data = self.data_lines.join("\n");
        self.data_lines.clear();
        match event.as_str() {
            "message" => Some(SseFrame::Message(data)),
            "error" => Some(SseFrame::Error(data)),
            _ => None,
        }
    }
}

/// Streaming watch core against explicit IO — the testable seam.
///
/// Writes every `message` JSON line to `out`, `error` diagnostics to
/// `err`, and returns the exit code: success on clean end, non-zero when
/// the exit policy fires (a filtered stream's error, or the unfiltered
/// aggregate ending in error).
///
/// # Errors
/// Returns an error for connection/request failures only, not for stream
/// errors (those follow the exit policy through the returned code).
pub async fn watch_with_io<W, E>(
    args: &WatchArgs,
    url_base: &str,
    client: &reqwest::Client,
    mut out: W,
    mut err: E,
) -> anyhow::Result<std::process::ExitCode>
where
    W: Send + Unpin + tokio::io::AsyncWrite,
    E: Send + Unpin + tokio::io::AsyncWrite,
{
    let mut request = client.get(format!("{url_base}/v1/events"));
    if let Some(provider) = &args.provider {
        request = request.query(&[("provider", provider)]);
    }
    if let Some(thread_id) = &args.thread_id {
        request = request.query(&[("thread_id", thread_id)]);
    }

    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        write_line(&mut err, &format!("watch: HTTP {status}: {body}")).await?;
        return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
    }

    let filtered = args.provider.is_some();
    let mut saw_error = false;
    let mut parser = SseParser::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        for frame in parser.feed(&chunk) {
            if handle_frame(frame, filtered, &mut out, &mut err, &mut saw_error).await? {
                return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
            }
        }
    }
    for frame in parser.finish() {
        if handle_frame(frame, filtered, &mut out, &mut err, &mut saw_error).await? {
            return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
        }
    }
    if saw_error {
        // Unfiltered aggregate: the stream ended, so the erroring branch
        // was the last one — its terminal error is the aggregate's.
        return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
    }
    Ok(std::process::ExitCode::SUCCESS)
}

/// Handle one parsed frame.
///
/// Returns `true` when the watch should stop immediately with a non-zero
/// exit (a provider-filtered stream's terminal error).
async fn handle_frame<W, E>(
    frame: SseFrame,
    filtered: bool,
    out: &mut W,
    err: &mut E,
    saw_error: &mut bool,
) -> anyhow::Result<bool>
where
    W: Send + Unpin + tokio::io::AsyncWrite,
    E: Send + Unpin + tokio::io::AsyncWrite,
{
    match frame {
        SseFrame::Message(data) => {
            write_line(out, &data).await?;
        }
        SseFrame::Error(data) => {
            *saw_error = true;
            write_line(err, &format!("watch: error frame: {data}")).await?;
            // A provider-filtered stream closes after its error.
            if filtered {
                return Ok(true);
            }
        }
        SseFrame::Comment => {}
    }
    Ok(false)
}

/// Write one line and flush.
async fn write_line<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    text: &str,
) -> anyhow::Result<()> {
    writer.write_all(text.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::WatchArgs;

    /// Frames parse from a complete chunk.
    #[test]
    fn parser_extracts_message_and_error_frames() {
        let mut parser = SseParser::new();
        let frames = parser.feed(
            b"event: message\ndata: {\"body\":\"hi\"}\n\nevent: error\ndata: {\"code\":\"slow_consumer\"}\n\n",
        );
        assert_eq!(
            frames,
            vec![
                SseFrame::Message("{\"body\":\"hi\"}".into()),
                SseFrame::Error("{\"code\":\"slow_consumer\"}".into()),
            ]
        );
    }

    /// Frames split across arbitrary chunk boundaries reassemble.
    #[test]
    fn parser_handles_split_chunks() {
        let mut parser = SseParser::new();
        assert!(parser.feed(b"event: mes").is_empty());
        assert!(parser.feed(b"sage\ndata: {\"a\":").is_empty());
        assert_eq!(
            parser.feed(b"1}\n\n"),
            vec![SseFrame::Message("{\"a\":1}".into())]
        );
    }

    /// Comment lines (heartbeats) surface as Comment frames.
    #[test]
    fn parser_surfaces_comments() {
        let mut parser = SseParser::new();
        assert_eq!(parser.feed(b": heartbeat\n\n"), vec![SseFrame::Comment]);
    }

    /// The default event name is `message` per the SSE spec.
    #[test]
    fn parser_defaults_event_to_message() {
        let mut parser = SseParser::new();
        assert_eq!(
            parser.feed(b"data: {\"x\":true}\n\n"),
            vec![SseFrame::Message("{\"x\":true}".into())]
        );
    }

    /// Multi-line data joins with newlines.
    #[test]
    fn parser_joins_multiline_data() {
        let mut parser = SseParser::new();
        assert_eq!(
            parser.feed(b"data: line1\ndata: line2\n\n"),
            vec![SseFrame::Message("line1\nline2".into())]
        );
    }

    /// A truncated final frame flushes at end-of-stream.
    #[test]
    fn parser_finish_flushes_partial_frame() {
        let mut parser = SseParser::new();
        assert!(
            parser
                .feed(b"event: error\ndata: {\"code\":\"provider_failed\"}")
                .is_empty()
        );
        assert_eq!(
            parser.finish(),
            vec![SseFrame::Error("{\"code\":\"provider_failed\"}".into())]
        );
    }

    /// CR-only line endings (some proxies) still terminate lines.
    #[test]
    fn parser_handles_cr_newlines() {
        let mut parser = SseParser::new();
        assert!(parser.feed(b"event: message\r").is_empty());
        assert_eq!(
            parser.feed(b"data: {\"ok\":1}\n\n"),
            vec![SseFrame::Message("{\"ok\":1}".into())]
        );
    }

    /// The server URL comes from the environment with a sane default.
    #[test]
    fn server_url_defaults_and_env_override() {
        assert_eq!(server_url_from_env(None), DEFAULT_SERVER_URL);
        assert_eq!(
            server_url_from_env(Some("  ")),
            DEFAULT_SERVER_URL,
            "blank env falls back to the default"
        );
        assert_eq!(
            server_url_from_env(Some(" http://iris.internal:9091 ")),
            "http://iris.internal:9091"
        );
    }

    /// Watch against a live in-process server: happy path (messages →
    /// JSONL stdout, clean end → exit 0).
    #[tokio::test]
    async fn watch_round_trips_messages_end_to_end() {
        let (addr, _flag) = spawn_test_server(false).await;
        let args = WatchArgs {
            provider: None,
            thread_id: None,
        };
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = watch_with_io(
            &args,
            &format!("http://{addr}"),
            &reqwest::Client::new(),
            &mut out,
            &mut err,
        )
        .await
        .unwrap();
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        let stdout = String::from_utf8(out).expect("utf8");
        let stderr = String::from_utf8(err).expect("utf8");
        assert!(stdout.contains("\"body\":\"e2e body\""), "stdout: {stdout}");
        assert!(stdout.contains('\n'), "JSONL: one line per message");
        assert!(stderr.is_empty(), "stderr: {stderr}");
    }

    /// Aggregate error at end-of-stream → non-zero exit, stderr carries
    /// the diagnostic, stdout still has prior messages.
    #[tokio::test]
    async fn watch_aggregate_error_exits_nonzero() {
        let (addr, _flag) = spawn_test_server(true).await;
        let args = WatchArgs {
            provider: None,
            thread_id: None,
        };
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = watch_with_io(
            &args,
            &format!("http://{addr}"),
            &reqwest::Client::new(),
            &mut out,
            &mut err,
        )
        .await
        .unwrap();
        assert_ne!(code, std::process::ExitCode::SUCCESS);
        let stdout = String::from_utf8(out).expect("utf8");
        let stderr = String::from_utf8(err).expect("utf8");
        assert!(stdout.contains("\"body\":\"e2e body\""), "stdout: {stdout}");
        assert!(stderr.contains("error frame"), "stderr: {stderr}");
        assert!(stderr.contains("telegram_conflict"), "stderr: {stderr}");
    }

    /// Filtered (provider=) stream error → immediate non-zero exit.
    #[tokio::test]
    async fn watch_filtered_error_exits_nonzero() {
        let (addr, _flag) = spawn_test_server(true).await;
        let args = WatchArgs {
            provider: Some("fake".into()),
            thread_id: None,
        };
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = watch_with_io(
            &args,
            &format!("http://{addr}"),
            &reqwest::Client::new(),
            &mut out,
            &mut err,
        )
        .await
        .unwrap();
        assert_ne!(code, std::process::ExitCode::SUCCESS);
        let stderr = String::from_utf8(err).expect("utf8");
        assert!(stderr.contains("error frame"), "stderr: {stderr}");
    }

    /// HTTP-level failure (unknown provider) → non-zero exit with the
    /// status on stderr.
    #[tokio::test]
    async fn watch_http_error_reports_to_stderr() {
        let (addr, _flag) = spawn_test_server(false).await;
        let args = WatchArgs {
            provider: Some("nonexistent".into()),
            thread_id: None,
        };
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = watch_with_io(
            &args,
            &format!("http://{addr}"),
            &reqwest::Client::new(),
            &mut out,
            &mut err,
        )
        .await
        .unwrap();
        assert_ne!(code, std::process::ExitCode::SUCCESS);
        let stderr = String::from_utf8(err).expect("utf8");
        assert!(stderr.contains("422"), "stderr: {stderr}");
        assert!(
            stderr.contains("unsupported_realtime_provider"),
            "stderr: {stderr}"
        );
    }

    /// Thread filter passes through as a query parameter and the server
    /// filters the wire accordingly.
    #[tokio::test]
    async fn watch_passes_thread_filter_query() {
        let (addr, _flag) = spawn_test_server(false).await;
        let args = WatchArgs {
            provider: None,
            thread_id: Some("00000000-0000-0000-0000-000000000042".into()),
        };
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = watch_with_io(
            &args,
            &format!("http://{addr}"),
            &reqwest::Client::new(),
            &mut out,
            &mut err,
        )
        .await
        .unwrap();
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        let stdout = String::from_utf8(out).expect("utf8");
        assert!(!stdout.contains("wrong thread"), "stdout: {stdout}");
        assert!(stdout.contains("\"body\":\"e2e body\""), "stdout: {stdout}");
    }

    // -------------------------------------------------------------------
    // In-process server harness
    // -------------------------------------------------------------------

    /// Spawn an in-process server whose `/v1/events` emits a wrong-thread
    /// message, a right-thread message, and (optionally) a terminal error,
    /// then ends the stream.
    #[allow(clippy::too_many_lines)]
    async fn spawn_test_server(
        error_at_end: bool,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        use async_trait::async_trait;
        use iris_core::{
            AttachmentStore, AuditEntry, AuditFilter, AuditLog, Contact, IrisError, Message,
            MessageKind, MessageProvider, MessageStream, ProviderCapability, ProviderMetadata,
            RecordOutcome, Result, Thread,
        };

        struct ScriptedProvider {
            metadata: ProviderMetadata,
            error_at_end: bool,
        }

        #[async_trait]
        impl MessageProvider for ScriptedProvider {
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
            async fn send_message(&self, _thread_id: &str, _body: &str) -> Result<Message> {
                Err(IrisError::UnsupportedCapability {
                    provider: self.metadata.id.to_string(),
                    capability: "SendMessages".to_string(),
                })
            }
            async fn subscribe_realtime(&self) -> Result<MessageStream> {
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message>>(16);
                let error_at_end = self.error_at_end;
                tokio::spawn(async move {
                    let base = Message {
                        id: uuid::Uuid::new_v4(),
                        thread_id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000042")
                            .unwrap(),
                        source: "fake".into(),
                        source_id: "s-1".into(),
                        sender: Contact {
                            id: uuid::Uuid::new_v4(),
                            source: "fake".into(),
                            source_id: "sender-1".into(),
                            display_name: Some("Sender".into()),
                            avatar_url: None,
                            metadata: serde_json::json!({}),
                        },
                        kind: MessageKind::Text,
                        body: "e2e body".into(),
                        attachments: Vec::new(),
                        timestamp: chrono::Utc::now(),
                        is_outbound: false,
                        metadata: serde_json::json!({}),
                    };
                    let wrong = Message {
                        thread_id: uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
                            .unwrap(),
                        body: "wrong thread".into(),
                        ..base.clone()
                    };
                    let _ = tx.send(Ok(wrong)).await;
                    let _ = tx.send(Ok(base)).await;
                    if error_at_end {
                        let _ = tx
                            .send(Err(IrisError::Provider {
                                provider: "fake".into(),
                                message: "telegram getUpdates conflict (HTTP 409)".into(),
                            }))
                            .await;
                    }
                    // `tx` drops here → the stream ends.
                });
                Ok(Box::pin(ChannelStream { rx }))
            }
        }

        struct ChannelStream {
            rx: tokio::sync::mpsc::Receiver<Result<Message>>,
        }

        impl tokio_stream::Stream for ChannelStream {
            type Item = Result<Message>;
            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                self.rx.poll_recv(cx)
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
                Err(IrisError::Storage("null".into()))
            }
            async fn get(&self, _id: &uuid::Uuid) -> Result<iris_core::AttachmentContent> {
                Err(IrisError::NotFound("null".into()))
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
                unimplemented!("selection-time placeholder")
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
                unimplemented!("selection-time placeholder")
            }
        }

        let provider = ScriptedProvider {
            metadata: ProviderMetadata {
                id: "fake",
                name: "Fake",
                capabilities: &[ProviderCapability::ReceiveRealtime],
            },
            error_at_end,
        };
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(error_at_end));
        let app = iris_server::create_app_with_sse(
            vec![std::sync::Arc::new(provider)],
            std::sync::Arc::new(NullStore),
            std::sync::Arc::new(NullAudit),
            iris_server::SseSettings::default(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (addr, flag)
    }
}

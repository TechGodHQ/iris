//! `iris watch` — the SSE client runtime for `subscribe_events`.
//!
//! Connects to `IRIS_SERVER_URL` (default `http://127.0.0.1:3000`), parses
//! SSE frames from `GET /v1/events`, writes every `message` JSON unchanged
//! as one stdout line (JSONL), or wraps each message with its server-issued
//! cursor when `--include-cursor` is selected. It writes `error` diagnostics
//! to stderr and applies the exit policy: non-zero when the selected
//! (provider-filtered) stream terminates in error, or when the unfiltered
//! aggregate's last branch terminates in error.

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
    /// `event: message` with its frame-local SSE ID and raw JSON data.
    Message {
        /// The `id:` field on this frame, if one was present.
        id: Option<String>,
        /// The raw JSON data payload.
        data: String,
    },
    /// `event: error` with its frame-local SSE ID and raw JSON data.
    Error {
        /// The `id:` field on this frame, if one was present.
        id: Option<String>,
        /// The raw JSON error payload.
        data: String,
    },
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
    /// Bytes are buffered until a complete line is available so a UTF-8 code
    /// point split across HTTP chunks is decoded exactly once.
    buffer: Vec<u8>,
    event: Option<String>,
    id: Option<String>,
    data_lines: Vec<String>,
    /// Whether a comment line arrived since the last yielded frame.
    pending_comment: bool,
    /// Whether the last consumed line ended with a CR at a chunk boundary.
    ///
    /// The CR is already a complete line terminator, so it must be processed
    /// immediately. If the next chunk starts with LF, that byte is the second
    /// half of a split CRLF pair and must be ignored rather than treated as a
    /// second blank line.
    pending_crlf: bool,
}

impl SseParser {
    /// Create an empty parser.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed raw bytes; return every complete frame they closed.
    ///
    /// Lines may end with LF, CRLF, or a lone CR (SSE spec). A CR at the end of
    /// one HTTP chunk is processed immediately as a line terminator; if the
    /// next chunk starts with LF, that byte is consumed as the second half of a
    /// split CRLF pair rather than dispatching a spurious empty event. A CR
    /// followed by any other byte is treated as a lone-CR terminator.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();
        if self.pending_crlf && !self.buffer.is_empty() {
            if self.buffer.first() == Some(&b'\n') {
                self.buffer.remove(0);
            }
            self.pending_crlf = false;
        }
        while let Some(pos) = self
            .buffer
            .iter()
            .position(|byte| *byte == b'\r' || *byte == b'\n')
        {
            let is_cr = self.buffer[pos] == b'\r';
            let crlf = is_cr && self.buffer.get(pos + 1) == Some(&b'\n');
            let terminator_len = usize::from(crlf) + 1;
            let split_crlf = is_cr && pos + 1 == self.buffer.len();
            let line_with_terminator: Vec<u8> = self.buffer.drain(..pos + terminator_len).collect();
            self.feed_line(
                &line_with_terminator[..line_with_terminator.len() - terminator_len],
                &mut frames,
            );
            if split_crlf {
                self.pending_crlf = true;
            }
        }
        frames
    }

    /// Process one complete line.
    fn feed_line(&mut self, line: &[u8], frames: &mut Vec<SseFrame>) {
        if line.is_empty() {
            if let Some(frame) = self.take_frame() {
                frames.push(frame);
            } else if self.pending_comment {
                frames.push(SseFrame::Comment);
                self.pending_comment = false;
            }
            return;
        }
        if line.first() == Some(&b':') {
            self.pending_comment = true;
            return;
        }
        let (field, mut value) = line.iter().position(|byte| *byte == b':').map_or_else(
            || (line, &[][..]),
            |index| (&line[..index], &line[index + 1..]),
        );
        if value.first() == Some(&b' ') {
            value = &value[1..];
        }
        match field {
            b"event" => self.event = Some(String::from_utf8_lossy(value).into_owned()),
            b"data" => self
                .data_lines
                .push(String::from_utf8_lossy(value).into_owned()),
            b"id" => self.id = Some(String::from_utf8_lossy(value).into_owned()),
            _ => {}
        }
    }

    /// Flush a partially-buffered frame at end-of-stream, if any.
    pub fn finish(&mut self) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        if !self.buffer.is_empty() {
            let line = std::mem::take(&mut self.buffer);
            let line = if line.last() == Some(&b'\r') {
                &line[..line.len() - 1]
            } else {
                &line[..]
            };
            self.feed_line(line, &mut frames);
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
        let id = self.id.take();
        let data_lines = std::mem::take(&mut self.data_lines);
        if data_lines.is_empty() {
            return None;
        }
        self.pending_comment = false;
        let data = data_lines.join("\n");
        match event.as_str() {
            "message" => Some(SseFrame::Message { id, data }),
            "error" => Some(SseFrame::Error { id, data }),
            _ => None,
        }
    }
}

/// Streaming watch core against explicit IO — the testable seam.
///
/// Writes every `message` JSON line to `out` (or a cursor envelope when
/// requested), `error` diagnostics to `err`, and returns the exit code:
/// success on clean end, non-zero when the exit policy fires (a filtered
/// stream's error, an invalid checkpoint frame, or the unfiltered aggregate
/// ending in error).
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
    let mut request = client.get(format!("{}/v1/events", url_base.trim_end_matches('/')));
    if let Some(provider) = &args.provider {
        request = request.query(&[("provider", provider)]);
    }
    if let Some(thread_id) = &args.thread_id {
        request = request.query(&[("thread_id", thread_id)]);
    }
    if let Some(cursor) = &args.cursor {
        request = request.query(&[("cursor", cursor)]);
    }

    let response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        write_line(&mut err, &format!("watch: HTTP {status}: {body}")).await?;
        return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
    }

    let filtered = args.provider.is_some();
    // The aggregate exit policy keys off how the stream ENDS: non-zero
    // only when the last event before close was a terminal error (a later
    // message proves another branch outlived the error).
    let mut ended_in_error = false;
    let mut parser = SseParser::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        for frame in parser.feed(&chunk) {
            if handle_frame(
                frame,
                filtered,
                args.include_cursor,
                &mut out,
                &mut err,
                &mut ended_in_error,
            )
            .await?
            {
                return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
            }
        }
    }
    for frame in parser.finish() {
        if handle_frame(
            frame,
            filtered,
            args.include_cursor,
            &mut out,
            &mut err,
            &mut ended_in_error,
        )
        .await?
        {
            return Ok(std::process::ExitCode::from(WATCH_EXIT_ERROR));
        }
    }
    if ended_in_error {
        // Unfiltered aggregate: the last branch terminated in error.
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
    include_cursor: bool,
    out: &mut W,
    err: &mut E,
    ended_in_error: &mut bool,
) -> anyhow::Result<bool>
where
    W: Send + Unpin + tokio::io::AsyncWrite,
    E: Send + Unpin + tokio::io::AsyncWrite,
{
    match frame {
        SseFrame::Message { id, data } => {
            *ended_in_error = false;
            if include_cursor {
                let Some(cursor) = id.filter(|id| !id.is_empty()) else {
                    write_line(
                        err,
                        "watch: message frame has no non-empty SSE cursor; refusing to emit a checkpoint",
                    )
                    .await?;
                    return Ok(true);
                };
                let Ok(message) = serde_json::from_str::<serde_json::Value>(&data) else {
                    write_line(
                        err,
                        "watch: message frame is not valid JSON; refusing to emit a checkpoint",
                    )
                    .await?;
                    return Ok(true);
                };
                if !message.is_object() {
                    write_line(
                        err,
                        "watch: message frame JSON must be an object; refusing to emit a checkpoint",
                    )
                    .await?;
                    return Ok(true);
                }
                let envelope = serde_json::json!({
                    "cursor": cursor,
                    "message": message,
                });
                write_line(out, &serde_json::to_string(&envelope)?).await?;
            } else {
                write_line(out, &data).await?;
            }
        }
        SseFrame::Error { id: _, data } => {
            *ended_in_error = true;
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
                SseFrame::Message {
                    id: None,
                    data: "{\"body\":\"hi\"}".into(),
                },
                SseFrame::Error {
                    id: None,
                    data: "{\"code\":\"slow_consumer\"}".into(),
                },
            ]
        );
    }

    /// The last repeated `id:` field wins, and the ID is attached only to its
    /// own frame.
    #[test]
    fn parser_retains_the_frame_id_and_last_id_wins() {
        let mut parser = SseParser::new();
        assert_eq!(
            parser.feed(b"id: first\nid: second\ndata: {\"body\":\"hi\"}\n\n"),
            vec![SseFrame::Message {
                id: Some("second".into()),
                data: "{\"body\":\"hi\"}".into(),
            }]
        );
    }

    /// Empty IDs and error frames must not leak into the next message frame.
    #[test]
    fn parser_resets_ids_between_message_and_error_frames() {
        let mut parser = SseParser::new();
        assert_eq!(
            parser.feed(
                b"id: first\ndata: {\"body\":\"one\"}\n\nid:\nevent: error\ndata: {\"code\":\"bad\"}\n\ndata: {\"body\":\"two\"}\n\n"
            ),
            vec![
                SseFrame::Message {
                    id: Some("first".into()),
                    data: "{\"body\":\"one\"}".into(),
                },
                SseFrame::Error {
                    id: Some(String::new()),
                    data: "{\"code\":\"bad\"}".into(),
                },
                SseFrame::Message {
                    id: None,
                    data: "{\"body\":\"two\"}".into(),
                },
            ]
        );
    }

    /// A code point split across transport chunks and a CRLF split at the
    /// chunk boundary must both survive unchanged.
    #[test]
    fn parser_preserves_split_utf8_and_crlf() {
        let wire = "id: cursor-1\r\ndata: {\"body\":\"héllo\\n世界\"}\r\n\r\n";
        let bytes = wire.as_bytes();
        let first_cr = bytes
            .iter()
            .position(|byte| *byte == b'\r')
            .expect("first CRLF");
        let rest = &bytes[first_cr + 1..];
        let utf8_split = rest
            .windows(2)
            .position(|pair| pair == "é".as_bytes())
            .expect("accented code point")
            + 1;
        let mut parser = SseParser::new();
        assert!(parser.feed(&bytes[..=first_cr]).is_empty());
        assert!(parser.feed(&rest[..utf8_split]).is_empty());
        assert_eq!(
            parser.feed(&rest[utf8_split..]),
            vec![SseFrame::Message {
                id: Some("cursor-1".into()),
                data: "{\"body\":\"héllo\\n世界\"}".into(),
            }]
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
            vec![SseFrame::Message {
                id: None,
                data: "{\"a\":1}".into(),
            }]
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
            vec![SseFrame::Message {
                id: None,
                data: "{\"x\":true}".into(),
            }]
        );
    }

    /// Multi-line data joins with newlines.
    #[test]
    fn parser_joins_multiline_data() {
        let mut parser = SseParser::new();
        assert_eq!(
            parser.feed(b"data: line1\ndata: line2\n\n"),
            vec![SseFrame::Message {
                id: None,
                data: "line1\nline2".into(),
            }]
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
            vec![SseFrame::Error {
                id: None,
                data: "{\"code\":\"provider_failed\"}".into(),
            }]
        );
    }

    /// CR-only line endings (some proxies) still terminate lines.
    #[test]
    fn parser_handles_cr_newlines() {
        let mut parser = SseParser::new();
        assert!(parser.feed(b"event: message\r").is_empty());
        assert_eq!(
            parser.feed(b"data: {\"ok\":1}\n\n"),
            vec![SseFrame::Message {
                id: None,
                data: "{\"ok\":1}".into(),
            }]
        );
    }

    /// A complete CR-only frame is emitted as soon as its final delimiter is
    /// available, without waiting for EOF or another byte.
    #[test]
    fn parser_emits_complete_cr_only_frame_without_finish() {
        let mut parser = SseParser::new();
        assert_eq!(
            parser.feed(b"id: cursor-1\rdata: {\"ok\":1}\r\r"),
            vec![SseFrame::Message {
                id: Some("cursor-1".into()),
                data: "{\"ok\":1}".into(),
            }]
        );
    }

    /// A CRLF split across chunks preserves the frame ID and does not turn the
    /// delayed LF into a second blank frame.
    #[test]
    fn parser_split_crlf_does_not_clear_frame_id_or_duplicate_frame() {
        let mut parser = SseParser::new();
        assert!(parser.feed(b"id: cursor-1\r").is_empty());
        assert!(parser.feed(&[]).is_empty());
        assert_eq!(
            parser.feed(b"\ndata: {\"ok\":1}\r\n\r\n"),
            vec![SseFrame::Message {
                id: Some("cursor-1".into()),
                data: "{\"ok\":1}".into(),
            }]
        );
        assert!(parser.finish().is_empty());
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
        let (addr, _flag) = spawn_test_server(Ending::Clean).await;
        let args = WatchArgs {
            provider: None,
            thread_id: None,
            cursor: None,
            include_cursor: false,
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

    /// Opt-in output carries the server-issued SSE ID without changing the
    /// message object.
    #[tokio::test]
    async fn watch_include_cursor_emits_a_checkpoint_envelope() {
        let (addr, _flag) = spawn_test_server(Ending::Clean).await;
        let args = WatchArgs {
            provider: None,
            thread_id: None,
            cursor: None,
            include_cursor: true,
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
        let values: Vec<serde_json::Value> = out
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).expect("checkpoint JSONL"))
            .collect();
        assert!(
            !values.is_empty(),
            "stdout: {}",
            String::from_utf8_lossy(&out)
        );
        let value = &values[0];
        assert!(
            value["cursor"]
                .as_str()
                .is_some_and(|cursor| !cursor.is_empty()),
            "value: {value}"
        );
        assert_eq!(value["message"]["body"], "wrong thread");
        assert!(values.iter().all(|value| value["message"].is_object()));
        assert!(err.is_empty(), "stderr: {}", String::from_utf8_lossy(&err));
    }

    /// A message without an ID cannot be represented as a resumable success.
    #[tokio::test]
    async fn include_cursor_rejects_a_message_without_an_id() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut ended_in_error = false;
        let should_exit = handle_frame(
            SseFrame::Message {
                id: None,
                data: "{\"body\":\"no cursor\"}".into(),
            },
            false,
            true,
            &mut out,
            &mut err,
            &mut ended_in_error,
        )
        .await
        .unwrap();
        assert!(should_exit);
        assert!(out.is_empty());
        assert!(
            String::from_utf8(err)
                .unwrap()
                .contains("no non-empty SSE cursor")
        );
    }

    /// Checkpoint mode accepts only message objects and never emits a
    /// checkpoint for a scalar/array/null payload.
    #[tokio::test]
    async fn include_cursor_rejects_non_object_checkpoint_payloads() {
        for data in ["null", "[]", "\"text\"", "42", "true"] {
            let mut out = Vec::new();
            let mut err = Vec::new();
            let mut ended_in_error = false;
            let should_exit = handle_frame(
                SseFrame::Message {
                    id: Some("cursor-1".into()),
                    data: data.into(),
                },
                false,
                true,
                &mut out,
                &mut err,
                &mut ended_in_error,
            )
            .await
            .unwrap();
            assert!(should_exit, "payload should be rejected: {data}");
            assert!(out.is_empty(), "payload emitted a checkpoint: {data}");
            let diagnostic = String::from_utf8(err).unwrap();
            assert!(
                diagnostic.contains("must be an object"),
                "diagnostic: {diagnostic}"
            );
            assert!(
                !diagnostic.contains(data),
                "raw payload leaked: {diagnostic}"
            );
        }
    }

    /// Malformed JSON and an explicitly empty frame ID are rejected without
    /// emitting a checkpoint or echoing the malformed payload.
    #[tokio::test]
    async fn include_cursor_rejects_malformed_json_and_empty_id() {
        let cases = [
            (
                Some(String::new()),
                r#"{"body":"valid"}"#,
                "no non-empty SSE cursor",
            ),
            (
                Some("cursor-1".into()),
                r#"{"body":"unterminated"#,
                "not valid JSON",
            ),
        ];
        for (id, data, expected_diagnostic) in cases {
            let mut out = Vec::new();
            let mut err = Vec::new();
            let mut ended_in_error = false;
            let should_exit = handle_frame(
                SseFrame::Message {
                    id,
                    data: data.into(),
                },
                false,
                true,
                &mut out,
                &mut err,
                &mut ended_in_error,
            )
            .await
            .unwrap();
            assert!(should_exit);
            assert!(out.is_empty());
            let diagnostic = String::from_utf8(err).unwrap();
            assert!(diagnostic.contains(expected_diagnostic), "{diagnostic}");
            assert!(
                !diagnostic.contains(data),
                "raw payload leaked: {diagnostic}"
            );
        }
    }

    /// A valid object checkpoint retains every field, including Unicode,
    /// escaped newlines, and nested extra data.
    #[tokio::test]
    async fn include_cursor_preserves_the_complete_message_object() {
        let data = r#"{"body":"héllo\n世界","extra":{"answer":42},"items":[true,null]}"#;
        let mut out = Vec::new();
        let mut err = Vec::new();
        let mut ended_in_error = false;
        let should_exit = handle_frame(
            SseFrame::Message {
                id: Some("cursor-1".into()),
                data: data.into(),
            },
            false,
            true,
            &mut out,
            &mut err,
            &mut ended_in_error,
        )
        .await
        .unwrap();
        assert!(!should_exit);
        assert!(err.is_empty());
        let value: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value["cursor"], "cursor-1");
        assert_eq!(
            value["message"],
            serde_json::json!({
                "body": "héllo\n世界",
                "extra": {"answer": 42},
                "items": [true, null],
            })
        );
    }

    /// An error followed by a later message (another branch outlived it)
    /// then a clean end → exit 0: the aggregate did not end in error.
    #[tokio::test]
    async fn watch_error_then_message_then_clean_end_exits_zero() {
        let (addr, _flag) = spawn_test_server(Ending::ErrorThenMessage).await;
        let args = WatchArgs {
            provider: None,
            thread_id: None,
            cursor: None,
            include_cursor: false,
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
        assert!(stdout.contains("survivor message"), "stdout: {stdout}");
        assert!(stderr.contains("error frame"), "stderr: {stderr}");
    }

    /// Aggregate error at end-of-stream → non-zero exit, stderr carries
    /// the diagnostic, stdout still has prior messages.
    #[tokio::test]
    async fn watch_aggregate_error_exits_nonzero() {
        let (addr, _flag) = spawn_test_server(Ending::Error).await;
        let args = WatchArgs {
            provider: None,
            thread_id: None,
            cursor: None,
            include_cursor: false,
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
        let (addr, _flag) = spawn_test_server(Ending::Error).await;
        let args = WatchArgs {
            provider: Some("fake".into()),
            thread_id: None,
            cursor: None,
            include_cursor: false,
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
        let (addr, _flag) = spawn_test_server(Ending::Clean).await;
        let args = WatchArgs {
            provider: Some("nonexistent".into()),
            thread_id: None,
            cursor: None,
            include_cursor: false,
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
        let (addr, _flag) = spawn_test_server(Ending::Clean).await;
        let args = WatchArgs {
            provider: None,
            thread_id: Some("00000000-0000-0000-0000-000000000042".into()),
            cursor: None,
            include_cursor: false,
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

    /// What the scripted provider emits after its two messages.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Ending {
        /// The stream ends cleanly (tx dropped).
        Clean,
        /// A terminal error is the last event before the stream ends.
        Error,
        /// A terminal error, then a further message proving another branch
        /// outlived it, then the stream ends cleanly.
        ErrorThenMessage,
    }

    /// Spawn an in-process server whose `/v1/events` emits a wrong-thread
    /// message, a right-thread message, then the chosen ending.
    #[allow(clippy::too_many_lines)]
    async fn spawn_test_server(
        ending: Ending,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        use async_trait::async_trait;
        use iris_core::{
            AttachmentStore, AuditEntry, AuditFilter, AuditLog, Contact, IrisError, Message,
            MessageKind, MessageProvider, MessageStream, OutboundMessage, ProviderCapability,
            ProviderMetadata, RecordOutcome, Result, Thread,
        };

        struct ScriptedProvider {
            metadata: ProviderMetadata,
            ending: Ending,
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
            async fn send_message(
                &self,
                _thread_id: &str,
                _message: &OutboundMessage,
            ) -> Result<Message> {
                Err(IrisError::UnsupportedCapability {
                    provider: self.metadata.id.to_string(),
                    capability: "SendMessages".to_string(),
                })
            }
            async fn subscribe_realtime(&self) -> Result<MessageStream> {
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message>>(16);
                let ending = self.ending;
                let is_survivor = self.metadata.id == "survivor";
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
                            provider_instance: None,
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
                    if is_survivor {
                        // A second branch that outlives fake's terminal
                        // error: its later message is what proves the
                        // aggregate did not end in error.
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        let survivor = Message {
                            body: "survivor message".into(),
                            ..base.clone()
                        };
                        let _ = tx.send(Ok(survivor)).await;
                        // `tx` drops here → this branch ends.
                        return;
                    }
                    let _ = tx.send(Ok(wrong)).await;
                    let _ = tx.send(Ok(base.clone())).await;
                    if ending != Ending::Clean {
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
            ending,
        };
        let survivor = ScriptedProvider {
            metadata: ProviderMetadata {
                id: "survivor",
                name: "Survivor",
                capabilities: &[ProviderCapability::ReceiveRealtime],
            },
            ending,
        };
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut providers =
            vec![std::sync::Arc::new(provider) as std::sync::Arc<dyn iris_core::MessageProvider>];
        if ending == Ending::ErrorThenMessage {
            providers.push(std::sync::Arc::new(survivor));
        }
        let app = iris_server::create_app_with_sse(
            providers,
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

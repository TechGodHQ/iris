//! Iris MCP server — exposes Iris operations as MCP tools.
//!
//! Tool definitions are generated from the core API definition by iris-codegen.
//! The runtime speaks newline-delimited JSON-RPC over stdio, which is the
//! transport agents expect for local MCP servers.

use std::{collections::BTreeSet, sync::Arc};

use iris_core::{
    AuditAction, AuditEntry, AuditFilter, AuditLog, Contact, IngestBatch, IngestOutcome,
    IngestStore, IrisError, Message, MessageProvider, OutboundMessage, Thread,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

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
pub fn generated_tools() -> serde_json::Result<Value> {
    serde_json::from_str(GENERATED_TOOLS_JSON)
}

/// MCP server runtime backed by Iris message providers.
#[derive(Clone)]
pub struct McpServer {
    providers: Vec<Arc<dyn MessageProvider>>,
    audit: Arc<dyn AuditLog>,
    ingest: Option<Arc<dyn IngestStore>>,
    ingest_sources: BTreeSet<String>,
    ingest_secret_sources: BTreeSet<String>,
}

impl McpServer {
    /// Create an MCP server backed by the given providers and audit log.
    #[must_use]
    pub fn new(providers: Vec<Arc<dyn MessageProvider>>, audit: Arc<dyn AuditLog>) -> Self {
        Self {
            providers,
            audit,
            ingest: None,
            ingest_sources: BTreeSet::new(),
            ingest_secret_sources: BTreeSet::new(),
        }
    }

    /// Attach the local transactional ingest backend for the generated ingest tool.
    #[must_use]
    pub fn with_ingest(
        mut self,
        ingest: Arc<dyn IngestStore>,
        sources: impl IntoIterator<Item = String>,
        secret_sources: impl IntoIterator<Item = String>,
    ) -> Self {
        self.ingest = Some(ingest);
        self.ingest_sources = sources.into_iter().collect();
        self.ingest_secret_sources = secret_sources.into_iter().collect();
        self
    }

    /// Handle a single JSON-RPC request value and return a JSON-RPC response.
    pub async fn handle_jsonrpc(&self, request: Value) -> Value {
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let Some(method) = request.get("method").and_then(Value::as_str) else {
            return error_response(&id, -32600, "missing JSON-RPC method");
        };
        let params = request.get("params").cloned().unwrap_or(Value::Null);

        match self.handle_method(method, params).await {
            Ok(result) => success_response(&id, &result),
            Err(error) => error_response(&id, -32000, &error.to_string()),
        }
    }

    async fn handle_method(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        match method {
            "initialize" => Ok(json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            })),
            "notifications/initialized" => Ok(Value::Null),
            "tools/list" => tools_list_result(),
            "tools/call" => self.tools_call(params).await,
            other => anyhow::bail!("unsupported MCP method: {other}"),
        }
    }

    async fn tools_call(&self, params: Value) -> anyhow::Result<Value> {
        let request: ToolCallRequest = serde_json::from_value(params)?;
        let result = match request.name.as_str() {
            "list_threads" => serde_json::to_value(self.list_threads(&request.arguments).await?)?,
            "list_contacts" => serde_json::to_value(self.list_contacts(&request.arguments).await?)?,
            "list_messages" => serde_json::to_value(self.list_messages(&request.arguments).await?)?,
            "send_message" => serde_json::to_value(self.send_message(&request.arguments).await?)?,
            "audit_query" => serde_json::to_value(self.audit_query(&request.arguments).await?)?,
            "ingest_batch" => serde_json::to_value(self.ingest_batch(&request.arguments).await?)?,
            other => anyhow::bail!("unknown Iris MCP tool: {other}"),
        };

        Ok(tool_result(&result, false))
    }

    async fn list_threads(&self, arguments: &Value) -> anyhow::Result<Vec<Thread>> {
        let args: ListArgs = serde_json::from_value(arguments.clone())?;
        let cursor = args
            .cursor
            .as_deref()
            .map(parse_thread_cursor)
            .transpose()?;
        let mut threads = Vec::new();
        for provider in &self.providers {
            threads.extend(
                provider
                    .list_threads(if cursor.is_some() { None } else { args.limit })
                    .await?,
            );
        }
        threads.sort_by(|a, b| {
            b.last_message_at
                .cmp(&a.last_message_at)
                .then_with(|| a.source.cmp(&b.source))
                .then_with(|| a.id.cmp(&b.id))
        });
        if let Some(cursor) = cursor {
            threads = threads_after_cursor(threads, &cursor)?;
        }
        truncate_limit(&mut threads, args.limit);
        Ok(threads)
    }

    async fn list_contacts(&self, arguments: &Value) -> anyhow::Result<Vec<Contact>> {
        let args: ListArgs = serde_json::from_value(arguments.clone())?;
        let mut contacts = Vec::new();
        for provider in &self.providers {
            contacts.extend(provider.list_contacts(args.limit).await?);
        }
        contacts.sort_by(|a, b| {
            a.display_name
                .cmp(&b.display_name)
                .then_with(|| a.source.cmp(&b.source))
                .then_with(|| a.source_id.cmp(&b.source_id))
                .then_with(|| a.id.cmp(&b.id))
        });
        if let Some(cursor) = args.cursor.as_deref() {
            contacts = contacts_after_cursor(contacts, cursor)?;
        }
        truncate_limit(&mut contacts, args.limit);
        Ok(contacts)
    }

    async fn list_messages(&self, arguments: &Value) -> anyhow::Result<Vec<Message>> {
        let args: ListMessagesArgs = serde_json::from_value(arguments.clone())?;
        let before = args
            .before
            .as_deref()
            .map(chrono::DateTime::parse_from_rfc3339)
            .transpose()?
            .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
        let provider = self.provider_for_thread(&args.thread_id).await?;
        let mut messages = provider
            .list_messages(&args.thread_id, before, args.limit)
            .await?;
        messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then_with(|| a.id.cmp(&b.id)));
        truncate_limit(&mut messages, args.limit);
        Ok(messages)
    }

    async fn send_message(&self, arguments: &Value) -> anyhow::Result<Message> {
        let args: SendMessageArgs = serde_json::from_value(arguments.clone())?;
        let attachments = iris_core::decode_attachments(args.attachments.as_ref())?;
        let outbound = OutboundMessage {
            body: args.body,
            attachments,
        };
        // An explicit configured instance is authoritative: callers discover it
        // from the provider listing and Iris dispatches to that exact instance.
        // Without one, resolve ownership from the thread.
        let provider = match args.provider.as_deref() {
            Some(provider_id) => self.provider_by_id(provider_id)?,
            None => self.provider_for_thread(&args.thread_id).await?,
        };
        Ok(provider.send_message(&args.thread_id, &outbound).await?)
    }

    async fn ingest_batch(&self, arguments: &Value) -> anyhow::Result<Value> {
        let batch: IngestBatch = serde_json::from_value(
            arguments
                .get("batch")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing ingest batch"))?,
        )?;
        if !self.ingest_sources.contains(&batch.source) {
            anyhow::bail!("ingest source is not configured: {}", batch.source);
        }
        if !self.ingest_secret_sources.contains(&batch.source) {
            anyhow::bail!("ingest source secret is not configured: {}", batch.source);
        }
        let store = self
            .ingest
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ingest service is not configured"))?;
        Ok(match store.apply_batch(batch).await? {
            IngestOutcome::Applied { committed_at } => {
                json!({"outcome": "applied", "committed_at": committed_at})
            }
            IngestOutcome::AlreadyApplied { committed_at } => {
                json!({"outcome": "already_applied", "committed_at": committed_at})
            }
            IngestOutcome::ReplayConflict => {
                anyhow::bail!("replay key conflicts with an existing batch")
            }
        })
    }

    async fn audit_query(&self, arguments: &Value) -> anyhow::Result<Vec<AuditEntry>> {
        let args: AuditQueryArgs = serde_json::from_value(arguments.clone())?;
        if args
            .since
            .is_some_and(|since| args.until.is_some_and(|until| since > until))
        {
            anyhow::bail!("since must be before or equal to until");
        }
        Ok(self
            .audit
            .query(&AuditFilter {
                provider: args.provider,
                action: args.action,
                since: args.since,
                until: args.until,
                limit: args.limit.map(|limit| limit as usize),
                source_id: args.source_id,
            })
            .await?)
    }

    async fn provider_for_thread(
        &self,
        thread_id: &str,
    ) -> anyhow::Result<Arc<dyn MessageProvider>> {
        for provider in &self.providers {
            let threads = provider.list_threads(None).await?;
            if threads
                .iter()
                .any(|thread| thread.id.to_string() == thread_id)
            {
                return Ok(Arc::clone(provider));
            }
        }
        Err(IrisError::NotFound(format!("thread not found: {thread_id}")).into())
    }

    fn provider_by_id(&self, provider_id: &str) -> anyhow::Result<Arc<dyn MessageProvider>> {
        self.providers
            .iter()
            .find(|provider| provider.id() == provider_id)
            .map(Arc::clone)
            .ok_or_else(|| IrisError::ProviderNotFound(provider_id.to_string()).into())
    }
}

/// Run the server over newline-delimited JSON-RPC streams.
pub async fn run_jsonrpc<R, W>(server: McpServer, reader: R, mut writer: W) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => server.handle_jsonrpc(request).await,
            Err(error) => error_response(&Value::Null, -32700, &format!("parse error: {error}")),
        };
        writer
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
    }
    Ok(())
}

fn tools_list_result() -> anyhow::Result<Value> {
    let tools = generated_tools()?;
    Ok(json!({"tools": tools["tools"].clone()}))
}

fn tool_result(value: &Value, is_error: bool) -> Value {
    json!({
        "content": [{"type": "text", "text": serde_json::to_string_pretty(&value).expect("JSON serialization cannot fail")}],
        "isError": is_error,
    })
}

fn success_response(id: &Value, result: &Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn truncate_limit<T>(items: &mut Vec<T>, limit: Option<u32>) {
    if let Some(limit) = limit {
        items.truncate(limit as usize);
    }
}

#[derive(Debug, Default, Deserialize)]
struct ListArgs {
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThreadCursor {
    timestamp: chrono::DateTime<chrono::Utc>,
    source: Option<String>,
    id: Option<uuid::Uuid>,
}

fn contacts_after_cursor(contacts: Vec<Contact>, cursor: &str) -> anyhow::Result<Vec<Contact>> {
    let cursor = uuid::Uuid::parse_str(cursor)?;
    let Some(index) = contacts.iter().position(|contact| contact.id == cursor) else {
        anyhow::bail!("contact cursor not found: {cursor}");
    };
    Ok(contacts.into_iter().skip(index + 1).collect())
}

fn threads_after_cursor(
    threads: Vec<Thread>,
    cursor: &ThreadCursor,
) -> anyhow::Result<Vec<Thread>> {
    let Some(source) = cursor.source.as_deref() else {
        return Ok(threads
            .into_iter()
            .filter(|thread| thread.last_message_at < cursor.timestamp)
            .collect());
    };
    let id = cursor.id.expect("composite cursor includes thread id");
    let Some(index) = threads.iter().position(|thread| {
        thread.last_message_at == cursor.timestamp && thread.source == source && thread.id == id
    }) else {
        anyhow::bail!(
            "thread cursor not found: {}|{}|{}",
            cursor.timestamp.to_rfc3339(),
            source,
            id
        );
    };
    Ok(threads.into_iter().skip(index + 1).collect())
}

fn parse_thread_cursor(cursor: &str) -> anyhow::Result<ThreadCursor> {
    let parts: Vec<_> = cursor.split('|').collect();
    match parts.as_slice() {
        [timestamp] => Ok(ThreadCursor {
            timestamp: chrono::DateTime::parse_from_rfc3339(timestamp)?.with_timezone(&chrono::Utc),
            source: None,
            id: None,
        }),
        [timestamp, source, id] => Ok(ThreadCursor {
            timestamp: chrono::DateTime::parse_from_rfc3339(timestamp)?.with_timezone(&chrono::Utc),
            source: Some((*source).to_string()),
            id: Some(uuid::Uuid::parse_str(id)?),
        }),
        _ => anyhow::bail!("thread cursor must be RFC3339 or '<RFC3339>|<source>|<uuid>'"),
    }
}

#[derive(Debug, Deserialize)]
struct ListMessagesArgs {
    thread_id: String,
    limit: Option<u32>,
    before: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SendMessageArgs {
    thread_id: String,
    body: String,
    provider: Option<String>,
    #[serde(default)]
    attachments: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct AuditQueryArgs {
    provider: Option<String>,
    action: Option<AuditAction>,
    since: Option<chrono::DateTime<chrono::Utc>>,
    until: Option<chrono::DateTime<chrono::Utc>>,
    source_id: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ToolCallRequest {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Serialize)]
struct _ProtocolMarker;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use iris_core::{AuditAction, AuditEvent, AuditLog};
    use iris_providers::mock::MockProvider;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tokio::io::BufReader;

    fn server() -> McpServer {
        McpServer::new(
            vec![Arc::new(MockProvider::new())],
            Arc::new(iris_audit::LocalFsAuditLog::new("/tmp/iris-mcp-test-audit")),
        )
    }

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
        assert!(names.contains(&"list_threads"));
        assert!(names.contains(&"list_contacts"));
        assert!(names.contains(&"send_message"));
        assert!(names.contains(&"audit_query"));
    }

    #[tokio::test]
    async fn initialize_returns_mcp_server_info() {
        let response = server()
            .handle_jsonrpc(json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
            .await;

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert_eq!(response["result"]["serverInfo"]["name"], SERVER_NAME);
        assert_eq!(response["result"]["capabilities"]["tools"], json!({}));
    }

    #[tokio::test]
    async fn tools_list_returns_all_generated_tools() {
        let response = server()
            .handle_jsonrpc(json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .await;
        let names: Vec<_> = response["result"]["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name"))
            .collect();

        assert!(names.contains(&"list_messages"));
        assert!(names.contains(&"list_threads"));
        assert!(names.contains(&"list_contacts"));
        assert!(names.contains(&"send_message"));
        assert!(names.contains(&"audit_query"));
    }

    #[tokio::test]
    async fn tools_call_executes_against_mock_provider() {
        let response = server()
            .handle_jsonrpc(json!({
                "jsonrpc":"2.0",
                "id":3,
                "method":"tools/call",
                "params":{"name":"list_threads","arguments":{"limit":1}}
            }))
            .await;

        assert_eq!(response["error"], Value::Null);
        assert_eq!(response["result"]["isError"], false);
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");
        let threads: Vec<Thread> = serde_json::from_str(text).expect("thread JSON");
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].source, "mock");
    }

    #[tokio::test]
    async fn tools_call_queries_audit_entries() {
        let temp = tempfile::tempdir().unwrap();
        let audit: Arc<dyn AuditLog> = Arc::new(iris_audit::LocalFsAuditLog::new(temp.path()));
        let expected = audit
            .record(AuditEvent {
                action: AuditAction::Send,
                provider: "telegram".into(),
                source_id: Some("outgoing-1".into()),
                timestamp: Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0).unwrap(),
                metadata: json!({}),
            })
            .await
            .unwrap();
        let server = McpServer::new(vec![Arc::new(MockProvider::new())], audit);

        let response = server
            .handle_jsonrpc(json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "audit_query",
                    "arguments": {"action": "send", "provider": "telegram"}
                }
            }))
            .await;

        assert_eq!(response["result"]["isError"], false);
        let entries: Vec<AuditEntry> =
            serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
                .unwrap();
        assert_eq!(entries, vec![expected]);
    }

    #[tokio::test]
    async fn run_jsonrpc_handles_line_delimited_requests() {
        let input = br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_contacts","arguments":{"limit":1}}}
"#;
        let mut output = Vec::new();

        run_jsonrpc(server(), BufReader::new(&input[..]), &mut output)
            .await
            .expect("stdio run succeeds");

        let lines: Vec<_> = std::str::from_utf8(&output)
            .expect("utf8")
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("response JSON"))
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0]["result"]["tools"].as_array().expect("tools").len(),
            6
        );
        assert_eq!(lines[1]["result"]["isError"], false);
    }

    #[tokio::test]
    async fn send_message_tool_accepts_inline_and_stored_attachments() {
        use iris_core::AttachmentStore as _;

        let tmp = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(iris_storage::LocalFsStore::new(tmp.path()));
        let stored = store
            .store(iris_core::AttachmentContent {
                mime_type: "text/plain".to_string(),
                filename: Some("stored.txt".to_string()),
                bytes: b"stored-bytes".to_vec(),
            })
            .await
            .unwrap();
        let mock = std::sync::Arc::new(MockProvider::new().with_store(store));
        let server = McpServer::new(
            vec![mock.clone() as Arc<dyn MessageProvider>],
            Arc::new(iris_audit::LocalFsAuditLog::new(tmp.path().join("audit"))),
        );
        // The mock mints a fresh thread UUID on every list_threads call, so
        // thread-owner routing cannot match; route by explicit provider id.
        let thread_id = "11111111-1111-1111-1111-111111111111".to_string();

        let response = server
            .handle_jsonrpc(json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "send_message",
                    "arguments": {
                        "thread_id": thread_id,
                        "body": "see files",
                        "provider": "mock",
                        "attachments": [
                            {"mime_type": "image/png", "filename": "a.png", "data_base64": "aGk="},
                            {"stored_id": stored.id.to_string()},
                        ],
                    }
                }
            }))
            .await;

        assert_eq!(response["error"], Value::Null, "{response}");
        assert_eq!(response["result"]["isError"], false);
        let sends = mock.recorded_sends().expect("records readable");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].attachments.len(), 2);
        assert_eq!(sends[0].attachments[0].bytes, b"hi".to_vec());
        assert_eq!(
            sends[0].attachments[1].filename.as_deref(),
            Some("stored.txt")
        );
    }

    #[tokio::test]
    async fn send_message_tool_rejects_mixed_attachment_union() {
        let response = server()
            .handle_jsonrpc(json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "send_message",
                    "arguments": {
                        "thread_id": "11111111-1111-1111-1111-111111111111",
                        "body": "hello",
                        "attachments": [{
                            "mime_type": "image/png",
                            "data_base64": "aGk=",
                            "stored_id": "22222222-2222-2222-2222-222222222222",
                        }],
                    }
                }
            }))
            .await;

        assert_ne!(response["error"], Value::Null, "{response}");
        let message = response["error"]["message"].as_str().unwrap();
        assert!(message.contains("mixes"), "{message}");
    }

    #[tokio::test]
    async fn send_message_tool_rejects_invalid_stored_uuid() {
        let response = server()
            .handle_jsonrpc(json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "send_message",
                    "arguments": {
                        "thread_id": "11111111-1111-1111-1111-111111111111",
                        "body": "hello",
                        "attachments": [{"stored_id": "not-a-uuid"}],
                    }
                }
            }))
            .await;

        assert_ne!(response["error"], Value::Null, "{response}");
        let message = response["error"]["message"].as_str().unwrap();
        assert!(message.contains("invalid UUID"), "{message}");
    }

    #[test]
    fn generated_send_message_schema_carries_declared_union() {
        let tools = generated_tools().expect("generated tools parse");
        let tool = tools["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .find(|tool| tool["name"] == "send_message")
            .expect("send_message tool");
        let attachments = &tool["inputSchema"]["properties"]["attachments"];
        assert_eq!(attachments["type"], "array");
        let variants = attachments["items"]["oneOf"]
            .as_array()
            .expect("closed union variants");
        assert_eq!(variants.len(), 2);
        assert_eq!(variants[0]["required"], json!(["mime_type", "data_base64"]));
        assert_eq!(variants[0]["additionalProperties"], false);
        assert_eq!(variants[1]["required"], json!(["stored_id"]));
        assert_eq!(variants[1]["additionalProperties"], false);
    }
}

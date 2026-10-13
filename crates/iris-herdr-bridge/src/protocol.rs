use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

use crate::spool::Spool;

const EXPECTED_HERDR_VERSION: &str = "0.8.0";
const EXPECTED_PROTOCOL: u64 = 19;
const MAX_WIRE_LINE_BYTES: usize = 4 * 1024 * 1024;
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// Recover forever from socket/server failures at the approved two-second
/// interval. A planned pane-set refresh reconnects immediately to minimize the
/// interval without pane-scoped status subscriptions. No socket payload, event
/// content, path, or credential is included in logs.
pub async fn run_source(socket_path: PathBuf, spool: Arc<Mutex<Spool>>) -> anyhow::Result<()> {
    loop {
        let delay = match consume_subscription(&socket_path, &spool).await {
            Ok(SubscriptionEnd::PeerClosed) => {
                info!("Herdr subscription ended; reconnecting");
                RECONNECT_DELAY
            }
            Ok(SubscriptionEnd::PaneSetChanged) => {
                info!("Herdr pane set changed; refreshing scoped status subscriptions");
                reconnect_delay(SubscriptionEnd::PaneSetChanged)
            }
            Err(error) => {
                warn!(
                    reason = error.class,
                    "Herdr subscription unavailable; retrying"
                );
                RECONNECT_DELAY
            }
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionEnd {
    PeerClosed,
    PaneSetChanged,
}

#[derive(Debug)]
struct SourceFailure {
    class: &'static str,
}

impl SourceFailure {
    const fn new(class: &'static str) -> Self {
        Self { class }
    }
}

const fn reconnect_delay(end: SubscriptionEnd) -> Duration {
    match end {
        SubscriptionEnd::PeerClosed => RECONNECT_DELAY,
        SubscriptionEnd::PaneSetChanged => Duration::ZERO,
    }
}

async fn consume_subscription(
    socket_path: &Path,
    spool: &Arc<Mutex<Spool>>,
) -> Result<SubscriptionEnd, SourceFailure> {
    verify_protocol(socket_path).await?;
    let mut pane_infos = list_panes(socket_path).await?;
    let mut workspace_snapshots = list_workspace_snapshots(socket_path).await?;
    resolve_unmapped_events(spool, &pane_infos, &workspace_snapshots).await?;
    let pane_ids = pane_infos.keys().cloned().collect::<BTreeSet<_>>();
    let request_id = format!("herdr-bridge-{}", Uuid::new_v4());
    let request = subscription_request(&request_id, &pane_ids);
    let stream = UnixStream::connect(socket_path)
        .await
        .map_err(|_| SourceFailure::new("socket_connect"))?;
    let mut reader = send_and_ack(stream, &request_id, &request).await?;

    // Herdr v0.8.0's unfiltered pane-status subscription reports transitions
    // after its baseline but does not emit the pane's current state. Refresh
    // snapshots after the ACK, then durably capture one current-status
    // observation per pane so startup and reconnects establish a usable read
    // model without narrowing the ongoing subscription to one status value.
    let current_panes = list_panes(socket_path).await?;
    let current_pane_ids = current_panes.keys().cloned().collect::<BTreeSet<_>>();
    let current_workspace_snapshots = list_workspace_snapshots(socket_path).await?;
    append_status_snapshots(spool, &current_panes, &current_workspace_snapshots).await?;
    if current_pane_ids != pane_ids {
        return Ok(SubscriptionEnd::PaneSetChanged);
    }
    pane_infos = current_panes;
    workspace_snapshots = current_workspace_snapshots;

    loop {
        let line = read_line(&mut reader)
            .await
            .map_err(|_| SourceFailure::new("subscription_read"))?;
        let Some(line) = line else {
            return Ok(SubscriptionEnd::PeerClosed);
        };
        let mut payload: Value =
            serde_json::from_slice(&line).map_err(|_| SourceFailure::new("subscription_json"))?;
        if payload.get("error").is_some() {
            return Err(SourceFailure::new("subscription_error"));
        }
        let event_kind = payload
            .get("event")
            .and_then(Value::as_str)
            .ok_or_else(|| SourceFailure::new("subscription_event_shape"))?
            .replace('.', "_");
        if !payload.get("data").is_some_and(Value::is_object) {
            return Err(SourceFailure::new("subscription_event_shape"));
        }

        // Persist a bounded normalized projection before per-event IPC. Keep
        // only fields used by the Iris mapper; raw pane titles, paths, token
        // maps, terminal data, and unknown source fields never enter the spool.
        sanitize_event_payload(&mut payload)?;
        let received_at = Utc::now();
        let sequence = spool
            .lock()
            .await
            .append_unmapped_upstream_event(payload.clone(), received_at)
            .map_err(|_| SourceFailure::new("spool_write"))?;
        // Event subscriptions can replay retained history and Herdr polls
        // independent event kinds without a total source order. Refresh the
        // authoritative workspace snapshot before normalizing any event that
        // can upsert a thread; never let an old event payload regress it.
        if refresh_workspace_snapshot(&event_kind) {
            workspace_snapshots = list_workspace_snapshots(socket_path).await?;
        }
        let current_panes = if refresh_pane_snapshot(&event_kind) {
            list_panes(socket_path).await?
        } else {
            pane_infos.clone()
        };
        enrich_workspace_snapshots(&mut payload, &workspace_snapshots, &current_panes);
        spool
            .lock()
            .await
            .mark_event_ready(sequence, payload)
            .map_err(|_| SourceFailure::new("spool_write"))?;
        // A subscription starts by replaying retained events. A replayed
        // pane.created/pane.moved payload may describe a pane that no longer
        // exists, so pane.list—not the event payload—is authoritative for this
        // cache. Otherwise the same retained stale frame can force reconnects
        // indefinitely.
        let current_pane_ids = current_panes.keys().cloned().collect::<BTreeSet<_>>();
        let pane_change = changed_pane(&event_kind, &pane_ids, &current_pane_ids);
        if pane_change {
            return Ok(SubscriptionEnd::PaneSetChanged);
        }
        pane_infos = current_panes;
    }
}

async fn resolve_unmapped_events(
    spool: &Arc<Mutex<Spool>>,
    pane_infos: &BTreeMap<String, Value>,
    workspace_snapshots: &BTreeMap<String, Value>,
) -> Result<(), SourceFailure> {
    let pending = spool
        .lock()
        .await
        .unmapped_event_payloads()
        .map_err(|_| SourceFailure::new("spool_read"))?;
    for (sequence, _received_at, mut payload) in pending {
        let is_status_snapshot = payload
            .pointer("/bridge/status_snapshot")
            .and_then(Value::as_bool)
            == Some(true);
        if is_status_snapshot {
            if let Some(pane_id) = payload.pointer("/data/pane_id").and_then(Value::as_str)
                && let Some(pane_info) = pane_infos.get(pane_id)
                && let Some(agent_status) = pane_info.get("agent_status")
            {
                payload["data"]["agent_status"] = agent_status.clone();
            }
            enrich_status_event(&mut payload, pane_infos);
        }
        enrich_workspace_snapshots(&mut payload, workspace_snapshots, pane_infos);
        strip_tokens(&mut payload);
        spool
            .lock()
            .await
            .mark_event_ready(sequence, payload)
            .map_err(|_| SourceFailure::new("spool_write"))?;
    }
    Ok(())
}

async fn append_status_snapshots(
    spool: &Arc<Mutex<Spool>>,
    pane_infos: &BTreeMap<String, Value>,
    workspace_snapshots: &BTreeMap<String, Value>,
) -> Result<(), SourceFailure> {
    for (pane_id, pane_info) in pane_infos {
        let agent_status = pane_info
            .get("agent_status")
            .and_then(Value::as_str)
            .ok_or_else(|| SourceFailure::new("pane_list_shape"))?;
        let mut payload = json!({
            "event": "pane.agent_status_changed",
            "data": {"pane_id": pane_id, "agent_status": agent_status}
        });
        sanitize_event_payload(&mut payload)?;
        payload["bridge"] = json!({"status_snapshot": true});
        let received_at = Utc::now();
        let sequence = spool
            .lock()
            .await
            .append_unmapped_upstream_event(payload.clone(), received_at)
            .map_err(|_| SourceFailure::new("spool_write"))?;
        enrich_status_event(&mut payload, pane_infos);
        enrich_workspace_snapshots(&mut payload, workspace_snapshots, pane_infos);
        spool
            .lock()
            .await
            .mark_event_ready(sequence, payload)
            .map_err(|_| SourceFailure::new("spool_write"))?;
    }
    Ok(())
}

async fn verify_protocol(socket_path: &Path) -> Result<(), SourceFailure> {
    let response = one_shot_request(
        socket_path,
        json!({"id": "herdr-bridge-ping", "method": "ping", "params": {}}),
    )
    .await?;
    let version = response.pointer("/result/version").and_then(Value::as_str);
    let protocol = response.pointer("/result/protocol").and_then(Value::as_u64);
    if response.pointer("/result/type").and_then(Value::as_str) != Some("pong")
        || version != Some(EXPECTED_HERDR_VERSION)
        || protocol != Some(EXPECTED_PROTOCOL)
    {
        return Err(SourceFailure::new("unsupported_herdr_version"));
    }
    Ok(())
}

async fn list_panes(socket_path: &Path) -> Result<BTreeMap<String, Value>, SourceFailure> {
    let response = one_shot_request(
        socket_path,
        json!({"id": "herdr-bridge-pane-list", "method": "pane.list", "params": {}}),
    )
    .await?;
    let panes = response
        .pointer("/result/panes")
        .and_then(Value::as_array)
        .ok_or_else(|| SourceFailure::new("pane_list_shape"))?;
    let mut panes_by_id = BTreeMap::new();
    for pane in panes {
        let id = pane
            .get("pane_id")
            .and_then(Value::as_str)
            .ok_or_else(|| SourceFailure::new("pane_list_shape"))?;
        if pane.get("agent_status").and_then(Value::as_str).is_none() {
            return Err(SourceFailure::new("pane_list_shape"));
        }
        if panes_by_id.insert(id.to_owned(), pane.clone()).is_some() {
            return Err(SourceFailure::new("pane_list_duplicate_id"));
        }
    }
    Ok(panes_by_id)
}

async fn list_workspace_snapshots(
    socket_path: &Path,
) -> Result<BTreeMap<String, Value>, SourceFailure> {
    let response = one_shot_request(
        socket_path,
        json!({
            "id": "herdr-bridge-workspace-list",
            "method": "workspace.list",
            "params": {}
        }),
    )
    .await?;
    if response.pointer("/result/type").and_then(Value::as_str) != Some("workspace_list") {
        return Err(SourceFailure::new("workspace_list_shape"));
    }
    let workspaces = response
        .pointer("/result/workspaces")
        .and_then(Value::as_array)
        .ok_or_else(|| SourceFailure::new("workspace_list_shape"))?;
    let mut snapshots = BTreeMap::new();
    for workspace in workspaces {
        let Some(workspace_id) = workspace.get("workspace_id").and_then(Value::as_str) else {
            return Err(SourceFailure::new("workspace_list_shape"));
        };
        if workspace.get("label").and_then(Value::as_str).is_none() {
            return Err(SourceFailure::new("workspace_list_shape"));
        }
        let mut snapshot = workspace.clone();
        strip_tokens(&mut snapshot);
        if snapshots
            .insert(workspace_id.to_owned(), snapshot)
            .is_some()
        {
            return Err(SourceFailure::new("workspace_list_duplicate_id"));
        }
    }
    Ok(snapshots)
}

fn sanitize_event_payload(payload: &mut Value) -> Result<(), SourceFailure> {
    let event = payload
        .get("event")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceFailure::new("subscription_event_shape"))?;
    let data = payload
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(|| SourceFailure::new("subscription_event_shape"))?;
    let mut normalized = Map::new();
    for field in [
        "workspace_id",
        "workspace_label",
        "closed_workspace_id",
        "previous_workspace_id",
        "pane_id",
        "tab_id",
        "agent_status",
        "agent",
        "display_agent",
        "host",
        "label",
    ] {
        if let Some(value) = data.get(field).filter(|value| value.is_string()) {
            normalized.insert(field.to_owned(), value.clone());
        }
    }
    for (field, allowed) in [
        ("workspace", &["workspace_id", "label"][..]),
        ("created_workspace", &["workspace_id", "label"][..]),
        ("tab", &["workspace_id", "tab_id"][..]),
        ("pane", &["workspace_id", "pane_id"][..]),
    ] {
        if let Some(object) = data.get(field).and_then(Value::as_object) {
            let mut nested = Map::new();
            for key in allowed {
                if let Some(value) = object.get(*key).filter(|value| value.is_string()) {
                    nested.insert((*key).to_owned(), value.clone());
                }
            }
            normalized.insert(field.to_owned(), Value::Object(nested));
        }
    }
    if let Some(ids) = data.get("workspace_ids").and_then(Value::as_array) {
        normalized.insert(
            "workspace_ids".to_owned(),
            Value::Array(
                ids.iter()
                    .filter(|value| value.is_string())
                    .cloned()
                    .collect(),
            ),
        );
    }
    for field in ["workspace_labels", "state_labels"] {
        if let Some(labels) = data.get(field).and_then(Value::as_object) {
            let labels = labels
                .iter()
                .filter(|(_, value)| value.is_string())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            normalized.insert(field.to_owned(), Value::Object(labels));
        }
    }
    *payload = json!({"event": event, "data": normalized});
    Ok(())
}

fn enrich_status_event(payload: &mut Value, pane_infos: &BTreeMap<String, Value>) {
    let Some(data) = payload.get_mut("data").and_then(Value::as_object_mut) else {
        return;
    };
    let Some(pane_id) = data.get("pane_id").and_then(Value::as_str) else {
        return;
    };
    let Some(pane_info) = pane_infos.get(pane_id) else {
        return;
    };
    let safe_pane = normalized_pane_snapshot(pane_info);
    let Some(pane) = safe_pane.as_object() else {
        return;
    };
    for field in ["workspace_id", "agent", "display_agent", "state_labels"] {
        if !data.contains_key(field)
            && let Some(value) = pane.get(field)
        {
            data.insert(field.to_owned(), value.clone());
        }
    }
}

async fn one_shot_request(socket_path: &Path, request: Value) -> Result<Value, SourceFailure> {
    let expected_id = request
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| SourceFailure::new("request_id_missing"))?;
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|_| SourceFailure::new("socket_connect"))?;
    write_json_line(&mut stream, &request)
        .await
        .map_err(|_| SourceFailure::new("request_write"))?;
    let mut reader = BufReader::new(stream);
    let line = read_line(&mut reader)
        .await
        .map_err(|_| SourceFailure::new("response_read"))?
        .ok_or_else(|| SourceFailure::new("empty_response"))?;
    let response: Value =
        serde_json::from_slice(&line).map_err(|_| SourceFailure::new("response_json"))?;
    if response.get("id").and_then(Value::as_str) != Some(expected_id) {
        return Err(SourceFailure::new("response_id_mismatch"));
    }
    if response.get("error").is_some() {
        return Err(SourceFailure::new("request_rejected"));
    }
    Ok(response)
}

async fn send_and_ack(
    mut stream: UnixStream,
    request_id: &str,
    request: &Value,
) -> Result<BufReader<UnixStream>, SourceFailure> {
    write_json_line(&mut stream, request)
        .await
        .map_err(|_| SourceFailure::new("request_write"))?;
    let mut reader = BufReader::new(stream);
    let line = read_line(&mut reader)
        .await
        .map_err(|_| SourceFailure::new("ack_read"))?
        .ok_or_else(|| SourceFailure::new("empty_ack"))?;
    let ack: Value = serde_json::from_slice(&line).map_err(|_| SourceFailure::new("ack_json"))?;
    if ack.get("id").and_then(Value::as_str) != Some(request_id)
        || ack.pointer("/result/type").and_then(Value::as_str) != Some("subscription_started")
    {
        return Err(SourceFailure::new("subscription_ack_mismatch"));
    }
    Ok(reader)
}

async fn read_line<R>(reader: &mut R) -> io::Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(None);
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(consumed) > MAX_WIRE_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "wire line exceeds bound",
            ));
        }
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            break;
        }
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    if line.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "empty JSON line",
        ));
    }
    Ok(Some(line))
}

async fn write_json_line<W>(writer: &mut W, value: &Value) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    writer.write_all(&bytes).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

fn subscription_request(request_id: &str, pane_ids: &BTreeSet<String>) -> Value {
    let mut subscriptions = [
        "workspace.created",
        "workspace.updated",
        "workspace.metadata_updated",
        "workspace.renamed",
        "workspace.moved",
        "workspace.reordered",
        "workspace.closed",
        "workspace.focused",
        "worktree.created",
        "worktree.opened",
        "worktree.removed",
        "tab.created",
        "tab.closed",
        "tab.focused",
        "tab.renamed",
        "tab.moved",
        "pane.created",
        "pane.closed",
        "pane.updated",
        "pane.focused",
        "pane.moved",
        "pane.exited",
        "pane.agent_detected",
        "layout.updated",
    ]
    .into_iter()
    .map(|kind| json!({"type": kind}))
    .collect::<Vec<_>>();
    subscriptions.extend(
        pane_ids
            .iter()
            .map(|pane_id| json!({"type": "pane.agent_status_changed", "pane_id": pane_id})),
    );
    json!({
        "id": request_id,
        "method": "events.subscribe",
        "params": {"subscriptions": subscriptions}
    })
}

fn changed_pane(kind: &str, subscribed: &BTreeSet<String>, current: &BTreeSet<String>) -> bool {
    match kind {
        "pane_created" | "pane_moved" | "pane_closed" => current != subscribed,
        _ => false,
    }
}

fn refresh_pane_snapshot(kind: &str) -> bool {
    matches!(
        kind,
        "pane_created"
            | "pane_closed"
            | "pane_updated"
            | "pane_moved"
            | "pane_exited"
            | "pane_agent_detected"
            | "pane_agent_status_changed"
    )
}

fn refresh_workspace_snapshot(kind: &str) -> bool {
    matches!(
        kind,
        "workspace_created"
            | "workspace_updated"
            | "workspace_metadata_updated"
            | "workspace_renamed"
            | "workspace_moved"
            | "workspace_reordered"
            | "workspace_closed"
            | "workspace_focused"
            | "tab_created"
            | "tab_closed"
            | "tab_focused"
            | "tab_renamed"
            | "tab_moved"
            | "pane_created"
            | "pane_updated"
            | "pane_focused"
            | "pane_moved"
            | "pane_exited"
            | "pane_agent_detected"
            | "pane_agent_status_changed"
    )
}

fn workspace_ids_in(data: &Map<String, Value>) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for value in [
        data.get("workspace_id"),
        data.get("workspace")
            .and_then(|item| item.get("workspace_id")),
        data.get("pane").and_then(|item| item.get("workspace_id")),
        data.get("tab").and_then(|item| item.get("workspace_id")),
        data.get("created_workspace")
            .and_then(|item| item.get("workspace_id")),
        data.get("closed_workspace_id"),
        data.get("previous_workspace_id"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    {
        ids.insert(value.to_owned());
    }
    if let Some(workspace_ids) = data.get("workspace_ids").and_then(Value::as_array) {
        ids.extend(
            workspace_ids
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned),
        );
    }
    ids
}

fn primary_workspace_id(data: &Map<String, Value>) -> Option<&str> {
    data.get("workspace_id")
        .and_then(Value::as_str)
        .or_else(|| {
            data.get("workspace")
                .and_then(|item| item.get("workspace_id"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            data.get("pane")
                .and_then(|item| item.get("workspace_id"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            data.get("tab")
                .and_then(|item| item.get("workspace_id"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            data.get("created_workspace")
                .and_then(|item| item.get("workspace_id"))
                .and_then(Value::as_str)
        })
}

fn normalized_pane_snapshot(value: &Value) -> Value {
    let Some(pane) = value.as_object() else {
        return Value::Object(Map::new());
    };
    let mut normalized = Map::new();
    for field in [
        "pane_id",
        "workspace_id",
        "tab_id",
        "agent",
        "display_agent",
        "agent_status",
    ] {
        if let Some(value) = pane.get(field).filter(|value| value.is_string()) {
            normalized.insert(field.to_owned(), value.clone());
        }
    }
    if let Some(value) = pane.get("focused").filter(|value| value.is_boolean()) {
        normalized.insert("focused".to_owned(), value.clone());
    }
    if let Some(labels) = pane.get("state_labels").and_then(Value::as_object) {
        normalized.insert(
            "state_labels".to_owned(),
            Value::Object(
                labels
                    .iter()
                    .filter(|(_, value)| value.is_string())
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
        );
    }
    Value::Object(normalized)
}

fn normalized_workspace_snapshot(
    workspace_id: &str,
    value: &Value,
    pane_infos: &BTreeMap<String, Value>,
) -> Value {
    let Some(workspace) = value.as_object() else {
        return Value::Object(Map::new());
    };
    let mut normalized = Map::new();
    for field in ["workspace_id", "label", "active_tab_id", "agent_status"] {
        if let Some(value) = workspace.get(field).filter(|value| value.is_string()) {
            normalized.insert(field.to_owned(), value.clone());
        }
    }
    for field in ["number", "pane_count", "tab_count"] {
        if let Some(value) = workspace.get(field).filter(|value| value.is_number()) {
            normalized.insert(field.to_owned(), value.clone());
        }
    }
    if let Some(value) = workspace.get("focused").filter(|value| value.is_boolean()) {
        normalized.insert("focused".to_owned(), value.clone());
    }
    let panes = pane_infos
        .values()
        .filter(|pane| pane.get("workspace_id").and_then(Value::as_str) == Some(workspace_id))
        .map(normalized_pane_snapshot)
        .collect();
    normalized.insert("panes".to_owned(), Value::Array(panes));
    Value::Object(normalized)
}

fn enrich_workspace_snapshots(
    payload: &mut Value,
    workspace_snapshots: &BTreeMap<String, Value>,
    pane_infos: &BTreeMap<String, Value>,
) {
    let Some(data) = payload.get("data").and_then(Value::as_object) else {
        return;
    };
    let ids = workspace_ids_in(data);
    let primary_id = primary_workspace_id(data).map(ToOwned::to_owned);
    let mut current_snapshots = Map::new();
    let mut current_labels = Map::new();
    for id in ids {
        let Some(snapshot) = workspace_snapshots.get(&id) else {
            continue;
        };
        let snapshot = normalized_workspace_snapshot(&id, snapshot, pane_infos);
        if let Some(label) = snapshot.get("label").and_then(Value::as_str) {
            current_labels.insert(id.clone(), Value::String(label.to_owned()));
        }
        current_snapshots.insert(id, snapshot.clone());
    }

    if let Some(data) = payload.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("workspace_label");
        data.remove("workspace_labels");
        if let Some(label) = primary_id.as_deref().and_then(|id| current_labels.get(id)) {
            data.insert("workspace_label".to_owned(), label.clone());
        }
        if !current_labels.is_empty() {
            data.insert("workspace_labels".to_owned(), Value::Object(current_labels));
        }
    }
    if let Some(payload) = payload.as_object_mut() {
        let bridge = payload
            .entry("bridge")
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(bridge) = bridge.as_object_mut() {
            bridge.insert(
                "workspace_snapshots".to_owned(),
                Value::Object(current_snapshots),
            );
        }
    }
}

fn strip_tokens(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.retain(|key, _| !key.eq_ignore_ascii_case("tokens"));
            for value in object.values_mut() {
                strip_tokens(value);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(strip_tokens),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use tempfile::tempdir;
    use tokio::net::UnixListener;

    use crate::spool::Spool;

    const ACK: &str = include_str!("../tests/fixtures/herdr-v0.8.0-subscription-ack.json");
    const STATUS_EVENT: &str =
        include_str!("../tests/fixtures/herdr-v0.8.0-pane-agent-status-event.json");
    const CLOSED_PANE_CREATED_EVENT: &str =
        include_str!("../tests/fixtures/herdr-v0.8.0-pane-created-event.json");
    const WORKSPACE_LIST_RESPONSE: &str =
        include_str!("../tests/fixtures/herdr-v0.8.0-workspace-list-response.json");

    async fn read_request(stream: &mut UnixStream) -> Value {
        let mut reader = BufReader::new(stream);
        let line = read_line(&mut reader).await.unwrap().unwrap();
        serde_json::from_slice(&line).unwrap()
    }

    async fn respond_workspace_list(listener: &UnixListener) {
        respond_workspace_list_with_status(listener, "working").await;
    }

    async fn respond_workspace_list_with_status(listener: &UnixListener, status: &str) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        assert_eq!(request["method"], "workspace.list");
        let mut response: Value = serde_json::from_str(WORKSPACE_LIST_RESPONSE).unwrap();
        response["id"] = request["id"].clone();
        response["result"]["workspaces"][0]["agent_status"] = json!(status);
        super::write_json_line(&mut stream, &response)
            .await
            .unwrap();
    }

    async fn respond_pane_list(listener: &UnixListener, panes: Value) {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        assert_eq!(request["method"], "pane.list");
        super::write_json_line(
            &mut stream,
            &json!({
                "id": request["id"],
                "result": {"type": "pane_list", "panes": panes}
            }),
        )
        .await
        .unwrap();
    }

    fn synthetic_pane() -> Value {
        json!({
            "pane_id": "pane-1",
            "terminal_id": "terminal-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "focused": true,
            "agent": "claude",
            "title": "fixture task",
            "display_agent": "Claude",
            "agent_status": "working",
            "state_labels": {"phase": "working"},
            "tokens": {},
            "revision": 1
        })
    }

    fn synthetic_idle_pane() -> Value {
        json!({
            "pane_id": "pane-1",
            "terminal_id": "terminal-1",
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "focused": true,
            "agent": "claude",
            "display_agent": "Claude",
            "agent_status": "idle",
            "tokens": {},
            "revision": 2
        })
    }

    #[tokio::test]
    async fn read_line_rejects_oversized_frames_at_the_configured_bound() {
        let mut frame = vec![b'x'; MAX_WIRE_LINE_BYTES + 1];
        frame.push(b'\n');
        let mut reader = BufReader::new(frame.as_slice());
        let error = read_line(&mut reader).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "wire line exceeds bound");
    }

    #[test]
    fn source_event_projection_discards_raw_pane_details_before_spooling() {
        let mut payload = json!({
            "event": "pane.agent_status_changed",
            "data": {
                "pane_id": "pane-1",
                "workspace_id": "workspace-1",
                "agent_status": "idle",
                "agent": "claude",
                "display_agent": "Claude",
                "state_labels": {"phase": "idle", "bad": 7},
                "title": "private pane title",
                "cwd": "/private/worktree",
                "tokens": {"access": "fixture-only-do-not-forward"},
                "private_payload": "fixture-only-do-not-forward"
            },
            "sequence": 999
        });
        sanitize_event_payload(&mut payload).unwrap();
        assert_eq!(
            payload,
            json!({
                "event": "pane.agent_status_changed",
                "data": {
                    "pane_id": "pane-1",
                    "workspace_id": "workspace-1",
                    "agent_status": "idle",
                    "agent": "claude",
                    "display_agent": "Claude",
                    "state_labels": {"phase": "idle"}
                }
            })
        );
        assert!(!payload.to_string().contains("private"));
        assert!(!payload.to_string().contains("tokens"));
    }

    #[test]
    fn pane_move_projection_keeps_both_workspace_ids_and_drops_private_pane_data() {
        let mut payload = json!({
            "event": "pane.moved",
            "data": {
                "previous_workspace_id": "workspace-old",
                "pane": {
                    "workspace_id": "workspace-new",
                    "pane_id": "pane-1",
                    "title": "private pane title",
                    "cwd": "/private/worktree",
                    "tokens": {"access": "fixture-only-do-not-forward"}
                }
            }
        });

        sanitize_event_payload(&mut payload).unwrap();

        let data = payload["data"].as_object().unwrap();
        assert_eq!(data["previous_workspace_id"], "workspace-old");
        assert_eq!(data["pane"]["workspace_id"], "workspace-new");
        assert_eq!(
            workspace_ids_in(data),
            BTreeSet::from(["workspace-new".to_owned(), "workspace-old".to_owned()])
        );
        let workspace_snapshots = BTreeMap::from([
            (
                "workspace-old".to_owned(),
                json!({"workspace_id": "workspace-old", "label": "Old"}),
            ),
            (
                "workspace-new".to_owned(),
                json!({"workspace_id": "workspace-new", "label": "New"}),
            ),
        ]);
        let pane_infos = BTreeMap::from([(
            "pane-1".to_owned(),
            json!({
                "workspace_id": "workspace-new",
                "pane_id": "pane-1",
                "tab_id": "tab-1",
                "agent_status": "working"
            }),
        )]);
        enrich_workspace_snapshots(&mut payload, &workspace_snapshots, &pane_infos);
        let snapshots = payload["bridge"]["workspace_snapshots"]
            .as_object()
            .unwrap();
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots["workspace-new"]["panes"][0]["pane_id"], "pane-1");
        assert_eq!(snapshots["workspace-old"]["panes"], json!([]));
        assert!(!payload.to_string().contains("private"));
        assert!(!payload.to_string().contains("tokens"));
    }

    #[test]
    fn synthetic_status_snapshot_enrichment_uses_only_normalized_fields() {
        let mut event = json!({
            "event": "pane.agent_status_changed",
            "data": {"pane_id": "pane-1", "agent_status": "working"}
        });
        let pane_infos = BTreeMap::from([(
            "pane-1".to_owned(),
            json!({
                "workspace_id": "workspace-1",
                "agent": "claude",
                "display_agent": "Claude",
                "title": "private pane title",
                "state_labels": {"phase": "working"}
            }),
        )]);

        enrich_status_event(&mut event, &pane_infos);

        assert_eq!(event["data"]["workspace_id"], "workspace-1");
        assert_eq!(event["data"]["agent"], "claude");
        assert_eq!(event["data"]["display_agent"], "Claude");
        assert_eq!(event["data"]["state_labels"]["phase"], "working");
        assert!(event["data"].get("title").is_none());
    }

    #[test]
    fn stale_workspace_event_uses_the_authoritative_snapshot() {
        let snapshots = BTreeMap::from([(
            "workspace-1".to_owned(),
            json!({
                "workspace_id": "workspace-1",
                "label": "Current label",
                "pane_count": 3,
                "tokens": {"synthetic_private_value": "fixture-only-do-not-forward"}
            }),
        )]);
        let mut rename = json!({
            "event": "workspace_renamed",
            "data": {"workspace_id": "workspace-1", "label": "Stale retained label"}
        });

        enrich_workspace_snapshots(&mut rename, &snapshots, &BTreeMap::new());
        strip_tokens(&mut rename);

        assert_eq!(rename["data"]["workspace_label"], "Current label");
        assert_eq!(
            rename["bridge"]["workspace_snapshots"]["workspace-1"]["label"],
            "Current label"
        );
        assert!(!rename.to_string().contains("fixture-only-do-not-forward"));
        assert!(
            rename["bridge"]["workspace_snapshots"]["workspace-1"]
                .get("worktree")
                .is_none()
        );
    }

    #[test]
    fn workspace_snapshot_refresh_covers_all_thread_mutation_events() {
        for kind in [
            "workspace_created",
            "workspace_renamed",
            "workspace_closed",
            "tab_created",
            "pane_created",
            "pane_agent_status_changed",
        ] {
            assert!(refresh_workspace_snapshot(kind), "{kind}");
        }
        assert!(refresh_pane_snapshot("pane_agent_status_changed"));
        for kind in [
            "pane_output_changed",
            "pane_scroll_changed",
            "worktree_opened",
        ] {
            assert!(!refresh_workspace_snapshot(kind), "{kind}");
        }
    }

    #[tokio::test]
    async fn durably_captured_unmapped_event_recovers_from_current_startup_snapshots() {
        let dir = tempdir().unwrap();
        let spool = Arc::new(Mutex::new(Spool::open(dir.path().join("state")).unwrap()));
        let received_at = Utc::now() - chrono::Duration::seconds(10);
        let raw_event = json!({
            "event": "workspace.renamed",
            "data": {"workspace_id": "workspace-1", "workspace_label": "stale label"}
        });
        spool
            .lock()
            .await
            .append_unmapped_upstream_event(raw_event, received_at)
            .unwrap();
        let pane_infos = BTreeMap::new();
        let workspace_snapshots = BTreeMap::from([(
            "workspace-1".to_owned(),
            json!({
                "workspace_id": "workspace-1",
                "label": "authoritative current label",
                "tokens": {"fixture_only": "do-not-forward"}
            }),
        )]);

        resolve_unmapped_events(&spool, &pane_infos, &workspace_snapshots)
            .await
            .unwrap();
        let mut spool_guard = spool.lock().await;
        assert_eq!(spool_guard.unmapped_event_payloads().unwrap(), Vec::new());
        assert!(spool_guard.flush_due(Utc::now()).unwrap());
        let batch = spool_guard.oldest_batch().unwrap().unwrap();
        assert!(batch.body.contains("authoritative current label"));
        assert!(!batch.body.contains("stale label"));
        assert!(!batch.body.contains("do-not-forward"));
        drop(spool_guard);
    }

    #[tokio::test]
    async fn unmapped_source_status_recovery_preserves_event_context_and_omitted_labels() {
        let dir = tempdir().unwrap();
        let spool = Arc::new(Mutex::new(Spool::open(dir.path().join("state")).unwrap()));
        let mut source_event = json!({
            "event": "pane.agent_status_changed",
            "data": {
                "pane_id": "pane-1",
                "workspace_id": "workspace-at-event",
                "agent_status": "working",
                "agent": "event-agent",
                "display_agent": "Event Agent",
                "title": "private title",
                "tokens": {"fixture": "do-not-forward"}
            }
        });
        sanitize_event_payload(&mut source_event).unwrap();
        spool
            .lock()
            .await
            .append_unmapped_upstream_event(source_event, Utc::now())
            .unwrap();
        let pane_infos = BTreeMap::from([(
            "pane-1".to_owned(),
            json!({
                "pane_id": "pane-1",
                "workspace_id": "workspace-current",
                "agent_status": "idle",
                "agent": "current-agent",
                "display_agent": "Current Agent",
                "state_labels": {"phase": "current"}
            }),
        )]);
        let workspace_snapshots = BTreeMap::from([
            (
                "workspace-at-event".to_owned(),
                json!({"workspace_id": "workspace-at-event", "label": "At event"}),
            ),
            (
                "workspace-current".to_owned(),
                json!({"workspace_id": "workspace-current", "label": "Current"}),
            ),
        ]);

        resolve_unmapped_events(&spool, &pane_infos, &workspace_snapshots)
            .await
            .unwrap();

        let mut spool = spool.lock().await;
        assert_eq!(spool.unmapped_event_payloads().unwrap(), Vec::new());
        assert!(
            spool
                .flush_due(Utc::now() + chrono::Duration::seconds(5))
                .unwrap()
        );
        let batch = spool.oldest_batch().unwrap().unwrap();
        drop(spool);
        let value: Value = serde_json::from_str(&batch.body).unwrap();
        let messages = value["mutations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|mutation| mutation["kind"] == "append_message")
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["message"]["body"], "event-agent: working");
        assert_eq!(
            messages[0]["message"]["metadata"]["state_labels"],
            json!({})
        );
        assert!(!batch.body.contains("current-agent"));
        assert!(!batch.body.contains("private title"));
        assert!(!batch.body.contains("do-not-forward"));
    }

    #[tokio::test]
    async fn interrupted_status_snapshot_reconciles_to_current_pane_state() {
        let dir = tempdir().unwrap();
        let spool = Arc::new(Mutex::new(Spool::open(dir.path().join("state")).unwrap()));
        spool
            .lock()
            .await
            .append_unmapped_upstream_event(
                json!({
                    "event": "pane.agent_status_changed",
                    "data": {"pane_id": "pane-1", "agent_status": "working"},
                    "bridge": {"status_snapshot": true}
                }),
                Utc::now() - chrono::Duration::seconds(10),
            )
            .unwrap();
        let pane_infos = BTreeMap::from([("pane-1".to_owned(), synthetic_idle_pane())]);
        let workspace_snapshots = BTreeMap::from([(
            "workspace-1".to_owned(),
            json!({"workspace_id": "workspace-1", "label": "Iris Pilot"}),
        )]);

        resolve_unmapped_events(&spool, &pane_infos, &workspace_snapshots)
            .await
            .unwrap();
        let mut spool = spool.lock().await;
        assert_eq!(spool.unmapped_event_payloads().unwrap(), Vec::new());
        assert!(spool.flush_due(Utc::now()).unwrap());
        let batch = spool.oldest_batch().unwrap().unwrap();
        drop(spool);
        let value: Value = serde_json::from_str(&batch.body).unwrap();
        let messages = value["mutations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|mutation| mutation["kind"] == "append_message")
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["message"]["body"], "claude: idle");
        assert!(!batch.body.contains("private pane title"));
        assert!(!batch.body.contains("/private/"));
    }

    #[tokio::test]
    async fn source_derived_ack_stream_and_eof_are_consumed_and_spooled() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert_eq!(request["method"], "events.subscribe");
            assert_eq!(request["id"], "fixture-subscription");
            assert!(
                request["params"]["subscriptions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| {
                        item["type"] == "pane.agent_status_changed" && item["pane_id"] == "pane-1"
                    })
            );
            let ack = ACK.replace("fixture-id", "fixture-subscription");
            stream.write_all(ack.as_bytes()).await.unwrap();
            stream.write_all(b"\n").await.unwrap();
            stream.write_all(STATUS_EVENT.as_bytes()).await.unwrap();
            stream.write_all(b"\n").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let stream = UnixStream::connect(&socket_path).await.unwrap();
        let request_id = "fixture-subscription";
        let pane_ids = BTreeSet::from(["pane-1".to_owned()]);
        let request = subscription_request(request_id, &pane_ids);
        let mut reader = send_and_ack(stream, request_id, &request).await.unwrap();
        let line = read_line(&mut reader).await.unwrap().unwrap();
        let mut event: Value = serde_json::from_slice(&line).unwrap();
        sanitize_event_payload(&mut event).unwrap();
        let spool = Arc::new(Mutex::new(Spool::open(dir.path().join("state")).unwrap()));
        spool
            .lock()
            .await
            .append_upstream_event(event, Utc::now())
            .unwrap();
        assert!(read_line(&mut reader).await.unwrap().is_none());
        server.await.unwrap();
        assert_eq!(spool.lock().await.pending_event_count(), 1);
        let event_files = std::fs::read_dir(dir.path().join("state"))
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
            .count();
        assert_eq!(event_files, 2, "event plus durable state metadata");
    }

    #[tokio::test]
    async fn new_subscription_captures_current_pane_status_without_a_transition() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut ping_stream, _) = listener.accept().await.unwrap();
            let ping = read_request(&mut ping_stream).await;
            super::write_json_line(
                &mut ping_stream,
                &json!({
                    "id": ping["id"],
                    "result": {"type": "pong", "version": "0.8.0", "protocol": 19}
                }),
            )
            .await
            .unwrap();
            drop(ping_stream);

            respond_pane_list(&listener, json!([synthetic_pane()])).await;
            respond_workspace_list(&listener).await;

            let (mut event_stream, _) = listener.accept().await.unwrap();
            let subscribe = read_request(&mut event_stream).await;
            let status_subscription = subscribe["params"]["subscriptions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| {
                    item["type"] == "pane.agent_status_changed" && item["pane_id"] == "pane-1"
                })
                .unwrap();
            assert!(status_subscription.get("agent_status").is_none());
            let ack = ACK.replace("fixture-id", subscribe["id"].as_str().unwrap());
            event_stream.write_all(ack.as_bytes()).await.unwrap();
            event_stream.write_all(b"\n").await.unwrap();

            // No transition is sent: the current pane snapshot must still
            // produce a normalized status observation on connect.
            respond_pane_list(&listener, json!([synthetic_pane()])).await;
            respond_workspace_list(&listener).await;
            event_stream.shutdown().await.unwrap();
        });

        let spool = Arc::new(Mutex::new(Spool::open(dir.path().join("state")).unwrap()));
        assert_eq!(
            consume_subscription(&socket_path, &spool).await.unwrap(),
            SubscriptionEnd::PeerClosed
        );
        server.await.unwrap();
        let mut spool = spool.lock().await;
        assert_eq!(spool.pending_event_count(), 1);
        assert!(
            spool
                .flush_due(Utc::now() + chrono::Duration::seconds(6))
                .unwrap()
        );
        let batch_body = spool.oldest_batch().unwrap().unwrap().body;
        drop(spool);
        let value: Value = serde_json::from_str(&batch_body).unwrap();
        let messages = value["mutations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|mutation| mutation["kind"] == "append_message")
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]["message"]["body"]
                .as_str()
                .is_some_and(|body| body.contains("claude: working"))
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn protocol_19_transition_is_spooled_and_reconnect_snapshots_current_status() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let current_pane = if attempt == 0 {
                    synthetic_pane()
                } else {
                    synthetic_idle_pane()
                };
                let (mut ping_stream, _) = listener.accept().await.unwrap();
                let ping_request = read_request(&mut ping_stream).await;
                assert_eq!(ping_request["method"], "ping");
                super::write_json_line(
                    &mut ping_stream,
                    &json!({
                        "id": ping_request["id"],
                        "result": {"type": "pong", "version": "0.8.0", "protocol": 19}
                    }),
                )
                .await
                .unwrap();
                drop(ping_stream);

                respond_pane_list(&listener, json!([current_pane.clone()])).await;
                let workspace_status = if attempt == 0 { "working" } else { "idle" };
                respond_workspace_list_with_status(&listener, workspace_status).await;

                let (mut event_stream, _) = listener.accept().await.unwrap();
                let subscribe_request = read_request(&mut event_stream).await;
                assert_eq!(subscribe_request["method"], "events.subscribe");
                assert!(
                    subscribe_request["params"]["subscriptions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|item| {
                            item["type"] == "pane.agent_status_changed"
                                && item["pane_id"] == "pane-1"
                        })
                );
                let ack = ACK.replace("fixture-id", subscribe_request["id"].as_str().unwrap());
                event_stream.write_all(ack.as_bytes()).await.unwrap();
                event_stream.write_all(&[10]).await.unwrap();
                if attempt == 0 {
                    event_stream
                        .write_all(STATUS_EVENT.as_bytes())
                        .await
                        .unwrap();
                    event_stream.write_all(&[10]).await.unwrap();
                }
                event_stream.shutdown().await.unwrap();
                respond_pane_list(&listener, json!([current_pane])).await;
                let current_status = if attempt == 0 { "working" } else { "idle" };
                respond_workspace_list_with_status(&listener, current_status).await;
                if attempt == 0 {
                    // The genuine status transition refreshes workspace and
                    // pane snapshots before its sanitized record is sealed.
                    respond_workspace_list_with_status(&listener, "idle").await;
                    respond_pane_list(&listener, json!([synthetic_idle_pane()])).await;
                }
            }
        });

        let state_dir = dir.path().join("state");
        let spool = Arc::new(Mutex::new(Spool::open(state_dir.clone()).unwrap()));
        assert_eq!(
            consume_subscription(&socket_path, &spool).await.unwrap(),
            SubscriptionEnd::PeerClosed
        );
        drop(spool);
        let spool = Arc::new(Mutex::new(Spool::open(state_dir.clone()).unwrap()));
        assert_eq!(
            consume_subscription(&socket_path, &spool).await.unwrap(),
            SubscriptionEnd::PeerClosed
        );
        server.await.unwrap();
        let mut spool = spool.lock().await;
        assert_eq!(spool.pending_event_count(), 3);
        let stored_events = std::fs::read_dir(&state_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("event-"))
            .map(|entry| {
                serde_json::from_slice::<Value>(&std::fs::read(entry.path()).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(stored_events.len(), 3);
        assert!(stored_events.iter().all(|event| {
            !event.to_string().contains("fixture-only-do-not-forward")
                && !event.to_string().contains("tokens")
                && !event
                    .to_string()
                    .contains("private-pane-title-do-not-forward")
                && !event.to_string().contains("/private/")
                && !event.to_string().contains("worktree")
        }));
        assert!(
            spool
                .flush_due(Utc::now() + chrono::Duration::seconds(5))
                .unwrap()
        );
        let batch = spool.oldest_batch().unwrap().unwrap();
        let value: Value = serde_json::from_str(&batch.body).unwrap();
        assert_eq!(value["audit"]["metadata"]["dropped_count"], 0);
        assert_eq!(
            value["audit"]["metadata"]["suppressed_thread_upsert_count"],
            0
        );
        assert!(
            value["mutations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|mutation| {
                    mutation["kind"] == "upsert_thread" && mutation["title"] == "Iris Pilot"
                })
        );
        assert!(!batch.body.contains("fixture-only-do-not-forward"));
        assert!(!batch.body.contains("private-pane-title-do-not-forward"));
        assert!(!batch.body.contains("/private/"));
        assert!(!batch.body.contains("worktree"));
        let threads = value["mutations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|mutation| mutation["kind"] == "upsert_thread")
            .collect::<Vec<_>>();
        assert_ne!(threads, Vec::<&Value>::new());
        assert!(threads.iter().all(|thread| {
            thread["metadata"]["panes"][0]["title"].is_null()
                && thread["metadata"]["panes"][0]["pane_id"] == "pane-1"
                && thread["metadata"]["panes"][0]["agent_status"].is_string()
        }));
        let messages = value["mutations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|mutation| mutation["kind"] == "append_message")
            .collect::<Vec<_>>();
        assert_eq!(messages.len(), 3);
        let idle_messages = messages
            .iter()
            .filter(|mutation| mutation["message"]["body"] == "claude: idle")
            .collect::<Vec<_>>();
        assert_eq!(idle_messages.len(), 2);
        assert_ne!(
            idle_messages[0]["message"]["source_id"],
            idle_messages[1]["message"]["source_id"]
        );
        assert!(
            messages.iter().any(|mutation| {
                mutation["message"]["body"] == "claude: working (phase=working)"
            })
        );
        assert!(
            messages
                .iter()
                .all(|mutation| { mutation["message"]["metadata"].get("data").is_none() })
        );
        drop(spool);
    }

    #[tokio::test]
    async fn retained_pane_created_for_a_closed_pane_does_not_trigger_refresh() {
        let dir = tempdir().unwrap();
        let socket_path = dir.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut ping_stream, _) = listener.accept().await.unwrap();
            let ping = read_request(&mut ping_stream).await;
            super::write_json_line(
                &mut ping_stream,
                &json!({
                    "id": ping["id"],
                    "result": {"type": "pong", "version": "0.8.0", "protocol": 19}
                }),
            )
            .await
            .unwrap();
            drop(ping_stream);

            let (mut panes_stream, _) = listener.accept().await.unwrap();
            let panes = read_request(&mut panes_stream).await;
            assert_eq!(panes["method"], "pane.list");
            super::write_json_line(
                &mut panes_stream,
                &json!({"id": panes["id"], "result": {"type": "pane_list", "panes": []}}),
            )
            .await
            .unwrap();
            drop(panes_stream);

            respond_workspace_list(&listener).await;

            let (mut event_stream, _) = listener.accept().await.unwrap();
            let subscribe = read_request(&mut event_stream).await;
            assert_eq!(subscribe["method"], "events.subscribe");
            let ack = ACK.replace("fixture-id", subscribe["id"].as_str().unwrap());
            event_stream.write_all(ack.as_bytes()).await.unwrap();
            event_stream.write_all(b"\n").await.unwrap();
            respond_pane_list(&listener, json!([])).await;
            respond_workspace_list(&listener).await;
            event_stream
                .write_all(CLOSED_PANE_CREATED_EVENT.trim_end().as_bytes())
                .await
                .unwrap();
            event_stream.write_all(b"\n").await.unwrap();
            respond_workspace_list(&listener).await;

            // The retained pane.created frame is stale. The bridge must ask
            // for current workspace metadata and pane.list before deciding
            // whether to refresh the pane-scoped subscription.
            let (mut refresh_stream, _) = listener.accept().await.unwrap();
            let refresh = read_request(&mut refresh_stream).await;
            assert_eq!(refresh["method"], "pane.list");
            super::write_json_line(
                &mut refresh_stream,
                &json!({
                    "id": refresh["id"],
                    "result": {"type": "pane_list", "panes": []}
                }),
            )
            .await
            .unwrap();
            drop(refresh_stream);
            event_stream.shutdown().await.unwrap();
        });

        let spool = Arc::new(Mutex::new(Spool::open(dir.path().join("state")).unwrap()));
        assert_eq!(
            consume_subscription(&socket_path, &spool).await.unwrap(),
            SubscriptionEnd::PeerClosed
        );
        server.await.unwrap();
        assert_eq!(spool.lock().await.pending_event_count(), 1);
    }

    #[test]
    fn planned_pane_refresh_is_immediate_but_peer_close_retries_after_two_seconds() {
        assert_eq!(
            reconnect_delay(SubscriptionEnd::PaneSetChanged),
            Duration::ZERO
        );
        assert_eq!(
            reconnect_delay(SubscriptionEnd::PeerClosed),
            RECONNECT_DELAY
        );
    }

    #[test]
    fn newly_created_pane_requests_subscription_refresh() {
        let subscribed = BTreeSet::from(["old-pane".to_owned()]);
        let current = BTreeSet::from(["old-pane".to_owned(), "new-pane".to_owned()]);
        assert!(changed_pane("pane_created", &subscribed, &current));
        assert!(!changed_pane("pane_created", &current, &current));
        assert!(!changed_pane(
            "pane_created",
            &subscribed,
            &BTreeSet::from(["old-pane".to_owned()])
        ));
    }

    #[test]
    fn cross_workspace_pane_move_keeps_the_same_pane_id_subscription() {
        let pane_id = BTreeSet::from(["pane-1".to_owned()]);
        assert!(!changed_pane("pane_moved", &pane_id, &pane_id));
        let replacement_id = BTreeSet::from(["replacement-pane".to_owned()]);
        assert!(changed_pane("pane_moved", &pane_id, &replacement_id));
    }

    #[test]
    fn every_protocol_event_subscription_is_type_scoped_and_status_is_pane_scoped() {
        let request = subscription_request("request", &BTreeSet::from(["pane-1".to_owned()]));
        let subscriptions = request["params"]["subscriptions"].as_array().unwrap();
        assert!(
            subscriptions
                .iter()
                .any(|entry| entry["type"] == "workspace.created")
        );
        assert!(subscriptions.iter().any(|entry| {
            entry["type"] == "pane.agent_status_changed" && entry["pane_id"] == "pane-1"
        }));
        assert!(
            subscriptions
                .iter()
                .filter(|entry| entry["type"] == "pane.agent_status_changed")
                .all(|entry| entry.get("agent_status").is_none())
        );
    }
}

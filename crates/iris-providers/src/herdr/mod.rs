//! Pure Herdr protocol-19 event mapping.
//!
//! Herdr is the system of record for terminal workspaces. This module turns its
//! push-event payloads into deterministic Iris write intents; the HTTP ingest
//! seam owns authentication, persistence, audit, and delivery.

use chrono::{DateTime, Utc};
use iris_core::{Contact, Message, MessageKind, Thread};
use serde_json::{Map, Value, json};
use uuid::Uuid;

const SOURCE: &str = "herdr";
const UUID_NAMESPACE: Uuid = Uuid::from_u128(0xc8a4_bec7_f33d_4fb0_93e1_c45e_0dd2_5cf7);

/// A Herdr event paired with bridge-owned replay identity and receipt time.
///
/// Protocol 19 event payloads do not contain an event ID or timestamp. The
/// bridge must attach both before submitting an event so persistence can dedupe
/// replayed spool entries without treating a later identical status update as a
/// duplicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdrEvent<'a> {
    /// Stable bridge event ID used as the idempotency key.
    pub event_id: &'a str,
    /// Protocol-19 `{ event, data }` JSON payload.
    pub payload: &'a Value,
    /// Time the bridge received the event.
    pub received_at: DateTime<Utc>,
}

/// A deterministic Iris persistence intent produced from a Herdr event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HerdrIntent {
    /// Create or update a workspace-backed Iris thread.
    UpsertThread(Thread),
    /// Mark a workspace-backed thread as archived.
    ArchiveThread { thread_id: Uuid, source_id: String },
    /// Create or update a detected agent contact.
    UpsertContact(Contact),
    /// Append an agent-status system message.
    AppendMessage(Message),
    /// Explicitly ignored protocol event, retained for structured logging.
    Dropped { kind: String, reason: &'static str },
}

/// Result of mapping one event, including its persistence dedupe key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdrMapping {
    /// Stable key for deduplicating the bridge's replayed event.
    pub dedupe_key: String,
    /// Ordered writes: contacts and threads precede messages that refer to them.
    pub intents: Vec<HerdrIntent>,
}

/// Maps one schema-tolerant protocol-19 event into ordered Iris write intents.
///
/// Unknown future event kinds and malformed payloads are deliberately dropped,
/// never panicked. Known event kinds with the fields required by their mapping
/// are converted into deterministic UUIDv5-backed Iris models.
#[allow(clippy::map_unwrap_or, clippy::too_many_lines)]
pub fn map_event(event: &HerdrEvent<'_>) -> HerdrMapping {
    if event.event_id.trim().is_empty() {
        return HerdrMapping {
            dedupe_key: "herdr:invalid-event-id".to_owned(),
            intents: dropped("unknown", "missing bridge event_id"),
        };
    }
    let data = event.payload.get("data").and_then(Value::as_object);
    let kind = event
        .payload
        .get("event")
        .and_then(Value::as_str)
        .or_else(|| {
            data.and_then(|value| value.get("type"))
                .and_then(Value::as_str)
        })
        .unwrap_or("unknown")
        .replace('.', "_");
    let dedupe_key = format!("herdr:{}", event.event_id);

    let intents = match (kind.as_str(), data) {
        ("workspace_created" | "workspace_updated" | "workspace_metadata_updated", Some(data)) => {
            data.get("workspace")
                .and_then(|workspace| workspace_thread(workspace, event.received_at))
                .map(|thread| vec![HerdrIntent::UpsertThread(thread)])
                .unwrap_or_else(|| dropped(&kind, "missing workspace identity"))
        }
        ("workspace_renamed", Some(data)) => workspace_id(data)
            .map(|id| {
                vec![HerdrIntent::UpsertThread(thread_with_metadata(
                    id,
                    data.get("label").and_then(Value::as_str),
                    event.received_at,
                    json!({"event": kind}),
                ))]
            })
            .unwrap_or_else(|| dropped(&kind, "missing workspace_id")),
        ("workspace_closed", Some(data)) => workspace_id(data)
            .map(|id| {
                vec![HerdrIntent::ArchiveThread {
                    thread_id: thread_uuid(id),
                    source_id: id.to_owned(),
                }]
            })
            .unwrap_or_else(|| dropped(&kind, "missing workspace_id")),
        ("workspace_moved" | "workspace_reordered" | "workspace_focused", Some(data)) => {
            let ids = workspace_ids(data);
            if ids.is_empty() {
                dropped(&kind, "missing workspace identity")
            } else {
                ids.into_iter()
                    .map(|id| {
                        HerdrIntent::UpsertThread(thread_with_metadata(
                            id,
                            None,
                            event.received_at,
                            json!({"event": kind, "data": canonical_value(&Value::Object(data.clone()))}),
                        ))
                    })
                    .collect()
            }
        }
        (
            "tab_created" | "tab_closed" | "tab_renamed" | "tab_moved" | "tab_focused",
            Some(data),
        ) => workspace_id_from_tab_event(data)
            .map(|id| {
                vec![HerdrIntent::UpsertThread(thread_with_metadata(
                    id,
                    None,
                    event.received_at,
                    json!({"event": kind, "data": canonical_value(&Value::Object(data.clone()))}),
                ))]
            })
            .unwrap_or_else(|| dropped(&kind, "missing workspace_id")),
        ("pane_moved", Some(data)) => map_pane_moved(&kind, data, event.received_at),
        (
            "pane_created"
            | "pane_updated"
            | "pane_focused"
            | "pane_exited"
            | "pane_agent_detected",
            Some(data),
        ) => workspace_id_from_pane_event(data)
            .map(|id| {
                vec![HerdrIntent::UpsertThread(thread_with_metadata(
                    id,
                    None,
                    event.received_at,
                    json!({"event": kind, "data": canonical_value(&Value::Object(data.clone()))}),
                ))]
            })
            .unwrap_or_else(|| dropped(&kind, "missing workspace_id")),
        ("pane_agent_status_changed", Some(data)) => map_agent_status(&kind, data, event),
        (
            "worktree_created"
            | "worktree_opened"
            | "worktree_removed"
            | "layout_updated"
            | "pane_output_changed"
            | "pane_output_matched"
            | "pane_scroll_changed"
            | "pane_closed",
            _,
        ) => dropped(&kind, "out of scope for v1"),
        _ => dropped(&kind, "unknown or malformed protocol event"),
    };

    HerdrMapping {
        dedupe_key,
        intents,
    }
}

fn map_agent_status(
    kind: &str,
    data: &Map<String, Value>,
    event: &HerdrEvent<'_>,
) -> Vec<HerdrIntent> {
    let Some(workspace_id) = workspace_id(data) else {
        return dropped(kind, "missing workspace_id");
    };
    let Some(pane_id) = pane_id(data).filter(|id| !id.trim().is_empty()) else {
        return dropped(kind, "missing pane_id");
    };
    let status = data
        .get("agent_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let agent = data
        .get("agent")
        .and_then(Value::as_str)
        .or_else(|| data.get("display_agent").and_then(Value::as_str))
        .unwrap_or("unknown");
    let host = data
        .get("host")
        .and_then(Value::as_str)
        .unwrap_or("unknown-host");
    let contact = agent_contact(agent, host);
    let labels = sorted_object(data.get("state_labels").and_then(Value::as_object));
    let body = format!("{agent}: {status}{}", labels_suffix(&labels));
    let message_source_id = event.event_id.to_owned();
    let message = Message {
        id: message_uuid(&message_source_id),
        thread_id: thread_uuid(workspace_id),
        source: SOURCE.to_owned(),
        source_id: message_source_id,
        sender: contact.clone(),
        kind: MessageKind::System,
        body,
        attachments: Vec::new(),
        timestamp: event.received_at,
        is_outbound: false,
        metadata: json!({
            "event": kind,
            "pane_id": pane_id,
            "agent_status": status,
            "state_labels": labels,
            "data": canonical_value(&Value::Object(data.clone())),
        }),
    };
    vec![
        HerdrIntent::UpsertContact(contact),
        HerdrIntent::UpsertThread(thread_with_metadata(
            workspace_id,
            None,
            event.received_at,
            json!({"event": kind, "data": canonical_value(&Value::Object(data.clone()))}),
        )),
        HerdrIntent::AppendMessage(message),
    ]
}

fn map_pane_moved(
    kind: &str,
    data: &Map<String, Value>,
    received_at: DateTime<Utc>,
) -> Vec<HerdrIntent> {
    let mut intents = Vec::new();
    if let Some(workspace) = data.get("created_workspace")
        && let Some(thread) = workspace_thread(workspace, received_at)
    {
        intents.push(HerdrIntent::UpsertThread(thread));
    }
    if let Some(closed_workspace_id) = data.get("closed_workspace_id").and_then(Value::as_str) {
        intents.push(HerdrIntent::ArchiveThread {
            thread_id: thread_uuid(closed_workspace_id),
            source_id: closed_workspace_id.to_owned(),
        });
    }
    if let Some(workspace_id) = workspace_id_from_pane_event(data) {
        intents.push(HerdrIntent::UpsertThread(thread_with_metadata(
            workspace_id,
            None,
            received_at,
            json!({"event": kind, "data": canonical_value(&Value::Object(data.clone()))}),
        )));
    }
    if intents.is_empty() {
        dropped(kind, "missing workspace identity")
    } else {
        intents
    }
}

fn workspace_thread(value: &Value, received_at: DateTime<Utc>) -> Option<Thread> {
    let workspace = value.as_object()?;
    let id = workspace_id(workspace)?;
    Some(thread_with_metadata(
        id,
        workspace.get("label").and_then(Value::as_str),
        received_at,
        Value::Object(sorted_object(Some(workspace))),
    ))
}

fn thread_with_metadata(
    id: &str,
    title: Option<&str>,
    at: DateTime<Utc>,
    metadata: Value,
) -> Thread {
    Thread {
        id: thread_uuid(id),
        source: SOURCE.to_owned(),
        source_id: id.to_owned(),
        title: title.map(ToOwned::to_owned),
        participants: Vec::new(),
        last_message_at: at,
        unread_count: None,
        metadata,
    }
}

fn agent_contact(agent: &str, host: &str) -> Contact {
    let source_id = format!("{host}:{agent}");
    Contact {
        id: contact_uuid(&source_id),
        source: SOURCE.to_owned(),
        source_id,
        display_name: Some(agent.to_owned()),
        avatar_url: None,
        metadata: json!({"host": host, "agent": agent}),
    }
}

fn workspace_id(data: &Map<String, Value>) -> Option<&str> {
    data.get("workspace_id")
        .and_then(Value::as_str)
        .or_else(|| {
            data.get("workspace")
                .and_then(|v| v.get("workspace_id"))
                .and_then(Value::as_str)
        })
}

fn workspace_ids(data: &Map<String, Value>) -> Vec<&str> {
    if let Some(id) = workspace_id(data) {
        return vec![id];
    }
    data.get("workspace_ids")
        .and_then(Value::as_array)
        .map(|ids| ids.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

fn workspace_id_from_tab_event(data: &Map<String, Value>) -> Option<&str> {
    workspace_id(data).or_else(|| {
        data.get("tab")
            .and_then(|v| v.get("workspace_id"))
            .and_then(Value::as_str)
    })
}

fn workspace_id_from_pane_event(data: &Map<String, Value>) -> Option<&str> {
    workspace_id(data).or_else(|| {
        data.get("pane")
            .and_then(|v| v.get("workspace_id"))
            .and_then(Value::as_str)
    })
}

fn pane_id(data: &Map<String, Value>) -> Option<&str> {
    data.get("pane_id").and_then(Value::as_str).or_else(|| {
        data.get("pane")
            .and_then(|v| v.get("pane_id"))
            .and_then(Value::as_str)
    })
}

fn sorted_object(object: Option<&Map<String, Value>>) -> Map<String, Value> {
    let mut entries: Vec<_> = object
        .into_iter()
        .flat_map(|object| object.iter())
        .collect();
    entries.sort_unstable_by_key(|(left, _)| *left);
    entries
        .into_iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), canonical_value(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(canonical_value).collect()),
        value => value.clone(),
    }
}

fn labels_suffix(labels: &Map<String, Value>) -> String {
    if labels.is_empty() {
        String::new()
    } else {
        let rendered = labels
            .iter()
            .filter_map(|(key, value)| value.as_str().map(|value| format!("{key}={value}")))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" ({rendered})")
    }
}

fn dropped(kind: &str, reason: &'static str) -> Vec<HerdrIntent> {
    vec![HerdrIntent::Dropped {
        kind: kind.to_owned(),
        reason,
    }]
}

fn thread_uuid(source_id: &str) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, format!("thread:{source_id}").as_bytes())
}

fn contact_uuid(source_id: &str) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, format!("contact:{source_id}").as_bytes())
}

fn message_uuid(source_id: &str) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, format!("message:{source_id}").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    #[allow(clippy::needless_pass_by_value)]
    fn map(id: &str, event: &str, data: Value) -> HerdrMapping {
        map_event(&HerdrEvent {
            event_id: id,
            payload: &json!({"event": event, "data": data}),
            received_at: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        })
    }

    #[test]
    fn maps_agent_status_with_deterministic_contact_thread_and_message() {
        let mapping = map(
            "evt-1",
            "pane_agent_status_changed",
            json!({
                "workspace_id": "workspace-1", "pane_id": "pane-2", "agent": "codex",
                "agent_status": "working", "state_labels": {"branch": "main", "task": "COD-438"}
            }),
        );
        assert_eq!(mapping.dedupe_key, "herdr:evt-1");
        assert_eq!(mapping.intents.len(), 3);
        let HerdrIntent::AppendMessage(message) = &mapping.intents[2] else {
            panic!("message intent")
        };
        assert_eq!(message.body, "codex: working (branch=main, task=COD-438)");
        assert_eq!(message.sender.source_id, "unknown-host:codex");
        assert_eq!(message.source_id, "evt-1");
        assert_eq!(message.thread_id, thread_uuid("workspace-1"));
    }

    #[test]
    fn accepts_dotted_kind_and_data_type_fallback_and_rejects_invalid_bridge_identity() {
        let dotted = map(
            "evt-dotted",
            "pane.agent_status_changed",
            json!({
                "workspace_id":"w", "pane_id":"p", "agent_status":"idle"
            }),
        );
        assert!(matches!(
            dotted.intents.last(),
            Some(HerdrIntent::AppendMessage(_))
        ));
        let fallback_payload = json!({"data":{"type":"pane_agent_status_changed","workspace_id":"w","pane_id":"p","agent_status":"idle"}});
        let fallback = map_event(&HerdrEvent {
            event_id: "evt-fallback",
            payload: &fallback_payload,
            received_at: Utc::now(),
        });
        assert!(matches!(
            fallback.intents.last(),
            Some(HerdrIntent::AppendMessage(_))
        ));
        let empty = map_event(&HerdrEvent {
            event_id: " ",
            payload: &fallback_payload,
            received_at: Utc::now(),
        });
        assert!(matches!(
            empty.intents.as_slice(),
            [HerdrIntent::Dropped {
                reason: "missing bridge event_id",
                ..
            }]
        ));
    }

    #[test]
    fn pane_moved_preserves_embedded_workspace_lifecycle() {
        let mapping = map(
            "move-1",
            "pane_moved",
            json!({
                "closed_workspace_id":"old", "created_workspace":{"workspace_id":"new","label":"New"},
                "pane":{"workspace_id":"current","pane_id":"p"}
            }),
        );
        assert!(
            matches!(&mapping.intents[0], HerdrIntent::UpsertThread(thread) if thread.source_id == "new")
        );
        assert!(
            matches!(&mapping.intents[1], HerdrIntent::ArchiveThread { source_id, .. } if source_id == "old")
        );
        assert!(
            matches!(&mapping.intents[2], HerdrIntent::UpsertThread(thread) if thread.source_id == "current")
        );
    }

    #[test]
    fn status_without_pane_identity_is_dropped() {
        let mapping = map(
            "missing-pane",
            "pane_agent_status_changed",
            json!({"workspace_id":"w","agent_status":"idle"}),
        );
        assert!(matches!(
            mapping.intents.as_slice(),
            [HerdrIntent::Dropped {
                reason: "missing pane_id",
                ..
            }]
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn maps_every_protocol_19_kind_or_explicitly_drops_it() {
        let cases = [
            (
                "workspace_created",
                json!({"workspace":{"workspace_id":"w","label":"W"}}),
                false,
            ),
            (
                "workspace_updated",
                json!({"workspace":{"workspace_id":"w"}}),
                false,
            ),
            (
                "workspace_metadata_updated",
                json!({"workspace":{"workspace_id":"w"}}),
                false,
            ),
            ("workspace_closed", json!({"workspace_id":"w"}), false),
            (
                "workspace_renamed",
                json!({"workspace_id":"w","label":"W"}),
                false,
            ),
            ("workspace_moved", json!({"workspace_id":"w"}), false),
            ("workspace_reordered", json!({"workspace_ids":["w"]}), false),
            ("workspace_focused", json!({"workspace_id":"w"}), false),
            ("worktree_created", json!({}), true),
            ("worktree_opened", json!({}), true),
            ("worktree_removed", json!({}), true),
            (
                "tab_created",
                json!({"tab":{"workspace_id":"w","tab_id":"t"}}),
                false,
            ),
            (
                "tab_closed",
                json!({"workspace_id":"w","tab_id":"t"}),
                false,
            ),
            (
                "tab_renamed",
                json!({"workspace_id":"w","tab_id":"t","label":"T"}),
                false,
            ),
            ("tab_moved", json!({"workspace_id":"w","tab_id":"t"}), false),
            (
                "tab_focused",
                json!({"workspace_id":"w","tab_id":"t"}),
                false,
            ),
            (
                "pane_created",
                json!({"pane":{"workspace_id":"w","pane_id":"p"}}),
                false,
            ),
            (
                "pane_closed",
                json!({"workspace_id":"w","pane_id":"p"}),
                true,
            ),
            (
                "pane_updated",
                json!({"pane":{"workspace_id":"w","pane_id":"p"}}),
                false,
            ),
            (
                "pane_focused",
                json!({"workspace_id":"w","pane_id":"p"}),
                false,
            ),
            (
                "pane_moved",
                json!({"pane":{"workspace_id":"w","pane_id":"p"}}),
                false,
            ),
            (
                "pane_output_changed",
                json!({"workspace_id":"w","pane_id":"p"}),
                true,
            ),
            ("pane.output_matched", json!({"pane_id":"p"}), true),
            (
                "pane.scroll_changed",
                json!({"workspace_id":"w","pane_id":"p"}),
                true,
            ),
            (
                "pane_exited",
                json!({"workspace_id":"w","pane_id":"p"}),
                false,
            ),
            (
                "pane_agent_detected",
                json!({"workspace_id":"w","pane_id":"p"}),
                false,
            ),
            (
                "pane_agent_status_changed",
                json!({"workspace_id":"w","pane_id":"p","agent_status":"idle"}),
                false,
            ),
            ("layout_updated", json!({}), true),
        ];
        for (index, (kind, data, should_drop)) in cases.into_iter().enumerate() {
            let mapping = map(&format!("event-{index}"), kind, data);
            assert!(!mapping.intents.is_empty(), "{kind}");
            assert_eq!(
                matches!(mapping.intents[0], HerdrIntent::Dropped { .. }),
                should_drop,
                "{kind}"
            );
        }
    }

    #[test]
    fn unknown_and_malformed_events_drop_without_panicking() {
        for payload in [
            json!({"event":"future_event","data":{}}),
            json!({"event":"workspace_created","data":{}}),
            json!({}),
        ] {
            let mapping = map_event(&HerdrEvent {
                event_id: "evt",
                payload: &payload,
                received_at: Utc::now(),
            });
            assert!(matches!(
                mapping.intents.as_slice(),
                [HerdrIntent::Dropped { .. }]
            ));
        }
    }

    #[test]
    fn identical_replays_keep_the_same_ids_and_dedupe_key() {
        let first = map(
            "spool-42",
            "pane_agent_status_changed",
            json!({"workspace_id":"w","pane_id":"p","agent_status":"done","agent":"hermes"}),
        );
        let second = map(
            "spool-42",
            "pane_agent_status_changed",
            json!({"workspace_id":"w","pane_id":"p","agent_status":"done","agent":"hermes"}),
        );
        assert_eq!(first, second);
    }
}

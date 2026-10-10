use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use iris_core::{
    AuditAction, AuditEvent, Contact, IngestBatch, IngestMutation, Message, MessageKind, Thread,
};
use iris_providers::herdr::{HerdrEvent, map_ingest_batch, map_workspace_snapshot};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const MAX_PENDING_EVENTS: usize = 10_000;
const MAX_BATCH_EVENTS: usize = 20;
const MAX_BATCH_BODY_BYTES: usize = 1024 * 1024;
const MAX_BATCH_AGE: Duration = Duration::seconds(5);
const HEARTBEAT_INTERVAL: Duration = Duration::hours(1);
const HEARTBEAT_THREAD_ID: Uuid = Uuid::from_u128(0x0b94_4f64_67a5_5f3b_87d9_119a_9b80_2a31);
const HEARTBEAT_CONTACT_ID: Uuid = Uuid::from_u128(0x18d9_7a44_2a83_58a7_98c5_d79d_19a6_30d4);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventRecord {
    sequence: u64,
    event_id: String,
    received_at: DateTime<Utc>,
    #[serde(default = "mapping_ready_by_default")]
    mapping_ready: bool,
    payload: Value,
}

const fn mapping_ready_by_default() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Immutable HTTP request persisted before the first delivery attempt.
pub struct BatchRecord {
    /// Monotonic sequence used to order pending batch files.
    pub sequence: u64,
    /// Number of source events represented by this batch.
    pub event_count: usize,
    event_sequences: Vec<u64>,
    /// Exact serialized `IngestBatch` body reused across retries/restarts.
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BatchIndex {
    event_count: usize,
    event_sequences: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolState {
    next_sequence: u64,
    #[serde(default)]
    pending_event_count: usize,
    last_upstream_event_at: DateTime<Utc>,
    last_heartbeat_at: Option<DateTime<Utc>>,
}

/// Filesystem-backed bounded outbox. Every request body is persisted exactly
/// once before any network attempt and reused byte-for-byte until accepted.
pub struct Spool {
    directory: PathBuf,
    state: SpoolState,
    max_events: usize,
    blocked_oversized_event: Option<u64>,
    has_warned_oversized_event: bool,
}

impl Spool {
    /// Open a private outbox and reconcile interrupted batch commits.
    pub fn open(directory: PathBuf) -> io::Result<Self> {
        Self::open_with_limit(directory, MAX_PENDING_EVENTS)
    }

    fn open_with_limit(directory: PathBuf, max_events: usize) -> io::Result<Self> {
        fs::create_dir_all(&directory)?;
        set_private_directory(&directory)?;
        remove_abandoned_temps(&directory)?;
        let now = Utc::now();
        let state_path = directory.join("bridge-state.json");
        let mut state = match fs::read(&state_path) {
            Ok(bytes) => serde_json::from_slice::<SpoolState>(&bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => SpoolState {
                next_sequence: 1,
                pending_event_count: 0,
                last_upstream_event_at: now,
                last_heartbeat_at: None,
            },
            Err(error) => return Err(error),
        };
        let max_on_disk = max_sequence(&directory)?;
        state.next_sequence = state.next_sequence.max(max_on_disk.saturating_add(1));
        let mut spool = Self {
            directory,
            state,
            max_events,
            blocked_oversized_event: None,
            has_warned_oversized_event: false,
        };
        spool.reconcile_batched_events()?;
        spool.state.pending_event_count = spool.count_pending_events()?;
        spool.enforce_bound()?;
        spool.persist_state()?;
        Ok(spool)
    }

    /// Durably append an upstream event that needs no further protocol enrichment.
    #[cfg(test)]
    pub fn append_upstream_event(
        &mut self,
        payload: Value,
        received_at: DateTime<Utc>,
    ) -> io::Result<()> {
        self.append_event(payload, received_at, true).map(|_| ())
    }

    /// Capture an upstream event durably before protocol enrichment that can fail.
    pub(crate) fn append_unmapped_upstream_event(
        &mut self,
        payload: Value,
        received_at: DateTime<Utc>,
    ) -> io::Result<u64> {
        self.append_event(payload, received_at, false)
    }

    fn append_event(
        &mut self,
        payload: Value,
        received_at: DateTime<Utc>,
        mapping_ready: bool,
    ) -> io::Result<u64> {
        let sequence = self.allocate_sequence();
        let record = EventRecord {
            sequence,
            event_id: Uuid::new_v4().to_string(),
            received_at,
            mapping_ready,
            payload,
        };
        self.write_event(&record)?;
        self.state.pending_event_count = self.state.pending_event_count.saturating_add(1);
        self.state.last_upstream_event_at = received_at;
        self.persist_state()?;
        self.enforce_bound()?;
        Ok(sequence)
    }

    pub(crate) fn unmapped_event_payloads(&self) -> io::Result<Vec<(u64, DateTime<Utc>, Value)>> {
        Ok(self
            .event_records()?
            .into_iter()
            .filter(|event| !event.mapping_ready)
            .map(|event| (event.sequence, event.received_at, event.payload))
            .collect())
    }

    pub(crate) fn mark_event_ready(&self, sequence: u64, payload: Value) -> io::Result<()> {
        let path = self.event_path(sequence);
        let mut record: EventRecord = read_json(&path)?;
        if record.sequence != sequence {
            return Err(io::Error::other("spool event sequence mismatch"));
        }
        record.payload = payload;
        record.mapping_ready = true;
        self.write_event(&record)
    }

    /// Enqueue the synthetic health message after an hour without source events.
    pub fn maybe_append_heartbeat(&mut self, now: DateTime<Utc>) -> io::Result<bool> {
        let idle_since = self
            .state
            .last_heartbeat_at
            .unwrap_or(self.state.last_upstream_event_at)
            .max(self.state.last_upstream_event_at);
        if now.signed_duration_since(idle_since) < HEARTBEAT_INTERVAL {
            return Ok(false);
        }
        let sequence = self.allocate_sequence();
        let payload = json!({
            "event": "bridge_heartbeat",
            "data": {"bridge": "herdr-iris-bridge", "received_at": now}
        });
        self.write_event(&EventRecord {
            sequence,
            event_id: Uuid::new_v4().to_string(),
            received_at: now,
            mapping_ready: true,
            payload,
        })?;
        self.state.pending_event_count = self.state.pending_event_count.saturating_add(1);
        self.state.last_heartbeat_at = Some(now);
        self.persist_state()?;
        self.enforce_bound()?;
        Ok(true)
    }

    /// Persist a due immutable batch and remove its source records only after the
    /// batch file is durable. Startup reconciles a crash between these steps.
    pub fn flush_due(&mut self, now: DateTime<Utc>) -> io::Result<bool> {
        let all_events = self.event_records()?;
        let ready_count = all_events
            .iter()
            .take_while(|event| event.mapping_ready)
            .count();
        let events = &all_events[..ready_count];
        let Some(oldest) = events.first() else {
            return Ok(false);
        };
        if self.blocked_oversized_event == Some(oldest.sequence) {
            return Ok(false);
        }
        self.blocked_oversized_event = None;
        let due = events.len() >= MAX_BATCH_EVENTS
            || now.signed_duration_since(oldest.received_at) >= MAX_BATCH_AGE;
        if !due {
            return Ok(false);
        }
        let mut selected = events
            .iter()
            .take(MAX_BATCH_EVENTS)
            .cloned()
            .collect::<Vec<_>>();
        let sequence = self.state.next_sequence;
        let batch = loop {
            let candidate = build_batch(&selected, sequence)?;
            if candidate.body.len() <= MAX_BATCH_BODY_BYTES {
                break candidate;
            }
            if selected.len() > 1 {
                selected.pop();
                continue;
            }
            let oversized = selected
                .first()
                .ok_or_else(|| io::Error::other("due event batch unexpectedly empty"))?;
            if !self.has_warned_oversized_event {
                tracing::warn!(
                    source_event_sequence = oversized.sequence,
                    body_limit_bytes = MAX_BATCH_BODY_BYTES,
                    "Herdr event exceeds Iris's ingest body limit; retaining it at the spool head"
                );
                self.has_warned_oversized_event = true;
            }
            self.blocked_oversized_event = Some(oversized.sequence);
            return Ok(false);
        };
        let file_path = self.batch_path(batch.sequence);
        write_atomic(
            &file_path,
            &serde_json::to_vec(&batch).map_err(io::Error::other)?,
        )?;
        let index = BatchIndex {
            event_count: batch.event_count,
            event_sequences: batch.event_sequences.clone(),
        };
        write_atomic(
            &self.batch_index_path(batch.sequence),
            &serde_json::to_vec(&index).map_err(io::Error::other)?,
        )?;
        self.state.next_sequence = self.state.next_sequence.saturating_add(1);
        self.persist_state()?;
        for event in &selected {
            remove_if_exists(&self.event_path(event.sequence))?;
        }
        sync_directory(&self.directory)?;
        Ok(true)
    }

    /// Read the oldest immutable request without changing its serialized body.
    pub fn oldest_batch(&self) -> io::Result<Option<BatchRecord>> {
        let Some((_, path)) = self.batch_paths()?.into_iter().next() else {
            return Ok(None);
        };
        read_json(&path).map(Some)
    }

    /// Remove a durable request only after Iris acknowledges a success status.
    pub fn remove_batch(&mut self, sequence: u64, event_count: usize) -> io::Result<()> {
        let batch_path = self.batch_path(sequence);
        match fs::remove_file(&batch_path) {
            Ok(()) => {
                remove_if_exists(&self.batch_index_path(sequence))?;
                self.state.pending_event_count =
                    self.state.pending_event_count.saturating_sub(event_count);
                sync_directory(&self.directory)?;
                self.persist_state()
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                remove_if_exists(&self.batch_index_path(sequence))?;
                sync_directory(&self.directory)
            }
            Err(error) => Err(error),
        }
    }

    const fn allocate_sequence(&mut self) -> u64 {
        let sequence = self.state.next_sequence;
        self.state.next_sequence = self.state.next_sequence.saturating_add(1);
        sequence
    }

    fn write_event(&self, event: &EventRecord) -> io::Result<()> {
        let bytes = serde_json::to_vec(event).map_err(io::Error::other)?;
        write_atomic(&self.event_path(event.sequence), &bytes)
    }

    fn persist_state(&self) -> io::Result<()> {
        let bytes = serde_json::to_vec(&self.state).map_err(io::Error::other)?;
        write_atomic(&self.directory.join("bridge-state.json"), &bytes)
    }

    fn event_path(&self, sequence: u64) -> PathBuf {
        self.directory.join(format!("event-{sequence:020}.json"))
    }

    fn batch_path(&self, sequence: u64) -> PathBuf {
        self.directory.join(format!("batch-{sequence:020}.json"))
    }

    fn batch_index_path(&self, sequence: u64) -> PathBuf {
        self.directory
            .join(format!("batch-{sequence:020}.index.json"))
    }

    fn event_records(&self) -> io::Result<Vec<EventRecord>> {
        let mut records = Vec::new();
        for (_, path) in self.event_paths()? {
            records.push(read_json(&path)?);
        }
        records.sort_by_key(|record: &EventRecord| record.sequence);
        Ok(records)
    }

    fn event_paths(&self) -> io::Result<Vec<(u64, PathBuf)>> {
        numbered_paths(&self.directory, "event-")
    }

    fn batch_paths(&self) -> io::Result<Vec<(u64, PathBuf)>> {
        numbered_paths(&self.directory, "batch-")
    }

    fn batch_index_paths(&self) -> io::Result<Vec<(u64, PathBuf)>> {
        let mut paths = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(sequence) = name
                .strip_prefix("batch-")
                .and_then(|name| name.strip_suffix(".index.json"))
                .and_then(|sequence| sequence.parse::<u64>().ok())
            else {
                continue;
            };
            paths.push((sequence, entry.path()));
        }
        paths.sort_by_key(|(sequence, _)| *sequence);
        Ok(paths)
    }

    fn batch_index_for(&self, sequence: u64, batch_path: &Path) -> io::Result<BatchIndex> {
        let index_path = self.batch_index_path(sequence);
        let index = match read_json::<BatchIndex>(&index_path) {
            Ok(index) => index,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // A crash may occur after the immutable batch file lands but
                // before its compact index. Rebuild the index once from it.
                let batch: BatchRecord = read_json(batch_path)?;
                let index = BatchIndex {
                    event_count: batch.event_count,
                    event_sequences: batch.event_sequences,
                };
                write_atomic(
                    &index_path,
                    &serde_json::to_vec(&index).map_err(io::Error::other)?,
                )?;
                index
            }
            Err(error) => return Err(error),
        };
        if index.event_count == 0 || index.event_count != index.event_sequences.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Herdr batch index has inconsistent event metadata",
            ));
        }
        Ok(index)
    }

    fn count_pending_events(&self) -> io::Result<usize> {
        let mut count = self.event_paths()?.len();
        for (sequence, path) in self.batch_paths()? {
            count = count.saturating_add(self.batch_index_for(sequence, &path)?.event_count);
        }
        Ok(count)
    }

    fn reconcile_batched_events(&self) -> io::Result<()> {
        let batch_paths = self.batch_paths()?;
        let mut batch_sequences = HashSet::new();
        let mut already_batched = HashSet::new();
        for (sequence, path) in &batch_paths {
            batch_sequences.insert(*sequence);
            already_batched.extend(self.batch_index_for(*sequence, path)?.event_sequences);
        }
        for (sequence, path) in self.batch_index_paths()? {
            if !batch_sequences.contains(&sequence) {
                remove_if_exists(&path)?;
            }
        }
        for (sequence, path) in self.event_paths()? {
            if already_batched.contains(&sequence) {
                remove_if_exists(&path)?;
            }
        }
        sync_directory(&self.directory)
    }

    fn enforce_bound(&mut self) -> io::Result<usize> {
        let mut dropped = 0;
        while self.state.pending_event_count > self.max_events {
            let oldest_event = self.event_paths()?.into_iter().next();
            let oldest_batch = if let Some((batch_sequence, batch_path)) =
                self.batch_paths()?.into_iter().next()
            {
                let index = self.batch_index_for(batch_sequence, &batch_path)?;
                let oldest_event_sequence =
                    index.event_sequences.iter().copied().min().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Herdr batch index has no source event sequences",
                        )
                    })?;
                Some((oldest_event_sequence, batch_sequence, batch_path, index))
            } else {
                None
            };
            match (oldest_event, oldest_batch) {
                (Some((event_sequence, event_path)), Some((batch_event_sequence, _, _, _)))
                    if event_sequence < batch_event_sequence =>
                {
                    remove_if_exists(&event_path)?;
                    self.state.pending_event_count =
                        self.state.pending_event_count.saturating_sub(1);
                    dropped += 1;
                }
                (_, Some((_, batch_sequence, batch_path, index))) => {
                    remove_if_exists(&batch_path)?;
                    remove_if_exists(&self.batch_index_path(batch_sequence))?;
                    self.state.pending_event_count = self
                        .state
                        .pending_event_count
                        .saturating_sub(index.event_count);
                    dropped += index.event_count;
                }
                (Some((_, event_path)), None) => {
                    remove_if_exists(&event_path)?;
                    self.state.pending_event_count =
                        self.state.pending_event_count.saturating_sub(1);
                    dropped += 1;
                }
                (None, None) => {
                    self.state.pending_event_count = 0;
                }
            }
        }
        if dropped > 0 {
            tracing::warn!(
                dropped_events = dropped,
                "bounded Herdr spool evicted oldest queued events"
            );
            sync_directory(&self.directory)?;
            self.persist_state()?;
        }
        Ok(dropped)
    }

    #[cfg(test)]
    /// Count all source events still queued or held by durable batches.
    pub const fn pending_event_count(&self) -> usize {
        self.state.pending_event_count
    }
}

fn build_batch(events: &[EventRecord], sequence: u64) -> io::Result<BatchRecord> {
    let batch_id = Uuid::new_v4().to_string();
    let replay_key = format!("herdr:bridge-batch:{batch_id}");
    let event_ids = events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<Vec<_>>();
    let event_sequences = events
        .iter()
        .map(|event| event.sequence)
        .collect::<Vec<_>>();
    let mut mutations = Vec::new();
    let mut dropped_count = 0;
    let mut suppressed_thread_upsert_count = 0;
    let mut missing_workspace_snapshot_event_count = 0;

    for event in events {
        if event.payload.get("event").and_then(Value::as_str) == Some("bridge_heartbeat") {
            mutations.extend(heartbeat_mutations(&event.event_id, event.received_at));
            continue;
        }
        let mapped = map_ingest_batch(&HerdrEvent {
            event_id: &event.event_id,
            payload: &event.payload,
            received_at: event.received_at,
        })
        .map_err(io::Error::other)?;
        dropped_count += mapped
            .audit
            .as_ref()
            .and_then(|audit| audit.metadata.get("dropped_count"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let mut event_mutations = mapped.mutations;
        let (suppressed_threads, missing_snapshot) =
            enrich_thread_snapshots(&event.payload, &mut event_mutations, event.received_at);
        suppressed_thread_upsert_count += suppressed_threads;
        if missing_snapshot {
            dropped_count += 1;
            missing_workspace_snapshot_event_count += 1;
        }
        mutations.extend(event_mutations);
    }

    let audit = AuditEvent {
        action: AuditAction::Normalize,
        provider: "herdr".to_owned(),
        source_id: Some(replay_key.clone()),
        timestamp: events[0].received_at,
        metadata: json!({
            "event_count": events.len(),
            "mutation_count": mutations.len(),
            "dropped_count": dropped_count,
            "suppressed_thread_upsert_count": suppressed_thread_upsert_count,
            "missing_workspace_snapshot_event_count": missing_workspace_snapshot_event_count,
            "bridge_event_ids": event_ids,
        }),
    };
    let batch = IngestBatch {
        source: "herdr".to_owned(),
        replay_key,
        mutations,
        cursor: None,
        audit: Some(audit),
    };
    let body = serde_json::to_string(&batch).map_err(io::Error::other)?;
    Ok(BatchRecord {
        sequence,
        event_count: events.len(),
        event_sequences,
        body,
    })
}

fn enrich_thread_snapshots(
    payload: &Value,
    mutations: &mut Vec<IngestMutation>,
    received_at: DateTime<Utc>,
) -> (usize, bool) {
    let snapshots = payload
        .pointer("/bridge/workspace_snapshots")
        .and_then(Value::as_object);
    let event_kind = payload
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event_kind == "pane_agent_status_changed" {
        let workspace_id = payload
            .pointer("/data/workspace_id")
            .and_then(Value::as_str);
        let current_thread = workspace_id
            .and_then(|id| snapshots.and_then(|items| items.get(id)))
            .and_then(|snapshot| map_workspace_snapshot(snapshot, received_at));
        if current_thread.is_none() {
            mutations.clear();
            return (1, true);
        }
    }

    let mut normalized = Vec::with_capacity(mutations.len());
    let mut suppressed = 0;
    for mutation in mutations.drain(..) {
        match mutation {
            IngestMutation::UpsertThread(thread) => {
                let current_thread = snapshots
                    .and_then(|items| items.get(&thread.source_id))
                    .and_then(|snapshot| map_workspace_snapshot(snapshot, received_at))
                    .filter(|snapshot| snapshot.source_id == thread.source_id);
                if let Some(snapshot) = current_thread {
                    normalized.push(IngestMutation::UpsertThread(snapshot));
                } else {
                    suppressed += 1;
                }
            }
            IngestMutation::ArchiveThread { source, source_id } => {
                let current_thread = snapshots
                    .and_then(|items| items.get(&source_id))
                    .and_then(|snapshot| map_workspace_snapshot(snapshot, received_at))
                    .filter(|snapshot| snapshot.source_id == source_id);
                if let Some(snapshot) = current_thread {
                    // A retained close event can be stale relative to the
                    // authoritative workspace list. Restore, don't archive.
                    normalized.push(IngestMutation::UpsertThread(snapshot));
                } else {
                    normalized.push(IngestMutation::ArchiveThread { source, source_id });
                }
            }
            other => normalized.push(other),
        }
    }
    *mutations = normalized;
    (suppressed, false)
}

fn heartbeat_mutations(event_id: &str, at: DateTime<Utc>) -> Vec<IngestMutation> {
    let contact = Contact {
        id: HEARTBEAT_CONTACT_ID,
        source: "herdr".to_owned(),
        provider_instance: None,
        source_id: "bridge".to_owned(),
        display_name: Some("Herdr bridge".to_owned()),
        avatar_url: None,
        metadata: json!({"role": "bridge_health"}),
    };
    let thread = Thread {
        id: HEARTBEAT_THREAD_ID,
        source: "herdr".to_owned(),
        provider_instance: None,
        source_id: "bridge-heartbeat".to_owned(),
        title: Some("Herdr bridge health".to_owned()),
        participants: vec![contact.clone()],
        last_message_at: at,
        unread_count: Some(0),
        metadata: json!({"kind": "bridge_heartbeat"}),
    };
    let message = Message {
        id: Uuid::new_v5(&HEARTBEAT_THREAD_ID, event_id.as_bytes()),
        thread_id: HEARTBEAT_THREAD_ID,
        source: "herdr".to_owned(),
        source_id: format!("bridge-heartbeat:{event_id}"),
        sender: contact.clone(),
        kind: MessageKind::System,
        body: "Herdr bridge heartbeat".to_owned(),
        attachments: Vec::new(),
        timestamp: at,
        is_outbound: false,
        metadata: json!({"event": "bridge_heartbeat", "bridge_event_id": event_id}),
    };
    vec![
        IngestMutation::UpsertContact(contact),
        IngestMutation::UpsertThread(thread),
        IngestMutation::AppendMessage { message },
    ]
}

fn numbered_paths(directory: &Path, prefix: &str) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if let Some(sequence) = name
            .strip_prefix(prefix)
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|digits| digits.parse::<u64>().ok())
        {
            result.push((sequence, path));
        }
    }
    result.sort_by_key(|(sequence, _)| *sequence);
    Ok(result)
}

fn max_sequence(directory: &Path) -> io::Result<u64> {
    let mut max = 0;
    for prefix in ["event-", "batch-"] {
        for (sequence, _) in numbered_paths(directory, prefix)? {
            max = max.max(sequence);
        }
    }
    Ok(max)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<T> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "spool path has no parent"))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("record");
    let temp = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let write_result = (|| {
        let mut file = options.open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        sync_directory(parent)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result
}

fn set_private_directory(directory: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn remove_abandoned_temps(directory: &Path) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
            continue;
        };
        if name.starts_with('.')
            && Path::new(&name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
        {
            remove_if_exists(&entry.path())?;
        }
    }
    sync_directory(directory)
}

fn remove_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn sync_directory(directory: &Path) -> io::Result<()> {
    File::open(directory)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::tempdir;

    fn unknown_event() -> Value {
        json!({"event": "future_event", "data": {"safe": true}})
    }

    fn pane_status_event(label_length: usize) -> Value {
        let mut event: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/herdr-v0.8.0-pane-agent-status-event.json"
        ))
        .unwrap();
        event["data"]["workspace_id"] = json!("workspace-1");
        event["data"]["agent"] = json!("claude");
        event["data"]["display_agent"] = json!("Claude");
        event["data"]["state_labels"]["detail"] = json!("x".repeat(label_length));
        event["data"]["workspace_label"] = json!("Synthetic workspace");
        event["bridge"]["workspace_snapshots"]["workspace-1"] = json!({
            "workspace_id": "workspace-1",
            "number": 1,
            "label": "Synthetic workspace",
            "focused": true,
            "pane_count": 1,
            "tab_count": 1,
            "active_tab_id": "tab-1",
            "agent_status": "working",
            "revision": 7
        });
        event
    }

    #[test]
    fn batches_at_twenty_and_after_five_seconds_and_preserves_body_on_reopen() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 100).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap();
        for index in 0..19 {
            let mut event = unknown_event();
            event["data"]["index"] = json!(index);
            spool.append_upstream_event(event, start).unwrap();
        }
        assert!(!spool.flush_due(start + Duration::seconds(4)).unwrap());
        spool
            .append_upstream_event(unknown_event(), start + Duration::seconds(4))
            .unwrap();
        assert!(spool.flush_due(start + Duration::seconds(4)).unwrap());
        let first = spool.oldest_batch().unwrap().unwrap();
        assert_eq!(first.event_count, 20);
        assert_eq!(spool.pending_event_count(), 20);
        fs::remove_file(spool.batch_index_path(first.sequence)).unwrap();
        drop(spool);

        let reopened = Spool::open_with_limit(dir.path().join("spool"), 100).unwrap();
        let restored = reopened.oldest_batch().unwrap().unwrap();
        assert_eq!(first.body, restored.body);
        assert_eq!(first.sequence, restored.sequence);

        let mut reopened = reopened;
        let mut after_reopen = unknown_event();
        after_reopen["data"]["index"] = json!(20);
        reopened.append_upstream_event(after_reopen, start).unwrap();
        assert!(reopened.flush_due(start + Duration::seconds(5)).unwrap());
        assert_eq!(reopened.pending_event_count(), 21);
    }

    #[test]
    fn bounded_spool_drops_oldest_records_only() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 3).unwrap();
        let now = Utc::now();
        for index in 0..4 {
            spool
                .append_upstream_event(json!({"event":"future", "data":{"n": index}}), now)
                .unwrap();
        }
        assert_eq!(spool.pending_event_count(), 3);
        let events = spool.event_records().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload["data"]["n"], 1);
    }

    #[test]
    fn thread_upserts_use_allowlisted_authoritative_workspace_snapshot() {
        let received_at = Utc::now();
        let snapshot = json!({
            "workspace_id": "workspace-1",
            "number": 1,
            "label": "Current label",
            "focused": true,
            "pane_count": 6,
            "tab_count": 2,
            "active_tab_id": "tab-2",
            "agent_status": "working",
            "revision": 7,
            "worktree": {"repo_root": "/private/repo"},
            "panes": [{
                "pane_id": "pane-1",
                "workspace_id": "workspace-1",
                "tab_id": "tab-2",
                "agent_status": "working",
                "title": "private pane title",
                "cwd": "/private/repo"
            }],
            "tokens": {"synthetic_private_value": "fixture-only-do-not-forward"}
        });
        let payload = json!({
            "event": "pane_created",
            "data": {
                "pane": {"workspace_id": "workspace-1", "pane_id": "pane-1"},
                "workspace_label": "Stale label"
            },
            "bridge": {"workspace_snapshots": {"workspace-1": snapshot}}
        });
        let mapped = map_ingest_batch(&HerdrEvent {
            event_id: "pane-created-snapshot",
            payload: &payload,
            received_at,
        })
        .unwrap();
        let mut mutations = mapped.mutations;
        assert_eq!(
            enrich_thread_snapshots(&payload, &mut mutations, received_at),
            (0, false)
        );
        let [IngestMutation::UpsertThread(thread)] = mutations.as_slice() else {
            panic!("expected one complete workspace thread upsert: {mutations:?}");
        };
        assert_eq!(thread.title.as_deref(), Some("Current label"));
        assert_eq!(thread.metadata["pane_count"], 6);
        assert_eq!(thread.metadata["tab_count"], 2);
        assert!(thread.metadata.get("revision").is_none());
        assert_eq!(thread.metadata["panes"][0]["pane_id"], "pane-1");
        assert_eq!(thread.metadata["panes"][0]["tab_id"], "tab-2");
        assert!(thread.metadata["panes"][0].get("title").is_none());
        assert!(thread.metadata["worktree"].is_null());
        assert!(!thread.metadata.to_string().contains("/private/repo"));
        assert!(thread.metadata.get("tokens").is_none());

        let status_payload = pane_status_event(8);
        let mapped = map_ingest_batch(&HerdrEvent {
            event_id: "status-with-full-snapshot",
            payload: &status_payload,
            received_at,
        })
        .unwrap();
        let mut status_mutations = mapped.mutations;
        assert_eq!(
            enrich_thread_snapshots(&status_payload, &mut status_mutations, received_at),
            (0, false)
        );
        let status_thread = status_mutations.iter().find_map(|mutation| match mutation {
            IngestMutation::UpsertThread(thread) => Some(thread),
            _ => None,
        });
        assert_eq!(status_thread.unwrap().metadata["pane_count"], 1);
        assert!(
            status_mutations
                .iter()
                .any(|mutation| matches!(mutation, IngestMutation::AppendMessage { .. }))
        );
    }

    #[test]
    fn status_without_an_authoritative_workspace_does_not_orphan_a_message() {
        let received_at = Utc::now();
        let payload = json!({
            "event": "pane_agent_status_changed",
            "data": {
                "workspace_id": "workspace-closed",
                "pane_id": "pane-1",
                "agent_status": "working"
            },
            "bridge": {"workspace_snapshots": {}}
        });
        let mapped = map_ingest_batch(&HerdrEvent {
            event_id: "stale-status",
            payload: &payload,
            received_at,
        })
        .unwrap();
        let mut mutations = mapped.mutations;
        assert_eq!(
            enrich_thread_snapshots(&payload, &mut mutations, received_at),
            (1, true)
        );
        assert!(mutations.is_empty());
    }

    #[test]
    fn batch_serialization_splits_at_iris_ingest_body_limit() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 10).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap();
        for _ in 0..2 {
            spool
                .append_upstream_event(pane_status_event(300_000), start)
                .unwrap();
        }

        assert!(spool.flush_due(start + Duration::seconds(5)).unwrap());
        let first = spool.oldest_batch().unwrap().unwrap();
        assert_eq!(first.event_count, 1);
        assert!(first.body.len() <= MAX_BATCH_BODY_BYTES);
        assert_eq!(spool.pending_event_count(), 2);

        assert!(spool.flush_due(start + Duration::seconds(5)).unwrap());
        let second = spool
            .batch_paths()
            .unwrap()
            .into_iter()
            .nth(1)
            .map(|(_, path)| read_json::<BatchRecord>(&path).unwrap())
            .unwrap();
        assert_eq!(second.event_count, 1);
        assert!(second.body.len() <= MAX_BATCH_BODY_BYTES);
        assert_ne!(first.sequence, second.sequence);
        assert_eq!(spool.pending_event_count(), 2);
    }

    #[test]
    fn overflow_compares_batches_by_their_oldest_source_event() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 2).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap();
        for _ in 0..2 {
            spool
                .append_upstream_event(pane_status_event(300_000), start)
                .unwrap();
        }

        assert!(spool.flush_due(start + Duration::seconds(5)).unwrap());
        let first = spool.oldest_batch().unwrap().unwrap();
        assert_eq!(first.event_count, 1);
        assert_eq!(first.event_sequences, vec![1]);
        assert_eq!(
            spool.event_records().unwrap()[0].sequence,
            2,
            "the later event remains queued while the older event is batched"
        );

        // Model overflow after the first event has been sealed. The batch's
        // filename sequence is 3, but its source event is sequence 1; event 2
        // must survive because it is newer than the event inside the batch.
        spool.max_events = 1;
        assert_eq!(spool.enforce_bound().unwrap(), 1);
        assert!(spool.batch_paths().unwrap().is_empty());
        let remaining = spool.event_records().unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].sequence, 2);
        assert_eq!(spool.pending_event_count(), 1);
    }

    #[test]
    fn unmapped_event_blocks_later_sealing_until_durable_enrichment() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 10).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap();
        let first_sequence = spool
            .append_unmapped_upstream_event(unknown_event(), start)
            .unwrap();
        spool
            .append_upstream_event(unknown_event(), start + Duration::seconds(1))
            .unwrap();

        assert!(!spool.flush_due(start + Duration::seconds(5)).unwrap());
        let records = spool.event_records().unwrap();
        assert_eq!(records.len(), 2);
        assert!(!records[0].mapping_ready);
        assert!(records[1].mapping_ready);

        let mut enriched = unknown_event();
        enriched["bridge"] = json!({"workspace_snapshots": []});
        spool.mark_event_ready(first_sequence, enriched).unwrap();
        assert!(spool.flush_due(start + Duration::seconds(5)).unwrap());
        assert!(spool.event_records().unwrap().is_empty());
        assert_eq!(spool.oldest_batch().unwrap().unwrap().event_count, 2);
    }

    #[test]
    fn oversized_event_is_retained_at_the_head_instead_of_silently_dropped() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 10).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap();
        spool
            .append_upstream_event(pane_status_event(600_000), start)
            .unwrap();
        spool
            .append_upstream_event(unknown_event(), start + Duration::seconds(1))
            .unwrap();

        let events = spool.event_records().unwrap();
        assert!(build_batch(&events[..1], 3).unwrap().body.len() > MAX_BATCH_BODY_BYTES);
        assert!(!spool.flush_due(start + Duration::seconds(5)).unwrap());
        assert!(!spool.flush_due(start + Duration::seconds(6)).unwrap());
        assert!(spool.oldest_batch().unwrap().is_none());
        let retained = spool.event_records().unwrap();
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].sequence, 1);
        assert_eq!(retained[1].sequence, 2);
        assert_eq!(
            retained[0].payload["data"]["state_labels"]["detail"]
                .as_str()
                .unwrap()
                .len(),
            600_000
        );
        assert_eq!(spool.pending_event_count(), 2);
    }

    #[test]
    fn idle_heartbeat_creates_a_normalized_freshness_message() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 20).unwrap();
        let start = Utc.with_ymd_and_hms(2026, 10, 9, 12, 0, 0).unwrap();
        spool.state.last_upstream_event_at = start;
        assert!(
            !spool
                .maybe_append_heartbeat(start + Duration::minutes(59))
                .unwrap()
        );
        let heartbeat_at = start + Duration::hours(1);
        assert!(spool.maybe_append_heartbeat(heartbeat_at).unwrap());
        assert!(
            spool
                .flush_due(heartbeat_at + Duration::seconds(5))
                .unwrap()
        );
        let batch = spool.oldest_batch().unwrap().unwrap();
        let value: Value = serde_json::from_str(&batch.body).unwrap();
        assert_eq!(value["audit"]["metadata"]["event_count"], 1);
        let mutations = value["mutations"].as_array().unwrap();
        assert_eq!(mutations.len(), 3);
        assert!(
            mutations.iter().any(|item| {
                item["kind"] == "append_message"
                    && item["message"]["kind"] == "system"
                    && item["message"]["body"] == "Herdr bridge heartbeat"
            }),
            "unexpected heartbeat batch: {value}"
        );
    }

    #[test]
    fn protocol_19_payload_maps_into_existing_ingest_mutations_without_raw_metadata_leaks() {
        let dir = tempdir().unwrap();
        let mut spool = Spool::open_with_limit(dir.path().join("spool"), 20).unwrap();
        let mut payload: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/herdr-v0.8.0-workspace-renamed-event.json"
        ))
        .unwrap();
        payload["bridge"]["workspace_snapshots"]["workspace-1"] = json!({
            "workspace_id": "workspace-1",
            "number": 1,
            "label": "Current workspace",
            "pane_count": 1,
            "tab_count": 1,
            "active_tab_id": "tab-1",
            "agent_status": "working"
        });
        spool.append_upstream_event(payload, Utc::now()).unwrap();
        spool.flush_due(Utc::now() + Duration::seconds(5)).unwrap();
        let batch = spool.oldest_batch().unwrap().unwrap();
        let value: Value = serde_json::from_str(&batch.body).unwrap();
        assert_eq!(value["source"], "herdr");
        assert_eq!(value["mutations"][0]["kind"], "upsert_thread");
        assert!(value["audit"]["metadata"].get("bridge_event_ids").is_some());
        assert!(value["audit"]["metadata"].get("raw_payload").is_none());
    }
}

//! SMS provider backed by Termux commands over SSH.
//!
//! The provider expects an Android device running Termux with the Termux:API
//! package installed. Iris executes `termux-sms-list` and `termux-sms-send`
//! through SSH, then normalizes the JSON records into the core Iris model.

use std::collections::{BTreeMap, HashMap};
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use iris_core::outbound::enforce_capability;
use iris_core::{
    AuditAction, AuditEvent, AuditLog, Contact, IrisError, Message, MessageKind, MessageProvider,
    OutboundMessage, ProviderCapability, ProviderMetadata, Result, Thread,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::process::Command;
use uuid::Uuid;

const PROVIDER_ID: &str = "sms";
const METADATA: ProviderMetadata = ProviderMetadata {
    id: PROVIDER_ID,
    name: "SMS",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::SendMessages,
        ProviderCapability::ListThreads,
        ProviderCapability::ListContacts,
    ],
};
const UUID_NAMESPACE: Uuid = Uuid::from_u128(0x6e6c_4e12_1a7d_4a3c_8b53_8914_42f3_0003);

/// SMS provider that talks to Termux:API commands over SSH.
#[derive(Debug, Clone)]
pub struct SmsProvider {
    ssh_target: String,
    ssh_command: String,
    self_number: Option<String>,
    phone_mappings: BTreeMap<String, String>,
    audit: Option<Arc<dyn AuditLog>>,
}

impl SmsProvider {
    /// Create an SMS provider from SSH connection settings.
    pub fn new(ssh_target: impl Into<String>) -> Result<Self> {
        Self::with_options(ssh_target, "ssh", None)
    }

    /// Create an SMS provider with an explicit SSH command and optional owner number.
    pub fn with_options(
        ssh_target: impl Into<String>,
        ssh_command: impl Into<String>,
        self_number: Option<String>,
    ) -> Result<Self> {
        let ssh_target = ssh_target.into();
        if ssh_target.trim().is_empty() {
            return Err(IrisError::Config("sms ssh_host is required".into()));
        }
        let ssh_command = ssh_command.into();
        if ssh_command.trim().is_empty() {
            return Err(IrisError::Config("sms ssh_command cannot be empty".into()));
        }

        Ok(Self {
            ssh_target,
            ssh_command,
            self_number: self_number.map(|number| normalize_phone(&number)),
            phone_mappings: BTreeMap::new(),
            audit: None,
        })
    }

    /// Attach an audit sink for non-secret operation metadata.
    #[must_use]
    pub fn with_audit(mut self, audit: Arc<dyn AuditLog>) -> Self {
        self.audit = Some(audit);
        self
    }

    async fn record(
        &self,
        action: AuditAction,
        source_id: Option<String>,
        metadata: serde_json::Value,
    ) -> Result<()> {
        if let Some(audit) = &self.audit {
            audit
                .record(AuditEvent {
                    action,
                    provider: PROVIDER_ID.into(),
                    source_id,
                    timestamp: Utc::now(),
                    metadata,
                })
                .await?;
        }
        Ok(())
    }

    /// Build from resolved provider credentials.
    pub fn from_credentials(credentials: &BTreeMap<String, String>) -> Result<Self> {
        let ssh_target = credentials
            .get("ssh_host")
            .or_else(|| credentials.get("host"))
            .ok_or_else(|| IrisError::Config("sms credentials.ssh_host is required".into()))?;
        let ssh_command = credentials
            .get("ssh_command")
            .cloned()
            .unwrap_or_else(|| "ssh".into());
        let self_number = credentials
            .get("self_number")
            .or_else(|| credentials.get("phone_number"))
            .cloned();
        let mut provider = Self::with_options(ssh_target.clone(), ssh_command, self_number)?;
        provider.phone_mappings = credentials
            .iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("phone_map.")
                    .map(|from| (normalize_phone(from), normalize_phone(value)))
            })
            .collect();
        Ok(provider)
    }

    async fn sms_records(&self) -> Result<Vec<TermuxSmsRecord>> {
        let output = self.run_termux(&["termux-sms-list", "-l", "1000"]).await?;
        serde_json::from_str(&output).map_err(|error| IrisError::Serialization(error.to_string()))
    }

    async fn run_termux(&self, remote_args: &[&str]) -> Result<String> {
        let remote_command = remote_args
            .iter()
            .map(|arg| shell_quote(arg))
            .collect::<Vec<_>>()
            .join(" ");
        let output = Command::new(&self.ssh_command)
            .arg(&self.ssh_target)
            .arg(remote_command)
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;

        if !output.status.success() {
            return Err(IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }

        String::from_utf8(output.stdout)
            .map_err(|error| IrisError::Serialization(error.to_string()))
    }

    fn message_from_record(&self, record: &TermuxSmsRecord) -> Message {
        record.to_message(self.self_number.as_deref(), &self.phone_mappings)
    }

    fn thread_key_for(&self, address: &str) -> String {
        thread_key(
            self.self_number.as_deref(),
            &map_phone(address, &self.phone_mappings),
        )
    }
}

#[async_trait]
impl MessageProvider for SmsProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &METADATA
    }

    async fn list_threads(&self, limit: Option<u32>) -> Result<Vec<Thread>> {
        let mut by_phone = BTreeMap::<String, Thread>::new();
        for record in self.sms_records().await? {
            let thread = record.to_thread(self.self_number.as_deref(), &self.phone_mappings);
            by_phone
                .entry(self.thread_key_for(&record.address))
                .and_modify(|existing| {
                    if thread.last_message_at > existing.last_message_at {
                        existing.last_message_at = thread.last_message_at;
                    }
                    existing.participants.extend(thread.participants.clone());
                    dedupe_contacts(&mut existing.participants);
                    existing.unread_count =
                        merge_unread(existing.unread_count, thread.unread_count);
                })
                .or_insert(thread);
        }

        let mut threads: Vec<_> = by_phone.into_values().collect();
        threads.sort_by_key(|thread| std::cmp::Reverse(thread.last_message_at));
        threads.truncate(limit.unwrap_or(50) as usize);
        self.record(
            AuditAction::Normalize,
            None,
            json!({ "operation": "list_threads", "count": threads.len() }),
        )
        .await?;
        Ok(threads)
    }

    async fn list_messages(
        &self,
        thread_id: &str,
        before: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        let mut messages: Vec<_> = self
            .sms_records()
            .await?
            .into_iter()
            .filter(|record| {
                record.matches_thread(thread_id, self.self_number.as_deref(), &self.phone_mappings)
            })
            .map(|record| self.message_from_record(&record))
            .filter(|message| before.is_none_or(|cursor| message.timestamp < cursor))
            .collect();

        messages.sort_by_key(|message| std::cmp::Reverse(message.timestamp));
        messages.truncate(limit.unwrap_or(50) as usize);
        messages.sort_by_key(|message| message.timestamp);
        self.record(
            AuditAction::Normalize,
            Some(thread_id.to_owned()),
            json!({ "operation": "list_messages", "count": messages.len() }),
        )
        .await?;
        Ok(messages)
    }

    async fn list_contacts(&self, limit: Option<u32>) -> Result<Vec<Contact>> {
        let mut contacts: Vec<_> = self
            .sms_records()
            .await?
            .iter()
            .map(|record| record.contact(&self.phone_mappings))
            .collect();
        dedupe_contacts(&mut contacts);
        contacts.sort_by(|left, right| left.source_id.cmp(&right.source_id));
        contacts.truncate(limit.unwrap_or(50) as usize);
        self.record(
            AuditAction::Normalize,
            None,
            json!({ "operation": "list_contacts", "count": contacts.len() }),
        )
        .await?;
        Ok(contacts)
    }

    async fn send_message(&self, thread_id: &str, message: &OutboundMessage) -> Result<Message> {
        enforce_capability(message, PROVIDER_ID, false)?;
        let body = message.body.as_str();
        let records = self.sms_records().await?;
        let phone = resolve_phone(
            thread_id,
            &records,
            self.self_number.as_deref(),
            &self.phone_mappings,
        )?;
        self.run_termux(&["termux-sms-send", "-n", &phone, body])
            .await?;

        let timestamp = Utc::now();
        let message = Message {
            id: message_uuid(
                &phone,
                &format!("outbound:{}", timestamp.timestamp_millis()),
            ),
            thread_id: thread_uuid(&thread_key(self.self_number.as_deref(), &phone)),
            source: PROVIDER_ID.into(),
            source_id: format!("outbound:{}", timestamp.timestamp_millis()),
            sender: self_contact(self.self_number.as_deref()),
            kind: MessageKind::Text,
            body: body.to_owned(),
            attachments: Vec::new(),
            timestamp,
            is_outbound: true,
            metadata: json!({ "address": phone, "delivery": "submitted" }),
        };
        self.record(
            AuditAction::Send,
            Some(thread_id.to_owned()),
            json!({ "operation": "send_message", "message_id": message.source_id }),
        )
        .await?;
        Ok(message)
    }
}

#[derive(Debug, Clone, Deserialize)]
struct TermuxSmsRecord {
    #[serde(default, alias = "_id")]
    id: Option<Value>,
    #[serde(default, alias = "thread_id")]
    threadid: Option<Value>,
    address: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    date: Option<Value>,
    #[serde(default, rename = "type")]
    message_type: Option<Value>,
    #[serde(default)]
    read: Option<Value>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

impl TermuxSmsRecord {
    fn to_message(
        &self,
        self_number: Option<&str>,
        phone_mappings: &BTreeMap<String, String>,
    ) -> Message {
        let address = self.mapped_address(phone_mappings);
        let key = thread_key(self_number, &address);
        Message {
            id: message_uuid(&address, &self.source_id()),
            thread_id: thread_uuid(&key),
            source: PROVIDER_ID.into(),
            source_id: self.source_id(),
            sender: if self.is_outbound() {
                self_contact(self_number)
            } else {
                self.contact(phone_mappings)
            },
            kind: MessageKind::Text,
            body: self.body.clone(),
            attachments: Vec::new(),
            timestamp: self.timestamp(),
            is_outbound: self.is_outbound(),
            metadata: self.metadata(),
        }
    }

    fn to_thread(
        &self,
        self_number: Option<&str>,
        phone_mappings: &BTreeMap<String, String>,
    ) -> Thread {
        let address = self.mapped_address(phone_mappings);
        let key = thread_key(self_number, &address);
        let mut participants = vec![self.contact(phone_mappings)];
        if self_number.is_some() {
            participants.push(self_contact(self_number));
        }
        dedupe_contacts(&mut participants);

        Thread {
            id: thread_uuid(&key),
            source: PROVIDER_ID.into(),
            source_id: key,
            title: Some(self.address.clone()),
            participants,
            last_message_at: self.timestamp(),
            unread_count: self.unread_count(),
            metadata: json!({
                "address": self.address,
                "threadid": self.threadid,
            }),
        }
    }

    fn contact(&self, phone_mappings: &BTreeMap<String, String>) -> Contact {
        let address = self.mapped_address(phone_mappings);
        Contact {
            id: contact_uuid(&address),
            source: PROVIDER_ID.into(),
            source_id: address,
            display_name: Some(self.address.clone()),
            avatar_url: None,
            metadata: json!({ "address": self.address }),
        }
    }

    fn matches_thread(
        &self,
        thread_id: &str,
        self_number: Option<&str>,
        phone_mappings: &BTreeMap<String, String>,
    ) -> bool {
        let address = self.mapped_address(phone_mappings);
        let key = thread_key(self_number, &address);
        address == map_phone(thread_id, phone_mappings)
            || key == thread_id
            || thread_uuid(&key).to_string() == thread_id
            || self
                .threadid
                .as_ref()
                .is_some_and(|value| value_to_string(value) == thread_id)
    }

    fn normalized_address(&self) -> String {
        normalize_phone(&self.address)
    }

    fn mapped_address(&self, phone_mappings: &BTreeMap<String, String>) -> String {
        map_phone(&self.address, phone_mappings)
    }

    fn source_id(&self) -> String {
        self.id
            .as_ref()
            .map(value_to_string)
            .or_else(|| self.threadid.as_ref().map(value_to_string))
            .unwrap_or_else(|| {
                format!(
                    "{}:{}",
                    self.normalized_address(),
                    self.timestamp().timestamp_millis()
                )
            })
    }

    fn timestamp(&self) -> DateTime<Utc> {
        let Some(value) = &self.date else {
            return DateTime::<Utc>::UNIX_EPOCH;
        };
        let timestamp = value
            .as_i64()
            .or_else(|| value.as_str()?.parse::<i64>().ok())
            .unwrap_or(0);
        if timestamp > 10_000_000_000 {
            DateTime::<Utc>::from_timestamp_millis(timestamp).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        } else {
            Utc.timestamp_opt(timestamp, 0)
                .single()
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        }
    }

    fn is_outbound(&self) -> bool {
        self.message_type.as_ref().is_some_and(|value| {
            let kind = value_to_string(value).to_ascii_lowercase();
            kind == "sent" || kind == "2" || kind == "outbox" || kind == "queued"
        })
    }

    fn unread_count(&self) -> Option<u32> {
        let read = self.read.as_ref()?;
        let is_unread = match read {
            Value::Bool(value) => !value,
            Value::Number(number) => number.as_u64() == Some(0),
            Value::String(value) => value == "0" || value.eq_ignore_ascii_case("false"),
            _ => false,
        };
        is_unread.then_some(1)
    }

    fn metadata(&self) -> Value {
        let mut metadata = serde_json::Map::new();
        metadata.insert("address".into(), json!(self.address));
        metadata.insert("threadid".into(), json!(self.threadid));
        metadata.insert("type".into(), json!(self.message_type));
        metadata.insert("read".into(), json!(self.read));
        for (key, value) in &self.extra {
            metadata.insert(key.clone(), value.clone());
        }
        Value::Object(metadata)
    }
}

fn resolve_phone(
    thread_id: &str,
    records: &[TermuxSmsRecord],
    self_number: Option<&str>,
    phone_mappings: &BTreeMap<String, String>,
) -> Result<String> {
    if let Some(record) = records
        .iter()
        .find(|record| record.matches_thread(thread_id, self_number, phone_mappings))
    {
        return Ok(record.mapped_address(phone_mappings));
    }
    if looks_like_phone(thread_id) {
        return Ok(map_phone(thread_id, phone_mappings));
    }
    Err(IrisError::NotFound(format!(
        "sms thread not found: {thread_id}"
    )))
}

fn self_contact(self_number: Option<&str>) -> Contact {
    let source_id = self_number.map_or_else(|| "self".to_owned(), normalize_phone);
    Contact {
        id: contact_uuid(&source_id),
        source: PROVIDER_ID.into(),
        source_id,
        display_name: Some("Me".into()),
        avatar_url: None,
        metadata: json!({ "owner": true }),
    }
}

fn dedupe_contacts(contacts: &mut Vec<Contact>) {
    let mut seen = HashMap::<String, usize>::new();
    contacts.retain(|contact| match seen.entry(contact.source_id.clone()) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(1);
            true
        }
        std::collections::hash_map::Entry::Occupied(_) => false,
    });
}

const fn merge_unread(left: Option<u32>, right: Option<u32>) -> Option<u32> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left + right),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn map_phone(input: &str, phone_mappings: &BTreeMap<String, String>) -> String {
    let normalized = normalize_phone(input);
    phone_mappings
        .get(&normalized)
        .cloned()
        .unwrap_or(normalized)
}

fn thread_key(self_number: Option<&str>, address: &str) -> String {
    self_number.map_or_else(
        || address.to_owned(),
        |number| format!("{}:{address}", normalize_phone(number)),
    )
}

fn shell_quote(input: &str) -> String {
    if input
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '+'))
    {
        input.to_owned()
    } else {
        format!("'{}'", input.replace('\'', "'\\''"))
    }
}

fn normalize_phone(input: &str) -> String {
    let trimmed = input.trim();
    let mut normalized = String::new();
    for (index, ch) in trimmed.chars().enumerate() {
        if ch.is_ascii_digit() || (index == 0 && ch == '+') {
            normalized.push(ch);
        }
    }
    if normalized.is_empty() {
        trimmed.to_owned()
    } else {
        normalized
    }
}

fn looks_like_phone(input: &str) -> bool {
    input.chars().filter(char::is_ascii_digit).count() >= 3
}

fn value_to_string(value: &Value) -> String {
    value.as_str().map_or_else(
        || value.to_string().trim_matches('"').to_owned(),
        ToOwned::to_owned,
    )
}

fn thread_uuid(address: &str) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, format!("thread:{address}").as_bytes())
}

fn message_uuid(address: &str, message_id: &str) -> Uuid {
    Uuid::new_v5(
        &UUID_NAMESPACE,
        format!("message:{address}:{message_id}").as_bytes(),
    )
}

fn contact_uuid(address: &str) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, format!("contact:{address}").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_inbound() -> TermuxSmsRecord {
        serde_json::from_value(json!({
            "_id": 12,
            "threadid": 5,
            "address": "+1 (575) 555-0100",
            "body": "status?",
            "date": 1_700_000_000_000i64,
            "type": "inbox",
            "read": 0
        }))
        .expect("sample sms parses")
    }

    #[test]
    fn maps_termux_sms_to_normalized_message() {
        let mappings = BTreeMap::new();
        let message = sample_inbound().to_message(Some("+157****0199"), &mappings);

        assert_eq!(message.source, "sms");
        assert_eq!(message.source_id, "12");
        assert_eq!(message.sender.source_id, "+15755550100");
        assert_eq!(message.kind, MessageKind::Text);
        assert_eq!(message.body, "status?");
        assert!(!message.is_outbound);
        assert_eq!(
            message.timestamp,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap()
        );
    }

    #[test]
    fn maps_sms_thread_and_contact() {
        let record = sample_inbound();
        let mappings = BTreeMap::new();
        let thread = record.to_thread(None, &mappings);

        assert_eq!(thread.source_id, "+15755550100");
        assert_eq!(thread.participants.len(), 1);
        assert_eq!(thread.unread_count, Some(1));
        assert!(record.matches_thread(&thread.id.to_string(), None, &mappings));
        assert!(record.matches_thread("5", None, &mappings));
    }

    #[test]
    fn outbound_messages_are_from_owner() {
        let record: TermuxSmsRecord = serde_json::from_value(json!({
            "_id": "abc",
            "address": "575-555-0100",
            "body": "ack",
            "date": "1700000001",
            "type": "sent"
        }))
        .expect("outbound sms parses");

        let mappings = BTreeMap::new();
        let message = record.to_message(Some("+1 575 555 0199"), &mappings);
        assert!(message.is_outbound);
        assert_eq!(message.sender.source_id, "+15755550199");
        assert_eq!(
            message.thread_id,
            thread_uuid(&thread_key(Some("+1 575 555 0199"), "5755550100"))
        );
    }

    #[test]
    fn validates_sms_credentials() {
        let empty = BTreeMap::new();
        assert!(SmsProvider::from_credentials(&empty).is_err());

        let mut credentials = BTreeMap::new();
        credentials.insert("ssh_host".into(), "phone".into());
        credentials.insert("self_number".into(), "+1 575 555 0199".into());
        let provider = SmsProvider::from_credentials(&credentials).expect("provider builds");
        assert_eq!(provider.metadata().id, "sms");
    }
}

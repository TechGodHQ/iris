//! Telegram Bot API provider.
//!
//! Telegram bots can only read messages delivered to the bot. This provider
//! therefore normalizes the visible `getUpdates` backlog rather than pretending
//! to provide an account-wide Telegram archive.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use iris_core::{
    Contact, IrisError, Message, MessageKind, MessageProvider, ProviderCapability,
    ProviderMetadata, Result, Thread,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

const PROVIDER_ID: &str = "telegram";
const METADATA: ProviderMetadata = ProviderMetadata {
    id: PROVIDER_ID,
    name: "Telegram",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::SendMessages,
        ProviderCapability::ListThreads,
        ProviderCapability::ListContacts,
        ProviderCapability::ReceiveRealtime,
    ],
};
const UUID_NAMESPACE: Uuid = Uuid::from_u128(0x6e6c_4e12_1a7d_4a3c_8b53_8914_42f3_0001);

/// Telegram provider backed by the Bot API.
#[derive(Debug, Clone)]
pub struct TelegramProvider {
    client: reqwest::Client,
    base_url: String,
    bot_token: String,
}

impl TelegramProvider {
    /// Create a Telegram provider using the public Bot API endpoint.
    pub fn new(bot_token: impl Into<String>) -> Result<Self> {
        Self::with_base_url(bot_token, "https://api.telegram.org")
    }

    /// Create a Telegram provider with a custom base URL for tests or proxies.
    pub fn with_base_url(
        bot_token: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self> {
        let bot_token = bot_token.into();
        if bot_token.trim().is_empty() {
            return Err(IrisError::Config("telegram bot_token is required".into()));
        }

        Ok(Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            bot_token,
        })
    }

    /// Build from resolved provider credentials.
    pub fn from_credentials(credentials: &BTreeMap<String, String>) -> Result<Self> {
        let token = credentials
            .get("bot_token")
            .or_else(|| credentials.get("token"))
            .ok_or_else(|| {
                IrisError::Config("telegram credentials.bot_token is required".into())
            })?;
        Self::new(token.clone())
    }

    /// Poll Telegram updates once, suitable for callers that want to drive
    /// realtime reception with their own cursor storage.
    pub async fn poll_updates(
        &self,
        offset: Option<i64>,
        timeout_seconds: Option<u32>,
    ) -> Result<Vec<TelegramPolledMessage>> {
        Ok(self
            .get_updates(offset, timeout_seconds)
            .await?
            .into_iter()
            .filter_map(|update| {
                update.message.map(|message| TelegramPolledMessage {
                    update_id: update.update_id,
                    next_offset: update.update_id + 1,
                    message: message.to_message(),
                })
            })
            .collect())
    }

    async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_seconds: Option<u32>,
    ) -> Result<Vec<TelegramUpdate>> {
        let mut query = vec![("allowed_updates".to_owned(), "[\"message\"]".to_owned())];
        if let Some(offset) = offset {
            query.push(("offset".to_owned(), offset.to_string()));
        }
        if let Some(timeout) = timeout_seconds {
            query.push(("timeout".to_owned(), timeout.to_string()));
        }

        let response = self
            .client
            .get(self.method_url("getUpdates"))
            .query(&query)
            .send()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        let envelope: TelegramResponse<Vec<TelegramUpdate>> = response
            .json()
            .await
            .map_err(|error| IrisError::Serialization(error.to_string()))?;
        envelope.into_result()
    }

    async fn resolve_chat_id(&self, thread_id: &str) -> Result<i64> {
        if let Ok(chat_id) = thread_id.parse::<i64>() {
            return Ok(chat_id);
        }
        let requested = Uuid::parse_str(thread_id).map_err(|_| {
            IrisError::Config("telegram thread id must be an Iris UUID or numeric chat id".into())
        })?;
        self.get_updates(None, None)
            .await?
            .into_iter()
            .filter_map(|update| update.message)
            .find_map(|message| {
                (thread_uuid(message.chat.id) == requested).then_some(message.chat.id)
            })
            .ok_or_else(|| IrisError::NotFound(format!("telegram thread not found: {thread_id}")))
    }

    fn method_url(&self, method: &str) -> String {
        format!("{}/bot{}/{}", self.base_url, self.bot_token, method)
    }
}

#[async_trait]
impl MessageProvider for TelegramProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &METADATA
    }

    async fn list_threads(&self, limit: Option<u32>) -> Result<Vec<Thread>> {
        let mut by_chat = BTreeMap::<i64, Thread>::new();
        for update in self.get_updates(None, None).await? {
            if let Some(message) = update.message {
                let thread = message.to_thread();
                by_chat
                    .entry(message.chat.id)
                    .and_modify(|existing| {
                        if thread.last_message_at > existing.last_message_at {
                            existing.last_message_at = thread.last_message_at;
                        }
                        existing.participants.extend(thread.participants.clone());
                        dedupe_contacts(&mut existing.participants);
                    })
                    .or_insert(thread);
            }
        }

        let mut threads: Vec<_> = by_chat.into_values().collect();
        threads.sort_by(|a, b| b.last_message_at.cmp(&a.last_message_at));
        threads.truncate(limit.unwrap_or(50) as usize);
        Ok(threads)
    }

    async fn list_messages(
        &self,
        thread_id: &str,
        before: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        let mut messages: Vec<_> = self
            .get_updates(None, None)
            .await?
            .into_iter()
            .filter_map(|update| update.message)
            .filter(|message| message.chat_matches_thread(thread_id))
            .map(|message| message.to_message())
            .filter(|message| before.is_none_or(|cursor| message.timestamp < cursor))
            .collect();

        messages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        messages.truncate(limit.unwrap_or(50) as usize);
        Ok(messages)
    }

    async fn list_contacts(&self, limit: Option<u32>) -> Result<Vec<Contact>> {
        let mut contacts = Vec::new();
        for update in self.get_updates(None, None).await? {
            if let Some(message) = update.message {
                if let Some(from) = message.from {
                    contacts.push(from.to_contact());
                }
                contacts.extend(
                    message
                        .new_chat_members
                        .iter()
                        .map(TelegramUser::to_contact),
                );
            }
        }
        dedupe_contacts(&mut contacts);
        contacts.truncate(limit.unwrap_or(50) as usize);
        Ok(contacts)
    }

    async fn send_message(&self, thread_id: &str, body: &str) -> Result<Message> {
        let chat_id = self.resolve_chat_id(thread_id).await?;
        let response = self
            .client
            .post(self.method_url("sendMessage"))
            .json(&json!({ "chat_id": chat_id, "text": body }))
            .send()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        let envelope: TelegramResponse<TelegramMessage> = response
            .json()
            .await
            .map_err(|error| IrisError::Serialization(error.to_string()))?;
        Ok(envelope.into_result()?.to_message())
    }
}

#[derive(Debug, Deserialize)]
struct TelegramResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

impl<T> TelegramResponse<T> {
    fn into_result(self) -> Result<T> {
        match (self.ok, self.result) {
            (true, Some(result)) => Ok(result),
            _ => Err(IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: self
                    .description
                    .unwrap_or_else(|| "Telegram API request failed".into()),
            }),
        }
    }
}

/// One normalized Telegram update returned by long polling.
#[derive(Debug, Clone)]
pub struct TelegramPolledMessage {
    /// Telegram update id used to advance the next polling offset.
    pub update_id: i64,
    /// Offset callers should pass to the next `poll_updates` call after processing this update.
    pub next_offset: i64,
    /// Normalized Iris message.
    pub message: Message,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramUpdate {
    update_id: i64,
    message: Option<TelegramMessage>,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramMessage {
    message_id: i64,
    date: i64,
    chat: TelegramChat,
    from: Option<TelegramUser>,
    text: Option<String>,
    caption: Option<String>,
    #[serde(default)]
    photo: Option<Vec<TelegramPhotoSize>>,
    audio: Option<TelegramFilePayload>,
    voice: Option<TelegramFilePayload>,
    video: Option<TelegramFilePayload>,
    document: Option<TelegramFilePayload>,
    sticker: Option<TelegramFilePayload>,
    location: Option<Value>,
    #[serde(default)]
    new_chat_members: Vec<TelegramUser>,
    left_chat_member: Option<TelegramUser>,
    new_chat_title: Option<String>,
    new_chat_photo: Option<Vec<TelegramPhotoSize>>,
    delete_chat_photo: Option<bool>,
    group_chat_created: Option<bool>,
    supergroup_chat_created: Option<bool>,
    channel_chat_created: Option<bool>,
    migrate_to_chat_id: Option<i64>,
    migrate_from_chat_id: Option<i64>,
    pinned_message: Option<Box<Self>>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramChat {
    id: i64,
    #[serde(rename = "type")]
    kind: String,
    title: Option<String>,
    username: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramUser {
    id: i64,
    is_bot: bool,
    first_name: String,
    last_name: Option<String>,
    username: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TelegramPhotoSize {
    file_id: String,
    file_unique_id: String,
    width: u32,
    height: u32,
    file_size: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TelegramFilePayload {
    file_id: String,
    file_unique_id: Option<String>,
    mime_type: Option<String>,
    file_name: Option<String>,
    file_size: Option<u64>,
}

impl TelegramMessage {
    fn chat_matches_thread(&self, thread_id: &str) -> bool {
        self.chat.id.to_string() == thread_id || thread_uuid(self.chat.id).to_string() == thread_id
    }

    fn to_thread(&self) -> Thread {
        let mut participants = Vec::new();
        if let Some(from) = &self.from {
            participants.push(from.to_contact());
        }
        participants.extend(self.new_chat_members.iter().map(TelegramUser::to_contact));
        if let Some(left) = &self.left_chat_member {
            participants.push(left.to_contact());
        }
        dedupe_contacts(&mut participants);

        Thread {
            id: thread_uuid(self.chat.id),
            source: PROVIDER_ID.into(),
            source_id: self.chat.id.to_string(),
            title: self.chat.title(),
            participants,
            last_message_at: unix_timestamp(self.date),
            unread_count: None,
            metadata: json!({
                "chat_type": self.chat.kind,
                "username": self.chat.username,
            }),
        }
    }

    fn to_message(&self) -> Message {
        Message {
            id: message_uuid(self.chat.id, self.message_id),
            thread_id: thread_uuid(self.chat.id),
            source: PROVIDER_ID.into(),
            source_id: self.message_id.to_string(),
            sender: self.sender_contact(),
            kind: self.kind(),
            body: self.body(),
            attachments: self.attachments(),
            timestamp: unix_timestamp(self.date),
            is_outbound: self.from.as_ref().is_some_and(|user| user.is_bot),
            metadata: self.metadata(),
        }
    }

    fn sender_contact(&self) -> Contact {
        self.from.as_ref().map_or_else(
            || Contact {
                id: contact_uuid(format!("chat:{}", self.chat.id).as_bytes()),
                source: PROVIDER_ID.into(),
                source_id: self.chat.id.to_string(),
                display_name: self.chat.title(),
                avatar_url: None,
                metadata: json!({ "chat_sender": true }),
            },
            TelegramUser::to_contact,
        )
    }

    const fn kind(&self) -> MessageKind {
        if self.text.is_some() {
            MessageKind::Text
        } else if self.photo.is_some() {
            MessageKind::Image
        } else if self.audio.is_some() || self.voice.is_some() {
            MessageKind::Audio
        } else if self.video.is_some() {
            MessageKind::Video
        } else if self.document.is_some() {
            MessageKind::File
        } else if self.sticker.is_some() {
            MessageKind::Sticker
        } else if self.location.is_some() {
            MessageKind::Location
        } else if self.is_system_event() {
            MessageKind::System
        } else {
            MessageKind::Unknown
        }
    }

    fn body(&self) -> String {
        self.text
            .clone()
            .or_else(|| self.caption.clone())
            .unwrap_or_default()
    }

    fn metadata(&self) -> Value {
        let mut metadata = serde_json::Map::new();
        metadata.insert("chat_id".into(), json!(self.chat.id));
        metadata.insert("chat_type".into(), json!(self.chat.kind));
        if let Some(photo) = &self.photo {
            metadata.insert("photo".into(), json!(photo));
        }
        for (key, value) in &self.extra {
            metadata.insert(key.clone(), value.clone());
        }
        Value::Object(metadata)
    }

    fn attachments(&self) -> Vec<iris_core::model::Attachment> {
        let mut attachments = Vec::new();
        if let Some(photo) = self.photo.as_ref().and_then(|sizes| sizes.last()) {
            attachments.push(iris_core::model::Attachment {
                mime_type: "image/jpeg".into(),
                url: format!("telegram:file_id:{}", photo.file_id),
                filename: None,
                size: photo.file_size,
            });
        }
        for payload in [
            self.audio.as_ref(),
            self.voice.as_ref(),
            self.video.as_ref(),
            self.document.as_ref(),
            self.sticker.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            attachments.push(iris_core::model::Attachment {
                mime_type: payload
                    .mime_type
                    .clone()
                    .unwrap_or_else(|| "application/octet-stream".into()),
                url: format!("telegram:file_id:{}", payload.file_id),
                filename: payload.file_name.clone(),
                size: payload.file_size,
            });
        }
        attachments
    }

    const fn is_system_event(&self) -> bool {
        self.left_chat_member.is_some()
            || !self.new_chat_members.is_empty()
            || self.new_chat_title.is_some()
            || self.new_chat_photo.is_some()
            || self.delete_chat_photo.is_some()
            || self.group_chat_created.is_some()
            || self.supergroup_chat_created.is_some()
            || self.channel_chat_created.is_some()
            || self.migrate_to_chat_id.is_some()
            || self.migrate_from_chat_id.is_some()
            || self.pinned_message.is_some()
    }
}

impl TelegramChat {
    fn title(&self) -> Option<String> {
        self.title.clone().or_else(|| {
            let mut parts = Vec::new();
            if let Some(first) = &self.first_name {
                parts.push(first.as_str());
            }
            if let Some(last) = &self.last_name {
                parts.push(last.as_str());
            }
            if parts.is_empty() {
                self.username.clone()
            } else {
                Some(parts.join(" "))
            }
        })
    }
}

impl TelegramUser {
    fn to_contact(&self) -> Contact {
        Contact {
            id: contact_uuid(format!("user:{}", self.id).as_bytes()),
            source: PROVIDER_ID.into(),
            source_id: self.id.to_string(),
            display_name: Some(self.display_name()),
            avatar_url: None,
            metadata: json!({
                "username": self.username.clone(),
                "is_bot": self.is_bot,
            }),
        }
    }

    fn display_name(&self) -> String {
        self.last_name.as_ref().map_or_else(
            || self.first_name.clone(),
            |last_name| format!("{} {last_name}", self.first_name),
        )
    }
}

fn dedupe_contacts(contacts: &mut Vec<Contact>) {
    let mut seen = HashMap::<String, usize>::new();
    contacts.retain(|contact| {
        let key = contact.source_id.clone();
        match seen.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(1);
                true
            }
            std::collections::hash_map::Entry::Occupied(_) => false,
        }
    });
}

fn thread_uuid(chat_id: i64) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, format!("thread:{chat_id}").as_bytes())
}

fn message_uuid(chat_id: i64, message_id: i64) -> Uuid {
    Uuid::new_v5(
        &UUID_NAMESPACE,
        format!("message:{chat_id}:{message_id}").as_bytes(),
    )
}

fn contact_uuid(name: &[u8]) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, name)
}

fn unix_timestamp(timestamp: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_text_message() -> TelegramMessage {
        serde_json::from_value(json!({
            "message_id": 42,
            "date": 1_700_000_000,
            "chat": {"id": -100, "type": "group", "title": "Ops"},
            "from": {"id": 7, "is_bot": false, "first_name": "Shiv", "last_name": "Rossi", "username": "shivros"},
            "text": "deploy the thing"
        }))
        .expect("sample message parses")
    }

    #[test]
    fn maps_text_message_to_normalized_message() {
        let message = sample_text_message().to_message();

        assert_eq!(message.source, "telegram");
        assert_eq!(message.source_id, "42");
        assert_eq!(message.thread_id, thread_uuid(-100));
        assert_eq!(message.sender.source_id, "7");
        assert_eq!(message.sender.display_name.as_deref(), Some("Shiv Rossi"));
        assert_eq!(message.kind, MessageKind::Text);
        assert_eq!(message.body, "deploy the thing");
        assert!(!message.is_outbound);
        assert_eq!(message.timestamp, unix_timestamp(1_700_000_000));
    }

    #[test]
    fn maps_group_chat_to_thread() {
        let thread = sample_text_message().to_thread();

        assert_eq!(thread.source, "telegram");
        assert_eq!(thread.source_id, "-100");
        assert_eq!(thread.title.as_deref(), Some("Ops"));
        assert_eq!(thread.participants.len(), 1);
        assert_eq!(thread.last_message_at, unix_timestamp(1_700_000_000));
        assert_eq!(thread.metadata["chat_type"], "group");
    }

    #[test]
    fn maps_media_and_system_message_kinds() {
        let photo: TelegramMessage = serde_json::from_value(json!({
            "message_id": 43,
            "date": 1_700_000_001,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "photo": [{"file_id": "f", "file_unique_id": "u", "width": 1, "height": 1}],
            "caption": "diagram"
        }))
        .expect("photo parses");
        assert_eq!(photo.to_message().kind, MessageKind::Image);
        assert_eq!(photo.to_message().body, "diagram");

        let system: TelegramMessage = serde_json::from_value(json!({
            "message_id": 44,
            "date": 1_700_000_002,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "new_chat_members": [{"id": 9, "is_bot": false, "first_name": "Grace"}]
        }))
        .expect("system parses");
        assert_eq!(system.to_message().kind, MessageKind::System);
    }

    #[test]
    fn validates_credentials() {
        let empty = BTreeMap::new();
        assert!(TelegramProvider::from_credentials(&empty).is_err());

        let mut credentials = BTreeMap::new();
        credentials.insert("bot_token".into(), "123:abc".into());
        assert!(TelegramProvider::from_credentials(&credentials).is_ok());
    }
}

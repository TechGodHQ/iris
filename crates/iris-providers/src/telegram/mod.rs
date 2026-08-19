//! Telegram Bot API provider.
//!
//! Telegram bots can only read messages delivered to the bot. This provider
//! therefore normalizes the visible `getUpdates` backlog rather than pretending
//! to provide an account-wide Telegram archive.
//!
//! Realtime support (audited long polling with bounded subscriber fan-out)
//! lives in the [`realtime`] submodule.

pub mod realtime;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use iris_core::model::Attachment;
use iris_core::outbound::enforce_capability;
use iris_core::{
    AttachmentContent, AttachmentStore, AuditAction, AuditEvent, AuditLog, Contact, IrisError,
    Message, MessageKind, MessageProvider, MessageStream, OutboundMessage, ProviderCapability,
    ProviderMetadata, Result, Thread,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

pub use realtime::{RealtimeHub, RealtimeSettings};

const PROVIDER_ID: &str = "telegram";
const METADATA: ProviderMetadata = ProviderMetadata {
    id: PROVIDER_ID,
    name: "Telegram",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::SendMessages,
        ProviderCapability::SendAttachments,
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
    /// Attachment storage backend. Inbound file bytes are eagerly downloaded
    /// and stored here during normalization so callers receive stable Iris URLs
    /// instead of transient Telegram `file_id` references that expire after ~1h.
    attachments: Arc<dyn AttachmentStore>,
    audit: Option<Arc<dyn AuditLog>>,
    /// Lazily-created realtime hub. Interior mutability without `Clone`
    /// breakage: the hub is created on the first `subscribe_realtime` call
    /// and shared between provider clones via `Arc`.
    realtime: std::sync::OnceLock<Arc<RealtimeHub>>,
    /// Optional realtime poller settings override (validated on hub creation).
    realtime_settings: Option<RealtimeSettings>,
}

impl TelegramProvider {
    /// Create a Telegram provider using the public Bot API endpoint.
    pub fn new(
        bot_token: impl Into<String>,
        attachments: Arc<dyn AttachmentStore>,
    ) -> Result<Self> {
        Self::with_base_url(bot_token, "https://api.telegram.org", attachments)
    }

    /// Create a Telegram provider with a custom base URL for tests or proxies.
    pub fn with_base_url(
        bot_token: impl Into<String>,
        base_url: impl Into<String>,
        attachments: Arc<dyn AttachmentStore>,
    ) -> Result<Self> {
        let bot_token = bot_token.into();
        if bot_token.trim().is_empty() {
            return Err(IrisError::Config("telegram bot_token is required".into()));
        }

        Ok(Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            bot_token,
            attachments,
            audit: None,
            realtime: std::sync::OnceLock::new(),
            realtime_settings: None,
        })
    }

    /// Attach an audit sink for non-secret operation metadata.
    #[must_use]
    pub fn with_audit(mut self, audit: Arc<dyn AuditLog>) -> Self {
        self.audit = Some(audit);
        self
    }

    /// Override realtime poller settings (retry budget, long-poll timeout).
    ///
    /// Settings are validated when the hub is constructed on the first
    /// subscription; invalid settings surface there.
    #[must_use]
    pub const fn with_realtime_settings(mut self, settings: RealtimeSettings) -> Self {
        self.realtime_settings = Some(settings);
        self
    }

    /// The lazily-created realtime hub shared by this provider instance.
    fn realtime_hub(&self) -> Arc<RealtimeHub> {
        let settings = self.realtime_settings.clone().unwrap_or_default();
        self.realtime
            .get_or_init(|| match RealtimeHub::new(settings) {
                Ok(hub) => Arc::new(hub),
                // Settings were validated by construction here; unreachable
                // in practice.
                Err(error) => panic!("invalid realtime settings: {error}"),
            })
            .clone()
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
    pub fn from_credentials(
        credentials: &BTreeMap<String, String>,
        attachments: Arc<dyn AttachmentStore>,
    ) -> Result<Self> {
        let token = credentials
            .get("bot_token")
            .or_else(|| credentials.get("token"))
            .ok_or_else(|| {
                IrisError::Config("telegram credentials.bot_token is required".into())
            })?;
        Self::new(token.clone(), attachments)
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

    /// Send one media request for a resolved attachment, selecting the
    /// Telegram Bot API method by MIME type.
    ///
    /// `sendPhoto` / `sendAudio` / `sendVideo` / `sendDocument` are selected
    /// from the attachment's MIME type; unknown types fall back to
    /// `sendDocument`. The caption rides on whichever request the caller
    /// assigns it to (the first attachment per the outbound contract).
    async fn send_media_request(
        &self,
        chat_id: i64,
        attachment: &iris_core::outbound::ResolvedAttachment,
        caption: &str,
    ) -> Result<Message> {
        let method = telegram_media_method(&attachment.mime_type);
        let filename = attachment
            .filename
            .clone()
            .unwrap_or_else(|| default_filename(&attachment.mime_type));
        let file_part = reqwest::multipart::Part::bytes(attachment.bytes.clone())
            .file_name(filename)
            .mime_str(&attachment.mime_type)
            .map_err(|error| IrisError::Config(format!("invalid attachment MIME type: {error}")))?;
        let form = reqwest::multipart::Form::new()
            .text("chat_id", chat_id.to_string())
            .text("caption", caption.to_owned())
            .part(media_field_name(method), file_part);

        let response = self
            .client
            .post(self.method_url(method))
            .multipart(form)
            .send()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        let envelope: TelegramResponse<TelegramMessage> = response
            .json()
            .await
            .map_err(|error| IrisError::Serialization(error.to_string()))?;
        Ok(envelope.into_result()?.to_message())
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

    /// Build the file download URL for a Telegram file path.
    ///
    /// Telegram files are downloaded via a different path than the Bot API
    /// methods: `{base_url}/file/bot{token}/{file_path}`.
    fn file_download_url(&self, file_path: &str) -> String {
        format!("{}/file/bot{}/{}", self.base_url, self.bot_token, file_path)
    }

    /// Download a file from Telegram and store it via the attachment store.
    ///
    /// Two-step process per the Telegram Bot API docs:
    /// 1. Call `getFile` to obtain the `file_path`.
    /// 2. HTTP GET the file bytes from the download URL.
    ///
    /// Returns the stored `Attachment` with a stable Iris URL.
    async fn download_and_store_attachment(
        &self,
        file_id: &str,
        mime_type: &str,
        filename: Option<&str>,
    ) -> Result<Attachment> {
        // Step 1: getFile
        let response = self
            .client
            .get(self.method_url("getFile"))
            .query(&[("file_id", file_id)])
            .send()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(IrisError::Transport(format!(
                "telegram getFile returned HTTP {}",
                response.status()
            )));
        }
        let envelope: TelegramResponse<TelegramFile> = response
            .json()
            .await
            .map_err(|error| IrisError::Serialization(error.to_string()))?;
        let file = envelope.into_result()?;

        // Step 2: download bytes
        let response = self
            .client
            .get(self.file_download_url(&file.file_path))
            .send()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        if !response.status().is_success() {
            return Err(IrisError::Transport(format!(
                "telegram file download returned HTTP {} for path {}",
                response.status(),
                file.file_path
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;

        let size = bytes.len() as u64;
        let reference = self
            .attachments
            .store(AttachmentContent {
                mime_type: mime_type.to_owned(),
                filename: filename.map(ToOwned::to_owned),
                bytes: bytes.to_vec(),
            })
            .await?;

        Ok(Attachment {
            id: reference.id,
            mime_type: reference.mime_type,
            url: reference.url,
            filename: reference.filename,
            size: Some(size),
        })
    }

    /// Eagerly download and store all attachments for a batch of messages,
    /// rewriting temporary `telegram:file_id:` URLs in place.
    ///
    /// If an individual download fails (e.g. file expired), the attachment
    /// keeps its original pseudo-URL so the message is still visible to
    /// consumers — one expired file should not block the whole listing.
    async fn store_message_attachments(&self, messages: &mut [Message]) -> Result<()> {
        for message in messages.iter_mut() {
            let message_source_id = message.source_id.clone();
            for attachment in &mut message.attachments {
                // Only process pseudo-URLs that haven't been stored yet.
                if !attachment.url.starts_with("telegram:file_id:") {
                    continue;
                }
                let Some(file_id) = attachment.url.strip_prefix("telegram:file_id:") else {
                    continue;
                };
                // Attempt download; on failure log and leave the pseudo-URL.
                match self
                    .download_and_store_attachment(
                        file_id,
                        &attachment.mime_type,
                        attachment.filename.as_deref(),
                    )
                    .await
                {
                    Ok(stored) => {
                        self.record(
                            AuditAction::FetchAttachment,
                            Some(message_source_id.clone()),
                            json!({
                                "operation": "fetch_attachment",
                                "mime_type": stored.mime_type,
                                "filename": stored.filename,
                                "size": stored.size,
                            }),
                        )
                        .await?;
                        *attachment = stored;
                    }
                    Err(error) => {
                        tracing::warn!(
                            file_id = file_id,
                            error = %error,
                            "failed to download telegram attachment; leaving pseudo-URL in place"
                        );
                    }
                }
            }
        }
        Ok(())
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
        threads.sort_by_key(|t| std::cmp::Reverse(t.last_message_at));
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
            .get_updates(None, None)
            .await?
            .into_iter()
            .filter_map(|update| update.message)
            .filter(|message| message.chat_matches_thread(thread_id))
            .map(|message| message.to_message())
            .filter(|message| before.is_none_or(|cursor| message.timestamp < cursor))
            .collect();

        // Eagerly download and store attachment bytes, rewriting pseudo-URLs
        // to stable Iris URLs. Failures are per-attachment, not fatal.
        self.store_message_attachments(&mut messages).await?;

        messages.sort_by_key(|m| m.timestamp);
        messages.truncate(limit.unwrap_or(50) as usize);
        self.record(
            AuditAction::Normalize,
            Some(thread_id.to_owned()),
            json!({ "operation": "list_messages", "count": messages.len() }),
        )
        .await?;
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
        self.record(
            AuditAction::Normalize,
            None,
            json!({ "operation": "list_contacts", "count": contacts.len() }),
        )
        .await?;
        Ok(contacts)
    }

    async fn send_message(&self, thread_id: &str, message: &OutboundMessage) -> Result<Message> {
        enforce_capability(message, PROVIDER_ID, true)?;
        let chat_id = self.resolve_chat_id(thread_id).await?;

        if message.is_text_only() {
            let response = self
                .client
                .post(self.method_url("sendMessage"))
                .json(&json!({ "chat_id": chat_id, "text": message.body }))
                .send()
                .await
                .map_err(|error| IrisError::Transport(error.to_string()))?;
            let envelope: TelegramResponse<TelegramMessage> = response
                .json()
                .await
                .map_err(|error| IrisError::Serialization(error.to_string()))?;
            let sent = envelope.into_result()?.to_message();
            self.record(
                AuditAction::Send,
                Some(thread_id.to_owned()),
                json!({
                    "operation": "send_message",
                    "message_id": sent.source_id,
                    "attachment_count": 0,
                }),
            )
            .await?;
            return Ok(sent);
        }

        // Attachment path: resolve every attachment to concrete bytes before
        // the first external request so a missing/unreadable stored reference
        // fails the send without partially dispatching.
        let resolved =
            iris_core::outbound::resolve_attachments(message, Some(&self.attachments)).await?;

        // First attachment carries the message body as its caption; each
        // additional attachment is a follow-up request with an empty caption.
        // No Telegram albums in this change.
        let mut sent_first: Option<Message> = None;
        let mut dispatched: Vec<iris_core::outbound::ResolvedAttachment> = Vec::new();
        for (index, attachment) in resolved.iter().enumerate() {
            let caption = if index == 0 {
                message.body.clone()
            } else {
                String::new()
            };
            match self.send_media_request(chat_id, attachment, &caption).await {
                Ok(sent) => {
                    if index == 0 {
                        sent_first = Some(sent);
                    }
                    dispatched.push(attachment.clone());
                }
                Err(error) => {
                    // A multi-request send that fails after earlier requests
                    // were accepted records a content-free partial-dispatch
                    // audit event with an explicit non-success outcome; the
                    // overall operation still returns the error.
                    self.record(
                        AuditAction::Send,
                        Some(thread_id.to_owned()),
                        json!({
                            "operation": "send_message",
                            "outcome": "partial_dispatch",
                            "attachment_count": dispatched.len(),
                            "attachments": iris_core::outbound::audit_summaries(&dispatched),
                        }),
                    )
                    .await?;
                    return Err(error);
                }
            }
        }

        let sent = sent_first.expect("attachment loop ran at least once");
        self.record(
            AuditAction::Send,
            Some(thread_id.to_owned()),
            json!({
                "operation": "send_message",
                "message_id": sent.source_id,
                "attachment_count": resolved.len(),
                "attachments": iris_core::outbound::audit_summaries(&resolved),
            }),
        )
        .await?;
        Ok(sent)
    }

    /// Subscribe to a fallible realtime stream of normalized messages.
    ///
    /// Requires an attached audit sink (realtime ingress is audited before
    /// fan-out); without one the subscription is rejected up front with
    /// [`IrisError::RealtimeUnavailable`] rather than yielding unaudited
    /// messages. See [`realtime`] for the delivery contract.
    async fn subscribe_realtime(&self) -> Result<MessageStream> {
        if self.audit.is_none() {
            return Err(IrisError::RealtimeUnavailable {
                provider: PROVIDER_ID.into(),
                code: "audit sink required for realtime ingress".into(),
            });
        }
        // Validate settings here (not inside hub construction) so invalid
        // config surfaces as `IrisError::Config` rather than a panic from
        // `get_or_init`, which cannot return an error.
        if let Some(settings) = &self.realtime_settings {
            settings.validate()?;
        }
        let hub = self.realtime_hub();
        hub.subscribe(self.clone())
    }

    /// Shut down realtime infrastructure owned by this provider.
    ///
    /// Cancels any in-flight long poll, joins the poller task, and ends
    /// every subscriber stream. Idempotent.
    async fn shutdown_realtime(&self) -> Result<()> {
        if let Some(hub) = self.realtime.get() {
            hub.shutdown().await;
        }
        Ok(())
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

/// Response from the Telegram `getFile` API method.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(clippy::struct_field_names)]
struct TelegramFile {
    file_id: String,
    file_unique_id: Option<String>,
    /// Path used to construct the download URL.
    file_path: String,
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
                id: uuid::Uuid::new_v4(),
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
                id: uuid::Uuid::new_v4(),
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

/// Select the Telegram Bot API method for an outbound attachment MIME type.
///
/// `image/*` routes to `sendPhoto`, `audio/*` to `sendAudio`, `video/*` to
/// `sendVideo`; everything else (including unknown types) falls back to
/// `sendDocument`, which accepts arbitrary files.
fn telegram_media_method(mime_type: &str) -> &'static str {
    let mime_type = mime_type.trim().to_ascii_lowercase();
    if mime_type.starts_with("image/") {
        "sendPhoto"
    } else if mime_type.starts_with("audio/") {
        "sendAudio"
    } else if mime_type.starts_with("video/") {
        "sendVideo"
    } else {
        "sendDocument"
    }
}

/// The multipart form field name carrying the media payload for a method.
fn media_field_name(method: &str) -> &'static str {
    match method {
        "sendPhoto" => "photo",
        "sendAudio" => "audio",
        "sendVideo" => "video",
        _ => "document",
    }
}

/// Best-effort filename for attachments that carry none, derived from the
/// common conventional extension of the MIME type.
fn default_filename(mime_type: &str) -> String {
    let extension = match mime_type.trim().to_ascii_lowercase().as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/wav" => "wav",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        _ => "bin",
    };
    format!("attachment.{extension}")
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
    use iris_core::AttachmentRef;
    use iris_core::outbound::OutboundAttachment;
    use std::sync::Mutex;

    /// In-memory attachment store for tests — captures every stored payload
    /// so tests can assert that telegram normalization eagerly persisted bytes
    /// and rewrote pseudo-URLs into Iris URLs.
    #[derive(Default, Debug)]
    struct InMemoryStore {
        entries: Mutex<Vec<(Uuid, AttachmentContent)>>,
    }

    #[async_trait]
    impl AttachmentStore for InMemoryStore {
        async fn store(&self, content: AttachmentContent) -> Result<AttachmentRef> {
            let id = Uuid::new_v4();
            let size = content.bytes.len() as u64;
            let mime_type = content.mime_type.clone();
            let filename = content.filename.clone();
            self.entries.lock().unwrap().push((id, content));
            Ok(AttachmentRef {
                id,
                url: format!("iris://attachment/{id}"),
                mime_type,
                filename,
                size,
            })
        }
        async fn get(&self, id: &Uuid) -> Result<AttachmentContent> {
            self.entries
                .lock()
                .unwrap()
                .iter()
                .find(|(stored_id, _)| stored_id == id)
                .map(|(_, content)| content.clone())
                .ok_or_else(|| IrisError::NotFound(format!("attachment: {id}")))
        }
        async fn delete(&self, id: &Uuid) -> Result<()> {
            self.entries
                .lock()
                .unwrap()
                .retain(|(stored_id, _)| stored_id != id);
            Ok(())
        }
    }

    fn test_store() -> Arc<dyn AttachmentStore> {
        Arc::new(InMemoryStore::default())
    }

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
        assert!(TelegramProvider::from_credentials(&empty, test_store()).is_err());

        let mut credentials = BTreeMap::new();
        credentials.insert("bot_token".into(), "123:abc".into());
        assert!(TelegramProvider::from_credentials(&credentials, test_store()).is_ok());
    }

    #[test]
    fn photo_attachment_uses_pseudo_url_before_storage() {
        // Before store_message_attachments runs, attachments carry
        // telegram:file_id: pseudo-URLs.
        let photo: TelegramMessage = serde_json::from_value(json!({
            "message_id": 50,
            "date": 1_700_000_010,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "photo": [
                {"file_id": "small", "file_unique_id": "u1", "width": 10, "height": 10},
                {"file_id": "large", "file_unique_id": "u2", "width": 100, "height": 100}
            ]
        }))
        .expect("photo parses");
        let message = photo.to_message();
        assert_eq!(message.attachments.len(), 1);
        assert_eq!(message.attachments[0].url, "telegram:file_id:large");
        assert_eq!(message.attachments[0].mime_type, "image/jpeg");
    }

    #[test]
    fn document_attachment_uses_pseudo_url_before_storage() {
        let doc: TelegramMessage = serde_json::from_value(json!({
            "message_id": 51,
            "date": 1_700_000_020,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "document": {
                "file_id": "doc123",
                "file_unique_id": "du",
                "mime_type": "application/pdf",
                "file_name": "report.pdf",
                "file_size": 4096
            }
        }))
        .expect("document parses");
        let message = doc.to_message();
        assert_eq!(message.attachments.len(), 1);
        assert_eq!(message.attachments[0].url, "telegram:file_id:doc123");
        assert_eq!(message.attachments[0].mime_type, "application/pdf");
        assert_eq!(
            message.attachments[0].filename.as_deref(),
            Some("report.pdf")
        );
    }

    #[test]
    fn voice_attachment_uses_pseudo_url_before_storage() {
        let voice: TelegramMessage = serde_json::from_value(json!({
            "message_id": 52,
            "date": 1_700_000_030,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "voice": {
                "file_id": "voice456",
                "file_unique_id": "vu",
                "mime_type": "audio/ogg",
                "duration": 5
            }
        }))
        .expect("voice parses");
        let message = voice.to_message();
        assert_eq!(message.attachments.len(), 1);
        assert_eq!(message.attachments[0].url, "telegram:file_id:voice456");
        assert_eq!(message.attachments[0].mime_type, "audio/ogg");
    }

    #[test]
    fn file_download_url_construction() {
        let store = test_store();
        let provider =
            TelegramProvider::with_base_url("123:abc", "https://api.telegram.org", store)
                .expect("provider builds");
        assert_eq!(
            provider.file_download_url("photos/file_1.jpg"),
            "https://api.telegram.org/file/bot123:abc/photos/file_1.jpg"
        );
    }

    #[test]
    fn telegram_file_deserializes() {
        let file: TelegramFile = serde_json::from_value(json!({
            "file_id": "ABC123",
            "file_unique_id": "unique123",
            "file_path": "documents/file_1.pdf",
            "file_size": 4096
        }))
        .expect("file parses");
        assert_eq!(file.file_id, "ABC123");
        assert_eq!(file.file_path, "documents/file_1.pdf");
        assert_eq!(file.file_size, Some(4096));
    }

    #[tokio::test]
    async fn store_message_attachments_leaves_iris_urls_untouched() {
        // If an attachment already has an Iris URL (e.g. stored by a prior
        // pass), store_message_attachments should not try to re-download it.
        let store: Arc<InMemoryStore> = Arc::new(InMemoryStore::default());
        let provider = TelegramProvider::with_base_url(
            "123:abc",
            "https://api.telegram.invalid",
            store.clone(),
        )
        .expect("provider builds");

        let iris_id = Uuid::new_v4();
        let mut messages = vec![Message {
            id: Uuid::new_v4(),
            thread_id: Uuid::new_v4(),
            source: PROVIDER_ID.into(),
            source_id: "99".into(),
            sender: sample_text_message().to_message().sender,
            kind: MessageKind::Image,
            body: String::new(),
            attachments: vec![Attachment {
                id: iris_id,
                mime_type: "image/jpeg".into(),
                url: format!("iris://attachment/{iris_id}"),
                filename: None,
                size: Some(100),
            }],
            timestamp: unix_timestamp(1_700_000_000),
            is_outbound: false,
            metadata: json!({}),
        }];

        provider
            .store_message_attachments(&mut messages)
            .await
            .expect("attachment storage succeeds");

        // URL should be unchanged — no download attempted.
        assert_eq!(
            messages[0].attachments[0].url,
            format!("iris://attachment/{iris_id}")
        );

        // Store should have zero entries (nothing was stored).
        assert!(
            store.entries.lock().unwrap().is_empty(),
            "no downloads should have occurred for already-stored attachments"
        );
    }

    #[tokio::test]
    async fn store_message_attachments_downloads_and_stores_photo() {
        // T17: Mock the Telegram Bot API getFile + file download endpoints,
        // then verify that store_message_attachments downloads the bytes,
        // stores them, and rewrites the pseudo-URL to an Iris URL.
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let store: Arc<InMemoryStore> = Arc::new(InMemoryStore::default());
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), store.clone())
            .expect("provider builds");

        // Mock the getFile response
        let file_path = "photos/file_123.jpg";
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getFile"))
            .and(query_param("file_id", "photo_file_id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ok": true,
                "result": {
                    "file_id": "photo_file_id",
                    "file_unique_id": "uniq",
                    "file_path": file_path,
                    "file_size": 11
                }
            })))
            .mount(&server)
            .await;

        // Mock the file download endpoint
        let image_bytes = b"hello image";
        Mock::given(method("GET"))
            .and(path(format!("/file/bot123:abc/{file_path}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(image_bytes.to_vec()))
            .mount(&server)
            .await;

        // Build a message with a pseudo-URL attachment
        let photo: TelegramMessage = serde_json::from_value(json!({
            "message_id": 70,
            "date": 1_700_000_050,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "photo": [{"file_id": "photo_file_id", "file_unique_id": "u", "width": 1, "height": 1}]
        }))
        .expect("photo parses");

        let mut messages = vec![photo.to_message()];
        assert_eq!(
            messages[0].attachments[0].url,
            "telegram:file_id:photo_file_id"
        );

        // Run the eager storage pass
        provider
            .store_message_attachments(&mut messages)
            .await
            .expect("attachment storage succeeds");

        // The pseudo-URL should be replaced with an Iris URL
        let attachment = &messages[0].attachments[0];
        assert!(
            attachment.url.starts_with("iris://attachment/"),
            "URL should be an Iris URL, got: {}",
            attachment.url
        );
        assert_eq!(attachment.mime_type, "image/jpeg");
        assert_eq!(attachment.size, Some(11));

        // The store should hold the downloaded bytes
        let stored_bytes = {
            let entries = store.entries.lock().unwrap();
            assert_eq!(entries.len(), 1);
            entries[0].1.bytes.clone()
        };
        assert_eq!(stored_bytes, image_bytes);

        // Verify mime type via a separate lock scope
        let stored_mime = store.entries.lock().unwrap()[0].1.mime_type.clone();
        assert_eq!(stored_mime, "image/jpeg");
    }

    #[tokio::test]
    async fn store_message_attachments_downloads_and_stores_document() {
        // Verify document attachments (with mime_type + filename) are
        // correctly downloaded and stored.
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let store: Arc<InMemoryStore> = Arc::new(InMemoryStore::default());
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), store.clone())
            .expect("provider builds");

        let file_path = "documents/file_456.pdf";
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getFile"))
            .and(query_param("file_id", "doc_file_id"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ok": true,
                "result": {
                    "file_id": "doc_file_id",
                    "file_unique_id": "uniq2",
                    "file_path": file_path
                }
            })))
            .mount(&server)
            .await;

        let pdf_bytes = b"%PDF-1.4 fake pdf content";
        Mock::given(method("GET"))
            .and(path(format!("/file/bot123:abc/{file_path}")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(pdf_bytes.to_vec()))
            .mount(&server)
            .await;

        let doc: TelegramMessage = serde_json::from_value(json!({
            "message_id": 71,
            "date": 1_700_000_060,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "document": {
                "file_id": "doc_file_id",
                "file_unique_id": "du",
                "mime_type": "application/pdf",
                "file_name": "report.pdf"
            }
        }))
        .expect("document parses");

        let mut messages = vec![doc.to_message()];
        assert_eq!(
            messages[0].attachments[0].url,
            "telegram:file_id:doc_file_id"
        );

        provider
            .store_message_attachments(&mut messages)
            .await
            .expect("attachment storage succeeds");

        let attachment = &messages[0].attachments[0];
        assert!(attachment.url.starts_with("iris://attachment/"));
        assert_eq!(attachment.mime_type, "application/pdf");
        assert_eq!(attachment.filename.as_deref(), Some("report.pdf"));
        assert_eq!(attachment.size, Some(pdf_bytes.len() as u64));

        let stored = {
            let entries = store.entries.lock().unwrap();
            assert_eq!(entries.len(), 1);
            entries[0].1.clone()
        };
        assert_eq!(stored.bytes, pdf_bytes);
        assert_eq!(stored.mime_type, "application/pdf");
        assert_eq!(stored.filename.as_deref(), Some("report.pdf"));
    }

    #[tokio::test]
    async fn store_message_attachments_leaves_failed_downloads_in_place() {
        // When the download fails (getFile returns an error), the pseudo-URL
        // should remain so the message is still visible to consumers.
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let store: Arc<InMemoryStore> = Arc::new(InMemoryStore::default());
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), store.clone())
            .expect("provider builds");

        // Mock getFile to return an error
        Mock::given(method("GET"))
            .and(path("/bot123:abc/getFile"))
            .and(query_param("file_id", "expired_file"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ok": false,
                "description": "file is too old"
            })))
            .mount(&server)
            .await;

        let photo: TelegramMessage = serde_json::from_value(json!({
            "message_id": 60,
            "date": 1_700_000_040,
            "chat": {"id": 1, "type": "private", "first_name": "Ada"},
            "photo": [{"file_id": "expired_file", "file_unique_id": "u", "width": 1, "height": 1}]
        }))
        .expect("photo parses");

        let mut messages = vec![photo.to_message()];
        assert_eq!(
            messages[0].attachments[0].url,
            "telegram:file_id:expired_file"
        );

        // store_message_attachments swallows the error and leaves the pseudo-URL.
        provider
            .store_message_attachments(&mut messages)
            .await
            .expect("attachment storage succeeds");

        // The pseudo-URL should still be there.
        assert_eq!(
            messages[0].attachments[0].url,
            "telegram:file_id:expired_file"
        );

        // Store should have zero entries.
        assert!(
            store.entries.lock().unwrap().is_empty(),
            "nothing should have been stored when download failed"
        );
    }

    // ===== T5: outbound multipart media sends =====

    /// Recording audit sink for send-path assertions.
    #[derive(Debug, Default)]
    struct SendAudit {
        events: Mutex<Vec<AuditEvent>>,
    }

    #[async_trait]
    impl iris_core::AuditLog for SendAudit {
        async fn record(&self, event: AuditEvent) -> iris_core::Result<iris_core::AuditEntry> {
            self.events.lock().unwrap().push(event.clone());
            Ok(iris_core::AuditEntry {
                id: Uuid::new_v4(),
                event,
                prev_hash: None,
                self_hash: "test".into(),
            })
        }

        async fn query(
            &self,
            _filter: &iris_core::AuditFilter,
        ) -> iris_core::Result<Vec<iris_core::AuditEntry>> {
            Ok(Vec::new())
        }

        async fn verify_chain(&self) -> iris_core::Result<bool> {
            Ok(true)
        }

        async fn record_once(
            &self,
            _provider: &str,
            _source_id: &str,
            event: AuditEvent,
        ) -> iris_core::Result<iris_core::RecordOutcome> {
            self.events.lock().unwrap().push(event);
            Ok(iris_core::RecordOutcome::Inserted)
        }
    }

    fn send_ok_body() -> serde_json::Value {
        json!({
            "ok": true,
            "result": {
                "message_id": 500,
                "date": 1_700_000_100,
                "chat": {"id": -100, "type": "group", "title": "Ops"},
                "from": {"id": 7, "is_bot": true, "first_name": "Iris", "username": "iris_bot"},
                "text": "sent"
            }
        })
    }

    /// Assert the multipart form posted to a Telegram media method: `chat_id`,
    /// caption, media field name, filename, and that the body carries the
    /// attachment bytes.
    fn assert_media_form(
        request: &wiremock::Request,
        expected_path: &str,
        expected_caption: &str,
        expected_field: &str,
        expected_filename: &str,
        expected_bytes: &[u8],
    ) {
        assert_eq!(request.url.path(), expected_path);
        let content_type = request
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(
            content_type.starts_with("multipart/form-data; boundary="),
            "expected multipart form, got content-type {content_type}"
        );
        let body = request.body.as_slice();
        let boundary = content_type
            .strip_prefix("multipart/form-data; boundary=")
            .expect("boundary present");
        let text = String::from_utf8_lossy(body);
        let parts: Vec<&str> = text.split(&format!("--{boundary}")).collect();
        let field_names: Vec<String> = parts
            .iter()
            .filter_map(|part| {
                let lowered = part.to_ascii_lowercase();
                lowered.find("form-data; name=\"").map(|start| {
                    let rest = &lowered[start + "form-data; name=\"".len()..];
                    let end = rest.find('"').unwrap_or(rest.len());
                    rest[..end].to_owned()
                })
            })
            .collect();
        let field_for = |name: &str| -> String {
            parts
                .iter()
                .find(|part| {
                    part.to_ascii_lowercase()
                        .contains(&format!("form-data; name=\"{name}\""))
                })
                .map(|part| {
                    let idx = part.find("\r\n\r\n").map_or(part.len(), |i| i + 4);
                    part[idx..].trim_end_matches("\r\n").to_owned()
                })
                .unwrap_or_default()
        };
        // Every form should carry chat_id and caption text fields plus the
        // media file part.
        assert!(
            field_names.contains(&"chat_id".to_owned()),
            "chat_id field missing from form: {field_names:?}"
        );
        assert!(
            field_names.contains(&expected_field.to_owned()),
            "media field {expected_field} missing from form: {field_names:?}"
        );
        assert_eq!(field_for("chat_id"), "-100");
        assert_eq!(field_for("caption"), expected_caption);
        // The media part must carry the filename and the raw bytes.
        let media_part = parts
            .iter()
            .find(|part| {
                part.to_ascii_lowercase()
                    .contains(&format!("form-data; name=\"{expected_field}\""))
            })
            .expect("media part present");
        let lowered = media_part.to_ascii_lowercase();
        assert!(
            lowered.contains(&format!("filename=\"{expected_filename}\"")),
            "filename {expected_filename} missing from media part headers"
        );
        assert!(
            media_part
                .as_bytes()
                .windows(expected_bytes.len())
                .any(|window| window == expected_bytes),
            "media part body does not contain the attachment bytes"
        );
    }

    #[tokio::test]
    async fn send_message_photo_routes_to_send_photo_with_caption() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), test_store())
            .expect("provider builds");

        let mock = Mock::given(method("POST"))
            .and(path("/bot123:abc/sendPhoto"))
            .respond_with(ResponseTemplate::new(200).set_body_json(send_ok_body()))
            .expect(1)
            .mount_as_scoped(&server)
            .await;

        let sent = provider
            .send_message(
                "-100",
                &OutboundMessage {
                    body: "see this diagram".to_owned(),
                    attachments: vec![OutboundAttachment::Bytes {
                        mime_type: "image/png".to_owned(),
                        filename: Some("diagram.png".to_owned()),
                        bytes: vec![1, 2, 3, 4],
                    }],
                },
            )
            .await
            .expect("photo send succeeds");

        mock.wait_until_satisfied().await;
        assert_eq!(
            mock.received_requests().await.len(),
            1,
            "exactly one sendPhoto request"
        );
        assert_eq!(sent.source_id, "500");
        assert_eq!(sent.kind, MessageKind::Text);
        let requests = server.received_requests().await.unwrap();
        assert_media_form(
            &requests[0],
            "/bot123:abc/sendPhoto",
            "see this diagram",
            "photo",
            "diagram.png",
            &[1, 2, 3, 4],
        );
    }

    #[tokio::test]
    async fn send_message_routes_by_mime_type() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), test_store())
            .expect("provider builds");

        for (mime, method_path) in [
            ("image/png", "/bot123:abc/sendPhoto"),
            ("audio/mpeg", "/bot123:abc/sendAudio"),
            ("video/mp4", "/bot123:abc/sendVideo"),
            ("application/pdf", "/bot123:abc/sendDocument"),
            ("application/x-iris-unknown", "/bot123:abc/sendDocument"),
        ] {
            let mock = Mock::given(method("POST"))
                .and(path(method_path))
                .respond_with(ResponseTemplate::new(200).set_body_json(send_ok_body()))
                .expect(1)
                .mount_as_scoped(&server)
                .await;

            provider
                .send_message(
                    "-100",
                    &OutboundMessage {
                        body: "file".to_owned(),
                        attachments: vec![OutboundAttachment::Bytes {
                            mime_type: mime.to_owned(),
                            filename: Some("file.bin".to_owned()),
                            bytes: vec![9, 9, 9],
                        }],
                    },
                )
                .await
                .unwrap_or_else(|error| panic!("send for {mime} failed: {error}"));

            mock.wait_until_satisfied().await;
            assert_eq!(
                mock.received_requests().await.len(),
                1,
                "exactly one request for {mime}"
            );
        }
        let requests = server.received_requests().await.unwrap();
        let paths: Vec<String> = requests.iter().map(|r| r.url.path().to_owned()).collect();
        assert_eq!(
            paths,
            vec![
                "/bot123:abc/sendPhoto",
                "/bot123:abc/sendAudio",
                "/bot123:abc/sendVideo",
                "/bot123:abc/sendDocument",
                "/bot123:abc/sendDocument",
            ]
        );
        assert_media_form(
            &requests[2],
            "/bot123:abc/sendVideo",
            "file",
            "video",
            "file.bin",
            &[9, 9, 9],
        );
    }

    #[tokio::test]
    async fn send_message_first_attachment_carries_caption_followups_empty() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), test_store())
            .expect("provider builds");

        // First request: sendPhoto with caption.
        let first = Mock::given(method("POST"))
            .and(path("/bot123:abc/sendPhoto"))
            .respond_with(ResponseTemplate::new(200).set_body_json(send_ok_body()))
            .expect(1)
            .mount_as_scoped(&server)
            .await;
        // Second request: sendDocument, empty caption.
        let second = Mock::given(method("POST"))
            .and(path("/bot123:abc/sendDocument"))
            .respond_with(ResponseTemplate::new(200).set_body_json(send_ok_body()))
            .expect(1)
            .mount_as_scoped(&server)
            .await;

        let message = OutboundMessage {
            body: "two files".to_owned(),
            attachments: vec![
                OutboundAttachment::Bytes {
                    mime_type: "image/png".to_owned(),
                    filename: Some("first.png".to_owned()),
                    bytes: vec![1],
                },
                OutboundAttachment::Bytes {
                    mime_type: "application/pdf".to_owned(),
                    filename: Some("second.pdf".to_owned()),
                    bytes: vec![2, 2],
                },
            ],
        };
        let sent = provider
            .send_message("-100", &message)
            .await
            .expect("two-part send succeeds");

        first.wait_until_satisfied().await;
        second.wait_until_satisfied().await;
        assert_eq!(first.received_requests().await.len(), 1);
        assert_eq!(second.received_requests().await.len(), 1);
        assert_eq!(sent.source_id, "500");
        let requests = server.received_requests().await.unwrap();
        // Order matters: photo (captioned) then document (empty caption).
        let paths: Vec<String> = requests.iter().map(|r| r.url.path().to_owned()).collect();
        assert_eq!(
            paths,
            vec!["/bot123:abc/sendPhoto", "/bot123:abc/sendDocument"]
        );
        assert_media_form(
            &requests[0],
            "/bot123:abc/sendPhoto",
            "two files",
            "photo",
            "first.png",
            &[1],
        );
        assert_media_form(
            &requests[1],
            "/bot123:abc/sendDocument",
            "",
            "document",
            "second.pdf",
            &[2, 2],
        );
    }

    #[tokio::test]
    async fn send_message_partial_dispatch_failure_records_audit_and_errors() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let audit = Arc::new(SendAudit::default());
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), test_store())
            .expect("provider builds")
            .with_audit(audit.clone());

        // First attachment succeeds...
        let ok_mock = Mock::given(method("POST"))
            .and(path("/bot123:abc/sendPhoto"))
            .respond_with(ResponseTemplate::new(200).set_body_json(send_ok_body()))
            .up_to_n_times(1)
            .mount_as_scoped(&server)
            .await;
        // ...second attachment fails.
        let fail_mock = Mock::given(method("POST"))
            .and(path("/bot123:abc/sendDocument"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "ok": false,
                "description": "file is too large"
            })))
            .up_to_n_times(1)
            .mount_as_scoped(&server)
            .await;

        let message = OutboundMessage {
            body: "attempt".to_owned(),
            attachments: vec![
                OutboundAttachment::Bytes {
                    mime_type: "image/png".to_owned(),
                    filename: Some("ok.png".to_owned()),
                    bytes: vec![1],
                },
                OutboundAttachment::Bytes {
                    mime_type: "application/pdf".to_owned(),
                    filename: Some("too-big.pdf".to_owned()),
                    bytes: vec![2, 2, 2],
                },
            ],
        };
        let error = provider
            .send_message("-100", &message)
            .await
            .expect_err("second request failure must error the send");

        assert!(
            error.to_string().contains("file is too large"),
            "error should carry the failed request description: {error}"
        );
        assert_eq!(ok_mock.received_requests().await.len(), 1);
        assert_eq!(fail_mock.received_requests().await.len(), 1);

        let metadata_text = {
            let mut events = audit.events.lock().unwrap();
            assert_eq!(
                events.len(),
                1,
                "exactly one audit event (partial dispatch)"
            );
            let event = events.remove(0);
            drop(events);
            assert_eq!(event.action, AuditAction::Send);
            assert_eq!(event.source_id.as_deref(), Some("-100"));
            assert_eq!(event.metadata["outcome"], "partial_dispatch");
            assert_eq!(event.metadata["attachment_count"], 1);
            let summaries = event.metadata["attachments"].as_array().unwrap();
            assert_eq!(summaries.len(), 1);
            assert_eq!(summaries[0]["filename"], "ok.png");
            event.metadata.to_string()
        };
        // Content-free: no raw bytes or base64 anywhere in the metadata.
        assert!(
            !metadata_text.contains("AQID"),
            "no base64 of [1,2,3,4] in audit"
        );
        assert!(
            !metadata_text.contains("base64"),
            "audit metadata must not contain base64 payloads"
        );
    }

    #[tokio::test]
    async fn send_message_successful_attachment_send_audit_is_content_free() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let audit = Arc::new(SendAudit::default());
        let provider = TelegramProvider::with_base_url("123:abc", server.uri(), test_store())
            .expect("provider builds")
            .with_audit(audit.clone());

        Mock::given(method("POST"))
            .and(path("/bot123:abc/sendPhoto"))
            .respond_with(ResponseTemplate::new(200).set_body_json(send_ok_body()))
            .mount(&server)
            .await;

        let message = OutboundMessage {
            body: "audit me".to_owned(),
            attachments: vec![OutboundAttachment::Bytes {
                mime_type: "image/png".to_owned(),
                filename: Some("a.png".to_owned()),
                bytes: vec![7, 7, 7],
            }],
        };
        provider
            .send_message("-100", &message)
            .await
            .expect("send succeeds");

        let metadata_text = {
            let mut events = audit.events.lock().unwrap();
            assert_eq!(events.len(), 1);
            let event = events.remove(0);
            drop(events);
            assert_eq!(event.action, AuditAction::Send);
            assert_eq!(event.metadata["attachment_count"], 1);
            assert_eq!(event.metadata["attachments"][0]["filename"], "a.png");
            assert_eq!(event.metadata["attachments"][0]["mime_type"], "image/png");
            assert_eq!(event.metadata["attachments"][0]["byte_count"], 3);
            assert!(event.metadata.get("message_id").is_some());
            event.metadata.to_string()
        };
        assert!(
            !metadata_text.contains("Bwc"),
            "no base64 of [7,7,7] in audit"
        );
        assert!(!metadata_text.contains("base64"));
    }
}

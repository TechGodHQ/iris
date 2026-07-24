//! Email provider backed by IMAP for reads and SMTP for sends.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use iris_core::model::Attachment;
use iris_core::{
    Contact, IrisError, Message, MessageKind, MessageProvider, ProviderCapability,
    ProviderMetadata, Result, Thread,
};
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message as SmtpMessage, Tokio1Executor};
use mail_parser::{Address as ParsedAddress, HeaderValue, MessageParser, MimeHeaders, PartType};
use native_tls::TlsConnector;
use serde_json::json;
use uuid::Uuid;

const PROVIDER_ID: &str = "email";
const METADATA: ProviderMetadata = ProviderMetadata {
    id: PROVIDER_ID,
    name: "Email",
    capabilities: &[
        ProviderCapability::ListMessages,
        ProviderCapability::SendMessages,
        ProviderCapability::ListThreads,
        ProviderCapability::ListContacts,
    ],
};
const UUID_NAMESPACE: Uuid = Uuid::from_u128(0x6e6c_4e12_1a7d_4a3c_8b53_8914_42f3_0002);

const DEFAULT_PAGE_SIZE: u32 = 50;
const DEFAULT_MAX_MESSAGES: u32 = 500;

/// Email provider using IMAP for reads and SMTP for outbound messages.
#[derive(Debug, Clone)]
pub struct EmailProvider {
    imap_host: String,
    imap_port: u16,
    smtp_host: String,
    smtp_port: u16,
    username: String,
    password: String,
    mailbox: String,
    from: String,
    page_size: u32,
    max_messages: u32,
}

impl EmailProvider {
    /// Build an email provider from explicit connection settings.
    pub fn new(config: EmailProviderConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            imap_host: config.imap_host,
            imap_port: config.imap_port,
            smtp_host: config.smtp_host,
            smtp_port: config.smtp_port,
            username: config.username,
            password: config.password,
            mailbox: config.mailbox,
            from: config.from,
            page_size: config.page_size,
            max_messages: config.max_messages,
        })
    }

    /// Build from resolved provider credentials.
    pub fn from_credentials(credentials: &BTreeMap<String, String>) -> Result<Self> {
        Self::new(EmailProviderConfig {
            imap_host: required(credentials, "imap_host")?,
            imap_port: optional_port(credentials, "imap_port", 993)?,
            smtp_host: required(credentials, "smtp_host")?,
            smtp_port: optional_port(credentials, "smtp_port", 587)?,
            username: required(credentials, "username")?,
            password: required(credentials, "password")?,
            mailbox: credentials
                .get("mailbox")
                .cloned()
                .unwrap_or_else(|| "INBOX".into()),
            from: credentials
                .get("from")
                .cloned()
                .unwrap_or_else(|| credentials["username"].clone()),
            page_size: optional_u32(credentials, "page_size", DEFAULT_PAGE_SIZE)?,
            max_messages: optional_u32(credentials, "max_messages", DEFAULT_MAX_MESSAGES)?,
        })
    }

    async fn fetch_messages(&self, options: FetchOptions) -> Result<FetchResult> {
        let config = self.clone();
        tokio::task::spawn_blocking(move || config.fetch_messages_blocking(&options))
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?
    }

    fn fetch_messages_blocking(&self, options: &FetchOptions) -> Result<FetchResult> {
        let tls = TlsConnector::builder()
            .build()
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        let client = imap::connect(
            (self.imap_host.as_str(), self.imap_port),
            self.imap_host.as_str(),
            &tls,
        )
        .map_err(|error| IrisError::Transport(error.to_string()))?;
        let mut session = client
            .login(&self.username, &self.password)
            .map_err(|(error, _)| IrisError::Transport(error.to_string()))?;
        let mailbox = session
            .select(&self.mailbox)
            .map_err(|error| IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: error.to_string(),
            })?;
        let uid_validity = mailbox.uid_validity;

        let uids = session
            .uid_search("ALL")
            .map_err(|error| IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: error.to_string(),
            })?;
        let mut uids: Vec<_> = uids.into_iter().collect();
        uids = select_uids(uids, options, self.max_messages);

        if uids.is_empty() {
            let _ = session.logout();
            return Ok(FetchResult {
                messages: Vec::new(),
                uid_validity,
                last_uid: None,
            });
        }

        // The highest UID in the selected set — callers can persist this
        // as a sync cursor and pass it as `since_uid` on the next call.
        let last_uid = *uids.last().unwrap();

        // Fetch in pages to avoid sending one massive request that could
        // overwhelm the server or consume excessive memory.
        let page_size = self.page_size as usize;
        let mut messages = Vec::with_capacity(uids.len());
        for chunk in uids.chunks(page_size) {
            let sequence = chunk
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let fetches =
                session
                    .uid_fetch(sequence, "RFC822")
                    .map_err(|error| IrisError::Provider {
                        provider: PROVIDER_ID.into(),
                        message: error.to_string(),
                    })?;
            for fetch in &fetches {
                if let Some(body) = fetch.body() {
                    messages.push(parse_email(body));
                }
            }
        }

        let _ = session.logout();
        Ok(FetchResult {
            messages,
            uid_validity,
            last_uid: Some(last_uid),
        })
    }

    async fn send_email(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
        thread_id: Uuid,
        in_reply_to: Option<&str>,
        references: &[String],
    ) -> Result<Message> {
        let from: Mailbox = self
            .from
            .parse()
            .map_err(|error| IrisError::Config(format!("invalid email from address: {error}")))?;
        let to: Mailbox = recipient
            .parse()
            .map_err(|error| IrisError::Config(format!("invalid email recipient: {error}")))?;
        let mut builder = SmtpMessage::builder().from(from).to(to).subject(subject);
        if let Some(message_id) = in_reply_to {
            builder = builder.in_reply_to(format_msgid(message_id));
        }
        if !references.is_empty() {
            let joined = references
                .iter()
                .map(|r| format_msgid(r))
                .collect::<Vec<_>>()
                .join(" ");
            builder = builder.references(joined);
        }
        let email = builder
            .body(body.to_owned())
            .map_err(|error| IrisError::Serialization(error.to_string()))?;
        let credentials = Credentials::new(self.username.clone(), self.password.clone());
        let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&self.smtp_host)
            .map_err(|error| IrisError::Transport(error.to_string()))?
            .port(self.smtp_port)
            .credentials(credentials)
            .build();
        mailer
            .send(email)
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?;
        Ok(outbound_message(
            &self.from, recipient, subject, body, thread_id,
        ))
    }

    fn configured_from_address(&self) -> String {
        self.from.parse::<Mailbox>().map_or_else(
            |_| self.from.to_lowercase(),
            |mailbox| mailbox.email.to_string().to_lowercase(),
        )
    }
}

/// Connection settings for [`EmailProvider`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailProviderConfig {
    /// IMAP server hostname.
    pub imap_host: String,
    /// IMAP TLS port.
    pub imap_port: u16,
    /// SMTP server hostname.
    pub smtp_host: String,
    /// SMTP STARTTLS port.
    pub smtp_port: u16,
    /// Login username.
    pub username: String,
    /// Login password or app password.
    pub password: String,
    /// IMAP mailbox to read.
    pub mailbox: String,
    /// Sender address used for SMTP.
    pub from: String,
    /// Number of messages to fetch per IMAP request (default 50).
    pub page_size: u32,
    /// Maximum number of messages to fetch in total, even when no limit is
    /// requested by the caller (default 500). Protects against loading an
    /// entire mailbox in one operation.
    pub max_messages: u32,
}

impl Default for EmailProviderConfig {
    fn default() -> Self {
        Self {
            imap_host: String::new(),
            imap_port: 993,
            smtp_host: String::new(),
            smtp_port: 587,
            username: String::new(),
            password: String::new(),
            mailbox: "INBOX".into(),
            from: String::new(),
            page_size: DEFAULT_PAGE_SIZE,
            max_messages: DEFAULT_MAX_MESSAGES,
        }
    }
}

impl EmailProviderConfig {
    fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("imap_host", &self.imap_host),
            ("smtp_host", &self.smtp_host),
            ("username", &self.username),
            ("password", &self.password),
            ("mailbox", &self.mailbox),
            ("from", &self.from),
        ] {
            if value.trim().is_empty() {
                return Err(IrisError::Config(format!("email {name} is required")));
            }
        }
        if self.page_size == 0 {
            return Err(IrisError::Config(
                "email page_size must be at least 1".into(),
            ));
        }
        if self.max_messages == 0 {
            return Err(IrisError::Config(
                "email max_messages must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Controls which UIDs [`EmailProvider::fetch_messages`] selects.
///
/// At most one of `since_uid` or `limit` should be `Some`. If both are `None`,
/// the provider returns the most recent messages up to its configured safety
/// cap (`max_messages`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FetchOptions {
    /// If set, only fetch messages whose UID is greater than this value
    /// (incremental sync cursor). The result is ordered oldest-first.
    pub since_uid: Option<u32>,
    /// Maximum number of messages to return, applied after the UID filter.
    pub limit: Option<u32>,
}

/// The result of a fetch, including mailbox metadata needed for incremental sync.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FetchResult {
    /// Parsed email envelopes, ordered oldest-first.
    pub messages: Vec<EmailEnvelope>,
    /// The UIDVALIDITY of the mailbox at the time of the fetch. If this
    /// changes between syncs, all cached UIDs are invalidated.
    pub uid_validity: Option<u32>,
    /// The highest UID in the fetched set. Callers can persist this as a
    /// sync cursor and pass it as `FetchOptions::since_uid` on the next call
    /// to retrieve only messages that arrived since.
    pub last_uid: Option<u32>,
}

/// Select a UID set based on fetch options.
///
/// - `since_uid` filters to UIDs strictly greater than the cursor.
/// - `limit` caps the result to the most recent `limit` UIDs.
/// - `max_messages` is an absolute safety cap.
fn select_uids(mut uids: Vec<u32>, options: &FetchOptions, max_messages: u32) -> Vec<u32> {
    uids.sort_unstable();
    uids.dedup();

    if let Some(since) = options.since_uid {
        uids.retain(|uid| *uid > since);
    }

    let max = options
        .limit
        .map_or(max_messages, |limit| limit.min(max_messages)) as usize;

    if uids.len() > max {
        uids = uids.split_off(uids.len() - max);
    }

    uids
}

#[async_trait]
impl MessageProvider for EmailProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &METADATA
    }

    async fn list_threads(&self, limit: Option<u32>) -> Result<Vec<Thread>> {
        let result = self
            .fetch_messages(FetchOptions {
                limit,
                ..Default::default()
            })
            .await?;
        let mut by_thread = BTreeMap::<String, Thread>::new();
        for email in result.messages {
            let thread = email.to_thread();
            by_thread
                .entry(email.thread_key())
                .and_modify(|existing| {
                    if thread.last_message_at > existing.last_message_at {
                        existing.last_message_at = thread.last_message_at;
                    }
                    existing.participants.extend(thread.participants.clone());
                    dedupe_contacts(&mut existing.participants);
                })
                .or_insert(thread);
        }
        let mut threads: Vec<_> = by_thread.into_values().collect();
        threads.sort_by_key(|thread| std::cmp::Reverse(thread.last_message_at));
        threads.truncate(limit.unwrap_or(50) as usize);
        Ok(threads)
    }

    async fn list_messages(
        &self,
        thread_id: &str,
        before: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<Message>> {
        // Fetch a bounded recent window for thread filtering. Unlike
        // list_threads/list_contacts which can use the caller's limit directly,
        // list_messages needs to search through messages to find thread matches,
        // so we fetch up to max_messages and filter locally.
        let result = self
            .fetch_messages(FetchOptions {
                limit: Some(self.max_messages),
                ..Default::default()
            })
            .await?;
        let mut messages: Vec<_> = result
            .messages
            .into_iter()
            .filter(|email| email.matches_thread(thread_id))
            .map(|email| email.to_message_from(&self.configured_from_address()))
            .filter(|message| before.is_none_or(|cursor| message.timestamp < cursor))
            .collect();
        messages.sort_by_key(|message| message.timestamp);
        messages.truncate(limit.unwrap_or(50) as usize);
        Ok(messages)
    }

    async fn list_contacts(&self, limit: Option<u32>) -> Result<Vec<Contact>> {
        let result = self
            .fetch_messages(FetchOptions {
                limit,
                ..Default::default()
            })
            .await?;
        let mut contacts = Vec::new();
        for email in result.messages {
            contacts.extend(email.contacts());
        }
        dedupe_contacts(&mut contacts);
        contacts.truncate(limit.unwrap_or(50) as usize);
        Ok(contacts)
    }

    async fn send_message(&self, thread_id: &str, body: &str) -> Result<Message> {
        if let Ok(parsed) = thread_id.parse::<Mailbox>() {
            return self
                .send_email(
                    parsed.email.as_ref(),
                    "Iris message",
                    body,
                    uuid_for(format!("thread:{}", parsed.email).as_bytes()),
                    None,
                    &[],
                )
                .await;
        }

        if let Some(reply_context) = self.reply_context(thread_id).await? {
            return self
                .send_email(
                    &reply_context.recipient,
                    &reply_context.reply_subject(),
                    body,
                    reply_context.thread_id,
                    reply_context.message_id.as_deref(),
                    &reply_context.references,
                )
                .await;
        }

        Err(IrisError::NotFound(format!(
            "email thread not found and value is not an email recipient: {thread_id}"
        )))
    }
}

impl EmailProvider {
    async fn reply_context(&self, thread_id: &str) -> Result<Option<EmailReplyContext>> {
        let result = self
            .fetch_messages(FetchOptions {
                limit: Some(self.max_messages),
                ..Default::default()
            })
            .await?;
        let mut messages: Vec<_> = result
            .messages
            .into_iter()
            .filter(|email| email.matches_thread(thread_id))
            .collect();
        messages.sort_by_key(|message| message.date);
        let Some(latest) = messages.last() else {
            return Ok(None);
        };
        let from_lc = self.configured_from_address();
        let recipient = latest
            .from
            .iter()
            .chain(latest.to.iter())
            .chain(latest.cc.iter())
            .find(|address| address.address.to_lowercase() != from_lc)
            .map(|address| address.address.clone())
            .ok_or_else(|| {
                IrisError::NotFound(format!("email thread has no reply recipient: {thread_id}"))
            })?;

        Ok(Some(EmailReplyContext {
            recipient,
            subject: latest.subject.clone(),
            thread_id: latest.thread_id(),
            message_id: Some(latest.source_id.clone()),
            references: build_reply_references(latest),
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmailReplyContext {
    recipient: String,
    subject: Option<String>,
    thread_id: Uuid,
    message_id: Option<String>,
    references: Vec<String>,
}

impl EmailReplyContext {
    fn reply_subject(&self) -> String {
        self.subject.as_ref().map_or_else(
            || "Iris message".into(),
            |subject| {
                if subject.to_ascii_lowercase().starts_with("re:") {
                    subject.clone()
                } else {
                    format!("Re: {subject}")
                }
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmailEnvelope {
    source_id: String,
    subject: Option<String>,
    from: Vec<EmailAddress>,
    to: Vec<EmailAddress>,
    cc: Vec<EmailAddress>,
    date: DateTime<Utc>,
    body: String,
    attachments: Vec<Attachment>,
    references: Vec<String>,
    in_reply_to: Option<String>,
}

impl EmailEnvelope {
    fn thread_key(&self) -> String {
        self.references
            .first()
            .cloned()
            .or_else(|| self.in_reply_to.clone())
            .unwrap_or_else(|| self.source_id.clone())
    }

    fn thread_id(&self) -> Uuid {
        uuid_for(format!("thread:{}", self.thread_key()).as_bytes())
    }

    fn matches_thread(&self, thread_id: &str) -> bool {
        self.thread_key() == thread_id || self.thread_id().to_string() == thread_id
    }

    fn to_thread(&self) -> Thread {
        let mut participants = self.contacts();
        dedupe_contacts(&mut participants);
        Thread {
            id: self.thread_id(),
            source: PROVIDER_ID.into(),
            source_id: self.thread_key(),
            title: self.subject.clone(),
            participants,
            last_message_at: self.date,
            unread_count: None,
            metadata: json!({
                "message_id": self.source_id,
                "references": self.references,
                "in_reply_to": self.in_reply_to,
            }),
        }
    }

    #[cfg(test)]
    fn to_message(&self) -> Message {
        self.to_message_from("")
    }

    fn to_message_from(&self, from_address: &str) -> Message {
        Message {
            id: uuid_for(format!("message:{}", self.source_id).as_bytes()),
            thread_id: self.thread_id(),
            source: PROVIDER_ID.into(),
            source_id: self.source_id.clone(),
            sender: self.sender(),
            kind: if self.body.contains('<') && self.body.contains('>') {
                MessageKind::RichText
            } else {
                MessageKind::Text
            },
            body: self.body.clone(),
            attachments: self.attachments.clone(),
            timestamp: self.date,
            is_outbound: !from_address.is_empty()
                && self
                    .from
                    .iter()
                    .any(|address| address.address.eq_ignore_ascii_case(from_address)),
            metadata: json!({
                "subject": self.subject,
                "to": self.to.iter().map(EmailAddress::display).collect::<Vec<_>>(),
                "cc": self.cc.iter().map(EmailAddress::display).collect::<Vec<_>>(),
            }),
        }
    }

    fn contacts(&self) -> Vec<Contact> {
        self.from
            .iter()
            .chain(self.to.iter())
            .chain(self.cc.iter())
            .map(EmailAddress::to_contact)
            .collect()
    }

    fn sender(&self) -> Contact {
        self.from.first().map_or_else(
            || {
                EmailAddress {
                    name: None,
                    address: "unknown@example.invalid".into(),
                }
                .to_contact()
            },
            EmailAddress::to_contact,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmailAddress {
    name: Option<String>,
    address: String,
}

impl EmailAddress {
    fn display(&self) -> String {
        self.name.as_ref().map_or_else(
            || self.address.clone(),
            |name| format!("{name} <{}>", self.address),
        )
    }

    fn to_contact(&self) -> Contact {
        Contact {
            id: uuid_for(format!("contact:{}", self.address.to_lowercase()).as_bytes()),
            source: PROVIDER_ID.into(),
            source_id: self.address.clone(),
            display_name: self.name.clone(),
            avatar_url: None,
            metadata: json!({ "email": self.address }),
        }
    }
}

fn parse_email(raw: &[u8]) -> EmailEnvelope {
    let Some(parsed) = MessageParser::default().parse(raw) else {
        return parse_lossy_email(raw);
    };
    let source_id = parsed
        .message_id()
        .map_or_else(|| format!("uuid-v5:{}", uuid_for(raw)), ToOwned::to_owned);
    EmailEnvelope {
        source_id: source_id.clone(),
        subject: parsed.subject().map(ToOwned::to_owned),
        from: parsed.from().map(addresses).unwrap_or_default(),
        to: parsed.to().map(addresses).unwrap_or_default(),
        cc: parsed.cc().map(addresses).unwrap_or_default(),
        date: parsed
            .date()
            .and_then(|date| Utc.timestamp_opt(date.to_timestamp(), 0).single())
            .unwrap_or_else(Utc::now),
        body: parsed
            .body_text(0)
            .or_else(|| parsed.body_html(0))
            .map(std::borrow::Cow::into_owned)
            .unwrap_or_default(),
        attachments: parsed
            .attachments()
            .enumerate()
            .map(|(index, part)| attachment_from_part(&source_id, index, part))
            .collect(),
        references: header_text_values(parsed.references()),
        in_reply_to: header_text_values(parsed.in_reply_to()).into_iter().next(),
    }
}

fn parse_lossy_email(raw: &[u8]) -> EmailEnvelope {
    let text = String::from_utf8_lossy(raw);
    let (headers, body) = split_headers_body(&text);
    let headers = parse_headers(headers);
    EmailEnvelope {
        source_id: headers
            .get("message-id")
            .and_then(|values| values.first())
            .cloned()
            .unwrap_or_else(|| format!("uuid-v5:{}", uuid_for(raw))),
        subject: first_header(&headers, "subject"),
        from: parse_address_list(
            first_header(&headers, "from")
                .as_deref()
                .unwrap_or_default(),
        ),
        to: parse_address_list(first_header(&headers, "to").as_deref().unwrap_or_default()),
        cc: parse_address_list(first_header(&headers, "cc").as_deref().unwrap_or_default()),
        date: first_header(&headers, "date")
            .and_then(|value| DateTime::parse_from_rfc2822(&value).ok())
            .map_or_else(Utc::now, |date| date.with_timezone(&Utc)),
        body: body.trim().to_owned(),
        attachments: Vec::new(),
        references: first_header(&headers, "references")
            .map(|value| value.split_whitespace().map(ToOwned::to_owned).collect())
            .unwrap_or_default(),
        in_reply_to: first_header(&headers, "in-reply-to"),
    }
}

fn addresses(addresses: &ParsedAddress<'_>) -> Vec<EmailAddress> {
    addresses
        .iter()
        .filter_map(|address| {
            Some(EmailAddress {
                name: address.name().map(ToOwned::to_owned),
                address: address.address()?.to_owned(),
            })
        })
        .collect()
}

fn header_text_values(value: &HeaderValue<'_>) -> Vec<String> {
    match value {
        HeaderValue::Text(text) => vec![text.to_string()],
        HeaderValue::TextList(values) => values.iter().map(ToString::to_string).collect(),
        _ => Vec::new(),
    }
}

fn attachment_from_part(
    source_id: &str,
    index: usize,
    part: &mail_parser::MessagePart<'_>,
) -> Attachment {
    let filename = part.attachment_name().map(ToOwned::to_owned);
    let mime_type = part.content_type().map_or_else(
        || "application/octet-stream".into(),
        |content_type| {
            content_type.c_subtype.as_ref().map_or_else(
                || content_type.c_type.to_string(),
                |subtype| format!("{}/{}", content_type.c_type, subtype),
            )
        },
    );
    Attachment {
        id: Uuid::new_v4(),
        mime_type,
        url: format!("email:message:{source_id}:attachment:{index}"),
        filename,
        size: Some(match &part.body {
            PartType::Binary(bytes) | PartType::InlineBinary(bytes) => bytes.len() as u64,
            PartType::Text(text) | PartType::Html(text) => text.len() as u64,
            PartType::Message(message) => message.raw_message().len() as u64,
            PartType::Multipart(_) => 0,
        }),
    }
}

fn split_headers_body(input: &str) -> (&str, &str) {
    input
        .split_once("\r\n\r\n")
        .or_else(|| input.split_once("\n\n"))
        .unwrap_or((input, ""))
}

fn parse_headers(input: &str) -> BTreeMap<String, Vec<String>> {
    let mut headers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in input.lines() {
        if line.starts_with(' ') || line.starts_with('\t') {
            if let Some(name) = &current
                && let Some(value) = headers.get_mut(name).and_then(|values| values.last_mut())
            {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            headers
                .entry(name.clone())
                .or_default()
                .push(value.trim().to_owned());
            current = Some(name);
        }
    }
    headers
}

fn first_header(headers: &BTreeMap<String, Vec<String>>, name: &str) -> Option<String> {
    headers.get(name).and_then(|values| values.first()).cloned()
}

fn parse_address_list(input: &str) -> Vec<EmailAddress> {
    input
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            if let Some((name, address)) = value.rsplit_once('<') {
                EmailAddress {
                    name: Some(name.trim().trim_matches('"').to_owned())
                        .filter(|name| !name.is_empty()),
                    address: address.trim_end_matches('>').trim().to_owned(),
                }
            } else {
                EmailAddress {
                    name: None,
                    address: value.to_owned(),
                }
            }
        })
        .collect()
}

fn outbound_message(
    from: &str,
    recipient: &str,
    subject: &str,
    body: &str,
    thread_id: Uuid,
) -> Message {
    let sender = EmailAddress {
        name: None,
        address: from.into(),
    }
    .to_contact();
    let recipient = EmailAddress {
        name: None,
        address: recipient.into(),
    }
    .to_contact();
    let now = Utc::now();
    Message {
        id: Uuid::new_v4(),
        thread_id,
        source: PROVIDER_ID.into(),
        source_id: format!("smtp:{}", now.timestamp_millis()),
        sender,
        kind: MessageKind::Text,
        body: body.into(),
        attachments: Vec::new(),
        timestamp: now,
        is_outbound: true,
        metadata: json!({
            "transport": "smtp",
            "subject": subject,
            "to": [recipient.source_id],
        }),
    }
}

fn required(credentials: &BTreeMap<String, String>, key: &str) -> Result<String> {
    credentials
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| IrisError::Config(format!("email credentials.{key} is required")))
}

fn optional_port(credentials: &BTreeMap<String, String>, key: &str, default: u16) -> Result<u16> {
    credentials.get(key).map_or(Ok(default), |value| {
        value.parse().map_err(|error| {
            IrisError::Config(format!("email credentials.{key} is invalid: {error}"))
        })
    })
}

fn optional_u32(credentials: &BTreeMap<String, String>, key: &str, default: u32) -> Result<u32> {
    credentials.get(key).map_or(Ok(default), |value| {
        value.parse().map_err(|error| {
            IrisError::Config(format!("email credentials.{key} is invalid: {error}"))
        })
    })
}

fn uuid_for(input: &[u8]) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, input)
}

/// Build the `References` header for a reply per RFC 5322 §3.6.4.
///
/// The reply's References should list the original message's references chain
/// (if any) followed by the original message's own Message-ID. This preserves
/// the full ancestry so mail clients can thread the conversation correctly.
fn build_reply_references(original: &EmailEnvelope) -> Vec<String> {
    let msg_id = strip_brackets(&original.source_id);
    let mut refs: Vec<String> = original
        .references
        .iter()
        .map(|r| strip_brackets(r).to_owned())
        .collect();
    if !refs.iter().any(|existing| existing == msg_id) {
        refs.push(msg_id.to_owned());
    }
    refs
}

/// Strip surrounding angle brackets from a message ID, if present.
fn strip_brackets(id: &str) -> &str {
    let id = id.trim();
    if id.starts_with('<') && id.ends_with('>') && id.len() >= 2 {
        &id[1..id.len() - 1]
    } else {
        id
    }
}

/// Wrap a message ID in RFC 5322 angle brackets (`<msg-id>`), if not already
/// bracketed. The `In-Reply-To` and `References` headers require bracketed
/// msg-ids per RFC 5322 §3.6.4.
fn format_msgid(id: &str) -> String {
    let stripped = strip_brackets(id);
    format!("<{stripped}>")
}

fn dedupe_contacts(contacts: &mut Vec<Contact>) {
    let mut seen = BTreeSet::new();
    contacts.retain(|contact| seen.insert(contact.source_id.to_lowercase()));
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &[u8] = b"Message-ID: <root@example.com>\r\nSubject: Status\r\nFrom: Alice <alice@example.com>\r\nTo: Bob <bob@example.com>\r\nCc: Carol <carol@example.com>\r\nDate: Tue, 14 Jul 2026 12:00:00 +0000\r\nContent-Type: multipart/mixed; boundary=demo\r\n\r\n--demo\r\nContent-Type: text/plain\r\n\r\nHello from email\r\n--demo\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=notes.txt\r\n\r\nsecret notes\r\n--demo--\r\n";

    #[test]
    fn parses_message_contacts_and_attachments() {
        let email = parse_email(SAMPLE);

        assert_eq!(email.source_id, "root@example.com");
        assert_eq!(email.subject.as_deref(), Some("Status"));
        assert_eq!(email.from[0].address, "alice@example.com");
        assert_eq!(email.to[0].address, "bob@example.com");
        assert_eq!(email.cc[0].address, "carol@example.com");
        assert_eq!(email.attachments.len(), 1);
        assert_eq!(email.attachments[0].filename.as_deref(), Some("notes.txt"));

        let message = email.to_message();
        assert_eq!(message.source, "email");
        assert_eq!(message.sender.source_id, "alice@example.com");
        assert!(message.body.contains("Hello from email"));
    }

    #[test]
    fn references_define_stable_threads() {
        let email = parse_email(
            b"Message-ID: <reply@example.com>\nReferences: <root@example.com> <mid@example.com>\nFrom: a@example.com\nTo: b@example.com\n\nbody",
        );

        assert_eq!(email.thread_key(), "root@example.com");
        assert!(email.matches_thread(&email.thread_id().to_string()));
    }

    #[test]
    fn validates_required_credentials() {
        let error = EmailProvider::from_credentials(&BTreeMap::new()).expect_err("missing config");
        assert!(error.to_string().contains("imap_host"));
    }

    #[test]
    fn reply_references_include_original_message_id() {
        // Original message with no References chain — reply should still
        // reference the original Message-ID.
        let original = parse_email(
            b"Message-ID: <orig@example.com>\nFrom: a@example.com\nTo: b@example.com\n\nbody",
        );
        let refs = build_reply_references(&original);
        assert_eq!(refs, vec!["orig@example.com"]);
    }

    #[test]
    fn reply_references_extend_existing_chain() {
        // Original message already has a References chain — reply should
        // preserve the chain and append the original Message-ID.
        let original = parse_email(
            b"Message-ID: <child@example.com>\nReferences: <root@example.com> <parent@example.com>\nFrom: a@example.com\nTo: b@example.com\n\nbody",
        );
        let refs = build_reply_references(&original);
        assert_eq!(
            refs,
            vec![
                "root@example.com",
                "parent@example.com",
                "child@example.com"
            ]
        );
    }

    #[test]
    fn reply_references_deduplicate_message_id() {
        // If the original Message-ID is already in its References chain,
        // it should not be duplicated.
        let original = parse_email(
            b"Message-ID: <dup@example.com>\nReferences: <root@example.com> <dup@example.com>\nFrom: a@example.com\nTo: b@example.com\n\nbody",
        );
        let refs = build_reply_references(&original);
        assert_eq!(refs, vec!["root@example.com", "dup@example.com"]);
    }

    #[test]
    fn reply_context_captures_message_id_and_references() {
        let original = parse_email(
            b"Message-ID: <target@example.com>\nReferences: <root@example.com>\nFrom: bob@example.com\nTo: alice@example.com\n\nreply me",
        );
        let ctx = EmailReplyContext {
            recipient: original.from[0].address.clone(),
            subject: original.subject.clone(),
            thread_id: original.thread_id(),
            message_id: Some(original.source_id.clone()),
            references: build_reply_references(&original),
        };
        assert_eq!(ctx.message_id.as_deref(), Some("target@example.com"));
        assert_eq!(
            ctx.references,
            vec!["root@example.com", "target@example.com"]
        );
    }

    #[test]
    fn format_msgid_wraps_and_normalizes() {
        assert_eq!(format_msgid("orig@example.com"), "<orig@example.com>");
        assert_eq!(
            format_msgid("<already@example.com>"),
            "<already@example.com>"
        );
    }

    #[test]
    fn smtp_emission_produces_valid_threading_headers() {
        // Verify that the exact builder calls used by send_email produce
        // RFC 5322-compliant headers in the serialized SMTP message.
        let refs = [
            "root@example.com".to_string(),
            "parent@example.com".to_string(),
            "child@example.com".to_string(),
        ];
        let in_reply_to = "child@example.com";

        let mut builder = SmtpMessage::builder()
            .from("sender@example.com".parse().unwrap())
            .to("recipient@example.com".parse().unwrap())
            .subject("Re: test");
        builder = builder.in_reply_to(format_msgid(in_reply_to));
        let joined = refs
            .iter()
            .map(|r| format_msgid(r))
            .collect::<Vec<_>>()
            .join(" ");
        builder = builder.references(joined);
        let email = builder.body("reply body".to_string()).unwrap();

        let buf = email.formatted();
        let text = String::from_utf8_lossy(&buf);
        let header_line = |prefix: &str| -> String {
            text.lines()
                .find(|line| line.starts_with(prefix))
                .map(std::string::ToString::to_string)
                .unwrap_or_default()
        };

        assert_eq!(
            header_line("In-Reply-To:"),
            "In-Reply-To: <child@example.com>"
        );
        assert_eq!(
            header_line("References:"),
            "References: <root@example.com> <parent@example.com> <child@example.com>"
        );
    }

    #[test]
    fn select_uids_truncates_to_limit_keeping_most_recent() {
        let uids = vec![5, 2, 8, 1, 3];
        let opts = FetchOptions {
            limit: Some(3),
            ..Default::default()
        };
        let selected = select_uids(uids, &opts, 500);
        assert_eq!(selected, vec![3, 5, 8]);
    }

    #[test]
    fn select_uids_caps_at_max_messages_when_no_limit() {
        let uids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let opts = FetchOptions::default();
        let selected = select_uids(uids, &opts, 5);
        assert_eq!(selected, vec![6, 7, 8, 9, 10]);
    }

    #[test]
    fn select_uids_limit_does_not_exceed_max_messages() {
        let uids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let opts = FetchOptions {
            limit: Some(100),
            ..Default::default()
        };
        let selected = select_uids(uids, &opts, 5);
        assert_eq!(selected, vec![6, 7, 8, 9, 10]);
    }

    #[test]
    fn select_uids_since_uid_filters_for_incremental_sync() {
        let uids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let opts = FetchOptions {
            since_uid: Some(4),
            ..Default::default()
        };
        let selected = select_uids(uids, &opts, 500);
        assert_eq!(selected, vec![5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn select_uids_since_uid_with_limit() {
        let uids = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let opts = FetchOptions {
            since_uid: Some(4),
            limit: Some(2),
        };
        let selected = select_uids(uids, &opts, 500);
        assert_eq!(selected, vec![9, 10]);
    }

    #[test]
    fn select_uids_deduplicates() {
        let uids = vec![1, 1, 2, 2, 3];
        let opts = FetchOptions::default();
        let selected = select_uids(uids, &opts, 500);
        assert_eq!(selected, vec![1, 2, 3]);
    }

    #[test]
    fn select_uids_empty_input() {
        let opts = FetchOptions {
            limit: Some(10),
            ..Default::default()
        };
        let selected = select_uids(Vec::new(), &opts, 500);
        assert!(selected.is_empty());
    }

    #[test]
    fn email_config_defaults() {
        let config = EmailProviderConfig::default();
        assert_eq!(config.imap_port, 993);
        assert_eq!(config.smtp_port, 587);
        assert_eq!(config.mailbox, "INBOX");
        assert_eq!(config.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(config.max_messages, DEFAULT_MAX_MESSAGES);
    }

    #[test]
    fn from_credentials_reads_pagination_settings() {
        let mut creds = BTreeMap::new();
        creds.insert("imap_host".into(), "imap.example.com".into());
        creds.insert("smtp_host".into(), "smtp.example.com".into());
        creds.insert("username".into(), "alice@example.com".into());
        creds.insert("password".into(), "secret".into());
        creds.insert("from".into(), "alice@example.com".into());
        creds.insert("page_size".into(), "25".into());
        creds.insert("max_messages".into(), "100".into());

        let provider = EmailProvider::from_credentials(&creds).expect("valid credentials");
        assert_eq!(provider.page_size, 25);
        assert_eq!(provider.max_messages, 100);
    }

    #[test]
    fn from_credentials_uses_defaults_when_pagination_unset() {
        let mut creds = BTreeMap::new();
        creds.insert("imap_host".into(), "imap.example.com".into());
        creds.insert("smtp_host".into(), "smtp.example.com".into());
        creds.insert("username".into(), "alice@example.com".into());
        creds.insert("password".into(), "secret".into());
        creds.insert("from".into(), "alice@example.com".into());

        let provider = EmailProvider::from_credentials(&creds).expect("valid credentials");
        assert_eq!(provider.page_size, DEFAULT_PAGE_SIZE);
        assert_eq!(provider.max_messages, DEFAULT_MAX_MESSAGES);
    }

    #[test]
    fn from_credentials_rejects_invalid_page_size() {
        let mut creds = BTreeMap::new();
        creds.insert("imap_host".into(), "imap.example.com".into());
        creds.insert("smtp_host".into(), "smtp.example.com".into());
        creds.insert("username".into(), "alice@example.com".into());
        creds.insert("password".into(), "secret".into());
        creds.insert("from".into(), "alice@example.com".into());
        creds.insert("page_size".into(), "not-a-number".into());

        let error = EmailProvider::from_credentials(&creds).expect_err("invalid page_size");
        assert!(error.to_string().contains("page_size"));
    }

    #[test]
    fn from_credentials_rejects_zero_page_size() {
        let mut creds = BTreeMap::new();
        creds.insert("imap_host".into(), "imap.example.com".into());
        creds.insert("smtp_host".into(), "smtp.example.com".into());
        creds.insert("username".into(), "alice@example.com".into());
        creds.insert("password".into(), "secret".into());
        creds.insert("from".into(), "alice@example.com".into());
        creds.insert("page_size".into(), "0".into());

        let error = EmailProvider::from_credentials(&creds).expect_err("zero page_size");
        assert!(error.to_string().contains("page_size must be at least 1"));
    }

    #[test]
    fn from_credentials_rejects_zero_max_messages() {
        let mut creds = BTreeMap::new();
        creds.insert("imap_host".into(), "imap.example.com".into());
        creds.insert("smtp_host".into(), "smtp.example.com".into());
        creds.insert("username".into(), "alice@example.com".into());
        creds.insert("password".into(), "secret".into());
        creds.insert("from".into(), "alice@example.com".into());
        creds.insert("max_messages".into(), "0".into());

        let error = EmailProvider::from_credentials(&creds).expect_err("zero max_messages");
        assert!(
            error
                .to_string()
                .contains("max_messages must be at least 1")
        );
    }
}

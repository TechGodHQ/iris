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
        })
    }

    async fn fetch_messages(&self, limit: Option<u32>) -> Result<Vec<EmailEnvelope>> {
        let config = self.clone();
        tokio::task::spawn_blocking(move || config.fetch_messages_blocking(limit))
            .await
            .map_err(|error| IrisError::Transport(error.to_string()))?
    }

    fn fetch_messages_blocking(&self, limit: Option<u32>) -> Result<Vec<EmailEnvelope>> {
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
        session
            .select(&self.mailbox)
            .map_err(|error| IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: error.to_string(),
            })?;

        let uids = session
            .uid_search("ALL")
            .map_err(|error| IrisError::Provider {
                provider: PROVIDER_ID.into(),
                message: error.to_string(),
            })?;
        let mut uids: Vec<_> = uids.into_iter().collect();
        uids.sort_unstable();
        let limit = limit.unwrap_or(50) as usize;
        if uids.len() > limit {
            uids = uids.split_off(uids.len() - limit);
        }
        if uids.is_empty() {
            let _ = session.logout();
            return Ok(Vec::new());
        }

        let sequence = uids
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
        let messages = fetches
            .iter()
            .filter_map(|fetch| fetch.body())
            .map(parse_email)
            .collect::<Vec<_>>();
        let _ = session.logout();
        Ok(messages)
    }

    async fn send_email(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
        thread_id: Uuid,
    ) -> Result<Message> {
        let from: Mailbox = self
            .from
            .parse()
            .map_err(|error| IrisError::Config(format!("invalid email from address: {error}")))?;
        let to: Mailbox = recipient
            .parse()
            .map_err(|error| IrisError::Config(format!("invalid email recipient: {error}")))?;
        let email = SmtpMessage::builder()
            .from(from)
            .to(to)
            .subject(subject)
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
        Ok(())
    }
}

#[async_trait]
impl MessageProvider for EmailProvider {
    fn metadata(&self) -> &ProviderMetadata {
        &METADATA
    }

    async fn list_threads(&self, limit: Option<u32>) -> Result<Vec<Thread>> {
        let mut by_thread = BTreeMap::<String, Thread>::new();
        for email in self.fetch_messages(limit).await? {
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
        let mut messages: Vec<_> = self
            .fetch_messages(None)
            .await?
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
        let mut contacts = Vec::new();
        for email in self.fetch_messages(limit).await? {
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
        let mut messages: Vec<_> = self
            .fetch_messages(None)
            .await?
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
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmailReplyContext {
    recipient: String,
    subject: Option<String>,
    thread_id: Uuid,
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

fn uuid_for(input: &[u8]) -> Uuid {
    Uuid::new_v5(&UUID_NAMESPACE, input)
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
}

//! Provider configuration loading and registry construction.
//!
//! Iris reads TOML configuration and overlays environment configuration to
//! decide which built-in providers to enable. Secrets may be provided inline
//! for local development or by naming an environment variable. Env-backed
//! secrets keep credentials out of config files.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iris_core::{
    AttachmentStore, AuditEntry, AuditEvent, AuditFilter, AuditLog, Contact, Message,
    MessageProvider, MessageStream, OutboundMessage, RealtimeStatus, RecordOutcome,
    Result as IrisResult, Thread,
};
use serde::Deserialize;

use crate::email::EmailProvider;
use crate::mock::MockProvider;
use crate::sms::SmsProvider;
use crate::telegram::TelegramProvider;

/// Environment variable used to point Iris at a config file.
pub const CONFIG_PATH_ENV: &str = "IRIS_CONFIG";

/// Comma-separated provider ids enabled by environment configuration.
pub const ENABLED_PROVIDERS_ENV: &str = "IRIS_ENABLED_PROVIDERS";

/// Top-level Iris configuration.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct IrisConfig {
    /// Provider declarations keyed by provider id.
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    /// Source-authorized normalized batch ingestion configuration.
    #[serde(default)]
    pub ingest: IngestConfig,
}

/// Configuration for normalized batch ingestion.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct IngestConfig {
    /// Sources permitted to submit normalized batches.
    #[serde(default = "default_ingest_sources")]
    pub sources: Vec<String>,
    /// Per-source bearer secrets.
    #[serde(default)]
    pub secrets: BTreeMap<String, SecretValue>,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            sources: default_ingest_sources(),
            secrets: BTreeMap::new(),
        }
    }
}

fn default_ingest_sources() -> Vec<String> {
    vec!["herdr".to_owned()]
}

/// Configuration for one provider.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ProviderConfig {
    /// Whether the provider should be registered.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Provider credentials and options.
    #[serde(default)]
    pub credentials: BTreeMap<String, SecretValue>,
    /// Named instances of this provider type. The enclosing declaration is the
    /// backwards-compatible default instance (for example `[providers.email]`).
    #[serde(default)]
    pub instances: BTreeMap<String, Self>,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            credentials: BTreeMap::new(),
            instances: BTreeMap::new(),
        }
    }
}

/// Secret or option value loaded from config.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum SecretValue {
    /// Inline literal value.
    Inline(String),
    /// Value loaded from an environment variable.
    FromEnv { env: String },
}

/// A provider entry with credentials resolved from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProviderConfig {
    /// Stable configured instance id (`email` or `email.ops-codefold`).
    pub id: String,
    /// Static provider type used for construction (`email`, never inferred from `id`).
    pub provider_type: String,
    /// Resolved credentials and options.
    pub credentials: BTreeMap<String, String>,
}

const fn default_enabled() -> bool {
    true
}

/// Load config from the default Iris config path.
///
/// If `IRIS_CONFIG` is set, that path is required. Otherwise Iris tries
/// `./iris.toml` and then `$HOME/.config/iris/config.toml`; if neither exists,
/// an empty config is returned.
pub fn load_default_config() -> anyhow::Result<IrisConfig> {
    let mut config = if let Ok(path) = env::var(CONFIG_PATH_ENV) {
        let path = PathBuf::from(path);
        if path.exists() {
            IrisConfig::from_path(path)?
        } else {
            IrisConfig::default()
        }
    } else {
        default_config_paths()
            .into_iter()
            .find(|path| path.exists())
            .map(IrisConfig::from_path)
            .transpose()?
            .unwrap_or_default()
    };
    config.apply_env_overrides()?;
    Ok(config)
}

/// Default config file candidates when `IRIS_CONFIG` is not set.
pub fn default_config_paths() -> Vec<PathBuf> {
    let mut paths = vec![PathBuf::from("iris.toml")];
    if let Some(home) = env::var_os("HOME") {
        paths.push(PathBuf::from(home).join(".config/iris/config.toml"));
    }
    paths
}

impl IrisConfig {
    /// Parse Iris config from TOML text.
    pub fn from_toml(input: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(input)?;
        config.validate_provider_identifiers()?;
        Ok(config)
    }

    /// Load Iris config from a TOML file.
    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let input = fs::read_to_string(path)?;
        Self::from_toml(&input)
    }

    /// Apply native Iris environment configuration over this file/default config.
    pub fn apply_env_overrides(&mut self) -> anyhow::Result<()> {
        if let Ok(providers) = env::var(ENABLED_PROVIDERS_ENV) {
            let ids: Vec<_> = providers
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .collect();
            if ids.is_empty() {
                anyhow::bail!("{ENABLED_PROVIDERS_ENV} must contain at least one provider id");
            }
            for provider in self.providers.values_mut() {
                provider.enabled = false;
                for instance in provider.instances.values_mut() {
                    instance.enabled = false;
                }
            }
            for id in ids {
                let (provider_type, instance) = id.split_once('.').unwrap_or((id, ""));
                if provider_type.is_empty() || !valid_identifier(provider_type) {
                    anyhow::bail!("invalid provider id `{id}` in {ENABLED_PROVIDERS_ENV}");
                }
                let provider = self
                    .providers
                    .entry(provider_type.to_owned())
                    .or_insert_with(|| ProviderConfig {
                        enabled: false,
                        ..ProviderConfig::default()
                    });
                if instance.is_empty() {
                    provider.enabled = true;
                } else {
                    if !valid_identifier(instance) {
                        anyhow::bail!(
                            "invalid provider instance `{id}` in {ENABLED_PROVIDERS_ENV}"
                        );
                    }
                    provider
                        .instances
                        .entry(instance.to_owned())
                        .or_default()
                        .enabled = true;
                }
            }
        }
        self.validate_provider_identifiers()?;
        for (provider_type, config) in &mut self.providers {
            apply_provider_env(config, provider_type, None);
            for (instance, instance_config) in &mut config.instances {
                apply_provider_env(instance_config, provider_type, Some(instance));
            }
        }
        Ok(())
    }

    fn validate_provider_identifiers(&self) -> anyhow::Result<()> {
        for (provider_type, config) in &self.providers {
            if provider_type.contains('.') || !valid_identifier(provider_type) {
                anyhow::bail!(
                    "invalid provider type `{provider_type}`; use a lowercase identifier without dots"
                );
            }
            for instance in config.instances.keys() {
                if !valid_identifier(instance) {
                    anyhow::bail!(
                        "invalid provider instance `{provider_type}.{instance}`; use lowercase letters, digits, and hyphens"
                    );
                }
            }
        }
        Ok(())
    }

    /// Resolve enabled provider entries and their env-backed secrets.
    pub fn resolved_enabled_providers(&self) -> anyhow::Result<Vec<ResolvedProviderConfig>> {
        self.validate_provider_identifiers()?;
        let mut resolved = Vec::new();
        for (provider_type, provider) in &self.providers {
            // A TOML parent table is synthesized when only named children are
            // declared. Do not turn that structural parent into a credentialless
            // default provider; an explicit/default-only declaration has no
            // instances and remains enabled as before.
            if provider.enabled
                && (provider.instances.is_empty() || !provider.credentials.is_empty())
            {
                resolved.push(resolve_provider(provider_type, provider_type, provider)?);
            }
            for (instance, instance_config) in &provider.instances {
                if instance_config.enabled {
                    resolved.push(resolve_provider(
                        &format!("{provider_type}.{instance}"),
                        provider_type,
                        instance_config,
                    )?);
                }
            }
        }
        Ok(resolved)
    }

    /// Resolve configured batch-ingest secrets. Missing environment-backed
    /// values remain absent so transports can report an unavailable service.
    pub fn resolved_ingest_secrets(&self) -> BTreeMap<String, String> {
        self.ingest
            .secrets
            .iter()
            .filter_map(|(source, secret)| {
                secret.resolve().ok().map(|value| (source.clone(), value))
            })
            .filter(|(_, secret)| !secret.trim().is_empty())
            .collect()
    }
}

const fn provider_env_fields() -> &'static [(&'static str, &'static [(&'static str, &'static str)])]
{
    &[
        ("mock", &[("mode", "IRIS_MOCK_MODE")]),
        ("telegram", &[("bot_token", "IRIS_TELEGRAM_BOT_TOKEN")]),
        (
            "email",
            &[
                ("imap_host", "IRIS_EMAIL_IMAP_HOST"),
                ("imap_port", "IRIS_EMAIL_IMAP_PORT"),
                ("smtp_host", "IRIS_EMAIL_SMTP_HOST"),
                ("smtp_port", "IRIS_EMAIL_SMTP_PORT"),
                ("username", "IRIS_EMAIL_USERNAME"),
                ("password", "IRIS_EMAIL_PASSWORD"),
                ("mailbox", "IRIS_EMAIL_MAILBOX"),
                ("from", "IRIS_EMAIL_FROM"),
                ("page_size", "IRIS_EMAIL_PAGE_SIZE"),
                ("max_messages", "IRIS_EMAIL_MAX_MESSAGES"),
            ],
        ),
        (
            "sms",
            &[
                ("ssh_host", "IRIS_SMS_SSH_HOST"),
                ("ssh_command", "IRIS_SMS_SSH_COMMAND"),
                ("self_number", "IRIS_SMS_SELF_NUMBER"),
            ],
        ),
    ]
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn resolve_provider(
    id: &str,
    provider_type: &str,
    provider: &ProviderConfig,
) -> anyhow::Result<ResolvedProviderConfig> {
    let credentials = provider
        .credentials
        .iter()
        .map(|(key, value)| value.resolve().map(|resolved| (key.clone(), resolved)))
        .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
    Ok(ResolvedProviderConfig {
        id: id.to_owned(),
        provider_type: provider_type.to_owned(),
        credentials,
    })
}

const LEGACY_TELEGRAM_BOT_TOKEN_ENV: &str = "TELEGRAM_BOT_TOKEN";
const CANONICAL_TELEGRAM_BOT_TOKEN_ENV: &str = "IRIS_TELEGRAM_BOT_TOKEN";

fn apply_provider_env(config: &mut ProviderConfig, provider_type: &str, instance: Option<&str>) {
    let fields: &[(&str, &str)] = provider_env_fields()
        .iter()
        .find(|(name, _)| *name == provider_type)
        .map_or(&[], |(_, fields)| *fields);
    for (field, default_variable) in fields {
        let variable = instance.map_or_else(
            || (*default_variable).to_owned(),
            |instance| {
                format!(
                    "IRIS_{}__{}__{}",
                    provider_type.to_ascii_uppercase(),
                    instance.replace('-', "_").to_ascii_uppercase(),
                    field.to_ascii_uppercase(),
                )
            },
        );
        if env::var_os(&variable).is_some() {
            config
                .credentials
                .insert((*field).to_owned(), SecretValue::FromEnv { env: variable });
        }
    }

    // v0.1.0 documented this unprefixed variable. Retain it only for the
    // default Telegram instance so existing deployments can upgrade in place.
    if provider_type == "telegram"
        && instance.is_none()
        && env::var_os(CANONICAL_TELEGRAM_BOT_TOKEN_ENV).is_none()
        && env::var_os(LEGACY_TELEGRAM_BOT_TOKEN_ENV).is_some()
    {
        tracing::warn!(
            legacy_env = LEGACY_TELEGRAM_BOT_TOKEN_ENV,
            canonical_env = CANONICAL_TELEGRAM_BOT_TOKEN_ENV,
            "deprecated Telegram credential environment variable selected; the canonical variable takes precedence when both are set"
        );
        config.credentials.insert(
            "bot_token".to_owned(),
            SecretValue::FromEnv {
                env: LEGACY_TELEGRAM_BOT_TOKEN_ENV.to_owned(),
            },
        );
    }
}

impl SecretValue {
    /// Resolve a secret value, reading from the environment when requested.
    pub fn resolve(&self) -> anyhow::Result<String> {
        match self {
            Self::Inline(value) => Ok(value.clone()),
            Self::FromEnv { env: name } => env::var(name).map_err(|error| {
                anyhow::anyhow!("failed to read environment variable {name}: {error}")
            }),
        }
    }
}

/// Build providers from the default config path.
pub fn providers_from_default_config(
    attachments: &Arc<dyn AttachmentStore>,
    audit: &Arc<dyn AuditLog>,
) -> anyhow::Result<Vec<Arc<dyn MessageProvider>>> {
    let has_config_file = env::var(CONFIG_PATH_ENV)
        .ok()
        .map(PathBuf::from)
        .is_some_and(|path| path.exists())
        || default_config_paths().into_iter().any(|path| path.exists());
    let config = load_default_config()?;
    if config.providers.is_empty() && !has_config_file {
        Ok(vec![Arc::new(MockProvider::with_audit(audit.clone()))])
    } else {
        providers_from_config(&config, attachments, audit)
    }
}

/// Build providers from a loaded config.
///
/// Only enabled known provider declarations are registered. An empty explicit
/// config returns an empty provider list; development fallback providers are
/// added only by [`providers_from_default_config`] when no config file exists.
pub fn providers_from_config(
    config: &IrisConfig,
    attachments: &Arc<dyn AttachmentStore>,
    audit: &Arc<dyn AuditLog>,
) -> anyhow::Result<Vec<Arc<dyn MessageProvider>>> {
    config
        .resolved_enabled_providers()?
        .into_iter()
        .map(|provider| build_provider(&provider, attachments, audit))
        .collect()
}

fn build_provider(
    provider: &ResolvedProviderConfig,
    attachments: &Arc<dyn AttachmentStore>,
    audit: &Arc<dyn AuditLog>,
) -> anyhow::Result<Arc<dyn MessageProvider>> {
    let audit: Arc<dyn AuditLog> = Arc::new(InstanceAuditLog {
        provider_id: provider.id.clone(),
        inner: audit.clone(),
    });
    let built: Arc<dyn MessageProvider> = match provider.provider_type.as_str() {
        "mock" => Arc::new(MockProvider::with_audit(audit.clone())),
        "telegram" => Arc::new(
            TelegramProvider::from_credentials(&provider.credentials, attachments.clone())?
                .with_audit(audit.clone()),
        ),
        "email" => Arc::new(
            EmailProvider::from_credentials(&provider.credentials, attachments.clone())?
                .with_audit(audit.clone()),
        ),
        "sms" => Arc::new(
            SmsProvider::from_credentials(&provider.credentials)?.with_audit(audit.clone()),
        ),
        other => anyhow::bail!("provider is configured but not available in this build: {other}"),
    };
    Ok(Arc::new(InstanceProvider {
        id: provider.id.clone(),
        inner: built,
    }))
}

/// Scopes provider-originated audit entries and idempotency keys to a configured
/// instance, so same-type providers do not share provenance.
#[derive(Debug)]
struct InstanceAuditLog {
    provider_id: String,
    inner: Arc<dyn AuditLog>,
}

#[async_trait]
impl AuditLog for InstanceAuditLog {
    async fn record(&self, mut event: AuditEvent) -> IrisResult<AuditEntry> {
        event.provider.clone_from(&self.provider_id);
        self.inner.record(event).await
    }

    async fn query(&self, filter: &AuditFilter) -> IrisResult<Vec<AuditEntry>> {
        self.inner.query(filter).await
    }

    async fn verify_chain(&self) -> IrisResult<bool> {
        self.inner.verify_chain().await
    }

    async fn record_once(
        &self,
        _provider: &str,
        source_id: &str,
        mut event: AuditEvent,
    ) -> IrisResult<RecordOutcome> {
        event.provider.clone_from(&self.provider_id);
        self.inner
            .record_once(&self.provider_id, source_id, event)
            .await
    }
}

/// Makes a concrete provider addressable by its configured instance id while
/// retaining its provider-type metadata and implementation.
struct InstanceProvider {
    id: String,
    inner: Arc<dyn MessageProvider>,
}

#[async_trait]
impl MessageProvider for InstanceProvider {
    fn metadata(&self) -> &iris_core::ProviderMetadata {
        self.inner.metadata()
    }

    fn id(&self) -> &str {
        &self.id
    }

    async fn list_threads(&self, limit: Option<u32>) -> IrisResult<Vec<Thread>> {
        let mut threads = self.inner.list_threads(limit).await?;
        for thread in &mut threads {
            thread.provider_instance = Some(self.id.clone());
            for contact in &mut thread.participants {
                contact.provider_instance = Some(self.id.clone());
            }
        }
        Ok(threads)
    }

    async fn list_messages(
        &self,
        thread_id: &str,
        before: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> IrisResult<Vec<Message>> {
        self.inner.list_messages(thread_id, before, limit).await
    }

    async fn list_contacts(&self, limit: Option<u32>) -> IrisResult<Vec<Contact>> {
        let mut contacts = self.inner.list_contacts(limit).await?;
        for contact in &mut contacts {
            contact.provider_instance = Some(self.id.clone());
        }
        Ok(contacts)
    }

    async fn send_message(
        &self,
        thread_id: &str,
        message: &OutboundMessage,
    ) -> IrisResult<Message> {
        self.inner.send_message(thread_id, message).await
    }

    async fn subscribe_realtime(&self) -> IrisResult<MessageStream> {
        self.inner.subscribe_realtime().await
    }

    fn realtime_status(&self) -> RealtimeStatus {
        self.inner.realtime_status()
    }

    async fn shutdown_realtime(&self) -> IrisResult<()> {
        self.inner.shutdown_realtime().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use async_trait::async_trait;
    use iris_core::{AttachmentContent, AttachmentRef, IrisError};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    /// No-op attachment store for config tests — these tests only verify
    /// provider construction, not attachment persistence.
    #[derive(Debug)]
    struct NullStore;

    #[async_trait]
    impl AttachmentStore for NullStore {
        async fn store(&self, _content: AttachmentContent) -> iris_core::Result<AttachmentRef> {
            unreachable!("config tests never store attachments")
        }
        async fn get(&self, _id: &uuid::Uuid) -> iris_core::Result<AttachmentContent> {
            Err(IrisError::NotFound("null store".into()))
        }
        async fn delete(&self, _id: &uuid::Uuid) -> iris_core::Result<()> {
            Ok(())
        }
    }

    fn test_store() -> Arc<dyn AttachmentStore> {
        Arc::new(NullStore)
    }

    #[derive(Debug)]
    struct NullAudit;

    #[async_trait]
    impl AuditLog for NullAudit {
        async fn record(
            &self,
            event: iris_core::AuditEvent,
        ) -> iris_core::Result<iris_core::AuditEntry> {
            Ok(iris_core::AuditEntry {
                id: uuid::Uuid::nil(),
                event,
                prev_hash: None,
                self_hash: "test".into(),
            })
        }
        async fn record_once(
            &self,
            _provider: &str,
            _source_id: &str,
            _event: iris_core::AuditEvent,
        ) -> iris_core::Result<iris_core::RecordOutcome> {
            unreachable!("config tests never record audit events")
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
    }

    fn test_audit() -> Arc<dyn AuditLog> {
        Arc::new(NullAudit)
    }

    #[test]
    fn parses_enabled_provider_with_env_secret() {
        let config = IrisConfig::from_toml(
            r#"
[providers.mock]
enabled = true

[providers.mock.credentials]
token = { env = "IRIS_TEST_TOKEN" }
mode = "development"
"#,
        )
        .expect("valid config");

        assert!(config.providers["mock"].enabled);
        assert_eq!(
            config.providers["mock"].credentials["token"],
            SecretValue::FromEnv {
                env: "IRIS_TEST_TOKEN".into()
            }
        );
    }

    #[test]
    fn disabled_providers_are_not_registered() {
        let config = IrisConfig::from_toml(
            r"
[providers.mock]
enabled = false
",
        )
        .expect("valid config");

        let providers =
            providers_from_config(&config, &test_store(), &test_audit()).expect("registry builds");
        assert!(providers.is_empty());
    }

    #[test]
    fn empty_explicit_config_registers_no_providers() {
        let providers = providers_from_config(&IrisConfig::default(), &test_store(), &test_audit())
            .expect("registry builds");
        assert!(providers.is_empty());
    }

    #[test]
    fn ingest_configuration_defaults_and_resolves_per_source_secrets() {
        let config = IrisConfig::from_toml(
            r#"
[ingest]
sources = ["alpha", "beta"]

[ingest.secrets]
alpha = "alpha-secret"
beta = "beta-secret"
"#,
        )
        .expect("valid config");
        assert_eq!(config.ingest.sources, ["alpha", "beta"]);
        assert_eq!(config.resolved_ingest_secrets().len(), 2);
        assert_eq!(IrisConfig::default().ingest.sources, ["herdr"]);
    }

    #[test]
    fn env_backed_secret_reports_missing_variable() {
        let error = SecretValue::FromEnv {
            env: "IRIS_TEST_SECRET_DOES_NOT_EXIST".into(),
        }
        .resolve()
        .expect_err("missing env value should fail");

        assert!(
            error
                .to_string()
                .contains("IRIS_TEST_SECRET_DOES_NOT_EXIST")
        );
    }

    #[test]
    fn builds_telegram_provider_from_bot_token() {
        let config = IrisConfig::from_toml(
            r#"
[providers.telegram]
enabled = true

[providers.telegram.credentials]
bot_token = "123:abc"
"#,
        )
        .expect("valid config");

        let providers =
            providers_from_config(&config, &test_store(), &test_audit()).expect("registry builds");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id(), "telegram");
    }

    #[test]
    fn legacy_telegram_token_env_builds_default_provider() {
        let _guard = env_lock().lock().expect("environment lock");
        temp_env::with_vars(
            [
                (ENABLED_PROVIDERS_ENV, Some("telegram")),
                (LEGACY_TELEGRAM_BOT_TOKEN_ENV, Some("legacy-token")),
                (CANONICAL_TELEGRAM_BOT_TOKEN_ENV, None),
            ],
            || {
                let mut config = IrisConfig::default();
                config.apply_env_overrides().expect("env applies");
                let resolved = config
                    .resolved_enabled_providers()
                    .expect("provider resolves");
                assert_eq!(resolved[0].credentials["bot_token"], "legacy-token");
                let providers = providers_from_config(&config, &test_store(), &test_audit())
                    .expect("legacy token constructs Telegram provider");
                assert_eq!(providers[0].id(), "telegram");
            },
        );
    }

    #[test]
    fn canonical_telegram_token_env_builds_default_provider() {
        let _guard = env_lock().lock().expect("environment lock");
        temp_env::with_vars(
            [
                (ENABLED_PROVIDERS_ENV, Some("telegram")),
                (LEGACY_TELEGRAM_BOT_TOKEN_ENV, None),
                (CANONICAL_TELEGRAM_BOT_TOKEN_ENV, Some("canonical-token")),
            ],
            || {
                let mut config = IrisConfig::default();
                config.apply_env_overrides().expect("env applies");
                let resolved = config
                    .resolved_enabled_providers()
                    .expect("provider resolves");
                assert_eq!(resolved[0].credentials["bot_token"], "canonical-token");
            },
        );
    }

    #[test]
    fn canonical_telegram_token_env_wins_over_legacy() {
        let _guard = env_lock().lock().expect("environment lock");
        temp_env::with_vars(
            [
                (ENABLED_PROVIDERS_ENV, Some("telegram")),
                (LEGACY_TELEGRAM_BOT_TOKEN_ENV, Some("legacy-token")),
                (CANONICAL_TELEGRAM_BOT_TOKEN_ENV, Some("canonical-token")),
            ],
            || {
                let mut config = IrisConfig::default();
                config.apply_env_overrides().expect("env applies");
                let resolved = config
                    .resolved_enabled_providers()
                    .expect("provider resolves");
                assert_eq!(resolved[0].credentials["bot_token"], "canonical-token");
            },
        );
    }

    #[test]
    fn legacy_telegram_token_env_does_not_overlay_unrelated_provider() {
        let _guard = env_lock().lock().expect("environment lock");
        temp_env::with_vars(
            [
                (ENABLED_PROVIDERS_ENV, Some("mock")),
                (LEGACY_TELEGRAM_BOT_TOKEN_ENV, Some("legacy-token")),
                (CANONICAL_TELEGRAM_BOT_TOKEN_ENV, None),
            ],
            || {
                let mut config = IrisConfig::default();
                config.apply_env_overrides().expect("env applies");
                let resolved = config
                    .resolved_enabled_providers()
                    .expect("provider resolves");
                assert_eq!(resolved.len(), 1);
                assert_eq!(resolved[0].id, "mock");
                assert!(resolved[0].credentials.is_empty());
            },
        );
    }

    #[test]
    fn builds_email_provider_from_credentials() {
        let config = IrisConfig::from_toml(
            r#"
[providers.email]
enabled = true

[providers.email.credentials]
imap_host = "imap.example.com"
smtp_host = "smtp.example.com"
username = "alice@example.com"
password = "app-password"
from = "alice@example.com"
"#,
        )
        .expect("valid config");

        let providers =
            providers_from_config(&config, &test_store(), &test_audit()).expect("registry builds");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id(), "email");
    }

    #[test]
    fn resolves_default_and_named_email_instances_without_type_inference() {
        let config = IrisConfig::from_toml(
            r#"
[providers.email]
enabled = true
[providers.email.credentials]
username = "default@example.com"

[providers.email.instances.ops-codefold]
enabled = true
[providers.email.instances.ops-codefold.credentials]
username = "ops@example.com"
"#,
        )
        .expect("valid config");

        let resolved = config
            .resolved_enabled_providers()
            .expect("providers resolve");
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].id, "email");
        assert_eq!(resolved[0].provider_type, "email");
        assert_eq!(resolved[1].id, "email.ops-codefold");
        assert_eq!(resolved[1].provider_type, "email");
        assert_eq!(resolved[1].credentials["username"], "ops@example.com");
    }

    #[test]
    fn rejects_invalid_instance_identifiers() {
        let error = IrisConfig::from_toml(
            r#"
[providers.email.instances."ops_codefold"]
enabled = true
"#,
        )
        .expect_err("underscore must be rejected during TOML parsing");
        assert!(error.to_string().contains("invalid provider instance"));
    }

    #[test]
    fn builds_named_instances_with_distinct_runtime_ids() {
        let config = IrisConfig::from_toml(
            r#"
[providers.email.instances.ops-codefold.credentials]
imap_host = "imap.ops.example.com"
smtp_host = "smtp.ops.example.com"
username = "ops@example.com"
password = "app-password"
from = "ops@example.com"

[providers.email.instances.support-codefold.credentials]
imap_host = "imap.support.example.com"
smtp_host = "smtp.support.example.com"
username = "support@example.com"
password = "app-password"
from = "support@example.com"
"#,
        )
        .expect("valid config");

        let providers =
            providers_from_config(&config, &test_store(), &test_audit()).expect("registry builds");
        let ids: Vec<_> = providers.iter().map(|provider| provider.id()).collect();
        assert_eq!(ids, ["email.ops-codefold", "email.support-codefold"]);
        assert!(
            providers
                .iter()
                .all(|provider| provider.metadata().id == "email")
        );
    }

    #[tokio::test]
    async fn named_mock_instances_attribute_threads_and_contacts() {
        let config = IrisConfig::from_toml(
            r"
[providers.mock.instances.alpha]
[providers.mock.instances.beta]
",
        )
        .expect("valid config");
        let providers =
            providers_from_config(&config, &test_store(), &test_audit()).expect("registry builds");
        assert_eq!(providers.len(), 2);
        for provider in providers {
            let instance = provider.id().to_owned();
            let threads = provider.list_threads(Some(1)).await.expect("list threads");
            let contacts = provider
                .list_contacts(Some(1))
                .await
                .expect("list contacts");
            assert_eq!(
                threads[0].provider_instance.as_deref(),
                Some(instance.as_str())
            );
            assert_eq!(
                contacts[0].provider_instance.as_deref(),
                Some(instance.as_str())
            );
        }
    }

    #[test]
    fn named_env_selection_disables_default_and_unselected_instances() {
        let _guard = env_lock().lock().expect("environment lock");
        temp_env::with_vars(
            [
                (ENABLED_PROVIDERS_ENV, Some("email.ops-codefold")),
                (
                    "IRIS_EMAIL__OPS_CODEFOLD__USERNAME",
                    Some("ops@example.com"),
                ),
            ],
            || {
                let mut config = IrisConfig::from_toml(
                    r#"
[providers.email.credentials]
username = "default@example.com"
[providers.email.instances.ops-codefold.credentials]
username = "configured-ops@example.com"
[providers.email.instances.support-codefold.credentials]
username = "support@example.com"
"#,
                )
                .expect("valid config");
                config.apply_env_overrides().expect("env applies");
                let resolved = config
                    .resolved_enabled_providers()
                    .expect("providers resolve");
                assert_eq!(resolved.len(), 1);
                assert_eq!(resolved[0].id, "email.ops-codefold");
                assert_eq!(resolved[0].credentials["username"], "ops@example.com");
            },
        );
    }

    #[test]
    fn named_env_selection_creates_no_default_instance() {
        let _guard = env_lock().lock().expect("environment lock");
        temp_env::with_vars(
            [
                (ENABLED_PROVIDERS_ENV, Some("email.ops-codefold")),
                ("IRIS_EMAIL_USERNAME", Some("legacy-default@example.com")),
                (
                    "IRIS_EMAIL__OPS_CODEFOLD__USERNAME",
                    Some("ops@example.com"),
                ),
            ],
            || {
                let mut config = IrisConfig::default();
                config.apply_env_overrides().expect("env applies");
                let resolved = config
                    .resolved_enabled_providers()
                    .expect("providers resolve");
                assert_eq!(resolved.len(), 1);
                assert_eq!(resolved[0].id, "email.ops-codefold");
                assert_eq!(resolved[0].credentials["username"], "ops@example.com");
            },
        );
    }

    #[test]
    fn rejects_invalid_identifiers_without_env_overlays() {
        let error = IrisConfig::from_toml(
            r#"
[providers.email.instances."ops_codefold"]
enabled = true
"#,
        )
        .expect_err("TOML loading must validate identifiers");
        assert!(error.to_string().contains("invalid provider instance"));
    }

    #[test]
    fn builds_sms_provider_from_ssh_host() {
        let config = IrisConfig::from_toml(
            r#"
[providers.sms]
enabled = true

[providers.sms.credentials]
ssh_host = "termux-phone"
self_number = "+1 575 555 0199"
"#,
        )
        .expect("valid config");

        let providers =
            providers_from_config(&config, &test_store(), &test_audit()).expect("registry builds");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id(), "sms");
    }
}

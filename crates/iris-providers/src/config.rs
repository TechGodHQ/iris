//! Provider configuration loading and registry construction.
//!
//! Iris reads a TOML config file to decide which built-in providers to enable.
//! Secrets may be provided inline for local development or by naming an
//! environment variable. Env-backed secrets keep credentials out of config files.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use iris_core::MessageProvider;
use serde::Deserialize;

use crate::email::EmailProvider;
use crate::mock::MockProvider;
use crate::telegram::TelegramProvider;

/// Environment variable used to point Iris at a config file.
pub const CONFIG_PATH_ENV: &str = "IRIS_CONFIG";

/// Top-level Iris configuration.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct IrisConfig {
    /// Provider declarations keyed by provider id.
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
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
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            credentials: BTreeMap::new(),
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
    /// Provider id.
    pub id: String,
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
    if let Ok(path) = env::var(CONFIG_PATH_ENV) {
        return IrisConfig::from_path(path);
    }

    for path in default_config_paths() {
        if path.exists() {
            return IrisConfig::from_path(path);
        }
    }

    Ok(IrisConfig::default())
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
        Ok(toml::from_str(input)?)
    }

    /// Load Iris config from a TOML file.
    pub fn from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let input = fs::read_to_string(path)?;
        Self::from_toml(&input)
    }

    /// Resolve enabled provider entries and their env-backed secrets.
    pub fn resolved_enabled_providers(&self) -> anyhow::Result<Vec<ResolvedProviderConfig>> {
        self.providers
            .iter()
            .filter(|(_, provider)| provider.enabled)
            .map(|(id, provider)| {
                let credentials = provider
                    .credentials
                    .iter()
                    .map(|(key, value)| value.resolve().map(|resolved| (key.clone(), resolved)))
                    .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
                Ok(ResolvedProviderConfig {
                    id: id.clone(),
                    credentials,
                })
            })
            .collect()
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
pub fn providers_from_default_config() -> anyhow::Result<Vec<Arc<dyn MessageProvider>>> {
    if let Ok(path) = env::var(CONFIG_PATH_ENV) {
        return providers_from_config(&IrisConfig::from_path(path)?);
    }

    for path in default_config_paths() {
        if path.exists() {
            return providers_from_config(&IrisConfig::from_path(path)?);
        }
    }

    Ok(vec![Arc::new(MockProvider::new())])
}

/// Build providers from a loaded config.
///
/// Only enabled known provider declarations are registered. An empty explicit
/// config returns an empty provider list; development fallback providers are
/// added only by [`providers_from_default_config`] when no config file exists.
pub fn providers_from_config(config: &IrisConfig) -> anyhow::Result<Vec<Arc<dyn MessageProvider>>> {
    config
        .resolved_enabled_providers()?
        .into_iter()
        .map(|provider| build_provider(&provider))
        .collect()
}

fn build_provider(provider: &ResolvedProviderConfig) -> anyhow::Result<Arc<dyn MessageProvider>> {
    match provider.id.as_str() {
        "mock" => Ok(Arc::new(MockProvider::new())),
        "telegram" => Ok(Arc::new(TelegramProvider::from_credentials(
            &provider.credentials,
        )?)),
        "email" => Ok(Arc::new(EmailProvider::from_credentials(
            &provider.credentials,
        )?)),
        other => anyhow::bail!("provider is configured but not available in this build: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let providers = providers_from_config(&config).expect("registry builds");
        assert!(providers.is_empty());
    }

    #[test]
    fn empty_explicit_config_registers_no_providers() {
        let providers = providers_from_config(&IrisConfig::default()).expect("registry builds");
        assert!(providers.is_empty());
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

        let providers = providers_from_config(&config).expect("registry builds");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id(), "telegram");
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

        let providers = providers_from_config(&config).expect("registry builds");
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id(), "email");
    }
}

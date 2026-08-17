//! Error types for Iris core.

use thiserror::Error;

/// All Iris errors flow through this type.
#[derive(Debug, Error)]
pub enum IrisError {
    /// A provider returned an error.
    #[error("provider '{provider}' error: {message}")]
    Provider { provider: String, message: String },

    /// A provider was not found or not configured.
    #[error("provider not found: {0}")]
    ProviderNotFound(String),

    /// A capability was requested that the provider doesn't support.
    #[error("provider '{provider}' does not support capability: {capability}")]
    UnsupportedCapability {
        provider: String,
        capability: String,
    },

    /// A realtime subscription cannot be established by this provider right
    /// now (capability advertised but runtime readiness not satisfied).
    #[error("realtime subscription unavailable for provider '{provider}': {code}")]
    RealtimeUnavailable { provider: String, code: String },

    /// A realtime subscriber's bounded queue overflowed; the subscriber is
    /// terminated with this terminal error.
    #[error("realtime subscriber terminated: consumer too slow (queue overflow)")]
    SlowConsumer,

    /// A realtime poller exhausted its transient retry budget.
    #[error("realtime transient retry budget exhausted after {attempts} attempts: {last_error}")]
    RealtimeRetryExhausted { attempts: u32, last_error: String },

    /// A resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Network or transport error.
    #[error("transport error: {0}")]
    Transport(String),

    /// Serialization or deserialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Storage error (file I/O, disk full, etc.).
    #[error("storage error: {0}")]
    Storage(String),
}

/// Convenience Result alias.
pub type Result<T> = std::result::Result<T, IrisError>;

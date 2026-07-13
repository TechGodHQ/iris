//! Iris core domain types and provider traits.
//!
//! This crate defines the unified message model and the [`MessageProvider`]
//! trait that all source connectors must implement. It has zero external
//! I/O dependencies — providers, storage, and transport live in other crates.

pub mod model;
pub mod provider;
pub mod error;

pub use error::{IrisError, Result};
pub use model::{Contact, Message, MessageKind, Thread};
pub use provider::{MessageProvider, ProviderCapability, ProviderMetadata};

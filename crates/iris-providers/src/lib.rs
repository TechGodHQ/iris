//! Iris message providers.
//!
//! Each module implements [`iris_core::MessageProvider`] for a specific
//! messaging source. Providers are independent — adding a new source is
//! just adding a new module here.

pub mod config;
pub mod email;
pub mod mock;
pub mod telegram;

//! Iris HTTP server — exposes the unified messaging API over REST.
//!
//! This crate is a thin transport layer. All business logic lives in
//! iris-core and iris-providers. The server's routes are auto-generated
//! from the core API definition by iris-codegen.

pub mod app;
pub mod routes;
pub mod sse;

pub use app::{create_app, create_app_with_sse};
pub use sse::SseSettings;

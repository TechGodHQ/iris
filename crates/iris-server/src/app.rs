//! Application builder — wires providers into the Axum router.

use std::sync::Arc;

use axum::Router;
use iris_core::MessageProvider;

use crate::routes;

/// Shared application state — holds all registered providers.
#[derive(Clone)]
pub struct AppState {
    /// All registered providers, keyed by their ID.
    pub providers: Vec<Arc<dyn MessageProvider>>,
}

/// Creates the Axum application with all routes wired.
pub fn create_app(providers: Vec<Arc<dyn MessageProvider>>) -> Router {
    let state = AppState { providers };
    routes::router(state)
}

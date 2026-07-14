//! HTTP route definitions.

mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../generated/http.rs"
    ));
}

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::get,
};
use iris_core::{Contact, Message, Thread};
use serde::{Deserialize, Serialize};

use crate::app::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    pub limit: Option<u32>,
    pub before: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessageRequest {
    pub body: String,
    pub provider: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

pub fn router(state: AppState) -> Router {
    debug_assert!(!generated::GENERATED_ROUTES.is_empty());
    Router::new()
        .route("/health", get(health))
        .route("/providers", get(list_providers))
        .merge(generated::generated_router())
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn list_providers(State(state): State<AppState>) -> Json<Vec<serde_json::Value>> {
    let providers: Vec<_> = state
        .providers
        .iter()
        .map(|p| {
            let m = p.metadata();
            serde_json::json!({
                "id": m.id,
                "name": m.name,
            })
        })
        .collect();
    Json(providers)
}

pub(crate) async fn execute_generated_operation(
    state: &AppState,
    operation_name: &str,
    input: generated::GeneratedOperationInput,
) -> Response {
    match operation_name {
        "list_threads" => match list_threads(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "list_contacts" => match list_contacts(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "list_messages" => match list_messages(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        "send_message" => match send_message(state, input).await {
            Ok(response) => response.into_response(),
            Err(response) => response.into_response(),
        },
        other => (
            StatusCode::NOT_IMPLEMENTED,
            Json(ErrorResponse {
                error: format!(
                    "generated operation is not implemented by the HTTP runtime: {other}"
                ),
            }),
        )
            .into_response(),
    }
}

async fn list_threads(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<Thread>>, (StatusCode, Json<ErrorResponse>)> {
    let q = parse_list_query(&input)?;
    let mut all_threads = Vec::new();
    for provider in &state.providers {
        if let Ok(threads) = provider.list_threads(q.limit).await {
            all_threads.extend(threads);
        }
    }
    Ok(Json(all_threads))
}

async fn list_contacts(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<Contact>>, (StatusCode, Json<ErrorResponse>)> {
    let q = parse_list_query(&input)?;
    let mut all_contacts = Vec::new();
    for provider in &state.providers {
        if let Ok(contacts) = provider.list_contacts(q.limit).await {
            all_contacts.extend(contacts);
        }
    }
    Ok(Json(all_contacts))
}

async fn list_messages(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Vec<Message>>, (StatusCode, Json<ErrorResponse>)> {
    let thread_id = required_path(&input, "thread_id")?;
    let q = parse_list_query(&input)?;
    let mut all_messages = Vec::new();
    for provider in &state.providers {
        if let Ok(messages) = provider.list_messages(&thread_id, q.before, q.limit).await {
            all_messages.extend(messages);
        }
    }
    Ok(Json(all_messages))
}

async fn send_message(
    state: &AppState,
    input: generated::GeneratedOperationInput,
) -> Result<Json<Option<Message>>, (StatusCode, Json<ErrorResponse>)> {
    let thread_id = required_path(&input, "thread_id")?;
    let request: SendMessageRequest = serde_json::from_value(input.body).map_err(bad_request)?;
    let providers: Vec<_> = request.provider.as_deref().map_or_else(
        || state.providers.iter().collect(),
        |provider_id| {
            state
                .providers
                .iter()
                .filter(|provider| provider.id() == provider_id)
                .collect()
        },
    );

    for provider in providers {
        if let Ok(message) = provider.send_message(&thread_id, &request.body).await {
            return Ok(Json(Some(message)));
        }
    }
    Ok(Json(None))
}

fn parse_list_query(
    input: &generated::GeneratedOperationInput,
) -> Result<ListQuery, (StatusCode, Json<ErrorResponse>)> {
    let limit = input
        .query
        .get("limit")
        .map(|value| value.parse::<u32>().map_err(bad_request))
        .transpose()?;
    let before = input
        .query
        .get("before")
        .map(|value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
                .map_err(bad_request)
        })
        .transpose()?;
    Ok(ListQuery { limit, before })
}

fn required_path(
    input: &generated::GeneratedOperationInput,
    name: &str,
) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
    input.path.get(name).cloned().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("missing path parameter: {name}"),
            }),
        )
    })
}

fn bad_request(error: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::generated::GENERATED_ROUTES;

    #[test]
    fn generated_routes_include_send_message() {
        assert!(
            GENERATED_ROUTES
                .iter()
                .any(|route| route.name == "send_message"
                    && route.method == "POST"
                    && route.path == "/messages/{thread_id}")
        );
    }

    #[test]
    fn generated_router_constructs_without_path_syntax_panic() {
        let app_state = crate::app::AppState {
            providers: Vec::new(),
        };
        let _router = super::router(app_state);
    }
}

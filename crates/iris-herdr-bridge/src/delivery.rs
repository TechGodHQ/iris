use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use reqwest::{Client, StatusCode};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::spool::{BatchRecord, Spool};

const POLL_INTERVAL: Duration = Duration::from_millis(250);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Seal due event batches and enqueue an idle heartbeat independently of HTTP.
pub async fn run_sealer(spool: Arc<Mutex<Spool>>) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let now = Utc::now();
        let mut spool = spool.lock().await;
        if spool.maybe_append_heartbeat(now)? {
            info!("idle bridge heartbeat durably queued");
        }
        spool.flush_due(now)?;
    }
}

/// Retry the oldest durable batch until it is accepted by Iris.
pub async fn run_delivery(
    client: Client,
    ingest_url: reqwest::Url,
    token: String,
    spool: Arc<Mutex<Spool>>,
) -> anyhow::Result<()> {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let batch = spool.lock().await.oldest_batch()?;

        let Some(batch) = batch else {
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        };

        match submit_batch(&client, &ingest_url, &token, &batch).await {
            Ok(status) if status.is_success() => {
                spool
                    .lock()
                    .await
                    .remove_batch(batch.sequence, batch.event_count)?;
                info!(
                    batch_sequence = batch.sequence,
                    event_count = batch.event_count,
                    "Herdr batch flush delivered to Iris"
                );
                backoff = INITIAL_BACKOFF;
            }
            Ok(status) => {
                if status.is_client_error() {
                    error!(
                        http_status = status.as_u16(),
                        event_count = batch.event_count,
                        "Iris rejected a durable batch; exact payload retained for retry"
                    );
                } else {
                    warn!(
                        http_status = status.as_u16(),
                        event_count = batch.event_count,
                        "Iris unavailable; exact durable batch retained for retry"
                    );
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(()) => {
                warn!(
                    event_count = batch.event_count,
                    "Iris request failed; exact durable batch retained for retry"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

async fn submit_batch(
    client: &Client,
    ingest_url: &reqwest::Url,
    token: &str,
    batch: &BatchRecord,
) -> Result<StatusCode, ()> {
    let response = client
        .post(ingest_url.clone())
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(batch.body.clone())
        .send()
        .await
        .map_err(|_| ())?;
    Ok(response.status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use serde_json::json;
    use tempfile::tempdir;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn retries_after_restart_reuse_the_same_identity_and_exact_request_body() {
        let dir = tempdir().unwrap();
        let state_dir = dir.path().join("state");
        let mut spool = Spool::open(state_dir.clone()).unwrap();
        let now = Utc::now();
        spool
            .append_upstream_event(json!({"event":"future_event", "data":{}}), now)
            .unwrap();
        spool.flush_due(now + ChronoDuration::seconds(5)).unwrap();
        let first_record = spool.oldest_batch().unwrap().unwrap();
        let expected_body = first_record.body.clone();
        let server = MockServer::start().await;
        let token = "synthetic-test-token";
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        Mock::given(method("POST"))
            .and(path("/ingest"))
            .and(header("authorization", format!("Bearer {token}")))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&server)
            .await;
        let first_status = submit_batch(
            &client,
            &reqwest::Url::parse(&format!("{}/ingest", server.uri())).unwrap(),
            token,
            &first_record,
        )
        .await
        .unwrap();
        assert_eq!(first_status, StatusCode::SERVICE_UNAVAILABLE);
        let first_request = server.received_requests().await.unwrap().remove(0);
        assert_eq!(first_request.body, expected_body.as_bytes());

        drop(spool);
        let mut reopened = Spool::open(state_dir).unwrap();
        let retry_record = reopened.oldest_batch().unwrap().unwrap();
        assert_eq!(retry_record.body, expected_body);
        assert_eq!(retry_record.sequence, first_record.sequence);
        server.reset().await;
        Mock::given(method("POST"))
            .and(path("/ingest"))
            .and(header("authorization", format!("Bearer {token}")))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let retry_status = submit_batch(
            &client,
            &reqwest::Url::parse(&format!("{}/ingest", server.uri())).unwrap(),
            token,
            &retry_record,
        )
        .await
        .unwrap();
        assert_eq!(retry_status, StatusCode::OK);
        let retry_request = server.received_requests().await.unwrap().remove(0);
        assert_eq!(retry_request.body, expected_body.as_bytes());
        reopened
            .remove_batch(retry_record.sequence, retry_record.event_count)
            .unwrap();
        assert!(reopened.oldest_batch().unwrap().is_none());
    }
}

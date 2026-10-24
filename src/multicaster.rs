//! Core HTTP multicaster router, header filtering, and concurrent fan-out logic.

use std::{sync::Arc, time::Instant};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::json;
use tokio::{sync::mpsc, time};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    telemetry::{format_headers_for_log, truncate_body},
};

/// Standard RFC 9110 hop-by-hop headers that must not be forwarded by proxies.
const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

const REQUEST_ID_HEADER: &str = "x-request-id";
const FORWARDED_HOST_HEADER: &str = "x-forwarded-host";

/// Shared application state for the multicaster router.
#[derive(Debug, Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub client: reqwest::Client,
}

impl AppState {
    /// Creates a new [`AppState`] with a connection-pooled `reqwest::Client`.
    pub fn new(config: Config) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        Ok(Self {
            config: Arc::new(config),
            client,
        })
    }
}

/// Builds the Axum [`Router`] for the multicaster service.
pub fn router(state: AppState) -> Router {
    let max_body_bytes = state.config.max_body_bytes;
    Router::new()
        .route("/_health", get(health_handler))
        .route("/healthz", get(health_handler))
        .fallback(multicast_handler)
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .with_state(state)
}

async fn health_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({
            "status": "ok",
            "service": "http-multicaster"
        })),
    )
}

#[derive(Debug)]
struct UpstreamResponse {
    endpoint: String,
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Debug)]
enum EndpointOutcome {
    Primary(UpstreamResponse),
    Fallback(UpstreamResponse),
    Unacceptable { endpoint: String, status: u16 },
    TransportError { endpoint: String, error: String },
}

async fn multicast_handler(
    State(state): State<AppState>,
    request: Request,
) -> Response {
    let started_at = Instant::now();
    let (parts, body) = request.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let incoming_headers = parts.headers;

    let request_id = extract_or_generate_request_id(&incoming_headers);
    let path_and_query = uri
        .path_and_query()
        .map_or_else(|| "/".to_owned(), |pq| pq.as_str().to_owned());

    let body_bytes = match axum::body::to_bytes(body, state.config.max_body_bytes).await {
        Ok(bytes) => bytes,
        Err(err) => {
            error!(
                request_id = %request_id,
                method = %method,
                path = %path_and_query,
                error = %err,
                "Failed to read incoming request body"
            );
            return json_error_response(
                StatusCode::BAD_REQUEST,
                &request_id,
                "Failed to read request body or body exceeded maximum size limit",
            );
        }
    };

    let forward_headers = prepare_forward_headers(&incoming_headers, &request_id);
    let logged_req_headers = format_headers_for_log(&incoming_headers);
    let logged_req_body = truncate_body(&body_bytes, state.config.log_max_body_chars);

    info!(
        request_id = %request_id,
        method = %method,
        path = %path_and_query,
        headers = ?logged_req_headers,
        body_preview = %logged_req_body,
        body_bytes = body_bytes.len(),
        target_count = state.config.target_endpoints.len(),
        "Incoming request received for multicasting"
    );

    let (tx, mut rx) = mpsc::unbounded_channel::<EndpointOutcome>();

    for endpoint in &state.config.target_endpoints {
        let target_url = build_target_url(endpoint, &uri);
        let client = state.client.clone();
        let method_clone = method.clone();
        let headers_clone = forward_headers.clone();
        let body_clone = body_bytes.clone();
        let config_clone = Arc::clone(&state.config);
        let request_id_clone = request_id.clone();
        let endpoint_clone = endpoint.clone();
        let tx_clone = tx.clone();

        // Spawn an independent background task per endpoint so slower endpoints
        // continue receiving and processing the webhook even after the first
        // acceptable response has been returned to the caller.
        tokio::spawn(async move {
            let outcome = send_to_endpoint(
                &client,
                &config_clone,
                &request_id_clone,
                &endpoint_clone,
                &target_url,
                method_clone,
                headers_clone,
                body_clone,
            )
            .await;
            let _ = tx_clone.send(outcome);
        });
    }
    drop(tx);

    let mut first_fallback: Option<UpstreamResponse> = None;
    let timeout_sleep = time::sleep(state.config.request_timeout);
    tokio::pin!(timeout_sleep);

    loop {
        tokio::select! {
            maybe_outcome = rx.recv() => {
                match maybe_outcome {
                    Some(EndpointOutcome::Primary(win)) => {
                        info!(
                            request_id = %request_id,
                            endpoint = %win.endpoint,
                            status = win.status.as_u16(),
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "Returning primary acceptable response to caller"
                        );
                        return build_downstream_response(win, &request_id);
                    }
                    Some(EndpointOutcome::Fallback(candidate)) => {
                        info!(
                            request_id = %request_id,
                            endpoint = %candidate.endpoint,
                            status = candidate.status.as_u16(),
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "Captured fallback response; waiting briefly in case another endpoint returns 2xx/3xx"
                        );
                        if first_fallback.is_none() {
                            first_fallback = Some(candidate);
                        }
                    }
                    Some(EndpointOutcome::Unacceptable { endpoint, status }) => {
                        warn!(
                            request_id = %request_id,
                            endpoint = %endpoint,
                            status = status,
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "Endpoint returned non-acceptable status"
                        );
                    }
                    Some(EndpointOutcome::TransportError { endpoint, error }) => {
                        warn!(
                            request_id = %request_id,
                            endpoint = %endpoint,
                            error = %error,
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "Endpoint request failed"
                        );
                    }
                    None => {
                        // All endpoints have completed without a 200..400 response.
                        if let Some(fallback) = first_fallback {
                            info!(
                                request_id = %request_id,
                                endpoint = %fallback.endpoint,
                                status = fallback.status.as_u16(),
                                elapsed_ms = started_at.elapsed().as_millis(),
                                "No endpoint returned 2xx/3xx; returning captured fallback response"
                            );
                            return build_downstream_response(fallback, &request_id);
                        }

                        warn!(
                            request_id = %request_id,
                            method = %method,
                            path = %path_and_query,
                            elapsed_ms = started_at.elapsed().as_millis(),
                            "All endpoints failed to respond acceptably"
                        );
                        return json_error_response(
                            StatusCode::BAD_GATEWAY,
                            &request_id,
                            "All endpoints failed to respond acceptably",
                        );
                    }
                }
            }
            () = &mut timeout_sleep => {
                if let Some(fallback) = first_fallback {
                    info!(
                        request_id = %request_id,
                        endpoint = %fallback.endpoint,
                        status = fallback.status.as_u16(),
                        elapsed_ms = started_at.elapsed().as_millis(),
                        "Timeout reached waiting for 2xx/3xx; returning captured fallback response"
                    );
                    return build_downstream_response(fallback, &request_id);
                }

                error!(
                    request_id = %request_id,
                    method = %method,
                    path = %path_and_query,
                    timeout_secs = state.config.request_timeout.as_secs(),
                    "Timed out waiting for acceptable response from endpoints"
                );
                return json_error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    &request_id,
                    "Timed out waiting for acceptable response from endpoints",
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_to_endpoint(
    client: &reqwest::Client,
    config: &Config,
    request_id: &str,
    endpoint: &str,
    target_url: &str,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> EndpointOutcome {
    let started_at = Instant::now();

    let response = match client
        .request(method.clone(), target_url)
        .headers(headers)
        .body(body)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            return EndpointOutcome::TransportError {
                endpoint: endpoint.to_owned(),
                error: err.to_string(),
            };
        }
    };

    let status = response.status();
    let resp_headers = response.headers().clone();

    let resp_body = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => {
            return EndpointOutcome::TransportError {
                endpoint: endpoint.to_owned(),
                error: format!("failed to read response body: {err}"),
            };
        }
    };

    let logged_resp_headers = format_headers_for_log(&resp_headers);
    let logged_resp_body = truncate_body(&resp_body, config.log_max_body_chars);

    info!(
        request_id = %request_id,
        method = %method,
        target_url = %target_url,
        status = status.as_u16(),
        elapsed_ms = started_at.elapsed().as_millis(),
        headers = ?logged_resp_headers,
        body_preview = %logged_resp_body,
        body_bytes = resp_body.len(),
        "Endpoint responded"
    );

    let code = status.as_u16();
    if (200..400).contains(&code) {
        EndpointOutcome::Primary(UpstreamResponse {
            endpoint: endpoint.to_owned(),
            status,
            headers: resp_headers,
            body: resp_body,
        })
    } else if config.fallback_status_codes.contains(&code) {
        EndpointOutcome::Fallback(UpstreamResponse {
            endpoint: endpoint.to_owned(),
            status,
            headers: resp_headers,
            body: resp_body,
        })
    } else {
        EndpointOutcome::Unacceptable {
            endpoint: endpoint.to_owned(),
            status: code,
        }
    }
}

/// Joins a normalized base endpoint URL with the incoming request URI's path and query string.
pub fn build_target_url(base_endpoint: &str, uri: &Uri) -> String {
    let base = base_endpoint.trim_end_matches('/');
    let path_and_query = uri.path_and_query().map_or("/", |pq| pq.as_str());
    if path_and_query.starts_with('/') {
        format!("{base}{path_and_query}")
    } else {
        format!("{base}/{path_and_query}")
    }
}

/// Extracts an existing `x-request-id` header or generates a fresh UUID v4 so all
/// multicasted target requests share the same correlation ID in `rook-rust` logs.
pub fn extract_or_generate_request_id(headers: &HeaderMap) -> String {
    headers
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map_or_else(|| Uuid::new_v4().to_string(), ToOwned::to_owned)
}

/// Prepares outgoing headers by stripping hop-by-hop headers, `host`, and `content-length`,
/// preserving `Host` in `x-forwarded-host` if not already set, and ensuring `x-request-id` is set.
pub fn prepare_forward_headers(incoming: &HeaderMap, request_id: &str) -> HeaderMap {
    let connection_tokens = connection_nominated_headers(incoming);
    let mut forwarded = HeaderMap::new();

    for (name, value) in incoming {
        let lower = name.as_str();
        if lower.eq_ignore_ascii_case("host")
            || lower.eq_ignore_ascii_case("content-length")
            || is_hop_by_hop_header(lower, &connection_tokens)
        {
            continue;
        }
        forwarded.append(name.clone(), value.clone());
    }

    if !forwarded.contains_key(FORWARDED_HOST_HEADER)
        && let Some(host) = incoming.get(header::HOST)
    {
        forwarded.insert(FORWARDED_HOST_HEADER, host.clone());
    }

    if let Ok(req_id_val) = HeaderValue::from_str(request_id) {
        forwarded.insert(HeaderName::from_static(REQUEST_ID_HEADER), req_id_val);
    }

    forwarded
}

/// Builds the HTTP response to return to the downstream caller, stripping hop-by-hop
/// headers and `content-length` so Axum calculates the exact body length.
fn build_downstream_response(upstream: UpstreamResponse, request_id: &str) -> Response {
    let connection_tokens = connection_nominated_headers(&upstream.headers);
    let mut response = Response::new(Body::from(upstream.body));
    *response.status_mut() = upstream.status;

    let headers = response.headers_mut();
    for (name, value) in &upstream.headers {
        let lower = name.as_str();
        if lower.eq_ignore_ascii_case("content-length")
            || is_hop_by_hop_header(lower, &connection_tokens)
        {
            continue;
        }
        headers.append(name.clone(), value.clone());
    }

    if !headers.contains_key(REQUEST_ID_HEADER)
        && let Ok(req_id_val) = HeaderValue::from_str(request_id)
    {
        headers.insert(HeaderName::from_static(REQUEST_ID_HEADER), req_id_val);
    }

    response
}

fn json_error_response(status: StatusCode, request_id: &str, message: &str) -> Response {
    let mut response = (status, Json(json!({ "error": message }))).into_response();
    if let Ok(req_id_val) = HeaderValue::from_str(request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), req_id_val);
    }
    response
}

fn connection_nominated_headers(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect()
}

fn is_hop_by_hop_header(name: &str, connection_tokens: &[String]) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|h| name.eq_ignore_ascii_case(h))
        || connection_tokens
            .iter()
            .any(|token| name.eq_ignore_ascii_case(token))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        net::Ipv4Addr,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use axum::{
        Router,
        body::Body,
        extract::Query,
        http::{Request, StatusCode},
        routing::{get, post},
    };
    use tokio::net::TcpListener;
    use tower::ServiceExt;

    use super::*;
    use crate::config::{Config, LogFormat};

    fn test_config(endpoints: Vec<String>, timeout_secs: u64) -> Config {
        Config {
            port: 8001,
            target_endpoints: endpoints,
            request_timeout: Duration::from_secs(timeout_secs),
            connect_timeout: Duration::from_secs(2),
            max_body_bytes: 1024 * 1024,
            log_max_body_chars: 200,
            fallback_status_codes: vec![401],
            log_level: "info".to_owned(),
            log_format: LogFormat::Text,
        }
    }

    async fn spawn_mock_server(app: Router) -> String {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap_or_else(|_| unreachable!());
        let addr = listener.local_addr().unwrap_or_else(|_| unreachable!());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    #[test]
    fn preserves_path_and_query_string_without_double_slashes() {
        let uri: Uri = "/v1/identity/didit-callback?session_id=123&status=Approved"
            .parse()
            .unwrap_or_default();
        assert_eq!(
            build_target_url("https://ikfixfi.share.zrok.io/", &uri),
            "https://ikfixfi.share.zrok.io/v1/identity/didit-callback?session_id=123&status=Approved"
        );
        assert_eq!(
            build_target_url("http://localhost:8001", &uri),
            "http://localhost:8001/v1/identity/didit-callback?session_id=123&status=Approved"
        );
    }

    #[test]
    fn strips_hop_by_hop_and_host_headers_and_sets_request_id() {
        let mut incoming = HeaderMap::new();
        incoming.insert(
            header::HOST,
            HeaderValue::from_static("multicaster.run.app"),
        );
        incoming.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, x-custom-hop"),
        );
        incoming.insert("keep-alive", HeaderValue::from_static("timeout=5"));
        incoming.insert("x-custom-hop", HeaderValue::from_static("drop-me"));
        incoming.insert(header::CONTENT_LENGTH, HeaderValue::from_static("42"));
        incoming.insert(
            "x-signature-v2",
            HeaderValue::from_static("didit-hmac-signature"),
        );

        let forwarded = prepare_forward_headers(&incoming, "req-abc-123");
        assert!(!forwarded.contains_key(header::HOST));
        assert!(!forwarded.contains_key(header::CONNECTION));
        assert!(!forwarded.contains_key("keep-alive"));
        assert!(!forwarded.contains_key("x-custom-hop"));
        assert!(!forwarded.contains_key(header::CONTENT_LENGTH));
        assert_eq!(
            forwarded
                .get("x-forwarded-host")
                .and_then(|v| v.to_str().ok()),
            Some("multicaster.run.app")
        );
        assert_eq!(
            forwarded.get("x-request-id").and_then(|v| v.to_str().ok()),
            Some("req-abc-123")
        );
        assert_eq!(
            forwarded
                .get("x-signature-v2")
                .and_then(|v| v.to_str().ok()),
            Some("didit-hmac-signature")
        );
    }

    #[tokio::test]
    async fn multicasts_to_all_endpoints_and_prefers_200_over_fast_401() {
        let hits_fast_401 = Arc::new(AtomicUsize::new(0));
        let hits_valid_200 = Arc::new(AtomicUsize::new(0));
        let hits_slow_bg = Arc::new(AtomicUsize::new(0));

        // Endpoint 1: returns 401 immediately (e.g., dev instance without matching credentials)
        let h1 = Arc::clone(&hits_fast_401);
        let srv1 = spawn_mock_server(Router::new().route(
            "/v1/externalservice/corecard/webhooks/auth/token",
            post(move || {
                let h1 = Arc::clone(&h1);
                async move {
                    h1.fetch_add(1, Ordering::SeqCst);
                    (
                        StatusCode::UNAUTHORIZED,
                        Json(json!({"reason": "wrong_secret"})),
                    )
                }
            }),
        ))
        .await;

        // Endpoint 2: returns 200 OK after 40ms with query params and headers verified
        let h2 = Arc::clone(&hits_valid_200);
        let srv2 = spawn_mock_server(Router::new().route(
            "/v1/externalservice/corecard/webhooks/auth/token",
            post(
                move |Query(params): Query<HashMap<String, String>>,
                      headers: HeaderMap,
                      body: Bytes| {
                    let h2 = Arc::clone(&h2);
                    async move {
                        time::sleep(Duration::from_millis(40)).await;
                        h2.fetch_add(1, Ordering::SeqCst);
                        let req_id = headers
                            .get("x-request-id")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_owned();
                        (
                            StatusCode::OK,
                            Json(json!({
                                "token": "jwt-from-srv2",
                                "env": params.get("env").cloned().unwrap_or_default(),
                                "req_id": req_id,
                                "received_len": body.len(),
                            })),
                        )
                    }
                },
            ),
        ))
        .await;

        // Endpoint 3: slow endpoint (150ms) that finishes in the background after caller gets response
        let h3 = Arc::clone(&hits_slow_bg);
        let srv3 = spawn_mock_server(Router::new().route(
            "/v1/externalservice/corecard/webhooks/auth/token",
            post(move || {
                let h3 = Arc::clone(&h3);
                async move {
                    time::sleep(Duration::from_millis(150)).await;
                    h3.fetch_add(1, Ordering::SeqCst);
                    StatusCode::OK
                }
            }),
        ))
        .await;

        let state = AppState::new(test_config(vec![srv1, srv2, srv3], 5))
            .unwrap_or_else(|_| unreachable!());
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/v1/externalservice/corecard/webhooks/auth/token?env=sandbox")
            .header("content-type", "application/json")
            .header("x-request-id", "corr-id-999")
            .body(Body::from(r#"{"client_id":"cc"}"#))
            .unwrap_or_else(|_| unreachable!());

        let resp = app.oneshot(req).await.unwrap_or_else(|_| unreachable!());
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
            Some("corr-id-999")
        );

        let body_bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap_or_else(|_| unreachable!());
        let payload: serde_json::Value =
            serde_json::from_slice(&body_bytes).unwrap_or_else(|_| unreachable!());
        assert_eq!(payload["token"], "jwt-from-srv2");
        assert_eq!(payload["env"], "sandbox");
        assert_eq!(payload["req_id"], "corr-id-999");
        assert_eq!(payload["received_len"], 18);

        // Wait briefly for the slow background endpoint to complete too.
        time::sleep(Duration::from_millis(200)).await;
        assert_eq!(hits_fast_401.load(Ordering::SeqCst), 1);
        assert_eq!(hits_valid_200.load(Ordering::SeqCst), 1);
        assert_eq!(hits_slow_bg.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn falls_back_to_401_when_no_endpoint_returns_2xx() {
        let srv1 = spawn_mock_server(Router::new().route(
            "/v1/identity/didit-callback",
            post(|| async {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error": "invalid_signature"})),
                )
            }),
        ))
        .await;

        let srv2 = spawn_mock_server(
            Router::new().route(
                "/v1/identity/didit-callback",
                post(|| async { StatusCode::BAD_GATEWAY }),
            ),
        )
        .await;

        let state =
            AppState::new(test_config(vec![srv1, srv2], 5)).unwrap_or_else(|_| unreachable!());
        let app = router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/v1/identity/didit-callback")
            .body(Body::from("{}"))
            .unwrap_or_else(|_| unreachable!());

        let resp = app.oneshot(req).await.unwrap_or_else(|_| unreachable!());
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn returns_502_when_all_endpoints_fail() {
        let srv1 = spawn_mock_server(
            Router::new().route("/v1/test", get(|| async { StatusCode::INTERNAL_SERVER_ERROR })),
        )
        .await;

        let state = AppState::new(test_config(vec![srv1], 5)).unwrap_or_else(|_| unreachable!());
        let app = router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/v1/test")
            .body(Body::empty())
            .unwrap_or_else(|_| unreachable!());

        let resp = app.oneshot(req).await.unwrap_or_else(|_| unreachable!());
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }
}


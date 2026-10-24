//! Structured logging, safe UTF-8 body truncation, and sensitive header redaction.

use std::collections::BTreeMap;

use axum::http::HeaderMap;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::LogFormat;

/// Headers whose values carry credentials or signatures and must be redacted in logs.
const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "x-admin-key",
    "x-signature-v2",
    "cookie",
    "set-cookie",
];

/// Initializes the global `tracing` subscriber according to `log_level` and `log_format`.
pub fn init(log_level: &str, log_format: LogFormat) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    match log_format {
        LogFormat::Text => {
            let _ = tracing_subscriber::registry()
                .with(filter)
                .with(fmt::layer().with_target(false))
                .try_init();
        }
        LogFormat::Json => {
            let _ = tracing_subscriber::registry()
                .with(filter)
                .with(
                    fmt::layer()
                        .json()
                        .flatten_event(true)
                        .with_current_span(true),
                )
                .try_init();
        }
    }
}

/// Converts a byte slice to UTF-8 (lossily) and safely truncates to at most `max_chars`
/// Unicode scalar values without panicking on multi-byte UTF-8 boundaries.
pub fn truncate_body(bytes: &[u8], max_chars: usize) -> String {
    if bytes.is_empty() || max_chars == 0 {
        return String::new();
    }
    let text = String::from_utf8_lossy(bytes);
    let mut chars = text.chars();
    let prefix: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        format!("{prefix}... [truncated]")
    } else {
        prefix
    }
}

/// Formats HTTP headers for logging, redacting sensitive credentials and signatures
/// while keeping non-sensitive headers intact.
pub fn format_headers_for_log(headers: &HeaderMap) -> BTreeMap<String, Vec<String>> {
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, value) in headers {
        let key = name.as_str().to_owned();
        let rendered = if is_sensitive_header(name.as_str()) {
            "[REDACTED]".to_owned()
        } else {
            value.to_str().unwrap_or("<non-ascii>").to_owned()
        };
        map.entry(key).or_default().push(rendered);
    }
    map
}

fn is_sensitive_header(name: &str) -> bool {
    SENSITIVE_HEADERS
        .iter()
        .any(|sensitive| name.eq_ignore_ascii_case(sensitive))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::*;

    #[test]
    fn truncates_multibyte_utf8_safely_by_character_count() {
        let cyrillic = "Привет мир! Это тест мультикастера.";
        let truncated = truncate_body(cyrillic.as_bytes(), 10);
        assert_eq!(truncated, "Привет мир... [truncated]");

        let short = truncate_body("hello".as_bytes(), 200);
        assert_eq!(short, "hello");
    }

    #[test]
    fn redacts_sensitive_headers_in_log_view() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-jwt-token"),
        );
        headers.insert("x-admin-key", HeaderValue::from_static("super-secret-key"));
        headers.insert("x-signature-v2", HeaderValue::from_static("hmac-sig"));
        headers.insert("x-request-id", HeaderValue::from_static("req-123"));

        let logged = format_headers_for_log(&headers);
        assert_eq!(
            logged.get("authorization"),
            Some(&vec!["[REDACTED]".to_owned()])
        );
        assert_eq!(
            logged.get("x-admin-key"),
            Some(&vec!["[REDACTED]".to_owned()])
        );
        assert_eq!(
            logged.get("x-signature-v2"),
            Some(&vec!["[REDACTED]".to_owned()])
        );
        assert_eq!(
            logged.get("x-request-id"),
            Some(&vec!["req-123".to_owned()])
        );
    }
}


//! Configuration loading and validation for `http-multicaster`.

use std::{collections::HashMap, env, time::Duration};

use thiserror::Error;

/// Default local and container port (`8001`).
///
/// Chosen so `http-multicaster` does not collide with `rook-rust`'s three app ports
/// (`8080` for `online`, `8081` for `offline`, `8082` for `reporting`) and matches the
/// port `8001` redirect convention.
pub const DEFAULT_PORT: u16 = 8001;

/// Default overall and per-endpoint request timeout (`30` seconds).
pub const DEFAULT_REQUEST_TIMEOUT_SECONDS: u64 = 30;

/// Default TCP/TLS connection timeout per target endpoint (`5` seconds).
pub const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 5;

/// Default maximum request body size (`10 MiB`), covering CoreCard webhook batches (`1 MiB`)
/// and media uploads (`10 MiB`) in `rook-rust`.
pub const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// Default character limit when logging request and response bodies (`200` chars).
pub const DEFAULT_LOG_MAX_BODY_CHARS: usize = 200;

/// Default fallback HTTP status codes (`401`) accepted when no endpoint returns `200..400`.
pub const DEFAULT_FALLBACK_STATUS_CODES: &[u16] = &[401];

/// Errors that can occur when parsing [`Config`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("TARGET_ENDPOINTS must be set and contain at least one valid URL")]
    MissingTargetEndpoints,

    #[error("invalid target endpoint URL `{0}`: must start with http:// or https://")]
    InvalidTargetEndpoint(String),

    #[error("invalid {var} value `{value}`: {reason}")]
    InvalidValue {
        var: &'static str,
        value: String,
        reason: &'static str,
    },
}

/// Output format for structured logs, matching `rook-rust`'s `LOG_FORMAT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    /// Human-readable console logs for local development.
    #[default]
    Text,
    /// Structured JSON logs with `severity` and `message` for Google Cloud Logging.
    Json,
}

/// Runtime configuration for the HTTP multicaster service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// TCP port to listen on (`PORT`, default `8001`).
    pub port: u16,
    /// Normalized target base URLs without trailing slashes (`TARGET_ENDPOINTS`).
    pub target_endpoints: Vec<String>,
    /// Overall and per-endpoint request timeout (`REQUEST_TIMEOUT_SECONDS`, default `30`s).
    pub request_timeout: Duration,
    /// Per-endpoint TCP/TLS connect timeout (`CONNECT_TIMEOUT_SECONDS`, default `5`s).
    pub connect_timeout: Duration,
    /// Maximum incoming request body size in bytes (`MAX_BODY_BYTES`, default `10 MiB`).
    pub max_body_bytes: usize,
    /// Maximum characters to log for request and response bodies (`LOG_MAX_BODY_CHARS`, default `200`).
    pub log_max_body_chars: usize,
    /// Status codes accepted as fallback if no endpoint responds with `200..400`
    /// (`FALLBACK_STATUS_CODES`, default `401`).
    pub fallback_status_codes: Vec<u16>,
    /// Log level filter string (`LOG_LEVEL`, default `info`).
    pub log_level: String,
    /// Log format (`LOG_FORMAT`, default `text`).
    pub log_format: LogFormat,
}

impl Config {
    /// Loads `.env` if present and parses configuration from process environment variables.
    pub fn from_env() -> Result<Self, ConfigError> {
        let _ = dotenvy::dotenv();
        let vars: HashMap<String, String> = env::vars().collect();
        Self::from_map(&vars)
    }

    /// Parses configuration from a key-value map.
    pub fn from_map(vars: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let port = match vars.get("PORT").map(|s| s.trim()).filter(|s| !s.is_empty()) {
            Some(raw) => raw.parse::<u16>().map_err(|_| ConfigError::InvalidValue {
                var: "PORT",
                value: raw.to_owned(),
                reason: "expected a valid TCP port (1..=65535)",
            })?,
            None => DEFAULT_PORT,
        };
        if port == 0 {
            return Err(ConfigError::InvalidValue {
                var: "PORT",
                value: "0".to_owned(),
                reason: "port must be between 1 and 65535",
            });
        }

        let raw_endpoints = vars
            .get("TARGET_ENDPOINTS")
            .ok_or(ConfigError::MissingTargetEndpoints)?;
        let target_endpoints = parse_target_endpoints(raw_endpoints)?;

        let request_timeout_secs = parse_u64_positive(
            vars,
            "REQUEST_TIMEOUT_SECONDS",
            DEFAULT_REQUEST_TIMEOUT_SECONDS,
        )?;
        let connect_timeout_secs = parse_u64_positive(
            vars,
            "CONNECT_TIMEOUT_SECONDS",
            DEFAULT_CONNECT_TIMEOUT_SECONDS,
        )?;

        let max_body_bytes = match vars
            .get("MAX_BODY_BYTES")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            Some(raw) => raw.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                var: "MAX_BODY_BYTES",
                value: raw.to_owned(),
                reason: "expected a positive byte count",
            })?,
            None => DEFAULT_MAX_BODY_BYTES,
        };
        if max_body_bytes == 0 {
            return Err(ConfigError::InvalidValue {
                var: "MAX_BODY_BYTES",
                value: "0".to_owned(),
                reason: "must be greater than 0",
            });
        }

        let log_max_body_chars = match vars
            .get("LOG_MAX_BODY_CHARS")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            Some(raw) => raw.parse::<usize>().map_err(|_| ConfigError::InvalidValue {
                var: "LOG_MAX_BODY_CHARS",
                value: raw.to_owned(),
                reason: "expected a non-negative integer",
            })?,
            None => DEFAULT_LOG_MAX_BODY_CHARS,
        };

        let fallback_status_codes = match vars.get("FALLBACK_STATUS_CODES").map(|s| s.trim()) {
            Some("") => Vec::new(),
            Some(raw) => parse_status_codes(raw)?,
            None => DEFAULT_FALLBACK_STATUS_CODES.to_vec(),
        };

        let log_level = vars
            .get("LOG_LEVEL")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("info")
            .to_owned();

        let log_format = match vars
            .get("LOG_FORMAT")
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            Some(raw) if raw.eq_ignore_ascii_case("text") => LogFormat::Text,
            Some(raw) if raw.eq_ignore_ascii_case("json") => LogFormat::Json,
            Some(raw) => {
                return Err(ConfigError::InvalidValue {
                    var: "LOG_FORMAT",
                    value: raw.to_owned(),
                    reason: "expected `text` or `json`",
                });
            }
            None => LogFormat::Text,
        };

        Ok(Self {
            port,
            target_endpoints,
            request_timeout: Duration::from_secs(request_timeout_secs),
            connect_timeout: Duration::from_secs(connect_timeout_secs),
            max_body_bytes,
            log_max_body_chars,
            fallback_status_codes,
            log_level,
            log_format,
        })
    }
}

fn parse_target_endpoints(raw: &str) -> Result<Vec<String>, ConfigError> {
    let mut endpoints = Vec::new();
    for item in raw.split(',') {
        let trimmed = item.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            continue;
        }
        if !trimmed.starts_with("http://") && !trimmed.starts_with("https://") {
            return Err(ConfigError::InvalidTargetEndpoint(trimmed.to_owned()));
        }
        let authority = trimmed
            .strip_prefix("http://")
            .or_else(|| trimmed.strip_prefix("https://"))
            .unwrap_or("");
        if authority.is_empty() {
            return Err(ConfigError::InvalidTargetEndpoint(trimmed.to_owned()));
        }
        endpoints.push(trimmed.to_owned());
    }
    if endpoints.is_empty() {
        return Err(ConfigError::MissingTargetEndpoints);
    }
    Ok(endpoints)
}

fn parse_u64_positive(
    vars: &HashMap<String, String>,
    var: &'static str,
    default: u64,
) -> Result<u64, ConfigError> {
    let Some(raw) = vars.get(var).map(|s| s.trim()).filter(|s| !s.is_empty()) else {
        return Ok(default);
    };
    let parsed = raw.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
        var,
        value: raw.to_owned(),
        reason: "expected a positive integer",
    })?;
    if parsed == 0 {
        return Err(ConfigError::InvalidValue {
            var,
            value: raw.to_owned(),
            reason: "must be greater than 0",
        });
    }
    Ok(parsed)
}

fn parse_status_codes(raw: &str) -> Result<Vec<u16>, ConfigError> {
    let mut codes = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let code = trimmed
            .parse::<u16>()
            .map_err(|_| ConfigError::InvalidValue {
                var: "FALLBACK_STATUS_CODES",
                value: trimmed.to_owned(),
                reason: "expected HTTP status codes between 100 and 599",
            })?;
        if !(100..=599).contains(&code) {
            return Err(ConfigError::InvalidValue {
                var: "FALLBACK_STATUS_CODES",
                value: trimmed.to_owned(),
                reason: "HTTP status code must be between 100 and 599",
            });
        }
        codes.push(code);
    }
    Ok(codes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn defaults_port_to_8001_and_normalizes_endpoints() {
        let cfg = Config::from_map(&vars(&[(
            "TARGET_ENDPOINTS",
            " https://ikfixfi.share.zrok.io/ , http://localhost:8001/ ",
        )]));
        assert!(cfg.is_ok());
        let Ok(cfg) = cfg else { return };
        assert_eq!(cfg.port, 8001);
        assert_eq!(
            cfg.target_endpoints,
            vec![
                "https://ikfixfi.share.zrok.io".to_owned(),
                "http://localhost:8001".to_owned()
            ]
        );
        assert_eq!(cfg.request_timeout, Duration::from_secs(30));
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
        assert_eq!(cfg.fallback_status_codes, vec![401]);
        assert_eq!(cfg.log_format, LogFormat::Text);
    }

    #[test]
    fn rejects_missing_or_invalid_target_endpoints() {
        assert_eq!(
            Config::from_map(&vars(&[])),
            Err(ConfigError::MissingTargetEndpoints)
        );
        assert_eq!(
            Config::from_map(&vars(&[("TARGET_ENDPOINTS", " , ")])),
            Err(ConfigError::MissingTargetEndpoints)
        );
        assert_eq!(
            Config::from_map(&vars(&[("TARGET_ENDPOINTS", "ftp://example.com")])),
            Err(ConfigError::InvalidTargetEndpoint(
                "ftp://example.com".to_owned()
            ))
        );
    }

    #[test]
    fn parses_custom_settings() {
        let cfg = Config::from_map(&vars(&[
            ("PORT", "8082"),
            ("TARGET_ENDPOINTS", "http://localhost:8001"),
            ("REQUEST_TIMEOUT_SECONDS", "45"),
            ("CONNECT_TIMEOUT_SECONDS", "3"),
            ("MAX_BODY_BYTES", "2097152"),
            ("LOG_MAX_BODY_CHARS", "500"),
            ("FALLBACK_STATUS_CODES", "401,403"),
            ("LOG_LEVEL", "debug"),
            ("LOG_FORMAT", "json"),
        ]));
        assert!(cfg.is_ok());
        let Ok(cfg) = cfg else { return };
        assert_eq!(cfg.port, 8082);
        assert_eq!(cfg.request_timeout, Duration::from_secs(45));
        assert_eq!(cfg.connect_timeout, Duration::from_secs(3));
        assert_eq!(cfg.max_body_bytes, 2_097_152);
        assert_eq!(cfg.log_max_body_chars, 500);
        assert_eq!(cfg.fallback_status_codes, vec![401, 403]);
        assert_eq!(cfg.log_level, "debug");
        assert_eq!(cfg.log_format, LogFormat::Json);
    }
}


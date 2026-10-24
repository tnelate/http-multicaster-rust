# HTTP Multicaster (Rust)

Rust port of `http-multicaster` tailored for the `rook-rust` backend. It accepts incoming HTTP requests (such as CoreCard and Didit webhooks) on any path and method, concurrently forwards them to all configured `TARGET_ENDPOINTS`, and returns the best response to the caller while slower endpoints finish processing in the background.

## Key Improvements over Legacy Go Multicaster

1. **Unified Port `8001` Default (`PORT=8001`)**:
   - Uses `8001` by default across code, `.env.sample`, and `Dockerfile` (`EXPOSE 8001`), avoiding port collisions with `rook-rust`'s local app ports (`8080` for `online`, `8081` for `offline`, `8082` for `reporting`).
2. **Query String Preservation & URL Normalization**:
   - Preserves the full URI `path_and_query` (e.g. `GET /v1/identity/didit-callback?verificationSessionId=...`) and normalizes trailing slashes on `TARGET_ENDPOINTS` so Axum routes never fail on `//` double slashes.
3. **Tiered Response Selection (`2xx/3xx` Priority over `401`)**:
   - Immediately returns the first `200..400` response.
   - If an endpoint responds with `401 Unauthorized` (e.g. a developer tunnel whose local DB has different webhook credentials), it is held as a fallback rather than immediately short-circuiting a valid `200 OK` from another developer's tunnel. If no endpoint returns `200..400`, the captured `401` is returned instead of a blind `502 Bad Gateway`.
4. **RFC 9110 Hop-by-Hop & `Host` Header Filtering + `x-request-id` Correlation**:
   - Strips hop-by-hop headers (`connection`, `keep-alive`, `transfer-encoding`, `upgrade`, etc.), `host`, and `content-length` so `reqwest` sets the proper target `Host` header for each `.zrok.io` / target endpoint.
   - Preserves or generates a shared `x-request-id` header across all multicasted requests for end-to-end log correlation in `rook-rust`.
5. **Connection Pooling & Independent Timeouts**:
   - Reuses a single pooled `reqwest::Client` (`rustls`) with configurable `CONNECT_TIMEOUT_SECONDS` (default `5`s) and `REQUEST_TIMEOUT_SECONDS` (default `30`s), without following upstream `3xx` redirects so redirect responses are returned intact to the caller.
6. **UTF-8 Safe Truncation, Sensitive Header Redaction & Cloud Logging**:
   - Truncates logged payloads by Unicode character boundaries (`LOG_MAX_BODY_CHARS=200`) rather than raw byte indices.
   - Redacts sensitive headers (`authorization`, `x-admin-key`, `x-signature-v2`, `cookie`) in logs while forwarding them untouched to target endpoints.
   - Supports `LOG_FORMAT=text|json` and `LOG_LEVEL`, `/_health` & `/healthz` probes, and graceful shutdown on `SIGTERM` / `Ctrl-C`.

## Configuration (`.env`)

Copy `.env.sample` to `.env`:

```env
PORT=8001
TARGET_ENDPOINTS=https://tnfixfi.share.zrok.io,https://fafixfi.share.zrok.io,https://ikfixfi.share.zrok.io,https://akfixfi.share.zrok.io,https://vdfixfi.share.zrok.io
REQUEST_TIMEOUT_SECONDS=30
CONNECT_TIMEOUT_SECONDS=5
MAX_BODY_BYTES=10485760
LOG_MAX_BODY_CHARS=200
FALLBACK_STATUS_CODES=401
LOG_LEVEL=info
LOG_FORMAT=text
```

For local forwarding (e.g. redirecting traffic to port `8001` or `rook-rust` services):

```env
PORT=8001
TARGET_ENDPOINTS=http://localhost:8001,http://localhost:8081
```

## Running Locally

```bash
make run
```

Or directly with Cargo:

```bash
cp .env.sample .env
cargo run
```

## Testing & Linting

```bash
make check
```

## Docker

```bash
docker build -t http-multicaster-rust .
docker run -p 8001:8001 --env-file .env http-multicaster-rust
```


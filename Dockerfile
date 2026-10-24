# syntax=docker/dockerfile:1

FROM rust:1.98.1-trixie AS builder
WORKDIR /build

COPY Cargo.toml rust-toolchain.toml ./
COPY src ./src

RUN cargo build --release --bin http-multicaster \
    && strip /build/target/release/http-multicaster

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --user-group --no-create-home multicaster

WORKDIR /app
COPY --from=builder /build/target/release/http-multicaster /app/http-multicaster

USER multicaster:multicaster
ENV PORT=8001 \
    LOG_FORMAT=json
EXPOSE 8001

ENTRYPOINT ["/app/http-multicaster"]


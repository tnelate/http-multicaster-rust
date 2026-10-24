.PHONY: run test clippy fmt fmt-check lint check

.env:
	cp .env.sample .env

run: .env
	cargo run

test:
	cargo test --all-targets

clippy:
	cargo clippy --all-targets -- -D warnings

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

lint: clippy fmt-check

check: lint test


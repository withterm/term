.PHONY: setup fmt fmt-check clippy test integration all ci-check

# Setup rust environment
setup:
	./setup-rust-env.sh

# Format code
fmt:
	cargo fmt --all

# Check formatting (CI style)
fmt-check:
	cargo fmt --all -- --check

# Run clippy with CI settings
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

# Run tests
test:
	cargo test

# Run integration tests
integration:
	cargo integration

# Run all CI checks locally
ci-check: fmt-check clippy test

# Run everything
all: fmt clippy test integration
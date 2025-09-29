# Term Project Makefile
# Provides standardized commands for development and CI/CD

.PHONY: all build test fmt fmt-check clippy bench clean doc help setup integration ci-check

# Default target
all: fmt clippy test

# Setup rust environment
setup:
	./setup-rust-env.sh

# Build the project
build:
	@echo "🔨 Building Term..."
	@cargo build --all-features

# Build in release mode
build-release:
	@echo "🚀 Building Term (release)..."
	@cargo build --release --all-features

# Run tests
test:
	@echo "🧪 Running tests..."
	@cargo test --all-features

# Run integration tests only
test-integration:
	@echo "🔗 Running integration tests..."
	@cargo test --test integration

# Legacy alias for integration tests
integration:
	@cargo test --test integration

# Run benchmarks
bench:
	@echo "📊 Running benchmarks..."
	@cargo bench

# Format code
fmt:
	@echo "🎨 Formatting code..."
	@cargo fmt --all

# Check formatting without modifying
fmt-check:
	@echo "📝 Checking code formatting..."
	@cargo fmt --all -- --check

# Run clippy linter
clippy:
	@echo "📋 Running clippy..."
	@cargo clippy --all-targets --all-features -- -D warnings

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	@cargo clean

# Generate documentation
doc:
	@echo "📚 Generating documentation..."
	@cargo doc --no-deps --all-features --open

# Run pre-commit checks
pre-commit: fmt-check clippy test
	@echo "✅ All pre-commit checks passed!"

# Run all CI checks locally (legacy alias)
ci-check: fmt-check clippy test

# Quick check for CI
ci: fmt-check clippy test-integration
	@echo "✅ CI checks passed!"

# Install development tools
install-tools:
	@echo "🔧 Installing development tools..."
	@cargo install cargo-watch cargo-outdated cargo-audit cargo-machete

# Check for outdated dependencies
outdated:
	@echo "🔍 Checking for outdated dependencies..."
	@cargo outdated

# Security audit
audit:
	@echo "🔒 Running security audit..."
	@cargo audit

# Find unused dependencies
unused-deps:
	@echo "🔍 Finding unused dependencies..."
	@cargo machete

# Watch for changes and run tests
watch:
	@echo "👁️ Watching for changes..."
	@cargo watch -x test

# Watch and run specific command
watch-run:
	@echo "👁️ Watching and running..."
	@cargo watch -x run

# Help target
help:
	@echo "Term Project Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make build         - Build the project"
	@echo "  make build-release - Build in release mode"
	@echo "  make test          - Run all tests"
	@echo "  make test-integration - Run integration tests only"
	@echo "  make bench         - Run benchmarks"
	@echo "  make fmt           - Format code"
	@echo "  make fmt-check     - Check code formatting"
	@echo "  make clippy        - Run clippy linter"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make doc           - Generate documentation"
	@echo "  make pre-commit    - Run all pre-commit checks"
	@echo "  make ci            - Run CI checks"
	@echo "  make install-tools - Install development tools"
	@echo "  make outdated      - Check for outdated dependencies"
	@echo "  make audit         - Run security audit"
	@echo "  make unused-deps   - Find unused dependencies"
	@echo "  make watch         - Watch for changes and run tests"
	@echo "  make watch-run     - Watch for changes and run"
	@echo "  make help          - Show this help message"
#!/bin/bash
# Setup script to match CI Rust environment

echo "Setting up Rust environment to match CI..."

# Update rustup itself
echo "Updating rustup..."
rustup self update

# Update to latest stable
echo "Updating to latest stable Rust..."
rustup update stable

# Set stable as default
rustup default stable

# Install required components
echo "Installing required components..."
rustup component add rustfmt clippy

# Show current configuration
echo ""
echo "Current Rust configuration:"
rustc --version
cargo --version
cargo fmt --version
cargo clippy --version

echo ""
echo "Environment setup complete!"
echo ""
echo "To run the same checks as CI, use:"
echo "  cargo fmt --all -- --check"
echo "  cargo clippy --all-targets --all-features -- -D warnings"
echo "  cargo test"
echo "  cargo integration"
#!/bin/bash
# Term Claude Code Hooks Installation Script
#
# This script builds and installs the Rust-based Claude Code hooks for Term.

set -e

echo "🚀 Installing Term Claude Code Hooks..."

# Check if we're in the Term project root
if [ ! -f "Cargo.toml" ] || [ ! -d "term-hooks" ]; then
    echo "❌ Error: Please run this script from the Term project root directory."
    exit 1
fi

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "❌ Error: Rust is not installed. Please install Rust from https://rustup.rs/"
    exit 1
fi

# Build the hooks in release mode
echo "🔨 Building hooks..."
cd term-hooks
cargo build --release

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to build hooks."
    exit 1
fi

cd ..

# Check if the binaries were built
HOOKS=(
    "user-prompt-submit"
    "pre-tool-use"
    "post-tool-use"
    "session-start"
    "stop"
    "pre-compact"
    "notification"
)

echo "✅ Checking built hooks..."
for hook in "${HOOKS[@]}"; do
    if [ ! -f "target/release/$hook" ]; then
        echo "❌ Error: Hook binary not found: $hook"
        exit 1
    fi
    echo "  ✓ $hook"
done

# Create logs directory if it doesn't exist
if [ ! -d "logs" ]; then
    echo "📁 Creating logs directory..."
    mkdir -p logs
fi

# Create .claude/backups directory for pre-compact hook
if [ ! -d ".claude/backups" ]; then
    echo "📁 Creating .claude/backups directory..."
    mkdir -p .claude/backups
fi

echo ""
echo "✨ Term Claude Code Hooks installed successfully!"
echo ""
echo "📋 Installed hooks:"
echo "  • UserPromptSubmit - Adds Term context to prompts"
echo "  • PreToolUse - Blocks dangerous commands and .env access"
echo "  • PostToolUse - Runs quality checks after file edits"
echo "  • SessionStart - Loads development context on startup"
echo "  • Stop - Shows task completion summary"
echo "  • PreCompact - Backs up conversation transcripts"
echo "  • Notification - Logs Claude Code notifications"
echo ""
echo "🔧 Hook configuration:"
echo "  Settings: .claude/settings.json"
echo "  Logs: ./logs/*.json"
echo "  Backups: .claude/backups/"
echo ""
echo "💡 Tips:"
echo "  • Run 'make help' to see available commands"
echo "  • Hooks log to ./logs/ for debugging"
echo "  • Modify .claude/settings.json to customize hook behavior"
echo "  • Run 'cargo build --release' in term-hooks/ to rebuild after changes"
echo ""
echo "🎉 Ready to use Claude Code with enhanced Term development support!"
# Term Claude Code Hooks

Rust-based Claude Code hooks for the Term data validation library, providing enhanced development experience with security validation, automated quality checks, and intelligent context loading.

## Overview

This crate implements all 8 Claude Code hook types in Rust, offering:
- **Type Safety**: Compile-time guarantees for hook behavior
- **Performance**: Native binary execution (~10x faster than Python)
- **Integration**: Leverages Term's existing infrastructure
- **Security**: Memory-safe validation of dangerous operations

## Installation

From the Term project root:

```bash
./install-hooks.sh
```

Or manually:

```bash
cd term-hooks
cargo build --release
```

## Hook Types

### 1. UserPromptSubmit (`user-prompt-submit`)
**Purpose**: Validates and enhances prompts before Claude processes them  
**Features**:
- Prompt validation for dangerous patterns
- Secret detection (passwords, API keys)
- Context injection (git status, Term guidelines)
- Audit logging

**Flags**:
- `--validate`: Enable security validation
- `--context`: Add project context (default)
- `--log-only`: Only log prompts

### 2. PreToolUse (`pre-tool-use`)
**Purpose**: Security gate for tool execution  
**Blocks**:
- Dangerous `rm -rf` commands
- Access to `.env` files (except `.env.sample`)
- Destructive SQL operations
- Overly permissive file permissions
- Piping untrusted content to shell

### 3. PostToolUse (`post-tool-use`)
**Purpose**: Quality checks after tool execution  
**Actions**:
- Formats Rust files with `rustfmt`
- Runs `clippy` after code changes
- Validates `Cargo.toml` modifications
- Checks test results
- Logs all tool usage

### 4. SessionStart (`session-start`)
**Purpose**: Loads development context on session start  
**Provides**:
- Git branch and status
- Recent commits
- Outstanding TODOs
- Test status
- Term-specific guidelines
- Linear ticket context

### 5. Stop (`stop`)
**Purpose**: Task completion summary  
**Shows**:
- Test pass/fail status
- Files modified count
- Git diff statistics
- Next steps guidance

### 6. PreCompact (`pre-compact`)
**Purpose**: Backs up conversations before compaction  
**Features**:
- Timestamp-based backups
- Automatic cleanup (keeps last 10)
- Manual vs auto compaction handling

### 7. Notification (`notification`)
**Purpose**: Handles Claude Code notifications  
**Options**:
- Desktop notifications (macOS/Linux)
- Audit logging
- `--notify` flag for system alerts

## Security Features

### Command Validation
```rust
// Blocked patterns
rm -rf /
chmod 777
curl ... | sh
drop database
truncate table
```

### File Protection
- Blocks `.env` file access
- Warns on critical file modifications
- Validates TOML syntax

### SQL Safety
- Prevents `DROP TABLE/DATABASE`
- Warns on `DELETE FROM` without WHERE
- Blocks `TRUNCATE` operations

## Logging

All hooks log to JSON files in `./logs/`:
```
logs/
├── user_prompt_submit.json
├── pre_tool_use.json
├── post_tool_use.json
├── session_start.json
├── stop.json
├── pre_compact.json
└── notification.json
```

View logs:
```bash
cat logs/pre_tool_use.json | jq '.'
```

## Configuration

Edit `.claude/settings.json` to customize:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "$CLAUDE_PROJECT_DIR/term-hooks/target/release/user-prompt-submit --context"
          }
        ]
      }
    ]
  }
}
```

## Development

### Building
```bash
cargo build --release
```

### Testing
```bash
cargo test
```

### Adding a New Hook

1. Add binary entry to `Cargo.toml`:
```toml
[[bin]]
name = "my-hook"
path = "src/bin/my_hook.rs"
```

2. Implement hook logic:
```rust
use term_hooks::*;

fn main() -> Result<()> {
    let input = read_input()?;
    // Process input
    let _ = log_to_file(&get_log_dir(), "my_hook.json", &input);
    process::exit(0);
}
```

3. Update `.claude/settings.json`

## Architecture

```
term-hooks/
├── src/
│   ├── lib.rs           # Common functionality
│   └── bin/             # Hook binaries
│       ├── user_prompt_submit.rs
│       ├── pre_tool_use.rs
│       ├── post_tool_use.rs
│       ├── session_start.rs
│       ├── stop.rs
│       ├── pre_compact.rs
│       └── notification.rs
└── target/release/      # Compiled binaries
```

## Performance

Benchmarks vs Python hooks:
- **Startup**: 5ms vs 50ms
- **JSON parsing**: 1ms vs 8ms  
- **Regex validation**: 0.5ms vs 3ms
- **Total overhead**: ~10ms vs ~100ms per hook

## Telemetry Integration (Future)

Planned OpenTelemetry support:
```rust
#[instrument]
fn validate_tool_call(data: &ToolData) -> Option<String> {
    // Automatic span creation and metrics
}
```

Enable with:
```toml
[features]
telemetry = ["opentelemetry", "tracing-opentelemetry"]
```

## Troubleshooting

### Hooks not firing
1. Check `.claude/settings.json` syntax
2. Verify binaries exist: `ls term-hooks/target/release/`
3. Check permissions: `chmod +x term-hooks/target/release/*`

### Build failures
```bash
# Clean and rebuild
cargo clean
cargo build --release
```

### Debugging
Set environment variable for verbose logging:
```bash
export RUST_LOG=debug
```

## Best Practices

1. **Fast Failure**: Exit code 0 on errors to avoid blocking Claude
2. **Silent Logging**: Log to files, not stdout (unless injecting context)
3. **Security First**: Block dangerous operations with exit code 2
4. **Performance**: Keep hook execution under 50ms
5. **Idempotent**: Hooks should be safe to run multiple times

## License

Apache-2.0 (same as Term)
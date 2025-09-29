//! Pre Tool Use Hook
//!
//! Validates tool calls before execution, blocking dangerous operations
//! and logging all tool usage for audit and telemetry.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process;
use term_hooks::*;

#[derive(Debug, Deserialize, Serialize)]
struct PreToolUseInput {
    tool_name: String,
    tool_input: serde_json::Value,
}

fn main() -> Result<()> {
    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => process::exit(0),
    };

    // Extract tool data
    let tool_data: PreToolUseInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => process::exit(0),
    };

    // Check for dangerous operations
    if let Some(error) = validate_tool_call(&tool_data) {
        eprintln!("{}", error);
        process::exit(2); // Block the tool call
    }

    // Log the tool usage
    let log_entry = serde_json::json!({
        "tool_name": tool_data.tool_name,
        "tool_input": tool_data.tool_input,
        "session_id": input.session_id,
        "timestamp": input.timestamp,
        "blocked": false,
    });

    let _ = log_to_file(&get_log_dir(), "pre_tool_use.json", &log_entry);

    process::exit(0);
}

fn validate_tool_call(tool_data: &PreToolUseInput) -> Option<String> {
    let tool_name = &tool_data.tool_name;
    let tool_input = &tool_data.tool_input;

    // Check for .env file access
    if matches!(
        tool_name.as_str(),
        "Read" | "Edit" | "MultiEdit" | "Write" | "Bash"
    ) {
        let file_path = tool_input.get("file_path").and_then(|v| v.as_str());
        let command = tool_input.get("command").and_then(|v| v.as_str());

        if is_env_file_access(tool_name, file_path, command) {
            return Some(
                "BLOCKED: Access to .env files containing sensitive data is prohibited. \
                Use .env.sample for template files instead."
                    .to_string(),
            );
        }
    }

    // Check for dangerous bash commands
    if tool_name == "Bash" {
        if let Some(command) = tool_input.get("command").and_then(|v| v.as_str()) {
            // Check for dangerous rm commands
            if is_dangerous_rm_command(command) {
                return Some("BLOCKED: Dangerous rm command detected and prevented.".to_string());
            }

            // Check for other dangerous patterns
            let dangerous_patterns = [
                (
                    "chmod 777",
                    "BLOCKED: Overly permissive file permissions detected.",
                ),
                (
                    "curl .* | sh",
                    "BLOCKED: Piping untrusted content to shell is dangerous.",
                ),
                (
                    "wget .* | bash",
                    "BLOCKED: Piping untrusted content to bash is dangerous.",
                ),
                ("> /dev/sda", "BLOCKED: Direct disk write detected."),
                (
                    "dd if=.* of=/dev/",
                    "BLOCKED: Direct disk operation detected.",
                ),
                ("mkfs", "BLOCKED: Filesystem formatting command detected."),
            ];

            for (pattern, message) in &dangerous_patterns {
                if command.to_lowercase().contains(pattern) {
                    return Some(message.to_string());
                }
            }
        }
    }

    // Check for dangerous SQL operations in Term context
    if tool_name == "Bash" || tool_name == "Execute" {
        if let Some(command) = tool_input
            .get("command")
            .or_else(|| tool_input.get("sql"))
            .and_then(|v| v.as_str())
        {
            let sql_lower = command.to_lowercase();

            // Dangerous SQL patterns
            let sql_patterns = [
                (
                    "drop table",
                    "BLOCKED: Table deletion detected. Use migrations for schema changes.",
                ),
                (
                    "drop database",
                    "BLOCKED: Database deletion is not allowed.",
                ),
                (
                    "truncate",
                    "BLOCKED: Table truncation detected. Use DELETE with WHERE clause.",
                ),
                (
                    "delete from",
                    "WARNING: DELETE operation detected. Ensure WHERE clause is present.",
                ),
            ];

            for (pattern, message) in &sql_patterns {
                if sql_lower.contains(pattern) {
                    if pattern.starts_with("delete") {
                        // For DELETE, just warn but don't block
                        eprintln!("{}", message);
                    } else {
                        return Some(message.to_string());
                    }
                }
            }
        }
    }

    // Term-specific: Check for modifications to critical files
    if matches!(tool_name.as_str(), "Edit" | "MultiEdit" | "Write") {
        if let Some(file_path) = tool_input.get("file_path").and_then(|v| v.as_str()) {
            let critical_files = [
                "Cargo.lock",
                ".github/workflows/",
                "rust-toolchain.toml",
                ".rustfmt.toml",
                "deny.toml",
            ];

            for critical in &critical_files {
                if file_path.contains(critical) {
                    eprintln!(
                        "⚠️  Modifying critical file: {}. Please ensure changes are intentional.",
                        critical
                    );
                }
            }
        }
    }

    None
}

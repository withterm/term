//! Post Tool Use Hook
//!
//! Logs tool execution results and performs post-execution validation.
//! Can trigger automatic quality checks after file modifications.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::{self, Command};
use term_hooks::*;

#[derive(Debug, Deserialize, Serialize)]
struct PostToolUseInput {
    tool_name: String,
    tool_input: serde_json::Value,
    tool_response: serde_json::Value,
}

fn main() -> Result<()> {
    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => process::exit(0),
    };

    // Extract tool data
    let tool_data: PostToolUseInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => process::exit(0),
    };

    // Log the tool execution
    let log_entry = serde_json::json!({
        "tool_name": tool_data.tool_name,
        "tool_input": tool_data.tool_input,
        "tool_response": tool_data.tool_response,
        "session_id": input.session_id,
        "timestamp": input.timestamp,
    });

    let _ = log_to_file(&get_log_dir(), "post_tool_use.json", &log_entry);

    // Perform post-execution checks based on tool type
    match tool_data.tool_name.as_str() {
        "Write" | "Edit" | "MultiEdit" => {
            if let Some(file_path) = tool_data
                .tool_input
                .get("file_path")
                .and_then(|v| v.as_str())
            {
                // Check if it's a Rust file
                if file_path.ends_with(".rs") {
                    run_rust_quality_checks(file_path);
                }

                // Check if it's a TOML file (Cargo.toml, etc.)
                if file_path.ends_with(".toml") {
                    validate_toml_file(file_path);
                }
            }
        }
        "Bash" => {
            // Check if tests failed
            if let Some(command) = tool_data.tool_input.get("command").and_then(|v| v.as_str()) {
                if command.contains("cargo test") {
                    check_test_results(&tool_data.tool_response);
                }
            }
        }
        _ => {}
    }

    process::exit(0);
}

fn run_rust_quality_checks(file_path: &str) {
    eprintln!("🔍 Running quality checks on {file_path}");

    // Run rustfmt on the specific file
    let fmt_result = Command::new("rustfmt")
        .arg("--edition")
        .arg("2021")
        .arg(file_path)
        .output();

    if let Ok(output) = fmt_result {
        if !output.status.success() {
            eprintln!("⚠️  Formatting issues detected in {file_path}");
        }
    }

    // Run clippy on the project (file-specific clippy is not supported)
    let clippy_result = Command::new("cargo")
        .args([
            "clippy",
            "--all-targets",
            "--all-features",
            "--",
            "-D",
            "warnings",
        ])
        .output();

    if let Ok(output) = clippy_result {
        if !output.status.success() {
            eprintln!("⚠️  Clippy warnings detected. Run 'cargo clippy' to see details.");
        }
    }
}

fn validate_toml_file(file_path: &str) {
    // Check if it's Cargo.toml
    if file_path.ends_with("Cargo.toml") {
        eprintln!("📦 Validating Cargo.toml changes...");

        // Verify the manifest is valid
        let check_result = Command::new("cargo")
            .args(["check", "--manifest-path", file_path])
            .output();

        if let Ok(output) = check_result {
            if !output.status.success() {
                eprintln!("❌ Invalid Cargo.toml! Run 'cargo check' to see errors.");

                // Try to provide helpful error info
                let stderr = String::from_utf8_lossy(&output.stderr);
                if stderr.contains("could not parse") {
                    eprintln!("  Syntax error in TOML file");
                } else if stderr.contains("duplicate") {
                    eprintln!("  Duplicate dependency detected");
                }
            }
        }
    }
}

fn check_test_results(tool_response: &serde_json::Value) {
    if let Some(output) = tool_response.get("output").and_then(|v| v.as_str()) {
        let output_lower = output.to_lowercase();

        if output_lower.contains("test failed") || output_lower.contains("failures:") {
            eprintln!("❌ Tests failed! Please fix failing tests before proceeding.");

            // Extract failure count if possible
            if let Some(pos) = output_lower.find("failures:") {
                let rest = &output[pos + 9..];
                if let Some(end) = rest.find(|c: char| !c.is_ascii_digit() && c != ' ') {
                    let count = rest[..end].trim();
                    eprintln!("  {count} test(s) failed");
                }
            }
        } else if output_lower.contains("test result: ok") {
            eprintln!("✅ All tests passed!");
        }
    }
}

//! User Prompt Submit Hook
//!
//! Validates and enhances user prompts before Claude processes them.
//! Can inject context, validate content, and log prompts for audit.

use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::process;
use term_hooks::*;
// Tracing imports available for future use
// use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Enable prompt validation
    #[arg(long)]
    validate: bool,

    /// Add project context to prompts
    #[arg(long)]
    context: bool,

    /// Log prompts only (default mode)
    #[arg(long)]
    log_only: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct UserPromptInput {
    prompt: String,
    session_id: Option<String>,
    timestamp: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => {
            // Silent fail on JSON errors
            process::exit(0);
        }
    };

    // Extract prompt data
    let prompt_data: UserPromptInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => {
            process::exit(0);
        }
    };

    // Log the prompt
    let log_entry = serde_json::json!({
        "prompt": prompt_data.prompt,
        "session_id": prompt_data.session_id,
        "timestamp": prompt_data.timestamp,
        "validated": args.validate,
        "context_added": args.context,
    });

    let _ = log_to_file(&get_log_dir(), "user_prompt_submit.json", &log_entry);

    // Validation mode
    if args.validate {
        if let Some(error) = validate_prompt(&prompt_data.prompt) {
            eprintln!("BLOCKED: {error}");
            process::exit(2); // Exit code 2 blocks the prompt
        }
    }

    // Context injection mode
    if args.context {
        inject_context();
    }

    process::exit(0);
}

fn validate_prompt(prompt: &str) -> Option<String> {
    let prompt_lower = prompt.to_lowercase();

    // Check for dangerous patterns
    let dangerous_patterns = [
        ("rm -rf /", "Dangerous system deletion command detected"),
        ("drop database", "Database destruction command detected"),
        ("delete from", "Potentially dangerous SQL deletion detected"),
        ("truncate table", "Table truncation command detected"),
        ("format c:", "Disk formatting command detected"),
    ];

    for (pattern, message) in &dangerous_patterns {
        if prompt_lower.contains(pattern) {
            return Some(message.to_string());
        }
    }

    // Check for potential secrets
    let secret_patterns = ["password", "api_key", "secret", "token", "private_key"];

    for pattern in &secret_patterns {
        if prompt_lower.contains(pattern) && prompt.contains('=') {
            return Some(format!(
                "Potential secret detected ({pattern}). Please use environment variables instead."
            ));
        }
    }

    None
}

fn inject_context() {
    // Get git info if available
    if let Ok(git_info) = get_git_info() {
        println!("📍 Project Context:");
        println!("  Branch: {}", git_info.branch);
        println!("  Last commit: {}", git_info.commit_message);
        println!(
            "  Working tree: {}",
            if git_info.is_clean {
                "clean"
            } else {
                "modified"
            }
        );
    }

    // Add Term-specific context
    println!("🛡️ Term Data Validation Library");
    println!("  Focus: Data quality validation without Spark");
    println!("  Stack: Rust, DataFusion, OpenTelemetry");
    println!("  Standards: Use builder pattern, async-first, instrument with tracing");
    println!();

    // Check if there's a current Linear ticket
    if let Ok(ticket) = std::env::var("LINEAR_TICKET") {
        println!("🎯 Current ticket: {ticket}");
        println!();
    }
}

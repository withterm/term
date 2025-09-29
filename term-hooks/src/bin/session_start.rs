//! Session Start Hook
//!
//! Loads development context when Claude Code sessions start or resume.
//! Provides git status, recent issues, and project context.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::{self, Command};
use term_hooks::*;

#[derive(Debug, Deserialize, Serialize)]
struct SessionStartInput {
    source: String, // "startup", "resume", or "clear"
    session_id: Option<String>,
}

fn main() -> Result<()> {
    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => process::exit(0),
    };

    // Extract session data
    let session_data: SessionStartInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => process::exit(0),
    };

    // Log the session start
    let log_entry = serde_json::json!({
        "source": session_data.source,
        "session_id": session_data.session_id,
        "timestamp": input.timestamp,
    });

    let _ = log_to_file(&get_log_dir(), "session_start.json", &log_entry);

    // Load context based on source
    match session_data.source.as_str() {
        "startup" | "resume" => {
            load_development_context();
        }
        "clear" => {
            println!("🧹 Session cleared. Starting fresh.");
        }
        _ => {}
    }

    process::exit(0);
}

fn load_development_context() {
    println!("🚀 Loading Term development context...\n");

    // Show git information
    if let Ok(git_info) = get_git_info() {
        println!("📍 Git Status:");
        println!("  Branch: {}", git_info.branch);
        println!("  Last commit: {}", git_info.commit_message);
        println!(
            "  Working tree: {}",
            if git_info.is_clean {
                "✅ clean"
            } else {
                "🔧 modified"
            }
        );

        // Show recent commits
        if let Ok(output) = Command::new("git")
            .args(["log", "--oneline", "-5"])
            .output()
        {
            if output.status.success() {
                println!("\n📜 Recent commits:");
                let commits = String::from_utf8_lossy(&output.stdout);
                for line in commits.lines() {
                    println!("  {line}");
                }
            }
        }
    }

    // Check for Linear ticket in environment
    if let Ok(ticket) = std::env::var("LINEAR_TICKET") {
        println!("\n🎯 Current Linear ticket: {ticket}");
        println!("  Use 'mcp__linear-server__get_issue' to fetch details");
    }

    // Show Term-specific context
    println!("\n🛡️ Term Project Guidelines:");
    println!("  • Use builder pattern for public APIs");
    println!("  • All async functions need #[instrument] for tracing");
    println!("  • Run 'make pre-commit' before committing");
    println!("  • Use DataFusion for queries, not raw SQL");
    println!("  • Error type: Result<T> = std::result::Result<T, TermError>");

    // Check for outstanding TODOs in current branch
    if let Ok(output) = Command::new("git")
        .args(["grep", "-n", "TODO", "--", "*.rs"])
        .output()
    {
        if output.status.success() && !output.stdout.is_empty() {
            let todos = String::from_utf8_lossy(&output.stdout);
            let todo_count = todos.lines().count();
            println!("\n📝 Found {todo_count} TODO(s) in Rust files:");
            for (i, line) in todos.lines().take(3).enumerate() {
                println!("  {line}");
                if i == 2 && todo_count > 3 {
                    println!("  ... and {} more", todo_count - 3);
                }
            }
        }
    }

    // Check if tests are passing
    println!("\n🧪 Test Status:");
    if let Ok(output) = Command::new("cargo")
        .args(["test", "--", "--nocapture", "--test-threads=1", "-q"])
        .output()
    {
        if output.status.success() {
            println!("  ✅ Tests passing");
        } else {
            println!("  ❌ Tests failing - run 'cargo test' to see details");
        }
    }

    // Check for uncommitted changes
    if let Ok(output) = Command::new("git").args(["status", "--porcelain"]).output() {
        if output.status.success() && !output.stdout.is_empty() {
            let changes = String::from_utf8_lossy(&output.stdout);
            let change_count = changes.lines().count();
            println!("\n⚠️  {change_count} uncommitted change(s) detected");
        }
    }

    println!("\n💡 Tip: Use 'make help' to see available commands");
    println!("Ready to assist with Term development! 🚀\n");
}

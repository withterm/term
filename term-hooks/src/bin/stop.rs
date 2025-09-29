//! Stop Hook
//!
//! Handles Claude Code stop events and can generate completion summaries.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::process::{self, Command};
use term_hooks::*;

#[derive(Debug, Deserialize, Serialize)]
struct StopInput {
    stop_hook_active: bool,
}

fn main() -> Result<()> {
    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => process::exit(0),
    };

    // Extract stop data
    let stop_data: StopInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => process::exit(0),
    };

    // Log the stop event
    let log_entry = serde_json::json!({
        "stop_hook_active": stop_data.stop_hook_active,
        "session_id": input.session_id,
        "timestamp": input.timestamp,
    });

    let _ = log_to_file(&get_log_dir(), "stop.json", &log_entry);

    // Generate completion summary
    if !stop_data.stop_hook_active {
        generate_completion_summary();
    }

    process::exit(0);
}

fn generate_completion_summary() {
    println!("\n✨ Task Complete!");

    // Check if tests are passing
    if let Ok(output) = Command::new("cargo").args(["test", "--", "-q"]).output() {
        if output.status.success() {
            println!("✅ All tests passing");
        } else {
            println!("⚠️  Some tests failing - review test output");
        }
    }

    // Check for uncommitted changes
    if let Ok(output) = Command::new("git").args(["status", "--porcelain"]).output() {
        if output.status.success() && !output.stdout.is_empty() {
            let changes = String::from_utf8_lossy(&output.stdout);
            let files_changed = changes.lines().count();
            println!("📝 {files_changed} file(s) modified");

            // Show quick diff stats
            if let Ok(diff_output) = Command::new("git").args(["diff", "--stat"]).output() {
                if diff_output.status.success() {
                    let stats = String::from_utf8_lossy(&diff_output.stdout);
                    if let Some(last_line) = stats.lines().last() {
                        println!("   {last_line}");
                    }
                }
            }
        }
    }

    println!("\nNext steps:");
    println!("  • Review changes with 'git diff'");
    println!("  • Run 'make pre-commit' before committing");
    println!("  • Use 'gt create' to create a stacked PR");
}

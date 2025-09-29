//! Pre Compact Hook
//!
//! Backs up conversation transcripts before Claude Code compaction.

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::process;
use term_hooks::*;

#[derive(Debug, Deserialize, Serialize)]
struct PreCompactInput {
    trigger: String, // "manual" or "auto"
    custom_instructions: Option<String>,
}

fn main() -> Result<()> {
    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => process::exit(0),
    };

    // Extract compact data
    let compact_data: PreCompactInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => process::exit(0),
    };

    // Create backup directory
    let backup_dir = get_project_dir().join(".claude").join("backups");
    fs::create_dir_all(&backup_dir)?;

    // Create timestamp-based backup name
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let backup_name = format!("transcript_{timestamp}.json");
    let backup_path = backup_dir.join(&backup_name);

    // Try to backup current transcript if it exists
    let transcript_path = get_project_dir().join(".claude").join("claude_chat.jsonl");
    if transcript_path.exists() {
        match fs::copy(&transcript_path, &backup_path) {
            Ok(bytes) => {
                println!("💾 Transcript backed up: {backup_name} ({bytes} bytes)");
            }
            Err(e) => {
                eprintln!("⚠️  Failed to backup transcript: {e}");
            }
        }
    }

    // Log the compaction event
    let log_entry = serde_json::json!({
        "trigger": compact_data.trigger,
        "custom_instructions": compact_data.custom_instructions,
        "backup_created": backup_path.exists(),
        "backup_path": backup_path.to_string_lossy(),
        "session_id": input.session_id,
        "timestamp": input.timestamp,
    });

    let _ = log_to_file(&get_log_dir(), "pre_compact.json", &log_entry);

    // Provide feedback based on trigger type
    match compact_data.trigger.as_str() {
        "manual" => {
            println!("🗜️ Manual compaction requested");
            if let Some(instructions) = &compact_data.custom_instructions {
                println!("📋 Custom instructions: {instructions}");
            }
            println!("💡 Tip: Previous context backed up to .claude/backups/");
        }
        "auto" => {
            println!("🤖 Automatic compaction triggered");
            println!("   Context limit approaching, optimizing conversation");
        }
        _ => {}
    }

    // Clean up old backups (keep only last 10)
    cleanup_old_backups(&backup_dir);

    process::exit(0);
}

fn cleanup_old_backups(backup_dir: &Path) {
    if let Ok(entries) = fs::read_dir(backup_dir) {
        let mut backups: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with("transcript_"))
            .collect();

        // Sort by modification time
        backups.sort_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
        });

        // Remove old backups if more than 10
        if backups.len() > 10 {
            for entry in backups.iter().take(backups.len() - 10) {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

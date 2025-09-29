//! Notification Hook
//!
//! Handles Claude Code notifications and can trigger alerts.

use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::process;
use term_hooks::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Enable desktop notifications
    #[arg(long)]
    notify: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct NotificationInput {
    message: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Read input from stdin
    let input = match read_input() {
        Ok(input) => input,
        Err(_) => process::exit(0),
    };

    // Extract notification data
    let notif_data: NotificationInput = match serde_json::from_value(input.data.clone()) {
        Ok(data) => data,
        Err(_) => process::exit(0),
    };

    // Log the notification
    let log_entry = serde_json::json!({
        "message": notif_data.message,
        "session_id": input.session_id,
        "timestamp": input.timestamp,
    });

    let _ = log_to_file(&get_log_dir(), "notification.json", &log_entry);

    // Send desktop notification if enabled
    if args.notify {
        send_notification(&notif_data.message);
    }

    process::exit(0);
}

fn send_notification(message: &str) {
    // Use system notification command based on OS
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("osascript")
            .arg("-e")
            .arg(format!(
                "display notification \"{message}\" with title \"Claude Code - Term\""
            ))
            .output();
    }

    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("notify-send")
            .arg("Claude Code - Term")
            .arg(message)
            .output();
    }

    // Windows notifications would require additional dependencies
    #[cfg(target_os = "windows")]
    {
        eprintln!("📢 {message}");
    }
}

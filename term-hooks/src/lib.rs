//! Term Hooks - Claude Code hooks for the Term data validation library
//!
//! This library provides common functionality for all Term Claude Code hooks.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
// Tracing imports available for future use
// use tracing::{debug, error, info};

/// Common hook input structure from Claude Code
#[derive(Debug, Deserialize, Serialize)]
pub struct HookInput {
    pub session_id: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub data: serde_json::Value,
}

/// Common hook response structure
#[derive(Debug, Serialize, Default)]
pub struct HookResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continue_execution: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suppress_output: Option<bool>,
}

/// Read JSON input from stdin
pub fn read_input() -> Result<HookInput> {
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;
    let input: HookInput = serde_json::from_str(&buffer)?;
    Ok(input)
}

/// Write response to stdout
pub fn write_response(response: &HookResponse) -> Result<()> {
    if response.continue_execution.is_some()
        || response.decision.is_some()
        || response.reason.is_some()
        || response.stop_reason.is_some()
        || response.suppress_output.is_some()
    {
        println!("{}", serde_json::to_string(response)?);
    }
    Ok(())
}

/// Log data to a JSON file
pub fn log_to_file<T: Serialize>(log_dir: &Path, filename: &str, data: &T) -> Result<()> {
    // Ensure log directory exists
    fs::create_dir_all(log_dir)?;

    let log_path = log_dir.join(filename);

    // Read existing data or create empty array
    let mut log_data: Vec<serde_json::Value> = if log_path.exists() {
        let content = fs::read_to_string(&log_path)?;
        serde_json::from_str(&content).unwrap_or_default()
    } else {
        Vec::new()
    };

    // Append new data
    log_data.push(serde_json::to_value(data)?);

    // Write back to file
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&log_path)?;

    file.write_all(serde_json::to_string_pretty(&log_data)?.as_bytes())?;

    Ok(())
}

/// Get the project directory
pub fn get_project_dir() -> PathBuf {
    std::env::var("CLAUDE_PROJECT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

/// Get the log directory
pub fn get_log_dir() -> PathBuf {
    get_project_dir().join("logs")
}

/// Initialize tracing
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(io::stderr)
        .json()
        .init();
}

/// Security validation for dangerous rm commands
pub fn is_dangerous_rm_command(command: &str) -> bool {
    use regex::Regex;

    let normalized = command.to_lowercase();
    let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");

    // Patterns for dangerous rm commands
    let patterns = [
        r"\brm\s+.*-[a-z]*r[a-z]*f",     // rm -rf variations
        r"\brm\s+.*-[a-z]*f[a-z]*r",     // rm -fr variations
        r"\brm\s+--recursive\s+--force", // rm --recursive --force
        r"\brm\s+--force\s+--recursive", // rm --force --recursive
        r"\brm\s+-r\s+.*-f",             // rm -r ... -f
        r"\brm\s+-f\s+.*-r",             // rm -f ... -r
    ];

    for pattern in &patterns {
        if let Ok(re) = Regex::new(pattern) {
            if re.is_match(&normalized) {
                return true;
            }
        }
    }

    // Check for dangerous paths with recursive flag
    if normalized.contains("rm ") && normalized.contains("-r") {
        let dangerous_paths = ["/", "/*", "~", "~/", "$HOME", "..", "*", ".", ". "];
        for path in &dangerous_paths {
            if normalized.contains(path) {
                return true;
            }
        }
    }

    false
}

/// Check if accessing .env files
pub fn is_env_file_access(tool_name: &str, file_path: Option<&str>, command: Option<&str>) -> bool {
    // Check file-based tools
    if let Some(path) = file_path {
        if path.contains(".env") && !path.ends_with(".env.sample") {
            return true;
        }
    }

    // Check bash commands
    if tool_name == "Bash" {
        if let Some(cmd) = command {
            use regex::Regex;

            let patterns = [
                r"\b\.env\b(?!\.sample)",            // .env but not .env.sample
                r"cat\s+.*\.env\b(?!\.sample)",      // cat .env
                r"echo\s+.*>\s*\.env\b(?!\.sample)", // echo > .env
                r"touch\s+.*\.env\b(?!\.sample)",    // touch .env
                r"cp\s+.*\.env\b(?!\.sample)",       // cp .env
                r"mv\s+.*\.env\b(?!\.sample)",       // mv .env
            ];

            for pattern in &patterns {
                if let Ok(re) = Regex::new(pattern) {
                    if re.is_match(cmd) {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Get git information for the current repository
pub fn get_git_info() -> Result<GitInfo> {
    use git2::Repository;

    let repo = Repository::discover(get_project_dir())?;
    let head = repo.head()?;

    let branch = head.shorthand().unwrap_or("unknown").to_string();

    // Get current commit
    let commit = head.peel_to_commit()?;
    let commit_id = commit.id().to_string();
    let commit_message = commit.summary().unwrap_or("").to_string();

    // Check if working directory is clean
    let statuses = repo.statuses(None)?;
    let is_clean = statuses.is_empty();

    Ok(GitInfo {
        branch,
        commit_id,
        commit_message,
        is_clean,
    })
}

#[derive(Debug, Serialize)]
pub struct GitInfo {
    pub branch: String,
    pub commit_id: String,
    pub commit_message: String,
    pub is_clean: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dangerous_rm_detection() {
        assert!(is_dangerous_rm_command("rm -rf /"));
        assert!(is_dangerous_rm_command("rm -fr /"));
        assert!(is_dangerous_rm_command("rm -r -f /"));
        assert!(is_dangerous_rm_command("rm --recursive --force /"));
        assert!(is_dangerous_rm_command("rm -rf ~"));
        assert!(is_dangerous_rm_command("rm -rf ."));
        assert!(is_dangerous_rm_command("rm -rf .."));

        assert!(!is_dangerous_rm_command("rm file.txt"));
        assert!(!is_dangerous_rm_command("rm -f file.txt"));
        assert!(!is_dangerous_rm_command("rm -r directory"));
    }

    #[test]
    fn test_env_file_detection() {
        assert!(is_env_file_access("Read", Some(".env"), None));
        assert!(is_env_file_access("Edit", Some("/path/.env"), None));
        assert!(!is_env_file_access("Read", Some(".env.sample"), None));

        assert!(is_env_file_access("Bash", None, Some("cat .env")));
        assert!(is_env_file_access("Bash", None, Some("echo SECRET > .env")));
        assert!(!is_env_file_access("Bash", None, Some("cat .env.sample")));
    }
}

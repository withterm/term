//! Webhook-based alerting for validation failures.
//!
//! This module provides webhook alerting capabilities that can be triggered
//! when validation checks fail. Supports custom headers, HMAC signing, and
//! severity-based filtering.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ring::hmac;
use serde::{Deserialize, Serialize};

use super::error::{CloudError, CloudResult};
use super::types::{CloudValidationIssue, CloudValidationResult};
use crate::security::SecureString;

/// Alert severity levels for filtering webhook notifications.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    #[default]
    Warning,
    Critical,
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Info => write!(f, "info"),
            Self::Warning => write!(f, "warning"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Configuration for webhook alerting.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    url: String,
    headers: HashMap<String, String>,
    include_details: bool,
    min_severity: AlertSeverity,
    secret: Option<SecureString>,
    timeout: Duration,
}

impl WebhookConfig {
    /// Create a new WebhookConfig with the given URL.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: HashMap::new(),
            include_details: false,
            min_severity: AlertSeverity::default(),
            secret: None,
            timeout: Duration::from_secs(10),
        }
    }

    /// Add a custom header to be sent with webhook requests.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set whether to include full validation details in the alert payload.
    pub fn with_details(mut self, include: bool) -> Self {
        self.include_details = include;
        self
    }

    /// Set the minimum severity level required to trigger an alert.
    pub fn with_min_severity(mut self, severity: AlertSeverity) -> Self {
        self.min_severity = severity;
        self
    }

    /// Set a secret for HMAC-SHA256 signing of payloads.
    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        self.secret = Some(SecureString::new(secret.into()));
        self
    }

    /// Set the request timeout duration.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Validate the webhook configuration.
    pub fn validate(&self) -> CloudResult<()> {
        if self.url.is_empty() {
            return Err(CloudError::Configuration {
                message: "Webhook URL cannot be empty".to_string(),
            });
        }

        let url_lower = self.url.to_lowercase();
        if !url_lower.starts_with("http://") && !url_lower.starts_with("https://") {
            return Err(CloudError::Configuration {
                message: "Webhook URL must start with http:// or https://".to_string(),
            });
        }

        if reqwest::Url::parse(&self.url).is_err() {
            return Err(CloudError::Configuration {
                message: format!("Invalid webhook URL: {}", self.url),
            });
        }

        Ok(())
    }

    /// Get the webhook URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Get the custom headers.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Check if details should be included.
    pub fn include_details(&self) -> bool {
        self.include_details
    }

    /// Get the minimum severity level.
    pub fn min_severity(&self) -> AlertSeverity {
        self.min_severity
    }

    /// Get the signing secret.
    pub fn secret(&self) -> Option<&SecureString> {
        self.secret.as_ref()
    }

    /// Get the timeout duration.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Summary information about the validation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertSummary {
    pub total_checks: usize,
    pub passed: usize,
    pub failed: usize,
    pub status: String,
}

/// Details about a specific validation failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDetail {
    pub check: String,
    pub constraint: String,
    pub level: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric: Option<f64>,
}

impl From<&CloudValidationIssue> for AlertDetail {
    fn from(issue: &CloudValidationIssue) -> Self {
        Self {
            check: issue.check_name.clone(),
            constraint: issue.constraint_name.clone(),
            level: issue.level.clone(),
            message: issue.message.clone(),
            metric: issue.metric,
        }
    }
}

/// Payload sent to the webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertPayload {
    pub title: String,
    pub severity: AlertSeverity,
    pub dataset: String,
    pub environment: String,
    pub summary: AlertSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<AlertDetail>>,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dashboard_url: Option<String>,
}

impl AlertPayload {
    /// Create an alert payload from a validation result.
    pub fn from_validation_result(
        result: &CloudValidationResult,
        dataset: impl Into<String>,
        environment: impl Into<String>,
    ) -> Self {
        let severity = Self::determine_severity(result);
        let title = Self::generate_title(result, &severity);

        let summary = AlertSummary {
            total_checks: result.total_checks,
            passed: result.passed_checks,
            failed: result.failed_checks,
            status: result.status.clone(),
        };

        let details: Option<Vec<AlertDetail>> = if result.issues.is_empty() {
            None
        } else {
            Some(result.issues.iter().map(AlertDetail::from).collect())
        };

        Self {
            title,
            severity,
            dataset: dataset.into(),
            environment: environment.into(),
            summary,
            details,
            timestamp: Utc::now(),
            dashboard_url: None,
        }
    }

    /// Set the dashboard URL for the alert.
    pub fn with_dashboard_url(mut self, url: impl Into<String>) -> Self {
        self.dashboard_url = Some(url.into());
        self
    }

    fn determine_severity(result: &CloudValidationResult) -> AlertSeverity {
        if result.failed_checks == 0 {
            return AlertSeverity::Info;
        }

        let failure_rate = result.failed_checks as f64 / result.total_checks.max(1) as f64;

        if failure_rate >= 0.5 || result.status == "error" {
            AlertSeverity::Critical
        } else if result.failed_checks > 0 {
            AlertSeverity::Warning
        } else {
            AlertSeverity::Info
        }
    }

    fn generate_title(result: &CloudValidationResult, severity: &AlertSeverity) -> String {
        match severity {
            AlertSeverity::Info => "Validation Passed".to_string(),
            AlertSeverity::Warning => format!(
                "Validation Warning: {} of {} checks failed",
                result.failed_checks, result.total_checks
            ),
            AlertSeverity::Critical => format!(
                "Validation Critical: {} of {} checks failed",
                result.failed_checks, result.total_checks
            ),
        }
    }
}

/// Client for sending webhook alerts.
pub struct WebhookClient {
    client: reqwest::Client,
    config: WebhookConfig,
}

impl WebhookClient {
    /// Create a new WebhookClient with the given configuration.
    pub fn new(config: WebhookConfig) -> CloudResult<Self> {
        config.validate()?;

        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| CloudError::Configuration {
                message: format!("Failed to build HTTP client: {e}"),
            })?;

        Ok(Self { client, config })
    }

    /// Send an alert to the configured webhook endpoint.
    pub async fn send(&self, payload: &AlertPayload) -> CloudResult<()> {
        if payload.severity < self.config.min_severity {
            tracing::debug!(
                severity = %payload.severity,
                min_severity = %self.config.min_severity,
                "Alert severity below threshold, skipping"
            );
            return Ok(());
        }

        let mut payload_to_send = payload.clone();
        if !self.config.include_details {
            payload_to_send.details = None;
        }

        let body =
            serde_json::to_string(&payload_to_send).map_err(|e| CloudError::Serialization {
                message: e.to_string(),
            })?;

        let mut request = self
            .client
            .post(&self.config.url)
            .header("Content-Type", "application/json");

        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        if let Some(secret) = &self.config.secret {
            let signature = Self::sign_payload(&body, secret.expose());
            request = request.header("X-Signature-256", format!("sha256={signature}"));
        }

        let response = request
            .body(body)
            .send()
            .await
            .map_err(|e| CloudError::Network {
                message: e.to_string(),
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(CloudError::ServerError { status, message });
        }

        tracing::info!(
            dataset = %payload.dataset,
            severity = %payload.severity,
            "Alert sent successfully"
        );

        Ok(())
    }

    /// Sign a payload using HMAC-SHA256.
    pub fn sign_payload(body: &str, secret: &str) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let signature = hmac::sign(&key, body.as_bytes());
        hex::encode(signature.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alert_severity_ordering() {
        assert!(AlertSeverity::Info < AlertSeverity::Warning);
        assert!(AlertSeverity::Warning < AlertSeverity::Critical);
        assert!(AlertSeverity::Info < AlertSeverity::Critical);
    }

    #[test]
    fn test_alert_severity_default() {
        assert_eq!(AlertSeverity::default(), AlertSeverity::Warning);
    }

    #[test]
    fn test_alert_severity_display() {
        assert_eq!(AlertSeverity::Info.to_string(), "info");
        assert_eq!(AlertSeverity::Warning.to_string(), "warning");
        assert_eq!(AlertSeverity::Critical.to_string(), "critical");
    }

    #[test]
    fn test_webhook_config_new() {
        let config = WebhookConfig::new("https://example.com/webhook");
        assert_eq!(config.url(), "https://example.com/webhook");
        assert!(config.headers().is_empty());
        assert!(!config.include_details());
        assert_eq!(config.min_severity(), AlertSeverity::Warning);
        assert!(config.secret().is_none());
        assert_eq!(config.timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_webhook_config_builder() {
        let config = WebhookConfig::new("https://example.com/webhook")
            .with_header("Authorization", "Bearer token123")
            .with_header("X-Custom", "value")
            .with_details(true)
            .with_min_severity(AlertSeverity::Critical)
            .with_secret("my-secret");

        assert_eq!(config.url(), "https://example.com/webhook");
        assert_eq!(
            config.headers().get("Authorization"),
            Some(&"Bearer token123".to_string())
        );
        assert_eq!(config.headers().get("X-Custom"), Some(&"value".to_string()));
        assert!(config.include_details());
        assert_eq!(config.min_severity(), AlertSeverity::Critical);
        assert_eq!(config.secret().map(|s| s.expose()), Some("my-secret"));
        assert_eq!(config.timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_webhook_config_with_timeout() {
        let config =
            WebhookConfig::new("https://example.com/webhook").with_timeout(Duration::from_secs(30));
        assert_eq!(config.timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_webhook_config_validate_success() {
        let config = WebhookConfig::new("https://example.com/webhook");
        assert!(config.validate().is_ok());

        let config = WebhookConfig::new("http://localhost:8080/hook");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_webhook_config_validate_empty_url() {
        let config = WebhookConfig::new("");
        let result = config.validate();
        assert!(result.is_err());
        match result.unwrap_err() {
            CloudError::Configuration { message } => {
                assert!(message.contains("cannot be empty"));
            }
            _ => panic!("Expected Configuration error"),
        }
    }

    #[test]
    fn test_webhook_config_validate_invalid_scheme() {
        let config = WebhookConfig::new("ftp://example.com/webhook");
        let result = config.validate();
        assert!(result.is_err());
        match result.unwrap_err() {
            CloudError::Configuration { message } => {
                assert!(message.contains("http://") || message.contains("https://"));
            }
            _ => panic!("Expected Configuration error"),
        }
    }

    #[test]
    fn test_webhook_config_validate_invalid_url() {
        let config = WebhookConfig::new("https://");
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_alert_payload_from_validation_result_success() {
        let result = CloudValidationResult {
            status: "success".to_string(),
            total_checks: 10,
            passed_checks: 10,
            failed_checks: 0,
            issues: vec![],
        };

        let payload = AlertPayload::from_validation_result(&result, "orders", "production");

        assert_eq!(payload.severity, AlertSeverity::Info);
        assert_eq!(payload.dataset, "orders");
        assert_eq!(payload.environment, "production");
        assert_eq!(payload.summary.total_checks, 10);
        assert_eq!(payload.summary.passed, 10);
        assert_eq!(payload.summary.failed, 0);
        assert!(payload.details.is_none());
    }

    #[test]
    fn test_alert_payload_from_validation_result_warning() {
        let result = CloudValidationResult {
            status: "warning".to_string(),
            total_checks: 10,
            passed_checks: 8,
            failed_checks: 2,
            issues: vec![CloudValidationIssue {
                check_name: "DataQuality".to_string(),
                constraint_name: "Completeness".to_string(),
                level: "warning".to_string(),
                message: "Column 'email' has nulls".to_string(),
                metric: Some(0.95),
            }],
        };

        let payload = AlertPayload::from_validation_result(&result, "users", "staging");

        assert_eq!(payload.severity, AlertSeverity::Warning);
        assert!(payload.title.contains("Warning"));
        assert!(payload.details.is_some());
        let details = payload.details.unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].check, "DataQuality");
        assert_eq!(details[0].metric, Some(0.95));
    }

    #[test]
    fn test_alert_payload_from_validation_result_critical() {
        let result = CloudValidationResult {
            status: "error".to_string(),
            total_checks: 10,
            passed_checks: 3,
            failed_checks: 7,
            issues: vec![],
        };

        let payload = AlertPayload::from_validation_result(&result, "orders", "production");

        assert_eq!(payload.severity, AlertSeverity::Critical);
        assert!(payload.title.contains("Critical"));
    }

    #[test]
    fn test_alert_payload_with_dashboard_url() {
        let result = CloudValidationResult {
            status: "success".to_string(),
            total_checks: 1,
            passed_checks: 1,
            failed_checks: 0,
            issues: vec![],
        };

        let payload = AlertPayload::from_validation_result(&result, "test", "dev")
            .with_dashboard_url("https://dashboard.example.com/run/123");

        assert_eq!(
            payload.dashboard_url,
            Some("https://dashboard.example.com/run/123".to_string())
        );
    }

    #[test]
    fn test_alert_detail_from_cloud_validation_issue() {
        let issue = CloudValidationIssue {
            check_name: "MyCheck".to_string(),
            constraint_name: "MyConstraint".to_string(),
            level: "error".to_string(),
            message: "Something went wrong".to_string(),
            metric: Some(0.5),
        };

        let detail = AlertDetail::from(&issue);

        assert_eq!(detail.check, "MyCheck");
        assert_eq!(detail.constraint, "MyConstraint");
        assert_eq!(detail.level, "error");
        assert_eq!(detail.message, "Something went wrong");
        assert_eq!(detail.metric, Some(0.5));
    }

    #[test]
    fn test_sign_payload() {
        let body = r#"{"title":"Test Alert"}"#;
        let secret = "test-secret";

        let signature1 = WebhookClient::sign_payload(body, secret);
        let signature2 = WebhookClient::sign_payload(body, secret);

        assert_eq!(signature1, signature2);
        assert!(!signature1.is_empty());
        assert_eq!(signature1.len(), 64);
    }

    #[test]
    fn test_sign_payload_different_secrets() {
        let body = r#"{"title":"Test Alert"}"#;

        let signature1 = WebhookClient::sign_payload(body, "secret1");
        let signature2 = WebhookClient::sign_payload(body, "secret2");

        assert_ne!(signature1, signature2);
    }

    #[test]
    fn test_webhook_client_new_valid_config() {
        let config = WebhookConfig::new("https://example.com/webhook");
        let client = WebhookClient::new(config);
        assert!(client.is_ok());
    }

    #[test]
    fn test_webhook_client_new_invalid_config() {
        let config = WebhookConfig::new("");
        let client = WebhookClient::new(config);
        assert!(client.is_err());
    }

    #[test]
    fn test_alert_severity_serialization() {
        let severity = AlertSeverity::Warning;
        let json = serde_json::to_string(&severity).unwrap();
        assert_eq!(json, "\"warning\"");

        let deserialized: AlertSeverity = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, AlertSeverity::Warning);
    }

    #[test]
    fn test_alert_payload_serialization() {
        let result = CloudValidationResult {
            status: "success".to_string(),
            total_checks: 5,
            passed_checks: 5,
            failed_checks: 0,
            issues: vec![],
        };

        let payload = AlertPayload::from_validation_result(&result, "test_dataset", "test_env");
        let json = serde_json::to_string(&payload).unwrap();

        assert!(json.contains("\"title\""));
        assert!(json.contains("\"severity\":\"info\""));
        assert!(json.contains("\"dataset\":\"test_dataset\""));
        assert!(json.contains("\"environment\":\"test_env\""));
        assert!(json.contains("\"summary\""));
        assert!(!json.contains("\"details\"")); // None should be skipped
    }
}

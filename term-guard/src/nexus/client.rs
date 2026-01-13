use std::sync::Arc;

use reqwest::Client;
use ring::hmac;
use serde::{Deserialize, Serialize};

use crate::nexus::{NexusConfig, NexusError, NexusMetric, NexusResult, NexusResultKey};

/// HTTP client for Term Nexus API.
#[derive(Clone)]
pub struct NexusClient {
    config: Arc<NexusConfig>,
    client: Client,
    signing_key: hmac::Key,
}

/// Response from the metrics ingestion endpoint.
#[derive(Debug, Deserialize)]
pub struct IngestResponse {
    pub accepted: usize,
    pub rejected: usize,
    #[serde(default)]
    pub errors: Vec<String>,
}

/// Response from the health check endpoint.
#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

/// Query parameters for listing metrics.
#[derive(Debug, Default, Serialize)]
pub struct MetricsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(flatten)]
    pub tags: std::collections::HashMap<String, String>,
}

/// Paginated response from metrics query.
#[derive(Debug, Deserialize)]
pub struct MetricsResponse {
    pub results: Vec<NexusMetric>,
    pub pagination: Pagination,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

impl NexusClient {
    /// Create a new client with the given configuration.
    pub fn new(config: NexusConfig) -> NexusResult<Self> {
        let client = Client::builder()
            .timeout(config.timeout())
            .build()
            .map_err(|e| NexusError::Configuration {
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        let signing_key = hmac::Key::new(hmac::HMAC_SHA256, config.api_key().expose().as_bytes());

        Ok(Self {
            config: Arc::new(config),
            client,
            signing_key,
        })
    }

    /// Check if the Term Nexus API is reachable.
    pub async fn health_check(&self) -> NexusResult<HealthResponse> {
        let url = format!("{}/v1/health", self.config.endpoint());

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| NexusError::Network {
                message: e.to_string(),
            })?;

        self.handle_response(response).await
    }

    /// Send metrics to Term Nexus.
    pub async fn ingest(&self, metrics: &[NexusMetric]) -> NexusResult<IngestResponse> {
        let url = format!("{}/v1/metrics", self.config.endpoint());
        let body = serde_json::to_vec(metrics).map_err(|e| NexusError::Serialization {
            message: e.to_string(),
        })?;

        let signature = self.sign_request(&body);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-Term-Signature", signature)
            .header("X-Term-Api-Key", self.config.api_key().expose())
            .body(body)
            .send()
            .await
            .map_err(|e| NexusError::Network {
                message: e.to_string(),
            })?;

        self.handle_response(response).await
    }

    /// Query metrics from Term Nexus.
    pub async fn query(&self, query: MetricsQuery) -> NexusResult<MetricsResponse> {
        let url = format!("{}/v1/metrics", self.config.endpoint());

        let response = self
            .client
            .get(&url)
            .header("X-Term-Api-Key", self.config.api_key().expose())
            .query(&query)
            .send()
            .await
            .map_err(|e| NexusError::Network {
                message: e.to_string(),
            })?;

        self.handle_response(response).await
    }

    /// Delete metrics by key.
    pub async fn delete(&self, key: &NexusResultKey) -> NexusResult<()> {
        let url = format!("{}/v1/metrics/{}", self.config.endpoint(), key.dataset_date);

        let response = self
            .client
            .delete(&url)
            .header("X-Term-Api-Key", self.config.api_key().expose())
            .query(&key.tags)
            .send()
            .await
            .map_err(|e| NexusError::Network {
                message: e.to_string(),
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Sign a request body using HMAC-SHA256.
    fn sign_request(&self, body: &[u8]) -> String {
        let tag = hmac::sign(&self.signing_key, body);
        hex::encode(tag.as_ref())
    }

    /// Handle a successful or error response.
    async fn handle_response<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> NexusResult<T> {
        let status = response.status();

        if status.is_success() {
            response
                .json::<T>()
                .await
                .map_err(|e| NexusError::Serialization {
                    message: e.to_string(),
                })
        } else {
            self.handle_error_response(response).await
        }
    }

    /// Convert an error response to a NexusError.
    async fn handle_error_response<T>(&self, response: reqwest::Response) -> NexusResult<T> {
        let status = response.status();
        let retry_after = response
            .headers()
            .get("Retry-After")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());

        let body = response.text().await.unwrap_or_default();

        match status.as_u16() {
            401 => Err(NexusError::Authentication { message: body }),
            429 => Err(NexusError::RateLimited {
                retry_after_secs: retry_after,
            }),
            400 => Err(NexusError::InvalidRequest { message: body }),
            status if status >= 500 => Err(NexusError::ServerError {
                status,
                message: body,
            }),
            _ => Err(NexusError::ServerError {
                status: status.as_u16(),
                message: body,
            }),
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &NexusConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = NexusConfig::new("test-api-key");
        let client = NexusClient::new(config);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_client_health_check_invalid_endpoint() {
        let config = NexusConfig::new("test-key").with_endpoint("http://localhost:1");
        let client = NexusClient::new(config).unwrap();

        let result = client.health_check().await;
        assert!(result.is_err());
    }
}

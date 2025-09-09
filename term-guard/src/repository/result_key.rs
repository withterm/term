//! Result key for identifying and tagging metrics in the repository.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Key for identifying metrics in the repository.
///
/// A `ResultKey` consists of a timestamp and a set of tags that uniquely
/// identify a set of metrics. This allows for efficient querying and
/// organization of metrics across different dimensions.
///
/// # Example
///
/// ```rust,ignore
/// use term_guard::repository::ResultKey;
///
/// let key = ResultKey::now()
///     .with_tag("environment", "production")
///     .with_tag("dataset", "users_table")
///     .with_tag("version", "v1.2.3");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultKey {
    /// Unix timestamp in milliseconds.
    pub timestamp: i64,

    /// Tags for categorizing and filtering metrics.
    pub tags: HashMap<String, String>,
}

impl ResultKey {
    /// Creates a new result key with the given timestamp.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Unix timestamp in milliseconds
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            tags: HashMap::new(),
        }
    }

    /// Creates a new result key with the current timestamp.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use term_guard::repository::ResultKey;
    ///
    /// let key = ResultKey::now();
    /// println!("Timestamp: {}", key.timestamp);
    /// ```
    pub fn now() -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        Self::new(timestamp)
    }

    /// Adds a tag to the result key.
    ///
    /// # Arguments
    ///
    /// * `key` - The tag key
    /// * `value` - The tag value
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use term_guard::repository::ResultKey;
    ///
    /// let key = ResultKey::now()
    ///     .with_tag("environment", "staging")
    ///     .with_tag("region", "us-west-2");
    /// ```
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key_str = key.into();
        let value_str = value.into();

        // Reject tags with null characters immediately (don't store them at all)
        // Other validations (empty keys, oversized keys, other control characters) happen later in validate_tags()
        if !key_str.chars().any(|c| c == '\0') && !value_str.chars().any(|c| c == '\0') {
            self.tags.insert(key_str, value_str);
        }
        self
    }

    /// Creates a new result key with validation.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Unix timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// Returns a validated ResultKey or an error if the timestamp is invalid.
    pub fn try_new(timestamp: i64) -> std::result::Result<Self, String> {
        // Validate timestamp is reasonable (not negative, not too far in future)
        if timestamp < 0 {
            return Err("Timestamp cannot be negative".to_string());
        }

        // Allow timestamps up to 1000 years in the future from 2024
        let max_timestamp = 1_704_067_200_000 + (1000 * 365 * 24 * 60 * 60 * 1000); // 2024-01-01 + 1000 years
        if timestamp > max_timestamp {
            return Err(format!(
                "Timestamp too far in the future: {timestamp} (max: {max_timestamp})"
            ));
        }

        Ok(Self::new(timestamp))
    }

    /// Adds multiple tags to the result key.
    ///
    /// # Arguments
    ///
    /// * `tags` - Iterator of (key, value) pairs
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use term_guard::repository::ResultKey;
    /// use std::collections::HashMap;
    ///
    /// let mut tags = HashMap::new();
    /// tags.insert("env".to_string(), "prod".to_string());
    /// tags.insert("version".to_string(), "1.0.0".to_string());
    ///
    /// let key = ResultKey::now().with_tags(tags);
    /// ```
    pub fn with_tags<I, K, V>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in tags {
            self.tags.insert(k.into(), v.into());
        }
        self
    }

    /// Returns the timestamp as a chrono DateTime.
    pub fn as_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.timestamp).unwrap_or_else(chrono::Utc::now)
    }

    /// Checks if the key has a specific tag.
    ///
    /// # Arguments
    ///
    /// * `key` - The tag key to check
    pub fn has_tag(&self, key: &str) -> bool {
        self.tags.contains_key(key)
    }

    /// Gets the value of a specific tag.
    ///
    /// # Arguments
    ///
    /// * `key` - The tag key to retrieve
    pub fn get_tag(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(|s| s.as_str())
    }

    /// Checks if all the specified tags match.
    ///
    /// # Arguments
    ///
    /// * `tags` - The tags to match against
    ///
    /// # Returns
    ///
    /// Returns `true` if all specified tags exist and have matching values.
    pub fn matches_tags(&self, tags: &HashMap<String, String>) -> bool {
        tags.iter().all(|(k, v)| self.tags.get(k) == Some(v))
    }

    /// Validates that tag keys and values are safe for storage.
    ///
    /// # Returns
    ///
    /// Returns an error if any tag key or value contains invalid characters
    /// or exceeds length limits.
    pub fn validate_tags(&self) -> std::result::Result<(), String> {
        const MAX_TAG_KEY_LENGTH: usize = 256;
        const MAX_TAG_VALUE_LENGTH: usize = 1024;
        const MAX_TAG_COUNT: usize = 100;

        if self.tags.len() > MAX_TAG_COUNT {
            return Err(format!(
                "Too many tags: {} (max: {MAX_TAG_COUNT})",
                self.tags.len()
            ));
        }

        for (key, value) in &self.tags {
            if key.is_empty() {
                return Err("Tag key cannot be empty".to_string());
            }

            if key.len() > MAX_TAG_KEY_LENGTH {
                return Err(format!(
                    "Tag key '{key}' is too long: {} chars (max: {MAX_TAG_KEY_LENGTH})",
                    key.len()
                ));
            }

            if value.len() > MAX_TAG_VALUE_LENGTH {
                return Err(format!(
                    "Tag value for key '{key}' is too long: {} chars (max: {MAX_TAG_VALUE_LENGTH})",
                    value.len()
                ));
            }

            // Check for control characters and other problematic characters
            if key.chars().any(|c| c.is_control() || c == '\0') {
                return Err(format!("Tag key '{key}' contains invalid characters"));
            }

            if value.chars().any(|c| c.is_control() || c == '\0') {
                return Err(format!(
                    "Tag value for key '{key}' contains invalid characters"
                ));
            }
        }

        Ok(())
    }

    /// Creates a normalized storage key that can be used for collision detection.
    ///
    /// This always uses SHA256 hashing for maximum collision resistance,
    /// regardless of tag complexity.
    pub fn to_normalized_storage_key(&self) -> String {
        let mut key = self.timestamp.to_string();

        if self.tags.is_empty() {
            return key;
        }

        // Sort tags for consistent key generation
        let mut sorted_tags: Vec<_> = self.tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| k.as_str());

        // Serialize tags to JSON for reliable encoding
        let tags_json = serde_json::to_string(&sorted_tags).unwrap_or_else(|_| String::from("[]"));

        // Always use SHA256 hash for maximum collision resistance
        let mut hasher = Sha256::new();
        hasher.update(tags_json.as_bytes());
        let hash = hasher.finalize();
        let hash_str = hex::encode(&hash[..16]); // Use first 16 bytes (32 hex chars)
        key.push_str(&format!("_sha_{hash_str}"));

        key
    }

    /// Creates a collision-resistant string representation suitable for use as a filename or key.
    ///
    /// Uses a hybrid approach:
    /// 1. For simple cases: `{timestamp}_{base64_encoded_tags}`
    /// 2. For complex cases: `{timestamp}_{hash_of_tags}`
    ///
    /// This prevents collisions while maintaining some human readability for simple cases.
    pub fn to_storage_key(&self) -> String {
        let mut key = self.timestamp.to_string();

        if self.tags.is_empty() {
            return key;
        }

        // Sort tags for consistent key generation
        let mut sorted_tags: Vec<_> = self.tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| k.as_str());

        // Serialize tags to JSON for reliable encoding
        let tags_json = serde_json::to_string(&sorted_tags).unwrap_or_else(|_| String::from("[]"));

        // For simple, short tag sets, use base64 encoding for readability
        if tags_json.len() <= 100 && self.is_safe_for_encoding() {
            let encoded = URL_SAFE_NO_PAD.encode(tags_json.as_bytes());
            key.push_str(&format!("_b64_{encoded}"));
        } else {
            // For complex tag sets, use SHA256 hash to prevent collisions
            let mut hasher = Sha256::new();
            hasher.update(tags_json.as_bytes());
            let hash = hasher.finalize();
            let hash_str = hex::encode(&hash[..16]); // Use first 16 bytes (32 hex chars)
            key.push_str(&format!("_sha_{hash_str}"));
        }

        key
    }

    /// Checks if the tags contain only safe characters for base64 encoding.
    fn is_safe_for_encoding(&self) -> bool {
        self.tags.iter().all(|(k, v)| {
            k.chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
                && v.chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
        })
    }

    /// Parses a storage key back into a ResultKey.
    ///
    /// Supports both the new collision-resistant format and the legacy format
    /// for backward compatibility.
    ///
    /// # Arguments
    ///
    /// * `storage_key` - The storage key string to parse
    ///
    /// # Returns
    ///
    /// Returns `None` if the key format is invalid.
    pub fn from_storage_key(storage_key: &str) -> Option<Self> {
        let parts: Vec<&str> = storage_key.split('_').collect();
        if parts.is_empty() {
            return None;
        }

        let timestamp = parts[0].parse().ok()?;

        if parts.len() == 1 {
            // No tags
            return Some(Self {
                timestamp,
                tags: HashMap::new(),
            });
        }

        // Check for new format
        if parts.len() >= 3 {
            if parts[1] == "b64" {
                // Base64 encoded format
                let encoded = parts[2..].join("_");
                let decoded = URL_SAFE_NO_PAD.decode(encoded.as_bytes()).ok()?;
                let json_str = String::from_utf8(decoded).ok()?;
                let tag_pairs: Vec<(String, String)> = serde_json::from_str(&json_str).ok()?;

                let mut tags = HashMap::new();
                for (k, v) in tag_pairs {
                    tags.insert(k, v);
                }

                return Some(Self { timestamp, tags });
            } else if parts[1] == "sha" {
                // SHA256 hash format - cannot be reconstructed
                // This is intentional - these keys are only for storage identification
                return Some(Self {
                    timestamp,
                    tags: HashMap::new(),
                });
            }
        }

        // Legacy format for backward compatibility
        let mut tags = HashMap::new();
        for part in &parts[1..] {
            if let Some((key, value)) = part.split_once('=') {
                tags.insert(key.to_string(), value.to_string());
            }
        }

        Some(Self { timestamp, tags })
    }
}

impl fmt::Display for ResultKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResultKey(timestamp={}", self.timestamp)?;
        if !self.tags.is_empty() {
            write!(f, ", tags={{")?;
            let mut first = true;
            for (k, v) in &self.tags {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "{k}={v}")?;
                first = false;
            }
            write!(f, "}}")?;
        }
        write!(f, ")")
    }
}

impl Default for ResultKey {
    fn default() -> Self {
        Self::now()
    }
}

impl Hash for ResultKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);

        // Sort tags for consistent hashing
        let mut sorted_tags: Vec<_> = self.tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| k.as_str());

        for (key, value) in sorted_tags {
            key.hash(state);
            value.hash(state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_result_key_creation() {
        let key = ResultKey::new(1234567890);
        assert_eq!(key.timestamp, 1234567890);
        assert!(key.tags.is_empty());
    }

    #[test]
    fn test_result_key_with_tags() {
        let key = ResultKey::now()
            .with_tag("env", "prod")
            .with_tag("region", "us-east-1");

        assert!(key.has_tag("env"));
        assert!(key.has_tag("region"));
        assert!(!key.has_tag("nonexistent"));

        assert_eq!(key.get_tag("env"), Some("prod"));
        assert_eq!(key.get_tag("region"), Some("us-east-1"));
        assert_eq!(key.get_tag("nonexistent"), None);
    }

    #[test]
    fn test_result_key_matches_tags() {
        let key = ResultKey::now()
            .with_tag("env", "prod")
            .with_tag("region", "us-east-1")
            .with_tag("version", "1.0.0");

        let mut filter_tags = HashMap::new();
        filter_tags.insert("env".to_string(), "prod".to_string());
        filter_tags.insert("region".to_string(), "us-east-1".to_string());

        assert!(key.matches_tags(&filter_tags));

        filter_tags.insert("env".to_string(), "staging".to_string());
        assert!(!key.matches_tags(&filter_tags));
    }

    #[test]
    fn test_storage_key_conversion() {
        // Test simple case (should use base64)
        let key = ResultKey::new(1234567890)
            .with_tag("env", "prod")
            .with_tag("region", "us-east-1");

        let storage_key = key.to_storage_key();
        assert!(storage_key.starts_with("1234567890"));
        assert!(storage_key.contains("_b64_"));

        let parsed = ResultKey::from_storage_key(&storage_key).unwrap();
        assert_eq!(parsed.timestamp, 1234567890);
        assert_eq!(parsed.get_tag("env"), Some("prod"));
        assert_eq!(parsed.get_tag("region"), Some("us-east-1"));
    }

    #[test]
    fn test_storage_key_with_special_chars() {
        // Test complex case with special characters (should use SHA256)
        let key = ResultKey::new(1234567890)
            .with_tag("path", "/home/user/data")
            .with_tag("query", "SELECT * FROM table WHERE id > 100");

        let storage_key = key.to_storage_key();
        assert!(storage_key.starts_with("1234567890"));
        assert!(storage_key.contains("_sha_"));
        assert_eq!(storage_key.len(), 1234567890.to_string().len() + 5 + 32); // timestamp + _sha_ + 32 hex chars

        let parsed = ResultKey::from_storage_key(&storage_key).unwrap();
        assert_eq!(parsed.timestamp, 1234567890);
        // SHA format cannot reconstruct original tags
        assert_eq!(parsed.tags.len(), 0);
    }

    #[test]
    fn test_normalized_storage_key() {
        let key1 = ResultKey::new(1234567890)
            .with_tag("env", "prod")
            .with_tag("region", "us-east-1");

        let key2 = ResultKey::new(1234567890)
            .with_tag("region", "us-east-1")
            .with_tag("env", "prod"); // Same tags, different order

        // Normalized keys should be identical regardless of tag insertion order
        assert_eq!(
            key1.to_normalized_storage_key(),
            key2.to_normalized_storage_key()
        );
    }

    #[test]
    fn test_tag_validation() {
        let mut key = ResultKey::new(1234567890);

        // Valid tags
        key = key.with_tag("env", "production");
        assert!(key.validate_tags().is_ok());

        // Test empty key
        let invalid_key = ResultKey::new(1234567890).with_tag("", "value");
        assert!(invalid_key.validate_tags().is_err());

        // Test oversized key
        let long_key = "a".repeat(300);
        let invalid_key = ResultKey::new(1234567890).with_tag(&long_key, "value");
        assert!(invalid_key.validate_tags().is_err());

        // Test oversized value
        let long_value = "a".repeat(2000);
        let invalid_key = ResultKey::new(1234567890).with_tag("key", &long_value);
        assert!(invalid_key.validate_tags().is_err());

        // Test control characters (tab character will be stored by with_tag but rejected by validate_tags)
        let invalid_key = ResultKey::new(1234567890).with_tag("key\t", "value");
        assert!(invalid_key.validate_tags().is_err());
    }

    #[test]
    fn test_collision_resistance() {
        // Test that different tag combinations produce different storage keys
        let key1 = ResultKey::new(1234567890).with_tag("a_b", "c");
        let key2 = ResultKey::new(1234567890).with_tag("a", "b_c");

        assert_ne!(key1.to_storage_key(), key2.to_storage_key());
        assert_ne!(
            key1.to_normalized_storage_key(),
            key2.to_normalized_storage_key()
        );
    }

    #[test]
    fn test_legacy_format_compatibility() {
        // Test that old format keys can still be parsed
        let legacy_storage_format = "1234567890_env=prod_region=us-east-1";
        let parsed = ResultKey::from_storage_key(legacy_storage_format).unwrap();

        assert_eq!(parsed.timestamp, 1234567890);
        assert_eq!(parsed.get_tag("env"), Some("prod"));
        assert_eq!(parsed.get_tag("region"), Some("us-east-1"));
    }

    #[test]
    fn test_try_new_validation() {
        // Valid timestamp
        assert!(ResultKey::try_new(1234567890).is_ok());

        // Invalid negative timestamp
        assert!(ResultKey::try_new(-1).is_err());

        // Valid current timestamp
        let now = chrono::Utc::now().timestamp_millis();
        assert!(ResultKey::try_new(now).is_ok());

        // Invalid far future timestamp
        let far_future = 1_704_067_200_000 + (2000 * 365 * 24 * 60 * 60 * 1000); // 2000 years in future
        assert!(ResultKey::try_new(far_future).is_err());
    }

    #[test]
    fn test_with_tag_validation() {
        let key = ResultKey::new(1234567890);

        // Valid tags should be added
        let key = key.with_tag("env", "prod");
        assert_eq!(key.get_tag("env"), Some("prod"));

        // Empty key should be rejected during validation, not during with_tag
        let invalid_key = ResultKey::new(1234567890).with_tag("", "value");
        assert!(invalid_key.validate_tags().is_err());

        // Null character should be rejected
        let key = key.with_tag("key\0", "value");
        assert!(key.get_tag("key\0").is_none());

        let key = key.with_tag("key", "value\0");
        assert!(key.get_tag("key").is_none());
    }

    #[test]
    fn test_display_formatting() {
        let key = ResultKey::new(1234567890)
            .with_tag("env", "prod")
            .with_tag("version", "1.0.0");

        let display = format!("{key}");
        assert!(display.contains("timestamp=1234567890"));
        assert!(display.contains("env=prod"));
        assert!(display.contains("version=1.0.0"));
    }
}

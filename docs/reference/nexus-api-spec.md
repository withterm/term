# Term Nexus API Specification

> **Type**: Reference Documentation
> **Audience**: Backend developers implementing Term Nexus
> **Version**: 1.0

## Overview

This document specifies the HTTP API that Term Nexus must implement to support the Term SDK. The API provides endpoints for metrics ingestion, querying, and deletion.

**Base URL**: `https://api.withterm.com`

## Authentication

All authenticated endpoints require two headers:

| Header | Description |
|--------|-------------|
| `X-Term-Api-Key` | The API key identifying the client |
| `X-Term-Signature` | HMAC-SHA256 signature of the request body (write operations only) |

### HMAC Signature

For write operations (`POST`, `DELETE`), the SDK signs the request body using HMAC-SHA256:

```
signature = HMAC-SHA256(api_key, request_body)
```

The signature is sent as a lowercase hex-encoded string in the `X-Term-Signature` header.

**Verification pseudocode:**
```python
expected = hmac.new(
    key=api_key.encode('utf-8'),
    msg=request_body,
    digestmod='sha256'
).hexdigest()

if not hmac.compare_digest(expected, request_signature):
    return 401 Unauthorized
```

---

## Endpoints

### Health Check

Check API availability and version.

```
GET /v1/health
```

**Authentication**: None required

**Response**: `200 OK`
```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Service status (`"ok"`, `"degraded"`, etc.) |
| `version` | string | API version |

---

### Ingest Metrics

Submit validation metrics for storage.

```
POST /v1/metrics
```

**Authentication**: Required (`X-Term-Api-Key` + `X-Term-Signature`)

**Headers**:
```
Content-Type: application/json
X-Term-Api-Key: <api_key>
X-Term-Signature: <hmac_signature>
```

**Request Body**: Array of `NexusMetric` objects

```json
[
  {
    "result_key": {
      "dataset_date": 1704931200000,
      "tags": {
        "environment": "production",
        "dataset": "orders",
        "pipeline": "daily-etl"
      }
    },
    "metrics": {
      "completeness.user_id": {
        "type": "double",
        "value": 0.98
      },
      "row_count": {
        "type": "long",
        "value": 1000000
      },
      "is_valid": {
        "type": "boolean",
        "value": true
      }
    },
    "metadata": {
      "dataset_name": "orders_table",
      "start_time": "2024-01-10T12:00:00Z",
      "end_time": "2024-01-10T12:05:00Z",
      "term_version": "0.0.2",
      "custom": {
        "spark_job_id": "job-12345"
      }
    },
    "validation_result": {
      "status": "warning",
      "total_checks": 10,
      "passed_checks": 9,
      "failed_checks": 1,
      "issues": [
        {
          "check_name": "QualityCheck",
          "constraint_name": "PatternMatch",
          "level": "warning",
          "message": "Pattern mismatch in 2% of rows",
          "metric": 0.98
        }
      ]
    }
  }
]
```

**Response**: `200 OK`
```json
{
  "accepted": 1,
  "rejected": 0,
  "errors": []
}
```

| Field | Type | Description |
|-------|------|-------------|
| `accepted` | integer | Number of metrics successfully stored |
| `rejected` | integer | Number of metrics rejected |
| `errors` | array[string] | Error messages for rejected metrics |

---

### Query Metrics

Retrieve stored metrics with filtering and pagination.

```
GET /v1/metrics
```

**Authentication**: Required (`X-Term-Api-Key`)

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `after` | integer | Filter: dataset_date > after (Unix millis) |
| `before` | integer | Filter: dataset_date < before (Unix millis) |
| `limit` | integer | Maximum results to return (default: 100) |
| `cursor` | string | Pagination cursor from previous response |
| `{tag_key}` | string | Filter by tag value (e.g., `environment=production`) |

**Example Request**:
```
GET /v1/metrics?after=1704844800000&environment=production&limit=50
```

**Response**: `200 OK`
```json
{
  "results": [
    {
      "result_key": {
        "dataset_date": 1704931200000,
        "tags": {
          "environment": "production",
          "dataset": "orders"
        }
      },
      "metrics": {
        "completeness.user_id": {
          "type": "double",
          "value": 0.98
        }
      },
      "metadata": {
        "dataset_name": "orders_table",
        "term_version": "0.0.2",
        "custom": {}
      }
    }
  ],
  "pagination": {
    "next_cursor": "eyJkYXRhc2V0X2RhdGUiOjE3MDQ5MzEyMDAwMDB9",
    "has_more": true
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `results` | array[NexusMetric] | Matching metrics |
| `pagination.next_cursor` | string | Cursor for next page (null if no more results) |
| `pagination.has_more` | boolean | Whether more results exist |

---

### Delete Metrics

Delete metrics by dataset_date and optional tag filters.

```
DELETE /v1/metrics/{dataset_date}
```

**Authentication**: Required (`X-Term-Api-Key`)

**Path Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset_date` | integer | Unix timestamp in milliseconds |

**Query Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `{tag_key}` | string | Filter by tag value |

**Example Request**:
```
DELETE /v1/metrics/1704931200000?environment=staging
```

**Response**: `204 No Content`

---

## Data Types

### NexusMetric

The primary data structure for validation metrics.

```json
{
  "result_key": NexusResultKey,
  "metrics": { [string]: NexusMetricValue },
  "metadata": NexusMetadata,
  "validation_result": NexusValidationResult | null
}
```

### NexusResultKey

Unique identifier for a metrics collection.

```json
{
  "dataset_date": 1704931200000,
  "tags": {
    "key1": "value1",
    "key2": "value2"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `dataset_date` | integer | Unix timestamp in milliseconds |
| `tags` | object | Key-value pairs for filtering/grouping |

**Tag Validation Rules**:
- Keys: non-empty, max 256 characters
- Values: max 1024 characters
- Maximum 100 tags per key
- No control characters or null bytes

### NexusMetricValue

Tagged union for metric values.

```json
// Double
{ "type": "double", "value": 0.98 }

// Long (integer)
{ "type": "long", "value": 1000000 }

// String
{ "type": "string", "value": "some text" }

// Boolean
{ "type": "boolean", "value": true }

// Histogram
{
  "type": "histogram",
  "value": {
    "buckets": [
      { "lower_bound": 0.0, "upper_bound": 10.0, "count": 150 },
      { "lower_bound": 10.0, "upper_bound": 20.0, "count": 300 }
    ],
    "total_count": 450,
    "min": 0.5,
    "max": 19.8,
    "mean": 12.3,
    "std_dev": 4.5
  }
}
```

### NexusHistogram

Statistical distribution data.

| Field | Type | Description |
|-------|------|-------------|
| `buckets` | array | Histogram buckets |
| `buckets[].lower_bound` | float | Bucket lower bound (inclusive) |
| `buckets[].upper_bound` | float | Bucket upper bound (exclusive) |
| `buckets[].count` | integer | Count in this bucket |
| `total_count` | integer | Total observations |
| `min` | float? | Minimum value (optional) |
| `max` | float? | Maximum value (optional) |
| `mean` | float? | Mean value (optional) |
| `std_dev` | float? | Standard deviation (optional) |

### NexusMetadata

Context about the metrics collection.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dataset_name` | string | No | Human-readable dataset identifier |
| `start_time` | string | No | ISO 8601 timestamp when collection started |
| `end_time` | string | No | ISO 8601 timestamp when collection ended |
| `term_version` | string | Yes | Version of the Term library |
| `custom` | object | No | User-defined key-value metadata |

### NexusValidationResult

Summary of validation check results.

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Overall status (`"success"`, `"warning"`, `"error"`) |
| `total_checks` | integer | Total number of checks run |
| `passed_checks` | integer | Number of checks that passed |
| `failed_checks` | integer | Number of checks that failed |
| `issues` | array | List of validation issues |

### NexusValidationIssue

Details about a validation failure.

| Field | Type | Description |
|-------|------|-------------|
| `check_name` | string | Name of the check (e.g., `"CompletenessCheck"`) |
| `constraint_name` | string | Name of the constraint (e.g., `"isComplete"`) |
| `level` | string | Severity (`"warning"`, `"error"`) |
| `message` | string | Human-readable description |
| `metric` | float? | Associated metric value (optional) |

---

## Error Handling

### HTTP Status Codes

| Status | Meaning | SDK Behavior |
|--------|---------|--------------|
| `200` | Success | Process response |
| `204` | Success (no content) | Operation complete |
| `400` | Bad Request | Do not retry, fix request |
| `401` | Unauthorized | Do not retry, check API key |
| `429` | Rate Limited | Retry after `Retry-After` header |
| `500+` | Server Error | Retry with exponential backoff |

### Error Response Format

For `4xx` and `5xx` errors, return a plain text or JSON error message in the body:

```json
{
  "error": "Invalid API key",
  "code": "INVALID_API_KEY"
}
```

Or plain text:
```
Invalid API key
```

### Rate Limiting

When rate limited, include the `Retry-After` header:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 60
```

The SDK respects this header and waits the specified number of seconds before retrying.

---

## SDK Retry Behavior

The SDK implements the following retry strategy:

1. **Retryable errors**: Network errors, 429, 5xx responses
2. **Non-retryable errors**: 400, 401 (fail immediately)
3. **Backoff**: Exponential with jitter
   - Base delay: 1 second
   - Max delay: 32 seconds
   - Formula: `min(32, 2^attempt) + random_jitter`
4. **Max retries**: Configurable (default: 3)
5. **Retry-After**: When present, overrides calculated backoff

---

## Example: Complete Ingest Flow

**1. Client sends request:**
```http
POST /v1/metrics HTTP/1.1
Host: api.withterm.com
Content-Type: application/json
X-Term-Api-Key: tk_live_abc123
X-Term-Signature: 7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069

[{"result_key":{"dataset_date":1704931200000,"tags":{"env":"prod"}},...}]
```

**2. Server validates:**
- Verify API key exists and is active
- Verify HMAC signature matches
- Parse and validate JSON schema
- Check tag validation rules

**3. Server responds:**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{"accepted":1,"rejected":0,"errors":[]}
```

---

## Implementation Checklist

- [ ] `GET /v1/health` - No auth required
- [ ] `POST /v1/metrics` - Verify API key + HMAC signature
- [ ] `GET /v1/metrics` - Verify API key, support query params
- [ ] `DELETE /v1/metrics/{dataset_date}` - Verify API key
- [ ] HMAC-SHA256 signature verification
- [ ] Rate limiting with `Retry-After` header
- [ ] Tag validation (length limits, character restrictions)
- [ ] Cursor-based pagination for queries
- [ ] Proper HTTP status codes for all error conditions

---

## Related Documentation

- [How to Use the Term Nexus SDK](../how-to/use-nexus.md) - Client usage guide
- [Explanation: Nexus Architecture](../explanation/nexus-architecture.md) - Design decisions


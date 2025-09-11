# ValidationSuite Reference

> **Type**: Reference (Information-oriented)
> **Audience**: Users looking up ValidationSuite details
> **Goal**: Provide complete technical information about ValidationSuite API

## Overview

`ValidationSuite` is the primary container for organizing and executing data quality checks in Term. It groups related checks and provides metadata for validation runs.

## Synopsis

```rust
use term::core::{ValidationSuite, Check};

let suite = ValidationSuite::builder("my_validation")
    .description("Validate customer data quality")
    .add_check(Check::new("completeness").is_complete("email"))
    .add_check(Check::new("uniqueness").is_unique("id"))
    .build();
```

## API Reference

### Types

#### `ValidationSuite`

```rust
pub struct ValidationSuite {
    name: String,
    description: Option<String>,
    checks: Vec<Check>,
    metadata: HashMap<String, String>,
    created_at: DateTime<Utc>,
}
```

Container for data validation checks with metadata and execution context.

##### Fields

- **`name`**: `String` - Unique identifier for the suite
- **`description`**: `Option<String>` - Optional human-readable description
- **`checks`**: `Vec<Check>` - Collection of validation checks to execute
- **`metadata`**: `HashMap<String, String>` - Custom key-value metadata
- **`created_at`**: `DateTime<Utc>` - Timestamp of suite creation

##### Example

```rust
let suite = ValidationSuite::builder("daily_validation")
    .description("Daily data quality checks")
    .build();
```

### Builder

#### `ValidationSuiteBuilder`

```rust
pub struct ValidationSuiteBuilder {
    name: String,
    description: Option<String>,
    checks: Vec<Check>,
    metadata: HashMap<String, String>,
}
```

Builder for constructing ValidationSuite instances.

##### Methods

#### `builder`

```rust
pub fn builder(name: impl Into<String>) -> ValidationSuiteBuilder
```

Creates a new ValidationSuiteBuilder with the specified name.

##### Parameters

- **`name`**: `impl Into<String>` - Unique name for the validation suite

##### Returns

- `ValidationSuiteBuilder` - Builder instance for method chaining

##### Example

```rust
let builder = ValidationSuite::builder("customer_validation");
```

#### `description`

```rust
pub fn description(mut self, description: impl Into<String>) -> Self
```

Sets the description for the validation suite.

##### Parameters

- **`description`**: `impl Into<String>` - Human-readable description

##### Returns

- `Self` - Builder instance for method chaining

#### `add_check`

```rust
pub fn add_check(mut self, check: Check) -> Self
```

Adds a validation check to the suite.

##### Parameters

- **`check`**: `Check` - Validation check to add

##### Returns

- `Self` - Builder instance for method chaining

#### `add_checks`

```rust
pub fn add_checks(mut self, checks: Vec<Check>) -> Self
```

Adds multiple validation checks to the suite.

##### Parameters

- **`checks`**: `Vec<Check>` - Collection of validation checks

##### Returns

- `Self` - Builder instance for method chaining

#### `with_metadata`

```rust
pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self
```

Adds custom metadata to the suite.

##### Parameters

- **`key`**: `impl Into<String>` - Metadata key
- **`value`**: `impl Into<String>` - Metadata value

##### Returns

- `Self` - Builder instance for method chaining

#### `build`

```rust
pub fn build(self) -> ValidationSuite
```

Constructs the ValidationSuite instance.

##### Returns

- `ValidationSuite` - Completed validation suite

##### Example

```rust
let suite = ValidationSuite::builder("validation")
    .description("Data quality checks")
    .add_check(check1)
    .add_check(check2)
    .build();
```

### Instance Methods

#### `name`

```rust
pub fn name(&self) -> &str
```

Returns the name of the validation suite.

##### Returns

- `&str` - Suite name

#### `description`

```rust
pub fn description(&self) -> Option<&str>
```

Returns the description if set.

##### Returns

- `Option<&str>` - Optional description

#### `checks`

```rust
pub fn checks(&self) -> &[Check]
```

Returns a slice of all checks in the suite.

##### Returns

- `&[Check]` - Slice of validation checks

#### `check_count`

```rust
pub fn check_count(&self) -> usize
```

Returns the number of checks in the suite.

##### Returns

- `usize` - Number of checks

#### `metadata`

```rust
pub fn metadata(&self) -> &HashMap<String, String>
```

Returns the metadata map.

##### Returns

- `&HashMap<String, String>` - Reference to metadata

#### `get_check`

```rust
pub fn get_check(&self, name: &str) -> Option<&Check>
```

Retrieves a check by name.

##### Parameters

- **`name`**: `&str` - Name of the check to retrieve

##### Returns

- `Option<&Check>` - Check if found, None otherwise

#### `has_check`

```rust
pub fn has_check(&self, name: &str) -> bool
```

Checks if a check with the given name exists.

##### Parameters

- **`name`**: `&str` - Name to check for

##### Returns

- `bool` - True if check exists

### Associated Functions

#### `from_json`

```rust
pub fn from_json(json: &str) -> Result<ValidationSuite>
```

Deserializes a ValidationSuite from JSON.

##### Parameters

- **`json`**: `&str` - JSON string representation

##### Returns

- `Result<ValidationSuite>` - Deserialized suite or error

##### Errors

- `TermError::ParseError` - Invalid JSON format

#### `to_json`

```rust
pub fn to_json(&self) -> Result<String>
```

Serializes the ValidationSuite to JSON.

##### Returns

- `Result<String>` - JSON string representation

##### Errors

- `TermError::SerializationError` - Serialization failure

### Traits

#### `Clone`

```rust
impl Clone for ValidationSuite
```

Enables cloning of ValidationSuite instances.

#### `Debug`

```rust
impl Debug for ValidationSuite
```

Provides debug formatting for ValidationSuite.

#### `Serialize` / `Deserialize`

```rust
impl Serialize for ValidationSuite
impl<'de> Deserialize<'de> for ValidationSuite
```

Enables serde serialization and deserialization.

## Behavior

### Check Execution Order

Checks are executed in the order they were added to the suite. Each check runs independently and failures do not stop subsequent checks from executing.

### Metadata Handling

Metadata is preserved through serialization and can be used for:
- Tracking suite version
- Recording data source information
- Storing execution context
- Custom application-specific data

### Thread Safety

ValidationSuite is Send + Sync, making it safe to share across threads and use in async contexts.

## Performance Characteristics

- **Time Complexity**: O(n) for check execution where n is number of checks
- **Space Complexity**: O(n) for storing checks
- **Allocations**: Minimal allocations during execution
- **Thread Safety**: Yes (Send + Sync)

## Version History

| Version | Changes |
|---------|---------|
| 0.0.1   | Initial implementation with builder pattern |
| 0.0.2   | Added metadata support |
| 0.0.3   | Added JSON serialization |

## Examples

### Basic Example

```rust
use term::core::{ValidationSuite, Check};

let suite = ValidationSuite::builder("basic_validation")
    .add_check(Check::new("id_check").is_unique("id"))
    .build();

assert_eq!(suite.name(), "basic_validation");
assert_eq!(suite.check_count(), 1);
```

### Advanced Example

```rust
use term::core::{ValidationSuite, Check, CheckLevel};
use chrono::Utc;

let suite = ValidationSuite::builder("advanced_validation")
    .description("Comprehensive data quality checks")
    .with_metadata("version", "1.0.0")
    .with_metadata("environment", "production")
    .with_metadata("created_by", "data_team")
    .add_check(
        Check::new("critical_checks")
            .level(CheckLevel::Error)
            .is_complete("customer_id")
            .is_unique("customer_id")
            .is_complete("email")
    )
    .add_check(
        Check::new("quality_checks")
            .level(CheckLevel::Warning)
            .has_completeness("phone", 0.8)
            .has_min("age", 0.0)
            .has_max("age", 150.0)
    )
    .build();

// Serialize for storage
let json = suite.to_json()?;

// Later, deserialize
let restored = ValidationSuite::from_json(&json)?;
assert_eq!(restored.name(), suite.name());
```

## See Also

- [`Check`](check.md) - Individual validation checks
- [`ValidationRunner`](validation-runner.md) - Executes validation suites
- [`ValidationResult`](validation-result.md) - Results from validation execution
- [How to Create Validation Suites](../how-to/create-validation-suite.md)
- [Understanding Validation Architecture](../explanation/validation-architecture.md)
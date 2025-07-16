# [Feature/API Name] Reference

<!-- 
This is a REFERENCE template following Diátaxis principles.
Reference docs are INFORMATION-oriented, not task-oriented.
They describe the machinery comprehensively and accurately.
-->

## Overview

[Brief, factual description of what this is]

## Synopsis

```rust
// Quick example showing typical usage
// Not a tutorial - just demonstrates syntax
```

## Description

[Detailed technical description of the feature/API]

## API Reference

### Types

#### `TypeName`

```rust
pub struct TypeName {
    // fields
}
```

[Description of the type and its purpose]

##### Fields

- **`field_name`**: `Type` - [Description]
- **`field_name`**: `Type` - [Description]

##### Example

```rust
// Simple example of using this type
```

### Functions

#### `function_name`

```rust
pub fn function_name(param1: Type1, param2: Type2) -> ReturnType
```

[Description of what the function does]

##### Parameters

- **`param1`**: `Type1` - [Description, constraints, valid values]
- **`param2`**: `Type2` - [Description, constraints, valid values]

##### Returns

- `ReturnType` - [Description of return value]

##### Errors

- `ErrorType1` - [When this error occurs]
- `ErrorType2` - [When this error occurs]

##### Example

```rust
// Example usage
```

### Traits

#### `TraitName`

```rust
pub trait TraitName {
    // trait definition
}
```

[Description of the trait]

##### Required Methods

- **`method_name`** - [Description]

##### Provided Methods

- **`method_name`** - [Description]

### Configuration

#### Option: `option_name`

- **Type**: `Type`
- **Default**: `value`
- **Environment Variable**: `TERM_OPTION_NAME`
- **Description**: [What this configures]
- **Valid Values**: [List or range]
- **Example**: 
  ```toml
  option_name = "value"
  ```

### Constants

#### `CONSTANT_NAME`

- **Type**: `Type`
- **Value**: `value`
- **Description**: [What this represents]

## Behavior

### [Behavior Category 1]

[Detailed description of how this behaves in specific scenarios]

### [Behavior Category 2]

[Detailed description of edge cases, limitations, etc.]

## Performance Characteristics

- **Time Complexity**: O(n)
- **Space Complexity**: O(1)
- **Allocations**: [Description]
- **Thread Safety**: [Yes/No, with details]

## Version History

| Version | Changes |
|---------|---------|
| 0.0.1   | Initial implementation |
| 0.0.2   | Added feature X |

## Examples

### Basic Example

```rust
// Complete, minimal example
```

### Advanced Example

```rust
// More complex example showing advanced usage
```

## See Also

- [`RelatedType`] - [Relationship]
- [`RelatedFunction`] - [Relationship]
- [How to Use This Feature](../how-to/relevant-guide.md)
- [Understanding This Concept](../explanation/relevant-explanation.md)

---

<!-- Reference Checklist for Authors:
- [ ] Describes what, not how or why
- [ ] Covers ALL parameters, options, and behaviors
- [ ] Uses consistent format and terminology
- [ ] Includes type signatures and constraints
- [ ] Provides minimal examples for syntax only
- [ ] Links to how-to guides for usage
- [ ] Links to explanations for concepts
- [ ] Is complete and accurate
-->
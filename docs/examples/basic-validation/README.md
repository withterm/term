# Basic Validation Example

This example demonstrates Term's core validation capabilities using a simple customer dataset.

## What This Example Shows

- Setting up a validation suite
- Adding multiple types of checks
- Running validations on CSV data
- Handling validation results

## Running the Example

```bash
cd docs/examples/basic-validation
cargo run
```

## Files

- `main.rs` - Main validation logic
- `data/customers.csv` - Sample customer data
- `Cargo.toml` - Dependencies

## Key Concepts Demonstrated

1. **Data Source Registration**: Loading CSV files into DataFusion
2. **Check Building**: Using the fluent API to create checks
3. **Validation Execution**: Running suites and processing results
4. **Error Handling**: Graceful handling of validation failures
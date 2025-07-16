# Term vs Deequ: Constraint Coverage Comparison

This document compares the constraint coverage between Term and AWS Deequ, highlighting what's implemented, what's missing, and what's unique to each library.

## ✅ Constraints Implemented in Both

### Basic Data Quality
| Constraint | Deequ | Term | Notes |
|------------|-------|------|-------|
| Completeness | `hasCompleteness` | `has_completeness`, `is_complete` | Term adds `are_complete`, `are_any_complete` for multiple columns |
| Uniqueness | `hasUniqueness`, `isPrimaryKey` | `is_unique`, `are_unique`, `is_primary_key` | Similar functionality |
| Size | `hasSize` | `has_size` | Row count validation |
| Pattern Match | `hasPattern` | `has_pattern` | Regex pattern matching |
| Contained In | `isContainedIn` | `is_contained_in` | Value allowlist |
| Non-negative | `isNonNegative` | `is_non_negative` | Numeric validation |

### Statistical Constraints
| Constraint | Deequ | Term | Notes |
|------------|-------|------|-------|
| Min | `hasMin` | `has_min` | Minimum value check |
| Max | `hasMax` | `has_max` | Maximum value check |
| Mean | `hasMean` | `has_mean` | Average value check |
| Sum | `hasSum` | `has_sum` | Total sum validation |
| Standard Deviation | `hasStandardDeviation` | `has_standard_deviation` | Statistical spread |
| Quantiles | `hasApproxQuantile`, `hasExactQuantile` | `has_approx_quantile` | Term only has approximate |
| Correlation | `hasCorrelation` | `has_correlation` | Column correlation |
| Entropy | `hasEntropy` | `has_entropy` | Information complexity |
| Mutual Information | `hasMutualInformation` | `has_mutual_information` | Column dependency |

### Custom Validation
| Constraint | Deequ | Term | Notes |
|------------|-------|------|-------|
| Compliance/Satisfies | `hasCompliance` | `satisfies` | Custom SQL expressions |

## 🔴 Deequ Constraints NOT in Term

1. **Column Count** - Validates the number of columns in a dataset
   - Deequ: `hasColumnCount`
   - Term: ❌ Not implemented

2. **Histogram** - Analyzes value distribution
   - Deequ: `hasHistogramValues`
   - Term: ❌ Not implemented

3. **Distinctness** - Ratio of distinct values (different from uniqueness)
   - Deequ: `hasDistinctness`
   - Term: ❌ Not implemented

4. **Unique Value Ratio** - Proportion of unique values
   - Deequ: `hasUniqueValueRatio`
   - Term: ❌ Not implemented

5. **Data Type Distribution** - Checks data type consistency
   - Deequ: `hasDataType`
   - Term: ❌ Not implemented

6. **Approximate Count Distinct** - Efficient unique count estimation
   - Deequ: `hasApproxCountDistinct`
   - Term: ❌ Not implemented

7. **String Length Constraints**
   - Deequ: `hasMaxLength`, `hasMinLength`
   - Term: ❌ Not implemented

8. **Exact Quantiles** - Precise quantile calculations
   - Deequ: `hasExactQuantile`
   - Term: ❌ Only has approximate quantiles

## 🟢 Term Constraints NOT in Deequ

1. **Security-Focused Pattern Matching**
   - `contains_credit_card_number` - PCI compliance
   - `contains_social_security_number` - PII detection
   - `contains_url` - URL validation
   - `contains_email` - Email validation

2. **Multiple Column Completeness**
   - `are_complete` - All columns must be complete
   - `are_any_complete` - At least one column must be complete

3. **Consistency Checks**
   - `has_consistency` - Cross-dataset or temporal consistency validation

## 📊 Coverage Summary

### Term has implemented:
- ✅ Core data quality constraints (completeness, uniqueness, patterns)
- ✅ Statistical constraints (min, max, mean, sum, std dev)
- ✅ Advanced statistics (correlation, entropy, mutual information)
- ✅ Custom SQL validation
- ✅ Security-focused pattern matching (unique to Term)
- ✅ Multi-column completeness variants (unique to Term)

### Term is missing:
- ❌ Column count validation
- ❌ Histogram analysis
- ❌ Distinctness ratio
- ❌ Unique value ratio
- ❌ Data type distribution
- ❌ Approximate count distinct
- ❌ String length constraints
- ❌ Exact quantiles (only has approximate)

## 🎯 Recommendations for Full Deequ Parity

To achieve full Deequ parity, Term should implement:

1. **High Priority** (commonly used):
   - String length constraints (`has_min_length`, `has_max_length`)
   - Data type distribution (`has_data_type`)
   - Distinctness ratio (`has_distinctness`)

2. **Medium Priority** (useful for specific use cases):
   - Column count (`has_column_count`)
   - Unique value ratio (`has_unique_value_ratio`)
   - Approximate count distinct (`has_approx_count_distinct`)

3. **Low Priority** (less commonly used):
   - Histogram analysis (`has_histogram`)
   - Exact quantiles (`has_exact_quantile`)

## 🚀 Term's Advantages

1. **Security Focus**: Built-in PII/PCI detection patterns
2. **Rust Performance**: Native performance without JVM overhead
3. **DataFusion Integration**: Leverages modern columnar execution
4. **Query Optimizer**: Automatic optimization for multiple constraints
5. **Async-First**: Built for modern async/await patterns

## 📝 Migration Guide

For users migrating from Deequ to Term:

| Deequ | Term Equivalent | Notes |
|-------|-----------------|-------|
| `hasCompleteness("col", _ >= 0.9)` | `has_completeness("col", 0.9)` | Direct mapping |
| `hasPattern("col", pattern, _ >= 0.9)` | `has_pattern("col", pattern, 0.9)` | Direct mapping |
| `hasCompliance("rule", predicate)` | `satisfies(predicate, Some("rule"))` | Syntax difference |
| `hasDataType("col", ...)` | ❌ Not available | Use custom SQL |
| `hasMinLength("col", 5)` | ❌ Not available | Use `satisfies("LENGTH(col) >= 5")` |

## Conclusion

Term covers approximately **75%** of Deequ's constraint functionality, with a focus on the most commonly used data quality checks. The missing 25% consists mainly of specialized constraints that can often be replicated using Term's `satisfies` constraint with custom SQL expressions.
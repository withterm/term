//! KLL (Karnin-Lang-Liberty) sketch implementation for memory-efficient approximate quantile computation.
//!
//! The KLL sketch is a streaming algorithm that maintains a small space summary of a data stream,
//! allowing for approximate quantile queries with provable error bounds. It uses O(k log n) memory
//! where k controls the accuracy/memory tradeoff and n is the number of items processed.

use std::cmp::Ordering;
use std::f64;

use crate::error::{Result, TermError};

/// A compactor holds items and performs periodic compaction operations.
#[derive(Debug, Clone)]
struct Compactor {
    /// Maximum capacity before compaction is triggered
    capacity: usize,
    /// Items stored in this compactor (may be unsorted)
    items: Vec<f64>,
    /// Whether the items are currently sorted
    sorted: bool,
}

impl Compactor {
    /// Creates a new compactor with the specified capacity.
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: Vec::with_capacity(capacity),
            sorted: true,
        }
    }

    /// Adds an item to the compactor.
    fn add(&mut self, value: f64) {
        self.items.push(value);
        self.sorted = false;
    }

    /// Returns true if the compactor is at capacity.
    fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    /// Sorts the items if needed.
    fn ensure_sorted(&mut self) {
        if !self.sorted {
            self.items.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            self.sorted = true;
        }
    }

    /// Performs compaction by randomly selecting half of the items to keep.
    /// Returns the items that were compacted out.
    fn compact(&mut self) -> Vec<f64> {
        self.ensure_sorted();

        // Randomly choose whether to keep odd or even indices
        let keep_odd = rand::random::<bool>();
        let mut compacted = Vec::with_capacity(self.items.len() / 2);
        let mut kept = Vec::with_capacity((self.items.len() + 1) / 2);

        for (i, &item) in self.items.iter().enumerate() {
            if (i % 2 == 1) == keep_odd {
                kept.push(item);
            } else {
                compacted.push(item);
            }
        }

        self.items = kept;
        self.sorted = true;
        compacted
    }

    /// Merges items from another compactor.
    fn merge_items(&mut self, items: Vec<f64>) {
        self.items.extend(items);
        self.sorted = false;
    }

    /// Returns the number of items in the compactor.
    fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true if the compactor is empty.
    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// KLL sketch for approximate quantile computation.
///
/// # Example
///
/// ```rust
/// use term_guard::analyzers::advanced::kll_sketch::KllSketch;
///
/// let mut sketch = KllSketch::new(200);
///
/// // Add values
/// for i in 0..1000 {
///     sketch.update(i as f64);
/// }
///
/// // Query quantiles
/// let median = sketch.get_quantile(0.5).unwrap();
/// let p95 = sketch.get_quantile(0.95).unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct KllSketch {
    /// Controls accuracy/memory tradeoff (higher k = better accuracy)
    k: usize,
    /// Stack of compactors, each with increasing capacity
    compactors: Vec<Compactor>,
    /// Total number of items seen
    n: u64,
    /// Minimum value seen
    min_value: f64,
    /// Maximum value seen
    max_value: f64,
}

impl KllSketch {
    /// Creates a new KLL sketch with the specified k parameter.
    ///
    /// The k parameter controls the accuracy/memory tradeoff:
    /// - Higher k = better accuracy but more memory
    /// - Memory usage is O(k log n)
    /// - Relative error is approximately 1.65 / sqrt(k)
    ///
    /// # Arguments
    ///
    /// * `k` - Controls accuracy (recommended: 200 for ~10% error, 1000 for ~5% error)
    pub fn new(k: usize) -> Self {
        if k < 2 {
            panic!("k must be at least 2");
        }

        Self {
            k,
            compactors: vec![Compactor::new(k)],
            n: 0,
            min_value: f64::INFINITY,
            max_value: f64::NEG_INFINITY,
        }
    }

    /// Updates the sketch with a new value.
    pub fn update(&mut self, value: f64) {
        // Handle NaN values
        if value.is_nan() {
            return;
        }

        self.n += 1;
        self.min_value = self.min_value.min(value);
        self.max_value = self.max_value.max(value);

        // Add to the first compactor
        self.compactors[0].add(value);

        // Cascade compactions if needed
        self.cascade_compact();
    }

    /// Performs cascading compaction when compactors are full.
    fn cascade_compact(&mut self) {
        let mut level = 0;
        
        while level < self.compactors.len() && self.compactors[level].is_full() {
            // Ensure we have a compactor at the next level
            if level + 1 >= self.compactors.len() {
                let capacity = self.k * (1 << (level + 1));
                self.compactors.push(Compactor::new(capacity));
            }

            // Compact current level and add to next level
            let compacted = self.compactors[level].compact();
            self.compactors[level + 1].merge_items(compacted);

            level += 1;
        }
    }

    /// Returns the approximate quantile for the given phi (0 <= phi <= 1).
    ///
    /// # Arguments
    ///
    /// * `phi` - Quantile to compute (0.0 = min, 0.5 = median, 1.0 = max)
    ///
    /// # Returns
    ///
    /// The approximate quantile value, or an error if the sketch is empty.
    pub fn get_quantile(&self, phi: f64) -> Result<f64> {
        if self.n == 0 {
            return Err(TermError::Internal(
                "Cannot compute quantile on empty sketch".to_string(),
            ));
        }

        if !(0.0..=1.0).contains(&phi) {
            return Err(TermError::Internal(format!(
                "Quantile phi must be in [0, 1], got {}",
                phi
            )));
        }

        // Special cases
        if phi == 0.0 {
            return Ok(self.min_value);
        }
        if phi == 1.0 {
            return Ok(self.max_value);
        }

        // Collect all items with their weights
        let mut weighted_items = Vec::new();
        
        for (level, compactor) in self.compactors.iter().enumerate() {
            let weight = 1u64 << level;
            for &item in &compactor.items {
                weighted_items.push((item, weight));
            }
        }

        // Sort by value
        weighted_items.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

        // Find the quantile
        let target_rank = (phi * self.n as f64) as u64;
        let mut cumulative_weight = 0u64;

        for (value, weight) in weighted_items {
            cumulative_weight += weight;
            if cumulative_weight >= target_rank {
                return Ok(value);
            }
        }

        // Should not reach here, but return max as fallback
        Ok(self.max_value)
    }

    /// Merges another KLL sketch into this one.
    ///
    /// Both sketches must have the same k parameter.
    pub fn merge(&mut self, other: &KllSketch) -> Result<()> {
        if self.k != other.k {
            return Err(TermError::Internal(format!(
                "Cannot merge sketches with different k values: {} vs {}",
                self.k, other.k
            )));
        }

        // Update metadata
        self.n += other.n;
        self.min_value = self.min_value.min(other.min_value);
        self.max_value = self.max_value.max(other.max_value);

        // Merge compactors
        for (level, other_compactor) in other.compactors.iter().enumerate() {
            // Ensure we have enough compactors
            while level >= self.compactors.len() {
                let capacity = self.k * (1 << self.compactors.len());
                self.compactors.push(Compactor::new(capacity));
            }

            // Merge items
            self.compactors[level].merge_items(other_compactor.items.clone());
        }

        // Cascade compact to maintain invariants
        for level in 0..self.compactors.len() {
            while self.compactors[level].is_full() {
                if level + 1 >= self.compactors.len() {
                    let capacity = self.k * (1 << (level + 1));
                    self.compactors.push(Compactor::new(capacity));
                }

                let compacted = self.compactors[level].compact();
                self.compactors[level + 1].merge_items(compacted);
            }
        }

        Ok(())
    }

    /// Returns the total number of items processed.
    pub fn count(&self) -> u64 {
        self.n
    }

    /// Returns true if the sketch is empty.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Returns the memory usage in bytes (approximate).
    pub fn memory_usage(&self) -> usize {
        let mut usage = std::mem::size_of::<Self>();
        for compactor in &self.compactors {
            usage += std::mem::size_of::<Compactor>();
            usage += compactor.items.capacity() * std::mem::size_of::<f64>();
        }
        usage
    }

    /// Returns the number of compactor levels.
    pub fn num_levels(&self) -> usize {
        self.compactors.len()
    }

    /// Returns the relative error bound for this sketch.
    ///
    /// The actual error may be lower, but is guaranteed not to exceed this bound
    /// with high probability.
    pub fn relative_error_bound(&self) -> f64 {
        1.65 / (self.k as f64).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kll_sketch_basic() {
        let mut sketch = KllSketch::new(100);
        
        // Add values 0..1000
        for i in 0..1000 {
            sketch.update(i as f64);
        }

        assert_eq!(sketch.count(), 1000);
        
        // Check quantiles
        let median = sketch.get_quantile(0.5).unwrap();
        assert!((median - 500.0).abs() < 50.0); // Within 10% error

        let p90 = sketch.get_quantile(0.9).unwrap();
        assert!((p90 - 900.0).abs() < 50.0);
    }

    #[test]
    fn test_kll_sketch_empty() {
        let sketch = KllSketch::new(100);
        assert!(sketch.is_empty());
        assert!(sketch.get_quantile(0.5).is_err());
    }

    #[test]
    fn test_kll_sketch_single_value() {
        let mut sketch = KllSketch::new(100);
        sketch.update(42.0);

        assert_eq!(sketch.get_quantile(0.0).unwrap(), 42.0);
        assert_eq!(sketch.get_quantile(0.5).unwrap(), 42.0);
        assert_eq!(sketch.get_quantile(1.0).unwrap(), 42.0);
    }

    #[test]
    fn test_kll_sketch_merge() {
        let mut sketch1 = KllSketch::new(100);
        let mut sketch2 = KllSketch::new(100);

        // Add 0..500 to sketch1
        for i in 0..500 {
            sketch1.update(i as f64);
        }

        // Add 500..1000 to sketch2
        for i in 500..1000 {
            sketch2.update(i as f64);
        }

        // Merge
        sketch1.merge(&sketch2).unwrap();

        assert_eq!(sketch1.count(), 1000);
        
        // Check that merged sketch has correct quantiles
        let median = sketch1.get_quantile(0.5).unwrap();
        assert!((median - 500.0).abs() < 50.0);
    }

    #[test]
    fn test_kll_sketch_nan_handling() {
        let mut sketch = KllSketch::new(100);
        
        sketch.update(1.0);
        sketch.update(f64::NAN);
        sketch.update(2.0);

        assert_eq!(sketch.count(), 2); // NaN should be ignored
    }

    #[test]
    fn test_kll_sketch_error_bounds() {
        let sketch = KllSketch::new(200);
        let expected_error = 1.65 / (200f64).sqrt();
        assert!((sketch.relative_error_bound() - expected_error).abs() < 0.001);
    }

    #[test]
    fn test_compactor_operations() {
        let mut compactor = Compactor::new(4);
        
        compactor.add(3.0);
        compactor.add(1.0);
        compactor.add(4.0);
        compactor.add(2.0);

        assert!(compactor.is_full());
        
        let compacted = compactor.compact();
        assert_eq!(compacted.len(), 2);
        assert_eq!(compactor.len(), 2);
        assert!(compactor.sorted);
    }
}
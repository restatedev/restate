use crate::types::PartitionKey;
use std::hash::{Hash, Hasher};

/// Computes the [`PartitionKey`] based on xxh3 hashing.
pub struct HashPartitioner;

impl HashPartitioner {
    pub fn compute_partition_key(value: &impl Hash) -> PartitionKey {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        value.hash(&mut hasher);
        hasher.finish()
    }
}

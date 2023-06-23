use crate::types::PartitionKey;
use std::hash::{Hash, Hasher};

/// Computes the [`PartitionKey`] based on xxh3 hashing.
pub struct HashPartitioner;

impl HashPartitioner {
    pub fn compute_partition_key(value: &impl Hash) -> PartitionKey {
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        value.hash(&mut hasher);
        // Safe to only take the lower 32 bits: See https://github.com/Cyan4973/xxHash/issues/453#issuecomment-696838445
        hasher.finish() as u32
    }
}

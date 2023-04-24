use crate::ScanError;
use restate_storage_rocksdb::{RocksDBKey, TableKind};

#[derive(Clone)]
pub(crate) struct RocksDBRange(pub Option<RocksDBKey>, pub Option<RocksDBKey>);

impl RocksDBRange {
    pub fn table_kind(&self) -> Result<TableKind, ScanError> {
        let start = match self.0 {
            Some(RocksDBKey::Full(table, _) | RocksDBKey::Partial(table, _)) => Some(table),
            None => None,
        };
        let end = match self.1 {
            Some(RocksDBKey::Full(table, _) | RocksDBKey::Partial(table, _)) => Some(table),
            None => None,
        };

        match (start, end) {
            (Some(start), Some(end)) => {
                if start.eq(&end) {
                    Ok(start)
                } else {
                    Err(ScanError::StartAndEndMustBeSameTable(start, end))
                }
            }
            (Some(table), None) | (None, Some(table)) => Ok(table),
            (None, None) => Err(ScanError::StartOrEndMustBeProvided),
        }
    }
}

impl rocksdb::IterateBounds for RocksDBRange {
    fn into_bounds(self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        (
            self.0.map(|key| match key {
                RocksDBKey::Full(_, key) | RocksDBKey::Partial(_, key) => key,
            }),
            self.1.map(|key| match key {
                // rocksDB upper bounds are exclusive; lexicographically increment our inclusive upper bound
                RocksDBKey::Full(_, key) | RocksDBKey::Partial(_, key) => increment(key),
            }),
        )
    }
}

// increment turns a byte slice into the lexicographically successive byte slice
// this is used because we would prefer a closed range api ie [1,3] but RocksDB requires half-open ranges ie [1,3)
fn increment(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in bytes.iter_mut().rev() {
        match byte.checked_add(1) {
            Some(incremented) => {
                *byte = incremented;
                return bytes;
            }
            None => continue,
        }
    }
    bytes.push(0);
    bytes
}

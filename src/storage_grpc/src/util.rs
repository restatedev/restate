use crate::ScanError;
use restate_storage_rocksdb::{RocksDBKey, TableKind};

#[derive(Clone)]
pub(crate) struct RocksDBRange {
    pub start: RocksDBKey,
    pub end: RocksDBKey,
}

impl RocksDBRange {
    pub fn table_kind(&self) -> Result<TableKind, ScanError> {
        let start = self.start.table();
        let end = self.end.table();

        if start.eq(&end) {
            Ok(start)
        } else {
            Err(ScanError::StartAndEndMustBeSameTable(start, end))
        }
    }
}

impl rocksdb::IterateBounds for RocksDBRange {
    fn into_bounds(self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let start = self.start.key();
        let start = if start.is_empty() { None } else { Some(start) };
        (start, increment(self.end.key()))
    }
}

// increment turns a byte slice into the lexicographically successive non-matching byte slice
// this is used because we would prefer a closed range api ie [1,3] but RocksDB requires half-open ranges ie [1,3)
fn increment(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    for byte in bytes.iter_mut().rev() {
        if let Some(incremented) = byte.checked_add(1) {
            *byte = incremented;
            return Some(bytes);
        }
    }
    None
}

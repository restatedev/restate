use crate::ScanError;
use restate_storage_rocksdb::{RocksDBKey, TableKind};

#[derive(Clone)]
pub(crate) struct RocksDBRange {
    pub start: Option<RocksDBKey>,
    pub end: Option<RocksDBKey>,
}

impl RocksDBRange {
    pub fn table_kind(&self) -> Result<TableKind, ScanError> {
        let start = self.start.as_ref().map(|start| start.table());
        let end = self.end.as_ref().map(|end| end.table());

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
            self.start.map(|key| key.key().clone()),
            self.end.and_then(|key| increment(key.key().clone())),
        )
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

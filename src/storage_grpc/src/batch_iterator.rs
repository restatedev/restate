use crate::storage::v1::{Batch, Key, Pair};
use bytes::Bytes;
use restate_storage_api::StorageError;
use restate_storage_rocksdb::TableKind;

pub(crate) struct BatchIterator<'a> {
    table: TableKind,
    inner: restate_storage_rocksdb::DBIterator<'a>,
    size: usize,
    done: bool,
}

impl<'a> BatchIterator<'a> {
    pub(crate) fn new(
        table: TableKind,
        iterator: restate_storage_rocksdb::DBIterator<'a>,
        size: usize,
    ) -> BatchIterator {
        Self {
            table,
            inner: iterator,
            size,
            done: false,
        }
    }
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = Result<Batch, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut batch = Vec::new();
        while let Some((k, v)) = self.inner.item() {
            let key = match restate_storage_rocksdb::Key::from_bytes(self.table, k) {
                Err(err) => return Some(Err(err)),
                Ok(key) => key,
            };
            batch.push(Pair {
                key: Some(Key {
                    key: Some(key.into()),
                }),
                value: Bytes::copy_from_slice(v),
            });
            self.inner.next();
            if batch.len() >= self.size {
                return Some(Ok(Batch { items: batch }));
            }
        }

        self.done = true;
        if let Err(err) = self.inner.status() {
            return Some(Err(StorageError::Generic(err.into())));
        }

        if batch.is_empty() {
            None
        } else {
            Some(Ok(Batch { items: batch }))
        }
    }
}

use crate::storage::v1::{Key, Pair};
use bytes::Bytes;
use restate_storage_api::StorageError;
use restate_storage_rocksdb::TableKind;

pub(crate) struct DBIterator<'a> {
    table: TableKind,
    inner: restate_storage_rocksdb::DBIterator<'a>,
    done: bool,
}

impl<'a> DBIterator<'a> {
    pub(crate) fn new(
        table: TableKind,
        iterator: restate_storage_rocksdb::DBIterator<'a>,
    ) -> DBIterator {
        Self {
            table,
            inner: iterator,
            done: false,
        }
    }
}

impl<'a> Iterator for DBIterator<'a> {
    type Item = Result<Pair, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if let Some((k, v)) = self.inner.item() {
            let key = match restate_storage_rocksdb::Key::from_bytes(self.table, k) {
                Err(err) => return Some(Err(err)),
                Ok(key) => key,
            };
            let res = Ok(Pair {
                key: Some(Key {
                    key: Some(key.into()),
                }),
                value: Bytes::copy_from_slice(v),
            });
            self.inner.next();
            Some(res)
        } else {
            self.done = true;
            if let Err(err) = self.inner.status() {
                Some(Err(StorageError::Generic(err.into())))
            } else {
                None
            }
        }
    }
}

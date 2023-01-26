#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TableKind {
    State,
    Inbox,
    Outbox,
    Deduplication,
    PartitionStateMachine,
    Timers,
}

pub trait StorageDeserializer {
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Self;
}

pub trait StorageReader {
    fn get<K: AsRef<[u8]>, V: StorageDeserializer>(&self, table: TableKind, key: K) -> Option<V>;

    fn copy_prefix_into<P, K, V>(
        &self,
        table: TableKind,
        start_key: P,
        start_key_prefix_len: usize,
        target: &mut Vec<(K, V)>,
    ) where
        P: AsRef<[u8]>,
        K: StorageDeserializer,
        V: StorageDeserializer;
}

pub trait Storage: StorageReader {
    type WriteTransactionType<'a>: WriteTransaction<'a>
    where
        Self: 'a;

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, table: TableKind, key: K, value: V);

    #[allow(clippy::needless_lifetimes)]
    fn transaction<'a>(&'a self) -> Self::WriteTransactionType<'a>;
}

pub trait WriteTransaction<'a> {
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, table: TableKind, key: K, value: V);

    fn delete(&mut self, table: TableKind, key: impl AsRef<[u8]>);

    fn commit(self);
}

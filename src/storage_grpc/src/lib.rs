use crate::batch_iterator::BatchIterator;
use crate::storage::v1::{
    key, scan_request::Filter, storage_server, Batch, Key, Pair, Range, ScanRequest,
};
use crate::util::RocksDBRange;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
pub use options::Options;
use restate_storage_api::Storage;
use restate_storage_rocksdb::{RocksDBKey, RocksDBStorage, RocksDBTransaction, TableKind};
pub use scanner_proto::storage;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic::{Request, Response};

mod batch_iterator;
mod options;
mod scanner_proto;
mod util;

pub struct StorageService {
    db: RocksDBStorage,
    addr: SocketAddr,
}

impl StorageService {
    pub fn new(db: RocksDBStorage, addr: SocketAddr) -> Self {
        Self { db, addr }
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), tonic::transport::Error> {
        let addr = self.addr;
        Server::builder()
            .add_service(storage_server::StorageServer::new(self))
            .serve_with_shutdown(addr, drain.signaled().map(|_| ()))
            .await
    }
}

#[derive(Debug, thiserror::Error)]
enum ScanError {
    #[error("start and end keys must refer to the same table. start: {0:?} end: {1:?}")]
    StartAndEndMustBeSameTable(TableKind, TableKind),
    #[error("a full key must be provided the key filter")]
    FullKeyMustBeProvided,
    #[error("a full or partial key must be provided for prefix scans; for full scan provide a key struct with None fields")]
    PartialOrFullKeyMustBeProvided,
    #[error(
        "either a start or end key is needed to infer the table; for full scan use an empty prefix"
    )]
    StartOrEndMustBeProvided,
    #[error("a filter must be provided")]
    FilterMustBeProvided,
}

#[tonic::async_trait]
impl storage_server::Storage for StorageService {
    type ScanStream = BoxStream<'static, Result<Batch, tonic::Status>>;

    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, tonic::Status> {
        let request = request.into_inner();

        let filter = match request.filter {
            None => {
                return Err(tonic::Status::internal(format!(
                    "{}",
                    ScanError::FilterMustBeProvided
                )));
            }
            Some(filter) => filter,
        };

        let tx = self.db.transaction();
        let stream = filter_stream(tx, filter)
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;

        Ok(Response::new(stream.boxed() as Self::ScanStream))
    }
}

fn filter_stream(
    tx: RocksDBTransaction,
    filter: Filter,
) -> Result<BoxStream<'static, Result<Batch, tonic::Status>>, ScanError> {
    match filter {
        Filter::Key(Key { key: None }) => Err(ScanError::FullKeyMustBeProvided.into()),
        Filter::Prefix(Key { key: None }) => Err(ScanError::PartialOrFullKeyMustBeProvided.into()),
        Filter::Range(Range {
            start: None,
            end: None,
        }) => Err(ScanError::StartOrEndMustBeProvided.into()),
        Filter::Key(Key { key: Some(key) }) => handle_key(tx, key),
        Filter::Prefix(Key { key: Some(prefix) }) => handle_prefix(tx, prefix),
        Filter::Range(range) => handle_range(tx, range),
    }
}

fn handle_key(
    tx: RocksDBTransaction,
    key: key::Key,
) -> Result<BoxStream<'static, Result<Batch, tonic::Status>>, ScanError> {
    let (table, key_bytes) =
        match Into::<restate_storage_rocksdb::Key>::into(key.clone()).to_bytes() {
            RocksDBKey::Partial(_, _) => return Err(ScanError::FullKeyMustBeProvided.into()),
            RocksDBKey::Full(table, key_bytes) => (table, key_bytes),
        };
    Ok(tx
        .spawn_background_scan(
            move |db, tx| match db.get_owned(table, key_bytes).transpose() {
                None => (),
                Some(result) => tx
                    .blocking_send(match result {
                        Ok(bytes) => Ok(Batch {
                            items: vec![Pair {
                                key: Some(Key { key: Some(key) }),
                                value: bytes,
                            }],
                        }),
                        Err(err) => Err(err),
                    })
                    .unwrap_or_else(|err| {
                        tracing::error!("unable to send batch during key query: {err}")
                    }),
            },
        )
        .map_err(|err| tonic::Status::internal(err.to_string()))
        .boxed())
}

fn handle_prefix(
    tx: RocksDBTransaction,
    prefix: key::Key,
) -> Result<BoxStream<'static, Result<Batch, tonic::Status>>, ScanError> {
    let (table, prefix) = match Into::<restate_storage_rocksdb::Key>::into(prefix).to_bytes() {
        RocksDBKey::Partial(table, prefix) | RocksDBKey::Full(table, prefix) => (table, prefix),
    };
    Ok(tx
        .spawn_background_scan(move |db, tx| {
            let mut iter = db.prefix_iterator(table, prefix.clone());
            iter.seek(prefix);
            BatchIterator::new(table, iter, 50)
                .try_for_each(|b| tx.blocking_send(b))
                .unwrap_or_else(|err| {
                    tracing::error!("unable to send result during prefix query: {err}")
                })
        })
        .map_err(|err| tonic::Status::internal(err.to_string()))
        .boxed())
}

fn handle_range(
    tx: RocksDBTransaction,
    Range { start, end }: Range,
) -> Result<BoxStream<'static, Result<Batch, tonic::Status>>, ScanError> {
    let (start, end) = (
        start
            .and_then(|k| k.key)
            .map(|k| Into::<restate_storage_rocksdb::Key>::into(k).to_bytes()),
        end.and_then(|k| k.key)
            .map(|k| Into::<restate_storage_rocksdb::Key>::into(k).to_bytes()),
    );
    let range = RocksDBRange(start.clone(), end);
    let table = range.table_kind()?;

    Ok(tx
        .spawn_background_scan(move |db, tx| {
            let mut iter = db.range_iterator(table, range);

            if let Some(RocksDBKey::Partial(_, start) | RocksDBKey::Full(_, start)) = start {
                iter.seek(start);
            } else {
                iter.seek_to_first();
            }
            BatchIterator::new(table, iter, 50)
                .try_for_each(|b| tx.blocking_send(b))
                .unwrap_or_else(|err| {
                    tracing::error!("unable to send result during range query: {err}")
                })
        })
        .map_err(|err| tonic::Status::internal(err.to_string()))
        .boxed())
}

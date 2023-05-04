use crate::iterator::DBIterator;
use crate::storage::v1::{
    key, scan_request::Filter, storage_server, Key, Pair, Range, ScanRequest,
};
use crate::util::RocksDBRange;
use codederror::CodedError;
use futures::stream::BoxStream;
use futures::TryFutureExt;
use futures::{FutureExt, StreamExt, TryStreamExt};
pub use options::Options;
use restate_storage_api::{GetStream, Storage, StorageError};
use restate_storage_rocksdb::{RocksDBKey, RocksDBStorage, RocksDBTransaction, TableKind};
pub use scanner_proto::storage;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic::{Request, Response};

mod iterator;
mod options;
mod scanner_proto;
mod util;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum Error {
    #[error("failed binding to address '{0}' specified in 'worker.storage_grpc.bind_address'")]
    #[code(restate_errors::RT0004)]
    AddrInUse(SocketAddr),
    #[error("tonic error: {0:?}")]
    #[code(unknown)]
    Other(#[from] tonic::transport::Error),
}

pub struct StorageService {
    db: RocksDBStorage,
    addr: SocketAddr,
}

impl StorageService {
    pub fn new(db: RocksDBStorage, addr: SocketAddr) -> Self {
        Self { db, addr }
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), Error> {
        let addr = self.addr;
        Server::builder()
            .add_service(storage_server::StorageServer::new(self))
            .serve_with_shutdown(addr, drain.signaled().map(|_| ()))
            .map_err(|err| {
                if is_addr_in_use_err(&err) {
                    Error::AddrInUse(addr)
                } else {
                    Error::Other(err)
                }
            })
            .await
    }
}

fn is_addr_in_use_err(err: &tonic::transport::Error) -> bool {
    let err_msg = format!("{err:?}");
    err_msg.contains("kind: AddrInUse")
}

#[derive(Debug, thiserror::Error)]
enum ScanError {
    #[error("start and end keys must refer to the same table. start: {0:?} end: {1:?}")]
    StartAndEndMustBeSameTable(TableKind, TableKind),
    #[error("a full key must be provided for the key filter")]
    FullKeyMustBeProvided,
    #[error("a full or partial key must be provided for prefix scans; for full scan provide a key struct with None fields")]
    PartialOrFullKeyMustBeProvided,
    #[error(
        "structured start and end keys must be provided; set all fields to None to represent no bound"
    )]
    StartAndEndMustBeProvided,
    #[error("a filter must be provided")]
    FilterMustBeProvided,
}

impl From<ScanError> for tonic::Status {
    fn from(value: ScanError) -> Self {
        tonic::Status::invalid_argument(value.to_string())
    }
}

#[tonic::async_trait]
impl storage_server::Storage for StorageService {
    type ScanStream = BoxStream<'static, Result<Pair, tonic::Status>>;

    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, tonic::Status> {
        let request = request.into_inner();

        let filter = match request.filter {
            None => {
                return Err(tonic::Status::invalid_argument(
                    ScanError::FilterMustBeProvided.to_string(),
                ));
            }
            Some(filter) => filter,
        };

        let db = self.db.transaction();
        let stream = filter_stream(db, filter)
            .map_err(|err| tonic::Status::invalid_argument(err.to_string()))?;
        let stream = stream.map_err(|err| tonic::Status::internal(err.to_string()));

        Ok(Response::new(stream.boxed() as Self::ScanStream))
    }
}

fn filter_stream(
    db: RocksDBTransaction,
    filter: Filter,
) -> Result<BoxStream<'static, Result<Pair, StorageError>>, ScanError> {
    match filter {
        Filter::Key(Key { key: None }) => Err(ScanError::FullKeyMustBeProvided),
        Filter::Prefix(Key { key: None }) => Err(ScanError::PartialOrFullKeyMustBeProvided),
        Filter::Range(
            Range {
                start: None | Some(Key { key: None }),
                end: _,
            }
            | Range {
                start: _,
                end: None | Some(Key { key: None }),
            },
        ) => Err(ScanError::StartAndEndMustBeProvided),
        Filter::Key(Key { key: Some(key) }) => handle_key(db, key),
        Filter::Prefix(Key { key: Some(prefix) }) => Ok(handle_prefix(db, prefix)),
        Filter::Range(Range {
            start: Some(Key { key: Some(start) }),
            end: Some(Key { key: Some(end) }),
        }) => handle_range(db, start, end),
    }
}

fn handle_key(
    db: RocksDBTransaction,
    key: key::Key,
) -> Result<GetStream<'static, Pair>, ScanError> {
    let (table, key_bytes) = match restate_storage_rocksdb::Key::from(key.clone()).to_bytes() {
        RocksDBKey::Partial(_, _) => return Err(ScanError::FullKeyMustBeProvided),
        RocksDBKey::Full(table, key_bytes) => (table, key_bytes),
    };
    Ok(db
        .spawn_background_scan(
            move |db, tx| match db.get_owned(table, key_bytes).transpose() {
                None => (),
                Some(result) => tx
                    .blocking_send(result.map(|bytes| Pair {
                        key: Some(Key { key: Some(key) }),
                        value: bytes,
                    }))
                    .unwrap_or_else(|err| {
                        tracing::debug!("unable to send pair during key query: {err}")
                    }),
            },
        )
        .boxed())
}

fn handle_prefix(db: RocksDBTransaction, prefix: key::Key) -> GetStream<'static, Pair> {
    let (table, prefix) = match restate_storage_rocksdb::Key::from(prefix).to_bytes() {
        RocksDBKey::Partial(table, prefix) | RocksDBKey::Full(table, prefix) => (table, prefix),
    };
    db.spawn_background_scan(move |db, tx| {
        let mut iter = db.prefix_iterator(table, prefix.clone());
        iter.seek(prefix);
        DBIterator::new(table, iter)
            .try_for_each(|b| tx.blocking_send(b))
            .unwrap_or_else(|err| {
                tracing::debug!("unable to send result during prefix query: {err}")
            })
    })
    .boxed()
}

fn handle_range(
    db: RocksDBTransaction,
    start: key::Key,
    end: key::Key,
) -> Result<GetStream<'static, Pair>, ScanError> {
    let start = restate_storage_rocksdb::Key::from(start).to_bytes();
    let end = restate_storage_rocksdb::Key::from(end).to_bytes();

    let range = RocksDBRange {
        start: start.clone(),
        end,
    };
    let table = range.table_kind()?;

    Ok(db
        .spawn_background_scan(move |db, tx| {
            let mut iter = db.range_iterator(table, range);

            iter.seek(start.key());
            DBIterator::new(table, iter)
                .try_for_each(|b| tx.blocking_send(b))
                .unwrap_or_else(|err| {
                    tracing::debug!("unable to send result during range query: {err}")
                })
        })
        .boxed())
}

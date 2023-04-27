use bytes::Bytes;
use std::io::Cursor;
use std::net::SocketAddr;

use codederror::CodedError;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use KeyWrapper::{Dedup, Fsm, Inbox, Journal, Outbox, State};

use crate::scanner_proto::KeyWrapper;
pub use options::Options;
use restate_storage_api::{GetStream, Storage, StorageError};
use restate_storage_rocksdb::keys::TableKey;
use restate_storage_rocksdb::{
    scan::TableScan, RocksDBStorage, RocksDBTransaction, TableKind, TableScanIterationDecision,
};
pub use scanner_proto::storage;

use crate::storage::v1::{scan_request::Filter, storage_server, Key, Pair, Range, ScanRequest};

use futures::TryFutureExt;

mod options;
mod scanner_proto;

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

impl From<ScanError> for Status {
    fn from(value: ScanError) -> Self {
        Status::invalid_argument(value.to_string())
    }
}

#[tonic::async_trait]
impl storage_server::Storage for StorageService {
    type ScanStream = BoxStream<'static, Result<Pair, Status>>;

    async fn scan(
        &self,
        request: Request<ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let request = request.into_inner();
        let filter = request
            .filter
            .ok_or_else(|| Status::invalid_argument(ScanError::FilterMustBeProvided.to_string()))?;
        let db = self.db.transaction();
        let stream =
            filter_stream(db, filter).map_err(|err| Status::invalid_argument(err.to_string()))?;
        let stream = stream.map_err(|err| Status::internal(err.to_string()));

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
        Filter::Key(Key { key: Some(key) }) => handle_key(db, key.into()),
        Filter::Prefix(Key { key: Some(key) }) => handle_prefix(db, key.into()),
        Filter::Range(Range {
            start: Some(Key { key: Some(start) }),
            end: Some(Key { key: Some(end) }),
        }) => handle_range(db, start.into(), end.into()),
    }
}

fn handle_key(
    db: RocksDBTransaction,
    key: KeyWrapper,
) -> Result<GetStream<'static, Pair>, ScanError> {
    match key {
        Dedup(k) => do_key(db, k),
        Fsm(k) => do_key(db, k),
        Inbox(k) => do_key(db, k),
        Journal(k) => do_key(db, k),
        Outbox(k) => do_key(db, k),
        State(k) => do_key(db, k),
        KeyWrapper::Status(k) => do_key(db, k),
        KeyWrapper::Timer(k) => do_key(db, k),
    }
}

fn handle_prefix(
    db: RocksDBTransaction,
    key: KeyWrapper,
) -> Result<GetStream<'static, Pair>, ScanError> {
    match key {
        Dedup(k) => do_prefix(db, k),
        Fsm(k) => do_prefix(db, k),
        Inbox(k) => do_prefix(db, k),
        Journal(k) => do_prefix(db, k),
        Outbox(k) => do_prefix(db, k),
        State(k) => do_prefix(db, k),
        KeyWrapper::Status(k) => do_prefix(db, k),
        KeyWrapper::Timer(k) => do_prefix(db, k),
    }
}

fn handle_range(
    db: RocksDBTransaction,
    start: KeyWrapper,
    end: KeyWrapper,
) -> Result<GetStream<'static, Pair>, ScanError> {
    match (start, end) {
        (Dedup(s), Dedup(e)) => do_range(db, s, e),
        (Fsm(s), Fsm(e)) => do_range(db, s, e),
        (Inbox(s), Inbox(e)) => do_range(db, s, e),
        (Journal(s), Journal(e)) => do_range(db, s, e),
        (Outbox(s), Outbox(e)) => do_range(db, s, e),
        (State(s), State(e)) => do_range(db, s, e),
        (KeyWrapper::Status(s), KeyWrapper::Status(e)) => do_range(db, s, e),
        (KeyWrapper::Timer(s), KeyWrapper::Timer(e)) => do_range(db, s, e),
        (l, r) => Err(ScanError::StartAndEndMustBeSameTable(l.table(), r.table())),
    }
}

fn do_key<K: TableKey + Into<Key>>(
    db: RocksDBTransaction,
    key: K,
) -> Result<GetStream<'static, Pair>, ScanError> {
    if !key.is_complete() {
        return Err(ScanError::FullKeyMustBeProvided);
    }
    let scan = TableScan::KeyPrefix(key);
    Ok(do_scan(db, scan))
}

fn do_prefix<K: TableKey + Into<Key>>(
    db: RocksDBTransaction,
    key: K,
) -> Result<GetStream<'static, Pair>, ScanError> {
    Ok(do_scan(db, TableScan::KeyPrefix(key)))
}

fn do_range<K: TableKey + Into<Key>>(
    db: RocksDBTransaction,
    start: K,
    end: K,
) -> Result<GetStream<'static, Pair>, ScanError> {
    let scan = TableScan::KeyRangeInclusive(start, end);
    Ok(do_scan(db, scan))
}

fn do_scan<K: TableKey + Into<Key>>(
    db: RocksDBTransaction,
    scan: TableScan<K>,
) -> GetStream<'static, Pair> {
    db.for_each_key_value(scan, move |k, v| {
        TableScanIterationDecision::Emit(deserialize_key_value::<K>(k, v))
    })
}

pub(crate) fn deserialize_key_value<K: TableKey + Into<Key>>(
    k: &[u8],
    v: &[u8],
) -> Result<Pair, StorageError> {
    let key = K::deserialize_from(&mut Cursor::new(k))?;
    Ok(Pair {
        key: Some(key.into()),
        value: Bytes::copy_from_slice(v),
    })
}

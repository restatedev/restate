mod options;

use crate::storage::v1::{
    key, scan_request::Filter, storage_server, Batch, Deduplication, Inbox, Journal, Key, Outbox,
    Pair, PartitionStateMachine, Range, ScanRequest, State, Status, Timers,
};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
pub use options::Options;
use restate_common::utils::GenericError;
use restate_storage_api::{Storage, StorageError};
use restate_storage_rocksdb::{RocksDBKey, RocksDBStorage, TableKind};
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic::{Request, Response};

pub mod storage {
    pub mod v1 {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.storage.scan.v1.rs"));
    }
}

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

struct BatchIterator<'a> {
    table: restate_storage_rocksdb::TableKind,
    inner: restate_storage_rocksdb::DBIterator<'a>,
    size: usize,
    done: bool,
}

impl<'a> BatchIterator<'a> {
    fn new(
        table: restate_storage_rocksdb::TableKind,
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

#[derive(Debug, thiserror::Error)]
enum ScanError {
    #[error("start and end keys must refer to the same table. start: {0:?} end: {1:?}")]
    StartAndEndMustBeSameTable(
        restate_storage_rocksdb::TableKind,
        restate_storage_rocksdb::TableKind,
    ),
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
                )))
            }
            Some(filter) => filter,
        };

        let tx = self.db.transaction();
        Ok(Response::new(
            match filter {
                Filter::Key(Key { key: None }) => stream::once(futures::future::ready(Err(
                    ScanError::FullKeyMustBeProvided.into(),
                )))
                .boxed(),
                Filter::Prefix(Key { key: None }) => stream::once(futures::future::ready(Err(
                    ScanError::PartialOrFullKeyMustBeProvided.into(),
                )))
                .boxed(),
                Filter::Range(Range {
                    start: None,
                    end: None,
                }) => stream::once(futures::future::ready(Err(
                    ScanError::StartOrEndMustBeProvided.into(),
                )))
                .boxed(),
                Filter::Key(Key { key: Some(key) }) => {
                    match Into::<restate_storage_rocksdb::Key>::into(key.clone()).to_bytes() {
                        RocksDBKey::Partial(_, _) => stream::once(futures::future::ready(Err(
                            ScanError::FullKeyMustBeProvided.into(),
                        )))
                        .boxed(),
                        RocksDBKey::Full(table, key_bytes) => tx
                            .spawn_background_scan(move |db, tx| {
                                match db.get_owned(table, key_bytes).transpose() {
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
                                            tracing::error!(
                                                "unable to send batch during key query: {err}"
                                            )
                                        }),
                                }
                            })
                            .map_err(|err| err.into())
                            .boxed(),
                    }
                }
                Filter::Prefix(Key { key: Some(prefix) }) => {
                    match Into::<restate_storage_rocksdb::Key>::into(prefix).to_bytes() {
                        // partial or full keys are fine for prefix queries
                        RocksDBKey::Partial(table, prefix) | RocksDBKey::Full(table, prefix) => tx
                            .spawn_background_scan(move |db, tx| {
                                let mut iter = db.prefix_iterator(table, prefix.clone());
                                iter.seek(prefix);
                                BatchIterator::new(table, iter, 50)
                                    .try_for_each(|b| tx.blocking_send(b))
                                    .unwrap_or_else(|err| {
                                        tracing::error!(
                                            "unable to send result during prefix query: {err}"
                                        )
                                    })
                            })
                            .map_err(|err| err.into())
                            .boxed(),
                    }
                }
                Filter::Range(Range { start, end }) => {
                    let (start, end) = (
                        start
                            .and_then(|k| k.key)
                            .map(|k| Into::<restate_storage_rocksdb::Key>::into(k).to_bytes()),
                        end.and_then(|k| k.key)
                            .map(|k| Into::<restate_storage_rocksdb::Key>::into(k).to_bytes()),
                    );
                    let range = RocksDBRange(start.clone(), end);
                    match range.table_kind() {
                        Ok(table) => tx
                            .spawn_background_scan(move |db, tx| {
                                let mut iter = db.range_iterator(table, range);

                                if let Some(
                                    RocksDBKey::Partial(_, start) | RocksDBKey::Full(_, start),
                                ) = start
                                {
                                    iter.seek(start);
                                } else {
                                    iter.seek_to_first();
                                }
                                BatchIterator::new(table, iter, 50)
                                    .try_for_each(|b| tx.blocking_send(b))
                                    .unwrap_or_else(|err| {
                                        tracing::error!(
                                            "unable to send result during range query: {err}"
                                        )
                                    })
                            })
                            .map_err(|err| err.into())
                            .boxed(),
                        Err(err) => stream::once(futures::future::ready(Err(err.into()))).boxed(),
                    }
                }
            }
            .map_err(|err: GenericError| {
                tonic::Status::internal(format!("Error while streaming results: {err}"))
            })
            .boxed() as Self::ScanStream,
        ))
    }
}

#[derive(Clone)]
struct RocksDBRange(pub Option<RocksDBKey>, pub Option<RocksDBKey>);

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

impl From<restate_storage_rocksdb::Key> for key::Key {
    fn from(value: restate_storage_rocksdb::Key) -> Self {
        match value {
            restate_storage_rocksdb::Key::Deduplication(
                restate_storage_rocksdb::deduplication_table::DeduplicationKeyComponents {
                    partition_id,
                    producing_partition_id,
                },
            ) => key::Key::Deduplication(Deduplication {
                partition_id,
                producing_partition_id,
            }),
            restate_storage_rocksdb::Key::PartitionStateMachine(
                restate_storage_rocksdb::fsm_table::PartitionStateMachineKeyComponents {
                    partition_id,
                    state_id,
                },
            ) => key::Key::PartitionStateMachine(PartitionStateMachine {
                partition_id,
                state_id,
            }),
            restate_storage_rocksdb::Key::Inbox(
                restate_storage_rocksdb::inbox_table::InboxKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                    sequence_number,
                },
            ) => key::Key::Inbox(Inbox {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
                sequence_number,
            }),
            restate_storage_rocksdb::Key::Journal(
                restate_storage_rocksdb::journal_table::JournalKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                    journal_index,
                },
            ) => key::Key::Journal(Journal {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
                journal_index,
            }),
            restate_storage_rocksdb::Key::Outbox(
                restate_storage_rocksdb::outbox_table::OutboxKeyComponents {
                    partition_id,
                    message_index,
                },
            ) => key::Key::Outbox(Outbox {
                partition_id,
                message_index,
            }),
            restate_storage_rocksdb::Key::State(
                restate_storage_rocksdb::state_table::StateKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                    state_key,
                },
            ) => key::Key::State(State {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
                state_key,
            }),
            restate_storage_rocksdb::Key::Status(
                restate_storage_rocksdb::status_table::StatusKeyComponents {
                    partition_key,
                    service_name,
                    service_key,
                },
            ) => key::Key::Status(Status {
                partition_key,
                service_name: service_name.map(|s| s.into()),
                service_key,
            }),
            restate_storage_rocksdb::Key::Timers(
                restate_storage_rocksdb::timer_table::TimersKeyComponents {
                    partition_id,
                    timestamp,
                    service_name,
                    service_key,
                    invocation_id,
                    journal_index,
                },
            ) => key::Key::Timers(Timers {
                partition_id,
                timestamp,
                service_name: service_name.map(|s| s.into()),
                service_key,
                invocation_id,
                journal_index,
            }),
        }
    }
}

impl From<key::Key> for restate_storage_rocksdb::Key {
    fn from(value: key::Key) -> restate_storage_rocksdb::Key {
        match value {
            key::Key::Deduplication(Deduplication {
                partition_id,
                producing_partition_id,
            }) => restate_storage_rocksdb::Key::Deduplication(
                restate_storage_rocksdb::deduplication_table::DeduplicationKeyComponents {
                    partition_id,
                    producing_partition_id,
                },
            ),
            key::Key::PartitionStateMachine(PartitionStateMachine {
                partition_id,
                state_id,
            }) => restate_storage_rocksdb::Key::PartitionStateMachine(
                restate_storage_rocksdb::fsm_table::PartitionStateMachineKeyComponents {
                    partition_id,
                    state_id,
                },
            ),
            key::Key::Inbox(Inbox {
                partition_key,
                service_name,
                service_key,
                sequence_number,
            }) => restate_storage_rocksdb::Key::Inbox(
                restate_storage_rocksdb::inbox_table::InboxKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    sequence_number,
                },
            ),
            key::Key::Journal(Journal {
                partition_key,
                service_name,
                service_key,
                journal_index,
            }) => restate_storage_rocksdb::Key::Journal(
                restate_storage_rocksdb::journal_table::JournalKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    journal_index,
                },
            ),
            key::Key::Outbox(Outbox {
                partition_id,
                message_index,
            }) => restate_storage_rocksdb::Key::Outbox(
                restate_storage_rocksdb::outbox_table::OutboxKeyComponents {
                    partition_id,
                    message_index,
                },
            ),
            key::Key::State(State {
                partition_key,
                service_name,
                service_key,
                state_key,
            }) => restate_storage_rocksdb::Key::State(
                restate_storage_rocksdb::state_table::StateKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    state_key,
                },
            ),
            key::Key::Status(Status {
                partition_key,
                service_name,
                service_key,
            }) => restate_storage_rocksdb::Key::Status(
                restate_storage_rocksdb::status_table::StatusKeyComponents {
                    partition_key,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                },
            ),
            key::Key::Timers(Timers {
                partition_id,
                timestamp,
                service_name,
                service_key,
                invocation_id,
                journal_index,
            }) => restate_storage_rocksdb::Key::Timers(
                restate_storage_rocksdb::timer_table::TimersKeyComponents {
                    partition_id,
                    timestamp,
                    service_name: service_name.map(|s| s.into()),
                    service_key,
                    invocation_id,
                    journal_index,
                },
            ),
        }
    }
}

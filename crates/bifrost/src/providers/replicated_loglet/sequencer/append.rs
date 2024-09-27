// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{cmp::Ordering, sync::Arc, time::Duration};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{sync::OwnedSemaphorePermit, time::timeout};

use restate_core::network::{
    rpc_router::{RpcError, RpcRouter},
    Incoming, NetworkError, Networking, Outgoing, TransportConnect,
};
use restate_types::{
    config::Configuration,
    live::Live,
    logs::{LogletOffset, Record, SequenceNumber, TailState},
    net::log_server::{Status, Store, StoreFlags, Stored},
    replicated_loglet::NodeSet,
};

use super::{BatchExt, SequencerSharedState};
use crate::{
    loglet::LogletCommitResolver,
    providers::replicated_loglet::{
        log_server_manager::{RemoteLogServer, RemoteLogServerManager},
        replication::NodeSetChecker,
    },
};

const DEFAULT_BACKOFF_TIME: Duration = Duration::from_millis(1000);

enum AppenderState {
    Wave {
        // nodes that should be avoided by the spread selector
        graylist: NodeSet,
    },
    Done,
    Backoff,
}

/// Appender makes sure a batch of records will run to completion
pub(crate) struct Appender<T> {
    sequencer_shared_state: Arc<SequencerSharedState>,
    log_server_manager: RemoteLogServerManager,
    store_router: RpcRouter<Store>,
    networking: Networking<T>,
    first_offset: LogletOffset,
    records: Arc<[Record]>,
    // permit is held during the entire live
    // of the batch to limit the number of
    // inflight batches
    permit: Option<OwnedSemaphorePermit>,
    commit_resolver: Option<LogletCommitResolver>,
    configuration: Live<Configuration>,
}

impl<T: TransportConnect> Appender<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sequencer_shared_state: Arc<SequencerSharedState>,
        log_server_manager: RemoteLogServerManager,
        store_router: RpcRouter<Store>,
        networking: Networking<T>,
        first_offset: LogletOffset,
        records: Arc<[Record]>,
        permit: OwnedSemaphorePermit,
        commit_resolver: LogletCommitResolver,
    ) -> Self {
        Self {
            sequencer_shared_state,
            log_server_manager,
            store_router,
            networking,
            first_offset,
            records,
            permit: Some(permit),
            commit_resolver: Some(commit_resolver),
            configuration: Configuration::updateable(),
        }
    }

    pub async fn run(mut self) {
        // initial wave has 0 replicated and 0 gray listed node
        let mut state = AppenderState::Wave {
            graylist: NodeSet::empty(),
        };

        let retry_policy = self
            .configuration
            .live_load()
            .bifrost
            .replicated_loglet
            .sequencer_backoff_strategy
            .clone();

        let mut retry = retry_policy.iter();

        loop {
            state = match state {
                AppenderState::Done => break,
                AppenderState::Wave {
                    graylist: gray_list,
                } => self.wave(gray_list).await,
                AppenderState::Backoff => {
                    // since backoff can be None, or run out of iterations,
                    // but appender should never give up we fall back to fixed backoff
                    let delay = retry.next().unwrap_or(DEFAULT_BACKOFF_TIME);
                    tokio::time::sleep(delay).await;

                    AppenderState::Wave {
                        // todo: introduce some backoff strategy
                        graylist: NodeSet::empty(),
                    }
                }
            }
        }
    }

    async fn wave(&mut self, mut gray_list: NodeSet) -> AppenderState {
        // select the spread
        let spread = match self.sequencer_shared_state.selector.select(
            &mut rand::thread_rng(),
            &self.networking.metadata().nodes_config_ref(),
            &gray_list,
        ) {
            Ok(spread) => spread,
            Err(_) => {
                if gray_list.is_empty() {
                    // gray list was empty during spread selection!
                    // yet we couldn't find a spread. there is
                    // no reason to retry immediately.
                    return AppenderState::Backoff;
                }
                // otherwise, we retry without a gray list.
                return AppenderState::Wave {
                    graylist: NodeSet::empty(),
                };
            }
        };

        let mut gray = false;
        let mut servers = Vec::with_capacity(spread.len());
        for id in spread {
            // at this stage, if we fail to get connection to this server it must be
            // a first time use. We can safely assume this has to be gray listed
            let server = match self.log_server_manager.get(id, &self.networking).await {
                Ok(server) => server,
                Err(err) => {
                    tracing::error!("failed to connect to {}: {}", id, err);
                    gray = true;
                    gray_list.insert(id);
                    continue;
                }
            };

            servers.push(server);
        }

        if gray {
            // Some nodes has been gray listed (wasn't in the original gray list)
            // todo(azmy): we should check if the remaining set of nodes can still achieve
            // write quorum

            // we basically try again with a new set of gray_list
            return AppenderState::Wave {
                graylist: gray_list,
            };
        }

        // otherwise, we try to send the wave.
        self.send_wave(servers).await
    }

    async fn send_wave(&mut self, spread_servers: Vec<RemoteLogServer>) -> AppenderState {
        let last_offset = self.records.last_offset(self.first_offset).unwrap();

        let mut checker = NodeSetChecker::new(
            self.sequencer_shared_state.selector.nodeset(),
            &self.networking.metadata().nodes_config_snapshot(),
            self.sequencer_shared_state.selector.replication_property(),
        );

        // track the in flight server ids
        let mut pending_servers = NodeSet::empty();

        let mut store_tasks = FuturesUnordered::new();

        for server in spread_servers {
            // it is possible that we have visited this server
            // in a previous wave. So we can short circuit here
            // and just skip
            if server.local_tail().latest_offset() > last_offset {
                checker.set_attribute(server.node_id(), true);
                continue;
            }

            pending_servers.insert(server.node_id());

            let task = LogServerStoreTask {
                sequencer_shared_state: &self.sequencer_shared_state,
                server_manager: &self.log_server_manager,
                server,
                networking: &self.networking,
                first_offset: self.first_offset,
                records: &self.records,
                rpc_router: &self.store_router,
            };

            store_tasks.push(task.run());
        }

        loop {
            let store_result = match timeout(
                self.configuration
                    .live_load()
                    .bifrost
                    .replicated_loglet
                    .log_server_timeout,
                store_tasks.next(),
            )
            .await
            {
                Ok(Some(result)) => result,
                Ok(None) => break, //no more tasks
                Err(_err) => {
                    // timed out!
                    // none of the pending tasks has finished in time! we will assume all pending server
                    // are gray listed and try again
                    return AppenderState::Wave {
                        graylist: pending_servers,
                    };
                }
            };

            let LogServerStoreTaskResult { server, status } = store_result;

            let node_id = server.node_id();
            let response = match status {
                StoreTaskStatus::Error(err) => {
                    // couldn't send store command to remote server
                    tracing::error!(node_id=%server.node_id(), "failed to send batch to node {}", err);
                    continue;
                }
                StoreTaskStatus::Sealed(_) => {
                    tracing::trace!(node_id=%server.node_id(), "node is sealed");
                    continue;
                }
                StoreTaskStatus::AdvancedLocalTail(_) => {
                    // node local tail is behind the batch first offset
                    // question(azmy): we assume node has been replicated?
                    checker.set_attribute(node_id, true);
                    pending_servers.remove(&node_id);
                    continue;
                }
                StoreTaskStatus::Stored(stored) => stored,
            };

            // we had a response from this node and there is still a lot we can do
            match response.status {
                Status::Ok => {
                    // only if status is okay that we remove this node
                    // from the gray list, and move to replicated list
                    checker.set_attribute(node_id, true);
                    pending_servers.remove(&node_id);
                }
                _ => {
                    // todo(azmy): handle other status
                    // note: we don't remove the node from the gray list
                }
            }

            if self.commit_resolver.is_some() && checker.check_write_quorum(|attr| *attr) {
                // resolve the commit if not resolved yet
                if let Some(resolver) = self.commit_resolver.take() {
                    self.sequencer_shared_state.record_cache.extend(
                        *self.sequencer_shared_state.loglet_id(),
                        self.first_offset,
                        &self.records,
                    );
                    self.sequencer_shared_state
                        .global_committed_tail()
                        .notify_offset_update(last_offset.next());
                    resolver.offset(last_offset);
                }

                // drop the permit
                self.permit.take();
            }
        }

        if checker.check_write_quorum(|attr| *attr) {
            AppenderState::Done
        } else {
            AppenderState::Wave {
                graylist: pending_servers,
            }
        }
    }
}

enum StoreTaskStatus {
    Sealed(LogletOffset),
    AdvancedLocalTail(LogletOffset),
    Stored(Stored),
    Error(NetworkError),
}

impl From<Result<StoreTaskStatus, NetworkError>> for StoreTaskStatus {
    fn from(value: Result<StoreTaskStatus, NetworkError>) -> Self {
        match value {
            Ok(result) => result,
            Err(err) => Self::Error(err),
        }
    }
}

struct LogServerStoreTaskResult {
    pub server: RemoteLogServer,
    pub status: StoreTaskStatus,
}

/// The LogServerStoreTask takes care of running a [`Store`] to end.
///
/// The task will retry to connect to the remote server is connection
/// was lost.
struct LogServerStoreTask<'a, T> {
    sequencer_shared_state: &'a Arc<SequencerSharedState>,
    server_manager: &'a RemoteLogServerManager,
    networking: &'a Networking<T>,
    server: RemoteLogServer,
    first_offset: LogletOffset,
    records: &'a [Record],
    rpc_router: &'a RpcRouter<Store>,
}

impl<'a, T: TransportConnect> LogServerStoreTask<'a, T> {
    async fn run(mut self) -> LogServerStoreTaskResult {
        let result = self.send().await;
        LogServerStoreTaskResult {
            server: self.server,
            status: result.into(),
        }
    }

    async fn send(&mut self) -> Result<StoreTaskStatus, NetworkError> {
        let server_local_tail = self
            .server
            .local_tail()
            .wait_for_offset_or_seal(self.first_offset)
            .await?;

        match server_local_tail {
            TailState::Sealed(offset) => return Ok(StoreTaskStatus::Sealed(offset)),
            TailState::Open(offset) => {
                match offset.cmp(&self.first_offset) {
                    Ordering::Equal => {
                        // we ready to send our write
                    }
                    Ordering::Less => {
                        // this should never happen since we waiting
                        // for local tail!
                        unreachable!()
                    }
                    Ordering::Greater => {
                        return Ok(StoreTaskStatus::AdvancedLocalTail(offset));
                    }
                };
            }
        }

        let incoming = match self.try_send().await {
            Ok(incoming) => incoming,
            Err(err) => {
                return Err(err);
            }
        };

        self.server
            .local_tail()
            .notify_offset_update(incoming.body().local_tail);

        match incoming.body().status {
            Status::Sealing | Status::Sealed => {
                self.server.local_tail().notify_seal();
            }
            _ => {}
        }

        Ok(StoreTaskStatus::Stored(incoming.into_body()))
    }

    async fn try_send(&mut self) -> Result<Incoming<Stored>, NetworkError> {
        let store = Store {
            first_offset: self.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            known_global_tail: self.sequencer_shared_state.committed_tail.latest_offset(),
            loglet_id: self.server.loglet_id(),
            payloads: Vec::from_iter(self.records.iter().cloned()),
            sequencer: self.sequencer_shared_state.my_node_id,
            timeout_at: None,
        };

        let mut msg = Outgoing::new(self.server.node_id(), store);

        loop {
            let with_connection = msg.assign_connection(self.server.connection().clone());
            match self.rpc_router.call_on_connection(with_connection).await {
                Ok(incoming) => return Ok(incoming),
                Err(RpcError::Shutdown(shutdown)) => return Err(NetworkError::Shutdown(shutdown)),
                Err(RpcError::SendError(err)) => {
                    msg = err.original.forget_connection();

                    match err.source {
                        NetworkError::ConnectionClosed
                        | NetworkError::ConnectError(_)
                        | NetworkError::Timeout(_) => {
                            self.server_manager
                                .renew(&mut self.server, self.networking)
                                .await?
                        }
                        _ => return Err(err.source),
                    }
                }
            }
        }
    }
}

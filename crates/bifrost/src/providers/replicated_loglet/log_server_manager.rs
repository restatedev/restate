// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use crossbeam_utils::CachePadded;
use metrics::Histogram;
use tokio::sync::Mutex;

use restate_core::network::{NetworkError, Networking, TransportConnect, WeakConnection};
use restate_types::{
    logs::{LogletOffset, SequenceNumber, TailState},
    replicated_loglet::NodeSet,
    PlainNodeId,
};

use super::metric_definitions::BIFROST_SEQ_STORE_DURATION;
use crate::loglet::util::TailOffsetWatch;

type LogServerLock = CachePadded<Mutex<Option<RemoteLogServer>>>;

/// LogServer instance
#[derive(Clone)]
pub struct RemoteLogServer {
    tail: TailOffsetWatch,
    connection: WeakConnection,
    store_latency: Histogram,
}

impl RemoteLogServer {
    fn new(connection: WeakConnection) -> Self {
        let node_id = connection.peer().as_plain();
        Self {
            tail: TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST)),
            connection,
            store_latency: metrics::histogram!(BIFROST_SEQ_STORE_DURATION, "node_id" => node_id.to_string()),
        }
    }

    pub fn local_tail(&self) -> &TailOffsetWatch {
        &self.tail
    }

    pub fn store_latency(&self) -> &Histogram {
        &self.store_latency
    }

    pub fn connection(&self) -> &WeakConnection {
        &self.connection
    }
}

/// LogServerManager maintains a set of [`RemoteLogServer`]s that provided via the
/// [`NodeSet`].
///
/// The manager makes sure there is only one active connection per server.
/// It's up to the user of the client to do [`LogServerManager::renew`] if needed
#[derive(Clone)]
pub struct RemoteLogServerManager {
    servers: Arc<BTreeMap<PlainNodeId, LogServerLock>>,
}

impl RemoteLogServerManager {
    /// creates the node set and start the appenders
    pub fn new(nodeset: &NodeSet) -> Self {
        let servers = nodeset
            .iter()
            .map(|node_id| (*node_id, LogServerLock::default()))
            .collect();
        let servers = Arc::new(servers);

        Self { servers }
    }

    /// Gets a log-server instance. On first time it will initialize a new connection
    /// to log server. It will make sure all following get call holds the same
    /// connection.
    ///
    /// It's up to the client to call [`Self::renew`] if the connection it holds
    /// is closed.
    pub async fn get<T: TransportConnect>(
        &self,
        id: PlainNodeId,
        networking: &Networking<T>,
    ) -> Result<RemoteLogServer, NetworkError> {
        let server = self.servers.get(&id).expect("node is in nodeset");

        let mut guard = server.lock().await;

        if let Some(current) = guard.deref() {
            return Ok(current.clone());
        }

        let connection = networking.node_connection(id).await?;
        let server = RemoteLogServer::new(connection);

        *guard = Some(server.clone());

        Ok(server)
    }

    /// Renew makes sure server connection is renewed if and only if
    /// the provided server holds an outdated connection. Otherwise
    /// the latest connection associated with this server is used.
    ///
    /// It's up the holder of the log server instance to retry to renew
    /// if that connection is not valid.
    ///
    /// It also guarantees that concurrent calls to renew on the same server instance
    /// will only renew the connection once for all callers
    ///
    /// However, this does not affect copies of LogServer that have been already retrieved
    /// by calling [`Self::get()`].
    ///
    /// Holder of old instances will have to call renew if they need to.
    pub async fn renew<T: TransportConnect>(
        &self,
        server: &mut RemoteLogServer,
        networking: &Networking<T>,
    ) -> Result<(), NetworkError> {
        // this key must already be in the map
        let current = self
            .servers
            .get(&server.connection.peer().as_plain())
            .expect("node is in nodeset");

        let mut guard = current.lock().await;

        // if you calling renew then the LogServer has already been initialized
        let inner = guard.as_mut().expect("initialized log server instance");

        if inner.connection != server.connection {
            // someone else has already renewed the connection
            server.connection = inner.connection.clone();
            return Ok(());
        }

        let connection = networking
            .node_connection(server.connection.peer().as_plain())
            .await?;
        inner.connection = connection.clone();
        server.connection = connection.clone();

        Ok(())
    }
}

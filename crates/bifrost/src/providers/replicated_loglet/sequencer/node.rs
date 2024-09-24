// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use tokio::sync::Mutex;

use restate_core::{
    network::{ConnectionSender, NetworkError, Networking},
    Metadata,
};
use restate_types::{
    logs::{LogletOffset, SequenceNumber, TailState},
    replicated_loglet::{NodeSet, ReplicatedLogletId},
    PlainNodeId,
};

use crate::loglet::util::TailOffsetWatch;

type LogServerLock = Mutex<Option<RemoteLogServer>>;

/// LogServer instance
#[derive(Clone)]
pub struct RemoteLogServer {
    loglet_id: ReplicatedLogletId,
    node: PlainNodeId,
    tail: TailOffsetWatch,
    //todo(azmy): maybe use ArcSwap here to update
    sender: ConnectionSender,
}

impl RemoteLogServer {
    pub fn node_id(&self) -> PlainNodeId {
        self.node
    }

    pub fn loglet_id(&self) -> ReplicatedLogletId {
        self.loglet_id
    }

    pub fn tail(&self) -> &TailOffsetWatch {
        &self.tail
    }

    pub fn sender(&mut self) -> &mut ConnectionSender {
        &mut self.sender
    }
}

struct RemoteLogServerManagerInner {
    loglet_id: ReplicatedLogletId,
    servers: BTreeMap<PlainNodeId, LogServerLock>,
    node_set: NodeSet,
    metadata: Metadata,
    networking: Networking,
}

/// LogServerManager maintains a set of [`RemoteLogServer`]s that provided via the
/// [`NodeSet`].
///
/// The manager makes sure there is only one active connection per server.
/// It's up to the user of the client to do [`LogServerManager::renew`] if needed
pub(crate) struct RemoteLogServerManager {
    inner: Arc<RemoteLogServerManagerInner>,
}

impl Clone for RemoteLogServerManager {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl RemoteLogServerManager {
    /// creates the node set and start the appenders
    pub fn new(
        loglet_id: ReplicatedLogletId,
        metadata: Metadata,
        networking: Networking,
        node_set: NodeSet,
    ) -> Self {
        let mut servers = BTreeMap::default();
        for node_id in node_set.iter() {
            servers.insert(*node_id, LogServerLock::default());
        }

        let inner = RemoteLogServerManagerInner {
            loglet_id,
            servers,
            node_set,
            metadata,
            networking,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    async fn connect(&self, id: PlainNodeId) -> Result<ConnectionSender, NetworkError> {
        let conf = self.inner.metadata.nodes_config_ref();
        let node = conf.find_node_by_id(id)?;
        let connection = self
            .inner
            .networking
            .connection_manager()
            .get_node_sender(node.current_generation)
            .await?;

        Ok(connection)
    }

    /// gets a log-server instance. On first time it will initialize a new connection
    /// to log server. It will make sure all following get call will hold the same
    /// connection.
    ///
    /// it's up to the client to call [`Self::renew`] if the connection it holds
    /// is closed
    pub async fn get(&self, id: PlainNodeId) -> Result<RemoteLogServer, NetworkError> {
        let server = self.inner.servers.get(&id).expect("node is in nodeset");

        let mut guard = server.lock().await;

        if let Some(current) = guard.deref() {
            return Ok(current.clone());
        }

        // initialize a new instance
        let server = RemoteLogServer {
            loglet_id: self.inner.loglet_id,
            node: id,
            tail: TailOffsetWatch::new(TailState::Open(LogletOffset::OLDEST)),
            sender: self.connect(id).await?,
        };

        // we need to update initialize it
        *guard = Some(server.clone());

        Ok(server)
    }

    /// renew makes sure server connection is renewed if and only if
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
    pub async fn renew(&self, server: &mut RemoteLogServer) -> Result<(), NetworkError> {
        // this key must already be in the map
        let current = self
            .inner
            .servers
            .get(&server.node)
            .expect("node is in nodeset");

        let mut guard = current.lock().await;

        // if you calling renew then the LogServer has already been initialized
        let inner = guard.as_mut().expect("initialized log server instance");

        if inner.sender != server.sender {
            // someone else has already renewed the connection
            server.sender = inner.sender.clone();
            return Ok(());
        }

        let sender = self.connect(server.node).await?;
        inner.sender = sender.clone();
        server.sender = sender.clone();

        Ok(())
    }
}

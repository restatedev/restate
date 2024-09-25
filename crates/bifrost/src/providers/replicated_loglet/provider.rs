// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::trace;

use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_core::{task_center, Metadata, TaskCenter, TaskKind};
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::ReplicatedLogletOptions;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::metadata::{LogletParams, ProviderKind, SegmentIndex};
use restate_types::logs::LogId;
use restate_types::replicated_loglet::ReplicatedLogletParams;

use super::loglet::ReplicatedLoglet;
use super::metric_definitions;
use super::network::RequestPump;
use super::record_cache::RecordCache;
use super::rpc_routers::{LogServersRpc, SequencersRpc};
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::Error;

pub struct Factory<T> {
    task_center: TaskCenter,
    opts: BoxedLiveLoad<ReplicatedLogletOptions>,
    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    networking: Networking<T>,
    logserver_rpc_routers: LogServersRpc,
    sequencer_rpc_routers: SequencersRpc,
    request_pump: RequestPump,
}

impl<T: TransportConnect> Factory<T> {
    pub fn new(
        task_center: TaskCenter,
        mut opts: BoxedLiveLoad<ReplicatedLogletOptions>,
        metadata_store_client: MetadataStoreClient,
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // Handling Sequencer(s) incoming requests
        let request_pump = RequestPump::new(opts.live_load(), metadata.clone(), router_builder);

        let logserver_rpc_routers = LogServersRpc::new(router_builder);
        let sequencer_rpc_routers = SequencersRpc::new(router_builder);
        // todo(asoli): Create a handler to answer to control plane monitoring questions
        Self {
            task_center,
            opts,
            metadata,
            metadata_store_client,
            networking,
            logserver_rpc_routers,
            sequencer_rpc_routers,
            request_pump,
        }
    }
}

#[async_trait]
impl<T: TransportConnect> LogletProviderFactory for Factory<T> {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Replicated
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, OperationError> {
        metric_definitions::describe_metrics();
        let provider = Arc::new(ReplicatedLogletProvider::new(
            self.task_center,
            self.opts,
            self.metadata,
            self.metadata_store_client,
            self.networking,
            self.logserver_rpc_routers,
            self.sequencer_rpc_routers,
        ));
        // run the request pump. The request pump handles/routes incoming messages to our
        // locally hosted sequencers.
        task_center().spawn(
            TaskKind::NetworkMessageHandler,
            "sequencers-ingress",
            None,
            {
                let request_pump = self.request_pump;
                let provider = provider.clone();
                async { request_pump.run(provider).await }
            },
        )?;

        Ok(provider)
    }
}

pub(super) struct ReplicatedLogletProvider<T> {
    active_loglets: DashMap<(LogId, SegmentIndex), Arc<ReplicatedLoglet<T>>>,
    task_center: TaskCenter,
    _opts: BoxedLiveLoad<ReplicatedLogletOptions>,
    metadata: Metadata,
    _metadata_store_client: MetadataStoreClient,
    networking: Networking<T>,
    record_cache: RecordCache,
    logserver_rpc_routers: LogServersRpc,
    sequencer_rpc_routers: SequencersRpc,
}

impl<T: TransportConnect> ReplicatedLogletProvider<T> {
    fn new(
        task_center: TaskCenter,
        _opts: BoxedLiveLoad<ReplicatedLogletOptions>,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        networking: Networking<T>,
        logserver_rpc_routers: LogServersRpc,
        sequencer_rpc_routers: SequencersRpc,
    ) -> Self {
        // todo(asoli): create all global state here that'll be shared across loglet instances
        // - NodeState map.
        Self {
            active_loglets: Default::default(),
            task_center,
            _opts,
            metadata,
            _metadata_store_client: metadata_store_client,
            networking,
            // todo(asoli): read memory budget from ReplicatedLogletOptions
            record_cache: RecordCache::new(20_000_000), // 20MB
            logserver_rpc_routers,
            sequencer_rpc_routers,
        }
    }
}

#[async_trait]
impl<T: TransportConnect> LogletProvider for ReplicatedLogletProvider<T> {
    async fn get_loglet(
        &self,
        log_id: LogId,
        segment_index: SegmentIndex,
        params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>, Error> {
        let loglet = match self.active_loglets.entry((log_id, segment_index)) {
            dashmap::Entry::Vacant(entry) => {
                // NOTE: replicated-loglet expects params to be a `json` string.
                let params =
                    ReplicatedLogletParams::deserialize_from(params.as_bytes()).map_err(|e| {
                        ReplicatedLogletError::LogletParamsParsingError(log_id, segment_index, e)
                    })?;

                trace!(
                    log_id = %log_id,
                    segment_index = %segment_index,
                    loglet_id = %params.loglet_id,
                    nodeset = ?params.nodeset,
                    sequencer = %params.sequencer,
                    replication = ?params.replication,
                    "Creating a replicated loglet client"
                );

                // Create loglet
                let loglet = ReplicatedLoglet::start(
                    log_id,
                    segment_index,
                    params,
                    self.task_center.clone(),
                    self.metadata.clone(),
                    self.networking.clone(),
                    self.logserver_rpc_routers.clone(),
                    &self.sequencer_rpc_routers,
                    self.record_cache.clone(),
                );
                let key_value = entry.insert(Arc::new(loglet));
                Arc::clone(key_value.value())
            }
            dashmap::Entry::Occupied(entry) => entry.get().clone(),
        };

        Ok(loglet as Arc<dyn Loglet>)
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}

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

use restate_core::network::{MessageRouterBuilder, Networking, TransportConnect};
use restate_core::{Metadata, TaskCenter};
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::ReplicatedLogletOptions;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::metadata::{LogletParams, ProviderKind, SegmentIndex};
use restate_types::logs::LogId;
use restate_types::replicated_loglet::ReplicatedLogletParams;

use super::loglet::ReplicatedLoglet;
use super::metric_definitions;
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::providers::replicated_loglet::error::ReplicatedLogletError;
use crate::Error;

pub struct Factory<T> {
    task_center: TaskCenter,
    opts: BoxedLiveLoad<ReplicatedLogletOptions>,
    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    networking: Networking<T>,
}

impl<T: TransportConnect> Factory<T> {
    pub fn new(
        task_center: TaskCenter,
        opts: BoxedLiveLoad<ReplicatedLogletOptions>,
        metadata_store_client: MetadataStoreClient,
        metadata: Metadata,
        networking: Networking<T>,
        _router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        // todo(asoli):
        // - Create the shared RpcRouter(s)
        // - A Handler to answer to control plane monitoring questions
        Self {
            task_center,
            opts,
            metadata,
            metadata_store_client,
            networking,
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
        Ok(Arc::new(ReplicatedLogletProvider::new(
            self.task_center,
            self.opts,
            self.metadata,
            self.metadata_store_client,
            self.networking,
        )))
    }
}

struct ReplicatedLogletProvider<T> {
    active_loglets: DashMap<(LogId, SegmentIndex), Arc<ReplicatedLoglet>>,
    _task_center: TaskCenter,
    _opts: BoxedLiveLoad<ReplicatedLogletOptions>,
    _metadata: Metadata,
    _metadata_store_client: MetadataStoreClient,
    _networking: Networking<T>,
}

impl<T: TransportConnect> ReplicatedLogletProvider<T> {
    fn new(
        task_center: TaskCenter,
        opts: BoxedLiveLoad<ReplicatedLogletOptions>,
        metadata: Metadata,
        metadata_store_client: MetadataStoreClient,
        networking: Networking<T>,
    ) -> Self {
        // todo(asoli): create all global state here that'll be shared across loglet instances
        // - RecordCache.
        // - NodeState map.
        Self {
            active_loglets: Default::default(),
            _task_center: task_center,
            _opts: opts,
            _metadata: metadata,
            _metadata_store_client: metadata_store_client,
            _networking: networking,
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

                // Create loglet
                let loglet = ReplicatedLoglet::new(params);
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

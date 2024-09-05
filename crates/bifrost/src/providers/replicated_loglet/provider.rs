// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove when fleshed out
#![allow(unused)]

use std::sync::Arc;

use async_trait::async_trait;

use restate_core::network::rpc_router::RpcRouter;
use restate_core::network::{MessageRouterBuilder, Networking};
use restate_core::Metadata;
use restate_metadata_store::MetadataStoreClient;
use restate_types::config::ReplicatedLogletOptions;
use restate_types::live::BoxedLiveLoad;
use restate_types::logs::metadata::{LogletParams, ProviderKind, SegmentIndex};
use restate_types::logs::LogId;

use super::metric_definitions;
use crate::loglet::{Loglet, LogletProvider, LogletProviderFactory, OperationError};
use crate::Error;

pub struct Factory {
    opts: BoxedLiveLoad<ReplicatedLogletOptions>,
    metadata: Metadata,
    metadata_store_client: MetadataStoreClient,
    networking: Networking,
}

impl Factory {
    pub fn new(
        opts: BoxedLiveLoad<ReplicatedLogletOptions>,
        metadata_store_client: MetadataStoreClient,
        metadata: Metadata,
        networking: Networking,
        _router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        Self {
            opts,
            metadata,
            metadata_store_client,
            networking,
        }
    }
}

#[async_trait]
impl LogletProviderFactory for Factory {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Replicated
    }

    async fn create(self: Box<Self>) -> Result<Arc<dyn LogletProvider>, OperationError> {
        metric_definitions::describe_metrics();
        Ok(Arc::new(ReplicatedLogletProvider))
    }
}

struct ReplicatedLogletProvider;

#[async_trait]
impl LogletProvider for ReplicatedLogletProvider {
    async fn get_loglet(
        &self,
        // todo: we need richer params
        _log_id: LogId,
        _segment_index: SegmentIndex,
        _params: &LogletParams,
    ) -> Result<Arc<dyn Loglet>, Error> {
        todo!("Not implemented yet")
    }

    async fn shutdown(&self) -> Result<(), OperationError> {
        Ok(())
    }
}

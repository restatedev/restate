// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;

use restate_core::{
    network::{MessageRouterBuilder, Networking, TransportConnect},
    task_center,
    worker_api::ProcessorsManagerHandle,
    Metadata, TaskKind,
};
use restate_types::cluster::cluster_state::ClusterStateWatch;

use crate::gossip::{self, Gossip};

pub struct BaseRole<T> {
    processor_manager_handle: Option<ProcessorsManagerHandle>,
    gossip: Option<Gossip<T>>,
}

impl<T> BaseRole<T>
where
    T: TransportConnect,
{
    pub fn create(
        metadata: Metadata,
        networking: Networking<T>,
        router_builder: &mut MessageRouterBuilder,
    ) -> Self {
        let gossip = gossip::Gossip::new(metadata, networking, router_builder);

        Self {
            processor_manager_handle: None,
            gossip: Some(gossip),
        }
    }

    pub fn cluster_state_watch(&self) -> ClusterStateWatch {
        self.gossip.as_ref().expect("is set").cluster_state_watch()
    }

    pub fn with_processor_manager_handle(&mut self, handle: ProcessorsManagerHandle) -> &mut Self {
        self.gossip
            .as_mut()
            .expect("is set")
            .with_processor_manager_handle(handle.clone());

        self.processor_manager_handle = Some(handle);
        self
    }

    pub fn start(mut self) -> anyhow::Result<()> {
        let tc = task_center();

        let gossip = self.gossip.take().expect("is set");
        tc.spawn_child(TaskKind::SystemService, "gossip", None, gossip.run())
            .context("Failed to start gossiping")?;

        Ok(())
    }
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::worker_api;
use restate_types::identifiers::LeaderEpoch;
use restate_types::net;
use restate_types::net::node::PartitionConfiguration;

#[derive(derive_more::Debug)]
pub struct PartitionState {
    pub(super) leader_epoch: LeaderEpoch,
    pub(super) observed_current_config: PartitionConfiguration,
    pub(super) observed_next_config: Option<PartitionConfiguration>,
}

impl Default for PartitionState {
    fn default() -> Self {
        Self {
            leader_epoch: LeaderEpoch::INVALID,
            observed_current_config: PartitionConfiguration::default(),
            observed_next_config: None,
        }
    }
}
impl PartitionState {
    pub fn merge_with_local_state(&mut self, _state: &worker_api::PartitionState) {
        // merge
    }

    pub fn merge_with_gossip(&mut self, incoming: net::node::Partition) {
        if self.leader_epoch > incoming.leader_epoch {
            return;
        }
        self.leader_epoch = self.leader_epoch.max(incoming.leader_epoch);
        if incoming.observed_current_config.version > self.observed_current_config.version {
            self.observed_current_config = incoming.observed_current_config.clone();
        }

        match (
            incoming.observed_next_config.as_ref().map(|i| i.version),
            self.observed_next_config.as_ref().map(|c| c.version),
        ) {
            (None, None) => {}
            (Some(_), None) => {
                self.observed_next_config = incoming.observed_next_config;
            }
            (Some(incoming_next), Some(my_next)) if incoming_next > my_next => {
                self.observed_next_config = incoming.observed_next_config;
            }
            (Some(_), Some(_)) => {}
            (None, Some(_)) => {
                self.observed_next_config = incoming.observed_next_config;
            }
        };

        // we reset the next config to None if the current observed config already caught up to the
        // next version or beyond
        if self
            .observed_next_config
            .as_ref()
            .is_some_and(|v| self.observed_current_config.version > v.version)
        {
            // reset my next to none if current caught up to next's version.
            self.observed_next_config = None;
        }
    }
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem;

use assert2::let_assert;
use futures::future::OptionFuture;
use itertools::Itertools;
use tracing::info;

use restate_core::network::TransportConnect;
use restate_core::{TaskCenter, TaskId, TaskKind, my_node_id};
use restate_types::nodes_config::NodesConfiguration;

use crate::cluster_controller::service::Service;
use crate::cluster_controller::service::scheduler::Scheduler;
use crate::cluster_controller::service::scheduler_task::SchedulerTask;

pub enum ClusterControllerState {
    Follower,
    Leader(Leader),
}

impl ClusterControllerState {
    pub async fn update<T: TransportConnect>(
        &mut self,
        service: &Service<T>,
        nodes_config: &NodesConfiguration,
        cs: &restate_types::cluster_state::ClusterState,
    ) {
        let maybe_leader = {
            let admin_nodes: Vec<_> = nodes_config
                .get_admin_nodes()
                .map(|c| c.current_generation)
                .sorted()
                .collect();
            let states = cs.map_from_ids(admin_nodes.iter().map(Into::into));

            admin_nodes
                .iter()
                .zip(states)
                .filter_map(|(node_id, state)| state.is_alive().then_some(*node_id))
                .next()
        };

        // A Cluster Controller is a leader if the node holds the smallest PlainNodeID
        let is_leader = match maybe_leader {
            None => false,
            Some(leader) => leader == my_node_id(),
        };

        match (is_leader, &self) {
            (true, ClusterControllerState::Leader(_))
            | (false, ClusterControllerState::Follower) => {
                // nothing to do
            }
            (true, ClusterControllerState::Follower) => {
                info!("Cluster controller switching to leader mode");
                *self = ClusterControllerState::Leader(Leader::from_service(service));
            }
            (false, ClusterControllerState::Leader(_)) => {
                info!(
                    "Cluster controller switching to follower mode, I think the leader is {}",
                    maybe_leader.expect("a leader must be identified"),
                );
                let_assert!(
                    ClusterControllerState::Leader(leader) =
                        mem::replace(self, ClusterControllerState::Follower)
                );
                // let's stop leader tasks
                leader.stop().await;
            }
        };
    }
}

pub struct Leader {
    scheduler_task: TaskId,
}

impl Leader {
    fn from_service<T: TransportConnect>(service: &Service<T>) -> Leader {
        let scheduler = Scheduler::new(
            service.metadata_writer.clone(),
            service.networking.clone(),
            service.replica_set_states.clone(),
        );

        // We spawn the scheduler task as a child task to make use of the built-in error handling of
        // managed tasks. Otherwise, we would have to monitor for failed tasks ourselves.
        let scheduler_task = TaskCenter::spawn_child(
            TaskKind::SystemService,
            "scheduler",
            SchedulerTask::new(
                service.cluster_state_refresher.cluster_state_watcher(),
                scheduler,
                service.metadata_writer.raw_metadata_store_client().clone(),
            )
            .run(),
        )
        .expect("failed to spawn scheduler task");

        Self { scheduler_task }
    }

    /// Stops the leader tasks to make sure that no other leader activity is running.
    async fn stop(self) {
        let scheduler_task = TaskCenter::cancel_task(self.scheduler_task);

        // ignore if the task failed during cancellation
        let _scheduler_task = OptionFuture::from(scheduler_task).await;
    }
}

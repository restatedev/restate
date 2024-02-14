// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Options;
use codederror::CodedError;
use futures::TryFutureExt;
use restate_admin::service::AdminService;
use restate_cluster_controller::ClusterControllerHandle;
use restate_meta::{FileMetaReader, FileMetaStorage, MetaService};
use restate_node_services::worker::{StateMutationRequest, TerminationRequest};
use restate_types::invocation::InvocationTermination;
use restate_types::state_mut::ExternalStateMutation;
use restate_worker::KafkaIngressOptions;
use restate_worker_api::{Error, Handle};
use tokio::task::{JoinError, JoinSet};
use tonic::transport::Channel;
use tracing::info;

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ClusterControllerRoleError {
    #[error("cluster controller failed: {0}")]
    ClusterController(
        #[from]
        #[code]
        restate_cluster_controller::Error,
    ),
    #[error("cluster controller role component panicked: {0}")]
    #[code(unknown)]
    ComponentPanic(#[from] JoinError),
    #[error("admin component failed: {0}")]
    Admin(
        #[from]
        #[code]
        restate_admin::Error,
    ),
    #[error("meta component failed: {0}")]
    Meta(
        #[from]
        #[code]
        restate_meta::Error,
    ),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum ClusterControllerRoleBuildError {
    #[error("failed creating meta: {0}")]
    Meta(
        #[from]
        #[code]
        restate_meta::BuildError,
    ),
}

#[derive(Debug)]
pub struct ClusterControllerRole {
    controller: restate_cluster_controller::Service,
    admin: AdminService,
    meta: MetaService<FileMetaStorage, KafkaIngressOptions>,
}

impl ClusterControllerRole {
    pub fn handle(&self) -> ClusterControllerHandle {
        self.controller.handle()
    }

    pub fn schema_reader(&self) -> FileMetaReader {
        self.meta.schema_reader()
    }

    pub async fn run(
        mut self,
        shutdown_watch: drain::Watch,
    ) -> Result<(), ClusterControllerRoleError> {
        info!("Running cluster controller role");

        let shutdown_signal = shutdown_watch.signaled();
        let (inner_shutdown_signal, inner_shutdown_watch) = drain::channel();

        let mut component_set = JoinSet::new();

        // Init the meta. This will reload the schemas in memory.
        self.meta
            .init()
            .await
            .map_err(ClusterControllerRoleError::Meta)?;

        component_set.spawn(
            self.meta
                .run(inner_shutdown_watch.clone())
                .map_ok(|_| "meta-service")
                .map_err(ClusterControllerRoleError::Meta),
        );

        component_set.spawn(
            self.controller
                .run(inner_shutdown_watch.clone())
                .map_ok(|_| "cluster-controller")
                .map_err(ClusterControllerRoleError::ClusterController),
        );

        // todo: Make address configurable
        let worker_channel =
            Channel::builder("http://127.0.0.1:5122/".parse().expect("valid uri")).connect_lazy();
        let worker_handle = GrpcWorkerHandle::new(worker_channel.clone());
        let worker_svc_client =
            restate_node_services::worker::worker_svc_client::WorkerSvcClient::new(worker_channel);

        component_set.spawn(
            self.admin
                .run(inner_shutdown_watch, worker_handle, Some(worker_svc_client))
                .map_ok(|_| "admin")
                .map_err(ClusterControllerRoleError::Admin),
        );

        tokio::select! {
            _ = shutdown_signal => {
                info!("Stopping cluster controller role");
                // ignore result because we are shutting down
                inner_shutdown_signal.drain().await;
                component_set.shutdown().await;
            },
            Some(component_result) = component_set.join_next() => {
                let component_name = component_result.map_err(ClusterControllerRoleError::ComponentPanic)??;
                panic!("Unexpected termination of cluster controller role component '{component_name}'");

            }

        }

        Ok(())
    }
}

impl TryFrom<Options> for ClusterControllerRole {
    type Error = ClusterControllerRoleBuildError;

    fn try_from(options: Options) -> Result<Self, Self::Error> {
        let meta = options.meta.build(options.worker.kafka.clone())?;
        let admin = options.admin.build(meta.schemas(), meta.meta_handle());

        Ok(ClusterControllerRole {
            controller: restate_cluster_controller::Service::new(options.cluster_controller),
            admin,
            meta,
        })
    }
}

#[derive(Debug, Clone)]
struct GrpcWorkerHandle {
    grpc_client: restate_node_services::worker::worker_svc_client::WorkerSvcClient<Channel>,
}

impl GrpcWorkerHandle {
    fn new(channel: Channel) -> Self {
        GrpcWorkerHandle {
            grpc_client: restate_node_services::worker::worker_svc_client::WorkerSvcClient::new(
                channel,
            ),
        }
    }
}

impl Handle for GrpcWorkerHandle {
    async fn terminate_invocation(
        &self,
        invocation_termination: InvocationTermination,
    ) -> Result<(), Error> {
        let invocation_termination =
            bincode::serde::encode_to_vec(invocation_termination, bincode::config::standard())
                .expect("serialization should work");

        self.grpc_client
            .clone()
            .terminate_invocation(TerminationRequest {
                invocation_termination: invocation_termination.into(),
            })
            .await
            .map(|resp| resp.into_inner())
            // todo: Proper error handling
            .map_err(|_err| Error::Unreachable)
    }

    async fn external_state_mutation(&self, mutation: ExternalStateMutation) -> Result<(), Error> {
        let state_mutation = bincode::serde::encode_to_vec(mutation, bincode::config::standard())
            .expect("serialization should work");

        self.grpc_client
            .clone()
            .mutate_state(StateMutationRequest {
                state_mutation: state_mutation.into(),
            })
            .await
            .map(|resp| resp.into_inner())
            // todo: Proper error handling
            .map_err(|_err| Error::Unreachable)
    }
}

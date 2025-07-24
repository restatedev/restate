// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::PlainNodeId;
use restate_types::logs::LogletId;
use restate_types::logs::metadata::{InternalKind, Logs, ProviderKind};
use restate_types::nodes_config::{
    MetadataServerState, NodesConfigError, NodesConfiguration, Role, StorageState,
};

#[derive(Debug, thiserror::Error)]
pub enum DisableNodeError {
    #[error(
        "log server is part of a node set of loglet {0}. You need to trim this loglet before disabling the log server"
    )]
    NodeSetMember(LogletId),
    #[error(
        "loglet {loglet_id} of kind {provider_kind} requires node {node_id}. You need to trim this loglet before disabling the node"
    )]
    LocalLoglet {
        provider_kind: ProviderKind,
        loglet_id: LogletId,
        node_id: PlainNodeId,
    },
    #[error("log server cannot be disabled because it is in read-write state")]
    ReadWrite,
    #[error("current default loglet provider '{0}', does not support disabling nodes")]
    DefaultLogletProvider(ProviderKind),
    #[error("metadata server is an active member")]
    MetadataMember,
}

pub struct DisableNodeChecker<'a, 'b> {
    nodes_configuration: &'a NodesConfiguration,
    logs: &'b Logs,
}

impl<'a, 'b> DisableNodeChecker<'a, 'b> {
    pub fn new(nodes_configuration: &'a NodesConfiguration, logs: &'b Logs) -> Self {
        DisableNodeChecker {
            nodes_configuration,
            logs,
        }
    }

    pub fn safe_to_disable_node(&self, node_id: PlainNodeId) -> Result<(), DisableNodeError> {
        let node_config = match self.nodes_configuration.find_node_by_id(node_id) {
            Ok(node_config) => node_config,
            // unknown or deleted nodes can be safely disabled
            Err(NodesConfigError::UnknownNodeId(_)) | Err(NodesConfigError::Deleted(_)) => {
                return Ok(());
            }
            Err(NodesConfigError::GenerationMismatch { .. }) => {
                unreachable!("impossible nodes config errors")
            }
        };

        self.safe_to_disable_log_server(node_id, node_config.log_server_config.storage_state)?;

        // only safe to disable node if it does not run a metadata server or is not a member
        // todo atm we must consider the role because the default metadata server state is MetadataServerState::Member.
        //  We need to introduce a provisioning state or make the metadata server state optional in the NodeConfig.
        if node_config.roles.contains(Role::MetadataServer)
            && node_config.metadata_server_config.metadata_server_state
                == MetadataServerState::Member
        {
            return Err(DisableNodeError::MetadataMember);
        }

        Ok(())
    }

    /// Checks whether it is safe to disable the given log server identified by the node_id. It is
    /// safe to disable the log server if it can no longer be added to new node sets (== not being
    /// a candidate) and it is no longer part of any known node sets.
    fn safe_to_disable_log_server(
        &self,
        node_id: PlainNodeId,
        storage_state: StorageState,
    ) -> Result<(), DisableNodeError> {
        match storage_state {
            StorageState::ReadWrite => {
                return Err(DisableNodeError::ReadWrite);
            }
            // it's safe to disable a disabled node
            StorageState::Disabled => return Ok(()),
            // we need to check whether this node is no longer part of any known node sets
            StorageState::ReadOnly
            | StorageState::Gone
            | StorageState::DataLoss
            | StorageState::Provisioning => {}
        }

        // if the default provider kind is local or in-memory than it is not safe to disable the
        // given node because it is included in the implicit node set
        if matches!(
            self.logs.configuration().default_provider.kind(),
            ProviderKind::Local
        ) {
            return Err(DisableNodeError::DefaultLogletProvider(
                self.logs.configuration().default_provider.kind(),
            ));
        }
        if matches!(
            self.logs.configuration().default_provider.kind(),
            ProviderKind::InMemory
        ) {
            return Err(DisableNodeError::DefaultLogletProvider(
                self.logs.configuration().default_provider.kind(),
            ));
        }

        // check for every log that the given node is not contained in any node set
        for (log_id, chain) in self.logs.iter() {
            for segment in chain.iter() {
                match segment.config.kind {
                    // we assume that the given node runs the local and memory loglet and, therefore,
                    // cannot be disabled
                    InternalKind::InMemory => {
                        return Err(DisableNodeError::LocalLoglet {
                            provider_kind: segment.config.kind.try_into().unwrap(),
                            loglet_id: LogletId::new(*log_id, segment.index()),
                            node_id,
                        });
                    }
                    InternalKind::Local => {
                        return Err(DisableNodeError::LocalLoglet {
                            provider_kind: segment.config.kind.try_into().unwrap(),
                            loglet_id: LogletId::new(*log_id, segment.index()),
                            node_id,
                        });
                    }
                    InternalKind::Replicated => {
                        if self
                            .logs
                            .get_replicated_loglet(&LogletId::new(*log_id, segment.index()))
                            .expect("to be present")
                            .params
                            .nodeset
                            .contains(node_id)
                        {
                            return Err(DisableNodeError::NodeSetMember(LogletId::new(
                                *log_id,
                                segment.index(),
                            )));
                        }
                    }
                    InternalKind::Sealed => {}
                }
            }
        }

        Ok(())
    }
}

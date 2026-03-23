// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;

use restate_types::logs::metadata::ProviderConfiguration;
use restate_types::partition_table::PartitionReplication;
use restate_types::replication::ReplicationProperty;

#[derive(Debug, Clone, Serialize)]
pub(super) struct LogsProviderView {
    /// Provider kind (in-memory, local, or replicated).
    pub kind: String,
    /// Replication property if provider kind is replicated.
    pub replication_property: Option<ReplicationProperty>,
    /// Target nodeset size if provider kind is replicated.
    pub target_nodeset_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PartitionReplicationView {
    /// Replication mode.
    pub mode: PartitionReplicationMode,
    /// Required copies per location scope when mode is `limit`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub copies: Option<ReplicationProperty>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PartitionReplicationMode {
    Everywhere,
    Limit,
}

pub(super) fn logs_provider_view(provider: ProviderConfiguration) -> LogsProviderView {
    match provider {
        ProviderConfiguration::InMemory => LogsProviderView {
            kind: "in-memory".to_owned(),
            replication_property: None,
            target_nodeset_size: None,
        },
        ProviderConfiguration::Local => LogsProviderView {
            kind: "local".to_owned(),
            replication_property: None,
            target_nodeset_size: None,
        },
        ProviderConfiguration::Replicated(config) => LogsProviderView {
            kind: "replicated".to_owned(),
            replication_property: Some(config.replication_property.clone()),
            target_nodeset_size: Some(config.target_nodeset_size.as_u32()),
        },
    }
}

pub(super) fn partition_replication_view(
    partition_replication: &PartitionReplication,
) -> PartitionReplicationView {
    match partition_replication {
        PartitionReplication::Everywhere => PartitionReplicationView {
            mode: PartitionReplicationMode::Everywhere,
            copies: None,
        },
        PartitionReplication::Limit(replication_property) => PartitionReplicationView {
            mode: PartitionReplicationMode::Limit,
            copies: Some(replication_property.clone()),
        },
    }
}

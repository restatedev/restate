// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, time::Duration};

use prost_dto::{FromProst, IntoProst};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use super::TargetName;
use super::codec::V2Convertible;
use crate::errors::ConversionError;
use crate::protobuf::net::cluster as proto;
use crate::{cluster::cluster_state::PartitionProcessorStatus, identifiers::PartitionId};

super::define_rpc! {
    @request=GetNodeState,
    @response=NodeStateResponse,
    @request_target=TargetName::NodeGetNodeStateRequest,
    @response_target=TargetName::NodeGetNodeStateResponse,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, IntoProst, FromProst)]
#[prost(target=crate::protobuf::net::cluster::GetNodeState)]
pub struct GetNodeState {}

impl V2Convertible for GetNodeState {
    type Target = proto::GetNodeState;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        Ok(target.into())
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStateResponse {
    /// Partition processor status per partition. Is set to None if this node is not a `Worker` node
    #[serde_as(as = "Option<serde_with::Seq<(_, _)>>")]
    pub partition_processor_state: Option<BTreeMap<PartitionId, PartitionProcessorStatus>>,

    /// node uptime.
    // serde(default) is required for backward compatibility when updating the cluster,
    // ensuring that older nodes can still interact with newer nodes that recognize this attribute.
    #[serde(default)]
    pub uptime: Duration,
}

impl From<NodeStateResponse> for proto::NodeStateResponse {
    fn from(value: NodeStateResponse) -> Self {
        let NodeStateResponse {
            partition_processor_state,
            uptime,
        } = value;

        Self {
            uptime: Some(uptime.try_into().expect("valid duration")),
            partition_processor_state: partition_processor_state
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl TryFrom<proto::NodeStateResponse> for NodeStateResponse {
    type Error = ConversionError;

    fn try_from(value: proto::NodeStateResponse) -> Result<Self, Self::Error> {
        let proto::NodeStateResponse {
            partition_processor_state,
            uptime,
        } = value;

        Ok(Self {
            uptime: uptime
                .ok_or_else(|| ConversionError::missing_field("uptime"))?
                .try_into()
                .map_err(|_| ConversionError::invalid_data("uptime"))?,
            partition_processor_state: if partition_processor_state.is_empty() {
                None
            } else {
                Some({
                    let mut map = BTreeMap::default();
                    for (id, v) in partition_processor_state {
                        map.insert(
                            PartitionId::from(
                                u16::try_from(id)
                                    .map_err(|_| ConversionError::invalid_data("partition_id"))?,
                            ),
                            v.into(),
                        );
                    }
                    map
                })
            },
        })
    }
}

impl V2Convertible for NodeStateResponse {
    type Target = proto::NodeStateResponse;

    fn from_v2(target: Self::Target) -> Result<Self, ConversionError> {
        target.try_into()
    }

    fn into_v2(self) -> Self::Target {
        self.into()
    }
}

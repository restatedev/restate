// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use restate_types::net::UnaryMessage;
use restate_types::net::codec::WireDecode;
use restate_types::net::{self, RpcRequest};

pub(super) enum V1Compat {
    Rpc {
        v2_service: net::ServiceTag,
        // the target of the RPC response for V1 protocol
        v1_response: net::ServiceTag,
        sort_code: Option<u64>,
        msg_type: String,
    },
    Unary {
        v2_service: net::ServiceTag,
        sort_code: Option<u64>,
        msg_type: String,
    },
    Invalid,
}

impl V1Compat {
    pub(super) fn new(v1_target: net::ServiceTag, body: &Bytes) -> V1Compat {
        use net::ServiceTag::*;
        match v1_target {
            Unknown
            | LogServerDataService
            | LogServerInfoService
            | ReplicatedLogletSequencerDataService
            | ReplicatedLogletSequencerInfoService
            | GossipService
            | RemoteDataFusionService
            | MetadataManagerService
            | PartitionManagerService
            | PartitionLeaderService
            | LogServerDigest
            | LogServerLogletInfo
            | LogServerRecords
            | LogServerSealed
            | LogServerStored
            | LogServerTailUpdated
            | LogServerTrimmed
            | NodeGetNodeStateResponse
            | PartitionCreateSnapshotResponse
            | PartitionProcessorRpcResponse
            | RemoteQueryScannerClosed
            | RemoteQueryScannerNextResult
            | RemoteQueryScannerOpened
            | ReplicatedLogletAppended
            | ReplicatedLogletSequencerState => V1Compat::Invalid,
            MetadataManager => V1Compat::Rpc {
                v2_service: MetadataManagerService,
                v1_response: MetadataManager,
                sort_code: None,
                msg_type: net::metadata::GetMetadataRequest::TYPE.to_owned(),
            },
            ControlProcessors => V1Compat::Unary {
                v2_service: PartitionManagerService,
                sort_code: None,
                msg_type: net::partition_processor_manager::ControlProcessors::TYPE.to_owned(),
            },
            PartitionCreateSnapshotRequest => {
                let decoded = <net::partition_processor_manager::CreateSnapshotRequest as WireDecode>::try_decode(
                       body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");

                V1Compat::Rpc {
                    v2_service: PartitionManagerService,
                    v1_response: PartitionCreateSnapshotResponse,
                    sort_code: Some(decoded.partition_id.into()),
                    msg_type: net::partition_processor_manager::CreateSnapshotRequest::TYPE
                        .to_owned(),
                }
            }
            PartitionProcessorRpc => {
                let decoded =
                    <net::partition_processor::PartitionProcessorRpcRequest as WireDecode>::try_decode(
                        body.clone(),
                        net::ProtocolVersion::V1,
                    )
                    .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: PartitionLeaderService,
                    v1_response: PartitionProcessorRpcResponse,
                    sort_code: Some(decoded.partition_id.into()),
                    msg_type: net::partition_processor::PartitionProcessorRpcRequest::TYPE
                        .to_owned(),
                }
            }
            LogServerRelease => {
                let decoded = <net::log_server::Release as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Unary {
                    v2_service: LogServerDataService,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::Release::TYPE.to_owned(),
                }
            }
            LogServerSeal => {
                let decoded = <net::log_server::Seal as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerInfoService,
                    v1_response: LogServerSealed,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::Seal::TYPE.to_owned(),
                }
            }
            LogServerGetLogletInfo => {
                let decoded = <net::log_server::GetLogletInfo as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerInfoService,
                    v1_response: LogServerLogletInfo,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::GetLogletInfo::TYPE.to_owned(),
                }
            }
            LogServerTrim => {
                let decoded = <net::log_server::Trim as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerInfoService,
                    v1_response: LogServerTrimmed,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::Trim::TYPE.to_owned(),
                }
            }
            LogServerWaitForTail => {
                let decoded = <net::log_server::WaitForTail as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerInfoService,
                    v1_response: LogServerTailUpdated,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::WaitForTail::TYPE.to_owned(),
                }
            }
            LogServerGetDigest => {
                let decoded = <net::log_server::GetDigest as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerInfoService,
                    v1_response: LogServerDigest,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::GetDigest::TYPE.to_owned(),
                }
            }
            LogServerStore => {
                let decoded = <net::log_server::Store as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerDataService,
                    v1_response: LogServerStored,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::Store::TYPE.to_owned(),
                }
            }
            LogServerGetRecords => {
                let decoded = <net::log_server::GetRecords as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: LogServerDataService,
                    v1_response: LogServerRecords,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::log_server::GetRecords::TYPE.to_owned(),
                }
            }
            ReplicatedLogletAppend => {
                let decoded = <net::replicated_loglet::Append as WireDecode>::try_decode(
                    body.clone(),
                    net::ProtocolVersion::V1,
                )
                .expect("deserialization infallible");

                V1Compat::Rpc {
                    v2_service: ReplicatedLogletSequencerDataService,
                    v1_response: ReplicatedLogletAppended,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::replicated_loglet::Append::TYPE.to_owned(),
                }
            }
            ReplicatedLogletGetSequencerState => {
                let decoded =
                    <net::replicated_loglet::GetSequencerState as WireDecode>::try_decode(
                        body.clone(),
                        net::ProtocolVersion::V1,
                    )
                    .expect("deserialization infallible");
                V1Compat::Rpc {
                    v2_service: ReplicatedLogletSequencerInfoService,
                    v1_response: ReplicatedLogletSequencerState,
                    sort_code: Some(decoded.header.loglet_id.into()),
                    msg_type: net::replicated_loglet::GetSequencerState::TYPE.to_owned(),
                }
            }
            NodeGetNodeStateRequest => V1Compat::Rpc {
                v2_service: GossipService,
                v1_response: NodeGetNodeStateResponse,
                sort_code: None,
                msg_type: net::node::GetNodeState::TYPE.to_owned(),
            },
            RemoteQueryScannerOpen => V1Compat::Rpc {
                v2_service: RemoteDataFusionService,
                v1_response: RemoteQueryScannerOpened,
                sort_code: None,
                msg_type: net::remote_query_scanner::RemoteQueryScannerOpen::TYPE.to_owned(),
            },
            RemoteQueryScannerNext => V1Compat::Rpc {
                v2_service: RemoteDataFusionService,
                v1_response: RemoteQueryScannerNextResult,
                sort_code: None,
                msg_type: net::remote_query_scanner::RemoteQueryScannerNext::TYPE.to_owned(),
            },
            RemoteQueryScannerClose => V1Compat::Rpc {
                v2_service: RemoteDataFusionService,
                v1_response: RemoteQueryScannerClosed,
                sort_code: None,
                msg_type: net::remote_query_scanner::RemoteQueryScannerClose::TYPE.to_owned(),
            },
        }
    }

    pub(super) fn translate_v2_tag_to_v1_target(
        tag: net::ServiceTag,
        msg_type: &str,
    ) -> Option<net::ServiceTag> {
        use net::ServiceTag::*;
        match tag {
            Unknown
            | MetadataManager
            | ControlProcessors
            | LogServerStore
            | LogServerStored
            | LogServerRelease
            | LogServerSeal
            | LogServerSealed
            | LogServerGetLogletInfo
            | LogServerLogletInfo
            | LogServerGetRecords
            | LogServerRecords
            | LogServerTrim
            | LogServerTrimmed
            | LogServerWaitForTail
            | LogServerTailUpdated
            | LogServerGetDigest
            | LogServerDigest
            | ReplicatedLogletAppend
            | ReplicatedLogletAppended
            | ReplicatedLogletGetSequencerState
            | ReplicatedLogletSequencerState
            | NodeGetNodeStateRequest
            | NodeGetNodeStateResponse
            | RemoteQueryScannerOpen
            | RemoteQueryScannerOpened
            | RemoteQueryScannerNext
            | RemoteQueryScannerNextResult
            | RemoteQueryScannerClose
            | RemoteQueryScannerClosed
            | PartitionProcessorRpc
            | PartitionProcessorRpcResponse
            | PartitionCreateSnapshotRequest
            | PartitionCreateSnapshotResponse => None,

            LogServerDataService => match msg_type {
                net::log_server::Store::TYPE => Some(LogServerStore),
                net::log_server::GetRecords::TYPE => Some(LogServerGetRecords),
                _ => None,
            },
            LogServerInfoService => match msg_type {
                net::log_server::Release::TYPE => Some(LogServerRelease),
                net::log_server::GetLogletInfo::TYPE => Some(LogServerGetLogletInfo),
                net::log_server::Trim::TYPE => Some(LogServerTrim),
                net::log_server::WaitForTail::TYPE => Some(LogServerWaitForTail),
                net::log_server::GetDigest::TYPE => Some(LogServerGetDigest),
                net::log_server::Seal::TYPE => Some(LogServerSeal),
                _ => None,
            },
            ReplicatedLogletSequencerDataService => match msg_type {
                net::replicated_loglet::Append::TYPE => Some(ReplicatedLogletAppend),
                _ => None,
            },
            ReplicatedLogletSequencerInfoService => match msg_type {
                net::replicated_loglet::GetSequencerState::TYPE => {
                    Some(ReplicatedLogletGetSequencerState)
                }
                _ => None,
            },
            PartitionManagerService => match msg_type {
                net::partition_processor_manager::CreateSnapshotRequest::TYPE => {
                    Some(PartitionCreateSnapshotRequest)
                }
                net::partition_processor_manager::ControlProcessors::TYPE => {
                    Some(ControlProcessors)
                }
                _ => None,
            },
            PartitionLeaderService => match msg_type {
                net::partition_processor::PartitionProcessorRpcRequest::TYPE => {
                    Some(PartitionProcessorRpc)
                }
                _ => None,
            },
            GossipService => match msg_type {
                net::node::GetNodeState::TYPE => Some(NodeGetNodeStateRequest),
                _ => None,
            },
            RemoteDataFusionService => match msg_type {
                net::remote_query_scanner::RemoteQueryScannerOpen::TYPE => {
                    Some(RemoteQueryScannerOpen)
                }
                net::remote_query_scanner::RemoteQueryScannerNext::TYPE => {
                    Some(RemoteQueryScannerNext)
                }
                net::remote_query_scanner::RemoteQueryScannerClose::TYPE => {
                    Some(RemoteQueryScannerClose)
                }
                _ => None,
            },
            MetadataManagerService => match msg_type {
                net::metadata::GetMetadataRequest::TYPE => Some(MetadataManager),
                _ => None,
            },
        }
    }
}

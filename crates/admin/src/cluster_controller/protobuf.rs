// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::grpc::DEFAULT_GRPC_COMPRESSION;

tonic::include_proto!("restate.cluster_ctrl");

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("cluster_ctrl_svc_descriptor");

/// Creates a new ClusterCtrlSvcClient with appropriate configuration
#[cfg(feature = "clients")]
pub fn new_cluster_ctrl_client(
    channel: tonic::transport::Channel,
) -> cluster_ctrl_svc_client::ClusterCtrlSvcClient<tonic::transport::Channel> {
    cluster_ctrl_svc_client::ClusterCtrlSvcClient::new(channel)
        // note: the order of those calls defines the priority
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(DEFAULT_GRPC_COMPRESSION)
}

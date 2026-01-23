// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::grpc::DEFAULT_GRPC_COMPRESSION;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

tonic::include_proto!("restate.metadata_server_network_svc");

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("metadata_server_network_svc");

/// Creates a new MetadataServerNetworkSvcClient with appropriate configuration
pub fn new_metadata_server_network_client(
    channel: Channel,
) -> metadata_server_network_svc_client::MetadataServerNetworkSvcClient<Channel> {
    metadata_server_network_svc_client::MetadataServerNetworkSvcClient::new(channel)
        // note: the order of those calls defines the priority
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(DEFAULT_GRPC_COMPRESSION)
}

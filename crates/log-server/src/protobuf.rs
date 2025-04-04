// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

tonic::include_proto!("restate.log_server");

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("log_server_svc_descriptor");

/// Creates a new ClusterCtrlSvcClient with appropriate configuration
#[cfg(feature = "clients")]
pub fn new_log_server_client(
    channel: tonic::transport::Channel,
) -> log_server_svc_client::LogServerSvcClient<tonic::transport::Channel> {
    use restate_core::network::grpc::{DEFAULT_GRPC_COMPRESSION, MAX_MESSAGE_SIZE};
    log_server_svc_client::LogServerSvcClient::new(channel)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE)
        // note: the order of those calls defines the priority
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .send_compressed(DEFAULT_GRPC_COMPRESSION)
}

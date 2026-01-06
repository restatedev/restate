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
#[cfg(feature = "grpc-client")]
pub fn new_log_server_client<O: restate_types::net::connect_opts::GrpcConnectionOptions>(
    channel: tonic::transport::Channel,
    connection_options: &O,
) -> log_server_svc_client::LogServerSvcClient<tonic::transport::Channel> {
    use tonic::codec::CompressionEncoding;
    /// Default send compression for grpc clients
    pub const DEFAULT_GRPC_COMPRESSION: CompressionEncoding = CompressionEncoding::Zstd;

    log_server_svc_client::LogServerSvcClient::new(channel)
        .max_decoding_message_size(connection_options.message_size_limit().get())
        // note: the order of those calls defines the priority
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(DEFAULT_GRPC_COMPRESSION)
}

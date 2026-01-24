// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::stream::BoxStream;
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use restate_types::config::NetworkingOptions;
use restate_types::net::connect_opts::GrpcConnectionOptions;

use crate::network::ConnectionManager;
use crate::network::protobuf::core_node_svc::core_node_svc_server::{
    CoreNodeSvc, CoreNodeSvcServer,
};
use crate::network::protobuf::core_node_svc::{RpcRequest, RpcResponse};
use crate::network::protobuf::network::Message;

pub struct CoreNodeSvcHandler {
    connections: ConnectionManager,
}

impl CoreNodeSvcHandler {
    pub fn new(connections: ConnectionManager) -> Self {
        Self { connections }
    }

    pub fn into_server(self, config: &NetworkingOptions) -> CoreNodeSvcServer<Self> {
        let server = CoreNodeSvcServer::new(self)
            .max_decoding_message_size(config.message_size_limit().get())
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip);
        if config.disable_compression {
            server
        } else {
            // note: the order of those calls defines the priority
            // deflate/gzip has significantly higher CPU overhead according to our CPU profiling,
            // so we prefer zstd over gzip.
            server
                .send_compressed(CompressionEncoding::Zstd)
                .send_compressed(CompressionEncoding::Gzip)
        }
    }
}

#[async_trait::async_trait]
impl CoreNodeSvc for CoreNodeSvcHandler {
    type CreateConnectionStream = BoxStream<'static, Result<Message, Status>>;

    // Status codes returned in different scenarios:
    // - DeadlineExceeded: No hello received within deadline
    // - InvalidArgument: Header should always be set or any other missing required part of the
    //                    handshake. This also happens if the client sent wrong message on handshake.
    // - AlreadyExists: A node with a newer generation has been observed already
    // - Cancelled: received an error from the client, or the client has dropped the stream during
    //              handshake.
    // - Unavailable: This node is shutting down
    async fn create_connection(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<Self::CreateConnectionStream>, Status> {
        let incoming = request.into_inner();
        let transformed = incoming.map_while(|x| match x {
            Ok(msg) => Some(msg),
            Err(err) => {
                warn!(%err, "Error while receiving network message from peer, connection will be dropped");
                None
            }
        });
        let output_stream = self
            .connections
            .accept_incoming_connection(transformed)
            .await?;

        // We map all responses to Ok, we never rely on sending tonic::Status errors explicitly.
        // We use ConnectionControl frames to communicate errors and/or drop the stream when necessary.
        Ok(Response::new(Box::pin(output_stream.map(Ok))))
    }

    async fn call_rpc(
        &self,
        _request: Request<RpcRequest>,
    ) -> Result<Response<RpcResponse>, Status> {
        todo!()
    }
}

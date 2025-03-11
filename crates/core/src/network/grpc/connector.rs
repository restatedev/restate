// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use tokio_stream::StreamExt;

use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::nodes_config::NodesConfiguration;
use tracing::trace;

use super::MAX_MESSAGE_SIZE;
use crate::network::net_util::create_tonic_channel;
use crate::network::protobuf::core_node_svc::core_node_svc_client::CoreNodeSvcClient;
use crate::network::protobuf::network::Message;
use crate::network::{NetworkError, ProtocolError, TransportConnect};

#[derive(Clone)]
pub struct GrpcConnector;

impl TransportConnect for GrpcConnector {
    async fn connect(
        &self,
        node_id: GenerationalNodeId,
        nodes_config: &NodesConfiguration,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> Result<
        impl Stream<Item = Result<Message, ProtocolError>> + Send + Unpin + 'static,
        NetworkError,
    > {
        let address = nodes_config.find_node_by_id(node_id)?.address.clone();

        trace!("Attempting to connect to node {} at {}", node_id, address);
        let channel = create_tonic_channel(address, &Configuration::pinned().networking);

        // Establish the connection
        let mut client = CoreNodeSvcClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_MESSAGE_SIZE);
        let incoming = client.create_connection(output_stream).await?.into_inner();
        Ok(incoming.map(|x| x.map_err(ProtocolError::from)))
    }
}

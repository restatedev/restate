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
#[cfg(unix)]
use http::Uri;
#[cfg(unix)]
use hyper_util::rt::TokioIo;
#[cfg(unix)]
use tokio::io;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
#[cfg(unix)]
use tonic::transport::Endpoint;
use tonic::transport::channel::Channel;
use tracing::debug;

use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::net::AdvertisedAddress;

use super::MAX_MESSAGE_SIZE;
use crate::network::grpc::DEFAULT_GRPC_COMPRESSION;
use crate::network::protobuf::core_node_svc::core_node_svc_client::CoreNodeSvcClient;
use crate::network::protobuf::network::Message;
use crate::network::transport_connector::find_node;
use crate::network::{ConnectError, Destination, Swimlane, TransportConnect};
use crate::{Metadata, TaskCenter, TaskKind};

#[derive(Clone, Default)]
pub struct GrpcConnector {
    _private: (),
}

impl TransportConnect for GrpcConnector {
    async fn connect(
        &self,
        destination: &Destination,
        swimlane: Swimlane,
        output_stream: impl Stream<Item = Message> + Send + Unpin + 'static,
    ) -> Result<impl Stream<Item = Message> + Send + Unpin + 'static, ConnectError> {
        let address = match destination {
            Destination::Node(node_id) => {
                find_node(&Metadata::with_current(|m| m.nodes_config_ref()), *node_id)?
                    .address
                    .clone()
            }
            Destination::Address(address) => address.clone(),
        };

        // Check for unsupported address types on Windows
        #[cfg(windows)]
        if matches!(address, AdvertisedAddress::Uds(_)) {
            return Err(ConnectError::Transport(
                "Unix domain sockets are not supported on Windows".to_string(),
            ));
        }

        debug!("Connecting to {} at {}", destination, address);
        let channel = create_channel(address, swimlane, &Configuration::pinned().networking);

        // Establish the connection
        let client = CoreNodeSvcClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip);
        // Apply send compression only if compression is enabled. Note that this doesn't impact the
        // "receive" compression. The receive compression is always applied if the peer compresses
        // its send stream.
        let mut client = if Configuration::pinned().networking.disable_compression {
            client
        } else {
            client.send_compressed(DEFAULT_GRPC_COMPRESSION)
        };

        let incoming = client.create_connection(output_stream).await?.into_inner();
        Ok(incoming.map_while(|x| x.ok()))
    }
}

fn create_channel(
    address: AdvertisedAddress,
    _swimlane: Swimlane,
    options: &NetworkingOptions,
) -> Channel {
    #[cfg(unix)]
    let endpoint = match &address {
        AdvertisedAddress::Uds(_) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1").expect("/ should be a valid Uri")
        }
        AdvertisedAddress::Http(uri) => Channel::builder(uri.clone()).executor(TaskCenterExecutor),
    };

    #[cfg(windows)]
    let endpoint = match &address {
        AdvertisedAddress::Uds(_) => {
            unreachable!("UDS addresses should be rejected before reaching create_channel on Windows")
        }
        AdvertisedAddress::Http(uri) => Channel::builder(uri.clone()).executor(TaskCenterExecutor),
    };

    let endpoint = endpoint
        .user_agent(format!(
            "restate/{}",
            option_env!("CARGO_PKG_VERSION").unwrap_or("dev")
        ))
        .unwrap()
        .connect_timeout(*options.connect_timeout)
        .http2_keep_alive_interval(*options.http2_keep_alive_interval)
        .keep_alive_timeout(*options.http2_keep_alive_timeout)
        .http2_adaptive_window(options.http2_adaptive_window)
        .initial_stream_window_size(options.stream_window_size())
        .initial_connection_window_size(options.connection_window_size())
        .keep_alive_while_idle(true)
        // this true by default, but this is to guard against any change in defaults
        .tcp_nodelay(true);

    #[cfg(unix)]
    match address {
        AdvertisedAddress::Uds(uds_path) => {
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let uds_path = uds_path.clone();
                async move {
                    Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                }
            }))
        }
        AdvertisedAddress::Http(_) => endpoint.connect_lazy()
    }

    #[cfg(windows)]
    match address {
        AdvertisedAddress::Uds(_) => {
            unreachable!("UDS addresses should be rejected before reaching create_channel on Windows")
        }
        AdvertisedAddress::Http(_) => endpoint.connect_lazy()
    }
}

#[derive(Clone, Default)]
struct TaskCenterExecutor;

impl<F> hyper::rt::Executor<F> for TaskCenterExecutor
where
    F: Future + 'static + Send,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        // This is unmanaged task because we don't want to bind the connection lifetime to the task
        // that created it, the connection reactor is already a managed task and will react to
        // global system shutdown and other graceful shutdown signals (i.e. dropping the owning
        // sender, or via egress_drop)
        //
        // Making this task managed will result in occasional lockups on shutdown.
        let _ = TaskCenter::spawn_unmanaged(TaskKind::H2ClientStream, "h2stream", async move {
            // ignore the future output
            let _ = fut.await;
        });
    }
}

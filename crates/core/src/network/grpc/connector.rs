// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use http::Uri;
use hyper_util::rt::TokioIo;
use tokio::io;
use tokio::net::UnixStream;
use rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::transport::Endpoint;
use tonic::transport::channel::Channel;
use tracing::{debug, warn};

use restate_types::config::{Configuration, NetworkingOptions};
use restate_types::net::address::{AdvertisedAddress, GrpcPort, ListenerPort, PeerNetAddress};
use restate_types::net::connect_opts::GrpcConnectionOptions;

use crate::network::grpc::DEFAULT_GRPC_COMPRESSION;
use crate::network::protobuf::core_node_svc::core_node_svc_client::CoreNodeSvcClient;
use crate::network::protobuf::network::Message;
use crate::network::tls::TlsCertResolver;
use crate::network::transport_connector::find_node;
use crate::network::{ConnectError, Destination, Swimlane, TransportConnect};
use crate::{Metadata, TaskCenter, TaskKind};

#[derive(Clone)]
pub struct GrpcConnector {
    tls: Option<TlsCertResolver>,
}

impl Default for GrpcConnector {
    fn default() -> Self {
        Self { tls: None }
    }
}

impl GrpcConnector {
    pub fn new(tls: Option<TlsCertResolver>) -> Self {
        Self { tls }
    }
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

        debug!("Connecting to {} at {}", destination, address);
        let networking = &Configuration::pinned().networking;
        let channel = create_channel(address, swimlane, networking, &self.tls);

        // Establish the connection
        let client = CoreNodeSvcClient::new(channel)
            .max_decoding_message_size(networking.message_size_limit().get())
            // note: the order of those calls defines the priority
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip);
        // Apply send compression only if compression is enabled. Note that this doesn't impact the
        // "receive" compression. The receive compression is always applied if the peer compresses
        // its send stream.
        let mut client = if networking.disable_compression {
            client
        } else {
            client.send_compressed(DEFAULT_GRPC_COMPRESSION)
        };

        let incoming = client.create_connection(output_stream).await?.into_inner();
        Ok(incoming.map_while(|x| match x {
            Ok(msg) => Some(msg),
            Err(err) => {
                warn!(%err, "Error while receiving network message from peer, connection will be dropped");
                None
            }
        }))
    }
}

fn create_channel<P: ListenerPort + GrpcPort>(
    address: AdvertisedAddress<P>,
    _swimlane: Swimlane,
    options: &NetworkingOptions,
    tls: &Option<TlsCertResolver>,
) -> Channel {
    let address = address.into_address().expect("valid address");
    let use_tls = address.is_tls() && tls.is_some();

    let endpoint = match &address {
        PeerNetAddress::Uds(_) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1").expect("/ should be a valid Uri")
        }
        PeerNetAddress::Http(uri) => Channel::builder(uri.clone()).executor(TaskCenterExecutor),
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
        .tcp_nodelay(true);

    match address {
        PeerNetAddress::Uds(uds_path) => {
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let uds_path = uds_path.clone();
                async move {
                    Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                }
            }))
        }
        PeerNetAddress::Http(uri) if use_tls => {
            let resolver = tls.as_ref().unwrap().clone();
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let resolver = resolver.clone();
                let host = uri.host().unwrap_or("localhost").to_owned();
                let port = uri.port_u16().unwrap_or(5122);
                async move {
                    let addr = format!("{host}:{port}");
                    let tcp_stream = tokio::net::TcpStream::connect(&addr).await?;
                    tcp_stream.set_nodelay(true)?;

                    let client_config = resolver.client_config();
                    let connector = TlsConnector::from(client_config);
                    let server_name = ServerName::try_from(host)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                    let tls_stream = connector.connect(server_name, tcp_stream).await?;
                    Ok::<_, io::Error>(TokioIo::new(tls_stream))
                }
            }))
        }
        PeerNetAddress::Http(_) => endpoint.connect_lazy(),
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

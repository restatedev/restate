// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status, Streaming};

use restate_types::GenerationalNodeId;
use restate_types::logs::{Keys, LogletId, LogletOffset, Record, SequenceNumber};
use restate_types::net::codec::{WireDecode, WireEncode};
use restate_types::net::log_server::{
    LogServerRequestHeader, PreparedPayloads, PreparedStore, StoreFlags,
};
use restate_types::net::{ProtocolVersion, RpcRequest, Service};
use restate_types::storage::PolyBytes;
use restate_types::time::{MillisSinceEpoch, NanosSinceEpoch};

use super::client::core_node_svc_client::CoreNodeSvcClient as PreparedClient;
use super::server::core_node_svc_server::{
    CoreNodeSvc as PreparedService, CoreNodeSvcServer as PreparedServer,
};
use crate::network::OutgoingMessage;
use crate::network::protobuf::core_node_svc::core_node_svc_client::CoreNodeSvcClient as StockClient;
use crate::network::protobuf::core_node_svc::core_node_svc_server::{
    CoreNodeSvc as StockService, CoreNodeSvcServer as StockServer,
};
use crate::network::protobuf::network::{Header, Message, RpcCall, datagram, message};

struct Echo;

fn prepare(message: Message) -> OutgoingMessage {
    let Some(message::Body::Datagram(datagram)) = message.body else {
        panic!("expected datagram")
    };
    let Some(datagram::Datagram::RpcCall(mut call)) = datagram.datagram else {
        panic!("expected RPC")
    };
    let request =
        PreparedStore::try_decode(std::mem::take(&mut call.payload), ProtocolVersion::V6).unwrap();
    OutgoingMessage::rpc_call(
        message.header,
        call,
        request.encode_payload(ProtocolVersion::V6).unwrap(),
    )
}

#[async_trait::async_trait]
impl PreparedService for Echo {
    type CreateConnectionStream = BoxStream<'static, Result<OutgoingMessage, Status>>;

    async fn create_connection(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<Self::CreateConnectionStream>, Status> {
        Ok(Response::new(request.into_inner().map_ok(prepare).boxed()))
    }
}

#[async_trait::async_trait]
impl StockService for Echo {
    type CreateConnectionStream = BoxStream<'static, Result<Message, Status>>;

    async fn create_connection(
        &self,
        request: Request<Streaming<Message>>,
    ) -> Result<Response<Self::CreateConnectionStream>, Status> {
        Ok(Response::new(request.into_inner().boxed()))
    }
}

fn messages() -> Vec<Message> {
    [(1, 512), (64, 512), (64, 4096), (256, 4096), (1, 1048576)]
        .into_iter()
        .enumerate()
        .map(|(index, (count, size))| {
            let records: Arc<[Record]> = (0..count)
                .map(|i| {
                    Record::from_parts(
                        NanosSinceEpoch::new(42),
                        Keys::None,
                        PolyBytes::Bytes(Bytes::from(vec![i as u8; size])),
                    )
                })
                .collect();
            let request = PreparedStore {
                header: LogServerRequestHeader::new(
                    LogletId::new_unchecked(123),
                    LogletOffset::from(index as u32),
                ),
                timeout_at: Some(MillisSinceEpoch::new(1_700_000_000_000 + index as u64)),
                flags: StoreFlags::empty(),
                first_offset: LogletOffset::OLDEST,
                sequencer: GenerationalNodeId::new(1, 1),
                known_archived: LogletOffset::INVALID,
                payloads: PreparedPayloads::new(records),
            };
            OutgoingMessage::rpc_call(
                Some(Header::default()),
                RpcCall {
                    id: index as u64,
                    service: <PreparedStore as RpcRequest>::Service::TAG.into(),
                    msg_type: PreparedStore::TYPE.into(),
                    sort_code: Some(123),
                    payload: Bytes::new(),
                },
                request.encode_payload(ProtocolVersion::V6).unwrap(),
            )
            .into_message()
        })
        .collect()
}

// Exercise both sides against stock generated Prost over actual HTTP/2, with
// concurrent streams on the same TCP channel, half-close, compression and limits.
#[tokio::test]
async fn prepared_transport_interoperates_with_stock_prost() {
    tokio::time::timeout(Duration::from_secs(30), async {
        for compression in [
            None,
            Some(CompressionEncoding::Gzip),
            Some(CompressionEncoding::Zstd),
        ] {
            for prepared_server in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (shutdown_tx, shutdown_rx) = oneshot::channel();
                let server = tokio::spawn(async move {
                    let incoming = TcpListenerStream::new(listener);
                    let shutdown = async {
                        let _ = shutdown_rx.await;
                    };
                    if prepared_server {
                        let mut service = PreparedServer::new(Echo);
                        if let Some(compression) = compression {
                            service = service
                                .accept_compressed(compression)
                                .send_compressed(compression);
                        }
                        tonic::transport::Server::builder()
                            .add_service(service)
                            .serve_with_incoming_shutdown(incoming, shutdown)
                            .await
                            .unwrap();
                    } else {
                        let mut service = StockServer::new(Echo);
                        if let Some(compression) = compression {
                            service = service
                                .accept_compressed(compression)
                                .send_compressed(compression);
                        }
                        tonic::transport::Server::builder()
                            .add_service(service)
                            .serve_with_incoming_shutdown(incoming, shutdown)
                            .await
                            .unwrap();
                    }
                });
                let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                let mut prepared = PreparedClient::new(channel.clone());
                let mut stock = StockClient::new(channel.clone());
                if let Some(compression) = compression {
                    prepared = prepared
                        .accept_compressed(compression)
                        .send_compressed(compression);
                    stock = stock
                        .accept_compressed(compression)
                        .send_compressed(compression);
                }
                let expected = messages();
                let outgoing = expected
                    .clone()
                    .into_iter()
                    .map(prepare)
                    .collect::<Vec<_>>();
                let (direct, ordinary) = tokio::join!(
                    prepared.create_connection(futures::stream::iter(outgoing)),
                    stock.create_connection(futures::stream::iter(expected.clone())),
                );
                let (direct, ordinary) = tokio::join!(
                    direct.unwrap().into_inner().try_collect::<Vec<_>>(),
                    ordinary.unwrap().into_inner().try_collect::<Vec<_>>(),
                );
                assert_eq!(direct.unwrap(), expected);
                assert_eq!(ordinary.unwrap(), expected);

                let mut limited = prepared.max_decoding_message_size(128);
                let mut response = limited
                    .create_connection(futures::stream::iter(expected.into_iter().map(prepare)))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(
                    response.message().await.unwrap_err().code(),
                    if compression.is_some() {
                        // Tonic reports the decompressed-size limit separately.
                        tonic::Code::ResourceExhausted
                    } else {
                        tonic::Code::OutOfRange
                    }
                );
                drop(response);
                drop(limited);
                drop(stock);
                drop(channel);
                shutdown_tx.send(()).unwrap();
                server.await.unwrap();
            }
        }
    })
    .await
    .expect("transport must finish and release all streams");
}

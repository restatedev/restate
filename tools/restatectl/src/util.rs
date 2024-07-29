// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hyper_util::rt::TokioIo;
use restate_cli_util::CliContext;
use restate_types::net::AdvertisedAddress;
use tokio::io;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

pub async fn grpc_connect(address: AdvertisedAddress) -> Result<Channel, tonic::transport::Error> {
    let ctx = CliContext::get();
    match address {
        AdvertisedAddress::Uds(uds_path) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1")
                .expect("/ should be a valid Uri")
                .connect_with_connector(service_fn(move |_: Uri| {
                    let uds_path = uds_path.clone();
                    async move {
                        Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                    }
                })).await
        }
        AdvertisedAddress::Http(uri) => {
            Channel::builder(uri)
                .connect_timeout(ctx.connect_timeout())
                .timeout(ctx.request_timeout())
                .http2_adaptive_window(true)
                .connect()
                .await
        }
    }
}

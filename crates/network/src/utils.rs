// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use http::Uri;
use restate_types::nodes_config::AdvertisedAddress;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

pub fn create_grpc_channel_from_network_address(
    address: AdvertisedAddress,
) -> Result<Channel, http::Error> {
    let channel = match address {
        AdvertisedAddress::Uds(uds_path) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("/")
                .expect("/ should be a valid Uri")
                .connect_with_connector_lazy(service_fn(move |_: Uri| {
                    UnixStream::connect(uds_path.clone())
                }))
        }
        AdvertisedAddress::Http(uri) => {
            // todo: Make the channel settings configurable
            Channel::builder(uri)
                .connect_timeout(Duration::from_secs(5))
                .connect_lazy()
        }
    };
    Ok(channel)
}

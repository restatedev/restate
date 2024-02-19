// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::Uri;
use restate_types::nodes_config::NetworkAddress;
use std::time::Duration;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;

pub trait NetworkAddressExt {
    fn network_address(&self) -> &NetworkAddress;

    fn connect_lazy(&self) -> Result<Channel, http::Error> {
        create_channel_from_network_address(self.network_address())
    }
}

impl NetworkAddressExt for NetworkAddress {
    fn network_address(&self) -> &NetworkAddress {
        self
    }
}

fn create_channel_from_network_address(
    network_address: &NetworkAddress,
) -> Result<Channel, http::Error> {
    let channel = match network_address {
        NetworkAddress::Uds(uds_path) => {
            let uds_path = uds_path.clone();
            // We need to specify a valid URI with scheme and authority since tower's AddOrigin expects it
            Endpoint::try_from("http://127.0.0.1")
                .expect("http://127.0.0.1 should be a valid Uri")
                .connect_with_connector_lazy(service_fn(move |_: Uri| {
                    UnixStream::connect(uds_path.clone())
                }))
        }
        NetworkAddress::TcpSocketAddr(socket_addr) => {
            let uri = create_uri(socket_addr)?;
            create_lazy_channel_from_uri(uri)
        }
        NetworkAddress::DnsName(dns_name) => {
            let uri = create_uri(dns_name)?;
            create_lazy_channel_from_uri(uri)
        }
    };
    Ok(channel)
}

fn create_uri(authority: impl ToString) -> Result<Uri, http::Error> {
    Uri::builder()
        // todo: Make the scheme configurable
        .scheme("http")
        .authority(authority.to_string())
        .path_and_query("/")
        .build()
}

fn create_lazy_channel_from_uri(uri: Uri) -> Channel {
    // todo: Make the channel settings configurable
    Channel::builder(uri)
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy()
}

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
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Hash,
    derive_more::Display,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub enum AdvertisedAddress {
    /// Unix domain socket
    #[display(fmt = "unix:{}", "_0.display()")]
    Uds(PathBuf),
    /// Hostname or host:port pair, or any unrecognizable string.
    #[display(fmt = "{}", _0)]
    Http(Uri),
}

impl FromStr for AdvertisedAddress {
    type Err = http::uri::InvalidUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(stripped_address) = s.strip_prefix("unix:") {
            Ok(AdvertisedAddress::Uds(
                stripped_address.parse().expect("infallible"),
            ))
        } else {
            // try to parse as a URI
            Ok(AdvertisedAddress::Http(s.parse()?))
        }
    }
}

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Hash,
    derive_more::Display,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub enum BindAddress {
    /// Unix domain socket
    #[display(fmt = "unix:{}", "_0.display()")]
    Uds(PathBuf),
    /// Socket addr.
    #[display(fmt = "{}", _0)]
    Socket(SocketAddr),
}

impl FromStr for BindAddress {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(stripped_address) = s.strip_prefix("unix:") {
            Ok(BindAddress::Uds(
                stripped_address.parse().expect("infallible"),
            ))
        } else {
            // try to parse as a URI
            Ok(BindAddress::Socket(s.parse()?))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::net::AdvertisedAddress;
    use http::Uri;

    // test parsing [`AdvertisedAddress`]
    #[test]
    fn test_parse_network_address() -> anyhow::Result<()> {
        let tcp: AdvertisedAddress = "127.0.0.1:5123".parse()?;
        restate_test_util::assert_eq!(tcp, AdvertisedAddress::Http("127.0.0.1:5123".parse()?));

        let tcp: AdvertisedAddress = "unix:/tmp/unix.socket".parse()?;
        restate_test_util::assert_eq!(
            tcp,
            AdvertisedAddress::Uds("/tmp/unix.socket".parse().unwrap())
        );

        let tcp: AdvertisedAddress = "localhost:5123".parse()?;
        restate_test_util::assert_eq!(tcp, AdvertisedAddress::Http("localhost:5123".parse()?));

        let tcp: AdvertisedAddress = "https://localhost:5123".parse()?;
        restate_test_util::assert_eq!(
            tcp,
            AdvertisedAddress::Http(Uri::from_static("https://localhost:5123"))
        );

        Ok(())
    }
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Picks the address a caller dials for a node from the node's location-qualified lists.
//!
//! The caller's location is walked outward: its zone, then its region, then the entries without
//! a location. At the first scope with a usable entry, the first such entry in list order wins.
//! The scalar address is the last resort and is returned even when it is not usable, so that a
//! failure surfaces exactly as it does without lists. Selection depends only on the caller's
//! location and the nodes configuration, so a caller that selects when it creates a connection
//! dials the same address on every retry and selects again only after observing a newer
//! configuration. Nothing dials candidates in order or falls through on failure.

use std::os::unix::net::SocketAddr as UnixSocketAddr;

use crate::config::TlsMode;
use crate::locality::{LocationScope, NodeLocation};
use crate::net::address::{AdvertisedAddress, ListenerPort, PeerNetAddress};
use crate::nodes_config::QualifiedAddress;

/// The first usable entry for a caller at `caller`, walking the caller's location outward: zone,
/// region, then the entries without a location. `None` when no entry is usable.
pub fn select_entry<'a, P: ListenerPort>(
    caller: &NodeLocation,
    list: &'a [QualifiedAddress<P>],
    usable: impl Fn(&AdvertisedAddress<P>) -> bool,
) -> Option<&'a AdvertisedAddress<P>> {
    for scope in [LocationScope::Zone, LocationScope::Region] {
        let found = list.iter().find(|entry| {
            entry.location.as_ref().is_some_and(|location| {
                location.smallest_defined_scope() == scope
                    && caller.shares_domain_with(location, scope)
            }) && usable(&entry.address)
        });
        if let Some(entry) = found {
            return Some(&entry.address);
        }
    }
    list.iter()
        .find(|entry| entry.location.is_none() && usable(&entry.address))
        .map(|entry| &entry.address)
}

/// [`select_entry`], ending with `scalar`, which is returned even when it is not usable.
pub fn select<'a, P: ListenerPort>(
    caller: &NodeLocation,
    scalar: &'a AdvertisedAddress<P>,
    list: &'a [QualifiedAddress<P>],
    usable: impl Fn(&AdvertisedAddress<P>) -> bool,
) -> &'a AdvertisedAddress<P> {
    select_entry(caller, list, usable).unwrap_or(scalar)
}

/// Whether this binary can dial `address` for its port kind under `tls_mode`: the scheme is one
/// the port kind accepts, a Unix socket path fits what the OS binds, and over TCP the TLS mode
/// decides as the connector does: `off` has no TLS client configuration and refuses `https`,
/// `require` refuses plaintext, and the modes between dial both. Reachability is never part of
/// the decision.
pub fn dialable<P: ListenerPort>(address: &AdvertisedAddress<P>, tls_mode: TlsMode) -> bool {
    match address.clone().into_address() {
        Ok(PeerNetAddress::Uds(path)) => UnixSocketAddr::from_pathname(&path).is_ok(),
        Ok(peer) => match tls_mode {
            TlsMode::Off => !peer.is_tls(),
            TlsMode::Allow | TlsMode::Prefer => true,
            TlsMode::Require => peer.is_tls(),
        },
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use crate::net::address::FabricPort;

    use super::*;

    fn entry(address: &str, location: Option<&str>) -> QualifiedAddress<FabricPort> {
        QualifiedAddress {
            address: address.parse().unwrap(),
            location: location.map(|location| location.parse().unwrap()),
        }
    }

    #[test]
    fn callers_walk_their_location_outward_and_end_with_the_scalar() {
        let scalar: AdvertisedAddress<FabricPort> = "https://node-7.example.com".parse().unwrap();
        // A future scheme sits first in the zone, so a caller that cannot use it takes the next
        // entry at the same location.
        let list = vec![
            entry("restate+tcp://10.1.7.3:5123", Some("us-east1.az1")),
            entry("http://10.1.7.3:5122", Some("us-east1.az1")),
            entry("http://node-7.us-east1.internal:5122", Some("us-east1")),
            entry("https://node-7.example.com", None),
            entry("http://node-7.example.com:5122", None),
        ];
        let dials = |caller: &str, tls_mode: TlsMode| {
            let caller: NodeLocation = caller.parse().unwrap();
            select(&caller, &scalar, &list, |address| {
                dialable(address, tls_mode)
            })
            .to_string()
        };
        for (caller, tls_mode, expected) in [
            ("us-east1.az1", TlsMode::Off, "http://10.1.7.3:5122/"),
            (
                "us-east1.az2",
                TlsMode::Off,
                "http://node-7.us-east1.internal:5122/",
            ),
            (
                "us-east1",
                TlsMode::Off,
                "http://node-7.us-east1.internal:5122/",
            ),
            // With TLS off, the `https` entry for every caller is skipped for the plaintext one.
            (
                "eu-west1.az1",
                TlsMode::Off,
                "http://node-7.example.com:5122/",
            ),
            ("", TlsMode::Off, "http://node-7.example.com:5122/"),
            // With TLS required every plaintext entry is skipped, down to the `https` entry for
            // every caller; with `prefer` the first entry at a scope wins either way.
            (
                "us-east1.az1",
                TlsMode::Require,
                "https://node-7.example.com/",
            ),
            ("us-east1.az1", TlsMode::Prefer, "http://10.1.7.3:5122/"),
            (
                "eu-west1.az1",
                TlsMode::Prefer,
                "https://node-7.example.com/",
            ),
        ] {
            assert_eq!(dials(caller, tls_mode), expected, "{caller} {tls_mode:?}");
        }
        // No entry at all: the scalar, usable or not.
        let caller: NodeLocation = "us-east1.az1".parse().unwrap();
        assert_eq!(select(&caller, &scalar, &[], |_| true), &scalar);
        assert_eq!(select(&caller, &scalar, &list, |_| false), &scalar);
        assert!(select_entry(&caller, &list, |_| false).is_none());

        // Dialability: the port kind's schemes, a bindable Unix path, and the TLS rule.
        let long_path = format!("unix:/{}", "p".repeat(120));
        for (address, tls_mode, expected) in [
            ("unix:/run/f.sock", TlsMode::Require, true),
            ("grpc+unix:/run/f.sock", TlsMode::Off, true),
            (long_path.as_str(), TlsMode::Off, false),
            ("ftp://h:21", TlsMode::Off, false),
            ("http://h:5122", TlsMode::Require, false),
            ("https://h", TlsMode::Require, true),
            ("https://h", TlsMode::Off, false),
            ("https://h", TlsMode::Prefer, true),
        ] {
            let address: AdvertisedAddress<FabricPort> = address.parse().unwrap();
            assert_eq!(
                dialable(&address, tls_mode),
                expected,
                "{address} {tls_mode:?}"
            );
        }
    }
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display};

use tonic::transport::Channel;

use restate_cli_util::CliContext;
use restate_core::network::net_util::create_tonic_channel;
use restate_types::{logs::metadata::ProviderConfiguration, net::AdvertisedAddress};

pub fn grpc_channel(address: AdvertisedAddress) -> Channel {
    let ctx = CliContext::get();
    create_tonic_channel(address, &ctx.network)
}

pub fn write_default_provider<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    provider: &ProviderConfiguration,
) -> Result<(), fmt::Error> {
    let title = "Logs Provider";
    match provider {
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderConfiguration::InMemory => {
            write_leaf(w, depth, true, title, "in-memory")?;
        }
        ProviderConfiguration::Local => {
            write_leaf(w, depth, true, title, "local")?;
        }
        #[cfg(feature = "replicated-loglet")]
        ProviderConfiguration::Replicated(config) => {
            write_leaf(w, depth, true, title, "replicated")?;
            let depth = depth + 1;
            write_leaf(
                w,
                depth,
                false,
                "Log replication",
                config.replication_property.to_string(),
            )?;
            write_leaf(
                w,
                depth,
                true,
                "Nodeset size",
                config.target_nodeset_size.to_string(),
            )?;
        }
    }
    Ok(())
}

pub fn write_leaf<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    last: bool,
    title: impl Display,
    value: impl Display,
) -> Result<(), fmt::Error> {
    let depth = depth + 1;
    let chr = if last { '└' } else { '├' };
    writeln!(w, "{chr:>depth$} {title}: {value}")
}

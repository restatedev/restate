// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    fmt::{self, Display},
    num::ParseIntError,
    ops::RangeInclusive,
    str::FromStr,
};

use cling::{Collect, prelude::Parser};
use tonic::transport::Channel;

use restate_cli_util::CliContext;
use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_types::{
    logs::metadata::ProviderConfiguration,
    net::address::{AdvertisedAddress, GrpcPort, ListenerPort},
};

pub fn grpc_channel<P: ListenerPort + GrpcPort>(address: AdvertisedAddress<P>) -> Channel {
    let ctx = CliContext::get();
    create_tonic_channel(address, &ctx.network, DNSResolution::Gai)
}

pub fn write_default_provider<W: fmt::Write>(
    w: &mut W,
    depth: usize,
    provider: &ProviderConfiguration,
) -> Result<(), fmt::Error> {
    let title = "Logs Provider";
    match provider {
        ProviderConfiguration::InMemory => {
            write_leaf(w, depth, true, title, "in-memory")?;
        }
        ProviderConfiguration::Local => {
            write_leaf(w, depth, true, title, "local")?;
        }
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

#[derive(Parser, Collect, Clone, Debug)]
pub struct RangeParam {
    from: u32,
    to: u32,
}

impl RangeParam {
    fn new(from: u32, to: u32) -> Result<Self, RangeParamError> {
        if from > to {
            Err(RangeParamError::InvalidRange(from, to))
        } else {
            Ok(RangeParam { from, to })
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> {
        self.from..=self.to
    }
}

impl<T> From<T> for RangeParam
where
    T: Into<u32>,
{
    fn from(value: T) -> Self {
        let id = value.into();
        Self::new(id, id).expect("equal values")
    }
}

impl IntoIterator for RangeParam {
    type IntoIter = RangeInclusive<u32>;
    type Item = u32;
    fn into_iter(self) -> Self::IntoIter {
        self.from..=self.to
    }
}

impl IntoIterator for &RangeParam {
    type IntoIter = RangeInclusive<u32>;
    type Item = u32;
    fn into_iter(self) -> Self::IntoIter {
        self.from..=self.to
    }
}

impl FromStr for RangeParam {
    type Err = RangeParamError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("-").collect();
        match parts.len() {
            1 => {
                let n = parts[0].parse()?;
                Ok(RangeParam::new(n, n)?)
            }
            2 => {
                let from = parts[0].parse()?;
                let to = parts[1].parse()?;
                Ok(RangeParam::new(from, to)?)
            }
            _ => Err(RangeParamError::InvalidSyntax(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RangeParamError {
    #[error("Invalid id range: {0}..{1} start must be <= end range")]
    InvalidRange(u32, u32),
    #[error("Invalid range syntax '{0}'")]
    InvalidSyntax(String),
    #[error(transparent)]
    ParseError(#[from] ParseIntError),
}

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
    str::FromStr,
};
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

/// A range parameter that parses `"5"` as a single value or `"1-10"` as a range.
/// The default type parameter is `u32`; use e.g. `RangeParam<u16>` for narrower types.
#[derive(Clone, Debug)]
pub struct RangeParam<T: RangeParamInt = u32> {
    from: T,
    to: T,
}

impl<T: RangeParamInt> RangeParam<T> {
    pub fn new(from: T, to: T) -> Result<Self, RangeParamError<T>> {
        if from > to {
            Err(RangeParamError::InvalidRange(from, to))
        } else {
            Ok(RangeParam { from, to })
        }
    }

    pub fn single(value: T) -> Self {
        Self {
            from: value,
            to: value,
        }
    }
}

impl<T: RangeParamInt> IntoIterator for RangeParam<T> {
    type Item = T;
    type IntoIter = RangeParamIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        RangeParamIter {
            current: self.from,
            end: self.to,
            done: false,
        }
    }
}

impl<T: RangeParamInt> IntoIterator for &RangeParam<T> {
    type Item = T;
    type IntoIter = RangeParamIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        RangeParamIter {
            current: self.from,
            end: self.to,
            done: false,
        }
    }
}

/// Iterator over a `RangeParam<T>` range (inclusive).
pub struct RangeParamIter<T: RangeParamInt> {
    current: T,
    end: T,
    done: bool,
}

impl<T: RangeParamInt> Iterator for RangeParamIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        if self.done {
            return None;
        }
        let value = self.current;
        if value == self.end {
            self.done = true;
        } else {
            self.current = value.next_value();
        }
        Some(value)
    }
}

impl<T: RangeParamInt> FromStr for RangeParam<T> {
    type Err = RangeParamError<T>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("-").collect();
        match parts.len() {
            1 => {
                let n = parts[0]
                    .parse()
                    .map_err(|_| RangeParamError::InvalidSyntax(s.to_string()))?;
                Ok(RangeParam::new(n, n)?)
            }
            2 => {
                let from = parts[0]
                    .parse()
                    .map_err(|_| RangeParamError::InvalidSyntax(s.to_string()))?;
                let to = parts[1]
                    .parse()
                    .map_err(|_| RangeParamError::InvalidSyntax(s.to_string()))?;
                Ok(RangeParam::new(from, to)?)
            }
            _ => Err(RangeParamError::InvalidSyntax(s.to_string())),
        }
    }
}

/// Trait bound for integer types usable with [`RangeParam`].
pub trait RangeParamInt: Copy + Ord + Eq + Display + fmt::Debug + FromStr + 'static {
    fn next_value(self) -> Self;
}

macro_rules! impl_range_param_int {
    ($($t:ty),*) => {
        $(impl RangeParamInt for $t {
            fn next_value(self) -> Self { self + 1 }
        })*
    };
}

impl_range_param_int!(u8, u16, u32, u64);

/// Convenience conversion for `From<u32>` on the default `RangeParam<u32>`.
impl From<u32> for RangeParam<u32> {
    fn from(value: u32) -> Self {
        Self::single(value)
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RangeParamError<T: RangeParamInt = u32> {
    #[error("Invalid id range: {0}..{1} start must be <= end range")]
    InvalidRange(T, T),
    #[error("Invalid range syntax '{0}'")]
    InvalidSyntax(String),
}

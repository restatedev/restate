// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use cling::prelude::*;
use restate_types::net::AdvertisedAddress;

use restate_cli_util::CliContext;
use restate_cli_util::CommonOpts;

use crate::commands::dump::Dump;
use crate::commands::log::Log;

#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about, infer_subcommands = true)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    pub common_opts: CommonOpts,
    #[clap(flatten)]
    pub connection: ConnectionInfo,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Parser, Collect, Debug, Clone)]
pub struct ConnectionInfo {
    /// Cluster Controller host:port (e.g. http://localhost:5122/)
    #[clap(long, value_hint= clap::ValueHint::Url, default_value_t = AdvertisedAddress::from_str("http://localhost:5122/").unwrap(), global = true)]
    pub cluster_controller: AdvertisedAddress,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    /// Dump various metadata from the cluster controller
    #[clap(subcommand)]
    Dump(Dump),
    /// Cluster distributed log operations
    #[clap(subcommand)]
    Logs(Log),
}

fn init(common_opts: &CommonOpts) {
    CliContext::new(common_opts.clone()).set_as_global();
}

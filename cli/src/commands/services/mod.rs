// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod config;
mod describe;
mod list;
mod status;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "svc", alias = "service")]
pub enum Services {
    /// List the registered services
    List(list::List),
    /// Prints detailed information about a given service
    Describe(describe::Describe),
    /// Prints activity information about a given service (and method)
    Status(status::Status),
    /// Configure a service
    #[clap(name = "config", alias = "conf")]
    #[clap(subcommand)]
    Config(config::Config),
}

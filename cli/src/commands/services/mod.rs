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

// Commands are documented on their own struct.
#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "svc", alias = "service")]
pub enum Services {
    List(list::List),
    Describe(describe::Describe),
    Status(status::Status),
    /// View and change a service's configuration: retention, timeouts, visibility
    #[clap(name = "config", alias = "conf")]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/services/configuration",
    ))]
    #[clap(subcommand)]
    Config(config::Config),
}

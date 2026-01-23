// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod configure;
mod tunnel;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "env", alias = "environment")]
pub enum Environments {
    /// Set up the CLI to talk to this Environment
    Configure(configure::Configure),
    /// Interact with your environment as if it was running on localhost
    ///
    /// Provide a local port to expose to your environment with --local-port,
    /// or remote ports to expose locally with --remote-port.
    /// If no ports are provided, localhost:9080 and all remote ports will be tunneled.
    Tunnel(tunnel::Tunnel),
}

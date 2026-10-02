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
    /// Set up the CLI to talk to a Cloud environment, and select it
    ///
    /// Writes a section for it in the CLI config file, named after the Cloud environment unless
    /// you pick another name when asked, and makes it the current environment (see
    /// `restate config use-environment`). A section with the same name is updated, after
    /// confirmation (or --yes). Needs `restate cloud login` first.
    Configure(configure::Configure),
    /// Connect a Cloud environment and this machine through a tunnel
    ///
    /// Uses the current environment (or -e), which must be a Cloud one. Exposes the
    /// environment's ports (ingress 8080, admin 9070) on localhost, and lets the environment
    /// call services running on this machine: register them with
    /// `restate deployments register --tunnel-name <name> http://localhost:9080` (use your
    /// service's URL). Runs until interrupted. By default, all remote ports are exposed and
    /// inbound calls are allowed.
    Tunnel(tunnel::Tunnel),
}

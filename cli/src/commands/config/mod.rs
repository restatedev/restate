// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod edit;
mod list_environments;
mod use_environment;
mod view;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Config {
    /// List the configured environments in the CLI config file
    ListEnvironments(list_environments::ListEnvironments),
    /// Set the current environment in $RESTATE_CONFIG_HOME/environment
    UseEnvironment(use_environment::UseEnvironment),
    /// Dump the current content of the CLI config file
    View(view::View),
    /// Edit the CLI config file
    Edit(edit::Edit),
}

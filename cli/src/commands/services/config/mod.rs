// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod edit;
mod patch;
mod view;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Config {
    /// Dump the current service configuration
    #[clap(name = "view", alias = "get")]
    View(view::View),
    /// Interactively edit the service configuration
    Edit(edit::Edit),
    /// Patch the service configuration either with a file or with the provided arguments.
    ///
    /// *NOTE:* Service re-discovery will update the settings based on the service endpoint configuration.
    Patch(patch::Patch),
}

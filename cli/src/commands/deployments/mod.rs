// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod describe;
mod list;
mod register;
mod remove;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "dp", alias = "deployment")]
pub enum Deployments {
    /// List the registered deployments
    List(list::List),
    /// Add or update deployments through deployment discovery
    Register(register::Register),
    /// Prints detailed information about a given deployment
    Describe(describe::Describe),
    /// Remove a drained deployment
    Remove(remove::Remove),
}

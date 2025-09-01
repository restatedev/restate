// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cancel;
mod describe;
mod kill;
mod list;
mod purge;
mod restart_as_new;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Invocations {
    /// List invocations
    List(list::List),
    /// Prints detailed information about a given invocation
    Describe(describe::Describe),
    /// Cancel a given invocation, or a set of invocations, and its children
    Cancel(cancel::Cancel),
    /// Cancel a given invocation, or a set of invocations, and its children
    Kill(cancel::Cancel),
    /// Purge a completed invocation, or a set of invocations. This command affects only completed invocations.
    Purge(purge::Purge),
    /// Restart a completed invocation, or a set of invocations. This command affects only completed invocations. Note: this command doesn't work on workflows yet.
    RestartAsNew(restart_as_new::RestartAsNew),
}

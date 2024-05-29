// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
mod list;
mod purge;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Invocations {
    /// List invocations of a service
    List(list::List),
    /// Prints detailed information about a given invocation
    Describe(describe::Describe),
    /// Cancel a given invocation, or a set of invocations, and its children
    Cancel(cancel::Cancel),
    /// Purge a completed invocation, or a set of invocations. This command affects only completed invocations.
    Purge(purge::Purge),
}

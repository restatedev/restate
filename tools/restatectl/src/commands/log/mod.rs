// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod get;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Log {
    /// Print the log state by partition
    State(get::StateOpts),
}

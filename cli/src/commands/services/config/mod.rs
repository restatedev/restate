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
mod patch;
mod view;

use cling::prelude::*;

// Commands are documented on their own struct.
#[derive(Run, Subcommand, Clone)]
pub enum Config {
    #[clap(name = "view", alias = "get")]
    View(view::View),
    Edit(edit::Edit),
    Patch(patch::Patch),
}

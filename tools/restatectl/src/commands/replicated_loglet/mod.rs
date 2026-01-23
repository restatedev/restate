// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod digest;
mod digest_util;
mod info;
mod list_servers;

use cling::prelude::*;

use restate_cli_util::_comfy_table::{Cell, Color};
use restate_types::nodes_config::StorageState;

#[derive(Run, Subcommand, Clone)]
pub enum ReplicatedLoglet {
    /// View digest of records in a given loglet.
    Digest(digest::DigestOpts),
    /// View loglet info
    Info(info::InfoOpts),
    /// View log-server(s) state
    ListServers(list_servers::ListServersOpts),
}

fn render_storage_state(state: StorageState) -> Cell {
    let cell = Cell::new(state);
    match state {
        StorageState::ReadWrite => cell.fg(Color::Green),
        StorageState::ReadOnly => cell.fg(Color::Yellow),
        StorageState::Gone => cell.fg(Color::Yellow),
        StorageState::DataLoss => cell.fg(Color::Red),
        StorageState::Provisioning => cell.fg(Color::Reset),
        StorageState::Disabled => cell.fg(Color::Grey),
    }
}

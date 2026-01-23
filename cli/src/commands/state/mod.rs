// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod clear;
mod edit;
mod get;
mod patch;
mod util;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum ServiceState {
    /// Get the persisted state stored for a service key
    Get(get::Get),
    /// Edit the persisted state stored for a service key
    Edit(edit::Edit),
    /// Patch persisted key-value state for a service key
    Patch(patch::Patch),
    /// Clear of the state of a given service
    Clear(clear::Clear),
}

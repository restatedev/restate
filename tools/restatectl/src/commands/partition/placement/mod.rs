// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod freeze;
mod set;
mod show;
mod unfreeze;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum Placement {
    /// Explicitly set the replicas for a partition
    Set(set::SetOpts),
    /// Freeze automatic placement changes for partitions
    Freeze(freeze::FreezeOpts),
    /// Unfreeze automatic placement changes for partitions
    Unfreeze(unfreeze::UnfreezeOpts),
    /// Show partition placement and policy
    Show(show::ShowOpts),
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod create;
mod delete;
mod describe;
mod edit;
mod list;
mod patch;
pub(crate) mod utils;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "kc", alias = "kafkacluster")]
pub enum KafkaClusters {
    /// List the registered Kafka clusters
    List(list::List),
    /// Register a new Kafka cluster
    Create(create::Create),
    /// Print detailed information about a Kafka cluster
    Describe(describe::Describe),
    /// Open an editor to interactively update a Kafka cluster's properties
    Edit(edit::Edit),
    /// Update a Kafka cluster's properties non-interactively (CI / scripting)
    Patch(patch::Patch),
    /// Remove a Kafka cluster
    Delete(delete::Delete),
}

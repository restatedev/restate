// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod add;
pub mod describe;
pub mod list;
pub mod remove;
pub mod rename;
pub mod sections;
pub mod use_profile;

use cling::prelude::*;

/// Manage the profiles the CLI talks to: list, describe, select, add, remove, rename
///
/// A profile is a section of the CLI config file naming a server: a Restate Cloud
/// environment, or a self-hosted server (its admin and ingress URLs, and a bearer token if it
/// needs one). The current one is used by every command, unless -p/--profile or
/// $RESTATE_PROFILE says otherwise; `local` (a server on this machine) is built in.
#[derive(Run, Subcommand, Clone)]
#[clap(alias = "profiles")]
#[allow(clippy::large_enum_variant)]
pub enum Profile {
    // Commands are documented on their own struct.
    List(list::List),
    Describe(describe::Describe),
    Use(use_profile::Use),
    Add(add::Add),
    Remove(remove::Remove),
    Rename(rename::Rename),
}

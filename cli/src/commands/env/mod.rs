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
pub mod list;
pub mod profile;
pub mod remove;
pub mod rename;
pub mod use_env;

use cling::prelude::*;

/// Manage the environments the CLI talks to: list, select, add, remove, rename
///
/// An environment is a section of the CLI config file naming a server: a Restate Cloud
/// environment, or a self-hosted server (its admin and ingress URLs, and a bearer token if it
/// needs one). The current one is used by every command, unless -e/--environment or
/// $RESTATE_ENVIRONMENT says otherwise; `local` (a server on this machine) is built in.
#[derive(Run, Subcommand, Clone)]
#[clap(alias = "environment", alias = "environments")]
#[allow(clippy::large_enum_variant)]
pub enum Env {
    // Commands are documented on their own struct.
    List(list::List),
    Use(use_env::Use),
    Add(add::Add),
    Remove(remove::Remove),
    Rename(rename::Rename),
}

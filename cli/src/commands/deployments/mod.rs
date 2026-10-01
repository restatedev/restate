// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod describe;
mod list;
mod register;
mod remove;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
#[clap(visible_alias = "dp", alias = "deployment")]
// `Register`'s GCP/Lambda auth flags make it much larger than its sibling variants, but this is a
// short-lived CLI command struct constructed once per invocation, so the allocation churn a
// smaller enum would trade for isn't worth the indirection.
#[allow(clippy::large_enum_variant)]
pub enum Deployments {
    // Commands are documented on their own struct.
    List(list::List),
    Register(register::Register),
    Describe(describe::Describe),
    Remove(remove::Remove),
}

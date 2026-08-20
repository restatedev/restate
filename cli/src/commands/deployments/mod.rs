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
pub enum Deployments {
    /// List the registered deployments
    List(list::List),
    /// Add or update deployments through deployment discovery
    Register(Box<register::Register>),
    /// Prints detailed information about a given deployment
    Describe(describe::Describe),
    /// Remove a drained deployment
    Remove(remove::Remove),
}

// `Register` carries several optional GCP/Lambda auth flags on top of the base discovery
// options, which otherwise made it (and so `Deployments`, one of clap's tightly-packed
// `Subcommand` enums) more than 6x larger than every sibling variant; boxing it keeps the enum
// itself small regardless of how many flags `Register` grows in the future. `cling::Run` has no
// blanket impl for `Box<T>` (unlike `clap`'s own `Args`/`FromArgMatches`, which do), so it needs
// this explicit one-line forward.
impl Run for Box<register::Register> {
    fn call<'a>(
        &'a self,
        args: &'a mut cling::_private::CollectedArgs,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), CliError>> + Send + 'a>>
    {
        (**self).call(args)
    }
}

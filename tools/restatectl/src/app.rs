// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;

use restate_cli_util::c_tip;
use restate_cli_util::CliContext;
use restate_cli_util::CommonOpts;

#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about, infer_subcommands = true)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    pub common_opts: CommonOpts,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    #[cling(run = "hello")]
    Hello,
}

fn init(common_opts: &CommonOpts) {
    CliContext::new(common_opts.clone()).set_as_global();
}

async fn hello() {
    c_tip!("Placeholder");
}

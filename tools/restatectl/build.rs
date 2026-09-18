// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use vergen_gitcl::{Build, Cargo, Emitter, Gitcl};

fn main() -> Result<(), Box<dyn Error>> {
    let cargo = Cargo::builder()
        .features(true)
        .opt_level(true)
        .target_triple(true)
        .debug(true)
        .build();
    let git = Gitcl::builder()
        .branch(true)
        .commit_date(true)
        .commit_timestamp(true)
        .sha(true)
        .build();
    Emitter::default()
        .add_instructions(&Build::all_build())?
        .add_instructions(&cargo)?
        .add_instructions(&git)?
        .emit()?;
    Ok(())
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    // Emit the instructions
    EmitBuilder::builder()
        .build_date()
        .build_timestamp()
        .cargo_features()
        .cargo_opt_level()
        .cargo_target_triple()
        .cargo_debug()
        .git_branch()
        .git_commit_date()
        .git_commit_timestamp()
        .git_sha(true)
        .emit()?;
    Ok(())
}

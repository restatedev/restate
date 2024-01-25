// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod edit;
mod get;
mod util;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum ServiceState {
    Get(get::Get),
    Edit(edit::Edit),
}

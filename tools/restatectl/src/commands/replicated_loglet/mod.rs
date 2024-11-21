// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod digest;
mod digest_util;
mod info;

use cling::prelude::*;

#[derive(Run, Subcommand, Clone)]
pub enum ReplicatedLoglet {
    /// View digest of records in a given loglet.
    Digest(digest::DigestOpts),
    /// View loglet info
    Info(info::InfoOpts),
}

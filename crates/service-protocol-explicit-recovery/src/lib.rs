// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wire format of the explicit recovery protocol: the generated messages of
//! `explicit_recovery.proto` and the framing codec.

pub mod codec;

use restate_workspace_hack as _;

#[allow(clippy::enum_variant_names)]
pub mod proto {
    include!(concat!(
        env!("OUT_DIR"),
        "/dev.restate.service.explicitrecovery.rs"
    ));
}

/// Lowest version of the explicit recovery protocol this crate speaks.
pub const MIN_VERSION: i32 = 1;
/// Highest version of the explicit recovery protocol this crate speaks.
pub const MAX_VERSION: i32 = 1;

/// Content type of version 1.
pub const CONTENT_TYPE_V1: &str = "application/vnd.restate.invocation.explicitrecovery.v1";

/// The content type of a version, if this crate speaks it.
pub fn content_type(version: i32) -> Option<&'static str> {
    match version {
        1 => Some(CONTENT_TYPE_V1),
        _ => None,
    }
}

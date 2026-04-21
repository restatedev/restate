// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]
// todo: Implementation comes in a later stage

use std::num::NonZeroU64;

use restate_limiter::Rules;
use restate_util_string::ReString;

// This lives here temporarily until it finds a proper home
#[derive(Default, Clone)]
pub struct UserLimits {
    // None means unlimited
    action_concurrency: Option<NonZeroU64>,
}

#[allow(dead_code)]
#[derive(Default)]
pub struct UserLimiter {
    /// User defined limits
    user_limits: Rules<ReString, UserLimits>,
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Useful test utilities for Restate crates. Import them with:
//!
//! ```rust
//! use restate_test_util::{assert, assert_eq, assert_ne};
//! ```
//!
//! Note: You cannot import them with a glob import, as the compiler won't be able to distinguish our imports and the stdlib imports.

/// Macro to assert no message is received.
///
/// Note: this check is based on a timeout, hence based on the context it might not be enough to use
/// this assert to check the behaviour of the producers.
#[macro_export]
macro_rules! assert_no_recv {
    ($ch:expr) => {
         assert!(let Err(_) = tokio::time::timeout(std::time::Duration::from_millis(100), $ch.recv()).await);
    };
}

// A couple of useful re-exports
pub use assert2::{assert, check, let_assert};
pub use pretty_assertions::{assert_eq, assert_ne};

pub mod matchers;
pub mod rand;

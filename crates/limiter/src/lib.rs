// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Hierarchical concurrency limiter with 3-level rule matching.
//!
//! This crate provides a concurrency control mechanism that enforces limits at multiple
//! granularity levels (Scope, Level 1, Level 2) with rule-based limit resolution.
//!
//! # Overview
//!
//! The limiter uses a 3-level hierarchy:
//! - **Scope**: Top-level grouping (aka. Scope)
//! - **Level 1**: Mid-level grouping
//! - **Level 2**: Fine-grained grouping
//!
//! # Value Validation
//!
//! All key components are validated using the [`restate_util_string::RestrictedValue`] type which
//! guarantees values are:
//! - Non-empty strings
//! - Maximum 36 bytes
//! - Only `[a-zA-Z0-9_.-]` characters
//!
//! # Rule Matching
//!
//! Rules can use wildcards (`*`) to match multiple values at any level:
//! - `SCOPE = 1000` - Limit for a specific Scope value
//! - `* = 5000` - Default limit for any scope value (default)
//! - `SCOPE/* = 100` - Default limit per Level 1 under specific scope (SCOPE)
//! - `*/* = 100` - Default limit per Level 1 under any scope
//! - `SCOPE/L1 = 250` - Specific limit for a Level 1 specific value (L1)
//! - `SCOPE/*/* = 2` - Default limit per Level 2
//! - `SCOPE/*/L2 = 10` - Specific limit for a Level 2 value across all Level 2 values

mod key;
mod rule;
#[cfg(feature = "rule-book")]
pub mod rule_book;
mod rule_store;
mod user_limits;

// Re-exports
pub use key::LimitKey;
pub use rule::{Pattern, RuleHandle, RulePattern};
#[cfg(feature = "rule-book")]
pub use rule_book::{
    LimitsPatch, NewRule, PersistedRule, RuleBook, RuleBookError, RuleChange, RuleId, RulePatch,
    UpdateField,
};
pub use rule_store::{Limit, Rules, StructuredLimits};
pub use user_limits::{RuleUpdate, UserLimits};

/// Represents the hierarchy level of counters or rules
///
/// This enum is used throughout the limiter to identify which level
/// of the hierarchy is being referenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Level {
    /// Level 1 - top-level grouping (aka. Scope)
    Scope = 1,
    /// Level 2 - mid-level grouping.
    Level1 = 2,
    /// Level 3 - fine-grained grouping.
    Level2 = 3,
}

impl Level {
    /// The number of levels in the hierarchy.
    pub const COUNT: usize = 3;

    /// Returns the level as a `u8` value (1, 2, or 3).
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Creates a Level from a u8 value.
    ///
    /// Returns `None` if the value is not 1, 2, or 3.
    #[inline]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Scope),
            2 => Some(Self::Level1),
            3 => Some(Self::Level2),
            _ => None,
        }
    }
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scope => write!(f, "Scope"),
            Self::Level1 => write!(f, "Level1"),
            Self::Level2 => write!(f, "Level2"),
        }
    }
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the core types used by various Restate components.

mod base62_util;
mod lambda;
mod merge;
mod node_id;
mod restate_version;
mod shared_string;
mod version;

pub mod errors;
pub mod identifiers;
pub mod invocation;
pub mod journal;
pub mod locality;
pub mod logs;
pub mod metadata;
pub mod net;
pub mod partitions;
pub mod protobuf;
pub mod storage;

pub use lambda::*;
pub use merge::*;
pub use node_id::*;
pub use restate_version::*;
pub use version::*;

// Re-export metrics' SharedString (Space-efficient Cow + RefCounted variant)
// pub type SharedString = metrics::SharedString;
// pub use shared_string::SharedString;

/// An allocation-optimized string.
///
/// `SharedString` uses a custom copy-on-write implementation that is optimized for metric keys,
/// providing ergonomic sharing of single instances, or slices, of strings and labels. This
/// copy-on-write implementation is optimized to allow for constant-time construction (using static
/// values), as well as accepting owned values and values shared through [`Arc<T>`](std::sync::Arc).
///
/// End users generally will not need to interact with this type directly, as the top-level macros
/// (`counter!`, etc), as well as the various conversion implementations
/// ([`From<T>`](std::convert::From)), generally allow users to pass whichever variant of a value
/// (static, owned, shared) is best for them.
pub type SharedString = shared_string::Cow<'static, str>;

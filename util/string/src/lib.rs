// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod restricted_value;

pub use restricted_value::{RestrictedValue, RestrictedValueError};

use std::fmt;
use std::hash::Hash;

/// Trait alias for types that can be used as string components (SharedString, ByteString, etc.)
///
/// This is satisfied by types that:
/// - Can be created from `&str` (via `From<&str>`)
/// - Can be viewed as `&str` (via `AsRef<str>`)
/// - Are `Clone + Eq + Hash + Debug`
///
/// `String` and `ByteString` satisfy these bounds.
pub trait StringLike: Clone + Eq + Hash + fmt::Debug + for<'a> From<&'a str> + AsRef<str> {}

impl<T> StringLike for T where T: Clone + Eq + Hash + fmt::Debug + for<'a> From<&'a str> + AsRef<str>
{}

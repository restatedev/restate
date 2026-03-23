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

/// Trait alias for types that can be used as strings (SharedString, &str, ByteString, etc.)
///
/// This is satisfied by types that:
/// - Can be viewed as `&str` (via `AsRef<str>`)
/// - Are `Eq + Hash + Debug`
pub trait StringLike:
    Eq + Hash + fmt::Debug + fmt::Display + AsRef<str> + std::ops::Deref<Target = str>
{
}

impl<T> StringLike for T where
    T: Eq + Hash + fmt::Debug + fmt::Display + AsRef<str> + std::ops::Deref<Target = str>
{
}

/// Trait alias for types that can be used as owned strings (SharedString, ByteString, etc.)
///
/// This is satisfied by types that:
/// - Can be created from `&str` (via `From<&str>`)
/// - Can be viewed as `&str` (via `AsRef<str>`)
/// - Are `Clone + Eq + Hash + Debug`
///
/// `String` and `ByteString` satisfy these bounds.
pub trait OwnedStringLike: StringLike + Clone + for<'a> From<&'a str> {}

impl<T> OwnedStringLike for T where T: StringLike + Clone + for<'a> From<&'a str> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_owned_string_like<T: OwnedStringLike>(_: T) {
        assert!(true);
    }

    fn _assert_string_like<T: StringLike>(_: T) {
        assert!(true);
    }

    #[test]
    fn owned_string_like() {
        let s: String = "hello".to_owned();
        assert_eq!(s, "hello");
        _assert_owned_string_like(s.clone());
        _assert_string_like(s);
        // non-owned

        let s = "hello";
        assert_eq!(s, "hello");
        _assert_string_like(s);
    }

}

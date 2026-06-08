// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Validated "Restricted Value" string type.
//!
//! A restricted value is a string that:
//! - Is non-empty
//! - Is at most [`MAX_LEN`] bytes long
//! - Contains only characters from `[a-zA-Z0-9_.-]`
//!
//! This is used as the foundation for validated limiter key components.
//!
//! # Examples
//!
//! ```
//! use restate_util_string::RestrictedValue;
//!
//! let val: RestrictedValue<String> = "my_scope.v2-beta".parse().unwrap();
//! assert_eq!(val.as_str(), "my_scope.v2-beta");
//!
//! // Invalid values are rejected
//! assert!("".parse::<RestrictedValue<String>>().is_err());
//! assert!("has space".parse::<RestrictedValue<String>>().is_err());
//! assert!("a/b".parse::<RestrictedValue<String>>().is_err());
//! ```

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use crate::{OwnedStringLike, ReString, RestateString, StringLike};

/// Maximum length of a restricted value in bytes.
pub const MAX_LEN: usize = 36;

/// Error returned when a string does not satisfy restricted value rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestrictedValueError {
    /// The value is empty.
    Empty,
    /// The value contains characters outside the allowed ASCII set
    /// `[a-zA-Z0-9_.-]` (or contains non-ASCII bytes).
    Invalid,
    /// The value contains invalid UTF-8.
    Utf8Error(std::str::Utf8Error),
    /// The value exceeds [`MAX_LEN`] bytes.
    TooLong { len: usize, max: usize },
}

impl fmt::Display for RestrictedValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "value cannot be empty"),
            Self::Invalid => write!(
                f,
                "value contains characters outside the allowed set [a-zA-Z0-9_.-]"
            ),
            Self::Utf8Error(e) => write!(f, "value contains invalid UTF-8: {}", e),
            Self::TooLong { len, max } => {
                write!(f, "value too long: {len} bytes (max {max})")
            }
        }
    }
}

impl std::error::Error for RestrictedValueError {}

/// Validate a string against restricted value rules.
///
/// Returns `Ok(())` if valid, or the first violation found.
pub fn validate(s: &str) -> Result<(), RestrictedValueError> {
    /// Byte lookup table: `true` for any byte in `[a-zA-Z0-9_.-]`. All other bytes
    /// (including every byte ≥ 0x80, i.e. non-ASCII / UTF-8 continuation/lead bytes)
    /// are `false`. The allowed set is ASCII-only, so byte-level scanning is
    /// equivalent to char-level scanning here.
    const TABLE: [bool; 256] = build_table();
    const fn build_table() -> [bool; 256] {
        let mut t = [false; 256];
        let mut i = 0u16;
        while i < 256 {
            let b = i as u8;
            t[i as usize] = b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-';
            i += 1;
        }
        t
    }

    if s.is_empty() {
        return Err(RestrictedValueError::Empty);
    }
    if s.len() > MAX_LEN {
        return Err(RestrictedValueError::TooLong {
            len: s.len(),
            max: MAX_LEN,
        });
    }
    if !s.as_bytes().iter().all(|&b| TABLE[b as usize]) {
        return Err(RestrictedValueError::Invalid);
    }
    Ok(())
}

/// A validated restricted value string.
///
/// Guarantees that the inner string:
/// - Is non-empty
/// - Is at most [`MAX_LEN`] bytes
/// - Contains only `[a-zA-Z0-9_.-]`
#[derive(Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct RestrictedValue<S>(S);

impl<T> RestrictedValue<T> {
    pub const MAX_LEN: usize = 36;
}

impl<S: Deref> RestrictedValue<S> {
    /// Converts from `RestrictedValue<T>` (or `&RestrictedValue<T>`) to `RestrictedValue<&T::Target>`.
    /// Leaves the original RestrictedValue in-place, creating a new one with a reference to the
    /// original one, additionally coercing the contents via Deref.
    #[inline]
    pub fn as_deref(&self) -> RestrictedValue<&<S as std::ops::Deref>::Target> {
        RestrictedValue(self.0.deref())
    }
}

impl RestateString for RestrictedValue<ReString> {
    type Err = RestrictedValueError;

    #[inline]
    unsafe fn new_unchecked(s: &str) -> Self {
        Self(ReString::new(s))
    }

    fn try_from_restring(s: ReString) -> Result<Self, Self::Err> {
        validate(&s)?;
        Ok(Self(s))
    }

    fn try_from_static(s: &'static str) -> Result<Self, Self::Err> {
        validate(s)?;
        Ok(Self(ReString::from_static(s)))
    }

    fn try_new(s: &str) -> Result<Self, Self::Err> {
        validate(s)?;
        Ok(Self(ReString::new(s)))
    }

    fn try_from_arc(s: &Arc<str>) -> Result<Self, Self::Err> {
        validate(s)?;
        Ok(Self(ReString::from(s)))
    }

    fn to_restring(&self) -> ReString {
        self.0.clone()
    }

    fn into_restring(self) -> ReString {
        self.0
    }

    /// Wrap a well-formed restricted value without checking it.
    ///
    /// # Safety
    ///
    /// This is unsafe because it skips input validation. It's the caller's responsibility to
    /// ensure that the input is a valid RestrictedValue.
    unsafe fn from_restring_unchecked(s: ReString) -> Self {
        Self(s)
    }
}

impl crate::interned::Internable for RestrictedValue<ReString> {
    fn intern_pool() -> &'static std::thread::LocalKey<crate::interned::InternPool> {
        thread_local! {
            static POOL: crate::interned::InternPool =
                std::cell::RefCell::new(hashbrown::HashSet::with_capacity(20_000));
        }
        &POOL
    }
}

impl<S: StringLike> RestrictedValue<S> {
    /// Create a new restricted value, validating the input.
    pub fn new(s: S) -> Result<Self, RestrictedValueError> {
        validate(s.as_ref())?;
        Ok(Self(s))
    }

    /// Wrap a well-formed restricted value without checking it.
    ///
    /// # Safety
    ///
    /// This is unsafe because it skips input validation. It's the caller's responsibility to
    /// ensure that the input is a valid RestrictedValue.
    pub unsafe fn new_unchecked(s: S) -> Self {
        Self(s)
    }

    // /// Returns a `RestrictedValue<ReString>` containing a copy of this value's
    // /// contents. Inline content stays inline; longer content is promoted to a
    // /// reference-counted `Arc<str>`. The returned value is always cheap to
    // /// clone.
    // pub fn to_cheap_cloneable(&self) -> RestrictedValue<ReString> {
    //     RestrictedValue(ReString::new(self.0.as_ref()))
    // }

    /// Returns the value as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Consumes the wrapper and returns the inner value.
    #[inline]
    pub fn into_inner(self) -> S {
        self.0
    }
}

impl<S: StringLike> fmt::Debug for RestrictedValue<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl<S: StringLike> fmt::Display for RestrictedValue<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl<S: StringLike> AsRef<str> for RestrictedValue<S> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<S: StringLike> AsRef<S> for RestrictedValue<S> {
    #[inline]
    fn as_ref(&self) -> &S {
        &self.0
    }
}

impl<S: StringLike> Borrow<S> for RestrictedValue<S> {
    #[inline]
    fn borrow(&self) -> &S {
        &self.0
    }
}

impl<S: StringLike + Borrow<str>> Borrow<str> for RestrictedValue<S> {
    #[inline]
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl<S: StringLike> std::ops::Deref for RestrictedValue<S> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

// Needed for hashbrown's entry_ref API to lazily convert the key reference on insert.
impl<S: Clone> From<&RestrictedValue<S>> for RestrictedValue<S> {
    fn from(value: &RestrictedValue<S>) -> Self {
        value.clone()
    }
}

impl<S: OwnedStringLike> FromStr for RestrictedValue<S> {
    type Err = RestrictedValueError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        validate(s)?;
        Ok(Self(S::from(s)))
    }
}

impl<S: StringLike> PartialEq<str> for RestrictedValue<S> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_values() {
        // Alphanumeric
        let v: RestrictedValue<String> = "scope1".parse().unwrap();
        assert_eq!(v.as_str(), "scope1");

        // With allowed special characters
        let v: RestrictedValue<String> = "my_scope.v2-beta".parse().unwrap();
        assert_eq!(v.as_str(), "my_scope.v2-beta");

        // Single character
        let v: RestrictedValue<String> = "a".parse().unwrap();
        assert_eq!(v.as_str(), "a");

        // Max length
        let max = "a".repeat(MAX_LEN);
        let v: RestrictedValue<String> = max.parse().unwrap();
        assert_eq!(v.as_str().len(), MAX_LEN);
    }

    #[test]
    fn references() {
        // Alphanumeric
        let input = "scope1";
        let v: RestrictedValue<&str> = RestrictedValue::new(input).unwrap();
        assert_eq!(v.as_str(), "scope1");
    }

    #[test]
    fn invalid_empty() {
        let err = "".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::Empty);
    }

    #[test]
    fn invalid_too_long() {
        let too_long = "a".repeat(MAX_LEN + 1);
        let err = too_long.parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(
            err,
            RestrictedValueError::TooLong {
                len: MAX_LEN + 1,
                max: MAX_LEN,
            }
        );
    }

    #[test]
    fn invalid_chars() {
        // Whitespace
        let err = "a b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::Invalid);

        // Slash
        let err = "a/b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::Invalid);

        // Asterisk
        let err = "a*b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::Invalid);

        // At sign
        let err = "a@b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::Invalid);

        // Non-ASCII
        let err = "café".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::Invalid);
    }
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use std::fmt;
use std::str::FromStr;

use crate::StringLike;

/// Maximum length of a restricted value in bytes.
pub const MAX_LEN: usize = 36;

/// Error returned when a string does not satisfy restricted value rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestrictedValueError {
    /// The value is empty.
    Empty,
    /// The value contains a character outside `[a-zA-Z0-9_.-]`.
    InvalidChar(char),
    /// The value exceeds [`MAX_LEN`] bytes.
    TooLong { len: usize, max: usize },
}

impl fmt::Display for RestrictedValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "value cannot be empty"),
            Self::InvalidChar(c) => write!(
                f,
                "value contains invalid character '{c}', only [a-zA-Z0-9_.-] are allowed",
            ),
            Self::TooLong { len, max } => {
                write!(f, "value too long: {len} bytes (max {max})")
            }
        }
    }
}

impl std::error::Error for RestrictedValueError {}

/// Returns `true` if the character is allowed in a restricted value.
///
/// Allowed: `[a-zA-Z0-9_.-]`
#[inline]
pub fn is_valid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'
}

/// Validate a string against restricted value rules.
///
/// Returns `Ok(())` if valid, or the first violation found.
pub fn validate(s: &str) -> Result<(), RestrictedValueError> {
    if s.is_empty() {
        return Err(RestrictedValueError::Empty);
    }
    if s.len() > MAX_LEN {
        return Err(RestrictedValueError::TooLong {
            len: s.len(),
            max: MAX_LEN,
        });
    }
    if let Some(c) = s.chars().find(|&c| !is_valid_char(c)) {
        return Err(RestrictedValueError::InvalidChar(c));
    }
    Ok(())
}

/// A validated restricted value string.
///
/// Guarantees that the inner string:
/// - Is non-empty
/// - Is at most [`MAX_LEN`] bytes
/// - Contains only `[a-zA-Z0-9_.-]`
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct RestrictedValue<S>(S);

impl<T> RestrictedValue<T> {
    pub const MAX_LEN: usize = 36;
}

impl<S: StringLike> RestrictedValue<S> {
    /// Create a new restricted value, validating the input.
    pub fn new(s: impl Into<S>) -> Result<Self, RestrictedValueError> {
        let s = s.into();
        validate(s.as_ref())?;
        Ok(Self(s))
    }

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

impl<S: StringLike> std::ops::Deref for RestrictedValue<S> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<S: StringLike> FromStr for RestrictedValue<S> {
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

impl<S: StringLike> PartialEq<&str> for RestrictedValue<S> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
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
        assert_eq!(err, RestrictedValueError::InvalidChar(' '));

        // Slash
        let err = "a/b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::InvalidChar('/'));

        // Asterisk
        let err = "a*b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::InvalidChar('*'));

        // At sign
        let err = "a@b".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::InvalidChar('@'));

        // Non-ASCII
        let err = "café".parse::<RestrictedValue<String>>().unwrap_err();
        assert_eq!(err, RestrictedValueError::InvalidChar('é'));
    }
}

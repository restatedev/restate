// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod format;
mod interned;
mod restricted_value;
mod string;

pub use format::_format;
pub use interned::Interned;
pub use restricted_value::{RestrictedValue, RestrictedValueError};
pub use string::{ReString, ToReString};

use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

/// Trait alias for types that can be used as strings (SharedString, &str, ByteString, etc.)
///
/// This is satisfied by types that:
/// - Can be viewed as `&str` (via `AsRef<str>`)
/// - Are `Eq + Hash + Debug`
pub trait StringLike: Eq + Hash + fmt::Debug + fmt::Display + AsRef<str> + Borrow<str> {
    #[inline]
    fn as_str(&self) -> &str {
        self.as_ref()
    }

    #[inline]
    fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }

    #[inline]
    fn len(&self) -> usize {
        self.as_str().len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.as_str().is_empty()
    }
}

impl<T> StringLike for T where T: Eq + Hash + fmt::Debug + fmt::Display + AsRef<str> + Borrow<str> {}

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

/// A string-shaped type backed by (or convertible to) a [`ReString`], plus an optional
/// validation invariant.
///
/// `RestateString` is the contract that lets generic code construct, validate, and
/// unwrap string types built on top of [`ReString`] — both the plain type itself and
/// validated wrappers like [`RestrictedValue<ReString>`]. It exists so that helpers such
/// as [`Interned<S>`] can compose orthogonally over interning *and* validation, without
/// each implementer reinventing the constructor surface.
///
/// # Implementer contract
///
/// Every implementation defines an *invariant* that all values of `Self` satisfy. For
/// [`ReString`] the invariant is trivial (any UTF-8 string). For
/// [`RestrictedValue<ReString>`] it is "non-empty, ≤ 36 bytes, `[a-zA-Z0-9_.-]` only".
/// All `try_*` constructors must enforce this invariant on input or fail with `Self::Err`.
/// [`from_unchecked`](Self::from_unchecked) is the single escape hatch — see its
/// `# Safety` clause.
///
/// # Method overview
///
/// All `try_*` constructors run `Self`'s validation. They differ only in how the input
/// is supplied — the right one to call is the one that lets the caller hand off
/// allocation ownership to `Self` cheaply:
///
/// | Method | Input | Allocation behavior |
/// |--------|-------|--------------------|
/// | [`try_new`](Self::try_new) | `&str` | Inlines if ≤ `INLINE_CAPACITY`, else allocates a fresh `Arc<str>`. |
/// | [`try_from_static`](Self::try_from_static) | `&'static str` | Zero-copy: keeps the static pointer, no allocation. |
/// | [`try_from_arc`](Self::try_from_arc) | `&Arc<str>` | Refcount bump, no string allocation. |
/// | [`try_from_restring`](Self::try_from_restring) | [`ReString`] | No allocation (consumes the existing representation). |
///
/// Conversions back to [`ReString`]:
/// - [`to_restring`](Self::to_restring): clone (typically a refcount bump or 24-byte memcpy).
/// - [`into_restring`](Self::into_restring): consume.
///
/// # Examples
///
/// Generic helpers parameterised on `RestateString`:
///
/// ```
/// use restate_util_string::{ReString, RestateString, RestrictedValue};
///
/// fn parse_or_default<S: RestateString + Default>(input: Option<&str>) -> S {
///     match input {
///         Some(s) => S::try_new(s).unwrap_or_default(),
///         None => S::default(),
///     }
/// }
///
/// let r: ReString = parse_or_default(Some("hello"));
/// assert_eq!(r.as_str(), "hello");
/// ```
///
/// Composing with [`Interned<S>`] gives interning *and* validation in one type:
///
/// ```
/// use restate_util_string::{Interned, ReString, RestateString, RestrictedValue};
///
/// let v: Interned<RestrictedValue<ReString>> = RestateString::try_new("my_scope.v2").unwrap();
/// assert_eq!(v.as_str(), "my_scope.v2");
/// ```
pub trait RestateString: Sized + Ord + PartialOrd + StringLike {
    /// Error returned by the `try_*` constructors when input fails validation.
    ///
    /// For unconditionally valid types like [`ReString`] this is typically
    /// [`std::str::Utf8Error`] (and the constructors are effectively infallible for
    /// `&str` inputs). For validated wrappers it carries the full structured error
    /// (e.g. [`RestrictedValueError`]).
    type Err: std::fmt::Display;

    /// Wraps `s` in `Self` without running validation.
    ///
    /// # Safety
    ///
    /// Caller must ensure `s` already satisfies whatever invariants the wrapper
    /// `Self` would enforce in [`try_from_restring`](Self::try_from_restring). This is
    /// useful when the value has already been validated upstream (e.g. read from a
    /// trusted store, or pulled from a per-`Self` interner pool whose entries were
    /// validated on insert) and re-running validation would be pure overhead.
    unsafe fn new_unchecked(s: &str) -> Self;

    /// Wraps `s` in `Self` without running validation.
    ///
    /// # Safety
    ///
    /// Caller must ensure `s` already satisfies whatever invariants the wrapper
    /// `Self` would enforce in [`try_from_restring`](Self::try_from_restring). This is
    /// useful when the value has already been validated upstream (e.g. read from a
    /// trusted store, or pulled from a per-`Self` interner pool whose entries were
    /// validated on insert) and re-running validation would be pure overhead.
    unsafe fn from_restring_unchecked(s: ReString) -> Self;

    /// Validates `s` and wraps it in `Self`. No allocation: takes ownership of the
    /// existing [`ReString`] representation (inline or `Arc<str>`).
    fn try_from_restring(s: ReString) -> Result<Self, Self::Err>;

    /// Validates `s` and wraps it in `Self` with a zero-copy `&'static str` backing
    /// (the static pointer is held inside the [`ReString`] inline slot).
    fn try_from_static(s: &'static str) -> Result<Self, Self::Err>;

    /// Validates `s` and constructs a fresh `Self`. Inlines if `s.len() ≤ INLINE_CAPACITY`,
    /// otherwise allocates an `Arc<str>`.
    fn try_new(s: &str) -> Result<Self, Self::Err>;

    /// Validates `s` and wraps it in `Self`, sharing the existing [`Arc<str>`]
    /// allocation (single refcount bump, no string copy).
    fn try_from_arc(s: &Arc<str>) -> Result<Self, Self::Err>;

    /// Returns a [`ReString`] holding the same value. Cheap — typically a refcount
    /// bump or 24-byte memcpy of the inline slot.
    fn to_restring(&self) -> ReString;

    /// Consumes `self` and returns the underlying [`ReString`].
    fn into_restring(self) -> ReString;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn _assert_owned_string_like<T: OwnedStringLike>() {}
    fn _assert_string_like<T: StringLike>() {}

    #[test]
    fn trait_bounds() {
        _assert_owned_string_like::<String>();
        _assert_string_like::<String>();
        _assert_string_like::<&str>();
    }
}

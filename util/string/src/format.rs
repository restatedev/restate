// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Creates a `[crate::ReString]` using interpolation of runtime expressions.
///
/// The first argument `format_restring!` receives is a format string.
/// This must be a string literal.
/// The power of the formatting string is in the `{}`s contained.
///
/// Additional parameters passed to `format_restring!` replace the `{}`s within
/// the formatting string in the order given unless named or
/// positional parameters are used; see [`std::fmt`] for more information.
///
/// A common use for `format_restring!` is concatenation and interpolation
/// of strings.
/// The same convention is used with [`print!`] and [`write!`] macros,
/// depending on the intended destination of the string.
///
/// To convert a single value to a string, use the
/// `ToReString::to_restring` method, which uses
/// the [`std::fmt::Display`] formatting trait.
///
/// # Panics
///
/// `format_restring!` panics if a formatting trait implementation returns
/// an error.
///
/// This indicates an incorrect implementation since
/// `ToReString::to_restring` never returns an error itself.
#[macro_export]
macro_rules! format_restring {
    ($($arg:tt)*) => {
        $crate::ToReString::to_restring(&$crate::_format::format_args!($($arg)*))
    }
}

pub mod _format {
    pub use ::compact_str::core::format_args;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_format_restring() {
        let s = format_restring!("Hello {}!", "world");
        assert_eq!(s, "Hello world!");
        assert!(!s.is_heap_allocated());

        let s = format_restring!("Hello {} and welcome to our universe!", "world");
        assert_eq!(s, "Hello world and welcome to our universe!");
        assert!(s.is_heap_allocated());
    }
}

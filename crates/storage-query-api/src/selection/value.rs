// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::CanonicalEntryId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

/// Borrowed logical literals understood by selection value types.
///
/// These describe values, not SQL operators or physical key encodings. See
/// [`SelectionValue`] for the literals each built-in value type accepts.
#[derive(Debug, Clone, Copy)]
pub enum SelectionLiteral<'a> {
    Null,
    String(&'a str),
    Unsigned(u64),
    Signed(i64),
    Bool(bool),
    /// Unix timestamp with millisecond precision, distinct from an integer literal.
    TimestampMillis(i64),
}

/// Why a literal cannot be converted to a column's value type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralConversionError {
    /// The conversion cannot preserve the comparison's semantics.
    ///
    /// The adapter must evaluate the condition itself. In particular, dropping
    /// an unsupported alternative from an `IN` list could discard valid matches.
    Unsupported,
    /// The literal is understood, but no column value can equal it.
    ///
    /// For example, a negative integer cannot equal a `u64` value. This is
    /// stronger than a parse failure: equality can safely match no records.
    /// An `IN` list may omit such an alternative, but a range with this endpoint
    /// is not necessarily empty: every `u64` is greater than `-1`.
    Unrepresentable,
}

/// Converts comparison literals to a column's logical value type.
///
/// Successful conversion must preserve equality. Implementations must only
/// report `Unrepresentable` when no value can equal the literal; otherwise they
/// must use `Unsupported` so adapters can retain the original predicate.
///
/// NULL is a value here. Adapters decide whether an operator selects it (such as
/// SQL `IS NULL`) or cannot match it (such as SQL equality).
///
/// # Built-in conversions
///
/// | Value type | Accepted non-NULL literals | Ordered conversion |
/// | --- | --- | --- |
/// | [`ReString`], `String` | `String` | Yes |
/// | `u32`, `u64`, `i32`, `i64` | `Unsigned` or `Signed`, if the value fits | Yes |
/// | `bool` | `Bool` | Yes |
/// | [`MillisSinceEpoch`] | Non-negative `TimestampMillis` | Yes |
/// | [`EntryKind`] | `String`: `"invocation"` or `"state-mutation"` | No |
/// | [`CanonicalEntryId`] | `String`, parsed as a canonical entry ID | No |
/// | [`VQueueId`](restate_types::vqueues::VQueueId) | `String`, parsed as a queue ID | No |
/// | `Option<V>` | The literals accepted by `V`, wrapped in `Some` | Delegates to `V` |
/// | `Vec<V>` (binary/list columns) | None; non-NULL comparisons remain residual | No |
///
/// `Null` converts to `None` for `Option<V>` in both methods, and is
/// [`Unrepresentable`](LiteralConversionError::Unrepresentable) for the other
/// types in `from_literal`, as are unparseable strings and out-of-range numbers.
/// Other literal kinds are [`Unsupported`](LiteralConversionError::Unsupported):
/// strings are never parsed as numbers, and integers are never timestamps.
///
/// # Example
///
/// ```
/// use restate_storage_query_api::selection::{LiteralConversionError, SelectionLiteral, SelectionValue};
///
/// assert_eq!(u64::from_literal(SelectionLiteral::Signed(7)), Ok(7));
/// // No u64 equals -1, so `= -1` can match nothing.
/// let negative = u64::from_literal(SelectionLiteral::Signed(-1));
/// assert_eq!(negative, Err(LiteralConversionError::Unrepresentable));
/// // A string is not understood as a u64, so the adapter must evaluate `= '7'`.
/// let string = u64::from_literal(SelectionLiteral::String("7"));
/// assert_eq!(string, Err(LiteralConversionError::Unsupported));
/// assert_eq!(Option::<u64>::from_literal(SelectionLiteral::Null), Ok(None));
/// ```
pub trait SelectionValue: Sized {
    /// Converts an equality or membership operand.
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError>;

    /// Converts a range endpoint while preserving the ordering of non-NULL values.
    ///
    /// Equality conversion alone does not guarantee ordering, so types opt in;
    /// the default returns `Unsupported`. See the
    /// [built-in conversions](SelectionValue#built-in-conversions).
    ///
    /// If `from_literal(Null)` succeeds, that NULL value must sort before every
    /// non-NULL value so adapters can exclude it from an upper-bounded range.
    /// An unrepresentable endpoint does not imply an empty range.
    fn from_ordered_literal(
        _literal: SelectionLiteral<'_>,
    ) -> Result<Self, LiteralConversionError> {
        Err(LiteralConversionError::Unsupported)
    }
}

impl SelectionValue for ReString {
    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::String(value) => Ok(value.into()),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl SelectionValue for String {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::String(value) => Ok(value.into()),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }

    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }
}

// Binary and list values support native clauses and SQL nullability without
// assuming that SQL element comparisons have Rust's lexicographic semantics.
impl<V> SelectionValue for Vec<V> {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl SelectionValue for EntryKind {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::String(value) => value
                .parse()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl SelectionValue for CanonicalEntryId {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::String(value) => value
                .parse()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }

    // ID string order differs from the persisted binary key order. Ordered SQL
    // comparisons must remain residual even though equality and IN are supported.
}

impl SelectionValue for restate_types::vqueues::VQueueId {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::String(value) => value
                .parse()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }

    // Like canonical IDs, ordered string comparisons must remain residual.
}

impl SelectionValue for MillisSinceEpoch {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::TimestampMillis(value) => u64::try_from(value)
                .map(Self::new)
                .map_err(|_| LiteralConversionError::Unrepresentable),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }

    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }
}

impl SelectionValue for u64 {
    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::Unsigned(value) => Ok(value),
            SelectionLiteral::Signed(value) => value
                .try_into()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl SelectionValue for i64 {
    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::Signed(value) => Ok(value),
            SelectionLiteral::Unsigned(value) => value
                .try_into()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl SelectionValue for u32 {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        u64::from_literal(literal)?
            .try_into()
            .map_err(|_| LiteralConversionError::Unrepresentable)
    }

    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }
}

impl SelectionValue for i32 {
    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        i64::from_literal(literal)?
            .try_into()
            .map_err(|_| LiteralConversionError::Unrepresentable)
    }

    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }
}

impl SelectionValue for bool {
    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::Bool(value) => Ok(value),
            SelectionLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl<V: SelectionValue> SelectionValue for Option<V> {
    fn from_ordered_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::Null => Ok(None),
            literal => V::from_ordered_literal(literal).map(Some),
        }
    }

    fn from_literal(literal: SelectionLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            SelectionLiteral::Null => Ok(None),
            literal => V::from_literal(literal).map(Some),
        }
    }
}

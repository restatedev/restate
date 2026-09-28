// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::time::MillisSinceEpoch;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

/// Borrowed logical literals understood by filter value types.
///
/// These describe values, not SQL operators or physical key encodings.
#[derive(Debug, Clone, Copy)]
pub enum FilterLiteral<'a> {
    Null,
    String(&'a str),
    Unsigned(u64),
    Signed(i64),
    Bool(bool),
    /// Unix timestamp with millisecond precision, distinct from an integer literal.
    TimestampMillis(i64),
}

/// Why a literal cannot be converted to a field's value type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralConversionError {
    /// The comparison is not understood and must remain a residual predicate.
    Unsupported,
    /// The literal is understood, but no field value can equal it.
    ///
    /// For example, a negative integer cannot equal a `u64` value. This is
    /// stronger than a parse failure: equality can safely match no records.
    Unrepresentable,
}

/// Converts comparison literals to a field's logical value type.
///
/// Successful conversion must preserve equality. Implementations must only
/// report `Unrepresentable` when no value can equal the literal; otherwise they
/// must use `Unsupported` so adapters can retain the original predicate.
///
/// NULL is a value here. Adapters decide whether an operator selects it (such as
/// SQL `IS NULL`) or cannot match it (such as SQL equality).
pub trait FilterValue: Sized {
    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError>;

    /// Converts a range endpoint while preserving the ordering of non-NULL values.
    ///
    /// Equality conversion alone does not guarantee ordering, so types opt in.
    /// If `from_literal(Null)` succeeds, that NULL value must sort before every
    /// non-NULL value so adapters can exclude it from an upper-bounded range.
    /// An unrepresentable endpoint does not imply an empty range.
    fn from_ordered_literal(_literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Err(LiteralConversionError::Unsupported)
    }
}

impl FilterValue for ReString {
    fn from_ordered_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::String(value) => Ok(value.into()),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl FilterValue for EntryKind {
    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::String(value) => value
                .parse()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl FilterValue for CanonicalEntryId {
    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::String(value) => value
                .parse()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }

    // ID string order differs from the persisted binary key order. Ordered SQL
    // comparisons must remain residual even though equality and IN are supported.
}

impl FilterValue for MillisSinceEpoch {
    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::TimestampMillis(value) => u64::try_from(value)
                .map(Self::new)
                .map_err(|_| LiteralConversionError::Unrepresentable),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }

    fn from_ordered_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }
}

impl FilterValue for u64 {
    fn from_ordered_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::Unsigned(value) => Ok(value),
            FilterLiteral::Signed(value) => value
                .try_into()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl FilterValue for i64 {
    fn from_ordered_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::Signed(value) => Ok(value),
            FilterLiteral::Unsigned(value) => value
                .try_into()
                .map_err(|_| LiteralConversionError::Unrepresentable),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl FilterValue for bool {
    fn from_ordered_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        Self::from_literal(literal)
    }

    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::Bool(value) => Ok(value),
            FilterLiteral::Null => Err(LiteralConversionError::Unrepresentable),
            _ => Err(LiteralConversionError::Unsupported),
        }
    }
}

impl<V: FilterValue> FilterValue for Option<V> {
    fn from_ordered_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::Null => Ok(None),
            literal => V::from_ordered_literal(literal).map(Some),
        }
    }

    fn from_literal(literal: FilterLiteral<'_>) -> Result<Self, LiteralConversionError> {
        match literal {
            FilterLiteral::Null => Ok(None),
            literal => V::from_literal(literal).map(Some),
        }
    }
}

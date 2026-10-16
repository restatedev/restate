// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::ops::RangeBounds;

use strum::VariantArray;

use restate_storage_api::StorageError;
use restate_storage_api::filter::ValuePredicate;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::ServiceName;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::{EntryKind, Seq, VQueueId};
use restate_util_string::ReString;

use crate::keys::predicate::PreparedIndexPredicate;
use crate::keys::{IndexFieldDecode, IndexFieldEncode};

use super::PreparedFieldPredicate;

mod timestamp;

#[cfg(test)]
mod tests;

/// Binds logical literals to a physical field codec without changing its encoding.
///
/// `Value` may differ from the decoded field type (e.g. string bounds for a
/// ServiceName). The default preparation requires byte- and order-compatible
/// encoding. Codecs with different logical and physical domains must override
/// preparation to translate predicates or provide a finite domain. Validation
/// and finite-domain matching happen before scanning.
pub(crate) trait IndexFilterCodec: IndexFieldDecode {
    type Value: IndexFieldEncode + PartialOrd;

    /// Whether the string-prefix matcher needs an Option presence tag.
    /// `None` means the codec does not support this prefix representation.
    const PREFIX_NULLABLE: Option<bool> = None;

    /// All logical/physical value pairs for a small, closed domain.
    /// Predicates over these domains become exact sets of encoded candidates.
    fn finite_domain() -> Option<impl Iterator<Item = (Self::Value, Self::Owned)>> {
        None::<std::iter::Empty<(Self::Value, Self::Owned)>>
    }

    /// Candidates whose logical string names start with `prefix`, excluding NULL.
    fn prefix_values(_prefix: &str) -> Option<impl Iterator<Item = Self::Owned>> {
        None::<std::iter::Empty<Self::Owned>>
    }

    fn validate(_value: &Self::Value) -> crate::Result<()> {
        Ok(())
    }

    fn prepare_value(
        predicate: &ValuePredicate<Self::Value>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        match predicate {
            ValuePredicate::Equal(value) => Self::validate(value)?,
            ValuePredicate::In(values) => {
                for value in values {
                    Self::validate(value)?;
                }
            }
            ValuePredicate::Range { lower, upper } => {
                for bound in [lower, upper] {
                    if let std::ops::Bound::Included(value) | std::ops::Bound::Excluded(value) =
                        bound
                    {
                        Self::validate(value)?;
                    }
                }
            }
        }
        if let Some(domain) = Self::finite_domain() {
            let values = domain.filter_map(|(value, encoded)| {
                let matches = match predicate {
                    ValuePredicate::Equal(expected) => value == *expected,
                    ValuePredicate::In(values) => values.contains(&value),
                    ValuePredicate::Range { lower, upper } => {
                        (lower.as_ref(), upper.as_ref()).contains(&&value)
                    }
                };
                matches.then_some(encoded)
            });
            return Ok(PreparedFieldPredicate::Value(
                PreparedIndexPredicate::from_values(values, literals),
            ));
        }
        Ok(PreparedFieldPredicate::Value(PreparedIndexPredicate::new(
            predicate, literals,
        )))
    }

    fn prepare_prefix(
        prefix: &str,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        if let Some(values) = Self::prefix_values(prefix) {
            return Ok(PreparedFieldPredicate::Value(
                PreparedIndexPredicate::from_values(values, literals),
            ));
        }
        let nullable = Self::PREFIX_NULLABLE.ok_or_else(|| {
            StorageError::Conversion(anyhow::anyhow!(
                "index field codec does not support string prefixes"
            ))
        })?;
        Ok(PreparedFieldPredicate::prefix(prefix, nullable))
    }
}

impl IndexFilterCodec for str {
    type Value = ReString;
    const PREFIX_NULLABLE: Option<bool> = Some(false);
}

impl IndexFilterCodec for ReString {
    type Value = ReString;
    const PREFIX_NULLABLE: Option<bool> = Some(false);
}

impl IndexFilterCodec for ServiceName {
    // Empty strings are valid bounds, even though they cannot be service names.
    type Value = ReString;
    const PREFIX_NULLABLE: Option<bool> = Some(false);
}

impl IndexFilterCodec for u64 {
    type Value = u64;
}

impl IndexFilterCodec for Seq {
    type Value = u64;
}

impl IndexFilterCodec for VQueueId {
    type Value = VQueueId;
}

impl IndexFilterCodec for Reverse<u64> {
    type Value = u64;

    fn prepare_value(
        predicate: &ValuePredicate<u64>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        let reversed = match predicate {
            ValuePredicate::Equal(value) => ValuePredicate::Equal(Reverse(*value)),
            ValuePredicate::In(values) => {
                ValuePredicate::In(values.iter().copied().map(Reverse).collect())
            }
            ValuePredicate::Range { lower, upper } => ValuePredicate::Range {
                lower: upper.as_ref().map(|value| Reverse(*value)),
                upper: lower.as_ref().map(|value| Reverse(*value)),
            },
        };
        Ok(PreparedFieldPredicate::Value(PreparedIndexPredicate::new(
            &reversed, literals,
        )))
    }
}

impl IndexFilterCodec for Stage {
    // SQL literals remain strings, including bounds that do not name a variant.
    type Value = ReString;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Value, Self::Owned)>> {
        Some(
            Self::VARIANTS
                .iter()
                .filter(|&&stage| stage != Self::Unknown)
                .map(|&stage| (ReString::from_static(stage.as_str()), stage)),
        )
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Self::Owned>> {
        Some(
            Self::VARIANTS
                .iter()
                .copied()
                .filter(move |stage| *stage != Self::Unknown && stage.as_str().starts_with(prefix)),
        )
    }
}

impl IndexFilterCodec for Status {
    type Value = ReString;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Value, Self::Owned)>> {
        Some(
            Self::VARIANTS
                .iter()
                .map(|&status| (ReString::from_static(status.as_str()), status)),
        )
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Self::Owned>> {
        Some(
            Self::VARIANTS
                .iter()
                .copied()
                .filter(move |status| status.as_str().starts_with(prefix)),
        )
    }
}

impl IndexFilterCodec for CanonicalEntryId {
    type Value = CanonicalEntryId;

    fn validate(value: &Self::Value) -> crate::Result<()> {
        if value.kind() == EntryKind::Unknown {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(())
    }
}

impl IndexFilterCodec for EntryKind {
    type Value = EntryKind;

    fn validate(value: &Self::Value) -> crate::Result<()> {
        if *value == EntryKind::Unknown {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "unknown entry kind cannot be encoded in an index filter"
            )));
        }
        Ok(())
    }
}

impl<C: IndexFilterCodec> IndexFilterCodec for Option<C> {
    type Value = Option<C::Value>;
    // The current matcher understands one presence tag, not nested Options.
    const PREFIX_NULLABLE: Option<bool> = match C::PREFIX_NULLABLE {
        Some(false) => Some(true),
        _ => None,
    };

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Value, Self::Owned)>> {
        C::finite_domain().map(|domain| {
            std::iter::once((None, None))
                .chain(domain.map(|(value, encoded)| (Some(value), Some(encoded))))
        })
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Self::Owned>> {
        C::prefix_values(prefix).map(|values| values.map(Some))
    }

    fn validate(value: &Self::Value) -> crate::Result<()> {
        if let Some(value) = value {
            C::validate(value)?;
        }
        Ok(())
    }
}

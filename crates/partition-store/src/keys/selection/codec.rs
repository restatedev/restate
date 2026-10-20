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
use restate_storage_api::selection::ValuePredicate;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::{EntryKind, VQueueId};
use restate_util_string::ReString;

use crate::keys::codecs::*;
use crate::keys::predicate::EncodedValuePredicate;
use crate::keys::{FieldCodec, OrderedFieldCodec};

use super::PreparedFieldPredicate;

mod timestamp;

#[cfg(test)]
mod tests;

/// Binds logical literals to a physical field codec without changing its encoding.
///
/// `Literal` may differ from the decoded field type (e.g. string bounds for a
/// service name). Without a finite domain, default preparation uses the selected
/// codec's `encode_literal`, whose bytes and order must match the physical field.
/// Codecs with different domains must override preparation or enumerate their
/// logical/physical pairs through [`Self::finite_domain`].
///
/// For a finite domain, logical comparisons select matching values before their
/// physical encodings are sorted into an IN predicate. A string bound therefore
/// need not name an enum variant. Custom translations also handle descending
/// fields and millisecond predicates over timestamp encodings.
///
/// Preparation may append to the shared literal buffer, but must preserve bytes
/// belonging to earlier predicates. Literal offsets in returned predicates refer
/// to this same buffer.
pub(crate) trait FieldPredicateCodec: OrderedFieldCodec + Sized {
    /// The logical literal type expected by this physical field's predicates.
    type Literal: PartialOrd;

    /// Encodes a logical endpoint in this codec's physical representation.
    /// Codecs needing whole-predicate translation leave this unsupported. In
    /// particular, nullable wrappers must not bypass custom time/descending logic.
    fn encode_literal(_value: &Self::Literal, _target: &mut Vec<u8>) -> crate::Result<()> {
        Err(StorageError::Conversion(anyhow::anyhow!(
            "index field codec requires whole-predicate translation"
        )))
    }

    /// Whether the encoded-string prefix matcher needs one `Option` presence tag.
    /// `Some(false)` selects a bare string; `Some(true)` selects a nullable string.
    /// `None` disables this representation, but [`Self::prefix_values`] can still
    /// supply exact candidates for a finite domain.
    const PREFIX_NULLABLE: Option<bool> = None;

    /// All logical/physical value pairs for a small, closed domain.
    /// Predicates over these domains become exact sets of encoded candidates.
    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        None::<std::iter::Empty<(Self::Literal, Self::Value)>>
    }

    /// Candidates whose logical string names start with `prefix`, excluding NULL.
    /// `Some(empty_iterator)` means no matches; `None` falls back to the encoded
    /// string matcher selected by [`Self::PREFIX_NULLABLE`].
    fn prefix_values(_prefix: &str) -> Option<impl Iterator<Item = Self::Value>> {
        None::<std::iter::Empty<Self::Value>>
    }

    /// Checks literals before default preparation, including both bounded range
    /// endpoints. This need not require that a literal is a storable field value.
    fn validate(_value: &Self::Literal) -> crate::Result<()> {
        Ok(())
    }

    /// Prepares equality, membership, or range matching in physical byte order.
    fn prepare_value(
        predicate: &ValuePredicate<Self::Literal>,
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
                EncodedValuePredicate::from_values::<Self>(values, literals),
            ));
        }
        Ok(PreparedFieldPredicate::Value(
            EncodedValuePredicate::try_new(predicate, literals, Self::encode_literal)?,
        ))
    }

    /// Prepares a case-sensitive prefix, excluding NULL even for an empty prefix.
    /// Returns an error when neither finite candidates nor string encoding apply.
    fn prepare_prefix(
        prefix: &str,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        if let Some(values) = Self::prefix_values(prefix) {
            return Ok(PreparedFieldPredicate::Value(
                EncodedValuePredicate::from_values::<Self>(values, literals),
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

impl FieldPredicateCodec for StringCodec {
    type Literal = ReString;
    const PREFIX_NULLABLE: Option<bool> = Some(false);

    fn encode_literal(value: &ReString, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for ServiceNameCodec {
    // Empty strings are valid bounds, even though they cannot be service names.
    type Literal = ReString;
    const PREFIX_NULLABLE: Option<bool> = Some(false);

    fn encode_literal(value: &ReString, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_ref(value.as_str(), target);
        Ok(())
    }
}

impl FieldPredicateCodec for U64Codec {
    type Literal = u64;

    fn encode_literal(value: &u64, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for SeqCodec {
    type Literal = u64;

    fn encode_literal(value: &u64, target: &mut Vec<u8>) -> crate::Result<()> {
        U64Codec::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for VQueueIdCodec {
    type Literal = VQueueId;

    fn encode_literal(value: &VQueueId, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for ReverseU64Codec {
    type Literal = u64;

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
        Ok(PreparedFieldPredicate::Value(EncodedValuePredicate::new::<
            Self,
        >(&reversed, literals)))
    }
}

impl FieldPredicateCodec for StageRankCodec {
    // SQL literals remain strings, including bounds that do not name a variant.
    type Literal = ReString;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        Some(
            Stage::VARIANTS
                .iter()
                .filter(|&&stage| stage != Stage::Unknown)
                .map(|&stage| (ReString::from_static(stage.as_str()), stage)),
        )
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Self::Value>> {
        Some(
            Stage::VARIANTS.iter().copied().filter(move |stage| {
                *stage != Stage::Unknown && stage.as_str().starts_with(prefix)
            }),
        )
    }
}

impl FieldPredicateCodec for StatusCodec {
    type Literal = ReString;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        Some(
            Status::VARIANTS
                .iter()
                .map(|&status| (ReString::from_static(status.as_str()), status)),
        )
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Self::Value>> {
        Some(
            Status::VARIANTS
                .iter()
                .copied()
                .filter(move |status| status.as_str().starts_with(prefix)),
        )
    }
}

impl FieldPredicateCodec for CanonicalEntryIdCodec {
    type Literal = CanonicalEntryId;

    fn validate(value: &Self::Literal) -> crate::Result<()> {
        if value.kind() == EntryKind::Unknown {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(())
    }

    fn encode_literal(value: &CanonicalEntryId, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for EntryKindCodec {
    type Literal = EntryKind;

    fn validate(value: &Self::Literal) -> crate::Result<()> {
        if *value == EntryKind::Unknown {
            return Err(StorageError::Conversion(anyhow::anyhow!(
                "unknown entry kind cannot be encoded in a field predicate"
            )));
        }
        Ok(())
    }

    fn encode_literal(value: &EntryKind, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

// Lift the default codec hooks to nullable fields. This does not delegate to
// C::prepare_value: custom translations such as timestamps or Reverse<u64> need
// their own nullable preparation; encoding their non-null endpoints returns an
// error rather than silently producing bounds in the wrong representation.
impl<C: FieldPredicateCodec> FieldPredicateCodec for Option<C> {
    type Literal = Option<C::Literal>;
    // The current matcher understands one presence tag, not nested Options.
    const PREFIX_NULLABLE: Option<bool> = match C::PREFIX_NULLABLE {
        Some(false) => Some(true),
        _ => None,
    };

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        C::finite_domain().map(|domain| {
            std::iter::once((None, None))
                .chain(domain.map(|(value, encoded)| (Some(value), Some(encoded))))
        })
    }

    fn prefix_values(prefix: &str) -> Option<impl Iterator<Item = Self::Value>> {
        C::prefix_values(prefix).map(|values| values.map(Some))
    }

    fn validate(value: &Self::Literal) -> crate::Result<()> {
        if let Some(value) = value {
            C::validate(value)?;
        }
        Ok(())
    }

    fn encode_literal(value: &Self::Literal, target: &mut Vec<u8>) -> crate::Result<()> {
        match value {
            None => target.push(0),
            Some(value) => {
                target.push(1);
                C::encode_literal(value, target)?;
            }
        }
        Ok(())
    }
}

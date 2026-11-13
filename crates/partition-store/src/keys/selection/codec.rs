// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Field predicate codecs: how a logical literal becomes bytes of one key field.
//!
//! A selection clause carries a logical value: a string, a timestamp in
//! milliseconds, an entry kind. To compare it with an encoded field, the field's
//! codec translates it into the field's own encoding and order
//! ([`FieldPredicateCodec`]). A plain codec encodes the literal as is; a codec
//! with a small closed domain (stages, statuses) picks the matching values and
//! lists them; the entry-kind codec validates the literal and encodes it directly,
//! so an unknown kind is an error rather than an empty set; a descending or
//! coarser timestamp codec rewrites the bounds. The result is a
//! [`PreparedFieldPredicate`] over encoded bytes.

use std::cmp::Reverse;
use std::ops::{Bound, RangeBounds};

use strum::VariantArray;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{Stage, Status};
use restate_storage_query_api::selection::ValuePredicate;
use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, InvocationId};
use restate_types::vqueues::{EntryKind, VQueueId};
use restate_util_string::ReString;

use crate::keys::codecs::*;
use crate::keys::predicate::EncodedValuePredicate;
use crate::keys::{FieldCodec, OrderedFieldCodec};

use super::PreparedFieldPredicate;

mod timestamp;

#[cfg(test)]
mod tests;

/// How a logical literal becomes bytes of one key field, so that a clause on a
/// column can be checked against the field without decoding it.
///
/// A codec translates a predicate in one of three ways:
///
/// - **Encode the literal.** The field's bytes sort like its values, so
///   `service_name = 'a'` becomes the equality on `encode('a')` and
///   `sequence >= 7` the range from `encode(7)`. This is the default
///   ([`Self::encode_literal`]).
/// - **List the matching values.** A small closed domain (stages, statuses,
///   booleans) enumerates its values with [`Self::finite_domain`]; the predicate
///   selects the ones that satisfy it and the result is an IN list of their
///   encodings. `stage >= running` thus becomes `IN (running, suspended, …)`,
///   and the logical order need not be the byte order.
/// - **Rewrite the predicate.** A codec whose byte order is not the literal's
///   order overrides [`Self::prepare_value`]: a descending field swaps and
///   inverts the bounds; a millisecond predicate on a timestamp that carries a
///   counter becomes the inclusive interval of encodings covering that
///   millisecond. `Option<C>` lifts the inner codec's translation.
///
/// `Literal` is the logical type of the clause, which may differ from the field's
/// decoded type (a service name is compared as a string). Preparation appends to
/// the selection's shared literal buffer and must leave the bytes of earlier
/// predicates in place; the returned predicate refers to the buffer by offsets.
pub(crate) trait FieldPredicateCodec: OrderedFieldCodec + Sized {
    /// The logical literal type expected by this physical field's predicates.
    type Literal: PartialOrd;

    /// `true` when [`Self::encode_literal`] is this codec's translation, so that
    /// `Option<Self>` may encode a non-NULL endpoint of a nullable predicate on
    /// its own. A codec that rewrites whole predicates leaves it `false`; the
    /// nullable wrapper then declines such predicates instead of encoding them
    /// wrongly.
    const LITERAL_ENCODING: bool = false;

    /// Encodes one literal as the field stores it. The default is an error, for
    /// codecs that rewrite whole predicates instead; it must stay so for them, or
    /// `Option<Self>` would bypass their rewriting.
    fn encode_literal(_value: &Self::Literal, _target: &mut Vec<u8>) -> crate::Result<()> {
        Err(StorageError::Conversion(anyhow::anyhow!(
            "index field codec requires whole-predicate translation"
        )))
    }

    /// How a `starts_with` clause is matched on this field: `Some(false)` on the
    /// plain string encoding, `Some(true)` on a nullable string, whose encoding
    /// carries a presence byte in front. `None` means prefixes cannot be matched
    /// on the bytes; [`Self::prefix_values`] can still list the candidates of a
    /// finite domain.
    const PREFIX_NULLABLE: Option<bool> = None;

    /// Every value of a small closed domain, with its encoding. Predicates on such
    /// a field become the exact list of encodings that satisfy them.
    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        None::<std::iter::Empty<(Self::Literal, Self::Value)>>
    }

    /// The encodings of the domain values whose names start with `prefix`, NULL
    /// excluded. `Some(empty)` means nothing matches; `None` means use the byte
    /// matcher chosen by [`Self::PREFIX_NULLABLE`].
    fn prefix_values(_prefix: &str) -> Option<impl Iterator<Item = Self::Value>> {
        None::<std::iter::Empty<Self::Value>>
    }

    /// Checks a literal before it is encoded, both endpoints of a range included.
    /// A literal may be valid without being a value the field could store, as a
    /// range endpoint often is.
    fn validate(_value: &Self::Literal) -> crate::Result<()> {
        Ok(())
    }

    /// Prepares `predicate` if this codec can. `None` declines the operation, so
    /// the caller keeps it as a residual; that is different from an error
    /// converting a literal, which propagates. A returned predicate must match
    /// exactly the stored values the logical predicate matches.
    fn try_prepare_value(
        predicate: &ValuePredicate<Self::Literal>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<Option<PreparedFieldPredicate>> {
        Self::prepare_value(predicate, literals).map(Some)
    }

    /// Translates equality, IN or a range into conditions on the field's bytes:
    /// with a finite domain by listing the matching values, otherwise by encoding
    /// each literal.
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
    #[cfg(test)]
    fn prepare_prefix(
        prefix: &str,
        literals: &mut Vec<u8>,
    ) -> crate::Result<PreparedFieldPredicate> {
        Self::try_prepare_prefix(prefix, literals)?.ok_or_else(|| {
            StorageError::Conversion(anyhow::anyhow!(
                "index field codec does not support string prefixes"
            ))
        })
    }

    /// `None` is unsupported acceleration, distinct from a preparation error.
    fn try_prepare_prefix(
        prefix: &str,
        literals: &mut Vec<u8>,
    ) -> crate::Result<Option<PreparedFieldPredicate>> {
        if let Some(values) = Self::prefix_values(prefix) {
            return Ok(Some(PreparedFieldPredicate::Value(
                EncodedValuePredicate::from_values::<Self>(values, literals),
            )));
        }
        Ok(Self::PREFIX_NULLABLE.map(|nullable| PreparedFieldPredicate::prefix(prefix, nullable)))
    }
}

/// Prepares a predicate on a nullable column for a field that never stores NULL:
/// `IS NULL` becomes an empty IN list, NULL drops out of IN lists, and a range
/// that ends at NULL is empty. Everything else goes to `C` unchanged, so `C`'s own
/// translation applies.
pub(crate) fn prepare_non_null<C: FieldPredicateCodec>(
    predicate: &ValuePredicate<Option<C::Literal>>,
    literals: &mut Vec<u8>,
) -> crate::Result<Option<PreparedFieldPredicate>>
where
    C::Literal: Clone,
{
    let predicate = match predicate {
        ValuePredicate::Equal(Some(value)) => ValuePredicate::Equal(value.clone()),
        ValuePredicate::Equal(None) => ValuePredicate::In(Vec::new()),
        ValuePredicate::In(values) => {
            ValuePredicate::In(values.iter().flatten().cloned().collect())
        }
        ValuePredicate::Range {
            upper: Bound::Included(None) | Bound::Excluded(None),
            ..
        } => ValuePredicate::In(Vec::new()),
        ValuePredicate::Range { lower, upper } => {
            let non_null_bound = |bound: &Bound<Option<C::Literal>>| match bound {
                Bound::Included(Some(value)) => Bound::Included(value.clone()),
                Bound::Excluded(Some(value)) => Bound::Excluded(value.clone()),
                _ => Bound::Unbounded,
            };
            ValuePredicate::Range {
                lower: non_null_bound(lower),
                upper: non_null_bound(upper),
            }
        }
    };
    C::try_prepare_value(&predicate, literals)
}

impl FieldPredicateCodec for StringCodec {
    type Literal = ReString;
    const LITERAL_ENCODING: bool = true;
    const PREFIX_NULLABLE: Option<bool> = Some(false);

    fn encode_literal(value: &ReString, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for ServiceNameCodec {
    // Empty strings are valid bounds, even though they cannot be service names.
    type Literal = ReString;
    const LITERAL_ENCODING: bool = true;
    const PREFIX_NULLABLE: Option<bool> = Some(false);

    fn encode_literal(value: &ReString, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_ref(value.as_str(), target);
        Ok(())
    }
}

impl FieldPredicateCodec for U64Codec {
    type Literal = u64;
    const LITERAL_ENCODING: bool = true;

    fn encode_literal(value: &u64, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

/// Translates equality and membership through `C`; ranges remain residual. For
/// codecs whose byte order differs from the logical order of `L`. If `encode`
/// cannot name a literal's stored representation, the predicate stays residual.
pub(crate) fn prepare_members<'v, C: FieldCodec, L>(
    predicate: &'v ValuePredicate<L>,
    literals: &mut Vec<u8>,
    encode: impl Fn(&'v L) -> Option<C::Ref<'v>>,
) -> Option<PreparedFieldPredicate> {
    let members = match predicate {
        ValuePredicate::Equal(value) => std::slice::from_ref(value),
        ValuePredicate::In(values) => values.as_slice(),
        ValuePredicate::Range { .. } => return None,
    };
    EncodedValuePredicate::try_from_members::<C, L>(members, literals, encode)
        .map(PreparedFieldPredicate::Value)
}

/// A vqueue ID selects its suffix; its partition key belongs to the key header,
/// which the caller restricts separately. ID order is not suffix order, so only
/// equality and membership are accelerated; ranges remain residual.
impl FieldPredicateCodec for VQueueIdSuffixCodec {
    type Literal = VQueueId;

    fn try_prepare_value(
        predicate: &ValuePredicate<VQueueId>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<Option<PreparedFieldPredicate>> {
        // SQL never produces ID ranges: text order differs from byte order, so
        // `VQueueId` has no ordered literal conversion.
        Ok(prepare_members::<Self, _>(predicate, literals, |id| {
            Some(Self::suffix(id))
        }))
    }
}

impl FieldPredicateCodec for EntryIdCodec {
    type Literal = BaseEntryId;

    fn try_prepare_value(
        predicate: &ValuePredicate<BaseEntryId>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<Option<PreparedFieldPredicate>> {
        // SQL never produces entry-ID ranges: text order differs from byte order.
        Ok(prepare_members::<Self, _>(predicate, literals, |id| {
            Some(id.as_entry_id())
        }))
    }
}

impl FieldPredicateCodec for U32Codec {
    type Literal = u32;
    const LITERAL_ENCODING: bool = true;

    fn encode_literal(value: &u32, target: &mut Vec<u8>) -> crate::Result<()> {
        Self::encode_field(value, target);
        Ok(())
    }
}

/// An invocation ID selects its UUID; its partition key belongs to the key header,
/// which the caller restricts separately. ID order is not UUID order, so only
/// equality and membership are accelerated; ranges remain residual.
impl FieldPredicateCodec for InvocationUuidCodec {
    type Literal = InvocationId;

    fn try_prepare_value(
        predicate: &ValuePredicate<InvocationId>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<Option<PreparedFieldPredicate>> {
        let ids = match predicate {
            ValuePredicate::Equal(id) => std::slice::from_ref(id),
            ValuePredicate::In(ids) => ids.as_slice(),
            ValuePredicate::Range { .. } => return Ok(None),
        };
        Ok(Some(PreparedFieldPredicate::Value(
            EncodedValuePredicate::from_values::<Self>(
                ids.iter().map(InvocationId::invocation_uuid),
                literals,
            ),
        )))
    }
}

impl FieldPredicateCodec for SeqCodec {
    type Literal = u64;
    const LITERAL_ENCODING: bool = true;

    fn encode_literal(value: &u64, target: &mut Vec<u8>) -> crate::Result<()> {
        U64Codec::encode_field(value, target);
        Ok(())
    }
}

impl FieldPredicateCodec for VQueueIdCodec {
    type Literal = VQueueId;
    const LITERAL_ENCODING: bool = true;

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

impl FieldPredicateCodec for ReverseBoolCodec {
    // Both values enumerate, so equality, IN, and ranges translate exactly
    // whatever the descending byte order.
    type Literal = bool;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        Some([(true, true), (false, false)].into_iter())
    }
}

impl FieldPredicateCodec for StageRankCodec {
    // SQL-name predicates have already become native sets. Native callers may
    // also supply ranges; enum discriminants do not share the stored rank order.
    type Literal = Stage;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        Some(
            Stage::VARIANTS
                .iter()
                .filter(|&&stage| stage != Stage::Unknown)
                .map(|&stage| (stage, stage)),
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
    type Literal = Status;

    fn finite_domain() -> Option<impl Iterator<Item = (Self::Literal, Self::Value)>> {
        Some(Status::VARIANTS.iter().map(|&status| (status, status)))
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
    const LITERAL_ENCODING: bool = true;

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
    const LITERAL_ENCODING: bool = true;

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
    const LITERAL_ENCODING: bool = C::LITERAL_ENCODING;

    fn try_prepare_value(
        predicate: &ValuePredicate<Self::Literal>,
        literals: &mut Vec<u8>,
    ) -> crate::Result<Option<PreparedFieldPredicate>> {
        if !C::LITERAL_ENCODING && C::finite_domain().is_none() {
            return Ok(None);
        }
        Self::prepare_value(predicate, literals).map(Some)
    }
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

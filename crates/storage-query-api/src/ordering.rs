// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ordering functions: named functions of an ID whose values order a table's
//! records where the ID's text doesn't.
//!
//! An ID's text doesn't sort like the ID. It encodes each number with its bytes
//! reversed, in a base62 alphabet that orders lowercase before uppercase. So a
//! table whose reads come out in ID order can't say it is ordered by its ID
//! column. It names an ordering function of the ID instead, whose value has a
//! byte form that sorts like the value:
//!
//! ```text
//! id (partition key 7)           invocation_id(id)       base_entry_id(id)
//! inv_10BgaEsBHSfe01SZwviYz…     00…07 | 00…00 01        00…07 | kind | 00…00 01   uuid 1
//! inv_10BgaEsBHSfe00…            00…07 | 00…01 00        00…07 | kind | 00…01 00   uuid 256
//! ```
//!
//! The values put uuid 1 first; the text puts uuid 256 first. The value types and
//! their byte forms are `restate_types`'; which function orders a table's records
//! is that table's claim ([`Table::NATIVE_ORDERS`](crate::Table::NATIVE_ORDERS)).
//! Two tables ordered by the same function come out in comparable order, and the
//! same entity has the same value in both: an invocation and its vqueue entry have
//! equal `base_entry_id`.
//!
//! # Native orders
//!
//! A table's reads in key order sort their rows by each of its native orders
//! ([`NativeOrder`]): a sequence of terms, each a column or a function of the ID
//! ([`OrderTerm`]), compared one after another. Journal entries, keyed by
//! `partition key | uuid | index`, come out in two:
//!
//! ```text
//! [partition_key]                 every row's partition key, ascending
//! [invocation_id(id), index]      by invocation, then each invocation's entries by index
//! ```
//!
//! A term that the next one starts with gets its own order rather than leading
//! the next one's: `invocation_id(id)` begins with the partition key, but a plan
//! matches a required order against the start of a declared one, so
//! `[partition_key, invocation_id(id)]` wouldn't serve a JOIN on IDs.

use restate_types::errors::IdDecodeError;
use restate_types::identifiers::{
    BaseEntryId, CanonicalEntryId, CanonicalIdParseError, InvocationId, ResourceId,
};
use restate_types::vqueues::VQueueId;

use crate::schema::sharding::ShardKeyKind;

/// A named function of an ID whose value orders records. See the
/// [module](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::VariantArray)]
#[allow(
    clippy::enum_variant_names,
    reason = "each is named after the ID it yields"
)]
pub enum OrderFunction {
    /// The invocation ID: partition key, then uuid.
    InvocationId,
    /// The base vqueue entry ID: partition key, entry kind, then the resource's
    /// remainder. An invocation's is its entry's.
    BaseEntryId,
    /// The canonical vqueue entry ID: partition key, the sequence number of the
    /// record that created the incarnation, entry kind, then the resource's
    /// remainder.
    CanonicalEntryId,
    /// The vqueue ID: partition key, then the queue's digest.
    VQueueId,
}

impl OrderFunction {
    /// The function's SQL-style name, as plans show it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::InvocationId => "invocation_id",
            Self::BaseEntryId => "base_entry_id",
            Self::CanonicalEntryId => "canonical_entry_id",
            Self::VQueueId => "vqueue_id",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        <Self as strum::VariantArray>::VARIANTS
            .iter()
            .copied()
            .find(|function| function.name() == name)
    }

    /// The length of every value's byte form.
    pub const fn width(self) -> usize {
        match self {
            Self::InvocationId => size_of::<restate_types::identifiers::EncodedInvocationId>(),
            Self::BaseEntryId => BaseEntryId::RAW_BYTES_LEN,
            Self::CanonicalEntryId => CanonicalEntryId::RAW_BYTES_LEN,
            Self::VQueueId => <VQueueId as ResourceId>::RAW_BYTES_LEN,
        }
    }

    /// Whether the function applies to the IDs of a sharding column with this
    /// derivation.
    pub const fn applies_to(self, kind: ShardKeyKind) -> bool {
        matches!(
            (self, kind),
            (Self::InvocationId, ShardKeyKind::InvocationId)
                | (
                    Self::BaseEntryId,
                    ShardKeyKind::InvocationId | ShardKeyKind::BaseEntryId
                )
                | (Self::CanonicalEntryId, ShardKeyKind::CanonicalEntryId)
                | (Self::VQueueId, ShardKeyKind::VQueueId)
        )
    }

    /// Calls `with` on the byte form of the function's value for the ID `text`;
    /// byte order is value order.
    pub fn encode<R>(self, text: &str, with: impl FnOnce(&[u8]) -> R) -> Result<R, NotAnId> {
        Ok(match self {
            Self::InvocationId => with(&text.parse::<InvocationId>()?.to_bytes()),
            Self::BaseEntryId => with(text.parse::<BaseEntryId>()?.as_bytes()),
            Self::CanonicalEntryId => with(text.parse::<CanonicalEntryId>()?.as_bytes()),
            Self::VQueueId => {
                let mut bytes = [0; <VQueueId as ResourceId>::RAW_BYTES_LEN];
                text.parse::<VQueueId>()?
                    .encode_raw_bytes(&mut bytes.as_mut_slice());
                with(&bytes)
            }
        })
    }
}

/// A term of a [`NativeOrder`]: what rows sort by once the terms before it tie.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderTerm {
    /// A column, by its SQL name, whose values the key holds in a form that sorts
    /// like them: a partition key or a journal index as a big-endian unsigned
    /// integer, a name as a mem-comparable string, with an absent value (NULL)
    /// first.
    Column(&'static str),
    /// A function of the record's ID, whose column the table's binding names.
    Function(OrderFunction),
}

/// One order of a table's key-ordered reads: its rows sort by the first term,
/// then by the next where that one ties, and so on. See the [module](self).
pub type NativeOrder = &'static [OrderTerm];

/// Text that isn't an ID a function takes.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct NotAnId(#[from] CanonicalIdParseError);

impl From<IdDecodeError> for NotAnId {
    fn from(error: IdDecodeError) -> Self {
        Self(error.into())
    }
}

#[cfg(test)]
mod tests {
    use restate_types::identifiers::InvocationUuid;
    use restate_types::vqueues::{EntryId, EntryKind, Seq};

    use super::*;

    /// Values sort where text doesn't, have their declared width, and an
    /// invocation's `base_entry_id` is its entry's.
    #[test]
    fn values_sort_like_ids_where_text_does_not() {
        let ids = [(7, 1), (7, 256), (8, 0)]
            .map(|(key, uuid)| InvocationId::from_parts(key, InvocationUuid::from_u128(uuid)));
        let texts = ids.map(|id| id.to_string());
        assert!(!texts.is_sorted());
        for function in [OrderFunction::InvocationId, OrderFunction::BaseEntryId] {
            let values: Vec<Vec<u8>> = texts
                .iter()
                .map(|text| function.encode(text, <[u8]>::to_vec).unwrap())
                .collect();
            assert!(values.is_sorted(), "{function:?}");
            assert!(values.iter().all(|value| value.len() == function.width()));
            assert_eq!(OrderFunction::from_name(function.name()), Some(function));
        }
        assert_eq!(
            OrderFunction::BaseEntryId
                .encode(&texts[0], <[u8]>::to_vec)
                .unwrap(),
            BaseEntryId::from(ids[0]).as_bytes()
        );
        // A state mutation has a base entry ID but no invocation ID.
        let mutation = BaseEntryId::new(7, EntryId::new(EntryKind::StateMutation, [0; 16]));
        assert!(
            OrderFunction::BaseEntryId
                .encode(&mutation.to_string(), |_| ())
                .is_ok()
        );
        assert!(
            OrderFunction::InvocationId
                .encode(&mutation.to_string(), |_| ())
                .is_err()
        );
        // Canonical IDs sort by partition key, then sequence number, where their
        // text doesn't; a base ID has none.
        let canonical = [(7, 1), (7, 256), (8, 0)].map(|(key, seq)| {
            BaseEntryId::from(ids[0])
                .to_entry_id()
                .to_base_id(key)
                .canonicalize(Seq::new(seq))
        });
        let values: Vec<Vec<u8>> = canonical
            .iter()
            .map(|id| {
                OrderFunction::CanonicalEntryId
                    .encode(&id.to_string(), <[u8]>::to_vec)
                    .unwrap()
            })
            .collect();
        assert!(values.is_sorted());
        assert!(
            values
                .iter()
                .all(|value| value.len() == OrderFunction::CanonicalEntryId.width())
        );
        assert_eq!(values[0], canonical[0].as_bytes());
        assert!(
            OrderFunction::CanonicalEntryId
                .encode(&mutation.to_string(), |_| ())
                .is_err()
        );
        assert!(OrderFunction::CanonicalEntryId.applies_to(ShardKeyKind::CanonicalEntryId));
        assert!(!OrderFunction::CanonicalEntryId.applies_to(ShardKeyKind::BaseEntryId));
        // Queue IDs sort by partition key, then digest, where their text doesn't.
        let queues = [1, 256, 257].map(|key| VQueueId::custom(key, "q"));
        let texts = queues.each_ref().map(ToString::to_string);
        assert!(!texts.is_sorted());
        let values = texts.each_ref().map(|text| {
            OrderFunction::VQueueId
                .encode(text, <[u8]>::to_vec)
                .unwrap()
        });
        assert!(values.is_sorted());
        let mut raw = Vec::new();
        queues[0].encode_raw_bytes(&mut raw);
        assert_eq!(values[0], raw);
        assert_eq!(raw.len(), OrderFunction::VQueueId.width());
        assert!(OrderFunction::VQueueId.applies_to(ShardKeyKind::VQueueId));
    }
}

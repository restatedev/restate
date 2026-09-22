// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::StorageError;
use restate_storage_api::filter::ValuePredicate;
use restate_storage_api::vqueue_table::Stage;
use restate_types::ServiceName;
use restate_types::identifiers::InvocationId;
use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

use crate::keys::predicate::PreparedIndexPredicate;
use crate::keys::{IndexFieldDecode, IndexFieldEncode};

use super::PreparedFieldPredicate;

mod timestamp;

/// Binds logical literals to a physical field codec without changing its encoding.
///
/// `Value` may differ from the decoded field type (e.g. string bounds for a
/// ServiceName). The default preparation requires byte- and order-compatible
/// encoding. Codecs with different logical and physical domains must override
/// preparation to translate predicates. Validation happens before scanning.
pub(crate) trait IndexFilterCodec: IndexFieldDecode {
    type Value: IndexFieldEncode;

    /// Whether the string-prefix matcher needs an Option presence tag.
    /// `None` means the codec does not support this prefix representation.
    const PREFIX_NULLABLE: Option<bool> = None;

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
        Ok(PreparedFieldPredicate::Value(PreparedIndexPredicate::new(
            predicate, literals,
        )))
    }

    fn prepare_prefix(prefix: &str) -> crate::Result<PreparedFieldPredicate> {
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

impl IndexFilterCodec for Stage {
    // Stage keys use the mem-comparable encoding of their names. Arbitrary
    // strings are valid bounds even if they do not name a stage.
    type Value = ReString;
    const PREFIX_NULLABLE: Option<bool> = Some(false);
}

impl IndexFilterCodec for InvocationId {
    type Value = InvocationId;
}

impl IndexFilterCodec for EntryKind {
    type Value = EntryKind;
    const PREFIX_NULLABLE: Option<bool> = Some(false);

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

    fn validate(value: &Self::Value) -> crate::Result<()> {
        if let Some(value) = value {
            C::validate(value)?;
        }
        Ok(())
    }
}

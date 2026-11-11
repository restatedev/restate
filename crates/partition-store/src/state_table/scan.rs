// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical reads over existing state encodings. The partition-key header bounds
//! a read, and the legacy suffix fields (optional scope, then length-delimited
//! service name, service key, and state key) are declared as ordered fields: an
//! equality or IN restriction positions the iterator at matching objects instead
//! of reading every object that shares the partition key (e.g. all objects of one
//! scope). Their codec order is not string order, so ranges and prefixes remain
//! residual.

use restate_storage_query_api::Table;

use crate::Result;
use crate::keys::codecs::{DelimitedBytesCodec, OptionalScopeCodec};
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::{KeySelection, OrderedKeyField, OrderedKeySchema};
use crate::keys::{DecodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyDecoder, KeyKind};
use crate::table::PhysicalTable;

use super::{ScopedStateKey, StateKey};

macro_rules! state_table {
    ($table:ident, $key:ty, $active:expr, fields [$($tag:literal: $codec:ty),+ $(,)?]) => {
        /// Physical state records using the existing key and value encodings.
        pub enum $table {}

        impl Table for $table {
            type Record<'a> = (KeyDecoder<'a, $key>, &'a [u8]);
            type Scan = KeySelection<$key>;
        }

        impl PhysicalTable for $table {
            type Key = $key;

            fn is_active(metadata: &crate::table::ReadMetadata<'_>) -> bool {
                ($active)(metadata)
            }
        }

        impl KeySchema for $key {
            type Header = PartitionKeyHeader<Self>;

            fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
                // Existing owned state keys include the partition key and their
                // decoder validates the key kind. Retain the borrowed full key.
                Ok(KeyDecoder::from_payload(key))
            }
        }

        impl KeyDecode for $key {
            fn decode(source: &mut &[u8]) -> Result<Self> {
                Self::deserialize_from(source)
            }
        }

        impl OrderedKeySchema for $key {
            const KEY_KIND: KeyKind = <$key as EncodeTableKeyPrefix>::KEY_KIND;
            const FIELDS: &'static [OrderedKeyField] =
                &[$(OrderedKeyField::new::<$codec>($tag)),+];
        }
    };
}

state_table!(
    StateTable,
    StateKey,
    |metadata: &crate::table::ReadMetadata<'_>| !metadata
        .features
        .is_migrated_to_scoped_state_table,
    fields [
        "service_name": DelimitedBytesCodec,
        "service_key": DelimitedBytesCodec,
        "key": DelimitedBytesCodec,
    ]
);
state_table!(
    ScopedStateTable,
    ScopedStateKey,
    |_: &crate::table::ReadMetadata<'_>| true,
    fields [
        "scope": OptionalScopeCodec,
        "service_name": DelimitedBytesCodec,
        "service_key": DelimitedBytesCodec,
        "key": DelimitedBytesCodec,
    ]
);

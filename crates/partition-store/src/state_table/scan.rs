// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical reads over existing state encodings. Only the partition-key header
//! supplies bounds; the legacy suffix has no generic ordered-field capability.

use restate_storage_query_api::Table;

use crate::Result;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::KeySelection;
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder};
use crate::table::PhysicalTable;

use super::{ScopedStateKey, StateKey};

macro_rules! state_table {
    ($table:ident, $key:ty) => {
        /// Physical state records using the existing key and value encodings.
        pub enum $table {}

        impl Table for $table {
            type Record<'a> = (KeyDecoder<'a, $key>, &'a [u8]);
            type Scan = KeySelection<$key>;
        }

        impl PhysicalTable for $table {
            type Key = $key;
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
    };
}

state_table!(StateTable, StateKey);
state_table!(ScopedStateTable, ScopedStateKey);

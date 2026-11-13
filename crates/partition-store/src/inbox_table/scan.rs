// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical inbox reads on an admitted snapshot. Only the partition-key header
//! bounds the read; service names and keys retain their legacy encoding. Values
//! stay opaque until the caller decodes them, as for other physical tables.

use restate_storage_query_api::Table;

use crate::Result;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::KeySelection;
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder};
use crate::table::PhysicalTable;

use super::InboxKey;

/// Stored inbox records, bounded by the snapshot's owner and the requested
/// partition-key range. For example, an owner of `20..=40` reading `10..=30`
/// visits only `20..=30`, without assuming an ordering of service identities.
pub enum InboxTable {}

impl Table for InboxTable {
    type Record<'a> = (KeyDecoder<'a, InboxKey>, &'a [u8]);
    type Scan = KeySelection<InboxKey>;
}

impl PhysicalTable for InboxTable {
    type Key = InboxKey;
}

impl KeySchema for InboxKey {
    type Header = PartitionKeyHeader<Self>;

    fn decoder(key: &[u8]) -> Result<KeyDecoder<'_, Self>> {
        // The legacy decoder also reads the key kind and partition key.
        Ok(KeyDecoder::from_payload(key))
    }
}

impl KeyDecode for InboxKey {
    fn decode(source: &mut &[u8]) -> Result<Self> {
        Self::deserialize_from(source)
    }
}

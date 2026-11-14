// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The inbox as a table that queries can scan.
//!
//! An inbox key is `[kind][partition key][service name][service key][sequence]`.
//! A read is bounded by partition key only. Service names are stored with a
//! length prefix, so they don't sort alphabetically: "b" comes before "aa".
//! Values are returned as stored; the caller decodes them.

use restate_storage_query_api::Table;

use crate::Result;
use crate::keys::header::{KeySchema, PartitionKeyHeader};
use crate::keys::selection::KeySelection;
use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder};
use crate::table::PhysicalTable;

use super::InboxKey;

/// Stored inbox entries. A read returns the keys in both the partition's owned
/// range and the requested range: a partition owning `20..=40` asked for
/// `10..=30` reads `20..=30`.
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

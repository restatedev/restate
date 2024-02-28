// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::raw::*;
use super::*;

use crate::identifiers::{InvocationId, InvocationUuid};
use crate::invocation::ServiceInvocationSpanContext;
use bytes::Bytes;

pub type EnrichedEntryHeader = EntryHeader<InvokeEnrichmentResult, AwakeableEnrichmentResult>;
pub type EnrichedRawEntry = RawEntry<InvokeEnrichmentResult, AwakeableEnrichmentResult>;

/// Result of the target service resolution
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InvokeEnrichmentResult {
    pub invocation_uuid: InvocationUuid,
    pub service_key: Bytes,
    pub service_name: ByteString,
    // When resolving the service and generating its id, we also generate the associated span
    pub span_context: ServiceInvocationSpanContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AwakeableEnrichmentResult {
    pub invocation_id: InvocationId,
    pub entry_index: EntryIndex,
}

impl From<EnrichedRawEntry> for PlainRawEntry {
    fn from(value: EnrichedRawEntry) -> Self {
        let (h, b) = value.into_inner();
        PlainRawEntry::new(h.erase_enrichment(), b)
    }
}

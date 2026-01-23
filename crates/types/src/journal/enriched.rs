// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use crate::identifiers::InvocationId;
use crate::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use std::time::Duration;

pub type EnrichedEntryHeader = EntryHeader<CallEnrichmentResult, AwakeableEnrichmentResult>;
pub type EnrichedRawEntry = RawEntry<CallEnrichmentResult, AwakeableEnrichmentResult>;

/// Result of the target service resolution
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CallEnrichmentResult {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub completion_retention_time: Option<Duration>,

    // When resolving the service and generating its id, we also generate the associated span
    pub span_context: ServiceInvocationSpanContext,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

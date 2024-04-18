// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::errors::InvocationError;
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::PlainRawEntry;

pub trait EntryEnricher {
    fn enrich_entry(
        &self,
        entry: PlainRawEntry,
        current_invocation_target: &InvocationTarget,
        current_invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, InvocationError>;
}

#[cfg(any(test, feature = "mocks"))]
pub mod mocks {
    use super::*;

    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
    use restate_types::journal::enriched::{
        AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry, InvokeEnrichmentResult,
    };
    use restate_types::journal::raw::{PlainEntryHeader, RawEntry};

    #[derive(Debug, Default, Clone)]
    pub struct MockEntryEnricher;

    impl EntryEnricher for MockEntryEnricher {
        fn enrich_entry(
            &self,
            entry: PlainRawEntry,
            _current_invocation_target: &InvocationTarget,
            current_invocation_span_context: &ServiceInvocationSpanContext,
        ) -> Result<EnrichedRawEntry, InvocationError> {
            let (header, entry) = entry.into_inner();
            let enriched_header = match header {
                PlainEntryHeader::Input {} => EnrichedEntryHeader::Input {},
                PlainEntryHeader::Output {} => EnrichedEntryHeader::Output {},
                PlainEntryHeader::GetState { is_completed } => {
                    EnrichedEntryHeader::GetState { is_completed }
                }
                PlainEntryHeader::SetState {} => EnrichedEntryHeader::SetState {},
                PlainEntryHeader::ClearState {} => EnrichedEntryHeader::ClearState {},
                PlainEntryHeader::GetStateKeys { is_completed } => {
                    EnrichedEntryHeader::GetStateKeys { is_completed }
                }
                PlainEntryHeader::ClearAllState {} => EnrichedEntryHeader::ClearAllState {},
                PlainEntryHeader::Sleep { is_completed } => {
                    EnrichedEntryHeader::Sleep { is_completed }
                }
                PlainEntryHeader::Invoke { is_completed, .. } => {
                    if !is_completed {
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            enrichment_result: Some(InvokeEnrichmentResult {
                                invocation_id: InvocationId::mock_random(),
                                invocation_target: InvocationTarget::service("", ""),
                                span_context: current_invocation_span_context.clone(),
                            }),
                        }
                    } else {
                        // No need to service resolution if the entry was completed by the service
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            enrichment_result: None,
                        }
                    }
                }
                PlainEntryHeader::BackgroundInvoke { .. } => {
                    EnrichedEntryHeader::BackgroundInvoke {
                        enrichment_result: InvokeEnrichmentResult {
                            invocation_id: InvocationId::mock_random(),
                            invocation_target: InvocationTarget::service("", ""),
                            span_context: current_invocation_span_context.clone(),
                        },
                    }
                }
                PlainEntryHeader::Awakeable { is_completed } => {
                    EnrichedEntryHeader::Awakeable { is_completed }
                }
                PlainEntryHeader::CompleteAwakeable { .. } => {
                    EnrichedEntryHeader::CompleteAwakeable {
                        enrichment_result: AwakeableEnrichmentResult {
                            invocation_id: InvocationId::from_parts(0, InvocationUuid::new()),
                            entry_index: 1,
                        },
                    }
                }
                PlainEntryHeader::SideEffect {} => EnrichedEntryHeader::SideEffect {},
                PlainEntryHeader::Custom { code } => EnrichedEntryHeader::Custom { code },
            };

            Ok(RawEntry::new(enriched_header, entry))
        }
    }
}

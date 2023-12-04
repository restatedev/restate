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
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::PlainRawEntry;

pub trait EntryEnricher {
    fn enrich_entry(
        &self,
        entry: PlainRawEntry,
        invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, InvocationError>;
}

#[cfg(any(test, feature = "mocks"))]
pub mod mocks {
    use super::*;

    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::invocation::ServiceInvocationSpanContext;
    use restate_types::journal::enriched::{
        AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry, InvokeEnrichmentResult,
    };
    use restate_types::journal::raw::{PlainEntryHeader, RawEntry};

    #[derive(Debug, Default, Clone)]
    pub struct MockEntryEnricher;

    impl EntryEnricher for MockEntryEnricher {
        fn enrich_entry(
            &self,
            raw_entry: PlainRawEntry,
            invocation_span_context: &ServiceInvocationSpanContext,
        ) -> Result<EnrichedRawEntry, InvocationError> {
            let (header, entry) = raw_entry.into_inner();
            let enriched_header = match header {
                PlainEntryHeader::PollInputStream { is_completed } => {
                    EnrichedEntryHeader::PollInputStream { is_completed }
                }
                PlainEntryHeader::OutputStream {} => EnrichedEntryHeader::OutputStream {},
                PlainEntryHeader::GetState { is_completed } => {
                    EnrichedEntryHeader::GetState { is_completed }
                }
                PlainEntryHeader::SetState {} => EnrichedEntryHeader::SetState {},
                PlainEntryHeader::ClearState {} => EnrichedEntryHeader::ClearState {},
                PlainEntryHeader::Sleep { is_completed } => {
                    EnrichedEntryHeader::Sleep { is_completed }
                }
                PlainEntryHeader::Invoke { is_completed, .. } => {
                    if !is_completed {
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            enrichment_result: Some(InvokeEnrichmentResult {
                                invocation_uuid: Default::default(),
                                service_key: Default::default(),
                                service_name: Default::default(),
                                span_context: invocation_span_context.clone(),
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
                            invocation_uuid: Default::default(),
                            service_key: Default::default(),
                            service_name: Default::default(),
                            span_context: invocation_span_context.clone(),
                        },
                    }
                }
                PlainEntryHeader::Awakeable { is_completed } => {
                    EnrichedEntryHeader::Awakeable { is_completed }
                }
                PlainEntryHeader::CompleteAwakeable { .. } => {
                    EnrichedEntryHeader::CompleteAwakeable {
                        enrichment_result: AwakeableEnrichmentResult {
                            invocation_id: InvocationId::new(0, InvocationUuid::now_v7()),
                            entry_index: 1,
                        },
                    }
                }
                PlainEntryHeader::Custom { code } => EnrichedEntryHeader::Custom { code },
            };

            Ok(RawEntry::new(enriched_header, entry))
        }
    }
}

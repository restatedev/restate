// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::PlainRawEntry;

pub trait EntryEnricher {
    fn enrich_entry(
        &self,
        entry: PlainRawEntry,
        invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, anyhow::Error>;
}

#[cfg(any(test, feature = "mocks"))]
pub mod mocks {
    use super::*;

    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::invocation::ServiceInvocationSpanContext;
    use restate_types::journal::enriched::{
        EnrichedEntryHeader, EnrichedRawEntry, ResolutionResult,
    };
    use restate_types::journal::raw::{RawEntry, RawEntryHeader};

    #[derive(Debug, Default, Clone)]
    pub struct MockEntryEnricher;

    impl EntryEnricher for MockEntryEnricher {
        fn enrich_entry(
            &self,
            raw_entry: PlainRawEntry,
            invocation_span_context: &ServiceInvocationSpanContext,
        ) -> Result<EnrichedRawEntry, anyhow::Error> {
            let enriched_header = match raw_entry.header {
                RawEntryHeader::PollInputStream { is_completed } => {
                    EnrichedEntryHeader::PollInputStream { is_completed }
                }
                RawEntryHeader::OutputStream => EnrichedEntryHeader::OutputStream,
                RawEntryHeader::GetState { is_completed } => {
                    EnrichedEntryHeader::GetState { is_completed }
                }
                RawEntryHeader::SetState => EnrichedEntryHeader::SetState,
                RawEntryHeader::ClearState => EnrichedEntryHeader::ClearState,
                RawEntryHeader::Sleep { is_completed } => {
                    EnrichedEntryHeader::Sleep { is_completed }
                }
                RawEntryHeader::Invoke { is_completed } => {
                    if !is_completed {
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            resolution_result: Some(ResolutionResult {
                                invocation_uuid: Default::default(),
                                service_key: Default::default(),
                                service_name: Default::default(),
                                span_context: invocation_span_context.clone(),
                            }),
                        }
                    } else {
                        // No need to service resolution if the entry was completed by the service endpoint
                        EnrichedEntryHeader::Invoke {
                            is_completed,
                            resolution_result: None,
                        }
                    }
                }
                RawEntryHeader::BackgroundInvoke => EnrichedEntryHeader::BackgroundInvoke {
                    resolution_result: ResolutionResult {
                        invocation_uuid: Default::default(),
                        service_key: Default::default(),
                        service_name: Default::default(),
                        span_context: invocation_span_context.clone(),
                    },
                },
                RawEntryHeader::Awakeable { is_completed } => {
                    EnrichedEntryHeader::Awakeable { is_completed }
                }
                RawEntryHeader::CompleteAwakeable => EnrichedEntryHeader::CompleteAwakeable {
                    invocation_id: InvocationId::new(0, InvocationUuid::now_v7()),
                    entry_index: 1,
                },
                RawEntryHeader::Custom { code, requires_ack } => {
                    EnrichedEntryHeader::Custom { code, requires_ack }
                }
            };

            Ok(RawEntry::new(enriched_header, raw_entry.entry))
        }
    }
}

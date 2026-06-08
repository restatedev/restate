// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
        &mut self,
        entry: PlainRawEntry,
        current_invocation_target: &InvocationTarget,
        current_invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, InvocationError>;
}

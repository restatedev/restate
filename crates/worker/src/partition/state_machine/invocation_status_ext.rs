// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::invocation::InvocationEpoch;
use restate_types::journal_v2::CompletionId;

// Few useful helpers for InvocationStatus, used by the state machine
pub(super) trait InvocationStatusExt {
    /// Returns true if this invocation should accept the entry with given source epoch and completion id.
    fn should_accept_completion(
        &self,
        source_invocation_epoch: InvocationEpoch,
        completion_id: CompletionId,
    ) -> bool;
}

impl InvocationStatusExt for InvocationStatus {
    fn should_accept_completion(
        &self,
        source_invocation_epoch: InvocationEpoch,
        completion_id: CompletionId,
    ) -> bool {
        if let Some(im) = self.get_invocation_metadata() {
            im.completion_range_epoch_map
                .maximum_epoch_for(completion_id)
                <= source_invocation_epoch
        } else {
            // This is not an in-flight state, so all good to ignore.
            false
        }
    }
}

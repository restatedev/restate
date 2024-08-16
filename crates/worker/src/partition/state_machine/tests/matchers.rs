// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use googletest::prelude::*;

pub mod storage {
    use super::*;

    use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::InvocationTarget;

    pub fn invocation_inbox_entry(
        invocation_id: InvocationId,
        invocation_target: &InvocationTarget,
    ) -> impl Matcher<ActualT = SequenceNumberInboxEntry> {
        pat!(SequenceNumberInboxEntry {
            inbox_entry: pat!(InboxEntry::Invocation(
                eq(invocation_target.as_keyed_service_id().unwrap()),
                eq(invocation_id)
            ))
        })
    }
}

pub mod actions {
    use super::*;

    use crate::partition::state_machine::Action;
    use restate_types::identifiers::InvocationId;

    pub fn invoke_for_id(invocation_id: InvocationId) -> impl Matcher<ActualT = Action> {
        pat!(Action::Invoke {
            invocation_id: eq(invocation_id)
        })
    }
}

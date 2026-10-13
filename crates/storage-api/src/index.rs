// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::time::MillisSinceEpoch;
use restate_types::identifiers::CanonicalEntryId;
use restate_util_string::ReString;

crate::define_table! {
    /// Persisted entry-index records ordered by stage, service, and transition time.
    /// This inspection surface does not imply completeness for pre-existing stores.
    pub EntryByService;
}

crate::define_filter! {
    pub EntryByService {
        /// The service name.
        service_name: ReString => starts_with,
        /// The VQueue stage, compared by its string representation.
        stage: ReString => starts_with,
        /// The entry's last stage transition as a Unix timestamp in milliseconds.
        transitioned_at: MillisSinceEpoch,
        /// The indexed entry's incarnation identity.
        canonical_id: CanonicalEntryId,
    }
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

crate::define_table! {
    /// Data table for service load statistics.
    #[derive(Debug)]
    pub ServiceLoad;
}

crate::define_filter! {
    /// Data table for service load statistics.
    pub ServiceLoad {
        /// The service name.
        service_name: ReString => starts_with,
        /// The handler name, or NULL when absent.
        handler: Option<ReString> => starts_with,
        /// The kind of the entry or operation.
        kind: EntryKind,
    }
}

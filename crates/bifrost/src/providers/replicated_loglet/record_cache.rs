// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A placeholder for a global record cache.
///
/// This can be safely shared between all ReplicatedLoglet(s) and the LocalSequencers or the
/// RemoteSequencers
///
///
/// This is a streaming LRU-cache with total memory budget tracking and enforcement.
#[derive(Clone, Debug)]
pub struct RecordCache {}

impl RecordCache {
    pub fn new(_memory_budget_bytes: usize) -> Self {
        Self {}
    }
}

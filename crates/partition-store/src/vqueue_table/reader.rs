// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::{EntryCard, VQueueStore};
use restate_types::vqueue::VQueueId;

use crate::PartitionDb;

use super::running_reader::VQueueRunningReader;
use super::waiting_reader::VQueueWaitingReader;

impl VQueueStore for PartitionDb {
    type Item = EntryCard;
    type RunningReader = VQueueRunningReader;
    type InboxReader = VQueueWaitingReader;

    fn new_run_reader(&self, qid: &VQueueId) -> Self::RunningReader {
        VQueueRunningReader::new(self, qid)
    }

    fn new_inbox_reader(&self, qid: &VQueueId) -> Self::InboxReader {
        VQueueWaitingReader::new(self, qid)
    }
}

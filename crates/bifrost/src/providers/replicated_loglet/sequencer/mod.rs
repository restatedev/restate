// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod append;
mod node;

use std::sync::Arc;

use restate_core::ShutdownError;
use restate_types::{
    logs::{LogletOffset, Record},
    replicated_loglet::ReplicatedLogletId,
    GenerationalNodeId,
};

use super::replication::spread_selector::SpreadSelector;
use crate::loglet::util::TailOffsetWatch;

#[derive(thiserror::Error, Debug)]
pub enum SequencerError {
    #[error("loglet offset exhausted")]
    LogletOffsetExhausted,
    #[error("batch exceeds possible length")]
    InvalidBatchLength,
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Sequencer shared state
pub struct SequencerSharedState {
    node_id: GenerationalNodeId,
    loglet_id: ReplicatedLogletId,
    committed_tail: TailOffsetWatch,
    selector: SpreadSelector,
}

impl SequencerSharedState {
    pub fn node_id(&self) -> &GenerationalNodeId {
        &self.node_id
    }

    pub fn loglet_id(&self) -> &ReplicatedLogletId {
        &self.loglet_id
    }

    pub fn global_committed_tail(&self) -> &TailOffsetWatch {
        &self.committed_tail
    }
}

trait BatchExt {
    /// tail computes inflight tail after this batch is committed
    fn last_offset(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError>;
}

impl BatchExt for Arc<[Record]> {
    fn last_offset(&self, first_offset: LogletOffset) -> Result<LogletOffset, SequencerError> {
        let len = u32::try_from(self.len()).map_err(|_| SequencerError::InvalidBatchLength)?;

        first_offset
            .checked_add(len - 1)
            .map(LogletOffset::from)
            .ok_or(SequencerError::LogletOffsetExhausted)
    }
}

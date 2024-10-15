// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod check_seal;
mod digests;
mod find_tail;
mod periodic_tail_checker;
mod repair_tail;
mod seal;

pub use check_seal::*;
pub use find_tail::*;
pub use periodic_tail_checker::*;
pub use repair_tail::*;
pub use seal::*;

use restate_types::logs::LogletOffset;

use super::replication::Merge;

#[derive(Debug, Default)]
enum NodeTailStatus {
    #[default]
    Unknown,
    Known {
        local_tail: LogletOffset,
        sealed: bool,
    },
}

impl NodeTailStatus {
    fn local_tail(&self) -> Option<LogletOffset> {
        match self {
            NodeTailStatus::Known { local_tail, .. } => Some(*local_tail),
            _ => None,
        }
    }

    fn is_known(&self) -> bool {
        matches!(self, NodeTailStatus::Known { .. })
    }

    #[allow(dead_code)]
    fn is_known_unsealed(&self) -> bool {
        matches!(self, NodeTailStatus::Known { sealed, .. } if !*sealed)
    }

    fn is_known_sealed(&self) -> bool {
        matches!(self, NodeTailStatus::Known { sealed, .. } if *sealed)
    }
}

impl Merge for NodeTailStatus {
    fn merge(&mut self, other: Self) {
        match (self, other) {
            (_, NodeTailStatus::Unknown) => {}
            (this @ NodeTailStatus::Unknown, o) => {
                *this = o;
            }
            (
                NodeTailStatus::Known {
                    local_tail: my_offset,
                    sealed: my_seal,
                },
                NodeTailStatus::Known {
                    local_tail: other_offset,
                    sealed: other_seal,
                },
            ) => {
                *my_offset = (*my_offset).max(other_offset);
                *my_seal |= other_seal;
            }
        }
    }
}

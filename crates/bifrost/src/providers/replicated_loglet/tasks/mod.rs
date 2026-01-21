// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
mod get_trim_point;
mod periodic_tail_checker;
mod repair_tail;
mod seal;
mod trim;
mod util;

pub use check_seal::*;
pub use find_tail::*;
pub use get_trim_point::*;
pub use periodic_tail_checker::*;
pub use repair_tail::*;
pub use seal::*;
pub use trim::*;

use restate_types::Merge;
use restate_types::logs::LogletOffset;

#[derive(Debug, Clone, Hash, Default, PartialEq, Eq, derive_more::Display)]
enum NodeTailStatus {
    #[default]
    #[display("Unknown")]
    Unknown,
    #[display("({local_tail}, sealed={sealed})")]
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
    // we don't care about accuracy of the return boolean as we don't use it for conditional
    // updates anywhere.
    fn merge(&mut self, other: Self) -> bool {
        match (self, other) {
            (_, NodeTailStatus::Unknown) => false,
            (this @ NodeTailStatus::Unknown, o) => {
                *this = o;
                true
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
                true
            }
        }
    }
}

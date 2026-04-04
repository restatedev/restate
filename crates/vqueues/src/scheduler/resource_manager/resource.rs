// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

struct Budget {
    in_flight: u32,
    returned: Arc<AtomicU32>,
}

impl Budget {
    fn available(&self, limit: u32) -> u32 {
        limit.saturating_sub(self.in_flight)
    }

    fn provision(&mut self, count: u32, limit: u32) -> Option<ProvisionalToken> {
        let available = self.available(limit);
        (available >= count).then_some(ProvisionalToken { count })
    }

    fn acquire(&mut self, provisional: ProvisionalToken) -> BudgetToken {
        let count = provisional.count;
        self.in_flight += count;
        BudgetToken {
            count,
            returned: self.returned.clone(),
        }
    }

    /// Returns true if any resources were reclaimed
    fn reclaim_returned(&mut self) -> bool {
        let mut cur_returned = self.returned.load(Ordering::Acquire);
        loop {
            if cur_returned == 0 {
                return false;
            }
            match self.returned.compare_exchange_weak(
                cur_returned,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.in_flight -= cur_returned;
                    return true;
                }
                Err(new_returned) => {
                    cur_returned = new_returned;
                }
            }
        }
    }
}

pub struct ProvisionalToken {
    count: u32,
}

pub struct BudgetToken {
    // resource: ResourceKind,
    // possibly a resource key (counter or counters)
    // dimensions: ReString
    count: u32,
    returned: mpsc::UnboundedSender<ResourceUpdate>,
    // perhaps this will be on the compound permit.
    // notify: Arc<Notify>,
}

impl Drop for BudgetToken {
    fn drop(&mut self) {
        // notify the resource manager that we're done with this token.
        self.returned.fetch_add(self.count, Ordering::AcqRel);
        // I need to mark the resource dirty.
    }
}

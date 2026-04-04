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
// how to deal with limiters.
//
// on release we need to find all the counters (those are the budgets above)
// that's fine, but how we'll poll all of them?
// those can be very high number of counters that mostly don't change.
// collect the ones that may have changed
// a token maybe linked to multiple arcs.
//
//
//
// do we care about atomicity of release? probably.
// return holds a mutex over the three counters.
// add released to all of them at the same time
//
// shit, i dont' want to hold three mutexes.
//
// alternatively if we return a token via a channel, we can return
// can execute one by one. the inbound channel becomes our notification scheme as well.
//
// but how many do we read? read everything as aggressive as possible, maybe.
// basically this means it's one big lock on all counters.
//
// scheduler will be reclaiming resources, more ticks.
// on poll, we'll go through.
//
// the compound token is then sent back via a channel.
// what's returned has information to help me locate what to return.
// big fat object. maybe box it?
//
//
// Ok, this is the reclamation scheme. Once we have resources that are reclaimed.
// we go through the list of resources (and specific counters?)
// and build up a list of the queues there were woken up by this round of release.
//
// those should go to the top of the ring, not sure ordered by what tbh.
//
//
// Using this method, we don't need to wait for end of invocations (via PP) to be woken up
//
// open question: what to do when limits change (and how to know?) wake up everybody. it's
// cheaper to find which limits have changed and find the counters that match.
// for scope level, find all counters (has waiters, maybe that's an important property)
// and for each one, signal it to be rescanned *wake up*.
// same thing goes for L1 and L2. (inversed scan is needed here)
//
// what's critical is that for each counter we need to keep the queues the are waiting.
//
// How to perform wake-ups?
//
//
// Pathological case:
// Only scope level limits.
// One scope.
// All vqueues run on the same scope.
//
//
// Maybe instead of returning all vqueues from the wait list. We can return the ones that
// _can_ be worken up. To achieve this goal
//
//
// I need to first reclaim those resources, keep a cache of all counters/resources that were
// updated.
//
// Once all updated. We check one resource at a time, and we perform a special scheduling round
//
// By going through the queues on 1st resource:
//
// design options:
// - check if the vqueue can pop, if yes, pop and accumulate the decision.
// - then place it back into the normal ready ring if it's still eligible.
// - move to the next vqueue on the same resource.
// - if a vqueue cannot be popped, rotate it in the ring (in the waiters list on this resource)
// - we use the same deficit mechanism as normal.
//
// Basically this makes this an N rings. each ring (wait/ready). Al
//
// Option 2:
// Just pop those vqueues (all) and place them on the normal ring but at the front.
// I like the simplicity of this approach tbh.
//
//
//
//
// Final thoughts.
// it seems that the atomic return approach wouldn't work if. But I'm not quite sure.
// maybe not. If it's okay to not return atomically.
//
// In atomic land. How do I know which has been updated? I can't easily do that.

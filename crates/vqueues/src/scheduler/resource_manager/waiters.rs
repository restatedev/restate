// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use restate_types::Priority;

use crate::scheduler::VQueueHandle;

/// A set of queues waiting on a resource, served by priority and then in arrival order.
///
/// Queues with a non-normal priority are kept in lazily allocated lists, so waiters that only
/// ever see normal priority cost a single pointer on top of a plain `VecDeque`.
#[derive(Debug, Default)]
pub struct Waiters {
    normal: VecDeque<VQueueHandle>,
    others: Option<Box<OtherWaiters>>,
}

#[derive(Debug, Default)]
struct OtherWaiters {
    live: VecDeque<VQueueHandle>,
    high: VecDeque<VQueueHandle>,
    low: VecDeque<VQueueHandle>,
}

impl Waiters {
    pub fn push_back(&mut self, handle: VQueueHandle, priority: Priority) {
        match priority {
            Priority::Normal => self.normal.push_back(handle),
            Priority::Live => self.others_mut().live.push_back(handle),
            Priority::High => self.others_mut().high.push_back(handle),
            Priority::Low => self.others_mut().low.push_back(handle),
        }
    }

    pub fn front(&self) -> Option<&VQueueHandle> {
        if self.others.is_none() {
            return self.normal.front();
        }
        self.lists().into_iter().flatten().find_map(VecDeque::front)
    }

    pub fn pop_front(&mut self) -> Option<VQueueHandle> {
        if self.others.is_none() {
            return self.normal.pop_front();
        }
        self.lists_mut()
            .into_iter()
            .flatten()
            .find_map(VecDeque::pop_front)
    }

    pub fn retain(&mut self, mut f: impl FnMut(&VQueueHandle) -> bool) {
        for list in self.lists_mut().into_iter().flatten() {
            list.retain(&mut f);
        }
    }

    pub fn contains(&self, handle: &VQueueHandle) -> bool {
        self.lists()
            .into_iter()
            .flatten()
            .any(|list| list.contains(handle))
    }

    pub fn is_empty(&self) -> bool {
        self.lists().into_iter().flatten().all(VecDeque::is_empty)
    }

    pub fn len(&self) -> usize {
        self.lists().into_iter().flatten().map(VecDeque::len).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = &VQueueHandle> {
        self.lists().into_iter().flatten().flat_map(VecDeque::iter)
    }

    pub fn drain(&mut self) -> impl Iterator<Item = VQueueHandle> + '_ {
        self.lists_mut()
            .into_iter()
            .flatten()
            .flat_map(|list| list.drain(..))
    }

    fn others_mut(&mut self) -> &mut OtherWaiters {
        self.others.get_or_insert_default()
    }

    fn lists(&self) -> [Option<&VecDeque<VQueueHandle>>; 4] {
        let others = self.others.as_deref();
        [
            others.map(|others| &others.live),
            others.map(|others| &others.high),
            Some(&self.normal),
            others.map(|others| &others.low),
        ]
    }

    fn lists_mut(&mut self) -> [Option<&mut VecDeque<VQueueHandle>>; 4] {
        match self.others.as_deref_mut() {
            Some(others) => [
                Some(&mut others.live),
                Some(&mut others.high),
                Some(&mut self.normal),
                Some(&mut others.low),
            ],
            None => [None, None, Some(&mut self.normal), None],
        }
    }
}

#[cfg(test)]
mod tests {
    use slotmap::SlotMap;

    use super::*;

    #[test]
    fn waiters_are_served_by_priority_then_arrival() {
        assert_eq!(
            std::mem::size_of::<Waiters>(),
            std::mem::size_of::<VecDeque<VQueueHandle>>() + std::mem::size_of::<usize>()
        );

        let mut handles = SlotMap::<VQueueHandle, ()>::with_key();
        let [normal_1, low, normal_2, live, high] = std::array::from_fn(|_| handles.insert(()));

        let mut waiters = Waiters::default();
        waiters.push_back(normal_1, Priority::Normal);
        waiters.push_back(normal_2, Priority::Normal);
        assert!(waiters.others.is_none());

        waiters.push_back(low, Priority::Low);
        waiters.push_back(live, Priority::Live);
        waiters.push_back(high, Priority::High);
        assert_eq!(waiters.len(), 5);
        assert_eq!(waiters.front(), Some(&live));
        assert_eq!(
            waiters.iter().copied().collect::<Vec<_>>(),
            [live, high, normal_1, normal_2, low]
        );

        waiters.retain(|handle| *handle != high && *handle != normal_1);
        assert!(!waiters.contains(&high));
        assert_eq!(waiters.pop_front(), Some(live));
        assert_eq!(waiters.pop_front(), Some(normal_2));
        assert_eq!(waiters.drain().collect::<Vec<_>>(), [low]);
        assert!(waiters.is_empty());
        assert_eq!(waiters.pop_front(), None);
    }
}

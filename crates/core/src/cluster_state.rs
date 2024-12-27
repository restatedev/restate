// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::{GenerationalNodeId, NodeId, PlainNodeId};
use std::{iter::Iterator, marker::PhantomData};

use crate::TaskCenter;

struct UnimplementedIter<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> UnimplementedIter<T> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Iterator for UnimplementedIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!();
    }
}

//todo(azmy): Default is temporary, remove it when the implementation is done
#[derive(Debug, Clone, Default)]
pub struct ClusterState {}

impl ClusterState {
    pub fn try_current() -> Option<Self> {
        TaskCenter::with_current(|h| h.cluster_state())
    }

    pub fn current() -> Self {
        TaskCenter::with_current(|h| h.cluster_state()).expect("called outside task-center scope")
    }

    pub fn alive(&self) -> impl Iterator<Item = GenerationalNodeId> {
        UnimplementedIter::new()
    }

    pub fn dead(&self) -> impl Iterator<Item = PlainNodeId> {
        UnimplementedIter::new()
    }

    pub fn is_alive(&self, _node_id: NodeId) -> Option<GenerationalNodeId> {
        unimplemented!();
    }

    pub fn first_alive(&self, _nodes: &[PlainNodeId]) -> Option<GenerationalNodeId> {
        unimplemented!();
    }
}

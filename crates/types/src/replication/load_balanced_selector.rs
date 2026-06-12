// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::collections::HashSet;
use std::hash::Hasher;

use xxhash_rust::xxh3::Xxh3;

use crate::PlainNodeId;
use crate::replication::NodeSet;

// This is the existing Restate placement salt. Changing it reshuffles every
// partition/log placement that uses this selector.
pub const HASH_SALT: u64 = 14712721395741015273;

/// Returns the deterministic HRW score for `node_id` under the shared Restate placement salt.
pub fn hash_node_id(hashing_id: u64, node_id: PlainNodeId) -> u64 {
    let mut hasher = Xxh3::with_seed(HASH_SALT);
    hasher.write_u64(hashing_id);
    hasher.write_u64(u32::from(node_id) as u64);
    hasher.finish()
}

/// Selects a nodeset by HRW order, but chooses the least-loaded node within the top-N window.
pub fn select_top_n_load_balanced(
    candidates: impl IntoIterator<Item = PlainNodeId>,
    hashing_id: u64,
    target_size: usize,
    required_node: Option<PlainNodeId>,
    top_n: usize,
    load: impl Fn(PlainNodeId) -> usize,
) -> Option<NodeSet> {
    let mut selected = NodeSet::with_capacity(target_size);
    if let Some(required_node) = required_node {
        selected.insert(required_node);
    }

    extend_top_n_load_balanced(candidates, hashing_id, target_size, selected, top_n, load)
}

/// Extends an existing nodeset with load-aware picks from the top-N HRW candidate window.
pub fn extend_top_n_load_balanced(
    candidates: impl IntoIterator<Item = PlainNodeId>,
    hashing_id: u64,
    target_size: usize,
    mut selected: NodeSet,
    top_n: usize,
    load: impl Fn(PlainNodeId) -> usize,
) -> Option<NodeSet> {
    assert!(target_size > 0, "target_size must be greater than zero");

    let mut order = candidates.into_iter().collect::<Vec<_>>();
    order.sort_by_key(|node_id| {
        (
            Reverse(hash_node_id(hashing_id, *node_id)),
            u32::from(*node_id),
        )
    });
    order.dedup();
    let candidate_set = order.iter().copied().collect::<HashSet<_>>();
    selected.retain(|node_id| candidate_set.contains(node_id));

    if order.len() < target_size || selected.len() > target_size {
        return None;
    }

    while selected.len() < target_size {
        let candidate = order
            .iter()
            .enumerate()
            .map(|(rank, node_id)| (rank, *node_id))
            .filter(|(_, node_id)| !selected.contains(*node_id))
            .take(top_n.max(1))
            .min_by_key(|(rank, node_id)| (load(*node_id), *rank))?
            .1;
        selected.insert(candidate);
    }

    Some(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u32) -> PlainNodeId {
        PlainNodeId::new(id)
    }

    #[test]
    fn selects_least_loaded_candidate_within_top_n_hash_ranking() {
        let hashing_id = 17;
        let mut candidates = [node(1), node(2), node(3), node(4)];
        candidates.sort_by_key(|node_id| {
            (
                Reverse(hash_node_id(hashing_id, *node_id)),
                u32::from(*node_id),
            )
        });

        let highest_ranked = candidates[0];
        let less_loaded = candidates[1];
        let selected = select_top_n_load_balanced(candidates, hashing_id, 1, None, 2, |node_id| {
            if node_id == highest_ranked { 1 } else { 0 }
        })
        .expect("selection should succeed");

        assert!(selected.contains(less_loaded));
    }

    #[test]
    fn extends_existing_selection_without_counting_filtered_nodes() {
        let selected = NodeSet::from_iter([node(10), node(2)]);
        let selected =
            extend_top_n_load_balanced([node(1), node(2), node(3)], 23, 2, selected, 3, |_| 0)
                .expect("selection should succeed");

        assert!(selected.contains(node(2)));
        assert_eq!(selected.len(), 2);
        assert!(!selected.contains(node(10)));
    }

    #[test]
    #[should_panic(expected = "target_size must be greater than zero")]
    fn rejects_zero_target_size() {
        let _ = extend_top_n_load_balanced([node(1)], 23, 0, NodeSet::new(), 1, |_| 0);
    }

    #[test]
    fn reports_impossible_selections() {
        let too_few_candidates =
            extend_top_n_load_balanced([node(1)], 23, 2, NodeSet::new(), 1, |_| 0);
        assert!(too_few_candidates.is_none());

        let too_many_selected = extend_top_n_load_balanced(
            [node(1), node(2)],
            23,
            1,
            NodeSet::from_iter([node(1), node(2)]),
            1,
            |_| 0,
        );
        assert!(too_many_selected.is_none());
    }

    #[test]
    fn preserves_hrw_determinism_when_loads_tie_or_top_n_is_zero() {
        let hashing_id = 17;
        let mut candidates = [node(1), node(2), node(3), node(4)];
        candidates.sort_by_key(|node_id| {
            (
                Reverse(hash_node_id(hashing_id, *node_id)),
                u32::from(*node_id),
            )
        });

        let selected = select_top_n_load_balanced(candidates, hashing_id, 1, None, 0, |_| 0)
            .expect("selection should succeed");

        assert!(selected.contains(candidates[0]));
    }
}

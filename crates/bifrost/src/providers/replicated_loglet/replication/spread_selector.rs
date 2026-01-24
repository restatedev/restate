// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(any(test, feature = "test-util"))]
use std::sync::Arc;

#[cfg(any(test, feature = "test-util"))]
use parking_lot::Mutex;
use rand::prelude::*;

use restate_types::nodes_config::NodesConfiguration;
use restate_types::replicated_loglet::Spread;
use restate_types::replication::{NodeSet, NodeSetChecker, ReplicationProperty};

#[derive(Debug, Clone, thiserror::Error)]
pub enum SpreadSelectorError {
    #[error("Insufficient writeable nodes in the nodeset")]
    InsufficientWriteableNodes,
}

#[derive(Debug, Clone)]
pub enum SelectorStrategy {
    /// Selects all writeable nodes in the nodeset, this might lead to over-replication,
    /// and it's up to the appender state machine to continue replicating beyond the
    /// write-quorum requirements or not.
    Flood,
    #[cfg(any(test, feature = "test-util"))]
    /// Used in testing, generates deterministically static spreads
    Fixed(FixedSpreadSelector),
}

/// Spread selector is thread-safe and can be used concurrently.
#[derive(Clone)]
pub struct SpreadSelector {
    nodeset: NodeSet,
    strategy: SelectorStrategy,
    replication_property: ReplicationProperty,
}

impl SpreadSelector {
    pub fn new(
        nodeset: NodeSet,
        strategy: SelectorStrategy,
        replication_property: ReplicationProperty,
    ) -> Self {
        Self {
            nodeset,
            strategy,
            replication_property,
        }
    }

    pub fn nodeset(&self) -> &NodeSet {
        &self.nodeset
    }

    pub fn replication_property(&self) -> &ReplicationProperty {
        &self.replication_property
    }

    /// Generates a spread or fails if it's not possible to generate a spread out of
    /// the nodeset modulo the non-writeable nodes in the nodes configuration and after excluding
    /// the set of nodes passed in `exclude_nodes`.
    ///
    /// The selector avoids nodes non-writeable nodes
    pub fn select<R: Rng + ?Sized>(
        &self,
        rng: &mut R,
        nodes_config: &NodesConfiguration,
        exclude_nodes: &NodeSet,
    ) -> Result<Spread, SpreadSelectorError> {
        // Get the list of non-empty nodes from the nodeset given the nodes configuration
        let mut writeable_nodes: NodeSet = self
            .nodeset
            .difference(exclude_nodes)
            .filter(|node_id| {
                nodes_config
                    .get_log_server_storage_state(node_id)
                    .can_write_to()
            })
            .collect();

        if writeable_nodes.len() < self.replication_property.num_copies().into() {
            return Err(SpreadSelectorError::InsufficientWriteableNodes);
        }

        let selected: Spread = match &self.strategy {
            SelectorStrategy::Flood => {
                writeable_nodes.shuffle(rng);
                Spread::from(writeable_nodes)
            }
            #[cfg(any(test, feature = "test-util"))]
            SelectorStrategy::Fixed(selector) => selector.select()?,
        };

        // validate that we can have write quorum with this spread
        let mut checker =
            NodeSetChecker::new(&self.nodeset, nodes_config, &self.replication_property);
        checker.set_attribute_on_each(selected.iter().copied(), true);
        if !checker.check_write_quorum(|attr| *attr) {
            return Err(SpreadSelectorError::InsufficientWriteableNodes);
        }

        Ok(selected)
    }

    /// Starting from an existing set of nodes that already have copies of a record, this
    /// returns additional nodes that we can replicate to, in order to satisfy the replication
    /// property. If not possible, it fails with `InsufficientWriteableNodes'
    ///
    /// Note that this can return _more_ nodes than needed, depending on the selector strategy.
    ///
    /// The selector automatically avoids nodes non-writeable nodes
    pub fn select_fixups<R: Rng + ?Sized>(
        &self,
        existing_copies: &NodeSet,
        rng: &mut R,
        nodes_config: &NodesConfiguration,
        exclude_nodes: &NodeSet,
    ) -> Result<Spread, SpreadSelectorError> {
        // Get the list of non-empty nodes from the nodeset given the nodes configuration
        let mut writeable_nodes: NodeSet = self
            .nodeset
            .difference(exclude_nodes)
            .filter(|node_id| {
                nodes_config
                    .get_log_server_storage_state(node_id)
                    .can_write_to()
            })
            .collect();
        if writeable_nodes.len() < self.replication_property.num_copies().into() {
            return Err(SpreadSelectorError::InsufficientWriteableNodes);
        }

        let selected: Spread = match &self.strategy {
            SelectorStrategy::Flood => {
                writeable_nodes.shuffle(rng);
                Spread::from(writeable_nodes)
            }
            #[cfg(any(test, feature = "test-util"))]
            SelectorStrategy::Fixed(selector) => selector.select()?,
        };

        // validate that we can have write quorum with this spread
        let mut checker =
            NodeSetChecker::new(&self.nodeset, nodes_config, &self.replication_property);
        checker.set_attribute_on_each(selected.iter().copied(), true);
        if !checker.check_write_quorum(|attr| *attr) {
            return Err(SpreadSelectorError::InsufficientWriteableNodes);
        }

        // Remove existing nodes from selected spread to return the fixups only.
        let selected: Vec<_> = selected
            .into_iter()
            // keep nodes that are not in existing_copies.
            .filter(|n| !existing_copies.contains(*n))
            .collect();

        Ok(selected.into())
    }
}

static_assertions::assert_impl_all!(SpreadSelector: Send, Sync);

#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone)]
pub struct FixedSpreadSelector {
    pub result: Arc<Mutex<Result<Spread, SpreadSelectorError>>>,
}

#[cfg(any(test, feature = "test-util"))]
impl FixedSpreadSelector {
    pub fn select(&self) -> Result<Spread, SpreadSelectorError> {
        let guard = self.result.lock();
        (*guard).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;

    use restate_types::PlainNodeId;
    use restate_types::nodes_config::StorageState;

    use crate::providers::replicated_loglet::test_util::generate_logserver_nodes_config;

    #[test]
    fn test_with_fixed_spread_selector() -> Result<()> {
        let nodes_config = generate_logserver_nodes_config(10, StorageState::ReadWrite);
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let nodeset: NodeSet = (1..=5).collect();

        // smoke test
        let strategy = FixedSpreadSelector {
            result: Arc::new(Mutex::new(Ok(Spread::from([1, 2, 3])))),
        };
        let selector = SpreadSelector::new(
            nodeset,
            SelectorStrategy::Fixed(strategy.clone()),
            replication,
        );
        let mut rng = rand::rng();
        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::default())?;
        assert_that!(spread, eq(Spread::from([1, 2, 3])));

        // Fixed selector ignores exclude nodes as long as sufficient nodes are passed down
        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::from([1]))?;
        assert_that!(spread, eq(Spread::from([1, 2, 3])));

        // No sufficient nodes to select from if nodes config is too small or sufficient nodes are
        // excluded to make the effective nodeset too small
        //
        // only 2 nodes left in the nodeset
        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::from([1, 2, 3]));
        assert_that!(
            spread,
            err(pat!(SpreadSelectorError::InsufficientWriteableNodes))
        );

        let nodes_config = generate_logserver_nodes_config(2, StorageState::ReadWrite);
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let nodeset: NodeSet = (1..=3).collect();
        let selector = SpreadSelector::new(nodeset, SelectorStrategy::Fixed(strategy), replication);

        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::default());
        assert_that!(
            spread,
            err(pat!(SpreadSelectorError::InsufficientWriteableNodes))
        );

        Ok(())
    }

    #[test]
    fn test_flood_spread_selector() -> Result<()> {
        let nodes_config = generate_logserver_nodes_config(10, StorageState::ReadWrite);
        let replication = ReplicationProperty::new(3.try_into().unwrap());
        let nodeset: NodeSet = (1..=5).collect();

        let selector = SpreadSelector::new(nodeset, SelectorStrategy::Flood, replication);
        let mut rng = rand::rng();
        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::default())?;
        let spread = spread.to_vec();

        assert_that!(
            spread,
            unordered_elements_are![
                eq(PlainNodeId::new(1)),
                eq(PlainNodeId::new(2)),
                eq(PlainNodeId::new(3)),
                eq(PlainNodeId::new(4)),
                eq(PlainNodeId::new(5))
            ]
        );

        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::from([1, 4]))?;
        let spread = spread.to_vec();

        assert_that!(
            spread,
            unordered_elements_are![
                eq(PlainNodeId::new(2)),
                eq(PlainNodeId::new(3)),
                eq(PlainNodeId::new(5))
            ]
        );

        let spread = selector.select(&mut rng, &nodes_config, &NodeSet::from([1, 4, 2]));
        assert_that!(
            spread,
            err(pat!(SpreadSelectorError::InsufficientWriteableNodes))
        );

        Ok(())
    }
}

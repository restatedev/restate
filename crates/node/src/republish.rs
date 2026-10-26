// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Keeps this node's advertised address lists published.
//!
//! Binaries from before the lists drop them when they rewrite the nodes configuration, and a
//! metadata membership change clears them until every metadata server keeps them. The node
//! publishes the lists when it joins; this task puts them back afterwards, so readers that
//! select among them wait at most for the next check. Missing lists never stop baseline
//! operation, because readers fall back to the scalar addresses.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use restate_core::{Metadata, MetadataKind, MetadataWriter, cancellation_watcher};
use restate_metadata_store::{MetadataStoreClient, ReadModifyWriteError, ReadWriteError};
use restate_types::GenerationalNodeId;
use restate_types::metadata_store::keys::NODES_CONFIG_KEY;
use restate_types::net::address::{ControlPort, FabricPort};
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, QualifiedAddress};
use restate_types::retries::RetryPolicy;

/// The lists this node published at join, unaffected by later configuration reloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddressLists {
    addresses: Vec<QualifiedAddress<FabricPort>>,
    ctrl_addresses: Vec<QualifiedAddress<ControlPort>>,
}

impl AddressLists {
    pub(crate) fn from_node_config(config: &NodeConfig) -> Self {
        Self {
            addresses: config.addresses.clone(),
            ctrl_addresses: config.ctrl_addresses.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        self.addresses.is_empty() && self.ctrl_addresses.is_empty()
    }

    fn published_by(&self, node: &NodeConfig) -> bool {
        node.addresses == self.addresses && node.ctrl_addresses == self.ctrl_addresses
    }
}

/// What one check found.
#[derive(Debug)]
pub(crate) enum Republication {
    /// The published lists already match.
    Unchanged,
    /// The lists were written back, producing this configuration.
    Republished(NodesConfiguration),
    /// This generation no longer owns the entry: a newer generation joined or the node was
    /// removed.
    Superseded,
}

/// Compares the published lists with `desired` and writes them back when they differ. The write
/// is conditional on the version just read, never touches another generation or a removed node,
/// and never bumps the version when the lists already match.
pub(crate) async fn republish_once(
    client: &MetadataStoreClient,
    my_node_id: GenerationalNodeId,
    desired: &AddressLists,
) -> Result<Republication, ReadWriteError> {
    enum Skip {
        Unchanged,
        Superseded,
    }
    let result = client
        .read_modify_write::<NodesConfiguration, _, _>(NODES_CONFIG_KEY.clone(), |nodes_config| {
            let mut nodes_config = nodes_config.ok_or(Skip::Superseded)?;
            let node = nodes_config
                .find_node_by_id(my_node_id)
                .map_err(|_| Skip::Superseded)?;
            if desired.published_by(node) {
                return Err(Skip::Unchanged);
            }
            let mut node = node.clone();
            node.addresses.clone_from(&desired.addresses);
            node.ctrl_addresses.clone_from(&desired.ctrl_addresses);
            nodes_config.upsert_node(node);
            nodes_config.increment_version();
            Ok(nodes_config)
        })
        .await;
    match result {
        Ok(nodes_config) => Ok(Republication::Republished(nodes_config)),
        Err(ReadModifyWriteError::FailedOperation(Skip::Unchanged)) => Ok(Republication::Unchanged),
        Err(ReadModifyWriteError::FailedOperation(Skip::Superseded)) => {
            Ok(Republication::Superseded)
        }
        Err(ReadModifyWriteError::ReadWrite(err)) => Err(err),
    }
}

/// The wait before each attempt. Its jitter spreads the writes of nodes that all saw the same
/// drop, and the backoff bounds the load while the metadata store is unavailable.
fn retry_policy() -> RetryPolicy {
    RetryPolicy::exponential(
        Duration::from_secs(1),
        2.0,
        None,
        Some(Duration::from_secs(30)),
    )
}

/// Runs until shutdown, or until this generation no longer owns its entry.
pub(crate) async fn run(
    metadata_writer: MetadataWriter,
    desired: AddressLists,
) -> anyhow::Result<()> {
    if desired.is_empty() {
        return Ok(());
    }
    tokio::select! {
        _ = cancellation_watcher() => {}
        _ = keep_published(&metadata_writer, &desired) => {}
    }
    Ok(())
}

/// Every new nodes configuration is checked against the local copy first, so the metadata store
/// is only read when the lists are missing. Returns once this generation no longer owns its
/// entry.
async fn keep_published(metadata_writer: &MetadataWriter, desired: &AddressLists) {
    let metadata = Metadata::current();
    let my_node_id = metadata.my_node_id();
    let client = metadata_writer.raw_metadata_store_client();
    let mut versions = metadata.watch(MetadataKind::NodesConfiguration);
    while versions.changed().await.is_ok() {
        match metadata.nodes_config_ref().find_node_by_id(my_node_id) {
            Err(_) => return,
            Ok(node) if desired.published_by(node) => continue,
            Ok(_) => {}
        }
        let mut retries = retry_policy().into_iter();
        loop {
            tokio::time::sleep(retries.next().expect("unbounded retries")).await;
            match republish_once(client, my_node_id, desired).await {
                Ok(Republication::Republished(nodes_config)) => {
                    info!(
                        version = %nodes_config.version(),
                        "Published the advertised address lists again after another writer \
                         dropped them"
                    );
                    if metadata_writer
                        .update(Arc::new(nodes_config))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    break;
                }
                Ok(Republication::Unchanged) => break,
                Ok(Republication::Superseded) => return,
                Err(err) => {
                    warn!(%err, "Failed to publish the advertised address lists again; retrying")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use enumset::EnumSet;

    use restate_core::{TaskCenter, TaskKind, TestCoreEnvBuilder};
    use restate_types::metadata::Precondition;
    use restate_types::nodes_config::{ClusterFingerprint, Role};
    use restate_types::{PlainNodeId, RestateVersion, Version};

    use super::*;

    fn desired() -> AddressLists {
        AddressLists {
            addresses: vec![QualifiedAddress {
                address: "http://10.1.7.3:5122".parse().unwrap(),
                location: Some("us-east1.az1".parse().unwrap()),
            }],
            ctrl_addresses: vec![QualifiedAddress {
                address: "http://localhost:15122".parse().unwrap(),
                location: Some("external".parse().unwrap()),
            }],
        }
    }

    async fn stored(client: &MetadataStoreClient) -> NodesConfiguration {
        client
            .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
            .await
            .unwrap()
            .unwrap()
    }

    /// Writes `nodes_config` the way a writer from before the lists would: whole value, next
    /// version, lists gone.
    async fn write_like_an_old_version(
        client: &MetadataStoreClient,
        mut nodes_config: NodesConfiguration,
    ) {
        let current = nodes_config.version();
        nodes_config.clear_address_lists();
        nodes_config.increment_version();
        client
            .put(
                NODES_CONFIG_KEY.clone(),
                &nodes_config,
                Precondition::MatchesVersion(current),
            )
            .await
            .unwrap();
    }

    #[test_log::test(restate_core::test)]
    async fn republishes_dropped_lists_and_stays_quiet_otherwise() {
        let desired = desired();
        let my_node_id = GenerationalNodeId::new(1, 1);
        let mut nodes_config = NodesConfiguration::new(
            Version::MIN,
            "republish".to_owned(),
            ClusterFingerprint::generate(),
        );
        nodes_config.upsert_node(
            NodeConfig::builder()
                .name("node-1".to_owned())
                .current_generation(my_node_id)
                .address("unix:/run/f.sock".parse().unwrap())
                .addresses(desired.addresses.clone())
                .ctrl_addresses(desired.ctrl_addresses.clone())
                .roles(EnumSet::only(Role::Worker))
                .binary_version(RestateVersion::current())
                .build(),
        );
        let env = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes_config)
            .build()
            .await;
        let client = env.metadata_store_client.clone();

        // Matching lists: no write, no version bump.
        let before = stored(&client).await;
        assert!(matches!(
            republish_once(&client, my_node_id, &desired).await.unwrap(),
            Republication::Unchanged
        ));
        assert_eq!(stored(&client).await.version(), before.version());

        // A writer from before the lists drops them; one check restores them with one bump and
        // leaves everything else alone.
        write_like_an_old_version(&client, before.clone()).await;
        let Republication::Republished(restored) =
            republish_once(&client, my_node_id, &desired).await.unwrap()
        else {
            panic!("expected a republication");
        };
        assert_eq!(restored.version(), before.version().next().next());
        assert_eq!(stored(&client).await.version(), restored.version());
        let node = restored.find_node_by_id(my_node_id).unwrap();
        assert!(desired.published_by(node));
        let expected = NodeConfig {
            addresses: Vec::new(),
            ctrl_addresses: Vec::new(),
            ..node.clone()
        };
        assert_eq!(
            NodeConfig {
                addresses: Vec::new(),
                ctrl_addresses: Vec::new(),
                ..before.find_node_by_id(my_node_id).unwrap().clone()
            },
            expected
        );

        // The task notices a drop in the local view and restores the lists on its own.
        TaskCenter::spawn(
            TaskKind::Background,
            "republish-address-lists",
            run(env.metadata_writer.clone(), desired.clone()),
        )
        .unwrap();
        let current = stored(&client).await;
        write_like_an_old_version(&client, current.clone()).await;
        let dropped = stored(&client).await;
        env.metadata_writer
            .update(Arc::new(dropped.clone()))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let local = env.metadata.nodes_config_ref();
            if local.version() > dropped.version()
                && desired.published_by(local.find_node_by_id(my_node_id).unwrap())
            {
                break;
            }
            drop(local);
            assert!(
                tokio::time::Instant::now() < deadline,
                "lists were not republished"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(stored(&client).await.version(), dropped.version().next());

        // Another generation owns the entry, or the node is gone: nothing is written.
        let mut newer = stored(&client).await;
        let mut node = newer.find_node_by_id(my_node_id).unwrap().clone();
        node.current_generation = GenerationalNodeId::new(1, 2);
        node.addresses.clear();
        newer.upsert_node(node);
        let current = newer.version();
        newer.increment_version();
        client
            .put(
                NODES_CONFIG_KEY.clone(),
                &newer,
                Precondition::MatchesVersion(current),
            )
            .await
            .unwrap();
        assert!(matches!(
            republish_once(&client, my_node_id, &desired).await.unwrap(),
            Republication::Superseded
        ));
        let mut removed = newer.clone();
        removed.remove_node_unchecked(PlainNodeId::new(1));
        removed.increment_version();
        client
            .put(
                NODES_CONFIG_KEY.clone(),
                &removed,
                Precondition::MatchesVersion(newer.version()),
            )
            .await
            .unwrap();
        assert!(matches!(
            republish_once(&client, my_node_id, &desired).await.unwrap(),
            Republication::Superseded
        ));
        assert_eq!(stored(&client).await.version(), removed.version());
    }
}

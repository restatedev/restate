// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use ahash::HashSet;
use arc_swap::ArcSwap;
use bytestring::ByteString;
use futures::future::OptionFuture;
use itertools::Itertools;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use tracing::{debug, error, trace, warn};

use restate_types::config::Configuration;
use restate_types::live::Live;
use restate_types::metadata::GlobalMetadata;
use restate_types::net::metadata::{GetMetadataRequest, MetadataMessage};
use restate_types::retries::{RetryPolicy, with_jitter};
use restate_types::{GenerationalNodeId, Version};

use crate::metadata_store::{MetadataStoreClient, ReadError, retry_on_retryable_error};
use crate::network::UnboundedConnectionRef;
use crate::{ShutdownError, TaskCenter, TaskHandle, TaskKind, cancellation_watcher};

use super::VersionInformation;

/// Updates a global metadata item in the background
///
/// ## Design Details
/// A single exclusive task per global metadata that manages all its updates. It's
/// always alive and will attempt to fetch initial metadata from the metadata store.
///
/// - The task monitors the observations made by the message fabric (network) layer
///   to determine when and how to update this metadata item.
/// - It also manages the in-flight tasks that fetch metadata from peers and fetches
///   from the metadata store.
/// - Puts an upper bound on staleness by enforcing a metadata read from metadata
///   store after being idle for a configurable amount of time.
pub struct GlobalMetadataUpdateTask<T> {
    config: Live<Configuration>,
    // we own it, we update it.
    item: Arc<ArcSwap<T>>,
    metadata_store_client: MetadataStoreClient,
    /// The last time received metadata from any source
    last_update: Instant,
    /// The time we last fetched metadata from metadata store
    last_metadata_store_fetch: Instant,
    /// external metadata writes/updates come through this channel.
    /// often used to push updates coming from peers (network) or manually
    /// acquired metadata changes.
    writes_rx: mpsc::UnboundedReceiver<Command<T>>,
    /// Notifier to those who are waiting for version updates
    write_watch: watch::Sender<Version>,
    /// The next tick to consider fetching metadata
    next_fetch_interval: Interval,

    // which version did we request from the last wave of asking peers?
    last_version_attempted_from_peers: Version,
    // which peers did we ask already?
    peers_attempted_for_this_version: HashSet<GenerationalNodeId>,
    in_flight_peer_requests: JoinSet<Version>,
    _marker: std::marker::PhantomData<T>,
}

pub enum Command<T> {
    /// Push a new value metadata item.
    ///
    /// The item will be ignored if existing metadata is newer, the callback will be notified with the
    /// latest known version in any case.
    Update {
        value: Arc<T>,
        callback: Option<oneshot::Sender<Version>>,
    },
}

impl<T: GlobalMetadata> GlobalMetadataUpdateTask<T> {
    pub fn start(
        metadata_store_client: MetadataStoreClient,
        item: Arc<ArcSwap<T>>,
        write_watch: watch::Sender<Version>,
        observations: &watch::Sender<VersionInformation>,
    ) -> Result<(mpsc::UnboundedSender<Command<T>>, TaskHandle<()>), ShutdownError> {
        let mut observer = observations.subscribe();
        observer.mark_changed();

        let (tx, writes_rx) = mpsc::unbounded_channel();

        // we have a path to reset this value dynamically based on the next known duration, but I'm
        // not sure if it's worth the effort.
        let mut next_fetch_interval =
            tokio::time::interval(with_jitter(Duration::from_millis(100), 0.5));
        next_fetch_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let task = Self {
            config: Configuration::updateable(),
            item,
            metadata_store_client,
            last_update: Instant::now(),
            last_metadata_store_fetch: Instant::now(),
            writes_rx,
            write_watch,
            next_fetch_interval,
            last_version_attempted_from_peers: Version::INVALID,
            peers_attempted_for_this_version: HashSet::default(),
            in_flight_peer_requests: JoinSet::new(),
            _marker: std::marker::PhantomData,
        };

        let handle = TaskCenter::spawn_unmanaged(
            TaskKind::MetadataBackgroundSync,
            "metadata-update-{}",
            task.run(observer),
        )?;
        Ok((tx, handle))
    }

    async fn run(mut self, mut observer: watch::Receiver<VersionInformation>) {
        let mut cancel = std::pin::pin!(cancellation_watcher());
        let mut in_flight_metadata_store_fetch = None;
        loop {
            tokio::select! {
                _ = &mut cancel => {
                    debug!(kind = %T::KIND, "Global metadata update task has stopped");
                    break;
                }
                Ok(_) = observer.changed() => {
                    self.handle_observation(&observer.borrow_and_update());
                }
                Some(_) = self.in_flight_peer_requests.join_next() => {
                    // reserved for future use.
                }
                Some(res) = OptionFuture::from(in_flight_metadata_store_fetch.as_mut()) => {
                    match res {
                        Ok(value) => {
                            let version = (&value as &Arc<T>).version();
                            debug!(kind = %T::KIND, %version, "Received metadata from metadata store");
                            self.update_internal(value);
                            self.last_metadata_store_fetch = Instant::now();
                        }
                        Err(e) => {
                            // task panicked, this shouldn't happen! might retry in the next tick
                            error!("fetching updates from metadata store panicked: {}", e);
                        }
                    }
                    // must be done to avoid polling the future after completion
                    in_flight_metadata_store_fetch = None;
                }
                Some(update) = self.writes_rx.recv() => {
                    self.handle_external_update(update);
                }
                _ = self.next_fetch_interval.tick() => {
                    let latest_observed = observer.borrow();
                    self.tick(&mut in_flight_metadata_store_fetch, &latest_observed);
                }
            }
        }
    }

    fn tick(
        &mut self,
        in_flight_metadata_store_fetch: &mut Option<TaskHandle<Arc<T>>>,
        latest_observed: &VersionInformation,
    ) {
        let config = self.config.live_load();
        let metadata_store_idle_dur: Duration =
            with_jitter(config.common.metadata_update_interval.into(), 0.3);
        // Attempt to read from metadata store if we couldn't get a value from peers within this
        // duration.
        let fallback_to_metadata_dur: Duration =
            with_jitter(config.common.metadata_fetch_from_peer_timeout.into(), 0.3);
        let current_version = self.item.load().version();

        // ## Fetching metadata from peers
        //
        // A) Reasons to fetch metadata from peers:
        // - we observed a new version
        // - we have peers that we have not attempted to fetch from
        // B) Reasons to **not** fetch from peers:
        // - we have an in-flight request for all peers that know about this version
        // - we know a new version but we don't have any peers to fetch from

        let is_new_version_anticipated = latest_observed.version > current_version;
        // we have a new version but we don't have any peers to fetch from
        let is_new_version_no_peers =
            is_new_version_anticipated && latest_observed.peers.is_empty();

        if is_new_version_anticipated {
            if latest_observed.version > self.last_version_attempted_from_peers {
                // we have a new version and we have not attempted to fetch from peers yet
                // warning: quick succession of metadata updates might cause us to ask the same
                // peer many times.
                // todo: consider throttling requests to on a per-peer basis.
                self.peers_attempted_for_this_version.clear();
                self.last_version_attempted_from_peers = latest_observed.version;
            }

            // We keep track of the peers we have already sent requests for a given version.
            for (node_id, connection) in latest_observed.peers.iter() {
                if self.peers_attempted_for_this_version.insert(*node_id) {
                    // we have not attempted to fetch from this peer yet
                    let version = latest_observed.version;
                    trace!(kind = %T::KIND, %node_id, %version, "Fetching metadata from peer");
                    let connection = connection.clone();
                    self.in_flight_peer_requests
                        .spawn(update_from_peer::<T>(connection, version));
                    // one at a time, in the next tick, we may ask another peer.
                    break;
                }
            }
        } else {
            self.peers_attempted_for_this_version.clear();
        }

        // ## Fetching metadata from metadata store
        //
        // A) Reasons to fetch from metadata store:
        // - observed a new version but we don't have known peers to fetch from
        // - idle, last time we fetched metadata update exceeds a threshold
        // - observed a new version and time since first observation exceeds a
        // threshold
        // B) Reasons to **not** fetch from metadata store:
        // - if we already have an in-flight fetch attempt
        // - we have attempted a read from metadata store recently (cool-off period)
        if in_flight_metadata_store_fetch.is_some() {
            return;
        }

        // It's been a while since we acquired a new version and we haven't fetched from metadata
        // store.
        let last_update = self.last_update.max(self.last_metadata_store_fetch);
        // We have not seen any update for a while
        let is_idle = last_update.elapsed() >= metadata_store_idle_dur;

        // We know about a new version and we have been waiting to fetch it for too long and we
        // don't have an active metadata store fetch in flight.
        let new_version_waiting_for_too_long =
            is_new_version_anticipated && (latest_observed.elapsed() >= fallback_to_metadata_dur);

        // We are at invalid version, this means we have just started and/or we have not
        // provisioned yet, let's schedule a sync and keep it running.
        let is_first_run = current_version == Version::INVALID;

        if is_first_run || is_idle || new_version_waiting_for_too_long || is_new_version_no_peers {
            let reasons = [
                is_first_run.then_some("first-start"),
                is_idle.then_some("idle"),
                new_version_waiting_for_too_long.then_some("new-version-peer-wait-timeout"),
                is_new_version_no_peers.then_some("new-version-no-peers"),
            ];
            // Report the combined reason of the fetch as a string in the log line
            let reason = reasons.iter().flatten().join("|");
            let client = self.metadata_store_client.clone();
            if let Ok(handle) = TaskCenter::spawn_unmanaged(
                TaskKind::MetadataBackgroundSync,
                format!("{}-metadata-store-get", T::KIND),
                update_from_metadata_store(client),
            ) {
                debug!(kind = %T::KIND,
                        last_update = ?self.last_update.elapsed(),
                        last_metadata_store_fetch = ?self.last_metadata_store_fetch.elapsed(),
                        %current_version,
                        %reason,
                        "Fetching metadata from metadata store");
                *in_flight_metadata_store_fetch = Some(handle);
            }
        }
    }

    fn handle_observation(&mut self, info: &VersionInformation) {
        // How do we react when observing a possible new version?
        // - For a new version, we'll attempt to fetch it:
        //   - Fetch from a peer if we have a list of peers
        //   - Fetch from metadata store if we don't have peers
        //   - Fetch from metadata store concurrently with fetching from peers if we have been
        //   trying to fetch from peers for too long.
        //
        // How to deal with failing fetches?
        // - As long as observed version is > than current version and we have a list of peers to
        // choose from, we shouldn't give up.
        let current_version = self.item.load().version();
        match info.version.cmp(&current_version) {
            std::cmp::Ordering::Greater => {
                trace!(
                    kind = %T::KIND,
                    current_version = %self.item.load().version(),
                    peer_version = %info.version,
                    "Observed new metadata version, peers: {:?}",
                    info.peers.values().map(|p| p.peer()).collect::<Vec<_>>()
                );
                // attempt to fetch asap
                self.next_fetch_interval.reset_immediately();
            }
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                // being notified about an old version, ignore
            }
        }
    }

    fn handle_external_update(&mut self, update: Command<T>) {
        match update {
            Command::Update { value, callback } => {
                let version = self.update_internal(value);
                if let Some(callback) = callback {
                    let _ = callback.send(version);
                }
            }
        }
    }

    fn update_internal(&mut self, new_value: Arc<T>) -> Version {
        let current_value = self.item.load();
        let mut maybe_new_version = new_value.version();

        if new_value.version() > current_value.version() {
            trace!(
                kind = %T::KIND,
                "Updating from {} to {}",
                current_value.version(),
                new_value.version(),
            );
            self.item.store(new_value);
            self.write_watch.send_replace(maybe_new_version);
            self.last_update = Instant::now();
        } else {
            /* Do nothing, current is already newer */
            maybe_new_version = current_value.version();
        }

        if maybe_new_version >= self.last_version_attempted_from_peers {
            // we have successfully updated to a new version, we can clear the list of peers to
            // prepare for future fetch attempts for next version(s)
            self.peers_attempted_for_this_version.clear();
        }

        maybe_new_version
    }
}

async fn update_from_metadata_store<T: GlobalMetadata>(client: MetadataStoreClient) -> Arc<T> {
    // When fetching from metadata store, we don't give up until we get a value. If the value is
    // not set yet, we continue retrying until we observe a value.
    //
    // This means that this future will not resolve unless the cluster is provisioned.
    // We use reasonable backoff policy without max attempts that saturates at 1s.
    let retry = RetryPolicy::exponential(
        Duration::from_millis(100),
        1.5,
        None,
        Some(Duration::from_secs(1)),
    );

    loop {
        // relentlessly retry, exponential backoff is applied
        match retry_on_retryable_error(retry.clone(), || async {
            match client.get::<T>(ByteString::from_static(T::KEY)).await {
                Ok(Some(value)) => Ok(Arc::new(value)),
                Ok(None) => Err(ReadError::retryable(EmptyValue)),
                Err(err) => Err(err),
            }
        })
        .await
        .map_err(|e| e.into_inner())
        {
            Ok(value) => return value,
            Err(e) => {
                warn!(
                    "Failed to fetch metadata from metadata store. This warning should not be printed. If it did, please report to restate developers: {}",
                    e
                );
            }
        }
    }
}

// why is this async? because in future networking, we'll be able to wait for the response, so we
// want to model this request as if it's a RPC.
async fn update_from_peer<T: GlobalMetadata>(
    connection: UnboundedConnectionRef,
    version: Version,
) -> Version {
    let _ = connection.encode_and_send(MetadataMessage::GetMetadataRequest(GetMetadataRequest {
        metadata_kind: T::KIND,
        min_version: Some(version),
    }));

    version
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("metadata store does not have a value for this key yet")]
struct EmptyValue;

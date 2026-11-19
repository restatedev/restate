// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use restate_core::ShutdownError;
use restate_limiter::rule_book::RuleBookObserver;
use restate_partition_store::ReadSnapshot;
use restate_storage_query_api::QueryMetadata;
use restate_types::sharding::KeyRange;
use restate_types::{cluster::cluster_state::PartitionProcessorStatus, identifiers::PartitionId};

pub use restate_types::net::distributed_query::PartitionReadError;

use crate::{
    CaptureSpan, CapturedPartitionRead, PartitionCaptureRequirements, PartitionQueryAccess,
};

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    GetState(oneshot::Sender<BTreeMap<PartitionId, PartitionProcessorStatus>>),
    ReadPartition(PartitionReadRequest),
}

/// A query's request to capture one partition. The partition processor manager
/// routes it to the partition's processor without waiting, and the processor
/// admits it between two committed turns. The query's metadata travels with it
/// unchanged.
#[derive(Debug)]
pub struct PartitionReadRequest {
    pub partition: PartitionId,
    /// Admit only on the partition's leader. Live state always requires it.
    pub require_leader: bool,
    pub metadata: Arc<QueryMetadata>,
    pub capture: PartitionCaptureRequirements,
    pub response: oneshot::Sender<Result<CapturedPartitionRead, PartitionReadError>>,
}

#[derive(Clone)]
pub struct ProcessorsManagerHandle {
    sender: mpsc::Sender<ProcessorsManagerCommand>,
    rule_book_observer: Arc<dyn RuleBookObserver>,
    query_access: Arc<dyn PartitionQueryAccess>,
}

impl ProcessorsManagerHandle {
    /// A storage snapshot of `partition`, admitted by its processor; see
    /// [`Self::capture_partition`].
    pub async fn read_partition(
        &self,
        partition: PartitionId,
        require_leader: bool,
        metadata: Arc<QueryMetadata>,
    ) -> Result<ReadSnapshot, PartitionReadError> {
        let read = self
            .capture_partition(
                partition,
                require_leader,
                metadata,
                PartitionCaptureRequirements {
                    storage: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(read.storage.expect("storage snapshot was requested"))
    }

    /// Captures `partition` through its processor: the storage snapshot and live
    /// state `capture` asks for, admitted between two committed turns. The
    /// invoker's rows are read right after admission, outside the processor (see
    /// below). Fails if this node doesn't host the partition, its processor isn't
    /// ready or is stopping, its admission queue is full, or the read needs the
    /// leader and this processor doesn't lead.
    pub async fn capture_partition(
        &self,
        partition: PartitionId,
        require_leader: bool,
        metadata: Arc<QueryMetadata>,
        capture: PartitionCaptureRequirements,
    ) -> Result<CapturedPartitionRead, PartitionReadError> {
        let (response, rx) = oneshot::channel();
        self.sender
            .send(ProcessorsManagerCommand::ReadPartition(
                PartitionReadRequest {
                    partition,
                    require_leader: require_leader || capture.needs_live_state(),
                    metadata,
                    capture,
                    response,
                },
            ))
            .await
            .map_err(|_| PartitionReadError::ManagerClosed(partition))?;
        let mut read = rx
            .await
            .map_err(|_| PartitionReadError::ResponseDropped(partition))??;
        if capture.invoker {
            // TODO: capture the invoker's rows at admission, with the snapshot. The
            // invoker keeps its status inside its own task and answers status
            // requests only between steps, and a step can wait to send effects to
            // the partition processor (a bounded channel the processor's loop
            // drains). So the processor can't wait for the status without risking
            // a stall, and the rows are read here instead, after admission. Ideally
            // the invoker would publish its status through shared access, for
            // example behind a lock, that admission reads directly.
            let start = Instant::now();
            let scan = self
                .query_access
                .scan_invoker_status(partition, KeyRange::FULL)
                .try_collect::<Vec<_>>();
            // A stopping processor cancels the snapshots it admitted: don't keep
            // holding one while waiting for the invoker. A read without a snapshot
            // ends when the stopping leader stops its invoker, which answers
            // pending status requests as unavailable.
            let rows = match read.storage.as_ref().map(ReadSnapshot::cancellation_token) {
                Some(cancel) => cancel
                    .run_until_cancelled(scan)
                    .await
                    .ok_or(PartitionReadError::Stopping(partition))?,
                None => scan.await,
            }
            .map_err(|_| PartitionReadError::LiveCaptureUnavailable(partition))?;
            read.live.invoker = Some(rows.into());
            read.timing.invoker = Some(CaptureSpan::since(start));
        }
        Ok(read)
    }

    pub fn new(
        sender: mpsc::Sender<ProcessorsManagerCommand>,
        rule_book_observer: Arc<dyn RuleBookObserver>,
        query_access: Arc<dyn PartitionQueryAccess>,
    ) -> Self {
        Self {
            sender,
            rule_book_observer,
            query_access,
        }
    }

    pub fn rule_book_observer(&self) -> Arc<dyn RuleBookObserver> {
        self.rule_book_observer.clone()
    }

    pub fn query_access(&self) -> Arc<dyn PartitionQueryAccess> {
        Arc::clone(&self.query_access)
    }

    pub async fn get_state(
        &self,
    ) -> Result<BTreeMap<PartitionId, PartitionProcessorStatus>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ProcessorsManagerCommand::GetState(tx))
            .await
            .map_err(|_| ShutdownError)?;
        rx.await.map_err(|_| ShutdownError)
    }
}

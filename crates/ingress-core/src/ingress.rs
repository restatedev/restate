// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::HashMap, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use restate_core::{
    network::{Networking, TransportConnect},
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::{PartitionId, PartitionKey},
    live::Live,
    net::ingress::IngestRecord,
    partitions::{FindPartition, PartitionTable, PartitionTableError},
};

use crate::{
    RecordCommit, SessionOptions,
    session::{SessionHandle, SessionManager},
};

/// Errors that can be observed when interacting with the ingress facade.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Ingress closed")]
    Closed,
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
}

/// High-level ingress entry point that allocates permits and hands out session handles per partition.
#[derive(Clone)]
pub struct Ingress<T> {
    manager: SessionManager<T>,
    partition_table: Live<PartitionTable>,
    // budget for inflight invocations.
    // this should be a memory budget but it's
    // not possible atm to compute the serialization
    // size of an invocation.
    permits: Arc<Semaphore>,

    // session handles cache just to avoid
    // cloning the handle on each ingest request
    handles: HashMap<PartitionId, SessionHandle>,
}

impl<T> Ingress<T> {
    /// Builds a new ingress facade with the provided networking stack, partition metadata, and memory
    /// budget for inflight records.
    pub fn new(
        networking: Networking<T>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        budget: usize,
        opts: Option<SessionOptions>,
    ) -> Self {
        Self {
            manager: SessionManager::new(networking, partition_routing, opts),
            partition_table,
            permits: Arc::new(Semaphore::new(budget)),
            handles: HashMap::default(),
        }
    }
}

impl<T> Ingress<T>
where
    T: TransportConnect,
{
    /// Reserves an inflight slot and ties it to an [`IngressPermit`] that can ingest exactly one record.
    pub async fn reserve(&mut self) -> Result<IngressPermit<'_, T>, IngestionError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IngestionError::Closed)?;

        Ok(IngressPermit {
            permit,
            ingress: self,
        })
    }

    /// Once closed, calls to ingest will return [`IngestionError::Closed`].
    /// Inflight records might still get committed.
    pub fn close(&self) {
        self.permits.close();
        self.manager.close();
    }
}

/// Permit that owns capacity for a single record ingest against an [`Ingress`] instance.
pub struct IngressPermit<'a, T> {
    permit: OwnedSemaphorePermit,
    ingress: &'a mut Ingress<T>,
}

impl<'a, T> IngressPermit<'a, T>
where
    T: TransportConnect,
{
    /// Sends a record to the partition derived from the supplied [`PartitionKey`], consuming the permit.
    pub fn ingest(
        self,
        partition_key: PartitionKey,
        record: impl Into<IngestRecord>,
    ) -> Result<RecordCommit, IngestionError> {
        let partition_id = self
            .ingress
            .partition_table
            .pinned()
            .find_partition_id(partition_key)?;

        let handle = self
            .ingress
            .handles
            .entry(partition_id)
            .or_insert_with(|| self.ingress.manager.get(partition_id));

        handle
            .ingest(self.permit, record.into())
            .map_err(|_| IngestionError::Closed)
    }
}

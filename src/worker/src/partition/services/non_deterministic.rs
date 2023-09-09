// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::storage::PartitionStorage;
use async_trait::async_trait;
use bytes::Bytes;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{FullInvocationId, ServiceId};
use tokio::sync::mpsc;
use tracing::info;

pub(crate) fn is_built_in_service(service_name: &str) -> bool {
    // TODO how to deal with changing sets of built in services? Should we just say dev.restate is always a built-in service?
    service_name == restate_pb::INGRESS_SERVICE_NAME
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StateAccessError {
    #[error("state access error")]
    Error,
}

#[async_trait]
pub(crate) trait StateAccess {
    async fn load_state(service_id: &ServiceId, key: &str) -> Option<Bytes>;

    async fn store_state(
        service_id: &ServiceId,
        key: &str,
        value: Bytes,
    ) -> Result<(), StateAccessError>;
}

#[derive(Debug)]
pub(crate) enum Effects {
    SetState,
    ClearState,
    OutboxMessage,
    RegisterTimer,
    End,
}

// TODO Replace with bounded channels but this requires support for spilling on the sender side
pub(crate) type OutputSender = mpsc::UnboundedSender<Effects>;
pub(crate) type OutputReceiver = mpsc::UnboundedReceiver<Effects>;

pub(crate) struct ServiceInvoker<'a> {
    storage: &'a PartitionStorage<RocksDBStorage>,

    output_tx: OutputSender,
}

impl<'a> ServiceInvoker<'a> {
    pub(crate) fn new(storage: &'a PartitionStorage<RocksDBStorage>) -> (Self, OutputReceiver) {
        let (output_tx, output_rx) = mpsc::unbounded_channel();

        (ServiceInvoker { storage, output_tx }, output_rx)
    }

    pub(crate) async fn invoke(&self, full_invocation_id: FullInvocationId, argument: Bytes) {
        info!("Running nbis invoker for {full_invocation_id:?} with argument {argument:?}");

        // the receiver channel should only be shut down if the system is shutting down
        let _ = self.output_tx.send(Effects::End);
    }
}

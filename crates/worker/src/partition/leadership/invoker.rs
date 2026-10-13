// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};

use restate_core::{Metadata, RuntimeTaskHandle, TaskCenter, TaskKind};
use restate_invoker_impl::{BuildError, InvokerHandle, Service};
use restate_partition_store::PartitionStore;
use restate_types::config::{Configuration, ServiceClientOptions};
use restate_types::identifiers::PartitionId;
use restate_types::live::LiveLoadExt;
use restate_types::sharding::KeyRange;
use restate_util_string::format_restring;
use restate_worker_api::invoker::capacity::TokenBucket;

use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::types::InvokerEffect;
use crate::partition_processor_manager::PartitionLeaderHandlesRegistry;

use super::Error;

/// Owns the invoker's dedicated runtime. Dropping it also cancels the invoker if
/// leadership initialization fails or the partition processor exits abnormally.
pub(super) struct InvokerRuntime(RuntimeTaskHandle<Result<(), BuildError>>);

impl InvokerRuntime {
    pub async fn start(
        partition_id: PartitionId,
        key_range: KeyRange,
        partition_store: PartitionStore,
        sender: mpsc::Sender<InvokerEffect>,
        service_client_options: &ServiceClientOptions,
        action_token_bucket: Option<TokenBucket>,
        registry: PartitionLeaderHandlesRegistry,
    ) -> Result<(InvokerHandle, Self), Error> {
        let schema = Metadata::with_current(|m| m.updateable_schema());
        let options = service_client_options.clone();
        let (handle_tx, handle_rx) = oneshot::channel();

        let mut runtime = Self(
            TaskCenter::current()
                .start_runtime(
                    TaskKind::SystemService,
                    format_restring!("invoker-{partition_id}"),
                    Some(partition_id),
                    move || async move {
                        // Construct the service here as well: its HTTP clients spawn
                        // background tasks and create runtime-bound resources.
                        let invoker = Service::from_options(
                            partition_id,
                            key_range,
                            InvokerStorageReader::new(partition_store),
                            sender,
                            &options,
                            schema,
                            action_token_bucket,
                        )?;
                        let _status_guard = registry.register_invoker_status(
                            partition_id,
                            key_range,
                            invoker.status_reader(),
                        );

                        // If startup was cancelled, don't start serving invocations.
                        if handle_tx.send(invoker.handle()).is_ok() {
                            invoker
                                .run(Configuration::live().map(|c| &c.worker.invoker))
                                .await;
                        }
                        Ok(())
                    },
                )
                .map_err(|err| Error::task_failed("invoker", err))?,
        );

        match handle_rx.await {
            Ok(handle) => Ok((handle, runtime)),
            Err(_) => {
                // Join the runtime before reporting construction failure so its
                // resources and registered name have already been released.
                (&mut runtime).await?;
                Err(Error::task_terminated_unexpectedly("invoker"))
            }
        }
    }

    pub fn cancel(&self) {
        self.0.cancel();
    }
}

impl Future for InvokerRuntime {
    type Output = Result<(), BuildError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

impl Drop for InvokerRuntime {
    fn drop(&mut self) {
        self.cancel();
    }
}

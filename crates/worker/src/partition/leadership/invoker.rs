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
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt;
use tokio::sync::{mpsc, oneshot};

use restate_core::{Metadata, RuntimeTaskHandle, TaskCenter, TaskKind};
use restate_invoker_impl::{InvokerHandle, Service};
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
pub(super) struct InvokerRuntime(RuntimeTaskHandle<Result<(), Error>>);

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

        let mut runtime = Self::spawn(partition_id, move || async move {
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
            let _status_guard =
                registry.register_invoker_status(partition_id, key_range, invoker.status_reader());

            // If startup was cancelled, don't start serving invocations.
            if handle_tx.send(invoker.handle()).is_ok() {
                invoker
                    .run(Configuration::live().map(|c| &c.worker.invoker))
                    .await;
            }
            Ok(())
        })?;

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

    fn spawn<F>(
        partition_id: PartitionId,
        run: impl FnOnce() -> F + Send + 'static,
    ) -> Result<Self, Error>
    where
        F: Future<Output = Result<(), Error>> + 'static,
    {
        TaskCenter::current()
            .start_runtime(
                TaskKind::SystemService,
                format_restring!("invoker-{partition_id}"),
                Some(partition_id),
                move || async move {
                    // Contain panics before they reach the runtime root, so TaskCenter
                    // can drain background tasks and unregister the runtime normally.
                    // No invoker state is reused after unwinding.
                    AssertUnwindSafe(async move { run().await })
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|payload| {
                            let message = payload
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| payload.downcast_ref::<&str>().copied())
                                .unwrap_or("non-string panic payload");
                            Err(Error::task_failed(
                                "invoker",
                                anyhow::anyhow!("invoker panicked: {message}"),
                            ))
                        })
                },
            )
            .map(Self)
            .map_err(|err| Error::task_failed("invoker", err))
    }
}

impl Future for InvokerRuntime {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

impl Drop for InvokerRuntime {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Ready, pending};

    use tokio::sync::oneshot;

    use restate_core::{TaskCenterBuilder, TaskCenterFutureExt};
    use restate_types::identifiers::PartitionId;

    use super::{Error, InvokerRuntime};

    #[tokio::test]
    async fn panic_is_reported_and_runtime_can_restart() {
        // The task-center test macro exits the process on any panic, including
        // deliberately caught ones, so install the task-center context manually.
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .unwrap()
            .into_handle();
        async {
            let partition_id = PartitionId::MIN;

            // Catch construction panics as well as panics while polling the invoker.
            let runtime = InvokerRuntime::spawn(partition_id, || -> Ready<Result<(), Error>> {
                panic!("construction failed");
            })
            .unwrap();
            let error = runtime.await.unwrap_err();
            assert!(matches!(error, Error::TaskFailed { .. }));
            assert!(error.to_string().contains("construction failed"));

            let (dropped_tx, dropped_rx) = oneshot::channel::<()>();
            let runtime = InvokerRuntime::spawn(partition_id, move || async move {
                let (started_tx, started_rx) = oneshot::channel();
                tokio::spawn(async move {
                    let _dropped_tx = dropped_tx;
                    started_tx.send(()).unwrap();
                    pending::<()>().await;
                });
                started_rx.await.unwrap();
                std::panic::panic_any(String::from("invocation failed"));
            })
            .unwrap();
            let error = runtime.await.unwrap_err();
            assert!(matches!(error, Error::TaskFailed { .. }));
            assert!(error.to_string().contains("invocation failed"));
            // Joining the runtime must also drop its outstanding background tasks.
            assert!(dropped_rx.await.is_err());

            InvokerRuntime::spawn(partition_id, || async { Ok(()) })
                .unwrap()
                .await
                .unwrap();
        }
        .in_tc(&tc)
        .await;
    }
}

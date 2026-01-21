// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin;

use bytestring::ByteString;
use restate_types::Version;
use tokio::sync::oneshot::Sender;
use tracing::{self, instrument};
use tracing::{trace, warn};

use restate_core::{ShutdownError, cancellation_watcher};
use restate_metadata_store::{ProvisionedMetadataStore, ReadError, WriteError};
use restate_types::metadata::{Precondition, VersionedValue};

use super::optimistic_store::OptimisticLockingMetadataStoreBuilder;

#[derive(Debug, strum::Display)]
pub(crate) enum Commands {
    Get {
        key: ByteString,
        tx: Sender<Result<Option<VersionedValue>, ReadError>>,
    },
    GetVersion {
        key: ByteString,
        tx: Sender<Result<Option<Version>, ReadError>>,
    },
    Put {
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
        tx: Sender<Result<(), WriteError>>,
    },
    Delete {
        key: ByteString,
        precondition: Precondition,
        tx: Sender<Result<(), WriteError>>,
    },
}

pub(crate) struct Server {
    receiver: tokio::sync::mpsc::UnboundedReceiver<Commands>,
    builder: OptimisticLockingMetadataStoreBuilder,
}

impl Server {
    pub(crate) fn new(
        builder: OptimisticLockingMetadataStoreBuilder,
        receiver: tokio::sync::mpsc::UnboundedReceiver<Commands>,
    ) -> Self {
        Self { builder, receiver }
    }

    pub(crate) async fn run(self) -> anyhow::Result<()> {
        let Server {
            mut receiver,
            builder,
        } = self;

        let mut shutdown = pin::pin!(cancellation_watcher());

        let mut delegate = match builder.build().await {
            Ok(delegate) => delegate,
            Err(err) => {
                warn!(error = ?err, "error while loading latest metastore version.");
                return Err(err);
            }
        };

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                            // stop accepting messages
                            return Ok(());
                }
                Some(cmd) = receiver.recv() => {
                    trace!("server delegating {}", cmd);
                    match cmd {
                        Commands::Get{key, tx} => {
                            let res = delegate.get(key).await;
                            let _ = tx.send(res);
                        }
                        Commands::GetVersion{key, tx} => {
                            let _ = tx.send(delegate.get_version(key).await);
                        }
                        Commands::Put{key, value, precondition, tx} => {
                            let _ = tx.send(delegate.put(key, value, precondition).await);
                        }
                        Commands::Delete{key, precondition, tx} => {
                            let _ = tx.send(delegate.delete(key, precondition).await);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    sender: tokio::sync::mpsc::UnboundedSender<Commands>,
}

impl Client {
    pub fn new(sender: tokio::sync::mpsc::UnboundedSender<Commands>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl ProvisionedMetadataStore for Client {
    #[instrument(level = "trace", skip_all, err(level = "trace"))]
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        trace!(%key, "client sending Get");

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Get { key, tx })
            .map_err(|_| ReadError::terminal(ShutdownError))?;

        rx.await.map_err(|_| ReadError::terminal(ShutdownError))?
    }

    #[instrument(level = "trace", skip_all, err(level = "trace"))]
    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        trace!(%key, "client sending GetVersion");

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::GetVersion { key, tx })
            .map_err(|_| ReadError::terminal(ShutdownError))?;

        rx.await.map_err(|_| ReadError::terminal(ShutdownError))?
    }

    #[instrument(level = "trace", skip_all, err(level = "trace"))]
    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        trace!(%key, "client sending Put");

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Put {
                key,
                value,
                precondition,
                tx,
            })
            .map_err(|_| WriteError::terminal(ShutdownError))?;

        rx.await.map_err(|_| WriteError::terminal(ShutdownError))?
    }

    #[instrument(level = "trace", skip_all, err(level = "trace"))]
    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        trace!(%key, %precondition, "client sending Delete");

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Delete {
                key,
                precondition,
                tx,
            })
            .map_err(|_| WriteError::terminal(ShutdownError))?;

        rx.await.map_err(|_| WriteError::terminal(ShutdownError))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objstore::in_memory_version_repository::InMemoryVersionRepository;
    use bytes::Bytes;
    use bytestring::ByteString;
    use restate_types::Version;
    use restate_types::config::MetadataClientKind;
    use restate_types::metadata::VersionedValue;
    use tokio::sync::mpsc;

    async fn setup_test_metadata_store() -> (Client, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let version_repository = Box::new(InMemoryVersionRepository::new());

        let builder = OptimisticLockingMetadataStoreBuilder {
            version_repository,
            configuration: MetadataClientKind::ObjectStore {
                path: "s3://test-bucket/test-prefix".to_string(),
                object_store: Default::default(),
                object_store_retry_policy: Default::default(),
            },
        };

        let (tx, rx) = mpsc::unbounded_channel();
        let server = Server::new(builder, rx);
        let server_handle = tokio::spawn(server.run());

        let client = Client::new(tx);
        (client, server_handle)
    }

    #[test_log::test(tokio::test)]
    async fn test_client_get() {
        let (client, _server_handle) = setup_test_metadata_store().await;
        let key = ByteString::from_static("test_key");

        let result = client.get(key).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_client_get_version() {
        let (client, _server_handle) = setup_test_metadata_store().await;
        let key = ByteString::from_static("test_key");

        let result = client.get_version(key).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_client_put() {
        let (client, _server_handle) = setup_test_metadata_store().await;
        let key = ByteString::from_static("test_key");
        let value = VersionedValue::new(Version::MIN, Bytes::from_static(b"test_value"));
        let precondition = Precondition::None;

        let result = client.put(key.clone(), value, precondition).await;

        assert!(result.is_ok());

        let get_result = client.get(key).await;
        assert!(get_result.is_ok());
        let retrieved_value = get_result.unwrap().unwrap();
        assert_eq!(retrieved_value.value, Bytes::from_static(b"test_value"));
        assert_eq!(retrieved_value.version, Version::MIN);
    }

    #[test_log::test(tokio::test)]
    async fn test_client_delete() {
        let (client, _server_handle) = setup_test_metadata_store().await;
        let key = ByteString::from_static("test_key");
        let value = VersionedValue::new(Version::MIN, Bytes::from_static(b"test_value"));

        let put_result = client.put(key.clone(), value, Precondition::None).await;
        assert!(put_result.is_ok());

        let get_result = client.get(key.clone()).await;
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_some());

        let delete_result = client.delete(key.clone(), Precondition::None).await;
        assert!(delete_result.is_ok());

        let get_result = client.get(key).await;
        assert!(get_result.is_ok());
        assert!(get_result.unwrap().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_full_stack_with_preconditions() {
        let (client, _server_handle) = setup_test_metadata_store().await;
        let key = ByteString::from_static("test_key");
        let value1 = VersionedValue::new(Version::MIN, Bytes::from_static(b"value1"));
        let value2 = VersionedValue::new(Version::MIN.next(), Bytes::from_static(b"value2"));

        client
            .put(key.clone(), value1, Precondition::None)
            .await
            .unwrap();

        let current_version = client.get_version(key.clone()).await.unwrap().unwrap();
        assert_eq!(current_version, Version::MIN);

        let result = client
            .put(
                key.clone(),
                value2.clone(),
                Precondition::MatchesVersion(current_version),
            )
            .await;
        assert!(result.is_ok());

        let new_version = client.get_version(key.clone()).await.unwrap().unwrap();
        assert_eq!(new_version, Version::MIN.next());

        let value3 = VersionedValue::new(Version::MIN.next().next(), Bytes::from_static(b"value3"));
        let result = client
            .put(
                key.clone(),
                value3,
                Precondition::MatchesVersion(current_version),
            )
            .await;
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::FailedPrecondition(_) => {} // Expected
            other => panic!("Expected FailedPrecondition, got {other:?}"),
        }
    }
}

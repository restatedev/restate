// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use tracing::warn;

use crate::cancellation_watcher;
use crate::metadata_store::providers::objstore::optimistic_store::OptimisticLockingMetadataStoreBuilder;
use crate::metadata_store::{
    Precondition, ProvisionedMetadataStore, ReadError, VersionedValue, WriteError,
};

#[derive(Debug)]
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
                    match cmd {
                        Commands::Get{key,tx  } => {
                               let res = delegate.get(key).await;
                               let _ = tx.send(res);
                        }
                        Commands::GetVersion{key,tx  } => {
                                let _ = tx.send(delegate.get_version(key).await);
                        }
                        Commands::Put{key,value,precondition,tx  } => {
                                let _ = tx.send(delegate.put(key, value, precondition).await);
                        }
                        Commands::Delete{key,precondition,tx  } => {
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
    async fn get(&self, key: ByteString) -> Result<Option<VersionedValue>, ReadError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Get { key, tx })
            .map_err(|_| ReadError::internal("Object store fetch channel ".into()))?;

        rx.await.map_err(|_| {
            ReadError::internal("Object store fetch channel disconnected".to_string())
        })?
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::GetVersion { key, tx })
            .map_err(|_| ReadError::internal("Object store fetch channel ".into()))?;

        rx.await.map_err(|_| {
            ReadError::internal("Object store fetch channel disconnected".to_string())
        })?
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Put {
                key,
                value,
                precondition,
                tx,
            })
            .map_err(|_| WriteError::internal("Object store channel ".into()))?;

        rx.await
            .map_err(|_| WriteError::internal("Object store channel disconnected".to_string()))?
    }

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Delete {
                key,
                precondition,
                tx,
            })
            .map_err(|_| WriteError::internal("Object store fetch channel ".into()))?;

        rx.await.map_err(|_| {
            WriteError::internal("Object store fetch channel disconnected".to_string())
        })?
    }
}

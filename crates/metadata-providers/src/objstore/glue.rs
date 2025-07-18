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
use tracing;
use tracing::{debug, warn};

use restate_core::{ShutdownError, cancellation_watcher};
use restate_metadata_store::{ProvisionedMetadataStore, ReadError, WriteError};
use restate_types::metadata::{Precondition, VersionedValue};

use super::optimistic_store::OptimisticLockingMetadataStoreBuilder;

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
                    debug!(?cmd, "received command");

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
        debug!(?key, "sending Get");

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::Get { key, tx })
            .map_err(|_| ReadError::terminal(ShutdownError))?;

        rx.await.map_err(|_| ReadError::terminal(ShutdownError))?
    }

    async fn get_version(&self, key: ByteString) -> Result<Option<Version>, ReadError> {
        debug!(?key, "sending GetVersion");

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Commands::GetVersion { key, tx })
            .map_err(|_| ReadError::terminal(ShutdownError))?;

        rx.await.map_err(|_| ReadError::terminal(ShutdownError))?
    }

    async fn put(
        &self,
        key: ByteString,
        value: VersionedValue,
        precondition: Precondition,
    ) -> Result<(), WriteError> {
        debug!(?key, "sending Put");

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

    async fn delete(&self, key: ByteString, precondition: Precondition) -> Result<(), WriteError> {
        debug!(?key, %precondition, "sending Delete");

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

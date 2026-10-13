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

use tokio::sync::{mpsc, oneshot};

use restate_core::ShutdownError;
use restate_limiter::rule_book::RuleBookObserver;
use restate_types::{cluster::cluster_state::PartitionProcessorStatus, identifiers::PartitionId};

use crate::PartitionQueryAccess;

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    GetState(oneshot::Sender<BTreeMap<PartitionId, PartitionProcessorStatus>>),
}

#[derive(Clone)]
pub struct ProcessorsManagerHandle {
    sender: mpsc::Sender<ProcessorsManagerCommand>,
    rule_book_observer: Arc<dyn RuleBookObserver>,
    query_access: Arc<dyn PartitionQueryAccess>,
}

impl ProcessorsManagerHandle {
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

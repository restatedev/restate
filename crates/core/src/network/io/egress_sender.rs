// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::sync::mpsc;
use tracing::trace;

use super::{CloseReason, EgressMessage};
#[derive(derive_more::Deref, Clone)]
pub struct EgressSender {
    pub(crate) cid: u64,
    #[deref]
    pub(crate) inner: mpsc::Sender<EgressMessage>,
}

impl EgressSender {
    pub async fn reserve_owned(
        self,
    ) -> Result<mpsc::OwnedPermit<EgressMessage>, mpsc::error::SendError<()>> {
        self.inner.reserve_owned().await
    }

    pub fn close(&self, reason: CloseReason) {
        if self
            .inner
            // todo: replace with send when we move to unbounded channel
            .try_send(EgressMessage::Close(CloseReason::Shutdown))
            .is_ok()
        {
            trace!(cid = %self.cid, ?reason, "Drain message was enqueued to egress stream");
        }
    }
}

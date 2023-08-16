// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::PeerId;
use restate_types::message::PeerTarget;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

/// Sender which attaches the identity id to a sent message.
#[derive(Debug)]
pub(super) struct IdentitySender<T> {
    id: PeerId,
    sender: mpsc::Sender<PeerTarget<T>>,
}

impl<T> IdentitySender<T> {
    pub(super) fn new(id: PeerId, sender: mpsc::Sender<PeerTarget<T>>) -> Self {
        Self { id, sender }
    }

    pub(super) async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        self.sender
            .send((self.id, msg))
            .await
            .map_err(|SendError((_, msg))| SendError(msg))
    }
}

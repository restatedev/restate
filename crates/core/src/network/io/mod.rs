// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod egress_sender;
mod egress_stream;
mod reactor;
mod rpc_tracker;
mod stream_tracker;

use std::sync::Arc;

pub use egress_sender::SendToken;
pub(crate) use egress_sender::Sent;
pub(super) use egress_stream::EgressMessage;
pub(super) use egress_stream::EgressStream;

pub(super) use egress_sender::{EgressSender, UnboundedEgressSender};
pub use egress_stream::{DrainReason, DropEgressStream};
pub use reactor::ConnectionReactor;

use super::ConnectionClosed;

pub struct Shared {
    /// The sender for the egress stream.
    tx: Option<UnboundedEgressSender>,
    drop_egress: Option<DropEgressStream>,
    reply_tracker: Arc<rpc_tracker::ReplyTracker>,
    local_initiated_streams: Arc<stream_tracker::StreamTracker>,
}

impl Shared {
    pub fn unbounded_send(&self, message: EgressMessage) -> Result<(), ConnectionClosed> {
        if let Some(ref sender) = self.tx {
            sender.unbounded_send(message)
        } else {
            Err(ConnectionClosed)
        }
    }
}

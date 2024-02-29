// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use enum_map::EnumMap;
use restate_core::is_cancellation_requested;
use restate_node_protocol::{MessageEnvelope, MessageKind};
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc;
use tracing::{debug, error};

#[derive(Default)]
pub struct MessageRouter {
    handlers: EnumMap<MessageKind, OnceLock<mpsc::Sender<MessageEnvelope>>>,
}

impl MessageRouter {
    pub fn new_with_metadata_writer() -> Arc<Self> {
        Arc::new(Self {
            handlers: Default::default(),
        })
    }

    #[track_caller]
    pub fn set_route(&self, kind: MessageKind, handler: mpsc::Sender<MessageEnvelope>) {
        if self.handlers[kind].set(handler).is_err() {
            panic!("Handler already set for message kind: {:?}", kind);
        }
    }

    #[track_caller]
    pub fn set_metadata_manager_subscriber(&self, subscriber: mpsc::Sender<MessageEnvelope>) {
        // metadata manager can handle the sync metadata messages.
        let interested_in = [MessageKind::GetMetadataRequest, MessageKind::MetadataUpdate];

        for kind in interested_in {
            if self.handlers[kind].set(subscriber.clone()).is_err() {
                panic!("Handler already set for message kind: {}", kind);
            }
        }
    }

    #[track_caller]
    pub fn set_ingress_subscriber(&self, _handler: mpsc::Sender<MessageEnvelope>) {
        // worker can handle the following message types.
        // if self.handlers[kind].set(handler).is_err() {
        //     panic!("Handler already set for message kind: {:?}", kind);
        // }
    }

    pub async fn route_message(&self, envelope: MessageEnvelope) {
        let kind = envelope.kind();
        if let Some(handler) = self.handlers[envelope.kind()].get() {
            if let Err(e) = handler.send(envelope).await {
                if !is_cancellation_requested() {
                    debug!("Failed to route message {}: {:?}", kind, e);
                }
            }
        } else {
            // Channel is not setup, or we have the wrong role to handle this message type.
            error!("No handler set for message kind: {}", envelope.kind());
        }
    }
}

static_assertions::assert_impl_all!(MessageRouter: Send, Sync);

// Copyright (c) 2024 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod client;
mod protocol;

use bytes::Bytes;
use std::collections::BTreeMap;

use crate::client::ConfiguredClient;
use anyhow::bail;
use restate_service_protocol_v4::message_codec::{Message, MessageHeader};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationTarget;

pub struct SimpleRequestResponseInvoker {
    client: ConfiguredClient,
    invocation_id: Bytes,
}

impl SimpleRequestResponseInvoker {
    pub fn new(client: ConfiguredClient) -> Self {
        let dummy = InvocationTarget::service("greeter", "greet");
        let invocation_id = InvocationId::generate(&dummy, None);
        let invocation_id = Bytes::copy_from_slice(&invocation_id.to_bytes());
        Self {
            client,
            invocation_id,
        }
    }

    pub async fn invoke(&self, key: Option<Bytes>) -> anyhow::Result<()> {
        let journal = protocol::initial_journal(self.invocation_id.clone(), key, Bytes::new());

        let _state: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        loop {
            let from_sdk = self.round_trip(journal.clone()).await?;
            for (_header, msg) in from_sdk {
                match msg {
                    Message::SetStateCommand(_) => {}
                    Message::GetEagerStateCommand(_) => {}
                    Message::ProposeRunCompletion(_) => {}
                    Message::RunCommand(_) => {}
                    Message::End(_) => {
                        return Ok(());
                    }
                    Message::Error(_) => {
                        bail!("An sdk replied with an Error entry, but we don't support it.");
                    }
                    Message::Suspension(_) => {
                        bail!("An sdk wanted to suspend");
                    }
                    // -----
                    Message::SignalNotification(_) => {}
                    Message::CompleteAwakeableCommand(_) => {}
                    Message::GetInvocationOutputCompletionNotification(_) => {}
                    Message::GetInvocationOutputCommand(_) => {}
                    Message::AttachInvocationCompletionNotification(_) => {}
                    Message::AttachInvocationCommand(_) => {}
                    Message::RunCompletionNotification(_) => {}
                    Message::SendSignalCommand(_) => {}
                    Message::OneWayCallCommand(_) => {}
                    Message::CallCompletionNotification(_) => {}
                    Message::CallInvocationIdCompletionNotification(_) => {}
                    Message::CallCommand(_) => {}
                    Message::SleepCompletionNotification(_) => {}
                    Message::SleepCommand(_) => {}
                    Message::CompletePromiseCompletionNotification(_) => {}
                    Message::CompletePromiseCommand(_) => {}
                    Message::PeekPromiseCompletionNotification(_) => {}
                    Message::PeekPromiseCommand(_) => {}
                    Message::GetPromiseCompletionNotification(_) => {}
                    Message::GetPromiseCommand(_) => {}
                    Message::GetEagerStateKeysCommand(_) => {}
                    Message::GetLazyStateKeysCompletionNotification(_) => {}
                    Message::GetLazyStateKeysCommand(_) => {}
                    Message::ClearAllStateCommand(_) => {}
                    Message::ClearStateCommand(_) => {}
                    Message::GetLazyStateCompletionNotification(_) => {}
                    Message::GetLazyStateCommand(_) => {}
                    Message::OutputCommand(_) => {}
                    Message::InputCommand(_) => {}
                    Message::CommandAck(_) => {}
                    Message::Start(_) => {}
                    Message::Custom(_, _) => {}
                }
            }
        }
    }

    async fn round_trip(
        &self,
        journal: Vec<Message>,
    ) -> anyhow::Result<Vec<(MessageHeader, Message)>> {
        let request_body = protocol::encode_protocol_messages(journal)?;
        let res = self.client.invoke(request_body).await?;
        let response_body = res
            .body
            .expect("response body must be present for successful responses");
        protocol::decode_protocol_messages(response_body)
    }
}

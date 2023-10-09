// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost::{encoding, Message};
use restate_pb::restate::Event;
use restate_schema_api::subscription::InputEventRemap;
use std::fmt;

/// Structure that implements the remapping of the event fields.
pub(super) struct MappedEvent<'a>(pub(super) &'a mut Event, pub(super) &'a InputEventRemap);

impl Message for MappedEvent<'_> {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: bytes::BufMut,
    {
        if self.1.key_index.is_some() && !self.0.key.is_empty() {
            encoding::bytes::encode(self.1.key_index.unwrap(), &self.0.key, buf);
        }
        if self.1.payload_index.is_some() && !self.0.payload.is_empty() {
            encoding::bytes::encode(self.1.payload_index.unwrap(), &self.0.payload, buf);
        }
        if self.1.attributes_index.is_some() {
            encoding::hash_map::encode(
                encoding::string::encode,
                encoding::string::encoded_len,
                encoding::string::encode,
                encoding::string::encoded_len,
                self.1.attributes_index.unwrap(),
                &self.0.attributes,
                buf,
            );
        }
    }

    fn merge_field<B>(
        &mut self,
        _: u32,
        _: encoding::WireType,
        _: &mut B,
        _: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        B: bytes::Buf,
    {
        unimplemented!("This method should not be used!")
    }

    #[inline]
    fn encoded_len(&self) -> usize {
        (if self.1.key_index.is_some() && !self.0.key.is_empty() {
            encoding::bytes::encoded_len(self.1.key_index.unwrap(), &self.0.key)
        } else {
            0
        }) + if self.1.payload_index.is_some() && !self.0.payload.is_empty() {
            encoding::bytes::encoded_len(self.1.payload_index.unwrap(), &self.0.payload)
        } else {
            0
        } + if self.1.attributes_index.is_some() {
            encoding::hash_map::encoded_len(
                encoding::string::encoded_len,
                encoding::string::encoded_len,
                self.1.attributes_index.unwrap(),
                &self.0.attributes,
            )
        } else {
            0
        }
    }
    fn clear(&mut self) {
        self.0.clear();
    }
}

impl fmt::Debug for MappedEvent<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

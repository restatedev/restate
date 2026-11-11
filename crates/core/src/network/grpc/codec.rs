// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic::Status;
use tonic::codec::{Codec, EncodeBuf, Encoder};
use tonic_prost::ProstDecoder;

use crate::network::OutgoingMessage;
use crate::network::protobuf::network::Message;

/// The standard Prost wire format with an outbound-only prepared representation.
/// Tonic owns framing, compression, batching, buffering and flow control.
#[derive(Default)]
pub struct FabricCodec {
    _private: (),
}

impl Codec for FabricCodec {
    type Encode = OutgoingMessage;
    type Decode = Message;
    type Encoder = FabricEncoder;
    type Decoder = ProstDecoder<Message>;

    fn encoder(&mut self) -> Self::Encoder {
        FabricEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder::default()
    }
}

pub struct FabricEncoder;

impl Encoder for FabricEncoder {
    type Item = OutgoingMessage;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        dst.reserve(item.encoded_len());
        item.encode_into(dst);
        Ok(())
    }
}

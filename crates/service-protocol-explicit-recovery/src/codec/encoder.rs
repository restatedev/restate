// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{BufMut, Bytes, BytesMut};

use super::{Message, MessageHeader};

/// Encodes a message with its header, in one buffer of the right size.
pub fn encode(msg: &Message) -> Bytes {
    let len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(8 + len);
    buf.put_u64(
        MessageHeader::new(
            msg.ty(),
            u32::try_from(len).expect("protocol messages can't be larger than u32"),
        )
        .into(),
    );
    msg.encode(&mut buf);
    buf.freeze()
}

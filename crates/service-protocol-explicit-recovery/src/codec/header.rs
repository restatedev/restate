// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::MessageType;

const RESERVED_MASK: u64 = 0x0000_FFFF_0000_0000;

/// The 8-byte header: type (16 bits), reserved (16 bits, zero), length (32 bits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    ty: MessageType,
    length: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HeaderError {
    #[error("unknown message type {0:#06x}")]
    UnknownType(u16),
    #[error("reserved header bits are set: {0:#06x}")]
    ReservedBits(u16),
}

impl MessageHeader {
    pub fn new(ty: MessageType, length: u32) -> Self {
        Self { ty, length }
    }

    pub fn message_type(&self) -> MessageType {
        self.ty
    }

    pub fn frame_length(&self) -> u32 {
        self.length
    }
}

impl TryFrom<u64> for MessageHeader {
    type Error = HeaderError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let ty = MessageType::try_from((value >> 48) as u16).map_err(HeaderError::UnknownType)?;
        if value & RESERVED_MASK != 0 {
            return Err(HeaderError::ReservedBits((value >> 32) as u16));
        }
        Ok(Self::new(ty, value as u32))
    }
}

impl From<MessageHeader> for u64 {
    fn from(header: MessageHeader) -> Self {
        ((header.ty.code() as u64) << 48) | header.length as u64
    }
}

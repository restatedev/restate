// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("bincode encode: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("bincode decode: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("protobuf decode: {0}")]
    ProtobufDecode(&'static str),
}

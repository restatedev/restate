// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the code-generated structs of [service-protocol](https://github.com/restatedev/service-protocol) and the codec to use them.

use restate_types::journal_v2::encoding::EncodingError;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{Decoder, Encoder, Entry};

pub struct ServiceProtocolV4Codec;

impl Encoder for ServiceProtocolV4Codec {
    fn encode_entry(_entry: &Entry) -> Result<RawEntry, EncodingError> {
        todo!()
    }
}

impl Decoder for ServiceProtocolV4Codec {
    fn decode_entry(_entry: &RawEntry) -> Result<Entry, EncodingError> {
        todo!()
    }
}

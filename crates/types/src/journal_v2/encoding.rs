// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::errors::GenericError;
use crate::journal_v2::Entry;
use crate::journal_v2::lite::EntryLite;
use crate::journal_v2::raw::RawEntry;

#[derive(Debug, thiserror::Error)]
#[error("decoding error: {0:?}")]
pub struct DecodingError(#[from] GenericError);

/// The decoder is the abstraction encapsulating how serialized data represents entries.
///
/// This is typically depending on the concrete service protocol implementation/format/version.
pub trait Decoder {
    fn decode_entry(entry: &RawEntry) -> Result<Entry, DecodingError>;

    fn decode_entry_lite(entry: &RawEntry) -> Result<EntryLite, DecodingError>;
}

/// The encoder is the abstraction encapsulating how to represent entries as serialized data.
///
/// This is typically depending on the concrete service protocol implementation/format/version.
pub trait Encoder {
    fn encode_entry(entry: Entry) -> RawEntry;
}

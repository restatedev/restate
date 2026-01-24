// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;
use rand::distr::{Alphanumeric, SampleString};
use rand::{RngCore, rng};

pub fn bytes() -> Bytes {
    bytes_of_len(128)
}

pub fn bytes_of_len(len: usize) -> Bytes {
    let mut vec = vec![0; len];
    rng().fill_bytes(vec.as_mut_slice());
    Bytes::from(vec)
}

pub fn bytestring() -> ByteString {
    bytestring_of_len(60)
}

pub fn bytestring_of_len(len: usize) -> ByteString {
    Alphanumeric.sample_string(&mut rand::rng(), len).into()
}

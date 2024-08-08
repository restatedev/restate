// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use restate_types::flexbuffers_storage_encode_decode;

use crate::Header;

/// Owned payload.
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    header: Header,
    body: Bytes,
}

impl Payload {
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self {
            header: Header::default(),
            body: body.into(),
        }
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn into_body(self) -> Bytes {
        self.body
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn into_header(self) -> Header {
        self.header
    }
}

flexbuffers_storage_encode_decode!(Payload);

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hyper::header::HeaderName;

use hyper::HeaderMap;

pub(crate) mod v1;

const SCHEME_HEADER: HeaderName = HeaderName::from_static("x-restate-signature-scheme");

pub trait SignRequest {
    type Error;
    fn insert_identity(self, headers: HeaderMap) -> Result<HeaderMap, Self::Error>;
}

impl<T: SignRequest> SignRequest for Option<T> {
    type Error = T::Error;
    fn insert_identity(self, headers: HeaderMap) -> Result<HeaderMap, Self::Error> {
        match self {
            Some(signer) => signer.insert_identity(headers),
            None => Ok(headers),
        }
    }
}

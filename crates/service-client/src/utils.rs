// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_sdk_lambda::error::SdkError;
use hyper::http;

pub trait ErrorExt {
    /// Retryable errors are those which can be caused by transient faults and where
    /// retrying can succeed.
    fn is_retryable(&self) -> bool;
}

impl<E, R> ErrorExt for SdkError<E, R> {
    fn is_retryable(&self) -> bool {
        match self {
            SdkError::ConstructionFailure(_) => false,
            SdkError::DispatchFailure(_) => true,
            SdkError::TimeoutError(_) => true,
            SdkError::ResponseError(_) => true,
            SdkError::ServiceError(_) => true,
            // Needed because SdkError is non-exhaustive. Since we don't know what new variants will be
            // added we are conservative and retry them by default. This can be unnecessary.
            _ => true,
        }
    }
}

impl ErrorExt for hyper_util::client::legacy::Error {
    fn is_retryable(&self) -> bool {
        self.is_connect()
    }
}

impl ErrorExt for http::Error {
    fn is_retryable(&self) -> bool {
        false
    }
}

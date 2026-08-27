// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, LazyLock};

use aws_sdk_lambda::error::SdkError;
use hyper::http;
use hyper_rustls::ConfigBuilderExt;
use rustls::{ClientConfig, KeyLogFile};

/// Shared by every outbound TLS client in this crate.
///
/// We need to explicitly configure the crypto provider since we activate the ring as well as
/// aws_lc_rs rustls feature, and they are mutually exclusive wrt auto installation. Moreover,
/// we don't want that tests need to install a crypto provider when using the HttpClient
pub(crate) static TLS_CLIENT_CONFIG: LazyLock<ClientConfig> = LazyLock::new(|| {
    let mut builder = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(rustls::DEFAULT_VERSIONS)
    .expect("default versions are supported")
    .with_native_roots()
    .expect("Can load native certificates")
    .with_no_client_auth();
    builder.dangerous().cfg.key_log = Arc::new(KeyLogFile::new());
    builder
});

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

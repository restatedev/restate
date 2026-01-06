// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{MetadataClientOptions, NetworkingOptions};

/// Overhead added to user-facing max_message_size.
///
/// This accounts for message wrapping (headers, envelopes, encoding overhead)
/// when messages are transmitted over the internal message fabric.
pub const MESSAGE_SIZE_OVERHEAD: usize = 1024 * 1024; // 1 MiB

pub trait GrpcConnectionOptions {
    /// The maximum message size for tonic gRPC configuration.
    /// Implementations should add [`MESSAGE_SIZE_OVERHEAD`] to account for message
    /// wrapping when transmitted over the internal network.
    fn message_size_limit(&self) -> NonZeroUsize;
}
/// Helper trait to extract common client connection options from different configuration types.
pub trait CommonClientConnectionOptions: GrpcConnectionOptions {
    fn connect_timeout(&self) -> Duration;
    fn request_timeout(&self) -> Option<Duration>;
    fn keep_alive_interval(&self) -> Duration;
    fn keep_alive_timeout(&self) -> Duration;
    fn http2_adaptive_window(&self) -> bool;
}

impl<T: GrpcConnectionOptions> GrpcConnectionOptions for &T {
    fn message_size_limit(&self) -> NonZeroUsize {
        (*self).message_size_limit()
    }
}

impl<T> CommonClientConnectionOptions for &T
where
    T: CommonClientConnectionOptions + GrpcConnectionOptions,
{
    fn connect_timeout(&self) -> Duration {
        (*self).connect_timeout()
    }

    fn request_timeout(&self) -> Option<Duration> {
        (*self).request_timeout()
    }

    fn keep_alive_interval(&self) -> Duration {
        (*self).keep_alive_interval()
    }

    fn keep_alive_timeout(&self) -> Duration {
        (*self).keep_alive_timeout()
    }

    fn http2_adaptive_window(&self) -> bool {
        (*self).http2_adaptive_window()
    }
}

impl<T> GrpcConnectionOptions for Arc<T>
where
    T: GrpcConnectionOptions,
{
    fn message_size_limit(&self) -> NonZeroUsize {
        (**self).message_size_limit()
    }
}

impl<T> CommonClientConnectionOptions for Arc<T>
where
    T: CommonClientConnectionOptions + GrpcConnectionOptions,
{
    fn connect_timeout(&self) -> Duration {
        (**self).connect_timeout()
    }

    fn request_timeout(&self) -> Option<Duration> {
        (**self).request_timeout()
    }

    fn keep_alive_interval(&self) -> Duration {
        (**self).keep_alive_interval()
    }

    fn keep_alive_timeout(&self) -> Duration {
        (**self).keep_alive_timeout()
    }

    fn http2_adaptive_window(&self) -> bool {
        (**self).http2_adaptive_window()
    }
}

impl GrpcConnectionOptions for NetworkingOptions {
    #[inline]
    fn message_size_limit(&self) -> NonZeroUsize {
        self.message_size_limit
            .as_non_zero_usize()
            .saturating_add(MESSAGE_SIZE_OVERHEAD)
    }
}

impl CommonClientConnectionOptions for NetworkingOptions {
    fn connect_timeout(&self) -> Duration {
        self.connect_timeout.into()
    }

    fn request_timeout(&self) -> Option<Duration> {
        None
    }

    fn keep_alive_interval(&self) -> Duration {
        self.http2_keep_alive_interval.into()
    }

    fn keep_alive_timeout(&self) -> Duration {
        self.http2_keep_alive_timeout.into()
    }

    fn http2_adaptive_window(&self) -> bool {
        self.http2_adaptive_window
    }
}

impl GrpcConnectionOptions for MetadataClientOptions {
    fn message_size_limit(&self) -> NonZeroUsize {
        Self::message_size_limit(self).saturating_add(MESSAGE_SIZE_OVERHEAD)
    }
}

impl CommonClientConnectionOptions for MetadataClientOptions {
    fn connect_timeout(&self) -> Duration {
        self.connect_timeout.into()
    }

    fn request_timeout(&self) -> Option<Duration> {
        None
    }

    fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval.into()
    }

    fn keep_alive_timeout(&self) -> Duration {
        self.keep_alive_timeout.into()
    }

    fn http2_adaptive_window(&self) -> bool {
        true
    }
}

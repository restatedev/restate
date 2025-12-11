// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crate::config::{MetadataClientOptions, NetworkingOptions};

/// Helper trait to extract common client connection options from different configuration types.
pub trait CommonClientConnectionOptions {
    fn connect_timeout(&self) -> Duration;
    fn request_timeout(&self) -> Option<Duration>;
    fn keep_alive_interval(&self) -> Duration;
    fn keep_alive_timeout(&self) -> Duration;
    fn http2_adaptive_window(&self) -> bool;
    fn max_encoding_message_size(&self) -> usize;
    fn max_decoding_message_size(&self) -> usize;
}

impl<T: CommonClientConnectionOptions> CommonClientConnectionOptions for &T {
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

    fn max_encoding_message_size(&self) -> usize {
        (*self).max_encoding_message_size()
    }

    fn max_decoding_message_size(&self) -> usize {
        (*self).max_decoding_message_size()
    }
}

impl<T> CommonClientConnectionOptions for Arc<T>
where
    T: CommonClientConnectionOptions,
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

    fn max_encoding_message_size(&self) -> usize {
        (**self).max_encoding_message_size()
    }

    fn max_decoding_message_size(&self) -> usize {
        (**self).max_decoding_message_size()
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

    fn max_encoding_message_size(&self) -> usize {
        self.max_encoding_message_size.as_usize()
    }

    fn max_decoding_message_size(&self) -> usize {
        self.max_decoding_message_size.as_usize()
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

    fn max_encoding_message_size(&self) -> usize {
        Self::max_encoding_message_size(self)
    }

    fn max_decoding_message_size(&self) -> usize {
        Self::max_decoding_message_size(self)
    }
}

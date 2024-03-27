// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use derive_getters::Getters;
use serde_with::serde_as;

pub use restate_service_client::{
    Options as ServiceClientOptions, OptionsBuilder as ServiceClientOptionsBuilder,
    OptionsBuilderError as ServiceClientOptionsBuilderError,
};
use restate_types::retries::RetryPolicy;

serde_with::with_prefix!(prefix_with_invoker "invoker_");

/// # Invoker options
#[serde_as]
#[derive(Debug, Getters, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "options_schema",
    schemars(rename = "InvokerOptions", default)
)]
#[builder(default)]
pub struct Options {
    /// # Retry policy
    ///
    /// Retry policy to use for all the invocations handled by this invoker.
    retry_policy: RetryPolicy,

    /// # Inactivity timeout
    ///
    /// This timer guards against stalled service/handler invocations. Once it expires,
    /// Restate triggers a graceful termination by asking the service invocation to
    /// suspend (which preserves intermediate progress).
    ///
    /// The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    /// the request to suspend.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    inactivity_timeout: humantime::Duration,

    /// # Abort timeout
    ///
    /// This timer guards against stalled service/handler invocations that are supposed to
    /// terminate. The abort timeout is started after the 'inactivity timeout' has expired
    /// and the service/handler invocation has been asked to gracefully terminate. Once the
    /// timer expires, it will abort the service/handler invocation.
    ///
    /// This timer potentially **interrupts** user code. If the user code needs longer to
    /// gracefully terminate, then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(feature = "options_schema", schemars(with = "String"))]
    abort_timeout: humantime::Duration,

    /// # Message size warning
    ///
    /// Threshold to log a warning in case protocol messages coming from a service are larger than the specified amount.
    message_size_warning: usize,

    /// # Message size limit
    ///
    /// Threshold to fail the invocation in case protocol messages coming from a service are larger than the specified amount.
    message_size_limit: Option<usize>,

    /// # Temporary directory
    ///
    /// Temporary directory to use for the invoker temporary files.
    /// If empty, the system temporary directory will be used instead.
    tmp_dir: PathBuf,

    /// # Limit number of concurrent invocations from this node
    ///
    /// Number of concurrent invocations that can be processed by the invoker.
    concurrent_invocations_limit: Option<usize>,

    #[serde(flatten)]
    service_client: ServiceClientOptions,

    // -- Private config options (not exposed in the schema)
    #[cfg_attr(feature = "options_schema", schemars(skip))]
    disable_eager_state: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::exponential(
                Duration::from_millis(50),
                2.0,
                usize::MAX,
                Some(Duration::from_secs(10)),
            ),
            inactivity_timeout: Duration::from_secs(60).into(),
            abort_timeout: Duration::from_secs(60).into(),
            message_size_warning: 1024 * 1024 * 10, // 10mb
            message_size_limit: None,
            tmp_dir: restate_fs_util::generate_temp_dir_name("invoker"),
            concurrent_invocations_limit: None,
            service_client: Default::default(),
            disable_eager_state: false,
        }
    }
}

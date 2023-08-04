use super::Service;

use futures::Stream;
use restate_hyper_util::proxy_connector::Proxy;
use restate_invoker_api::{EntryEnricher, JournalReader};
use restate_schema_api::endpoint::EndpointMetadataResolver;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::retries::RetryPolicy;
use serde_with::serde_as;
use std::path::PathBuf;
use std::time::Duration;

/// # HTTP/2 Keep alive options
///
/// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
///
/// Please note: most gateways don't propagate the HTTP/2 keep-alive between downstream and upstream hosts.
/// In those environments, you need to make sure the gateway can detect a broken connection to the upstream service endpoint(s).
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
pub struct Http2KeepAliveOptions {
    /// # HTTP/2 Keep-alive interval
    ///
    /// Sets an interval for HTTP/2 PING frames should be sent to keep a
    /// connection alive.
    ///
    /// You should set this timeout with a value lower than the `response_abort_timeout`.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Http2KeepAliveOptions::default_interval")
    )]
    pub(crate) interval: humantime::Duration,

    /// # Timeout
    ///
    /// Sets a timeout for receiving an acknowledgement of the keep-alive ping.
    ///
    /// If the ping is not acknowledged within the timeout, the connection will
    /// be closed.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Http2KeepAliveOptions::default_timeout")
    )]
    pub(crate) timeout: humantime::Duration,
}

impl Default for Http2KeepAliveOptions {
    fn default() -> Self {
        Self {
            interval: Http2KeepAliveOptions::default_interval(),
            timeout: Http2KeepAliveOptions::default_timeout(),
        }
    }
}

impl Http2KeepAliveOptions {
    fn default_interval() -> humantime::Duration {
        (Duration::from_secs(40)).into()
    }

    fn default_timeout() -> humantime::Duration {
        (Duration::from_secs(20)).into()
    }
}

/// # Invoker options
#[serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "InvokerOptions"))]
#[builder(default)]
pub struct Options {
    /// # Retry policy
    ///
    /// Retry policy to use for all the invocations handled by this invoker.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_retry_policy")
    )]
    retry_policy: RetryPolicy,

    /// # Inactivity timeout
    ///
    /// This timer is used to gracefully shutdown a bidirectional stream
    /// after inactivity on both request and response streams.
    ///
    /// When this timer is fired, the invocation will be closed gracefully
    /// by closing the request stream, triggering a suspension on the sdk,
    /// in case the sdk is waiting on a completion. This won't affect the response stream.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_inactivity_timeout")
    )]
    inactivity_timeout: humantime::Duration,

    /// # Abort timeout
    ///
    /// This timer is used to forcefully shutdown an invocation when only the response stream is open.
    ///
    /// When protocol mode is `restate_service_metadata::ProtocolType::RequestResponse`,
    /// this timer will start as soon as the replay of the journal is completed.
    /// When protocol mode is `restate_service_metadata::ProtocolType::BidiStream`,
    /// this timer will start after the request stream has been closed.
    /// Check `inactivity_timeout` to configure a timer on the request stream.
    ///
    /// When this timer is fired, the response stream will be aborted,
    /// potentially **interrupting** user code! If the user code needs longer to complete,
    /// then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_abort_timeout")
    )]
    abort_timeout: humantime::Duration,

    /// # Message size warning
    ///
    /// Threshold to log a warning in case protocol messages coming from service endpoint are larger than the specified amount.
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_message_size_warning")
    )]
    message_size_warning: usize,

    /// # Message size limit
    ///
    /// Threshold to fail the invocation in case protocol messages coming from service endpoint are larger than the specified amount.
    message_size_limit: Option<usize>,

    /// # Proxy URI
    ///
    /// A URI, such as `http://127.0.0.1:10001`, of a server to which all invocations should be sent, with the `Host` header set to the service endpoint URI.
    /// HTTPS proxy URIs are supported, but only HTTP endpoint traffic will be proxied currently.
    /// Can be overridden by the `HTTP_PROXY` environment variable.
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    #[cfg_attr(feature = "options_schema", schemars(with = "Option<String>"))]
    proxy_uri: Option<Proxy>,

    /// # Temporary directory
    ///
    /// Temporary directory to use for the invoker temporary files.
    /// If empty, the system temporary directory will be used instead.
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_tmp_dir")
    )]
    tmp_dir: PathBuf,

    /// # Concurrency limit
    ///
    /// Number of concurrent invocations that can be processed by the invoker.
    concurrency_limit: Option<usize>,

    /// # HTTP/2 Keep-alive
    ///
    /// Configuration for the HTTP/2 keep-alive mechanism, using PING frames.
    /// If unset, HTTP/2 keep-alive are disabled.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_http2_keep_alive")
    )]
    http2_keep_alive: Option<Http2KeepAliveOptions>,

    // -- Private config options (not exposed in the schema)
    #[cfg_attr(feature = "options_schema", schemars(skip))]
    disable_eager_state: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            retry_policy: Options::default_retry_policy(),
            inactivity_timeout: Options::default_inactivity_timeout(),
            abort_timeout: Options::default_abort_timeout(),
            message_size_warning: Options::default_message_size_warning(),
            message_size_limit: None,
            proxy_uri: None,
            tmp_dir: Options::default_tmp_dir(),
            concurrency_limit: None,
            http2_keep_alive: Options::default_http2_keep_alive(),
            disable_eager_state: false,
        }
    }
}

impl Options {
    fn default_retry_policy() -> RetryPolicy {
        RetryPolicy::exponential(
            Duration::from_millis(50),
            2.0,
            usize::MAX,
            Some(Duration::from_secs(10)),
        )
    }

    fn default_inactivity_timeout() -> humantime::Duration {
        Duration::from_secs(60).into()
    }

    fn default_abort_timeout() -> humantime::Duration {
        Duration::from_secs(60).into()
    }

    fn default_message_size_warning() -> usize {
        1024 * 1024 * 10 // 10mb
    }

    fn default_tmp_dir() -> PathBuf {
        restate_fs_util::generate_temp_dir_name("invoker")
    }

    fn default_http2_keep_alive() -> Option<Http2KeepAliveOptions> {
        Some(Http2KeepAliveOptions::default())
    }

    pub fn build<JR, JS, SR, EE, EMR>(
        self,
        journal_reader: JR,
        state_reader: SR,
        entry_enricher: EE,
        service_endpoint_registry: EMR,
    ) -> Service<JR, SR, EE, EMR>
    where
        JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
        JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
        EE: EntryEnricher,
        EMR: EndpointMetadataResolver,
    {
        Service::new(
            service_endpoint_registry,
            self.retry_policy,
            *self.inactivity_timeout,
            *self.abort_timeout,
            self.disable_eager_state,
            self.message_size_warning,
            self.message_size_limit,
            self.proxy_uri,
            self.http2_keep_alive,
            self.tmp_dir,
            self.concurrency_limit,
            journal_reader,
            state_reader,
            entry_enricher,
        )
    }
}

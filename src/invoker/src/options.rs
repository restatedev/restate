use super::*;

use futures::Stream;
use std::path::PathBuf;
use std::time::Duration;

use restate_common::journal::raw::{PlainRawEntry, RawEntryCodec};
use restate_common::journal::EntryEnricher;
use restate_common::retry_policy::RetryPolicy;
use restate_hyper_util::proxy_connector::Proxy;
use restate_service_metadata::ServiceEndpointRegistry;
use serde_with::serde_as;

/// # Invoker options
#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "InvokerOptions"))]
pub struct Options {
    /// # Retry policy
    ///
    /// Retry policy to use for all the invocations handled by this invoker.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_retry_policy")
    )]
    retry_policy: RetryPolicy,

    /// # Suspension timeout
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
        schemars(with = "String", default = "Options::default_suspension_timeout")
    )]
    suspension_timeout: humantime::Duration,

    /// # Response abort timeout
    ///
    /// This timer is used to forcefully shutdown an invocation when only the response stream is open.
    ///
    /// When protocol mode is `restate_service_metadata::ProtocolType::RequestResponse`,
    /// this timer will start as soon as the replay of the journal is completed.
    /// When protocol mode is `restate_service_metadata::ProtocolType::BidiStream`,
    /// this timer will start after the request stream has been closed.
    /// Check `suspension_timeout` to configure a timer on the request stream.
    ///
    /// When this timer is fired, the response stream will be aborted,
    /// potentially **interrupting** user code! If the user code needs longer to complete,
    /// then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_response_abort_timeout")
    )]
    response_abort_timeout: humantime::Duration,

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

    #[cfg_attr(feature = "options_schema", schemars(skip))]
    disable_eager_state: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            retry_policy: Options::default_retry_policy(),
            suspension_timeout: Options::default_suspension_timeout(),
            response_abort_timeout: Options::default_response_abort_timeout(),
            message_size_warning: Options::default_message_size_warning(),
            message_size_limit: None,
            proxy_uri: None,
            tmp_dir: Options::default_tmp_dir(),
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

    fn default_suspension_timeout() -> humantime::Duration {
        Duration::from_secs(60).into()
    }

    fn default_response_abort_timeout() -> humantime::Duration {
        (Duration::from_secs(60) * 60).into()
    }

    fn default_message_size_warning() -> usize {
        1024 * 1024 * 10 // 10mb
    }

    fn default_tmp_dir() -> PathBuf {
        restate_fs_util::generate_temp_dir_name("invoker")
    }

    pub fn build<C, JR, JS, SR, EE, SER>(
        self,
        journal_reader: JR,
        state_reader: SR,
        entry_enricher: EE,
        service_endpoint_registry: SER,
    ) -> Service<C, JR, SR, EE, SER>
    where
        C: RawEntryCodec,
        JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
        JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
        EE: EntryEnricher,
        SER: ServiceEndpointRegistry,
    {
        Service::new(
            service_endpoint_registry,
            self.retry_policy,
            *self.suspension_timeout,
            *self.response_abort_timeout,
            self.disable_eager_state,
            self.message_size_warning,
            self.message_size_limit,
            self.proxy_uri,
            self.tmp_dir,
            journal_reader,
            state_reader,
            entry_enricher,
        )
    }
}

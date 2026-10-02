// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [`RestateCliError`]: how a failed command is reported, whatever the output format.

use std::borrow::Cow;
use std::error::Error;
use std::fmt;

use cling::CliError;
use reqwest::StatusCode;
use serde::Serialize;

use restate_cli_util::exit;

use crate::app::Command;
use crate::clients::{ApiError, ClientError, error_docs_url};
use crate::commands::{
    deployments, invocations, kafkaclusters, rules, services, subscriptions, vqueues,
};
use crate::ui::fmt::IncludeFormatting;

/// The class of a failure: its process exit code and JSON `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    NotFound,
    BadInput,
    Auth,
    Network,
    Server,
    ConfirmationRequired,
    Aborted,
    #[serde(rename = "error")]
    Generic,
}

impl ErrorKind {
    pub fn exit_code(self) -> u8 {
        match self {
            ErrorKind::NotFound => exit::NOT_FOUND,
            ErrorKind::BadInput => exit::USAGE,
            ErrorKind::Auth => exit::AUTH,
            ErrorKind::Network => exit::NETWORK,
            ErrorKind::Server => exit::SERVER,
            ErrorKind::ConfirmationRequired => exit::CONFIRMATION_REQUIRED,
            ErrorKind::Aborted => exit::ABORTED,
            ErrorKind::Generic => exit::GENERIC_ERROR,
        }
    }

    /// An HTTP error status is the server answering (e.g. via `error_for_status`), not a
    /// connection problem.
    fn from_reqwest(err: &reqwest::Error) -> Self {
        err.status()
            .map_or(ErrorKind::Network, ErrorKind::from_status)
    }

    fn from_status(status: StatusCode) -> Self {
        match status.as_u16() {
            404 => ErrorKind::NotFound,
            401 | 403 => ErrorKind::Auth,
            // The request itself is wrong. Other 4xx (e.g. 409, resuming a completed
            // invocation) are about the resource's state, not the command's usage.
            400 | 422 => ErrorKind::BadInput,
            500..=599 => ErrorKind::Server,
            _ => ErrorKind::Generic,
        }
    }

    /// The follow-ups for a failure of this kind, refined by the failed command (where
    /// known).
    fn next_steps(self, command: Option<&Command>) -> Vec<NextStep> {
        use ErrorKind::*;
        let step = |command: &str, description: &str| vec![NextStep::new(command, description)];
        match (self, command) {
            (Network, _) => vec![
                NextStep::new(
                    "restate whoami",
                    "check the configured admin URL and whether it is reachable",
                ),
                NextStep::new("restate config view", "inspect the CLI configuration"),
            ],
            (Auth, _) => step(
                "restate whoami",
                "check the configured environment and credentials",
            ),
            // Not found: the `list` command for the resource a (non-list) command operates on.
            (NotFound, Some(Command::Services(cmd)))
                if !matches!(cmd, services::Services::List(_)) =>
            {
                step("restate services list", "see the registered services")
            }
            (NotFound, Some(Command::State(_))) => {
                step("restate services list", "see the registered services")
            }
            (NotFound, Some(Command::Deployments(cmd)))
                if !matches!(cmd, deployments::Deployments::List(_)) =>
            {
                step("restate deployments list", "see the registered deployments")
            }
            (NotFound, Some(Command::Invocations(cmd)))
                if !matches!(cmd, invocations::Invocations::List(_)) =>
            {
                step("restate invocations list", "see the current invocations")
            }
            (NotFound, Some(Command::Subscriptions(cmd)))
                if !matches!(cmd, subscriptions::Subscriptions::List(_)) =>
            {
                step(
                    "restate subscriptions list",
                    "see the existing subscriptions",
                )
            }
            (NotFound, Some(Command::KafkaClusters(cmd)))
                if !matches!(cmd, kafkaclusters::KafkaClusters::List(_)) =>
            {
                step(
                    "restate kafka-clusters list",
                    "see the configured Kafka clusters",
                )
            }
            (NotFound, Some(Command::VQueues(cmd)))
                if !matches!(cmd, vqueues::VQueues::List(_)) =>
            {
                step("restate vqueues list", "see the existing virtual queues")
            }
            (NotFound, Some(Command::Rules(cmd))) if !matches!(cmd, rules::Rules::List(_)) => {
                step("restate rules list", "see the existing rules")
            }
            // The server answers some query mistakes (e.g. an unknown table) with a 500.
            (BadInput | Server, Some(Command::Sql(_))) => vec![NextStep {
                formatting: IncludeFormatting::No,
                ..NextStep::new("restate sql --help", "see the queryable tables")
            }],
            _ => Vec::new(),
        }
    }
}

/// A follow-up suggested with a failure, see
/// [`OutputFormatter::next_step`](crate::ui::fmt::OutputFormatter::next_step).
#[derive(Debug, Clone)]
pub struct NextStep {
    pub command: String,
    pub description: String,
    pub formatting: IncludeFormatting,
}

impl NextStep {
    pub fn new(command: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            description: description.into(),
            formatting: IncludeFormatting::Yes,
        }
    }
}

/// A failed command, as reported to the user: rendered by
/// [`OutputFormatter::error`](crate::ui::fmt::OutputFormatter::error), and mapped to the
/// process exit code by its [`ErrorKind`].
///
/// Return it (inside `anyhow`, e.g. with `?` or `.into()`) wherever the command knows
/// what went wrong; errors of other types are classified when reported.
#[derive(Debug)]
pub struct RestateCliError {
    kind: ErrorKind,
    message: String,
    /// The Restate error code (e.g. `META0003`), linked to its documentation.
    restate_code: Option<String>,
    /// Overrides [`ErrorKind::next_steps`] when not empty.
    next_steps: Vec<NextStep>,
    cause: Option<Box<dyn Error + Send + Sync>>,
}

impl RestateCliError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            restate_code: None,
            next_steps: Vec::new(),
            cause: None,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    pub fn bad_input(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::BadInput, message)
    }

    /// `err`'s message, with its sources as the causes.
    pub fn from_error(kind: ErrorKind, err: &(dyn Error + 'static)) -> Self {
        Self {
            cause: Cause::chain(err.source()),
            ..Self::new(kind, err.to_string())
        }
    }

    pub fn with_next_step(
        mut self,
        command: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.next_steps.push(NextStep::new(command, description));
        self
    }

    /// Prefix the message with the `anyhow` context messages that wrapped this error.
    pub(crate) fn with_context(mut self, contexts: Vec<String>) -> Self {
        if !contexts.is_empty() {
            self.message = contexts
                .into_iter()
                .chain([self.message])
                .collect::<Vec<_>>()
                .join(": ");
        }
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Where the Restate error code is documented.
    pub fn docs_url(&self) -> Option<String> {
        self.restate_code.as_deref().map(error_docs_url)
    }

    /// The follow-ups to suggest: this error's own, or the defaults of its kind for
    /// the failed `command` (where known).
    pub fn next_steps(&self, command: Option<&Command>) -> Cow<'_, [NextStep]> {
        if self.next_steps.is_empty() {
            Cow::Owned(self.kind.next_steps(command))
        } else {
            Cow::Borrowed(&self.next_steps)
        }
    }

    /// The causes, outermost first.
    pub fn causes(&self) -> impl Iterator<Item = &(dyn Error + 'static)> {
        std::iter::successors(self.source(), |&cause| cause.source())
    }
}

/// Copies keep the causes as text.
impl Clone for RestateCliError {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            message: self.message.clone(),
            restate_code: self.restate_code.clone(),
            next_steps: self.next_steps.clone(),
            cause: Cause::chain(self.source()),
        }
    }
}

impl fmt::Display for RestateCliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for RestateCliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause
            .as_deref()
            .map(|cause| cause as &(dyn Error + 'static))
    }
}

/// The server's message followed by the HTTP exchange, e.g. `access denied (403 Forbidden
/// at 'http://…')`, or just the exchange when the server gave no message.
impl From<&ApiError> for RestateCliError {
    fn from(api: &ApiError) -> Self {
        let message = match api.body.message().trim() {
            "" => api.to_string(),
            message => format!("{message} ({api})"),
        };
        Self {
            restate_code: api.body.restate_code.clone(),
            ..Self::new(ErrorKind::from_status(api.http_status_code), message)
        }
    }
}

impl From<ApiError> for RestateCliError {
    fn from(api: ApiError) -> Self {
        Self::from(&api)
    }
}

impl From<ClientError> for RestateCliError {
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::Api(api) => api.into(),
            err => Self::from(&err),
        }
    }
}

impl From<&ClientError> for RestateCliError {
    fn from(err: &ClientError) -> Self {
        match err {
            ClientError::Api(api) => api.into(),
            ClientError::Network(err) => err.into(),
            err => Self::from_error(ErrorKind::Generic, err),
        }
    }
}

impl From<&reqwest::Error> for RestateCliError {
    fn from(err: &reqwest::Error) -> Self {
        Self::from_error(ErrorKind::from_reqwest(err), err)
    }
}

/// A failure as returned by a command (see the `From<&anyhow::Error>` impl).
impl From<CliError> for RestateCliError {
    fn from(err: CliError) -> Self {
        match err {
            CliError::Other(source) | CliError::OtherWithCode(source, _) => Self::from(&source),
            CliError::Failed => RestateCliError::new(ErrorKind::Generic, "Aborted!"),
            CliError::FailedWithMessage(message)
            | CliError::FailedWithMessageAndCode(message, _) => {
                RestateCliError::new(ErrorKind::Generic, message)
            }
            other => RestateCliError::new(ErrorKind::Generic, other.to_string()),
        }
    }
}

/// The outermost typed cause (prefixed with the context messages above it), or a generic
/// error with the whole chain.
impl From<&anyhow::Error> for RestateCliError {
    fn from(source: &anyhow::Error) -> Self {
        let mut contexts = Vec::new();
        for cause in source.chain() {
            if let Some(err) = typed_cause(cause) {
                return err.with_context(contexts);
            }
            contexts.push(cause.to_string());
        }
        RestateCliError::from_error(ErrorKind::Generic, source.as_ref())
    }
}

/// `cause` as a [`RestateCliError`], if it is of a type that says what went wrong.
fn typed_cause(cause: &(dyn Error + 'static)) -> Option<RestateCliError> {
    if let Some(err) = cause.downcast_ref::<RestateCliError>() {
        return Some(err.clone());
    }
    if let Some(err) = cause.downcast_ref::<ClientError>() {
        return Some(err.into());
    }
    if let Some(err) = cause.downcast_ref::<reqwest::Error>() {
        return Some(err.into());
    }
    let kind = if cause.is::<exit::Aborted>() {
        ErrorKind::Aborted
    } else if cause.is::<exit::ConfirmationRequired>() {
        ErrorKind::ConfirmationRequired
    } else if cause.is::<exit::BadInput>() {
        ErrorKind::BadInput
    } else {
        return None;
    };
    Some(RestateCliError::new(kind, cause.to_string()))
}

/// An error's cause kept as text, for causes that can't be owned (e.g. borrowed from
/// another error).
#[derive(Debug)]
struct Cause {
    message: String,
    source: Option<Box<Cause>>,
}

impl Cause {
    /// `err` and its sources, as text.
    fn chain(err: Option<&(dyn Error + 'static)>) -> Option<Box<dyn Error + Send + Sync>> {
        let messages: Vec<String> = std::iter::successors(err, |&cause| cause.source())
            .map(ToString::to_string)
            .collect();
        messages
            .into_iter()
            .rev()
            .fold(None, |source, message| {
                Some(Box::new(Cause { message, source }))
            })
            .map(|cause| cause as Box<dyn Error + Send + Sync>)
    }
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for Cause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|cause| cause as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::ApiErrorBody;

    fn causes(err: &RestateCliError) -> Vec<String> {
        err.causes().map(ToString::to_string).collect()
    }

    #[test]
    fn typed_causes_are_kept_with_their_contexts() {
        let api = ApiError {
            http_status_code: StatusCode::NOT_FOUND,
            url: "http://localhost:9070/deployments/dp_x".to_owned(),
            body: ApiErrorBody::parse(
                r#"{"restate_code":"META0003","message":"[META0003] [META0003] no deployment"}"#
                    .to_owned(),
            ),
        };
        let err = RestateCliError::from(CliError::Other(
            anyhow::Error::from(ClientError::from(api)).context("Describing dp_x"),
        ));
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(
            err.message(),
            "Describing dp_x: no deployment (404 Not Found at 'http://localhost:9070/deployments/dp_x')"
        );
        assert_eq!(
            err.docs_url().as_deref(),
            Some("https://docs.restate.dev/references/errors#meta0003")
        );
        assert_eq!(err.causes().count(), 0);

        let err = RestateCliError::from(CliError::Other(
            anyhow::Error::from(
                RestateCliError::not_found("Unknown example 'x'")
                    .with_next_step("restate example --list", "see the available examples"),
            )
            .context("Downloading"),
        ));
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(err.message(), "Downloading: Unknown example 'x'");
        assert_eq!(err.next_steps(None)[0].command, "restate example --list");

        let err = RestateCliError::from(CliError::Other(
            anyhow::anyhow!("Invocation inv_x not found!").context("Describing"),
        ));
        assert_eq!(err.kind(), ErrorKind::Generic);
        assert_eq!(err.message(), "Describing");
        assert_eq!(causes(&err), ["Invocation inv_x not found!"]);
    }

    #[test]
    fn wrapped_http_errors_use_their_status() {
        // `Envelope::success_or_error` wraps an HTTP error status as `Network`.
        let response = http::Response::builder().status(404).body("").unwrap();
        let err = reqwest::Response::from(response)
            .error_for_status()
            .unwrap_err();
        let err = RestateCliError::from(CliError::Other(ClientError::Network(err).into()));
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    fn api_client_error(body: &str) -> ClientError {
        ClientError::Api(ApiError {
            http_status_code: StatusCode::FORBIDDEN,
            url: "http://localhost:9070/services/Greeter".to_owned(),
            body: ApiErrorBody::parse(body.to_owned()),
        })
    }

    #[test]
    fn api_client_errors_report_the_servers_message_and_the_exchange() {
        let err = RestateCliError::from(api_client_error(r#"{"message":"access denied"}"#));
        assert_eq!(
            err.message(),
            "access denied (403 Forbidden at 'http://localhost:9070/services/Greeter')"
        );
    }

    #[test]
    fn api_client_errors_without_a_message_report_the_exchange() {
        let err = RestateCliError::from(api_client_error(r#"{"message":" "}"#));
        assert_eq!(
            err.message(),
            "403 Forbidden at 'http://localhost:9070/services/Greeter'"
        );
    }

    #[test]
    fn api_client_errors_have_no_causes() {
        let err = RestateCliError::from(api_client_error(r#"{"message":"access denied"}"#));
        assert_eq!(causes(&err), Vec::<String>::new());
    }

    fn serialization_error() -> RestateCliError {
        let err = serde_json::from_str::<u32>("x").unwrap_err();
        RestateCliError::from(ClientError::from(err))
    }

    #[test]
    fn serialization_errors_say_the_response_is_unexpected() {
        assert!(
            serialization_error()
                .message()
                .starts_with("Unexpected response from the server: ")
        );
    }

    #[test]
    fn serialization_errors_are_not_repeated_as_causes() {
        assert_eq!(causes(&serialization_error()), Vec::<String>::new());
    }
}

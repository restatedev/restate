// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command failure reporting: classify errors into a stable taxonomy, emit a JSON
//! error object under `--json`, and return a differentiated process exit code so
//! scripts and agents can react to the failure class.

use std::process::ExitCode;

use cling::CliError;
use reqwest::StatusCode;
use serde_json::Value;

use restate_cli_util::exit;
use restate_cli_util::{CliContext, c_eprintln, c_println};

use crate::app::Command;
use crate::clients::{ApiError, MetasClientError};
use crate::commands::{
    deployments, invocations, kafkaclusters, rules, services, subscriptions, vqueues,
};
use crate::ui::fmt::{NEXT_STEPS, next_step_json, next_step_line, print_next_steps};

/// A read-only follow-up suggestion: `(command, description)`.
type NextStep = (&'static str, &'static str);

#[derive(Clone, Copy)]
enum ErrorKind {
    NotFound,
    BadInput,
    Auth,
    Network,
    Server,
    ConfirmationRequired,
    Aborted,
    Generic,
}

impl ErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            ErrorKind::NotFound => "not_found",
            ErrorKind::BadInput => "bad_input",
            ErrorKind::Auth => "auth",
            ErrorKind::Network => "network",
            ErrorKind::Server => "server",
            ErrorKind::ConfirmationRequired => "confirmation_required",
            ErrorKind::Aborted => "aborted",
            ErrorKind::Generic => "error",
        }
    }

    fn exit_code(self) -> u8 {
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

    fn from_status(status: StatusCode) -> Self {
        match status.as_u16() {
            404 => ErrorKind::NotFound,
            401 | 403 => ErrorKind::Auth,
            400..=499 => ErrorKind::BadInput,
            500..=599 => ErrorKind::Server,
            _ => ErrorKind::Generic,
        }
    }
}

/// Print a command failure and return a differentiated process exit code.
///
/// With `--json`, the error is emitted as a JSON object on stdout
/// (`{"error": {"kind": …, "message": …, "next_steps": […]}}`) so agents can parse it;
/// otherwise the human-formatted error goes to stderr (mirroring cling's own
/// reporting), followed by a tip with the next steps. `command` is the command that
/// failed, if parsing got that far; it refines the suggested next steps.
pub fn report_error(err: CliError, command: Option<&Command>) -> ExitCode {
    // clap prints its own help/usage and owns its exit code (0 for --help, 2 for usage).
    if let CliError::ClapError(_) = &err {
        let _ = err.print();
        return ExitCode::from(err.exit_code());
    }

    // `--dry-run` stops the command on purpose once the plan is shown; and a JSON
    // plan awaiting `--yes` was already emitted as the stdout document.
    if find_cause::<exit::DryRunComplete>(&err).is_some() {
        return ExitCode::SUCCESS;
    }
    if find_cause::<exit::ConfirmationRequired>(&err).is_some_and(|c| c.plan_emitted) {
        return ExitCode::from(exit::CONFIRMATION_REQUIRED);
    }
    if let Some(reported) = find_cause::<exit::AlreadyReported>(&err) {
        return ExitCode::from(reported.code);
    }

    let kind = classify(&err);
    let steps = next_steps(kind, command);

    if CliContext::get().json_output() {
        let document = error_document(kind, error_message(&err), &steps);
        c_println!(
            "{}",
            serde_json::to_string_pretty(&document).unwrap_or_default()
        );
    } else {
        if CliContext::get().colors_enabled() {
            let _ = err.print();
        } else {
            print_plain(&err);
        }
        if !steps.is_empty() {
            let lines: Vec<_> = steps.iter().map(|(c, d)| next_step_line(c, d)).collect();
            print_next_steps(&lines);
        }
    }

    ExitCode::from(kind.exit_code())
}

/// The first cause of `err` of type `E`, if any.
fn find_cause<E: std::error::Error + Send + Sync + 'static>(err: &CliError) -> Option<&E> {
    match err {
        CliError::Other(source) | CliError::OtherWithCode(source, _) => {
            source.chain().find_map(|cause| cause.downcast_ref::<E>())
        }
        _ => None,
    }
}

fn error_document(kind: ErrorKind, message: String, steps: &[NextStep]) -> Value {
    let mut error = serde_json::json!({ "kind": kind.as_str(), "message": message });
    if !steps.is_empty() {
        error[NEXT_STEPS] = steps.iter().map(|(c, d)| next_step_json(c, d)).collect();
    }
    serde_json::json!({ "error": error })
}

/// Read-only follow-ups for a failure, by error kind and (where known) command.
fn next_steps(kind: ErrorKind, command: Option<&Command>) -> Vec<NextStep> {
    match kind {
        ErrorKind::Network => vec![
            (
                "restate whoami",
                "check the configured admin URL and whether it is reachable",
            ),
            ("restate config view", "inspect the CLI configuration"),
        ],
        ErrorKind::Auth => vec![(
            "restate whoami",
            "check the configured environment and credentials",
        )],
        ErrorKind::NotFound => command.and_then(list_command).into_iter().collect(),
        ErrorKind::BadInput if matches!(command, Some(Command::Sql(_))) => {
            vec![("restate sql tables", "see the queryable tables")]
        }
        _ => Vec::new(),
    }
}

/// The `list` command for the resource a (non-list) command operates on.
fn list_command(command: &Command) -> Option<NextStep> {
    Some(match command {
        Command::Services(cmd) if !matches!(cmd, services::Services::List(_)) => {
            ("restate services list", "see the registered services")
        }
        Command::State(_) => ("restate services list", "see the registered services"),
        Command::Deployments(cmd) if !matches!(cmd, deployments::Deployments::List(_)) => {
            ("restate deployments list", "see the registered deployments")
        }
        Command::Invocations(cmd) if !matches!(cmd, invocations::Invocations::List(_)) => {
            ("restate invocations list", "see the current invocations")
        }
        Command::Subscriptions(cmd) if !matches!(cmd, subscriptions::Subscriptions::List(_)) => (
            "restate subscriptions list",
            "see the existing subscriptions",
        ),
        Command::KafkaClusters(cmd) if !matches!(cmd, kafkaclusters::KafkaClusters::List(_)) => (
            "restate kafka-clusters list",
            "see the configured Kafka clusters",
        ),
        Command::VQueues(cmd) if !matches!(cmd, vqueues::VQueues::List(_)) => {
            ("restate vqueues list", "see the existing virtual queues")
        }
        Command::Rules(cmd) if !matches!(cmd, rules::Rules::List(_)) => {
            ("restate rules list", "see the existing rules")
        }
        _ => return None,
    })
}

fn classify(err: &CliError) -> ErrorKind {
    let source = match err {
        CliError::Other(source) | CliError::OtherWithCode(source, _) => source,
        _ => return ErrorKind::Generic,
    };

    for cause in source.chain() {
        if cause.downcast_ref::<exit::Aborted>().is_some() {
            return ErrorKind::Aborted;
        }
        if cause.downcast_ref::<exit::ConfirmationRequired>().is_some() {
            return ErrorKind::ConfirmationRequired;
        }
        if cause.downcast_ref::<exit::NotFound>().is_some() {
            return ErrorKind::NotFound;
        }
        if cause.downcast_ref::<exit::BadInput>().is_some() {
            return ErrorKind::BadInput;
        }
        if let Some(err) = cause.downcast_ref::<MetasClientError>() {
            return match err {
                MetasClientError::Network(_) => ErrorKind::Network,
                MetasClientError::Api(api) => ErrorKind::from_status(api.http_status_code),
                MetasClientError::Serialization(_) => ErrorKind::Generic,
            };
        }
        if cause.downcast_ref::<reqwest::Error>().is_some() {
            return ErrorKind::Network;
        }
        if let Some(api) = cause.downcast_ref::<ApiError>() {
            return ErrorKind::from_status(api.http_status_code);
        }
        if let Some(api) = cause.downcast_ref::<Box<ApiError>>() {
            return ErrorKind::from_status(api.http_status_code);
        }
    }

    classify_from_message(&format!("{source:#}"))
}

fn classify_from_message(message: &str) -> ErrorKind {
    let stripped = strip_ansi(message);
    if let Some(status) = extract_http_status(&stripped) {
        return ErrorKind::from_status(status);
    }
    if stripped.to_lowercase().contains("not found") {
        return ErrorKind::NotFound;
    }
    ErrorKind::Generic
}

fn extract_http_status(message: &str) -> Option<StatusCode> {
    const MARKER: &str = "Http status code ";
    let start = message.find(MARKER)? + MARKER.len();
    let digits: String = message[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    StatusCode::from_u16(digits.parse().ok()?).ok()
}

/// Strip ANSI CSI escape sequences so classification and JSON messages are clean.
/// Colorless equivalent of `CliError::print`. cling's printer only honors `NO_COLOR`
/// (termcolor's auto mode), not `--color never`, and the messages themselves may carry
/// styling, so strip that too.
fn print_plain(err: &CliError) {
    let (message, causes): (String, Vec<String>) = match err {
        CliError::Other(source) | CliError::OtherWithCode(source, _) => (
            format!("Error: {source}"),
            source.chain().skip(1).map(|c| c.to_string()).collect(),
        ),
        other => (other.to_string(), Vec::new()),
    };
    c_eprintln!("{}", strip_ansi(&message));
    if !causes.is_empty() {
        c_eprintln!();
        c_eprintln!("Caused by:");
        let last = causes.len() - 1;
        for (i, cause) in causes.iter().enumerate() {
            let symbol = if i == last { "└─" } else { "├─" };
            c_eprintln!("  {symbol} {}", strip_ansi(cause));
        }
    }
}

fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn error_message(err: &CliError) -> String {
    let raw = match err {
        CliError::Other(source) | CliError::OtherWithCode(source, _) => format!("{source:#}"),
        other => other.to_string(),
    };
    let clean = strip_ansi(&raw);
    // Drop the trailing "  -> Http status code … at '…'" internals for JSON consumers;
    // the `kind` field already conveys the failure class.
    match clean.find("\n  -> Http status code") {
        Some(idx) => clean[..idx].trim_end().to_owned(),
        None => clean,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_from_message_reads_status_and_not_found() {
        assert!(matches!(
            classify_from_message("boom -> Http status code 404 at 'x'"),
            ErrorKind::NotFound
        ));
        assert!(matches!(
            classify_from_message("Http status code 503 at 'x'"),
            ErrorKind::Server
        ));
        assert!(matches!(
            classify_from_message("Invocation inv_x not found!"),
            ErrorKind::NotFound
        ));
        assert!(matches!(classify_from_message("weird"), ErrorKind::Generic));
    }

    #[test]
    fn json_error_document_carries_next_steps_only_when_present() {
        let network = error_document(
            ErrorKind::Network,
            "boom".to_owned(),
            &next_steps(ErrorKind::Network, None),
        );
        assert_eq!(network["error"]["kind"], "network");
        assert_eq!(network["error"]["message"], "boom");
        assert_eq!(
            network["error"][NEXT_STEPS][0]["command"],
            "restate whoami --json"
        );

        let server = error_document(
            ErrorKind::Server,
            "boom".to_owned(),
            &next_steps(ErrorKind::Server, None),
        );
        assert_eq!(server["error"]["kind"], "server");
        assert_eq!(server["error"].get(NEXT_STEPS), None);
    }

    #[test]
    fn strip_ansi_removes_escape_sequences() {
        assert_eq!(strip_ansi("\u{1b}[33m500\u{1b}[0m ok"), "500 ok");
        assert_eq!(
            extract_http_status(&strip_ansi("Http status code \u{1b}[33m500\u{1b}[0m"))
                .map(|s| s.as_u16()),
            Some(500)
        );
    }
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command failure reporting: turn a failure into a [`RestateCliError`] (classifying
//! errors of other types), render it with the output formatter, and return the
//! differentiated process exit code so scripts and agents can react to the failure class.

use std::error::Error;
use std::process::ExitCode;

use cling::CliError;

use restate_cli_util::exit;

use crate::app::Command;
use crate::error::RestateCliError;
use crate::ui::fmt::{Formatter, OutputFormatter};

/// Print a command failure and return a differentiated process exit code.
///
/// The error is rendered by [`OutputFormatter::error`], with its next steps. `command`
/// is the command that failed, if parsing got that far; it refines the default next
/// steps.
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

    let err = RestateCliError::from(err);
    let mut f = Formatter::new();
    for step in err.next_steps(command).iter() {
        f.next_step(&step.command, &step.description, step.formatting);
    }
    let _ = f.error(&err);
    ExitCode::from(err.kind().exit_code())
}

/// The first cause of `err` of type `E`, if any.
fn find_cause<E: Error + Send + Sync + 'static>(err: &CliError) -> Option<&E> {
    match err {
        CliError::Other(source) | CliError::OtherWithCode(source, _) => {
            source.chain().find_map(|cause| cause.downcast_ref::<E>())
        }
        _ => None,
    }
}

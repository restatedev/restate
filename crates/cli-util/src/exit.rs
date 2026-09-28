// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Process exit codes for scripting and CI.
//!
//! Scripts and agents need to distinguish *why* a command failed. These codes give
//! a small, stable taxonomy on top of the usual 0/1. The binary's `main` maps
//! errors to these codes; a plain `anyhow` failure stays [`GENERIC_ERROR`].

use std::error::Error;
use std::fmt;

/// The command succeeded.
pub const SUCCESS: u8 = 0;
/// An unclassified failure.
pub const GENERIC_ERROR: u8 = 1;
/// Invalid command-line usage (produced by clap).
pub const USAGE: u8 = 2;
/// A change needs confirmation that could not be asked for (non-interactive, e.g.
/// `--json`, without `--yes`). Nothing was changed; re-run with `--yes` to apply.
pub const CONFIRMATION_REQUIRED: u8 = 3;
/// A requested resource was not found.
pub const NOT_FOUND: u8 = 4;
/// A network / connection error talking to the server.
pub const NETWORK: u8 = 5;
/// An authentication / authorization error.
pub const AUTH: u8 = 6;
/// The user declined a prompt, or a prompt was refused in non-interactive mode.
pub const ABORTED: u8 = 7;
/// The server returned a 5xx error.
pub const SERVER: u8 = 8;

/// Error returned when the user declines a confirmation prompt, or when a prompt is
/// refused because the CLI is running non-interactively. Maps to [`ABORTED`].
#[derive(Debug, Default, Clone, Copy)]
pub struct Aborted;

impl fmt::Display for Aborted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "User aborted")
    }
}

impl Error for Aborted {}

/// Error returned when a change needs confirmation but prompting is not possible
/// (non-interactive mode without `--yes`). Maps to [`CONFIRMATION_REQUIRED`].
#[derive(Debug, Default, Clone, Copy)]
pub struct ConfirmationRequired {
    /// The planned changes were already written to stdout (as the `--json` plan
    /// document), so the error reporter must not print anything else there.
    pub plan_emitted: bool,
}

impl fmt::Display for ConfirmationRequired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Confirmation required: nothing was changed. Re-run with --yes to apply, or with --dry-run to only preview the changes"
        )
    }
}

impl Error for ConfirmationRequired {}

/// Invalid input detected by the command itself (e.g. a malformed id), before asking
/// the server. Maps to [`USAGE`].
#[derive(Debug, Clone)]
pub struct BadInput(pub String);

impl fmt::Display for BadInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for BadInput {}

/// A requested resource does not exist (e.g. no state for a key, or a journal range
/// past the end). Maps to [`NOT_FOUND`].
#[derive(Debug, Clone)]
pub struct NotFound(pub String);

impl fmt::Display for NotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for NotFound {}

/// Returned by `--dry-run` once the planned changes have been shown: stops the command
/// before it changes anything, and exits with [`SUCCESS`] without printing an error.
#[derive(Debug, Default, Clone, Copy)]
pub struct DryRunComplete;

impl fmt::Display for DryRunComplete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Dry run complete: no changes were applied")
    }
}

impl Error for DryRunComplete {}

/// The command already wrote its full result (e.g. a `--json` document with
/// per-item outcomes) but still failed: exit with `code` without printing anything
/// else, so stdout stays a single document.
#[derive(Debug, Clone, Copy)]
pub struct AlreadyReported {
    pub code: u8,
}

impl fmt::Display for AlreadyReported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Command failed (exit code {})", self.code)
    }
}

impl Error for AlreadyReported {}

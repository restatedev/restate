// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Features in preview: new commands, options and output that are off unless `--preview` or
//! `RESTATECTL_PREVIEW=1` turns them on, so that they can be tried before they replace what's
//! there. Without preview, restatectl behaves as it did before them.
//!
//! - An option is in preview when it's under the [`HEADING`] help heading: it's hidden from the
//!   help and refused unless preview is on. That's all a new option needs.
//! - A command whose behavior changes keeps its previous implementation (in a `legacy` module
//!   next to it) and picks one with [`enabled`].
//! - Elsewhere, [`require`] refuses what needs preview, such as a new value of an option.
//!
//! Once a feature is out of preview, its options leave the heading and the legacy code goes.

use std::ffi::OsString;
use std::fmt::Display;
use std::sync::atomic::{AtomicU8, Ordering};

use clap::builder::FalseyValueParser;
use clap::error::ErrorKind;
use clap::parser::ValueSource;
use clap::{Arg, ArgMatches, Args, Command, Parser};
use cling::Collect;

const ENV: &str = "RESTATECTL_PREVIEW";
const FLAG: &str = "--preview";
const HINT: &str = "pass --preview or set RESTATECTL_PREVIEW=1 to use it";

/// The help heading of the options in preview.
pub const HEADING: &str = "Preview options";

/// How preview was turned on, if it was
static STATE: AtomicU8 = AtomicU8::new(OFF);
const OFF: u8 = 0;
const BY_ENV: u8 = 1;
const BY_FLAG: u8 = 2;

#[derive(Args, Collect, Clone, Default)]
pub struct PreviewOpts {
    /// Turn on the features in preview: the new output of `status`, `partitions list` and `logs
    /// list`, their live dashboard, and saving the status to a file. They may still change
    #[arg(
        long,
        global = true,
        env = ENV,
        value_parser = FalseyValueParser::new(),
        help_heading = "Global options"
    )]
    preview: bool,
}

/// Whether the features in preview are on.
pub fn enabled() -> bool {
    STATE.load(Ordering::Relaxed) != OFF
}

/// `restatectl <args>`, for a command to suggest that needs preview: with `--preview` when that's
/// how preview was turned on, so that the command runs as it's written.
pub fn command(args: impl Display) -> String {
    match STATE.load(Ordering::Relaxed) {
        BY_FLAG => format!("restatectl {FLAG} {args}"),
        _ => format!("restatectl {args}"),
    }
}

/// Fails unless preview is on; `what` names what needs it, e.g. "--sort health".
pub fn require(what: &str) -> anyhow::Result<()> {
    anyhow::ensure!(enabled(), "{what} is in preview; {HINT}");
    Ok(())
}

/// Parses the command line, from `RESTATECTL_PREVIEW` and `args`, into `T`: the options in
/// preview are hidden from the help and refused unless preview is on.
pub fn parse<T: Parser>() -> Result<T, clap::Error> {
    let env = std::env::var(ENV).ok();
    parse_from(std::env::args_os().collect(), env.as_deref())
}

fn parse_from<T: Parser>(args: Vec<OsString>, env: Option<&str>) -> Result<T, clap::Error> {
    // the help depends on it, so it's known before clap parses the arguments
    let mut flag = args.iter().skip(1).take_while(|arg| *arg != "--");
    let state = if env.is_some_and(|value| !falsey(value)) {
        BY_ENV
    } else if flag.any(|arg| arg == FLAG) {
        BY_FLAG
    } else {
        OFF
    };
    STATE.store(state, Ordering::Relaxed);
    let on = state != OFF;

    let mut cmd = hide(T::command(), !on);
    let mut matches = cmd.try_get_matches_from_mut(args)?;
    if !on {
        refuse(&cmd, &matches)?;
    }
    T::from_arg_matches_mut(&mut matches).map_err(|err| err.format(&mut cmd))
}

/// As clap reads the environment variable of a flag: false for n, no, f, false, off, 0 and empty.
fn falsey(value: &str) -> bool {
    let value = value.trim().to_ascii_lowercase();
    ["", "n", "no", "f", "false", "off", "0"].contains(&value.as_str())
}

fn in_preview(arg: &Arg) -> bool {
    arg.get_help_heading() == Some(HEADING)
}

/// Hides the options in preview of `cmd` and its subcommands from the help, or shows them.
fn hide(cmd: Command, hidden: bool) -> Command {
    cmd.mut_args(|arg| {
        if in_preview(&arg) {
            arg.hide(hidden)
        } else {
            arg
        }
    })
    .mut_subcommands(|sub| hide(sub, hidden))
}

/// Fails if `matches` has an option in preview, given on the command line or in the environment.
fn refuse(cmd: &Command, matches: &ArgMatches) -> Result<(), clap::Error> {
    for arg in cmd.get_arguments().filter(|arg| in_preview(arg)) {
        if let Some(ValueSource::CommandLine | ValueSource::EnvVariable) =
            matches.value_source(arg.get_id().as_str())
        {
            let name = match (arg.get_long(), arg.get_short()) {
                (Some(long), _) => format!("--{long}"),
                (None, Some(short)) => format!("-{short}"),
                (None, None) => format!("<{}>", arg.get_id().as_str().to_uppercase()),
            };
            return Err(cmd.clone().error(
                ErrorKind::ArgumentConflict,
                format!("{name} is in preview; {HINT}"),
            ));
        }
    }
    match matches.subcommand() {
        Some((name, sub_matches)) => match cmd.find_subcommand(name) {
            Some(sub) => refuse(sub, sub_matches),
            None => Ok(()),
        },
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CliApp;

    fn parse(args: &str, env: Option<&str>) -> Result<CliApp, String> {
        let args = args.split(' ').map(OsString::from).collect();
        parse_from(args, env).map_err(|err| err.to_string())
    }

    fn help(args: &str) -> String {
        parse(&format!("{args} --help"), None).err().unwrap()
    }

    /// Without preview, its options are hidden from the help and refused, the commands that
    /// changed run as before, and the rest is unaffected; with preview, all of them work.
    #[test]
    fn preview_options_need_preview() {
        let err = parse("restatectl status -w", None).err().unwrap();
        assert!(err.contains("-w is in preview; pass --preview"), "{err}");
        let err = parse("restatectl partitions list 1-3", Some("0"))
            .err()
            .unwrap();
        assert!(err.contains("<PARTITIONS> is in preview"), "{err}");
        // global options in preview are refused after the subcommand too
        let err = parse("restatectl logs list --plain", None).err().unwrap();
        assert!(err.contains("--plain is in preview"), "{err}");
        assert!(parse("restatectl status --extra", None).is_ok());
        assert!(!enabled());
        let status = help("restatectl status");
        assert!(
            status.contains("--extra") && !status.contains("Watch mode"),
            "{status}"
        );
        assert!(
            !status.contains(HEADING) && !status.contains("--plain\n"),
            "{status}"
        );

        assert!(parse("restatectl --preview status -w", None).is_ok());
        assert!(enabled());
        // suggested commands need the flag as well
        assert_eq!(command("logs list 3"), "restatectl --preview logs list 3");
        assert!(parse("restatectl partitions list 1-3 --plain -w", Some("1")).is_ok());
        assert!(enabled());
        assert_eq!(command("logs list 3"), "restatectl logs list 3");
        let status = help("restatectl status --preview");
        assert!(
            status.contains(HEADING) && status.contains("Watch mode"),
            "{status}"
        );
        assert!(require("--sort health").is_ok());
    }
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::large_futures)]

use std::io::Write;
use std::process::ExitCode;

use cling::prelude::*;
use crossterm::execute;
use rustls::crypto::aws_lc_rs;

use restate_cli::CliApp;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    // We need to install a crypto provider explicitly because the workspace hack activates the
    // ring as well aws_lc_rs rustls features. Unfortunately, these features are not additive. See
    // https://github.com/rustls/rustls/issues/1877. We can remove this line of code once all our
    // dependencies activate only one of the features or once rustls allows both features to be
    // activated.
    aws_lc_rs::default_provider()
        .install_default()
        .expect("no other default crypto provider being installed");
    let _ = ctrlc::set_handler(move || {
        if let Some(exit) = restate_cli::EXIT_HANDLER.lock().unwrap().as_ref() {
            (exit)()
        } else {
            // Showing cursor again if it was hidden by dialoguer.
            let mut stdout = std::io::stdout();
            let _ = execute!(
                stdout,
                crossterm::terminal::LeaveAlternateScreen,
                crossterm::cursor::Show,
                crossterm::style::ResetColor
            );

            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr);
            let _ = writeln!(stderr, "Ctrl-C pressed, aborting...");

            std::process::exit(1);
        }
    });

    // Parse outside cling (as `Cling::parse_and_run` does) to keep the failed command
    // around: it refines the next steps suggested on error.
    let app = match CliApp::try_parse() {
        Ok(app) => app,
        Err(err) => {
            let err = err.format(&mut CliApp::command());
            return restate_cli::report_error(err.into(), None);
        }
    };
    let command = app.cmd.clone();
    match Cling::new(app).run().await.result() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => restate_cli::report_error(err, Some(&command)),
    }
}

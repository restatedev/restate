// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use cling::prelude::*;
use crossterm::execute;

use restate_doctor::CliApp;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ClingFinished<CliApp> {
    // Increase file descriptor limit to handle large RocksDB databases with many SST files
    if rlimit::increase_nofile_limit(u64::MAX).is_err() {
        eprintln!("Warning: Failed to increase the number of open file descriptors limit.");
    }

    let _ = ctrlc::set_handler(move || {
        // Showing cursor again if it was hidden.
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
    });

    Cling::parse_and_run().await
}

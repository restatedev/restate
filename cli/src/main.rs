// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cling::prelude::*;

use restate_cli::CliApp;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ClingFinished<CliApp> {
    let _ = ctrlc::set_handler(move || {
        // Showning cursor again if it was hidden by dialoguer.
        let stdout = dialoguer::console::Term::stdout();
        let _ = stdout.write_line("");
        let _ = stdout.show_cursor();
    });

    Cling::parse_and_run().await
}

// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::CliApp;
use restate_cli_util::{completion_commands, completions::CompletionProvider};

// Use the completion_commands macro to generate standard completion structures
completion_commands!(CliApp);
impl CompletionProvider for CliApp {}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Restate's Web UI, embedded at build time from the zip in `assets/`. To update the UI, replace that
//! zip with the `ui-v<version>.zip` built by the web UI repo.

use include_dir::{Dir, include_dir};

pub static ASSETS: Dir<'_> = include_dir!("$OUT_DIR/assets");

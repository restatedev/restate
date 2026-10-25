// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;

use tempfile::TempDir;

/// Creates a [`TempDir`] that is cleaned up even if its destructor never runs.
///
/// `TempDir` only removes its directory on drop, which never happens for values held in statics
/// or when the process exits via [`std::process::exit`] (the `#[restate_core::test]` panic hook
/// does this when a test fails). Directories created here live under a per-process root that is
/// removed when the process exits, even if they were [`TempDir::keep`]-ed. A process killed by a
/// signal still leaves the root behind.
pub fn tempdir() -> io::Result<TempDir> {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();

    extern "C" fn remove_root() {
        if let Some(root) = ROOT.get() {
            let _ = std::fs::remove_dir_all(root);
        }
    }

    let root = ROOT.get_or_init(|| {
        let root = TempDir::new()
            .expect("to create the per-process test temp dir")
            .keep();
        // SAFETY: `remove_root` is a plain `extern "C" fn()` that does not unwind.
        unsafe { libc::atexit(remove_root) };
        root
    });
    TempDir::new_in(root)
}

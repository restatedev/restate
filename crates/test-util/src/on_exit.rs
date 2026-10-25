// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cleanup that runs even when destructors don't: values held in statics are never dropped, and
//! the `#[restate_core::test]` panic hook ends a failing test via [`std::process::exit`]. Both
//! still run `atexit` handlers, which is where the cleanup happens. A process killed by a signal
//! runs neither.

use std::io;
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::{Mutex, PoisonError};
use std::sync::{Once, OnceLock};

use tempfile::TempDir;

static TEMP_ROOT: OnceLock<PathBuf> = OnceLock::new();
#[cfg(unix)]
static CHILDREN: Mutex<Vec<libc::pid_t>> = Mutex::new(Vec::new());

/// Creates a [`TempDir`] that is cleaned up even if its destructor never runs.
///
/// Directories created here live under a per-process root that is removed when the process
/// exits, even if they were [`TempDir::keep`]-ed.
pub fn tempdir() -> io::Result<TempDir> {
    let root = TEMP_ROOT.get_or_init(|| {
        let root = TempDir::new()
            .expect("to create the per-process test temp dir")
            .keep();
        register_exit_hook();
        root
    });
    TempDir::new_in(root)
}

/// Sends SIGKILL to the child process `pid` when this process exits, before the directories
/// created by [`tempdir`] are removed, unless the returned guard was dropped by then.
///
/// Drop the guard as soon as the child has been reaped: its pid may be reused afterwards.
#[cfg(unix)]
pub fn kill_on_exit(pid: u32) -> KillOnExit {
    let pid = libc::pid_t::try_from(pid).expect("pid to fit into pid_t");
    register_exit_hook();
    CHILDREN
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(pid);
    KillOnExit(pid)
}

/// Guard returned by [`kill_on_exit`].
#[cfg(unix)]
#[must_use = "the child is not killed at exit once the guard is dropped"]
pub struct KillOnExit(libc::pid_t);

#[cfg(unix)]
impl Drop for KillOnExit {
    fn drop(&mut self) {
        CHILDREN
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|pid| *pid != self.0);
    }
}

fn register_exit_hook() {
    static REGISTER: Once = Once::new();
    // SAFETY: `on_exit` is a plain `extern "C" fn()` that does not unwind.
    REGISTER.call_once(|| unsafe {
        libc::atexit(on_exit);
    });
}

extern "C" fn on_exit() {
    // Children go first, they might still be writing into the temp dirs.
    #[cfg(unix)]
    {
        let children = CHILDREN.lock().unwrap_or_else(PoisonError::into_inner);
        for &pid in children.iter() {
            // SAFETY: plain syscalls; `pid` belongs to a child that has not been reaped yet.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
                // fails with ECHILD if the runtime reaped it concurrently, which is fine too
                libc::waitpid(pid, std::ptr::null_mut(), 0);
            }
        }
    }
    if let Some(root) = TEMP_ROOT.get() {
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::process::Command;
    use std::time::{Duration, Instant};

    const PROBE_ENV: &str = "RESTATE_TEST_UTIL_ON_EXIT_PROBE";

    /// Runs this test in a child process that exits like a failing `#[restate_core::test]` does,
    /// and checks that its temp dir and its own child process are gone afterwards.
    #[test]
    fn cleans_up_on_process_exit() {
        if std::env::var_os(PROBE_ENV).is_some() {
            let dir = super::tempdir().unwrap().keep();
            std::fs::write(dir.join("data"), b"x").unwrap();
            let child = Command::new("sleep").arg("60").spawn().unwrap();
            let _guard = super::kill_on_exit(child.id());
            println!("PROBE {} {}", dir.display(), child.id());
            std::process::exit(1);
        }

        let started = Instant::now();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "on_exit::tests::cleans_up_on_process_exit",
                "--nocapture",
            ])
            .env(PROBE_ENV, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let (dir, pid) = stdout
            .lines()
            .find_map(|line| line.split_once("PROBE ")?.1.split_once(' '))
            .unwrap_or_else(|| panic!("probe did not report its state: {stdout}"));

        assert_eq!(output.status.code(), Some(1));
        // the exit hook waits for the child, which would sleep for 60s if it wasn't killed
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "pid {pid} was not killed"
        );
        assert!(!std::path::Path::new(dir).exists(), "{dir} was not removed");
        // the probe reaped its child, so the pid is gone rather than a zombie
        let pid: libc::pid_t = pid.parse().unwrap();
        // SAFETY: signal 0 only checks whether the process exists.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "pid {pid} still exists");
    }
}

// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::sync::{Arc, Mutex};

use std::sync::OnceLock;

const BUF_INITIAL_CAPACITY: usize = 2048;

static GLOBAL_STDOUT_CONSOLE: OnceLock<Console> = OnceLock::new();
static GLOBAL_STDERR_CONSOLE: OnceLock<Console> = OnceLock::new();

pub fn set_stdout(out: Console) {
    let _ = GLOBAL_STDOUT_CONSOLE.set(out);
}

pub fn set_stderr(err: Console) {
    let _ = GLOBAL_STDERR_CONSOLE.set(err);
}

pub fn stdout() -> Console {
    let out = GLOBAL_STDOUT_CONSOLE.get_or_init(Console::stdout);
    out.clone()
}

pub fn stderr() -> Console {
    let err = GLOBAL_STDERR_CONSOLE.get_or_init(Console::stderr);
    err.clone()
}

#[derive(Debug, Clone)]
pub struct Console {
    inner: Arc<BufferedOutput>,
}

#[derive(Debug)]
enum BufferedOutput {
    Stdout,
    Stderr,
    Memory(Mutex<String>),
}

impl Console {
    pub fn stdout() -> Self {
        Self {
            inner: Arc::new(BufferedOutput::Stdout),
        }
    }

    pub fn stderr() -> Self {
        Self {
            inner: Arc::new(BufferedOutput::Stderr),
        }
    }

    pub fn in_memory() -> Self {
        Self {
            inner: Arc::new(BufferedOutput::Memory(Mutex::new(String::with_capacity(
                BUF_INITIAL_CAPACITY,
            )))),
        }
    }

    pub fn take_buffer(&self) -> Option<String> {
        if let BufferedOutput::Memory(ref buffer) = *self.inner {
            Some(std::mem::take(&mut buffer.lock().unwrap()))
        } else {
            None
        }
    }
}

impl Write for Console {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        use std::io::Write;
        match *self.inner {
            BufferedOutput::Stdout => std::io::stdout()
                .write_all(s.as_bytes())
                .map_err(|_| std::fmt::Error),
            BufferedOutput::Stderr => std::io::stderr()
                .write_all(s.as_bytes())
                .map_err(|_| std::fmt::Error),
            BufferedOutput::Memory(ref buf) => {
                let mut guard = buf.lock().unwrap();
                write!(guard, "{s}")
            }
        }
    }
}

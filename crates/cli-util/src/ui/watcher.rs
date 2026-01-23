// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use cling::Collect;
use crossterm::style::ResetColor;
use crossterm::terminal::{
    BeginSynchronizedUpdate, Clear, ClearType, EndSynchronizedUpdate, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use crossterm::{cursor, execute, queue};

use crate::ui::console::Icon;

use super::output::Console;

#[derive(clap::Args, Clone, Collect, Debug)]
pub struct Watch {
    /// Watch mode. Continuously refreshing the output.
    #[clap(short, global = true)]
    watch: bool,
    /// Watch interval in seconds
    #[clap(short = 'n', default_value = "1.0")]
    interval: f32,
}

impl Watch {
    pub async fn run<F, O>(&self, mut what: F) -> Result<()>
    where
        F: FnMut() -> O,
        O: Future<Output = Result<()>>,
    {
        if !self.watch {
            return what().await;
        }
        // Redirect console output to in-memory buffer
        let console = Console::in_memory();
        crate::ui::output::set_stdout(console.clone());
        crate::ui::output::set_stderr(console);

        queue!(
            std::io::stdout(),
            cursor::Hide,
            EnterAlternateScreen,
            Clear(ClearType::All),
            ResetColor
        )?;

        let mut interval = tokio::time::interval(Duration::from_secs_f32(self.interval));
        loop {
            interval.tick().await;
            match what().await {
                Ok(_) => print_the_output(self.interval)?,
                Err(e) => {
                    handle_error();
                    return Err(e);
                }
            }
        }
    }
}

fn handle_error() {
    use std::io::Write;
    let mut stdout = std::io::stdout();
    let _ = execute!(stdout, cursor::Show, LeaveAlternateScreen, ResetColor);
    // Print everything we buffered and clear the buffer
    if let Some(b) = crate::ui::output::stdout().take_buffer() {
        let _ = write!(stdout, "{b}");
    }
}

fn print_the_output(interval: f32) -> Result<()> {
    use std::io::Write;
    let (_, rows) = crossterm::terminal::size()?;
    let mut lock = std::io::stdout().lock();
    queue!(lock, cursor::MoveTo(0, 0), BeginSynchronizedUpdate,)?;
    let _ = queue!(lock, Clear(ClearType::CurrentLine));
    let _ = writeln!(lock, "{} Refreshing every {}s.", Icon("👀", ""), interval);

    if let Some(b) = crate::ui::output::stdout().take_buffer() {
        // truncate to fit the screen
        for line in b.lines() {
            let (_, cur_row) = crossterm::cursor::position()?;
            if cur_row >= rows - 2 {
                // enough printing...
                writeln!(lock, "(output truncated to fit screen)")?;
                queue!(lock, Clear(ClearType::UntilNewLine))?;
                break;
            }
            let _ = writeln!(lock, "{line}");
            queue!(lock, Clear(ClearType::UntilNewLine))?;
        }
    }

    queue!(
        lock,
        Clear(ClearType::FromCursorDown),
        EndSynchronizedUpdate
    )?;
    let _ = lock.flush();
    Ok(())
}

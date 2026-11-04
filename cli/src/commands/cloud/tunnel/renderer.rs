// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hash::Hasher;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::{io::Write, sync::Arc};

use arc_swap::ArcSwapOption;
use comfy_table::{Cell, Table};
use crossterm::cursor::MoveTo;
use crossterm::execute;
use crossterm::{
    queue,
    terminal::{BeginSynchronizedUpdate, Clear, ClearType},
};

use restate_cli_util::ui::console::StyledTable;
use restate_cli_util::ui::output::Console;
use restate_cli_util::ui::stylesheet;
use restate_cli_util::{CliContext, c_indent_table, c_println, c_tip, c_warn};

use super::remote::RemotePort;

pub(crate) struct TunnelRenderer {
    last_hash: AtomicU64,
    pub local: LocalRenderer,
    pub remote: Vec<RemoteRenderer>,
    last_error: ArcSwapOption<String>,
}

impl TunnelRenderer {
    pub(crate) fn new(
        tunnel_name: String,
        environment_name: String,
        remote_ports: &[RemotePort],
    ) -> std::io::Result<Self> {
        // Redirect console output to in-memory buffer
        let console = Console::in_memory();
        restate_cli_util::ui::output::set_stdout(console.clone());
        restate_cli_util::ui::output::set_stderr(console);

        queue!(
            std::io::stdout(),
            crossterm::cursor::SavePosition,
            crossterm::terminal::EnterAlternateScreen,
            crossterm::terminal::DisableLineWrap,
            crossterm::cursor::Hide
        )?;
        Ok(Self {
            last_hash: AtomicU64::default(),
            local: LocalRenderer::new(tunnel_name, environment_name),
            remote: remote_ports
                .iter()
                .map(|p| RemoteRenderer::new(*p))
                .collect(),
            last_error: ArcSwapOption::empty(),
        })
    }

    pub(crate) fn store_error<E: ToString>(&self, error: E) {
        self.last_error.store(Some(Arc::new(error.to_string())));
        self.render()
    }

    pub(crate) fn clear_error(&self) {
        self.last_error.store(None);
        self.render()
    }

    pub(crate) fn render(&self) {
        let mut stdout = std::io::stdout();

        let (_, rows) = if let Ok(size) = crossterm::terminal::size() {
            size
        } else {
            return;
        };
        let rows = rows as usize;

        let mut tunnel_table = Table::new_styled();

        tunnel_table.set_header(vec![
            comfy_table::Cell::new(format!(" {} ", stylesheet::TIP_ICON))
                .set_alignment(comfy_table::CellAlignment::Center),
            Cell::new("Source").add_attribute(comfy_table::Attribute::Bold),
            Cell::new(" → ").add_attribute(comfy_table::Attribute::Bold),
            comfy_table::Cell::new(format!(" {} ", stylesheet::TIP_ICON))
                .set_alignment(comfy_table::CellAlignment::Center),
            Cell::new("Destination").add_attribute(comfy_table::Attribute::Bold),
        ]);

        let (connected_count, target_connected_count) = self.local.connected_count();
        if target_connected_count > 0 {
            let tunnel_color = if connected_count == target_connected_count {
                comfy_table::Color::Green
            } else if connected_count == 0 {
                comfy_table::Color::Red
            } else {
                comfy_table::Color::Yellow
            };
            tunnel_table.add_row(vec![
                comfy_table::Cell::new(format!(" {} ", stylesheet::HANDSHAKE_ICON))
                    .set_alignment(comfy_table::CellAlignment::Center),
                Cell::new(format!(
                    "tunnel://{} ({connected_count}/{target_connected_count} connected)",
                    self.local.tunnel_name,
                ))
                .fg(tunnel_color),
                " → ".into(),
                comfy_table::Cell::new(format!(" {} ", stylesheet::HOME_ICON))
                    .set_alignment(comfy_table::CellAlignment::Center),
                "your machine".into(),
            ]);
        }

        for remote in self.remote.iter() {
            tunnel_table.add_row(vec![
                comfy_table::Cell::new(format!(" {} ", stylesheet::HOME_ICON))
                    .set_alignment(comfy_table::CellAlignment::Center),
                format!("http://localhost:{}", u16::from(remote.port))
                    .as_str()
                    .into(),
                " → ".into(),
                comfy_table::Cell::new(format!(
                    " {} ",
                    match remote.port {
                        RemotePort::Admin => stylesheet::LOCK_ICON,
                        RemotePort::Ingress => stylesheet::GLOBE_ICON,
                    }
                ))
                .set_alignment(comfy_table::CellAlignment::Center),
                match remote.port {
                    RemotePort::Admin => "admin API",
                    RemotePort::Ingress => "public ingress",
                }
                .into(),
            ]);
        }

        c_indent_table!(0, tunnel_table);
        c_println!();

        if let Some(last_error) = self.last_error.load().as_deref() {
            c_warn!("Error: {last_error}")
        }

        if self.local.target_connected.load(Ordering::Relaxed) > 0 {
            c_tip!(
                "To discover a local service:\nrestate deployments register --tunnel-name {} http://localhost:9080\nThe deployment is only reachable from this Restate Cloud environment ({}).",
                self.local.tunnel_name,
                self.local.environment_name,
            );
        }

        let b = if let Some(b) = restate_cli_util::ui::output::stdout().take_buffer() {
            b
        } else {
            return;
        };

        let mut output = "restate cloud environment tunnel - Press Ctrl-C to exit".to_string();
        let line_count = b.lines().count();

        let logo = restate_types::art::render_restate_logo(CliContext::get().colors_enabled());
        let logo_line_count = logo.lines().count();

        if line_count + logo_line_count + 3 < rows {
            output.reserve_exact(1 + logo.len() + 1 + b.len());
            output.push('\n');
            output.push_str(&logo);
            output.push('\n');
            output.push_str(&b);
        } else {
            output.reserve_exact(2 + b.len());
            output.push('\n');
            output.push('\n');
            output.push_str(&b);
        };

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hasher.write(output.as_bytes());
        hasher.write_u8(0xFF);
        let hash = hasher.finish();

        if self.last_hash.swap(hash, Ordering::Relaxed) == hash {
            // no change; avoid rewriting everything
            return;
        }

        let mut lock = std::io::stdout().lock();

        let _ = execute!(
            lock,
            BeginSynchronizedUpdate,
            MoveTo(0, 0),
            Clear(ClearType::All)
        );

        for line in output.lines() {
            let _ = writeln!(lock, "{line}");
        }

        let _ = execute!(stdout, crossterm::terminal::EndSynchronizedUpdate,);
    }
}

pub(crate) struct LocalRenderer {
    pub tunnel_name: String,
    pub environment_name: String,
    pub connected: AtomicU64,
    pub target_connected: AtomicU8,
}

impl LocalRenderer {
    pub(crate) fn new(tunnel_name: String, environment_name: String) -> Self {
        Self {
            tunnel_name,
            environment_name,
            connected: AtomicU64::new(0),
            target_connected: AtomicU8::new(0),
        }
    }

    pub(crate) fn set_connected(&self, tunnel_index: usize, set: bool) {
        if tunnel_index >= 64 {
            return;
        }

        if set {
            self.connected
                .fetch_or(1 << tunnel_index, Ordering::Relaxed);
        } else {
            self.connected
                .fetch_and(!(1 << tunnel_index), Ordering::Relaxed);
        }
    }

    fn connected_count(&self) -> (u8, u8) {
        let target_connected_count = self.target_connected.load(Ordering::Relaxed);
        if target_connected_count == 0 {
            return (0, 0);
        }

        let connected_bitmap = self.connected.load(Ordering::Relaxed);
        let connected_count = connected_bitmap.count_ones() as u8;
        (connected_count, target_connected_count)
    }
}

impl Drop for TunnelRenderer {
    fn drop(&mut self) {
        execute!(
            std::io::stdout(),
            crossterm::terminal::LeaveAlternateScreen,
            crossterm::cursor::Show,
            crossterm::terminal::EnableLineWrap,
            crossterm::cursor::RestorePosition,
            crossterm::cursor::MoveToPreviousLine(1),
        )
        .unwrap();
        restate_cli_util::ui::output::set_stdout(Console::stdout());
        restate_cli_util::ui::output::set_stderr(Console::stderr());
        println!();
    }
}

pub(crate) struct RemoteRenderer {
    port: RemotePort,
}

impl RemoteRenderer {
    pub(crate) fn new(port: RemotePort) -> Self {
        Self { port }
    }
}

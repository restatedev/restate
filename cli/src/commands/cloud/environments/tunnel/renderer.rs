use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{io::Write, sync::Arc};

use std::sync::OnceLock;

use arc_swap::ArcSwapOption;
use comfy_table::{Cell, Table};

use crossterm::cursor::MoveTo;
use crossterm::execute;
use crossterm::{
    queue,
    terminal::{BeginSynchronizedUpdate, Clear, ClearType},
};

use crate::app::TableStyle;
use crate::ui::stylesheet;
use crate::{
    app::UiConfig, c_indent_table, c_println, c_tip, c_warn, console::StyledTable,
    ui::output::Console,
};

use super::remote::RemotePort;

pub(crate) struct TunnelRenderer {
    last_hash: AtomicU64,
    pub local: OnceLock<LocalRenderer>,
    pub remote: Vec<RemoteRenderer>,
    last_error: ArcSwapOption<String>,
}

impl TunnelRenderer {
    pub(crate) fn new(remote_ports: &[RemotePort]) -> std::io::Result<Self> {
        // Redirect console output to in-memory buffer
        let console = Console::in_memory();
        crate::ui::output::set_stdout(console.clone());
        crate::ui::output::set_stderr(console);

        queue!(
            std::io::stdout(),
            crossterm::cursor::SavePosition,
            crossterm::terminal::EnterAlternateScreen,
            crossterm::terminal::DisableLineWrap,
            crossterm::cursor::Hide
        )?;
        Ok(Self {
            last_hash: AtomicU64::default(),
            local: OnceLock::new(),
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

        let mut tunnel_table = Table::new_styled(&UiConfig {
            table_style: TableStyle::Compact,
        });

        tunnel_table.set_header(vec![
            comfy_table::Cell::new(format!(" {} ", stylesheet::TIP_ICON))
                .set_alignment(comfy_table::CellAlignment::Center),
            Cell::new("Source").add_attribute(comfy_table::Attribute::Bold),
            Cell::new(" → ").add_attribute(comfy_table::Attribute::Bold),
            comfy_table::Cell::new(format!(" {} ", stylesheet::TIP_ICON))
                .set_alignment(comfy_table::CellAlignment::Center),
            Cell::new("Destination").add_attribute(comfy_table::Attribute::Bold),
        ]);

        if let Some(local) = self.local.get() {
            tunnel_table.add_row(vec![
                comfy_table::Cell::new(format!(" {} ", stylesheet::HANDSHAKE_ICON))
                    .set_alignment(comfy_table::CellAlignment::Center),
                format!("tunnel://{}:{}", local.tunnel_name, local.proxy_port)
                    .as_str()
                    .into(),
                " → ".into(),
                comfy_table::Cell::new(format!(" {} ", stylesheet::HOME_ICON))
                    .set_alignment(comfy_table::CellAlignment::Center),
                format!("http://localhost:{}", local.port).into(),
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

        if let Some(local) = self.local.get() {
            c_tip!(
                "To discover:\nrestate deployments register tunnel://{}:{}\nThe deployment is only reachable from this Restate Cloud environment ({}).",
                local.tunnel_name,
                local.proxy_port,
                local.environment_name,
            );
        }

        let b = if let Some(b) = crate::ui::output::stdout().take_buffer() {
            b
        } else {
            return;
        };

        let mut output = "restate cloud environment tunnel - Press Ctrl-C to exit".to_string();
        let line_count = b.lines().count();

        let logo = restate_types::art::render_restate_logo(crate::console::colors_enabled());
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
            let _ = writeln!(lock, "{}", line);
        }

        let _ = execute!(stdout, crossterm::terminal::EndSynchronizedUpdate,);
    }
}

pub(crate) struct LocalRenderer {
    pub tunnel_url: String,
    pub proxy_port: u16,
    pub tunnel_name: String,
    pub environment_name: String,
    pub port: u16,
}

impl LocalRenderer {
    pub(crate) fn new(
        proxy_port: u16,
        tunnel_url: String,
        tunnel_name: String,
        environment_name: String,
        port: u16,
    ) -> Self {
        Self {
            proxy_port,
            tunnel_url,
            tunnel_name,
            environment_name,
            port,
        }
    }

    pub(crate) fn into_tunnel_details(mut self) -> (String, String, u16) {
        (
            std::mem::take(&mut self.tunnel_url),
            std::mem::take(&mut self.tunnel_name),
            self.port,
        )
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
        crate::ui::output::set_stdout(Console::stdout());
        crate::ui::output::set_stderr(Console::stderr());
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

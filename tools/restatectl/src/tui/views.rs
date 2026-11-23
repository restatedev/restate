// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The rich output of the one-shot commands, printed into the scrollback.

use chrono::{DateTime, Local};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Widget;

use restate_cli_util::c_println;

use super::theme;
use super::to_ansi;
use super::widgets::{
    Findings, HEADER_HEIGHT, Header, NodeCards, PartitionTiles, PlacementBars, RichTable,
    logs_table, metadata_table, nodes_table, partitions_table, replicas_table, show_node_cards,
};
use crate::observe::{
    ClusterView, Finding, LogPlacement, LogView, PartitionPlacement, PartitionView,
};
use crate::render::{FindingLine, Verdict, group_findings, snapshot_taken};

/// Rendered output, a line at a time.
pub struct Page {
    width: u16,
    lines: Vec<String>,
}

impl Page {
    pub fn new(width: u16) -> Self {
        Self {
            width,
            lines: Vec::new(),
        }
    }

    /// Renders `widget` into a `width` × `height` area.
    pub fn add(&mut self, widget: impl Widget, width: u16, height: u16) {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        widget.render(area, &mut buffer);
        self.lines.extend(to_ansi(&buffer));
    }

    /// Adds `table` as wide as its content, up to the page's width.
    pub fn table(&mut self, table: RichTable) {
        let height = table.height();
        let width = table.width().min(self.width);
        self.add(table, width, height);
    }

    fn findings(&mut self, findings: &[Finding]) {
        self.finding_lines(group_findings(findings, 3));
    }

    pub fn finding_lines(&mut self, lines: Vec<FindingLine>) {
        let findings = Findings {
            lines,
            scroll: 0,
            selected: None,
        };
        let height = findings.height(self.width);
        self.add(findings, self.width, height);
    }

    pub fn print(self) {
        for line in self.lines {
            c_println!("{line}");
        }
    }

    #[cfg(test)]
    pub fn into_lines(self) -> Vec<String> {
        self.lines
    }
}

fn captured(view: &ClusterView) -> Line<'static> {
    let at: DateTime<Local> = view.captured_at.into();
    let text = match &view.file {
        Some(_) => snapshot_taken(view.captured_at),
        None => at.format("%H:%M:%S").to_string(),
    };
    Line::styled(format!(" {text} "), theme::muted())
}

pub fn status(view: &ClusterView, findings: &[Finding], extra: bool, width: u16) -> Page {
    let mut page = Page::new(width);
    page.add(
        Header {
            view,
            verdict: Verdict::of(findings),
            status: captured(view),
        },
        width,
        HEADER_HEIGHT,
    );

    if show_node_cards(view) {
        let cards = NodeCards {
            view,
            selected: None,
        };
        let height = cards.height(width);
        page.add(cards, width, height);
    } else {
        page.table(nodes_table(
            view,
            &view.nodes.iter().collect::<Vec<_>>(),
            false,
        ));
    }

    if !view.partitions.is_empty() {
        let tiles = PartitionTiles {
            view,
            partitions: view.partitions.iter().collect(),
            selected: None,
        };
        let height = tiles.height(width);
        page.add(tiles, width, height);
    }

    if extra {
        page.table(metadata_table(view));
        page.table(logs_table(view, &view.logs));
        page.table(partitions_table(view, &view.partitions, false));
    }

    page.findings(findings);
    page
}

pub fn partitions(
    view: &ClusterView,
    partitions: &[&PartitionView],
    findings: &[Finding],
    placement: Option<&PartitionPlacement>,
    replicas: bool,
    extra: bool,
    width: u16,
) -> Page {
    let mut page = Page::new(width);
    page.table(if replicas {
        replicas_table(partitions, extra, view.captured_at)
    } else {
        partitions_table(view, partitions.iter().copied(), extra)
    });
    if let Some(placement) = placement {
        let bars = PlacementBars::partitions(placement);
        let height = bars.height();
        page.add(bars, width.min(130), height);
    }
    if !findings.is_empty() {
        page.findings(findings);
    }
    page
}

pub fn logs(
    view: &ClusterView,
    logs: &[&LogView],
    findings: &[Finding],
    placement: Option<&LogPlacement>,
    width: u16,
) -> Page {
    let mut page = Page::new(width);
    page.table(logs_table(view, logs.iter().copied()));
    if let Some(placement) = placement {
        let bars = PlacementBars::logs(placement);
        let height = bars.height();
        page.add(bars, width.min(130), height);
    }
    if !findings.is_empty() {
        page.findings(findings);
    }
    page
}

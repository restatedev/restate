// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The live dashboard of watch mode: the cluster refreshed in the background, views to switch
//! between, a detail pane for the selected partition, log or node, and rates over time.

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use itertools::Itertools;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Sparkline, TableState, Tabs, Wrap};
use tokio::sync::{Notify, mpsc};

use restate_types::PlainNodeId;

use super::theme::{self, AMBER, BLUE, GREEN, RED};
use super::widgets::{
    Findings, HEADER_HEIGHT, Header, NodeCards, NodeTiles, PartitionTiles, PlacementBars,
    RichTable, SortedBy, chain_view, generational_span, logs_table, node_span, nodes_table, panel,
    partitions_table, replicas_table, show_node_cards,
};
use crate::connection::ConnectionInfo;
use crate::observe::{
    ClusterView, Finding, LocatedAddress, LogPlacement, LogView, NodeView, PartitionHealth,
    PartitionPlacement, PartitionView, ReaderPhase, SequencerState, Severity, Subject, analyze,
    short_duration,
};
use crate::render::{Verdict, compress_ids, group_findings};

/// Samples of the rates kept for the sparklines.
const HISTORY: usize = 120;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Overview,
    Partitions,
    Logs,
    Nodes,
    Findings,
}

const TABS: [Tab; 5] = [
    Tab::Overview,
    Tab::Partitions,
    Tab::Logs,
    Tab::Nodes,
    Tab::Findings,
];

pub struct Options {
    pub tab: Tab,
    pub interval: Duration,
    pub unhealthy_only: bool,
    pub partition_sort: PartitionSort,
}

/// What a list can be ordered by. Each key has a natural direction: ids ascending, the worst
/// or the largest first.
trait SortKey: Copy + PartialEq + 'static {
    type Item;
    const ALL: &'static [Self];
    /// the table column showing the key
    fn column(self) -> usize;
    /// whether the natural direction puts the largest first
    fn descending(self) -> bool;
    fn compare(self, a: &Self::Item, b: &Self::Item) -> Ordering;
}

/// A list's order: a key, in its natural direction or reversed.
#[derive(Clone, Copy)]
struct Sorting<K> {
    key: K,
    reversed: bool,
}

impl<K: SortKey> Sorting<K> {
    fn new(key: K) -> Self {
        Self {
            key,
            reversed: false,
        }
    }

    /// The next key, in its natural direction.
    fn cycle(&mut self) {
        let at = K::ALL.iter().position(|k| *k == self.key).unwrap_or(0);
        self.key = K::ALL[(at + 1) % K::ALL.len()];
        self.reversed = false;
    }

    fn sort(&self, items: &mut [&K::Item]) {
        items.sort_by(|a, b| {
            let order = self.key.compare(a, b);
            if self.reversed {
                order.reverse()
            } else {
                order
            }
        });
    }

    /// The column to highlight in the table's header, with the direction the rows go.
    fn sorted_by(&self) -> SortedBy {
        SortedBy {
            column: self.key.column(),
            descending: self.key.descending() != self.reversed,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PartitionSort {
    Partition,
    Health,
    Leader,
    Lag,
    Epoch,
}

impl SortKey for PartitionSort {
    type Item = PartitionView;
    const ALL: &'static [Self] = &[
        Self::Partition,
        Self::Health,
        Self::Leader,
        Self::Lag,
        Self::Epoch,
    ];

    fn column(self) -> usize {
        match self {
            Self::Partition => 0,
            Self::Health => 1,
            Self::Leader => 2,
            Self::Epoch => 3,
            Self::Lag => 6,
        }
    }

    fn descending(self) -> bool {
        !matches!(self, Self::Partition | Self::Leader)
    }

    fn compare(self, a: &PartitionView, b: &PartitionView) -> Ordering {
        let lag = |p: &PartitionView| {
            p.replicas
                .iter()
                .filter(|r| p.counts(r))
                .filter_map(|r| r.lag)
                .max()
        };
        match self {
            Self::Partition => Ordering::Equal,
            Self::Health => b.health.cmp(&a.health),
            Self::Leader => a.leader.cmp(&b.leader),
            Self::Lag => lag(b).cmp(&lag(a)),
            Self::Epoch => b.epoch.cmp(&a.epoch),
        }
        .then(a.id.cmp(&b.id))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LogSort {
    Log,
    State,
    Segments,
    Retained,
    Sequencer,
}

impl SortKey for LogSort {
    type Item = LogView;
    const ALL: &'static [Self] = &[
        Self::Log,
        Self::State,
        Self::Segments,
        Self::Retained,
        Self::Sequencer,
    ];

    fn column(self) -> usize {
        match self {
            Self::Log => 0,
            Self::State => 1,
            Self::Segments => 2,
            Self::Retained => 6,
            Self::Sequencer => 7,
        }
    }

    fn descending(self) -> bool {
        !matches!(self, Self::Log | Self::Sequencer)
    }

    fn compare(self, a: &LogView, b: &LogView) -> Ordering {
        // how much attention the log needs, as its state column shows
        let rank = |l: &LogView| {
            if l.is_down() {
                3
            } else if l.sealed || l.sequencer_state == Some(SequencerState::Restarted) {
                2
            } else if l.replication_factor.is_some() && l.writable_nodes < l.nodeset.len() {
                1
            } else {
                0
            }
        };
        match self {
            Self::Log => Ordering::Equal,
            Self::State => rank(b).cmp(&rank(a)),
            Self::Segments => b.segments.cmp(&a.segments),
            Self::Retained => b.retained.cmp(&a.retained),
            Self::Sequencer => a.sequencer.cmp(&b.sequencer),
        }
        .then(a.id.cmp(&b.id))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeSort {
    Node,
    Status,
    Leaders,
    Replicas,
    Sequencers,
    Uptime,
}

impl SortKey for NodeSort {
    type Item = NodeView;
    const ALL: &'static [Self] = &[
        Self::Node,
        Self::Status,
        Self::Leaders,
        Self::Replicas,
        Self::Sequencers,
        Self::Uptime,
    ];

    fn column(self) -> usize {
        match self {
            Self::Node => 0,
            Self::Status => 2,
            Self::Uptime => 3,
            // the leadership bar shows the replicas a node leads and follows
            Self::Replicas => 5,
            Self::Leaders => 6,
            Self::Sequencers => 8,
        }
    }

    fn descending(self) -> bool {
        !matches!(self, Self::Node | Self::Uptime)
    }

    fn compare(self, a: &NodeView, b: &NodeView) -> Ordering {
        let replicas = |n: &NodeView| n.partitions.leaders + n.partitions.followers;
        match self {
            Self::Node => Ordering::Equal,
            // the status column's health travels with the node, see `node_health`
            Self::Status => node_rank(b).cmp(&node_rank(a)),
            Self::Leaders => b.partitions.leaders.cmp(&a.partitions.leaders),
            Self::Replicas => replicas(b).cmp(&replicas(a)),
            Self::Sequencers => b.logs.sequencer.cmp(&a.logs.sequencer),
            // dead nodes have no uptime, and come first with the most recent restarts
            Self::Uptime => a.uptime_s.cmp(&b.uptime_s),
        }
        .then(a.plain().cmp(&b.plain()))
    }
}

/// How much attention a node needs: dead, unreachable, then the rest.
fn node_rank(node: &NodeView) -> u8 {
    match node.liveness {
        crate::observe::Liveness::Dead | crate::observe::Liveness::Unknown => 2,
        _ if node.seen_versions.is_none() => 1,
        _ => 0,
    }
}

/// Runs the dashboard until the user quits.
pub async fn run(connection: &ConnectionInfo, options: Options) -> anyhow::Result<()> {
    let (updates_tx, mut updates) = mpsc::channel(1);
    let refresh = Arc::new(Notify::new());
    let fetcher = tokio::spawn(fetch_loop(
        connection.clone(),
        options.interval,
        updates_tx,
        Arc::clone(&refresh),
    ));

    let (keys_tx, mut keys) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        while !keys_tx.is_closed() {
            if event::poll(Duration::from_millis(100)).unwrap_or(false)
                && let Ok(event) = event::read()
                && keys_tx.send(event).is_err()
            {
                break;
            }
        }
    });

    let mut terminal = ratatui::try_init()?;
    // restores the terminal however the loop ends
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            ratatui::restore();
        }
    }
    let _restore = Restore;

    let mut app = App::new(options);
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        terminal.draw(|frame| app.draw(frame))?;
        tokio::select! {
            Some(update) = updates.recv() => app.update(update),
            Some(event) = keys.recv() => {
                if let Event::Key(key) = event
                    && key.kind == KeyEventKind::Press
                {
                    match app.on_key(key) {
                        Action::Quit => break,
                        Action::Refresh => refresh.notify_one(),
                        Action::None => {}
                    }
                }
            }
            _ = tick.tick() => {}
        }
    }
    fetcher.abort();
    Ok(())
}

type Update = Result<(ClusterView, Vec<Finding>), String>;

async fn fetch_loop(
    connection: ConnectionInfo,
    interval: Duration,
    updates: mpsc::Sender<Update>,
    refresh: Arc<Notify>,
) {
    loop {
        connection.reset().await;
        let update = ClusterView::fetch(&connection)
            .await
            .map(|view| {
                let findings = analyze(&view);
                (view, findings)
            })
            .map_err(|err| err.to_string());
        if updates.send(update).await.is_err() {
            return;
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = refresh.notified() => {}
        }
    }
}

enum Action {
    None,
    Quit,
    Refresh,
}

/// Values sampled at each refresh, turned into per-second rates.
#[derive(Default)]
struct Series(VecDeque<(Instant, u64)>);

impl Series {
    fn push(&mut self, at: Instant, value: u64) {
        self.0.push_back((at, value));
        while self.0.len() > HISTORY + 1 {
            self.0.pop_front();
        }
    }

    /// Increase per second between consecutive samples.
    fn rates(&self) -> Vec<u64> {
        self.0
            .iter()
            .tuple_windows()
            .map(|((t0, v0), (t1, v1))| {
                let secs = (*t1 - *t0).as_secs_f64().max(0.001);
                (v1.saturating_sub(*v0) as f64 / secs) as u64
            })
            .collect()
    }

    fn values(&self) -> Vec<u64> {
        self.0.iter().map(|(_, v)| *v).collect()
    }
}

#[derive(Default)]
struct History {
    /// sum of all log tails: the cluster's appends
    appends: Series,
    /// per log: its tail
    tails: HashMap<u32, Series>,
    /// per partition: its leader's applied LSN
    applied: HashMap<u32, Series>,
    /// per partition: the largest lag of its replicas
    lag: HashMap<u32, Series>,
    /// per log: how far its slowest tailing reader is behind the tail
    reader_lag: HashMap<u32, Series>,
    /// per read stream: its state and since when the dashboard has seen it in that state
    reader_since: HashMap<(u32, PlainNodeId, bool), (String, Instant)>,
}

impl History {
    fn record(&mut self, view: &ClusterView, at: Instant) {
        let mut total = 0;
        for log in &view.logs {
            if let Some(tail) = log.tail_lsn {
                total += tail;
                self.tails.entry(log.id).or_default().push(at, tail);
            }
            if let Some(behind) = log.readers.iter().filter_map(|r| r.behind).max() {
                self.reader_lag.entry(log.id).or_default().push(at, behind);
            }
            for reader in &log.readers {
                let key = (log.id, reader.node, reader.tailing);
                match self.reader_since.get(&key) {
                    Some((state, _)) if *state == reader.state => {}
                    _ => {
                        self.reader_since.insert(key, (reader.state.clone(), at));
                    }
                }
            }
        }
        if total > 0 {
            self.appends.push(at, total);
        }
        for p in &view.partitions {
            if let Some(applied) = p.leader_replica().and_then(|r| r.applied_lsn) {
                self.applied.entry(p.id).or_default().push(at, applied);
            }
            if let Some(lag) = p.replicas.iter().filter_map(|r| r.lag).max() {
                self.lag.entry(p.id).or_default().push(at, lag);
            }
        }
    }
}

struct App {
    tab: Tab,
    interval: Duration,
    unhealthy_only: bool,
    show_help: bool,
    view: Option<ClusterView>,
    findings: Vec<Finding>,
    updated_at: Option<Instant>,
    error: Option<(Instant, String)>,
    history: History,
    selected_partition: Option<u32>,
    selected_log: Option<u32>,
    selected_node: Option<PlainNodeId>,
    partitions_state: TableState,
    logs_state: TableState,
    nodes_state: TableState,
    findings_scroll: u16,
    /// the finding selected in the findings view
    selected_finding: usize,
    partition_sort: Sorting<PartitionSort>,
    log_sort: Sorting<LogSort>,
    node_sort: Sorting<NodeSort>,
}

impl App {
    fn new(options: Options) -> Self {
        Self {
            tab: options.tab,
            interval: options.interval,
            unhealthy_only: options.unhealthy_only,
            show_help: false,
            view: None,
            findings: Vec::new(),
            updated_at: None,
            error: None,
            history: History::default(),
            selected_partition: None,
            selected_log: None,
            selected_node: None,
            partitions_state: TableState::default(),
            logs_state: TableState::default(),
            nodes_state: TableState::default(),
            findings_scroll: 0,
            selected_finding: 0,
            partition_sort: Sorting::new(options.partition_sort),
            log_sort: Sorting::new(LogSort::Log),
            node_sort: Sorting::new(NodeSort::Node),
        }
    }

    fn update(&mut self, update: Update) {
        let now = Instant::now();
        match update {
            Ok((view, findings)) => {
                self.history.record(&view, now);
                self.view = Some(view);
                self.findings = findings;
                self.updated_at = Some(now);
                self.error = None;
            }
            Err(err) => self.error = Some((now, err)),
        }
    }

    fn partitions(&self) -> Vec<&PartitionView> {
        let mut partitions = self
            .view
            .as_ref()
            .map(|v| filter_partitions(v, self.unhealthy_only))
            .unwrap_or_default();
        self.partition_sort.sort(&mut partitions);
        partitions
    }

    fn logs(&self) -> Vec<&LogView> {
        let mut logs = self
            .view
            .as_ref()
            .map(|v| filter_logs(v, &self.findings, self.unhealthy_only))
            .unwrap_or_default();
        self.log_sort.sort(&mut logs);
        logs
    }

    fn nodes(&self) -> Vec<&NodeView> {
        let mut nodes: Vec<&NodeView> = self.view.iter().flat_map(|v| &v.nodes).collect();
        self.node_sort.sort(&mut nodes);
        nodes
    }

    fn on_key(&mut self, key: KeyEvent) -> Action {
        if self.show_help {
            self.show_help = false;
            return Action::None;
        }
        let tab_index = TABS.iter().position(|t| *t == self.tab).unwrap_or(0);
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Action::Quit,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Action::Quit;
            }
            KeyCode::Char('r') => return Action::Refresh,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('u') => self.unhealthy_only = !self.unhealthy_only,
            KeyCode::Char('s') => match self.tab {
                Tab::Partitions => self.partition_sort.cycle(),
                Tab::Logs => self.log_sort.cycle(),
                Tab::Nodes => self.node_sort.cycle(),
                Tab::Overview | Tab::Findings => {}
            },
            KeyCode::Char('S') => match self.tab {
                Tab::Partitions => self.partition_sort.reversed ^= true,
                Tab::Logs => self.log_sort.reversed ^= true,
                Tab::Nodes => self.node_sort.reversed ^= true,
                Tab::Overview | Tab::Findings => {}
            },
            KeyCode::Tab | KeyCode::Right | KeyCode::Char('l') => {
                self.tab = TABS[(tab_index + 1) % TABS.len()];
            }
            KeyCode::BackTab | KeyCode::Left | KeyCode::Char('h') => {
                self.tab = TABS[(tab_index + TABS.len() - 1) % TABS.len()];
            }
            KeyCode::Char(c @ '1'..='5') => self.tab = TABS[c as usize - '1' as usize],
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::PageDown => self.move_selection(10),
            KeyCode::PageUp => self.move_selection(-10),
            KeyCode::Home | KeyCode::Char('g') => self.move_selection(i64::MIN / 2),
            KeyCode::End | KeyCode::Char('G') => self.move_selection(i64::MAX / 2),
            KeyCode::Enter => self.open(),
            _ => {}
        }
        Action::None
    }

    fn move_selection(&mut self, by: i64) {
        fn step<T: Copy + PartialEq>(ids: &[T], current: Option<T>, by: i64) -> Option<T> {
            if ids.is_empty() {
                return None;
            }
            let at = current.and_then(|c| ids.iter().position(|id| *id == c));
            let next = match at {
                Some(at) => (at as i64).saturating_add(by),
                None => 0,
            };
            Some(ids[next.clamp(0, ids.len() as i64 - 1) as usize])
        }
        match self.tab {
            Tab::Partitions => {
                let ids: Vec<u32> = self.partitions().iter().map(|p| p.id).collect();
                self.selected_partition = step(&ids, self.selected_partition, by);
            }
            Tab::Logs => {
                let ids: Vec<u32> = self.logs().iter().map(|l| l.id).collect();
                self.selected_log = step(&ids, self.selected_log, by);
            }
            Tab::Nodes => {
                let ids: Vec<PlainNodeId> = self.nodes().iter().map(|n| n.plain()).collect();
                self.selected_node = step(&ids, self.selected_node, by);
            }
            Tab::Overview => {
                self.findings_scroll =
                    (i64::from(self.findings_scroll) + by).clamp(0, 10_000) as u16;
            }
            Tab::Findings => {
                let last = self.findings.len().saturating_sub(1) as i64;
                self.selected_finding = (self.selected_finding as i64)
                    .saturating_add(by)
                    .clamp(0, last) as usize;
            }
        }
    }

    /// Enter: from the overview, the findings; from the findings, the selected finding's subject.
    fn open(&mut self) {
        match self.tab {
            Tab::Overview => {
                self.tab = Tab::Findings;
                self.selected_finding = 0;
            }
            Tab::Findings => {
                if let Some(finding) = self.findings.get(self.selected_finding) {
                    self.go_to(finding.subject);
                }
            }
            Tab::Partitions | Tab::Logs | Tab::Nodes => {}
        }
    }

    fn go_to(&mut self, subject: Subject) {
        match subject {
            Subject::Partition(id) => {
                self.tab = Tab::Partitions;
                self.selected_partition = Some(id);
            }
            Subject::Log(id) => {
                self.tab = Tab::Logs;
                self.selected_log = Some(id);
            }
            Subject::Node(id) => {
                self.tab = Tab::Nodes;
                self.selected_node = Some(id);
            }
            Subject::Metadata => self.tab = Tab::Nodes,
            Subject::Cluster => {}
        }
    }

    fn draw(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let [header, tabs, body, footer] = Layout::vertical([
            Constraint::Length(HEADER_HEIGHT),
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(area);

        let Some(view) = &self.view else {
            let message = match &self.error {
                Some((_, err)) => {
                    Line::styled(format!("Can't read the cluster: {err}"), theme::fg(RED))
                }
                None => Line::styled("Connecting to the cluster…", theme::muted()),
            };
            frame.render_widget(
                Paragraph::new(message).centered().wrap(Wrap { trim: true }),
                Rect {
                    y: area.height / 2,
                    height: 3,
                    ..area
                },
            );
            return;
        };

        frame.render_widget(
            Header {
                view,
                verdict: Verdict::of(&self.findings),
                status: self.status_line(),
            },
            header,
        );
        self.draw_tabs(frame, tabs);
        match self.tab {
            Tab::Overview => self.draw_overview(frame, body),
            Tab::Partitions => self.draw_partitions(frame, body),
            Tab::Logs => self.draw_logs(frame, body),
            Tab::Nodes => self.draw_nodes(frame, body),
            Tab::Findings => {
                self.selected_finding = self
                    .selected_finding
                    .min(self.findings.len().saturating_sub(1));
                let findings = Findings {
                    lines: group_findings(&self.findings, usize::MAX),
                    scroll: 0,
                    selected: (!self.findings.is_empty()).then_some(self.selected_finding),
                };
                frame.render_widget(findings, body);
            }
        }
        self.draw_footer(frame, footer);
        if self.show_help {
            draw_help(frame, area);
        }
    }

    fn status_line(&self) -> Line<'static> {
        let ago = |at: Instant| short_duration(at.elapsed().as_secs() as i64);
        let mut spans = vec![Span::raw(" ")];
        if let Some((at, err)) = &self.error {
            spans.push(Span::styled(
                format!("▲ refresh failed {} ago: {} ", ago(*at), first_line(err)),
                theme::fg(AMBER),
            ));
        }
        spans.push(Span::styled("● live", theme::fg(GREEN)));
        spans.push(Span::styled(
            format!(
                " · every {}s · updated {} ago ",
                self.interval.as_secs_f32(),
                self.updated_at.map_or_else(|| "-".to_owned(), ago)
            ),
            theme::muted(),
        ));
        Line::from(spans)
    }

    fn draw_tabs(&self, frame: &mut Frame, area: Rect) {
        let view = self.view.as_ref().expect("drawn with a view");
        let problems = |subject: fn(&Subject) -> bool| {
            self.findings
                .iter()
                .filter(|f| f.severity > Severity::Info && subject(&f.subject))
                .map(|f| f.subject)
                .unique()
                .count()
        };
        let count = |n: usize, bad: usize| {
            if bad > 0 {
                Span::styled(format!(" {bad}/{n}"), theme::fg(AMBER))
            } else {
                Span::styled(format!(" {n}"), theme::muted())
            }
        };
        let worst = self.findings.iter().map(|f| f.severity).max();
        let titles = vec![
            Line::from(" Overview "),
            Line::from(vec![
                Span::raw(" Partitions"),
                count(
                    view.partitions.len(),
                    problems(|s| matches!(s, Subject::Partition(_))),
                ),
                Span::raw(" "),
            ]),
            Line::from(vec![
                Span::raw(" Logs"),
                count(view.logs.len(), problems(|s| matches!(s, Subject::Log(_)))),
                Span::raw(" "),
            ]),
            Line::from(vec![
                Span::raw(" Nodes"),
                count(
                    view.nodes.len(),
                    problems(|s| matches!(s, Subject::Node(_))),
                ),
                Span::raw(" "),
            ]),
            Line::from(vec![
                Span::raw(" Findings "),
                Span::styled(
                    self.findings
                        .iter()
                        .filter(|f| f.severity > Severity::Info)
                        .count()
                        .to_string(),
                    worst
                        .filter(|s| *s > Severity::Info)
                        .map_or_else(theme::muted, |s| theme::fg(theme::severity(s))),
                ),
                Span::raw(" "),
            ]),
        ];
        let selected = TABS.iter().position(|t| *t == self.tab).unwrap_or(0);
        frame.render_widget(
            Tabs::new(titles)
                .select(selected)
                .divider(Span::styled("│", theme::border()))
                .padding(" ", "")
                .highlight_style(theme::pill(theme::BLUE)),
            area,
        );
    }

    fn draw_footer(&self, frame: &mut Frame, area: Rect) {
        let key = |k: &str, label: &str| {
            vec![
                Span::styled(format!(" {k} "), theme::bold()),
                Span::styled(format!("{label}  "), theme::muted()),
            ]
        };
        let mut spans = Vec::new();
        spans.extend(key("q", "quit"));
        spans.extend(key("⇥/1-5", "views"));
        spans.extend(key("↑↓", "select"));
        spans.extend(key(
            "u",
            if self.unhealthy_only {
                "show all"
            } else {
                "unhealthy only"
            },
        ));
        if matches!(self.tab, Tab::Partitions | Tab::Logs | Tab::Nodes) {
            spans.extend(key("s/S", "sort, reverse"));
        }
        match self.tab {
            Tab::Overview => spans.extend(key("⏎", "findings")),
            Tab::Findings => spans.extend(key("⏎", "open the finding's subject")),
            _ => {}
        }
        spans.extend(key("r", "refresh"));
        spans.extend(key("?", "help"));
        frame.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    fn draw_overview(&self, frame: &mut Frame, area: Rect) {
        let view = self.view.as_ref().expect("drawn with a view");
        let cards = show_node_cards(view).then_some(NodeCards {
            view,
            selected: None,
        });
        // many nodes get a tile each; the nodes view has their details
        let tiles_of_nodes = NodeTiles { view };
        let nodes_height = match &cards {
            Some(cards) => cards.height(area.width),
            None => tiles_of_nodes.height(area.width),
        };
        let tiles = PartitionTiles {
            view,
            partitions: view.partitions.iter().collect(),
            selected: None,
        };
        let [nodes_area, rate_area, tiles_area, findings_area] = Layout::vertical([
            Constraint::Length(nodes_height),
            Constraint::Length(4),
            Constraint::Length(tiles.height(area.width)),
            Constraint::Min(3),
        ])
        .areas(area);
        match cards {
            Some(cards) => frame.render_widget(cards, nodes_area),
            None => frame.render_widget(tiles_of_nodes, nodes_area),
        }
        self.draw_rate(
            frame,
            rate_area,
            "Appends",
            "records/s across all logs",
            &self.history.appends,
            BLUE,
        );
        frame.render_widget(tiles, tiles_area);
        frame.render_widget(
            Findings {
                lines: group_findings(&self.findings, 3),
                scroll: self.findings_scroll,
                selected: None,
            },
            findings_area,
        );
    }

    /// A panel with a sparkline of `series`' rate and its latest value in the title.
    fn draw_rate(
        &self,
        frame: &mut Frame,
        area: Rect,
        title: &str,
        unit: &str,
        series: &Series,
        color: theme::Rgb,
    ) {
        let rates = series.rates();
        let current = rates.last().copied();
        let block = panel(vec![
            Span::styled(title.to_owned(), theme::bold()),
            Span::styled(
                format!("  {}", current.map_or_else(|| "…".to_owned(), human_rate)),
                theme::fg(color).add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!(" {unit}"), theme::muted()),
        ])
        .title_top(
            Line::styled(
                format!(
                    " peak {} ",
                    rates
                        .iter()
                        .max()
                        .copied()
                        .map_or_else(|| "-".to_owned(), human_rate)
                ),
                theme::muted(),
            )
            .right_aligned(),
        );
        let inner = block.inner(area);
        frame.render_widget(block, area);
        frame.render_widget(
            Sparkline::default()
                .data(right_anchored(&rates, inner.width))
                .style(theme::fg(color))
                .absent_value_symbol(" "),
            inner,
        );
    }

    fn split_detail(area: Rect) -> (Rect, Rect) {
        if area.width >= 150 {
            let [list, detail] =
                Layout::horizontal([Constraint::Fill(1), Constraint::Length(62)]).areas(area);
            (list, detail)
        } else {
            let [list, detail] =
                Layout::vertical([Constraint::Fill(1), Constraint::Length(16)]).areas(area);
            (list, detail)
        }
    }

    fn draw_partitions(&mut self, frame: &mut Frame, area: Rect) {
        let view = self.view.as_ref().expect("drawn with a view");
        let mut partitions = filter_partitions(view, self.unhealthy_only);
        self.partition_sort.sort(&mut partitions);
        if self
            .selected_partition
            .is_none_or(|id| !partitions.iter().any(|p| p.id == id))
        {
            self.selected_partition = partitions.first().map(|p| p.id);
        }
        let (list, detail) = Self::split_detail(area);
        let list = with_placement(
            frame,
            list,
            PlacementBars::partitions(&PartitionPlacement::of(view, &partitions)),
        );
        let mut table = partitions_table(view, partitions.iter().copied(), false);
        table.sorted = Some(self.partition_sort.sorted_by());
        if self.unhealthy_only {
            table
                .title
                .push(Span::styled("  unhealthy only", theme::fg(AMBER)));
        }
        let selected = self
            .selected_partition
            .and_then(|id| partitions.iter().position(|p| p.id == id));
        self.partitions_state.select(selected);
        frame.render_stateful_widget(selectable(&table), list, &mut self.partitions_state);

        let Some(p) = selected.map(|i| partitions[i]) else {
            frame.render_widget(panel(vec![Span::raw("No partitions")]), detail);
            return;
        };
        let block = panel(vec![
            Span::styled(format!("Partition {}", p.id), theme::bold()),
            Span::raw("  "),
            Span::styled(
                crate::render::health_label(p.health),
                theme::fg(theme::health(p.health)),
            ),
        ]);
        let inner = block.inner(detail);
        frame.render_widget(block, detail);

        let width = pad(inner).width;
        let label = |text: &str| vec![Span::styled(format!("{text:<9}"), theme::muted())];
        let mut lines = Vec::new();
        let mut leader = Vec::new();
        match p.leader {
            Some(l) => leader.push(generational_span(l)),
            None => leader.push(Span::styled("none", theme::fg(RED))),
        }
        if let Some(epoch) = p.epoch {
            leader.push(Span::styled(format!("  epoch e{epoch}"), theme::muted()));
        }
        lines.extend(hanging(label("leader"), leader, width));
        if let Some(m) = &p.membership {
            let mut spans = vec![Span::styled(format!("v{} ", m.version), theme::muted())];
            spans.extend(Itertools::intersperse(
                m.members.iter().map(|n| node_span(*n)),
                Span::raw(" "),
            ));
            if let Some(next) = &m.next {
                spans.push(Span::styled(
                    format!("  → v{} ", next.version),
                    theme::fg(BLUE),
                ));
                spans.extend(Itertools::intersperse(
                    next.members.iter().map(|n| node_span(*n)),
                    Span::raw(" "),
                ));
            }
            lines.extend(hanging(label("replicas"), spans, width));
        }
        let mut log = vec![Span::raw(
            p.log_id.map_or_else(|| "-".to_owned(), |l| l.to_string()),
        )];
        if let Some(sequencer) = p.sequencer {
            log.push(Span::styled("  sequencer ", theme::muted()));
            log.push(generational_span(sequencer));
        }
        if let Some(tail) = p.log_tail_lsn {
            log.push(Span::styled(format!("  tail {tail}"), theme::muted()));
        }
        lines.extend(hanging(label("log"), log, width));
        if let Some(keys) = p.key_range {
            lines.extend(hanging(
                label("keys"),
                vec![Span::styled(
                    format!("{:016x}..{:016x}", keys.start, keys.end),
                    theme::muted(),
                )],
                width,
            ));
        }

        let [info, replicas, applied, lag] = Layout::vertical([
            Constraint::Length(lines.len() as u16 + 1),
            Constraint::Length(p.replicas.len() as u16 + p.missing.len() as u16 + 3),
            Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .areas(inner);
        frame.render_widget(Paragraph::new(lines), pad(info));
        let mut replica_table = replicas_table(&[p], false);
        // the detail pane is narrow: keep the replica's own columns
        replica_table.title = vec![Span::styled("Replicas", theme::bold())];
        frame.render_widget(narrow(replica_table, &[2, 3, 7, 8, 9, 10]), replicas);
        self.draw_series(
            frame,
            applied,
            "applied/s",
            self.history.applied.get(&p.id),
            true,
            GREEN,
        );
        self.draw_series(
            frame,
            lag,
            "max lag",
            self.history.lag.get(&p.id),
            false,
            AMBER,
        );
    }

    fn draw_series(
        &self,
        frame: &mut Frame,
        area: Rect,
        label: &str,
        series: Option<&Series>,
        as_rate: bool,
        color: theme::Rgb,
    ) {
        let data = series.map_or_else(Vec::new, |s| if as_rate { s.rates() } else { s.values() });
        let current = data.last().copied();
        let [label_area, chart] =
            Layout::horizontal([Constraint::Length(22), Constraint::Fill(1)]).areas(pad(area));
        frame.render_widget(
            Paragraph::new(vec![
                Line::styled(label.to_owned(), theme::muted()),
                Line::styled(
                    current.map_or_else(
                        || "…".to_owned(),
                        |v| {
                            if as_rate {
                                human_rate(v)
                            } else {
                                v.to_string()
                            }
                        },
                    ),
                    theme::fg(color).add_modifier(Modifier::BOLD),
                ),
            ]),
            label_area,
        );
        frame.render_widget(
            Sparkline::default()
                .data(right_anchored(&data, chart.width))
                .style(theme::fg(color))
                .absent_value_symbol(" "),
            chart,
        );
    }

    fn draw_logs(&mut self, frame: &mut Frame, area: Rect) {
        let view = self.view.as_ref().expect("drawn with a view");
        let mut logs = filter_logs(view, &self.findings, self.unhealthy_only);
        self.log_sort.sort(&mut logs);
        if self
            .selected_log
            .is_none_or(|id| !logs.iter().any(|l| l.id == id))
        {
            self.selected_log = logs.first().map(|l| l.id);
        }
        let (list, detail) = Self::split_detail(area);
        let list = with_placement(
            frame,
            list,
            PlacementBars::logs(&LogPlacement::of(view, &logs)),
        );
        let mut table = logs_table(view, logs.iter().copied());
        table.sorted = Some(self.log_sort.sorted_by());
        if self.unhealthy_only {
            table
                .title
                .push(Span::styled("  unhealthy only", theme::fg(AMBER)));
        }
        let selected = self
            .selected_log
            .and_then(|id| logs.iter().position(|l| l.id == id));
        self.logs_state.select(selected);
        frame.render_stateful_widget(selectable(&table), list, &mut self.logs_state);

        let Some(log) = selected.map(|i| logs[i]) else {
            frame.render_widget(panel(vec![Span::raw("No logs")]), detail);
            return;
        };
        let block = panel(vec![Span::styled(format!("Log {}", log.id), theme::bold())]);
        let inner = block.inner(detail);
        frame.render_widget(block, detail);
        let width = pad(inner).width;
        let field = |name: &str, value: Vec<Span<'static>>| {
            hanging(
                vec![Span::styled(format!("{name:<11}"), theme::muted())],
                value,
                width,
            )
        };
        let num = |v: Option<u64>| Span::raw(v.map_or_else(|| "-".to_owned(), |v| v.to_string()));
        let mut lines: Vec<Line<'static>> = [
            field(
                "partition",
                vec![Span::raw(
                    log.partition_id
                        .map_or_else(|| "-".to_owned(), |p| p.to_string()),
                )],
            ),
            field(
                "segment",
                vec![
                    Span::raw(log.tail_segment.to_string()),
                    Span::styled(
                        format!(
                            "  · {} in the chain · base {} · {}",
                            log.segments, log.base_lsn, log.kind
                        ),
                        theme::muted(),
                    ),
                ],
            ),
            field("tail", vec![num(log.tail_lsn)]),
            field("trim point", vec![num(log.trim_point)]),
            field("retained", vec![num(log.retained)]),
            field(
                "sequencer",
                log.sequencer
                    .map_or_else(|| vec![Span::raw("-")], |s| vec![generational_span(s)]),
            ),
            field(
                "nodeset",
                Itertools::intersperse(log.nodeset.iter().map(|n| node_span(*n)), Span::raw(" "))
                    .collect(),
            ),
            field(
                "writable",
                vec![Span::raw(format!(
                    "{} of {} · replication {}",
                    log.writable_nodes,
                    log.nodeset.len(),
                    log.replication.as_deref().unwrap_or("-")
                ))],
            ),
        ]
        .concat();
        if log.sealed {
            lines.push(Line::styled("tail segment sealed", theme::fg(AMBER)));
        }
        let readers = self.readers_table(log);
        let chain = chain_view(view, log, inner.width.saturating_sub(2));
        let [info, lane, readers_area, rate, lag] = Layout::vertical([
            Constraint::Length(lines.len() as u16 + 1),
            Constraint::Length(chain.len() as u16 + 1),
            Constraint::Length(readers.height()),
            Constraint::Fill(1),
            Constraint::Fill(1),
        ])
        .areas(inner);
        frame.render_widget(Paragraph::new(lines), pad(info));
        frame.render_widget(Paragraph::new(chain), pad(lane));
        frame.render_widget(readers, readers_area);
        self.draw_series(
            frame,
            rate,
            "appends/s",
            self.history.tails.get(&log.id),
            true,
            BLUE,
        );
        self.draw_series(
            frame,
            lag,
            "slowest reader behind",
            self.history.reader_lag.get(&log.id),
            false,
            AMBER,
        );
    }

    /// The log's read streams, with how long the dashboard has seen each in its state.
    fn readers_table(&self, log: &LogView) -> RichTable {
        let rows = log
            .readers
            .iter()
            .map(|r| {
                let since = self
                    .history
                    .reader_since
                    .get(&(log.id, r.node, r.tailing))
                    .map(|(_, at)| short_duration(at.elapsed().as_secs() as i64));
                let state_style = match r.phase() {
                    ReaderPhase::Reading => theme::muted(),
                    ReaderPhase::Switching => theme::fg(AMBER),
                    ReaderPhase::AwaitingSeal | ReaderPhase::Sealing => theme::fg(RED),
                };
                vec![
                    Line::from(node_span(r.node)),
                    Line::styled(r.state.clone(), state_style),
                    segment_cell(log, r.segment),
                    Line::raw(
                        r.read_pointer
                            .map_or_else(|| "-".to_owned(), |p| p.to_string()),
                    ),
                    Line::raw(r.behind.map_or_else(|| "-".to_owned(), |b| b.to_string())),
                    Line::styled(since.unwrap_or_else(|| "-".to_owned()), theme::muted()),
                ]
            })
            .collect();
        RichTable {
            title: vec![Span::styled("Readers", theme::bold())],
            header: vec!["NODE", "STATE", "SEGMENT", "AT", "BEHIND", "FOR"],
            right: vec![3, 4, 5],
            rows,
            legend: None,
            sorted: None,
        }
    }

    fn draw_nodes(&mut self, frame: &mut Frame, area: Rect) {
        let view = self.view.as_ref().expect("drawn with a view");
        let mut nodes: Vec<&NodeView> = view.nodes.iter().collect();
        self.node_sort.sort(&mut nodes);
        if self.selected_node.is_none_or(|id| view.node(id).is_none()) {
            self.selected_node = nodes.first().map(|n| n.plain());
        }
        let (list, detail) = Self::split_detail(area);
        let mut table = nodes_table(view, &nodes, true);
        table.sorted = Some(self.node_sort.sorted_by());
        let selected = self
            .selected_node
            .and_then(|id| nodes.iter().position(|n| n.plain() == id));
        self.nodes_state.select(selected);
        frame.render_stateful_widget(selectable(&table), list, &mut self.nodes_state);

        let Some(node) = selected.map(|i| nodes[i]) else {
            return;
        };
        let block = panel(vec![
            node_span(node.plain()),
            Span::raw(" "),
            Span::styled(node.name.clone(), theme::bold()),
        ]);
        let inner = block.inner(detail);
        frame.render_widget(block, detail);
        let width = pad(inner).width;
        let field = |name: &str, value: String| {
            hanging(
                vec![Span::styled(format!("{name:<12}"), theme::muted())],
                vec![Span::raw(value)],
                width,
            )
        };
        let leads = compress_ids(
            view.partitions
                .iter()
                .filter(|p| p.leader.is_some_and(|l| l.as_plain() == node.plain()))
                .map(|p| p.id),
        );
        let follows = compress_ids(
            view.partitions
                .iter()
                .filter(|p| {
                    p.replica_on(node.plain()).is_some()
                        && p.leader.is_none_or(|l| l.as_plain() != node.plain())
                })
                .map(|p| p.id),
        );
        let mut lines: Vec<Line<'static>> = [
            field("generation", node.id.to_string()),
            field("roles", node.roles.join(", ")),
            field(
                "version",
                node.binary_version
                    .clone()
                    .unwrap_or_else(|| "-".to_owned()),
            ),
            field(
                "leads",
                if leads.is_empty() {
                    "-".to_owned()
                } else {
                    leads
                },
            ),
            field(
                "follows",
                if follows.is_empty() {
                    "-".to_owned()
                } else {
                    follows
                },
            ),
        ]
        .concat();
        if !node.location.is_empty() {
            lines.extend(field("location", node.location.clone()));
        }
        if let Some(seen) = &node.seen_versions {
            lines.extend(field(
                "has loaded",
                format!(
                    "nodes v{} · logs v{} · partitions v{} · schema v{}",
                    seen.nodes_config,
                    seen.logs.unwrap_or_default(),
                    seen.partition_table.unwrap_or_default(),
                    seen.schema.unwrap_or_default()
                ),
            ));
        }
        if let Some(m) = &node.metadata_server {
            lines.extend(field(
                "metadata",
                match (&m.error, &m.raft) {
                    (Some(err), _) => err.clone(),
                    (None, Some(raft)) => format!(
                        "{} · term {} · applied {} · log {}",
                        m.status.as_deref().unwrap_or(&m.state),
                        raft.term,
                        raft.applied,
                        raft.log_length
                    ),
                    (None, None) => m.status.clone().unwrap_or_else(|| m.state.clone()),
                },
            ));
        }
        lines.push(Line::default());
        lines.extend(addresses(
            "control",
            &node.ctrl_address,
            &node.ctrl_addresses,
            node.dialed_address.as_deref(),
            width,
        ));
        lines.extend(addresses(
            "fabric",
            &node.address,
            &node.addresses,
            None,
            width,
        ));
        lines.extend(servers(node, width));
        if node.dialed_address.is_some() {
            lines.push(Line::from(vec![
                Span::styled("▸", theme::fg(BLUE).add_modifier(Modifier::BOLD)),
                Span::styled(" the address restatectl dials", theme::muted()),
            ]));
        }
        frame.render_widget(Paragraph::new(lines), pad(inner));
    }
}

/// `label` then `value`, wrapped to `width` with continuation lines starting under the value. A
/// line breaks after a `/` in its second half where there is one, else after a space or `,`,
/// else after a `.`, `:` or `-`, so addresses, paths and lists split where they read naturally.
fn hanging(label: Vec<Span<'static>>, value: Vec<Span<'static>>, width: u16) -> Vec<Line<'static>> {
    let indent: usize = label.iter().map(Span::width).sum();
    let room = usize::from(width).saturating_sub(indent).max(8);
    let chars: Vec<(char, Style)> = value
        .iter()
        .flat_map(|span| {
            let style = span.style;
            span.content.chars().map(move |c| (c, style))
        })
        .collect();
    let mut lines = Vec::new();
    let mut label = Some(label);
    let mut rest = chars.as_slice();
    loop {
        let take = if rest.len() <= room {
            rest.len()
        } else {
            let after = |separators: &str| {
                rest[..room]
                    .iter()
                    .rposition(|(c, _)| separators.contains(*c))
                    .filter(|at| *at >= room / 2)
            };
            after("/")
                .or_else(|| after(" ,"))
                .or_else(|| after(".:-"))
                .map_or(room, |at| at + 1)
        };
        let (head, tail) = rest.split_at(take);
        let mut spans = label
            .take()
            .unwrap_or_else(|| vec![Span::raw(" ".repeat(indent))]);
        for (style, run) in &head.iter().chunk_by(|(_, style)| *style) {
            spans.push(Span::styled(
                run.map(|(c, _)| *c).collect::<String>(),
                style,
            ));
        }
        lines.push(Line::from(spans));
        rest = tail;
        while rest.first().is_some_and(|(c, _)| *c == ' ') {
            rest = &rest[1..];
        }
        if rest.is_empty() {
            return lines;
        }
    }
}

/// The node's servers, as it reports them: where each is advertised and where it listens.
fn servers(node: &NodeView, width: u16) -> Vec<Line<'static>> {
    let mut lines = vec![Line::styled(
        "servers, as the node reports them",
        theme::bold(),
    )];
    if node.servers.is_empty() {
        lines.push(Line::styled(
            "  none reported: the node couldn't be reached",
            theme::muted(),
        ));
        return lines;
    }
    for server in &node.servers {
        let name = match server.server.as_str() {
            "http-ingress-server" => "ingress",
            "admin-api-server" => "admin",
            "message-fabric-server" => "fabric",
            "control-server" => "control",
            "tokio-console-server" => "tokio console",
            other => other,
        };
        let label = |text: String| vec![Span::styled(text, theme::muted())];
        lines.extend(hanging(
            label(format!("  {name:<13}")),
            vec![match &server.advertised {
                Some(advertised) => Span::raw(advertised.clone()),
                None => Span::styled("not advertised", theme::muted()),
            }],
            width,
        ));
        for (i, address) in server.listens_on.iter().enumerate() {
            let text = if i == 0 { "listens on" } else { "" };
            lines.extend(hanging(
                label(format!("  {:<13}{text:<11}", "")),
                vec![Span::raw(address.clone())],
                width,
            ));
        }
    }
    lines
}

/// A node's addresses of one kind: those for callers in specific locations, in the node's order
/// of preference, then the one for everyone else; `▸` marks the one restatectl dials.
fn addresses(
    kind: &str,
    default: &str,
    located: &[LocatedAddress],
    dialed: Option<&str>,
    width: u16,
) -> Vec<Line<'static>> {
    let mut dialed = dialed;
    // the first entry matching what restatectl dials is the one it picked
    let mut mark = |address: &str| {
        if dialed == Some(address) {
            dialed = None;
            Span::styled("▸ ", theme::fg(BLUE).add_modifier(Modifier::BOLD))
        } else {
            Span::raw("  ")
        }
    };
    let location_width = located
        .iter()
        .filter_map(|a| a.location.as_ref().map(String::len))
        .max()
        .unwrap_or(0)
        .max(10);
    let mut lines = vec![Line::styled(format!("{kind} addresses"), theme::bold())];
    let entries = located
        .iter()
        .map(|a| {
            (
                a.location.as_deref().unwrap_or("any caller"),
                a.address.as_str(),
            )
        })
        .chain([(
            if located.is_empty() {
                "any caller"
            } else {
                "otherwise"
            },
            default,
        )]);
    for (location, address) in entries {
        lines.extend(hanging(
            vec![
                mark(address),
                Span::styled(format!("{location:<location_width$}  "), theme::muted()),
            ],
            vec![Span::raw(address.to_owned())],
            width,
        ));
    }
    lines
}

/// A reader's segment, and how many segments it is behind the tail segment.
fn segment_cell(log: &LogView, segment: Option<u32>) -> Line<'static> {
    let Some(segment) = segment else {
        return Line::styled("-", theme::muted());
    };
    let position = |index: u32| log.chain.iter().position(|s| s.index == index);
    match position(segment).zip(position(log.tail_segment)) {
        Some((at, tail)) if at < tail => Line::from(vec![
            Span::raw(segment.to_string()),
            Span::styled(format!(" {} behind", tail - at), theme::muted()),
        ]),
        _ if segment == log.tail_segment => Line::from(vec![
            Span::raw(segment.to_string()),
            Span::styled(" tail", theme::muted()),
        ]),
        _ => Line::raw(segment.to_string()),
    }
}

/// Draws `placement` above the list when there's room for both, and returns the list's area.
fn with_placement(frame: &mut Frame, area: Rect, placement: PlacementBars) -> Rect {
    // keep a few rows of the list in view
    if area.height < placement.height() + 6 {
        return area;
    }
    let [placement_area, list] =
        Layout::vertical([Constraint::Length(placement.height()), Constraint::Fill(1)]).areas(area);
    frame.render_widget(placement, placement_area);
    list
}

fn filter_partitions(view: &ClusterView, unhealthy_only: bool) -> Vec<&PartitionView> {
    view.partitions
        .iter()
        .filter(|p| !unhealthy_only || p.health != PartitionHealth::Healthy)
        .collect()
}

fn filter_logs<'a>(
    view: &'a ClusterView,
    findings: &[Finding],
    unhealthy_only: bool,
) -> Vec<&'a LogView> {
    view.logs
        .iter()
        .filter(|l| {
            !unhealthy_only
                || findings
                    .iter()
                    .any(|f| f.subject == Subject::Log(l.id) && f.severity > Severity::Info)
        })
        .collect()
}

/// The newest `width` values, padded with absent bars on the left so the latest sits at the
/// right edge and the chart scrolls left as samples come in.
fn right_anchored(values: &[u64], width: u16) -> Vec<Option<u64>> {
    let width = usize::from(width);
    let shown = &values[values.len().saturating_sub(width)..];
    std::iter::repeat_n(None, width - shown.len())
        .chain(shown.iter().copied().map(Some))
        .collect()
}

fn selectable(table: &RichTable) -> ratatui::widgets::Table<'static> {
    table
        .table()
        .row_highlight_style(
            Style::new()
                .bg(theme::rgb(theme::TRACK))
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▶ ")
}

/// Keeps the columns `keep` of `table`.
fn narrow(mut table: RichTable, keep: &[usize]) -> RichTable {
    table.header = keep.iter().map(|i| table.header[*i]).collect();
    table.rows = table
        .rows
        .into_iter()
        .map(|row| {
            keep.iter()
                .map(|i| row.get(*i).cloned().unwrap_or_default())
                .collect()
        })
        .collect();
    table.right = keep
        .iter()
        .enumerate()
        .filter(|(_, i)| table.right.contains(i))
        .map(|(at, _)| at)
        .collect();
    table
}

fn pad(area: Rect) -> Rect {
    Rect {
        x: area.x + 1,
        width: area.width.saturating_sub(2),
        ..area
    }
}

fn first_line(text: &str) -> &str {
    text.lines().next().unwrap_or_default()
}

/// `1234` → `1.2k/s`
fn human_rate(rate: u64) -> String {
    match rate {
        r if r >= 1_000_000 => format!("{:.1}M/s", r as f64 / 1_000_000.0),
        r if r >= 1_000 => format!("{:.1}k/s", r as f64 / 1_000.0),
        r => format!("{r}/s"),
    }
}

fn draw_help(frame: &mut Frame, area: Rect) {
    let rows = [
        ("q, Esc, Ctrl-C", "quit"),
        ("Tab, →, l / Shift-Tab, ←, h", "next / previous view"),
        ("1 … 5", "overview, partitions, logs, nodes, findings"),
        ("↑ ↓, j k, PgUp PgDn, g G", "select / scroll"),
        (
            "Enter",
            "overview: list the findings; findings: open the selected one",
        ),
        ("u", "only partitions and logs that need attention"),
        (
            "s / S",
            "sort the list by its next column / reverse the order",
        ),
        ("r", "refresh now"),
    ];
    let mut lines = vec![Line::default()];
    for (keys, what) in rows {
        lines.push(Line::from(vec![
            Span::styled(format!("  {keys:<30}"), theme::bold()),
            Span::styled(what, theme::muted()),
        ]));
    }
    lines.push(Line::default());
    lines.push(Line::styled(
        "  Partition tiles: color = health, left edge = leader's node.",
        theme::muted(),
    ));
    lines.push(Line::styled(
        "  Rates are measured between refreshes.",
        theme::muted(),
    ));
    let height = lines.len() as u16 + 2;
    let popup = Rect {
        x: area.x + area.width.saturating_sub(80) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width: area.width.min(80),
        height: height.min(area.height),
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).block(panel(vec![Span::styled("Keys", theme::bold())])),
        popup,
    );
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;
    use ratatui::widgets::Widget;

    use restate_types::PlainNodeId;

    use super::*;
    use crate::observe::tests::cluster_that_lost_a_node;
    use crate::tui::to_ansi;
    use crate::tui::widgets::metadata_table;

    fn text(buffer: &Buffer) -> String {
        buffer.content().iter().map(|c| c.symbol()).collect()
    }

    /// Every view of the dashboard, and the inline tables, render at wide and cramped sizes.
    #[test]
    fn every_view_renders_at_any_size() {
        let mut app = App::new(Options {
            tab: Tab::Overview,
            interval: Duration::from_secs(1),
            unhealthy_only: false,
            partition_sort: PartitionSort::Partition,
        });
        for _ in 0..2 {
            let view = cluster_that_lost_a_node();
            let findings = analyze(&view);
            app.update(Ok((view, findings)));
        }

        for (width, height) in [(180, 50), (100, 30), (60, 16), (20, 5)] {
            for tab in TABS {
                app.tab = tab;
                app.show_help = tab == Tab::Findings;
                let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
                terminal.draw(|frame| app.draw(frame)).unwrap();
                let screen = text(terminal.backend().buffer());
                if width >= 100 {
                    assert!(screen.contains("CRITICAL"), "{screen}");
                    match tab {
                        Tab::Partitions => assert!(screen.contains("unavailable")),
                        Tab::Logs => assert!(screen.contains("down")),
                        Tab::Nodes => assert!(screen.contains("dead")),
                        _ => {}
                    }
                }
            }
        }

        let view = app.view.as_ref().unwrap();
        let partitions: Vec<_> = view.partitions.iter().collect();
        for table in [
            metadata_table(view),
            replicas_table(&partitions, true),
            partitions_table(view, &view.partitions, true),
            logs_table(view, &view.logs),
        ] {
            let area = Rect::new(0, 0, table.width(), table.height());
            let mut buffer = Buffer::empty(area);
            table.render(area, &mut buffer);
            assert!(!to_ansi(&buffer).is_empty());
        }
    }

    /// Long values continue under themselves, breaking after path separators first.
    #[test]
    fn long_values_wrap_under_themselves() {
        let text = |lines: Vec<Line<'static>>| {
            lines
                .iter()
                .map(|l| {
                    l.spans
                        .iter()
                        .map(|s| s.content.as_ref())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
        };
        let lines = hanging(
            vec![Span::raw("listens on ")],
            vec![Span::raw(
                "unix:/var/lib/restate/restate-data/node-1/fabric.sock",
            )],
            30,
        );
        assert_eq!(
            text(lines),
            [
                "listens on unix:/var/lib/",
                "           restate/restate-",
                "           data/node-1/",
                "           fabric.sock",
            ]
        );
        // what fits stays on one line
        assert_eq!(
            text(hanging(
                vec![Span::raw("tail ")],
                vec![Span::raw("4190045")],
                30
            )),
            ["tail 4190045"]
        );
    }

    /// `s` orders a list by its next key and `S` reverses it; the selection stays on its item.
    #[test]
    fn sort_keys_reorder_the_lists() {
        let key = |c| KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE);
        let mut app = App::new(Options {
            tab: Tab::Partitions,
            interval: Duration::from_secs(1),
            unhealthy_only: false,
            partition_sort: PartitionSort::Partition,
        });
        let view = cluster_that_lost_a_node();
        let findings = analyze(&view);
        app.update(Ok((view, findings)));

        let ids = |app: &App| app.partitions().iter().map(|p| p.id).collect::<Vec<_>>();
        assert_eq!(ids(&app), [0, 1, 2]);
        app.selected_partition = Some(0);
        app.on_key(key('s'));
        // by health: the unavailable ones first
        assert_eq!(ids(&app), [1, 2, 0]);
        app.on_key(key('S'));
        assert_eq!(ids(&app), [0, 2, 1]);
        assert_eq!(app.selected_partition, Some(0));

        app.tab = Tab::Nodes;
        app.on_key(key('s'));
        // by status: the dead node first
        assert_eq!(app.nodes()[0].plain(), PlainNodeId::new(3));
    }

    /// Enter lists the findings from the overview, and opens the selected finding's subject.
    #[test]
    fn enter_opens_the_selected_finding() {
        let key = |code| KeyEvent::new(code, KeyModifiers::NONE);
        let mut app = App::new(Options {
            tab: Tab::Overview,
            interval: Duration::from_secs(1),
            unhealthy_only: false,
            partition_sort: PartitionSort::Partition,
        });
        let view = cluster_that_lost_a_node();
        let findings = analyze(&view);
        app.update(Ok((view, findings)));

        app.on_key(key(KeyCode::Enter));
        assert!(app.tab == Tab::Findings);

        // the first log finding, wherever it is in the list
        let at = app
            .findings
            .iter()
            .position(|f| matches!(f.subject, Subject::Log(_)))
            .unwrap();
        for _ in 0..at {
            app.on_key(key(KeyCode::Down));
        }
        let Subject::Log(log) = app.findings[at].subject else {
            unreachable!()
        };
        app.on_key(key(KeyCode::Enter));
        assert!(app.tab == Tab::Logs);
        assert_eq!(app.selected_log, Some(log));

        // Enter does nothing outside the overview and the findings
        app.on_key(key(KeyCode::Enter));
        assert!(app.tab == Tab::Logs);
    }

    /// A 20-node, 256-partition cluster renders in every view, inline and in the dashboard,
    /// with a minute of history behind the dashboard's charts.
    #[test]
    fn large_cluster_renders() {
        use crate::observe::tests::large_cluster;
        use crate::tui::views;

        let mut app = App::new(Options {
            tab: Tab::Overview,
            interval: Duration::from_secs(1),
            unhealthy_only: false,
            partition_sort: PartitionSort::Partition,
        });
        // a minute of samples: tails and applied LSNs move on, N12 catches up
        let mut view = large_cluster();
        let start = Instant::now() - Duration::from_secs(61);
        for second in 0..60u64 {
            for log in &mut view.logs {
                let rate = 40 + (u64::from(log.id) * 7 + second * 5) % 90;
                log.tail_lsn = log.tail_lsn.map(|t| t + rate);
                for reader in &mut log.readers {
                    reader.behind = reader.behind.map(|b| b.saturating_sub(b / 30));
                }
            }
            for p in &mut view.partitions {
                let rate = 30 + (u64::from(p.id) * 11 + second * 3) % 70;
                for r in &mut p.replicas {
                    r.applied_lsn = r.applied_lsn.map(|a| a + rate);
                    r.lag = r.lag.map(|l| l.saturating_sub(l / 25));
                }
            }
            app.history
                .record(&view, start + Duration::from_secs(second));
        }
        let findings = analyze(&view);
        app.view = Some(view);
        app.findings = findings;
        app.updated_at = Some(Instant::now());

        let view = app.view.as_ref().unwrap();
        let findings = &app.findings;
        let partitions: Vec<_> = view.partitions.iter().collect();
        let logs: Vec<_> = view.logs.iter().collect();
        let mut frames: Vec<(String, Vec<String>)> = vec![
            (
                "status".to_owned(),
                views::status(view, findings, false, 180).into_lines(),
            ),
            (
                "status --extra".to_owned(),
                views::status(view, findings, true, 180).into_lines(),
            ),
            (
                "partitions list".to_owned(),
                views::partitions(
                    view,
                    &partitions,
                    findings,
                    Some(&PartitionPlacement::of(view, &partitions)),
                    false,
                    false,
                    180,
                )
                .into_lines(),
            ),
            (
                "partitions list --replicas 0-15".to_owned(),
                views::partitions(view, &partitions[..16], &[], None, true, false, 180)
                    .into_lines(),
            ),
            (
                "logs list".to_owned(),
                views::logs(
                    view,
                    &logs,
                    findings,
                    Some(&LogPlacement::of(view, &logs)),
                    180,
                )
                .into_lines(),
            ),
        ];
        assert!(frames[0].1.iter().any(|l| l.contains("CRITICAL")));

        // a partition N12 catches up on, whose log has older segments
        let catching_up = view
            .partitions
            .iter()
            .find(|p| {
                p.replica_on(PlainNodeId::new(12)).is_some()
                    && p.log_id
                        .and_then(|l| view.logs.iter().find(|log| log.id == l))
                        .is_some_and(|l| l.segments > 1)
            })
            .unwrap()
            .id;
        app.selected_partition = Some(catching_up);
        app.selected_log = Some(catching_up);
        app.selected_node = Some(PlainNodeId::new(1));
        app.partition_sort = Sorting::new(PartitionSort::Lag);
        app.node_sort = Sorting::new(NodeSort::Leaders);
        app.selected_finding = 2;
        for tab in TABS {
            app.tab = tab;
            let mut terminal = Terminal::new(TestBackend::new(200, 56)).unwrap();
            terminal.draw(|frame| app.draw(frame)).unwrap();
            let name = match tab {
                Tab::Overview => "dashboard: overview",
                Tab::Partitions => "dashboard: partitions",
                Tab::Logs => "dashboard: logs",
                Tab::Nodes => "dashboard: nodes",
                Tab::Findings => "dashboard: findings",
            };
            frames.push((name.to_owned(), to_ansi(terminal.backend().buffer())));
        }

        assert!(frames.iter().all(|(_, lines)| !lines.is_empty()));
        // the selected node's addresses, by location, and the one restatectl dials
        let nodes = frames[8].1.join("\n");
        assert!(nodes.contains("eu-west-1.az1"));
        assert!(nodes.contains("the address restatectl dials"));
        // N1 runs every server, and the nodes are sorted by the partitions they lead
        assert!(nodes.contains("listens on"));
        assert!(nodes.contains("LEADS ▼"));
    }
}

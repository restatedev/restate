use std::collections::{HashMap as StdHashMap, VecDeque};
use std::time::Duration;

use bilrost::Message;
use itertools::Itertools;
use restate_types::NodeId;
use restate_types::cluster_state::{ClusterState, NodeState as PublishedNodeState};
use restate_types::net::address::AdvertisedAddress;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, RestateVersion};

use super::*;

const A: GenerationalNodeId = GenerationalNodeId::new(1, 1);
const B: GenerationalNodeId = GenerationalNodeId::new(2, 1);
const C: GenerationalNodeId = GenerationalNodeId::new(3, 1);
const D: GenerationalNodeId = GenerationalNodeId::new(4, 1);

/// A compact, deterministic record of one production-state-machine event.
#[derive(Debug)]
struct TraceEntry {
    elapsed: Duration,
    event: String,
    actor: GenerationalNodeId,
    full_intervals: Option<u32>,
    stable: bool,
    peers: Vec<PeerTrace>,
}

#[derive(Debug)]
struct PeerTrace {
    node_id: GenerationalNodeId,
    gossip_age: u32,
    state: NodeState,
    published_state: PublishedNodeState,
}

#[derive(Debug)]
struct FdTrace {
    started_at: Instant,
    entries: Vec<TraceEntry>,
}

impl FdTrace {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            entries: Vec::new(),
        }
    }

    fn record(
        &mut self,
        event: impl Into<String>,
        actor: GenerationalNodeId,
        full_intervals: Option<u32>,
        stable: bool,
        peers: Vec<PeerTrace>,
    ) {
        self.entries.push(TraceEntry {
            elapsed: self.started_at.elapsed(),
            event: event.into(),
            actor,
            full_intervals,
            stable,
            peers,
        });
    }

    fn render(&self) -> String {
        self.entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let peers = entry
                    .peers
                    .iter()
                    .map(|peer| {
                        format!(
                            "{} age={} local={} published={:?}",
                            peer.node_id,
                            peer.gossip_age,
                            state_name(peer.state),
                            peer.published_state
                        )
                    })
                    .join(", ");
                format!(
                    "#{index} +{:?} actor={} event={} full_intervals={:?} stable={} [{}]",
                    entry.elapsed,
                    entry.actor,
                    entry.event,
                    entry.full_intervals,
                    entry.stable,
                    peers
                )
            })
            .join("\n")
    }
}

fn state_name(state: NodeState) -> &'static str {
    match state {
        NodeState::Alive => "Alive",
        NodeState::Suspect { .. } => "Suspect",
        NodeState::Dead => "Dead",
        NodeState::FailingOver => "FailingOver",
    }
}

struct FdActor {
    state: FdState,
    cluster_state: ClusterState,
}

impl FdActor {
    fn new(node_id: GenerationalNodeId, nodes_config: &NodesConfiguration) -> Self {
        let cluster_state = ClusterState::default();
        let state = FdState::new(
            node_id,
            nodes_config,
            PartitionReplicaSetStates::default(),
            cluster_state.clone().updater(),
        );
        Self {
            state,
            cluster_state,
        }
    }

    fn peer_state(&self, peer: GenerationalNodeId) -> NodeState {
        self.state.node_states[&peer.as_plain()].state
    }

    fn peer_age(&self, peer: GenerationalNodeId) -> u32 {
        self.state.node_states[&peer.as_plain()].gossip_age
    }

    fn establish_stable_alive_view(&mut self, opts: &GossipOptions) {
        self.state.num_gossip_received = opts.gossip_fd_stability_threshold.get() as usize;
        for node in self.state.node_states.values_mut() {
            node.gossip_age = 0;
            node.state = NodeState::Alive;
            self.state
                .cs_updater
                .upsert_node_state(node.gen_node_id, node.state.into());
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Delivery {
    Queue,
    Drop,
    Duplicate,
}

#[derive(Debug)]
struct QueuedGossip {
    from: GenerationalNodeId,
    message: Gossip,
}

/// Deterministically drives production `FdState` instances without a Tokio select loop.
struct FdSimulation {
    opts: GossipOptions,
    nodes_config: NodesConfiguration,
    actors: Vec<FdActor>,
    mailboxes: Vec<VecDeque<QueuedGossip>>,
    trace: FdTrace,
    local_stale_polls: StdHashMap<(GenerationalNodeId, PlainNodeId), u32>,
    normal_polls_since_discontinuity: StdHashMap<GenerationalNodeId, u32>,
}

impl FdSimulation {
    fn three_node_cluster() -> Self {
        Self::cluster([A, B, C])
    }

    fn four_node_cluster() -> Self {
        Self::cluster([A, B, C, D])
    }

    fn stable_four_node_cluster() -> Self {
        let mut simulation = Self::four_node_cluster();
        for actor in &mut simulation.actors {
            actor.establish_stable_alive_view(&simulation.opts);
        }
        simulation
    }

    fn cluster(node_ids: impl IntoIterator<Item = GenerationalNodeId>) -> Self {
        let opts = GossipOptions::default();
        let node_ids = node_ids.into_iter().collect_vec();
        let node_count = node_ids.len();
        let nodes_config = nodes_config(&node_ids);
        let actors = node_ids
            .iter()
            .copied()
            .map(|node_id| FdActor::new(node_id, &nodes_config))
            .collect_vec();
        let local_stale_polls = node_ids
            .iter()
            .flat_map(|&observer| {
                node_ids
                    .iter()
                    .filter(move |&&peer| peer != observer)
                    .map(move |peer| ((observer, peer.as_plain()), 0))
            })
            .collect();
        let normal_polls_since_discontinuity = node_ids
            .iter()
            .copied()
            .map(|node_id| {
                (
                    node_id,
                    opts.gossip_failure_threshold.get().saturating_add(1),
                )
            })
            .collect();
        Self {
            opts,
            nodes_config,
            actors,
            mailboxes: (0..node_count).map(|_| VecDeque::new()).collect(),
            trace: FdTrace::new(),
            local_stale_polls,
            normal_polls_since_discontinuity,
        }
    }

    fn stable_three_node_cluster() -> Self {
        let mut simulation = Self::three_node_cluster();
        for actor in &mut simulation.actors {
            actor.establish_stable_alive_view(&simulation.opts);
        }
        simulation
    }

    fn actor_index(node_id: GenerationalNodeId) -> usize {
        match node_id {
            A => 0,
            B => 1,
            C => 2,
            D => 3,
            _ => panic!("unexpected test node {node_id}"),
        }
    }

    fn actor(&self, node_id: GenerationalNodeId) -> &FdActor {
        &self.actors[Self::actor_index(node_id)]
    }

    fn actor_mut(&mut self, node_id: GenerationalNodeId) -> &mut FdActor {
        &mut self.actors[Self::actor_index(node_id)]
    }

    fn record(&mut self, event: impl Into<String>, actor: GenerationalNodeId, full: Option<u32>) {
        let fd_actor = self.actor(actor);
        let mut peers = fd_actor
            .state
            .node_states
            .values()
            .map(|node| PeerTrace {
                node_id: node.gen_node_id,
                gossip_age: node.gossip_age,
                state: node.state,
                published_state: fd_actor
                    .cluster_state
                    .get_node_state(NodeId::Generational(node.gen_node_id)),
            })
            .collect_vec();
        peers.sort_unstable_by_key(|peer| peer.node_id);
        self.trace.record(
            event,
            actor,
            full,
            fd_actor.state.is_stable(&self.opts),
            peers,
        );
    }

    async fn advance_one_interval(&mut self) {
        tokio::time::advance(*self.opts.gossip_tick_interval).await;
    }

    fn tick(&mut self, node_id: GenerationalNodeId) -> u32 {
        let opts = self.opts.clone();
        let full_intervals = self
            .actor(node_id)
            .state
            .last_gossip_tick
            .elapsed()
            .div_duration_f32(*opts.gossip_tick_interval)
            .floor() as u32;
        let interval_passed = {
            let actor = self.actor_mut(node_id);
            let interval_passed = actor.state.gossip_tick(&opts);
            if actor.state.is_stable(&opts) {
                actor.state.detect_peer_failures(&opts);
            }
            interval_passed
        };
        self.record(
            format!("tick(interval_passed={interval_passed})"),
            node_id,
            Some(full_intervals),
        );
        full_intervals
    }

    /// Models the rejected logical-wire experiment against main's production bulk-wire behavior.
    ///
    /// `FdState::gossip_tick` on this clean branch retains the wall-time age carried on the wire.
    /// The test-only experiment removes all but one observed interval from each remote peer.
    fn tick_with_logical_wire_aging(&mut self, node_id: GenerationalNodeId) -> u32 {
        let opts = self.opts.clone();
        let full_intervals = self
            .actor(node_id)
            .state
            .last_gossip_tick
            .elapsed()
            .div_duration_f32(*opts.gossip_tick_interval)
            .floor() as u32;
        {
            let actor = self.actor_mut(node_id);
            let interval_passed = actor.state.gossip_tick(&opts);
            debug_assert_eq!(interval_passed, full_intervals > 0);
            if full_intervals > 1 {
                for node in actor.state.node_states.values_mut() {
                    if node.gen_node_id != node_id && node.gossip_age != u32::MAX {
                        node.gossip_age = node
                            .gossip_age
                            .saturating_sub(full_intervals.saturating_sub(1));
                    }
                }
            }
            if actor.state.is_stable(&opts) {
                actor.state.detect_peer_failures(&opts);
            }
        }
        self.record(
            format!("logical-wire-tick(intervals={full_intervals})"),
            node_id,
            Some(full_intervals),
        );
        full_intervals
    }

    /// Names the bulk-wire baseline in mixed-policy tests.
    fn tick_with_legacy_wire_aging(&mut self, node_id: GenerationalNodeId) -> u32 {
        self.tick(node_id)
    }

    /// Models a wire-compatible local gate: bulk wire age is retained, but local age-only death
    /// requires eleven actual observer polls since fresh direct or indirect gossip about a peer.
    fn tick_with_local_dual_gate(&mut self, node_id: GenerationalNodeId) -> u32 {
        let opts = self.opts.clone();
        let full_intervals = self.tick_with_legacy_wire_aging_without_detection(node_id, &opts);
        if full_intervals > 0 {
            for peer in [A, B, C, D] {
                if peer != node_id {
                    *self
                        .local_stale_polls
                        .get_mut(&(node_id, peer.as_plain()))
                        .expect("test peer exists") += 1;
                }
            }
        }

        let local_stale_polls = self.local_stale_polls.clone();
        let guarded_ages = {
            let actor = self.actor_mut(node_id);
            actor
                .state
                .node_states
                .iter_mut()
                .filter_map(|(peer, node)| {
                    if node.gen_node_id == node_id {
                        return None;
                    }
                    let stale_polls = local_stale_polls[&(node_id, *peer)];
                    (stale_polls <= opts.gossip_failure_threshold.get()
                        && node.gossip_age > opts.gossip_failure_threshold.get())
                    .then(|| {
                        (
                            *peer,
                            std::mem::replace(
                                &mut node.gossip_age,
                                opts.gossip_failure_threshold.get(),
                            ),
                        )
                    })
                })
                .collect_vec()
        };
        {
            let actor = self.actor_mut(node_id);
            if actor.state.is_stable(&opts) {
                actor.state.detect_peer_failures(&opts);
            }
            for (peer, gossip_age) in guarded_ages {
                actor
                    .state
                    .node_states
                    .get_mut(&peer)
                    .expect("test peer exists")
                    .gossip_age = gossip_age;
            }
        }
        self.record(
            format!("dual-gate-tick(intervals={full_intervals})"),
            node_id,
            Some(full_intervals),
        );
        full_intervals
    }

    /// Models a local observer-discontinuity grace period. Gossip age and the wire format stay
    /// unchanged; only local age-based death decisions are delayed after a skipped timer tick.
    fn tick_with_observer_discontinuity_grace(&mut self, node_id: GenerationalNodeId) -> u32 {
        let opts = self.opts.clone();
        let full_intervals = self.tick_with_legacy_wire_aging_without_detection(node_id, &opts);
        let normal_polls = {
            let normal_polls = self
                .normal_polls_since_discontinuity
                .get_mut(&node_id)
                .expect("test observer exists");
            if full_intervals > 1 {
                *normal_polls = 0;
            } else if full_intervals > 0 {
                *normal_polls = normal_polls.saturating_add(1);
            }
            *normal_polls
        };

        let actor = self.actor_mut(node_id);
        if actor.state.is_stable(&opts) {
            if normal_polls <= opts.gossip_failure_threshold.get() {
                actor.state.detect_peer_failures_holding_age_expiry(&opts);
            } else {
                actor.state.detect_peer_failures(&opts);
            }
        }
        self.record(
            format!(
                "discontinuity-grace-tick(intervals={full_intervals}, normal_polls={normal_polls})"
            ),
            node_id,
            Some(full_intervals),
        );
        full_intervals
    }

    fn tick_with_legacy_wire_aging_without_detection(
        &mut self,
        node_id: GenerationalNodeId,
        opts: &GossipOptions,
    ) -> u32 {
        let full_intervals = self
            .actor(node_id)
            .state
            .last_gossip_tick
            .elapsed()
            .div_duration_f32(*opts.gossip_tick_interval)
            .floor() as u32;
        let actor = self.actor_mut(node_id);
        let interval_passed = actor.state.gossip_tick(opts);
        debug_assert_eq!(interval_passed, full_intervals > 0);
        full_intervals
    }

    fn send_gossip(
        &mut self,
        from: GenerationalNodeId,
        to: GenerationalNodeId,
        delivery: Delivery,
    ) {
        let opts = self.opts.clone();
        let message = self.actors[Self::actor_index(from)]
            .state
            .make_gossip_message(&opts, false, &self.nodes_config);
        let mailbox = &mut self.mailboxes[Self::actor_index(to)];
        match delivery {
            Delivery::Queue => mailbox.push_back(QueuedGossip { from, message }),
            Delivery::Drop => {}
            Delivery::Duplicate => {
                mailbox.push_back(QueuedGossip {
                    from,
                    message: message.clone(),
                });
                mailbox.push_back(QueuedGossip { from, message });
            }
        }
        self.record(format!("gossip {from} -> {to} {delivery:?}"), to, None);
    }

    fn deliver(&mut self, to: GenerationalNodeId, queued: QueuedGossip) {
        let opts = self.opts.clone();
        let nodes_config_version = self.nodes_config.version();
        let from = queued.from;
        let refreshed_peers = {
            let actor = self.actor_mut(to);
            assert!(actor.state.can_admit_message(
                &opts,
                from,
                nodes_config_version,
                &queued.message
            ));
            let previous_ages = queued
                .message
                .nodes
                .iter()
                .filter(|incoming| incoming.node_id.as_plain() != to.as_plain())
                .filter_map(|incoming| {
                    actor
                        .state
                        .node_states
                        .get(&incoming.node_id.as_plain())
                        .map(|node| (incoming.node_id.as_plain(), node.gossip_age))
                })
                .collect_vec();
            actor.state.update_from_gossip_message(
                &opts,
                from,
                nodes_config_version,
                queued.message,
            );
            previous_ages
                .into_iter()
                .filter_map(|(peer, previous_age)| {
                    (actor.state.node_states[&peer].gossip_age < previous_age).then_some(peer)
                })
                .collect_vec()
        };
        *self
            .local_stale_polls
            .get_mut(&(to, from.as_plain()))
            .expect("test peer exists") = 0;
        for peer in refreshed_peers {
            *self
                .local_stale_polls
                .get_mut(&(to, peer))
                .expect("test peer exists") = 0;
        }
        self.record(format!("deliver gossip {from} -> {to}"), to, None);
    }

    fn local_stale_polls(&self, observer: GenerationalNodeId, peer: GenerationalNodeId) -> u32 {
        self.local_stale_polls[&(observer, peer.as_plain())]
    }

    fn gossip_bytes(&mut self, from: GenerationalNodeId) -> Vec<u8> {
        let opts = self.opts.clone();
        let nodes_config = self.nodes_config.clone();
        let mut message =
            self.actor_mut(from)
                .state
                .make_gossip_message(&opts, false, &nodes_config);
        // Separate simulations use independently seeded maps and wall-clock timestamps. Normalize
        // those irrelevant fields so this is a byte-for-byte comparison of the wire content that
        // the baseline and grace decision policies control.
        message.instance_ts = MillisSinceEpoch::UNIX_EPOCH;
        message.sent_at = MillisSinceEpoch::UNIX_EPOCH;
        for node in &mut message.nodes {
            node.instance_ts = MillisSinceEpoch::UNIX_EPOCH;
        }
        message.nodes.sort_unstable_by_key(|node| node.node_id);
        message.encode_to_vec()
    }

    fn normal_polls_since_discontinuity(&self, observer: GenerationalNodeId) -> u32 {
        self.normal_polls_since_discontinuity[&observer]
    }

    fn deliver_next_from(&mut self, to: GenerationalNodeId, from: GenerationalNodeId) {
        let mailbox = &mut self.mailboxes[Self::actor_index(to)];
        let position = mailbox
            .iter()
            .position(|queued| queued.from == from)
            .expect("the requested gossip message is queued");
        let queued = mailbox.remove(position).expect("mailbox entry exists");
        self.deliver(to, queued);
    }

    fn drain_mailbox(&mut self, to: GenerationalNodeId) {
        while let Some(queued) = self.mailboxes[Self::actor_index(to)].pop_front() {
            self.deliver(to, queued);
        }
    }

    fn reset_tick_baseline(&mut self, node_id: GenerationalNodeId) {
        self.actor_mut(node_id).state.last_gossip_tick = Instant::now();
        self.record("reset FD tick baseline", node_id, Some(0));
    }

    fn reset_gossip_timing(&mut self, node_id: GenerationalNodeId) {
        self.actor_mut(node_id).state.reset_gossip_timing();
        self.record("reset FD gossip timing", node_id, Some(0));
    }

    fn mark_terminal_connection(&mut self, observer: GenerationalNodeId, peer: GenerationalNodeId) {
        self.actor_mut(observer)
            .state
            .node_states
            .get_mut(&peer.as_plain())
            .expect("test peer exists")
            .mark_terminally_closed_for_test();
        self.record(
            format!("terminal connection {observer} -> {peer}"),
            observer,
            None,
        );
    }

    fn trace(&self) -> String {
        self.trace.render()
    }
}

fn nodes_config(node_ids: &[GenerationalNodeId]) -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new_for_testing();
    for &node_id in node_ids {
        nodes_config.upsert_node(
            NodeConfig::builder()
                .name(format!("node-{node_id}"))
                .current_generation(node_id)
                .address(AdvertisedAddress::default())
                .roles(Role::Admin | Role::Worker)
                .binary_version(RestateVersion::current())
                .build(),
        );
    }
    nodes_config
}

async fn pause_a_while_b_and_c_continue(sim: &mut FdSimulation, intervals: u32) {
    for _ in 0..intervals {
        sim.advance_one_interval().await;
        keep_b_and_c_healthy(sim);
        sim.send_gossip(B, A, Delivery::Queue);
        sim.send_gossip(C, A, Delivery::Queue);
    }
}

fn keep_b_and_c_healthy(sim: &mut FdSimulation) {
    sim.tick(B);
    sim.tick(C);
    sim.send_gossip(B, C, Delivery::Queue);
    sim.deliver_next_from(C, B);
    sim.send_gossip(C, B, Delivery::Queue);
    sim.deliver_next_from(B, C);
}

async fn establish_a_stable_alive_view_with_real_gossip(sim: &mut FdSimulation) {
    for _ in 0..60 {
        for (from, to) in [(A, B), (A, C), (B, A), (B, C), (C, A), (C, B)] {
            sim.send_gossip(from, to, Delivery::Queue);
        }
        for node in [A, B, C] {
            sim.drain_mailbox(node);
        }
        sim.advance_one_interval().await;
        for node in [A, B, C] {
            sim.tick(node);
        }
    }
    for (from, to) in [(B, A), (C, A)] {
        sim.send_gossip(from, to, Delivery::Queue);
    }
    sim.drain_mailbox(A);
}

#[tokio::test(start_paused = true)]
async fn real_gossip_reaches_the_same_stable_alive_peer_fixture_as_the_fast_fixture() {
    let fast = FdSimulation::stable_three_node_cluster();
    let mut real = FdSimulation::three_node_cluster();
    establish_a_stable_alive_view_with_real_gossip(&mut real).await;

    assert!(
        real.actor(A).state.is_stable(&real.opts),
        "trace:\n{}",
        real.trace()
    );
    for peer in [B, C] {
        assert_eq!(
            real.actor(A).peer_state(peer),
            fast.actor(A).peer_state(peer)
        );
        assert_eq!(real.actor(A).peer_age(peer), fast.actor(A).peer_age(peer));
        assert_eq!(
            real.actor(A)
                .cluster_state
                .get_node_state(NodeId::Generational(peer)),
            fast.actor(A)
                .cluster_state
                .get_node_state(NodeId::Generational(peer)),
            "trace:\n{}",
            real.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn resetting_only_the_tokio_interval_does_not_prevent_startup_pre_aging() {
    let opts = GossipOptions::default();
    let mut actor = FdActor::new(A, &nodes_config(&[A, B, C]));
    actor.establish_stable_alive_view(&opts);
    let mut interval = tokio::time::interval(*opts.gossip_tick_interval);

    tokio::time::advance(*opts.gossip_tick_interval * 12).await;
    interval.reset_immediately();
    interval.tick().await;

    assert_eq!(
        actor
            .state
            .last_gossip_tick
            .elapsed()
            .div_duration_f32(*opts.gossip_tick_interval)
            .floor() as u32,
        12
    );
    assert!(actor.state.gossip_tick(&opts));
    actor.state.detect_peer_failures(&opts);
    assert_eq!(
        actor.peer_state(B),
        NodeState::Dead,
        "resetting the scheduler interval leaves FdState's age baseline in the past"
    );
}

#[tokio::test(start_paused = true)]
async fn resetting_fd_gossip_timing_excludes_startup_time() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    tokio::time::advance(
        *sim.opts.gossip_tick_interval * (sim.opts.gossip_loneliness_threshold.get() + 1),
    )
    .await;
    assert!(sim.actor(A).state.is_lonely(&sim.opts));

    sim.reset_gossip_timing(A);

    assert!(!sim.actor(A).state.is_lonely(&sim.opts));
    assert_eq!(sim.tick(A), 0, "trace:\n{}", sim.trace());
    for peer in [B, C] {
        assert_eq!(sim.actor(A).peer_age(peer), 0, "trace:\n{}", sim.trace());
        assert_eq!(sim.actor(A).peer_state(peer), NodeState::Alive);
    }

    sim.advance_one_interval().await;
    assert_eq!(sim.tick(A), 1, "trace:\n{}", sim.trace());
    assert_eq!(sim.actor(A).peer_age(B), 1);
    assert!(!sim.actor(A).state.is_lonely(&sim.opts));
}

#[tokio::test(start_paused = true)]
async fn paused_observer_bulk_ages_healthy_peers_to_dead() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    pause_a_while_b_and_c_continue(&mut sim, 12).await;
    assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace());
    for peer in [B, C] {
        assert_eq!(sim.actor(A).peer_age(peer), 12, "trace:\n{}", sim.trace());
        assert_eq!(
            sim.actor(A).peer_state(peer),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observed_failure_significant_pause_lengths_false_kill_healthy_peers() {
    for intervals in [12, 20, 29, 30, 38, 39, 51] {
        let mut sim = FdSimulation::stable_three_node_cluster();
        pause_a_while_b_and_c_continue(&mut sim, intervals).await;
        assert_eq!(sim.tick(A), intervals, "trace:\n{}", sim.trace());
        for peer in [B, C] {
            assert_eq!(
                sim.actor(A).peer_state(peer),
                NodeState::Dead,
                "pause={intervals}; trace:\n{}",
                sim.trace()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn draining_every_accumulated_message_before_the_delayed_tick_still_false_kills_healthy_peers()
 {
    let mut sim = FdSimulation::stable_three_node_cluster();
    pause_a_while_b_and_c_continue(&mut sim, 12).await;
    sim.drain_mailbox(A);
    assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace());
    assert_eq!(
        sim.actor(A).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(
        sim.actor(A).peer_state(C),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
}

#[tokio::test(start_paused = true)]
async fn terminal_connection_bypasses_a_timing_baseline_reset_after_an_observer_discontinuity() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    pause_a_while_b_and_c_continue(&mut sim, 12).await;
    sim.reset_tick_baseline(A);
    sim.mark_terminal_connection(A, B);

    assert_eq!(sim.tick(A), 0, "trace:\n{}", sim.trace());
    assert_eq!(
        sim.actor(A).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(
        sim.actor(A).peer_state(C),
        NodeState::Alive,
        "trace:\n{}",
        sim.trace()
    );
}

#[tokio::test(start_paused = true)]
async fn a_genuinely_silent_peer_during_an_observer_pause_is_eventually_detected() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    for _ in 0..12 {
        sim.advance_one_interval().await;
        sim.tick(C);
        sim.send_gossip(C, A, Delivery::Queue);
    }

    sim.tick(A);
    assert_eq!(
        sim.actor(A).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(
        sim.actor(A).peer_state(C),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );

    sim.drain_mailbox(A);
    assert_eq!(
        sim.actor(A).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(sim.actor(A).peer_age(C), 0, "trace:\n{}", sim.trace());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResumeEvent {
    DeliverB,
    DeliverC,
    DelayedTick,
}

#[tokio::test(start_paused = true)]
async fn resume_order_permutations_produce_divergent_views() {
    for (schedule, expected) in [
        (
            [
                ResumeEvent::DeliverB,
                ResumeEvent::DeliverC,
                ResumeEvent::DelayedTick,
            ],
            ("Dead", "Dead"),
        ),
        (
            [
                ResumeEvent::DeliverB,
                ResumeEvent::DelayedTick,
                ResumeEvent::DeliverC,
            ],
            ("Suspect", "Suspect"),
        ),
        (
            [
                ResumeEvent::DeliverC,
                ResumeEvent::DeliverB,
                ResumeEvent::DelayedTick,
            ],
            ("Dead", "Dead"),
        ),
        (
            [
                ResumeEvent::DeliverC,
                ResumeEvent::DelayedTick,
                ResumeEvent::DeliverB,
            ],
            ("Suspect", "Suspect"),
        ),
        (
            [
                ResumeEvent::DelayedTick,
                ResumeEvent::DeliverB,
                ResumeEvent::DeliverC,
            ],
            ("Suspect", "Suspect"),
        ),
        (
            [
                ResumeEvent::DelayedTick,
                ResumeEvent::DeliverC,
                ResumeEvent::DeliverB,
            ],
            ("Suspect", "Suspect"),
        ),
    ] {
        let mut sim = FdSimulation::stable_three_node_cluster();
        pause_a_while_b_and_c_continue(&mut sim, 12).await;
        for event in schedule {
            match event {
                ResumeEvent::DeliverB => sim.deliver_next_from(A, B),
                ResumeEvent::DeliverC => sim.deliver_next_from(A, C),
                ResumeEvent::DelayedTick => assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace()),
            }
        }
        sim.advance_one_interval().await;
        assert_eq!(sim.tick(A), 1, "trace:\n{}", sim.trace());
        assert_eq!(
            (
                state_name(sim.actor(A).peer_state(B)),
                state_name(sim.actor(A).peer_state(C))
            ),
            expected,
            "schedule={schedule:?}; trace:\n{}",
            sim.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn paused_observer_cannot_distinguish_a_silent_peer_from_a_healthy_queued_peer() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    for _ in 0..12 {
        sim.advance_one_interval().await;
        sim.tick(C);
        sim.send_gossip(C, A, Delivery::Queue);
    }
    assert_eq!(sim.tick(A), 12, "trace:\n{}", sim.trace());
    assert_eq!(
        sim.actor(A).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(
        sim.actor(A).peer_state(C),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
}

#[tokio::test(start_paused = true)]
async fn duplicate_and_dropped_gossip_are_explicitly_replayable() {
    let mut duplicate = FdSimulation::stable_three_node_cluster();
    duplicate.send_gossip(B, A, Delivery::Duplicate);
    duplicate.deliver_next_from(A, B);
    duplicate.deliver_next_from(A, B);
    assert_eq!(
        duplicate.actor(A).peer_state(B),
        NodeState::Alive,
        "trace:\n{}",
        duplicate.trace()
    );
    assert_eq!(
        duplicate.actor(A).peer_age(B),
        0,
        "trace:\n{}",
        duplicate.trace()
    );

    let mut dropped = FdSimulation::stable_three_node_cluster();
    dropped.send_gossip(B, A, Delivery::Drop);
    for _ in 0..11 {
        dropped.advance_one_interval().await;
        dropped.tick(A);
    }
    assert_eq!(
        dropped.actor(A).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        dropped.trace()
    );
}

fn keep_c_and_d_healthy_with_legacy_wire_aging(sim: &mut FdSimulation) {
    sim.tick_with_legacy_wire_aging(C);
    sim.tick_with_legacy_wire_aging(D);
    sim.send_gossip(C, D, Delivery::Queue);
    sim.deliver_next_from(D, C);
    sim.send_gossip(D, C, Delivery::Queue);
    sim.deliver_next_from(C, D);
}

async fn run_logical_a_against_legacy_c_for(
    sim: &mut FdSimulation,
    intervals_per_a_resume: u32,
    resumes: u32,
) {
    for _ in 0..resumes {
        for _ in 0..intervals_per_a_resume {
            sim.advance_one_interval().await;
            keep_c_and_d_healthy_with_legacy_wire_aging(sim);
            sim.send_gossip(C, A, Delivery::Queue);
        }
        assert!(
            sim.tick_with_logical_wire_aging(A) >= intervals_per_a_resume,
            "trace:\n{}",
            sim.trace()
        );
        sim.send_gossip(A, C, Delivery::Queue);
        sim.deliver_next_from(C, A);
        sim.drain_mailbox(A);
    }
}

async fn run_logical_a_against_dual_gated_c_for(
    sim: &mut FdSimulation,
    intervals_per_a_resume: u32,
    resumes: u32,
) {
    for _ in 0..resumes {
        for _ in 0..intervals_per_a_resume {
            sim.advance_one_interval().await;
            sim.tick_with_local_dual_gate(C);
            sim.tick_with_legacy_wire_aging(D);
            sim.send_gossip(C, A, Delivery::Queue);
        }
        assert!(
            sim.tick_with_logical_wire_aging(A) >= intervals_per_a_resume,
            "trace:\n{}",
            sim.trace()
        );
        sim.send_gossip(A, C, Delivery::Queue);
        sim.deliver_next_from(C, A);
        sim.drain_mailbox(A);
    }
}

async fn run_bulk_wire_a_against_dual_gated_c_for(
    sim: &mut FdSimulation,
    intervals_per_a_resume: u32,
    resumes: u32,
    drain_a_before_sending: bool,
) {
    for _ in 0..resumes {
        for _ in 0..intervals_per_a_resume {
            sim.advance_one_interval().await;
            sim.tick_with_local_dual_gate(C);
            sim.tick_with_legacy_wire_aging(D);
            sim.send_gossip(C, A, Delivery::Queue);
        }
        assert!(
            sim.tick_with_legacy_wire_aging(A) >= intervals_per_a_resume,
            "trace:\n{}",
            sim.trace()
        );
        if drain_a_before_sending {
            sim.drain_mailbox(A);
        }
        sim.send_gossip(A, C, Delivery::Queue);
        sim.deliver_next_from(C, A);
        if !drain_a_before_sending {
            sim.drain_mailbox(A);
        }
    }
}

async fn run_bulk_wire_a_against_grace_c_for(
    sim: &mut FdSimulation,
    intervals_per_a_resume: u32,
    resumes: u32,
) {
    for _ in 0..resumes {
        for _ in 0..intervals_per_a_resume {
            sim.advance_one_interval().await;
            sim.tick_with_observer_discontinuity_grace(C);
            sim.tick_with_legacy_wire_aging(D);
            sim.send_gossip(C, A, Delivery::Queue);
        }
        assert!(
            sim.tick_with_observer_discontinuity_grace(A) >= intervals_per_a_resume,
            "trace:\n{}",
            sim.trace()
        );
        sim.send_gossip(A, C, Delivery::Queue);
        sim.deliver_next_from(C, A);
        sim.drain_mailbox(A);
    }
}

#[tokio::test(start_paused = true)]
async fn a_thirty_interval_logical_observer_does_not_prevent_a_legacy_observer_from_detecting_b() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    run_logical_a_against_legacy_c_for(&mut sim, 30, 1).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(sim.actor(C).peer_age(B), 1, "trace:\n{}", sim.trace());
    assert_eq!(
        sim.actor(C).peer_state(A),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );

    sim.advance_one_interval().await;
    keep_c_and_d_healthy_with_legacy_wire_aging(&mut sim);
    assert!(matches!(
        sim.actor(C).peer_state(B),
        NodeState::Suspect { .. }
    ));
    assert!(matches!(
        sim.actor(C).peer_state(A),
        NodeState::Suspect { .. }
    ));

    run_logical_a_against_legacy_c_for(&mut sim, 30, 10).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
}

#[tokio::test(start_paused = true)]
async fn a_frequently_resuming_logical_observer_delays_a_legacy_observers_dead_peer_detection() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    // Without A's low, min-merged B age, C reaches Dead after eleven ordinary polls. A's
    // five-interval resumes instead keep lowering C's age through six resumes, postponing C's
    // first Dead transition until the seventh resume.
    run_logical_a_against_legacy_c_for(&mut sim, 5, 6).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Alive,
        "trace:\n{}",
        sim.trace()
    );
    assert!(sim.actor(C).peer_age(B) <= 10, "trace:\n{}", sim.trace());

    run_logical_a_against_legacy_c_for(&mut sim, 5, 1).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
}

#[tokio::test(start_paused = true)]
async fn a_wire_compatible_local_dual_gate_keeps_a_healthy_observer_on_wall_time_detection() {
    let mut sim = FdSimulation::stable_four_node_cluster();
    for poll in 1..=11 {
        for _ in 0..30 {
            sim.advance_one_interval().await;
            keep_c_and_d_healthy_with_legacy_wire_aging(&mut sim);
        }
        assert!(
            sim.tick_with_local_dual_gate(A) >= 30,
            "trace:\n{}",
            sim.trace()
        );
        sim.send_gossip(A, C, Delivery::Queue);
        sim.deliver_next_from(C, A);
        assert_eq!(
            sim.actor(A).peer_state(B),
            if poll == 11 {
                NodeState::Dead
            } else {
                NodeState::Alive
            },
            "poll={poll}; trace:\n{}",
            sim.trace()
        );
        assert_eq!(
            sim.actor(C).peer_state(B),
            NodeState::Dead,
            "trace:\n{}",
            sim.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_wire_compatible_local_dual_gate_accepts_fresh_indirect_observations() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    // A observes B only through C. Each message from C lowers A's wire age for B, which the
    // test-only policy treats as a fresh indirect observation rather than letting the local
    // counter accumulate as if C had never observed B.
    for poll in 1..=20 {
        sim.advance_one_interval().await;
        sim.tick_with_legacy_wire_aging(B);
        sim.tick_with_legacy_wire_aging(C);
        sim.send_gossip(B, C, Delivery::Queue);
        sim.deliver_next_from(C, B);
        sim.send_gossip(C, A, Delivery::Queue);
        sim.tick_with_local_dual_gate(A);
        sim.drain_mailbox(A);

        assert_eq!(
            sim.actor(A).peer_state(B),
            NodeState::Alive,
            "poll={poll}; trace:\n{}",
            sim.trace()
        );
        assert_eq!(
            sim.local_stale_polls(A, B),
            0,
            "poll={poll}; trace:\n{}",
            sim.trace()
        );
    }

    // Once B becomes silent, C continues to gossip its increasingly stale B age. It no longer
    // lowers A's B age, so it must not reset A's local freshness counter. A reaches Dead after
    // eleven resumed polls despite continuing indirect gossip from C.
    for poll in 1..=11 {
        sim.advance_one_interval().await;
        sim.tick_with_legacy_wire_aging(C);
        sim.send_gossip(C, A, Delivery::Queue);
        sim.tick_with_local_dual_gate(A);
        sim.drain_mailbox(A);
        assert_eq!(
            sim.actor(A).peer_state(B),
            if poll == 11 {
                NodeState::Dead
            } else {
                NodeState::Alive
            },
            "poll={poll}; trace:\n{}",
            sim.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_logical_wire_observer_can_indefinitely_refresh_a_dual_gate() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    // This is a rollout incompatibility of logical wire aging, not a dual-gate result: A's stale
    // low B age resets C's local counter every five C polls.
    run_logical_a_against_dual_gated_c_for(&mut sim, 5, 20).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Alive,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(sim.local_stale_polls(C, B), 0, "trace:\n{}", sim.trace());
}

#[tokio::test(start_paused = true)]
async fn a_bulk_wire_observer_can_indefinitely_replay_paused_stale_indirect_evidence() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    // This is the selected FD-loop order: tick first, send the gossip constructed by that tick,
    // then return to the select loop to consume the queued C -> A messages. Each later A tick
    // replays the oldest B age from its preceding paused mailbox batch, still below C's current
    // age, so A can indefinitely reset C's local freshness counter even with bulk wire aging.
    run_bulk_wire_a_against_dual_gated_c_for(&mut sim, 5, 20, false).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Alive,
        "trace:\n{}",
        sim.trace()
    );
    assert!(sim.local_stale_polls(C, B) < 11, "trace:\n{}", sim.trace());
}

#[tokio::test(start_paused = true)]
async fn draining_a_paused_observers_mailbox_before_sending_can_replay_stale_indirect_evidence() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    // This is an adversarial ordering control, not the selected FD-loop order. It establishes
    // why the production loop must construct and send its tick gossip before consuming queued
    // messages: processing C's old messages first gives A a low stale B age to replay to C.
    run_bulk_wire_a_against_dual_gated_c_for(&mut sim, 5, 20, true).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Alive,
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(sim.local_stale_polls(C, B), 0, "trace:\n{}", sim.trace());
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_keeps_healthy_queued_peers_alive() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    for _ in 0..30 {
        sim.advance_one_interval().await;
        sim.tick_with_legacy_wire_aging(B);
        sim.tick_with_legacy_wire_aging(C);
        sim.send_gossip(B, A, Delivery::Queue);
        sim.send_gossip(C, A, Delivery::Queue);
    }

    assert!(
        sim.tick_with_observer_discontinuity_grace(A) >= 30,
        "trace:\n{}",
        sim.trace()
    );
    for peer in [B, C] {
        assert_eq!(sim.actor(A).peer_state(peer), NodeState::Alive);
    }
    sim.drain_mailbox(A);
    assert_eq!(sim.normal_polls_since_discontinuity(A), 0);
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_detects_a_silent_peer_after_eleven_normal_polls() {
    let mut sim = FdSimulation::stable_four_node_cluster();
    tokio::time::advance(*sim.opts.gossip_tick_interval * 30).await;
    sim.tick_with_observer_discontinuity_grace(A);

    for poll in 1..=11 {
        sim.advance_one_interval().await;
        sim.tick_with_observer_discontinuity_grace(A);
        assert_eq!(
            sim.actor(A).peer_state(B),
            if poll == 11 {
                NodeState::Dead
            } else {
                NodeState::Alive
            },
            "poll={poll}; trace:\n{}",
            sim.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_restarts_after_each_stall() {
    let mut sim = FdSimulation::stable_four_node_cluster();
    for stall in 0..2 {
        tokio::time::advance(*sim.opts.gossip_tick_interval * 12).await;
        sim.tick_with_observer_discontinuity_grace(A);
        if stall == 0 {
            for _ in 0..5 {
                sim.advance_one_interval().await;
                sim.tick_with_observer_discontinuity_grace(A);
            }
        }
        assert_eq!(sim.actor(A).peer_state(B), NodeState::Alive);
    }

    for poll in 1..=11 {
        sim.advance_one_interval().await;
        sim.tick_with_observer_discontinuity_grace(A);
        assert_eq!(
            sim.actor(A).peer_state(B),
            if poll == 11 {
                NodeState::Dead
            } else {
                NodeState::Alive
            },
            "poll={poll}; trace:\n{}",
            sim.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_handles_startup_pre_aging_and_normal_transport_loss() {
    let mut startup = FdSimulation::stable_three_node_cluster();
    tokio::time::advance(*startup.opts.gossip_tick_interval * 12).await;
    assert_eq!(startup.tick_with_observer_discontinuity_grace(A), 12);
    assert_eq!(startup.actor(A).peer_state(B), NodeState::Alive);
    assert_eq!(startup.normal_polls_since_discontinuity(A), 0);

    let mut normal = FdSimulation::stable_three_node_cluster();
    for poll in 1..=11 {
        normal.advance_one_interval().await;
        normal.tick_with_observer_discontinuity_grace(A);
        assert_eq!(
            normal.actor(A).peer_state(B),
            if poll == 11 {
                NodeState::Dead
            } else {
                NodeState::Alive
            },
            "poll={poll}; trace:\n{}",
            normal.trace()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_preserves_bulk_wire_stale_age_delay_for_other_observers() {
    let mut sim = FdSimulation::stable_four_node_cluster();

    // C never misses a tick, so its observer-local grace is inactive. A's paused mailbox still
    // replays a low bulk-wire B age. The grace policy does not change this pre-existing
    // cross-observer gossip-age delay.
    run_bulk_wire_a_against_grace_c_for(&mut sim, 5, 2).await;
    assert!(
        sim.normal_polls_since_discontinuity(C) > sim.opts.gossip_failure_threshold.get(),
        "trace:\n{}",
        sim.trace()
    );
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Alive,
        "trace:\n{}",
        sim.trace()
    );

    run_bulk_wire_a_against_grace_c_for(&mut sim, 5, 1).await;
    assert_eq!(
        sim.actor(C).peer_state(B),
        NodeState::Dead,
        "trace:\n{}",
        sim.trace()
    );
}

#[tokio::test(start_paused = true)]
async fn terminal_connection_bypasses_observer_discontinuity_grace() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    tokio::time::advance(*sim.opts.gossip_tick_interval * 12).await;
    sim.tick_with_observer_discontinuity_grace(A);
    sim.mark_terminal_connection(A, B);

    sim.advance_one_interval().await;
    sim.tick_with_observer_discontinuity_grace(A);
    assert_eq!(sim.actor(A).peer_state(B), NodeState::Dead);
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_never_reanimates_a_dead_peer_without_gossip() {
    let mut sim = FdSimulation::stable_three_node_cluster();
    for _ in 0..11 {
        sim.advance_one_interval().await;
        sim.tick_with_legacy_wire_aging(A);
    }
    assert_eq!(sim.actor(A).peer_state(B), NodeState::Dead);

    // More than five seconds of repeated stalls must hold the terminal local state; no clamped
    // age may manufacture a Dead -> Suspect -> Alive recovery or reset the published view.
    for _ in 0..6 {
        tokio::time::advance(*sim.opts.gossip_tick_interval * 12).await;
        sim.tick_with_observer_discontinuity_grace(A);
        assert_eq!(sim.actor(A).peer_state(B), NodeState::Dead);
        assert_eq!(
            sim.actor(A)
                .cluster_state
                .get_node_state(NodeId::Generational(B)),
            PublishedNodeState::Dead
        );
    }
}

#[tokio::test(start_paused = true)]
async fn observer_discontinuity_grace_preserves_dead_decisions_and_gossip_bytes() {
    let mut baseline = FdSimulation::stable_three_node_cluster();
    let mut grace = FdSimulation::stable_three_node_cluster();

    // This compact corpus includes ordinary ticks, two observer discontinuities, and the
    // recovery period following each. Grace may defer an age-only Dead, but can never introduce
    // one before baseline. It must never change the wire message.
    for intervals in [1, 12, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 12, 1, 1, 1] {
        tokio::time::advance(*baseline.opts.gossip_tick_interval * intervals).await;
        baseline.tick_with_legacy_wire_aging(A);
        grace.tick_with_observer_discontinuity_grace(A);

        for peer in [B, C] {
            assert!(
                grace.actor(A).peer_state(peer) != NodeState::Dead
                    || baseline.actor(A).peer_state(peer) == NodeState::Dead,
                "peer={peer}; baseline trace:\n{}\ngrace trace:\n{}",
                baseline.trace(),
                grace.trace()
            );
        }
        assert_eq!(baseline.gossip_bytes(A), grace.gossip_bytes(A));
    }

    // With no discontinuity, both decision paths—including a real direct-gossip recovery—are
    // identical. The direct message is the only evidence that permits improvement.
    let mut baseline = FdSimulation::stable_three_node_cluster();
    let mut grace = FdSimulation::stable_three_node_cluster();
    for _ in 0..11 {
        baseline.advance_one_interval().await;
        baseline.tick_with_legacy_wire_aging(A);
        grace.tick_with_observer_discontinuity_grace(A);
    }
    baseline.send_gossip(B, A, Delivery::Queue);
    grace.send_gossip(B, A, Delivery::Queue);
    baseline.drain_mailbox(A);
    grace.drain_mailbox(A);
    baseline.advance_one_interval().await;
    baseline.tick_with_legacy_wire_aging(A);
    grace.tick_with_observer_discontinuity_grace(A);
    assert_eq!(
        baseline.actor(A).peer_state(B),
        grace.actor(A).peer_state(B)
    );
    assert_eq!(baseline.gossip_bytes(A), grace.gossip_bytes(A));
}

mod partition_7 {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use restate_admin::cluster_controller::service::scheduler_test_support::{
        PartitionEvaluation, ReconfigurationGate, evaluate_partition,
    };
    use restate_types::Version;
    use restate_types::cluster::cluster_state::{
        AliveNode, LegacyClusterState, NodeState as LegacyNodeState, PartitionProcessorStatus,
        ReplayStatus, RunMode,
    };
    use restate_types::identifiers::PartitionId;
    use restate_types::net::partition_processor_manager::ProcessorCommand;
    use restate_types::partitions::PartitionConfiguration;
    use restate_types::replication::{NodeSet, ReplicationProperty};
    use restate_types::time::MillisSinceEpoch;

    use super::*;

    const PARTITION: PartitionId = PartitionId::MIN;

    #[derive(Debug, Clone, Copy)]
    enum Event {
        Evaluate,
        StopOldLeader,
    }

    fn configuration(nodes: impl IntoIterator<Item = u32>) -> PartitionConfiguration {
        let replica_set: NodeSet = nodes.into_iter().map(PlainNodeId::from).collect();
        PartitionConfiguration::new(
            ReplicationProperty::new_unchecked(u8::try_from(replica_set.len()).unwrap()),
            replica_set,
            Default::default(),
        )
    }

    fn statuses(cold_is_active: bool) -> LegacyClusterState {
        let node_status = |node_id: u32, replay_status| {
            let mut partitions = BTreeMap::new();
            partitions.insert(
                PARTITION,
                PartitionProcessorStatus {
                    replay_status,
                    effective_mode: if node_id == 3 {
                        RunMode::Leader
                    } else {
                        RunMode::Follower
                    },
                    ..PartitionProcessorStatus::default()
                },
            );
            (
                PlainNodeId::from(node_id),
                LegacyNodeState::Alive(AliveNode {
                    last_heartbeat_at: MillisSinceEpoch::now(),
                    generational_node_id: GenerationalNodeId::new(node_id, 1),
                    partitions,
                    uptime: Duration::ZERO,
                }),
            )
        };

        LegacyClusterState {
            last_refreshed: None,
            nodes_config_version: Version::INVALID,
            partition_table_version: Version::INVALID,
            logs_metadata_version: Version::INVALID,
            nodes: [
                node_status(
                    1,
                    if cold_is_active {
                        ReplayStatus::Active
                    } else {
                        ReplayStatus::Starting
                    },
                ),
                node_status(2, ReplayStatus::Active),
                // This deliberately remains stale after N2 is stopped. The scheduler must combine
                // the PP status with the FD-published ClusterState when selecting a leader.
                node_status(3, ReplayStatus::Active),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn format_configuration(configuration: &PartitionConfiguration) -> String {
        configuration
            .replica_set()
            .iter()
            .map(ToString::to_string)
            .join(",")
    }

    fn record_evaluation(
        trace: &mut Vec<String>,
        sim: &FdSimulation,
        evaluation: &PartitionEvaluation,
    ) {
        trace.push(format!(
            "current=[{}] next=[{}] fd(n0={:?}, n1={:?}, n2={:?}) complete={} leader={:?} commands={:?}",
            format_configuration(&evaluation.current),
            evaluation
                .next
                .as_ref()
                .map(format_configuration)
                .unwrap_or_default(),
            sim.actor(A).peer_state(A),
            sim.actor(A).peer_state(B),
            sim.actor(A).peer_state(C),
            evaluation.completed_reconfiguration,
            evaluation.target_leader,
            evaluation.commands,
        ));
    }

    async fn stop_old_leader(sim: &mut FdSimulation) {
        for _ in 0..=sim.opts.gossip_failure_threshold.get() {
            sim.advance_one_interval().await;
            // N1 and the controller both make a real gossip round. This keeps direct B evidence
            // fresh without replaying a stale low age for C through B's outgoing view.
            sim.tick_with_legacy_wire_aging(B);
            sim.send_gossip(B, A, Delivery::Queue);
            sim.drain_mailbox(A);
            // `f926c5dbb` is intentionally experimental. This helper models current-main's
            // bulk wire age while still executing production FdState transition/merge logic.
            sim.tick_with_legacy_wire_aging(A);
        }
        assert_eq!(sim.actor(A).peer_state(B), NodeState::Alive);
        assert_eq!(sim.actor(A).peer_state(C), NodeState::Dead);
    }

    fn evaluate(
        sim: &FdSimulation,
        current: PartitionConfiguration,
        next: Option<PartitionConfiguration>,
        gate: ReconfigurationGate,
        cold_is_active: bool,
    ) -> PartitionEvaluation {
        evaluate_partition(
            PARTITION,
            current,
            next,
            &sim.actor(A).cluster_state,
            &statuses(cold_is_active),
            &sim.nodes_config,
            gate,
        )
    }

    async fn run(
        gate: ReconfigurationGate,
        events: &[Event],
        cold_is_active: bool,
        falsely_exclude_warm: bool,
    ) -> (PartitionEvaluation, Vec<String>) {
        let mut sim = FdSimulation::stable_three_node_cluster();
        let mut current = configuration([3, 2]);
        let mut next = Some(configuration([3, 1]));
        let mut trace = vec![
            "initial: current=[3,2] next=[3,1]; n2=Active leader, n1=Active warm, n0=Starting cold"
                .to_owned(),
        ];
        for event in events {
            match event {
                Event::StopOldLeader => {
                    stop_old_leader(&mut sim).await;
                    trace.push("stop n2; FD publishes n2=Dead while n1 remains Alive".to_owned());
                }
                Event::Evaluate => {
                    let evaluation = evaluate(&sim, current, next, gate, cold_is_active);
                    current = evaluation.current.clone();
                    next = evaluation.next.clone();
                    record_evaluation(&mut trace, &sim, &evaluation);
                }
            }
        }

        if falsely_exclude_warm {
            sim.mark_terminal_connection(A, B);
            sim.advance_one_interval().await;
            sim.tick_with_legacy_wire_aging(A);
            trace.push("inject false FD exclusion for warm n1".to_owned());
        }

        let evaluation = evaluate(&sim, current, next, gate, cold_is_active);
        record_evaluation(&mut trace, &sim, &evaluation);
        (evaluation, trace)
    }

    fn assert_leader_command(evaluation: &PartitionEvaluation, node: PlainNodeId) {
        assert!(
            evaluation.commands.get(&node).is_some_and(|commands| {
                commands
                    .iter()
                    .any(|command| command.command == ProcessorCommand::Leader)
            }),
            "expected a leader command for {node}; commands={:?}",
            evaluation.commands
        );
    }

    #[tokio::test(start_paused = true)]
    async fn p7_current_gate_has_a_cold_leader_trace_in_both_event_orderings() {
        for events in [
            [Event::Evaluate, Event::StopOldLeader],
            [Event::StopOldLeader, Event::Evaluate],
        ] {
            let (evaluation, trace) =
                run(ReconfigurationGate::Current, &events, false, false).await;
            assert_eq!(
                format_configuration(&evaluation.current),
                "N3,N1",
                "trace:\n{}",
                trace.join("\n")
            );
            assert_eq!(
                evaluation.target_leader,
                Some(A.as_plain()),
                "trace:\n{}",
                trace.join("\n")
            );
            assert_leader_command(&evaluation, A.as_plain());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn p7_added_replica_gate_keeps_the_warm_follower_eligible() {
        for events in [
            [Event::Evaluate, Event::StopOldLeader],
            [Event::StopOldLeader, Event::Evaluate],
        ] {
            let (evaluation, trace) = run(
                ReconfigurationGate::WaitForAddedReplica,
                &events,
                false,
                false,
            )
            .await;
            assert!(
                !evaluation.completed_reconfiguration,
                "trace:\n{}",
                trace.join("\n")
            );
            assert_eq!(
                format_configuration(&evaluation.current),
                "N3,N2",
                "trace:\n{}",
                trace.join("\n")
            );
            assert_eq!(
                evaluation.target_leader,
                Some(B.as_plain()),
                "trace:\n{}",
                trace.join("\n")
            );
            assert_leader_command(&evaluation, B.as_plain());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn p7_added_replica_gate_never_promotes_cold_n0_when_warm_n1_is_falsely_excluded() {
        let (evaluation, trace) = run(
            ReconfigurationGate::WaitForAddedReplica,
            &[Event::StopOldLeader],
            false,
            true,
        )
        .await;

        assert!(
            !evaluation.completed_reconfiguration,
            "trace:\n{}",
            trace.join("\n")
        );
        assert_eq!(
            format_configuration(&evaluation.current),
            "N3,N2",
            "trace:\n{}",
            trace.join("\n")
        );
        assert_ne!(
            evaluation.target_leader,
            Some(A.as_plain()),
            "trace:\n{}",
            trace.join("\n")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn p7_added_replica_gate_allows_completion_after_cold_n0_is_current_generation_active() {
        let (evaluation, trace) =
            run(ReconfigurationGate::WaitForAddedReplica, &[], true, false).await;

        assert!(
            evaluation.completed_reconfiguration,
            "trace:\n{}",
            trace.join("\n")
        );
        assert_eq!(
            format_configuration(&evaluation.current),
            "N3,N1",
            "trace:\n{}",
            trace.join("\n")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn removal_only_gate_requires_a_retained_current_generation_replica() {
        let sim = FdSimulation::stable_three_node_cluster();
        let mut stale_status = statuses(false);
        let Some(LegacyNodeState::Alive(node)) = stale_status.nodes.get_mut(&B.as_plain()) else {
            panic!("N2 must have an Alive status");
        };
        node.generational_node_id = GenerationalNodeId::new(2, 2);

        let stale = evaluate_partition(
            PARTITION,
            configuration([3, 2]),
            Some(configuration([2])),
            &sim.actor(A).cluster_state,
            &stale_status,
            &sim.nodes_config,
            ReconfigurationGate::WaitForAddedReplica,
        );
        assert!(
            !stale.completed_reconfiguration,
            "a stale Active status cannot complete a removal-only transition"
        );

        let current_generation = evaluate_partition(
            PARTITION,
            configuration([3, 2]),
            Some(configuration([2])),
            &sim.actor(A).cluster_state,
            &statuses(false),
            &sim.nodes_config,
            ReconfigurationGate::WaitForAddedReplica,
        );
        assert!(current_generation.completed_reconfiguration);
        assert_eq!(format_configuration(&current_generation.current), "N2");
    }

    #[tokio::test(start_paused = true)]
    async fn p7_rf_two_to_three_waits_for_the_added_cold_replica() {
        let mut sim = FdSimulation::stable_three_node_cluster();
        stop_old_leader(&mut sim).await;

        let evaluation = evaluate_partition(
            PARTITION,
            configuration([3, 2]),
            Some(configuration([3, 2, 1])),
            &sim.actor(A).cluster_state,
            &statuses(false),
            &sim.nodes_config,
            ReconfigurationGate::WaitForAddedReplica,
        );

        assert!(!evaluation.completed_reconfiguration);
        assert_eq!(format_configuration(&evaluation.current), "N3,N2");
        assert_eq!(evaluation.target_leader, Some(B.as_plain()));
        assert_leader_command(&evaluation, B.as_plain());
    }
}

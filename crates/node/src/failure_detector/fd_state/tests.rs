use std::collections::VecDeque;
use std::time::Duration;

use itertools::Itertools;
use restate_types::NodeId;
use restate_types::cluster_state::{ClusterState, NodeState as PublishedNodeState};
use restate_types::net::address::AdvertisedAddress;
use restate_types::nodes_config::{NodeConfig, NodesConfiguration, Role};
use restate_types::{GenerationalNodeId, RestateVersion};

use super::*;

const A: GenerationalNodeId = GenerationalNodeId::new(1, 1);
const B: GenerationalNodeId = GenerationalNodeId::new(2, 1);
const C: GenerationalNodeId = GenerationalNodeId::new(3, 1);

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
}

impl FdSimulation {
    fn three_node_cluster() -> Self {
        let opts = GossipOptions::default();
        let nodes_config = nodes_config();
        let actors = [A, B, C]
            .into_iter()
            .map(|node_id| FdActor::new(node_id, &nodes_config))
            .collect_vec();
        Self {
            opts,
            nodes_config,
            actors,
            mailboxes: (0..3).map(|_| VecDeque::new()).collect(),
            trace: FdTrace::new(),
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
        let actor = self.actor_mut(to);
        assert!(
            actor
                .state
                .can_admit_message(&opts, from, nodes_config_version, &queued.message)
        );
        actor
            .state
            .update_from_gossip_message(&opts, from, nodes_config_version, queued.message);
        self.record(format!("deliver gossip {from} -> {to}"), to, None);
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

fn nodes_config() -> NodesConfiguration {
    let mut nodes_config = NodesConfiguration::new_for_testing();
    for node_id in [A, B, C] {
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
    let mut actor = FdActor::new(A, &nodes_config());
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

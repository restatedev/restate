use std::collections::HashSet;
use std::hash::Hasher;

use xxhash_rust::xxh3::Xxh3;

// Keep in sync with restate_types::replication::HASH_SALT. Changing this salt reshuffles every
// simulated placement.
const HASH_SALT: u64 = 14_712_721_395_741_015_273;
const PARTITION_RF: usize = 2;
const LOG_NODESET_SIZE: usize = 3;

type Node = usize;

#[derive(Clone, Debug)]
struct Placement {
    replicas: Vec<Node>,
    leader: Node,
    nodeset: Vec<Node>,
}

#[derive(Clone, Copy, Debug)]
enum BalancedPolicy {
    CurrentHrw,
    TopM { m: usize },
    All,
}

#[derive(Clone, Copy, Debug)]
enum LeaderPolicy {
    CurrentTie,
    Balanced,
}

#[derive(Clone, Copy, Debug)]
enum StabilityPolicy {
    Stateless,
    RepairOnly,
    RepairDownRestoreWhenAllAlive,
}

#[derive(Clone, Copy, Debug)]
struct Algorithm {
    name: &'static str,
    replica_policy: BalancedPolicy,
    leader_policy: LeaderPolicy,
    nodeset_policy: BalancedPolicy,
    stability_policy: StabilityPolicy,
}

#[derive(Clone, Debug)]
struct Fairness {
    counts: Vec<usize>,
    range: usize,
    rmse: f64,
}

#[derive(Clone, Debug, Default)]
struct Movement {
    changed_partitions: usize,
    replica_replacements: usize,
    leader_changes: usize,
    changed_nodesets: usize,
    nodeset_replacements: usize,
}

#[derive(Clone, Debug)]
struct RunResult {
    nodes: usize,
    partitions: usize,
    algorithm: &'static str,
    initial_replica: Fairness,
    initial_leader: Fairness,
    initial_nodeset: Fairness,
    final_replica: Fairness,
    final_leader: Fairness,
    final_nodeset: Fairness,
    worst_down_replica_range: usize,
    worst_down_leader_range: usize,
    worst_down_nodeset_range: usize,
    rolling: Movement,
}

fn hash_node(salt: u64, id: usize, node: Node) -> u64 {
    let mut hasher = Xxh3::with_seed(salt);
    hasher.write_u64(id as u64);
    hasher.write_u64(node as u64);
    hasher.finish()
}

fn hrw_order(alive: &[Node], id: usize, salt: u64) -> Vec<Node> {
    let mut nodes = alive.to_vec();
    nodes.sort_by(|a, b| {
        hash_node(salt, id, *b)
            .cmp(&hash_node(salt, id, *a))
            .then_with(|| a.cmp(b))
    });
    nodes
}

fn natural_leader(replicas: &[Node], partition_id: usize, salt: u64) -> Node {
    replicas
        .iter()
        .copied()
        .min_by_key(|node| hash_node(salt, partition_id, *node))
        .expect("replica set is non-empty")
}

fn select_balanced_member(
    order: &[Node],
    selected: &[Node],
    counts: &[usize],
    policy: BalancedPolicy,
) -> Node {
    let limit = match policy {
        BalancedPolicy::CurrentHrw => 1,
        BalancedPolicy::TopM { m } => m,
        BalancedPolicy::All => usize::MAX,
    };
    let mut pool = order
        .iter()
        .copied()
        .filter(|node| !selected.contains(node))
        .take(limit)
        .collect::<Vec<_>>();

    if pool.is_empty() {
        pool = order
            .iter()
            .copied()
            .filter(|node| !selected.contains(node))
            .collect();
    }

    pool.into_iter()
        .min_by_key(|node| {
            let rank = order
                .iter()
                .position(|candidate| candidate == node)
                .expect("candidate is in order");
            (counts[*node], rank)
        })
        .expect("enough live nodes")
}

fn select_stateless_replicas(
    policy: BalancedPolicy,
    node_count: usize,
    partition_count: usize,
    alive: &[Node],
    salt: u64,
) -> Vec<Vec<Node>> {
    let mut replica_counts = vec![0; node_count];
    let mut replicas = Vec::with_capacity(partition_count);

    for pid in 0..partition_count {
        let order = hrw_order(alive, pid, salt);
        let mut selected = Vec::with_capacity(PARTITION_RF);
        while selected.len() < PARTITION_RF {
            let node = select_balanced_member(&order, &selected, &replica_counts, policy);
            selected.push(node);
            replica_counts[node] += 1;
        }
        replicas.push(selected);
    }

    replicas
}

fn repair_replicas(
    policy: BalancedPolicy,
    node_count: usize,
    partition_count: usize,
    alive: &[Node],
    previous: &[Placement],
    salt: u64,
) -> Vec<Vec<Node>> {
    let alive_set = alive.iter().copied().collect::<HashSet<_>>();
    let mut replica_counts = vec![0; node_count];
    let mut replicas = vec![Vec::new(); partition_count];
    let mut needs_repair = Vec::new();

    for pid in 0..partition_count {
        replicas[pid] = previous[pid]
            .replicas
            .iter()
            .copied()
            .filter(|node| alive_set.contains(node))
            .collect();
        if replicas[pid].len() < PARTITION_RF {
            needs_repair.push(pid);
        }
        for node in &replicas[pid] {
            replica_counts[*node] += 1;
        }
    }

    for pid in needs_repair {
        let order = hrw_order(alive, pid, salt);
        while replicas[pid].len() < PARTITION_RF {
            let node = select_balanced_member(&order, &replicas[pid], &replica_counts, policy);
            replicas[pid].push(node);
            replica_counts[node] += 1;
        }
    }

    replicas
}

fn assign_leaders(
    policy: LeaderPolicy,
    replicas: Vec<Vec<Node>>,
    node_count: usize,
    alive: &[Node],
    previous: Option<&[Placement]>,
    salt: u64,
) -> Vec<Node> {
    let alive_set = alive.iter().copied().collect::<HashSet<_>>();
    let mut leaders = vec![0; replicas.len()];
    let mut leader_counts = vec![0; node_count];
    let mut needs_leader = Vec::new();

    for pid in 0..replicas.len() {
        if let Some(previous) = previous {
            let old = previous[pid].leader;
            if replicas[pid].contains(&old) && alive_set.contains(&old) {
                leaders[pid] = old;
                leader_counts[old] += 1;
                continue;
            }
        }
        needs_leader.push(pid);
    }

    for pid in needs_leader {
        let natural = natural_leader(&replicas[pid], pid, salt);
        let leader = match policy {
            LeaderPolicy::CurrentTie => natural,
            LeaderPolicy::Balanced => replicas[pid]
                .iter()
                .copied()
                .min_by_key(|node| {
                    (
                        leader_counts[*node],
                        usize::from(*node != natural),
                        std::cmp::Reverse(hash_node(salt, pid, *node)),
                    )
                })
                .expect("replica set is non-empty"),
        };
        leaders[pid] = leader;
        leader_counts[leader] += 1;
    }

    leaders
}

fn select_stateless_nodesets(
    policy: BalancedPolicy,
    node_count: usize,
    partition_count: usize,
    alive: &[Node],
    leaders: &[Node],
    salt: u64,
) -> Vec<Vec<Node>> {
    let mut nodeset_counts = vec![0; node_count];
    let mut nodesets = Vec::with_capacity(partition_count);
    let target = LOG_NODESET_SIZE.min(alive.len());

    for pid in 0..partition_count {
        let mut selected = Vec::with_capacity(target);
        if alive.contains(&leaders[pid]) {
            selected.push(leaders[pid]);
            nodeset_counts[leaders[pid]] += 1;
        }

        let order = hrw_order(alive, pid, salt);
        while selected.len() < target {
            let node = select_balanced_member(&order, &selected, &nodeset_counts, policy);
            selected.push(node);
            nodeset_counts[node] += 1;
        }
        nodesets.push(selected);
    }

    nodesets
}

fn repair_nodesets(
    policy: BalancedPolicy,
    node_count: usize,
    partition_count: usize,
    alive: &[Node],
    leaders: &[Node],
    previous: &[Placement],
    salt: u64,
) -> Vec<Vec<Node>> {
    let alive_set = alive.iter().copied().collect::<HashSet<_>>();
    let target = LOG_NODESET_SIZE.min(alive.len());
    let mut nodeset_counts = vec![0; node_count];
    let mut nodesets = vec![Vec::new(); partition_count];
    let mut needs_repair = Vec::new();

    for pid in 0..partition_count {
        nodesets[pid] = previous[pid]
            .nodeset
            .iter()
            .copied()
            .filter(|node| alive_set.contains(node))
            .collect();
        if alive_set.contains(&leaders[pid]) && !nodesets[pid].contains(&leaders[pid]) {
            if nodesets[pid].len() == target {
                nodesets[pid].pop();
            }
            nodesets[pid].insert(0, leaders[pid]);
        }
        if nodesets[pid].len() < target {
            needs_repair.push(pid);
        }
        for node in &nodesets[pid] {
            nodeset_counts[*node] += 1;
        }
    }

    for pid in needs_repair {
        let order = hrw_order(alive, pid, salt);
        while nodesets[pid].len() < target {
            let node = select_balanced_member(&order, &nodesets[pid], &nodeset_counts, policy);
            nodesets[pid].push(node);
            nodeset_counts[node] += 1;
        }
    }

    nodesets
}

fn select(
    algorithm: Algorithm,
    node_count: usize,
    partition_count: usize,
    alive: &[Node],
    previous: Option<&[Placement]>,
    salt: u64,
) -> Vec<Placement> {
    let restore = matches!(
        algorithm.stability_policy,
        StabilityPolicy::RepairDownRestoreWhenAllAlive
    ) && alive.len() == node_count;

    let stateless = matches!(algorithm.stability_policy, StabilityPolicy::Stateless);
    let repair_previous = previous.filter(|_| !restore && !stateless);

    let replicas = if let Some(previous) = repair_previous {
        repair_replicas(
            algorithm.replica_policy,
            node_count,
            partition_count,
            alive,
            previous,
            salt,
        )
    } else {
        select_stateless_replicas(
            algorithm.replica_policy,
            node_count,
            partition_count,
            alive,
            salt,
        )
    };

    let leaders = assign_leaders(
        algorithm.leader_policy,
        replicas.clone(),
        node_count,
        alive,
        repair_previous,
        salt,
    );

    let nodesets = if let Some(previous) = repair_previous {
        repair_nodesets(
            algorithm.nodeset_policy,
            node_count,
            partition_count,
            alive,
            &leaders,
            previous,
            salt,
        )
    } else {
        select_stateless_nodesets(
            algorithm.nodeset_policy,
            node_count,
            partition_count,
            alive,
            &leaders,
            salt,
        )
    };

    replicas
        .into_iter()
        .zip(leaders)
        .zip(nodesets)
        .map(|((replicas, leader), nodeset)| Placement {
            replicas,
            leader,
            nodeset,
        })
        .collect()
}

fn fairness_from_counts(counts: Vec<usize>) -> Fairness {
    let ideal = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
    let min = *counts.iter().min().unwrap_or(&0);
    let max = *counts.iter().max().unwrap_or(&0);
    let rmse = (counts
        .iter()
        .map(|count| {
            let diff = *count as f64 - ideal;
            diff * diff
        })
        .sum::<f64>()
        / counts.len() as f64)
        .sqrt();

    Fairness {
        counts,
        range: max - min,
        rmse,
    }
}

fn fairness(placements: &[Placement], node_count: usize) -> (Fairness, Fairness, Fairness) {
    let mut replicas = vec![0; node_count];
    let mut leaders = vec![0; node_count];
    let mut nodesets = vec![0; node_count];
    for placement in placements {
        for node in &placement.replicas {
            replicas[*node] += 1;
        }
        leaders[placement.leader] += 1;
        for node in &placement.nodeset {
            nodesets[*node] += 1;
        }
    }
    (
        fairness_from_counts(replicas),
        fairness_from_counts(leaders),
        fairness_from_counts(nodesets),
    )
}

fn set_of(nodes: &[Node]) -> HashSet<Node> {
    nodes.iter().copied().collect()
}

fn movement(previous: &[Placement], next: &[Placement]) -> Movement {
    let mut movement = Movement::default();
    for (old, new) in previous.iter().zip(next) {
        let old_replicas = set_of(&old.replicas);
        let new_replicas = set_of(&new.replicas);
        if old_replicas != new_replicas {
            movement.changed_partitions += 1;
            movement.replica_replacements += old_replicas.difference(&new_replicas).count();
        }

        if old.leader != new.leader {
            movement.leader_changes += 1;
        }

        let old_nodeset = set_of(&old.nodeset);
        let new_nodeset = set_of(&new.nodeset);
        if old_nodeset != new_nodeset {
            movement.changed_nodesets += 1;
            movement.nodeset_replacements += old_nodeset.difference(&new_nodeset).count();
        }
    }
    movement
}

fn add_movement(total: &mut Movement, step: Movement) {
    total.changed_partitions += step.changed_partitions;
    total.replica_replacements += step.replica_replacements;
    total.leader_changes += step.leader_changes;
    total.changed_nodesets += step.changed_nodesets;
    total.nodeset_replacements += step.nodeset_replacements;
}

fn run_scenario(algorithm: Algorithm, nodes: usize, partitions: usize, salt: u64) -> RunResult {
    let all_alive = (0..nodes).collect::<Vec<_>>();
    let initial = select(algorithm, nodes, partitions, &all_alive, None, salt);
    let (initial_replica, initial_leader, initial_nodeset) = fairness(&initial, nodes);

    let mut current = initial;
    let mut rolling = Movement::default();
    let mut worst_down_replica_range = 0;
    let mut worst_down_leader_range = 0;
    let mut worst_down_nodeset_range = 0;

    for down_node in (0..nodes).rev() {
        let alive = (0..nodes)
            .filter(|node| *node != down_node)
            .collect::<Vec<_>>();
        let down = select(algorithm, nodes, partitions, &alive, Some(&current), salt);
        add_movement(&mut rolling, movement(&current, &down));
        let (replica_fairness, leader_fairness, nodeset_fairness) = fairness(&down, nodes);
        worst_down_replica_range = worst_down_replica_range.max(replica_fairness.range);
        worst_down_leader_range = worst_down_leader_range.max(leader_fairness.range);
        worst_down_nodeset_range = worst_down_nodeset_range.max(nodeset_fairness.range);
        current = down;

        let up = select(
            algorithm,
            nodes,
            partitions,
            &all_alive,
            Some(&current),
            salt,
        );
        add_movement(&mut rolling, movement(&current, &up));
        current = up;
    }

    let (final_replica, final_leader, final_nodeset) = fairness(&current, nodes);
    RunResult {
        nodes,
        partitions,
        algorithm: algorithm.name,
        initial_replica,
        initial_leader,
        initial_nodeset,
        final_replica,
        final_leader,
        final_nodeset,
        worst_down_replica_range,
        worst_down_leader_range,
        worst_down_nodeset_range,
        rolling,
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn count_string(counts: &[usize]) -> String {
    counts
        .iter()
        .map(|count| count.to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn average(values: &[usize]) -> f64 {
    values.iter().sum::<usize>() as f64 / values.len() as f64
}

fn percentile(values: &mut [usize], percentile: f64) -> usize {
    values.sort_unstable();
    let index = ((values.len() - 1) as f64 * percentile).round() as usize;
    values[index]
}

fn current_algorithm() -> Algorithm {
    Algorithm {
        name: "current",
        replica_policy: BalancedPolicy::CurrentHrw,
        leader_policy: LeaderPolicy::CurrentTie,
        nodeset_policy: BalancedPolicy::CurrentHrw,
        stability_policy: StabilityPolicy::Stateless,
    }
}

fn algorithms() -> Vec<Algorithm> {
    vec![
        current_algorithm(),
        Algorithm {
            name: "current+balanced-leaders",
            replica_policy: BalancedPolicy::CurrentHrw,
            leader_policy: LeaderPolicy::Balanced,
            nodeset_policy: BalancedPolicy::CurrentHrw,
            stability_policy: StabilityPolicy::Stateless,
        },
        Algorithm {
            name: "nodeset-top3-only+repair-restore",
            replica_policy: BalancedPolicy::CurrentHrw,
            leader_policy: LeaderPolicy::CurrentTie,
            nodeset_policy: BalancedPolicy::TopM { m: 3 },
            stability_policy: StabilityPolicy::RepairDownRestoreWhenAllAlive,
        },
        Algorithm {
            name: "partition-top3-only+repair-restore",
            replica_policy: BalancedPolicy::TopM { m: 3 },
            leader_policy: LeaderPolicy::Balanced,
            nodeset_policy: BalancedPolicy::CurrentHrw,
            stability_policy: StabilityPolicy::RepairDownRestoreWhenAllAlive,
        },
        Algorithm {
            name: "combined-top3+repair-only",
            replica_policy: BalancedPolicy::TopM { m: 3 },
            leader_policy: LeaderPolicy::Balanced,
            nodeset_policy: BalancedPolicy::TopM { m: 3 },
            stability_policy: StabilityPolicy::RepairOnly,
        },
        Algorithm {
            name: "combined-top3+repair-restore",
            replica_policy: BalancedPolicy::TopM { m: 3 },
            leader_policy: LeaderPolicy::Balanced,
            nodeset_policy: BalancedPolicy::TopM { m: 3 },
            stability_policy: StabilityPolicy::RepairDownRestoreWhenAllAlive,
        },
        Algorithm {
            name: "combined-all+repair-restore",
            replica_policy: BalancedPolicy::All,
            leader_policy: LeaderPolicy::Balanced,
            nodeset_policy: BalancedPolicy::All,
            stability_policy: StabilityPolicy::RepairDownRestoreWhenAllAlive,
        },
    ]
}

fn print_baseline() {
    let current = current_algorithm();
    println!("## Current algorithm, exact Restate salt");
    println!(
        "_Model: flat all-in-one nodes, partition RF=2, log nodeset size=3, sequencer=leader._"
    );
    println!(
        "| nodes | partitions | partition replicas | replica range | leaders | leader range | log nodesets | nodeset range |"
    );
    println!("|---:|---:|---|---:|---|---:|---|---:|");
    for nodes in [3, 5] {
        for partitions in [24, 48, 96, 128] {
            let result = run_scenario(current, nodes, partitions, HASH_SALT);
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {} |",
                nodes,
                partitions,
                count_string(&result.initial_replica.counts),
                result.initial_replica.range,
                count_string(&result.initial_leader.counts),
                result.initial_leader.range,
                count_string(&result.initial_nodeset.counts),
                result.initial_nodeset.range,
            );
        }
    }
    println!();
}

fn print_exact_comparison(algorithms: &[Algorithm]) {
    println!("## Exact-salt aggregate across 8 scenarios");
    println!(
        "| algorithm | avg leader range | max leader range | avg replica range | max replica range | avg nodeset range | max nodeset range | one-time replica moves | one-time leader moves | one-time nodeset moves | rolling replica moves | rolling leader moves | rolling nodeset moves |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

    for algorithm in algorithms {
        let mut results = Vec::new();
        let mut one_time = Movement::default();
        for nodes in [3, 5] {
            for partitions in [24, 48, 96, 128] {
                let current = select(
                    current_algorithm(),
                    nodes,
                    partitions,
                    &(0..nodes).collect::<Vec<_>>(),
                    None,
                    HASH_SALT,
                );
                let next = select(
                    *algorithm,
                    nodes,
                    partitions,
                    &(0..nodes).collect::<Vec<_>>(),
                    Some(&current),
                    HASH_SALT,
                );
                add_movement(&mut one_time, movement(&current, &next));
                results.push(run_scenario(*algorithm, nodes, partitions, HASH_SALT));
            }
        }

        let avg_leader_range = results
            .iter()
            .map(|r| r.initial_leader.range)
            .sum::<usize>() as f64
            / results.len() as f64;
        let avg_replica_range = results
            .iter()
            .map(|r| r.initial_replica.range)
            .sum::<usize>() as f64
            / results.len() as f64;
        let avg_nodeset_range = results
            .iter()
            .map(|r| r.initial_nodeset.range)
            .sum::<usize>() as f64
            / results.len() as f64;
        let max_leader_range = results
            .iter()
            .map(|r| r.initial_leader.range)
            .max()
            .unwrap();
        let max_replica_range = results
            .iter()
            .map(|r| r.initial_replica.range)
            .max()
            .unwrap();
        let max_nodeset_range = results
            .iter()
            .map(|r| r.initial_nodeset.range)
            .max()
            .unwrap();
        let rolling_replica = results
            .iter()
            .map(|r| r.rolling.replica_replacements)
            .sum::<usize>();
        let rolling_leader = results
            .iter()
            .map(|r| r.rolling.leader_changes)
            .sum::<usize>();
        let rolling_nodeset = results
            .iter()
            .map(|r| r.rolling.nodeset_replacements)
            .sum::<usize>();

        println!(
            "| {} | {:.2} | {} | {:.2} | {} | {:.2} | {} | {} | {} | {} | {} | {} | {} |",
            algorithm.name,
            avg_leader_range,
            max_leader_range,
            avg_replica_range,
            max_replica_range,
            avg_nodeset_range,
            max_nodeset_range,
            one_time.replica_replacements,
            one_time.leader_changes,
            one_time.nodeset_replacements,
            rolling_replica,
            rolling_leader,
            rolling_nodeset,
        );
    }
    println!();
}

fn print_salt_sweep(algorithms: &[Algorithm]) {
    println!("## Salt sweep aggregate");
    println!(
        "_256 deterministic salt trials per scenario; reports averages across all 2048 runs._"
    );
    println!(
        "| algorithm | avg leader range | p95 leader range | avg replica range | p95 replica range | avg nodeset range | p95 nodeset range | avg rolling replica moves | avg rolling leader moves | avg rolling nodeset moves |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

    for algorithm in algorithms {
        let mut leader_ranges = Vec::new();
        let mut replica_ranges = Vec::new();
        let mut nodeset_ranges = Vec::new();
        let mut rolling_replicas = Vec::new();
        let mut rolling_leaders = Vec::new();
        let mut rolling_nodesets = Vec::new();

        for trial in 0..256 {
            let salt = splitmix64(HASH_SALT ^ trial);
            for nodes in [3, 5] {
                for partitions in [24, 48, 96, 128] {
                    let result = run_scenario(*algorithm, nodes, partitions, salt);
                    leader_ranges.push(result.initial_leader.range);
                    replica_ranges.push(result.initial_replica.range);
                    nodeset_ranges.push(result.initial_nodeset.range);
                    rolling_replicas.push(result.rolling.replica_replacements);
                    rolling_leaders.push(result.rolling.leader_changes);
                    rolling_nodesets.push(result.rolling.nodeset_replacements);
                }
            }
        }

        let avg_leader = average(&leader_ranges);
        let avg_replica = average(&replica_ranges);
        let avg_nodeset = average(&nodeset_ranges);
        let avg_roll_replica = average(&rolling_replicas);
        let avg_roll_leader = average(&rolling_leaders);
        let avg_roll_nodeset = average(&rolling_nodesets);
        let p95_leader = percentile(&mut leader_ranges, 0.95);
        let p95_replica = percentile(&mut replica_ranges, 0.95);
        let p95_nodeset = percentile(&mut nodeset_ranges, 0.95);

        println!(
            "| {} | {:.2} | {} | {:.2} | {} | {:.2} | {} | {:.2} | {:.2} | {:.2} |",
            algorithm.name,
            avg_leader,
            p95_leader,
            avg_replica,
            p95_replica,
            avg_nodeset,
            p95_nodeset,
            avg_roll_replica,
            avg_roll_leader,
            avg_roll_nodeset,
        );
    }
    println!();
}

fn print_csv(algorithms: &[Algorithm]) {
    eprintln!(
        "algorithm,nodes,partitions,initial_replica_counts,initial_replica_range,initial_replica_rmse,initial_leader_counts,initial_leader_range,initial_leader_rmse,initial_nodeset_counts,initial_nodeset_range,initial_nodeset_rmse,final_replica_counts,final_replica_range,final_leader_counts,final_leader_range,final_nodeset_counts,final_nodeset_range,worst_down_replica_range,worst_down_leader_range,worst_down_nodeset_range,rolling_changed_partitions,rolling_replica_replacements,rolling_leader_changes,rolling_changed_nodesets,rolling_nodeset_replacements"
    );
    for algorithm in algorithms {
        for nodes in [3, 5] {
            for partitions in [24, 48, 96, 128] {
                let result = run_scenario(*algorithm, nodes, partitions, HASH_SALT);
                eprintln!(
                    "{},{},{},{},{},{:.4},{},{},{:.4},{},{},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    result.algorithm,
                    result.nodes,
                    result.partitions,
                    count_string(&result.initial_replica.counts),
                    result.initial_replica.range,
                    result.initial_replica.rmse,
                    count_string(&result.initial_leader.counts),
                    result.initial_leader.range,
                    result.initial_leader.rmse,
                    count_string(&result.initial_nodeset.counts),
                    result.initial_nodeset.range,
                    result.initial_nodeset.rmse,
                    count_string(&result.final_replica.counts),
                    result.final_replica.range,
                    count_string(&result.final_leader.counts),
                    result.final_leader.range,
                    count_string(&result.final_nodeset.counts),
                    result.final_nodeset.range,
                    result.worst_down_replica_range,
                    result.worst_down_leader_range,
                    result.worst_down_nodeset_range,
                    result.rolling.changed_partitions,
                    result.rolling.replica_replacements,
                    result.rolling.leader_changes,
                    result.rolling.changed_nodesets,
                    result.rolling.nodeset_replacements,
                );
            }
        }
    }
}

fn main() {
    let algorithms = algorithms();

    print_baseline();
    print_exact_comparison(&algorithms);
    print_salt_sweep(&algorithms);

    if std::env::args().any(|arg| arg == "--csv") {
        print_csv(&algorithms);
    }
}

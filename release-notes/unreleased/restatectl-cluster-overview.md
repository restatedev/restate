# Release Notes: `restatectl status`, `partitions list` and `logs list` show what needs attention (preview)

## New Feature

### What Changed

`restatectl` has a preview of new features: they're off unless you pass the new global
`--preview` flag or set `RESTATECTL_PREVIEW=1`. Without it, `restatectl` behaves as before.

With preview, `restatectl status`, `restatectl partitions list` and `restatectl logs list` are
redesigned to answer "is my cluster healthy, and if not, why?" at a glance. Each comes in three
forms:

- **On a terminal**, as panels and charts: colored cards, tiles, bars and badges.
- **As plain text tables** when the output isn't a terminal, with `NO_COLOR` or `TERM=dumb`, or
  with the new global `--plain` flag.
- **As JSON** with `--json`, for scripts and agents.

`restatectl status` shows:

- A verdict: `HEALTHY`, `DEGRADED` (the cluster works with reduced redundancy) or `CRITICAL`
  (part of the cluster is unavailable or needs intervention), with the key numbers: nodes alive,
  partitions led, logs that take writes, metadata members up and the metadata versions.
- A card per node with its liveness, uptime, metadata server role, a bar of the partitions it
  leads and follows, the logs it sequences, and its storage and worker state. Clusters with more
  than eight nodes get a table with the same bar on each row instead, and the dashboard's
  overview a tile per node.
- A tile per partition, colored by its health (healthy, reconfiguring, degraded, unavailable, or
  unknown when the cluster state can't be read), with an edge in the color of the node leading it.
- Findings: everything that needs attention, most severe first, each with a command to look
  closer. For example dead or unreachable nodes, partitions without a leader or whose log can't
  take appends, replicas that are missing or catching up, replica sets being reconfigured, logs
  whose sequencer is down or whose tail segment is sealed, nodesets with too few writable nodes,
  read streams that wait for a log's chain to be sealed, a metadata cluster that lost its quorum
  or leader, and nodes running different versions. How far replicas or readers are behind the
  log tail is shown, but doesn't count as a problem: it changes with load.

`restatectl status --extra` (or `-x`) adds the metadata servers, and the logs and partitions
tables; in the plain output also the node addresses and the metadata versions each node has
loaded.

`restatectl partitions list` shows one row per partition: its health, leader, epoch, replica set
(`N1 N2 → N1 N3` while it's being reconfigured, with markers for replicas that aren't running
normally), applied LSN, lag, durable and archived LSNs, and sequencer. New options:

- `--replicas` (`-r`): one row per partition processor, as before, plus the replica set members
  that don't run one.
- Partition ids or ranges, e.g. `restatectl partitions list 3 7-9`.
- `--node N2`: only the partitions with a replica on that node.
- `--unhealthy`: only the partitions that aren't healthy.
- `--extra` (`-x`): the log, log tail and key range; with `--replicas`, the schema and rule-book
  versions and the enabled features.
- `--sort health`: the least healthy partitions first.

Without filters, it also shows how the partitions spread over the nodes: leaders and replicas per
node next to an even split over the active workers, with nodes that are off by more than a fifth
highlighted, to spot imbalances.

`restatectl logs list` shows each log's tail segment with its state, the number of segments in
the chain, the tail LSN, the trim point, the number of records retained, the sequencer
(highlighted when it runs away from the partition's leader or is down) and the nodeset (members
that can't take writes are highlighted). It accepts log ids or ranges and `--unhealthy`. Without
filters, it also shows sequencers per node next to an even split over the active workers, and
nodeset memberships per node next to an even split over the writable log-servers.

`restatectl partitions list --replicas` also shows where each replica reads the log: at the tail,
behind it, or waiting. This tells a replica that can't read its log fast enough apart from one
that applies slowly.

#### Live dashboard

On a terminal, `-w` on any of the three commands opens a live dashboard that refreshes every
second (`-n <seconds>` changes the interval):

- Views for the overview, partitions, logs, nodes and findings: switch with `Tab` or `1`-`5`.
- Select a partition, log or node with the arrow keys to see its details: replicas with their
  LSNs, the replica set, the sequencer, and charts of its apply rate and lag, or its append rate,
  over the time the dashboard has been running. The overview charts the cluster's appends per
  second. A log shows its chain of segments, each in the color of the node that sequences it,
  with trimmed records shaded, its partition's newest snapshot, and where each reader is on it;
  and how long each reader has been in its state. The partitions and logs views start with their
  spread over the nodes.
- `s` sorts the partitions, logs or nodes by their next column (for example health, lag,
  leader or epoch for partitions; state, segments or retained records for logs; status, leaders
  or uptime for nodes), `S` reverses the order. The sorted column's header is highlighted, with
  an arrow for the direction. `partitions list -w --sort` picks the first order.
- A node's details list its control and fabric addresses for each location, marking the one
  `restatectl` dials, and the servers the node reports (ingress, admin, fabric, control): where
  each is advertised and where it listens.
- `u` shows only the partitions and logs that need attention. In the findings view, select a
  finding and press `Enter` to open its partition, log or node; `Enter` on the overview lists the
  findings. `r` refreshes, `?` lists the keys, `q` quits.
- The mouse works too: click a view, a row, a tile to open its partition or node, a finding to
  select it (again to open it), a key in the bottom bar, or the header of a column the list can
  be sorted by (again to reverse the order). The wheel moves the selection. Dragging selects text
  and copies it to the clipboard, and a double-click copies the word under the pointer, such as
  an LSN or an address; in the findings view, it opens the finding. An address wrapped over
  several lines is copied whole. Copying goes through the
  terminal, so it also works over SSH; in tmux it needs `set -g set-clipboard on`. For the
  terminal's own selection, hold Shift while dragging (Option in iTerm2).

Without a terminal (or with `--plain`), `-w` refreshes the plain output instead. It also refreshes
the command's own output when the command selects partitions or logs by id, or uses `--node` or
`--replicas`.

### Why This Matters

Without preview, `restatectl status --extra` prints four long tables and leaves it to you to spot
the problem. With preview, the cluster's health, ongoing reconfigurations and the partitions, logs or nodes
that need attention are visible in one screen, you can watch the cluster recover live, and the
same information is available as JSON for automation.

### Impact on Users

Nothing changes unless you turn on preview. With preview:

- The human-readable output of `restatectl status`, `restatectl partitions list` and
  `restatectl logs list` is different. Scripts that parse it should use `--json` instead.
- The commands gather information from more sources, including the cluster's SQL tables. When a
  source is unavailable, for example when connecting to an older server, the commands still work
  and report what they couldn't read as a finding.
- The exit code of `restatectl status` doesn't depend on the verdict; read `health` from the
  `--json` output to check it in scripts.

### Migration Guidance

None. To try the preview, run for example `restatectl --preview status`, or set
`RESTATECTL_PREVIEW=1` in your shell; scripts that use it should read the `--json` output, for
example `restatectl --preview status --json | jq -r .health`. The preview may still change before
it becomes the default.

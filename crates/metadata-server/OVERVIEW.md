# Restate's Replicated Metadata Server

> A linearizable, versioned key-value store for Restate's cluster-wide control-plane metadata.

---

## 1. The Mental Model

The metadata server stores small but critical pieces of cluster state, including the nodes
configuration, Bifrost configuration, partition table, schema registry, rule book, and
partition-processor epochs.

```text
                     RESTATE CLUSTER

  +--------------+     +---------------------+     +----------------+
  | Node joining |     | Partition placement |     | Schema updates |
  +------+-------+     +----------+----------+     +-------+--------+
         |                        |                        |
         +------------------------+------------------------+
                                  |
                                  v
                    +---------------------------+
                    | Replicated metadata store |
                    |                           |
                    | versioned key/value data  |
                    | linearizable reads        |
                    | conditional writes        |
                    +-------------+-------------+
                                  |
                            replicated by Raft
```

The public storage contract provides `get`, `get_version`, conditional `put`, conditional
`delete`, and idempotent provisioning. Conditional writes support `DoesNotExist` and
`MatchesVersion`, which provide compare-and-swap semantics.

---

## 2. Architecture at a Glance

```text
 MetadataStoreClient
         |
         | Get / GetVersion / Put / Delete / Provision
         v
 +-----------------------+       bounded channels
 | MetadataServerSvc     | -----------------------------+
 | gRPC API              |                              |
 +-----------------------+                              v
                                            +-----------------------+
 MetadataServerNetworkSvc                   | RaftMetadataServer    |
 +-----------------------+                  | lifecycle             |
 | ConnectTo stream      |                  |                       |
 | JoinCluster RPC       |                  | Uninitialized         |
 +-----------+-----------+                  | Standby               |
             |                              | Member                |
             |                              +-----------+-----------+
             |                                          |
             |                               +----------+----------+
             +------------------------------>| raft::RawNode       |
                                             +----+-----------+----+
                                                  |           |
                                      committed   |           | Raft state,
                                      operations  |           | log, snapshots
                                                  v           v
                                       +----------------+  +----------------+
                                       | KvMemoryStorage|  | RocksDbStorage |
                                       | application    |  | durability     |
                                       | state          |  |                |
                                       +----------------+  +----------------+
```

The implementation uses Restate's fork of TiKV `raft-rs`. A member owns a
`raft::RawNode<RocksDbStorage>`, an in-memory application state machine, peer networking, timers,
and pending RPC callbacks.

Both metadata gRPC services are registered on the node's RPC server. The Raft transport has its
own protocol and connection manager, but not a separate TCP listener.

---

## 3. Reads and Writes

### Linearizable read

```text
 Client              Leader                  Raft              KvMemoryStorage
   |                    |                      |                       |
   | Get(key)           |                      |                       |
   +------------------->|                      |                       |
   |                    | read_index(ULID)     |                       |
   |                    +--------------------->|                       |
   |                    |                      | confirm safe index    |
   |                    |<---------------------+                       |
   |                    | wait until applied_index >= read_index      |
   |                    |-------------------------------------------->|
   |                    |                       value                  |
   |                    |<--------------------------------------------+
   | value              |                      |                       |
   |<-------------------+                      |                       |
```

Followers do not serve reads. The leader uses Raft's safe `ReadIndex` mechanism and only reads the
in-memory state after the corresponding index has been applied.

### Conditional write

```text
 Client          Leader             Raft quorum       All state machines
   |                |                    |                    |
   | Put(k,v,p)     |                    |                    |
   +--------------->|                    |                    |
   |                | propose(request)   |                    |
   |                +------------------->|                    |
   |                |                    | replicate + commit |
   |                |                    +------------------->|
   |                |                    |                    | check p
   |                |                    |                    | mutate map
   |                |<----------------------------------------+ callback
   | success/error  |                    |                    |
   |<---------------+                    |                    |
```

The leader may reject an obviously stale precondition before proposing, but every member checks the
precondition again while applying the committed entry. The application-time check determines the
replicated result.

---

## 4. Server Lifecycle

The metadata-server lifecycle is separate from Raft's leader/follower role.

```text
                            first provisioning
                      +--------------------------+
                      |                          v
              +-------+-------+          +---------------+
              | Uninitialized |          |    Member     |
              |               |          | runs RawNode  |
              +-------+-------+          +-------+-------+
                      |                          ^ |
       restored state |                          | | removed or asked
       says Standby   |                  join    | | to leave
                      v                          | v
              +---------------+          +-------+-------+
              |    Standby    |----------+               |
              | no RawNode    |                          |
              +---------------+<-------------------------+
```

- **Uninitialized:** restore persisted state or wait for first provisioning.
- **Standby:** host the metadata-server role without participating as a Raft voter.
- **Member:** run a local `RawNode`; the member may independently be leader or follower.

A fresh seed creates a single-voter Raft configuration and an initial snapshot containing the
initial `NodesConfiguration`. A standby joins by calling `JoinCluster`; the leader proposes a Raft
configuration change. Removal follows the same replicated configuration-change path, and removing
the final member is forbidden.

```text
 NodesConfiguration.MetadataServerState
                 |
                 +---- Provisioning / Member ---> standby attempts JoinCluster
                 |
                 +---- Standby -----------------> existing member leaves
```

---

## 5. `NodesConfiguration` Is Part of the Control Loop

`NodesConfiguration` is an ordinary replicated key with special local handling. Whenever its value
is applied, `KvMemoryStorage` decodes it and submits it through `MetadataWriter` to the process-wide
metadata manager.

```text
               Raft commits nodes_config
                          |
                          v
                 +-----------------+
                 | KvMemoryStorage |
                 +--------+--------+
                          |
                          | MetadataWriter::submit
                          v
                 +------------------+
                 | Metadata manager |
                 +--------+---------+
                          |
             +------------+-------------+
             |            |             |
             v            v             v
       peer addresses  join/leave    client endpoints
```

After a Raft membership change, the server updates the corresponding metadata-server states inside
`NodesConfiguration`: current voters become `Member`, and removed voters become `Standby`. It then
increments the nodes-configuration version.

---

## 6. Durability and Recovery

The application state lives in memory. Durability comes from the Raft log and complete state-machine
snapshots in RocksDB.

```text
                           RocksDB
          +---------------------------------------+
          | data column family                    |
          |                                       |
          |   Raft log entries                    |
          +---------------------------------------+
          | metadata column family                |
          |                                       |
          |   hard state       conf state         |
          |   snapshot         lifecycle state    |
          |   storage marker   last nodes config  |
          +---------------------------------------+

 Restart:

   open RocksDB --> restore latest snapshot --> replay remaining log
                         |                         |
                         +------------+------------+
                                      v
                              KvMemoryStorage
```

Every snapshot contains all key/value entries and the metadata-server membership configuration,
together with its Raft index, term, and voter configuration. Snapshots are created after membership
changes, when the log-trim threshold is reached, or when Raft requests one.

RocksDB batches use WAL and synchronous writes. A storage marker records the node name and creation
time; startup rejects a database marked for another node name.

---

## 7. Why Metadata Raft Has Its Own Network Transport

The shared Restate network identifies nodes by `GenerationalNodeId`. It refuses inbound connections
until the local generational ID has been installed, and requires peers to supply a valid
generational ID during the `Hello`/`Welcome` handshake.

The metadata server starts before `NodeInit` obtains and installs that ID.

```text
 Startup order
 =============

  1. Start metadata manager
  2. Start node RPC listener
  3. Start metadata server  <-------------------------------+
  4. Provision metadata if needed                           |
  5. Run NodeInit                                           |
  6. Install GenerationalNodeId                             |
                                                             |
 Bootstrap cycle without a dedicated transport              |
 =================================================          |
                                                             |
  shared network needs GenerationalNodeId                    |
             ^                                               |
             |                                               |
  NodeInit gets ID from NodesConfiguration                   |
             ^                                               |
             |                                               |
  NodesConfiguration comes from metadata store --------------+
```

Metadata Raft breaks the cycle with a separate logical transport:

```text
 Shared network                         Metadata Raft network
 +----------------------------+         +----------------------------+
 | GenerationalNodeId         |         | PlainNodeId                |
 | Hello / Welcome handshake  |         | gRPC peer header           |
 | generation fencing         |         | cluster name/fingerprint   |
 +----------------------------+         +----------------------------+
```

Metadata Raft uses `PlainNodeId` for transport and Raft IDs. It fences metadata-storage incarnations
separately with `MemberId = PlainNodeId + storage-marker creation timestamp`.

---

## 8. Failure and Operational Model

```text
                    request to follower
                            |
                            v
                  +--------------------+
                  | UNAVAILABLE        |
                  | + known leader     |
                  +---------+----------+
                            |
                            v
                    client retries leader


                    leader loses leadership
                            |
             +--------------+---------------+
             |              |               |
             v              v               v
       fail pending     clear reads     fail membership
       write callbacks  awaiting index  callbacks
             |
             v
       outcome may be indeterminate:
       the entry may still commit under the next leader
```

Followers and standbys reject data operations and return a leader or member hint when one is known.
The replicated client follows leader hints and rotates among known endpoints.

When leadership is lost, pending operations fail because the old leader cannot know whether the new
leader retained each uncommitted entry. A failed response therefore does not prove that a write did
not commit; callers reconcile through versions and conditional writes.

During graceful shutdown, a leader attempts to transfer leadership to the alive member with the
highest replicated position.

Operator-facing commands are available under:

```text
restatectl metadata-server list-servers
restatectl metadata-server add-node
restatectl metadata-server remove-node
```

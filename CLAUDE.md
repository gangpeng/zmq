# CLAUDE.md — ZMQ Project Guidelines

## What this project is

ZMQ is a Zig reimplementation of [AutoMQ](https://github.com/AutoMQ/automq), a cloud-native Apache Kafka built on S3 object storage. The AutoMQ Java codebase (~500K+ lines) is the **source of truth** for architectural decisions. ZMQ should match AutoMQ's design as closely as Zig's idioms allow.

Key files: `DESIGN.md` (architecture), `README.md` (overview), `src/` (all source code).

## Build and test

```bash
zig build              # compile
zig build test         # run all unit tests
zig build -Doptimize=.ReleaseFast  # optimized build
```

Single-file tests: `zig test src/raft/state.zig`

The codebase currently has **436 unit tests** across 50+ files covering Raft consensus, consumer groups, compaction, failover, storage, protocol serialization, and more.

## Lessons learned during initial port

During the initial port from Java to Zig, three categories of mistakes repeatedly occurred. The rules below were established to prevent recurrence:

1. **Silent feature dropping** — AutoMQ features were simplified or omitted without documenting the gap. Example: AutoMQ uses 4 streams per partition (log/tim/idx/txn); ZMQ uses 1 (intentional simplification, now documented).

2. **Missing failure handling** — Happy-path code was written without considering crashes, network partitions, or partial failures. These have since been fixed: compaction uses write-before-delete, S3 errors are logged (not swallowed), consumer group cleanup is complete, and the Raft state machine persists `voted_for` and `current_epoch` to survive restarts.

3. **Insufficient test coverage for failure paths** — Tests now cover failure injection (S3 failures during compaction, merge abort on partial read), crash recovery (epoch/voted_for persistence across restart, double-vote prevention), and edge cases (split votes, stale epochs, network partitions).

## Rules for all code changes

### Rule 1: Check AutoMQ before simplifying

Before implementing any feature, read the corresponding AutoMQ Java code. When the Zig implementation differs from AutoMQ, explicitly state:
- **What AutoMQ does** (with Java class/method reference)
- **What ZMQ does differently** and why
- **What is lost** by the simplification

Do NOT silently drop features. If something is intentionally omitted, add a comment:
```zig
// NOTE: AutoMQ uses 4 streams per partition (log/tim/idx/txn). ZMQ uses
// a single stream because RecordBatch is self-contained. This means
// time-index lookups (ListOffsets by timestamp) require a linear scan
// instead of a precomputed index.
```

### Rule 2: Failure handling is not optional

This is a **distributed system**. Every I/O operation can fail. For every function that does I/O (S3, network, disk), answer these questions before writing the code:

1. **What if this operation fails?** (network timeout, disk full, S3 error)
2. **What if the process crashes between this operation and the next?**
3. **What state is the system in after a partial failure?**
4. **Can a consumer/producer see duplicate or missing data?**

Specific patterns required:

- **Write-before-delete**: Always write new data and update metadata BEFORE deleting old data. Never delete first. This is enforced in compaction (`splitStreamSetObject`, `mergeStreamObjects`).
- **No silent error swallowing**: Never write `catch {}` for I/O errors. At minimum log the error. Prefer returning the error or a bool success indicator. See `deleteS3Object()` which returns `bool`.
- **Fencing checks**: Any code path that writes to S3 must check `is_fenced` first. Any code path that accepts produces must check `is_fenced_by_controller`. Broker-only nodes self-fence when the controller lease expires (30s without heartbeat).
- **Epoch validation**: Raft operations must validate epochs. `becomeFollower` rejects stale epochs. `updateCommitIndex` verifies the entry at the commit offset has the current epoch (Raft Figure 8). `handleVoteRequest` resets the election timer on grant.
- **Persist before responding**: `voted_for` and `current_epoch` are persisted to `raft.meta` BEFORE granting a vote or broadcasting an election. This prevents double-voting after a crash-restart.

### Rule 3: Tests must cover failure paths

Every new feature needs tests for:
- **Happy path** — normal operation works
- **Failure injection** — what happens when an I/O operation fails mid-way? (e.g., `"CompactionManager merge aborts if SO read fails"`)
- **Crash recovery** — what state is the system in if the process restarts at each step? (e.g., `"RaftState voted_for persists prevents double-voting after restart"`)
- **Edge cases** — empty inputs, max values, concurrent operations

For distributed system components specifically:
- **Split vote** — two candidates at the same epoch (tested: `"Election Safety: at most one leader per epoch"`)
- **Network partition** — broker can't reach controller (tested: `"MetadataClient staleness detection fences broker"`)
- **Stale messages** — old-epoch heartbeats (tested: `"becomeFollower ignores stale epoch"`)
- **Duplicate data** — same record visible from two objects (tested: `"CompactionManager split uses write-before-delete pattern"`)

### Rule 4: Don't change existing architecture without discussion

If a change would affect the interaction between components (e.g., changing how PartitionStore talks to ObjectManager, or how Controller talks to Broker), describe the change and its implications before implementing it. Use the plan mode.

## Architecture reference

See `DESIGN.md` for the full architecture with component diagram. Key points:

### Dual-role architecture
- **Controller** (port 9093): KRaft consensus, broker registry. Owns `RaftState`.
- **Broker** (port 9092): Kafka client APIs. Holds `?*RaftState` (null in broker-only mode).
- **Combined** (default): Both roles, two ports, backward-compatible.
- Config: `--process-roles controller|broker|controller,broker`

### KRaft consensus safety
- **Pre-vote** (KIP-996): Candidates run `startPreVote()` before real elections. Pre-vote does not increment epoch or change voted_for. Prevents partitioned nodes from disrupting the cluster.
- **Persistence**: `current_epoch` and `voted_for` persisted to `raft.meta` before every vote grant or epoch change.
- **Snapshotting**: `takeSnapshot()` truncates committed log entries periodically (>1000 entries). Metadata persisted to `snapshot.meta`.
- **Commit safety**: `updateCommitIndex()` only advances if the entry at the commit offset has the current epoch (Raft Figure 8).
- **AppendEntries validation**: `handleAppendEntries()` validates prev_log match, truncates conflicting entries, advances follower commit_index.
- **Dynamic voter changes**: `proposeAddVoter()`/`proposeRemoveVoter()` append config change entries to the Raft log. Applied on commit via `applyCommittedConfigs()`. One change at a time (single-server approach, KIP-853).

### Data flow
```
Produce → Broker.handleProduce → PartitionStore.produce
  → WAL + LogCache + S3WalBatcher.append (+ trackPendingHW in group_commit mode)
  → [group_commit: deferred to epoll batch boundary]
  → Server.batch_flush_fn → Broker.flushPendingWal
  → S3WalBatcher.flushNow → S3 PUT + ObjectManager.commitStreamSetObject
  → applyDeferredHWUpdates (HW advances for all flushed partitions)

Fetch → Broker.handleFetch → PartitionStore.fetchWithIsolation
  → LogCache → S3BlockCache → ObjectManager.getObjects → S3 GET

Compaction → CompactionManager.maybeCompact (every 5 min)
  → cleanupOrphans: retry failed S3 deletes from previous cycle
  → forceSplitAll: SSO (multi-stream) → SOs (per-stream) [idempotent]
  → mergeAll: many small SOs → fewer large SOs [aborts on partial read]
  → cleanupExpired: remove SOs behind retention trim point
  → Write-before-delete: register new, remove old from index, then delete from S3
  → Journal: journalBegin/journalComplete track in-progress operations
```

### Crash safety invariant
ObjectManager metadata is always updated BEFORE S3 objects are deleted. If the process crashes:
- After writing new objects but before removing old from index: next compaction retries (harmless overlap). Idempotency check (`hasStreamObjectCovering`) prevents duplicate SOs.
- After removing old from index but before S3 delete: orphaned S3 files tracked in `orphaned_keys`, retried on next compaction cycle.
- Never: data in index pointing to deleted S3 objects (would cause data loss).

### Consumer group safety
- **State machine**: Empty → PreparingRebalance → CompletingRebalance → Stable
- **Heartbeat signaling**: Returns REBALANCE_IN_PROGRESS (error 27) during rebalance states
- **Leader validation**: Only the group leader can send SyncGroup assignments
- **Sticky assignment**: `computeStickyAssignment()` parses prior assignments and minimizes partition movement
- **Full cleanup**: `leaveGroup()` calls `deinitMember()` (frees all member resources)
- **Fencing**: Produces rejected with NOT_LEADER_OR_FOLLOWER (error 6) when broker is fenced by controller

## Known gaps vs AutoMQ

| Feature | AutoMQ | ZMQ Status |
|---------|--------|------------|
| Streams per partition | 4 (log/tim/idx/txn) | 1 (intentional — RecordBatch is self-contained; time-index lookups scan linearly) |
| Compaction strategies | 6 (CLEANUP, MINOR, MAJOR, V1 variants) | 3 (force-split, merge, cleanup). V1 variants handle AutoMQ-specific composite objects. |
| Compaction analyzer | Full planning with size/dirty-byte thresholds | Time-based interval only. No adaptive scheduling. |

## File organization

```
src/
├── main.zig                    # Entry point, CLI, role-based startup
├── roles.zig                   # ProcessRoles (controller/broker/combined)
├── config.zig                  # Properties file parser
├── controller/
│   ├── controller.zig          # KRaft controller, API dispatch (52-55, 62, 63, 71, 72)
│   ├── broker_registry.zig     # Live broker tracking, epoch assignment, eviction
│   └── metadata_client.zig     # Broker→controller: discover, register, heartbeat
├── broker/
│   ├── handler.zig             # Kafka API dispatch (44+ APIs), Broker struct
│   ├── partition_store.zig     # Multi-tier storage: WAL → LogCache → S3BlockCache → S3
│   ├── group_coordinator.zig   # Consumer groups: join, sync, heartbeat, offsets
│   ├── txn_coordinator.zig     # Exactly-once transactions (2PC, control batches)
│   ├── failover.zig            # Broker failure detection, WAL epoch fencing
│   ├── auto_balancer.zig       # Traffic-aware partition reassignment
│   ├── quota_manager.zig       # Client rate limiting
│   ├── metrics.zig             # Prometheus-compatible metric registry
│   ├── purgatory.zig           # Timer wheel for delayed operations
│   ├── persistence.zig         # Topic/offset metadata persistence
│   ├── fetch_session.zig       # KIP-227 incremental fetch sessions
│   └── response_builder.zig    # Kafka response serialization helpers
├── raft/
│   ├── state.zig               # RaftState, RaftLog, ElectionTimer, MetadataImage
│   └── election_loop.zig       # Background: elections, heartbeats, replication, snapshots
├── storage/
│   ├── wal.zig                 # Filesystem WAL + S3WalBatcher (group commit, epoch-fenced)
│   ├── cache.zig               # LogCache (FIFO) + S3BlockCache (LRU)
│   ├── s3.zig                  # ObjectWriter/Reader (DataBlocks + Index + Footer)
│   ├── s3_client.zig           # HTTP S3 client with AWS SigV4
│   ├── stream.zig              # ObjectManager: Stream, SSO, SO metadata registry
│   ├── compaction.zig          # Force-split, merge, cleanup, journal, orphan tracking
│   └── aws_sigv4.zig           # AWS Signature Version 4 request signing
├── network/
│   ├── server.zig              # TCP server (io_uring with epoll fallback)
│   ├── raft_client.zig         # Raft peer RPC client (Vote, BeginQuorumEpoch, etc.)
│   ├── handler_routing.zig     # Dual-port handler dispatch
│   ├── connection_manager.zig  # Connection lifecycle tracking
│   └── metrics_server.zig      # Prometheus /metrics + /health + /ready endpoints
├── protocol/
│   ├── serialization.zig       # Kafka wire format primitives
│   ├── header.zig              # Request/Response headers (v0, v1, v2)
│   ├── api_key.zig             # All 74 standard + 18 AutoMQ-compatible API keys
│   ├── record_batch.zig        # RecordBatch V0/V1/V2 format
│   ├── error_codes.zig         # Kafka error code definitions
│   ├── messages/               # Hand-written message types
│   └── generated/              # 200+ auto-generated message structs
├── security/
│   ├── auth.zig                # SASL/PLAIN, SCRAM-SHA-256, ACL authorizer
│   └── tls.zig                 # TLS connection wrapper (documented limitation)
├── core/                       # ByteBuffer, CRC32C, UUID, Varint, Time
├── allocators/                 # Pool allocator, tracking allocator
├── compression.zig             # Gzip, Snappy, LZ4, Zstd codecs
├── streams/                    # Kafka Streams client (Topology, KStream, stores)
├── connect/                    # Kafka Connect (Source/Sink connectors)
└── tools/                      # CLI client (produce, consume, topics, groups)
```

## Code style

- No global mutable state except `global_server`, `global_controller_server`, `global_metrics_server`, `global_shutdown` (for signal handlers only)
- Single-threaded event loop (epoll) — no mutexes needed for request handling
- Background threads: ElectionLoop, MetricsServer, MetadataClient — communicate via shared atomic flags only
- Error handling: return errors, don't panic. Use `catch` with logging, not `catch {}`
- Tests go at the bottom of each file after `// Tests` separator
- Comments explain WHY, not WHAT. Don't prefix comments with issue tracker references like "Fix #3:"
- When referencing AutoMQ as the upstream project, keep "AutoMQ". When referring to this project, use "ZMQ".

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

## The core problem this file exists to solve

During the initial port from Java to Zig, three categories of mistakes repeatedly occurred:

1. **Silent feature dropping** — AutoMQ features were simplified or omitted without documenting the gap. Examples: AutoMQ uses 4 streams per partition, ZMQ uses 1. AutoMQ has 6 compaction strategies, ZMQ has 2. AutoMQ persists `voted_for` and `current_epoch` to disk, ZMQ only persists the log.

2. **Missing failure handling** — Happy-path code was written without considering crashes, network partitions, or partial failures. Examples: S3 delete errors were silently swallowed (`catch {}`), compaction could delete old objects before confirming new ones were written, consumer group `leaveGroup` leaked memory.

3. **Insufficient test coverage for failure paths** — Tests covered normal operations but not "what happens when X fails mid-operation?" scenarios.

## Rules for all code changes

### Rule 1: Check AutoMQ before simplifying

Before implementing any feature, read the corresponding AutoMQ Java code. When the Zig implementation differs from AutoMQ, explicitly state:
- **What AutoMQ does** (with Java class/method reference)
- **What ZMQ does differently** and why
- **What is lost** by the simplification

Do NOT silently drop features. If something is intentionally omitted, add a comment:
```zig
// NOTE: AutoMQ's CompactionAnalyzer supports 6 strategies (CLEANUP, MINOR, MAJOR,
// CLEANUP_V1, MINOR_V1, MAJOR_V1). ZMQ implements only force-split and merge.
// Missing: size-based compaction triggers, dirty-byte thresholds, adaptive scheduling.
```

### Rule 2: Failure handling is not optional

This is a **distributed system**. Every I/O operation can fail. For every function that does I/O (S3, network, disk), answer these questions before writing the code:

1. **What if this operation fails?** (network timeout, disk full, S3 error)
2. **What if the process crashes between this operation and the next?**
3. **What state is the system in after a partial failure?**
4. **Can a consumer/producer see duplicate or missing data?**

Specific patterns to follow:

- **Write-before-delete**: Always write new data and update metadata BEFORE deleting old data. Never delete first.
- **No silent error swallowing**: Never write `catch {}` for I/O errors. At minimum log the error. Prefer returning the error or a bool success indicator.
- **Fencing checks**: Any code path that writes to S3 must check `is_fenced` first. Any code path that accepts produces must check `is_fenced_by_controller`.
- **Epoch validation**: Raft operations must validate epochs. Never accept a lower epoch. `becomeFollower` must reject stale epochs. `updateCommitIndex` must verify the entry at the commit offset has the current epoch (Raft Figure 8).

### Rule 3: Tests must cover failure paths

Every new feature needs tests for:
- **Happy path** — normal operation works
- **Failure injection** — what happens when an I/O operation fails mid-way?
- **Crash recovery** — what state is the system in if the process restarts at each step?
- **Edge cases** — empty inputs, max values, concurrent operations

For distributed system components specifically:
- **Split vote** — two candidates at the same epoch
- **Network partition** — broker can't reach controller
- **Stale messages** — old-epoch heartbeats, old-generation heartbeats
- **Duplicate data** — same record visible from two objects

### Rule 4: Don't change existing architecture without discussion

If a change would affect the interaction between components (e.g., changing how PartitionStore talks to ObjectManager, or how Controller talks to Broker), describe the change and its implications before implementing it. Use the plan mode.

## Architecture reference

See `DESIGN.md` for the full architecture. Key points:

### Dual-role architecture
- **Controller** (port 9093): KRaft consensus, broker registry. Owns `RaftState`.
- **Broker** (port 9092): Kafka client APIs. Holds `?*RaftState` (null in broker-only mode).
- **Combined** (default): Both roles, two ports, backward-compatible.
- Config: `--process-roles controller|broker|controller,broker`

### Data flow
```
Produce → Broker.handleProduce → PartitionStore.produce
  → WAL + LogCache + S3WalBatcher.append
  → S3WalBatcher.flushNow → S3 PUT + ObjectManager.commitStreamSetObject

Fetch → Broker.handleFetch → PartitionStore.fetchWithIsolation
  → LogCache → S3BlockCache → ObjectManager.getObjects → S3 GET

Compaction → CompactionManager.maybeCompact (every 5 min)
  → forceSplitAll: SSO (multi-stream) → SOs (per-stream)
  → mergeAll: many small SOs → fewer large SOs
  → Write-before-delete: register new, remove old from index, then delete from S3
```

### Crash safety invariant
ObjectManager metadata is always updated BEFORE S3 objects are deleted. If the process crashes:
- After writing new objects but before removing old from index: next compaction retries (harmless overlap)
- After removing old from index but before S3 delete: orphaned S3 files (harmless, wasted space)
- Never: data in index pointing to deleted S3 objects (would cause data loss)

## Known gaps vs AutoMQ

These features exist in AutoMQ but are missing or simplified in ZMQ. Each is a potential work item:

| Feature | AutoMQ | ZMQ Status |
|---------|--------|------------|
| Streams per partition | 4 (log/tim/idx/txn) | 1 (simplified — intentional, RecordBatch is self-contained) |
| Compaction strategies | 6 (CLEANUP, MINOR, MAJOR, V1 variants) | 3 (force-split, merge, cleanup). V1 variants not needed. |
| Compaction analyzer | Full planning with size/dirty-byte thresholds | Time-based interval + cleanup of expired SOs |
| KRaft metadata persistence | `voted_for` + `current_epoch` persisted | ✅ Implemented (`raft.meta` file) |
| Dynamic voter changes | AddRaftVoter/RemoveRaftVoter APIs | Not implemented (API keys exist but return UNSUPPORTED). Requires joint consensus — very complex. |
| Pre-vote protocol (KIP-996) | Prevents disruptive elections | ✅ Implemented (`startPreVote`, `handlePreVoteRequest`) |
| Sticky partition assignment | Full cooperative sticky | ✅ Implemented (parses prior assignments, minimizes movement) |
| S3 object TTL cleanup | Automatic cleanup of uncommitted objects | ✅ Implemented (orphaned key tracking + retry in `cleanupOrphans`) |
| Compaction state persistence | Checkpoints for crash recovery | ✅ Implemented (compaction journal: `journalBegin`/`journalComplete`) |
| Multi-part S3 upload | For large objects | Implemented but limited |
| Idempotent compaction | Re-entrant, dedup on replay | ✅ Implemented (`hasStreamObjectCovering` dedup check) |
| RaftLog snapshotting | Log truncation + InstallSnapshot | ✅ Implemented (`takeSnapshot`, `truncateBefore`, periodic check in election loop) |

## File organization

```
src/
├── main.zig                    # Entry point, CLI, role-based startup
├── roles.zig                   # ProcessRoles (controller/broker/combined)
├── config.zig                  # Properties file parser
├── controller/
│   ├── controller.zig          # KRaft controller, API dispatch
│   ├── broker_registry.zig     # Live broker tracking, epoch assignment
│   └── metadata_client.zig     # Broker→controller communication
├── broker/
│   ├── handler.zig             # Kafka API dispatch (44+ APIs)
│   ├── partition_store.zig     # Multi-tier storage engine
│   ├── group_coordinator.zig   # Consumer group lifecycle
│   ├── txn_coordinator.zig     # Transaction coordinator
│   ├── failover.zig            # Broker failure detection
│   ├── auto_balancer.zig       # Partition load balancing
│   └── ...                     # quota, metrics, purgatory, etc.
├── raft/
│   ├── state.zig               # RaftState, RaftLog, ElectionTimer
│   └── election_loop.zig       # Background election thread
├── storage/
│   ├── wal.zig                 # WAL + S3WalBatcher
│   ├── cache.zig               # LogCache (FIFO) + S3BlockCache (LRU)
│   ├── s3.zig                  # ObjectWriter/Reader (S3 object format)
│   ├── s3_client.zig           # HTTP S3 client with SigV4
│   ├── stream.zig              # ObjectManager, Stream, SSO, SO
│   └── compaction.zig          # Force-split + merge compaction
├── network/
│   ├── server.zig              # TCP server (epoll/io_uring)
│   ├── raft_client.zig         # Raft peer RPC client
│   └── handler_routing.zig     # Dual-port handler dispatch
├── protocol/                   # Kafka wire protocol
├── security/                   # SASL, ACL, TLS
└── ...                         # core, allocators, compression, etc.
```

## Code style

- No global mutable state except `global_server`, `global_controller_server`, `global_metrics_server`, `global_shutdown` (for signal handlers only)
- Single-threaded event loop (epoll) — no mutexes needed for request handling
- Background threads: ElectionLoop, MetricsServer, MetadataClient — communicate via shared atomic flags only
- Error handling: return errors, don't panic. Use `catch` with logging, not `catch {}`
- Tests go at the bottom of each file after `// Tests` separator
- Comments explain WHY, not WHAT. Don't prefix comments with issue tracker references like "Fix #3:"

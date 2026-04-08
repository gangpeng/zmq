# ZMQ — Key Design

## Overview

ZMQ is a high-performance, Kafka-compatible broker that reimplements AutoMQ's cloud-native architecture in Zig. Like AutoMQ, it separates compute from storage — brokers are stateless compute nodes that delegate all durability to S3. This eliminates inter-broker data replication (RF=1 always), enabling instant failover via metadata reassignment.

### Performance Comparison

| Metric | Zig | Java | Zig Advantage |
|---|---|---|---|
| Produce (single msg/req) | 5,430 msg/s | 1,400 msg/s | **3.9x faster** |
| Produce (batched 50/req) | 166,525 msg/s | 77,125 msg/s | **2.2x faster** |
| Produce p50 / p99 latency | 0.17 / 0.33 ms | 0.58 / 2.69 ms | **3.4x / 8.2x lower** |
| ApiVersions throughput | 5,544/s | 1,514/s | **3.7x faster** |
| Metadata throughput | 3,247/s | 1,985/s | **1.6x faster** |
| Startup time | 502 ms | 7,511 ms | **15x faster** |
| Memory (3 brokers loaded) | 7.8 MB | 1,547 MB | **198x less** |
| Binary size | 4.5 MB | ~200+ MB | **44x smaller** |

---

## Key Components and Interactions

### Component Overview

ZMQ is organized into distinct modules that interact through well-defined interfaces. The architecture follows Apache Kafka's KRaft model with separate controller and broker roles.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ZMQ Process                                      │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     Controller (controller.zig)                     │    │
│  │  ┌──────────────┐  ┌─────────────────┐  ┌───────────────────────┐  │    │
│  │  │  RaftState    │  │ BrokerRegistry  │  │  ElectionLoop         │  │    │
│  │  │  (state.zig)  │  │ (broker_reg.zig)│  │  (election_loop.zig)  │  │    │
│  │  │              │  │                 │  │                       │  │    │
│  │  │ role/epoch   │  │ live brokers    │  │ 100ms poll:           │  │    │
│  │  │ voters       │  │ epochs          │  │  election timeout?    │  │    │
│  │  │ RaftLog      │  │ heartbeats      │  │  send heartbeats?     │  │    │
│  │  │ commit_index │  │ fencing         │  │  count votes?         │  │    │
│  │  └──────┬───────┘  └────────┬────────┘  └──────────┬────────────┘  │    │
│  │         │                   │                      │               │    │
│  │         │ Server :9093 (KRaft APIs 52-55, BrokerRegistration 62,   │    │
│  │         │                  BrokerHeartbeat 63, ApiVersions 18)     │    │
│  └─────────┼───────────────────┼──────────────────────┼───────────────┘    │
│            │ setRaftState()    │ broker_registry ptr   │                    │
│            ▼                   ▼                       │                    │
│  ┌─────────────────────────────────────────────────────┼───────────────┐    │
│  │                     Broker (handler.zig)             │               │    │
│  │  ┌────────────────┐  ┌──────────────┐  ┌───────────┴─────────────┐ │    │
│  │  │ PartitionStore │  │ Group        │  │ TxnCoordinator          │ │    │
│  │  │ (part_store.zig)│ │ Coordinator  │  │ (txn_coordinator.zig)   │ │    │
│  │  │                │  │ (group_co.zig)│ │                         │ │    │
│  │  │ WAL+Cache+S3   │  │ JoinGroup    │  │ 2PC transactions        │ │    │
│  │  │ produce/fetch  │  │ SyncGroup    │  │ control batches         │ │    │
│  │  └───────┬────────┘  │ Heartbeat    │  │ producer epochs         │ │    │
│  │          │           │ Offsets      │  └─────────────────────────┘ │    │
│  │          │           └──────────────┘                              │    │
│  │          │  ┌──────────────────────┐  ┌─────────────────────────┐  │    │
│  │          │  │ FailoverController   │  │ AutoBalancer            │  │    │
│  │          │  │ (failover.zig)       │  │ (auto_balancer.zig)     │  │    │
│  │          │  │ WAL epoch fencing    │  │ traffic-aware moves     │  │    │
│  │          │  └──────────────────────┘  └─────────────────────────┘  │    │
│  │          │                                                         │    │
│  │          │ Server :9092 (Kafka client APIs 0-47, 60-61, 18)        │    │
│  └──────────┼─────────────────────────────────────────────────────────┘    │
│             │                                                              │
│             ▼                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     Storage Engine                                   │   │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────┐  │   │
│  │  │ S3WalBatcher │ │ LogCache     │ │ S3BlockCache │ │ Compaction │  │   │
│  │  │ (wal.zig)    │ │ (cache.zig)  │ │ (cache.zig)  │ │ Manager    │  │   │
│  │  │              │ │              │ │              │ │(compact.zig)│  │   │
│  │  │ batch→S3     │ │ FIFO hot data│ │ LRU S3 reads│ │ split+merge│  │   │
│  │  │ epoch fence  │ │ zero-copy    │ │ hit/miss    │ │ write-before│  │   │
│  │  │ sync/async   │ │              │ │             │ │ -delete     │  │   │
│  │  └──────┬───────┘ └──────────────┘ └──────────────┘ └─────┬──────┘  │   │
│  │         │              ▲  ObjectManager (stream.zig)       │         │   │
│  │         │              │  Stream/SSO/SO metadata registry  │         │   │
│  │         └──────────────┴───────────────────────────────────┘         │   │
│  └──────────────────────────────┬───────────────────────────────────────┘   │
│                                 │                                          │
└─────────────────────────────────┼──────────────────────────────────────────┘
                                  │
                                  ▼
                     ┌──────────────────────┐
                     │    S3 / MinIO        │
                     │  wal/epoch-N/bulk/NN │  ← StreamSetObjects
                     │  data/so/{stream}/   │  ← StreamObjects
                     └──────────────────────┘
```

### Component Descriptions

| Component | File(s) | Responsibility |
|-----------|---------|----------------|
| **Controller** | `src/controller/controller.zig` | Owns `RaftState`. Handles KRaft APIs (Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum) and broker lifecycle (BrokerRegistration, BrokerHeartbeat). |
| **BrokerRegistry** | `src/controller/broker_registry.zig` | Tracks live broker-only nodes. Assigns broker epochs for WAL fencing. Evicts brokers that miss heartbeats (30s timeout). |
| **RaftState** | `src/raft/state.zig` | Raft consensus state machine: role (unattached/candidate/leader/follower), epoch, voted_for, RaftLog, commit_index. Handles vote requests with log-recency checks and epoch-based commit safety. |
| **ElectionLoop** | `src/raft/election_loop.zig` | Background thread polling every 100ms. Checks election timeout, broadcasts vote requests, counts grants, sends leader heartbeats every ~500ms. |
| **RaftClientPool** | `src/network/raft_client.zig` | TCP connections to controller peers. Sends Vote (API 52), BeginQuorumEpoch (API 53) RPCs. Also provides raw request transport for MetadataClient. |
| **MetadataClient** | `src/controller/metadata_client.zig` | Broker-only nodes use this to discover the controller leader, register via BrokerRegistration (API 62), and heartbeat via BrokerHeartbeat (API 63). Implements lease-based staleness detection to self-fence on network partition. |
| **Broker** | `src/broker/handler.zig` | Stateful Kafka broker. Dispatches all 44+ Kafka client APIs. Owns PartitionStore, GroupCoordinator, TxnCoordinator, QuotaManager, Metrics. Holds optional `?*RaftState` pointer (null in broker-only mode). |
| **PartitionStore** | `src/broker/partition_store.zig` | Multi-tier storage engine: produce writes to WAL + LogCache + S3WalBatcher; fetch reads from LogCache → S3BlockCache → S3. Manages per-partition state (offsets, HW, LSO). |
| **S3WalBatcher** | `src/storage/wal.zig` | Batches records and uploads to S3 via ObjectWriter format. Epoch-fenced: rejects writes after `fence()`. Two modes: sync (flush per produce) and async (flush on tick). |
| **ObjectManager** | `src/storage/stream.zig` | Metadata registry for Streams, StreamSetObjects (multi-stream), and StreamObjects (per-stream). Resolves fetch queries by merging both object types sorted by offset. |
| **CompactionManager** | `src/storage/compaction.zig` | Periodic compaction: force-splits multi-stream SSOs into per-stream SOs, then merges small SOs. Uses write-before-delete pattern for crash safety. |
| **GroupCoordinator** | `src/broker/group_coordinator.zig` | Consumer group lifecycle: state machine (Empty → PreparingRebalance → CompletingRebalance → Stable), JoinGroup, SyncGroup (leader-only validation), Heartbeat (rebalance signaling), offset commit/fetch. |
| **TxnCoordinator** | `src/broker/txn_coordinator.zig` | Exactly-once transactions: 2PC with AddPartitionsToTxn, EndTxn, WriteTxnMarkers. Builds control record batches for commit/abort markers. |
| **FailoverController** | `src/broker/failover.zig` | Detects broker failures via heartbeat timeout (30s). Fences failed nodes by bumping WAL epoch and reassigning partitions. |
| **AutoBalancer** | `src/broker/auto_balancer.zig` | Traffic-aware partition reassignment. Detects load imbalance (20% threshold), generates greedy move plan. Moves are metadata-only since data is in S3. |

### How Components Interact

**Produce path:** Client → Broker (handler.zig) → PartitionStore.produce() → S3WalBatcher.append() → flushNow() → S3 PUT + ObjectManager.commitStreamSetObject()

**Fetch path:** Client → Broker → PartitionStore.fetchWithIsolation() → LogCache.get() → S3BlockCache.get() → ObjectManager.getObjects() → S3 GET + ObjectReader.parse()

**Compaction:** Broker.tick() → CompactionManager.maybeCompact() → forceSplitAll() [SSOs → SOs] → mergeAll() [small SOs → large SOs]

**Election:** ElectionLoop.run() → RaftState.startElection() → RaftClientPool.broadcastVoteRequest() → becomeLeader() or retry

**Broker registration (broker-only):** MetadataClient.run() → discoverLeader() via DescribeQuorum (API 55) → registerWithController() via BrokerRegistration (API 62) → heartbeat loop via BrokerHeartbeat (API 63)

**Failover:** FailoverController.tick() → detect heartbeat timeout → failoverNode() → S3WalBatcher.fence() + wal_epoch++ → partition reassignment

**Consumer group:** Client JoinGroup → GroupCoordinator.joinGroup() → state → PreparingRebalance → timeout → CompletingRebalance → leader SyncGroup → Stable → Heartbeat loop

---

## Dual-Role Architecture (Controller vs Broker)

ZMQ supports Apache Kafka's KRaft dual-role model via the `--process-roles` flag:

| Mode | Flag | Description |
|------|------|-------------|
| **Controller** | `--process-roles controller` | Runs Raft consensus + metadata management. Listens on controller port (default 9093). No Kafka client APIs. |
| **Broker** | `--process-roles broker` | Handles Kafka client requests. Listens on broker port (default 9092). Connects to controllers for metadata. |
| **Combined** | `--process-roles controller,broker` (default) | Both roles in one process. Two TCP listeners on separate ports. Backward-compatible with single-node deployment. |

```
CONTROLLER-ONLY MODE                  BROKER-ONLY MODE
┌────────────────────────┐           ┌────────────────────────┐
│ Controller             │           │ Broker                 │
│ ├─ RaftState (owned)   │◄──────────│ ├─ PartitionStore      │
│ ├─ BrokerRegistry      │  register │ ├─ Groups, Txn, etc.   │
│ ├─ ElectionLoop        │  heartbt  │ └─ MetadataClient ─────┘
│ └─ Server :9093        │           │     (→ controllers)
│    APIs: 52-55,62,63   │           │ Server :9092
└────────────────────────┘           │ APIs: 0-47,60-61,18
                                     └────────────────────────┘

COMBINED MODE (default, backward-compatible)
┌──────────────────────────────────────────┐
│ Controller :9093    │  Broker :9092      │
│ (KRaft APIs)        │  (Client APIs)     │
│ Broker gets ptr ────┘                    │
│ to Controller's RaftState                │
└──────────────────────────────────────────┘
```

### Dynamic Broker Scaling

This architecture enables dynamic broker scaling — the controller quorum is fixed, but broker-only nodes can be added or removed freely:

1. **New broker starts** with `--process-roles broker --voters "0@c1:9093,1@c2:9093,2@c3:9093"`
2. **MetadataClient discovers controller leader** via DescribeQuorum (API 55) to any voter
3. **Broker registers** via BrokerRegistration (API 62) → controller assigns a broker epoch
4. **Periodic heartbeats** via BrokerHeartbeat (API 63) every 2 seconds
5. **Controller fences dead brokers** — if heartbeat stops for 30 seconds, the broker is evicted
6. **Broker self-fences on network partition** — if no successful heartbeat within 30 seconds (lease expiry), the broker stops accepting produces to prevent split-brain

### Network Partition Protection

Broker-only nodes implement a **lease-based staleness check** (`controller_lease_ms`, default 30s). If the broker cannot heartbeat to any controller within the lease period, it self-fences by setting `is_fenced_by_controller = true`. All subsequent produce requests are rejected with NOT_LEADER_OR_FOLLOWER (error 6), telling clients to find another broker. When connectivity is restored and a heartbeat succeeds, the broker unfences automatically.

---

## Write Path

### S3 Metadata Model: Streams, StreamSetObjects, and StreamObjects

AutoMQ introduces a metadata layer between Kafka partitions and S3 objects. The Zig implementation (`src/storage/stream.zig`) includes full Stream, StreamSetObject, StreamObject, and ObjectManager data structures matching AutoMQ's architecture.

#### Stream

Each partition maps to a **Stream** (`stream.Stream`), identified by `hashPartitionKey(topic, partition_id)`:

```
Kafka Partition (e.g., topic-A, partition 0)
  │
  └── Stream {
        stream_id: hash("topic-A", 0),    // globally unique u64
        epoch: 1,                          // bumped on ownership transfer
        start_offset: 0,                   // trimmed start
        end_offset: 1000,                  // next available offset
        state: .opened,                    // opened or closed
        node_id: 0,                        // owning broker
        ranges: [                          // ownership history
          { epoch: 1, start: 0, end: 1000, node: 0 }
        ]
      }
```

Note: AutoMQ uses 4 streams per partition (log/tim/idx/txn). The Zig implementation simplifies this to a single stream per partition — the RecordBatch data includes all necessary information.

Streams are managed by `ObjectManager` (`stream.ObjectManager`), which provides:
- `createStreamWithId()` — called from `PartitionStore.ensurePartition()`
- `advanceEndOffset()` — called after each `produce()`
- `transferOwnership()` — called during failover (bumps epoch, creates new range)
- `trim()` — called during log retention

#### Object Types

Data from streams is stored in two types of S3 objects, both tracked by `ObjectManager`:

**StreamSetObject** (`stream.StreamSetObject`) — Contains data from **multiple streams interleaved**. Created by `S3WalBatcher.flushNow()` when it flushes buffered records from all partitions. The S3 object uses `ObjectWriter` format (DataBlocks + IndexBlock + Footer), and the ObjectManager tracks which stream-offset ranges are in each object.

**StreamObject** (`stream.StreamObject`) — Contains data from **exactly one stream**. Created by `CompactionManager` when it splits multi-stream StreamSetObjects into per-stream objects.

```
S3WalBatcher.flushNow() produces:    CompactionManager splits to:
┌─────────────────────────┐          ┌─────────────────────┐
│ StreamSetObject (SSO)    │         │ StreamObject (SO)     │
│ objectId: 1              │         │ objectId: 5           │
│ s3_key: data/sso/0/1     │         │ streamId: 0x1A2B      │
│ stream_ranges:           │         │ offset: 0..200        │
│  [{0x1A2B, 0..50},      │  ──►    │ s3_key: data/so/...   │
│   {0x3C4D, 0..30},      │         └─────────────────────┘
│   {0x1A2B, 50..99}]     │         ┌─────────────────────┐
│ DataBlockIndex[] entries │         │ StreamObject (SO)     │
│ Footer (16B, magic=AUTO) │         │ streamId: 0x3C4D      │
└─────────────────────────┘         │ offset: 0..100        │
                                     └─────────────────────┘
```

#### ObjectManager — Metadata Registry

`ObjectManager` (`src/storage/stream.zig`) maintains:
- `streams: AutoHashMap(u64, Stream)` — all known streams
- `stream_set_objects: AutoHashMap(u64, StreamSetObject)` — all SSOs
- `stream_objects: AutoHashMap(u64, StreamObject)` — all SOs
- `stream_object_index: AutoHashMap(u64, ArrayList(u64))` — per-stream sorted SO index
- `stream_sso_index: AutoHashMap(u64, ArrayList(u64))` — per-stream SSO index

The critical method is `getObjects(stream_id, start_offset, end_offset, limit)` which resolves fetch queries by merging both StreamObjects and StreamSetObjects that overlap the requested range, sorted by offset.

### S3 Object Internal Format

Each S3 object (both StreamSetObject and StreamObject) uses this binary format, implemented by `ObjectWriter` and `ObjectReader` in `src/storage/s3.zig`:

```
┌──────────────────────────────────────────────────┐
│  Data Block 0  (raw RecordBatch bytes)           │
├──────────────────────────────────────────────────┤
│  Data Block 1 ...                                │
├──────────────────────────────────────────────────┤
│  Data Block N ...                                │
├──────────────────────────────────────────────────┤
│  Index Block                                     │
│  DataBlockIndex[0]:                              │
│    streamId(8B) startOffset(8B) endOffsetDelta(4B)│
│    recordCount(4B) blockPosition(8B) blockSize(4B)│
│  DataBlockIndex[1] ...                           │
│  DataBlockIndex[N] ...                           │
│  (each index entry = 36 bytes)                   │
├──────────────────────────────────────────────────┤
│  Footer (16 bytes)                               │
│  indexStartPosition(8B) indexBlockLength(4B)     │
│  magic: 0x4155544F (4B, ASCII "AUTO")            │
└──────────────────────────────────────────────────┘
```

To read data for a specific stream and offset range:
1. Read the last 16 bytes → Footer → validate magic (`0x4155544F`) → get `indexStartPosition`
2. Range-read the index block → parse `DataBlockIndex[]` entries
3. Scan the index for matching `(streamId, startOffset, endOffset)` via `ObjectReader.findEntries()`
4. Range-read only the matching data blocks via `ObjectReader.readBlock()`

This allows efficient S3 range reads — you never need to read the entire object.

### Compaction: S3 Object Consolidation and Log Compaction

The Zig implementation has two levels of compaction:

**1. S3 Object Compaction — CompactionManager (`src/storage/compaction.zig`)**

The `CompactionManager` runs periodically (configurable via `compaction_interval_ms`, default 5 minutes) and performs two operations:

**Force Split** — For each StreamSetObject containing multiple streams:
1. Read the SSO from S3 via `ObjectReader.parse()`
2. For each distinct `stream_id`, extract only that stream's data blocks
3. Write a new StreamObject per stream via `ObjectWriter.build()`
4. Upload each StreamObject to S3, register with `ObjectManager.commitStreamObject()`
5. Remove the SSO from ObjectManager (so fetch queries stop returning it immediately)
6. Delete the SSO from S3 (orphaned file if delete fails — cleaned up later)

**Merge** — For each stream with ≥ `merge_min_object_count` (default 10) StreamObjects:
1. Group contiguous StreamObjects up to `merge_max_size` (default 256MB)
2. Read ALL SOs in the group — **if any read fails, abort the entire merge** to prevent data loss
3. Combine all blocks via `ObjectWriter`, upload the merged SO, register with ObjectManager
4. Remove old SOs from ObjectManager (fetch queries immediately see only the merged SO)
5. Delete old SOs from S3 (orphaned files if delete fails — logged as warnings)

**Crash Safety — Write-Before-Delete Pattern:**

Following AutoMQ's compaction design, ZMQ uses a strict write-before-delete ordering to ensure crash safety:

```
SAFE ORDER:                         CRASH SCENARIOS:
1. Write new objects to S3          If crash after step 1-2:
2. Register new objects in            → New objects exist but old also exist
   ObjectManager                      → getObjects() may return both (overlap)
3. Remove old objects from            → Next compaction cycle retries
   ObjectManager                    If crash after step 3:
4. Delete old objects from S3         → Orphaned files in S3 (harmless)
                                      → No data duplication in queries
                                    If crash after step 4:
                                      → Clean completion
```

The key invariant: **ObjectManager is updated before S3 deletion**. This ensures fetch queries never see stale data, even if S3 deletions fail or the broker crashes mid-compaction.

```
Before Compaction:                    After Compaction:
┌────────────────────┐               ┌────────────────────┐
│ StreamSetObject #1 │               │ StreamObject       │
│  stream-A: 0..50   │               │  stream-A: 0..200  │
│  stream-B: 0..30   │               │  (contiguous)      │
│  stream-C: 0..20   │──── Split ──►│                    │
├────────────────────┤               ├────────────────────┤
│ StreamSetObject #2 │               │ StreamObject       │
│  stream-A: 50..100 │               │  stream-B: 0..100  │
│  stream-B: 30..60  │               │  (contiguous)      │
│  stream-C: 20..50  │               ├────────────────────┤
├────────────────────┤               │ StreamObject       │
│ StreamSetObject #3 │               │  stream-C: 0..80   │
│  stream-A: 100..200│               │  (contiguous)      │
│  stream-B: 60..100 │               └────────────────────┘
│  stream-C: 50..80  │
└────────────────────┘
```

**2. Log Compaction for Internal Topics (`PartitionStore.compactLogs()`)**

For `__consumer_offsets` and `__transaction_state` topics (cleanup_policy = `.compact`):
- Scans LogCache records for each compacted partition
- Parses RecordBatch keys using `record_batch.zig`
- Builds a `key → latest_offset` map
- Removes superseded entries, keeping only the latest value per key
- Triggered via `applyRetention()` with `CleanupPolicy.compact` or `.compact_delete`

### Write Path Data Flow

The produce path is implemented in `PartitionStore.produce()` (`src/broker/partition_store.zig`) called from `Broker.handleProduce()` (`src/broker/handler.zig`):

```
Producer (Kafka Client)
    │
    │  Produce Request (API key 0)
    ▼
┌───────────────────────────────────────────────────────────────┐
│                     Zig Broker (handler.zig)                   │
│                                                               │
│  1. Parse RecordBatch header (CRC-32C, record_count, etc.)   │
│  2. Idempotent dedup check (producer_id + sequence)          │
│  3. Record size validation (< MAX_MESSAGE_BYTES = 1MB)       │
│  4. Assign broker offsets (next_offset += record_count)      │
│  5. Rewrite base_offset in RecordBatch bytes 0-7             │
│                                                               │
│  6. Write to storage (partition_store.zig):                  │
│     ├─ Filesystem WAL (wal.zig):                             │
│     │  [magic=0x01][CRC32C][len][data] + batch fsync         │
│     │  (every_n_records policy, fsync_interval=100)          │
│     ├─ LogCache (cache.zig):                                 │
│     │  In-memory FIFO cache (putOwned, zero-copy ownership)  │
│     └─ S3WalBatcher (wal.zig):                               │
│        buffer → batch upload to S3 via ObjectWriter format   │
│        ├─ Epoch fencing check (reject if is_fenced)          │
│        ├─ SYNC mode: flushNow() → S3 PUT before ack         │
│        └─ ASYNC mode: buffer (flush on tick: 4MB/250ms)      │
│                                                               │
│  7. Advance high_watermark (only after durable write)        │
│  8. Update last_stable_offset for transactions               │
│  9. Wake delayed fetchers for this partition                  │
│  10. Send Produce response with base_offset                  │
└───────────────────┬───────────────────────────────────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │  S3 / MinIO          │
         │  wal/epoch-N/bulk/NN │  ← StreamSetObject (WAL batch)
         │  data/topic/part/obj │  ← StreamObject (post-compaction)
         └──────────────────────┘
```

### Metadata Maintained During Writes

The following metadata is maintained in-memory by the Zig broker. Partition state is tracked in `PartitionStore.PartitionState` (`src/broker/partition_store.zig`), while cluster-wide metadata is managed by `RaftState` and `MetadataImage` (`src/raft/state.zig`):

| Metadata | Purpose | Zig Implementation |
|----------|---------|-------------------|
| `PartitionState.next_offset` | Next assignable offset per partition | `PartitionStore.partitions` (`StringHashMap`) |
| `PartitionState.high_watermark` | Highest durable offset (gated by S3 flush in sync mode) | Updated in `produce()` after successful write |
| `PartitionState.last_stable_offset` | `min(first_unstable_txn_offset, HW)` for READ_COMMITTED | Updated on each produce; used by `fetchWithIsolation()` |
| `S3WalBatcher.last_flushed_offset` | Highest offset durably written to S3 | Tracked in `S3WalBatcher`; checked via `isFlushed()` |
| `S3WalBatcher.wal_epoch` | Current WAL writer epoch for fencing | Bumped by `FailoverController.failoverNode()` |
| `TopicInfo` → partitions, config | Topic metadata (name, partition count, retention, etc.) | `Broker.topics` (`StringHashMap(TopicInfo)`) |
| `MetadataImage.brokers` | Registered broker endpoints | `AutoHashMap(i32, BrokerMetadata)` in `state.zig` |
| `MetadataImage.topics` | Topic → partition → leader mapping | `StringHashMap(TopicMetadata)` in `state.zig` |
| `producer_sequences` | Idempotent producer dedup (producer_id + seq) | `Broker.producer_sequences` (`AutoHashMap`) |

---

## Read Path

### How Metadata Is Used to Read Data from S3

When a consumer fetches data, the broker uses in-memory metadata to locate data across three storage tiers. Implemented in `PartitionStore.fetch()` and `PartitionStore.fetchWithIsolation()` (`src/broker/partition_store.zig`):

```
Consumer: Fetch(topic-A, partition-0, offset=150, max_bytes=1MB)
    │
    ▼
┌───────────────────────────────────────────────────────────────┐
│  Step 1: Resolve partition → PartitionState                   │
│  partitions.get("topic-A-0") → PartitionState                │
│    {next_offset, high_watermark, last_stable_offset}          │
│                                                               │
│  Step 2: Check isolation level (fetchWithIsolation)           │
│  READ_UNCOMMITTED (0) → effective_end = next_offset           │
│  READ_COMMITTED   (1) → effective_end = last_stable_offset    │
│    where LSO = min(first_unstable_txn_offset, high_watermark) │
│                                                               │
│  Step 3: Try Tier 1 — LogCache (cache.zig, in-memory FIFO)   │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ cache.get(stream_id, start_offset=150, end_offset)      │ │
│  │ Scans CacheBlocks (ArrayList of CachedRecord)           │ │
│  │ If found: return records (microsecond latency)           │ │
│  │ If not: fall through to Tier 2                           │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  Step 4: Try Tier 2 — S3BlockCache (cache.zig, LRU)          │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ s3_block_cache.get(cache_key)                            │ │
│  │ LRU cache backed by StringHashMap + access_order list    │ │
│  │ If hit: return cached S3 block (no network request)      │ │
│  │         s3_block_cache.hits++                            │ │
│  │ If miss: s3_block_cache.misses++, fall through to Tier 3 │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  Step 5: Tier 3 — S3 Object Storage via ObjectManager            │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ a) Query ObjectManager for matching S3 objects:          │ │
│  │    ObjectManager.getObjects(stream_id, start_off,        │ │
│  │                              end_off, limit=100)         │ │
│  │    → Returns []S3ObjectMetadata sorted by start_offset   │ │
│  │    (merges both StreamObjects and StreamSetObjects)      │ │
│  │                                                          │ │
│  │ b) For each matching object:                             │ │
│  │    - Read S3 object by s3_key from metadata              │ │
│  │    - ObjectReader.parse() → validate magic (0x4155544F)  │ │
│  │    - ObjectReader.findEntries(stream_id, start, end)     │ │
│  │    - ObjectReader.readBlock(idx) → raw record bytes      │ │
│  │                                                          │ │
│  │ c) Cache fetched blocks in S3BlockCache for future reads │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                               │
│  Step 6: Honor max_bytes                                      │
│  Include at least 1 record, then stop at max_bytes limit      │
│                                                               │
│  Step 7: Build Fetch response                                │
│  [partition][error_code][high_watermark][LSO][records...]     │
└───────────────────────────────────────────────────────────────┘
```

### Fetch Purgatory (Long Polling)

If the fetch returns no data and `max_wait_ms > 0`, the request is stored as a `DelayedFetch` in `Broker.delayed_fetches` (`src/broker/handler.zig`):

```
Empty fetch result
    │
    ▼
Store as DelayedFetch (handler.zig):
  {fd, topic, partition, offset, expiry_ms, correlation_id}
    │
    │                    ┌──────────────────┐
    │  Wake condition 1: │ New produce to    │
    │◄───────────────────│ same partition    │ → immediate response
    │                    └──────────────────┘
    │
    │                    ┌──────────────────┐
    │  Wake condition 2: │ expiry_ms reached │
    │◄───────────────────│ (checked in tick)│ → empty response
    │                    └──────────────────┘
```

---

## Failover: Handling Broker Failure

### The Ownership Race Problem

When a broker fails, there's a critical race condition: the old broker may still have in-flight writes to S3 while the new broker starts writing. Without fencing, both could write to the same stream, creating duplicate or out-of-order data.

The Zig implementation solves this with a **two-layer fencing mechanism** implemented across `FailoverController` (`src/broker/failover.zig`) and `S3WalBatcher` (`src/storage/wal.zig`):

```
                     TIME ──────────────────────────────►

Old Broker (wal_epoch=1)             New Broker (wal_epoch=2)
────────────────────────             ────────────────────────
  Writing to S3...
  S3WalBatcher.flushNow()
  in-flight
        │                            FailoverController.tick()
        │                            detects heartbeat timeout
        │                            (30 seconds, FAILOVER_TIMEOUT_MS)
        │                                 │
        │                            ┌────▼─────────────┐
        │                            │ Layer 1: WAL      │
        │                            │ Epoch Fencing      │
        │                            │                   │
        │                            │ failoverNode():   │
        │                            │  failed.is_fenced │
        │                            │    = true         │
        │                            │  wal_epoch += 1   │
        │                            │  (now epoch=2)    │
        │                            │                   │
        │                            │ Reassign failed   │
        │                            │ node's partitions │
        │                            │ to self           │
        │                            └────┬──────────────┘
        │                                 │
        │                            ┌────▼─────────────┐
  Old flush may                      │ Layer 2:          │
  succeed (S3 is                     │ S3WalBatcher      │
  unaware of epochs)                 │ Fencing           │
        │                            │                   │
  Next append() call:                │ S3WalBatcher      │
  ┌─────▼──────────┐                │ .setEpoch(2)      │
  │ S3WalBatcher   │                │ .is_fenced=false  │
  │ checks          │                │                   │
  │ is_fenced=true  │                │ New WAL objects   │
  │                 │                │ use key:          │
  │ Returns         │                │ wal/epoch-2/...   │
  │ error.WalFenced │                │                   │
  │                 │                │ Old epoch-1       │
  │ All subsequent  │                │ objects are stale │
  │ writes REJECTED │                │ (detectable by    │
  └─────────────────┘                │  epoch in key)    │
                                     └───────────────────┘
```

### Two Fencing Layers in Detail

**Layer 1: FailoverController WAL Epoch (`src/broker/failover.zig`)**
- `FailoverController` tracks a monotonically increasing `wal_epoch` (starts at 1)
- Each known node has a `NodeState` with `last_heartbeat_ms` and `is_fenced` flag
- `tick()` runs periodically: if `(now - last_heartbeat_ms) > 30s`, triggers `failoverNode()`
- `failoverNode()` sets `failed.is_fenced = true`, increments `wal_epoch`, reassigns partitions
- `isEpochValid()` returns `false` if this node's own `is_fenced` flag is set (zombie detection)

**Layer 2: S3WalBatcher Epoch Guard (`src/storage/wal.zig`)**
- `S3WalBatcher` has its own `wal_epoch` and `is_fenced` fields
- `append()` rejects writes with `error.WalFenced` when `is_fenced == true`
- `flushNow()` also checks `is_fenced` before uploading to S3
- `fence()` sets `is_fenced = true` — called when failover is triggered
- `setEpoch(new_epoch)` bumps the epoch and clears `is_fenced` — called by the new owner
- S3 object keys include the epoch: `wal/epoch-{wal_epoch}/bulk/{counter}` — stale objects from old epochs are identifiable by their key prefix
- `last_flushed_offset` tracks the highest offset durably written to S3; HW only advances past this in sync mode

### Failover Timeline

```
T=0:     Broker 0 is healthy, owning partition-A (wal_epoch=1)
T=10:    Broker 0 crashes
T=10-40: FailoverController on Broker 1 checks heartbeats every tick()
T=40:    Heartbeat timeout: (now - last_heartbeat_ms) > 30,000ms
T=40:    Broker 1 triggers failoverNode(broker_0):
           1. broker_0.is_fenced = true
           2. wal_epoch incremented to 2
           3. broker_0's partitions reassigned to Broker 1
           4. Broker 1's S3WalBatcher.setEpoch(2)
T=40:    Broker 1 starts serving partition-A
           - Reads existing data from S3 (same objects Broker 0 wrote)
           - New writes use wal/epoch-2/ key prefix
T=45:    Broker 0 restarts (zombie)
           - S3WalBatcher.is_fenced = true → all writes rejected
           - Must re-register with controller and get new epoch
```

### WAL Recovery on Restart

When a broker restarts after a clean shutdown or crash, implemented in `Wal.recover()` (`src/storage/wal.zig`):
1. `recover()` scans all WAL segment files in `data_dir`
2. For each record: validate magic byte (`0x01`) and CRC-32C checksum
3. Valid records are fed back into the LogCache via callback function
4. Corrupted or truncated records are skipped (logged as warnings)
5. The broker resumes serving from the recovered state

---

## Auto Load Balancing

### How It Works

Since all data lives in S3, partition reassignment is a **metadata-only operation** — no data needs to be copied. The `AutoBalancer` (`src/broker/auto_balancer.zig`) exploits this by freely moving partitions between brokers to equalize load.

```
Step 1: Collect Load Metrics (every 5 minutes, shouldCheck())
══════════════════════════════════════════════════════════════

  AutoBalancer tracks PartitionLoad per partition:
    {topic, partition_id, bytes_in_rate, bytes_out_rate, leader_node}

  Broker 0: 150 MB/s total (bytes_in + bytes_out)  → hot
  Broker 1:  80 MB/s total                          → cold
  Broker 2: 100 MB/s total                          → cold

Step 2: Detect Imbalance (20% threshold)
════════════════════════════════════════

  Average load = (150+80+100) / 3 = 110 MB/s
  Threshold    = average × 1.2 = 132 MB/s

  Broker 0: 150 > 132 → OVERLOADED ⚠️
  Broker 1:  80 < 132 → OK
  Broker 2: 100 < 132 → OK

Step 3: Generate Moves (computeRebalancePlan, greedy algorithm)
═══════════════════════════════════════════════════════════════

  Sort partitions on Broker 0 by totalLoad() (bytes_in + bytes_out)
  For each partition:
    Find target node where target_load < source_load × 0.8
    If found: add PartitionMove to RebalancePlan

  Move 1: topic-X/part-3 (40 MB/s)
    from Broker 0 → to Broker 1
    → PartitionMove{topic: "topic-X", partition_id: 3,
                      from_node: 0, to_node: 1}

  Move 2: topic-Y/part-1 (30 MB/s)
    from Broker 0 → to Broker 2
    → PartitionMove{topic: "topic-Y", partition_id: 1,
                      from_node: 0, to_node: 2}

Step 4: Execute Moves (metadata-only)
═════════════════════════════════════

  For each PartitionMove:
    1. Flush pending data to S3 on source broker
    2. Fence source broker's S3WalBatcher for the partition
    3. Update partition leader in MetadataImage
    4. Target broker reads existing data from S3 — same objects
    5. New writes go to target broker's WAL

  No data copied. Move completes in seconds, not hours.
```

### Why Metadata-Only Moves Work

Traditional Kafka must copy all partition data between brokers during reassignment (hours for large partitions). AutoMQ eliminates this because:

1. **All data is in S3** — every broker reads from the same S3 bucket via `PartitionStore`'s 3-tier read path
2. **Partition ownership is metadata** — `PartitionState` in `PartitionStore` tracks which node owns each partition
3. **New owner reads existing data** — S3 objects written by the old owner are still accessible; `ObjectReader` can parse any object regardless of which broker wrote it
4. **LogCache is rebuilt** — the in-memory hot data cache on the new broker is populated on first read from S3

The only state that needs to transfer is the `LogCache` content (in-memory hot data), which is rebuilt from S3 on the new broker's first fetch request.

---

## KRaft Consensus

### What KRaft Is Used For

KRaft (Kafka Raft) serves as the cluster's **metadata consensus layer**. In the Zig implementation, KRaft manages cluster membership and leader election. The metadata it tracks is stored in `MetadataImage` (`src/raft/state.zig`):

| Metadata Category | Zig Data Structure | Purpose |
|-------------------|--------------------|---------|
| **Brokers** | `MetadataImage.brokers` (`AutoHashMap(i32, BrokerMetadata)`) | Track active brokers (node_id, host, port, rack) |
| **Topics** | `MetadataImage.topics` (`StringHashMap(TopicMetadata)`) | Topic CRUD with partition metadata |
| **Partitions** | `TopicMetadata.partitions` (`AutoHashMap(i32, PartitionMetadata)`) | Partition → leader, epoch, replicas, ISR |
| **Raft Log** | `RaftLog.entries` (`ArrayList(LogEntry)`) | Ordered metadata change records |
| **Topic Config** | `Broker.topics` → `TopicConfig` | retention_ms, max_message_bytes, cleanup_policy, etc. |
| **ACLs** | `Authorizer` rules | Authorization rules for SASL-authenticated clients |

Note: Unlike AutoMQ which extends KRaft with stream-level metadata (S3StreamRecord, S3StreamSetObjectRecord, S3StreamObjectRecord, RangeRecord), the Zig implementation manages S3 object tracking directly in `PartitionStore` and `S3WalBatcher`. The design pattern is the same (metadata drives data location), but the Zig implementation uses a simpler per-partition model rather than the stream abstraction.

### How KRaft Achieves High Availability and Consistency

The Zig KRaft implementation (`src/raft/state.zig`, `src/raft/election_loop.zig`, `src/network/raft_client.zig`) provides Raft consensus with the following architecture:

```
                    KRaft Controller Quorum
                    (3 or 5 nodes for HA)

    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │ Controller 0 │    │ Controller 1 │    │ Controller 2 │
    │   (LEADER)   │◄──►│  (FOLLOWER) │◄──►│  (FOLLOWER) │
    │              │    │              │    │              │
    │ RaftState:   │    │ RaftState:   │    │ RaftState:   │
    │  role=leader │    │  role=follower│   │  role=follower│
    │  epoch=5     │    │  epoch=5     │    │  epoch=5     │
    │  RaftLog     │    │  RaftLog     │    │  RaftLog     │
    │  (replicated)│    │  (replicated)│    │  (replicated)│
    │  MetadataImg │    │  MetadataImg │    │  MetadataImg │
    └──────────────┘    └──────────────┘    └──────────────┘
```

**Raft Log Replication:**
- All metadata changes are appended to `RaftLog` as `LogEntry` records (each has `offset`, `epoch`, `data`)
- The leader appends via `RaftState.appendEntry()`, which also persists to `{data_dir}/raft.log`
- Entries are replicated to followers via `RaftClient.sendAppendEntries()` (API key 53)
- A record is committed after a majority acknowledges — tracked by `updateCommitIndex()` which computes the median of `match_index` across all voters

**Consistency Guarantees:**
- **Linearizable writes**: All metadata changes go through the leader, serialized by offset in `RaftLog`
- **Majority commit**: `commit_index` only advances to offset N when a majority have `match_index >= N`
- **Vote safety**: Each voter grants at most one vote per epoch (`voted_for` field); candidate's log must be at least as up-to-date (epoch + offset comparison in `handleVoteRequest()`)

**Controller Failover:**

```
T=0:   Controller 0 is leader (current_epoch=5)
T=10:  Controller 0 crashes
T=11:  ElectionTimer expires on Controller 1 (randomized 1.5-3s)
       (ElectionTimer.isExpired() returns true)
T=11:  Controller 1 starts election:
         - RaftState.startElection(): epoch=6, role=candidate, voted_for=self
         - RaftClientPool.broadcastVoteRequest() sends Vote RPCs (API 52)
           to all peers
         - Controller 2 receives Vote(epoch=6):
             handleVoteRequest(): candidate_epoch(6) > current_epoch(5)
             → grant vote, become follower at epoch=6
         - Controller 1 gets 2/3 votes → majority reached
         - RaftState.becomeLeader(): role=leader, init next_index/match_index
T=11:  Controller 1 becomes leader:
         - Begins accepting new metadata changes via appendEntry()
         - Sends BeginQuorumEpoch heartbeats (API 53) to all peers
         - Total failover time: ~2 seconds (election timeout)
```

**MetadataImage — In-Memory Materialized View:**

`MetadataImage` (`src/raft/state.zig`) provides a materialized snapshot of all cluster metadata:

```
MetadataImage (in-memory, populated from Raft log and broker state)
├── topics: StringHashMap(TopicMetadata)
│     └── "topic-A" → {name, topic_id (UUID), partitions: AutoHashMap}
│           └── partition 0 → {leader_id: 1, leader_epoch: 3,
│                               replicas: [1], isr: [1]}
├── brokers: AutoHashMap(i32, BrokerMetadata)
│     ├── 0 → {host: "10.0.0.1", port: 9092, rack: null}
│     ├── 1 → {host: "10.0.0.2", port: 9092, rack: null}
│     └── 2 → {host: "10.0.0.3", port: 9092, rack: null}
└── epoch: i64  (metadata version, incremented on changes)
```

On startup, `RaftState.loadPersistedLog()` replays entries from `{data_dir}/raft.log` (format: `epoch(8B) + offset(8B) + len(4B) + data(lenB)`), recovering the full Raft log state. The `MetadataImage` is then rebuilt from the replayed log and broker registration.

### Zig KRaft Implementation Details

| Component | File | Key Structures |
|-----------|------|---------------|
| **Raft state machine** | `src/raft/state.zig` | `RaftState` (role: unattached/candidate/leader/follower/resigned) |
| **Vote handling** | `src/raft/state.zig` | `handleVoteRequest()` — epoch comparison, log up-to-date check, one-vote-per-epoch |
| **Election loop** | `src/raft/election_loop.zig` | Background thread with `ElectionTimer` (1.5–3s randomized), vote counting |
| **RPC transport** | `src/network/raft_client.zig` | `RaftClientPool` — TCP connections to all peers for Vote (52) and BeginQuorumEpoch (53) |
| **Log persistence** | `src/raft/state.zig` | `persistEntry()` appends to `raft.log`; `loadPersistedLog()` replays on restart |
| **Commit tracking** | `src/raft/state.zig` | `updateCommitIndex()` — median of match_index across majority |
| **Metadata view** | `src/raft/state.zig` | `MetadataImage` — topics, brokers, partitions via `StringHashMap`/`AutoHashMap` |
| **Multi-node verified** | — | 3-node cluster successfully elects leader through Raft vote counting |

---

## Architecture Summary

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    ZMQ Process (4.5 MB binary)                            │
│           --process-roles controller,broker (default)                    │
│                                                                          │
│  Controller Port :9093                │  Broker Port :9092               │
│  ┌─────────────────────────────────┐  │  ┌───────────────────────────┐  │
│  │ Controller (controller.zig)     │  │  │ Broker (handler.zig)      │  │
│  │  KRaft APIs:                    │  │  │  44 Kafka client APIs:    │  │
│  │   Vote (52)                     │  │  │   Produce / Fetch         │  │
│  │   BeginQuorumEpoch (53)         │  │  │   Groups / Transactions   │  │
│  │   EndQuorumEpoch (54)           │  │  │   Admin / Metadata        │  │
│  │   DescribeQuorum (55)           │  │  │                           │  │
│  │  Broker lifecycle:              │  │  │  PartitionStore           │  │
│  │   BrokerRegistration (62)       │  │  │   ├─ LogCache (FIFO)     │  │
│  │   BrokerHeartbeat (63)          │  │  │   ├─ S3BlockCache (LRU)  │  │
│  │                                 │  │  │   ├─ S3WalBatcher        │  │
│  │  ┌─────────────┐ ┌───────────┐ │  │  │   │   (epoch-fenced)     │  │
│  │  │ RaftState    │ │ Broker    │ │  │  │   └─ ObjectManager       │  │
│  │  │ (owned)      │ │ Registry │ │  │  │       (SSO/SO registry)   │  │
│  │  └──────┬───────┘ └──────────┘ │  │  │                           │  │
│  └─────────┼───────────────────────┘  │  │  ┌─────────────────────┐ │  │
│            │ ptr                      │  │  │ CompactionManager   │ │  │
│            └──────────────────────────┼──│  │ (write-before-delete│ │  │
│                                       │  │  │  crash-safe)        │ │  │
│  Background Tasks                     │  │  └─────────────────────┘ │  │
│  ┌─────────────────────────────────┐  │  └───────────────────────────┘  │
│  │ ElectionLoop (100ms poll)       │  │                                  │
│  │ FailoverController (30s timeout)│  │  Security                        │
│  │ AutoBalancer (5min check)       │  │  ┌─────────────────────────┐    │
│  │ MetricsServer (:9090)           │  │  │ SASL/PLAIN+SCRAM-256   │    │
│  │ MetadataClient (broker-only)    │  │  │ ACL authorizer          │    │
│  └─────────────────────────────────┘  │  │ WAL epoch fencing       │    │
│                                       │  │ Controller lease fencing│    │
│                                       │  └─────────────────────────┘    │
└───────────────────────┬───────────────┴──────────────────────────────────┘
                        │
                        ▼
           ┌──────────────────────┐
           │      S3 / MinIO      │
           │ wal/epoch-N/bulk/NN  │  ← StreamSetObjects (WAL batches)
           │ data/so/{stream}/    │  ← StreamObjects (post-compaction)
           │ (11 nines durability)│
           └──────────────────────┘
```

### Design Principles Summary

| # | Principle | How Satisfied |
|---|-----------|--------------|
| 1 | **Kafka Protocol** | 44 API keys in `handler.zig`, flexible versions, standard Kafka clients compatible |
| 2 | **Write Durability** | S3 WAL sync mode: ack only after S3 upload. 2-layer epoch fencing. Controller lease fencing. |
| 3 | **Read Consistency** | HW gated by `last_flushed_offset`. LSO for READ_COMMITTED. 3-tier read cache. |
| 4 | **Broker Failover** | `FailoverController` + `BrokerRegistry`: 30s timeout → WAL fence → partition reassignment |
| 5 | **Load Balancing** | `AutoBalancer`: 20% threshold, greedy moves, metadata-only (no data copy) |
| 6 | **Configuration** | 20+ tunable knobs via CLI and `ConfigFile`: WAL, cache, compaction, roles, S3 |
| 7 | **Exactly-Once** | `TxnCoordinator`: 2PC transactions with control batch markers, producer epoch fencing |
| 8 | **Consumer Groups** | `GroupCoordinator`: state machine with leader-validated SyncGroup, rebalance signaling |
| 9 | **Dual Roles** | `--process-roles controller\|broker\|controller,broker` — fixed controller quorum, dynamic broker scaling |
| 10 | **Crash Safety** | Compaction uses write-before-delete. Merge aborts on partial read. S3 delete failures logged, not fatal. |

# AutoMQ Parity Roadmap

This document defines what "AutoMQ-complete" means for ZMQ and turns it into
tracked implementation gates. The target is open-source AutoMQ compatibility:
Kafka protocol behavior, AutoMQ S3Stream storage semantics, S3-backed durability,
stateless broker operation, cluster management, balancing, observability, and
operator-facing behavior.

## Current Baseline

- Zig toolchain target: Zig 0.16.0.
- Generated protocol request schemas: 110 entries in `src/protocol/api_support.zig`.
- Broker-advertised APIs: 67 entries in `api_support.broker_supported_apis`.
- AutoMQ extension API keys 501-519 and 600-602 are broker-dispatched and
  advertised. Stream/object APIs have `ObjectManager` side effects; KV,
  node, router, license, manifest, snapshot, and group-link APIs are backed by
  persisted local broker metadata with single-node semantics.
- Kafka protocol support is functional for common single-node broker paths, but
  semantic parity across all generated APIs and versions is incomplete.
- S3 WAL/object storage paths exist, but full AutoMQ S3Stream lifecycle
  compatibility, crash recovery, fencing, and cross-provider validation are
  incomplete. Stream/object metadata now has local file snapshot/restart
  coverage, partition offset/HW/LSO state is snapshotted for local restart, and
  filesystem WAL segments replay into the fetch cache after broker restart.
  If the local object snapshot is absent, flushed S3 WAL objects can rebuild
  ObjectManager stream-set metadata from their object indexes.

## Parity Gates

1. Protocol correctness: every advertised API/version has request decode,
   response encode, malformed-frame tests, golden Kafka wire fixtures, and real
   client compatibility tests.
2. AutoMQ extension APIs: stream/object/node extension APIs are dispatched only
   after matching generated schema keys, version ranges, error codes, and storage
   side effects.
3. S3 durability: acknowledged produce data survives process crash, restart,
   S3 transient failures, multipart edge cases, and metadata rebuild.
4. Stateless broker behavior: broker replacement/restart works without local
   disk dependency beyond cache; controller fencing prevents split-brain writes.
5. Cluster behavior: controller quorum, broker registration/heartbeat, leader
   epoch changes, reassignment, and scale in/out are exercised with multi-node
   tests.
6. Balancing/routing: auto-balancer and rack-aware routing converge under load
   and do not produce cross-AZ regressions in topology-aware tests.
7. Observability/security: metrics, readiness, structured logs, TLS/SASL/ACLs,
   and operational errors match documented behavior.
8. Performance: sustained produce/fetch throughput, tail latency, S3 request
   volume, recovery time, and memory usage have repeatable benchmark gates.

## Remaining Gap Backlog

This backlog is the execution order for closing the remaining AutoMQ-completion
gaps. Each item should land as a small, verified tranche with tests and a commit.

| Priority | Gap | Required completion gate | Status |
| --- | --- | --- | --- |
| P0 | Advertised API audit | ApiVersions, request/response header versions, generated schema ranges, dispatch coverage, malformed-frame tests, golden fixtures, and client compatibility matrix all fail closed when an advertised API drifts. | In progress. Header flexible-version and handler-switch coverage are explicit and tested; ApiVersions catalog fixtures plus OffsetForLeaderEpoch, TxnOffsetCommit, Vote, Begin/EndQuorumEpoch, and OffsetDelete generated/malformed coverage are in place. Client matrix coverage remains. |
| P0 | Quorum-backed AutoMQ/controller metadata | Replace local-only AutoMQ KV/node/router/license/manifest/group/object metadata with quorum-backed records, snapshots, replay, fencing, and failover tests. | Not started. |
| P0 | S3/MinIO crash and fault-injection harness | Exercise produce/flush/restart/fetch/rebuild with transient 5xx, timeout, partial multipart, bad ETag, checksum, range-read, list inconsistency, and provider-specific behavior. | Partial local/S3-object recovery coverage exists; fault matrix is incomplete. |
| P1 | Multi-node KRaft and broker lifecycle | Broker registration, heartbeat, fencing/unfencing, controller failover, rolling restart, and leader epoch behavior are covered by three-node tests. | Not started beyond local scaffolding and generated handlers. |
| P1 | Stateless broker replacement | A replacement broker can rebuild state from shared storage and quorum metadata without local disk metadata or manual repair. | Partial S3/object repair exists; quorum metadata rebuild is missing. |
| P1 | Real reassignment and autobalancing | Partition movement, rack-aware placement, load convergence, and scale in/out semantics are implemented and tested under load. | Handler compatibility only. |
| P1 | Consumer group and transaction coordinator failover | Rebalances, offset lifecycle, transactions, idempotent producers, fencing, and coordinator migration survive restart and failover. | Partial single-node coordinator behavior. |
| P2 | Security gates | TLS/SASL/OAuth/SCRAM/ACL interop, negative cases, cert rotation, and authz coverage exist for every advertised API. | Partial components exist. |
| P2 | Observability gates | Metrics, structured logs, readiness/liveness, dashboards, and alertable SLOs match the documented operational contract. | Partial components exist. |
| P2 | Performance and client matrix gates | Repeatable benchmarks and Java/librdkafka/Go/Python/Kafka CLI compatibility suites pass across supported versions. | Not started. |

## Capability Matrix

| Area | Current status | Required to call complete |
| --- | --- | --- |
| Kafka ApiVersions/version catalog | In progress. Canonical table now drives advertised APIs and version checks, including AutoMQ extensions. | Keep catalog generated or audited against schemas and handler dispatch in CI. |
| Kafka broker APIs | Partial. 67 advertised APIs; many handlers are simplified single-node semantics. DescribeLogDirs, partition reassignment APIs, DescribeCluster, DescribeQuorum, ElectLeaders, and DescribeProducers now decode generated requests and return request-scoped generated responses instead of blanket hand-encoded results. | Full schema-valid decode/encode and Kafka-compatible semantics for every advertised version. |
| Kafka generated schemas | Broad. 110 request schemas generated. | Round-trip tests and golden fixtures for every generated request/response pair. |
| AutoMQ extension APIs | Implemented locally. Keys 501-519 and 600-602 dispatch through generated schemas; stream/object APIs mutate ObjectManager and controller-like APIs mutate persisted local broker metadata. | Replace local-only controller semantics with quorum-backed metadata, failover, and client compatibility fixtures. |
| S3 WAL | Partial. Sync durability path exists and failed uploads are not acknowledged. Filesystem WAL produces now fsync before ack, advance HW on durable write, and replay after local broker restart. Flushed S3 WAL objects can rebuild stream-set metadata idempotently when the local object snapshot is missing, including paginated and XML-escaped ListObjectsV2 responses. S3 WAL recovery now fails closed on unreadable WAL objects instead of silently skipping them. S3 WAL object upload has bounded retry for transient put failures, fetch returns storage errors for unreadable indexed S3 objects, malformed object indexes fail cleanly, interleaved stream-set objects fetch only the requested stream blocks, S3 block-cache keys include the exact visible fetch window, partition offsets are repaired from recovered stream metadata, and multipart completion rejects malformed part ETags. | S3 crash/restart recovery, idempotent retry, fencing, provider matrix, and fault injection. |
| S3Stream object lifecycle | Improved. Create/open/close/delete/trim/describe plus prepare/commit SO/SSO are wired to ObjectManager; object/prepared snapshots and partition offset/HW/LSO state are persisted locally with fsync and covered by broker restart tests. | Match full AutoMQ recovery, fencing, prepared-object expiry, quorum-backed object-state replay, and S3-backed metadata durability. |
| Controller/KRaft | Partial. Local Raft/controller scaffolding exists. | Multi-node quorum, broker registration, heartbeats, fencing, metadata snapshots, rolling restart. |
| Stateless brokers | Partial. Local cache/state still has single-node assumptions. | Rebuild broker state from shared storage/controller metadata without data loss or manual repair. |
| Reassignment/autobalancing | Partial. Basic handlers and balancer code exist. | Real partition movement semantics, convergence tests, and load/rack-aware placement. |
| Consumer groups/transactions | Partial. Core flows exist with simplified persistence. | Kafka-compatible rebalances, offset lifecycle, transactions, fencing, and coordinator failover. |
| Security | Partial. TLS, SASL, OAuth, SCRAM, and ACL pieces exist. | Interop suites, negative tests, cert rotation, authz coverage for all APIs. |
| Observability | Partial. Metrics and JSON logging exist. | Complete metric compatibility, readiness contracts, dashboards, and alertable SLOs. |
| Tests | Improving. Unit/integration tests run under Zig 0.16. | Add protocol golden, S3 fault injection, multi-node e2e, chaos, perf, and client matrix gates. |

## Execution Plan

### Phase 0: Make Gaps Measurable

Status: completed for the initial catalog and DeleteGroups slice.

- Add a canonical broker API support catalog.
- Fix Kafka API-key drift around DeleteGroups, IncrementalAlterConfigs, and
  partition reassignment APIs.
- Correct AutoMQ extension key metadata to match generated schemas.
- Add tests that fail if broker-advertised APIs exceed generated schema ranges.
- Add DeleteGroups behavior for empty groups and offset cleanup.

### Phase 1: Protocol Parity

- Generate or maintain a complete request/response schema catalog from
  `src/protocol/schemas`.
- For each generated schema, add serialize/deserialize/calcSize round-trip
  tests at min, max, and first flexible version.
- For each advertised broker API, add malformed request tests and at least one
  real Kafka client e2e test.
- Stop advertising any API/version whose handler is only a stub or whose
  response shape is not schema-compatible.
- Add CI checks that compare dispatch switch keys, ApiVersions output, header
  flexible-version mapping, and generated schemas.
- Status: DescribeLogDirs now uses generated request/response schemas across
  legacy and flexible versions, rejects malformed frames, and scopes results to
  requested topics/partitions. AlterPartitionReassignments and
  ListPartitionReassignments now decode generated requests, reject malformed
  frames, and return generated single-node responses instead of manual compact
  writes. DescribeCluster now decodes generated requests, rejects malformed
  frames, and returns generated endpoint-scoped single-node metadata.
  DescribeQuorum now decodes generated requests, returns generated quorum or
  not-controller responses, and scopes metadata-partition errors to the request.
  ElectLeaders now uses generated request/response schemas and returns requested
  topic-partition results with per-partition errors under single-node semantics.
  DescribeProducers now uses generated request/response schemas, rejects
  malformed frames, and returns only requested topic-partition results with
  unknown topic/partition errors under single-node semantics.

### Phase 2: AutoMQ S3Stream APIs

- Status: completed for schema-compatible single-node dispatch.
- Implemented handlers for APIs 501-519 and 600-602 behind the support catalog.
- Wired Create/Open/Close/Delete/Trim/DescribeStreams to ObjectManager and stream
  metadata.
- Wired PrepareS3Object, CommitStreamSetObject, and CommitStreamObject to object
  ID allocation, object indexes, compaction-visible metadata, and source-object
  destruction marking.
- Added local metadata backing for Get/Put/DeleteKVs, node registration/listing,
  next node ID allocation, zone router metadata, partition snapshot export,
  license update/describe, cluster manifest export, and group promotion state.
- Remaining gap: controller-style metadata is locally persisted only. To call
  this production AutoMQ-complete, move this state behind quorum metadata and add
  multi-node compatibility tests.
- Add state-machine tests for prepared, committed, destroyed, expired, and
  compacted objects.

### Phase 3: Durability And Recovery

- Add MinIO-backed integration tests for produce, flush, restart, fetch, and
  rebuild-from-S3.
- Add S3 fault injection for timeout, 5xx, partial multipart, bad ETag,
  checksum mismatch, range read failure, and list inconsistency.
- Prove the ack path never acknowledges records that are not durable under the
  configured durability mode.
- Add metadata snapshot/replay tests for topics, offsets, transactions,
  producers, and expanded stream/object state under crash/fault scenarios.
- Status: local partition offset/HW/LSO snapshots now reload across restart and
  clamp stale/corrupt invariants; filesystem WAL records now replay into
  LogCache and broker-level fetch after restart; filesystem WAL acks now wait
  for fsync and advance HW only after that barrier; S3 WAL object indexes now
  rebuild ObjectManager stream-set metadata if the local snapshot is absent and
  handle paginated and XML-escaped ListObjectsV2 recovery; duplicate S3 WAL keys
  are skipped during rebuild so repeated recovery is idempotent; unreadable S3
  WAL objects now fail startup/recovery when no local object snapshot exists
  instead of being silently skipped; S3 WAL object upload now has bounded
  transient-failure retry; fetch returns
  KAFKA_STORAGE_ERROR for unreadable indexed S3 objects instead of silently
  returning an empty success; partition next offset/HW/LSO are repaired from
  recovered S3 stream metadata when partition_state.meta is missing or stale;
  interleaved stream-set objects now fetch by matched index so one partition
  cannot read another partition's S3 blocks; malformed object indexes and block
  ranges now fail with parser errors instead of traps or bogus reads; S3 block
  cache keys include start/end/max-bytes/isolation so cached S3 data is not
  reused for the wrong visible fetch window; multipart upload rejects missing or
  malformed part ETags before completion; local metadata snapshots now fsync
  before save calls return. Remaining durability work is crash/fault recovery
  against quorum/controller metadata, deeper multipart fault injection, and
  provider compatibility coverage.

### Phase 4: Multi-Node AutoMQ Behavior

- Implement broker registration, controller heartbeat, fencing, and unfencing
  using the generated controller APIs.
- Add three-node tests for leader election, controller failover, broker restart,
  reassignment, and scale in/out.
- Replace single-node no-op inter-broker handlers with real controller-backed
  behavior or stop advertising them. Status: legacy inter-broker keys 4-7 are
  no longer advertised by ApiVersions; handlers remain non-advertised until
  controller-backed semantics are implemented.
- Validate rack-aware routing and auto-balancer decisions under load.

### Phase 5: Production Gates

- Add compatibility runs with Java, librdkafka, Go, Python, and Kafka CLI
  clients across supported API versions.
- Add chaos tests for SIGKILL, network partition, S3 outage, clock skew, and
  slow/partial clients.
- Add performance baselines for produce/fetch throughput, p99 latency, S3
  operations per MiB, recovery time, and memory growth.
- Add release criteria: no known data-loss bug, no advertised stub API, passing
  client matrix, passing MinIO/S3 matrix, and documented unsupported features.

## Rules For Future Changes

- Generated schema support does not imply broker support.
- ApiVersions must be driven only by `broker_supported_apis`.
- Every new advertised API needs a handler, version/header mapping, unit tests,
  malformed-frame tests, and at least one integration/client test.
- AutoMQ extension APIs must use the generated schema keys: 501-519 and 600-602.
- If an API is intentionally single-node-only, document the degraded semantics
  and add a test proving the response is schema-compatible.

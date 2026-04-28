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
  node, router, license, node-id allocation, and group-link mutations append
  committed Raft metadata records when a Raft state is attached. Stream/object
  metadata mutations append committed Raft snapshot records for attached
  single-node leaders. Single-node leaders can compact AutoMQ metadata/object
  records into a replayable full snapshot before Raft log truncation. Attached
  non-leaders reject these metadata mutations, and local snapshot compatibility
  remains for single-node mode. Manifest and partition snapshot APIs are
  read-side views.
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
| P0 | Advertised API audit | ApiVersions, request/response header versions, generated schema ranges, dispatch coverage, malformed-frame tests, golden fixtures, and client compatibility matrix all fail closed when an advertised API drifts. | In progress. Header flexible-version and handler-switch coverage are explicit and tested; source-level tests now parse `generated_index.zig` and the real broker/controller `handleRequest` switches so generated schema counts and dispatch cases cannot drift from the audit tables silently. Controller ApiVersions now uses a controller-specific support catalog and advertises generated AddRaftVoter/RemoveRaftVoter keys 80/81 instead of telemetry keys 71/72. ApiVersions catalog fixtures plus AddPartitionsToTxn, AddOffsetsToTxn, CreateTopics, DeleteTopics, DeleteRecords, DescribeConfigs, DescribeGroups, EndTxn, Fetch, FindCoordinator, Heartbeat, InitProducerId, JoinGroup, LeaveGroup, ListGroups, ListOffsets, Metadata, Produce, SaslHandshake, SaslAuthenticate, SyncGroup, OffsetCommit, OffsetFetch, OffsetForLeaderEpoch, WriteTxnMarkers, TxnOffsetCommit, DescribeAcls/CreateAcls/DeleteAcls, AlterConfigs/IncrementalAlterConfigs, CreatePartitions, Vote, Begin/EndQuorumEpoch, and OffsetDelete generated/malformed coverage are in place. Gated `test-client-matrix` now probes installed external clients (`kcat` and Kafka CLI) against a running broker. Broader language/client-version matrix remains. |
| P0 | Quorum-backed AutoMQ/controller metadata | Replace local-only AutoMQ KV/node/router/license/manifest/group/object metadata with quorum-backed records, snapshots, replay, fencing, and failover tests. | In progress. KV/node/router/license/node-id/group mutations now append committed Raft records for attached single-node leaders. ObjectManager stream/object mutations now append committed Raft snapshot records. Both paths replay on broker open and reject attached non-leader mutations. Single-node leaders now append a full AutoMQ metadata/ObjectManager snapshot record before Raft snapshot truncation and replay it after compaction. Internal AppendEntries now carries log entry bytes; followers apply appended AutoMQ metadata/object snapshot records and replay them after promotion. Multi-process failover and client compatibility remain. |
| P0 | S3/MinIO crash and fault-injection harness | Exercise produce/flush/restart/fetch/rebuild with transient 5xx, timeout, partial multipart, bad ETag, checksum, range-read, list inconsistency, and provider-specific behavior. | In progress. Local MockS3 fault injection now covers bounded put retry, get/list/range/delete failures, temporary list omission, recovery retry, fetch failure, compaction orphan retry, multipart bad/missing part ETags, complete failure, embedded complete errors, exact range-window validation, abort verification, broker stateless replacement, and S3 WAL resume without object-key overwrite. Gated `test-minio` covers live object, multipart, S3 WAL rebuild/fetch, and PartitionStore S3 WAL produce/rebuild/resume against MinIO/S3. Gated `test-s3-process-crash` starts a real broker process, kills it after acknowledged S3 WAL produce, restarts a replacement with a fresh local data dir, fetches from S3, and appends new data. S3 request signing now omits default HTTP/HTTPS ports from the canonical Host header for AWS-style providers while preserving explicit custom ports. Broader live provider matrix remains. |
| P1 | Multi-node KRaft and broker lifecycle | Broker registration, heartbeat, fencing/unfencing, controller failover, rolling restart, and leader epoch behavior are covered by three-node tests. | In progress. Controller Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, BrokerRegistration, BrokerHeartbeat, AllocateProducerIds, AddRaftVoter, and RemoveRaftVoter now use generated schemas on the controller port; controller tests are part of the default Zig test suite. Gated `test-kraft-failover` now starts three controller-only processes plus a broker-only process, kills the discovered controller leader, verifies surviving controllers elect a replacement, restarts a surviving controller, restarts the broker with the same data dir, and verifies the broker can produce and fetch acknowledged records throughout. Broker registrations and producer-id allocations now append controller metadata records, followers persist replicated AppendEntries, controller startup replays durable records from Raft, promoted followers can heartbeat brokers without forced re-registration or producer-id reuse, and controller full snapshot records preserve broker/PID state across Raft log compaction. Broader failover gates remain. |
| P1 | Stateless broker replacement | A replacement broker can rebuild state from shared storage and quorum metadata without local disk metadata or manual repair. | Partial S3/object repair exists; quorum metadata rebuild is missing. |
| P1 | Real reassignment and autobalancing | Partition movement, rack-aware placement, load convergence, and scale in/out semantics are implemented and tested under load. | In progress. Handler compatibility exists for reassignment APIs; AlterPartitionReassignments now tracks in-memory ongoing reassignments, ListPartitionReassignments returns requested ongoing state, and cancel clears it. The auto-balancer has a rack-aware planning path with deterministic coverage for cross-rack target preference and same-rack fallback. Controller-backed reassignment execution and load convergence gates remain. |
| P1 | Consumer group and transaction coordinator failover | Rebalances, offset lifecycle, transactions, idempotent producers, fencing, and coordinator migration survive restart and failover. | Partial single-node coordinator behavior. |
| P2 | Security gates | TLS/SASL/OAuth/SCRAM/ACL interop, negative cases, cert rotation, and authz coverage exist for every advertised API. | Partial components exist. |
| P2 | Observability gates | Metrics, structured logs, readiness/liveness, dashboards, and alertable SLOs match the documented operational contract. | Partial components exist. |
| P2 | Performance and client matrix gates | Repeatable benchmarks and Java/librdkafka/Go/Python/Kafka CLI compatibility suites pass across supported versions. | Started. A gated external-client matrix step exists for kcat and Kafka CLI metadata/API probes; broader Java/librdkafka/Go/Python versioned suites remain. |

## Capability Matrix

| Area | Current status | Required to call complete |
| --- | --- | --- |
| Kafka ApiVersions/version catalog | In progress. Canonical tables now drive broker and controller advertised APIs and version checks, including AutoMQ extensions. Source-level tests audit generated schema counts and the actual broker/controller dispatch switches against the catalogs; `test-client-matrix` provides a gated real-client compatibility hook. | Keep expanding golden fixtures and broaden client compatibility runs. |
| Kafka broker APIs | Partial. 67 advertised APIs; many handlers are simplified single-node semantics. DescribeLogDirs, partition reassignment APIs, DescribeCluster, DescribeQuorum, ElectLeaders, and DescribeProducers now decode generated requests and return request-scoped generated responses instead of blanket hand-encoded results. | Full schema-valid decode/encode and Kafka-compatible semantics for every advertised version. |
| Kafka generated schemas | Broad. 110 request schemas generated. | Round-trip tests and golden fixtures for every generated request/response pair. |
| AutoMQ extension APIs | Implemented locally with quorum-backed metadata starting. Keys 501-519 and 600-602 dispatch through generated schemas; stream/object mutations can be backed by committed Raft snapshot records; KV/node/router/license/node-id/group mutations can be backed by committed Raft records; attached non-leaders fail closed for these metadata mutations; single-node leaders compact these records through a replayable full snapshot record before Raft truncation. | Finish multi-node replication/failover and client compatibility fixtures. |
| S3 WAL | Partial. Sync durability path exists and failed uploads are not acknowledged. Filesystem WAL produces now fsync before ack, advance HW on durable write, and replay after local broker restart. Flushed S3 WAL objects can rebuild stream-set metadata idempotently when the local object snapshot is missing, including paginated and XML-escaped ListObjectsV2 responses. S3 WAL recovery now fails closed on unreadable WAL objects instead of silently skipping them. S3 WAL object upload has bounded retry for transient put failures, including injected MockS3 failures; failed sync S3 WAL produces do not advance offsets/HW/cache or retain duplicate pending entries; a replacement local store or broker can rebuild and fetch acknowledged S3 WAL data from object storage, seed the WAL object counter from existing objects, and resume producing without overwriting old WAL keys; S3Storage propagates read/list/range/delete faults; recovery retries after transient get/list failures and temporary list omissions; range fetches return exactly the requested byte window or fail closed; fetch returns storage errors for unreadable indexed S3 objects; mixed hot-cache/cold-S3 fetches do not drop the S3 prefix when a restarted broker has only the tail in LogCache; overlapping S3 WAL offset ranges now fail recovery instead of returning duplicate/conflicting data; malformed object indexes fail cleanly; interleaved stream-set objects fetch only the requested stream blocks; S3 block-cache keys include the exact visible fetch window; partition offsets are repaired from recovered stream metadata; multipart upload rejects missing or malformed part ETags, aborts failed uploads, and fails closed on non-2xx or embedded XML complete errors. Gated MinIO/S3 test steps validate live object round-trip, multipart round-trip, WAL rebuild/fetch, PartitionStore S3 WAL produce/rebuild/resume, and real broker-process kill/replacement recovery. S3 signing has deterministic coverage for AWS-style default-port Host canonicalization. | Broader live provider matrix, remaining provider-specific multipart faults, and full fencing. |
| S3Stream object lifecycle | Improved. Create/open/close/delete/trim/describe plus prepare/commit SO/SSO are wired to ObjectManager; object/prepared snapshots and partition offset/HW/LSO state are persisted locally with fsync and covered by broker restart tests. | Match full AutoMQ recovery, fencing, prepared-object expiry, quorum-backed object-state replay, and S3-backed metadata durability. |
| Controller/KRaft | Partial. Local Raft/controller scaffolding exists. Controller ApiVersions and quorum/lifecycle RPC framing now use generated schemas for ApiVersions, Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, BrokerRegistration, BrokerHeartbeat, AllocateProducerIds, AddRaftVoter, and RemoveRaftVoter; telemetry keys 71/72 are no longer treated as voter APIs. Controller tests now run in the default Zig suite and cover malformed Vote plus internal AppendEntries compatibility. A gated controller/broker failover plus rolling-restart harness exists and broker-only nodes start fenced until controller heartbeat succeeds. Broker registrations and producer-id allocation cursors are now stored as controller metadata records, replayed from persisted Raft logs on follower promotion/restart, and compacted through full controller snapshot records before Raft truncation. | Add broader broker+controller failover gates. |
| Stateless brokers | Partial. Local cache/state still has single-node assumptions. | Rebuild broker state from shared storage/controller metadata without data loss or manual repair. |
| Reassignment/autobalancing | Partial. Generated reassignment handlers now retain and list in-memory ongoing reassignment state, and auto-balancer planning can prefer less-loaded targets in a different rack when topology is available. | Real partition movement semantics, convergence tests, and controller-backed load/rack-aware placement. |
| Consumer groups/transactions | Partial. Core flows exist with simplified persistence. | Kafka-compatible rebalances, offset lifecycle, transactions, fencing, and coordinator failover. |
| Security | Partial. TLS, SASL, OAuth, SCRAM, and ACL pieces exist. | Interop suites, negative tests, cert rotation, authz coverage for all APIs. |
| Observability | Partial. Metrics and JSON logging exist. | Complete metric compatibility, readiness contracts, dashboards, and alertable SLOs. |
| Tests | Improving. Unit/integration tests run under Zig 0.16. Controller, broker registry, and metadata client tests are included in the default suite. | Add protocol golden, broader multi-node e2e, chaos, perf, and client matrix gates. |

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
- Status: Source-level protocol audit tests now parse the generated index and
  broker dispatch switch so schema-count and handler-key drift fail in local/CI
  tests. DescribeLogDirs now uses generated request/response schemas across
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
- Controller-style KV/node/router/license/node-id/group mutations now have a
  committed Raft record path with replay and non-leader fencing. Stream/object
  mutations now have a committed Raft snapshot path with replay and non-leader
  fencing. Single-node leaders now compact these records by appending a full
  AutoMQ metadata/ObjectManager snapshot record before Raft truncation. Internal
  AppendEntries now carries actual log entry bytes and followers apply appended
  AutoMQ metadata and object snapshot records, including follower-promotion
  replay coverage. Remaining gap: add multi-process failover compatibility tests.
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
  transient-failure retry; failed sync S3 WAL produces now return errors without
  advancing offsets/HW/cache or retaining duplicate pending entries; replacement
  local stores can rebuild acknowledged S3 WAL produce data from object storage
  and fetch it after offset repair; fetch returns
  KAFKA_STORAGE_ERROR for unreadable indexed S3 objects instead of silently
  returning an empty success; partition next offset/HW/LSO are repaired from
  recovered S3 stream metadata when partition_state.meta is missing or stale;
  interleaved stream-set objects now fetch by matched index so one partition
  cannot read another partition's S3 blocks; malformed object indexes and block
  ranges now fail with parser errors instead of traps or bogus reads; S3 block
  cache keys include start/end/max-bytes/isolation so cached S3 data is not
  reused for the wrong visible fetch window; multipart upload rejects missing or
  malformed part ETags before completion; local metadata snapshots now fsync
  before save calls return; MockS3 fault injection now covers bounded put retry,
  propagated get/list/range/delete failures, temporary list omission, recovery
  retry, fetch storage errors, and compaction orphan retry; restarted S3 WAL
  writers now seed their object counters from existing WAL keys before accepting
  writes, and broker replacement coverage verifies recovered S3 data plus new
  hot-cache data fetch as a single range without overwriting prior WAL objects;
  overlapping S3 WAL offset ranges now fail recovery instead of surfacing
  duplicate or conflicting records from stale writers;
  S3Client multipart fault tests now cover missing/malformed part ETags,
  abort-after-part failure, abort-after-complete failure, and HTTP 200
  CompleteMultipartUpload responses carrying embedded XML errors; a gated
  `test-minio` build step now covers live object round-trip, multipart
  round-trip, S3 WAL metadata rebuild plus fetch, and PartitionStore S3 WAL
  produce/rebuild/resume against MinIO/S3; `test-s3-process-crash` adds a gated
  real broker-process kill/replacement harness against MinIO/S3. S3 request
  signing now omits default HTTP/HTTPS ports from the canonical Host header for
  AWS-style providers and preserves explicit custom ports. Remaining durability
  work is broader live provider matrix coverage, provider-specific multipart
  faults, and crash/fault recovery against quorum/controller metadata.

### Phase 4: Multi-Node AutoMQ Behavior

- Implement broker registration, controller heartbeat, fencing, and unfencing
  using the generated controller APIs.
- Add three-node tests for leader election, controller failover, broker restart,
  reassignment, and scale in/out.
- Replace single-node no-op inter-broker handlers with real controller-backed
  behavior or stop advertising them. Status: legacy inter-broker keys 4-7 are
  no longer advertised by ApiVersions; handlers remain non-advertised until
  controller-backed semantics are implemented. Controller ApiVersions now has a
  separate audited support catalog; generated controller quorum/lifecycle
  framing is in place for Vote, BeginQuorumEpoch, EndQuorumEpoch,
  DescribeQuorum, BrokerRegistration, BrokerHeartbeat, AllocateProducerIds,
  AddRaftVoter, and RemoveRaftVoter, and keys 71/72 remain
  telemetry-only/unsupported on the controller port. A gated
  `test-kraft-failover` step covers three controller-only processes, a
  broker-only process, controller leader discovery, broker produce before
  failover, leader kill, replacement leader convergence, controller rolling
  restart, broker rolling restart, broker produce after each transition, and
  broker fetch/read-after-transition checks for all acknowledged records.
  Broker registration state and producer-id allocation cursors now have
  Raft-backed controller metadata records; followers persist replicated
  AppendEntries and promoted or restarted controllers replay registered brokers
  and PID cursors before serving lifecycle APIs. Controller metadata full
  snapshot records now preserve broker/PID state across Raft log compaction.
  Remaining gap: broader failover gates and client compatibility fixtures.
- Validate rack-aware routing and auto-balancer decisions under load. Status:
  rack-aware planning has unit coverage for cross-rack target preference and
  same-rack fallback; reassignment APIs can retain/list/cancel ongoing in-memory
  state. Controller-backed reassignment execution remains.

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

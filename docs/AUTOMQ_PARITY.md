# AutoMQ Parity Roadmap

This document defines what "AutoMQ-complete" means for ZMQ and turns it into
tracked implementation gates. The target is open-source AutoMQ compatibility:
Kafka protocol behavior, AutoMQ S3Stream storage semantics, S3-backed durability,
stateless broker operation, cluster management, balancing, observability, and
operator-facing behavior.

## Current Baseline

- Zig toolchain target: Zig 0.16.0.
- Generated protocol request schemas: 110 entries in `src/protocol/api_support.zig`.
- Broker-advertised APIs: 79 entries in `api_support.broker_supported_apis`.
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
  coverage, topic-level numeric configs and partition offset/HW/LSO state are
  snapshotted for local restart, and filesystem WAL segments replay into the
  fetch cache after broker restart.
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
| P0 | Advertised API audit | ApiVersions, request/response header versions, generated schema ranges, dispatch coverage, malformed-frame tests, golden fixtures, and client compatibility matrix all fail closed when an advertised API drifts. | In progress. Header flexible-version and handler-switch coverage are explicit and tested; source-level tests now parse `generated_index.zig` and the real broker/controller `handleRequest` switches so generated schema counts and dispatch cases cannot drift from the audit tables silently. Source-level generated-schema audits now pin all 20 nullable-array fields to optional-slice null/empty preservation paths and all 23 currently modeled tagged fields to schema-visible encode/decode/duplicate-tag coverage, so protocol regeneration drift fails the default Zig suite. Default generated-message round-trip smoke tests now cover all 230 top-level generated request/response/header/record types across common protocol versions 0-20, and non-default golden wire fixtures now cover legacy and flexible encodings for generated request headers, MetadataRequest, FetchRequest v17 tagged `ClusterId`/`ReplicaState`/`ReplicaDirectoryId`, ProduceRequest, ProduceResponse v10 and FetchResponse v16 KIP-951 tagged leader endpoints/partition metadata, Vote/BeginQuorumEpoch/EndQuorumEpoch v1 KIP-853 tagged `NodeEndpoints`, FetchSnapshot request/response v1 KIP-853 tagged `ClusterId`/`ReplicaDirectoryId`/`CurrentLeader`/`NodeEndpoints`, BrokerHeartbeatRequest v1 tagged `OfflineLogDirs`, CreateTopicsResponse v5+ tagged `TopicConfigErrorCode`, UpdateMetadataRequest v8 tagged `Type` plus legacy upper-bound fields, ApiVersionsResponse, and CreateTopicsRequest including nested compact arrays and tagged-field boundaries. Manual ApiVersions v3+ response encoding now round-trips KRaft feature metadata tagged fields (`SupportedFeatures`, `FinalizedFeaturesEpoch`, `FinalizedFeatures`, `ZkMigrationReady`) and preserves unknown response tags without duplicating known tags after decode. Controller ApiVersions now uses a controller-specific support catalog and advertises generated ControllerRegistration key 70 plus AddRaftVoter/RemoveRaftVoter keys 80/81 instead of telemetry keys 71/72; UpdateRaftVoter key 82 remains non-advertised until endpoint updates are implemented, but direct requests return generated fail-closed responses. ConsumerGroupHeartbeat key 68 remains non-advertised until KIP-848 coordinator semantics exist, AssignReplicasToDirs key 73 remains non-advertised until JBOD directory semantics exist, ShareGroupHeartbeat/ShareGroupDescribe/ShareFetch/ShareAcknowledge keys 76-79 remain non-advertised until share-group coordinator/session semantics exist, and Initialize/Read/Write/DeleteShareGroupState plus ReadShareGroupStateSummary keys 83-87 remain non-advertised until share-state storage exists, but direct requests for all eleven return generated fail-closed responses. Advertised broker and controller APIs now have catalog-level `max_version + 1` fail-closed coverage before body decode. ApiVersions catalog fixtures plus AddPartitionsToTxn, AddOffsetsToTxn, AlterClientQuotas, AlterUserScramCredentials, AssignReplicasToDirs, ConsumerGroupDescribe, ConsumerGroupHeartbeat, ControllerRegistration, CreateTopics including supported creation-time configs, DeleteTopics, DeleteRecords, DeleteShareGroupState, DescribeClientQuotas, DescribeConfigs, DescribeGroups, DescribeTopicPartitions including cursor pagination, DescribeTransactions, DescribeUserScramCredentials, EndTxn, Fetch, FetchSnapshot, FindCoordinator including invalid coordinator-type, empty-key, and unimplemented share-coordinator negatives, GetTelemetrySubscriptions, Heartbeat, InitProducerId, InitializeShareGroupState, JoinGroup, LeaveGroup, ListClientMetricsResources, ListGroups including v5 group-type filters, ListOffsets, ListTransactions, Metadata, Produce, PushTelemetry, ReadShareGroupState, ReadShareGroupStateSummary, SaslHandshake, SaslAuthenticate, ShareAcknowledge, ShareFetch, ShareGroupDescribe, ShareGroupHeartbeat, SyncGroup, UnregisterBroker, UpdateFeatures, UpdateRaftVoter, OffsetCommit, OffsetFetch, OffsetForLeaderEpoch, WriteShareGroupState, WriteTxnMarkers, TxnOffsetCommit, DescribeAcls/CreateAcls/DeleteAcls, AlterConfigs/IncrementalAlterConfigs, CreatePartitions, Vote, Begin/EndQuorumEpoch, and OffsetDelete generated/malformed coverage are in place. Gated `test-client-matrix` now probes installed external clients (`kcat` and Kafka CLI) against a running broker. Broader language/client-version matrix remains. |
| P0 | Quorum-backed AutoMQ/controller metadata | Replace local-only AutoMQ KV/node/router/license/manifest/group/object metadata with quorum-backed records, snapshots, replay, fencing, and failover tests. | In progress. KV/node/router/license/node-id/group mutations now append committed Raft records for attached leaders. ObjectManager stream/object mutations now append committed Raft snapshot records. Both paths replay on broker open and reject attached non-leader mutations. Single-node leaders now append a full AutoMQ metadata/ObjectManager snapshot record before Raft snapshot truncation and replay it after compaction. Internal AppendEntries now carries log entry bytes; followers apply committed AutoMQ metadata/object snapshot records on AppendEntries and replay them after promotion; leaders also wait briefly for post-commit propagation so a promoted follower does not reuse a stale node-id allocator cursor. Combined controller+broker multi-process failover now covers AutoMQ KV put/get/delete, zone router, node registry, license, node-id allocator, group promote/demote, stream create/prepare/commit/open/close/trim/delete, stream-set object commit, manifest stream/group-count probes, partition-snapshot protocol smoke, and stream metadata replication, leader kill, replacement-leader mutation, and old-leader restart/rejoin through gated `test-kraft-failover`. The default Zig suite now includes topic-backed partition snapshot fixtures. Broader client compatibility remains. |
| P0 | S3/MinIO crash and fault-injection harness | Exercise produce/flush/restart/fetch/rebuild with transient 5xx, timeout, partial multipart, bad ETag, checksum, range-read, list inconsistency, and provider-specific behavior. | In progress. Local MockS3 fault injection now covers bounded put retry, get/list/range/delete failures, temporary list omission, recovery retry, fetch failure, compaction orphan retry, multipart bad/missing/XML-unsafe part ETags, complete failure, embedded complete errors including chunked responses with split XML tags, exact range-window validation including high-level `206 Content-Range` mismatch/missing-header rejection, abort verification, broker stateless replacement, coordinator snapshot upload failure, and S3 WAL resume without object-key overwrite. Gated `test-minio` covers live object, multipart, S3 WAL rebuild/fetch, and PartitionStore S3 WAL produce/rebuild/resume against MinIO/S3. Gated `test-s3-process-crash` starts a real broker process, kills it after acknowledged S3 WAL produce and OffsetCommit, restarts a replacement with a fresh local data dir, fetches data and committed offsets from S3, and appends new data. S3 request signing now omits default HTTP/HTTPS ports from the canonical Host header for AWS-style providers while preserving explicit custom ports. Broader live provider matrix remains. |
| P1 | Multi-node KRaft and broker lifecycle | Broker registration, heartbeat, fencing/unfencing, controller failover, rolling restart, and leader epoch behavior are covered by three-node tests. | In progress. Controller Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat, UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter, and RemoveRaftVoter now use generated schemas on the controller port; controller tests are part of the default Zig test suite. Gated `test-kraft-failover` now starts three controller-only processes plus a broker-only process, kills the discovered controller leader, verifies surviving controllers elect a replacement, restarts the killed old leader with a fresh local data dir under the replacement leader, restarts a surviving controller, restarts the broker with the same data dir, and verifies the broker can produce and fetch acknowledged records throughout. Broker registrations, broker unregistrations, and producer-id allocations now append controller metadata records, followers persist replicated AppendEntries, controller startup replays durable records from Raft, promoted followers can heartbeat brokers without forced re-registration or producer-id reuse, and controller full snapshot records preserve broker/PID state across Raft log compaction. Local FailoverController ownership metadata now tracks broker topic create/delete/create-partition/restore paths and transfers tracked partitions from fenced timed-out brokers to the surviving broker in default tests. Broader failover gates remain. |
| P1 | Stateless broker replacement | A replacement broker can rebuild state from shared storage and quorum metadata without local disk metadata or manual repair. | In progress. S3/object repair exists, committed offsets are now written to versioned `__consumer_offsets` records and replayed from recovered S3 WAL objects, OffsetDelete and DeleteGroups now write `__consumer_offsets` tombstones before local offset removal and replay them during replacement, consumer group lifecycle snapshots are now written to `__consumer_offsets` and replayed from S3 WAL, transaction coordinator snapshots are now written to `__transaction_state` and replayed from S3 WAL, atomic EndTxn and WriteTxnMarkers S3 WAL objects now restore both commit/abort marker partition offsets and completed transaction snapshots after fresh-dir replacement, client-facing group/transaction mutations fail closed when coordinator snapshot S3 WAL writes fail, topic IDs, partition counts, supported topic configs, and ongoing partition reassignment snapshots are now written to `__cluster_metadata` and replayed from recovered S3 WAL, idempotent producer sequence state is rebuilt from durable log batches after S3 WAL replacement, and the combined controller+broker failover gate now restarts the killed AutoMQ leader after deleting its local data dir and verifies it rebuilds quorum-backed AutoMQ metadata from Raft. Broader broker-only metadata replacement remains. |
| P1 | Real reassignment and autobalancing | Partition movement, rack-aware placement, load convergence, and scale in/out semantics are implemented and tested under load. | In progress. Handler compatibility exists for reassignment APIs; AlterPartitionReassignments now tracks ongoing reassignments, persists them across local broker restart and fresh-dir S3 WAL replacement through `__cluster_metadata`, fails closed when the shared reassignment snapshot cannot be written, ListPartitionReassignments returns requested ongoing state, and cancel clears persisted state. The auto-balancer has a rack-aware planning path with deterministic coverage for cross-rack target preference and same-rack fallback, ignores stale unknown-node load samples when computing known-node averages, clamps negative metric rates to zero, and now has a simulated load-convergence unit gate. Controller-backed reassignment execution and live load/scale convergence gates remain. |
| P1 | Consumer group and transaction coordinator failover | Rebalances, offset lifecycle, transactions, idempotent producers, fencing, and coordinator migration survive restart and failover. | Partial single-node coordinator behavior. OffsetDelete and DeleteGroups now write and replay `__consumer_offsets` tombstones and fail closed on shared tombstone write failure; DeleteGroups, JoinGroup, LeaveGroup, and SyncGroup roll back local group visibility when lifecycle snapshots cannot be written. |
| P2 | Security gates | TLS/SASL/OAuth/SCRAM/ACL interop, negative cases, cert rotation, and authz coverage exist for every advertised API. | In progress. TLS/SASL/OAuth/SCRAM/ACL components exist. Broker ACL resource/operation mapping is now audited against every advertised broker API except pre-auth SASL handshake/authenticate, missing mappings for DeleteRecords, OffsetForLeaderEpoch, AddOffsetsToTxn, ACL admin APIs, quorum APIs, DescribeCluster, and DescribeProducers are covered in the default Zig suite, DescribeAcls/CreateAcls/DeleteAcls, DescribeQuorum/DescribeCluster/ListTransactions, InitProducerId/AddOffsetsToTxn/EndTxn, topic-scoped ListOffsets/DeleteRecords/OffsetForLeaderEpoch/DescribeProducers, group-introspection ListGroups/DescribeGroups/ConsumerGroupDescribe, coordinator/session FindCoordinator/JoinGroup/Heartbeat/LeaveGroup/SyncGroup, and offset OffsetCommit/OffsetFetch authorization denials now return generated schema-compatible responses, and unsupported versions are rejected before ACL denial builders can serialize unsupported schemas. Interop suites, negative cases, cert rotation, and full authz response-shape coverage remain. |
| P2 | Observability gates | Metrics, structured logs, readiness/liveness, dashboards, and alertable SLOs match the documented operational contract. | In progress. Metrics and JSON logging exist; `/health` and `/ready` now share a deterministic routing/response contract across plain and TLS metrics transports, including exact-path matching plus startup and shutdown readiness transitions in the default Zig suite. Prometheus export now escapes HELP text and label values, rejects invalid labeled-metric arity, and uses canonical label-set keys in registry tests. Broker metric registration is audited against the advertised broker API metric catalog and includes produce/fetch throttle counters so request metrics are not silently dropped. Dashboards, broader metric-name compatibility, and alertable SLO gates remain. |
| P2 | Performance and client matrix gates | Repeatable benchmarks and Java/librdkafka/Go/Python/Kafka CLI compatibility suites pass across supported versions. | Started. A gated external-client matrix step exists for kcat and Kafka CLI metadata/API probes; broader Java/librdkafka/Go/Python versioned suites remain. |

## Capability Matrix

| Area | Current status | Required to call complete |
| --- | --- | --- |
| Kafka ApiVersions/version catalog | In progress. Canonical tables now drive broker and controller advertised APIs and version checks, including AutoMQ extensions. Source-level tests audit generated schema counts and the actual broker/controller dispatch switches against the catalogs; manual v3+ ApiVersions responses now encode/decode KRaft feature metadata tagged fields and reject duplicate known feature tags; `test-client-matrix` provides a gated real-client compatibility hook. | Keep expanding golden fixtures and broaden client compatibility runs. |
| Kafka broker APIs | Partial. 79 advertised APIs; many handlers are simplified single-node semantics. Auto-created topics and CreateTopics write topic snapshots before exposing topic creation and roll back local topic/partition visibility when the shared snapshot write fails; CreateTopics also applies supported numeric configs at creation time and rejects manual assignments outside current single-node semantics. CreatePartitions likewise rejects manual assignments outside current single-node semantics, writes topic snapshots before acknowledging partition-count expansion, and rolls back local partition visibility when the shared snapshot write fails; DeleteTopics writes topic snapshots before acknowledging deletion, defers local partition cleanup until the shared snapshot succeeds, and rolls back topic visibility when the shared snapshot write fails. ConsumerGroupDescribe, DescribeLogDirs, partition reassignment APIs, AlterClientQuotas, AlterUserScramCredentials, DescribeClientQuotas, DescribeCluster, DescribeQuorum, DescribeUserScramCredentials, ElectLeaders, DescribeProducers, DescribeTopicPartitions, DescribeTransactions, GetTelemetrySubscriptions, ListClientMetricsResources, ListTransactions, PushTelemetry, and UpdateFeatures now decode generated requests and return request-scoped generated responses instead of blanket hand-encoded results. | Full schema-valid decode/encode and Kafka-compatible semantics for every advertised version. |
| Kafka generated schemas | Broad. 110 request schemas generated. Default-value serialize/deserialize/calcSize smoke tests now cover all 230 top-level request/response/header/record types across common protocol versions 0-20. Representative non-default golden wire fixtures now cover generated request headers plus MetadataRequest, FetchRequest v17 tagged `ClusterId`/`ReplicaState`/`ReplicaDirectoryId`, ProduceRequest, ProduceResponse v10 and FetchResponse v16 KIP-951 tagged `CurrentLeader`/`NodeEndpoints` metadata, Vote/BeginQuorumEpoch/EndQuorumEpoch v1 KIP-853 tagged `NodeEndpoints`, FetchSnapshot request/response v1 KIP-853 tagged `ClusterId`/`ReplicaDirectoryId`/`CurrentLeader`/`NodeEndpoints`, BrokerHeartbeatRequest v1 tagged `OfflineLogDirs`, CreateTopicsResponse v5+ tagged `TopicConfigErrorCode`, UpdateMetadataRequest v8 tagged `Type` plus legacy upper-bound fields, ApiVersionsResponse, and CreateTopicsRequest across legacy and first-flexible encodings. | Extend non-default field fixtures, min/max/first-flexible semantic vectors, and golden wire fixtures to every generated request/response pair. |
| AutoMQ extension APIs | Implemented locally with quorum-backed metadata starting. Keys 501-519 and 600-602 dispatch through generated schemas; stream/object mutations can be backed by committed Raft snapshot records; KV/node/router/license/node-id/group mutations can be backed by committed Raft records; attached non-leaders fail closed for these metadata mutations; leaders wait for quorum commit before acknowledging attached multi-node metadata mutations; combined controller+broker failover now verifies KV put/get/delete, zone router, node registry, license, node-id allocator, group promote/demote, stream create/prepare/commit/open/close/trim/delete, stream-set object commit, manifest stream/group-count probes, partition-snapshot protocol smoke, and stream metadata survive leader kill, replacement-leader mutation, and old-leader restart. Topic-backed partition snapshot content is covered in the default Zig suite. | Finish broader client compatibility fixtures. |
| S3 WAL | Partial. Sync durability path exists and failed uploads are not acknowledged. Filesystem WAL produces now fsync before ack, advance HW on durable write, and replay after local broker restart. Flushed S3 WAL objects can rebuild stream-set metadata idempotently when the local object snapshot is missing, including paginated and XML-escaped ListObjectsV2 responses. S3 WAL recovery now fails closed on unreadable WAL objects instead of silently skipping them. S3 WAL object upload has bounded retry for transient put failures, including injected MockS3 failures; failed sync S3 WAL produces do not advance offsets/HW/cache, producer sequence state, or retain duplicate pending entries; a replacement local store or broker can rebuild and fetch acknowledged S3 WAL data from object storage, seed the WAL object counter from existing objects, rebuild idempotent producer sequence state from recovered record batches, replay topic metadata/config and ongoing partition reassignment snapshots from recovered `__cluster_metadata` objects, replay committed offsets and consumer group lifecycle snapshots from recovered `__consumer_offsets` objects, replay transaction coordinator snapshots from recovered `__transaction_state` objects, and resume producing without overwriting old WAL keys; S3Storage propagates read/list/range/delete faults; recovery retries after transient get/list failures and temporary list omissions; range fetches return exactly the requested byte window or fail closed; fetch returns storage errors for unreadable indexed S3 objects; mixed hot-cache/cold-S3 fetches do not drop the S3 prefix when a restarted broker has only the tail in LogCache; overlapping S3 WAL offset ranges now fail recovery instead of returning duplicate/conflicting data; malformed object indexes fail cleanly; interleaved stream-set objects fetch only the requested stream blocks; S3 block-cache keys include the exact visible fetch window; partition offsets are repaired from recovered stream metadata; multipart upload rejects missing or malformed part ETags, aborts failed uploads, and fails closed on non-2xx or embedded XML complete errors. Gated MinIO/S3 test steps validate live object round-trip, multipart round-trip, WAL rebuild/fetch, PartitionStore S3 WAL produce/rebuild/resume, and real broker-process kill/replacement recovery including committed offsets. S3 signing has deterministic coverage for AWS-style default-port Host canonicalization. | Broader live provider matrix, remaining provider-specific multipart faults, and full fencing. |
| S3Stream object lifecycle | Improved. Create/open/close/delete/trim/describe plus prepare/commit SO/SSO are wired to ObjectManager; object/prepared snapshots and partition offset/HW/LSO state are persisted locally with fsync and covered by broker restart tests. PrepareS3Object now honors request TTL for registry-only allocations and expires stale prepared IDs. | Match full AutoMQ recovery, fencing, quorum-backed object-state replay, and S3-backed metadata durability. |
| Controller/KRaft | Partial. Local Raft/controller scaffolding exists. Controller ApiVersions and quorum/lifecycle RPC framing now use generated schemas for ApiVersions, Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat, UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter, and RemoveRaftVoter; telemetry keys 71/72 are no longer treated as voter APIs. Controller FetchSnapshot advertises key 59 v0-v1, decodes generated flexible requests, rejects malformed frames with `invalid_request`, and returns request-scoped `snapshot_not_found` until real snapshot transfer is implemented. Controller UnregisterBroker advertises key 64 v0, decodes generated flexible requests, rejects malformed frames, removes registered brokers, and writes replayable broker-unregistration metadata records. ControllerRegistration now advertises key 70 v0, decodes generated flexible requests, rejects malformed frames, fails closed on followers or unknown controller IDs, and accepts configured voter controllers. UpdateRaftVoter now has a non-advertised generated handler for key 82 that validates/decode requests and returns generated fail-closed errors until endpoint update semantics are implemented. Controller tests now run in the default Zig suite and cover malformed Vote/ControllerRegistration/AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter plus internal AppendEntries compatibility. A gated controller/broker failover plus rolling-restart harness exists, restarts the killed old leader from a fresh local data dir, and broker-only nodes start fenced until controller heartbeat succeeds. Broker registrations, broker unregistrations, and producer-id allocation cursors are now stored as controller metadata records, replayed from persisted Raft logs on follower promotion/restart, and compacted through full controller snapshot records before Raft truncation. | Add broader broker+controller failover gates. |
| Stateless brokers | Partial. Combined AutoMQ metadata can rejoin from a fresh local data dir through quorum replay, local/S3 object repair covers data-path replacement, topic IDs, partition counts, supported configs, and ongoing partition reassignment snapshots can rebuild from `__cluster_metadata` S3 WAL records, committed offsets, OffsetDelete/DeleteGroups tombstones, and consumer group lifecycle snapshots can rebuild from `__consumer_offsets` S3 WAL records, transaction coordinator snapshots plus atomic EndTxn/WriteTxnMarkers commit/abort marker/snapshot objects can rebuild from `__transaction_state` plus data S3 WAL records after fresh-dir broker replacement, idempotent producer sequence state can rebuild from durable record batches after S3 WAL replacement, and client-facing coordinator/reassignment mutations now return storage errors when their shared-storage snapshots cannot be written. Local cache/state still has single-node assumptions for broader broker-only metadata. | Rebuild all broker state from shared storage/controller metadata without data loss or manual repair. |
| Reassignment/autobalancing | Partial. Generated reassignment handlers now retain, list, cancel, locally persist ongoing reassignment state across broker restart, replay it from `__cluster_metadata` S3 WAL after fresh-dir broker replacement, fail closed and roll back local visibility when the shared snapshot write fails, and clear stale entries when a topic is deleted; auto-balancer planning can prefer less-loaded targets in a different rack when topology is available, ignores stale unknown-node load samples, clamps negative metric rates to zero, and has deterministic simulated convergence coverage. | Real partition movement semantics, live convergence tests, and controller-backed load/rack-aware placement. |
| Consumer groups/transactions | Partial. Core flows exist with simplified persistence; OffsetCommit now reports per-partition commit failures instead of acknowledging failed coordinator mutations, writes versioned internal `__consumer_offsets` records before acknowledging, validates managed member identity/generation, rejects empty group IDs, preserves committed leader epoch and metadata through OffsetFetch and restart, rejects oversized normal and transactional offset commit metadata, and rejects unknown topic-partitions without committing offsets, OffsetFetch reports missing groups at version-appropriate partition/top/group error levels, OffsetDelete writes `__consumer_offsets` tombstones before deleting local offsets, replays those tombstones during S3 WAL replacement, fails closed on tombstone write failure, flushes offset mutations, reports missing groups, rejects empty group IDs, rejects subscribed group topics without removing offsets, and reports unknown topic-partitions without deleting stale offsets, DeleteGroups writes tombstones for all group offsets before local group deletion, fails closed on tombstone write failure, rolls back local group visibility on lifecycle snapshot write failure, flushes offset mutations, and returns protocol `ErrorCode` values for invalid, missing, and non-empty group cases, TxnOffsetCommit now requires valid transactional identity/epoch plus prior AddOffsetsToTxn registration, validates managed group member identity/generation, rejects unknown topic-partitions without committing offsets, writes versioned internal offset records before acknowledging, and flushes successful offset mutations before committing, broker open can replay committed offsets from recovered S3 WAL `__consumer_offsets` objects, JoinGroup/SyncGroup/LeaveGroup/DeleteGroups now flush local group lifecycle snapshots, JoinGroup/LeaveGroup/SyncGroup restore the previous local group snapshot when shared lifecycle snapshot writes fail, consumer group lifecycle snapshots are also written to `__consumer_offsets` and replayed from recovered S3 WAL, transaction coordinator snapshots are written to `__transaction_state` and replayed from recovered S3 WAL, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and WriteTxnMarkers restore the previous transaction snapshot when their coordinator mutations cannot be written to shared storage, EndTxn, WriteTxnMarkers, and timed-out transaction aborts in S3 WAL mode now flush marker control batches and the updated transaction snapshot in one shared WAL object before advancing local state, JoinGroup, SyncGroup, LeaveGroup, DeleteGroups, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and WriteTxnMarkers now return storage errors instead of successful acknowledgements when their coordinator snapshot S3 WAL write fails, JoinGroup stores selected protocol metadata, applies and persists session/rebalance timeouts, broker tick enforces each group's configured session timeout, persists group/member protocol metadata across restart, DescribeGroups reports stored group protocol and member metadata/assignments, ConsumerGroupDescribe advertises key 69 v0 and returns generated read-only state/member/subscription views over the existing group coordinator, ConsumerGroupHeartbeat remains non-advertised but returns generated fail-closed responses for direct KIP-848 probes, Heartbeat/SyncGroup/LeaveGroup use protocol error codes for missing members/groups and static-member fencing, and rejects incompatible protocol joins, SyncGroup v5 validates protocol type/name against group state, LeaveGroup v3+ can resolve static members by `group_instance_id`, AddPartitionsToTxn v4+ and EndTxn validate transactional identity/epoch before mutating state, AddPartitionsToTxn/EndTxn/WriteTxnMarkers fail closed for unknown topic-partitions, WriteTxnMarkers validates local producer epoch/state before writing local markers and skips local completion after any marker partition error, AddOffsetsToTxn validates transactional identity/epoch and the internal offsets partition before registering it, DescribeTransactions reports transaction state, PID/epoch, timeout/start time, grouped partitions, and missing transactional IDs through generated schemas, ListTransactions applies state, producer ID, and duration filters and reports unknown state filters, transaction introspection is covered after local broker restart, timed-out transactions now write abort control markers before coordinator completion, transaction coordinator errors now use protocol `ErrorCode` values for invalid PID/epoch/state paths, InitProducerId validates v3+ requested producer id/epoch recovery before bumping, applies transactional timeouts, rejects invalid transactional timeouts, flushes producer allocations, transaction snapshots retain registered partitions, and idempotent producer sequence updates now advance only after durable Produce success, persist the last sequence in a batch, and rebuild from durable log batches after S3 WAL replacement. | Kafka-compatible rebalances, offset lifecycle, transactions, fencing, and coordinator failover. |
| Security | Partial. TLS, SASL, OAuth, SCRAM, and ACL pieces exist. Advertised broker APIs now have default-suite ACL resource/operation mapping coverage except pre-auth SASL frames. Generated response-shape coverage now includes ACL admin, selected cluster/transaction introspection, simple transaction coordinator, topic-scoped ListOffsets/DeleteRecords/OffsetForLeaderEpoch/DescribeProducers, group-introspection ListGroups/DescribeGroups/ConsumerGroupDescribe, coordinator/session FindCoordinator/JoinGroup/Heartbeat/LeaveGroup/SyncGroup, and offset OffsetCommit/OffsetFetch authorization denials, and unsupported-version rejection now runs before ACL denial response construction. | Interop suites, negative tests, cert rotation, and API-specific authorization response-shape coverage for the remaining advertised APIs. |
| Observability | Partial. Metrics and JSON logging exist; `/health` and `/ready` have shared plain/TLS routing, exact-path tests, and startup/shutdown readiness response coverage. Prometheus HELP/label escaping and labeled-metric arity checks are covered in the default Zig suite, and broker metric registration is pinned to the broker API metric catalog. | Complete metric compatibility, dashboards, and alertable SLOs. |
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
  tests. Generated default-message round-trip tests now exercise serialize,
  deserialize, and calcSize for all 230 generated top-level messages across
  common protocol versions 0-20. DescribeLogDirs now uses generated
  request/response schemas across
  legacy and flexible versions, rejects malformed frames, and scopes results to
  requested topics/partitions. AlterPartitionReassignments and
  ListPartitionReassignments now decode generated requests, reject malformed
  frames, and return generated single-node responses instead of manual compact
  writes. DescribeCluster now decodes generated requests, rejects malformed
  frames, and returns generated endpoint-scoped single-node metadata.
  DescribeQuorum now decodes generated requests, returns generated quorum or
  not-controller responses, and scopes metadata-partition errors to the request.
  FetchSnapshot now advertises controller key 59 v0-v1, decodes generated
  flexible requests, rejects malformed frames, and returns request-scoped
  `snapshot_not_found` until snapshot transfer is implemented.
  UnregisterBroker now advertises controller key 64 v0, decodes generated
  flexible requests, rejects malformed frames, removes registered brokers, and
  appends replayable broker-unregistration controller metadata records.
  ControllerRegistration now advertises controller key 70 v0, decodes generated
  flexible requests, rejects malformed frames, fails closed on followers or
  unknown controller IDs, and accepts configured voter controllers.
  UpdateRaftVoter now has a non-advertised generated handler for key 82 that
  validates and decodes direct requests, rejects malformed frames, and returns a
  generated fail-closed error until endpoint update semantics are implemented.
  Advertised controller APIs now reject `max_version + 1` before body decode,
  matching the broker-side catalog enforcement.
  AddRaftVoter and RemoveRaftVoter now validate flexible request frames before
  generated decode so truncated fixed-width fields return `invalid_request`
  instead of relying on unchecked generated reads.
  ElectLeaders now uses generated request/response schemas and returns requested
  topic-partition results with per-partition errors under single-node semantics.
  DescribeProducers now uses generated request/response schemas, rejects
  malformed frames, and returns only requested topic-partition results with
  unknown topic/partition errors under single-node semantics.
  DescribeTopicPartitions now advertises key 75 v0, decodes generated requests,
  rejects malformed frames, returns generated single-node partition metadata,
  reports unknown topics, and honors the response partition cursor.
  ConsumerGroupDescribe now advertises key 69 v0, decodes generated flexible
  requests, rejects malformed frames, and reports read-only group state, members,
  subscriptions, and missing-group errors from the existing group coordinator.
  ConsumerGroupHeartbeat key 68 remains non-advertised until KIP-848 coordinator
  semantics exist, but direct requests now validate/decode generated flexible
  frames and return generated fail-closed responses.
  AssignReplicasToDirs key 73 remains non-advertised until JBOD directory
  semantics exist, but direct requests now validate/decode generated flexible
  frames and return generated fail-closed responses.
  DescribeAcls/CreateAcls/DeleteAcls now use generated schemas, reject malformed
  frames, validate enum fields, write full ACL snapshots to `__cluster_metadata`
  for broker replacement replay, fail closed and roll back local ACL visibility
  when shared snapshot writes fail, and return generated ACL resources/results.
  AlterConfigs/IncrementalAlterConfigs now validate generated topic config
  resources against temporary configs before mutating local topic state, write
  full topic snapshots to `__cluster_metadata` before acknowledging successful
  mutations, preserve validate-only as a no-write path, and fail closed with
  rollback when the shared snapshot write fails.
  DescribeClientQuotas now advertises key 48 v0-v1, decodes generated
  legacy/flexible requests, rejects malformed and semantically invalid filters,
  and returns generated QuotaManager-backed per-client quota entries.
  AlterClientQuotas now advertises key 49 v0-v1, decodes generated
  legacy/flexible requests, validates quota entities and keys, supports
  validate-only, mutates/removes QuotaManager-backed client/default quotas,
  writes full quota snapshots to `__cluster_metadata` for broker replacement
  replay, and fails closed without leaving local quota state visible when the
  shared snapshot write fails. Default produce/fetch quotas are now enforced per
  client for clients without explicit overrides, and partial client quota
  overrides fall back to default limits for unset keys.
  DescribeUserScramCredentials now advertises key 50 v0, decodes generated
  flexible requests, rejects malformed frames, describes requested or all
  SCRAM-SHA-256 users, reports missing users per result, and preserves the
  nullable `Users` wire encoding while keeping null and empty as describe-all.
  AlterUserScramCredentials now advertises key 51 v0, decodes generated
  flexible requests, upserts/removes precomputed SCRAM-SHA-256 credentials,
  rejects unsupported mechanisms, exposes mutations through Describe, writes
  full credential snapshots to `__cluster_metadata` for broker replacement
  replay, and rolls back local credential visibility when the shared snapshot
  write fails.
  ListClientMetricsResources now advertises key 74 v0, decodes generated
  flexible requests, rejects malformed frames, and returns an empty generated
  resource list until client telemetry resources are implemented.
  UpdateFeatures now advertises key 57 v0-v1, decodes generated flexible
  requests with correct v1 field gating, rejects malformed frames and invalid
  upgrade types, and fails closed per requested feature until finalized feature
  metadata is implemented.
  GetTelemetrySubscriptions and PushTelemetry now advertise keys 71/72 v0,
  decode generated flexible requests, reject malformed frames, return an empty
  telemetry subscription set, and reject unsolicited pushes with
  `unknown_subscription_id` until client telemetry collection is implemented.
  MetadataRequest generated decoding now preserves nullable `Topics` semantics:
  v1+ null requests all topics, explicit empty arrays request no topic results,
  and v0 null topics are rejected as malformed.
  OffsetFetchRequest generated round-trips now preserve nullable legacy and
  grouped `Topics` arrays instead of collapsing fetch-all requests to empty
  topic lists.
  DescribeConfigsRequest now preserves nullable `ConfigurationKeys` so null
  requests all configs while explicit empty arrays request no config entries.
  DescribeDelegationTokenRequest now preserves nullable `Owners` so null
  describe-all filters and explicit empty owner filters no longer collapse.
  DescribeLogDirsRequest now preserves nullable `Topics` so null lists all
  topic log dirs while explicit empty arrays return no topic entries.
  ElectLeadersRequest now preserves nullable `TopicPartitions` so null elects
  across all known topics while explicit empty arrays return no topic results.
  CreatePartitionsRequest now preserves nullable `Assignments` so null uses
  automatic partition assignment while explicit empty assignment lists fail.
  AlterPartitionReassignmentsRequest now preserves nullable `Replicas` so null
  cancels pending reassignments while explicit empty assignments fail closed.
  ListPartitionReassignmentsRequest now preserves nullable `Topics` so null
  lists all ongoing reassignments while explicit empty filters return none.
  ConsumerGroupHeartbeatRequest and ShareGroupHeartbeatRequest now preserve
  nullable subscription arrays so null heartbeats mean unchanged subscriptions
  while explicit empty arrays mean an empty subscription set.
  FetchResponse now preserves nullable `AbortedTransactions` so
  read-uncommitted responses can encode null while read-committed responses can
  encode an explicit empty transaction list.
  CreateTopicsResponse now preserves nullable `Configs` so responses that do
  not return topic configs encode null instead of an explicit empty config list.
  DescribeClientQuotasResponse now preserves nullable `Entries` so top-level
  errors can encode null while successful empty matches encode an explicit
  empty result list.
  AutomqGetPartitionSnapshotResponse now preserves nullable partition
  `StreamMetadata` so absent stream deltas stay distinct from explicit empty
  stream metadata lists.
  DescribeTopicPartitionsResponse now preserves nullable
  `EligibleLeaderReplicas` and `LastKnownElr` so unknown ELR state remains
  distinct from explicit empty ELR state.
  `codegen_v2.py` now emits nullable arrays as optional slices with null-length
  encode/decode and size calculation so future regeneration preserves this
  wire distinction. Source-level generated-schema audits pin all current
  nullable-array fields and modeled tagged fields so regeneration drift fails
  the default suite.
  Generated ApiVersionsResponse now also models v3+ feature metadata tagged
  fields and preserves unknown top-level response tags in non-default golden
  round-trips.
  StopReplicaRequest generated encoding now honors its legacy field version
  bounds exactly: v0 `UngroupedPartitions`, v0-v2 `DeletePartitions`, v1-v2
  grouped `Topics`, and v3+ `TopicStates` have non-default golden fixtures.
  AlterPartitionRequest generated encoding now honors v0-v1 topic-name fields,
  v2+ topic IDs, and the v3 switch from `NewIsr` to `NewIsrWithEpochs`.
  LeaderAndIsrRequest generated encoding now honors v0-v1 ungrouped partition
  states and omits per-partition topic names from v2+ grouped-topic frames.
  LeaderAndIsrResponse generated encoding now switches from v0-v4 top-level
  topic-name partition errors to v5+ topic-id grouped partition errors.
  DescribeConfigsResponse generated encoding now keeps `IsDefault` v0-only so
  v1+ config entries align `ConfigSource`, sensitivity, and synonym fields.
  EndQuorumEpochRequest generated encoding now keeps v0-only
  `PreferredSuccessors` out of v1 flexible frames and uses v1+
  `PreferredCandidates` instead.

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
  fencing. Attached multi-node leaders now wait for quorum commit before
  acknowledging these AutoMQ metadata/object mutations. Single-node leaders now
  compact these records by appending a full AutoMQ metadata/ObjectManager
  snapshot record before Raft truncation. Internal AppendEntries now carries
  actual log entry bytes and followers apply appended AutoMQ metadata and object
  snapshot records, including follower-promotion replay coverage. Gated
  `test-kraft-failover` now starts an additional
  combined controller+broker quorum and verifies AutoMQ KV put/get/delete, zone
  router metadata, node registry, license, node-id allocator, group
  promote/demote, stream
  create/prepare/commit/open/close/trim/delete metadata, stream-set object
  commit, manifest stream/group-count probes, and partition-snapshot protocol
  smoke survive leader kill, replacement-leader mutation, and old-leader
  restart. Topic-backed partition snapshot content is covered in the default
  Zig suite.
  Remaining gap: broaden client compatibility fixtures.
- Prepared-object lifecycle coverage includes request-TTL tracking, registry-only
  allocation expiry, prepared/committed/destroyed state transitions, and
  compaction cleanup paths.

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
  malformed part ETags before completion; topic configs are now included in
  local topic metadata snapshots, and topic IDs, partition counts, and supported
  configs are now written to `__cluster_metadata` and replayed from recovered S3
  WAL during fresh-dir replacement; AlterConfigs/IncrementalAlterConfigs now
  roll back topic config visibility and return storage errors when that shared
  snapshot cannot be written; auto-created topics now fail closed and roll back
  local visibility when their shared topic snapshot cannot be written, and
  CreateTopics/CreatePartitions/DeleteTopics return storage errors for the same
  shared-snapshot failure; local metadata snapshots now fsync before save calls
  return; filesystem WAL now implements its periodic fsync policy in addition
  to explicit/every-record/every-N-record barriers; MockS3 fault injection now
  covers bounded put retry,
  propagated get/list/range/delete failures, temporary list omission, recovery
  retry, fetch storage errors, and compaction orphan retry; restarted S3 WAL
  writers now seed their object counters from existing WAL keys before accepting
  writes, and broker replacement coverage verifies recovered S3 data plus new
  hot-cache data fetch as a single range without overwriting prior WAL objects;
  overlapping S3 WAL offset ranges now fail recovery instead of surfacing
  duplicate or conflicting records from stale writers;
  consumer group lifecycle snapshots now persist active group membership,
  generation, leader, assignments, protocol metadata, and group timeouts across
  local broker restart, the same lifecycle snapshot is written to
  `__consumer_offsets` and replayed from recovered S3 WAL during fresh-dir
  replacement, transaction coordinator snapshots are written to
  `__transaction_state` and replayed from recovered S3 WAL during fresh-dir
  replacement, client-facing coordinator mutations now fail closed when these
  snapshot writes fail, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn,
  EndTxn, and WriteTxnMarkers now restore the previous transaction snapshot
  when their coordinator mutations cannot be written to shared storage, and
  EndTxn, WriteTxnMarkers, and timed-out transaction aborts in S3 WAL mode now
  flush marker control batches and the updated transaction snapshot in one
  shared WAL object before advancing local state; broker tick now enforces each
  group's configured session timeout instead of a global timeout;
  S3Client multipart fault tests now cover missing/malformed/XML-unsafe part ETags,
  abort-after-part failure, abort-after-complete failure, and HTTP 200
  CompleteMultipartUpload responses carrying embedded XML errors; a gated
  `test-minio` build step now covers live object round-trip, multipart
  round-trip, S3 WAL metadata rebuild plus fetch, and PartitionStore S3 WAL
  produce/rebuild/resume against MinIO/S3; OffsetCommit and TxnOffsetCommit now
  write versioned `__consumer_offsets` records before acknowledging and broker
  open replays committed offsets from recovered S3 WAL objects; OffsetDelete
  and DeleteGroups now write `__consumer_offsets` tombstones before local
  removal, replay those tombstones during fresh-dir replacement, and preserve
  local offsets/groups when the shared tombstone write fails; DeleteGroups,
  JoinGroup, LeaveGroup, and SyncGroup now restore the previous local group
  snapshot when their shared lifecycle snapshot write fails; client quota
  configuration snapshots are now appended to `__cluster_metadata`, replayed
  from recovered S3 WAL on fresh-dir broker replacement, and rolled back on
  failed snapshot writes before AlterClientQuotas is acknowledged; SCRAM
  credential snapshots are likewise appended to `__cluster_metadata`, replayed
  during broker replacement, and rolled back before AlterUserScramCredentials is
  acknowledged when the snapshot write fails; ACL snapshots are appended to
  `__cluster_metadata`, replayed during broker replacement, and rolled back
  before CreateAcls/DeleteAcls are acknowledged when the snapshot write fails;
  atomic EndTxn and WriteTxnMarkers S3 WAL objects now restore both commit/abort
  marker partition offsets and completed transaction snapshots after fresh-dir
  replacement;
  `test-s3-process-crash`
  adds a gated real broker-process kill/replacement harness against MinIO/S3 and
  now verifies both data and committed offsets after a fresh local data dir
  replacement. S3 request signing now omits default HTTP/HTTPS ports from the
  canonical Host header for AWS-style providers and preserves explicit custom
  ports. Remaining durability work is broader live provider matrix coverage,
  provider-specific multipart faults, and crash/fault recovery against
  quorum/controller metadata. S3 range reads now validate `206 Content-Range`
  windows in addition to response body length, with high-level response tests
  for missing and mismatched range headers, so provider range mismatches fail
  closed instead of returning wrong bytes. Multipart init and complete response
  handling now decodes chunked HTTP bodies before parsing XML, including split
  `UploadId` tags and split embedded complete errors.

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
  DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat,
  UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter,
  RemoveRaftVoter, and non-advertised UpdateRaftVoter fail-closed handling, and
  keys 71/72 remain telemetry-only/unsupported on the controller port. A gated
  `test-kraft-failover` step covers three controller-only processes, a
  broker-only process, controller leader discovery, broker produce before
  failover, leader kill, replacement leader convergence, controller rolling
  restart, killed old-leader restart/rejoin, broker rolling restart, broker
  produce after each transition, and broker fetch/read-after-transition checks
  for all acknowledged records. The same gate also starts a three-node combined
  controller+broker quorum and verifies AutoMQ KV put/get/delete, zone router,
  node registry, license, node-id allocator, group promote/demote, stream
  create/prepare/commit/open/close/trim/delete metadata, stream-set object
  commit, manifest stream/group-count probes, and partition-snapshot protocol
  smoke survive controller leader kill, replacement-leader mutation, and
  old-leader restart/rejoin; broker-side AutoMQ AppendEntries now applies
  committed metadata/object records immediately and the node-id allocator waits
  for post-commit propagation before acknowledging.
  Broker registration/unregistration state and producer-id allocation cursors
  now have Raft-backed controller metadata records; followers persist replicated
  AppendEntries and promoted or restarted controllers replay registered brokers
  and PID cursors before serving lifecycle APIs. Local failover ownership
  metadata now tracks broker topic create/delete/create-partition/restore paths
  and moves tracked partitions from timed-out fenced brokers to the surviving
  broker instead of treating reassignment as a no-op. Controller metadata full
  snapshot records now preserve broker/PID state across Raft log compaction.
  Remaining gap: broader failover gates and client compatibility fixtures.
- Validate rack-aware routing and auto-balancer decisions under load. Status:
  rack-aware planning has unit coverage for cross-rack target preference,
  same-rack fallback, stale unknown-node metric filtering, non-negative metric
  normalization, and simulated post-plan load convergence; reassignment APIs can
  retain/list/cancel ongoing in-memory state. Controller-backed reassignment
  execution and live load/scale gates remain.

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

# AutoMQ Parity Roadmap

This document defines what "AutoMQ-complete" means for ZMQ and turns it into
tracked implementation gates. The target is open-source AutoMQ compatibility:
Kafka protocol behavior, AutoMQ S3Stream storage semantics, S3-backed durability,
stateless broker operation, cluster management, balancing, observability, and
operator-facing behavior.

## Current Baseline

- Zig toolchain target: Zig 0.16.0.
- Generated protocol request schemas: 110 entries in `src/protocol/api_support.zig`.
- Broker-advertised APIs: 90 entries in `api_support.broker_supported_apis`.
- AutoMQ extension API keys 501-519 and 600-602 are broker-dispatched and
  advertised. Stream/object APIs have `ObjectManager` side effects; KV,
  node, router, license, node-id allocation, and group-link mutations append
  committed Raft metadata records when a Raft state is attached. Stream/object
  metadata mutations append committed Raft snapshot records for attached
  single-node leaders. Single-node leaders can compact AutoMQ metadata/object
  records into a replayable full snapshot before Raft log truncation. Attached
  non-leaders reject these metadata mutations, and local snapshot compatibility
  remains for single-node mode. Manifest and partition snapshot APIs are
  read-side views. BrokerRegistration v2 is controller-advertised for broker
  log-directory identity, and registered directory IDs are persisted/replayed
  through controller Raft metadata records and full snapshots.
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
  Latest default-suite protocol tranche: generated AddPartitionsToTxn v3/v4,
  AssignReplicasToDirs v0, ShareFetch/ShareAcknowledge v0,
  Initialize/Read/WriteShareGroupState v0, DescribeTransactions v0, and
  ListTransactions v1 request/response golden fixtures now pin schema-shape
  transitions, nested compact arrays, UUID fields, share acknowledgement/state
  batches, compact record bytes, transaction filters/results, flexible
  encodings, and tagged-field terminators.

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
| P0 | Advertised API audit | ApiVersions, request/response header versions, generated schema ranges, dispatch coverage, malformed-frame tests, golden fixtures, and client compatibility matrix all fail closed when an advertised API drifts. | In progress. Header flexible-version and handler-switch coverage are explicit and tested; source-level tests now parse `generated_index.zig` and the real broker/controller `handleRequest` switches so generated schema counts and dispatch cases cannot drift from the audit tables silently. Source-level generated-schema audits now pin all 20 nullable-array fields to optional-slice null/empty preservation paths and all 23 currently modeled tagged fields to schema-visible encode/decode/duplicate-tag coverage, so protocol regeneration drift fails the default Zig suite. Default generated-message round-trip smoke tests now cover all 230 top-level generated request/response/header/record types across common protocol versions 0-20, and non-default golden wire fixtures now cover legacy and flexible encodings for generated request headers, MetadataRequest, FetchRequest v17 tagged `ClusterId`/`ReplicaState`/`ReplicaDirectoryId`, ProduceRequest, ProduceResponse v10 and FetchResponse v16 KIP-951 tagged leader endpoints/partition metadata, Vote/BeginQuorumEpoch/EndQuorumEpoch v1 KIP-853 tagged `NodeEndpoints`, FetchSnapshot request/response v1 KIP-853 tagged `ClusterId`/`ReplicaDirectoryId`/`CurrentLeader`/`NodeEndpoints`, BrokerHeartbeatRequest v1 tagged `OfflineLogDirs`, CreateTopicsResponse v5+ tagged `TopicConfigErrorCode`, UpdateMetadataRequest v8 tagged `Type` plus legacy upper-bound fields, ApiVersionsResponse, AddPartitionsToTxn v3/v4 request/response shape transition fixtures, AssignReplicasToDirs v0 UUID/nested-array fixtures, ShareFetch/ShareAcknowledge v0 acknowledgement/record/leader-endpoint fixtures, Initialize/Read/WriteShareGroupState v0 state epoch/start-offset/state-batch fixtures, DescribeTransactions v0 request/response fixtures, ListTransactions v1 request/response filters/results, and CreateTopicsRequest including nested compact arrays and tagged-field boundaries. Manual ApiVersions v3+ response encoding now round-trips KRaft feature metadata tagged fields (`SupportedFeatures`, `FinalizedFeaturesEpoch`, `FinalizedFeatures`, `ZkMigrationReady`) and preserves unknown response tags without duplicating known tags after decode. Controller ApiVersions now uses a controller-specific support catalog and advertises generated ControllerRegistration key 70 plus AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter keys 80/81/82 instead of telemetry keys 71/72; UpdateRaftVoter validates generated requests and appends replayable Raft config records for voter endpoint metadata instead of failing closed. ConsumerGroupHeartbeat key 68 is now advertised with generated v0 flexible framing, persisted KIP-848 member epochs, static-member fencing, cooperative range assignment revocation, stored owned-partition echoes, schema-shaped authorization denial, and local/S3 consumer-group snapshot rollback; AssignReplicasToDirs key 73 is now advertised with generated v0 framing, configured logical multi-directory target validation, local/S3 assignment snapshot rollback and replay from `__cluster_metadata`, schema-shaped authorization denial, and DescribeLogDirs partition mirroring across assigned directories; ShareGroupHeartbeat/ShareGroupDescribe keys 76-77, ShareFetch/ShareAcknowledge keys 78-79, and Initialize/Read/Write/DeleteShareGroupState plus ReadShareGroupStateSummary keys 83-87 are now advertised with generated v0 flexible framing, local/S3 durable share-group/session/state snapshots through `__consumer_offsets`, share-session epoch restore after fresh-dir replacement, strict malformed-frame coverage, schema-shaped authorization denial, and rollback on local or shared persistence failures. Advertised broker and controller APIs now have catalog-level `max_version + 1` fail-closed coverage before body decode. ApiVersions catalog fixtures plus AddPartitionsToTxn, AddOffsetsToTxn, AlterClientQuotas, AlterUserScramCredentials, AssignReplicasToDirs, ConsumerGroupDescribe, ConsumerGroupHeartbeat, ControllerRegistration, CreateTopics including validated supported creation-time configs, DeleteTopics, DeleteRecords, DeleteShareGroupState, DescribeClientQuotas, DescribeConfigs, DescribeGroups, DescribeTopicPartitions including cursor pagination, DescribeTransactions, DescribeUserScramCredentials, EndTxn, Fetch, FetchSnapshot, FindCoordinator including invalid coordinator-type, empty-key, and unimplemented share-coordinator negatives, GetTelemetrySubscriptions, Heartbeat, InitProducerId, InitializeShareGroupState, JoinGroup, LeaveGroup, ListClientMetricsResources, ListGroups including v5 group-type filters, ListOffsets, ListTransactions, Metadata, Produce, PushTelemetry, ReadShareGroupState, ReadShareGroupStateSummary, SaslHandshake, SaslAuthenticate, ShareAcknowledge, ShareFetch, ShareGroupDescribe, ShareGroupHeartbeat, SyncGroup, UnregisterBroker, UpdateFeatures, UpdateRaftVoter, OffsetCommit, OffsetFetch, OffsetForLeaderEpoch, WriteShareGroupState, WriteTxnMarkers, TxnOffsetCommit, DescribeAcls/CreateAcls/DeleteAcls, AlterConfigs/IncrementalAlterConfigs, CreatePartitions, Vote, Begin/EndQuorumEpoch, and OffsetDelete generated/malformed coverage are in place. Gated `test-client-matrix` now probes metadata, topic admin where available, produce/fetch, and committed offsets with installed external clients (`kcat`, Kafka CLI, `kafka-python`, `confluent-kafka`, Java `kafka-clients`, and Go `kafka-go`) against a running broker and supports named version profiles with per-profile bootstrap, tool, Java classpath, Go module, Python interpreter, semantic-suite, TLS, SASL/OAuth, and exact version labels. Release jobs can require named profiles, exact version-pinned profiles, secured-client profiles, and negative-security profiles before probes run, and the matrix can require real-client admin, consumer-group, rebalance, transactional, secured-client, bad-SASL, bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce probes in addition to default metadata/produce/fetch/offset checks; required secured/OAuth profiles now fail before execution if selected tools cannot run the requested secured positive/negative fixtures. Broader live client execution remains. |
| P0 | Quorum-backed AutoMQ/controller metadata | Replace local-only AutoMQ KV/node/router/license/manifest/group/object metadata with quorum-backed records, snapshots, replay, fencing, and failover tests. | In progress. KV/node/router/license/node-id/group mutations now append committed Raft records for attached leaders. Local-mode PutKVs/DeleteKVs, AutomqRegisterNode, AutomqZoneRouter, UpdateLicense, GetNextNodeId, and AutomqUpdateGroup now fail closed and roll back visible metadata state when the AutoMQ metadata snapshot cannot be written. ObjectManager stream/object mutations now append committed Raft snapshot records; local-mode Create/Open/Close/Delete/TrimStreams, PrepareS3Object, CommitStreamObject, and CommitStreamSetObject now fail closed and roll back visible ObjectManager state when the local object snapshot cannot be written. Both paths replay on broker open and reject attached non-leader mutations. Single-node leaders now append a full AutoMQ metadata/ObjectManager snapshot record before Raft snapshot truncation and replay it after compaction. Internal AppendEntries now carries log entry bytes; followers apply committed AutoMQ metadata/object snapshot records on AppendEntries and replay them after promotion; leaders also wait briefly for post-commit propagation so a promoted follower does not reuse a stale node-id allocator cursor. Combined controller+broker multi-process failover now covers AutoMQ KV put/get/delete, zone router, node registry, license, node-id allocator, group promote/demote, stream create/prepare/commit/open/close/trim/delete, stream-set object commit, manifest stream/group-count probes, partition-snapshot protocol smoke, and stream metadata replication, leader kill, replacement-leader mutation, and old-leader restart/rejoin through gated `test-kraft-failover`. The default Zig suite now includes topic-backed partition snapshot fixtures. Broader client compatibility remains. |
| P0 | S3/MinIO crash and fault-injection harness | Exercise produce/flush/restart/fetch/rebuild with transient 5xx, timeout, partial multipart, bad ETag, checksum, range-read, list inconsistency, and provider-specific behavior. | In progress. Local MockS3 fault injection now covers bounded put retry, get/list/range/delete failures, temporary list omission, recovery retry, fetch failure, compaction orphan retry, multipart bad/missing/XML-unsafe part ETags, XML-escaped upload IDs, complete failure, embedded complete errors including chunked responses with split XML tags, exact range-window validation including high-level `206 Content-Range` mismatch/missing-header rejection, SHA-256 checksum-header mismatch rejection for S3 GET/range bodies, abort verification, broker stateless replacement, coordinator snapshot upload failure, and S3 WAL resume without object-key overwrite. Gated `test-minio` covers live object, multipart, S3 WAL rebuild/fetch, and PartitionStore S3 WAL produce/rebuild/resume against MinIO/S3. Gated `test-s3-process-crash` starts a real broker process, kills it after acknowledged S3 WAL produce and OffsetCommit, restarts a replacement with a fresh local data dir, fetches data and committed offsets from S3, and appends new data. Gated `test-s3-provider-matrix` can now run the live MinIO suite across named S3-compatible provider profiles with per-profile endpoint, port, bucket, credential, scheme, region, TLS CA, path-style, existing-bucket, process-crash/replacement overrides, live-S3 outage chaos hooks that verify produce fails closed during provider isolation and recovers after heal, provider-specific multipart-fault commands with the same credential/scheme/region/path-style/TLS context as the selected live provider profile, and release-job validation for required provider, outage, process-crash/replacement, ListObjectsV2 pagination, multipart-edge, and multipart-fault profile coverage. S3 request signing now honors configured regions, virtual-hosted addressing, and default HTTP/HTTPS Host-header port elision while preserving explicit custom ports. Broader live execution of provider-specific multipart fault scripts remains. |
| P1 | Multi-node KRaft and broker lifecycle | Broker registration, heartbeat, fencing/unfencing, controller failover, rolling restart, and leader epoch behavior are covered by three-node tests. | In progress. Controller Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat, UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter, and RemoveRaftVoter now use generated schemas on the controller port; BrokerRegistration now advertises v2 when a broker has local log-directory IDs, persists those IDs in controller metadata records, replays them after controller restart/follower promotion, and preserves them in controller full snapshots; BrokerHeartbeat now advertises v1, records valid offline log-directory reports, rejects zero/unknown/duplicate directory IDs, honors broker-requested fencing/shutdown, and fences brokers whose registered directories are all offline; AddRaftVoter persists voter directory/listener metadata through replayable Raft config records when clients supply endpoints. ControllerRegistration now persists accepted listener and `kraft.version` metadata through the same replayable voter endpoint records. The election loop now reconciles the Raft RPC client pool from committed voter endpoint metadata before votes, heartbeats, and AppendEntries, including endpoint updates and removed voters. Controller startup registers configured voters before replaying persisted logs, and idempotent voter registration preserves replayed endpoint metadata. Controller tests are part of the default Zig test suite. Gated `test-kraft-failover` now starts three controller-only processes plus a broker-only process, can run CI-provided controller/broker network-partition hooks or scheduled `ZMQ_KRAFT_NETWORK_MATRIX` phases before the leader-kill path, kills the discovered controller leader, verifies surviving controllers elect a replacement, restarts the killed old leader with a fresh local data dir under the replacement leader, restarts a surviving controller, restarts the broker with the same data dir, and verifies the broker can produce and fetch acknowledged records throughout. Broker registrations, broker unregistrations, broker rack metadata, log-directory IDs, offline log-directory health, and producer-id allocations now append controller metadata records, followers persist replicated AppendEntries, controller startup replays durable records from Raft, promoted followers can heartbeat brokers without forced re-registration or producer-id reuse, and controller full snapshot records preserve broker/rack/log-dir/PID state across Raft log compaction. Local FailoverController ownership metadata now tracks broker topic create/delete/create-partition/restore paths and transfers tracked partitions from fenced timed-out brokers to the surviving broker in default tests. Broader failover gates remain. |
| P1 | Stateless broker replacement | A replacement broker can rebuild state from shared storage and quorum metadata without local disk metadata or manual repair. | In progress. S3/object repair exists, committed offsets are now written to versioned `__consumer_offsets` records and replayed from recovered S3 WAL objects, OffsetDelete and DeleteGroups now write `__consumer_offsets` tombstones before local offset removal and replay them during replacement, consumer group lifecycle snapshots are now written to `__consumer_offsets` and replayed from S3 WAL, transaction coordinator snapshots are now written to `__transaction_state` and replayed from S3 WAL, atomic EndTxn and WriteTxnMarkers S3 WAL objects now restore both commit/abort marker partition offsets and completed transaction snapshots after fresh-dir replacement, client-facing group/transaction mutations fail closed when coordinator snapshot S3 WAL writes or post-marker local checkpoints fail, topic IDs, partition counts, common topic configs (`retention.*`, `max.message.bytes`, `min.insync.replicas`, `segment.bytes`, `cleanup.policy`, `compression.type`), finalized feature snapshots, DeleteRecords/retention low-watermark snapshots, replica-directory assignment snapshots, ongoing partition reassignment snapshots, and CreateTopics/CreatePartitions initial non-local assignment ownership snapshots are now written to `__cluster_metadata` and replayed from recovered S3 WAL, idempotent producer sequence state is rebuilt from durable log batches after S3 WAL replacement, and the combined controller+broker failover gate now restarts the killed AutoMQ leader after deleting its local data dir and verifies it rebuilds quorum-backed AutoMQ metadata from Raft. Broader broker-only metadata replacement remains. |
| P1 | Real reassignment and autobalancing | Partition movement, rack-aware placement, load convergence, and scale in/out semantics are implemented and tested under load. | In progress. Handler compatibility exists for reassignment APIs; CreateTopics and CreatePartitions now accept explicit single-replica broker assignments, install remote owners into failover metadata, persist those non-local initial owners through the existing reassignment snapshot path, replay them after fresh-dir S3 WAL replacement, and fail closed with rollback if assignment metadata cannot be written. AlterPartitionReassignments now tracks ongoing reassignments, applies the target owner into local failover ownership metadata, persists and replays ownership across local broker restart and fresh-dir S3 WAL replacement through `__cluster_metadata`, writes committed topic and reassignment snapshot records through Raft when a quorum is attached, fails closed with `NOT_CONTROLLER` on attached non-leaders and storage errors when shared snapshots cannot be written, ListPartitionReassignments returns requested ongoing state, cancel clears persisted/quorum state and restores local ownership, committed quorum replay restores topic metadata before assignment/cancellation ownership, and Metadata/DescribeTopicPartitions plus Produce/Fetch now respect non-local owners. The auto-balancer has a rack-aware planning path with deterministic coverage for cross-rack target preference and same-rack fallback, ignores stale unknown-node load samples when computing known-node averages, clamps negative metric rates to zero, persists broker rack metadata from controller registrations through Raft records/snapshots, can build controller-aware plans from active unfenced brokers that move load off fenced/scale-in leaders before normal rack-aware balancing, and the broker can execute validated plan moves through the durable reassignment path while rejecting stale or duplicate plans before mutation. Broker-side controller-aware orchestration now computes from controller broker snapshots plus load samples, applies moves through durable reassignments, returns planned/applied counts, fails closed on stale plans without partial mutation, no-ops without active targets, and can run automatically from broker `tick()` when cached controller/load samples are due; default tests cover elapsed-interval execution and interval-based skip behavior. The gated KRaft failover harness now includes a live broker-process reassignment check that creates a topic on the controller leader, alters ownership to another broker id, observes ListPartitionReassignments plus Metadata leader convergence on both old owner and target, verifies old-owner Produce is fenced, and verifies target-broker Produce/Fetch succeeds after topic-metadata quorum replay. Broader live load/scale orchestration and cross-broker chaos coverage remain. |
| P1 | Consumer group and transaction coordinator failover | Rebalances, offset lifecycle, transactions, idempotent producers, fencing, and coordinator migration survive restart and failover. | Partial single-node coordinator behavior. OffsetCommit, TxnOffsetCommit, and OffsetDelete now roll back local committed-offset visibility and return per-partition storage errors when the local offset snapshot cannot be written; OffsetDelete and DeleteGroups now write and replay `__consumer_offsets` tombstones and fail closed on shared tombstone write failure; DeleteGroups, JoinGroup, LeaveGroup, and SyncGroup roll back local group visibility when lifecycle snapshots cannot be written; DeleteGroups also rolls back share-session cleanup when the session snapshot cannot be written. Gated `test-kraft-failover` now validates OffsetCommit/OffsetFetch durability, classic JoinGroup/SyncGroup/Heartbeat continuity, InitProducerId/AddPartitionsToTxn/EndTxn continuity, and idempotent Produce v9 duplicate suppression, next-sequence progress, InitProducerId epoch-bump fencing, and next-epoch recovery through controller leader kill, replacement election, old-leader fresh rejoin, surviving-controller restart, and broker restart. |
| P2 | Security gates | TLS/SASL/OAuth/SCRAM/ACL interop, negative cases, cert rotation, and authz coverage exist for every advertised API. | In progress. TLS/SASL/OAuth/SCRAM/ACL components exist. TLS config now fails closed for unsupported JKS keystore/truststore fields, inverted protocol-version ranges, and mTLS client-auth without CA trust anchors instead of silently starting with incomplete verification. Broker ACL resource/operation mapping is now audited against every advertised broker API except pre-auth SASL handshake/authenticate, missing mappings for DeleteRecords, OffsetForLeaderEpoch, AddOffsetsToTxn, ACL admin APIs, quorum APIs, DescribeCluster, and DescribeProducers are covered in the default Zig suite, Produce/Fetch now use generated per-partition authorization denials with exact flexible-version topic ACL extraction, ApiVersions/Metadata/CreateTopics/DeleteTopics, DescribeConfigs/AlterConfigs/IncrementalAlterConfigs, DescribeLogDirs/CreatePartitions/ElectLeaders/AlterPartitionReassignments/ListPartitionReassignments, DescribeClientQuotas/AlterClientQuotas/DescribeUserScramCredentials/AlterUserScramCredentials, DescribeAcls/CreateAcls/DeleteAcls including local/shared ACL snapshot rollback, quorum Vote/BeginQuorumEpoch/EndQuorumEpoch, DescribeQuorum/DescribeCluster/UpdateFeatures/ListTransactions/DescribeTransactions, telemetry/client-metrics GetTelemetrySubscriptions/PushTelemetry/ListClientMetricsResources, DescribeTopicPartitions, InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/WriteTxnMarkers/TxnOffsetCommit, topic-scoped ListOffsets/DeleteRecords/OffsetForLeaderEpoch/DescribeProducers, group-introspection ListGroups/DescribeGroups/ConsumerGroupDescribe, coordinator/session FindCoordinator/JoinGroup/ConsumerGroupHeartbeat/Heartbeat/LeaveGroup/SyncGroup, offset OffsetCommit/OffsetFetch, group/offset deletion DeleteGroups/OffsetDelete, and all AutoMQ extension keys 501-519/600-602 authorization denials now return generated schema-compatible responses, and unsupported versions are rejected before ACL denial builders can serialize unsupported schemas. TLS contexts now fingerprint configured cert/key/CA PEM files after load and reload the OpenSSL context before accepting new TLS connections when any configured PEM file is rotated or deleted. SASL-enabled brokers now reject non-auth APIs until a client completes SASL authentication while keeping ApiVersions/SaslHandshake/SaslAuthenticate available for negotiation; default negative coverage pins that ACL denial cannot block pre-auth SASL frames, unsupported versions reject before SASL/authz gates, SaslAuthenticate fails closed when no enabled mechanism was negotiated or a negotiated mechanism is later disabled, and OAuthBearer principal extraction frees decoded JWT state with leak-checked missing-subject coverage. The real-client matrix now has configurable bad-SASL, bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce negative vectors for supported secured clients, with deterministic self-test coverage in the default build. Broader OAuth provider interoperability and live secured-client CI profiles remain. |
| P2 | Observability gates | Metrics, structured logs, readiness/liveness, dashboards, and alertable SLOs match the documented operational contract. | In progress. Metrics and JSON logging exist; `/health` and `/ready` now share a deterministic routing/response contract across plain and TLS metrics transports, including exact-path matching plus startup and shutdown readiness transitions in the default Zig suite. Prometheus export now escapes HELP text and label values, rejects invalid labeled-metric arity, and uses canonical label-set keys in registry tests. Broker metric registration is audited against the advertised broker API metric catalog and includes produce/fetch throttle counters so request metrics are not silently dropped. Client-metrics APIs now expose a default resource, retain accepted uncompressed telemetry samples by client instance, drop samples on terminating pushes, list active client resources, update Prometheus counters/gauges for accepted pushes, terminating pushes, retained sample count, and retained sample bytes, and cover unknown subscription, unsupported compression, and oversized metric rejections. Accepted PushTelemetry payloads can now be exported to a configured append-only JSONL sink, export failures fail closed with generated `KAFKA_STORAGE_ERROR` responses before retained telemetry state mutates, and export success/error/byte metrics are registered and referenced by checked-in dashboards/alerts. AutoMQ-compatible Kafka request count/size/time/error metrics, produce/fetch request counters, produce/fetch/request latency histograms, and broker connection/topic/group/partition gauges are exported and pinned by default tests. JMX-compatible request metrics, broker-topic request/byte/error counters, replica-manager partition/leader/offline/reassigning gauges, delayed-operation purgatory gauges, broker-state gauge, request-handler idle gauge, network-processor idle gauge, and active-controller gauges are now registered and emitted from request and broker-state paths. Checked-in Grafana and Prometheus PromQL expressions are now parsed against the registered metric corpus so artifact drift fails the default Zig suite; JMX controller health, replica-manager health, failed produce/fetch, broker-state, idle, delayed-fetch purgatory, broker-topic, and request-metric dashboard/alert fixtures are pinned; compaction lifecycle counters are registered instead of being silently dropped. Broader AutoMQ/JMX metric corpus coverage remains. |
| P2 | Performance and client matrix gates | Repeatable benchmarks and Java/librdkafka/Go/Python/Kafka CLI compatibility suites pass across supported versions. | In progress. `zig build bench` now includes local `PartitionStore` produce/fetch throughput and p99 latency gates, mock S3 WAL sync-produce latency, S3 request-volume reporting, S3 WAL rebuild timing, bounded-cache memory-growth checks, and an opt-in `ZMQ_RUN_BENCH_LIVE_S3=1` live S3 provider object put/get throughput, p99, and requests/MiB trend gate with environment-tunable thresholds. Gated `bench-compare` now exposes the ZMQ/Kafka/AutoMQ comparative benchmark runner as a release command, requires a ZMQ result plus at least one Kafka/AutoMQ baseline when run through the release gate, can require exact target coverage through `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`, and enforces environment-tunable throughput, latency, and error-rate regression thresholds with default-suite self-tests. A gated external-client matrix step exists for kcat, Kafka CLI, kafka-python, confluent-kafka, Java kafka-clients, and Go kafka-go metadata, topic admin, produce/fetch, and committed-offset probes with required profile, exact version-label, secured profile, tool, and semantic validation. Broader live comparative performance runs remain. |

Latest default-suite share data-plane tranche: share sessions and share-partition
state now write combined snapshots to `__consumer_offsets`, replay from S3 WAL
after fresh-dir replacement, skip stale local share files when shared snapshots
exist, and roll back failed shared snapshot writes before exposing session
epochs.

## Capability Matrix

| Area | Current status | Required to call complete |
| --- | --- | --- |
| Kafka ApiVersions/version catalog | In progress. Canonical tables now drive broker and controller advertised APIs and version checks, including AutoMQ extensions. Source-level tests audit generated schema counts and the actual broker/controller dispatch switches against the catalogs; manual v3+ ApiVersions responses now encode/decode KRaft feature metadata tagged fields and reject duplicate known feature tags; `test-client-matrix` provides a gated real-client metadata and produce/fetch compatibility hook for kcat, Kafka CLI, kafka-python, confluent-kafka, Java kafka-clients, and Go kafka-go when installed/configured. | Keep expanding golden fixtures and broaden versioned client compatibility runs. |
| Kafka broker APIs | Partial. 90 advertised APIs; many handlers are simplified single-node semantics. Auto-created topics and CreateTopics write topic snapshots before exposing topic creation and roll back local topic/partition visibility when the shared snapshot write fails; CreateTopics also applies common supported configs at creation time (`retention.ms`, `retention.bytes`, `max.message.bytes`, `min.insync.replicas`, `segment.bytes`, `cleanup.policy`, and `compression.type`), accepts explicit single-replica manual assignments to local or remote broker IDs, persists non-local initial ownership through the partition-reassignment snapshot path, rejects multi-replica assignments until replica-set semantics exist, and rolls back visible topic/partition/ObjectManager/assignment state when local `topics.meta`, assignment metadata, or object snapshots cannot be written after the shared snapshot. Produce now returns per-partition storage errors instead of successful acknowledgements when post-append local partition-state, ObjectManager, or producer-sequence checkpoints cannot be written. CreatePartitions likewise accepts explicit single-replica assignments for newly added partitions, persists non-local ownership, rejects multi-replica assignments, writes topic snapshots before acknowledging partition-count expansion, and rolls back local partition/ObjectManager/assignment visibility when shared or local topic/ObjectManager/assignment snapshots cannot be written. AlterConfigs and IncrementalAlterConfigs apply the same common topic-config set, persist it through local and shared topic snapshots, and roll back topic config visibility when shared or local topic/ObjectManager snapshots cannot be written; DeleteTopics writes topic snapshots before acknowledging deletion, defers local partition, share-state, and replica-directory cleanup until the shared snapshot succeeds, persists side-state cleanup across local restart, rolls back topic visibility without dropping those side states when the shared snapshot write fails, and reports storage errors when post-snapshot local cleanup checkpoints fail. DeleteRecords now writes a `__cluster_metadata` low-watermark snapshot before acknowledging trims, replays that trim after fresh-dir S3 WAL replacement without hiding later acknowledged records, rolls back visible partition start offsets and ObjectManager trim metadata, and returns per-partition storage errors when shared partition-state, local partition-state, or object snapshots cannot be written. Internal metadata-log snapshot writers for `__cluster_metadata`, `__consumer_offsets`, and `__transaction_state` now propagate partition-state checkpoint failures so request handlers fail closed instead of acknowledging records that may not replay after restart. ConsumerGroupHeartbeat, ConsumerGroupDescribe, ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch, ShareAcknowledge, Initialize/Read/Write/DeleteShareGroupState, ReadShareGroupStateSummary, DescribeLogDirs, partition reassignment APIs, AlterClientQuotas, AlterUserScramCredentials, DescribeClientQuotas, DescribeCluster, DescribeQuorum, DescribeUserScramCredentials, ElectLeaders, DescribeProducers, DescribeTopicPartitions, DescribeTransactions, GetTelemetrySubscriptions, ListClientMetricsResources, ListTransactions, PushTelemetry, and UpdateFeatures now decode generated requests and return request-scoped generated responses instead of blanket hand-encoded results. | Full schema-valid decode/encode and Kafka-compatible semantics for every advertised version. |
| Kafka generated schemas | Broad. 110 request schemas generated. Default-value serialize/deserialize/calcSize smoke tests now cover all 230 top-level request/response/header/record types across common protocol versions 0-20. Representative non-default golden wire fixtures now cover generated request headers plus MetadataRequest, FetchRequest v17 tagged `ClusterId`/`ReplicaState`/`ReplicaDirectoryId`, ProduceRequest, ProduceResponse v10 and FetchResponse v16 KIP-951 tagged `CurrentLeader`/`NodeEndpoints` metadata, Vote/BeginQuorumEpoch/EndQuorumEpoch v1 KIP-853 tagged `NodeEndpoints`, FetchSnapshot request/response v1 KIP-853 tagged `ClusterId`/`ReplicaDirectoryId`/`CurrentLeader`/`NodeEndpoints`, BrokerHeartbeatRequest v1 tagged `OfflineLogDirs`, CreateTopicsResponse v5+ tagged `TopicConfigErrorCode`, UpdateMetadataRequest v8 tagged `Type` plus legacy upper-bound fields, ApiVersionsResponse, ListTransactions v1 request/response filters/results, and CreateTopicsRequest across legacy and first-flexible encodings. | Extend non-default field fixtures, min/max/first-flexible semantic vectors, and golden wire fixtures to every generated request/response pair. |
| AutoMQ extension APIs | Implemented locally with quorum-backed metadata starting. Keys 501-519 and 600-602 dispatch through generated schemas; stream/object mutations can be backed by committed Raft snapshot records; KV/node/router/license/node-id/group mutations can be backed by committed Raft records; attached non-leaders fail closed for these metadata mutations; leaders wait for quorum commit before acknowledging attached multi-node metadata mutations; combined controller+broker failover now verifies KV put/get/delete, zone router, node registry, license, node-id allocator, group promote/demote, stream create/prepare/commit/open/close/trim/delete, stream-set object commit, manifest stream/group-count probes, partition-snapshot protocol smoke, and stream metadata survive leader kill, replacement-leader mutation, and old-leader restart. Topic-backed partition snapshot content is covered in the default Zig suite. | Finish broader client compatibility fixtures. |
| S3 WAL | Partial. Sync durability path exists and failed uploads are not acknowledged. Filesystem WAL produces now fsync before ack, advance HW on durable write, and replay after local broker restart. Flushed S3 WAL objects can rebuild stream-set metadata idempotently when the local object snapshot is missing, including paginated and XML-escaped ListObjectsV2 responses. S3 WAL recovery now fails closed on unreadable WAL objects instead of silently skipping them. S3 WAL object upload has bounded retry for transient put failures, including injected MockS3 failures; failed sync S3 WAL produces do not advance offsets/HW/cache, producer sequence state, or retain duplicate pending entries; a replacement local store or broker can rebuild and fetch acknowledged S3 WAL data from object storage, seed the WAL object counter from existing objects, rebuild idempotent producer sequence state from recovered record batches, replay topic metadata/config and ongoing partition reassignment snapshots from recovered `__cluster_metadata` objects, replay committed offsets and consumer group lifecycle snapshots from recovered `__consumer_offsets` objects, replay transaction coordinator snapshots from recovered `__transaction_state` objects, and resume producing without overwriting old WAL keys; S3Storage propagates read/list/range/delete faults; recovery retries after transient get/list failures and temporary list omissions; real-client listing follows continuation pagination and fails closed on truncated ListObjectsV2 pages without continuation tokens; range fetches return exactly the requested byte window or fail closed; fetch returns storage errors for unreadable indexed S3 objects; mixed hot-cache/cold-S3 fetches do not drop the S3 prefix when a restarted broker has only the tail in LogCache; overlapping S3 WAL offset ranges now fail recovery instead of returning duplicate/conflicting data; stale lower-epoch S3 WAL writers now fail closed before upload when newer epoch objects are visible; controller-observed higher KRaft leader epochs, including internal AppendEntries payloads, fence the local S3 WAL writer and reject subsequent produce without advancing offsets/cache/S3 objects in the default Zig suite; malformed object indexes fail cleanly; interleaved stream-set objects fetch only the requested stream blocks; S3 block-cache keys include the exact visible fetch window; partition offsets are repaired from recovered stream metadata; multipart upload rejects missing or malformed part ETags, decodes XML-escaped upload IDs before URI encoding part/complete/abort requests, aborts failed uploads, and fails closed on non-2xx or embedded XML complete errors. Gated MinIO/S3 test steps validate live object round-trip, multipart round-trip, WAL rebuild/fetch, PartitionStore S3 WAL produce/rebuild/resume, and real broker-process kill/replacement recovery including committed offsets. A gated S3 provider matrix wrapper now runs the live MinIO suite across named provider profiles and can fail release jobs when required outage-enabled, process-crash/replacement, ListObjectsV2 pagination, multipart-edge, or multipart-fault profiles are omitted. S3 signing has deterministic coverage for AWS-style default-port Host canonicalization. | Remaining live execution of provider-specific multipart fault scripts and broader controller-integrated fencing. |
| S3Stream object lifecycle | Improved. Create/open/close/delete/trim/describe plus prepare/commit SO/SSO are wired to ObjectManager; object/prepared snapshots and partition offset/HW/LSO state are persisted locally with fsync and covered by broker restart tests. PrepareS3Object now honors request TTL for registry-only allocations and expires stale prepared IDs. | Match full AutoMQ recovery, fencing, quorum-backed object-state replay, and S3-backed metadata durability. |
| Controller/KRaft | Partial. Local Raft/controller scaffolding exists. Controller ApiVersions and quorum/lifecycle RPC framing now use generated schemas for ApiVersions, Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot, BrokerRegistration v0-v2, BrokerHeartbeat v0-v1, UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter, RemoveRaftVoter, and UpdateRaftVoter; telemetry keys 71/72 are no longer treated as voter APIs. Controller FetchSnapshot advertises key 59 v0-v1, decodes generated flexible requests, rejects malformed frames with `invalid_request`, serves compacted controller full-snapshot record bytes with `max_bytes` chunking and `position_out_of_range` bounds checks when a requested `__cluster_metadata` snapshot exists, returns request-scoped `snapshot_not_found` for unavailable snapshots, and includes v1 current-leader/node-endpoint routing metadata from committed voter endpoints. Controller UnregisterBroker advertises key 64 v0, decodes generated flexible requests, rejects malformed frames, removes registered brokers, and writes replayable broker-unregistration metadata records. ControllerRegistration now advertises key 70 v0, decodes generated flexible requests, rejects malformed frames, fails closed on followers, unknown controller IDs, invalid feature ranges, and invalid listeners, accepts configured voter controllers, and appends replayable voter endpoint metadata for accepted listeners and `kraft.version`. BrokerRegistration v2 log-directory IDs are stored as controller metadata records, replayed after restart/follower promotion, and compacted through full controller snapshot records. BrokerHeartbeat v1 accepts tagged offline log-directory reports, validates them against registered directories, persists latest offline health through controller metadata records and full snapshots, and fences fully-offline brokers. AddRaftVoter now persists supplied voter directory/listener metadata through replayable Raft config records and applies the voter plus endpoint metadata after commit. UpdateRaftVoter now advertises key 82 v0, validates voter existence and KRaft feature ranges, appends replayable Raft config records for voter endpoint metadata, and applies endpoint updates after commit. Committed voter endpoints now update the Raft RPC client pool before elections and replication instead of remaining DescribeQuorum-only metadata, and persisted UpdateRaftVoter endpoint metadata is replayed after static voter registration on controller restart. Controller tests now run in the default Zig suite and cover malformed Vote/ControllerRegistration/AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter, ControllerRegistration invalid feature/listener paths, endpoint-add/update success and error cases, and internal AppendEntries compatibility. A gated controller/broker failover plus rolling-restart harness exists, restarts the killed old leader from a fresh local data dir, and broker-only nodes start fenced until controller heartbeat succeeds. Broker registrations, broker unregistrations, broker rack metadata, log-directory IDs, offline log-directory health, and producer-id allocation cursors are now stored as controller metadata records, replayed from persisted Raft logs on follower promotion/restart, and compacted through full controller snapshot records before Raft truncation. | Add broader broker+controller failover gates. |
| Stateless brokers | Partial. Combined AutoMQ metadata can rejoin from a fresh local data dir through quorum replay, local/S3 object repair covers data-path replacement, topic IDs, partition counts, common topic configs including segment/cleanup/compression policy, finalized feature snapshots, DeleteRecords/retention partition low-watermarks, replica-directory assignment snapshots, and ongoing partition reassignment snapshots can rebuild from `__cluster_metadata` S3 WAL records, committed offsets, OffsetDelete/DeleteGroups tombstones, and consumer group lifecycle snapshots can rebuild from `__consumer_offsets` S3 WAL records, transaction coordinator snapshots plus atomic EndTxn/WriteTxnMarkers commit/abort marker/snapshot objects can rebuild from `__transaction_state` plus data S3 WAL records after fresh-dir broker replacement, idempotent producer sequence state can rebuild from durable record batches after S3 WAL replacement, and client-facing coordinator/reassignment/ACL/produce/transaction-marker mutations now return storage errors when their shared-storage or local checkpoints cannot be written. Local cache/state still has single-node assumptions for broader broker-only metadata. | Rebuild all broker state from shared storage/controller metadata without data loss or manual repair. |
| Reassignment/autobalancing | Partial. Generated reassignment handlers now retain, list, cancel, locally persist ongoing reassignment state across broker restart, replay it from `__cluster_metadata` S3 WAL after fresh-dir broker replacement, commit topic and reassignment snapshot records through Raft when attached to the quorum leader, reject attached non-leader mutations with `NOT_CONTROLLER`, replay committed topic records before quorum assignment/cancellation records after restart or promotion, apply target owners into local failover ownership metadata, expose non-local owners in Metadata/DescribeTopicPartitions, reject local Produce/Fetch to non-local owners, fail closed and roll back local visibility when shared snapshot writes fail, and clear stale entries when a topic is deleted; auto-balancer planning can prefer less-loaded targets in a different rack when topology is available, uses controller-backed broker rack metadata, ignores fenced brokers as targets, moves load off fenced/scale-in leaders, executes validated plan moves through the durable reassignment path, rejects stale/duplicate plans before mutation, ignores stale unknown-node load samples, clamps negative metric rates to zero, and has deterministic simulated convergence coverage. Broker-side controller-aware orchestration is covered for fenced-broker movement, simulated convergence after ownership changes, stale-plan fail-closed behavior, no active target no-ops, and scheduled `tick()` execution/skip from cached controller/load samples. The gated KRaft failover harness now exercises live reassignment protocol convergence, old-owner write fencing, and target-broker topic/data convergence over real broker/controller processes. | Broader live load/scale orchestration plus cross-broker chaos coverage. |
| Consumer groups/transactions | Partial. Core flows exist with simplified persistence; OffsetCommit now reports per-partition commit failures instead of acknowledging failed coordinator mutations, writes versioned internal `__consumer_offsets` records before acknowledging, rolls back local committed-offset visibility when the local offset snapshot cannot be written, validates managed member identity/generation, rejects empty group IDs, preserves committed leader epoch and metadata through OffsetFetch and restart, rejects oversized normal and transactional offset commit metadata, and rejects unknown topic-partitions without committing offsets, OffsetFetch reports missing groups at version-appropriate partition/top/group error levels and rejects empty group IDs at legacy and grouped response levels, OffsetDelete writes `__consumer_offsets` tombstones before deleting local offsets, replays those tombstones during S3 WAL replacement, fails closed on tombstone write failure, rolls back local committed-offset visibility when the local offset snapshot cannot be written, flushes offset mutations, reports missing groups, rejects empty group IDs, rejects subscribed group topics without removing offsets, and reports unknown topic-partitions without deleting stale offsets, DeleteGroups writes tombstones for all group offsets before local group deletion, fails closed on tombstone write failure, rolls back local group visibility on lifecycle snapshot write failure, flushes offset mutations, and returns protocol `ErrorCode` values for invalid, missing, and non-empty group cases, TxnOffsetCommit now advertises v4, requires valid transactional identity/epoch plus prior AddOffsetsToTxn registration, maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v4 clients, validates managed group member identity/generation, rejects empty group IDs and unknown topic-partitions without committing offsets, writes versioned internal offset records before acknowledging, and rolls back local committed-offset visibility when the local offset snapshot cannot be written, broker open can replay committed offsets from recovered S3 WAL `__consumer_offsets` objects, JoinGroup/SyncGroup/LeaveGroup/DeleteGroups now flush local group lifecycle snapshots, JoinGroup/LeaveGroup/SyncGroup restore the previous local group snapshot when shared lifecycle snapshot writes fail, consumer group lifecycle snapshots are also written to `__consumer_offsets` and replayed from recovered S3 WAL, transaction coordinator snapshots are written to `__transaction_state` and replayed from recovered S3 WAL, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and WriteTxnMarkers restore the previous transaction snapshot when their coordinator mutations cannot be written to shared storage, EndTxn now advertises v4 and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v4 commit attempts before marker writes, EndTxn, WriteTxnMarkers, and timed-out transaction aborts in S3 WAL mode now flush marker control batches and the updated transaction snapshot in one shared WAL object before advancing local state, JoinGroup, SyncGroup, LeaveGroup, DeleteGroups, ConsumerGroupHeartbeat, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and WriteTxnMarkers now return storage errors instead of successful acknowledgements when their coordinator snapshot S3 WAL write fails, EndTxn/WriteTxnMarkers marker paths now also return storage errors when post-marker local partition-state, ObjectManager, or transaction checkpoints cannot be written, JoinGroup stores selected protocol metadata, applies and persists session/rebalance timeouts, broker tick enforces each group's configured session timeout, persists group/member protocol metadata across restart, DescribeGroups reports stored group protocol and member metadata/assignments, ConsumerGroupDescribe advertises key 69 v0 and returns generated read-only group state/member/subscription views over the existing group coordinator, ConsumerGroupHeartbeat advertises key 68 v0 with heartbeat-driven membership, persisted KIP-848 owned-assignment echoes, cooperative assignment revocation, generated authorization denial, and no classic rebalance-in-progress rejection, Heartbeat/SyncGroup/LeaveGroup use protocol error codes for missing members/groups and static-member fencing, and rejects incompatible protocol joins, SyncGroup v5 validates protocol type/name against group state, LeaveGroup v3+ can resolve static members by `group_instance_id`, AddPartitionsToTxn now advertises v5 and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v5 clients, AddPartitionsToTxn v4+ and EndTxn validate transactional identity/epoch before mutating state, AddPartitionsToTxn/EndTxn/WriteTxnMarkers fail closed for unknown topic-partitions, WriteTxnMarkers validates local producer epoch/state before writing local markers and skips local completion after any marker partition error, AddOffsetsToTxn now advertises v4, maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v4 clients, validates transactional identity/epoch and the internal offsets partition before registering it, DescribeTransactions reports transaction state, PID/epoch, timeout/start time, grouped partitions, and missing transactional IDs through generated schemas, ListTransactions applies state, producer ID, and duration filters and reports unknown state filters, transaction introspection is covered after local broker restart, timed-out transactions now write abort control markers before coordinator completion, transaction coordinator errors now use protocol `ErrorCode` values for invalid PID/epoch/state paths, Produce v11 validates transactional producer state before append and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` without advancing the log, InitProducerId now advertises v5 and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v5 recovery attempts, validates v3+ requested producer id/epoch recovery before bumping, applies transactional timeouts, rejects invalid transactional timeouts, flushes producer allocations, transaction snapshots retain registered partitions, Produce rejects invalid required acks and enforces `min.insync.replicas` for `acks=-1` before append, idempotent producer sequence updates now advance only after durable Produce success, reject same-epoch sequence gaps and stale producer epochs without appending, persist the last sequence in a batch, rebuild from durable log batches after S3 WAL replacement, and fence restored or in-memory per-partition sequence state when InitProducerId bumps a producer epoch, and gated KRaft failover validates OffsetCommit/OffsetFetch, classic JoinGroup/SyncGroup/Heartbeat, InitProducerId/AddPartitionsToTxn/EndTxn continuity, and idempotent Produce v9 duplicate suppression, next-sequence progress, InitProducerId epoch-bump fencing, and next-epoch recovery through controller failover, controller restarts, and broker restart. | Kafka-compatible rebalances, offset lifecycle, transactions, fencing, and coordinator failover. |
| Security | Partial. TLS, SASL, OAuth, SCRAM, and ACL pieces exist. TLS config rejects unsupported JKS keystore/truststore paths, invalid protocol-version ranges, and mTLS client-auth without CA trust anchors. Configured cert/key/CA PEM files are fingerprinted after OpenSSL context load, and new TLS accepts reload the context when configured PEM files are rotated or deleted. SASL-enabled brokers reject non-auth APIs until the client completes SASL authentication. Advertised broker APIs now have default-suite ACL resource/operation mapping coverage except pre-auth SASL frames. Generated response-shape coverage now includes Produce/Fetch, ApiVersions/Metadata/CreateTopics/DeleteTopics, config admin, reassignment/logdir/partition admin, quota/SCRAM admin, ACL admin, quorum Vote/BeginQuorumEpoch/EndQuorumEpoch, selected cluster/transaction introspection including UpdateFeatures and DescribeTransactions, telemetry/client-metrics GetTelemetrySubscriptions/PushTelemetry/ListClientMetricsResources, DescribeTopicPartitions, transaction coordinator InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/WriteTxnMarkers/TxnOffsetCommit, topic-scoped ListOffsets/DeleteRecords/OffsetForLeaderEpoch/DescribeProducers, group-introspection ListGroups/DescribeGroups/ConsumerGroupDescribe/ShareGroupDescribe, coordinator/session FindCoordinator/JoinGroup/ConsumerGroupHeartbeat/ShareGroupHeartbeat/ShareFetch/ShareAcknowledge/Heartbeat/LeaveGroup/SyncGroup, share-state Initialize/Read/Write/DeleteShareGroupState/ReadShareGroupStateSummary, offset OffsetCommit/OffsetFetch, group/offset deletion DeleteGroups/OffsetDelete, and all AutoMQ extension keys 501-519/600-602 authorization denials, unsupported-version rejection now runs before ACL denial response construction or SASL pre-auth gating, restrictive ACLs cannot block SaslHandshake/SaslAuthenticate negotiation, disabled or unnegotiated SASL mechanisms fail closed before authenticating, and OAuthBearer auth no longer leaks decoded JWT state on success. The real-client matrix can require secured profiles plus bad-SASL, bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce fail-closed checks for those profiles. | Broader OAuth provider interop suites and live secured-client CI environments. |
| Observability | Partial. Metrics and JSON logging exist; `/health` and `/ready` have shared plain/TLS routing, exact-path tests, and startup/shutdown readiness response coverage. Prometheus HELP/label escaping and labeled-metric arity checks are covered in the default Zig suite, broker metric registration is pinned to the broker API metric catalog, client-metrics APIs expose a default resource plus retained active-client telemetry samples with terminating cleanup and Prometheus counters/gauges for accepted pushes, terminating pushes, retained sample count, retained sample bytes, and external JSONL export success/error/byte totals, export failures fail closed before state mutation, checked-in Grafana/Prometheus alert artifacts are parsed against the registered metric corpus, produce/fetch/request/S3 p99 SLO alerts plus JMX controller/replica-manager/broker-topic/broker-state/idle/purgatory failure alerts are pinned in the default suite, AutoMQ-compatible Kafka request/gauge metric names including produce/fetch counters and latency histograms are emitted from the real request and broker-state paths, compaction lifecycle counters are registered before emission, and a broader JMX-compatible request/broker-topic/replica-manager/controller/broker-state/purgatory metric set is registered and emitted. | Continue expanding the AutoMQ/JMX metric corpus and operational SLO fixtures. |
| Tests | Improving. Unit/integration tests run under Zig 0.16. Controller, broker registry, and metadata client tests are included in the default suite. `zig build bench` now exercises repeatable local produce/fetch, S3 WAL request-volume, recovery-time, and bounded memory-growth performance gates. Gated `test-client-matrix` can enforce required version, tool, semantic, secured-client, and negative-security profile coverage; gated `test-chaos` now covers broker SIGKILL/local-WAL restart, slow/partial client frames, far-future client timestamps, sync S3 WAL outage fail-closed behavior, CI-provided network-partition hooks, and CI-provided live-S3 outage/heal hooks; `test-s3-provider-matrix` can enforce required live provider, outage, process-crash/replacement, pagination, multipart-edge, and multipart-fault profile coverage; gated `test-kraft-failover` can now run CI-provided controller/broker network-partition hooks or scheduled `ZMQ_KRAFT_NETWORK_MATRIX` phases before failover/restart sequencing; and gated `test-e2e` exposes the Docker three-node combined-mode cluster suite with required cross-broker chaos/load-scale phase validation plus hook context for topic, broker/controller/metrics ports, containers, and MinIO. Deterministic fixture self-tests run in the default suite. | Add protocol golden, broader multi-node e2e, broader multi-broker chaos, comparative perf, and expanded client matrix gates. |

Latest default-suite observability tranche: JMX-compatible broker-state,
request-handler idle, network-processor idle, delayed-operation purgatory, and
reassigning-partition gauges are registered and emitted from broker tick state.
Checked-in Grafana and Prometheus fixtures now reference these metrics,
including broker-not-running, low-idle, stuck-reassignment, and delayed-fetch
purgatory alerts, and default readiness tests fail if the artifacts drift from
the registered metric corpus.

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
  not-controller responses, scopes metadata-partition errors to the request,
  and includes committed voter directory IDs plus v2 node listener endpoints
  from Raft voter metadata on controller and broker paths.
  FetchSnapshot now advertises controller key 59 v0-v1, decodes generated
  flexible requests, rejects malformed frames, serves compacted controller
  full-snapshot record bytes with request `max_bytes` chunking, returns
  `position_out_of_range` for invalid byte positions, returns request-scoped
  `snapshot_not_found` for unavailable snapshots, and includes v1
  current-leader/node-endpoint metadata from committed voter endpoints.
  UnregisterBroker now advertises controller key 64 v0, decodes generated
  flexible requests, rejects malformed frames, removes registered brokers, and
  appends replayable broker-unregistration controller metadata records.
  ControllerRegistration now advertises controller key 70 v0, decodes generated
  flexible requests, rejects malformed frames, fails closed on followers or
  unknown controller IDs, and accepts configured voter controllers.
  UpdateRaftVoter now advertises controller key 82 v0, decodes generated
  flexible requests, rejects malformed frames, validates feature ranges and
  voter existence, appends replayable Raft config records for endpoint updates,
  and applies committed endpoint metadata into the voter state.
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
  subscriptions, current/target range assignments, and missing-group errors
  from the existing group coordinator.
  ConsumerGroupHeartbeat key 68 now advertises v0, validates/decodes generated
  flexible frames, can join, heartbeat, and leave through the existing group
  coordinator, returns deterministic range assignments for known subscribed
  topics, reconciles changed subscription lists with persisted member-epoch
  bumps, persists member rack IDs and KIP-848 owned assignment echoes through
  local/S3 group snapshots, withholds newly targeted partitions until the
  previous owner echoes revocation, exposes current/target assignment through
  ConsumerGroupDescribe, rejects unsupported/incompatible server assignors and
  duplicate subscription names, returns terminal leave responses and
  join/rejoin error responses without scheduling another heartbeat, maps stale
  member epochs/static instance conflicts to KIP-848
  `FENCED_MEMBER_EPOCH`/`UNRELEASED_INSTANCE_ID` errors, and returns generated
  authorization-denial responses.
  OffsetFetch now rejects empty group IDs at legacy per-partition and grouped
  response levels, OffsetFetch v9 validates supplied KIP-848
  `member_id`/`member_epoch` fields at the group response level while
  preserving no-identity admin offset fetches, and OffsetCommit v9 maps stale
  member epochs to
  `FENCED_MEMBER_EPOCH` and missing member groups to `UNKNOWN_MEMBER_ID`
  instead of classic-generation/group-id errors.
  AssignReplicasToDirs key 73 is now advertised with generated flexible v0
  framing, validates broker identity and cached controller-assigned broker
  epochs for broker-only nodes, advertises all configured local logical JBOD
  directory IDs through BrokerRegistration v2, persists and replays registered
  directory IDs through controller Raft metadata, validates
  directory/topic/partition targets against the configured local directory set,
  rejects duplicate assignments, returns generated per-partition errors, uses a
  generated authorization-denial response, maintains local replica-directory
  assignment state, mirrors assigned partitions through DescribeLogDirs,
  restores that state across local broker restart, appends assignment snapshots
  to `__cluster_metadata` for broker replacement replay, and rolls back
  local/shared assignment visibility when snapshot persistence fails.
  ShareGroupHeartbeat and ShareGroupDescribe now advertise v0, join, heartbeat,
  update subscriptions/rack metadata, leave, return deterministic range
  assignments, fail closed on leave/session-cleanup persistence errors, return
  generated authorization denials, and describe local share groups through the
  existing group coordinator.
  ShareFetch and ShareAcknowledge now advertise v0, validate local share
  sessions, fetch records from the partition store, return acquired-record
  ranges, validate acknowledgement batches, advance local share start offsets,
  clear sessions on share-member leave/group delete, restore share session
  epochs across local broker restart and fresh-dir S3 WAL replacement, write
  combined share data-plane snapshots to `__consumer_offsets`, return generated
  authorization denials, and roll back session plus share-state mutation
  changes, including DeleteGroups cleanup, when local or shared persistence
  fails.
  InitializeShareGroupState, ReadShareGroupState, WriteShareGroupState,
  DeleteShareGroupState, and ReadShareGroupStateSummary now advertise v0,
  validate topic IDs, group IDs, partitions, state epochs, start offsets, and
  state batches while maintaining local share-partition state and restoring it
  across local broker restart and fresh-dir S3 WAL replacement.
  Initialize/write/delete mutations now fail closed with default-suite rollback
  coverage when local or shared share-state persistence fails, and all share
  state APIs return generated authorization denials.
  DescribeAcls/CreateAcls/DeleteAcls now use generated schemas, reject malformed
  frames, validate enum fields, write full ACL snapshots to `__cluster_metadata`
  for broker replacement replay, fail closed and roll back local ACL visibility
  when local or shared snapshot writes fail, and return generated ACL
  resources/results.
  CreateTopics, AlterConfigs, and IncrementalAlterConfigs now validate common
  supported topic config values against temporary configs before mutating local
  topic state, including positive `max.message.bytes`, positive
  `segment.bytes`, normalized `cleanup.policy`, normalized `compression.type`,
  positive `min.insync.replicas`, and
  `min.insync.replicas <= replication.factor`.
  Config mutations write full topic snapshots to `__cluster_metadata` before
  acknowledging successful mutations, preserve validate-only as a no-write
  path, and fail closed with rollback when the shared snapshot or local
  topic/ObjectManager snapshots cannot be written.
  DeleteTopics now defers share-state and replica-directory cleanup until the
  durable topic snapshot succeeds, persists successful cleanup across local
  restart, and restores topic visibility without dropping those side states
  when snapshot writes fail. After the durable topic snapshot commits,
  DeleteTopics now returns storage errors for successfully deleted topics if
  local cleanup checkpoints fail.
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
  flexible requests, rejects malformed frames, returns a default generated
  resource, and lists active client resources for retained telemetry samples.
  UpdateFeatures now advertises key 57 v0-v1, decodes generated flexible
  requests with correct v1 field gating, rejects malformed frames, invalid
  upgrade types, unsupported features, and unsupported finalized versions,
  honors validate-only requests, mutates local finalized feature metadata for
  supported features, writes finalized feature snapshots to `__cluster_metadata`
  for broker replacement replay, fails closed with rollback when shared or local
  persistence fails, persists it across local restart, and exposes supported and
  finalized features through ApiVersions v3+ tagged fields.
  GetTelemetrySubscriptions and PushTelemetry now advertise keys 71/72 v0,
  decode generated flexible requests, reject malformed frames, return a minimal
  all-metrics telemetry subscription, preserve assigned client instance IDs,
  retain matching uncompressed pushes, remove retained samples on terminating
  pushes, update Prometheus counters/gauges for accepted pushes, terminating
  pushes, retained sample count, and retained sample bytes, export accepted
  payloads to a configured append-only JSONL sink with success/error/byte
  metrics, fail closed with generated `KAFKA_STORAGE_ERROR` responses when the
  sink cannot be written before retained telemetry state mutates, and reject
  unknown subscription IDs, unsupported compression, and oversized metrics.
  Flexible DescribeProducers, SCRAM credential, UpdateFeatures, telemetry, and
  client-metrics request validators now reject trailing bytes after the final
  tagged-fields section.
  DeleteGroups, DescribeCluster, partition reassignment, and
  OffsetForLeaderEpoch validators now apply the same fail-closed trailing-byte
  check.
  DescribeClientQuotas and AlterClientQuotas now reject trailing bytes after
  their final tagged-fields section as well.
  DescribeAcls, CreateAcls, and DeleteAcls now reject trailing bytes after the
  final filter/creation tagged-fields section before normal handling.
  OffsetDelete now rejects trailing bytes after the final requested partition
  index.
  FindCoordinator now rejects trailing bytes after the legacy key or v4+
  coordinator key-list tagged-fields section.
  WriteTxnMarkers now rejects trailing bytes after the final transaction-marker
  tagged-fields section.
  TxnOffsetCommit now rejects trailing bytes after the final offset topic
  tagged-fields section.
  InitProducerId now rejects trailing bytes after the final flexible request
  tagged-fields section.
  AddOffsetsToTxn now rejects trailing bytes after the final flexible group
  tagged-fields section.
  AddPartitionsToTxn now rejects trailing bytes after the final flexible
  transaction batch tagged-fields section.
  EndTxn now rejects trailing bytes after the final flexible request
  tagged-fields section.
  ListOffsets now rejects trailing bytes after the final legacy or flexible
  topic/partition request section.
  Metadata now rejects trailing bytes after the final legacy or flexible topic
  list and authorization flags.
  Produce now rejects trailing bytes after the final legacy or flexible topic
  record-data section.
  Fetch now rejects trailing bytes after the final legacy or flexible forgotten
  topic/rack/tagged-fields section.
  SaslHandshake and SaslAuthenticate now reject trailing bytes after the final
  mechanism/auth-bytes fields.
  JoinGroup now rejects trailing bytes after the final protocol metadata,
  reason, and tagged-fields section.
  SyncGroup now rejects trailing bytes after the final assignment and
  tagged-fields section.
  DescribeGroups now rejects trailing bytes after the group list,
  authorization flag, and tagged-fields section.
  Heartbeat now rejects trailing bytes after the final member/static-instance
  fields and tagged-fields section.
  LeaveGroup now rejects trailing bytes after the final legacy member or
  flexible member-identity list and tagged-fields section.
  OffsetCommit now rejects trailing bytes after the final offset topic,
  partition, and tagged-fields section.
  OffsetFetch now rejects trailing bytes after the final legacy or grouped
  topic list, require-stable flag, and tagged-fields section.
  DescribeTransactions now rejects trailing bytes after the final transactional
  ID and tagged-fields section.
  ListTransactions now rejects trailing bytes after state/producer/duration
  filters and tagged-fields section.
  DescribeTopicPartitions now rejects trailing bytes after topic filters,
  optional cursor, and tagged-fields section.
  ListGroups now rejects trailing bytes after state/type filters and
  tagged-fields section.
  DescribeConfigs now rejects trailing bytes after resources, config-key
  filters, option flags, and tagged-fields section.
  AlterConfigs now rejects trailing bytes after resources, config values,
  validate-only flag, and tagged-fields section.
  CreateTopics now rejects trailing bytes after topic assignments/configs,
  timeout, validate-only flag, and tagged-fields section.
  DeleteTopics now rejects trailing bytes after topic names/IDs, timeout,
  and tagged-fields section.
  DeleteRecords now rejects trailing bytes after topic partitions, timeout,
  and tagged-fields section.
  CreatePartitions now rejects trailing bytes after topic assignments, timeout,
  validate-only flag, and tagged-fields section.
  IncrementalAlterConfigs now rejects trailing bytes after resources,
  config operations/values, validate-only flag, and tagged-fields section.
  Vote now rejects trailing bytes after cluster ID, voter ID, topic
  partitions, and tagged-fields section.
  EndQuorumEpoch now rejects trailing bytes after cluster ID, topic
  partitions, leader endpoints, and tagged-fields section.
  DescribeQuorum now rejects trailing bytes after topic partitions and
  tagged-fields section.
  Source-level audits now fail the default suite if broker or controller
  request-frame validators accept trailing bytes, with only the documented
  BeginQuorumEpoch internal AppendEntries bridge exempted.
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
  acknowledging these AutoMQ metadata/object mutations. In local mode,
  PutKVs/DeleteKVs, AutomqRegisterNode, AutomqZoneRouter, UpdateLicense,
  GetNextNodeId, and AutomqUpdateGroup now use a durable AutoMQ metadata
  snapshot boundary and roll back visible metadata changes when that write
  fails. Single-node leaders now
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
  Gated client compatibility now includes metadata, topic-admin where the
  selected tool supports it, produce/fetch, and committed-offset probes for
  installed/configured kcat, Kafka CLI, kafka-python, confluent-kafka,
  Java kafka-clients, and Go kafka-go clients. The matrix now accepts named
  version profiles with per-profile bootstrap, tool list, Java classpath, Go
  module, Python interpreter, semantic-suite, TLS, and SASL overrides so CI can
  run explicit client/library version sets. `ZMQ_CLIENT_MATRIX_SEMANTICS` can
  now require admin, consumer-group, rebalance, transactional, secured-client,
  bad-credential, bad-OAuth-token/JAAS/config, bad-TLS-trust, and
  ACL-denied-produce real-client probes in addition to default
  metadata/produce/fetch/offset checks.
- Prepared-object lifecycle coverage includes request-TTL tracking, registry-only
  allocation expiry, prepared/committed/destroyed state transitions, and
  compaction cleanup paths. Local-mode Create/Open/Close/Delete/TrimStreams,
  PrepareS3Object, CommitStreamObject, and CommitStreamSetObject now use a
  durable ObjectManager snapshot boundary and roll back visible stream,
  prepared-object, or committed-object changes when that write fails.

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
  malformed part ETags before completion; common topic configs are now included
  in local topic metadata snapshots, and topic IDs, partition counts, supported
  configs, finalized feature snapshots, and replica-directory assignment
  snapshots are now written to
  `__cluster_metadata` and replayed from recovered S3 WAL during fresh-dir
  replacement; Produce now returns per-partition storage
  errors when post-append local partition-state, ObjectManager, or
  producer-sequence checkpoints cannot be written; AlterConfigs/
  IncrementalAlterConfigs now roll back topic config visibility and return
  storage errors when shared or local topic/ObjectManager snapshots cannot be
  written; auto-created topics now fail closed and roll back local visibility
  when their shared topic snapshot cannot be written, and
  CreateTopics/CreatePartitions/DeleteTopics return storage errors for the same
  shared-snapshot failure; DeleteTopics also reports post-snapshot local cleanup
  checkpoint failures; local metadata snapshots now fsync before save calls
  return; filesystem WAL now implements its periodic fsync policy in addition
  to explicit/every-record/every-N-record barriers; MockS3 fault injection now
  covers bounded put retry,
  propagated get/list/range/delete failures, temporary list omission, recovery
  retry, fetch storage errors, and compaction orphan retry; restarted S3 WAL
  writers now seed their object counters from existing WAL keys before accepting
  writes, and broker replacement coverage verifies recovered S3 data plus new
  hot-cache data fetch as a single range without overwriting prior WAL objects;
  overlapping S3 WAL offset ranges now fail recovery instead of surfacing
  duplicate or conflicting records from stale writers; S3 WAL epoch changes are
  now monotonic, and sync flushes fence stale writers before upload when object
  storage already contains newer epoch WAL objects;
  consumer group lifecycle snapshots now persist active group membership,
  generation, leader, assignments, protocol metadata, and group timeouts across
  local broker restart, the same lifecycle snapshot is written to
  `__consumer_offsets` and replayed from recovered S3 WAL during fresh-dir
  replacement, share sessions and share-partition state are written as combined
  `__consumer_offsets` snapshots and replayed from recovered S3 WAL during
  fresh-dir replacement, transaction coordinator snapshots are written to
  `__transaction_state` and replayed from recovered S3 WAL during fresh-dir
  replacement, client-facing coordinator mutations now fail closed when these
  snapshot writes fail, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn,
  EndTxn, and WriteTxnMarkers now restore the previous transaction snapshot
  when their coordinator mutations cannot be written to shared storage, and
  EndTxn, WriteTxnMarkers, and timed-out transaction aborts in S3 WAL mode now
  flush marker control batches and the updated transaction snapshot in one
  shared WAL object before advancing local state. EndTxn/WriteTxnMarkers marker
  paths now return storage errors when post-marker local partition-state,
  ObjectManager, or transaction checkpoints cannot be written; broker tick now
  enforces each group's configured session timeout instead of a global timeout;
  S3Client multipart fault tests now cover missing/malformed/XML-unsafe part ETags,
  abort-after-part failure, abort-after-complete failure, and HTTP 200
  CompleteMultipartUpload responses carrying embedded XML errors; a gated
  `test-minio` build step now covers live object round-trip, multipart
  round-trip, S3 WAL metadata rebuild plus fetch, and PartitionStore S3 WAL
  produce/rebuild/resume against MinIO/S3; OffsetCommit and TxnOffsetCommit now
  write versioned `__consumer_offsets` records before acknowledging and broker
  open replays committed offsets from recovered S3 WAL objects; OffsetCommit,
  TxnOffsetCommit, and OffsetDelete now restore the previous local committed
  offset snapshot when `offsets.meta` cannot be written; OffsetDelete and
  DeleteGroups now write `__consumer_offsets` tombstones before local removal,
  replay those tombstones during fresh-dir replacement, and preserve local
  offsets/groups when the shared tombstone write fails; DeleteGroups,
  JoinGroup, LeaveGroup, and SyncGroup now restore the previous local group
  snapshot when their shared lifecycle snapshot write fails; share data-plane
  snapshot writes restore prior session/state visibility when shared persistence
  fails; client quota
  configuration snapshots are now appended to `__cluster_metadata`, replayed
  from recovered S3 WAL on fresh-dir broker replacement, and rolled back on
  failed snapshot writes before AlterClientQuotas is acknowledged; SCRAM
  credential snapshots are likewise appended to `__cluster_metadata`, replayed
  during broker replacement, and rolled back before AlterUserScramCredentials is
  acknowledged when the snapshot write fails; ACL snapshots are appended to
  `__cluster_metadata`, replayed during broker replacement, and rolled back
  before CreateAcls/DeleteAcls are acknowledged when local or shared ACL
  snapshot writes fail; UpdateFeatures finalized feature snapshots are appended
  to `__cluster_metadata`, replayed during broker replacement, and rolled back
  before acknowledgement when shared or local finalized-feature snapshot writes
  fail; AssignReplicasToDirs replica-directory assignment snapshots are appended
  to `__cluster_metadata`, replayed during broker replacement, pruned when topic
  snapshots remove referenced partitions, and rolled back before acknowledgement
  when shared or local assignment snapshots fail;
  atomic EndTxn and WriteTxnMarkers S3 WAL objects now restore both commit/abort
  marker partition offsets and completed transaction snapshots after fresh-dir
  replacement;
  `test-s3-process-crash`
  adds a gated real broker-process kill/replacement harness against MinIO/S3 and
  now verifies both data and committed offsets after a fresh local data dir
  replacement. `test-s3-provider-matrix` now runs the live MinIO/S3 suite across
  named S3-compatible provider profiles with per-profile endpoint, port, bucket,
  credential, scheme, region, TLS CA, path-style, existing-bucket,
  ListObjectsV2 pagination through
  `ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES`, required live
  multipart-edge coverage through
  `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES`, provider-specific
  multipart-fault commands through
  `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES`, and required
  process-crash/replacement coverage through
  `ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES`. S3
  request signing now honors configured regions, supports virtual-hosted
  addressing, omits default HTTP/HTTPS ports from the canonical Host header for
  AWS-style providers, and preserves explicit custom ports. Real-client
  ListObjectsV2 pagination now follows continuation tokens and fails closed when
  a provider marks a page truncated without returning a token. Remaining
  durability work is live execution of provider-specific multipart fault
  scripts with the enforced provider context and live provider outage/fault
  recovery against quorum/controller metadata.
  S3 range reads now validate `206 Content-Range`
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
  behavior or stop advertising them. Status: ZooKeeper-era inter-broker keys
  4-7 are generated-only in KRaft/AutoMQ mode; ApiVersions omits them, the
  broker has no dispatch/no-op path for them, and direct probes fail closed
  before body decode. Controller ApiVersions now has a separate audited support
  catalog; generated controller quorum/lifecycle
  framing is in place for Vote, BeginQuorumEpoch, EndQuorumEpoch,
  DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat,
  UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter,
  RemoveRaftVoter, UpdateRaftVoter endpoint-update handling, and
  DescribeQuorum v2 endpoint/directory metadata, and
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
  Broker registration/unregistration state, broker rack metadata, registered
  broker log-directory IDs, and
  producer-id allocation cursors now have Raft-backed controller metadata
  records; followers persist replicated AppendEntries and promoted or restarted
  controllers replay registered brokers, rack metadata, and PID cursors before
  serving lifecycle APIs. Local failover ownership
  metadata now tracks broker topic create/delete/create-partition/restore paths
  and moves tracked partitions from timed-out fenced brokers to the surviving
  broker instead of treating reassignment as a no-op. Controller metadata full
  snapshot records now preserve broker/rack/PID state across Raft log
  compaction. The gated failover harness can now run CI-provided
  `ZMQ_KRAFT_NETWORK_DOWN`/`ZMQ_KRAFT_NETWORK_UP` hooks after initial
  broker/controller convergence, or schedule multiple named phases with
  `ZMQ_KRAFT_NETWORK_MATRIX` and per-phase down/up/expect overrides. Each phase
  receives controller/broker PID and port context plus the active controller
  leader, verifies configured produce behavior during the partition, heals the
  cluster, reconverges controllers, and verifies broker data continuity before
  the leader-kill/restart sequence. Release jobs can require specific scheduled
  controller/broker partition phases with `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES`,
  so missing failover matrix coverage fails before the harness starts.
  The existing Docker three-node combined-mode E2E suite is now exposed as
  gated `test-e2e` with default self-test coverage, so release jobs can require
  the multi-node Docker path explicitly with `ZMQ_RUN_E2E_TESTS=1`. The E2E
  harness can also run named cross-broker chaos phases through
  `ZMQ_E2E_CHAOS_MATRIX` plus per-phase down/up/expect hooks, exports
  broker/controller/container context to those hooks, verifies cross-node
  produce/fetch behavior after each heal, and fails release jobs when required
  phases are absent through `ZMQ_E2E_REQUIRED_CHAOS_PHASES`. Live Docker
  load/scale orchestration can now be required separately with
  `ZMQ_E2E_LOAD_SCALE_MATRIX`, per-phase apply/restore hooks, cross-node
  produce/fetch checks after both apply and restore, and
  `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` release coverage validation.
  Remaining gap: broader failover gates and client compatibility fixtures.
- Validate rack-aware routing and auto-balancer decisions under load. Status:
  rack-aware planning has unit coverage for cross-rack target preference,
  same-rack fallback, stale unknown-node metric filtering, non-negative metric
  normalization, controller-backed active-broker/rack planning, fenced-broker
  target exclusion, scale-in leader movement, durable execution of validated
  plan moves, stale/duplicate plan rejection, and simulated post-plan load
  convergence; reassignment APIs can
  retain/list/cancel ongoing state, commit reassignment snapshots through Raft
  when attached to a quorum leader, fail closed with `NOT_CONTROLLER` on
  attached non-leaders, replay committed assignment/cancellation records,
  apply target ownership into local failover metadata, restore that ownership
  after restart/replacement replay, expose the owner through metadata APIs, and
  fence local Produce/Fetch when ownership is non-local. Broker-side
  controller-aware orchestration now computes from controller broker snapshots
  plus load samples, applies moves through durable reassignments, fails closed
  on stale plans without partial mutation, no-ops without active targets, can
  execute automatically from broker `tick()` when cached inputs are due, and
  has simulated convergence coverage after ownership changes. The gated KRaft
  failover harness now exercises live broker-process reassignment protocol
  convergence, old-owner write fencing, and target-broker topic/data
  convergence. Controller-aware scale-out planning now has deterministic
  coverage that spreads hot partitions from an overloaded broker to multiple
  newly active broker targets. Docker E2E can require named cross-broker chaos
  phases and verifies cross-node produce/fetch recovery after each heal.
  Live Docker load/scale orchestration hooks and required-phase validation are
  now pinned by `test-e2e`; broader CI execution across real scale-in/out/load
  environments remains.

### Phase 5: Production Gates

- Add compatibility runs with Java, librdkafka, Go, Python, and Kafka CLI
  clients across supported API versions.
  Status: gated metadata plus produce/fetch probes exist for kcat, Kafka CLI,
  kafka-python, confluent-kafka/librdkafka, Java kafka-clients, and Go
  kafka-go; Kafka CLI now also performs explicit topic admin checks, and
  kafka-python, confluent-kafka/librdkafka, Java kafka-clients, and Go kafka-go
  now verify committed offsets after consuming produced records. The harness
  supports named version profiles with per-profile Java classpaths, Go modules,
  Python interpreters, tool lists, semantic suites, TLS/SASL settings, exact
  version labels, and bootstrap endpoints. Release jobs can now pin required
  version profiles with `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`, require exact
  version labels for those profiles with
  `ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES`, required secured-client
  profiles with `ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES`, required
  negative-security profiles with
  `ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES`,
  required OAuth-positive profiles with
  `ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES`, and required OAuth-negative
  profiles with `ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES`. Release
  jobs can also require aggregate client implementation and semantic-suite
  coverage across the selected profile set with
  `ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS` and
  `ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS`.
  `ZMQ_CLIENT_MATRIX_SEMANTICS` can require real-client admin topic/config
  operations, consumer-group list/describe or group reads, multi-consumer
  rebalance assignment convergence for kafka-python, confluent-kafka, and Java
  kafka-clients, transactional produce paths for confluent-kafka and Java
  kafka-clients, secured-client runs for kcat, Kafka CLI, kafka-python,
  confluent-kafka, and Java kafka-clients, and bad-credential,
  bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce
  fail-closed checks over those secured-client profiles. Release jobs can now
  require named OAUTHBEARER profiles and OAuth-specific negative profiles so
  generic SASL/TLS security coverage cannot satisfy the OAuth interop gate;
  required secured/OAuth profiles also fail before execution if any selected
  tool lacks the required positive or negative security fixture.
  Remaining work: expand live secured-client CI environments.
- Add chaos tests for SIGKILL, network partition, S3 outage, clock skew, and
  slow/partial clients.
  Status: gated `test-chaos` starts real broker processes and now verifies
  SIGKILL/restart recovery from local WAL, continued service while slow or
  truncated clients hold partial frames, lenient handling of far-future client
  record timestamps without broker instability, and sync S3 WAL fail-closed
  startup behavior with a non-zero process exit against an unavailable
  object-store endpoint. The same harness supports explicit
  `ZMQ_CHAOS_NETWORK_DOWN`/`ZMQ_CHAOS_NETWORK_UP` hooks for CI jobs that can
  inject network partitions, and `ZMQ_CHAOS_S3_DOWN`/`ZMQ_CHAOS_S3_UP` hooks
  for live provider outage/heal gates. `test-s3-provider-matrix` can require
  that live-S3 outage chaos scenario per provider profile with
  `ZMQ_S3_<PROFILE>_RUN_LIVE_OUTAGE=1`; release jobs can now pin required live
  provider coverage with `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`, required
  outage-enabled profiles with `ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES`, and
  required process-crash/replacement profiles with
  `ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES`, required
  ListObjectsV2 pagination profiles with
  `ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES`, required multipart-edge
  profiles with `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES`, and
  required multipart-fault profiles with
  `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES`.
  `test-chaos` can require selected scenario coverage with
  `ZMQ_CHAOS_REQUIRED_SCENARIOS`, run scheduled named network-partition phases
  with `ZMQ_CHAOS_NETWORK_MATRIX`, override per-phase
  `ZMQ_CHAOS_NETWORK_<PHASE>_{DOWN,UP,EXPECT}` hooks, and fail release jobs
  when required network phases are missing through
  `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES`.
  `test-kraft-failover` can require controller/broker network-partition hooks
  with `ZMQ_KRAFT_NETWORK_DOWN` and `ZMQ_KRAFT_NETWORK_UP`, or scheduled named
  controller/broker partition phases with `ZMQ_KRAFT_NETWORK_MATRIX` and
  `ZMQ_KRAFT_NETWORK_<PHASE>_{DOWN,UP,EXPECT}` overrides, and can fail release
  jobs when required failover phases are omitted through
  `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES`. `test-e2e` can now require named
  Docker cross-broker chaos phases with `ZMQ_E2E_REQUIRED_CHAOS_PHASES` and
  per-phase `ZMQ_E2E_CHAOS_<PHASE>_{DOWN,UP,EXPECT}` hooks plus named Docker
  live load/scale phases with `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` and
  per-phase `ZMQ_E2E_LOAD_SCALE_<PHASE>_{APPLY,RESTORE}` hooks. Those hooks
  now receive the active topic plus broker, controller, metrics, container, and
  MinIO context, with deterministic default-suite self-test coverage.
  Remaining work: broader environment execution coverage for live provider
  outage profiles, scheduled partition matrices, and scheduled load/scale
  matrices.
- Add performance baselines for produce/fetch throughput, p99 latency, S3
  operations per MiB, recovery time, and memory growth.
  Status: `zig build bench` now compiles against the broker/storage/protocol
  modules and reports gated `PartitionStore` produce/fetch throughput plus p99
  latency, mock S3 WAL sync-produce latency, S3 request volume per MiB, and S3
  WAL metadata rebuild time. It also gates bounded-cache steady-state memory
  growth with the project's tracking allocator, and `ZMQ_RUN_BENCH_LIVE_S3=1`
  adds a live MinIO/S3 provider object put/get throughput, p99, and
  requests/MiB trend gate with CI-tunable threshold environment variables.
  Gated `bench-compare` now runs the existing ZMQ/Kafka/AutoMQ comparison
  harness only when `ZMQ_RUN_BENCH_COMPARE=1` is set, requires ZMQ plus at
  least one Kafka/AutoMQ baseline result for release-gate execution, can require
  exact selected/result targets with `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`, and
  enforces configurable `ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO`,
  `ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO`,
  `ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO`, and
  `ZMQ_BENCH_COMPARE_MAX_ERROR_RATE` thresholds. The default suite self-tests
  target parsing, required-target validation, ratio formatting, threshold
  parsing, missing-baseline detection, throughput/latency regression detection,
  and error-rate enforcement. Remaining work: CI execution of broader live
  comparative performance profiles.
- Add release criteria: no known data-loss bug, no advertised stub API, passing
  client matrix, passing MinIO/S3 matrix, and documented unsupported features.
  Status: `docs/RELEASE_CRITERIA.md` now pins required protocol, durability,
  stateless, multi-node, security, observability, performance, chaos, and
  comparative benchmark gates, lists the release commands, and documents
  currently unsupported/partial surfaces. The default production-readiness
  suite verifies the document keeps those gates, required provider/security
  profile variables, and unsupported surfaces explicit.

## Rules For Future Changes

- Generated schema support does not imply broker support.
- ApiVersions must be driven only by `broker_supported_apis`.
- Every new advertised API needs a handler, version/header mapping, unit tests,
  malformed-frame tests, and at least one integration/client test.
- AutoMQ extension APIs must use the generated schema keys: 501-519 and 600-602.
- If an API is intentionally single-node-only, document the degraded semantics
  and add a test proving the response is schema-compatible.

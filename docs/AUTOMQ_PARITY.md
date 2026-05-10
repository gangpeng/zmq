# AutoMQ Parity Roadmap

This document defines what "AutoMQ-complete" means for ZMQ and turns it into
tracked implementation gates. The target is open-source AutoMQ compatibility:
Kafka protocol behavior, AutoMQ S3Stream storage semantics, S3-backed durability,
stateless broker operation, cluster management, balancing, observability, and
operator-facing behavior.

## Current Baseline

- Zig toolchain target: Zig 0.16.0.
- Generated protocol request schemas: 110 entries in `src/protocol/api_support.zig`.
- Broker-advertised APIs: 95 entries in `api_support.broker_supported_apis`.
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
  through controller Raft metadata records and full snapshots. Non-leader
  controllers are live-probed for BrokerRegistration `NOT_CONTROLLER`
  rejection without registering synthetic brokers.
- Kafka protocol support is functional for common single-node broker paths, but
  semantic parity across all generated APIs and versions is incomplete.
- AlterReplicaLogDirs is now advertised and handled through generated request
  and response schemas, and the gated KRaft failover harness now live-probes
  its v2 partition directory mutation through controller failover and broker
  restart. AssignReplicasToDirs v0 is also live-probed through the same gate
  using the broker registration epoch and configured directory ID. Default S3
  replacement coverage now also verifies replica-directory assignment replay,
  generated DescribeLogDirs read-back, and replacement-side directory
  reassignment through another fresh-dir replay, and
  ElectLeaders v2 is live-probed for the active single-replica topic partition.
  CreateTopics v7 is live-probed with supported creation-time configs, exact
  DescribeConfigs read-back, validate-only creation checks that leave the probe
  topic absent, and default-suite generated DescribeTopicPartitions read-backs
  for local creation, rejection, and rollback paths.
  CreatePartitions v2 is live-probed with a real one-to-two partition expansion
  plus repeat validate-only checks and generated DescribeTopicPartitions topic
  metadata verification.
  DeleteTopics v6 is live-probed by deleting an isolated topic and verifying
  DescribeTopicPartitions reports it unknown plus repeat delete attempts remain
  non-resurrecting through failover checkpoints. Default S3 replacement
  coverage now also verifies DeleteTopics tombstone replay from a fresh local
  data dir, keeping the deleted topic, partition state, share state,
  reassignment, and replica-directory side state absent, then deletes a second
  replayed topic from the replacement broker and verifies both tombstones remain
  non-resurrecting through generated DescribeTopicPartitions unknown-topic and
  ListPartitionReassignments empty-state read-back plus another fresh-dir replay.
  AllocateProducerIds v0 is live-probed directly against the active controller
  and verifies non-overlapping monotonic PID blocks through failover
  checkpoints, while non-leader controllers are probed for `NOT_CONTROLLER`
  rejection without advancing PID state.
  AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter negative paths are live-probed
  on non-leader controllers for `NOT_CONTROLLER` rejection without mutating the
  voter set or endpoints.
  BrokerHeartbeat/UnregisterBroker and ControllerRegistration negative paths
  are also live-probed on non-leader controllers for `NOT_CONTROLLER`
  rejection without mutating broker registration or voter endpoint state.
  DescribeQuorum v2 is live-probed directly against the active controller and
  verifies controller listener endpoints plus voter directory IDs through the
  same failover checkpoints.
  FetchSnapshot v1 is live-probed directly against every alive controller and
  verifies request-scoped `SNAPSHOT_NOT_FOUND` responses plus current-leader
  endpoint routing metadata through failover checkpoints.
  Controller ApiVersions v3 is live-probed directly against every alive
  controller and verifies the audited controller catalog while keeping telemetry
  keys 71/72 absent through failover checkpoints.
  AlterClientQuotas/DescribeClientQuotas v1 are live-probed with a real client
  quota mutation plus repeat validate-only checks and exact quota visibility.
  Default S3 replacement coverage now also verifies client and default quota
  addition/update replay across explicit initial/replacement/continuation data
  dirs, with the replacement broker updating default rates, updating a replayed
  client quota, and adding another client quota before a second fresh-dir replay
  confirms DescribeClientQuotas visibility. It also verifies client and default
  quota removal replay from a fresh local data dir, keeping the removed client
  quota absent, retained client quota visible, and removed default quota rates
  reset to unlimited, then removes the retained client quota from the
  replacement broker and verifies both client quotas remain absent after another
  fresh-dir replay.
  AlterUserScramCredentials/DescribeUserScramCredentials v0 are live-probed
  with precomputed SCRAM-SHA-256 credential upsertion and exact mechanism and
  iteration visibility. Default S3 replacement coverage now also verifies SCRAM
  credential addition/update replay across explicit initial/replacement/
  continuation data dirs, with the replacement broker updating a replayed
  credential and adding another before a second fresh-dir replay confirms salts,
  stored/server keys, and DescribeUserScramCredentials visibility. It also
  verifies SCRAM credential deletion replay from a fresh local data dir, keeping
  the deleted credential absent while a retained credential remains usable, then
  deletes the retained credential from the replacement broker and verifies both
  deletions survive another fresh-dir replay.
  GetTelemetrySubscriptions/PushTelemetry/ListClientMetricsResources v0 are
  live-probed with a stable client instance id, accepted uncompressed sample,
  and resource-list visibility.
  Create/Renew/Expire/DescribeDelegationToken are live-probed with a durable
  token owned by the harness client and exact token-id/HMAC visibility. Default
  S3 replacement coverage now also verifies token creation replay, a
  replacement-side token creation, renewed token-expiry replay, a
  replacement-side renewal, and second fresh-dir replay of those updated states.
  It also verifies immediate token-expiry removal replay from a fresh local data
  dir while a retained token remains visible. The replacement broker now expires
  the retained token and another fresh-dir replay verifies both token removals
  remain absent.
  UpdateFeatures v1 plus ApiVersions v3 finalized-feature tagged fields are
  live-probed with a durable `metadata.version` finalization, validate-only
  checks, and exact finalized feature-level visibility. Default S3 replacement
  coverage now also verifies finalized-feature addition replay across explicit
  initial/replacement/continuation data dirs, with the replacement broker adding
  `kraft.version` after replay and a second fresh-dir broker verifying both
  finalizations plus the advanced epoch. It also verifies finalized-feature
  deletion replay from a fresh local data dir, keeping the deleted
  `metadata.version` finalization absent while retained `kraft.version` metadata
  survives, then deletes the retained finalization from the replacement broker
  and verifies both removals survive another fresh-dir replay.
  DescribeAcls/CreateAcls/DeleteAcls v2 are live-probed with an isolated
  broad-allow seed ACL plus a deleted topic ACL, including exact DescribeAcls
  visibility and absence checks. Default S3 replacement coverage now also
  rebuilds ACL state from a fresh local data dir after DeleteAcls and verifies
  the deleted ACL remains absent while a retained ACL is still authorized, then
  deletes the retained ACL from the replacement broker and verifies both ACL
  tombstones survive another fresh-dir replay.
  AlterConfigs v2 and IncrementalAlterConfigs v1 are live-probed against an
  isolated topic, including real config mutations, repeat validate-only checks,
  and exact DescribeConfigs read-back for cleanup policy, min.insync replicas,
  and segment bytes. Default S3 replacement coverage now also verifies
  generated AlterConfigs and IncrementalAlterConfigs set/update replay across
  explicit initial/replacement/continuation data dirs with generated
  DescribeConfigs visibility, plus AlterConfigs null-value reset and
  IncrementalAlterConfigs DELETE replay from a fresh local data dir, keeping
  deleted topic config overrides at their defaults through generated
  DescribeConfigs while retained overrides survive replay, then
  resets/deletes the retained `compression.type` override from the replacement
  broker and verifies another fresh-dir replay keeps all deleted overrides at
  defaults.
  Delegation-token APIs 38-41 are advertised with strict
  generated decoding, schema-shaped responses, and broker-local create,
  describe, renew, and expire semantics. Tokens persist across local broker
  restart through `delegation_tokens.meta` and replay from
  `__cluster_metadata` delegation-token snapshots after shared-storage broker
  replacement; malformed local delegation-token snapshots fail broker startup.
  SASL/SCRAM token authentication now accepts
  delegation-token IDs with HMAC secrets, maps authenticated sessions to the
  token owner principal, returns token-bound SASL session lifetimes, fails
  closed on bad proofs, expires token-authenticated sessions before later Kafka
  requests, and rejects delegation-token lifecycle APIs from token-authenticated
  sessions.
- Client telemetry subscription/push and client-metrics resource APIs now retry
  generated storage-error responses after transient response-allocation
  failures, and PushTelemetry pre-serializes success responses before
  recording/exporting telemetry so allocation pressure cannot acknowledge
  telemetry with no response frame.
- AlterClientQuotas now also pre-serializes successful mutation responses and
  retries generated storage-error responses so allocation pressure cannot
  apply quota state while returning no response frame.
- DescribeClientQuotas and DescribeUserScramCredentials generated error
  responses now also retry as storage errors after transient response
  allocation failures.
- AlterUserScramCredentials now pre-serializes successful mutation responses and
  retries generated storage-error responses so allocation pressure cannot change
  SCRAM credential state while returning no response frame.
- CreateAcls now pre-serializes successful mutation responses and retries
  generated storage-error responses so allocation pressure cannot add ACLs while
  returning no response frame.
- DeleteAcls now restores the pre-delete ACL snapshot and retries generated
  storage-error responses when allocation pressure prevents the success response
  from being serialized.
- UpdateLicense now pre-serializes successful mutation responses and retries
  generated storage-error responses so allocation pressure cannot change visible
  AutoMQ license metadata while returning no response frame.
- GetNextNodeId now pre-serializes successful mutation responses and retries
  generated storage-error responses so allocation pressure cannot advance the
  AutoMQ node-id cursor while returning no response frame.
- UpdateFeatures now pre-serializes successful mutation responses and retries
  generated storage-error responses so allocation pressure cannot finalize
  feature metadata while returning no response frame.
- S3 WAL/object storage paths exist, but full AutoMQ S3Stream lifecycle
  compatibility, crash recovery, fencing, and cross-provider validation are
  incomplete. Stream/object metadata now has local file snapshot/restart
  coverage, topic-level numeric configs and partition offset/HW/LSO state are
  snapshotted for local restart, and filesystem WAL segments replay into the
  fetch cache after broker restart and are visible through generated Fetch v12
  read-back while malformed WAL segment names and malformed or corrupt
  partition WAL records fail broker open instead of being skipped. Existing
  syntactically or semantically malformed broker-local
  metadata snapshots for topics,
  offsets, consumer groups, transactions, producer sequences, partition state,
  reassignments, replica-directory assignments, share-group state/session
  epochs, delegation tokens, finalized features, ACLs, AutoMQ metadata, and
  object/prepared registries now fail closed during load instead of partially
  skipping state or returning empty defaults.
  Persisted committed-offset restore now also fails closed on malformed offset
  keys instead of skipping them, and preserves valid topic names that contain
  colons, with the restored offset verified through generated OffsetFetch v8
  read-back. All-topic committed-offset enumeration now also fails closed when
  a matching in-memory coordinator key is malformed, while still preserving
  valid topic names that contain colons. Internal `__consumer_offsets` replay now
  fails closed on malformed committed-offset values for parseable offset keys
  instead of silently omitting those offsets during rebuild.
  Internal compacted-topic log compaction now fails closed on cache allocation
  failures and malformed Kafka record-batch headers instead of skipping corrupt
  internal batches.
  Stateful coordinator mutation APIs now fail closed on malformed frames,
  response materialization failures, rollback snapshot failures, and final
  serialization failures across committed-offset commits, transactional
  coordinator mutations, TxnOffsetCommit, and DeleteGroups. These paths
  pre-materialize response state before local mutation and restore transaction,
  group, share-session, and committed-offset snapshots on persistence failures
  so stateless broker replacement does not observe partially applied
  coordinator state.
  Broker-owned internal log replay for `__cluster_metadata`,
  `__consumer_offsets`, and `__transaction_state` now rejects malformed headers,
  truncated records, trailing bytes, null internal record keys, and invalid
  snapshot tombstones instead of returning partial replay success.
  Produce requests with full-size malformed Kafka record-batch headers or
  invalid batch-length envelopes now return `CORRUPT_MESSAGE` without
  appending data.
  Idempotent Produce now reserves producer-sequence state before append so
  allocation failure returns `KAFKA_STORAGE_ERROR` without advancing offsets or
  acknowledging data with stale deduplication state.
  Producer-sequence recovery from durable user logs now fails closed on
  malformed record-batch envelopes and advances one logical offset at a time so
  short raw legacy/test records do not hide later idempotent batches.
  Persisted topic and partition-state restore now propagates local partition
  storage rebuild failures instead of logging and continuing with visible
  metadata ahead of local storage, and filesystem WAL cleanup/retention now
  fails closed without dropping segment metadata when a segment cannot be
  deleted.
  Auto-created and internal topic creation now fails closed and rolls back
  visible topic metadata when partition-state allocation or local failover
  ownership tracking fails instead of advertising a topic without local
  partition state or owner metadata.
  Failover and reassignment ownership transfers now reserve target bookkeeping
  before removing the prior owner, so allocation pressure cannot leave a
  partition ownerless.
  Consumer-group session-timeout eviction no longer allocates while scanning
  expired members, so allocation pressure cannot silently leave timed-out
  members active or suppress the required rebalance. Controller broker-heartbeat
  eviction, consumer-group heartbeat/rebalance timeouts, AutoMQ quorum waits,
  and transaction timeout/auto-abort gates now use monotonic runtime baselines;
  recovered consumer-group and transaction snapshots reset runtime timers while
  Kafka-visible transaction start times and record timestamps remain wall-clock
  values. Compaction S3 read faults now propagate the original storage error
  instead of being collapsed into object-not-found while leaving ObjectManager
  metadata untouched.
  Raft/controller metadata append paths now fail closed when leader or follower
  log persistence fails, follower AppendEntries rejects allocation failures and
  non-contiguous entries instead of reporting success, and controller startup
  rejects truncated, invalid, or non-contiguous persisted Raft log records
  instead of recovering a partial metadata image. Raft heartbeat broadcasts now
  report and log failed peer RPCs instead of silently swallowing quorum
  communication errors, and controller Vote/BeginQuorumEpoch/EndQuorumEpoch
  v1 leader-endpoint materialization failures now return storage errors instead
  of successful responses with missing endpoint metadata. Committed Raft voter
  config records now fail closed on malformed endpoint metadata or application
  errors instead of marking the config offset applied with partial voter state. Raft
  snapshot compaction now fails closed before log truncation when
  `snapshot.meta` or `prepared.snapshot` cannot be persisted, and malformed
  `raft.meta`, `snapshot.meta`, or unreadable/malformed `prepared.snapshot`
  rejects startup recovery. Raft epoch/vote metadata writes now use an
  fsynced temporary file plus rename, self-elections fail closed when
  `raft.meta` cannot be persisted, vote requests are denied and rolled back on
  persistence failure, AppendEntries leader epochs are rejected until durable,
  and higher-epoch follower responses force an in-memory step-down even when
  the metadata write fails. Election-loop stale Raft peer cleanup no longer
  allocates while reconciling committed voter metadata, so allocation pressure
  cannot leave removed peers in the RPC client pool.
  Controller quorum startup now reads Kafka-style `controller.quorum.voters`
  from config files when CLI voters are absent and rejects malformed voter
  entries instead of silently omitting controller, metadata-client, or broker
  peer endpoints; controller-role processes also reject voter sets that omit the
  local `node.id`. Startup now also fails closed on unreadable config files,
  invalid cluster-critical integer settings, negative node IDs, malformed
  `process.roles`, invalid S3 scheme/path-style settings, invalid S3 WAL flush
  modes, missing CLI flag values, and unknown CLI arguments instead of silently
  falling back to default node identity, S3 defaults, or listener ports.
  If the local object snapshot is absent, flushed S3 WAL objects can rebuild
  ObjectManager stream-set metadata from their object indexes.
  Latest default-suite protocol tranche: generated AddPartitionsToTxn v3/v4,
  AddOffsetsToTxn v2/v4, InitProducerId v1/v5, EndTxn v2/v4,
  WriteTxnMarkers v0/v1, FindCoordinator v2/v4, ListOffsets v0/v7,
  BrokerRegistration v2, BrokerHeartbeatResponse v1, ControllerRegistration
  v0, AddRaftVoter v0, RemoveRaftVoter v0, UpdateRaftVoter v0,
  DescribeClientQuotas v1, AlterClientQuotas v1,
  DescribeUserScramCredentials v0, AlterUserScramCredentials v0,
  DescribeAcls/CreateAcls/DeleteAcls v2,
  OffsetForLeaderEpoch v4, DeleteRecords v2,
  SaslHandshake v1, SaslAuthenticate v2,
  JoinGroup v5/v9, SyncGroup v3/v5, Heartbeat v3/v4, LeaveGroup v2/v5,
  ListGroups v2/v5, DescribeGroups v4/v5,
  GetTelemetrySubscriptions/PushTelemetry/ListClientMetricsResources v0,
  AlterConfigs v0/v2, IncrementalAlterConfigs v0/v1,
  DeleteTopics v3/v6, DeleteGroups v0/v2,
  DescribeCluster v1, DescribeQuorum v2, UpdateFeatures v0/v1,
  UnregisterBroker v0, AllocateProducerIds v0,
  AssignReplicasToDirs v0, ShareFetch/ShareAcknowledge v0,
  ConsumerGroupHeartbeatResponse v0, ShareGroupHeartbeatResponse and
  ShareGroupDescribe request/response v0,
  ElectLeadersResponse v1/v2, Alter/ListPartitionReassignmentsResponse v0,
  StopReplicaResponse v1/v2, DescribeLogDirsResponse v1/v4,
  UpdateMetadataResponse v5/v6,
  ApiVersionsRequest v3, VoteRequest v1, BeginQuorumEpochRequest v1,
  DescribeTopicPartitionsRequest v0,
  Initialize/Read/Write/DeleteShareGroupState and ReadShareGroupStateSummary
  v0, ConsumerGroupDescribe v0, OffsetCommit v1/v8, OffsetFetch v7/v8,
  TxnOffsetCommit v3, DescribeProducers v0, CreatePartitionsResponse v2,
  OffsetDelete v0, DescribeTransactions v0, ListTransactions v1,
  AutoMQ Get/Put/DeleteKVs v0, GetNextNodeId v0, and
  Update/DescribeLicense v0,
  Create/Open/Close/Delete/Trim/DescribeStreams v0/v1,
  PrepareS3Object v0, CommitStreamSetObject v1, CommitStreamObject v1,
  GetOpeningStreams v0, AutomqRegisterNode/GetNodes v0,
  AutomqZoneRouter v1, AutomqGetPartitionSnapshotRequest v2,
  ExportClusterManifest v0, AutomqUpdateGroup v0, MetadataResponse v10,
  ControlledShutdown v3, AlterReplicaLogDirs v2, delegation-token APIs
  v2/v3, AlterPartitionResponse v3, and Envelope v0
  request/response golden fixtures now pin schema-shape transitions, nested
  compact arrays, nullable compact strings, UUID fields, topic-admin legacy and
  flexible responses, producer-state introspection, share
  acknowledgement/state batches, commit timestamps/member epochs/static member
  identity, offset-fetch grouped response transitions, transactional identity,
  PID recovery, commit/abort markers, coordinator batched lookup results,
  old-style offset arrays, leader-epoch timestamp offsets, controller/broker
  lifecycle UUID/listener/feature metadata, voter endpoint/KRaft-version
  mutations, compact quota entity/operation arrays, nullable default quota
  entities, f64 quota values, SCRAM credential mechanism/iteration lists,
  compact salt/password bytes, ACL filters/resources/matching results,
  leader-epoch end offsets, delete-records low-watermarks, compact record
  bytes, SASL mechanism arrays/auth bytes/session lifetime, classic group
  membership metadata/assignments/static identities/state/type filters,
  consumer/share heartbeat assignment responses, share group member/assignment
  describe views,
  election response legacy/flexible errors, reassignment per-partition errors
  and replica deltas,
  stop-replica partition errors, log-directory capacity/future-replica state,
  update-metadata response tagged terminators,
  client software identity, KRaft voter directory IDs, leader endpoint
  announcements, describe-topic-partitions cursors,
  telemetry UUID/compression/metric-prefix/payload resource fields,
  config admin null values/operation types, topic deletion name-to-UUID
  transitions, group deletion compact arrays,
  cluster endpoint/rack metadata, KRaft quorum directory/listener state,
  feature update downgrade/upgrade gates, controller PID allocation ranges,
  transaction filters/results, flexible encodings, and tagged-field
  terminators.

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
| P0 | Advertised API audit | ApiVersions, request/response header versions, generated schema ranges, dispatch coverage, malformed-frame tests, golden fixtures, and client compatibility matrix all fail closed when an advertised API drifts. | In progress. Header flexible-version and handler-switch coverage are explicit and tested; source-level tests now parse `generated_index.zig` and the real broker/controller `handleRequest` switches so generated schema counts and dispatch cases cannot drift from the audit tables silently. The protocol static audit now self-tests strict JSON parsing in both checked-in schema generators, rejecting non-standard schema constants such as `NaN`, `Infinity`, and `-Infinity` plus duplicate JSON object keys before generated Zig protocol schemas are written, and it verifies codegen exits nonzero on schema parse errors. Source-level generated-schema audits now pin all 20 nullable-array fields to optional-slice null/empty preservation paths and all 23 currently modeled tagged fields to schema-visible encode/decode/duplicate-tag coverage, so protocol regeneration drift fails the default Zig suite. Default generated-message round-trip smoke tests now cover all 230 top-level generated request/response/header/record types across common protocol versions 0-20, and non-default golden wire fixtures now cover every currently advertised generated module in `api_support.zig`, including all AutoMQ extension keys 501-519/600-602 plus MetadataResponse, ControlledShutdown, AlterReplicaLogDirs, delegation-token APIs, AlterPartitionResponse v3, and Envelope. AlterPartitionResponse v2+ now correctly omits legacy `TopicName` and uses `TopicId`-only topic grouping. Manual ApiVersions v3+ response encoding now round-trips KRaft feature metadata tagged fields (`SupportedFeatures`, `FinalizedFeaturesEpoch`, `FinalizedFeatures`, `ZkMigrationReady`) and preserves unknown response tags without duplicating known tags after decode. Controller ApiVersions now uses a controller-specific support catalog and advertises generated ControllerRegistration key 70 plus AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter keys 80/81/82 instead of telemetry keys 71/72; the live failover gate also verifies the exact controller catalog remains stable and telemetry keys 71/72 stay absent on the active controller through controller failover/restart checkpoints. UpdateRaftVoter validates generated requests and appends replayable Raft config records for voter endpoint metadata instead of failing closed. ConsumerGroupHeartbeat key 68 is now advertised with generated v0 flexible framing, persisted KIP-848 member epochs, static-member fencing, cooperative range assignment revocation, stored owned-partition echoes, schema-shaped authorization denial, and local/S3 consumer-group snapshot rollback; AssignReplicasToDirs key 73 is now advertised with generated v0 framing, configured logical multi-directory target validation, local/S3 assignment snapshot rollback and replay from `__cluster_metadata`, schema-shaped authorization denial, and DescribeLogDirs partition mirroring across assigned directories; ShareGroupHeartbeat/ShareGroupDescribe keys 76-77, ShareFetch/ShareAcknowledge keys 78-79, and Initialize/Read/Write/DeleteShareGroupState plus ReadShareGroupStateSummary keys 83-87 are now advertised with generated v0 flexible framing, local/S3 durable share-group/session/state snapshots through `__consumer_offsets`, share-session epoch restore after fresh-dir replacement, strict malformed-frame coverage, schema-shaped authorization denial, and rollback on local or shared persistence failures. Advertised broker and controller APIs now have catalog-level `max_version + 1` fail-closed coverage before body decode. ApiVersions catalog fixtures plus AddPartitionsToTxn, AddOffsetsToTxn, AlterClientQuotas, AlterUserScramCredentials, AssignReplicasToDirs, ConsumerGroupDescribe, ConsumerGroupHeartbeat, ControllerRegistration, CreateTopics including validated supported creation-time configs, DeleteTopics, DeleteRecords, DeleteShareGroupState, DescribeClientQuotas, DescribeConfigs, DescribeGroups, DescribeTopicPartitions including cursor pagination, DescribeTransactions, DescribeUserScramCredentials, EndTxn, Fetch, FetchSnapshot, FindCoordinator including invalid coordinator-type, empty-key, and unimplemented share-coordinator negatives, GetTelemetrySubscriptions, Heartbeat, InitProducerId, InitializeShareGroupState, JoinGroup, LeaveGroup, ListClientMetricsResources, ListGroups including v5 group-type filters, ListOffsets, ListTransactions, Metadata, Produce, PushTelemetry, ReadShareGroupState, ReadShareGroupStateSummary, SaslHandshake, SaslAuthenticate, ShareAcknowledge, ShareFetch, ShareGroupDescribe, ShareGroupHeartbeat, SyncGroup, UnregisterBroker, UpdateFeatures, UpdateRaftVoter, OffsetCommit, OffsetFetch, OffsetForLeaderEpoch, WriteShareGroupState, WriteTxnMarkers, TxnOffsetCommit, DescribeAcls/CreateAcls/DeleteAcls, AlterConfigs/IncrementalAlterConfigs, CreatePartitions, Vote, Begin/EndQuorumEpoch, and OffsetDelete generated/malformed coverage are in place. Gated `test-client-matrix` now probes metadata, topic admin where available, produce/fetch, and committed offsets with installed external clients (`kcat`, Kafka CLI, `kafka-python`, `confluent-kafka`, Java `kafka-clients`, and Go `kafka-go`) against a running broker and supports named version profiles with per-profile bootstrap, tool, Java classpath, Go module, Python interpreter, semantic-suite, TLS, SASL/OAuth, and exact version labels. Release jobs can require named profiles, exact version-pinned profiles, secured-client profiles, and negative-security profiles before probes run, and the matrix can require real-client admin, consumer-group, rebalance, transactional, secured-client, bad-SASL, bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce probes in addition to default metadata/produce/fetch/offset checks; required secured/OAuth profiles now fail before execution if selected tools cannot run the requested secured positive/negative fixtures. Broader live client execution remains. |
| P0 | Quorum-backed AutoMQ/controller metadata | Replace local-only AutoMQ KV/node/router/license/manifest/group/object metadata with quorum-backed records, snapshots, replay, fencing, and failover tests. | In progress. KV/node/router/license/node-id/group mutations now append committed Raft records for attached leaders. AutoMQ node registration tags are now preserved through AutomqRegisterNode/GetNodes, local `automq.meta` v2 snapshots, rollback snapshots, committed register-node records, replay, and full-snapshot-v2 compaction. AutoMQ stream tags from Create/Open/DescribeStreams are now preserved through ObjectManager v3 snapshots, local restart, rollback snapshots, committed object snapshot records, full-snapshot compaction, AppendEntries replay, follower promotion, and the gated failover harness. Local-mode PutKVs/DeleteKVs, AutomqRegisterNode, AutomqZoneRouter, UpdateLicense, GetNextNodeId, and AutomqUpdateGroup now fail closed and roll back visible metadata state when the AutoMQ metadata snapshot cannot be written, and they return generated storage errors without visible state changes when local copies, rollback snapshots, or map capacity reservations cannot be materialized before mutation. ObjectManager stream/object mutations now append committed Raft snapshot records; Create/Open/Close/Delete/TrimStreams, PrepareS3Object, CommitStreamObject, and CommitStreamSetObject now fail closed with generated storage errors and roll back visible ObjectManager state when rollback snapshots, local materialization, local mutation, quorum append, or local object snapshot writes fail. Both paths replay on broker open and reject attached non-leader KV/node/router/license/group plus stream/object mutations with generated non-mutation read-backs and cursor non-advance checks. Single-node leaders now append a full AutoMQ metadata/ObjectManager snapshot record before Raft snapshot truncation and replay it after compaction. Internal AppendEntries now carries log entry bytes; followers apply committed AutoMQ metadata/object snapshot records on AppendEntries and replay them after promotion; leaders also wait briefly for post-commit propagation so a promoted follower does not reuse a stale node-id allocator cursor. Combined controller+broker multi-process failover now covers AutoMQ KV put/get/delete, zone router, node registry including tag clearing, license, node-id allocator, group promote/demote, stream create/prepare/commit/open/close/trim/delete, stream tag replacement and clearing, stream-set object commit, strict JSON manifest stream/group-count probes including duplicate-key rejection, partition-snapshot protocol smoke, and stream metadata replication, leader kill, replacement-leader mutation, and old-leader restart/rejoin through gated `test-kraft-failover`. The default Zig suite now includes topic-backed partition snapshot fixtures. Broader client compatibility remains. |
| P0 | S3/MinIO crash and fault-injection harness | Exercise produce/flush/restart/fetch/rebuild with transient 5xx, timeout, partial multipart, bad ETag, checksum, range-read, list inconsistency, and provider-specific behavior. | In progress. Local MockS3 fault injection now covers bounded put retry, get/list/range/delete failures, temporary list omission, recovery retry, fetch failure, compaction orphan retry, multipart bad/missing/XML-unsafe part ETags, XML-escaped upload IDs, complete failure, embedded complete errors including chunked responses with split XML tags, exact range-window validation including high-level `206 Content-Range` mismatch/missing-header rejection, SHA-256 checksum-header mismatch rejection for S3 GET/range bodies, abort verification, broker stateless replacement, coordinator snapshot upload failure, and S3 WAL resume without object-key overwrite. Gated `test-minio` covers live object, multipart, S3 WAL rebuild/fetch, and PartitionStore S3 WAL produce/rebuild/resume against MinIO/S3. Gated `test-s3-process-crash` starts a real broker process, kills it after acknowledged S3 WAL produce and OffsetCommit, restarts a replacement with a fresh local data dir, fetches data and committed offsets from S3, and appends new data; the local live gate now passes with single-node controller election completed before request serving and valid Kafka v0 MessageSet produce records in the harness. Gated `test-s3-provider-matrix` can now run the live MinIO suite across named S3-compatible provider profiles with per-profile endpoint, port, bucket, credential, scheme, region, TLS CA, path-style, existing-bucket, process-crash/replacement overrides, live-S3 outage chaos hooks that verify produce fails closed during provider isolation and recovers after heal, provider-specific multipart-fault commands with the same credential/scheme/region/path-style/TLS context as the selected live provider profile, command-owned injected/recovered evidence before the release marker, and release-job validation for required provider, outage, process-crash/replacement, ListObjectsV2 pagination, multipart-edge, and multipart-fault profile coverage; the local MinIO profile now passes with required provider, ListObjectsV2 pagination, multipart-edge, and process-crash/replacement coverage enabled. S3 request signing now honors configured regions, virtual-hosted addressing, and default HTTP/HTTPS Host-header port elision while preserving explicit custom ports. Broader live execution of provider-specific multipart fault scripts remains. |
| P1 | Multi-node KRaft and broker lifecycle | Broker registration, heartbeat, fencing/unfencing, controller failover, rolling restart, and leader epoch behavior are covered by three-node tests. | In progress. Controller Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat, UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter, and RemoveRaftVoter now use generated schemas on the controller port; BrokerRegistration now advertises v2 when a broker has local log-directory IDs, persists those IDs in controller metadata records, replays them after controller restart/follower promotion, and preserves them in controller full snapshots; BrokerHeartbeat now advertises v1, records valid offline log-directory reports, rejects zero/unknown/duplicate directory IDs, honors broker-requested fencing/shutdown, and fences brokers whose registered directories are all offline; AddRaftVoter persists voter directory/listener metadata through replayable Raft config records when clients supply endpoints. ControllerRegistration now persists accepted listener and `kraft.version` metadata through the same replayable voter endpoint records. The election loop now reconciles the Raft RPC client pool from committed voter endpoint metadata before votes, heartbeats, and AppendEntries, including endpoint updates and removed voters. Controller startup registers configured voters before replaying persisted logs, and idempotent voter registration preserves replayed endpoint metadata. Controller tests are part of the default Zig test suite. Gated `test-kraft-failover` now starts three controller-only processes plus a broker-only process, can run CI-provided controller/broker network-partition hooks or scheduled `ZMQ_KRAFT_NETWORK_MATRIX` phases before the leader-kill path, kills the discovered controller leader, verifies surviving controllers elect a replacement, restarts the killed old leader with a fresh local data dir under the replacement leader, restarts a surviving controller, restarts the broker with the same data dir, and verifies the broker can produce and fetch acknowledged records throughout; the local gate has been re-executed successfully after adding live DescribeCluster endpoint/configured cluster-id coverage, CreateTopics configured creation/validate-only coverage, CreatePartitions expansion coverage, DeleteTopics deleted-topic absence coverage, finalized feature mutation/read-back coverage, ACL admin mutation/visibility coverage, config admin mutation/read-back coverage, client quota mutation/visibility coverage, ElectLeaders coverage, AlterReplicaLogDirs/AssignReplicasToDirs mutation coverage, direct AllocateProducerIds monotonic block coverage, direct DescribeQuorum v2 endpoint/directory coverage, direct FetchSnapshot v1 routing coverage, direct controller ApiVersions catalog coverage, and broker-port generated non-broker API rejection coverage. Broker registrations, broker unregistrations, broker rack metadata, log-directory IDs, offline log-directory health, and producer-id allocations now append controller metadata records, followers persist replicated AppendEntries, controller startup replays durable records from Raft, promoted followers can heartbeat brokers without forced re-registration or producer-id reuse, and controller full snapshot records preserve broker/rack/log-dir/PID state across Raft log compaction. The same failover gate now calls controller ApiVersions v3, AllocateProducerIds v0, DescribeQuorum v2, and FetchSnapshot v1 against the active controller at each transition and generated non-broker APIs against the broker port, verifying exact controller API visibility, non-overlapping monotonic PID blocks, controller listener endpoints/voter directory IDs, current-leader snapshot routing metadata, and broker-port fail-closed routing through controller leader kill, old-leader fresh rejoin, surviving-controller restart, and broker restart. Local FailoverController ownership metadata now tracks broker topic create/delete/create-partition/restore paths and transfers tracked partitions from fenced timed-out brokers to the surviving broker in default tests. Raft log recovery now replays append-only follower conflict truncation/replacement records instead of treating later lower offsets as corruption, and a rebuilt Docker E2E repeat-restart run passed `53/53` with node0 restarted by both `scale-in` and `scale-out`. Broader failover gates remain. |
| P1 | Stateless broker replacement | A replacement broker can rebuild state from shared storage and quorum metadata without local disk metadata or manual repair. | In progress. S3/object repair exists, committed offsets are now written to versioned `__consumer_offsets` records and replayed from recovered S3 WAL objects, OffsetDelete and DeleteGroups now write `__consumer_offsets` tombstones before local offset removal and replay them during replacement, consumer group lifecycle snapshots plus DeleteGroups share-session cleanup snapshots are now written to `__consumer_offsets` and replayed from S3 WAL, transaction coordinator snapshots are now written to `__transaction_state` and replayed from S3 WAL, atomic EndTxn and WriteTxnMarkers S3 WAL objects now restore both commit/abort marker partition offsets and completed transaction snapshots after fresh-dir replacement, client-facing group/transaction mutations fail closed when coordinator snapshot S3 WAL writes or post-marker local checkpoints fail, topic IDs, partition counts, common topic configs (`retention.*`, `max.message.bytes`, `min.insync.replicas`, `segment.bytes`, `cleanup.policy`, `compression.type`), finalized feature snapshots including deletion replay, DeleteRecords/retention low-watermark snapshots, replica-directory assignment snapshots, ongoing partition reassignment snapshots plus replacement-side local-failover completion checkpoints with generated ListPartitionReassignments and DescribeTopicPartitions read-back, client quota, SCRAM credential, ACL, delegation-token lifecycle snapshots including renew/expire replay, broker-only AutoMQ KV/node/router/license/node-id/group snapshots including KV deletion, node tag clearing, and group demotion replay, broker-only AutoMQ stream/object metadata snapshots including stream deletion, stream tag clearing, prepared object TTL replay/expiry, stream and stream-set mark-destroyed object state/deletion readiness, stream-set object ranges, and post-snapshot S3 WAL refresh, and CreateTopics/CreatePartitions initial non-local assignment ownership snapshots are now written to `__cluster_metadata`, replayed from recovered S3 WAL, and verified through generated DescribeTopicPartitions read-back, idempotent producer sequence state is rebuilt from durable log batches after S3 WAL replacement, and the combined controller+broker failover gate now restarts the killed AutoMQ leader after deleting its local data dir and verifies it rebuilds quorum-backed AutoMQ metadata from Raft. Broader broker-only metadata replacement remains. |
| P1 | Real reassignment and autobalancing | Partition movement, rack-aware placement, load convergence, and scale in/out semantics are implemented and tested under load. | In progress. Handler compatibility exists for reassignment APIs; CreateTopics and CreatePartitions now accept explicit single-replica broker assignments, install remote owners into failover metadata, persist those non-local initial owners through the existing reassignment snapshot path, replay them after fresh-dir S3 WAL replacement, and fail closed with rollback if assignment metadata cannot be written. AlterPartitionReassignments now tracks ongoing reassignments, applies the target owner into local failover ownership metadata, persists and replays ownership across local broker restart and fresh-dir S3 WAL replacement through `__cluster_metadata`, writes committed topic and reassignment snapshot records through Raft when a quorum is attached, fails closed with `NOT_CONTROLLER` on attached non-leaders and storage errors when shared snapshots cannot be written, ListPartitionReassignments returns requested ongoing state, cancel clears persisted/quorum state and restores local ownership, committed quorum replay restores topic metadata before assignment/cancellation ownership, and Metadata/DescribeTopicPartitions plus Produce/Fetch now respect non-local owners. The auto-balancer has a rack-aware planning path with deterministic coverage for cross-rack target preference and same-rack fallback, ignores stale unknown-node load samples when computing known-node averages, clamps negative metric rates to zero, persists broker rack metadata from controller registrations through Raft records/snapshots, can build controller-aware plans from active unfenced brokers that move load off fenced/scale-in leaders before normal rack-aware balancing, and the broker can execute validated plan moves through the durable reassignment path while rejecting stale or duplicate plans before mutation. Broker-side controller-aware orchestration now computes from controller broker snapshots plus load samples, applies moves through durable reassignments, returns planned/applied counts, fails closed on stale plans without partial mutation, no-ops without active targets, and can run automatically from broker `tick()` when cached controller/load samples are due; default tests cover elapsed-interval execution and interval-based skip behavior. The gated KRaft failover harness now includes a live broker-process reassignment check that creates a topic on the current controller leader, alters ownership to the next broker id, observes ListPartitionReassignments plus Metadata leader convergence on both old owner and target, verifies old-owner Produce is fenced, and verifies target-broker Produce/Fetch succeeds after topic-metadata quorum replay; Docker E2E can now satisfy required load/scale phases with the built-in fixture via `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1`, infers fixture phases from the required list when no explicit matrix is set, verifies hook-owned apply/restore marker payloads before printing phase success, and has clean `53/53` runs with dynamic reassignment source selection plus required fixture-backed `load`, inferred `scale-in`, and fixture-prepared `scale-out` phase coverage. Broader live load/scale orchestration and cross-broker chaos coverage remain. |
| P1 | Consumer group and transaction coordinator failover | Rebalances, offset lifecycle, transactions, idempotent producers, fencing, and coordinator migration survive restart and failover. | Partial single-node coordinator behavior. OffsetCommit, TxnOffsetCommit, and OffsetDelete now roll back local committed-offset visibility and return per-partition storage errors when the local offset snapshot cannot be written; OffsetDelete and DeleteGroups now write and replay `__consumer_offsets` tombstones and fail closed on shared tombstone write failure; DeleteGroups, JoinGroup, LeaveGroup, and SyncGroup roll back local group visibility when lifecycle snapshots cannot be written; DeleteGroups also rolls back share-session cleanup when the session snapshot cannot be written. Gated `test-kraft-failover` now validates OffsetCommit plus legacy and grouped OffsetFetch v8 durability, OffsetCommit/OffsetFetch v9 KIP-848 member-identity errors, OffsetDelete and DeleteGroups tombstone retention, classic JoinGroup/SyncGroup/Heartbeat/DescribeGroups plus generated ConsumerGroupDescribe/ListGroups/FindCoordinator continuity, KIP-848 ConsumerGroupHeartbeat join/assignment/owned-assignment/subscription-update/duplicate-subscription/unsupported-assignor/heartbeat/rack/leave/rejoin/static-rejoin and ConsumerGroupDescribe member/subscription/assignment introspection continuity, ShareGroupHeartbeat join/heartbeat/rack metadata plus ShareGroupDescribe member/subscription/assignment continuity, ShareFetch session open/record acquisition, ShareAcknowledge accept-range mutation, share-state Initialize/Write/Read/Delete/Summary continuity, and share-session epoch continuity, ListOffsets/OffsetForLeaderEpoch log-position visibility, DeleteRecords low-watermark visibility, CreatePartitions expansion plus validate-only continuity, DescribeTopicPartitions generated topic metadata visibility, DescribeConfigs topic-config visibility, DescribeLogDirs topic/partition log-dir visibility, AlterReplicaLogDirs and AssignReplicasToDirs partition-directory mutations, ElectLeaders preferred-election responses, DescribeCluster endpoint/configured-cluster visibility, InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/TxnOffsetCommit/EndTxn commit and abort continuity, DescribeTransactions/ListTransactions transaction-introspection continuity, and idempotent Produce v9 duplicate suppression, DescribeProducers visibility, next-sequence progress, InitProducerId epoch-bump fencing, and next-epoch recovery through controller leader kill, replacement election, old-leader fresh rejoin, surviving-controller restart, and broker restart. |
| P2 | Security gates | TLS/SASL/OAuth/SCRAM/ACL interop, negative cases, cert rotation, and authz coverage exist for every advertised API. | In progress. TLS/SASL/OAuth/SCRAM/ACL components exist. TLS config now fails closed for unsupported JKS keystore/truststore fields, inverted protocol-version ranges, and mTLS client-auth without CA trust anchors instead of silently starting with incomplete verification; outbound TLS client config also rejects unsupported JKS/truststore fields, inverted protocol-version ranges, partial client cert/key pairs, and hostname-aware handshakes when OpenSSL hostname verification cannot be enabled. Broker ACL resource/operation mapping is now audited against every advertised broker API except pre-auth SASL handshake/authenticate, missing mappings for DeleteRecords, OffsetForLeaderEpoch, AddOffsetsToTxn, ACL admin APIs, quorum APIs, DescribeCluster, and DescribeProducers are covered in the default Zig suite, Produce/Fetch now use generated per-partition authorization denials with exact flexible-version topic ACL extraction, ApiVersions/Metadata/CreateTopics/DeleteTopics, DescribeConfigs/AlterConfigs/IncrementalAlterConfigs, DescribeLogDirs/CreatePartitions/ElectLeaders/AlterPartitionReassignments/ListPartitionReassignments, DescribeClientQuotas/AlterClientQuotas/DescribeUserScramCredentials/AlterUserScramCredentials, DescribeAcls/CreateAcls/DeleteAcls including local/shared ACL snapshot rollback, quorum Vote/BeginQuorumEpoch/EndQuorumEpoch, DescribeQuorum/DescribeCluster/UpdateFeatures/ListTransactions/DescribeTransactions, telemetry/client-metrics GetTelemetrySubscriptions/PushTelemetry/ListClientMetricsResources, DescribeTopicPartitions, InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/WriteTxnMarkers/TxnOffsetCommit, topic-scoped ListOffsets/DeleteRecords/OffsetForLeaderEpoch/DescribeProducers, group-introspection ListGroups/DescribeGroups/ConsumerGroupDescribe, coordinator/session FindCoordinator/JoinGroup/ConsumerGroupHeartbeat/Heartbeat/LeaveGroup/SyncGroup, offset OffsetCommit/OffsetFetch, group/offset deletion DeleteGroups/OffsetDelete, and all AutoMQ extension keys 501-519/600-602 authorization denials now return generated schema-compatible responses, and unsupported versions are rejected before ACL denial builders can serialize unsupported schemas. TLS contexts now fingerprint configured cert/key/CA PEM files after load and reload the OpenSSL context before accepting new TLS connections when any configured PEM file is rotated or deleted. mTLS principal extraction preserves the default `User:<CN>` behavior and supports strict Kafka-style `ssl.principal.mapping.rules` for common `RULE:.../[LU]` plus `DEFAULT` DN mapping, including up to nine replacement captures and common capture character classes, with unsupported mapper syntax rejected during TLS config validation. SASL-enabled brokers now reject non-auth APIs until a client completes SASL authentication while keeping ApiVersions/SaslHandshake/SaslAuthenticate available for negotiation; default negative coverage pins that ACL denial cannot block pre-auth SASL frames, unsupported versions reject before SASL/authz gates, SaslAuthenticate fails closed when no enabled mechanism was negotiated or a negotiated mechanism is later disabled, OAuthBearer principal extraction frees decoded JWT state with leak-checked missing-subject coverage, and OAuthBearer validation accepts array-valued `aud` claims while rejecting future `nbf` tokens and tokens without `exp` before principal extraction. The real-client matrix now has configurable bad-SASL, bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce negative vectors for supported secured clients, with deterministic self-test coverage in the default build. Broader OAuth provider interoperability and live secured-client CI profiles remain. |
| P2 | Observability gates | Metrics, structured logs, readiness/liveness, dashboards, and alertable SLOs match the documented operational contract. | In progress. Metrics and JSON logging exist; `/health` and `/ready` now share a deterministic routing/response contract across plain and TLS metrics transports, including exact-path matching plus startup and shutdown readiness transitions in the default Zig suite. `/metrics` now returns a shaped HTTP 500 response when Prometheus export or response allocation fails instead of silently closing the scrape, and Prometheus export frees partially built buffers on allocation failure. Broker metric registration is audited against the advertised broker API metric catalog and includes produce/fetch throttle counters so request metrics are not silently dropped. Client-metrics APIs now expose a default resource, retain accepted uncompressed telemetry samples by client instance, drop samples on terminating pushes, list active client resources, update Prometheus counters/gauges for accepted pushes, terminating pushes, retained sample count, and retained sample bytes, and cover unknown subscription, unsupported compression, and oversized metric rejections. Accepted PushTelemetry payloads can now be exported to a configured append-only JSONL sink, export failures fail closed with generated `KAFKA_STORAGE_ERROR` responses before retained telemetry state mutates, and export success/error/byte metrics are registered and referenced by checked-in dashboards/alerts. AutoMQ-compatible Kafka request count/size/time/error metrics, produce/fetch request counters, produce/fetch/request latency histograms, broker connection/topic/group/partition gauges, and AutoMQ object-manager stream/object/prepared/mark-destroyed gauges are exported and pinned by default tests. JMX-compatible request count/byte/time/error metrics, request-channel queue gauges, broker-topic request/byte/error counters, replica-manager partition/leader/offline/reassigning/under-min-ISR/at-min-ISR gauges, replica-manager ISR shrink/expand counters, delayed-operation purgatory gauges, broker-state gauge, request-handler idle gauge, network-processor idle gauge, and active-controller gauges are now registered and emitted from request and broker-state paths where local state exists. Checked-in Grafana and Prometheus PromQL expressions are now parsed against the registered metric corpus so artifact drift fails the default Zig suite; the Python observability static audit now rejects non-standard JSON constants and duplicate object keys in checked-in Grafana dashboard JSON before metric-reference checks; JMX controller health, replica-manager health, request-channel backlog, request total/local/remote/queue/response-send timing, request errors, failed produce/fetch, broker-state, idle, delayed-fetch purgatory, broker-topic, request-metric, and AutoMQ object-manager fanout/backlog dashboard/alert fixtures are pinned; compaction lifecycle counters are registered instead of being silently dropped. Broader AutoMQ/JMX metric corpus coverage remains. |
| P2 | Performance and client matrix gates | Repeatable benchmarks and Java/librdkafka/Go/Python/Kafka CLI compatibility suites pass across supported versions. | In progress. `zig build bench` now includes local `PartitionStore` produce/fetch throughput and p99 latency gates, mock S3 WAL sync-produce latency, S3 request-volume and rebuild-time gates, bounded-cache memory-growth checks, and an opt-in `ZMQ_RUN_BENCH_LIVE_S3=1` live S3 provider object put/get throughput, p99, and requests/MiB gate with environment-tunable thresholds; live-S3 benchmark runs now reject placeholder endpoint, bucket, credential, region, path-style, TLS CA, non-positive port, and invalid `ZMQ_RUN_BENCH_LIVE_S3`/`ZMQ_S3_SKIP_ENSURE_BUCKET` boolean settings before touching the provider, and the local benchmark gate has been re-executed successfully. Gated `bench-compare` now exposes the ZMQ/Kafka/AutoMQ comparative benchmark runner as a release command, requires a ZMQ result plus at least one Kafka/AutoMQ baseline when run through the release gate, fails closed when any selected target does not produce results, can require exact target coverage through `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`, and enforces environment-tunable throughput, latency, error-rate, and historical trend regression thresholds from a prior results artifact with default-suite self-tests. A gated external-client matrix step exists for kcat, Kafka CLI, kafka-python, confluent-kafka, Java kafka-clients, and Go kafka-go metadata, topic admin, produce/fetch, and committed-offset probes with required profile, exact version-label, secured profile, tool, and semantic validation. Broader live comparative performance runs remain. |

Latest default-suite filesystem WAL replay tranche: acknowledged local
filesystem WAL records replay into broker LogCache after restart and are now
verified through generated Fetch v12 read-back in addition to the internal store
fetch check.

Latest default-suite S3 WAL resume tranche: stateless S3 WAL replacement now
uses explicit initial/replacement/continuation data dirs. The replacement broker
appends after replay, and a second fresh-dir broker verifies the recovered log,
resumed S3 object counter, and another append without object-key reuse.
Replacement and continuation checkpoints now also verify replayed and
post-replay appended records through generated AutomqGetPartitionSnapshot,
ListOffsets earliest/latest, and Fetch v12 read-back.

Latest default-suite legacy S3 WAL fallback tranche: legacy synchronous S3 WAL
fallback writes indexed replayable objects before acknowledgement, and a fresh
local store rebuilds and fetches them through recovered ObjectManager metadata.
The replacement store appends after replay, and a second fresh local store
verifies the legacy fallback objects plus replacement-side append remain
fetchable from recovered ObjectManager metadata.

Latest default-suite S3 WAL partition-repair tranche: partition offsets repaired
from recovered S3 WAL objects now use explicit initial/replacement/continuation
data dirs. The replacement broker repairs next offset, high watermark, last
stable offset, and ObjectManager stream end offset from S3 WAL metadata, appends
another record after repair, and a second fresh-dir broker verifies the repaired
offsets and appended records remain authoritative through generated
AutomqGetPartitionSnapshot, ListOffsets earliest/latest, and Fetch v12
read-back.

Latest default-suite S3 WAL object-refresh tranche: user records rebuilt after
local snapshot removal and AutoMQ object metadata replay are now verified through
generated ListOffsets earliest/latest and Fetch v12 read-back on the replacement
broker and again after a second fresh-dir replay with replacement-side appended
records.

Latest default-suite idempotent-producer S3 tranche: producer sequence recovery
from S3 WAL now uses explicit initial/replacement/continuation data dirs. The
replacement broker rejects a duplicate generated Produce without advancing the
partition offset, appends the next sequence for the recovered producer, and a
second fresh-dir broker verifies both the advanced sequence state and another
duplicate generated Produce remain non-appending through generated ListOffsets
and Fetch v12 read-back of the replayed and post-replay idempotent batches.

Latest default-suite share data-plane tranche: share sessions and share-partition
state now write combined snapshots to `__consumer_offsets`, replay from S3 WAL
across explicit initial/replacement/continuation data dirs, and skip stale local
share files when shared snapshots exist. The replacement broker advances the
share session and partition state after replay, a second fresh-dir broker
verifies those updates, and failed shared snapshot writes roll back before
exposing session epochs. Replacement and continuation checkpoints now also
verify share-group membership and share-partition state through generated
ShareGroupDescribe and ReadShareGroupState read-back, the replacement checkpoint
advances and rechecks the session epoch through generated ShareFetch
accept/reject responses, and local share-state restart rechecks restored
partition state through generated ReadShareGroupState. Local share-session
restart also exercises the restored epoch through generated ShareFetch
accept/reject responses.

Latest default-suite local group/share rollback read-back tranche: local and S3
snapshot-write rollback tests now pair internal coordinator assertions with
generated protocol read-backs. JoinGroup, LeaveGroup, SyncGroup, and
DeleteGroups rollback checkpoints verify group absence, empty groups, or live
members through ConsumerGroupDescribe; ShareGroupHeartbeat, DeleteGroups
share-session cleanup, shared/local share-session rollback, ShareFetch
acquisition rollback, and ShareAcknowledge state rollback verify member/group
visibility through ShareGroupDescribe, non-advanced session epochs through
ShareFetch invalid-epoch probes, and partition state through ReadShareGroupState.
DeleteGroups S3 offset tombstone replay now also checks generated
ConsumerGroupDescribe group absence alongside OffsetFetch offset absence.
ShareFetch success now also rechecks acquired share-state batches through
generated ReadShareGroupState instead of relying only on coordinator maps.
The same ConsumerGroupDescribe read-back coverage now includes normal
JoinGroup/ConsumerGroupHeartbeat allocator failures, JoinGroup materialization
failure, invalid JoinGroup timeouts, unsupported ConsumerGroupHeartbeat
assignors, invalid group IDs, and duplicate subscription rejection.
Generated JoinGroup, SyncGroup, Heartbeat, and LeaveGroup success/static-fence
tests now also recheck preparing/stable members, emptied groups, and fenced
retained members through ConsumerGroupDescribe instead of relying only on
coordinator maps. ConsumerGroupDescribe read-backs now also cover generated
JoinGroup authorization denial, the legacy SyncGroup stable-state path, and
KIP-848 static-member fencing. Broker tick group-timeout eviction now also
checks evicted and retained members through generated ConsumerGroupDescribe.
Local JoinGroup/SyncGroup restart coverage now also rechecks the restored stable
member through generated ConsumerGroupDescribe.

Latest default-suite transaction timeout read-back tranche: the local broker
tick timeout abort-marker path now pairs the internal coordinator completion and
Fetch v12 control-record checks with generated ListTransactions and
DescribeTransactions read-back for the completed abort. The generated
WriteTxnMarkers local success path now also rechecks the committed control batch
through Fetch v12 plus CompleteCommit introspection, and local producer-epoch
and transaction-state marker rejections now verify retained PrepareCommit or
PrepareAbort state through generated ListTransactions and DescribeTransactions,
with no-marker partition visibility rechecked through generated DescribeStreams,
AutomqGetPartitionSnapshot, ListOffsets, and Fetch v12.
Generated InitProducerId allocation, epoch recovery, abortable-state rejection,
legacy recovery, timeout application, invalid-timeout rejection, trailing-byte
rejection, authorization denial, and S3 snapshot-write rollback tests now also
verify Empty, PrepareAbort, or missing-ID state through generated
ListTransactions and DescribeTransactions instead of relying only on
transaction-coordinator maps. Produce abortable-state rejection now verifies
retained PrepareAbort state through generated transaction introspection.
AddPartitionsToTxn and AddOffsetsToTxn abortable/rejection paths,
including missing-topic and transactional-id mismatch partition-registration
rejections, now likewise verify retained Empty or PrepareAbort coordinator state
through generated transaction introspection after the generated error response.
EndTxn abortable, local checkpoint failure, transactional-id mismatch, and
unknown-partition rejection paths now also pair their generated error responses
with generated ListTransactions and DescribeTransactions read-backs for retained
PrepareAbort or Ongoing state, and transactional-id/trailing-byte no-marker
paths recheck partition visibility through generated DescribeStreams,
AutomqGetPartitionSnapshot, ListOffsets, and Fetch v12 while the unknown-topic
path rechecks absence through generated DescribeTopicPartitions.
WriteTxnMarkers partition-write, object-snapshot, shared-snapshot, and local
transaction-checkpoint failure paths now recheck retained PrepareCommit or
CompleteCommit visibility through generated transaction introspection, and the
shared transaction-snapshot rollback and post-marker local checkpoint failures
now also recheck no-marker or visible-marker partition state through generated
DescribeStreams, AutomqGetPartitionSnapshot, ListOffsets, and Fetch v12. The S3
WAL atomic marker path also rechecks the committed control batch through
generated Fetch v12.

Generated KIP-848 ConsumerGroupHeartbeat and ShareGroupHeartbeat success paths
now also recheck join, subscription/rack update, and leave-visible state through
ConsumerGroupDescribe or ShareGroupDescribe. KIP-848 range-assignment
convergence now also rechecks member assignments and target assignments through
generated ConsumerGroupDescribe after heartbeat-owned partition reconciliation.
ConsumerGroupHeartbeat and ShareGroupHeartbeat stale/error paths now recheck
unchanged member state or missing groups through generated describe APIs.
ConsumerGroupHeartbeat and ShareGroupHeartbeat trailing-byte rejection now
rechecks that join-shaped requests leave groups absent through generated
describe APIs. ShareFetch/ShareAcknowledge malformed and storage-error
missing-group paths now recheck group absence through generated
ShareGroupDescribe, and trailing-byte rejection now rechecks that otherwise
session-opening requests do not advance share-session visibility through
generated ShareFetch probes.
Generated ReadShareGroupState read-backs now also cover
InitializeShareGroupState success and local persistence failure,
WriteShareGroupState local persistence failure, stale/invalid
WriteShareGroupState rejection retaining the prior state, DeleteShareGroupState
successful removal, and DeleteShareGroupState local persistence rollback
retaining the prior state. DeleteTopics side-state cleanup now also checks
deleted topics through generated DescribeTopicPartitions and removed share state
through generated ReadShareGroupState unknown-topic errors after local
delete/restart and S3 replay.
Generated share-state trailing-byte mutation tests now use otherwise mutating
Initialize/Write/DeleteShareGroupState bodies and recheck absent or retained
partition state through generated ReadShareGroupState after rejection.

Latest default-suite AlterConfigs tranche: generated AlterConfigs set/update
records replayed from the S3 cluster metadata log now use explicit
initial/replacement/continuation data dirs. The replacement broker verifies the
initial topic overrides through generated DescribeConfigs, updates retention,
segment, and compression overrides through the generated request path after
replay, and a second fresh-dir broker verifies the latest generated
DescribeConfigs values. AlterConfigs null-value reset replay now also verifies
deleted defaults and retained overrides through generated DescribeConfigs at
initial, replacement, and second fresh-dir checkpoints. Default-suite
validate-only, invalid-config, authorization-denial, denied-response
materialization/serialization failure, snapshot-write failure, local checkpoint
failure, and response allocation failure paths now also pair their internal
rollback checks with generated DescribeConfigs read-backs.

Latest default-suite IncrementalAlterConfigs tranche: generated
IncrementalAlterConfigs SET records replayed from the S3 cluster metadata log
now use explicit initial/replacement/continuation data dirs. The replacement
broker verifies the initial topic overrides through generated DescribeConfigs,
updates retention, segment, cleanup, and compression overrides through the
generated request path after replay, and a second fresh-dir broker verifies the
latest generated DescribeConfigs values. IncrementalAlterConfigs DELETE replay
now also verifies deleted defaults and retained overrides through generated
DescribeConfigs at initial, replacement, and second fresh-dir checkpoints.
Default-suite validate-only, invalid-config, authorization-denial,
denied-response materialization/serialization failure, snapshot-write failure,
local checkpoint failure, and response allocation failure paths now also pair
their internal rollback checks with generated DescribeConfigs read-backs.
DescribeConfigs authorization-denied storage-error fallbacks now also seed
non-default topic configs and re-read them through generated DescribeConfigs
after the denied response materialization or serialization failure.

Latest default-suite committed-offset tranche: committed offsets replayed from
`__consumer_offsets` S3 WAL now use explicit initial/replacement/continuation
data dirs. The replacement broker verifies the initially replayed offset through
generated OffsetFetch v8 read-back, writes a newer direct committed-offset record
after replay, and a second fresh-dir broker verifies the latest offset, leader
epoch, and metadata win through generated OffsetFetch v8 read-back.

Latest default-suite TxnOffsetCommit tranche: generated TxnOffsetCommit offset
records replayed from `__consumer_offsets` S3 WAL now use explicit
initial/replacement/continuation data dirs. The initial transactional commit and
replacement replay are now both verified through generated OffsetFetch v8
read-back, then the replacement broker writes a newer transactional committed
offset after replay, and a second fresh-dir broker verifies the latest offset,
leader epoch, and metadata win through generated OffsetFetch v8 read-back.

Latest default-suite offset-tombstone tranche: OffsetDelete tombstones replayed
from `__consumer_offsets` S3 WAL now use explicit initial/replacement/
continuation data dirs. The replacement broker verifies an initial tombstone and
a retained offset, writes another tombstone for the retained offset, and a second
fresh-dir broker verifies both deleted offsets remain absent through generated
OffsetFetch v8 read-back.

Latest default-suite DeleteGroups tombstone tranche: DeleteGroups offset
tombstones replayed from `__consumer_offsets` S3 WAL now use explicit
initial/replacement/continuation data dirs. The replacement broker verifies the
first deleted group and offset remain absent, deletes a second group with an
offset, and a second fresh-dir broker verifies both group/offset tombstones stay
authoritative through generated OffsetFetch v8 group-level read-back.

Latest default-suite DeleteGroups share-session cleanup tranche: DeleteGroups
share-session cleanup replay now uses explicit initial/replacement/continuation
data dirs. The replacement broker verifies the first deleted share session stays
absent, deletes a second share group with a live session, and a second fresh-dir
broker verifies both share-session removals remain authoritative. Generated
ShareGroupDescribe group-not-found read-back now verifies deleted share groups
stay absent at initial, replacement, and continuation checkpoints.

Latest default-suite consumer-group lifecycle tranche: consumer group lifecycle
snapshots replayed from `__consumer_offsets` S3 WAL now use explicit
initial/replacement/continuation data dirs. The replacement broker adds a second
member, updates group timeouts/rack metadata, resyncs assignments, and a second
fresh-dir broker verifies the stable two-member generation and assignments.
Replacement replay, replacement-side update, and continuation checkpoints now
also verify member epochs, rack metadata, and subscriptions through generated
ConsumerGroupDescribe read-back.

Latest default-suite transaction-metadata tranche: transaction coordinator
snapshots replayed from `__transaction_state` S3 WAL now use explicit
initial/replacement/continuation data dirs. The replacement broker bumps the
replayed producer epoch with a new timeout, registers a replacement transaction
partition, and a second fresh-dir broker verifies that updated epoch, timeout,
status, and partition set. Replacement checkpoints verify both the replayed
transaction state and the bumped replacement-side state through generated
ListTransactions and DescribeTransactions responses before a continuation broker
verifies the same state after another fresh-dir replay.

Latest default-suite InitProducerId tranche: generated InitProducerId state
replayed from `__transaction_state` S3 WAL now uses explicit
initial/replacement/continuation data dirs. The replacement broker verifies the
initial transactional ID, timeout, epoch, and status, bumps the producer epoch
through the generated request path after replay, and a second fresh-dir broker
verifies the bumped epoch and timeout. Initial, replacement, replacement-side
epoch-bump, and continuation checkpoints now also verify the empty transaction
state through generated ListTransactions and DescribeTransactions read-back.
The local persisted producer-allocation snapshot test now also verifies that
the allocated transactional ID is visible through generated ListTransactions
and DescribeTransactions responses, not just the local snapshot file.

Latest default-suite AddPartitionsToTxn tranche: generated AddPartitionsToTxn
registrations replayed from `__transaction_state` S3 WAL now use explicit
initial/replacement/continuation data dirs. The replacement broker verifies the
initial user-topic registration, registers another topic partition after replay,
and a second fresh-dir broker verifies both registered partitions and the
ongoing transactional state. Initial, replacement, and continuation checkpoints
now also verify the registered topic partitions, including the post-replay
registration checkpoint, through generated ListTransactions and
DescribeTransactions read-back.

Latest default-suite AddOffsetsToTxn tranche: generated AddOffsetsToTxn
registrations replayed from `__transaction_state` S3 WAL now use explicit
initial/replacement/continuation data dirs. The replacement broker verifies the
initial `__consumer_offsets` registration, registers another transactional
offsets partition after replay, and a second fresh-dir broker verifies both
transactional IDs, producer epochs, and offsets-partition registrations.
Initial, replacement, and continuation checkpoints now also verify the
offsets-topic registrations, including the post-replay registration checkpoint,
through generated ListTransactions and DescribeTransactions read-back.

Latest default-suite local transaction read-back tranche: generated
AddPartitionsToTxn/AddOffsetsToTxn/EndTxn success, allocation-failure rollback,
shared transaction-snapshot rollback, post-marker local-checkpoint failure, and
timed-out transaction rollback paths now pair transaction coordinator internal
assertions with generated ListTransactions and DescribeTransactions read-back;
the EndTxn shared transaction-snapshot rollback path now also rechecks the
no-marker partition state through generated DescribeStreams,
AutomqGetPartitionSnapshot, ListOffsets, and Fetch v12.

Latest default-suite WriteTxnMarkers commit/abort tranche: committed and aborted
transaction markers replayed from S3 WAL now use explicit initial/replacement/
continuation data dirs. The replacement broker writes another marker and
transaction snapshot after replay for each path, and a second fresh-dir broker
verifies both completed transactions plus both control batches and advanced
partition offsets. Replacement and continuation checkpoints now also verify the
completed transaction snapshots through generated ListTransactions and
DescribeTransactions read-back, and marker offsets through generated
ListOffsets read-back. Replacement and continuation Fetch v12 read-back now also
verifies the recovered commit and abort control batches, while generated
DescribeStreams and AutomqGetPartitionSnapshot read-backs verify the same
advanced partition offsets through AutoMQ topic-backed stream metadata.

Latest default-suite EndTxn commit/abort tranche: atomic EndTxn replay now uses
explicit initial/replacement/continuation data dirs for both outcomes. The
replacement broker commits or aborts another transaction through the generated
EndTxn path after replay, and a second fresh-dir broker verifies both completed
transaction snapshots, both control batches, and advanced partition offsets.
Replacement and continuation checkpoints now also verify the completed
transaction snapshots through generated ListTransactions and
DescribeTransactions read-back, and marker offsets through generated
ListOffsets read-back. Replacement and continuation Fetch v12 read-back now also
verifies the recovered commit and abort control batches, while generated
DescribeStreams and AutomqGetPartitionSnapshot read-backs verify the same
advanced partition offsets through AutoMQ topic-backed stream metadata.

Latest default-suite transaction-timeout tranche: timed-out transaction
auto-aborts replayed from S3 WAL now use explicit initial/replacement/
continuation data dirs. The replacement broker ages and auto-aborts another
transaction after replay, and a second fresh-dir broker verifies both completed
abort snapshots, both abort control batches, and advanced partition offsets.
Replacement and continuation checkpoints now also verify the timeout-completed
transaction snapshots through generated ListTransactions and
DescribeTransactions read-back, and marker offsets through generated
ListOffsets read-back. Replacement and continuation Fetch v12 read-back now also
verifies the recovered abort control batches, while generated DescribeStreams
and AutomqGetPartitionSnapshot read-backs verify the same advanced partition
offsets through AutoMQ topic-backed stream metadata. Local broker-tick timeout
marker coverage also verifies the generated abort control batch through Fetch
v12 read-back.

Latest default-suite stateless ACL tranche: DeleteAcls now writes a replayable
full ACL snapshot to `__cluster_metadata`, and fresh-dir S3 replacement coverage
verifies retained/deleted ACL visibility through generated DescribeAcls after
the initial tombstone, replacement replay, replacement-side tombstone, and
second fresh-dir replay.

Latest default-suite stateless ACL-addition tranche: CreateAcls replay now uses
explicit initial/replacement/continuation data dirs. The replacement broker adds
another ACL after replay, and a second fresh-dir broker verifies both ACLs
remain authorized and visible through generated DescribeAcls after initial
creation, replacement replay, replacement-side addition, and second fresh-dir
replay.

Latest default-suite stateless quota tranche: AlterClientQuotas addition/update
now uses explicit initial/replacement/continuation data dirs. The replacement
broker updates default quota rates, updates a replayed client quota, and adds a
post-replay client quota; generated DescribeClientQuotas now verifies the
initial mutation, replacement replay, replacement-side update/addition, and
second fresh-dir replay. AlterClientQuotas removal also has fresh-dir S3
replacement coverage that applies successive full quota snapshots from
`__cluster_metadata`, proving removed client quota entries are not resurrected,
removed default quota rates reset to unlimited, and retained client quotas
survive replay through generated DescribeClientQuotas. The replacement broker
removes the retained client quota and a second fresh-dir replay verifies both
client quotas stay absent while default quota rates remain unlimited.
AlterClientQuotas validate-only, invalid-key, pre-mutation materialization/
serialization failure, and quota snapshot S3 WAL failure paths now also pair
their internal quota-manager absence checks with generated DescribeClientQuotas
empty read-backs.

Latest default-suite stateless SCRAM tranche: AlterUserScramCredentials
addition/update now uses explicit initial/replacement/continuation data dirs.
The replacement broker updates a replayed SCRAM credential and adds a post-replay
credential; generated DescribeUserScramCredentials now verifies the initial
upsert, replacement replay, replacement-side update/addition, and second
fresh-dir replay. The second fresh-dir broker also verifies both credentials'
  salts, stored/server keys, and iteration metadata. AlterUserScramCredentials
  deletion also has fresh-dir S3 replacement coverage that replays successive full
  SCRAM snapshots from `__cluster_metadata`, proving deleted credentials are not
  resurrected while retained credentials keep their salt, stored/server keys,
  iteration metadata, and generated DescribeUserScramCredentials visibility. The
  replacement broker deletes the retained credential and a second fresh-dir replay
  verifies both deleted credentials remain absent through generated
DescribeUserScramCredentials. Authorization denial, unsupported-mechanism,
snapshot-write failure, and response allocation/serialization failure paths now
also pair their internal rollback checks with generated
DescribeUserScramCredentials read-backs.

Latest default-suite stateless delegation-token tranche: CreateDelegationToken
and RenewDelegationToken now use explicit initial/replacement/continuation data
dirs. The replacement broker creates a post-replay token and renews a replayed
token again; generated DescribeDelegationToken now verifies initial creation,
replacement replay, replacement-side creation/renewal, and second fresh-dir
replay of both token creations plus the latest renewed expiry. Immediate
ExpireDelegationToken removal also has fresh-dir S3 replacement coverage that
replays successive full delegation-token snapshots from `__cluster_metadata`,
proving expired tokens are not resurrected while retained tokens keep their
token-id and HMAC metadata through generated DescribeDelegationToken. The
replacement broker expires the retained token and a second fresh-dir replay
verifies both expired tokens remain absent through generated
DescribeDelegationToken. Create/Renew/Expire rollback paths for snapshot-write
or success-response serialization failures now also verify token absence or
restored token expiry through generated DescribeDelegationToken read-backs.

Latest default-suite stateless finalized-feature tranche: UpdateFeatures
addition now uses explicit initial/replacement/continuation data dirs. The
replacement broker adds a `kraft.version` finalization after replay, and a
second fresh-dir broker verifies both `metadata.version` and `kraft.version`
plus the advanced epoch through generated ApiVersions v3 read-back.
UpdateFeatures deletion also has fresh-dir S3 replacement coverage that replays
successive finalized feature snapshots from `__cluster_metadata`, proving
deleted finalizations are not resurrected while retained supported features keep
their finalized version and epoch metadata. The replacement broker deletes the
retained `kraft.version` finalization and a second fresh-dir replay verifies
both finalizations remain absent with the advanced finalized-feature epoch
through generated ApiVersions v3 read-back.

Latest default-suite stateless topic-config tranche: common topic config
snapshots now have fresh-dir S3 replacement coverage that replays successive
full topic snapshots from `__cluster_metadata`. The replacement broker now
updates the replayed partition count and all supported common config fields, and
a second fresh-dir replay verifies those replacement-side values remain
authoritative.

Latest default-suite stateless config-deletion tranche: AlterConfigs null-value
resets and IncrementalAlterConfigs DELETE operations now have fresh-dir S3
replacement coverage that replays successive full topic snapshots from
`__cluster_metadata`, proving deleted topic config overrides reset to defaults
while retained overrides survive. The replacement broker now resets/deletes the
retained `compression.type` override and a second fresh-dir replay verifies all
deleted config overrides remain at defaults.

Latest default-suite local reassignment-restart tranche:
broker-local AlterPartitionReassignments restart coverage now verifies the
in-flight reassignment through generated ListPartitionReassignments read-back
and generated DescribeTopicPartitions owner visibility before shutdown and
again after local restart, instead of relying only on internal reassignment
maps.

Latest default-suite local reassignment read-back tranche:
request-scoped AlterPartitionReassignments, generated CreateTopics/
CreatePartitions manual-assignment acceptance, and auto-balancer/controller-
aware/scheduled rebalance paths now pair their internal reassignment-count
assertions with generated ListPartitionReassignments active or empty-state
read-back and generated DescribeTopicPartitions ownership visibility.

Latest default-suite reassignment failure/quorum read-back tranche:
AlterPartitionReassignments response-materialization and authorization-denial
materialization/serialization failures, ListPartitionReassignments
authorization-denial serialization failures, S3 WAL reassignment snapshot write failures,
ListPartitionReassignments materialization failure, stale rebalance plans,
controller-aware convergence, quorum replay/cancellation, and non-leader
rejection now recheck generated ListPartitionReassignments active or empty state
and generated DescribeTopicPartitions absence or ownership after the internal
assertions.

Latest default-suite stateless reassignment-state tranche:
AlterPartitionReassignments active move state now has fresh-dir S3 replacement
coverage that replays ongoing reassignment ownership from `__cluster_metadata`.
The replacement broker now supersedes the replayed move with a new target broker,
and a second fresh-dir replay verifies the updated active reassignment state and
owner remain authoritative through generated ListPartitionReassignments and
DescribeTopicPartitions read-back.

Latest default-suite stateless reassignment-cancel tranche:
AlterPartitionReassignments cancellation now has fresh-dir S3 replacement
coverage that replays the empty reassignment snapshot from
`__cluster_metadata`, proving canceled moves are not resurrected and local
ownership is restored. The replacement broker now starts and cancels another
move, and a second fresh-dir replay verifies the reassignment table stays empty
with local ownership restored through generated ListPartitionReassignments and
DescribeTopicPartitions read-back.

Latest default-suite stateless local-failover-completion tranche: broker-local
failover completion checkpoints for reassigned partitions now use explicit
initial/replacement/continuation data dirs. The replacement broker verifies the
first completed reassignment stays local, completes another reassignment through
`tick()` after replay, and a second fresh-dir broker verifies both topics have
no ongoing reassignment and local ownership restored through generated
ListPartitionReassignments and DescribeTopicPartitions read-back.

Latest default-suite stateless manual-assignment tranche: CreateTopics and
CreatePartitions explicit single-replica remote assignments now have fresh-dir
S3 replacement coverage that replays non-local ownership from
`__cluster_metadata`. The replacement broker now creates another manually
assigned topic and adds another manually assigned partition, and a second
fresh-dir replay verifies all remote owners and assignment records survive
through generated DescribeTopicPartitions read-back. CreateTopics and
CreatePartitions manual-assignment checkpoints also verify the active
reassignments through generated ListPartitionReassignments read-back before and
after replacement-side continuation.

Latest default-suite CreateTopics read-back tranche: generated
DescribeTopicPartitions and DescribeConfigs now verify successful local
creation, supported creation-time configs, remote manual-assignment ownership,
validate-only non-creation, authorization/config/follower/assignment rejections,
shared snapshot failure, local checkpoint/object failures, and response or
snapshot materialization rollback paths.

Latest default-suite CreatePartitions read-back tranche: generated
DescribeTopicPartitions now verifies successful local expansion,
validate-only non-expansion, authorization-denied non-expansion, invalid
assignment rejection, remote-assignment ownership, snapshot-write rollback,
local checkpoint rollback, allocation-failure rollback paths, and
authorization-denial storage-error fallbacks after denied-response
materialization or serialization fails.

Latest default-suite stateless replica-directory tranche: AssignReplicasToDirs
now has fresh-dir S3 replacement coverage that replays local partition directory
assignments from `__cluster_metadata`. The replacement broker now rewrites the
directory assignment, and a second fresh-dir replay verifies the replacement-side
directory remains authoritative through generated DescribeLogDirs read-back.
Local/shared persistence, stale broker epoch, response materialization,
authorization-denial materialization, and authorization-denial serialization
failure paths also verify retained directory visibility through generated
DescribeLogDirs read-back. Local AlterReplicaLogDirs and
AssignReplicasToDirs store, validation, failure, denial, multi-directory, and
restart paths now also pair the internal replica-directory assignment checks
with generated DescribeLogDirs read-back for the assigned or retained default
partition.

Latest default-suite stateless DeleteTopics tranche: DeleteTopics v6 tombstones
now have fresh-dir S3 replacement coverage that replays topic deletion from
`__cluster_metadata` and verifies the deleted topic, partition state, share
state, reassignment state, and replica-directory assignments remain absent. The
replacement broker now deletes a second replayed topic, appends another
tombstone, and a third fresh-dir broker verifies both deleted topics and their
side state remain absent, including generated ListPartitionReassignments
empty-state read-back for each deleted topic and active-state read-back for the
retained reassignment before the second tombstone.
Surviving share-state and replica-directory side state is now read back through
generated ReadShareGroupState and DescribeLogDirs before and after fresh-dir
replay, and S3 WAL write-failure rollback verifies retained side state through the
same generated APIs.

Latest default-suite DeleteTopics local read-back tranche: local v0/v6 delete,
authorization-denial, reassignment cleanup, local cleanup checkpoint, and
allocation-failure rollback paths now recheck removed or retained topic
visibility through generated DescribeTopicPartitions responses, with deleted
reassignments also verified through generated ListPartitionReassignments.

Latest default-suite AutoMQ metadata tranche: node registration tags and stream
tags now round-trip through their AutoMQ APIs and survive local metadata/object
restart, rollback snapshots, committed quorum replay, compacted full-snapshot
records, and the KRaft failover harness now validates both node and stream tags
in its AutoMQ metadata scenario. Fresh-broker committed Raft replay for
broker-only KV/node/router/license/node-id/group metadata now verifies generated
GetKVs, AutomqGetNodes, AutomqZoneRouter, DescribeLicense, and
GetNextNodeId plus ExportClusterManifest read-back after opening from the
committed log and after replaying a compacted full-snapshot record; committed
object snapshot replay verifies restored stream visibility through generated
DescribeStreams and prepared-object cursor advancement through generated
PrepareS3Object, and compacted object snapshot replay also verifies restored
stream tags through generated DescribeStreams and restored prepared-object
cursor advancement through generated PrepareS3Object. Internal AppendEntries
and follower-promotion replay now also verify generated GetKVs, DescribeStreams,
GetOpeningStreams, and ExportClusterManifest read-back for restored AutoMQ
metadata/object state, and follower-promotion replay also verifies generated
AutomqGetNodes, AutomqZoneRouter, and DescribeLicense read-back. The
router/snapshot API smoke now also backs opening stream offsets, stream-set
range offsets, tag clearing, and AutomqUpdateGroup promotion visibility with
generated GetOpeningStreams, DescribeStreams, and ExportClusterManifest
read-back. The local stream/object lifecycle API smoke now backs
CreateStreams/CommitStreamObject/TrimStreams/DeleteStreams offsets and stream
tombstones with generated DescribeStreams read-back. Stream/object rollback
failure paths now also verify restored visibility, offsets, opened/closed state,
and stream counts through generated DescribeStreams/GetOpeningStreams read-back,
including CommitStreamSetObject success-serialization and local snapshot
failure checks through generated GetOpeningStreams and ExportClusterManifest,
plus safe PrepareS3Object cursor read-back after prepare and commit-object
rollback. The same
rollback tranche also backs KV, node, zone-router, license, and group
persistence-failure restoration with generated GetKVs, AutomqGetNodes,
AutomqZoneRouter, DescribeLicense, and ExportClusterManifest read-back where the
read is non-mutating. AutoMQ authorization-denial and success-serialization
rollback paths now also recheck non-mutation through generated
DescribeStreams/GetOpeningStreams/GetKVs/DescribeLicense/GetNextNodeId/
AutomqGetNodes/AutomqZoneRouter/ExportClusterManifest read-back where safe,
including remaining AutoMQ metadata/controller extension authorization denials
for node, license, stream, manifest, zone-router, and node-id cursor visibility.
Attached non-leader AutoMQ metadata/object mutation rejections now also cover
KV/node/router/license/group and Create/Prepare/Commit object paths, then
recheck non-mutation through generated GetKVs, AutomqGetNodes,
AutomqZoneRouter, DescribeLicense, DescribeStreams, GetOpeningStreams, and
ExportClusterManifest read-back. The node-id allocator and PrepareS3Object
cursor mutations are rejected through generated `NOT_CONTROLLER` responses
without advancing their cursors before relying on internal state checks. The
mutation-materialization failure paths for KV, node, zone-router, license,
node-id, and group metadata now also recheck restoration through generated
GetKVs, ExportClusterManifest, AutomqZoneRouter, DescribeLicense, and
GetNextNodeId read-back where safe. The same mutations now also append full
AutoMQ metadata snapshots to `__cluster_metadata`, and
fresh-dir S3 replacement coverage verifies replay without local `automq.meta`,
including generated GetKVs, AutomqGetNodes,
AutomqZoneRouter, DescribeLicense, ExportClusterManifest, and GetNextNodeId
responses plus post-replay PutKVs/DeleteKVs, AutomqRegisterNode,
AutomqZoneRouter, UpdateLicense, and AutomqUpdateGroup continuation, including
post-replay KV deletion, node tag clearing, and group demotion removals, that
survives a second fresh-dir S3 replay. Initial and replacement-side
deletion/clearing/demotion checkpoints now also recheck KV absence, cleared node
tags, and manifest group counts through generated GetKVs, AutomqGetNodes, and
ExportClusterManifest read-back before the next replay boundary. That second
fresh-dir replay now also rechecks the continued KV, node/tag, zone-router,
license, and manifest state through generated GetKVs, AutomqGetNodes,
AutomqZoneRouter, DescribeLicense, GetNextNodeId, and ExportClusterManifest
responses, including deleted KV/group removals and cleared node tags.
Broker-only stream/object metadata replacement also verifies generated
DescribeStreams, GetOpeningStreams, ExportClusterManifest, and PrepareS3Object
cursor read-back after local restart, plus generated CreateStreams and
PrepareS3Object cursor allocation, OpenStreams epoch/tag mutation,
CommitStreamObject/CommitStreamSetObject continuation, TrimStreams offset
advancement, CloseStreams/DeleteStreams lifecycle continuation,
GetOpeningStreams, DescribeStreams, and ExportClusterManifest read-back after S3
replay, with initial stream deletion/tag-clearing, mark-destroyed stream-object,
stream-set, and stream-set mark-destroyed checkpoints also rechecked through
generated DescribeStreams/ExportClusterManifest before the first fresh-dir
replay. Initial stream/object snapshot and post-snapshot S3 WAL refresh
checkpoints now also recheck generated DescribeStreams/GetOpeningStreams/
ExportClusterManifest, AutomqGetPartitionSnapshot, and Fetch visibility before
the first replacement, including second fresh-dir replay of post-replay
prepared-object, stream-object, and stream-set continuation with generated
PrepareS3Object cursor read-back plus generated DescribeStreams tag/offset
read-back after the second fresh-dir replay, stream-set objects and
mark-destroyed object states with repeated fresh-dir deletion-readiness replay
and generated PrepareS3Object cursor read-back, plus topic-partition stream
visibility after post-snapshot S3 WAL refresh and post-replay append
continuation through DescribeStreams and AutomqGetPartitionSnapshot.
Latest default-suite AutoMQ metadata trailing-byte tranche: malformed
PutKVs/DeleteKVs, AutomqRegisterNode, AutomqZoneRouter, UpdateLicense,
GetNextNodeId, and AutomqUpdateGroup frames now use otherwise mutating payloads
and pair internal no-mutation assertions with generated GetKVs,
ExportClusterManifest, AutomqZoneRouter, DescribeLicense, and GetNextNodeId
read-backs. Read-only GetKVs, GetOpeningStreams, AutomqGetNodes,
AutomqGetPartitionSnapshot, DescribeLicense, ExportClusterManifest, and
DescribeStreams trailing-byte paths now also seed retained state and recheck it
through their generated AutoMQ read APIs after rejection.
Latest default-suite AutoMQ stream/object trailing-byte tranche: malformed
CreateStreams, TrimStreams, OpenStreams, CloseStreams, DeleteStreams,
PrepareS3Object, CommitStreamSetObject, and CommitStreamObject frames now use
otherwise mutating payloads and pair internal no-mutation assertions with
generated DescribeStreams, GetOpeningStreams, ExportClusterManifest, and
PrepareS3Object cursor read-backs.
Latest default-suite stateless prepared-object TTL tranche: prepared-object
expiry now persists after replacement-side expiration, and a second fresh-dir
S3 replay verifies the expired prepared ID stays absent while the post-replay
prepared ID retains its TTL metadata and the generated PrepareS3Object cursor
continues beyond the replayed prepared ID.
Latest default-suite AutoMQ allocator continuation tranche: generated
GetNextNodeId and PrepareS3Object allocations are now replayed through repeated
fresh-dir S3 replacements, so continuation-side node/object cursor advances are
verified before another generated allocation is accepted.
Latest default-suite stateless stream-removal tranche: AutoMQ stream/object
removal replay now has fresh-dir S3 continuation coverage that applies
replacement-side stream deletion and tag clearing, verifies both are immediately
visible through generated DescribeStreams, GetOpeningStreams, and
ExportClusterManifest, then verifies a second fresh-dir
replay keeps the original and replacement-side deleted streams/objects absent
while retained streams remain visible through generated DescribeStreams,
GetOpeningStreams, and ExportClusterManifest.
Default S3 replacement coverage for DeleteRecords now uses explicit
initial/replacement/continuation data dirs and verifies post-replay
low-watermark trim continuation survives another fresh-dir replay through
generated ListOffsets earliest/latest read-back plus generated Fetch v12
read-back of records that remain after each trim.
Generated ExportClusterManifest read-back helpers now also pin cluster id and
broker node id alongside stream, node, and group counts.

Latest default-suite Produce/idempotent read-back tranche: generated ListOffsets
and DescribeTopicPartitions/DescribeProducers now verify malformed-batch
rejection, KRaft-follower non-auto-creation, required-acks rejection,
transactional partition-registration partial success, abortable-transaction
non-append behavior, post-append local checkpoint failure visibility,
producer-sequence persistence/failure visibility, S3 WAL failure non-advancement,
idempotent sequence gap/stale-epoch rejection, and InitProducerId epoch-bump
fencing. The idempotent sequence validation path now also re-fetches the
retained first batch after sequence-gap and stale-epoch rejections, proving those
negative responses do not append client-visible records, and re-fetches the
broker-assigned next batch after the follow-up valid append.
Malformed record-batch header/length, required-acks, abortable-transaction,
producer-sequence reservation, and S3 WAL Produce rejections now also recheck
generated Fetch v12 returns no records at the rejected partition.

Latest default-suite producer recovery read-back tranche: direct raw-log
producer-sequence rebuild and S3 WAL initial/replacement/continuation recovery
checkpoints now recheck the recovered idempotent producer state through generated
DescribeProducers responses instead of relying only on broker-local sequence
maps. The mixed raw-record/idempotent-batch rebuild path also rechecks the
client-visible end offset and broker-assigned rebuilt batch bytes through
generated ListOffsets and Fetch v12 read-back.

Latest default-suite Produce client-visibility read-back tranche: legacy v0
Produce success, v9 flexible success, and ACL-allowed v9 Produce now recheck
the appended record through generated ListOffsets and Fetch. Invalid-acks and
rejected-metrics paths recheck the topic has no appended records through
generated ListOffsets/Fetch, transactional partition-registration partial
success rechecks both the appended partition and the rejected partition through
generated Fetch, idempotent sequence persistence rechecks the persisted batch
through generated Fetch, and authorization denial rechecks the topic stays absent
through generated DescribeTopicPartitions.

Latest default-suite data-plane authorization read-back tranche: Fetch
authorization denial now starts with retained records, validates the generated
denial response, then re-authorizes and rechecks both partition end offsets and
records through generated ListOffsets and Fetch. DescribeProducers authorization
denial now also rechecks the denied topic stays absent through generated
DescribeTopicPartitions.

Latest default-suite Produce storage-error read-back tranche: post-append local
partition-state checkpoint failure, ObjectManager snapshot failure, and
producer-sequence checkpoint failure now recheck the storage-error-visible
records through generated DescribeStreams, AutomqGetPartitionSnapshot, and
Fetch v12 in addition to generated ListOffsets and DescribeProducers state
assertions. Producer-sequence reservation and S3 WAL storage-error no-append
paths now also recheck generated DescribeStreams and AutomqGetPartitionSnapshot
stay at offset zero.

Latest default-suite Produce/fallback ListOffsets read-back tranche: Produce
partition-response materialization failure now rechecks the partition offset does
not advance through generated ListOffsets/Fetch, and long-topic transaction-marker
state plus max-length partition-request fallback coverage now rechecks visible
offset and record state through generated ListOffsets/Fetch instead of relying
only on broker partition internals.

Latest default-suite S3 WAL fencing read-back tranche: generated
BeginQuorumEpoch and internal AppendEntries higher-epoch fencing now follow the
fencing response with a generated Produce v9 attempt that returns
NOT_LEADER_OR_FOLLOWER, plus generated ListOffsets and Fetch read-back proving
the fenced broker did not append client-visible records.

Latest default-suite transaction introspection read-back audit: the local
transaction restart test now uses the shared generated ListTransactions and
DescribeTransactions helpers, so producer id/epoch, timeout, state, and topic
partition visibility are checked through the same read-back path as the S3
replacement transaction tests.

Latest default-suite shutdown response helper audit: shutdown Produce and Fetch
rejection tests now validate the shared small-error response wire shape, and the
shutdown ApiVersions allowlist test now validates the generated catalog response
instead of manually reading only the first response fields.

Latest default-suite unsupported-version helper audit: unknown API,
legacy-interbroker, generated-nonbroker, catalog-max, ACL-bypass, and SASL-bypass
fail-closed tests now validate the shared small-error response wire shape instead
of manually reading only correlation ids and error codes.

Latest default-suite InitProducerId read-back helper audit: v0 allocation and
epoch-bump fencing tests now validate generated InitProducerId responses through
the shared helper, including throttle/error, producer id, and bumped producer
epoch fields before checking producer-sequence read-back.

Latest default-suite DeleteGroups generated-response audit: empty-group deletion
now builds the v2 request through the generated schema and validates the
generated DeleteGroupsResponse throttle/result/group/error fields before the
ConsumerGroupDescribe tombstone read-back proves the group remains absent.

Latest default-suite legacy group-coordinator generated-response audit:
JoinGroup v0 and SyncGroup v0 smoke tests now build requests through generated
schemas, validate generated response headers and bodies, and retain generated
ConsumerGroupDescribe read-backs for preparing and stable group state.

Latest default-suite OffsetCommit/OffsetFetch generated-response audit: the
legacy v5/v1 round-trip now builds both requests through generated schemas and
validates generated OffsetCommitResponse and OffsetFetchResponse bodies,
including partition error, committed offset, leader epoch, and metadata fields.
The generic correlation-id smoke test also validates response headers through
the shared generated header parser instead of a raw i32 read.

Latest default-suite legacy smoke generated-response audit: CreateTopics v0,
DeleteTopics v0, Heartbeat v0, AddPartitionsToTxn v0, and EndTxn v0 smoke tests
now build generated requests and validate generated response headers/bodies,
while fenced Produce validates the shared small-error response helper. The local
manual response-read scan is now reduced to request-header decoding and that
shared helper.

Latest default-suite valid-request generated-body audit: DeleteGroups offset
persistence, OffsetFetch v8 null-topic fetch-all, DeleteTopics v0 reassignment
and partition-state cleanup, and DescribeConfigs v0 now build request bodies
through generated schemas and validate generated response headers/bodies before
side-effect and read-back assertions.

Latest default-suite legacy request builder audit: UpdateFeatures v1,
Metadata v0/v1 null and empty topic filters, Produce v0 success/rejection/fence
paths, and the Heartbeat error-metrics probe now use generated request bodies
instead of ad hoc field writes while retaining generated response and metric
assertions.

Latest default-suite Fetch v0 request builder audit: the legacy Fetch
round-trip now uses generated ProduceRequest and FetchRequest bodies before
validating the generated FetchResponse payload and fetch metrics.

Latest release-evidence documentation audit: the required command block now
uses non-placeholder live-S3 example values and the release-evidence self-test
rejects angle-bracket placeholders there, matching the manifest-level
placeholder rules.

Latest release-evidence command-block audit: the release-evidence self-test now
parses the fenced `Required Commands` block and requires each documented command
line to match the validator's required gate list in order, so command-block
drift is caught before release evidence can depend on stale docs. The same
audit now applies release command-shape checks to those documented examples,
including duplicate-assignment, shell-operator, detached-segment, and embedded
output-marker rejection.

Latest comparative release-evidence audit: the comparative benchmark gate
banner must now appear exactly once as the exact stripped `COMPARATIVE BENCHMARK GATE` line,
the comparison title must be an exact selected-target `COMPARISON:` line, and
the self-test rejects suffixed wrapper copies beside the real comparison or gate
banner.

Latest benchmark release-evidence audit: local and live-S3 benchmark gates now
require the exact stripped `=== Benchmarks complete ===` line exactly once, and
the self-test rejects suffixed wrapper completion markers.

Latest default-suite OffsetDelete read-back tranche: generated OffsetFetch v8
now verifies deleted groups/offsets, subscribed-group rejection, partial unknown
topic-partition deletion, invalid-group rejection, local offset checkpoint
rollback, S3 tombstone-write rollback, rollback-snapshot materialization
failure, and authorization-denied preservation.

Latest default-suite OffsetCommit/TxnOffsetCommit read-back tranche: generated
OffsetFetch v8 now verifies committed-offset visibility, partial unknown
topic-partition preservation, invalid group/member/generation non-commit paths,
metadata-size rejection, local checkpoint rollback, persistence replay, and
authorization-denied preservation.

Latest default-suite DeleteGroups lifecycle read-back tranche: generated
ConsumerGroupDescribe now verifies successful deletion, active/missing/invalid
group non-deletion, authorization-denied preservation, and local persisted group
removal, while generated OffsetFetch v8 verifies removed and tombstone-failure
preserved offsets.

Latest default-suite DeleteRecords read-back tranche: generated ListOffsets now
verifies direct trim visibility, partition-state metadata lookup trims, rollback
snapshot materialization failure, and authorization-denied non-trims; generated
DescribeStreams, AutomqGetPartitionSnapshot, ListOffsets, and Fetch v12 now
also verify a successful no-op trim cannot move the low watermark backward and
an offset-out-of-range trim cannot mutate retained records, while generated
ListOffsets plus Fetch v12 verify local partition/object checkpoint rollback
keeps acknowledged records visible.

Latest KRaft failover visibility tranche: broker-only processes now retain the
configured `cluster.id` for generated Metadata, DescribeCluster, and AutoMQ
manifest identity responses, and the gated failover harness validates
DescribeCluster v1 broker/controller endpoint views, DescribeConfigs v4
topic-config views, DescribeLogDirs v4 partition log-dir views, and
AlterReplicaLogDirs v2 plus AssignReplicasToDirs v0 partition directory
mutations plus ElectLeaders v2 preferred-election responses through controller
failover, controller restarts, and broker restart. The same gate now creates a
configured topic with CreateTopics v7, verifies DescribeConfigs read-back and
validate-only non-mutation at each checkpoint, expands a dedicated topic with
CreatePartitions v2 and validates the expanded partition metadata after each
transition, deletes a separate topic with DeleteTopics v6
and verifies unknown-topic DescribeTopicPartitions plus repeat delete responses
at each checkpoint, mutates/describes a client quota through
AlterClientQuotas/DescribeClientQuotas v1 while verifying validate-only quota
requests remain non-mutating, and upserts/describes a SCRAM-SHA-256 user
through AlterUserScramCredentials/DescribeUserScramCredentials v0. The same
gate now validates the client telemetry subscription/push/resource APIs through
each transition with a stable client instance id, and validates delegation-token
create/renew/non-immediate-expire/describe continuity using the issued token id
and HMAC. It also mutates an isolated topic through AlterConfigs v2 and
IncrementalAlterConfigs v1, repeats validate-only requests without changing the
final values, and verifies DescribeConfigs read-back for cleanup policy, min
insync replicas, and segment bytes through each controller failover, controller
restart, and broker restart checkpoint. Direct AllocateProducerIds v0 probes
now verify successful monotonic PID block allocation through the same controller
leader kill, old-leader fresh rejoin, surviving-controller restart, and broker
restart checkpoints. Direct DescribeQuorum v2 probes now verify controller
listener endpoint metadata and voter directory IDs through the same transition
set. Direct FetchSnapshot v1 probes now verify request-scoped
`SNAPSHOT_NOT_FOUND` handling and current-leader endpoint routing metadata on
every alive controller through the same transition set. Direct controller
ApiVersions v3 probes now
verify the exact audited controller catalog and telemetry-key absence on every
alive controller through the same transition set.

Latest default-suite fail-closed tranche: metadata-client controller discovery
now surfaces malformed DescribeQuorum responses, including trailing response
bytes, separately from no-leader quorum state, S3 WAL recovery fails closed when
a listed WAL object is missing during GET and can retry cleanly, malformed
epoch-prefixed WAL object keys fence S3 writers instead of being skipped, and
AddRaftVoter now rejects empty listener sets so committed voters cannot be
silently omitted from Raft peer wiring, static controller quorum voter strings
reject duplicate node IDs/endpoints, and startup registers static controller
endpoints into Raft voter metadata with rollback on allocation failure;
MetadataClient voter registration now rejects duplicate controller peers and
rolls back the discovery list when RaftClientPool peer allocation fails;
DescribeQuorum and FetchSnapshot response materialization now fail closed
without leaking partially built endpoint metadata when allocation fails;
broker-side DescribeQuorum v2 now separately covers leader/voter listener
materialization failure after quorum topic materialization has succeeded;
Vote response topic materialization now fails closed before mutating vote state
when allocation fails;
broker-side Vote request validation and response materialization now return
generated fail-closed responses instead of silently closing or mutating state
after response allocation fails;
broker-side BeginQuorumEpoch, EndQuorumEpoch, and DescribeQuorum malformed
request paths now return generated invalid-request responses, and
DescribeQuorum allocation failures return storage errors instead of silent
connection closes;
broker ApiVersions malformed flexible requests and finalized-feature
materialization failures now return generated invalid-request or storage-error
responses instead of silent connection closes;
SaslHandshake and SaslAuthenticate malformed frames now return generated
invalid-request responses, and SaslHandshake fails closed if mechanism response
or selected-mechanism session storage allocation fails;
Metadata and FindCoordinator malformed frames now return generated
invalid-request responses, and response materialization allocation failures now
surface storage errors instead of silent connection closes;
OffsetForLeaderEpoch malformed frames now return generated invalid-request
responses, and response topic/partition allocation failures now surface storage
errors instead of silent connection closes;
ListTransactions malformed frames now return generated invalid-request
responses, and unknown-state/transaction-state materialization failures now
surface storage errors instead of silent connection closes;
DescribeTransactions malformed frames now return generated invalid-request
responses, and transaction-state materialization failures now surface storage
errors instead of silent connection closes;
DescribeAcls malformed frames now return generated invalid-request responses,
and ACL response materialization failures now surface storage errors instead of
silent connection closes;
DescribeLogDirs malformed frames now return generated invalid-request responses,
and log-directory response materialization failures now surface storage errors
instead of silent connection closes;
DescribeClientQuotas malformed frames now return generated invalid-request
responses, and quota-entry materialization failures now surface storage errors
instead of silent connection closes;
DescribeUserScramCredentials malformed frames now return generated
invalid-request responses, and result materialization failures now surface
storage errors instead of silent connection closes;
AlterUserScramCredentials malformed frames now return generated
invalid-request responses, and result materialization/serialization failures now
surface storage errors instead of silent connection closes;
UpdateFeatures malformed frames now return generated invalid-request responses,
and response materialization/serialization failures now surface storage errors
instead of silent connection closes;
AlterReplicaLogDirs malformed frames now return generated partition-scoped
invalid-request responses, and response materialization/serialization failures
now surface storage errors instead of silent connection closes;
AssignReplicasToDirs response materialization/serialization failures now
surface storage errors instead of silent connection closes;
GetTelemetrySubscriptions and PushTelemetry malformed frames now return
generated invalid-request responses, and telemetry subscription serialization
failures now surface storage errors instead of silent connection closes;
ListClientMetricsResources malformed frames now return generated invalid-request
responses, and resource materialization/serialization failures now surface
storage errors instead of silent connection closes;
ListPartitionReassignments malformed frames now return generated
invalid-request responses, and topic materialization/serialization failures now
surface storage errors instead of silent connection closes;
DescribeCluster malformed frames now return generated invalid-request responses,
and response serialization failures now surface storage errors instead of silent
connection closes;
DescribeProducers malformed frames now return generated invalid-request
responses, and producer response materialization failures now surface storage
errors instead of silent connection closes;
DescribeTopicPartitions malformed frames now return generated invalid-request
responses, and paginated topic response materialization failures now surface
storage errors instead of silent connection closes;
generated VoteRequest decode now frees partially materialized topic arrays when
nested allocation fails before handler validation;
ControllerRegistration, AddRaftVoter, and UpdateRaftVoter endpoint
materialization now return storage errors without mutating voter state when
allocation fails;
DeleteTopics partition cleanup removes known local partition state without heap
key formatting;
transaction marker completion/LSO cleanup for long topic names now verifies the
control batch through generated Fetch v12 read-back.

Latest default-suite Metadata read-back tranche: KRaft-follower auto-create
suppression and Metadata authorization-denial paths now recheck that the
requested topic remains absent through generated DescribeTopicPartitions
unknown-topic responses instead of relying only on broker-local topic maps.

## Capability Matrix

| Area | Current status | Required to call complete |
| --- | --- | --- |
| Kafka ApiVersions/version catalog | In progress. Canonical tables now drive broker and controller advertised APIs and version checks, including AutoMQ extensions. Source-level tests audit generated schema counts and the actual broker/controller dispatch switches against the catalogs; manual v3+ ApiVersions responses now encode/decode KRaft feature metadata tagged fields and reject duplicate known feature tags; `test-client-matrix` provides a gated real-client metadata and produce/fetch compatibility hook for kcat, Kafka CLI, kafka-python, confluent-kafka, Java kafka-clients, and Go kafka-go when installed/configured. | Keep expanding golden fixtures and broaden versioned client compatibility runs. |
| Kafka broker APIs | Partial. 95 advertised APIs; many handlers are simplified single-node semantics. Auto-created topics and CreateTopics write topic snapshots before exposing topic creation and roll back local topic/partition visibility when the shared snapshot write fails; KRaft followers now suppress Metadata/Produce client auto-create and wait for leader quorum replay so requests cannot create split-brain local topic ownership; CreateTopics also applies common supported configs at creation time (`retention.ms`, `retention.bytes`, `max.message.bytes`, `min.insync.replicas`, `segment.bytes`, `cleanup.policy`, and `compression.type`), accepts explicit single-replica manual assignments to local or remote broker IDs, persists non-local initial ownership through the partition-reassignment snapshot path, rejects multi-replica assignments until replica-set semantics exist, and rolls back visible topic/partition/ObjectManager/assignment state when local `topics.meta`, assignment metadata, or object snapshots cannot be written after the shared snapshot. Produce now returns per-partition storage errors instead of successful acknowledgements when post-append local partition-state, ObjectManager, or producer-sequence checkpoints cannot be written. Native Snappy, LZ4, and stored Zstd paths now fail closed on truncated literals/payloads, invalid copy/match offsets, negative sizes, output-length mismatches, and trailing bytes instead of returning partially decoded buffers or trapping on size casts; release builds can now opt into explicit native codec system-library linkage with `-Dnative-compression=true` and configurable `-Dnative-compression-libs`. CreatePartitions likewise accepts explicit single-replica assignments for newly added partitions, persists non-local ownership, rejects multi-replica assignments, writes topic snapshots before acknowledging partition-count expansion, and rolls back local partition/ObjectManager/assignment visibility when shared or local topic/ObjectManager/assignment snapshots cannot be written. AlterConfigs and IncrementalAlterConfigs apply the same common topic-config set, persist it through local and shared topic snapshots, and roll back topic config visibility when shared or local topic/ObjectManager snapshots cannot be written; DeleteTopics writes topic snapshots before acknowledging deletion, defers local partition, share-state, and replica-directory cleanup until the shared snapshot succeeds, persists side-state cleanup across local restart, rolls back topic visibility without dropping those side states when the shared snapshot write fails, and reports storage errors when post-snapshot local cleanup checkpoints fail. DeleteRecords now writes a `__cluster_metadata` low-watermark snapshot before acknowledging trims, replays that trim after fresh-dir S3 WAL replacement without hiding later acknowledged records, rolls back visible partition start offsets and ObjectManager trim metadata, and returns per-partition storage errors when shared partition-state, local partition-state, or object snapshots cannot be written. Internal metadata-log snapshot writers for `__cluster_metadata`, `__consumer_offsets`, and `__transaction_state` now propagate partition-state checkpoint failures so request handlers fail closed instead of acknowledging records that may not replay after restart. ConsumerGroupHeartbeat, ConsumerGroupDescribe, ShareGroupHeartbeat, ShareGroupDescribe, ShareFetch, ShareAcknowledge, Initialize/Read/Write/DeleteShareGroupState, ReadShareGroupStateSummary, AlterReplicaLogDirs, DescribeLogDirs, partition reassignment APIs, delegation-token APIs, AlterClientQuotas, AlterUserScramCredentials, DescribeClientQuotas, DescribeCluster, DescribeQuorum, DescribeUserScramCredentials, ElectLeaders, DescribeProducers, DescribeTopicPartitions, DescribeTransactions, GetTelemetrySubscriptions, ListClientMetricsResources, ListTransactions, PushTelemetry, and UpdateFeatures now decode generated requests and return request-scoped generated responses instead of blanket hand-encoded results. | Full schema-valid decode/encode and Kafka-compatible semantics for every advertised version. |
| Kafka generated schemas | Broad. 110 request schemas generated. Default-value serialize/deserialize/calcSize smoke tests now cover all 230 top-level request/response/header/record types across common protocol versions 0-20. Non-default golden wire fixtures now cover all currently advertised generated modules, including request/response headers, Metadata request/response, Produce/Fetch, KRaft quorum/snapshot APIs, broker/controller lifecycle APIs, topic/config/admin APIs, quota/SCRAM/ACL APIs, group/share/transaction APIs, telemetry APIs, all AutoMQ extension APIs, delegation-token APIs, AlterPartitionResponse v3 topic-ID grouping, and Envelope. | Extend semantic vectors and live client compatibility beyond checked-in generated wire fixtures. |
| AutoMQ extension APIs | Implemented locally with quorum-backed metadata starting. Keys 501-519 and 600-602 dispatch through generated schemas; stream/object mutations can be backed by committed Raft snapshot records; KV/node/router/license/node-id/group mutations can be backed by committed Raft records; attached non-leaders fail closed for these metadata mutations; leaders wait for quorum commit before acknowledging attached multi-node metadata mutations; combined controller+broker failover now verifies KV put/get/delete, zone router, node registry including tag clearing, license, node-id allocator, group promote/demote, stream create/prepare/commit/open/close/trim/delete, stream tag clearing, stream-set object commit, manifest stream/group-count probes, partition-snapshot protocol smoke, and stream metadata survive leader kill, replacement-leader mutation, and old-leader restart. Topic-backed partition snapshot content is covered in the default Zig suite. | Finish broader client compatibility fixtures. |
| S3 WAL | Partial. Sync durability path exists and failed uploads are not acknowledged. Filesystem WAL produces now fsync before ack, advance HW on durable write, and replay after local broker restart. Flushed S3 WAL objects can rebuild stream-set metadata idempotently when the local object snapshot is missing, including paginated and XML-escaped ListObjectsV2 responses. S3 WAL recovery now fails closed on unreadable or listed-but-missing WAL objects instead of silently skipping them. S3 WAL object upload has bounded retry for transient put failures, including injected MockS3 failures; failed sync S3 WAL produces, including legacy synchronous WAL object-key allocation failures, do not advance offsets/HW/cache, producer sequence state, or retain duplicate pending entries; S3 WAL flush now also fails closed when stream-range computation or ObjectManager stream-set registration fails, so unindexed objects are not treated as acknowledged; legacy synchronous fallback writes indexed replayable objects, and a replacement local store or broker can rebuild and fetch acknowledged S3 WAL data from object storage, seed the WAL object counter from existing objects, rebuild idempotent producer sequence state from recovered record batches, replay topic metadata/config and ongoing partition reassignment snapshots from recovered `__cluster_metadata` objects, replay committed offsets and consumer group lifecycle snapshots from recovered `__consumer_offsets` objects, replay transaction coordinator snapshots from recovered `__transaction_state` objects, and resume producing without overwriting old WAL keys; S3Storage propagates read/list/range/delete faults; recovery retries after transient get/list failures, listed object misses, and temporary list omissions; real-client listing follows continuation pagination and fails closed on truncated ListObjectsV2 pages without continuation tokens; range fetches return exactly the requested byte window or fail closed; fetch returns storage errors for ObjectManager metadata lookup failures, unreadable indexed S3 objects, and legacy S3 fallback GET faults; mixed hot-cache/cold-S3 fetches do not drop the S3 prefix when a restarted broker has only the tail in LogCache; overlapping S3 WAL offset ranges now fail recovery instead of returning duplicate/conflicting data; stale lower-epoch S3 WAL writers now fail closed before upload when newer epoch objects are visible, and malformed epoch-prefixed WAL keys fence writers instead of being skipped; controller-observed higher KRaft leader epochs, including internal AppendEntries payloads, fence the local S3 WAL writer and reject subsequent produce without advancing offsets/cache/S3 objects in the default Zig suite; malformed object indexes fail cleanly; interleaved stream-set objects fetch only the requested stream blocks; S3 block-cache keys include the exact visible fetch window; partition offsets are repaired from recovered stream metadata; multipart upload rejects missing or malformed part ETags, decodes XML-escaped upload IDs before URI encoding part/complete/abort requests, aborts failed uploads, and fails closed on non-2xx or embedded XML complete errors. Gated MinIO/S3 test steps validate live object round-trip, multipart round-trip, WAL rebuild/fetch, PartitionStore S3 WAL produce/rebuild/resume, and real broker-process kill/replacement recovery including committed offsets. A gated S3 provider matrix wrapper now runs the live MinIO suite across named provider profiles and can fail release jobs when required outage-enabled, process-crash/replacement, ListObjectsV2 pagination, multipart-edge, or multipart-fault profiles are omitted, when process-crash output omits or misattributes the selected provider bucket, or when multipart-fault commands omit provider-context injected/recovered evidence. S3 signing has deterministic coverage for AWS-style default-port Host canonicalization. | Remaining live execution of provider-specific multipart fault scripts and broader controller-integrated fencing. |
| S3Stream object lifecycle | Improved. Create/open/close/delete/trim/describe plus prepare/commit SO/SSO are wired to ObjectManager; object/prepared snapshots and partition offset/HW/LSO state are persisted locally with fsync and covered by broker restart tests. PrepareS3Object now honors request TTL for registry-only allocations and expires stale prepared IDs. Destroyed/prepared object cleanup now fails closed on allocation failures, and compaction split/merge keeps old metadata visible until it has preserved the old S3 keys needed for delete/orphan tracking. Broker-wired compaction now checkpoints ObjectManager metadata through the local snapshot or quorum object-metadata path before retiring old S3 objects, and uploaded split/merge outputs are removed or tracked for orphan retry when post-upload metadata checkpoints fail. | Match full AutoMQ recovery, fencing, quorum-backed object-state replay, and S3-backed metadata durability. |
| Controller/KRaft | Partial. Local Raft/controller scaffolding exists. Controller ApiVersions and quorum/lifecycle RPC framing now use generated schemas for ApiVersions, Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum, FetchSnapshot, BrokerRegistration v0-v2, BrokerHeartbeat v0-v1, UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter, RemoveRaftVoter, and UpdateRaftVoter; telemetry keys 71/72 are no longer treated as voter APIs. Controller unsupported API/version guard frames retry materialization after transient allocation failure. Controller FetchSnapshot advertises key 59 v0-v1, decodes generated flexible requests, rejects malformed frames with `invalid_request`, serves compacted controller full-snapshot record bytes with `max_bytes` chunking and `position_out_of_range` bounds checks when a requested `__cluster_metadata` snapshot exists, returns request-scoped `snapshot_not_found` for unavailable snapshots, and includes v1 current-leader/node-endpoint routing metadata from committed voter endpoints. Controller UnregisterBroker advertises key 64 v0, decodes generated flexible requests, rejects malformed frames, removes registered brokers, and writes replayable broker-unregistration metadata records. ControllerRegistration now advertises key 70 v0, decodes generated flexible requests, rejects malformed frames, fails closed on followers, unknown controller IDs, invalid feature ranges, and invalid listeners, accepts configured voter controllers, and appends replayable voter endpoint metadata for accepted listeners and `kraft.version`. BrokerRegistration v2 log-directory IDs are stored as controller metadata records, replayed after restart/follower promotion, and compacted through full controller snapshot records. BrokerHeartbeat v1 accepts tagged offline log-directory reports, validates them against registered directories, persists latest offline health through controller metadata records and full snapshots, and fences fully-offline brokers. AddRaftVoter now persists supplied voter directory/listener metadata through replayable Raft config records and applies the voter plus endpoint metadata after commit. UpdateRaftVoter now advertises key 82 v0, validates voter existence and KRaft feature ranges, appends replayable Raft config records for voter endpoint metadata, and applies endpoint updates after commit. Committed voter endpoints now update the Raft RPC client pool before elections and replication instead of remaining DescribeQuorum-only metadata, and persisted UpdateRaftVoter endpoint metadata is replayed after static voter registration on controller restart. Controller tests now run in the default Zig suite and cover malformed Vote/ControllerRegistration/AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter, ControllerRegistration invalid feature/listener paths, endpoint-add/update success and error cases, and internal AppendEntries compatibility. A gated controller/broker failover plus rolling-restart harness exists, restarts the killed old leader from a fresh local data dir, broker-only nodes start fenced until controller heartbeat succeeds, and directly probes controller ApiVersions v3, AllocateProducerIds v0, DescribeQuorum v2 on every alive controller, FetchSnapshot v1, generated dynamic voter negative responses, generated broker lifecycle unknown-broker responses, generated ControllerRegistration negative responses, follower `NOT_CONTROLLER` responses for BrokerRegistration/AllocateProducerIds/dynamic voter/broker lifecycle/ControllerRegistration probes, controller unsupported-version/unsupported-key guard responses including ZooKeeper-era generated-only keys 4-7, and broker-port generated non-broker API rejection responses at failover/restart checkpoints. Broker registrations, broker unregistrations, broker rack metadata, log-directory IDs, offline log-directory health, and producer-id allocation cursors are now stored as controller metadata records, replayed from persisted Raft logs on follower promotion/restart, and compacted through full controller snapshot records before Raft truncation. | Add broader broker+controller failover gates. |
| Stateless brokers | Partial. Combined AutoMQ metadata can rejoin from a fresh local data dir through quorum replay, broker-only AutoMQ KV/node/router/license/node-id/group snapshots including KV deletion, node tag clearing, and group demotion plus stream/object metadata snapshots including stream deletion, tag clearing, prepared object TTL replay/expiry, stream and stream-set mark-destroyed object state/deletion readiness, and stream-set object ranges can rebuild from `__cluster_metadata` S3 WAL records, local/S3 object repair covers data-path replacement, topic IDs, partition counts, common topic configs including segment/cleanup/compression policy, finalized feature snapshots including deletion replay, DeleteRecords/retention partition low-watermarks, replica-directory assignment snapshots, ongoing partition reassignment snapshots plus replacement-side local-failover completion checkpoints with generated ListPartitionReassignments and DescribeTopicPartitions read-back, client quota/SCRAM credential/ACL snapshots, and delegation-token lifecycle snapshots including renew/expire replay can rebuild from `__cluster_metadata` S3 WAL records, committed offsets, OffsetDelete/DeleteGroups tombstones, consumer group lifecycle snapshots, and DeleteGroups share-session cleanup snapshots can rebuild from `__consumer_offsets` S3 WAL records, transaction coordinator snapshots plus atomic EndTxn/WriteTxnMarkers commit/abort marker/snapshot objects can rebuild from `__transaction_state` plus data S3 WAL records after fresh-dir broker replacement, idempotent producer sequence state can rebuild from durable record batches after S3 WAL replacement, and client-facing coordinator/reassignment/ACL/produce/transaction-marker mutations now return storage errors when their shared-storage or local checkpoints cannot be written. Local cache/state still has single-node assumptions for broader broker-only metadata. | Rebuild all broker state from shared storage/controller metadata without data loss or manual repair. |
| Reassignment/autobalancing | Partial. Generated reassignment handlers now retain, list, cancel, locally persist ongoing reassignment state across broker restart, replay it from `__cluster_metadata` S3 WAL after fresh-dir broker replacement, commit topic and reassignment snapshot records through Raft when attached to the quorum leader, reject attached non-leader mutations with `NOT_CONTROLLER`, replay committed topic records before quorum assignment/cancellation records after restart or promotion, apply target owners into local failover ownership metadata, expose non-local owners in Metadata/DescribeTopicPartitions, reject local Produce/Fetch to non-local owners, fail closed and roll back local visibility when shared snapshot writes fail, and clear stale entries when a topic is deleted; auto-balancer planning can prefer less-loaded targets in a different rack when topology is available, uses controller-backed broker rack metadata, ignores fenced brokers as targets, moves load off fenced/scale-in leaders, executes validated plan moves through the durable reassignment path, rejects stale/duplicate plans before mutation, ignores stale unknown-node load samples, clamps negative metric rates to zero, propagates planner allocation failures instead of executing partial plans, and has deterministic simulated convergence coverage. Broker-side controller-aware orchestration is covered for fenced-broker movement, simulated convergence after ownership changes, stale-plan fail-closed behavior, no active target no-ops, scheduled `tick()` execution/skip from cached controller/load samples, and fail-closed planner allocation errors. The gated KRaft failover harness now exercises live reassignment protocol convergence, old-owner write fencing, and target-broker topic/data convergence over real broker/controller processes; Docker E2E now selects the active controller leader as reassignment source, can run required load/scale phases through the built-in fixture when `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1` is set, infers missing fixture matrices from `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES`, and has clean `53/53` runs with required fixture-backed `load`, inferred `scale-in`, and fixture-prepared `scale-out` coverage. | Broader live load/scale orchestration plus cross-broker chaos coverage. |
| Consumer groups/transactions | Partial. Core flows exist with simplified persistence; OffsetCommit now reports per-partition commit failures instead of acknowledging failed coordinator mutations, writes versioned internal `__consumer_offsets` records before acknowledging, rolls back local committed-offset visibility when the local offset snapshot cannot be written, validates managed member identity/generation, rejects empty group IDs, preserves committed leader epoch and metadata through OffsetFetch and restart, rejects oversized normal and transactional offset commit metadata, and rejects unknown topic-partitions without committing offsets, OffsetFetch reports missing groups at version-appropriate partition/top/group error levels and rejects empty group IDs at legacy and grouped response levels, OffsetDelete writes `__consumer_offsets` tombstones before deleting local offsets, replays those tombstones during S3 WAL replacement, fails closed on tombstone write failure, rolls back local committed-offset visibility when the local offset snapshot cannot be written, flushes offset mutations, reports missing groups, rejects empty group IDs, rejects subscribed group topics without removing offsets, and reports unknown topic-partitions without deleting stale offsets, DeleteGroups writes tombstones for all group offsets before local group deletion, fails closed on tombstone write failure, rolls back local group visibility on lifecycle snapshot write failure, flushes offset mutations, and returns protocol `ErrorCode` values for invalid, missing, and non-empty group cases, TxnOffsetCommit now advertises v4, requires valid transactional identity/epoch plus prior AddOffsetsToTxn registration, maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v4 clients, validates managed group member identity/generation, rejects empty group IDs and unknown topic-partitions without committing offsets, writes versioned internal offset records before acknowledging, and rolls back local committed-offset visibility when the local offset snapshot cannot be written, broker open can replay committed offsets from recovered S3 WAL `__consumer_offsets` objects, JoinGroup/SyncGroup/LeaveGroup/DeleteGroups now flush local group lifecycle snapshots, JoinGroup/LeaveGroup/SyncGroup restore the previous local group snapshot when rollback snapshots, local member/assignment materialization, or shared lifecycle snapshot writes fail, and JoinGroup/LeaveGroup/SyncGroup/ConsumerGroupHeartbeat return generated invalid-request or storage-error responses instead of dropping malformed frames or failed response serialization, consumer group lifecycle snapshots are also written to `__consumer_offsets` and replayed from recovered S3 WAL, transaction coordinator snapshots are written to `__transaction_state` and replayed from recovered S3 WAL, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and WriteTxnMarkers restore the previous transaction snapshot when their coordinator mutations cannot be written to shared storage, EndTxn now advertises v4 and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v4 commit attempts before marker writes, EndTxn, WriteTxnMarkers, and timed-out transaction aborts in S3 WAL mode now flush marker control batches and the updated transaction snapshot in one shared WAL object before advancing local state, JoinGroup, SyncGroup, LeaveGroup, DeleteGroups, ConsumerGroupHeartbeat, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and WriteTxnMarkers now return storage errors instead of successful acknowledgements when their coordinator snapshot S3 WAL write fails, EndTxn/WriteTxnMarkers marker paths now also return storage errors when post-marker local partition-state, ObjectManager, or transaction checkpoints cannot be written, JoinGroup stores selected protocol metadata, applies and persists session/rebalance timeouts, broker tick enforces each group's configured session timeout, persists group/member protocol metadata across restart, DescribeGroups reports stored group protocol and member metadata/assignments, ConsumerGroupDescribe advertises key 69 v0 and returns generated read-only group state/member/subscription views over the existing group coordinator, ConsumerGroupHeartbeat advertises key 68 v0 with heartbeat-driven membership, persisted KIP-848 owned-assignment echoes, cooperative assignment revocation, generated authorization denial, and no classic rebalance-in-progress rejection, Heartbeat/SyncGroup/LeaveGroup use protocol error codes for missing members/groups and static-member fencing, and rejects incompatible protocol joins, SyncGroup v5 validates protocol type/name against group state, LeaveGroup v3+ can resolve static members by `group_instance_id`, AddPartitionsToTxn now advertises v5 and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v5 clients, AddPartitionsToTxn v4+ and EndTxn validate transactional identity/epoch before mutating state, AddPartitionsToTxn/EndTxn/WriteTxnMarkers fail closed for unknown topic-partitions, WriteTxnMarkers validates local producer epoch/state before writing local markers and skips local completion after any marker partition error, AddOffsetsToTxn now advertises v4, maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v4 clients, validates transactional identity/epoch and the internal offsets partition before registering it, DescribeTransactions reports transaction state, PID/epoch, timeout/start time, grouped partitions, and missing transactional IDs through generated schemas, ListTransactions applies state, producer ID, and duration filters and reports unknown state filters, transaction introspection is covered after local broker restart, timed-out transactions now write abort control markers before coordinator completion, transaction coordinator errors now use protocol `ErrorCode` values for invalid PID/epoch/state paths, Produce v11 validates transactional producer state before append and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` without advancing the log, InitProducerId now advertises v5 and maps aborting transactions to KIP-890 `TRANSACTION_ABORTABLE` for v5 recovery attempts, validates v3+ requested producer id/epoch recovery before bumping, applies transactional timeouts, rejects invalid transactional timeouts, flushes producer allocations, transaction snapshots retain registered partitions, Produce rejects invalid required acks and enforces `min.insync.replicas` for `acks=-1` before append, idempotent producer sequence updates now advance only after durable Produce success, reject same-epoch sequence gaps and stale producer epochs without appending, persist the last sequence in a batch, rebuild from durable log batches after S3 WAL replacement, and fence restored or in-memory per-partition sequence state when InitProducerId bumps a producer epoch, and gated KRaft failover validates OffsetCommit plus legacy and grouped OffsetFetch v8, OffsetCommit/OffsetFetch v9 KIP-848 member identity, OffsetDelete tombstone retention, DeleteGroups group/offset tombstone retention, classic JoinGroup/SyncGroup/Heartbeat/DescribeGroups plus generated ConsumerGroupDescribe/ListGroups/FindCoordinator continuity, KIP-848 ConsumerGroupHeartbeat join/assignment/owned-assignment/subscription-update/duplicate-subscription/unsupported-assignor/heartbeat/rack/leave/rejoin/static-rejoin plus ConsumerGroupDescribe member/subscription/assignment introspection continuity, InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/TxnOffsetCommit/EndTxn commit/abort continuity, DescribeTransactions/ListTransactions introspection continuity, and idempotent Produce v9 duplicate suppression, next-sequence progress, InitProducerId epoch-bump fencing, and next-epoch recovery through controller failover, controller restarts, and broker restart. | Kafka-compatible rebalances, offset lifecycle, transactions, fencing, and coordinator failover. |
| Security | Partial. TLS, SASL, OAuth, SCRAM, and ACL pieces exist. TLS config rejects unsupported JKS keystore/truststore paths, invalid protocol-version ranges, and mTLS client-auth without CA trust anchors; outbound TLS config rejects the same unsupported/risky settings and hostname-aware S3/Raft handshakes fail closed if hostname verification cannot be enabled. Configured cert/key/CA PEM files are fingerprinted after OpenSSL context load, and new TLS accepts reload the context when configured PEM files are rotated or deleted. mTLS principal extraction keeps default `User:<CN>` behavior while supporting strict Kafka-style `ssl.principal.mapping.rules` for common multi-capture DN mapping rules. SASL-enabled brokers reject non-auth APIs until the client completes SASL authentication. Advertised broker APIs now have default-suite ACL resource/operation mapping coverage except pre-auth SASL frames. Generated response-shape coverage now includes Produce/Fetch, ApiVersions/Metadata/CreateTopics/DeleteTopics, config admin, reassignment/logdir/partition admin, quota/SCRAM admin, ACL admin, quorum Vote/BeginQuorumEpoch/EndQuorumEpoch, selected cluster/transaction introspection including UpdateFeatures and DescribeTransactions, telemetry/client-metrics GetTelemetrySubscriptions/PushTelemetry/ListClientMetricsResources, DescribeTopicPartitions, transaction coordinator InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/WriteTxnMarkers/TxnOffsetCommit, topic-scoped ListOffsets/DeleteRecords/OffsetForLeaderEpoch/DescribeProducers, group-introspection ListGroups/DescribeGroups/ConsumerGroupDescribe/ShareGroupDescribe, coordinator/session FindCoordinator/JoinGroup/ConsumerGroupHeartbeat/ShareGroupHeartbeat/ShareFetch/ShareAcknowledge/Heartbeat/LeaveGroup/SyncGroup, share-state Initialize/Read/Write/DeleteShareGroupState/ReadShareGroupStateSummary, offset OffsetCommit/OffsetFetch, group/offset deletion DeleteGroups/OffsetDelete, and all AutoMQ extension keys 501-519/600-602 authorization denials, unsupported-version rejection now runs before ACL denial response construction or SASL pre-auth gating, small generic authorization, unsupported-version, not-controller, and shutdown rejection frames retry materialization instead of dropping the response on a transient allocation failure, restrictive ACLs cannot block SaslHandshake/SaslAuthenticate negotiation, SASL negotiation/authentication responses fail closed on response serialization failures, successful SaslHandshake mechanism selection and SaslAuthenticate principal storage happen only after success frames are materialized, disabled or unnegotiated SASL mechanisms fail closed before authenticating, OAuthBearer auth no longer leaks decoded JWT state on success, OAuthBearer validation accepts provider-style array `aud` claims while rejecting future `nbf` tokens and tokens without `exp`, and OAUTHBEARER sessions now return JWT-bound `SessionLifetimeMs` values and expire before follow-up Kafka requests after the JWT expiry. The real-client matrix can require secured profiles plus bad-SASL, bad-OAuth-token/JAAS/config, bad-TLS-trust, and ACL-denied-produce fail-closed checks for those profiles. | Broader OAuth provider interop suites and live secured-client CI environments. |
| Observability | Partial. Metrics and JSON logging exist; `/health` and `/ready` have shared plain/TLS routing, exact-path tests, and startup/shutdown readiness response coverage. Prometheus HELP/label escaping and labeled-metric arity checks are covered in the default Zig suite, broker metric registration is pinned to the broker API metric catalog, client-metrics APIs expose a default resource plus retained active-client telemetry samples with terminating cleanup and Prometheus counters/gauges for accepted pushes, terminating pushes, retained sample count, retained sample bytes, and external JSONL export success/error/byte totals, export failures fail closed before state mutation, checked-in Grafana/Prometheus alert artifacts are parsed against the registered metric corpus, checked-in Grafana dashboard JSON is strict JSON in both Zig and Python static audits including duplicate-key rejection, dashboard panel IDs/titles/types/targets/grid positions plus closed target-schema and alert group/name structure are statically audited, produce/fetch/request/S3 p99 SLO alerts plus S3 request/byte-rate, cache-miss-ratio, cache byte-gauge, compaction-cycle p99, retained client-telemetry sample, AutoMQ object-manager fanout/prepared/destroyed backlog, consumer-lag, group-coordinator failed-partition, Raft election-churn/vote-rejection, and JMX controller/replica-manager/request-channel/request-total-time/request-local-time/request-remote-time/request-queue-time/response-queue-time/response-send-time/request-error/broker-topic/broker-state/idle/purgatory failure alerts are pinned in the default suite, critical availability/durability alerts are pinned against accidental severity downgrade in the static audit, AutoMQ-compatible Kafka request/gauge metric names including produce/fetch counters and latency histograms plus AutoMQ object-manager stream/object/prepared/mark-destroyed gauges are emitted from the real request and broker-state paths, compaction lifecycle counters are registered before emission, and a broader JMX-compatible request/request-channel/broker-topic/replica-manager/controller/broker-state/purgatory metric set is registered and emitted. | Continue expanding the AutoMQ/JMX metric corpus and operational SLO fixtures. |
| Tests | Improving. Unit/integration tests run under Zig 0.16. Controller, broker registry, and metadata client tests are included in the default suite. `zig build bench` now exercises repeatable local produce/fetch, S3 WAL request-volume, recovery-time, and bounded memory-growth performance gates. Gated `test-client-matrix` can enforce required version, tool, semantic, secured-client, and negative-security profile coverage; gated `test-chaos` now covers broker SIGKILL/local-WAL restart, slow/partial client frames, far-future client timestamps, sync S3 WAL outage fail-closed behavior, CI-provided network-partition hooks, and CI-provided live-S3 outage/heal hooks, and the local safe-scenario gate passes with required SIGKILL, slow-client, clock-skew, and S3-outage coverage; `test-s3-provider-matrix` can enforce required live provider, outage, process-crash/replacement, pagination, multipart-edge, and multipart-fault profile coverage; gated `test-kraft-failover` can now run CI-provided controller/broker network-partition hooks or scheduled `ZMQ_KRAFT_NETWORK_MATRIX` phases before failover/restart sequencing; and gated `test-e2e` exposes the Docker three-node combined-mode cluster suite with required cross-broker chaos/load-scale phase validation, built-in load/scale fixture selection, plus hook context for topic, broker/controller/metrics ports, containers, and MinIO, rejecting duplicate named hook-context entries before fixture orchestration. Deterministic fixture self-tests run in the default suite, `test-protocol-static-audit` mirrors source-level ApiVersions catalog, generated-index, handler-switch, and non-default golden-fixture drift checks, `test-observability-static-audit` mirrors dashboard/alert metric-reference checks, and `test-build-static-audit` pins Python gate build wiring plus Zig 0.16 toolchain documentation without needing Zig test execution. | Add broader multi-node e2e, broader multi-broker chaos, comparative perf, and expanded client matrix gates. |

Latest coordinator failover tranche: gated `test-kraft-failover` now commits
and deletes a dedicated consumer-group offset, deletes an empty group with a
committed offset, then verifies the OffsetDelete tombstone remains visible as a
missing committed offset and the DeleteGroups tombstone remains visible as a
missing group, and verifies an AddOffsetsToTxn/TxnOffsetCommit path remains
durable after controller network-partition phases. It now also verifies
DescribeTransactions/ListTransactions visibility for ongoing and completed
committed and aborted transactions, plus DescribeGroups visibility for stable
classic group protocol metadata and assignment, ConsumerGroupDescribe
visibility for generated group/member epochs, ListGroups v5 visibility for
stable classic group state/type filters, FindCoordinator v4 discovery for group
and transaction coordinators, KIP-848 ConsumerGroupHeartbeat
join/assignment/owned-assignment/subscription-update/duplicate-subscription/unsupported-assignor/heartbeat/rack/leave/rejoin/static-rejoin continuity, and KIP-848
ConsumerGroupDescribe member/subscription/assignment visibility, through controller leader
kill/election, old-leader fresh rejoin, surviving-controller restart, and broker
restart. It also keeps a live share group active with ShareGroupHeartbeat
join/heartbeat/rack metadata and ShareGroupDescribe member/subscription/assignment
visibility through the same failover and restart path, and opens a ShareFetch
session that acquires the initial record, acknowledges the acquired range with
ShareAcknowledge, mutates a separate share-state probe through Initialize/Write
and a delete probe through Delete, and verifies Read plus Summary views at each
checkpoint while advancing the same share-session epoch through both data-plane
APIs. It now also verifies ListOffsets earliest/latest, OffsetForLeaderEpoch
end-offset visibility, DeleteRecords low-watermark visibility,
DescribeTopicPartitions generated topic metadata, DescribeConfigs topic configs,
DescribeLogDirs partition log-dir state, and DescribeCluster
broker/controller endpoint views with the configured cluster ID through the same
controller failover, controller restart, and broker restart checkpoints. It now
also finalizes `metadata.version` through UpdateFeatures v1 and verifies the
finalized feature through ApiVersions v3 tagged fields at each checkpoint. It
also seeds a broad ACL allow rule, creates and deletes a separate topic ACL
through CreateAcls/DeleteAcls v2, and verifies DescribeAcls v2 retains the
allow rule while the deleted ACL stays absent at each checkpoint. It
also produces an idempotent batch before failover and verifies DescribeProducers
visibility for that producer through the same checkpoints, including broker
restart, next-sequence progress, and epoch-bump recovery. It now also sends live
grouped OffsetFetch v8 requests covering a
normal group, a null-topic all-offset fetch, an OffsetDelete-cleared group, a
DeleteGroups-removed group, and a transactional offset-commit group at each
checkpoint. It now also sends OffsetFetch v9 requests against the live KIP-848
group at each checkpoint, covering a valid member, no-identity admin fetch,
unknown member, and stale member epoch, and sends OffsetCommit v9 requests that
commit a KIP-848 member offset while verifying unknown-member and stale-epoch
commits fail without changing the committed offset.

Latest default-suite coordinator read-back tranche: AddPartitionsToTxn
registered-partition persistence now verifies the ongoing transaction and both
registered partitions through generated ListTransactions and DescribeTransactions
before and after a broker reopen from the same local data directory.
ListGroups response-materialization failure coverage now also rechecks the
retained consumer group through generated ConsumerGroupDescribe instead of
relying only on the storage-error response.
ListGroups state/type filter coverage now also rechecks the underlying
consumer groups through generated ConsumerGroupDescribe so filtered-empty
responses are tied to filter behavior rather than missing group state, and
ListGroups v5 now exposes share groups under the `share` type/protocol with a
generated ShareGroupDescribe read-back. KIP-848 ConsumerGroupHeartbeat-created
groups now also surface under the `consumer` group type through generated
ListGroups v5 read-back while classic JoinGroup-backed consumer protocol groups
remain `classic`. DescribeGroups, ConsumerGroupDescribe, and ShareGroupDescribe
generated-response tests now also cross-check their visible groups through
generated ListGroups v5 read-backs.
ConsumerGroupHeartbeat rack metadata coverage now also reuses the generated
ConsumerGroupDescribe read-back helper for the updated rack value.
WriteShareGroupState success coverage now also reuses the generated
ReadShareGroupState read-back helper for the persisted batch state.
DescribeProducers request-scope filtering now also rechecks both the requested
producer state and the intentionally filtered producer state through generated
DescribeProducers read-back helpers.
ListTransactions filter coverage now also proves filtered-but-present
transaction state through generated DescribeTransactions, and the generated
DescribeTransactions partition-detail path cross-checks the same transaction
through generated ListTransactions.
OffsetFetch v7/v8/v9 generated-response coverage now also rechecks retained
committed offsets through generated grouped OffsetFetch v8 read-backs,
including null-topic all-offset enumeration and response-materialization
fail-closed paths after the storage-error response is verified.
Committed-offset authorization-denial response-construction and serialization
failures now also recheck the denied OffsetCommit/OffsetFetch/OffsetDelete
groups through generated OffsetFetch v8 group-level absence read-backs.
Corrupt all-topic committed-offset key coverage now also verifies a valid
scoped committed offset in the same group remains readable through generated
OffsetFetch v8 after the all-topic storage-error response.
DescribeConfigs v0/v4 success coverage and the null-vs-empty
`configuration_keys` path now also recheck the described topics through
generated DescribeTopicPartitions read-backs, so config visibility assertions are
tied to independently visible topic metadata.

Latest default-suite WriteTxnMarkers read-back tranche: unknown-topic rejection
now rechecks topic absence through generated DescribeTopicPartitions, and
authorization denial now rechecks the empty transaction listing plus topic
absence through generated ListTransactions and DescribeTopicPartitions instead
of relying only on broker-local coordinator/topic state.

Latest default-suite topic/legacy-transaction read-back tranche: direct
ensureTopic creation, existing-topic, S3-WAL rollback, DeleteTopics local
partition-state cleanup, S3 topic-config replay, and legacy AddPartitionsToTxn
and EndTxn v0 lifecycle tests now recheck client-visible state through generated
DescribeTopicPartitions, DescribeConfigs, Fetch, ListOffsets, ListTransactions,
and DescribeTransactions responses.
OffsetForLeaderEpoch v0/v4 high-watermark coverage now also cross-checks the
same latest offsets through generated ListOffsets. Local failover tick coverage
now rechecks KRaft-owned and failed-over partition ownership through generated
DescribeTopicPartitions, and CreateTopics partition-storage allocation failure
plus allocator-independent topic-partition cleanup now recheck removed AutoMQ
stream visibility through generated DescribeStreams.

Latest default-suite ensureTopic rollback read-back tranche: partition-state,
failover-ownership, and internal-topic rollback injection tests now restore their
failing allocators and recheck topic absence through generated
DescribeTopicPartitions instead of relying only on broker-local maps.

Latest default-suite semantic-restore fail-closed read-back tranche: malformed
partition-state, replica-directory, reassignment, share-state, and share-session
snapshot entries now recheck surviving topic ownership, empty reassignment state,
uninitialized share state, and absent share-group visibility through generated
DescribeTopicPartitions, ListPartitionReassignments, ReadShareGroupState, and
ShareGroupDescribe responses.

Latest default-suite partition-state rebuild failure read-back tranche: storage
rebuild allocation failures during partition-state restore now recheck the failed
partition remains unavailable through a generated ListOffsets partition error
instead of relying only on the broker-local store map.

Latest default-suite malformed committed-offset replay read-back tranche:
malformed `__consumer_offsets` committed-offset records now recheck the rejected
group remains absent through generated OffsetFetch v8 instead of relying only on
the coordinator-local offset map.

Latest default-suite DeleteTopics malformed-request read-back tranche:
trailing-byte rejection now rechecks the requested topic remains visible through
generated DescribeTopicPartitions instead of relying only on the generated error
response.

Latest default-suite malformed write no-mutation read-back tranche:
trailing-byte rejection for TxnOffsetCommit and OffsetCommit now uses otherwise
valid mutation frames and rechecks the offsets remain absent through generated
OffsetFetch; OffsetDelete now rechecks the retained committed offset through
generated OffsetFetch, and DeleteRecords now rechecks unchanged stream,
low/high watermark, and retained payload visibility through generated
DescribeStreams, ListOffsets, and Fetch.

Latest default-suite transaction trailing-byte read-back tranche:
InitProducerId trailing-byte rejection now rechecks the transaction list stays
empty; AddPartitionsToTxn, AddOffsetsToTxn, and EndTxn trailing-byte rejection
now use valid coordinator preconditions and recheck unchanged transaction state
through generated ListTransactions and DescribeTransactions responses, with
EndTxn also rechecking that no control marker advanced the partition through
generated DescribeStreams, AutomqGetPartitionSnapshot, ListOffsets, and Fetch
v12.

Latest default-suite transaction no-mutation read-back tranche: InitProducerId
S3 snapshot-write failure, trailing-byte rejection, invalid-timeout rejection,
and InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/TxnOffsetCommit
authorization denials now recheck that no transactions are visible through
generated ListTransactions where applicable; denied or rejected InitProducerId,
AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, and TxnOffsetCommit also recheck the
requested transactional IDs are missing through generated DescribeTransactions,
denied AddPartitionsToTxn rechecks requested topics stay unknown through
generated DescribeTopicPartitions, and AddPartitionsToTxn/TxnOffsetCommit
authorization-denial response-construction failures now recheck transaction
absence plus committed-offset absence through generated ListTransactions,
DescribeTransactions, and grouped OffsetFetch v8 where applicable.
Denied serialization failures for WriteTxnMarkers and AddPartitionsToTxn also
recheck retained transaction state, while TxnOffsetCommit, InitProducerId,
AddOffsetsToTxn, and EndTxn denied serialization failures recheck transaction
absence plus grouped-offset absence where applicable.
Group coordinator authorization-denial response-construction failures now
recheck denied DeleteGroups/LeaveGroup groups through generated
ConsumerGroupDescribe, topic-data denial construction failures recheck requested
topics remain absent through generated DescribeTopicPartitions, and
WriteTxnMarkers denial construction failures recheck empty transactions plus
topic absence through generated ListTransactions and DescribeTopicPartitions.
Group coordinator denied serialization failures now also recheck denied
JoinGroup/Heartbeat/LeaveGroup/SyncGroup groups through generated
ConsumerGroupDescribe, and topic-data denied serialization failures recheck
retained topic ownership through generated DescribeTopicPartitions.
Cluster/quorum/election denied failure read-backs now cover DescribeCluster
endpoint state, DescribeQuorum/Vote/BeginQuorumEpoch/EndQuorumEpoch
not-controller visibility, and retained ElectLeaders topic ownership through
generated DescribeCluster, DescribeQuorum, and DescribeTopicPartitions
responses.
Final authorization-denial read-back tranche closes the scanner: denied
ConsumerGroupHeartbeat/ShareGroupHeartbeat/ShareFetch/ShareAcknowledge
serialization failures recheck group absence through generated
ConsumerGroupDescribe/ShareGroupDescribe, and topic-admin denied
serialization/construction failures recheck retained or absent topic visibility
through generated DescribeTopicPartitions.

Latest default-suite ACL read-back tranche: CreateAcls and DeleteAcls mutation,
authorization-denial, serialization-rollback, and local-persistence rollback
tests now recheck retained or removed ACL visibility through generated
DescribeAcls responses instead of relying only on broker-local authorizer
counts.

Latest default-suite ACL no-mutation read-back tranche: invalid CreateAcls
enums, malformed CreateAcls storage-error retries, response-construction and
rollback-snapshot failures, success-serialization failures, S3 ACL snapshot
write failures, and denied DeleteAcls now recheck global empty ACL listings or
retained ACL visibility through generated DescribeAcls.

Latest default-suite quota no-mutation read-back tranche: AlterClientQuotas
authorization denial and denied-response materialization failure now recheck
that the denied client quota remains absent through generated
DescribeClientQuotas after the denial response is verified.

Latest default-suite AutoMQ extension authorization read-back tranche:
stream/KV denied-response serialization and materialization failures now recheck
retained stream metadata, opening-stream visibility, stream counts, retained KV
values, manifest stream counts, and the PrepareS3Object cursor through generated
AutoMQ read APIs after the storage-error denial response is verified. Remaining
AutoMQ metadata extension denied-response serialization failures now also
recheck denied node registration stays absent, retained node and stream metadata
stay visible, license/zone-router state stays empty, manifest counts stay
unchanged, and the GetNextNodeId cursor does not advance before read-back.

Latest default-suite AutoMQ mutation response-construction read-back tranche:
normal mutation response-construction failures now seed retained stream, KV,
zone-router, group, S3 object cursor, and node-id cursor state, then recheck
those states through generated DescribeStreams, GetOpeningStreams, GetKVs,
AutomqZoneRouter, ExportClusterManifest, PrepareS3Object, and GetNextNodeId
read-backs after all storage-error responses are verified.
Read-only AutoMQ response-materialization failures now also seed retained
stream, KV, node, license, group, and topic-partition state, then recheck those
states through generated DescribeStreams, GetOpeningStreams, GetKVs,
AutomqGetNodes, DescribeLicense, AutomqGetPartitionSnapshot, and
ExportClusterManifest read-backs after the storage-error responses are
verified. Read-only AutoMQ final response serialization failures now seed
retained stream, KV, node, license, and group state, then recheck stream,
opening-stream, KV, node, license, and manifest visibility through generated
read-backs after the storage-error responses are verified.
Local AutoMQ success-serialization and persistence rollback failures now also
prove cursor restoration through generated PrepareS3Object and GetNextNodeId
read-backs after rollback, unblocking the local snapshot path where needed for
storage-error injection.

Latest default-suite AutoMQ malformed-error serialization read-back tranche:
normal mutation malformed-frame paths with error-response serialization failure
now seed retained stream, KV, node, license, zone-router, group, S3 object
cursor, and node-id cursor state, then recheck those states through generated
DescribeStreams, GetOpeningStreams, GetKVs, AutomqGetNodes, DescribeLicense,
AutomqZoneRouter, ExportClusterManifest, PrepareS3Object, and GetNextNodeId
read-backs after all storage-error responses are verified.
Read-only AutoMQ malformed-frame paths with error-response serialization
failure now also seed retained stream, KV, node, license, group, and
topic-partition state, then recheck those states through generated
DescribeStreams, GetOpeningStreams, GetKVs, AutomqGetNodes, DescribeLicense,
AutomqGetPartitionSnapshot, and ExportClusterManifest read-backs after the
storage-error responses are verified.

Latest default-suite finalized-feature read-back tranche: direct UpdateFeatures
mutation, validate-only, authorization-denial, local-persistence rollback,
S3-snapshot rollback, and response-materialization/serialization failure tests
now recheck finalized-feature visibility or absence through generated ApiVersions
v3 tagged fields.

Latest default-suite telemetry read-back tranche: PushTelemetry accepted-sample,
export, export-failure, termination, authorization-denial serialization-failure,
and success-serialization-failure tests now recheck active client resource
visibility or absence through generated ListClientMetricsResources responses
instead of relying only on broker-local telemetry sample maps.
GetTelemetrySubscriptions and ListClientMetricsResources authorization-denial
serialization-failure tests now also seed retained client resources and recheck
their generated ListClientMetricsResources visibility after the denied response
fails closed.

Latest default-suite protocol/security tranche: AlterReplicaLogDirs and
delegation-token keys 38-41 are advertised and broker-dispatched through
generated schemas with strict truncated/trailing-byte rejection, generated
authorization-denial responses, and ACL resource/operation mapping coverage.
Delegation-token create/describe/renew/expire now operate against a
broker-local token store with HMAC lookup, owner/requester/renewer principal
metadata, missing-token fail-closed responses, expiry removal coverage, and
local `delegation_tokens.meta` restart persistence plus `__cluster_metadata`
snapshot replay for shared-storage replacement. Create/renew/expire now roll
back visible token state when the delegation-token snapshot cannot be written,
return generated storage errors instead of dropped responses when rollback
snapshots, token materialization, persistence, or success-response
serialization fail, and the renew snapshot-write failure path now rechecks the
retained token expiry through generated DescribeDelegationToken read-back.
Malformed local `delegation_tokens.meta` rows now fail closed during load instead
of being skipped.
SASL/SCRAM token authentication now accepts `tokenauth=true` client-first
messages that use the delegation token ID as SCRAM username and the token HMAC
as the SCRAM secret; successful sessions authenticate as the token owner
principal with a token-bound `SessionLifetimeMs`, bad proofs fail closed,
expired token-authenticated sessions are removed before follow-up Kafka
requests, and token-authenticated sessions cannot call delegation-token
lifecycle APIs.
OAUTHBEARER authentication now requires JWT `exp`, carries it into
`SaslAuthenticateResponse.SessionLifetimeMs`, stores the corresponding SASL
session expiry, and removes expired OAuth sessions before later Kafka requests
fall through to ACL checks. JWT claim parsing now uses structured JSON parsing
instead of substring extraction, so escaped claim strings and array-valued
audiences are handled correctly while duplicate claims fail closed. The
real-client matrix raw JWT fixtures now reject missing-exp or otherwise
broker-invalid positive OAuth tokens before execution, reject non-standard JSON
constants such as `NaN`, `Infinity`, or `-Infinity` plus duplicate object keys in raw JWT payloads, and
pin missing-exp JWTs as valid OAuth-negative vectors. Java/Kafka CLI OAUTHBEARER JAAS
fixtures are now preflighted the same way: positive fixtures must use
`OAuthBearerLoginModule`, a non-empty `sub`, and a future numeric `exp`, while
malformed, missing-sub, missing-exp, expired, and future-`nbf` fixtures count
as OAuth-negative vectors.
Outbound TLS client contexts now validate their own fail-closed surface instead
of inheriting only server-side checks: unsupported JKS/truststore settings,
inverted protocol-version ranges, and partial client cert/key pairs are rejected
before OpenSSL load, CA-only HTTPS client config remains valid, and
hostname-aware client handshakes return `HostnameVerificationUnavailable` instead
of proceeding when OpenSSL cannot enable hostname verification. S3 HTTPS and
Raft/controller outbound TLS calls now use the hostname-aware wrapper so known
peer hosts participate in certificate verification.
Generated controller-only/non-broker
request APIs 56/58/59/62/63/64/67/70/80/81/82 are cataloged in
`api_support.zig` and now probed on the broker port, including the live KRaft
failover gate, to fail closed before body decode.

Latest default-suite observability tranche: JMX-compatible broker-state,
request-handler idle, network-processor idle, request-channel queue,
delayed-operation purgatory, reassigning-partition, and min-ISR gauges are
registered and emitted from broker tick state. Additional JMX-compatible
controller-side gauges (`globaltopiccount`, `globalpartitioncount`,
`activebrokercount`, `fencedbrokercount`, `offlinepartitionscount`,
`preferredreplicaimbalancecount`),
`kafka_log_logmanager_offlinelogdirectorycount`, and the
`kafka_server_replicamanager_failedisrupdatesperseccount_total` counter are
now registered and the gauges emit from broker tick state with default-suite
coverage. The ISR shrink and failed-ISR-update counters now also increment
when the local FailoverController fences nodes during broker tick.
The controller-stat leader-election and unclean-leader-election counters are
now part of the registered JMX corpus, the dashboard pins leader-election,
unclean-election, ISR shrink/expand, and failed-ISR-update series, and the
Prometheus fixtures include critical unclean-leader-election plus warning
leader-election-churn, ISR-shrink, ISR-expand, and failed-ISR-update alerts.
Raft election-start, pre-vote, epoch-change, log append/commit, and
vote-rejection counters are now also pinned by warning alerts for quorum churn
and commit stalls. The network
server now also emits the JMX-compatible socket-server connection-count gauge
and expired-connection-kill counter, and the checked-in Grafana/Prometheus
fixtures pin both metrics, including high connection-count and expired-kill
alerts. The network server also updates the JMX-compatible
response-queue gauge from connections with buffered or pending response sends,
so the request-channel dashboard no longer depends only on the broker tick
placeholder.
The broker tick now also emits JMX-compatible
`kafka_server_groupmetadatamanager_numgroups` and
`kafka_server_groupmetadatamanager_numoffsets` gauges from real consumer-group
and committed-offset state; checked-in Grafana fixtures pin both metrics, and
the Prometheus fixtures alert on high committed-offset fanout per group.
The newer group-coordinator metrics family is registered and emitted as well:
partition-count gauges by state, group-count gauges by protocol, offset-commit
counts from the coordinator commit path, and broker tick gauges for event-queue
depth and coordinator idle ratio; dashboard and alert fixtures pin the event
queue, failed-partition, and idle SLOs. Consumer-lag gauges emitted by the group
coordinator are now pinned in the checked-in Grafana dashboard, Prometheus
fixtures include a sustained high-lag alert, and the broker-level lag test now
rechecks the committed offset through generated OffsetFetch v8 read-back.
The AutoMQ-compatible broker gauge test now also rechecks its seeded committed
offset and reassignment state through generated OffsetFetch v8 and
ListPartitionReassignments read-back.
The same broker tick path now exports GroupMetadataManager per-state group
gauges for empty, preparing-rebalance, completing-rebalance, stable, and dead
consumer groups, with a checked-in alert for lingering dead groups.
The transaction-coordinator metrics family now exports broker tick gauges for
transaction count by status, active transactional IDs, registered transaction
partition fanout, and `__transaction_state` partition counts by state; dashboard
fixtures pin the new series and Prometheus fixtures alert on dead transactions,
high partition fanout, and failed transaction-state partitions. The broker-level
transaction metrics test now also rechecks the active transaction through
generated ListTransactions and DescribeTransactions read-back.
Quota-manager visibility now includes broker tick gauges for explicit client
quota overrides, default quota window fanout, and default produce/fetch/request
limits. Dashboard fixtures pin those gauges plus produce/fetch throttle rates,
and Prometheus fixtures alert when produce or fetch throttling is sustained.
Storage observability fixtures now also pin the registered S3 byte, cache
operation/eviction, cache entry, compaction lifecycle, compaction duration, and
orphaned-key metrics, with alerts for high S3 request/byte rates, orphaned
compaction keys, slow compaction-cycle p99, high cache-miss ratio, and sustained
cache eviction pressure. The memory-oriented gauge panel is now alert-pinned for
large log-cache and S3-block-cache byte usage, and client telemetry alerting now
pins retained sample count in addition to retained bytes and export errors.
JMX-compatible `kafka_network_requestmetrics_errors_total` is now registered
and emitted beside the existing request count/byte/time metrics using the same
request, version, and error labels as the broker's request accounting path;
the checked-in Grafana contract now also pins the JMX request and response
byte-rate series alongside request count, error, and time-series panels, and
Prometheus fixtures now alert on sustained high JMX request and response byte
rates, plus total, local, remote, request queue, response queue, and response
send time SLO breaches.
Replica-manager alert fixtures now also pin
`kafka_server_replicamanager_atminisrpartitioncount` with an at-min-ISR
warning, complementing the existing under-replicated and under-min-ISR
durability alerts.
Checked-in Grafana/readiness fixtures now also pin the AutoMQ-compatible
`Kafka_request_count_total`, request/response byte counters, request-time
counter, and request-error counter, and Prometheus fixtures alert on sustained
AutoMQ-compatible request error ratio, request time, and request/response byte
rates.
The network server now updates the legacy `kafka_server_active_connections`
gauge together with the JMX/AutoMQ connection-count aliases, so all registered
connection gauges reflect the same active socket count.
Dashboard/readiness fixtures now also pin the base broker byte counters,
server member/partition gauges, AutoMQ-compatible broker alias gauges, the
client telemetry export-byte counter, and Raft event counters for elections,
pre-votes, vote grants/rejections, leader wins, epoch changes, log appends,
commits, and snapshots.
The remaining registered JMX counters in that corpus now have deterministic
local emission paths as well: Raft leader promotion increments the
controller-stat leader-election counter, successful unclean ElectLeaders
partitions increment the unclean-leader-election counter, and controller-aware
broker unfence snapshots increment the ISR-expand counter.
The Python observability static audit now also fails if a literal metric
registered in `metrics.zig` lacks a non-test source reference, so checked-in
observability artifacts cannot drift ahead of implementation paths unnoticed.
Metrics scrapes now return a deterministic HTTP 500 response if Prometheus
export or response construction fails, and exporter allocation failures release
partially built response buffers instead of leaking them.
Kafka TCP response enqueue and write failures now fail closed by closing the
connection, rolling back partial response frames, and rejecting oversized frame
headers immediately instead of silently dropping a response or waiting for idle
timeout. The dormant io_uring transport path now uses the same fail-closed
connection semantics for recv-buffer slot exhaustion, missing recv bookkeeping,
recv/send submission failures, failed send completions, and idle cleanup slot
release instead of leaving accepted sockets without pending I/O.
Checked-in Grafana and Prometheus fixtures now reference these
metrics, including no-active-broker, fenced-broker, broker-not-running,
low-idle, request-channel backlog, under-min-ISR, stuck-reassignment,
delayed-fetch purgatory, offline log directory, controller offline partition,
request errors, and preferred-replica imbalance
alerts, and default readiness tests fail if the artifacts drift from the
registered metric corpus. Default-suite Prometheus export coverage pins
HELP/TYPE headers and value rendering for the new JMX gauges and counter. The
Zig readiness audit now also extracts YAML block-scalar PromQL expressions, so
checked-in alert rules cannot hide unregistered metrics by switching from
single-line `expr:` scalars to multi-line block expressions. The Python
observability static audit now also rejects checked-in Prometheus alert rules
whose alert names are not explicitly pinned by the readiness contract, and it
rejects checked-in Grafana PromQL metric references that are not explicitly
pinned by the readiness dashboard metric contract. Checked-in alert PromQL
metric references now use the same explicit readiness metric contract. The Zig
and Python PromQL audits now also recognize exact registered broker API catalog
metric names, even when those counters do not use the usual Kafka/JMX metric
prefixes, while skipping quoted label values so registered API names embedded
as labels are not mistaken for additional series.
The Python observability static audit now also rejects dashboard panels with
non-timeseries types, non-positive or out-of-bounds 24-column grid positions,
target objects outside the closed `expr`/`legendFormat` schema, empty target
legends, and Prometheus alert groups outside the pinned group contract.

Latest runtime-timer tranche: elapsed-time gates now use monotonic clocks for
Python live-harness deadlines and elapsed-duration checks,
TLS handshake timeouts, network idle checks, broker event-loop ticks, graceful
drain, local failover, broker-only metadata-client controller leases, quota
windows, S3 WAL refresh throttling, WAL periodic-fsync and group-commit
intervals, compaction scheduling, cache/fetch session timestamps,
delayed-operation purgatory deadlines, Raft election deadlines, and AutoMQ
quorum metadata commit/propagation waits. Kafka-visible or persisted wall-clock
semantics remain wall-clock based, including record timestamps from live
harnesses and comparative benchmark RecordBatch fixtures, unique object names,
token/session expiry, S3 signing, controller/group heartbeat snapshots, and
transaction start timestamps. The build static audit now derives its Python
monotonic-deadline coverage from the checked-in Python runtime gate list, so
new gated Python harnesses cannot be added without the same wall-clock deadline
screening. The wall-clock deadline screen now uses whitespace-tolerant patterns
instead of exact string fragments, so compact forms such as
`deadline=time.time()+...` fail the static audit as well.

Latest build-static audit tranche: the Python self-test raise-shape catalogue
now covers the checked Python self-test gate list. Self-test raise messages
must remain scanner-supported literal strings, f-strings, concatenated strings, and loop-selected messages;
any new self-test raise message form must extend the build static audit scanner
before it can be counted as deterministic release evidence.
The release-evidence output-marker dispatch catalogue now covers
requirement-specific output validators for broker chaos, client matrix, S3, KRaft, Docker E2E, and benchmark markers.
Any new release-evidence output validator must be listed in the build static audit dispatch catalogue before it can be counted as deterministic marker
evidence.
The unsupported-surface catalogue is also pinned across the release-evidence verifier, release criteria, parity notes, and production-readiness pins:
each known surface label must remain represented, and any new unsupported or partial surface must be added to the build static audit unsupported-surface catalogue before it can affect release evidence.
It also pins the release-evidence unsupported surface status-marker catalogue:
UNSUPPORTED_SURFACE_STATUS_MARKERS entries must stay present in the release
criteria, parity notes, and production-readiness pins so explicit unsupported/partial status markers
cannot drift from the verifier vocabulary. The current unsupported surface
status markers are `unsupported`, `not advertised`, `fail closed`,
`fail-closed`, `generated-only`, `partial`, `blocked`, `blocker`,
`release-ci-required`, `release ci required`, `ci required`, and `must run`.
Any unsupported status change must update the build static audit unsupported-status catalogue.
It also pins the release-evidence unsupported surface text-field catalogue: UNSUPPORTED_SURFACE_TEXT_FIELDS must stay present in the release criteria,
parity notes, and production-readiness pins so unsupported-surface text aggregation continues to scan `id`, `surface`, `status`, `evidence`,
`mitigation`, and `notes`. The current text fields are id, surface, status, evidence, mitigation, and notes. Any unsupported surface text-field change must
update the build static audit unsupported-surface-text-field catalogue.
The build static audit now also keeps a required command catalogue mirror between
the release-evidence REQUIRED_COMMANDS list and the fenced release criteria command block:
the same command lines must stay in the same order, and command-list changes
must update the build static audit command-block catalogue.
It also pins the required environment-variable catalogue: release-evidence REQUIRED_ENV_VARS entries must stay present in the release criteria, parity notes, and production-readiness pins, so every required coverage variable remains documented and changes update the build static audit environment catalogue.
It also pins the command environment-assignment catalogue: per-gate command_env_assignments entries must stay present in the release criteria,
parity notes, and production-readiness pins, so each same-gate command provenance variable remains documented and changes update the build static audit command-env catalogue.
The current command-env gates are `broker chaos harness`,
`external client matrix`, `S3 provider matrix`, `KRaft failover gate`,
`live-S3 benchmark gate`, and `comparative benchmark gate`.
That command-assignment surface includes the live-S3
benchmark gate settings `ZMQ_S3_ENDPOINT`, `ZMQ_S3_PORT`, `ZMQ_S3_BUCKET`,
`ZMQ_S3_SCHEME`, `ZMQ_S3_REGION`, and `ZMQ_S3_PATH_STYLE`.
It also pins the release-evidence command-shape catalogue:
ENV_ASSIGNMENT_RE, ENV_NAME_RE, SHELL_COMMAND_SEPARATORS,
SUCCESS_SHELL_COMMAND_SEPARATOR, DISALLOWED_SHELL_OPERATOR_TOKENS,
DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS, DISALLOWED_COMMAND_LINE_BREAKS,
DISALLOWED_COMMAND_QUOTE_CHARS, DISALLOWED_COMMAND_ESCAPE_CHARS,
ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS, ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS,
and FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS entries must stay present in the
release criteria, parity notes, and production-readiness pins so release gate
commands remain single-line direct invocations. The current command token
regexes are `^[A-Za-z_][A-Za-z0-9_]*=.*$` and
`^[A-Za-z_][A-Za-z0-9_]*$`; shell separators are `&&`, `;`, and `||`, with
`&&` as the success shell command separator. Disallowed shell operator tokens
are `&`, `&>`, `&>>`, `|`, `|&`, `>`, `>>`, `<`, `<<`, `<<<`, `<>`, `<&`,
`>&`, `>|`, `(`, `)`, `{`, and `}`. Disallowed command-substitution,
line-break, quote, and escape tokens are `$(`, `backtick`, `\n`, `\r`,
`single quote`, `double quote`, and `backslash`. Allowed command output marker
fragments are `echo ok: root compose config`, `echo ok: kafka compose config`,
and `echo ok: automq compose config`; allowed multi-segment command chains are
`docker compose -f docker-compose.yml config --quiet && echo ok: root compose config`,
`docker compose -f benchmarks/kafka-compose.yml config --quiet && echo ok: kafka compose config`,
and `docker compose -f benchmarks/automq-compose.yml config --quiet && echo ok: automq compose config`.
Forbidden embedded-output marker fragments include `Build Summary:`,
`tests passed`, `test success`, `bench success`, `bench-compare success`,
`ok:`, `COMPARISON:`, `COMPARATIVE BENCHMARK GATE`, `thresholds:`,
`trend thresholds:`, `trend baseline:`, `result: pass`, `8/8 tests passed`,
`3-Node E2E Test Suite`, `Results:`, `S3 WAL request volume`,
`Live S3 provider`, and `Live S3 request volume`. Any command-shape change
must update the build static audit command-shape catalogue.
It also pins the release-evidence skip-marker catalogue: per-gate skip_markers entries must stay present in the release criteria, parity notes, and production-readiness pins, so each skipped live gate has documented negative-path evidence.
The current skip-marker gates are `broker chaos harness`,
`external client matrix`, `MinIO/S3 integration gate`,
`S3 process-crash replacement gate`, `S3 provider matrix`,
`KRaft failover gate`, `Docker E2E gate`, `live-S3 benchmark gate`, and
`comparative benchmark gate`, with skip markers `skip: set ZMQ_RUN_CHAOS_TESTS=1`,
`skip: set ZMQ_RUN_CLIENT_MATRIX=1`, `skipped`,
`skip: set ZMQ_RUN_PROCESS_CRASH_TESTS=1`,
`skip: set ZMQ_RUN_S3_PROVIDER_MATRIX=1`,
`skip: set ZMQ_RUN_KRAFT_FAILOVER_TESTS=1`,
`skip: set ZMQ_RUN_E2E_TESTS=1`, `Live S3 provider benchmark skipped`, and
`skip: set ZMQ_RUN_BENCH_COMPARE=1`.
Any skip-marker change must update the build static audit skip-marker catalogue.
It also pins the release-evidence output-marker catalogue: per-gate output_markers entries must stay present in the release criteria, parity notes, and production-readiness pins, so each required success marker is documented before release evidence can require it.
The current output-marker gates are `protocol static audit`,
`observability static audit`, `build static audit`,
`root compose config validation`, `Kafka benchmark compose config validation`,
`AutoMQ benchmark compose config validation`, `broker chaos harness`,
`external client matrix`, `MinIO/S3 integration gate`,
`S3 process-crash replacement gate`, `S3 provider matrix`,
`KRaft failover gate`, `Docker E2E gate`, `local benchmark gate`,
`live-S3 benchmark gate`, and `comparative benchmark gate`.
The current output markers are `ok: protocol static audit`,
`ok: observability static audit`, `ok: build static audit`,
`ok: root compose config`, `ok: kafka compose config`,
`ok: automq compose config`, `ok: chaos network-partition source=command`,
`ok: chaos harness passed for`, `ok: client matrix profile`,
`ok: client matrix passed`, `8/8 tests passed`,
`ok: S3 process crash/replacement harness passed`,
`ok: S3 provider live-suite profile`, `ok: S3 provider profile`,
`ok: S3 provider matrix passed`,
`ok: KRaft controller failover harness passed ... source=command`, `network_partition=[`,
`automq_stream_id=`, `automq_deleted_stream_id=`,
`automq_stream_set_object_id=`, `automq_node_id=`,
`automq_zone_router_epoch=`, `old_leader=`, `new_leader=`,
`restarted_controller=`, `epoch=`, `automq_old_leader=`,
`automq_new_leader=`, `old_leader_rejoined=true`,
`old_leader_fresh_rejoin=true`, `automq_old_leader_fresh_rejoin=true`,
`allocate_producer_ids_checked=true`,
`allocate_producer_ids_follower_rejection_checked=true`,
`describe_quorum_v2_checked=true`, `fetch_snapshot_v1_checked=true`,
`all_controller_fetch_snapshot_v1_checked=true`,
`controller_api_versions_checked=true`,
`all_controller_api_versions_checked=true`,
`controller_unsupported_checked=true`, `all_controller_unsupported_checked=true`,
`controller_unsupported_cases=[`,
`dynamic_raft_voter_negative_checked=true`,
`dynamic_raft_voter_follower_rejection_checked=true`,
`all_controller_describe_quorum_v2_checked=true`,
`broker_lifecycle_negative_checked=true`,
`broker_lifecycle_follower_rejection_checked=true`,
`controller_registration_negative_checked=true`,
`controller_registration_follower_rejection_checked=true`,
`broker_registration_follower_rejection_checked=true`,
`broker_non_broker_api_rejection_checked=true`,
`broker_non_broker_api_rejection_cases=[`, `committed_offset=`,
`transactions_checked=5`, `transaction_introspection_checked=true`,
`transaction_abort_checked=true`, `txn_offset_commit_checked=true`,
`offset_fetch_v8_grouped_checked=true`, `log_position_apis_checked=true`,
`delete_records_checked=true`, `delete_topics_checked=true`,
`create_topics_checked=true`, `create_partitions_checked=true`,
`client_quotas_checked=true`, `scram_credentials_checked=true`,
`client_telemetry_checked=true`, `delegation_tokens_checked=true`,
`finalized_features_checked=true`, `acl_admin_checked=true`,
`config_admin_checked=true`, `describe_topic_partitions_checked=true`,
`describe_configs_checked=true`, `describe_log_dirs_checked=true`,
`alter_replica_log_dirs_checked=true`,
`assign_replicas_to_dirs_checked=true`, `elect_leaders_checked=true`,
`describe_cluster_checked=true`,
`idempotent_producer_fencing=true`, `consumer_group_heartbeat_checked=true`,
`describe_producers_checked=true`,
`delete_groups_checked=true`, `classic_group_heartbeats=true`,
`group_describe_checked=true`, `consumer_group_describe_checked=true`,
`list_groups_checked=true`, `find_coordinator_checked=true`,
`share_group_heartbeat_checked=true`, `share_group_describe_checked=true`,
`share_fetch_session_checked=true`, `share_acknowledge_checked=true`,
`share_state_apis_checked=true`, `kip848_describe_checked=true`,
`kip848_rejoin_checked=true`, `kip848_rack_checked=true`,
`kip848_owned_assignment_checked=true`,
`kip848_subscription_update_checked=true`,
`kip848_negative_join_checked=true`, `kip848_static_rejoin_checked=true`,
`offset_commit_v9_member_checked=true`, `offset_fetch_v9_member_checked=true`,
`reassignment_topic=`, `reassignment_target=`, `reassignment_target_offset=`,
`reassignment_old_owner_rejected=true`,
`reassignment_target_fetch_verified=true`, `3-Node E2E Test Suite`,
`[Test m] Cross-broker chaos phases`, `[Test n] Live load/scale phases`,
`Results:`, `=== Benchmarks complete ===`,
`ok: local benchmark gate source=command`,
`ok: live-S3 benchmark gate source=command`, `S3 WAL request volume`,
`PartitionStore memory`, `Live S3 provider`, `Live S3 put`, `Live S3 get`,
`Live S3 request volume`, `COMPARISON:`, `Benchmark`, `ApiVersions`,
`Produce (reuse)`, `Produce (fresh)`, `Fetch`, `Metadata`,
`COMPARATIVE BENCHMARK GATE`, `thresholds:`, and `result: pass`.
Each static audit output marker must appear exactly once as its own stripped
line, and each compose config output marker must appear exactly once as its own
stripped line.
Any output-marker change must update the build static audit output-marker catalogue.
It also pins the release-evidence detail output marker catalogue:
COMPARATIVE_TABLE_ROW_MARKERS, BENCHMARK_OUTPUT_LINE_MARKERS,
KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS, KRAFT_DETAIL_OUTPUT_MARKERS, and
E2E_OUTPUT_LINE_MARKERS entries must stay present in the release criteria,
parity notes, and production-readiness pins. These KRaft, Docker E2E, benchmark, and comparative benchmark detail markers
are checked separately from per-gate summary markers so line-aware verifier
coverage cannot drift. Any detail output marker change must update the
build static audit detail-output-marker catalogue.
It also pins the forbidden command-fragment catalogue: per-gate forbidden fragments must stay present in the release criteria, parity notes, and production-readiness pins, so the local benchmark gate cannot accidentally satisfy live-S3 benchmark evidence with `ZMQ_RUN_BENCH_LIVE_S3=1`.
Any forbidden-fragment change must update the build static audit forbidden-fragment catalogue.
It also pins the release-evidence schema field catalogue:
RELEASE_EVIDENCE_FIELDS, COMMAND_ENTRY_FIELDS, and UNSUPPORTED_SURFACE_FIELDS entries must stay present in the release criteria, parity notes, and production-readiness pins, so every closed schema field remains documented before the manifest verifier can rely on it.
The current release manifest fields are `commit`, `environment`, `commands`,
`unsupported_or_partial_surfaces`, `known_data_loss_bug`,
`advertised_stub_api`, `untriaged_durability_failure`, and `automq_complete`;
the current command entry fields are `command`, `exit_code`, and `output`; and
the current unsupported surface fields are `surface`, `status`, `evidence`,
`id`, `mitigation`, and `notes`.
Any schema-field change must update the build static audit schema-field catalogue.
It also pins the release-evidence blocking-flag catalogue: BLOCKING_FLAGS
entries must stay present as explicit false manifest booleans in the release
criteria, parity notes, and production-readiness pins, so every blocking flag
remains documented before the manifest verifier can rely on it. The current
blocking flags are `known_data_loss_bug=false`, `advertised_stub_api=false`,
and `untriaged_durability_failure=false`. Any blocking flag change must update
the build static audit blocking-flag catalogue.

Latest startup-config fail-closed tranche: Startup configuration must fail closed
on malformed properties lines, empty property keys, blank or embedded-blank
`log.dirs`/`--data-dir` entries, blank S3 string settings, blank CLI string
values, invalid ports/node IDs, invalid S3 scheme/path-style values, invalid S3
WAL flush modes, conflicting `broker.id`/`node.id` aliases, invalid Kafka listener endpoints,
invalid listener-name and listener security-protocol-map settings, invalid `security.protocol`,
invalid `security.inter.broker.protocol`, mutually exclusive
`inter.broker.listener.name`/`security.inter.broker.protocol` settings, and
invalid `ssl.client.auth` before broker/controller storage is opened. Invalid duplicate listener names across listener lists/maps now fail closed instead of picking an
ambiguous endpoint or protocol, and `advertised.listeners` names that do not match configured `listeners`
now fail closed because advertised listener names must match configured listeners when both settings are present. Config-file
application now validates the same comma-separated directory contract as broker
startup, rejects blank and malformed SASL security settings, wires validated
SASL/OAuth settings into executable broker startup, maps standard Kafka
`listeners` to the broker port, maps `controller.listener.names` to the
controller port, maps `inter.broker.listener.name` to broker listener selection,
uses `security.inter.broker.protocol` as the executable security protocol when
no explicit `security.protocol`/CLI value is present,
requires provided `listener.security.protocol.map` settings to cover every
configured listener, applies selected listener-map security protocols, and
derives the executable broker security protocol from the selected listener map
entry when no explicit security protocol is set,
maps `advertised.listeners` to the advertised host, accepts standard KRaft
`node.id` as the executable node id when it matches any configured `broker.id`,
rejects controller-role voter sets whose local voter endpoint does not match
the configured controller listener port; configs that point the local voter at a different controller listener port fail before serving,
controller self-election persistence failures now return startup errors instead
of exiting successfully,
critical startup thread failures for election, metadata-client, and combined-mode
controller serving now return startup errors instead of warning and exiting
successfully,
TLS context initialization failures, including map-selected SSL without cert/key,
now return a startup error instead of exiting successfully, and local replica
directory ID derivation rejects blank directory entries instead
of silently shrinking the advertised JBOD set.

Latest live-hook preflight tranche: operator-provided hook commands for broker
chaos network partitions, S3 provider multipart faults, KRaft network
partitions, and Docker E2E chaos/load-scale phases now fail closed during
deterministic self-tests when a required command is blank, malformed, or cannot
be started, before a live gate can report coverage from an invalid hook. Named
client/S3 profiles and chaos/load-scale/network phases also fail closed when
they use placeholder names, when explicit selector values are blank, or when
distinct coverage names normalize to the same environment-variable token.
The release-evidence verifier now also requires selector/provenance variables
for required live coverage (`ZMQ_CHAOS_NETWORK_MATRIX`,
`ZMQ_KRAFT_NETWORK_MATRIX`, `ZMQ_E2E_CHAOS_MATRIX`,
`ZMQ_E2E_LOAD_SCALE_MATRIX` or fixture-backed inference,
`ZMQ_S3_PROVIDER_PROFILES`, and `ZMQ_CLIENT_MATRIX_PROFILES`) and checks that
required phases/profiles are selected without environment-token collisions
before output markers can satisfy the release manifest.
The build static audit also pins the release-evidence comma-separated environment catalogue:
COMMA_SEPARATED_ENV_VARS must remain derived from REQUIRED_ENV_VARS except
`ZMQ_BENCH_COMPARE_REQUIRE_TREND` and `ZMQ_BENCH_COMPARE_TREND_BASELINE`, so
blank comma-separated entries and duplicate comma-separated entries are checked on list-like coverage variables but
not on the scalar trend-required flag or trend-baseline path. Any comma-separated
environment change must update the build static audit comma-env catalogue.
The build static audit also pins the release-evidence coverage selector catalogue:
COVERAGE_SELECTOR_REQUIREMENTS entries must document
selector, required, label, token_style, and fixture fields for coverage selector assignments. The current
pairs are `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES` to `ZMQ_CHAOS_NETWORK_MATRIX`
for `chaos network phases`, `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` to
`ZMQ_KRAFT_NETWORK_MATRIX` for `KRaft network phases`,
`ZMQ_E2E_REQUIRED_CHAOS_PHASES` to `ZMQ_E2E_CHAOS_MATRIX` for
`E2E chaos phases`, `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` to
`ZMQ_E2E_LOAD_SCALE_MATRIX` for `E2E load/scale phases` with fixture
`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE`, `ZMQ_S3_PROVIDER_REQUIRED_PROFILES` to
`ZMQ_S3_PROVIDER_PROFILES` for `S3 provider profiles`, and
`ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES` to `ZMQ_CLIENT_MATRIX_PROFILES` for
`client matrix profiles`. Any coverage selector change must update the
build static audit coverage-selector catalogue.
It now also requires non-placeholder, nonblank, parseable hook command
provenance for required live phases and S3 outage/multipart-fault profiles,
using either phase/profile-specific hook variables or the documented global
fallbacks, with the documented E2E load/scale fixture exception.
Required S3 sub-profile markers also need truthy release-evidence provenance
for their enabling toggles (`RUN_LIVE_OUTAGE`, `RUN_PROCESS_CRASH`,
`REQUIRE_LIST_PAGINATION`, `REQUIRE_MULTIPART_EDGE`, and
`RUN_MULTIPART_FAULT`) so markers cannot satisfy the manifest without the
matching provider-matrix sub-gate being selected; the S3 provider matrix
command must include those selected enable assignments, including documented
global fallbacks, so activation provenance stays tied to the captured run.
The live provider matrix now also strictly parses those profile/global enable
toggles and the provider `PATH_STYLE`, `SKIP_ENSURE_BUCKET`, and
`SKIP_MINIO_HEALTH` booleans before live execution, so placeholders and
arbitrary strings cannot silently disable coverage.
The release-evidence verifier now strictly parses the same boolean provenance
surface for benchmark trend requirements, E2E fixture toggles, client
enable-go toggles, and S3 provider path-style/enable flags before markers can
count. It also rejects blank, placeholder, non-string, or invalid-scheme S3
endpoint/bucket/credential/region/scheme/TLS CA provenance before provider
markers can satisfy the manifest. Captured environment variables must remain strings
with valid shell variable names, and blank or placeholder values are
rejected, so JSON booleans cannot stand in for actual shell boolean text.
Top-level `ZMQ_RUN_*` opt-in gates and `ZMQ_BENCH_COMPARE_ENFORCE_GATES`
must parse as real booleans in the live harnesses and release-evidence
manifest, so blank, placeholder, or arbitrary values fail closed and cannot
silently skip required coverage.
The build static audit also pins the release-evidence boolean environment catalogue:
BOOLEAN_ENV_VARS, CLIENT_PROFILE_BOOL_SUFFIXES,
E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES, and S3_BOOL_SUFFIXES entries must stay
present in the release criteria, parity notes, and production-readiness pins so
every verifier boolean stays documented. The current fixed boolean variables are
`ZMQ_BENCH_COMPARE_ENFORCE_GATES`, `ZMQ_BENCH_COMPARE_REQUIRE_TREND`,
`ZMQ_RUN_BENCH_COMPARE`, `ZMQ_RUN_BENCH_LIVE_S3`, `ZMQ_RUN_CHAOS_TESTS`,
`ZMQ_RUN_CLIENT_MATRIX`, `ZMQ_RUN_E2E_TESTS`,
`ZMQ_RUN_KRAFT_FAILOVER_TESTS`, `ZMQ_RUN_MINIO_TESTS`,
`ZMQ_RUN_PROCESS_CRASH_TESTS`, `ZMQ_RUN_S3_PROVIDER_MATRIX`,
`ZMQ_CLIENT_MATRIX_ENABLE_GO`, and `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE`. The
dynamic boolean suffixes are `ENABLE_GO`, `FIXTURE_DRY_RUN`,
`FIXTURE_PRESTOP`, `PATH_STYLE`, `SKIP_ENSURE_BUCKET`, `SKIP_MINIO_HEALTH`,
`REQUIRE_LIST_PAGINATION`, `REQUIRE_MULTIPART_EDGE`, `RUN_LIVE_OUTAGE`,
`RUN_MULTIPART_FAULT`, and `RUN_PROCESS_CRASH`. Any boolean environment change
must update the build static audit boolean-env catalogue.
It also pins the release-evidence token vocabulary catalogue:
PLACEHOLDER_ENV_VALUES, BOOL_TRUE_VALUES, and BOOL_FALSE_VALUES entries must
keep placeholder and boolean token values stable across the release criteria,
parity notes, production-readiness pins, and verifier preflight. The current
placeholder tokens are `...`, `placeholder`, `required`, `tbd`, and `todo`;
boolean true tokens are `1`, `on`, `true`, and `yes`; and boolean false tokens
are `0`, `false`, `no`, and `off`. Any token vocabulary change must update the
build static audit token-vocabulary catalogue.
The build static audit also pins the release-evidence S3 string environment catalogue:
S3_STRING_SUFFIXES entries must stay present in the release criteria, parity
notes, and production-readiness pins so nonblank S3 string settings cannot drift
from verifier coverage. The current S3 string suffixes are `ENDPOINT`,
`BUCKET`, `ACCESS_KEY`, `SECRET_KEY`, `REGION`, `SCHEME`, and `TLS_CA_FILE`.
Any S3 string suffix change must update the build static audit S3-string catalogue.
The build static audit also pins the release-evidence S3 provider scoped marker catalogue:
S3_PROVIDER_SCOPED_MARKER_TEMPLATES entries must stay present in the release
criteria, parity notes, and production-readiness pins so profile-scoped provider markers remain tied to live-suite, outage, process-crash, list-pagination, multipart-edge, and multipart-fault coverage. The current templates are
`ok: S3 provider live-suite profile <profile> command_started=true completed=true source=command`,
`ok: S3 provider outage profile <profile> down=true healed=true fail_closed=true recovered=true source=command`,
`ok: S3 provider process-crash profile <profile> killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command`,
`ok: S3 provider list-pagination profile <profile> required=true completed=true source=command`,
`ok: S3 provider multipart-edge profile <profile> required=true completed=true source=command`,
and `ok: S3 provider multipart-fault profile <profile> command_started=true completed=true injected=true recovered=true source=command`.
Any S3 provider scoped marker change must update the build static audit S3-scoped-marker catalogue.
The build static audit also pins the release-evidence sample environment output-marker catalogue:
SAMPLE_ENVIRONMENT_OUTPUT_MARKERS entries must stay present in the release
criteria, parity notes, and production-readiness pins so sample release evidence manifests keep representative live-marker evidence for broker chaos harness, external client matrix, S3 provider matrix, KRaft failover gate, Docker E2E gate, and comparative benchmark gate. The current sample markers are:
`ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=1 source=command`;
`ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command`;
`ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command`;
`ok: chaos s3-outage rejected=true error_code=56 base_offset_negative=true serving=true source=command`;
`ok: chaos network-partition phase broker-link down=true observed=failed healed=true recovered=true expect=fail source=command`;
`ok: chaos network-partition source=command`;
`ok: chaos harness passed for sigkill-restart, slow-partial-client, clock-skewed-records, s3-outage, network-partition source=command`;
`ok: kcat probes (basic,security,security-negative) source=command`;
`ok: client security detail profile kcat_sec tool=kcat protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command`;
`ok: client matrix profile kcat_sec passed for kcat against localhost:9092 version=kcat-1.7.1 source=command`;
`ok: kafka CLI probes (basic,admin,security,security-negative) source=command`;
`ok: client security detail profile kafka_cli_sec tool=kafka-cli protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command`;
`ok: client matrix profile kafka_cli_sec passed for kafka-cli against localhost:9092 version=apache-kafka-cli-3.7.1 source=command`;
`ok: kafka-python probes (basic,admin,groups,security,security-negative) source=command`;
`ok: client security detail profile kafka_python_sec tool=kafka-python protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command`;
`ok: client matrix profile kafka_python_sec passed for kafka-python against localhost:9092 version=kafka-python-2.0.2 source=command`;
`ok: confluent-kafka probes (basic,admin,groups,rebalance,transactions,security,security-negative) source=command`;
`ok: client security detail profile confluent_2_3 tool=confluent-kafka protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command`;
`ok: client matrix profile confluent_2_3 passed for confluent-kafka against localhost:9092 version=confluent-kafka-2.3.0 source=command`;
`ok: java-kafka probes (basic,admin,rebalance,transactions,security,security-negative) source=command`;
`ok: client security detail profile java_3_7 tool=java-kafka protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER oauth=true positive=true security_negative=true oauth_negative=true sasl_negative=false tls_negative=false acl_negative=false source=command`;
`ok: client matrix profile java_3_7 passed for java-kafka against localhost:9092 version=apache-kafka-clients-3.7.1 source=command`;
`ok: go-kafka probes (basic,admin,groups) source=command`;
`ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command`;
`ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command`;
`ok: S3 provider live-suite profile minio command_started=true completed=true source=command`;
`ok: S3 provider profile minio endpoint=127.0.0.1:9000 bucket=zmq-minio-it scheme=http region=us-east-1 path_style=true source=command`;
`ok: S3 provider live-suite profile aws_us_east_1 command_started=true completed=true source=command`;
`ok: S3 provider outage detail profile aws_us_east_1 endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it scheme=https region=us-east-1 path_style=false down=true healed=true fail_closed=true recovered=true source=command`;
`ok: S3 provider outage profile aws_us_east_1 down=true healed=true fail_closed=true recovered=true source=command`;
`ok: S3 provider process-crash detail profile aws_us_east_1 bucket=zmq-aws-it topic=zmq-process-crash group=zmq-process-crash-group killed_broker=true fresh_data_dir=true first_offset=0 committed_offset=1 replacement_offset=2 recovered_payloads=2 source=command`;
`ok: S3 provider process-crash profile aws_us_east_1 killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command`;
`ok: S3 provider list-pagination profile aws_us_east_1 required=true completed=true source=command`;
`ok: S3 provider multipart-edge profile aws_us_east_1 required=true completed=true source=command`;
`ok: S3 multipart fault profile aws_us_east_1 endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it scheme=https region=us-east-1 path_style=false injected=true recovered=true source=command`;
`ok: S3 provider multipart-fault profile aws_us_east_1 command_started=true completed=true injected=true recovered=true source=command`;
`ok: S3 provider profile aws_us_east_1 endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it scheme=https region=us-east-1 path_style=false source=command`;
`ok: S3 provider matrix passed for minio, aws_us_east_1 source=command`;
`ok: KRaft network partition phase leader-isolation down=true observed=failed healed=true healed_leader=1 healed_fetch=true expect=fail source=command`;
`ok: KRaft network partition phase broker-link down=true observed=survived healed=true healed_leader=2 healed_fetch=true expect=survive source=command`;
`ok: E2E chaos phase cross-broker down=true observed=failed healed=true recovered=true expect=fail source=command`;
`ok: E2E chaos passed for cross-broker phase(s) source=command`;
`ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command`;
`ok: E2E load/scale phase scale-in applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command`;
`ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command`;
`ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command`;
`Results: 53/53 passed, 0 failed`;
`ZMQ (Zig)`;
`Apache Kafka`;
`AutoMQ (Java)`;
and `trend thresholds:`. Any sample environment output-marker change must update the build static audit sample-env-output catalogue.
The build static audit also pins the release-evidence build summary and benchmark artifact catalogue:
BENCHMARK_RESULTS_ARTIFACT and ZIG_BUILD_SUMMARY_RE must stay present in the
release criteria, parity notes, and production-readiness pins so the verifier
continues to treat `benchmarks/results.json` as the current comparative trend
artifact, requires the comparative benchmark output line
`Results saved to benchmarks/results.json` after the gate result, requires the
command-owned `ok: comparative benchmark profile ... source=command` marker
after that artifact line, and continues
to parse Zig `Build Summary:` lines with `steps succeeded` and `tests passed` counts. Any build-summary parsing or benchmark
artifact change must update the build static audit build-summary catalogue.
The build static audit also pins the release-evidence hook-provenance catalogue:
PHASE_HOOK_PROVENANCE_REQUIREMENTS, PROFILE_HOOK_PROVENANCE_REQUIREMENTS, and
S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS entries must stay present in the
release criteria, parity notes, and production-readiness pins so phase hook, profile hook, and S3 enable provenance cannot drift from verifier coverage. The
current phase hook rows are `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES` with
`ZMQ_CHAOS_NETWORK`, `chaos network phase`, `DOWN`, `UP`, and `collapsed`;
`ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` with `ZMQ_KRAFT_NETWORK`,
`KRaft network phase`, `DOWN`, `UP`, and `collapsed`;
`ZMQ_E2E_REQUIRED_CHAOS_PHASES` with `ZMQ_E2E_CHAOS`, `E2E chaos phase`,
`DOWN`, `UP`, and `collapsed`; and `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` with
`ZMQ_E2E_LOAD_SCALE`, `E2E load/scale phase`, `APPLY`, `RESTORE`, `collapsed`,
and fixture `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE`. The current profile hook rows are
`ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES` with `ZMQ_S3`, `S3 outage profile`,
`OUTAGE_DOWN`, `OUTAGE_UP`, and `literal`; and
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES` with `ZMQ_S3`,
`S3 multipart-fault profile`, `MULTIPART_FAULT_CMD`, and `literal`. The current
S3 profile enable rows are `ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES` with
`RUN_LIVE_OUTAGE` and `S3 outage profile`;
`ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES` with `RUN_PROCESS_CRASH` and
`S3 process-crash profile`; `ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES`
with `REQUIRE_LIST_PAGINATION` and `S3 list-pagination profile`;
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES` with
`REQUIRE_MULTIPART_EDGE` and `S3 multipart-edge profile`; and
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES` with `RUN_MULTIPART_FAULT`
and `S3 multipart-fault profile`. Any phase hook, profile hook, or S3 enable
provenance change must update the build static audit hook-provenance catalogue.
The Docker E2E load/scale fixture now applies the same fail-closed boolean
parsing to `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE`, fixture `DRY_RUN`, and fixture
`PRESTOP` toggles before it starts or stops containers.
Configured live-harness ports, live-S3 benchmark iteration/payload-size
settings, and E2E fixture phase indexes now also fail closed on placeholders,
malformed text, JSON numbers in release evidence, non-positive ports, and
negative phase indexes instead of falling back to defaults.
E2E fixture target and producer node selectors now also reject blank or
placeholder phase-specific overrides instead of silently falling back to global
fixture nodes or built-in defaults.
Required client-profile markers now need `source=command` plus matching release-evidence
provenance for the profile settings that selected the tools and semantic suites,
exact version labels, Java classpaths, Python executables, pinned Go module versions,
secured-client protocol/SASL/TLS settings, and tool-compatible
positive/negative OAuth fixtures. The verifier rejects `auto` tool selection,
floating `@latest` or implicit-latest go-kafka modules, missing security protocol provenance,
unknown security protocol/SASL mechanism provenance, and
missing OAUTHBEARER positive or negative fixture variables before captured
`ok: client matrix profile ...` markers can satisfy the manifest. It also
rejects blank or duplicate profile-scoped client `TOOLS`/`SEMANTICS`
comma-separated entries and profile semantic/tool mismatches, such as
rebalance, transactional, or security semantics assigned to a client tool that
the live matrix does not probe for that behavior.
The build static audit also pins the release-evidence client capability catalogue:
REQUIRED_CLIENT_TOOLS, REQUIRED_CLIENT_SEMANTICS, CLIENT_SECURITY_PROTOCOLS,
CLIENT_SASL_MECHANISMS, CLIENT_SECURITY_TOOLS, CLIENT_REBALANCE_TOOLS, and
CLIENT_TRANSACTION_TOOLS entries must stay present in the release criteria,
parity notes, and production-readiness pins so client capability validation
cannot drift from verifier coverage. The current required client tools are
`kcat`, `kafka-cli`, `kafka-python`, `confluent-kafka`, `java-kafka`, and
`go-kafka`; the current required client semantics are `basic`, `admin`,
`groups`, `rebalance`, `transactions`, `security`, and `security-negative`; the
current client security protocols are `PLAINTEXT`, `SASL_PLAINTEXT`, `SSL`, and
`SASL_SSL`; and the current client SASL mechanisms are `PLAIN`,
`SCRAM-SHA-256`, and `OAUTHBEARER`. The current security-compatible client
tools are `kcat`, `kafka-cli`, `kafka-python`, `confluent-kafka`, and
`java-kafka`; rebalance-compatible tools are `kafka-python`, `confluent-kafka`,
and `java-kafka`; and transaction-compatible tools are `confluent-kafka` and
`java-kafka`. Any client capability change must update the build static audit client-capability catalogue.
The build static audit also pins the release-evidence client tool marker catalogue:
CLIENT_TOOL_OUTPUT_MARKERS entries must stay aligned with REQUIRED_CLIENT_TOOLS
in the release criteria, parity notes, and production-readiness pins so
per-tool probe markers cannot drift from verifier coverage. The current client
tool markers are `kcat` to `ok: kcat probes`, `kafka-cli` to
`ok: kafka CLI probes`, `kafka-python` to `ok: kafka-python probes`,
`confluent-kafka` to `ok: confluent-kafka probes`, `java-kafka` to
`ok: java-kafka probes`, and `go-kafka` to `ok: go-kafka probes`. Any client
tool marker change must update the build static audit client-tool-marker catalogue.
The build static audit also pins the release-evidence client version/provenance catalogue:
CLIENT_PYTHON_TOOLS and CLIENT_UNPINNED_VERSION_LABELS entries must stay present
in the release criteria, parity notes, and production-readiness pins so Python client matrix profile provenance and exact client/library version validation
cannot drift from verifier coverage. The current Python client tools are
`kafka-python` and `confluent-kafka`, and the current unpinned client version
labels are `auto`, `default`, and `latest`. Any client version/provenance change
must update the build static audit client-version catalogue.
The live client matrix now also strictly parses global
and profile-scoped Go auto-discovery enable flags so placeholders or arbitrary
values cannot silently drop go-kafka auto coverage, and it validates selected
bootstrap values as comma-separated `host:port` entries with nonzero numeric
ports before touching external clients. Release-evidence OAuth fixture validation now mirrors
the client-matrix preflight for raw JWTs, Java/Kafka CLI JAAS configs, and
kcat/librdkafka OAUTHBEARER configs, so
malformed positive fixtures and future-valid negative fixtures fail before
profile markers can count.
Secured/OAuth client evidence now also requires a command-owned client security detail marker
in the same profile block, using
`ok: client security detail profile <profile> ... source=command`, so a same-block client
security detail marker must report the selected tool, protocol, mechanism,
OAuth-positive execution, and compatible negative-vector results before the
matching profile marker can count; the same-block client security detail marker
is required for secured/OAuth profile evidence.
The live client matrix now mirrors the required-profile provenance gate before
touching external clients: required profiles must explicitly select valid
bootstrap, tool, semantic, and exact-version settings; `TOOLS=auto`,
missing/default semantic provenance, placeholder version labels, and `PLAINTEXT`
secured/OAuth profiles fail during preflight, and versioned/security/OAuth
sub-profile requirements must stay within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`.

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
  instead of relying on unchecked generated reads. The gated failover harness
  now also verifies generated dynamic voter negative paths for AddRaftVoter
  empty listeners, RemoveRaftVoter unknown voters, UpdateRaftVoter unknown
  voters, and UpdateRaftVoter invalid KRaft feature ranges at controller
  leader/failover/restart checkpoints without mutating the committed voter set.
  Dynamic voter probes now also verify live follower `NOT_CONTROLLER`
  responses for duplicate AddRaftVoter, unknown RemoveRaftVoter, unknown
  UpdateRaftVoter, and invalid-feature UpdateRaftVoter frames without mutating
  voter membership or endpoints.
  BrokerRegistration v2 now also verifies live follower `NOT_CONTROLLER`
  responses across controller failover/restart checkpoints without registering
  synthetic brokers on non-leaders.
  It also verifies BrokerHeartbeat v1 unknown-broker/offline-log-dir tagged
  responses and UnregisterBroker unknown-broker responses across the same
  checkpoints without mutating broker registration state.
  BrokerHeartbeat and UnregisterBroker now also verify live follower
  `NOT_CONTROLLER` responses across controller failover/restart checkpoints
  without mutating broker registration state.
  AllocateProducerIds v0 now also verifies live follower `NOT_CONTROLLER`
  responses across controller failover/restart checkpoints without allocating
  PID blocks on non-leaders.
  ControllerRegistration unknown-controller, invalid feature-range, and invalid
  listener responses are likewise gated across controller failover/restart
  checkpoints without mutating committed voter endpoints.
  ControllerRegistration now also verifies live follower `NOT_CONTROLLER`
  responses for those non-mutating negative frames across the same checkpoints.
  The same gate now probes each advertised controller key at `max_version + 1`
  plus telemetry keys 71/72, verifying live `unsupported_version` responses
  on every alive controller across controller failover and restart.
  Controller ApiVersions v3 catalog and telemetry-key absence are now checked on
  every alive controller, not just the active leader, after controller failover
  and rolling restart transitions.
  DescribeQuorum v2 endpoint/directory metadata is now checked on every alive
  controller, not just the active leader, after controller failover and rolling
  restart transitions.
  FetchSnapshot v1 unavailable-snapshot routing metadata is likewise checked on
  every alive controller after those transitions.
  Generated controller-only/non-broker request APIs are now rejected on the live
  broker port across the same failover/restart checkpoints.
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
  local/S3 group snapshots, reads back valid and rejected owned-partition
  echoes through ConsumerGroupDescribe, withholds newly targeted partitions
  until the previous owner echoes revocation, exposes current/target assignment
  through ConsumerGroupDescribe, reads back stale-epoch and server-assignor
  rejections without group-state drift, rejects unsupported/incompatible server
  assignors and duplicate subscription names, returns terminal leave responses and
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
  generated authorization denials, read back stale/error paths without
  group-state drift, and describe local share groups through the existing group
  coordinator.
  ShareFetch and ShareAcknowledge now advertise v0, validate local share
  sessions, fetch records from the partition store, return acquired-record
  ranges, validate acknowledgement batches, advance local share start offsets,
  clear sessions on share-member leave/group delete, restore share session
  epochs across local broker restart with generated ShareFetch read-back and
  fresh-dir S3 WAL replacement, write combined share data-plane snapshots to
  `__consumer_offsets`, return generated
  authorization denials, and roll back session plus share-state mutation
  changes, including DeleteGroups cleanup, when local or shared persistence
  fails.
  InitializeShareGroupState, ReadShareGroupState, WriteShareGroupState,
  DeleteShareGroupState, and ReadShareGroupStateSummary now advertise v0,
  validate topic IDs, group IDs, partitions, state epochs, start offsets, and
  state batches while maintaining local share-partition state and restoring it
  across local broker restart with generated ReadShareGroupState read-back and
  fresh-dir S3 WAL replacement.
  Initialize/write/delete mutations now fail closed with default-suite rollback
  coverage when local or shared share-state persistence fails, and all share
  state APIs return generated authorization denials.
  DescribeAcls/CreateAcls/DeleteAcls now use generated schemas, reject malformed
  frames, validate enum fields, write full ACL snapshots to `__cluster_metadata`
  for broker replacement replay, fail closed and roll back local ACL visibility
  when local or shared snapshot writes fail, restore DeleteAcls visibility when
  final success serialization fails, and return generated ACL resources/results.
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
  returns generated QuotaManager-backed per-client quota entries, and emits
  generated storage-error responses when response materialization or
  serialization fails.
  AlterClientQuotas now advertises key 49 v0-v1, decodes generated
  legacy/flexible requests, validates quota entities and keys, supports
  validate-only, mutates/removes QuotaManager-backed client/default quotas,
  writes full quota snapshots to `__cluster_metadata` for broker replacement
  replay, and fails closed without leaving local quota state visible when the
  shared snapshot write fails. Malformed frames and handler materialization or
  serialization failures now return generated schema-shaped per-entry errors
  instead of dropping the connection. Default produce/fetch quotas are now
  enforced per client for clients without explicit overrides, and partial client
  quota overrides fall back to default limits for unset keys.
  DescribeUserScramCredentials now advertises key 50 v0, decodes generated
  flexible requests, rejects malformed frames, describes requested or all
  SCRAM-SHA-256 users, reports missing users per result, and preserves the
  nullable `Users` wire encoding while keeping null and empty as describe-all.
  Response materialization and serialization failures now return generated
  storage-error responses instead of dropping the connection.
  AlterUserScramCredentials now advertises key 51 v0, decodes generated
  flexible requests, upserts/removes precomputed SCRAM-SHA-256 credentials,
  rejects unsupported mechanisms, exposes mutations through Describe, writes
  full credential snapshots to `__cluster_metadata` for broker replacement
  replay, and rolls back local credential visibility when the shared snapshot
  write fails.
  Quota and SCRAM admin authorization-denial builders now also return generated
  invalid-request or storage-error responses when denied request frames are
  malformed or response construction fails; DescribeClientQuotas and
  DescribeUserScramCredentials denied response construction failures now
  recheck retained quota/SCRAM state through generated Describe read-backs.
  ACL authorization-denial serialization and response-construction failures now
  recheck retained ACLs and denied-create absence through generated DescribeAcls
  read-backs.
  UpdateFeatures and ListPartitionReassignments authorization-denial builders
  now do the same for malformed denied frames and response construction
  failures.
  AlterReplicaLogDirs, AlterPartitionReassignments, and AssignReplicasToDirs
  authorization-denial builders now also return generated invalid-request or
  storage-error responses instead of dropping denied connections when malformed
  frames or response construction failures are encountered; denied
  Alter/ListPartitionReassignments serialization failures now recheck empty
  reassignment state and retained topic ownership through generated read-backs,
  while denied AlterReplicaLogDirs and AssignReplicasToDirs serialization
  failures recheck retained default directory visibility through generated
  DescribeLogDirs.
  Telemetry, ListClientMetricsResources, and DescribeTopicPartitions
  authorization-denial builders now follow the same generated fail-closed
  behavior for malformed denied frames and response serialization or
  materialization failures.
  DescribeCluster, DescribeProducers, DescribeTransactions, and
  ListTransactions authorization-denial builders now also fail closed with
  generated invalid-request or storage-error responses when denied frames or
  denial response construction fail; DescribeCluster denied serialization
  failures now recheck generated cluster endpoint state.
  DescribeLogDirs and ElectLeaders authorization-denial builders now also
  return generated invalid-request or storage-error responses when denied
  frames are malformed or denial response materialization/serialization fails,
  with ElectLeaders rechecking retained topic ownership through generated
  DescribeTopicPartitions read-backs.
  CreatePartitions and IncrementalAlterConfigs authorization-denial builders
  now do the same, using generated per-resource error entries when the denied
  request cannot be trusted or the denial response cannot be built; denied
  CreatePartitions response materialization/serialization failures also recheck
  unchanged partition ownership through generated DescribeTopicPartitions.
  DescribeConfigs and AlterConfigs authorization-denial builders also return
  generated per-resource invalid-request or storage-error entries for malformed
  denied frames and denial response construction failures, with AlterConfigs
  and IncrementalAlterConfigs storage-error fallbacks rechecked through
  generated DescribeConfigs read-backs.
  Delegation-token authorization-denial builders now also return generated
  top-level invalid-request or storage-error responses for malformed denied
  frames and denial response serialization failures, with retained tokens
  rechecked through generated DescribeDelegationToken read-backs.
  Vote, BeginQuorumEpoch, EndQuorumEpoch, and DescribeQuorum
  authorization-denial builders now fail closed with generated invalid-request
  or storage-error responses when denied quorum frames or denial response
  construction or serialization fail, with DescribeQuorum and quorum RPC failure
  paths rechecked through generated DescribeQuorum read-backs.
  Transaction authorization-denial builders now also fail closed: malformed
  AddPartitionsToTxn, WriteTxnMarkers, and TxnOffsetCommit denied frames return
  generated invalid-request responses, and InitProducerId,
  AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, WriteTxnMarkers, and
  TxnOffsetCommit denial response materialization or serialization failures
  return generated storage-error responses; ListTransactions denied
  serialization failures now also recheck retained transaction state through a
  generated ListTransactions read-back. Transaction coordinator denied
  serialization failures now also recheck retained or absent transaction state
  through generated ListTransactions/DescribeTransactions read-backs, with
  TxnOffsetCommit also rechecking grouped OffsetFetch absence.
  Group/coordinator authorization-denial builders for FindCoordinator,
  JoinGroup, Heartbeat, LeaveGroup, SyncGroup, DescribeGroups, ListGroups, and
  DeleteGroups now also fail closed with generated invalid-request or
  storage-error responses when denied frames are malformed or denial response
  construction/serialization fails, with denied group mutations rechecked through
  generated ConsumerGroupDescribe read-backs.
  Topic/data authorization-denial builders for Metadata, Produce, Fetch,
  ListOffsets, DeleteRecords, and OffsetForLeaderEpoch now likewise fail closed
  with generated invalid-request or storage-error responses when denied frames
  are malformed or denial response construction/serialization fails, with
  retained topic ownership rechecked through generated DescribeTopicPartitions.
  Normal Metadata, ListOffsets, FindCoordinator, JoinGroup, Heartbeat,
  LeaveGroup, SyncGroup, ConsumerGroupHeartbeat, ListGroups, DescribeGroups,
  ConsumerGroupDescribe, ShareGroupDescribe, OffsetFetch, OffsetForLeaderEpoch,
  DescribeConfigs, DescribeDelegationToken, DescribeAcls, and
  DescribeTransactions/ListTransactions request paths now also return generated
  invalid-request or storage-error responses when client frames are malformed or
  response
  materialization/serialization fails, instead of silently dropping the
  connection. Group membership mutation paths also fail closed when rollback
  snapshots or local assignment/member materialization fail before a response
  can be produced.
  Normal delegation-token Create/Renew/Expire paths now return generated
  invalid-request responses for malformed/trailing frames and generated
  storage-error responses for rollback snapshot materialization, token
  materialization, persistence, or response serialization failures. Their
  success frames are materialized before durable token mutations are
  acknowledged, and the visible token state is restored if a success response
  cannot be built. Delegation-token generated error helpers now also retry with
  storage-error frames if an invalid-request or storage-error response
  allocation fails transiently.
  Normal AutoMQ read paths for GetKVs, GetOpeningStreams, AutomqGetNodes,
  AutomqGetPartitionSnapshot, DescribeLicense, ExportClusterManifest, and
  DescribeStreams now use the same generated invalid-request/storage-error
  fallbacks for malformed frames and response materialization or serialization
  failures, and their read-only error helpers retry generated storage-error
  frames when the initial fallback response serialization allocation fails.
  Normal AutoMQ stream/object mutation paths for CreateStreams, OpenStreams,
  CloseStreams, DeleteStreams, PrepareS3Object, CommitStreamSetObject, and
  CommitStreamObject now also return generated storage errors when response
  construction, rollback snapshots, object-key materialization, local
  ObjectManager mutation, quorum/persistence writes, or final serialization
  fail.
  Normal AutoMQ mutation/controller paths for PutKVs, DeleteKVs, TrimStreams,
  AutomqRegisterNode, AutomqZoneRouter, UpdateLicense, GetNextNodeId, and
  AutomqUpdateGroup now also use generated invalid-request/storage-error
  fallbacks when malformed-frame error responses, early response
  materialization, mutation error responses, or final serialization fail.
  Local AutoMQ metadata mutations now also fail closed before visible state
  changes when rollback snapshots, key/value copies, metadata/license/link
  copies, or map capacity reservations cannot be materialized.
  TrimStreams now delays its local rollback snapshot until a valid trim mutation
  is about to execute, so empty or invalid no-op requests do not drop responses
  under allocation pressure.
  Committed-offset authorization-denial builders for OffsetCommit,
  OffsetFetch, and OffsetDelete now also fail closed with generated
  invalid-request or storage-error responses when denied frames are malformed or
  denial response construction or serialization fails, with denied groups
  rechecked through generated OffsetFetch v8 absence read-backs.
  Topic-admin authorization-denial builders for ApiVersions, CreateTopics, and
  DeleteTopics now do the same for malformed denied frames and denial response
  materialization or serialization failures, with retained or denied topic
  visibility rechecked through generated DescribeTopicPartitions.
  ACL authorization-denial builders for DescribeAcls, CreateAcls, and
  DeleteAcls now fail closed on serialization failures, and the mutation APIs
  also return generated invalid-request/storage-error responses for malformed
  denied frames or denial response construction failures.
  Group/share authorization-denial builders for ConsumerGroupDescribe,
  ShareGroupDescribe, and the share-state APIs now return generated
  invalid-request/storage-error fallbacks for malformed denied frames and
  denial response materialization or serialization failures.
  ConsumerGroupHeartbeat, ShareGroupHeartbeat, ShareFetch, and
  ShareAcknowledge authorization-denial builders now also recover from denied
  response serialization failures with generated storage-error responses, with
  denied consumer/share groups rechecked through generated group-describe
  read-backs.
  AutoMQ stream/object/KV authorization-denial builders for
  Create/Open/Close/Delete/TrimStreams, PrepareS3Object,
  CommitStreamSetObject, CommitStreamObject, GetOpeningStreams, and
  Get/Put/DeleteKVs now do the same for malformed denied frames and denial
  response materialization or serialization failures.
  AutoMQ metadata/controller extension authorization-denial builders for
  AutomqRegisterNode/GetNodes/ZoneRouter/GetPartitionSnapshot,
  Update/DescribeLicense, ExportClusterManifest, GetNextNodeId,
  DescribeStreams, and AutomqUpdateGroup now also return generated
  invalid-request/storage-error fallbacks for malformed denied frames and
  denial response serialization failures, with denied stream visibility
  rechecked through generated DescribeStreams and GetOpeningStreams.
  SaslHandshake and SaslAuthenticate now return generated storage-error
  responses when negotiation/authentication response serialization fails, and
  successful SaslHandshake mechanism selection and SaslAuthenticate principal
  storage happen only after success frames have been materialized.
  Small hand-written generic authorization, unsupported-version,
  not-controller, and shutdown rejection frames now retry materialization on
  transient allocation failure and return storage-error fallback codes where a
  security denial response cannot be serialized.
  Controller-port unsupported API/version guard frames now retry
  materialization after transient allocation failure.
  Controller ApiVersions now decodes generated request bodies, rejects
  malformed flexible frames, and returns generated storage-error responses when
  catalog response materialization fails.
  Read-only controller DescribeQuorum and FetchSnapshot responses now also
  return generated storage-error fallbacks when final response serialization
  fails under allocation pressure.
  Broker-port DescribeQuorum now uses the same generated storage-error
  fallback for malformed/error and final read-only response serialization
  failures.
  Broker-port DescribeDelegationToken normal read paths now return generated
  invalid-request responses for malformed/trailing frames and generated
  storage-error responses for description materialization or final response
  serialization failures.
  ListClientMetricsResources now advertises key 74 v0, decodes generated
  flexible requests, rejects malformed frames, returns a default generated
  resource, and lists active client resources for retained telemetry samples.
  UpdateFeatures now advertises key 57 v0-v1, decodes generated flexible
  requests with correct v1 field gating, rejects malformed frames, invalid
  upgrade types, unsupported features, and unsupported finalized versions,
  honors validate-only requests, mutates local finalized feature metadata for
  supported features, writes finalized feature snapshots including deletions to
  `__cluster_metadata` for broker replacement replay, fails closed with rollback
  when shared or local persistence fails, persists it across local restart, and
  exposes supported and finalized features through ApiVersions v3+ tagged fields.
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
  Controller-side trailing-byte coverage now also runs end-to-end through
  `Controller.handleRequest` for Vote, EndQuorumEpoch, UnregisterBroker,
  AddRaftVoter, RemoveRaftVoter, BrokerRegistration, BrokerHeartbeat,
  AllocateProducerIds, DescribeQuorum, ControllerRegistration,
  UpdateRaftVoter, and FetchSnapshot, asserting schema-shaped
  `invalid_request` responses with preserved correlation IDs and
  throttle/error prefixes. BrokerRegistration, BrokerHeartbeat,
  AllocateProducerIds, and DescribeQuorum now enforce
  `pos == request_bytes.len` after generated decode in addition to existing
  per-validator coverage for Vote/EndQuorumEpoch/UnregisterBroker/
  AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter/ControllerRegistration/
  FetchSnapshot.
  Vote v1 and EndQuorumEpoch v1 responses now populate `node_endpoints`
  with the current leader's controller listener, matching Kafka KIP-595
  wire semantics. EndQuorumEpoch v1 frame validation no longer expects the
  v0-only `preferred_successors` array; the validator now matches the
  generated serializer's per-version layout.
  BeginQuorumEpoch v1 responses also populate `node_endpoints` symmetrically.
  All 22 AutoMQ extension key handlers (501-519, 600-602) now use a strict
  `parseGeneratedRequestStrict` that fails closed with schema-shaped
  `invalid_request` when clients append trailing bytes the schema does not
  consume; default suite pins end-to-end trailing-byte rejection for every
  AutoMQ extension key with preserved correlation IDs and throttle/error
  prefix ordering.
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
  acknowledging these AutoMQ metadata/object mutations. AutomqRegisterNode now
  stores request tags and AutomqGetNodes returns them;
  those node tags are encoded in local `automq.meta` v2 snapshots, rollback
  snapshots, committed register-node records, and full-snapshot-v2 compaction
  records while preserving replay compatibility with older untagged records.
  CreateStreams v1 now stores stream tags, OpenStreams v1 replaces current
  stream tags with the supplied v1 tag set, and DescribeStreams returns the
  current tags. Stream tags are encoded in ObjectManager v3 snapshots,
  ObjectManager rollback snapshots, committed object snapshot records, and
  full-snapshot compaction records while preserving load compatibility with
  older untagged ObjectManager snapshots.
  In local mode,
  PutKVs/DeleteKVs, AutomqRegisterNode, AutomqZoneRouter, UpdateLicense,
  GetNextNodeId, and AutomqUpdateGroup now use a durable AutoMQ metadata
  snapshot boundary and roll back visible metadata changes when that write
  fails. PutKVs/DeleteKVs serialize completed responses before local metadata
  persistence, restore the local metadata snapshot if response serialization
  fails, and cover KV deletion visibility with generated GetKVs read-back.
  AutomqRegisterNode, AutomqZoneRouter, UpdateLicense, GetNextNodeId,
  and AutomqUpdateGroup materialize success frames before visible local
  metadata mutation, and these local metadata mutation paths also reserve
  required copies and map capacity before committing or mutating state,
  returning generated storage errors when materialization fails.
  CreateStreams, OpenStreams, CloseStreams, DeleteStreams, TrimStreams,
  PrepareS3Object, CommitStreamSetObject, and CommitStreamObject serialize
  completed responses before durable ObjectManager persistence and restore the
  local ObjectManager snapshot if response serialization fails. Stream/object
  handlers now also materialize ObjectManager rollback snapshots and S3 object
  keys before mutation, restore visible state on local
  mutation or quorum/persistence failures, and return
  generated storage errors instead of dropping responses under allocation
  pressure. Single-node leaders now
  compact these records by appending a full AutoMQ metadata/ObjectManager
  snapshot record before Raft truncation. Internal AppendEntries now carries
  actual log entry bytes and followers apply appended AutoMQ metadata and object
  snapshot records, including follower-promotion replay coverage. Gated
  `test-kraft-failover` now starts an additional
  combined controller+broker quorum and verifies AutoMQ KV put/get/delete, zone
  router metadata, node registry including tag clearing, license, node-id allocator, group
  promote/demote, stream
  create/prepare/commit/open/close/trim/delete metadata, stream tag replacement
  and clearing,
  stream-set object commit, manifest stream/group-count probes, and
  partition-snapshot protocol
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
  LogCache and generated Fetch v12 read-back after restart, and malformed
  filesystem WAL segment names plus malformed or corrupt partition WAL records
  fail closed during broker open instead of being skipped; existing unreadable or malformed
  broker-local snapshots now fail closed during load for topics, offsets,
  consumer groups, transactions, producer sequences, partition visibility
  state, reassignments, replica-directory assignments, share-group state and
  session epochs, delegation tokens, finalized features, ACLs, AutoMQ metadata,
  and object/prepared registry snapshots while still treating missing snapshots
  as empty/default state;
  filesystem WAL acks now wait for fsync and advance HW only after that barrier;
  S3 WAL object indexes now rebuild ObjectManager stream-set metadata if the local snapshot is absent and
  handle paginated and XML-escaped ListObjectsV2 recovery; duplicate S3 WAL keys
  are skipped during rebuild so repeated recovery is idempotent; unreadable S3
  WAL objects now fail startup/recovery when no local object snapshot exists
  instead of being silently skipped; S3 WAL object upload now has bounded
  transient-failure retry; failed sync S3 WAL produces now return errors without
  advancing offsets/HW/cache or retaining duplicate pending entries, and S3 WAL
  flush fails closed when stream-range computation or ObjectManager stream-set
  registration fails; legacy synchronous S3 WAL fallback writes indexed
  replayable objects before acknowledgement and is covered through
  replacement-side append plus second fresh replay; replacement
  local stores can rebuild acknowledged S3 WAL produce data from object storage
  and fetch it after offset repair; fetch returns
  KAFKA_STORAGE_ERROR for ObjectManager metadata lookup failures, unreadable
  indexed S3 objects, and legacy S3 fallback GET faults instead of silently
  returning an empty success; internal compacted-topic log compaction now
  propagates cache allocation and malformed record-batch parse errors instead
  of silently skipping corrupt internal batches; broker-owned internal log
  replay now fails closed on malformed record-batch headers, truncated records,
  and trailing bytes; Produce rejects full-size malformed record-batch headers
  with `CORRUPT_MESSAGE` before append; idempotent Produce reserves
  producer-sequence state before append and fails closed on reservation errors;
  compaction split/merge/destroy cleanup now
  preserves orphan S3 keys before metadata removal, checkpoints replacement
  ObjectManager metadata before deleting old S3 objects, removes or tracks
  uploaded replacement objects when post-upload metadata checkpoints fail, and
  destroyed-object collection removes metadata only after all delete keys have
  been copied;
  partition next offset/HW/LSO are repaired from
  recovered S3 stream metadata when partition_state.meta is missing or stale,
  and S3 WAL object rebuild, partition-offset repair, S3 WAL resume, and
  DeleteRecords trim/rollback recovery now recheck stream offsets through
  generated DescribeStreams read-back; S3 WAL object-refresh, partition-repair,
  resume, DeleteRecords trim/rollback, and local partition-state
  restart/clamp/metadata-lookup paths also recheck topic-partition snapshot
  visibility through generated AutomqGetPartitionSnapshot; DeleteRecords
  successful no-op trim plus trailing-byte, rollback-materialization, and
  authorization-denial no-mutation paths also recheck generated DescribeStreams,
  partition snapshot, ListOffsets, and Fetch/no-record visibility; S3 WAL
  object-refresh, partition-repair, resume, and local partition-state
  restart/clamp/metadata-lookup paths also recheck earliest/latest visibility
  through generated ListOffsets, and the restart/replacement paths verify
  retained record ranges through generated Fetch v12 read-back;
  interleaved stream-set objects now fetch by matched index so one partition
  cannot read another partition's S3 blocks; malformed object indexes and block
  ranges now fail with parser errors instead of traps or bogus reads; S3 block
  cache keys include start/end/max-bytes/isolation so cached S3 data is not
  reused for the wrong visible fetch window; multipart upload rejects missing or
  malformed part ETags before completion; common topic configs are now included
  in local topic metadata snapshots and rechecked after restart through
  generated DescribeConfigs read-back, finalized-feature local restart now
  rechecks generated ApiVersions v3 visibility, and topic IDs, partition counts,
  supported configs, finalized feature snapshots including deletion replay, and
  replica-directory assignment snapshots are now written to
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
  ObjectManager, or transaction checkpoints cannot be written, and long valid
  topic names no longer bypass PartitionStore Produce/Fetch/info lookups,
  broker request validation, ListOffsets, partition-state restore,
  OffsetCommit lag calculation, DeleteRecords trims, marker state updates,
  EndTxn marker LSO cleanup, or log-dir estimates through formatted partition
  keys; AddPartitionsToTxn coordinator allocation failures now return storage
  errors without advancing empty transactions into ongoing state; broker tick
  now enforces each group's configured session timeout
  instead of a global timeout;
  S3Client multipart fault tests now cover missing/malformed/XML-unsafe part ETags,
  abort-after-part failure, abort-after-complete failure, and HTTP 200
  CompleteMultipartUpload responses carrying embedded XML errors; a gated
  `test-minio` build step now covers live object round-trip, multipart
  round-trip, S3 WAL metadata rebuild plus fetch, and PartitionStore S3 WAL
  produce/rebuild/resume plus second fresh-dir continuation against MinIO/S3,
  with release commands explicitly
  enabling `ZMQ_S3_REQUIRE_MULTIPART_EDGE=1` and
  `ZMQ_S3_REQUIRE_LIST_PAGINATION=1` so the eight-test gate cannot pass with
  provider-edge subtests skipped. Direct MinIO/S3 and process-crash live gates
  now reject blank/placeholder S3 endpoint, bucket, credential, region, TLS CA,
  and non-positive port settings, and strictly parse S3 boolean toggles such
  as `ZMQ_S3_PATH_STYLE`, `ZMQ_S3_SKIP_ENSURE_BUCKET`,
  `ZMQ_S3_SKIP_MINIO_HEALTH`, `ZMQ_S3_REQUIRE_MULTIPART_EDGE`, and
  `ZMQ_S3_REQUIRE_LIST_PAGINATION`, so invalid or placeholder values cannot
  silently select default behavior;
  OffsetCommit and TxnOffsetCommit now
  write versioned `__consumer_offsets` records before acknowledging and broker
  open replays committed offsets from recovered S3 WAL objects; OffsetCommit,
  TxnOffsetCommit, and OffsetDelete now restore the previous local committed
  offset snapshot when `offsets.meta` cannot be written; OffsetDelete and
  DeleteGroups now write `__consumer_offsets` tombstones before local removal,
  replay those tombstones during fresh-dir replacement, and preserve local
  offsets/groups when the shared tombstone write fails; OffsetFetch-all now
  returns storage errors instead of silently omitting corrupt committed-offset
  keys during coordinator enumeration; DeleteGroups,
  JoinGroup, LeaveGroup, and SyncGroup now restore the previous local group
  snapshot when their shared lifecycle snapshot write fails; DeleteGroups
  share-session cleanup snapshots replay during fresh-dir replacement so stale
  share sessions for deleted groups stay absent; share data-plane snapshot writes
  restore prior session/state visibility when shared persistence fails; client quota
  configuration snapshots are now appended to `__cluster_metadata`, replayed
  from recovered S3 WAL on fresh-dir broker replacement, and rolled back on
  failed snapshot writes before AlterClientQuotas is acknowledged; SCRAM
  credential snapshots are likewise appended to `__cluster_metadata`, replayed
  during broker replacement, and rolled back before AlterUserScramCredentials is
  acknowledged when the snapshot write fails; ACL snapshots are appended to
  `__cluster_metadata`, replayed during broker replacement, and rolled back
  before CreateAcls/DeleteAcls are acknowledged when local or shared ACL
  snapshot writes fail; delegation-token lifecycle snapshots are appended to
  `__cluster_metadata`, replayed during broker replacement including renewed
  expiry timestamps and immediate expiry removals, and rolled back before
  CreateDelegationToken/RenewDelegationToken/ExpireDelegationToken
  acknowledgement when shared snapshot writes fail; UpdateFeatures finalized
  feature snapshots are appended
  to `__cluster_metadata`, replayed during broker replacement including deletion
  snapshots, and rolled back before acknowledgement when shared or local
  finalized-feature snapshot writes fail; AssignReplicasToDirs replica-directory
  assignment snapshots are appended to `__cluster_metadata`, replayed during
  broker replacement, pruned when topic snapshots remove referenced partitions,
  and rolled back before acknowledgement
  when shared or local assignment snapshots fail;
  broker-only AutoMQ KV/node/router/license/node-id/group mutations append full
  AutoMQ metadata snapshots to `__cluster_metadata` before local `automq.meta`
  persistence, replay them from recovered S3 WAL during fresh-dir replacement,
  cover deleted KVs, cleared node tags, demoted groups, and protocol-visible
  GetKVs/GetNodes/ZoneRouter/DescribeLicense/GetNextNodeId/ExportClusterManifest
  read-back, including replacement-side ZoneRouter/UpdateLicense continuation
  through a second fresh-dir replay, and skip stale local `automq.meta` loads
  when shared replay supplies metadata;
  broker-only AutoMQ stream/object metadata snapshots are likewise appended to
  `__cluster_metadata`, replayed during fresh-dir replacement including stream
  deletion, tag clearing, prepared object TTL replay/expiry, stream and
  stream-set mark-destroyed object state/deletion readiness, and stream-set
  object ranges, verify generated DescribeStreams/ExportClusterManifest
  read-back, and are followed by S3 WAL object refresh with
  default-suite coverage so
  the snapshot record does not hide later WAL objects;
  atomic EndTxn and WriteTxnMarkers S3 WAL objects now restore both commit/abort
  marker partition offsets and completed transaction snapshots after fresh-dir
  replacement;
  `test-s3-process-crash`
  adds a gated real broker-process kill/replacement harness against MinIO/S3 and
  now verifies both data and committed offsets after a fresh local data dir
  replacement. The local live gate passes after single-node controllers elect
  before serving requests and the harness emits valid Kafka v0 MessageSet
  records so broker offset rewrites preserve payload values. Its release
  marker now includes non-placeholder bucket/topic/group values,
  `killed_broker=true`, `fresh_data_dir=true`, `first_offset=0`,
  `committed_offset=1`, `replacement_offset=<offset>`, and
  `recovered_payloads=2`, and the live harness now rejects placeholder S3
  endpoint, bucket, credential, scheme, region, path-style, TLS CA, and
  non-positive port settings before starting the broker, so captured evidence
  cannot rely on a bare process-crash success line.
  `test-s3-provider-matrix` now runs the live MinIO/S3 suite across
  named S3-compatible provider profiles with per-profile endpoint, port, bucket,
  credential, scheme, region, TLS CA, path-style, existing-bucket,
  ListObjectsV2 pagination through
  `ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES`, required live
  multipart-edge coverage through
  `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES`, provider-specific
  multipart-fault commands through
  `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES`, and required
  process-crash/replacement coverage through
  `ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES`. Required outage,
  process-crash/replacement, ListObjectsV2 pagination, multipart-edge, and
  multipart-fault sub-profiles must also be listed within
  `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`, and release evidence must include
  per-required coverage markers such as
  `ok: S3 provider live-suite profile ... command_started=true completed=true source=command`,
  `ok: S3 provider profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command`,
  `ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command`,
  `ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command`,
  `ok: S3 provider list-pagination profile ... required=true completed=true source=command`,
  `ok: S3 provider multipart-edge profile ... required=true completed=true source=command`, and
  `ok: S3 provider multipart-fault profile ... command_started=true completed=true injected=true recovered=true source=command`, plus
  `ok: S3 provider matrix passed for <profiles> source=command` with `<profiles>`
  exactly matching `ZMQ_S3_PROVIDER_PROFILES`. The S3 provider matrix command must include
  `ZMQ_S3_PROVIDER_REQUIRED_PROFILES` and `ZMQ_S3_PROVIDER_PROFILES`, required
  sub-profile selector assignments, and matching truthy
  `ZMQ_S3_<PROFILE>_{RUN_LIVE_OUTAGE,RUN_PROCESS_CRASH,REQUIRE_LIST_PAGINATION,REQUIRE_MULTIPART_EDGE,RUN_MULTIPART_FAULT}`
  or documented global fallback enable assignments from the manifest, so
  profile coverage cannot be claimed from a different selector than the executed
  gate used. Release evidence must also record
  non-placeholder
  `ZMQ_S3_<PROFILE>_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`
  profile settings or documented global
  `ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` fallbacks,
  and non-`minio` provider profiles must set explicit profile/global S3 settings
  instead of inheriting built-in local MinIO defaults. Each profile marker must
  match the selected profile/global endpoint and
  effective scheme/region/path-style settings; `SCHEME` must parse as `http` or `https`,
  `PATH_STYLE` must parse as `true` or `false`, and the live provider matrix now
  rejects blank/placeholder endpoint, bucket, credential, region, configured
  TLS CA, outage-hook, multipart-fault command settings, and
  placeholder/invalid profile/global provider boolean toggles before live
  execution starts. The
  live-suite and sub-profile markers must appear in the same profile block
  before the matching provider-settings profile marker, and required
  multipart-fault commands must emit
  `ok: S3 multipart fault profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> injected=true recovered=true source=command`
  matching the selected provider context, and the matrix now preserves that
  command-owned marker in the same profile block before it prints its
  multipart-fault release marker. Provider profile, sub-profile, and detail
  markers must appear before the final S3 provider matrix summary, so
  post-summary provider blocks cannot satisfy provider evidence. The S3 provider matrix summary must appear exactly once with `source=command`
  as its own stripped line. S3 request signing now honors configured regions, supports virtual-hosted
  addressing, omits default HTTP/HTTPS ports from the canonical Host header for
  AWS-style providers, and preserves explicit custom ports. Real-client
  ListObjectsV2 pagination now follows continuation tokens and fails closed when
  a provider marks a page truncated without returning a token. The local MinIO
  provider matrix has been executed with required provider, ListObjectsV2
  pagination, multipart-edge, and process-crash/replacement coverage enabled.
  Remaining
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
  4-7 are generated-only in KRaft/AutoMQ mode; broker and controller ApiVersions omit them,
  neither port has a dispatch/no-op path for them, and direct broker/controller
  probes fail closed before body decode. Controller ApiVersions now has a separate audited support
  catalog; generated controller quorum/lifecycle
  framing is in place for Vote, BeginQuorumEpoch, EndQuorumEpoch,
  DescribeQuorum, FetchSnapshot, BrokerRegistration, BrokerHeartbeat,
  UnregisterBroker, AllocateProducerIds, ControllerRegistration, AddRaftVoter,
  RemoveRaftVoter, UpdateRaftVoter endpoint-update handling, and
  DescribeQuorum v2 endpoint/directory metadata, and
  live generated dynamic voter negative responses remain stable across
  controller leader failover and rolling restart without mutating voters, and
  live dynamic voter follower `NOT_CONTROLLER` responses remain stable without
  mutating voters or endpoints, and
  live BrokerRegistration follower `NOT_CONTROLLER` responses remain stable
  without registering synthetic brokers on non-leaders, and
  live AllocateProducerIds follower `NOT_CONTROLLER` responses remain stable
  without allocating PID blocks on non-leaders, and
  live BrokerHeartbeat/UnregisterBroker unknown-broker responses remain stable
  without mutating broker registrations, and live follower `NOT_CONTROLLER`
  responses remain stable for those broker lifecycle probes, and
  live ControllerRegistration negative responses remain stable without mutating
  committed voter endpoints, and live follower `NOT_CONTROLLER` responses
  remain stable for those ControllerRegistration probes, and
  live unsupported-version/unsupported-key controller guard responses remain
  stable across the advertised controller catalog on every alive controller, and
  Controller ApiVersions v3 catalog visibility is checked on every alive
  controller after failover/restart transitions, and
  DescribeQuorum v2 endpoint metadata is checked on every alive controller
  after failover/restart transitions, and
  FetchSnapshot v1 current-leader routing metadata is checked on every alive
  controller after failover/restart transitions, and
  generated controller-only/non-broker request APIs are rejected on the live
  broker port across failover/restart transitions, and
  keys 71/72 remain telemetry-only/unsupported on the controller port.
  Generated controller-only/non-broker request APIs
  56/58/59/62/63/64/67/70/80/81/82 are cataloged in `api_support.zig` and
  likewise rejected on the broker port before generated body decode. A gated
  `test-kraft-failover` step covers three controller-only processes, a
  broker-only process, controller leader discovery, broker produce before
  failover, leader kill, replacement leader convergence, controller rolling
  restart, killed old-leader restart/rejoin, broker rolling restart, broker
  produce after each transition, and broker fetch/read-after-transition checks
  for all acknowledged records. The same gate also starts a three-node combined
  controller+broker quorum and verifies AutoMQ KV put/get/delete, zone router,
  node registry including tag clearing, license, node-id allocator, group promote/demote,
  stream create/prepare/commit/open/close/trim/delete metadata plus stream tag clearing, stream-set object
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
  compaction. Startup Raft log replay now accepts the append-only replacement
  pattern produced when a follower truncates a conflicting suffix and appends a
  lower-offset entry later in the same file, while still rejecting real offset
  gaps. The gated failover harness can now run CI-provided
  `ZMQ_KRAFT_NETWORK_DOWN`/`ZMQ_KRAFT_NETWORK_UP` hooks after initial
  broker/controller convergence, or schedule multiple named phases with
  `ZMQ_KRAFT_NETWORK_MATRIX` and per-phase down/up/expect overrides. Each phase
  receives controller/broker PID and port context plus the active controller
  leader, verifies configured produce behavior during the partition, heals the
  cluster, reconverges controllers, verifies broker data continuity, and emits
  `ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command`
  before the leader-kill/restart sequence. Release jobs can require specific scheduled
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
  fetch checks for hook-owned marker payloads after both apply and restore, and
  `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` release coverage validation. Setting
  `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1` fills missing apply/restore hooks with
  the built-in Docker fixture and infers the fixture matrix from
  `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` when no explicit matrix is set, so named
  `load`, `scale-in`, and `scale-out` phases can run without external wrapper
  scripts. Fixture-backed `scale-out` phases pre-stop their target by default
  before starting it, with `ZMQ_E2E_LOAD_SCALE_<PHASE>_FIXTURE_PRESTOP=0` for
  jobs that prepare a stopped node externally; fixture enable, dry-run, and
  pre-stop flags now strictly parse as booleans before Docker is touched. E2E
  load/scale hooks now receive explicit apply/restore marker payloads and the
  parent harness only prints phase success markers after those hook-owned
  payloads are visible through Fetch. The
  Docker build now selects the Zig 0.16.0 archive from Docker `TARGETARCH` so
  arm64 and amd64 E2E builders do not silently download an incompatible compiler. The
  E2E Produce helper now emits valid Kafka v0 MessageSet records, so broker
  offset rewrites preserve payload values, and the local Docker combined-mode
  S3 WAL gate now discovers the active controller leader through DescribeQuorum,
  stops that leader instead of a fixed node, verifies replacement leader
  election with an advanced epoch, restarts the failed node, reconfirms quorum
  health, and passes all 37 checks including restart recovery and cross-node
  produce/fetch visibility.
  The local `test-kraft-failover` gate has been re-executed successfully after
  adding AutoMQ stream/node tag-clearing coverage plus live non-leader
  `NOT_CONTROLLER` probes for BrokerRegistration, AllocateProducerIds, dynamic
  voter changes, broker lifecycle calls, and ControllerRegistration. A rebuilt
  Docker E2E repeat-restart run also passed `53/53` with the fixture forcing
  node0 through both `scale-in` restore and `scale-out` pre-stop/start.
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
  after restart/replacement replay, checkpoint local-failover completion so
  fresh-dir S3 replacements do not resurrect stale remote reassignment owners,
  expose the owner through metadata APIs, and
  fence local Produce/Fetch when ownership is non-local. Broker-side
  controller-aware orchestration now computes from controller broker snapshots
  plus load samples, applies moves through durable reassignments, fails closed
  on stale plans without partial mutation, no-ops without active targets, can
  execute automatically from broker `tick()` when cached inputs are due, and
  has simulated convergence coverage after ownership changes. The gated KRaft
  failover harness now exercises live broker-process reassignment protocol
  convergence, old-owner write fencing, and target-broker topic/data
  convergence. Docker E2E now includes a default live reassignment check that
  creates a single-partition topic on the current controller leader, moves it
  to the next combined-mode broker, verifies
  ListPartitionReassignments and Metadata convergence, asserts the old owner is
  fenced for Produce, and verifies target-broker Produce/Fetch after movement.
  Controller-aware scale-out planning now has deterministic coverage that
  spreads hot partitions from an overloaded broker to multiple newly active
  broker targets. Docker E2E can require named cross-broker chaos phases and
  verifies cross-node produce/fetch recovery after each heal.
  S3 WAL live refresh now forces shared-WAL discovery before Produce/Fetch
  visibility decisions, skips already-covered stale WAL overlaps during live
  refresh without weakening strict replacement recovery, maps recovery failures
  to storage errors, and has default-suite coverage for immediate cross-broker
  read-after-write plus reassignment-target recovery. KRaft followers now leave
  client-requested topic auto-create to the controller leader for Metadata and
  Produce, preventing local split-brain ownership before quorum replay. The
  clean three-node Docker E2E gate was re-executed successfully with `53/53`
  passing, including cross-node fetch, dynamic source reassignment,
  target-owner Produce/Fetch after reassignment, required fixture-backed
  `load`, inferred `scale-in`, and fixture-prepared `scale-out` phases from
  `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` without an explicit matrix.
  Its classic consumer-group probe now parses the JoinGroup generation/member
  state, verifies a SyncGroup v0 assignment round trip, and heartbeats with the
  accepted member identity instead of accepting any heartbeat response bytes.
  Live Docker load/scale orchestration hooks, built-in fixture selection
  including required-phase inference and scale-out target preparation, and
  required-phase validation are now pinned by `test-e2e`; the build static
  audit now also pins the Docker E2E self-test assertion catalogue for run gates,
  chaos/load-scale phase validation, hook context, fixture payloads, and fixture override rejection.
  Broader CI execution across real scale-in/out/load
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
  tool lacks the required positive or negative security fixture, if selected
  tools or semantic suites are implicit/defaulted, if a secured/OAuth profile
  omits explicit security or security-negative semantics, if an OAuth-positive
  profile omits the `security` semantic, or if a secured/OAuth profile
  still uses `PLAINTEXT`; versioned, secured, security-negative, OAuth, and
  OAuth-negative sub-profile requirements also fail live preflight when they
  are outside `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`. Release
  evidence must include the harness-owned profile marker line shape
  `ok: client matrix profile <profile> passed for <tools> against <bootstrap> version=<version> source=command`
  for each profile named by `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`, with
  `<tools>` matching the profile-selected tools, `<bootstrap>` matching the
  selected bootstrap from the profile or global `ZMQ_CLIENT_MATRIX_BOOTSTRAP`,
  and `<version>` matching the exact label for required versioned profiles
  setting; explicit blank profile-specific client settings now fail release
  evidence instead of falling back to global client matrix values.
  profile-scoped tool probe markers must appear before the corresponding profile pass marker
  in the same profile block as the matching passed-for tools/bootstrap/version/source line.
  Required secured, security-negative, OAuth, and OAuth-negative profiles must
  also emit `ok: client security detail profile <profile> ... source=command` in that same profile
  block, with the profile-selected tool, protocol, mechanism, OAuth-positive,
  and compatible negative-vector booleans matching the release environment. The final
  client matrix summary must use
  `ok: client matrix passed for <profiles> profile(s) source=command` with `<profiles>`
  exactly matching `ZMQ_CLIENT_MATRIX_PROFILES`; required versioned, secured,
  security-negative, OAuth, and OAuth-negative sub-profiles must also stay
  within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`. The build static audit now pins the
  client matrix self-test error catalogue for required client profile/tool/semantic coverage,
  security and OAuth fixture validation, bootstrap provenance, and strict OAuth fixture parsing.
  The external client matrix command must include
  required profile, selected profile, required tool, required semantic, and
  required sub-profile assignments matching the manifest environment, so client
  coverage cannot be claimed from a different selector than the executed gate
  used. Release manifests must also pin
  client tool coverage across `kcat`, `kafka-cli`, `kafka-python`,
  `confluent-kafka`, `java-kafka`, and `go-kafka`, and semantic coverage across
  `basic`, `admin`, `groups`, `rebalance`, `transactions`, `security`, and
  `security-negative`. Captured evidence must include per-required client tool
  probe markers using `ok: <client> probes (<semantics>) source=command`,
  such as `ok: kcat probes`, `ok: kafka CLI probes`,
  `ok: kafka-python probes`, `ok: confluent-kafka probes`,
  `ok: java-kafka probes`, and `ok: go-kafka probes`, plus exact semantic tokens inside client probe marker
  parentheses for every semantic named by
  `ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS`; those tokens must appear on a
  recognized profile-selected required client-tool marker for a tool whose
  profile enabled that semantic. Deterministic
  OAuth validation now uses structured JWT claim parsing, accepts provider-style
  array `aud` claims including escaped JSON strings, rejects duplicate claims,
  future `nbf` tokens, and tokens without `exp` before principal extraction,
  and preflights raw real-client JWT fixtures so missing-exp positive tokens
  cannot satisfy OAuth coverage, non-standard JSON constants and duplicate object keys in raw JWT payloads
  are rejected, and missing-exp negative tokens can. Java and
  Kafka CLI OAuth JAAS fixtures are also preflighted so missing-exp positive
  JAAS configs cannot satisfy OAuth coverage, and future-valid bad JAAS configs
  cannot satisfy OAuth-negative coverage. kcat/librdkafka unsecured
  OAUTHBEARER configs are now preflighted as well: positive fixtures must emit
  the broker-supported `sub` principal claim with a positive token lifetime,
  while malformed, missing-principal, unsupported-principal-claim, and expired
  lifetime configs count as OAuth-negative vectors.
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
  for live provider outage/heal gates. Hook-driven chaos, KRaft, and Docker
E2E gates now reject placeholder required phase/scenario values, expectations,
hook commands, configured port/phase-index values, and live-S3 chaos
endpoint, bucket, credential, scheme, region, and path-style settings before
executing operator-provided hooks. Broker live-S3 chaos hooks are parsed before
any broker work starts, and successful live-S3 outage runs emit a provider
summary line that release evidence must match to `ZMQ_CHAOS_S3_*` or documented
`ZMQ_S3_*` fallback settings, including
`ZMQ_CHAOS_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`. Explicit blank
`ZMQ_CHAOS_S3_*` live-provider settings, including
`ZMQ_CHAOS_S3_TLS_CA_FILE`, fail closed instead of falling back to `ZMQ_S3_*`. When
`live-s3-outage` is required, the broker chaos command must include
non-sensitive live-S3 outage provider assignments for the selected endpoint,
port, bucket, scheme, region, and path-style values, so command provenance
cannot drift from the captured provider marker.
The S3 provider matrix now requires that same selected-provider summary in the
underlying live-outage chaos output before it emits the provider outage release
marker; it also emits a same-block
`ok: S3 provider outage detail profile ... source=command` marker that release
evidence must match to the selected provider endpoint, bucket, and recovery result. It
rejects process-crash/replacement output that lacks the detailed
summary or whose bucket differs from the selected provider bucket before it
emits the provider process-crash release marker; it also emits a same-block
`ok: S3 provider process-crash detail profile ... source=command` marker that
release evidence must match to the selected provider bucket and replacement offsets.
The Docker E2E gate now also parses configured chaos and load/scale hook
commands during phase-selection preflight, so placeholder, blank, or malformed
hook text fails before Docker work begins. Explicit blank global and
phase-specific chaos, KRaft, and E2E hook variables now fail closed instead of
falling back to another hook source or the E2E fixture. Broker chaos
`all` scenario selection treats present global network or live-S3 hook
variables as activating those scenarios even when the hook text is blank, so
the blank value is rejected by hook preflight instead of skipping coverage. The
build static audit now pins the broker chaos self-test error catalogue for
scenario selection, hook preflight, required coverage lists, live-S3 provider
config, and record-batch fixtures. It also pins the KRaft failover self-test error catalogue
for run gates, network partitions, required phases, hook context, protocol fixture parsers,
and record-batch fixture invariants.
Built-in E2E load/scale fixture actions, node selectors, and load-record counts
now also fail closed on placeholders, unknown actions, malformed integers, zero,
negative counts, or blank phase-specific overrides before fixture hooks can run.
Fixture-backed `action=load` phase markers must report `load_records=<count>`
matching the effective fixture load-record setting, and the harness verifies
those load payloads are readable before the restore hook runs.
The build static audit also pins the release-evidence E2E load/scale fixture action catalogue:
E2E_LOAD_SCALE_FIXTURE_ACTIONS entries must stay present in the release
criteria, parity notes, and production-readiness pins so built-in Docker E2E load/scale fixture actions
cannot drift from verifier preflight. The current fixture actions are
`scale-in`, `scale-out`, `load`, `probe`, and `noop`. Any fixture action change
must update the build static audit E2E-fixture-action catalogue.
`test-s3-provider-matrix` can require
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
  `ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES`. The S3 provider matrix
  command must include `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`,
  `ZMQ_S3_PROVIDER_PROFILES`, and each
  `ZMQ_S3_PROVIDER_REQUIRED_{OUTAGE,PROCESS_CRASH,LIST_PAGINATION,MULTIPART_EDGE,MULTIPART_FAULT}_PROFILES`
  selector matching the manifest environment. The build static audit now pins the
  S3 provider matrix self-test error catalogue for provider profile fallback validation,
  outage, process-crash, and multipart-fault evidence validation, and required sub-profile
  coverage checks.
  `test-chaos` can require selected scenario coverage with
  `ZMQ_CHAOS_REQUIRED_SCENARIOS`, run scheduled named network-partition phases
  with `ZMQ_CHAOS_NETWORK_MATRIX`, override per-phase
  `ZMQ_CHAOS_NETWORK_<PHASE>_{DOWN,UP,EXPECT}` hooks, and fail release jobs
  when required network phases are missing through
  `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES`. The broker chaos command must include
  coverage selector assignments for `ZMQ_CHAOS_REQUIRED_SCENARIOS`,
  `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES`, and `ZMQ_CHAOS_NETWORK_MATRIX` matching
  the manifest environment.
  The local safe-scenario gate has been executed with required
  SIGKILL/restart, slow-partial-client, clock-skewed-records, and S3-outage
  coverage enabled.
  `test-kraft-failover` can require controller/broker network-partition hooks
  with `ZMQ_KRAFT_NETWORK_DOWN` and `ZMQ_KRAFT_NETWORK_UP`, or scheduled named
  controller/broker partition phases with `ZMQ_KRAFT_NETWORK_MATRIX` and
  `ZMQ_KRAFT_NETWORK_<PHASE>_{DOWN,UP,EXPECT}` overrides, and can fail release
  jobs when required failover phases are omitted through
  `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES`. The KRaft failover command must include
  coverage selector assignments for `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` and
  `ZMQ_KRAFT_NETWORK_MATRIX` matching the manifest environment. `test-e2e` can now require named
  Docker cross-broker chaos phases with `ZMQ_E2E_REQUIRED_CHAOS_PHASES` and
  per-phase `ZMQ_E2E_CHAOS_<PHASE>_{DOWN,UP,EXPECT}` hooks plus named Docker
  live load/scale phases with `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` and
  per-phase `ZMQ_E2E_LOAD_SCALE_<PHASE>_{APPLY,RESTORE}` hooks, or with
  `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1` for the built-in Docker fixture, which
  can infer phases from the required phase list and prepare stopped targets for
  `scale-out` phases. The Docker E2E command must include
  `ZMQ_E2E_REQUIRED_CHAOS_PHASES`, `ZMQ_E2E_CHAOS_MATRIX`, and
  `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` matching the manifest environment; it
  must also include `ZMQ_E2E_LOAD_SCALE_MATRIX` when an explicit load/scale
  matrix is recorded, and `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE` whenever fixture
  mode is enabled. Those hooks now receive the active topic plus broker,
  controller, metrics, container, and MinIO context, with deterministic
  default-suite self-test coverage. Release evidence now treats explicit blank
  global and phase/profile hook variables plus phase-specific chaos/KRaft
  expectation variables, along with explicit blank `ZMQ_CHAOS_S3_*`
  live-provider settings and profile-specific S3 provider settings/enable
  variables, as selected and failing instead of falling back to alternate hooks,
  global expectations, `ZMQ_S3_*`, or the load/scale fixture.
  Release evidence for broker chaos and scheduled matrix coverage must include
  per-required coverage markers:
  `ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=<positive> source=command`,
  `ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command`,
  `ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command`,
  `ok: chaos s3-outage ... startup_fail_closed=true source=command` or
  `ok: chaos s3-outage ... rejected=true error_code=<nonzero> base_offset_negative=true serving=true source=command`,
  `ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command`,
  `ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command`
  for required live-S3 outage scenarios,
  `ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`,
  `ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command`,
  `ok: E2E chaos phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`, and
  `ok: E2E load/scale phase ... applied=true restored=true marker_payloads=hook-owned apply_source=<hook|fixture> restore_source=<hook|fixture> source=command`, with `load_records=<count>` on fixture `action=load` markers, plus final Docker E2E summaries
  `ok: E2E chaos passed for <phases> phase(s) source=command` and
  `ok: E2E load/scale passed for <phases> phase(s) source=command`. Broker chaos scenario
  detail markers must appear before the broker chaos harness summary line so
  detached scenario output cannot satisfy required scenario coverage, and
  scenario detail markers must be unique per required scenario. The chaos network-partition scenario summary must appear exactly once as its own stripped line. The broker chaos harness summary must appear exactly once with `source=command` as its own stripped line. The build
  verifier now requires fixture-backed E2E load/scale markers to include the
  effective fixture action, fixture `action=load` markers must report the
  effective load-record count, and hook-owned markers must not report a fixture action.
  The build
  static audit also pins the release-evidence chaos scenario catalogue:
  CHAOS_SCENARIO_ALIASES, REQUIRED_CHAOS_SCENARIOS, and CHAOS_SCENARIO_MARKERS
  entries must stay present in the release criteria, parity notes, and
  production-readiness pins so broker chaos evidence cannot drift from verifier
  coverage. The current chaos scenario aliases are `sigkill` to
  `sigkill-restart`, `partial-client` to `slow-partial-client`, `clock-skew` to
  `clock-skewed-records`, `s3` to `s3-outage`, `network` to
  `network-partition`, `live-s3` to `live-s3-outage`, and `s3-live` to
  `live-s3-outage`. The canonical broker chaos scenarios are
  `sigkill-restart`, `slow-partial-client`, `clock-skewed-records`, `s3-outage`,
  and `network-partition`. The current chaos scenario markers are
  `sigkill-restart` to
  `ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=<positive> source=command`,
  `slow-partial-client` to
  `ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command`,
  `clock-skewed-records` to
  `ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command`,
  `s3-outage` to `ok: chaos s3-outage`, `network-partition` to
  `ok: chaos network-partition source=command`, and `live-s3-outage` to
  `ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command`.
  Any chaos scenario change must update the build static audit chaos-scenario catalogue.
  KRaft evidence must also
  show a real network matrix with `network_partition=[<phases>]` exactly
  matching `ZMQ_KRAFT_NETWORK_MATRIX`, plus controller, coordinator, AutoMQ
  metadata, and reassignment proof fields on the
  `ok: KRaft controller failover harness passed ... source=command` line, not a placeholder,
  empty partition result, or detached marker line.
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
  requests/MiB gate with CI-tunable threshold environment variables:
  `ZMQ_BENCH_S3_WAL_MAX_REQUESTS_PER_MIB`,
  `ZMQ_BENCH_S3_WAL_MAX_REBUILD_MS`, and
  `ZMQ_BENCH_LIVE_S3_MAX_REQUESTS_PER_MIB`. Local and live-S3 benchmark
  threshold variables now fail closed unless they parse as finite non-negative
  floats, and live-S3 iteration/payload-size variables must parse as positive
  integers and must appear as matching live-S3 benchmark command assignments
  when release evidence records `ZMQ_BENCH_LIVE_S3_ITERATIONS` or
  `ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES`. The build static audit also pins the
  release-evidence numeric environment catalogue:
  BENCHMARK_THRESHOLD_ENV_VARS and POSITIVE_INTEGER_ENV_VARS entries must stay
  present in the release criteria, parity notes, and production-readiness pins,
  so finite non-negative floats and positive integers cannot drift from the
  verifier catalogue. Any numeric environment change must update the
  build static audit numeric-env catalogue. The local benchmark gate has been re-executed successfully. The
  live-S3 benchmark now strictly parses
  `ZMQ_RUN_BENCH_LIVE_S3` and `ZMQ_S3_SKIP_ENSURE_BUCKET` as booleans, so
  placeholder or arbitrary values cannot silently skip the provider benchmark
  or bucket preflight.
  Gated `bench-compare` now runs the existing ZMQ/Kafka/AutoMQ comparison
  harness only when `ZMQ_RUN_BENCH_COMPARE=1` is set, requires ZMQ plus at
  least one Kafka/AutoMQ baseline result for release-gate execution, can require
  exact selected/result targets with lowercase
  `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`, rejects blank or duplicate required-target entries,
  fails closed when any selected target does not produce results, and
  enforces configurable `ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO`,
  `ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO`,
  `ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO`, and
  `ZMQ_BENCH_COMPARE_MAX_ERROR_RATE` thresholds, and rejects blank,
  placeholder, negative, and non-finite threshold values such as `nan` or `inf`
  before comparisons run. The same release gate can
  require a previous `benchmarks/results.json` artifact with
  `ZMQ_BENCH_COMPARE_REQUIRE_TREND=1` and
  `ZMQ_BENCH_COMPARE_TREND_BASELINE`, then enforce
  `ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO`,
  `ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO`, and
  `ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO` against current ZMQ results;
  the trend baseline must be a concrete non-placeholder path, and relative
  baseline paths resolve from the project root rather than the caller's working
  directory.
  The trend baseline must not resolve to the current `benchmarks/results.json`
  output path, and the
  trend-required flag must parse as a real boolean so placeholder or arbitrary
  values cannot silently disable the trend gate.
  Trend baseline metrics must be strict structured numeric finite benchmark
  data: non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity`
  and duplicate JSON object keys are rejected while parsing archived baselines,
  and non-standard constants are rejected while writing current
  `benchmarks/results.json`; current results are serialized before replacing the existing artifact so invalid payloads cannot truncate prior benchmark
  evidence. Enforced comparative benchmark gates only replace
  `benchmarks/results.json` after the gate passes, so failed release-gate runs
  cannot clobber the prior artifact that future trend comparisons may depend
  on. Current result artifacts now also record schema-version,
  selected/required target, target-label, iteration/warmup, threshold, gate,
  and trend-baseline metadata, and selected/required target metadata must list
  concrete known unique targets, `targets_with_results` must match result
  targets, each result target must be included in selected target metadata, and
  required target metadata must be a subset of selected target metadata, so
  archived trend inputs can be traced back to the selected comparative profile.
  Artifact target-label, iteration/warmup, threshold, gate, and trend-baseline
  metadata must match the current benchmark profile shape with finite
  non-negative thresholds, real boolean gate flags, and concrete
  non-placeholder trend baseline paths whenever trend metadata requires one.
  Result artifact maps must be objects with
  only known target keys and per-target object results. Per-target artifact
  result maps must contain the current benchmark row keys and no unknown
  benchmark result keys. Archived trend baselines must include schema-version 1
  artifact metadata whose targets_with_results includes zmq. Malformed, missing,
  non-numeric, non-finite, negative, or zero `throughput`, `p50`, and `p99`
  values fail closed before ratio checks can pass. Current comparative result rows now apply the same fail-closed checks for malformed target/result
  objects, non-numeric or non-finite throughput/latency metrics, non-integral
  error/request/success counts, negative counts, and zero throughput/latency
  values, and the saved artifact writer applies the same row validation before
  replacing `benchmarks/results.json`.
  Release evidence must include comparative target labels for every required
  target in `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`: `ZMQ (Zig)`, `Apache Kafka`,
  and `AutoMQ (Java)` when those targets are selected. The comparative benchmark command must include
  a `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS` assignment matching the manifest environment
  so target provenance cannot drift from the captured output. When the manifest
  records `ZMQ_BENCH_COMPARE_ENFORCE_GATES`, the comparative benchmark command
  must include the same gate-control assignment. When the manifest
  records custom comparative benchmark thresholds, the comparative benchmark
  command must also include matching
  `ZMQ_BENCH_COMPARE_{MIN_THROUGHPUT_RATIO,MAX_P50_LATENCY_RATIO,MAX_P99_LATENCY_RATIO,MAX_ERROR_RATE,MIN_TREND_THROUGHPUT_RATIO,MAX_TREND_P50_LATENCY_RATIO,MAX_TREND_P99_LATENCY_RATIO}`
  assignments.
  Every comparative
  table row must include positive finite target measurements for each required
  target inside the comparison table before the `COMPARATIVE BENCHMARK GATE`
  section. The comparison line, selected target labels, table header, and each
  benchmark metric row must appear exactly once before the gate so duplicated
  stale comparison output cannot satisfy release evidence. When
  `ZMQ_BENCH_COMPARE_REQUIRE_TREND=1`, the comparative output must also include
  detailed `trend thresholds:` inside the bounded `COMPARATIVE BENCHMARK GATE`
  section, a `trend baseline:` line matching
  `ZMQ_BENCH_COMPARE_TREND_BASELINE`, and threshold lines must match the selected
  gate environment and must not repeat so
  archived-baseline enforcement is visible in captured evidence.
  The default suite self-tests target parsing, required-target validation,
  duplicate required-target rejection, ratio formatting, threshold parsing,
  missing-baseline detection, project-rooted relative trend-baseline loading,
  benchmark artifact metadata, throughput/latency
  regression detection, trend-artifact validation, malformed trend-baseline
  metric rejection, malformed comparison-result rejection, trend regression
  detection, and error-rate enforcement. The build static audit now pins the
  comparative benchmark self-test assertion catalogue, including target parsing,
  table-header target labels, gate/regression, strict trend JSON, threshold, and
  artifact-metadata failure cases.
  Remaining work: CI execution of broader live comparative performance profiles
  with archived trend artifacts.
- Add release criteria: no known data-loss bug, no advertised stub API, passing
  client matrix, passing MinIO/S3 matrix, and documented unsupported features.
  Status: `docs/RELEASE_CRITERIA.md` now pins required protocol, durability,
  stateless, multi-node, security, observability, performance, chaos, and
  comparative benchmark gates, lists the release commands, and documents
  currently unsupported/partial surfaces. `test-release-evidence` validates a
  release evidence manifest with the exact commit, successful command outputs
  for each required gate, including protocol, observability, and build static
  audits, using the pinned Zig 0.16 executable, required
  environment coverage variables, structured accounting for each known
  unsupported/partial surface, concrete non-placeholder coverage values
  that reject angle-bracket placeholders such as `<host>` and `<port>`,
  including benchmark baseline target coverage, comma-separated coverage
  variables that parse to at least one value, rejection of blank comma-separated entries and duplicate comma-separated entries,
  explicit blank selector rejection, strict JSON manifest parsing that
  rejects non-standard JSON constants such as
  `NaN`, `Infinity`, or
  `-Infinity` before schema validation plus duplicate object keys before
  release accounting, matching fail-closed parser
  checks in the client/S3/chaos/KRaft/E2E/benchmark harnesses, current clean
  checkout validation for the manifest commit that fails closed when git commit
  or worktree cleanliness cannot be determined, S3/E2E coverage consistency checks,
  rejection of placeholder `ZMQ_RELEASE_EVIDENCE` manifest paths, captured gate
  skip output, explicit false
  `known_data_loss_bug`, `advertised_stub_api`, and
  `untriaged_durability_failure` blocking bug flags, plus explicit
  `automq_complete=false` while unsupported/partial surfaces remain, all of
  which must be JSON booleans rather than strings. It also
  requires per-required coverage markers for live matrix/profile gates,
  including
  `ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=<positive> source=command`,
  `ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command`,
  `ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command`,
  `ok: chaos s3-outage ... startup_fail_closed=true source=command` or
  `ok: chaos s3-outage ... rejected=true error_code=<nonzero> base_offset_negative=true serving=true source=command`,
  `ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command`,
  `ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command`
  for required live-S3 outage scenarios,
  `ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`,
  `ok: client matrix profile ... source=command`,
  `ok: S3 provider live-suite profile ... command_started=true completed=true source=command`,
  `ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command`,
  `ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command`,
  `ok: S3 provider list-pagination profile ... required=true completed=true source=command`,
  `ok: S3 provider multipart-edge profile ... required=true completed=true source=command`,
  `ok: S3 provider multipart-fault profile ... command_started=true completed=true injected=true recovered=true source=command`,
  `ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command`,
  `ok: E2E chaos phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`, and
  `ok: E2E load/scale phase ... applied=true restored=true marker_payloads=hook-owned apply_source=<hook|fixture> restore_source=<hook|fixture> source=command`, with `load_records=<count>` on fixture `action=load` markers, plus comparative benchmark `COMPARISON:`
  output with `Benchmark` rows for `ApiVersions`, `Produce (reuse)`,
  `Produce (fresh)`, `Fetch`, and `Metadata`, the `thresholds:` line,
  comparative target labels, `trend thresholds:` for required trend gates, and
  `Results saved to benchmarks/results.json` after the gate result, followed by
  an `ok: comparative benchmark profile` marker. That profile marker must
  include `selected=`, `required=`, `results_targets=`,
  `results=benchmarks/results.json`, `gates_enforced=true`,
  `trend_required=true`, `trend_baseline=`, `iterations=`, `warmup=`, and
  `source=command`, with targets and trend baseline matching the release
  environment and profile values matching the benchmark runner. The profile marker is a closed key=value schema:
  every required field must appear exactly once,
  fields must not be blank, and unknown fields are rejected.
  The saved-results artifact line must appear exactly once after the bounded
  `COMPARATIVE BENCHMARK GATE` section's `result: pass` line, not as a detached
  line elsewhere in the captured output.
  The comparison table header must include each required target's result column
  before ratio columns: `ZMQ` for `zmq`, `Kafka` for `kafka`, and `AutoMQ` for
  `automq`. Required target columns must stay in the same relative order as the
  comparative target catalogue. Table target columns are limited to the known target headers.
  Required ZMQ-to-baseline ratio columns (`ZMQ/Kafka` and `ZMQ/AutoMQ`) must
  appear after target columns and follow the same comparative target catalogue order;
  ratio columns are limited to known ZMQ-to-baseline pairs.
  The `COMPARISON:` line target labels must also follow the comparative target catalogue order.
  Each comparative table metric row must include exactly one positive finite
  target measurement cell per table target column, followed by exactly one
  positive finite ratio cell per table ratio column and no extra cells.
  The build static audit also pins the release-evidence comparative benchmark catalogue:
  COMPARATIVE_TARGET_LABELS, COMPARATIVE_TABLE_TARGET_HEADERS,
  COMPARATIVE_TABLE_METRICS, COMPARATIVE_MEASUREMENT_RE, and
  COMPARATIVE_RATIO_RE entries must keep the comparative target labels, table
  target headers, table metric keys, ratio parser, and comparative benchmark profile marker aligned across the release
  criteria, parity notes, production-readiness pins, and verifier parser.
  COMPARATIVE_PROFILE_MARKER_KEYS must remain part of that catalogue so the
  command-owned profile marker stays aligned with the verifier parser.
  The current comparative targets are `zmq` as `ZMQ (Zig)` with table header
  `ZMQ`, `kafka` as `Apache Kafka` with table header `Kafka`, and `automq` as
  `AutoMQ (Java)` with table header `AutoMQ`. The current comparative table
  metrics are `tput`, `p50`, and `p99`. Any comparative
  benchmark parser change must update the build static audit comparative-benchmark catalogue.
  The build static audit also requires `benchmark_compare.py` `TARGET_SHORT_LABELS`
  to match release-evidence `COMPARATIVE_TABLE_TARGET_HEADERS`, and
  `benchmark_compare.py` `ALL_TARGETS`/`TARGET_LABELS`
  to match release-evidence `COMPARATIVE_TARGET_LABELS`.
  The build static audit also pins the release-evidence comparative threshold default catalogue:
  DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS and COMPARATIVE_BENCHMARK_THRESHOLD_ENV
  must keep default comparative threshold keys and values aligned with the
  environment override mapping. The current comparative threshold defaults are
  `max_error_rate=0.0`, `max_p50_latency_ratio=20.0`,
  `max_p99_latency_ratio=20.0`, `max_trend_p50_latency_ratio=1.25`,
  `max_trend_p99_latency_ratio=1.25`, `min_throughput_ratio=0.05`, and
  `min_trend_throughput_ratio=0.9`. Any comparative threshold default change
  must update the build static audit comparative-threshold-default catalogue.
  Broker chaos scenario detail markers must appear before the broker chaos
  harness summary line and must be unique per required scenario.
  MinIO `8/8 tests passed` evidence must appear as its own line or on a
  successful Zig `Build Summary:` line. The MinIO `8/8 tests passed` marker must appear exactly once. Captured Zig output
  must include exactly one successful `Build Summary:` line and must not contain any unsuccessful `Build Summary:` line.
  KRaft detail
  evidence such as `network_partition=[<phases>]`,
  `old_leader_rejoined=true`, `old_leader_fresh_rejoin=true`,
  `automq_old_leader_fresh_rejoin=true`, `automq_stream_id=`,
  `automq_deleted_stream_id=`, `automq_stream_set_object_id=`,
  `automq_node_id=`, `automq_zone_router_epoch=`,
  `old_leader=`, `new_leader=`, `restarted_controller=`, `epoch=`,
  `automq_old_leader=`, `automq_new_leader=`,
  `allocate_producer_ids_checked=true`,
  `allocate_producer_ids_follower_rejection_checked=true`,
  `describe_quorum_v2_checked=true`, `fetch_snapshot_v1_checked=true`,
  `all_controller_fetch_snapshot_v1_checked=true`,
  `controller_api_versions_checked=true`,
  `all_controller_api_versions_checked=true`,
  `controller_unsupported_checked=true`,
  `all_controller_unsupported_checked=true`,
  `controller_unsupported_cases=[<api_key>:<version>,...]`,
  `dynamic_raft_voter_negative_checked=true`,
  `dynamic_raft_voter_follower_rejection_checked=true`,
  `all_controller_describe_quorum_v2_checked=true`,
  `broker_lifecycle_negative_checked=true`,
  `broker_lifecycle_follower_rejection_checked=true`,
  `controller_registration_negative_checked=true`,
  `controller_registration_follower_rejection_checked=true`,
  `broker_registration_follower_rejection_checked=true`,
  `broker_non_broker_api_rejection_checked=true`,
  `broker_non_broker_api_rejection_cases=[<api_key>:<version>,...]`,
  `committed_offset=`, `transactions_checked=5`,
  `transaction_introspection_checked=true`, `transaction_abort_checked=true`,
  `txn_offset_commit_checked=true`, `offset_fetch_v8_grouped_checked=true`,
  `log_position_apis_checked=true`, `idempotent_producer_fencing=true`,
  `delete_records_checked=true`, `delete_topics_checked=true`,
  `create_topics_checked=true`, `create_partitions_checked=true`,
  `client_quotas_checked=true`, `scram_credentials_checked=true`,
  `client_telemetry_checked=true`, `delegation_tokens_checked=true`,
  `finalized_features_checked=true`, `acl_admin_checked=true`,
  `config_admin_checked=true`, `describe_topic_partitions_checked=true`,
  `describe_configs_checked=true`, `describe_log_dirs_checked=true`,
  `alter_replica_log_dirs_checked=true`,
  `assign_replicas_to_dirs_checked=true`, `elect_leaders_checked=true`,
  `describe_cluster_checked=true`, `describe_producers_checked=true`,
  `delete_groups_checked=true`, `classic_group_heartbeats=true`,
  `group_describe_checked=true`, `consumer_group_describe_checked=true`,
  `list_groups_checked=true`, `find_coordinator_checked=true`,
  `share_group_heartbeat_checked=true`, `share_group_describe_checked=true`,
  `consumer_group_heartbeat_checked=true`,
  `share_acknowledge_checked=true`, `kip848_describe_checked=true`,
  `kip848_rejoin_checked=true`, `kip848_rack_checked=true`,
  `kip848_owned_assignment_checked=true`,
  `kip848_subscription_update_checked=true`,
  `kip848_negative_join_checked=true`, `kip848_static_rejoin_checked=true`,
  `offset_commit_v9_member_checked=true`, `offset_fetch_v9_member_checked=true`,
  `share_fetch_session_checked=true`, `share_state_apis_checked=true`, plus
  reassignment evidence fields
  `reassignment_topic=<topic>`, `reassignment_target=<broker>`,
  `reassignment_target_offset=<offset>`,
  `reassignment_old_owner_rejected=true`, and
  `reassignment_target_fetch_verified=true`, must appear on the
  `ok: KRaft controller failover harness passed ... source=command` line, with the network
  phases exactly matching `ZMQ_KRAFT_NETWORK_MATRIX`; the AutoMQ metadata ids
  and zone-router epoch on that same line must parse as non-placeholder non-negative integers,
  `controller_unsupported_cases` must include generated-only ZooKeeper-era
  min/max probes `4:0`, `4:7`, `5:0`, `5:4`, `6:0`, `6:8`, `7:0`, `7:3`
  plus telemetry keys `71:0` and `72:0`,
  `broker_non_broker_api_rejection_cases` must include controller-only/non-broker
  broker-port probes `56:3`, `58:0`, `59:1`, `62:4`, `63:1`, `64:0`,
  `67:0`, `70:0`, `80:0`, `81:0`, and `82:0`, and `transactions_checked` must parse as exactly `5`.
  The KRaft failover summary must appear exactly once with `source=command` as
  its own stripped line.
  Each selected KRaft network phase must also emit
  `ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command`
  matching its selected phase expectation before the KRaft failover summary
  line, so detached or stale phase output cannot satisfy the network matrix.
  The verifier requires this evidence before the KRaft failover summary line.
  S3 process-crash evidence must include the detailed
  `ok: S3 process crash/replacement harness passed (bucket=<bucket>,
  topic=<topic>, group=<group>, killed_broker=true, fresh_data_dir=true,
  first_offset=0, committed_offset=1, replacement_offset=<offset>,
  recovered_payloads=2) source=command` marker, with a replacement offset greater than the
  first acknowledged offset and no duplicate or unknown summary key fields. The
  S3 process-crash summary marker must appear exactly once with `source=command`
  as its own stripped line.
  Deterministic release output markers now use line-aware output marker matching
  so `ok: ...`, `thresholds:`, `trend thresholds:`, and `result: pass` markers
  cannot be satisfied by arbitrary substrings such as `not ok:` or
  `previous result: pass`.
  Captured skip markers are also line-aware, so `skip: ...` markers must appear
  as stripped output lines or line prefixes and skipped MinIO summaries must
  come from Zig `Build Summary:` skip counts.
  Docker E2E section markers are line-aware too: `3-Node E2E Test Suite` must
  be on the suite title line, while `[Test m] Cross-broker chaos phases`,
  `[Test n] Live load/scale phases`, and `Results:` must appear as stripped
  output lines or line prefixes. Docker E2E output line markers must appear exactly once.
  The final Docker E2E result line must report `Results: <passed>/<total> passed, 0 failed`
  with `<passed>` equal to `<total>` after the required E2E phase summaries, so earlier detached results output
  cannot satisfy final completion evidence. The Docker E2E final results line must appear exactly once.
  Docker E2E phase details must appear before their matching E2E chaos summary
  line or E2E load/scale summary line, so detached phase output cannot satisfy
  phase coverage. The verifier requires details before the E2E chaos summary line.
  It also requires details before the E2E load/scale summary line.
  Docker E2E phase summaries must exactly match
  `ZMQ_E2E_CHAOS_MATRIX` and `ZMQ_E2E_LOAD_SCALE_MATRIX`, or the
  fixture-inferred `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` list when
  `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1`.
  Docker E2E phase summaries must appear exactly once with `source=command` as
  their own stripped lines.
  Local and live-S3 benchmark markers are also line-aware, including
  `=== Benchmarks complete ===`, `ok: local benchmark gate source=command`,
  `ok: live-S3 benchmark gate source=command`, `S3 WAL request volume`,
  `PartitionStore memory`, `Live S3 provider`, `Live S3 put`, `Live S3 get`,
  and `Live S3 request volume`, and detailed local/live-S3 benchmark markers
  must appear before the `=== Benchmarks complete ===` marker. The verifier
  reports detached benchmark details as missing evidence before the benchmark completion marker,
  and each detailed local/live-S3 benchmark marker must appear exactly once before
  completion so duplicate request-volume, memory, provider, put, or get lines
  cannot hide stale measurements behind a later passing line.
  The local benchmark summary must appear exactly once as its own stripped line,
  and the live-S3 benchmark summary must appear exactly once as its own stripped line.
  Successful local benchmark output now emits
  `S3 WAL request volume puts=<puts> lists=<lists> requests/MiB=<value>` and
  `PartitionStore memory <rate>/s retained=<retained> KiB peak=<peak> KiB
  max_current=<max_current> KiB`, and successful live-S3 benchmark output emits
  `Live S3 provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>`
  matching command/env-selected `ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`
  settings. The live-S3 benchmark command must include
  `ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` assignments, and the
  manifest environment must record the same values, with `SCHEME` parsing as `http` or `https`, `PATH_STYLE` parsing as `true` or `false`,
  and optional `ZMQ_BENCH_LIVE_S3_{ITERATIONS,PAYLOAD_BYTES}` assignments matching
  the manifest whenever release evidence records those sizing values,
  `Live S3 put <MiB/s> MiB/s p99=<ms> ms objects=<objects>`,
  `Live S3 get <MiB/s> MiB/s p99=<ms> ms requests/MiB=<value>`, and
  `Live S3 request volume puts=<puts> gets=<gets> requests/MiB=<value>` so
  request-volume, throughput, p99, and memory evidence carries concrete measurements.
  Comparative benchmark table markers are also line-aware and section-scoped:
  target labels must appear on the `COMPARISON:` line before the gate, the
  `Benchmark` marker must be a table header containing `Metric`, and each
  benchmark row label must appear as the throughput (`tput`) row for that
  benchmark rather than an arbitrary substring or detached post-gate line.
  Comparative benchmark release evidence now also requires concrete `tput`, `p50`, and `p99`
  metric rows for each benchmark before the gate and requires
  `result: pass` to be the gate section result inside the bounded
  `COMPARATIVE BENCHMARK GATE` section rather than a detached line elsewhere.
  Client semantic evidence must come from recognized profile-selected
  required client-tool probe markers rather than arbitrary `ok: ... probes`
  lines or tools whose profile did not enable the semantic. Client profile
  evidence must also use the `passed for <tools> against <bootstrap> version=<version> source=command` profile
  marker line shape and list the profile-selected tools, so bare
  `ok: client matrix profile ...` prefixes cannot satisfy profile evidence;
  the marker bootstrap must match the selected profile or global
  `ZMQ_CLIENT_MATRIX_BOOTSTRAP` setting, and
  the selected tool probe markers must appear before that profile pass marker
  in the same profile block as the matching passed-for tools/bootstrap/version/source line,
  so another profile's probe line cannot satisfy profile-scoped evidence. Client
  profile, probe, and security detail markers must also appear before the final client matrix summary,
  so post-summary profile blocks cannot satisfy profile evidence. The
  final `ok: client matrix passed for <profiles> profile(s) source=command` summary must list
  exactly the selected `ZMQ_CLIENT_MATRIX_PROFILES` values, and each selected
  client profile pass marker is now unique before that final summary so
  contradictory bootstrap/tool evidence cannot hide behind another passing
  line. The client matrix summary must appear exactly once with `source=command`
  as its own stripped line. The
  `ok: chaos network-partition source=command`
  scenario summary must appear as its own stripped line exactly once; per-phase
  `ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`
  markers cannot satisfy it, and required network phase details must appear
  before the chaos network-partition scenario marker so detached stale phase
  output cannot count. The
  `ok: chaos harness passed for <scenarios> source=command` summary must match
  every `ZMQ_CHAOS_REQUIRED_SCENARIOS` entry exactly, without extra unrequired
  scenario claims.
  KRaft failover and S3 process-crash summary key/value fields must be unique,
  and both summaries are closed to unknown fields, so repeated keys or unchecked
  extra values cannot hide stale or contradictory evidence behind a later
  passing value.
  Required broker-chaos, KRaft, and Docker E2E phase detail markers now also
  fail closed when the same phase is repeated before its owning summary line,
  preventing contradictory phase output from being overwritten by a later
  passing marker.
  Required S3 provider live-suite, sub-profile, and detail markers are now
  unique within each provider block, so repeated provider evidence cannot hide
  stale or contradictory endpoint, bucket, outage, process-crash, or
  multipart-fault output behind a later passing marker.
  Selected S3 provider-settings profile markers are also unique before the
  final provider matrix summary, so contradictory endpoint/bucket evidence
  cannot hide behind another passing line.
  Final comma-separated output summaries for client profiles, S3 provider
  profiles, KRaft network phases, Docker E2E phases, and broker chaos scenarios
  now reject blank, duplicate, or placeholder entries before matrix matching.
  Client profile `passed for <tools>` lists and client tool probe semantic lists
  now use the same strict output CSV rules, rejecting blank, duplicate, or
  placeholder entries before they can satisfy tool or semantic coverage.
  Profile-scoped client tool probe markers now require `source=command`, and those
  markers plus required client security detail markers are unique within the matching client profile block, preventing
  repeated client evidence from hiding stale or contradictory semantic or
  security-negative results behind a later passing marker.
  Release evidence command
  matching now uses token-aware command validation so required gate environment
  assignments must be on the same shell command segment as the pinned Zig
  invocation, command strings must be single-line and unquoted so CR/LF line breaks
  cannot detach environment assignments through newline command separators and
  shell quote characters cannot make quoted assignment words masquerade as active gate environment,
  and backslash escapes are rejected so escaped assignment words cannot satisfy required gate environment.
  Required command environment assignments must also be recorded in the manifest
  environment so the verifier cannot accept untracked shell provenance.
  Angle-bracket placeholders such as `<host>`, `<port>`, and `<bucket>` are
  rejected anywhere concrete release command, environment, or manifest values are required.
  Repeated environment assignments are rejected within each command segment so
  release evidence cannot contain contradictory provenance for the same gate.
  Duplicate successful command entries for the same required gate are rejected
  so release evidence cannot retain stale or contradictory reruns beside the
  gate output chosen by the verifier.
  Multi-segment release gate command chains must use
  success-dependent `&&` separators only, only documented compose config
  commands may use multi-segment release gate chains, compose config commands
  must precede their echo markers through `&&`, and quoted/echoed command text cannot satisfy
  a gate. Aside from the required compose echo markers, release gate commands
  must be direct invocations: pipes, backgrounding, redirection, subshell
  grouping, and command substitution are rejected, including Bash `&>`/`&>>` combined redirects,
  so captured evidence cannot
  be detached from the required gate process. Non-compose command strings also
  cannot embed release output marker text
  such as `ok:`, `Build Summary:`, `thresholds:`, `trend thresholds:`, or
  `result: pass`; those markers must come from captured command output.
	  Unsupported/partial surface
	  entries must be structured objects with non-empty `surface`, `status`, and
	  `evidence` fields, so bare strings or placeholders cannot satisfy release accounting.
	  Top-level manifest, command entry, and unsupported-surface objects are closed
	  schemas; unknown fields are rejected instead of being carried as unvalidated
	  release status.
	  Optional unsupported-surface accounting fields such as `id`, `mitigation`,
	  and `notes`, when present, must be non-empty strings or lists of non-empty strings;
	  Optional accounting lists must be non-empty, and placeholder optional accounting fields are rejected.
	  Each `surface` field must name the known surface it accounts for; evidence, mitigation, and notes cannot be the only matching fields.
	  Each required surface must be covered by a distinct object; catch-all entries
  cannot satisfy multiple known surfaces. Duplicate objects for the same known surface are rejected,
  and entries outside the verifier catalog are rejected so new release blockers
  cannot appear only in an assembled release manifest.
  The release criteria Known Unsupported Or Partial Surfaces bullets are also
  status-class checked against the verifier catalog, so the documented surface
  text must keep generated-only/not-advertised/fail-closed, partial/blocked, and
  release-CI-required wording aligned with the manifest validator.
  Each surface status must explicitly mark unsupported, partial, blocked,
  fail-closed/not-advertised, or release-CI-required coverage; vague completion-style statuses are rejected.
  The status class must match the surface: ZooKeeper-era inter-broker API keys 4-7 accounting must
  mark generated-only/not-advertised/fail-closed behavior, broker-only stateless replacement must remain partial/blocked, and live CI matrix/performance accounting, including each external-client, chaos, load/scale, failover, provider, and performance surface, must remain release-CI-required or blocked until those gates run.
  The required live/external release blockers are tracked as distinct surfaces:
  external client/security/OAuth live matrix; cross-broker chaos live matrix; Docker E2E load/scale live orchestration; KRaft failover network matrices; live S3 provider outage and multipart-fault profile execution; and comparative Kafka/AutoMQ performance profile/trend gates.
  These cover external-client, secured-client, and OAuth profile execution;
  scheduled cross-broker chaos and broader multi-broker chaos; and live provider outage and multipart-fault profile execution.
  Release manifests must explicitly set the blocking bug flags
  `known_data_loss_bug=false`, `advertised_stub_api=false`, and
  `untriaged_durability_failure=false`, plus `automq_complete=false` while
  unsupported/partial surfaces remain. The `automq_complete` flag is checked
  against the verifier catalog as well as the manifest surface list, so eliding
  unsupported/partial surfaces cannot enable a complete claim. Those flags must
  be JSON booleans rather than strings or placeholder values.
  The default
  production-readiness suite verifies the document keeps those gates, required
  evidence, provider/security profile variables, and unsupported surfaces
  explicit.

## Rules For Future Changes

- Generated schema support does not imply broker support.
- ApiVersions must be driven only by `broker_supported_apis`.
- Every new advertised API needs a handler, version/header mapping, unit tests,
  malformed-frame tests, and at least one integration/client test.
- AutoMQ extension APIs must use the generated schema keys: 501-519 and 600-602.
- If an API is intentionally single-node-only, document the degraded semantics
  and add a test proving the response is schema-compatible.

# AutoMQ Parity Release Criteria

ZMQ can only be labeled AutoMQ-complete when every required gate below passes
for the target release commit. Generated Kafka schemas, local unit tests, or a
single-node demo are not sufficient by themselves.

## Required Gates

- `Protocol`: every advertised broker/controller API version has generated
  schema-compatible decode/encode, malformed-frame coverage, golden fixtures,
  and at least one external-client compatibility path. ApiVersions must not
  advertise stubs.
- `Durability`: acknowledged Produce, OffsetCommit, transaction, and AutoMQ
  metadata mutations survive process crash, fresh-local-dir replacement,
  S3/MinIO rebuild, and provider fault injection without data loss. Existing
  local recovery snapshots must fail closed when unreadable, syntactically
  malformed, or semantically invalid rather than being silently skipped, and S3
  fetch/flush metadata faults, including WAL
  object-key allocation failures, must not be reported as empty successful reads
  or acknowledged writes. Repeated stateless S3 WAL replacement must resume
  object keys from recovered WAL state and retain post-replay appends through a
  second fresh-dir replay, and legacy synchronous S3 WAL fallback objects must
  use indexed replayable encoding before acknowledgement so fresh-local-dir
  replacement and replacement-side continuation replay can rebuild them. Object lifecycle
  cleanup must not remove metadata before preserving the S3 keys needed for
  delete or orphan retry, and compaction must checkpoint replacement
  ObjectManager metadata before deleting old S3 objects or else roll back or
  track uploaded replacements for retry cleanup. Coordinator offset
  enumerations and persisted offset restore must not silently skip malformed
  keys that belong to the requested group, and internal committed-offset replay
  must fail closed on malformed values for parseable offset keys while replaying
  latest replacement-side direct committed-offset updates through generated
  OffsetFetch after a second fresh-dir S3 WAL replay, plus retaining initial and
  replacement-side transactional committed-offset updates through generated
  OffsetFetch read-back and a second fresh-dir S3 WAL replay;
  OffsetDelete tombstones must also remain authoritative through
  generated OffsetFetch read-back, replacement-side tombstone writes, and a
  second fresh-dir S3 WAL replay.
  DeleteGroups offset tombstones must retain replacement-side group and offset
  removals through generated OffsetFetch read-back and a second fresh-dir S3 WAL
  replay. DeleteGroups share-session cleanup snapshots must retain
  local rollback visibility and replacement-side share-session removals through
  generated ShareGroupDescribe read-back and a second fresh-dir S3 WAL replay.
  Consumer-group lifecycle replay and local snapshot-write rollback checkpoints
  from `__consumer_offsets` must retain
  replacement-side membership, timeout, rack, subscription, and assignment
  updates through generated ConsumerGroupDescribe read-back and a second
  fresh-dir S3 WAL replay. Share-session and share-partition replay from
  `__consumer_offsets`, plus local share-session and partition-state rollback
  checkpoints, must retain replacement-side session-epoch and partition-state
  updates through generated ShareGroupDescribe/ShareFetch/
  ReadShareGroupState read-back and a second fresh-dir S3 WAL replay. Transaction
  coordinator replay from `__transaction_state` must retain replacement-side
  generated InitProducerId empty-state allocations and epoch bumps, generated
  AddPartitionsToTxn and AddOffsetsToTxn registrations, transaction timeouts,
  statuses, registered partitions, and generated ListTransactions/
  DescribeTransactions visibility at local success/rollback checkpoints,
  while WriteTxnMarkers/EndTxn local marker rollback/rejection checkpoints must
  also prove no-marker or post-marker partition visibility through generated
  ListOffsets, DescribeStreams, AutomqGetPartitionSnapshot, and Fetch read-back,
  transaction authorization-denial no-mutation checks through generated
  ListTransactions/DescribeTransactions read-back where applicable,
  replacement-side mutation checkpoints, and through a second fresh-dir S3 WAL
  replay.
  WriteTxnMarkers commit/abort replay must retain
  replacement-side marker batches, completed transaction snapshots, and advanced
  partition offsets through generated transaction-introspection, ListOffsets,
  DescribeStreams, AutomqGetPartitionSnapshot, and Fetch control-batch
  read-back plus a second fresh-dir S3 WAL replay.
  EndTxn commit/abort replay
  must retain replacement-side generated EndTxn commits and aborts, completed
  transaction snapshots, control batches, and advanced partition offsets through
  generated transaction-introspection, ListOffsets, DescribeStreams,
  AutomqGetPartitionSnapshot, and Fetch control-batch read-back plus a second
  fresh-dir S3 WAL replay. Timed-out transaction
  auto-abort replay must retain replacement-side timeout aborts, completed
  transaction snapshots, abort control batches, and advanced partition offsets
  through generated transaction-introspection, ListOffsets, DescribeStreams,
  AutomqGetPartitionSnapshot, and Fetch control-batch read-back plus a second
  fresh-dir S3 WAL replay. Local
  timed-out transaction abort markers must also be visible through generated
  Fetch control-batch read-back. Internal
  compacted-topic compaction must fail closed on cache allocation and malformed
  record-batch parser errors, and broker-owned internal log replay must reject malformed
  record-batch headers, truncated records, trailing bytes, and records missing
  required internal keys or values instead of partial replay. Produce must reject
  full-size malformed Kafka record-batch headers and
  invalid batch-length envelopes before append, and idempotent Produce must
  reserve producer-sequence state before append so reservation failures do not
  acknowledge data with stale deduplication state. Generated Fetch read-back
  must prove successful Produce visibility, rejected no-append visibility, and
  partial transactional partition-registration visibility through the client
  protocol instead of broker-local partition counters; generated
  authorization-denial read-back must prove Fetch leaves retained records
  visible after reauthorization and DescribeProducers does not create denied
  topic metadata; generated
  DescribeStreams and AutomqGetPartitionSnapshot read-back must also prove
  Produce local-checkpoint storage-error visibility and no-append rollback
  visibility. Producer-sequence recovery
  from durable user logs must reject malformed record-batch envelopes and must
  not let short raw records hide later idempotent batches, and S3 WAL recovery
  must retain replacement-side producer-sequence progress, duplicate generated
  Produce suppression, repaired partition offsets, ObjectManager stream end
  offsets, generated Fetch visibility for replayed and post-replay appended user
  records, and generated ListOffsets visibility through a second fresh-dir
  replay. Local filesystem WAL restart must retain acknowledged records through
  generated Fetch read-back. Local share-session restart must retain epoch
  continuity through generated ShareFetch read-back. Local share-partition state
  restart must retain state through generated ReadShareGroupState read-back.
  Local topic-config restart must retain common config overrides through
  generated DescribeConfigs read-back. Local finalized-feature restart must
  retain generated ApiVersions v3 visibility.
  Topic and partition-state restore must not expose metadata when local partition storage
  cannot be rebuilt. Filesystem WAL cleanup and retention must surface segment
  deletion failures and retain segment metadata for retry. Topic creation must
  fail closed and roll back visible topic metadata when local partition-state
  allocation or failover ownership tracking fails, and DeleteTopics partition
  cleanup must not depend on heap allocation to remove already-known local
  state. Generated AlterConfigs and IncrementalAlterConfigs replay must retain
  replacement-side topic config updates through a second fresh-dir S3 WAL replay
  and keep those values visible through generated DescribeConfigs; generated
  DescribeConfigs must also show deleted AlterConfigs and IncrementalAlterConfigs
  overrides reset to defaults at each replacement checkpoint.
  Partition-state lookups used by Produce/Fetch/admin info,
  ListOffsets, DeleteRecords, OffsetCommit lag calculation, broker request
  validation, partition-state restore, log-dir estimates, and transaction marker
  completion/LSO cleanup must not depend on formatted keys that can miss long
  but valid topic names, including generated Fetch read-back of long-topic
  transaction control batches. AutoMQ extension authorization denials,
  denied-response serialization/materialization failures, normal read
  response-materialization/final-serialization failures, normal mutation
  response-construction failures, read-only and mutation malformed-error
  serialization failures,
  success-serialization rollback paths, local persistence rollback paths, and
  attached-follower mutation rejections must leave stream, KV, metadata, license,
  router, manifest, S3 object cursor, and node-id cursor state unchanged and
  prove that through generated AutoMQ read-backs. Transaction
  partition-registration allocation failures must be reported as storage errors
  without advancing empty transactions into ongoing state. Consumer-group timeout eviction must not silently
  keep expired members active because an allocation failed while collecting expired
  member IDs. Raft/controller metadata replication and startup recovery must not
  acknowledge or apply entries that failed local log allocation or persistence,
  and persisted Raft logs must reject truncated, invalid, or non-contiguous
  records instead of recovering a partial controller metadata image. Raft
  heartbeat broadcasts must surface peer RPC failures to callers and logs.
  Controller quorum responses that carry leader endpoints must not report
  success when leader-endpoint materialization fails.
  Committed Raft voter config records must not be marked applied when endpoint
  metadata is malformed or config application fails. Raft snapshot compaction
  must not truncate log entries unless `snapshot.meta` and the prepared-object
  registry snapshot have both been persisted, and malformed Raft epoch/vote or
  snapshot metadata must fail startup recovery. Raft epoch/vote metadata writes
  must be persisted atomically before granting votes, starting elections, or
  accepting leader epochs; self-election persistence failures must deny or
  reject the transition instead of leaving externally visible quorum state ahead
  of durable `raft.meta`.
  Unreadable or malformed prepared-object registry snapshots must fail startup
  recovery rather than silently losing prepared-object tracking after log
  compaction.
- `Stateless`: a replacement broker can rebuild topic, partition, offset,
  transaction, producer, ACL, quota, SCRAM, reassignment, and AutoMQ stream/object
  metadata from quorum records and shared storage without manual repair.
- `MultiNode`: three-node controller and broker gates cover leader election,
  controller failover, broker fencing/unfencing, rolling restart, reassignment,
  scale in/out, and rack-aware/autobalancer convergence. Controller quorum voter
  configuration must fail closed on malformed entries instead of silently
  shrinking or miswiring the controller, metadata-client, or broker peer sets,
  and controller-role processes must reject voter sets that omit the local
  `node.id` or point that local voter at a different controller listener port;
  the local voter endpoint does not match startup when its port differs from
  the configured controller listener.
  Metadata-client controller discovery must distinguish malformed
  DescribeQuorum responses from a legitimate no-leader response.
  Startup configuration must fail closed on unreadable config files, malformed
  properties lines, empty property keys, malformed cluster identity/listener
  integers including `node.id`, conflicting `broker.id`/`node.id` aliases,
  negative node IDs, invalid `process.roles`, blank or embedded-blank
  `log.dirs`/`--data-dir` entries, blank S3 string settings,
  malformed SASL security settings, invalid Kafka listener endpoints, invalid
  `controller.listener.names`, invalid `inter.broker.listener.name`, invalid
  `listener.security.protocol.map` including maps that omit configured listener
  names, selected listener-map security protocols that are not applied to broker startup,
  listener maps where startup derives the executable broker security protocol from the wrong listener,
  duplicate listener names across listener lists/maps,
  `advertised.listeners` names that do not match configured `listeners`
  (advertised listener names must match configured listeners), blank CLI string values, invalid S3
  scheme/path-style values, invalid S3 WAL flush modes, invalid `security.protocol`,
  invalid `security.inter.broker.protocol`, mutually exclusive
  `inter.broker.listener.name`/`security.inter.broker.protocol` settings,
  TLS context initialization failures, critical startup thread failures,
  invalid `ssl.client.auth`, missing CLI flag values, and unknown CLI arguments.
  Stale Raft peer cleanup
  must not depend on heap allocation while reconciling committed voter metadata.
  Auto-balancer planning must not execute partial reassignment plans when
  planner bookkeeping allocations fail.
- `Security`: TLS, mTLS, outbound TLS hostname verification, SASL/PLAIN,
  SCRAM, SCRAM delegation-token authentication, OAuthBearer, ACL authorization,
  and negative authentication/authorization cases pass for every advertised API
  shape.
- `Observability`: health/readiness, JSON logs, Prometheus metrics, Grafana
  panels, and alert rules cover the production SLOs and reference only
  registered metrics, including AutoMQ-compatible request count/error/size/time
  aliases, JMX-compatible socket-server connection-count and
  expired-connection-kill metrics, controller leader-election and
  unclean-leader-election counters, request total/local/remote/queue/response-send
  timing SLO alerts, at-min-ISR warning alerts, ISR shrink/expand/failure metrics, and
  AutoMQ object-manager stream/object/prepared/mark-destroyed metadata gauges
  with object fanout, prepared-object, and destroyed-object backlog alerts,
  plus consumer-lag, group-coordinator failed-partition, and Raft
  election/pre-vote/epoch-churn, commit-stall, and vote-rejection SLO coverage, plus storage SLO coverage for
  high S3 request/byte rates, cache-miss ratio, cache byte gauges, and
  compaction-cycle p99, plus retained client-telemetry sample pressure.
  Checked-in Grafana dashboard JSON must be strict JSON:
  non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity` and
  duplicate JSON object keys are rejected before dashboard metric-reference checks.
  The observability static audit also enforces positive 24-column dashboard
  grid positions, timeseries panel types, non-empty target legends, a closed
  dashboard target schema containing only `expr` and `legendFormat`, and a
  closed Prometheus alert group set.
  Metrics scrape construction failures must return shaped 5xx responses
  instead of silent connection closes.
- `Performance`: repeatable local and live-S3 benchmarks enforce produce/fetch
  throughput, p99 latency, S3 operations per MiB, recovery time, and bounded
  memory growth. Runtime elapsed-time gates must use monotonic clocks,
  including Python live-harness deadlines and elapsed-duration checks; wall
  clocks may remain only for Kafka-visible timestamps and unique object names;
  Kafka-visible timestamps include comparative benchmark RecordBatch fixtures.
  Broker/controller heartbeat leases, consumer-group
  session/rebalance timeouts, quorum waits, and transaction timeouts, while
  Kafka-visible timestamps continue to use wall-clock time. Comparative
  benchmark gates must run against Kafka or AutoMQ baselines before release,
  enforce configured target coverage, and enforce configured throughput,
  latency, and error-rate regression thresholds. Historical trend gates must
  compare current ZMQ results against an archived ZMQ benchmark artifact.
  Release evidence command strings must not retain placeholder paths such as
  `/path/to/...`, or angle-bracket placeholders such as `<host>` and `<port>`.
- `Chaos`: SIGKILL, broker/controller restart, network partition, S3 outage,
  clock-skewed records, and slow/partial client scenarios must pass in the
  gated chaos suites.

## Required Commands

These commands define the minimum release gate set. Environment-specific gates
may require MinIO, S3-compatible providers, external Kafka clients, or network
fault-injection hooks. Release evidence command strings must preserve the
pinned Zig executable path below so a local `zig` symlink cannot satisfy the
release gate accidentally, and captured output must show real gate execution
rather than a gated harness skip message. Release evidence uses token-aware command validation:
required gate environment assignments, including `ZMQ_RUN_*` and benchmark
trend-baseline assignments, must prefix the same shell command segment as the
pinned Zig invocation. Command strings must be single-line and unquoted: CR/LF line breaks
are rejected so environment assignments cannot be detached from the required
gate by newline command separators, and shell quote characters are rejected so
quoted assignment words cannot masquerade as active gate environment.
Backslash escapes are rejected for the same reason: escaped assignment words
cannot satisfy required gate environment.
Required command environment assignments must also be recorded in the manifest
environment so release evidence cannot rely on untracked shell provenance.
Angle-bracket placeholder values such as `<host>`, `<port>`, and `<bucket>` are
rejected wherever concrete command, environment, or manifest values are required.
Repeated environment assignments are rejected within each command segment, even
when the final shell value would match the required setting, so release evidence
cannot contain contradictory provenance for the same gate.
The fenced command block itself is parsed by the release-evidence self-test with
the same token-aware command-shape checks, so stale examples with duplicate
assignments, detached environment values, shell operators, or embedded output
markers cannot remain in the release criteria.
The build static audit also maintains a required command catalogue mirror of
the release-evidence REQUIRED_COMMANDS list and the fenced release criteria command block:
the same command lines must appear in the same order, and any change to the
release-evidence command catalogue must update the build static audit command-block catalogue.
The build static audit also pins the required environment-variable catalogue:
release-evidence REQUIRED_ENV_VARS entries must stay present in the release criteria, parity notes, and production-readiness pins, so every required coverage variable is documented before release evidence can rely on it.
Any required coverage-variable change must update the build static audit environment catalogue.
The build static audit also pins the command environment-assignment catalogue:
per-gate command_env_assignments entries must stay present in the release
criteria, parity notes, and production-readiness pins, so each same-gate command provenance variable is documented before release evidence can require it.
The current command-env gates are `broker chaos harness`,
`external client matrix`, `S3 provider matrix`, `KRaft failover gate`,
`live-S3 benchmark gate`, and `comparative benchmark gate`.
Any command assignment change must update the build static audit command-env catalogue.
The build static audit also pins the release-evidence command-shape catalogue:
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
The build static audit also pins the release-evidence skip-marker catalogue:
per-gate skip_markers entries must stay present in the release criteria, parity notes, and production-readiness pins, so each skipped live gate has documented negative-path evidence.
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
The build static audit also pins the release-evidence output-marker catalogue:
per-gate output_markers entries must stay present in the release criteria, parity notes, and production-readiness pins, so each required success marker is documented before release evidence can require it.
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
Any output-marker change must update the build static audit output-marker catalogue.
The build static audit also pins the release-evidence detail output marker catalogue:
COMPARATIVE_TABLE_ROW_MARKERS, BENCHMARK_OUTPUT_LINE_MARKERS,
KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS, KRAFT_DETAIL_OUTPUT_MARKERS, and
E2E_OUTPUT_LINE_MARKERS entries must stay present in the release criteria,
parity notes, and production-readiness pins. These KRaft, Docker E2E, benchmark, and comparative benchmark detail markers
are checked separately from per-gate summary markers so line-aware verifier
coverage cannot drift. Any detail output marker change must update the
build static audit detail-output-marker catalogue.
The build static audit also pins the forbidden command-fragment catalogue:
per-gate forbidden fragments must stay present in the release criteria, parity notes, and production-readiness pins, so the local benchmark gate cannot accidentally satisfy live-S3 benchmark evidence with `ZMQ_RUN_BENCH_LIVE_S3=1`.
Any forbidden-fragment change must update the build static audit forbidden-fragment catalogue.
The build static audit also pins the release-evidence schema field catalogue:
RELEASE_EVIDENCE_FIELDS, COMMAND_ENTRY_FIELDS, and UNSUPPORTED_SURFACE_FIELDS entries must stay present in the release criteria, parity notes, and production-readiness pins, so every closed schema field remains documented before the manifest verifier can rely on it.
The current release manifest fields are `commit`, `environment`, `commands`,
`unsupported_or_partial_surfaces`, `known_data_loss_bug`,
`advertised_stub_api`, `untriaged_durability_failure`, and `automq_complete`;
the current command entry fields are `command`, `exit_code`, and `output`; and
the current unsupported surface fields are `surface`, `status`, `evidence`,
`id`, `mitigation`, and `notes`.
Any schema-field change must update the build static audit schema-field catalogue.
The build static audit also pins the release-evidence blocking-flag catalogue:
BLOCKING_FLAGS entries must stay present as explicit false manifest booleans in
the release criteria, parity notes, and production-readiness pins, so every
blocking flag remains documented before the manifest verifier can rely on it.
The current blocking flags are `known_data_loss_bug=false`,
`advertised_stub_api=false`, and `untriaged_durability_failure=false`.
Any blocking flag change must update the build static audit blocking-flag catalogue.
Duplicate successful command entries for the same required gate are rejected so
release evidence cannot choose one passing run while retaining another stale or
contradictory run for that gate.
Top-level `ZMQ_RUN_*` opt-in gates and `ZMQ_BENCH_COMPARE_ENFORCE_GATES`
must parse as real booleans in live harnesses and release evidence, so blank,
placeholder, or arbitrary values fail closed and cannot silently skip required
coverage.
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
The build static audit also pins the release-evidence token vocabulary catalogue:
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
Configured live-harness ports, benchmark live-S3 iteration/payload-size settings,
E2E fixture phase indexes, and E2E fixture load-record counts must parse as
concrete integer strings in release evidence; placeholders, malformed text, JSON
numbers, non-positive ports/load-record counts, and negative phase indexes fail
closed. E2E fixture actions must be one of `scale-in`, `scale-out`, `load`,
`probe`, or `noop`, and fixture node selectors must be nonblank,
non-placeholder names instead of falling back from explicit blank overrides.
Multi-segment release gate command chains must use
success-dependent `&&` separators only; `;` and `||` cannot connect or trail
release-gate command fragments because they can mask a failed gate; only
documented compose config commands may use multi-segment release gate chains:
the compose command must run before its `echo ok: ...` marker through `&&`,
and quoted/echoed command text cannot satisfy a gate. Except for the required
compose config `echo ok: ...` markers, release gate commands must be direct
invocations: pipes, backgrounding, redirection, subshell grouping, and command
substitution are rejected, including Bash `&>`/`&>>` combined redirects, so
captured evidence cannot be detached from the
required gate process. Aside from those required
compose config `echo ok: ...` markers, command strings must not embed release
output marker text such as `ok:`, `Build Summary:`, `thresholds:`,
`trend thresholds:`, or `result: pass`; markers must come from captured command output.
Every captured Zig build output must include exactly one successful `Build Summary: N/N steps succeeded`
line, matching `N/N tests passed` counts when the summary includes test totals,
must not contain any unsuccessful `Build Summary:` line,
and must include a non-negated build success line matching the invoked Zig build step,
such as `test success` or `bench-compare success`.
The live-S3 benchmark command below uses non-placeholder example values only so
the documented command block exercises the same placeholder rejection rules as
release evidence; release jobs must replace those values with their actual
provider settings and record the same values in the release manifest.

```sh
/tmp/zig-aarch64-linux-0.16.0/zig build test --summary all
/tmp/zig-aarch64-linux-0.16.0/zig build test-protocol-static-audit --summary all
/tmp/zig-aarch64-linux-0.16.0/zig build test-observability-static-audit --summary all
/tmp/zig-aarch64-linux-0.16.0/zig build test-build-static-audit --summary all
docker compose -f docker-compose.yml config --quiet && echo ok: root compose config
docker compose -f benchmarks/kafka-compose.yml config --quiet && echo ok: kafka compose config
docker compose -f benchmarks/automq-compose.yml config --quiet && echo ok: automq compose config
ZMQ_RUN_CHAOS_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-chaos --summary all
ZMQ_RUN_CLIENT_MATRIX=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-client-matrix --summary all
ZMQ_RUN_MINIO_TESTS=1 ZMQ_S3_REQUIRE_MULTIPART_EDGE=1 ZMQ_S3_REQUIRE_LIST_PAGINATION=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-minio --summary all
ZMQ_RUN_PROCESS_CRASH_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-s3-process-crash --summary all
ZMQ_RUN_S3_PROVIDER_MATRIX=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-s3-provider-matrix --summary all
ZMQ_RUN_KRAFT_FAILOVER_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-kraft-failover --summary all
ZMQ_RUN_E2E_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-e2e --summary all
/tmp/zig-aarch64-linux-0.16.0/zig build bench --summary all
ZMQ_S3_ENDPOINT=s3.release.internal ZMQ_S3_PORT=9443 ZMQ_S3_BUCKET=zmq-release-bench ZMQ_S3_SCHEME=https ZMQ_S3_REGION=us-east-1 ZMQ_S3_PATH_STYLE=false ZMQ_RUN_BENCH_LIVE_S3=1 /tmp/zig-aarch64-linux-0.16.0/zig build bench --summary all
ZMQ_RUN_BENCH_COMPARE=1 ZMQ_BENCH_COMPARE_REQUIRED_TARGETS=zmq,kafka,automq ZMQ_BENCH_COMPARE_REQUIRE_TREND=1 ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json /tmp/zig-aarch64-linux-0.16.0/zig build bench-compare --summary all
```

Release CI must set required coverage variables for environment-specific
matrices, including `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` for scheduled
controller/broker partition phases, `ZMQ_CHAOS_REQUIRED_SCENARIOS` for the
broker chaos scenario set, `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES` for broker
chaos partitions, `ZMQ_E2E_REQUIRED_CHAOS_PHASES` for Docker cross-broker
chaos phases, `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` for live Docker
scale-in/scale-out/load phases, and provider/client/profile requirement
variables for S3 and external-client matrices. The MinIO/S3 integration gate
must enable `ZMQ_S3_REQUIRE_MULTIPART_EDGE=1` and
`ZMQ_S3_REQUIRE_LIST_PAGINATION=1` so all six live MinIO tests plus the two
local preflight self-tests run rather than skipping provider-edge coverage.
Direct MinIO/S3 and process-crash live gates
must reject placeholder S3 endpoint, bucket, credential, region, TLS CA, and
non-positive port settings, and must strictly parse S3 boolean toggles such as
`ZMQ_S3_PATH_STYLE`, `ZMQ_S3_SKIP_ENSURE_BUCKET`,
`ZMQ_S3_SKIP_MINIO_HEALTH`, `ZMQ_S3_REQUIRE_MULTIPART_EDGE`, and
`ZMQ_S3_REQUIRE_LIST_PAGINATION`, so placeholder or arbitrary values cannot
silently select default behavior.
Docker E2E load/scale jobs may
set `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1` to satisfy required named phases with
the built-in fixture when external orchestration hooks are not needed; in that
mode the fixture can infer its phase matrix directly from
`ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES`. Fixture-backed `scale-out` phases
pre-stop their target by default before starting it; jobs that prepare a
stopped node externally can set
`ZMQ_E2E_LOAD_SCALE_<PHASE>_FIXTURE_PRESTOP=0`. The fixture enable flag and
fixture `DRY_RUN`/`PRESTOP` toggles must parse as real booleans, so blank,
placeholder, or arbitrary values fail before the fixture touches Docker.
Every E2E load/scale apply/restore hook receives
`ZMQ_E2E_LOAD_SCALE_APPLY_MARKER` and
`ZMQ_E2E_LOAD_SCALE_RESTORE_MARKER`; release-qualifying hooks, including the
built-in fixture, must publish those marker payloads to the active test topic
so the parent E2E harness can fetch hook-owned evidence instead of producing
the marker itself. E2E hook context maps such as broker, controller, metrics,
and container name/value lists must reject duplicate names before fixture
orchestration so stale context cannot overwrite the active node mapping. S3
provider coverage must pin provider, outage, process-crash/replacement,
ListObjectsV2 pagination, multipart-edge, and multipart-fault profiles with
`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES`, and
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES`. External-client coverage
must pin required profile coverage with `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`,
required client implementations and semantic suites with
`ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS` and
`ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS`, exact version-labeled profiles with
`ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES`, and secured-client plus
negative security coverage with `ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES` and
`ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES`; OAuth-secured client
coverage must also be pinned with `ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES`
and `ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES` when OAUTHBEARER is
part of the release target. Required client profiles must also record concrete
profile setting provenance for selected tools, bootstrap endpoints, semantic
suites, exact version labels, Java classpaths, Go module versions,
secured-client protocol settings, and positive/negative OAuth fixtures before
command-owned profile markers can satisfy release evidence. Secured, security-negative, and
OAuth client profiles must also emit same-block command-owned security detail markers after
the live tool probe succeeds, so generic `security` semantic tokens cannot
stand in for OAUTHBEARER positive/negative fixture execution. The live client matrix must
preflight the same required-profile contract before execution, so required
profiles cannot rely on implicit default bootstrap values, `TOOLS=auto`,
implicit/default semantic lists, placeholder version labels, or `PLAINTEXT`
security protocols for secured/OAuth gates; secured profiles must explicitly
enable `security` or `security-negative` semantics, and OAuth-positive profiles
must explicitly enable `security` semantics. Versioned/security/OAuth
sub-profile requirements must stay within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`.
Bootstrap provenance must parse as one or more comma-separated `host:port`
entries with ports in `1..65535`.
Client matrix Go auto-discovery toggles, including `ZMQ_CLIENT_MATRIX_ENABLE_GO`
and profile-scoped `ZMQ_CLIENT_MATRIX_<PROFILE>_ENABLE_GO`, must parse as real
booleans so placeholder or arbitrary values cannot silently drop Go coverage.
Profile semantics must also match
the selected tools, so rebalance,
transactional, and security semantics cannot be claimed by client tools that do
not expose those probes in the live matrix.
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
Release evidence also validates
tool-specific OAuth fixture semantics, rejecting malformed positive fixtures and
future-valid or otherwise successful fixtures that are claimed as negative
coverage. OAuth raw JWT fixtures must be strict JSON:
non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity` are
rejected before client execution, and duplicate JSON object keys are rejected
before claim validation. The build static audit also pins the client matrix self-test error catalogue
for required client profile/tool/semantic coverage, security and OAuth fixture validation,
bootstrap provenance, and strict OAuth fixture parsing. Benchmark jobs can tighten local and live-S3
thresholds with `ZMQ_BENCH_S3_WAL_MAX_P99_MS`,
`ZMQ_BENCH_S3_WAL_MAX_REQUESTS_PER_MIB`,
`ZMQ_BENCH_S3_WAL_MAX_REBUILD_MS`,
`ZMQ_BENCH_LIVE_S3_MAX_REQUESTS_PER_MIB`,
`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`,
`ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO`,
`ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO`,
`ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO`, and
`ZMQ_BENCH_COMPARE_MAX_ERROR_RATE`; local, live-S3, and comparative benchmark
threshold variables must be nonblank, non-placeholder strings that parse as
finite non-negative floats instead of falling back to defaults, and live-S3
benchmark iteration/payload-size variables must parse as positive integers.
When release evidence records `ZMQ_BENCH_LIVE_S3_ITERATIONS` or
`ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES`, the live-S3 benchmark command must also
include the same assignment so sizing provenance cannot drift from the captured
run.
The build static audit also pins the release-evidence numeric environment catalogue:
BENCHMARK_THRESHOLD_ENV_VARS and POSITIVE_INTEGER_ENV_VARS entries must stay
present in the release criteria, parity notes, and production-readiness pins, so
finite non-negative floats and positive integers cannot drift from the verifier
catalogue. Any numeric environment change must update the
build static audit numeric-env catalogue.
Comparative benchmark release jobs must set
`ZMQ_BENCH_COMPARE_REQUIRE_TREND=1` and provide a prior
`benchmarks/results.json` artifact with
`ZMQ_BENCH_COMPARE_TREND_BASELINE`; the trend baseline must be a concrete
non-placeholder path; relative trend baseline paths resolve from the project
root, and the trend baseline must not resolve to the current `benchmarks/results.json` output path.
The trend-required flag must parse as a real boolean, so placeholder or
arbitrary values cannot silently disable the trend gate.
They can tune trend thresholds with
`ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO`,
`ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO`, and
`ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO`. Trend baseline artifacts must
remain strict structured benchmark JSON: non-standard JSON constants such as
`NaN`, `Infinity`, or `-Infinity` and duplicate JSON object keys are rejected while parsing archived baselines, and non-standard constants are rejected while
writing current `benchmarks/results.json`; current results are serialized before replacing the existing artifact so invalid payloads cannot truncate prior
benchmark evidence. Enforced comparative benchmark gates only replace
`benchmarks/results.json` after the gate passes, so failed release-gate runs
cannot clobber the prior artifact that future trend comparisons may depend on.
Current result artifacts also record schema-version,
selected/required target, target-label, iteration/warmup, threshold, gate, and
trend-baseline metadata; selected/required target metadata must list concrete
known unique targets, `targets_with_results` must match result targets,
each result target must be included in selected target metadata, and required
target metadata must be a subset of selected target metadata. Artifact
target-label, iteration/warmup, threshold, gate, and trend-baseline metadata
must match the current benchmark profile shape with finite non-negative
thresholds, real boolean gate flags, and concrete non-placeholder trend
baseline paths whenever trend metadata requires one.
Result artifact maps must be objects with only known target keys and per-target
object results. Per-target artifact result maps must contain the current
benchmark row keys and no unknown benchmark result keys.
Archived trend baselines must include schema-version 1 artifact metadata whose targets_with_results includes zmq.
Comparative output must include a
`trend baseline:` line matching `ZMQ_BENCH_COMPARE_TREND_BASELINE` inside the
`COMPARATIVE BENCHMARK GATE` section when trend gating is required. Every ZMQ
benchmark row used for trend comparison must include numeric finite non-negative `throughput`, `p50`, and
`p99` metrics, and missing, non-numeric, non-finite, negative, or zero trend metrics fail closed before ratio checks can pass.
Current comparative result rows are validated the same way: malformed
target/result objects, non-numeric or non-finite throughput/latency metrics,
non-integral error/request/success counts, negative counts, and zero
throughput/latency values fail the gate before ratios can pass, and the saved
artifact writer applies the same row validation before replacing
`benchmarks/results.json`. Release evidence and gated
matrix/phase harnesses must use concrete non-placeholder values for required
coverage variables, comma-separated coverage variables must parse to at least
one value, blank comma-separated entries and duplicate comma-separated entries are rejected,
explicitly blank selector values are rejected, and required live coverage must include selector/provenance
variables: `ZMQ_CHAOS_NETWORK_MATRIX`, `ZMQ_KRAFT_NETWORK_MATRIX`,
`ZMQ_E2E_CHAOS_MATRIX`, `ZMQ_E2E_LOAD_SCALE_MATRIX` unless
`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1` is intentionally relying on fixture
inference from `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES`,
`ZMQ_S3_PROVIDER_PROFILES`, and `ZMQ_CLIENT_MATRIX_PROFILES`. Required values
must be subsets of those selector variables, and distinct selected names that
normalize to the same environment-variable token or use placeholder profile
names are rejected before release evidence can count the corresponding output
markers.
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
S3 provider matrix evidence
must also record non-placeholder
`ZMQ_S3_<PROFILE>_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`
profile settings or documented global
`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` fallbacks,
and non-`minio` provider profiles must set explicit profile/global S3 settings
instead of inheriting built-in local MinIO defaults. Each provider profile
output marker must match the selected
profile/global endpoint and effective scheme/region/path-style settings;
`SCHEME` must parse as `http` or `https`, and `PATH_STYLE` must parse as
`true` or `false`. When a provider profile requires live outage coverage, the
provider matrix must also verify the underlying chaos output includes the
matching `ok: chaos live-s3-outage provider ...` line for that selected profile
before it emits
`ok: S3 provider outage detail profile ... endpoint=<endpoint>:<port> ... source=command` and
`ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command`.
When a provider profile requires process-crash/replacement coverage, the
provider matrix must verify the underlying process-crash output includes the
detailed `ok: S3 process crash/replacement harness passed ...` marker with
`bucket=<bucket>` matching the selected provider bucket and `source=command`
before it emits
`ok: S3 provider process-crash detail profile ... bucket=<bucket> ... source=command` and
`ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command`.
Release evidence must also
record the hook command variables that make required live coverage active:
`ZMQ_CHAOS_S3_{DOWN,UP}` for required broker live-S3 outage scenarios,
`ZMQ_CHAOS_NETWORK_<PHASE>_{DOWN,UP}` or the global
`ZMQ_CHAOS_NETWORK_{DOWN,UP}`, `ZMQ_KRAFT_NETWORK_<PHASE>_{DOWN,UP}` or the
global `ZMQ_KRAFT_NETWORK_{DOWN,UP}`, `ZMQ_E2E_CHAOS_<PHASE>_{DOWN,UP}` or
the global `ZMQ_E2E_CHAOS_{DOWN,UP}`, `ZMQ_E2E_LOAD_SCALE_<PHASE>_{APPLY,RESTORE}`
or the global `ZMQ_E2E_LOAD_SCALE_{APPLY,RESTORE}` unless
`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1`, and required S3 outage/multipart-fault
profile hooks such as `ZMQ_S3_<PROFILE>_{OUTAGE_DOWN,OUTAGE_UP}` and
`ZMQ_S3_<PROFILE>_MULTIPART_FAULT_CMD` or their global fallbacks. These hook
commands must be non-placeholder, nonblank, and parseable before output markers
can satisfy live coverage. Explicit hook variables, including global and
phase/profile-specific hook variables, must fail closed when blank instead of
falling back to another hook source or fixture. With
`ZMQ_CHAOS_SCENARIOS=all`, the presence of global broker chaos hook variables
selects the corresponding live scenario even when the hook text is blank, so
blank hook text fails preflight instead of silently skipping coverage. The
same fail-closed rule applies to phase-specific chaos/KRaft expectation
variables and explicit `ZMQ_CHAOS_S3_*` live-provider settings, including
`ZMQ_CHAOS_S3_TLS_CA_FILE`, instead of falling back to `ZMQ_S3_*`. The
corresponding live chaos, KRaft, and Docker E2E
harnesses must preflight the same non-placeholder phase, expectation, and hook
command contract plus configured port/phase-index integer contract before
executing operator-provided hooks; the Docker E2E harness must reject malformed
configured chaos/load-scale hooks before starting Docker work. Required S3 sub-profile evidence must also record
truthy enable toggles for the sub-gates that produced those markers:
`ZMQ_S3_<PROFILE>_RUN_LIVE_OUTAGE`, `ZMQ_S3_<PROFILE>_RUN_PROCESS_CRASH`,
`ZMQ_S3_<PROFILE>_REQUIRE_LIST_PAGINATION`,
`ZMQ_S3_<PROFILE>_REQUIRE_MULTIPART_EDGE`, and
`ZMQ_S3_<PROFILE>_RUN_MULTIPART_FAULT`, or their documented global fallbacks.
The S3 provider matrix command must include those same selected enable
assignments so required sub-profile activation cannot rely on untracked shell
environment.
The live provider matrix must strictly parse these profile/global enable
toggles, plus provider `PATH_STYLE`, `SKIP_ENSURE_BUCKET`, and
`SKIP_MINIO_HEALTH` booleans, so placeholder or arbitrary strings fail before
the MinIO/S3 live suite or sub-gates run.
The release-evidence verifier must strictly parse boolean provenance variables
for benchmark trend requirements, E2E load/scale fixtures, client matrix
Go auto-discovery, and S3 provider path-style/enable toggles, so invalid
boolean text cannot silently remove required coverage from the manifest.
Known S3 string provenance such as endpoint, bucket, credential, region,
scheme, and configured TLS CA variables must also be nonblank strings, must not
use placeholder values, and schemes must be `http` or `https`.
Captured environment variables must be strings with valid shell variable names;
JSON booleans are rejected for environment provenance so the manifest records
actual shell values, and blank or placeholder values are rejected for captured
environment entries.
Required client profile evidence must likewise record
`ZMQ_CLIENT_MATRIX_<PROFILE>_BOOTSTRAP` or the documented global
`ZMQ_CLIENT_MATRIX_BOOTSTRAP`,
`ZMQ_CLIENT_MATRIX_<PROFILE>_TOOLS`,
`ZMQ_CLIENT_MATRIX_<PROFILE>_SEMANTICS`,
`ZMQ_CLIENT_MATRIX_<PROFILE>_VERSION`, Java `JAVA_CLASSPATH` when
`java-kafka` is selected, `PYTHON` when `kafka-python` or `confluent-kafka`
is selected, and a non-`@latest` `GO_MODULE` with an explicit `@version` when
`go-kafka` is selected, using documented global fallbacks only when they
represent the actual profile values. Bootstrap values must be valid
comma-separated `host:port` entries, not URLs, blank hosts, blank ports,
non-numeric ports, or port `0`. Secured and OAuth-required profiles must also record
`SECURITY_PROTOCOL` as `SASL_PLAINTEXT`, `SSL`, or `SASL_SSL`,
`SASL_MECHANISM` as `PLAIN`, `SCRAM-SHA-256`, or `OAUTHBEARER` when SASL is
selected, SASL credentials or TLS CA settings as applicable, plus
tool-compatible `OAUTH_TOKEN`, `OAUTH_JAAS_CONFIG`,
`OAUTHBEARER_CONFIG`, `BAD_OAUTH_TOKEN`, `BAD_OAUTH_JAAS_CONFIG`, and
`BAD_OAUTHBEARER_CONFIG` fixtures for the selected tools, and required
OAuth-positive profiles must include the `security` semantic in their
profile-scoped semantic suite. Explicit blank profile-specific client settings
are selected and fail release evidence instead of falling back to global client
matrix values. Profile-scoped client `TOOLS` and `SEMANTICS` entries must also
reject blank or duplicate comma-separated values in both the live matrix and
release-evidence verifier before coverage is credited.
Also,
`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS` must include `zmq` plus at least one
Kafka or AutoMQ baseline target using lowercase target IDs and must reject
blank or duplicate required-target entries.
The comparative benchmark command must include
`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`, and that command assignment must match the manifest environment
so captured output can be traced to the selected baseline set. If the manifest
records `ZMQ_BENCH_COMPARE_ENFORCE_GATES`, the comparative benchmark command
must include the same gate-control assignment. If the manifest
records custom comparative benchmark threshold variables, the comparative
benchmark command must include matching
`ZMQ_BENCH_COMPARE_{MIN_THROUGHPUT_RATIO,MAX_P50_LATENCY_RATIO,MAX_P99_LATENCY_RATIO,MAX_ERROR_RATE,MIN_TREND_THROUGHPUT_RATIO,MAX_TREND_P50_LATENCY_RATIO,MAX_TREND_P99_LATENCY_RATIO}`
assignments. Release evidence must also keep required
S3 outage, process-crash/replacement, pagination,
multipart-edge, and multipart-fault profiles within `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`,
and the S3 provider matrix command must include
`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`, `ZMQ_S3_PROVIDER_PROFILES`, and each
`ZMQ_S3_PROVIDER_REQUIRED_{OUTAGE,PROCESS_CRASH,LIST_PAGINATION,MULTIPART_EDGE,MULTIPART_FAULT}_PROFILES`
selector matching the manifest environment, plus the matching
`ZMQ_S3_<PROFILE>_{RUN_LIVE_OUTAGE,RUN_PROCESS_CRASH,REQUIRE_LIST_PAGINATION,REQUIRE_MULTIPART_EDGE,RUN_MULTIPART_FAULT}`
or documented global fallback enable assignments for every required S3
sub-profile. Explicit blank profile-specific S3 provider settings and enable
assignments are selected and fail release evidence instead of falling back to global `ZMQ_S3_*` values.
The build static audit also pins the S3 provider matrix self-test error catalogue for
provider profile fallback validation, outage, process-crash, and multipart-fault evidence validation,
and required sub-profile coverage checks. Broker chaos required scenarios must include
`sigkill-restart`, `slow-partial-client`, `clock-skewed-records`,
`s3-outage`, and `network-partition` in
`ZMQ_CHAOS_REQUIRED_SCENARIOS`, and the broker chaos command must include
coverage selector assignments for `ZMQ_CHAOS_REQUIRED_SCENARIOS`,
`ZMQ_CHAOS_REQUIRED_NETWORK_PHASES`, and `ZMQ_CHAOS_NETWORK_MATRIX` matching
the manifest environment. Required E2E evidence must include
`load`, `scale-in`, and `scale-out` in
`ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES`, and include cross-broker coverage in
`ZMQ_E2E_REQUIRED_CHAOS_PHASES`. The Docker E2E command must include
`ZMQ_E2E_REQUIRED_CHAOS_PHASES`, `ZMQ_E2E_CHAOS_MATRIX`, and
`ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` matching the manifest environment; it must
also include `ZMQ_E2E_LOAD_SCALE_MATRIX` when an explicit load/scale matrix is
recorded, and `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE` whenever fixture mode is enabled.
Required versioned, secured,
security-negative, OAuth, and OAuth-negative client profiles must also stay
within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`. Client tool coverage must include
`kcat`, `kafka-cli`, `kafka-python`, `confluent-kafka`, `java-kafka`, and
`go-kafka`; client semantic coverage must include `basic`, `admin`, `groups`,
`rebalance`, `transactions`, `security`, and `security-negative`.
Static audit evidence must include the deterministic `ok: protocol static audit`,
`ok: observability static audit`, and `ok: build static audit` output markers,
and each static audit output marker must appear exactly once as its own stripped line.
Protocol static audit evidence also pins strict schema codegen JSON parsing:
`src/protocol/codegen/codegen.py` and `src/protocol/codegen/codegen_v2.py`
must reject non-standard JSON constants such as `NaN`, `Infinity`, or
`-Infinity` and duplicate JSON object keys before generated Zig protocol schemas are written, and codegen scripts must exit nonzero on schema parse errors.
Compose config evidence must include deterministic `ok: root compose config`,
`ok: kafka compose config`, and `ok: automq compose config` output markers
after `docker compose config --quiet` succeeds for the root, Kafka benchmark,
and AutoMQ benchmark compose files, and each compose config output marker must appear exactly once as its own stripped line.
Build static audit evidence also pins Docker compose release contracts: root
`docker-compose.yml`, `benchmarks/kafka-compose.yml`, and
`benchmarks/automq-compose.yml` must use explicit image tags
(`apache/kafka:4.0.2`, `automqinc/automq:1.6.5`,
`minio/minio:RELEASE.2025-09-07T16-13-09Z`, and
`minio/mc:RELEASE.2025-08-13T08-35-41Z`) and must not use `:latest`; README
local compose topology must stay aligned with `node0`/`node1`/`node2` and
published host ports. It also pins the comparative benchmark self-test assertion catalogue
so target parsing, table-header target labels, gate/regression, strict trend
JSON, threshold, and artifact-metadata failure cases cannot be added or weakened
without updating the static audit. The same audit pins the Docker E2E self-test assertion catalogue
for run gates, chaos/load-scale phase validation, hook context, fixture
payloads, and fixture override rejection. It also pins the broker chaos self-test error catalogue
for scenario selection, hook preflight, required
coverage lists, live-S3 provider config, and record-batch fixtures. It also pins the
KRaft failover self-test error catalogue for run gates, network partitions,
required phases, hook context, protocol fixture parsers, and record-batch fixture invariants.
The build static audit also pins the Python self-test raise-shape catalogue:
the catalogue is scoped to the checked Python self-test gate list, and checked
`--self-test` raise messages must stay within the scanner-supported
literal strings, f-strings, concatenated strings, and loop-selected messages.
Any new self-test raise message form must extend the build static audit scanner
before it can count as release-gate evidence.
The build static audit also pins the release-evidence output-marker dispatch catalogue:
requirement-specific output validators for broker chaos, client matrix, S3, KRaft, Docker E2E, and benchmark markers must stay wired to the
matching release-evidence requirement block. Any new release-evidence output validator must be listed in the build static audit dispatch catalogue before it
can count as deterministic marker evidence.
The build static audit also pins the unsupported-surface catalogue across the
release-evidence verifier, release criteria, parity notes, and production-readiness pins:
each known surface label must stay represented in release criteria bullets and
production readiness checks. Any new unsupported or partial surface must be
listed in the build static audit unsupported-surface catalogue before it can
block or qualify release evidence.
The build static audit also pins the release-evidence unsupported surface status-marker catalogue:
UNSUPPORTED_SURFACE_STATUS_MARKERS entries must stay present in the release
criteria, parity notes, and production-readiness pins so explicit unsupported/partial status markers
cannot drift from the verifier vocabulary. The current unsupported surface
status markers are `unsupported`, `not advertised`, `fail closed`,
`fail-closed`, `generated-only`, `partial`, `blocked`, `blocker`,
`release-ci-required`, `release ci required`, `ci required`, and `must run`.
Any unsupported status change must update the build static audit unsupported-status catalogue.
The build static audit also pins the release-evidence unsupported surface text-field catalogue: UNSUPPORTED_SURFACE_TEXT_FIELDS must stay present in the
release criteria, parity notes, and production-readiness pins so
unsupported-surface text aggregation continues to scan `id`, `surface`,
`status`, `evidence`, `mitigation`, and `notes`. The current text fields are
id, surface, status, evidence, mitigation, and notes. Any unsupported surface
text-field change must update the build static audit unsupported-surface-text-field catalogue.
Live harness evidence must include the harness-owned success markers rather
than only a Zig build summary:
`ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=<positive> source=command`,
`ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command`,
`ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command`,
`ok: chaos s3-outage ... startup_fail_closed=true source=command` or
`ok: chaos s3-outage ... rejected=true error_code=<nonzero> base_offset_negative=true serving=true source=command`,
`ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command`
with endpoint, bucket, scheme, region, and path-style matching the selected
`ZMQ_CHAOS_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` or documented
`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` fallback settings,
while explicit blank `ZMQ_CHAOS_S3_*` values fail closed instead of falling back,
and the broker chaos command must include non-sensitive live-S3 outage provider
assignments for those selected endpoint, port, bucket, scheme, region, and
path-style values,
`ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command`
for required live-S3 outage scenarios,
`ok: chaos network-partition source=command`, and
`ok: chaos harness passed for <scenarios> source=command` for broker chaos, with the summary
matching every `ZMQ_CHAOS_REQUIRED_SCENARIOS` entry exactly; broker chaos scenario
detail markers must appear before the broker chaos harness summary line so
detached output cannot satisfy scenario coverage, and scenario detail markers
must be unique per required scenario. The chaos network-partition scenario summary must appear exactly once as its own stripped line.
The broker chaos harness summary must appear exactly once with `source=command`
as its own stripped line.
The build static audit also pins the release-evidence chaos scenario catalogue:
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

`ok: client matrix profile ... source=command` and
`ok: client matrix passed for <profiles> profile(s) source=command` for external clients,
with profile pass markers matching the selected tools, selected bootstrap, and
exact version label plus command provenance, and
`<profiles>` exactly matching `ZMQ_CLIENT_MATRIX_PROFILES`; the external client matrix command must include
required profile, selected profile, required tool, required semantic, and
required sub-profile assignments matching the manifest environment; client profile,
probe, and security detail markers must appear before the final client matrix summary,
so post-summary profile blocks cannot satisfy release evidence; each selected
client profile pass marker must be unique before that final summary, so
contradictory bootstrap/tool evidence cannot be hidden by another passing line.
The client matrix summary must appear exactly once with `source=command` as its
own stripped line.
`8/8 tests passed` for the
MinIO/S3 integration gate; `ok: S3 process crash/replacement harness passed
(bucket=<bucket>, topic=<topic>, group=<group>, killed_broker=true,
fresh_data_dir=true, first_offset=0, committed_offset=1,
replacement_offset=<offset>, recovered_payloads=2) source=command` for process-crash
replacement; `ok: S3 provider live-suite profile ... command_started=true completed=true source=command`,
`ok: S3 provider profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command`, and
`ok: S3 provider matrix passed for <profiles> source=command` for the provider matrix, with
`<profiles>` exactly matching `ZMQ_S3_PROVIDER_PROFILES` and
the S3 provider matrix command including `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`
and `ZMQ_S3_PROVIDER_PROFILES` assignments matching the manifest environment;
`endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>`
matching the selected profile/global settings; provider profile, sub-profile,
and detail markers must appear before the final S3 provider matrix summary, and
each selected provider-settings profile marker must be unique before that final
summary so contradictory endpoint/bucket evidence cannot be hidden by another
passing line;
The S3 provider matrix summary must appear exactly once with `source=command` as
its own stripped line.
`ok: KRaft controller failover harness passed ... source=command`,
`network_partition=[<phases>]`, `old_leader_rejoined=true`,
`old_leader_fresh_rejoin=true`, `automq_old_leader_fresh_rejoin=true`,
`automq_stream_id=`, `automq_deleted_stream_id=`,
`automq_stream_set_object_id=`, `automq_node_id=`,
`automq_zone_router_epoch=`,
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
`share_fetch_session_checked=true`, `share_state_apis_checked=true`,
`reassignment_topic=<topic>`, `reassignment_target=<broker>`,
`reassignment_target_offset=<offset>`,
`reassignment_old_owner_rejected=true`, and
`reassignment_target_fetch_verified=true` for KRaft failover, with
`<phases>` exactly matching `ZMQ_KRAFT_NETWORK_MATRIX` and each selected
network phase emitting, before the KRaft failover summary line,
`ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command`;
and
`3-Node E2E Test Suite`, `[Test m] Cross-broker chaos phases`,
`[Test n] Live load/scale phases`, `ok: E2E chaos passed for <phases> phase(s) source=command`,
`ok: E2E load/scale passed for <phases> phase(s) source=command`, and `Results:` for Docker E2E.
The MinIO `8/8 tests passed` marker must appear exactly once as its own stripped
output line or on a successful Zig `Build Summary:` line; KRaft detail markers such as
`network_partition=[<phases>]`, `old_leader_rejoined=true`,
`old_leader_fresh_rejoin=true`, `automq_old_leader_fresh_rejoin=true`,
`automq_stream_id=`, `automq_deleted_stream_id=`,
`automq_stream_set_object_id=`, `automq_node_id=`,
`automq_zone_router_epoch=`,
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
`share_fetch_session_checked=true`, `share_state_apis_checked=true`,
`reassignment_topic=<topic>`, `reassignment_target=<broker>`,
`reassignment_target_offset=<offset>`,
`reassignment_old_owner_rejected=true`, and
`reassignment_target_fetch_verified=true` must appear on the
`ok: KRaft controller failover harness passed ... source=command` line. KRaft network
partition summary phases must list the selected `ZMQ_KRAFT_NETWORK_MATRIX`
values exactly, `controller_unsupported_cases` must include the generated-only
ZooKeeper-era min/max probes `4:0`, `4:7`, `5:0`, `5:4`, `6:0`, `6:8`,
`7:0`, `7:3` plus telemetry keys `71:0` and `72:0`,
`broker_non_broker_api_rejection_cases` must include controller-only/non-broker
broker-port probes `56:3`, `58:0`, `59:1`, `62:4`, `63:1`, `64:0`, `67:0`,
`70:0`, `80:0`, `81:0`, and `82:0`, and the KRaft failover command must include coverage selector
assignments for `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` and
`ZMQ_KRAFT_NETWORK_MATRIX` matching the manifest environment, not a placeholder,
empty result, or detached marker line.
The KRaft failover summary must appear exactly once with `source=command` as
its own stripped line.
Each selected KRaft network phase detail marker must appear before the KRaft
failover summary line so detached or stale phase output cannot satisfy the
network matrix.
Required broker-chaos, KRaft, and Docker E2E phase detail markers must be
unique per phase before their owning summary line, so repeated or contradictory
phase markers cannot hide stale failure output behind a later passing marker.
Final comma-separated output summaries for client profiles, S3 provider profiles,
KRaft network phases, Docker E2E phases, and broker chaos scenarios must not
contain blank, duplicate, or placeholder entries before matching the selected
matrix values.
Client profile `passed for <tools>` lists and client tool probe semantic lists
must follow the same strict output CSV rules, rejecting blank, duplicate, or
placeholder entries before they can satisfy selected tool or semantic coverage.
Profile-scoped client tool probe markers now require `source=command`, and those
markers plus required client security detail markers must be unique within the matching profile block, so repeated client
evidence cannot hide stale or contradictory semantic or security-negative
results behind a later passing marker.
KRaft AutoMQ metadata ids and the zone-router epoch in that same summary line
must parse as non-placeholder non-negative integers, and
`transactions_checked` must parse as exactly `5`; duplicate key fields on the
KRaft failover summary line and unknown summary fields are rejected so stale,
contradictory, or unchecked values cannot be hidden by a later marker.
The S3 process-crash summary marker must include non-placeholder bucket, topic,
and group values, `killed_broker=true`, `fresh_data_dir=true`,
`first_offset=0`, `committed_offset=1`, `recovered_payloads=2`, and a
`replacement_offset` greater than `first_offset`; duplicate summary key fields
and unknown summary fields are rejected. The S3 process-crash summary marker must appear exactly once with `source=command` as its own stripped line.
The live harness must preflight non-placeholder S3 endpoint, bucket, credential, scheme, region,
path-style, and TLS CA settings plus a positive integer port before starting
the broker.
S3 provider live-suite and sub-profile markers must appear in the same profile
block before the matching provider-settings profile marker.
Required S3 provider live-suite, sub-profile, and detail markers must be unique
within that profile block, so repeated provider evidence cannot hide stale or
contradictory endpoint, bucket, outage, crash-recovery, or multipart-fault
output behind a later passing marker.
Deterministic success markers use line-aware output marker matching: `ok: ...`,
`thresholds:`, `trend thresholds:`, and `result: pass` markers must appear as
stripped output lines or line prefixes rather than arbitrary substrings.
Captured skip markers are also line-aware: `skip: ...` markers must appear as
stripped output lines or line prefixes, and MinIO skipped-test summaries must
come from a Zig `Build Summary:` skip count rather than arbitrary substrings.
Docker E2E section markers are line-aware: `3-Node E2E Test Suite` must appear
on the suite title line, `[Test m] Cross-broker chaos phases`,
`[Test n] Live load/scale phases`, and `Results:` must appear as stripped
output lines or line prefixes. Docker E2E output line markers must appear exactly once.
The final Docker E2E result line must report `Results: <passed>/<total> passed, 0 failed`
with `<passed>` equal to `<total>` after the required E2E phase summaries, so earlier detached results output
cannot satisfy final completion evidence. The Docker E2E final results line must appear exactly once.
Docker E2E phase summaries must list the selected phases from
`ZMQ_E2E_CHAOS_MATRIX` and `ZMQ_E2E_LOAD_SCALE_MATRIX`, or the fixture-inferred
`ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` list when `ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1`.
Docker E2E phase summaries must appear exactly once with `source=command` as
their own stripped lines.
Each Docker E2E load/scale phase detail marker must also include
`marker_payloads=hook-owned`, `apply_source=<hook|fixture>`, and
`restore_source=<hook|fixture> source=command` matching the effective per-phase hook
selection; when either side uses the built-in fixture, the marker must include
the effective `action=<scale-in|scale-out|load|probe|noop>` value. When both
apply and restore are hook-owned, the marker must not report a fixture action.
Fixture-backed `action=load` markers must also report `load_records=<count>`
matching the effective `ZMQ_E2E_LOAD_SCALE[_<PHASE>]_FIXTURE_LOAD_RECORDS`
value so injected load volume cannot be omitted from release evidence.
The build static audit also pins the release-evidence E2E load/scale fixture action catalogue:
E2E_LOAD_SCALE_FIXTURE_ACTIONS entries must stay present in the release
criteria, parity notes, and production-readiness pins so built-in Docker E2E load/scale fixture actions
cannot drift from verifier preflight. The current fixture actions are
`scale-in`, `scale-out`, `load`, `probe`, and `noop`. Any fixture action change
must update the build static audit E2E-fixture-action catalogue.
Docker E2E chaos phase details must include `source=command` and appear before
the E2E chaos summary line, and Docker E2E load/scale phase details must appear
before the E2E load/scale summary line, so detached phase output cannot satisfy
phase coverage.
Local and live-S3 benchmark markers are also line-aware: `=== Benchmarks
complete ===`, `S3 WAL request volume`, `PartitionStore memory`, `Live S3 put`,
`Live S3 get`, and `Live S3 request volume` must appear as stripped output
lines or line prefixes. The completion marker itself must appear exactly once as
the exact stripped `=== Benchmarks complete ===` line for local and live-S3
benchmark gates. Detailed local and live-S3 benchmark markers must appear
before the `=== Benchmarks complete ===` marker, so stale benchmark output
appended after completion cannot satisfy release evidence. Each detailed local
and live-S3 benchmark marker must appear exactly once before completion, so
duplicate request-volume, memory, provider, put, or get lines cannot hide stale
measurements behind a later passing line.
The local benchmark summary must appear exactly once as its own stripped line,
and the live-S3 benchmark summary must appear exactly once as its own stripped line.
Comparative benchmark table markers are also line-aware and section-scoped:
target labels must appear on the `COMPARISON:` line before the gate, the
`Benchmark` marker must be a table header containing `Metric`, and each
benchmark row label must appear as the throughput (`tput`) row for that
benchmark rather than an arbitrary substring or detached post-gate line.
The comparative table must also include concrete `tput`, `p50`, and `p99`
metric rows before the gate with positive finite target measurements for every
required target on every benchmark row label. The comparison line, required
target labels, table header, and each benchmark metric row must appear exactly
once before the gate, so duplicate stale comparison tables cannot satisfy the
release contract.
For required matrix/profile coverage, evidence must include per-required
coverage markers, including
`ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=<positive> source=command`,
`ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command`,
`ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command`,
`ok: chaos s3-outage ... startup_fail_closed=true source=command` or
`ok: chaos s3-outage ... rejected=true error_code=<nonzero> base_offset_negative=true serving=true source=command`,
`ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command`
matching the selected `ZMQ_CHAOS_S3_*` or documented `ZMQ_S3_*` fallback
settings, with explicit blank `ZMQ_CHAOS_S3_*` provider settings failing closed,
`ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command`
for required live-S3 outage scenarios,
`ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`
before the chaos network-partition scenario marker,
broker chaos scenario detail markers before the broker chaos harness summary line
and unique per required scenario,
broker chaos harness summary must appear exactly once with `source=command` as
its own stripped line,
per-required client profile markers using the harness-owned line shape
`ok: client matrix profile <profile> passed for <tools> against <bootstrap> version=<version> source=command`
with `<tools>` matching the profile-selected tools and `<bootstrap>` matching
the selected valid bootstrap setting, and `<version>` matching the exact
version label for required versioned profiles; profile-scoped tool probe markers must appear
before the corresponding profile pass marker in the same profile block as the
matching passed-for tools/bootstrap/version/source line,
same-block client security detail markers for secured/OAuth profiles using
`ok: client security detail profile <profile> tool=<tool> protocol=<protocol> mechanism=<mechanism> oauth=<true|false> positive=true security_negative=<true|false> oauth_negative=<true|false> sasl_negative=<true|false> tls_negative=<true|false> acl_negative=<true|false> source=command`,
and all client profile, probe, and security detail markers must appear before
the final `ok: client matrix passed for <profiles> profile(s) source=command` summary so
post-summary profile blocks cannot satisfy profile evidence,
per-required client tool probe markers using `ok: <client> probes (<semantics>) source=command`,
such as `ok: kcat probes`, `ok: kafka CLI probes`,
`ok: kafka-python probes`, `ok: confluent-kafka probes`,
`ok: java-kafka probes`, and `ok: go-kafka probes`,
exact semantic tokens inside client probe marker
parentheses for every semantic named by `ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS`,
and those semantic tokens must appear on recognized required client-tool probe markers, specifically recognized profile-selected required client-tool probe markers for tools whose profile enabled that semantic, rather than arbitrary `ok: ... probes` lines or tools whose profile did not enable that semantic,
`ok: S3 provider live-suite profile ... command_started=true completed=true source=command`,
`ok: S3 provider outage detail profile ... endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> down=true healed=true fail_closed=true recovered=true source=command`,
`ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command`,
`ok: S3 provider process-crash detail profile ... bucket=<bucket> topic=<topic> group=<group> killed_broker=true fresh_data_dir=true first_offset=0 committed_offset=1 replacement_offset=<offset> recovered_payloads=2 source=command`,
`ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command`,
`ok: S3 provider list-pagination profile ... required=true completed=true source=command`,
`ok: S3 provider multipart-edge profile ... required=true completed=true source=command`,
`ok: S3 provider multipart-fault profile ... command_started=true completed=true injected=true recovered=true source=command`,
with required multipart-fault commands first emitting
`ok: S3 multipart fault profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> injected=true recovered=true source=command`
for the selected provider context; the provider matrix must preserve that
command-owned marker in the same provider block before the wrapper
multipart-fault marker, and all provider profile, sub-profile, and detail
markers must appear before the final `ok: S3 provider matrix passed for <profiles> source=command`
summary so post-summary provider blocks cannot satisfy provider evidence,
`ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command`,
`ok: E2E chaos phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command` before
the E2E chaos summary line, and
`ok: E2E load/scale phase ... applied=true restored=true marker_payloads=hook-owned apply_source=<hook|fixture> restore_source=<hook|fixture> source=command` before the E2E load/scale summary line, with `load_records=<count>` required on fixture `action=load` markers. The
`ok: chaos network-partition source=command`
scenario summary must appear as its own stripped line exactly once; per-phase
`ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command`
markers cannot satisfy the scenario summary, and detached phase markers after
that scenario marker cannot satisfy required network phase evidence.
The `ok: chaos harness passed for <scenarios> source=command` broker chaos
harness summary must list exactly the required scenarios from
`ZMQ_CHAOS_REQUIRED_SCENARIOS`, without extra unrequired scenario claims.
Benchmark evidence must include `=== Benchmarks complete ===`,
`ok: local benchmark gate source=command`, `S3 WAL request volume`, and
`PartitionStore memory` for the local benchmark, `=== Benchmarks complete ===`,
`ok: live-S3 benchmark gate source=command`, `Live S3 provider`, `Live S3 put`,
`Live S3 get`, and `Live S3 request volume` for the live-S3 benchmark, and
`COMPARATIVE BENCHMARK GATE` plus `result: pass` for the
comparative benchmark. The local benchmark request-volume marker must be
emitted as `S3 WAL request volume puts=<puts> lists=<lists>
requests/MiB=<value>`, and the memory marker must be emitted as
`PartitionStore memory <rate>/s retained=<retained> KiB peak=<peak> KiB
max_current=<max_current> KiB`. The live-S3 benchmark provider marker must be
emitted as
`Live S3 provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>`
and must match command/env-selected
`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` settings, with
`SCHEME` parsing as `http` or `https` and `PATH_STYLE` parsing as `true` or
`false`. The live-S3 benchmark command must include
`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}` assignments and the
manifest environment must record the same values. When the manifest records
`ZMQ_BENCH_LIVE_S3_{ITERATIONS,PAYLOAD_BYTES}`, the command must include those
matching positive-integer assignments too. The benchmark must preflight non-placeholder endpoint, bucket,
credential, region, path-style, TLS CA, and positive integer port settings
before touching the live provider; `ZMQ_RUN_BENCH_LIVE_S3` and
`ZMQ_S3_SKIP_ENSURE_BUCKET` must also parse as real booleans instead of
silently falling back on placeholder or arbitrary values. The
live-S3 benchmark put/get markers must be emitted as
`Live S3 put <MiB/s> MiB/s p99=<ms> ms objects=<objects>` and
`Live S3 get <MiB/s> MiB/s p99=<ms> ms requests/MiB=<value>`, and the
request-volume marker must be emitted as `Live S3 request volume puts=<puts>
gets=<gets> requests/MiB=<value>` so successful evidence includes concrete
throughput, p99, object-count, and request-count context. These detailed local
and live-S3 benchmark markers must appear before the benchmark completion
marker and must not repeat before completion. Comparative benchmark evidence must also include
`COMPARISON:`, the `Benchmark` table header, row labels for `ApiVersions`,
`Produce (reuse)`, `Produce (fresh)`, `Fetch`, and `Metadata`, the
`thresholds:` line, labels for every target named by
`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`, and `trend thresholds:` when
`ZMQ_BENCH_COMPARE_REQUIRE_TREND=1`, plus
`Results saved to benchmarks/results.json` after the gate result and an
`ok: comparative benchmark profile` marker after the artifact line. That
profile marker must include `selected=`, `required=`, `results_targets=`,
`results=benchmarks/results.json`, `gates_enforced=true`,
`trend_required=true`, `trend_baseline=`, `iterations=`, `warmup=`, and
`source=command`, with targets and trend baseline matching the release
environment and profile values matching the benchmark runner. The profile marker is a closed key=value schema:
every required field must appear exactly once,
fields must not be blank, and unknown fields are rejected. Comparative benchmark table evidence is
section-scoped: the comparison line, table header, target labels, and metric
rows must appear before the gate (`COMPARATIVE BENCHMARK GATE`) section rather
than as a detached post-gate line, and must not repeat inside that section.
The gate banner itself must appear exactly once as the exact stripped
`COMPARATIVE BENCHMARK GATE` line, so suffixed wrapper output cannot create a
second apparent gate marker.
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
It is an exact selected-target `COMPARISON:` line and must not carry suffixed
wrapper output.
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
The `thresholds:`,
`trend thresholds:`, and required `trend baseline:` lines must appear inside the
bounded `COMPARATIVE BENCHMARK GATE` section with the values selected by the gate
environment, and `result: pass` must be the gate section result rather than a
detached line elsewhere in the captured output. Those gate lines must also
appear exactly once.

## Known Unsupported Or Partial Surfaces

The following surfaces must remain documented and either non-advertised,
explicitly fail-closed, or become fully implemented and covered by the gates
above before an AutoMQ-complete release:

- ZooKeeper-era inter-broker API keys 4-7 are generated-only in KRaft mode:
  broker and controller ApiVersions omit them, neither port has a dispatch/no-op
  path for them, and direct broker/controller probes fail closed before body
  decode.
- Broader broker-only stateless replacement remains partial because local
  cache/state assumptions still exist outside the covered S3/quorum replay paths,
  including completed
  local failover of broker-owned reassignment state with replacement-side
  completion continuation, broker-only AutoMQ metadata replay including KV
  deletion, node tag clearing, group demotion, and
  generated GetKVs/GetNodes/ZoneRouter/DescribeLicense/GetNextNodeId/
  ExportClusterManifest read-back plus generated post-replay PutKVs/DeleteKVs,
  AutomqRegisterNode, AutomqZoneRouter, UpdateLicense, and AutomqUpdateGroup
  continuation, including initial and replacement-side generated read-back for
  KV deletion, node tag clearing, group demotion removals, and
  zone-router/license visibility, through a second fresh-dir S3 replay with generated
  GetKVs/GetNodes/ZoneRouter/DescribeLicense/GetNextNodeId/
  ExportClusterManifest continuation read-back, and broker-only AutoMQ
  stream/object metadata local restart generated DescribeStreams/
	  GetOpeningStreams/ExportClusterManifest/PrepareS3Object cursor read-back,
	  initial stream/object snapshot generated DescribeStreams/GetOpeningStreams/
	  ExportClusterManifest read-back,
	  replay including stream deletion, stream tag clearing, initial
	  mark-destroyed stream-object, stream-set, and stream-set mark-destroyed
	  generated DescribeStreams/ExportClusterManifest read-back, replacement-side
	  stream deletion/tag-clearing continuation with generated DescribeStreams,
	  GetOpeningStreams, and ExportClusterManifest read-back before and after
	  second fresh-dir replay, generated CreateStreams
	  cursor allocation, generated OpenStreams epoch/tag mutation,
  CommitStreamObject/CommitStreamSetObject continuation, generated TrimStreams
	  offset advancement, CloseStreams/DeleteStreams lifecycle continuation,
	  prepared object TTL replay/expiry with generated PrepareS3Object cursor
	  read-back, replacement-side expiry persistence, and second fresh-dir
	  continuation replay with generated cursor read-back, stream and stream-set
	  mark-destroyed object state/deletion readiness through repeated fresh-dir
	  replay with generated cursor read-back, stream-set object ranges, generated
	  GetOpeningStreams/DescribeStreams/ExportClusterManifest read-back for stream
  and stream-set replacement, stream/object rollback failure paths with generated
  DescribeStreams/GetOpeningStreams stream state/count read-back, including
  CommitStreamSetObject success-serialization and local snapshot failures, plus
  safe PrepareS3Object cursor read-back, KV/node/router/license/group rollback
  persistence-failure paths with generated GetKVs/AutomqGetNodes/
  AutomqZoneRouter/DescribeLicense/ExportClusterManifest read-back where
  non-mutating, plus mutation-materialization failure
  restoration checks with generated GetKVs/ExportClusterManifest/
  AutomqZoneRouter/DescribeLicense/GetNextNodeId read-back where safe, second
  fresh-dir replay of post-replay
  stream-object/stream-set continuation with generated cursor and DescribeStreams
  tag/offset read-back, and
  topic-partition DescribeStreams/AutomqGetPartitionSnapshot/Fetch read-back
  after initial post-snapshot S3 WAL refresh plus post-replay append
  continuation, generated
  DescribeStreams read-back for S3 WAL object rebuild, partition-offset repair,
  S3 WAL resume, legacy S3 WAL fallback rebuild, DeleteRecords trim/rollback
  recovery, and local
  partition-state restart/restore clamp/metadata-lookup paths, generated
	  AutomqGetPartitionSnapshot visibility for S3 WAL
	  object-refresh/partition-repair/resume replacement paths plus DeleteRecords
	  trim/rollback/no-mutation and local partition-state restart/restore paths,
	  generated ListOffsets visibility for S3 WAL
	  object-refresh/partition-repair/resume and local partition-state
	  restart/restore paths, DeleteRecords successful and failure no-mutation
	  DescribeStreams/ListOffsets and Fetch/no-record read-back, plus restart/replacement Fetch read-back, common
  topic-config
  replacement-side update continuation including generated
  AlterConfigs/IncrementalAlterConfigs set/update read-back, DeleteRecords
  low-watermark trim with generated ListOffsets and Fetch read-back
	  continuation plus DeleteTopics replacement-side tombstone continuation with
	  generated DescribeTopicPartitions unknown-topic and
	  ListPartitionReassignments empty-state read-back,
  AlterConfigs/IncrementalAlterConfigs replacement-side config-deletion
  continuation with generated DescribeConfigs read-back,
  local AlterPartitionReassignments restart read-back plus replacement-side
  active-state and cancellation with generated ListPartitionReassignments and
  DescribeTopicPartitions read-back plus local-failover-completion continuation
  with generated ListPartitionReassignments and DescribeTopicPartitions
  read-back, local AlterPartitionReassignments, CreateTopics/CreatePartitions
  manual-assignment acceptance, and auto-balancer/controller-aware rebalance
  paths with generated ListPartitionReassignments and DescribeTopicPartitions
  read-back,
	  CreateTopics/CreatePartitions replacement-side manual-assignment
	  continuation with generated DescribeTopicPartitions read-back plus
	  generated ListPartitionReassignments read-back,
  AssignReplicasToDirs
  replacement-side directory-assignment continuation with generated
  DescribeLogDirs read-back, CreateAcls
  replacement-side addition continuation with generated DescribeAcls read-back,
  DeleteAcls replacement-side tombstone continuation with generated DescribeAcls
  read-back, client-quota replacement-side addition/update and removal
  continuation with generated DescribeClientQuotas read-back, SCRAM
  replacement-side credential addition/update and deletion
  continuation with generated DescribeUserScramCredentials read-back,
  delegation-token replacement-side creation/renewal and expiry
  continuation with generated DescribeDelegationToken read-back, and
  finalized-feature replacement-side addition and deletion
  continuation with generated ApiVersions v3 read-back are covered by
  `__cluster_metadata` checkpoints; generated AutoMQ GetNextNodeId and
  PrepareS3Object allocator cursor advances are also replayed through repeated
  fresh-dir S3 replacement, but this does not complete the broader surface.
- The external client/security/OAuth live matrix, covering external-client,
  secured-client, and OAuth profile execution, is still release-CI-required.
- The cross-broker chaos live matrix, covering scheduled cross-broker chaos and
  broader multi-broker chaos, remains release-CI-required.
- The Docker E2E load/scale live orchestration surface remains release-CI-required.
- The KRaft failover network matrix surface remains release-CI-required for
  broader KRaft failover network matrices.
- The live S3 provider outage and multipart-fault profile execution surface,
  covering live provider outage and multipart-fault profile execution, remains
  release-CI-required.
- The comparative Kafka/AutoMQ performance profile/trend gates surface remains
  release-CI-required.

## Release Decision

A release decision must include the exact commit, command outputs for every
required gate, the required environment coverage variables, and structured
`unsupported_or_partial_surfaces` entries that account for every surface listed
in `Known Unsupported Or Partial Surfaces`. Each unsupported/partial surface
entry must be an object with non-empty `surface`, `status`, and `evidence` fields;
bare strings and placeholder values are rejected. Top-level manifest, command
entry, and unsupported-surface objects are closed schemas; unknown fields are
rejected instead of being carried as unvalidated release status. Optional
unsupported-surface accounting fields such as `id`, `mitigation`, and `notes`,
when present, must be non-empty strings or lists of non-empty strings;
Optional accounting lists must be non-empty, and placeholder optional accounting fields are rejected.
Each `surface` field must name the known surface it accounts for; evidence, mitigation, and notes cannot be the only matching fields. Each required surface must be covered by a distinct object; catch-all entries cannot satisfy multiple known surfaces. Duplicate objects for the same
known surface are rejected, and entries outside the verifier catalog are also
rejected so new release blockers cannot appear only in an assembled manifest.
The release-evidence self-test pins the top-level
Known Unsupported Or Partial Surfaces bullet list one-to-one against the
verifier required-surface catalog, so adding or removing a documented surface
requires verifier coverage updates; it also checks each bullet's status wording
against the verifier status class for that surface. Each `status` must explicitly
mark the surface as unsupported, partial, blocked, fail-closed/not-advertised,
or release-CI-required; vague completion-style statuses are rejected. The
status class must match the surface: ZooKeeper-era API accounting must mark
generated-only/not-advertised/fail-closed behavior, broker-only stateless
replacement must remain partial/blocked, and live CI matrix/performance
accounting, including each external-client, chaos, load/scale, failover,
provider, and performance surface, must remain release-CI-required or blocked
until those gates run. The
release evidence manifest is validated after assembly with `ZMQ_RELEASE_EVIDENCE`
pointing at the manifest and `zig build test-release-evidence --summary all`,
and it must be validated from the same clean tracked checkout named by the
manifest commit. The release evidence manifest must be strict JSON:
non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity` are
rejected before schema validation, and duplicate JSON object keys are rejected
before release accounting. The verifier
must fail if it cannot determine the current git commit or tracked worktree cleanliness.
The `ZMQ_RELEASE_EVIDENCE` manifest path must be a concrete non-placeholder
path.
The manifest must explicitly set `known_data_loss_bug=false`,
`advertised_stub_api=false`, `untriaged_durability_failure=false`, and
`automq_complete=false` while unsupported or partial surfaces remain; any known
data-loss bug, advertised stub API, untriaged durability failure, or premature
AutoMQ-complete claim blocks an AutoMQ-complete release. The `automq_complete`
flag is checked against the verifier catalog as well as the manifest surface
list, so eliding unsupported/partial surfaces cannot enable a complete claim.
These manifest fields must be JSON booleans rather than strings or placeholder
values.

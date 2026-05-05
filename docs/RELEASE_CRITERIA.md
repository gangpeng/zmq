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
  local recovery snapshots must fail closed when unreadable or malformed rather
  than being silently skipped, and S3 fetch/flush metadata faults must not be
  reported as empty successful reads or acknowledged writes. Object lifecycle
  cleanup must not remove metadata before preserving the S3 keys needed for
  delete or orphan retry, and compaction must checkpoint replacement
  ObjectManager metadata before deleting old S3 objects or else roll back or
  track uploaded replacements for retry cleanup. Coordinator offset
  enumerations and persisted offset restore must not silently skip malformed
  keys that belong to the requested group. Internal compacted-topic
  compaction must fail closed on cache allocation and malformed record-batch
  parser errors, and broker-owned internal log replay must reject malformed
  record-batch headers, truncated records, and trailing bytes instead of partial
  replay. Produce must reject full-size malformed Kafka record-batch headers and
  invalid batch-length envelopes before append, and idempotent Produce must
  reserve producer-sequence state before append so reservation failures do not
  acknowledge data with stale deduplication state. Producer-sequence recovery
  from durable user logs must reject malformed record-batch envelopes and must
  not let short raw records hide later idempotent batches. Topic creation must
  fail closed and roll back visible topic metadata when local partition-state
  allocation fails. Consumer-group timeout eviction must not silently keep
  expired members active because an allocation failed while collecting expired
  member IDs. Raft/controller metadata replication and startup recovery must not
  acknowledge or apply entries that failed local log allocation or persistence,
  and persisted Raft logs must reject truncated, invalid, or non-contiguous
  records instead of recovering a partial controller metadata image. Committed
  Raft voter config records must not be marked applied when endpoint metadata is
  malformed or config application fails. Raft snapshot compaction must not
  truncate log entries unless `snapshot.meta` and the prepared-object registry
  snapshot have both been persisted, and malformed Raft epoch/vote or snapshot
  metadata must fail startup recovery. Raft epoch/vote metadata writes must be
  persisted atomically before granting votes, starting elections, or accepting
  leader epochs; failures must deny or reject the transition instead of leaving
  externally visible quorum state ahead of durable `raft.meta`. Unreadable or
  malformed prepared-object registry snapshots must fail startup recovery rather
  than silently losing prepared-object tracking after log compaction.
- `Stateless`: a replacement broker can rebuild topic, partition, offset,
  transaction, producer, ACL, quota, SCRAM, reassignment, and AutoMQ stream/object
  metadata from quorum records and shared storage without manual repair.
- `MultiNode`: three-node controller and broker gates cover leader election,
  controller failover, broker fencing/unfencing, rolling restart, reassignment,
  scale in/out, and rack-aware/autobalancer convergence. Controller quorum voter
  configuration must fail closed on malformed entries instead of silently
  shrinking or miswiring the controller, metadata-client, or broker peer sets.
  Startup configuration must fail closed on unreadable config files, malformed
  cluster identity/listener integers, negative node IDs, invalid `process.roles`,
  missing CLI flag values, and unknown CLI arguments. Stale Raft peer cleanup
  must not depend on heap allocation while reconciling committed voter metadata.
  Auto-balancer planning must not execute partial reassignment plans when
  planner bookkeeping allocations fail.
- `Security`: TLS, mTLS, outbound TLS hostname verification, SASL/PLAIN,
  SCRAM, SCRAM delegation-token authentication, OAuthBearer, ACL authorization,
  and negative authentication/authorization cases pass for every advertised API
  shape.
- `Observability`: health/readiness, JSON logs, Prometheus metrics, Grafana
  panels, and alert rules cover the production SLOs and reference only
  registered metrics. Metrics scrape construction failures must return shaped
  5xx responses instead of silent connection closes.
- `Performance`: repeatable local and live-S3 benchmarks enforce produce/fetch
  throughput, p99 latency, S3 operations per MiB, recovery time, and bounded
  memory growth. Runtime elapsed-time gates must use monotonic clocks,
  including broker/controller heartbeat leases, consumer-group
  session/rebalance timeouts, quorum waits, and transaction timeouts, while
  Kafka-visible timestamps continue to use wall-clock time. Comparative trend
  gates must run against Kafka or AutoMQ baselines before release, enforce
  configured target coverage, and enforce configured throughput, latency, and
  error-rate regression thresholds.
- `Chaos`: SIGKILL, broker/controller restart, network partition, S3 outage,
  clock-skewed records, and slow/partial client scenarios must pass in the
  gated chaos suites.

## Required Commands

These commands define the minimum release gate set. Environment-specific gates
may require MinIO, S3-compatible providers, external Kafka clients, or network
fault-injection hooks.

```sh
/tmp/zig-aarch64-linux-0.16.0/zig build test --summary all
ZMQ_RUN_CHAOS_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-chaos --summary all
ZMQ_RUN_CLIENT_MATRIX=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-client-matrix --summary all
ZMQ_RUN_MINIO_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-minio --summary all
ZMQ_RUN_PROCESS_CRASH_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-s3-process-crash --summary all
ZMQ_RUN_S3_PROVIDER_MATRIX=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-s3-provider-matrix --summary all
ZMQ_RUN_KRAFT_FAILOVER_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-kraft-failover --summary all
ZMQ_RUN_E2E_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-e2e --summary all
/tmp/zig-aarch64-linux-0.16.0/zig build bench --summary all
ZMQ_RUN_BENCH_LIVE_S3=1 /tmp/zig-aarch64-linux-0.16.0/zig build bench --summary all
ZMQ_RUN_BENCH_COMPARE=1 /tmp/zig-aarch64-linux-0.16.0/zig build bench-compare --summary all
```

Release CI must set required coverage variables for environment-specific
matrices, including `ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` for scheduled
controller/broker partition phases, `ZMQ_CHAOS_REQUIRED_NETWORK_PHASES` for
broker chaos partitions, `ZMQ_E2E_REQUIRED_CHAOS_PHASES` for Docker
cross-broker chaos phases, `ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` for live Docker
scale-in/scale-out/load phases, and provider/client/profile requirement
variables for S3 and external-client matrices. S3 provider coverage must pin
provider, outage, process-crash/replacement, ListObjectsV2 pagination,
multipart-edge, and multipart-fault profiles with
`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES`,
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES`, and
`ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES`. External-client coverage
must pin required client implementations and semantic suites with
`ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS` and
`ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS`, exact version-labeled profiles with
`ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES`, and secured-client plus
negative security coverage with `ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES` and
`ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES`; OAuth-secured client
coverage must also be pinned with `ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES`
and `ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES` when OAUTHBEARER is
part of the release target. Benchmark jobs can tighten local and live-S3
thresholds with `ZMQ_BENCH_S3_WAL_MAX_REQUESTS_PER_MIB`,
`ZMQ_BENCH_S3_WAL_MAX_REBUILD_MS`,
`ZMQ_BENCH_LIVE_S3_MAX_REQUESTS_PER_MIB`,
`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`,
`ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO`,
`ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO`,
`ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO`, and
`ZMQ_BENCH_COMPARE_MAX_ERROR_RATE`.

## Known Unsupported Or Partial Surfaces

The following surfaces must remain documented and either non-advertised,
explicitly fail-closed, or become fully implemented and covered by the gates
above before an AutoMQ-complete release:

- ZooKeeper-era inter-broker API keys 4-7 are generated-only in KRaft mode:
  ApiVersions omits them, the broker has no dispatch/no-op path for them, and
  direct probes fail closed before body decode.
- Broader broker-only stateless replacement still has local cache/state
  assumptions outside the covered S3/quorum replay paths.
- CI execution of required scheduled cross-broker chaos, E2E load/scale, KRaft
  failover matrices, scheduled live provider outage profiles, and comparative Kafka/AutoMQ performance
  trend gates are still release blockers.

## Release Decision

A release decision must include the exact commit, command outputs for every
required gate, and a list of any intentionally unsupported features. Any known
data-loss bug, advertised stub API, or untriaged durability failure blocks an
AutoMQ-complete release.

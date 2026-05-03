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
  S3/MinIO rebuild, and provider fault injection without data loss.
- `Stateless`: a replacement broker can rebuild topic, partition, offset,
  transaction, producer, ACL, quota, SCRAM, reassignment, and AutoMQ stream/object
  metadata from quorum records and shared storage without manual repair.
- `MultiNode`: three-node controller and broker gates cover leader election,
  controller failover, broker fencing/unfencing, rolling restart, reassignment,
  scale in/out, and rack-aware/autobalancer convergence.
- `Security`: TLS, mTLS, SASL/PLAIN, SCRAM, OAuthBearer, ACL authorization, and
  negative authentication/authorization cases pass for every advertised API
  shape.
- `Observability`: health/readiness, JSON logs, Prometheus metrics, Grafana
  panels, and alert rules cover the production SLOs and reference only
  registered metrics.
- `Performance`: repeatable local and live-S3 benchmarks report produce/fetch
  throughput, p99 latency, S3 operations per MiB, recovery time, and bounded
  memory growth. Comparative trend gates must run against Kafka or AutoMQ
  baselines before release.
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
/tmp/zig-aarch64-linux-0.16.0/zig build bench --summary all
ZMQ_RUN_BENCH_LIVE_S3=1 /tmp/zig-aarch64-linux-0.16.0/zig build bench --summary all
ZMQ_RUN_BENCH_COMPARE=1 /tmp/zig-aarch64-linux-0.16.0/zig build bench-compare --summary all
```

## Known Unsupported Or Partial Surfaces

The following surfaces must remain documented and non-advertised, or become
fully implemented and covered by the gates above, before an AutoMQ-complete
release:

- KIP-848 `ConsumerGroupHeartbeat` remains non-advertised until full epoch,
  assignment revocation, and coordinator migration semantics are implemented.
- Share-group APIs remain non-advertised until durable share-group coordinator
  and share-session data-plane semantics are complete.
- `AssignReplicasToDirs` remains non-advertised until controller-backed JBOD
  directory semantics are complete.
- Legacy inter-broker APIs remain non-advertised until real controller-backed
  inter-broker behavior replaces single-node/no-op compatibility handlers.
- Broader broker-only stateless replacement still has local cache/state
  assumptions outside the covered S3/quorum replay paths.
- Broader live load/scale orchestration, cross-broker chaos, live provider
  outage runs, and comparative Kafka/AutoMQ performance trend gates are still
  release blockers.

## Release Decision

A release decision must include the exact commit, command outputs for every
required gate, and a list of any intentionally unsupported features. Any known
data-loss bug, advertised stub API, or untriaged durability failure blocks an
AutoMQ-complete release.

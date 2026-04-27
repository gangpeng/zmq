# ZMQ

An experimental Zig implementation inspired by [AutoMQ](https://github.com/AutoMQ/automq): a cloud-native, S3-backed Kafka-compatible broker prototype with a small runtime footprint.

```
  ┌─────────────────────────────────────────────┐
  │            ZMQ Broker v0.8.0                 │
  │   Cloud-native Kafka, rewritten in Zig      │
  └─────────────────────────────────────────────┘
```

## What Is This?

[AutoMQ](https://github.com/AutoMQ/automq) is an open-source streaming platform that reimagines Apache Kafka for the cloud. It replaces Kafka's traditional local-disk storage with **S3-compatible object storage** (AWS S3, MinIO, etc.), turning brokers into stateless, elastically scalable nodes.

ZMQ is a work-in-progress implementation of AutoMQ-style storage and Kafka protocol handling in Zig. It is useful for development, protocol experiments, and local MinIO-backed clusters, but it is not yet a production-equivalent replacement for AutoMQ.

- **~38,000 lines of AI-generated Zig** (plus ~33,000 lines of auto-generated protocol structs) across 303 source files
- **No JVM** — a single Zig broker binary with no Java classpath or GC
- **Sub-second startup** — broker is ready to serve in milliseconds
- **S3/MinIO-backed WAL and object storage path** — with sync durability as the default S3 WAL mode
- **Kafka wire protocol coverage for common broker APIs** — compatibility is partial and should be validated against your client/version mix

## Status And Gaps

This repository intentionally does not claim full AutoMQ parity. Major remaining gaps include production-grade controller quorum behavior, complete Kafka protocol semantics across every API/version, full AutoMQ stream/object lifecycle compatibility, mature rebalance/failover behavior, broad S3 provider coverage, and exhaustive performance/chaos validation.

Current S3 support covers HTTP and HTTPS endpoints, AWS SigV4 signing, single-part and multipart uploads, object reads, deletes, ranges, and listing. HTTPS uses runtime-loaded OpenSSL and system CA paths or an optional CA file; verify this in your target container/base image.

The tracked parity plan and acceptance matrix live in `docs/AUTOMQ_PARITY.md`.

## Architecture

```
                ┌──────────────────────────────────────────────────────┐
                │                   Kafka Clients                      │
                │            (any language, any version)               │
                └────────────────────┬─────────────────────────────────┘
                                     │ Kafka Wire Protocol (41 APIs)
                ┌────────────────────▼─────────────────────────────────┐
                │               Network Layer                          │
                │         io_uring / epoll + TLS                       │
                ├──────────────────────────────────────────────────────┤
                │                Broker Layer                          │
                │    Request Handlers · Purgatory · Partition Mgmt     │
                ├────────────────┬─────────────────────────────────────┤
                │  KRaft / Raft  │          Storage Engine             │
                │  Consensus     │   WAL → LogCache → S3BlockCache     │
                │  (Elections,   │              │                       │
                │   Metadata)    │              ▼                       │
                └────────────────┘        ┌──────────┐                 │
                                          │ S3 / MinIO│                 │
                                          └──────────┘                 │
                └──────────────────────────────────────────────────────┘
```

### Module Map

| Module | Description |
|--------|-------------|
| `core` | Byte buffers, varint encoding, CRC32c, UUID, config, logging |
| `allocators` | Arena, pool, slab, and tracking memory allocators |
| `concurrency` | Future, ConcurrentMap, Channel, Scheduler |
| `compression` | Gzip, LZ4, Zstd, Snappy codec wrappers |
| `protocol` | Code-generated Kafka message types (230+ message schemas), headers, record batches |
| `network` | io_uring/epoll TCP server, TLS, connection management |
| `storage` | Write-ahead log (WAL), LogCache, S3BlockCache, S3 object format with CRC32C checksums, S3 object lifecycle (prepared→committed→mark_destroyed), data expiration |
| `raft` | KRaft state machine, leader election, log replication |
| `broker` | Kafka API request handlers, delayed operation purgatory, partition management |
| `security` | SASL authentication (PLAIN, SCRAM-SHA-256, OAUTHBEARER), TLS with hostname/cert validation, mTLS, ACL authorization |
| `streams` | Kafka Streams-compatible processing primitives |
| `connect` | Kafka Connect-compatible connector framework |
| `config` | Configuration file parser (Kafka-style `key=value` properties) |
| `tools` | CLI admin tools |
| `coordinator` | Group coordinator and transaction coordinator |
| `metrics` | Prometheus-compatible metrics (45+ metric types), structured JSON logging, /health and /ready probes |

### Multi-Tier Storage

Data flows through four tiers, balancing latency and cost:

1. **WAL (Write-Ahead Log)** — local disk, sub-millisecond appends
2. **LogCache** — in-memory hot data for recent reads
3. **S3BlockCache** — local cache of S3 blocks for warm reads
4. **S3 Object Storage** — durable, cheap, infinite capacity

## Prerequisites

- **Zig 0.16.0** — [Install Zig](https://ziglang.org/download/)
- **Docker & Docker Compose** — for MinIO and multi-broker clusters
- **Python 3** — only for running E2E tests and protocol codegen (optional)

## Building

```bash
# Debug build
zig build

# Optimized release build
zig build -Doptimize=ReleaseFast

# Or use the Makefile shortcuts
make build          # debug
make release        # release
```

The broker binary is output to `./zig-out/bin/zmq`.

## Running a Single Broker

```bash
# In-memory storage (no persistence, good for quick testing)
./zig-out/bin/zmq

# With local WAL persistence
./zig-out/bin/zmq 9092 --data-dir /tmp/zmq-data

# With MinIO S3 backend
./zig-out/bin/zmq 9092 \
  --data-dir /tmp/zmq-data \
  --s3-endpoint 127.0.0.1 \
  --s3-port 9000 \
  --s3-bucket automq

# With a configuration file
./zig-out/bin/zmq 9092 --config config/server.properties
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `[port]` | `9092` | Kafka listener port (positional argument) |
| `--data-dir` | *(none — in-memory)* | WAL and metadata directory |
| `--s3-endpoint` | *(none)* | S3/MinIO hostname, optionally with `http://` or `https://` |
| `--s3-port` | `9000` | S3/MinIO port |
| `--s3-bucket` | `automq` | S3 bucket name |
| `--s3-access-key` | `minioadmin` | S3 access key (env vars also supported) |
| `--s3-secret-key` | `minioadmin` | S3 secret key (env vars also supported) |
| `--s3-ca-file` | *(none)* | CA bundle/path for HTTPS S3 endpoints |
| `--metrics-port` | `9090` | Prometheus metrics & health endpoint port |
| `--node-id` | `0` | Broker node ID |
| `--cluster-id` | `automq-cluster` | Cluster identifier |
| `--voters` | *(none — single-node)* | KRaft voter list: `0@host1:9092,1@host2:9093,...` |
| `--advertised-host` | `localhost` | Hostname advertised to clients |
| `--workers` | `4` | Number of network I/O worker threads |
| `--config` | *(none)* | Path to `server.properties` config file |
| `--s3-wal-batch-size` | `4194304` (4 MB) | WAL batch size before S3 flush |
| `--s3-wal-flush-interval` | `250` | WAL flush interval in ms |
| `--s3-wal-flush-mode` | `sync` | WAL flush mode (`sync`, `async`, or `group_commit`; `sync` is the durable default) |
| `--cache-max-size` | `268435456` (256 MB) | LogCache max size |
| `--s3-block-cache-size` | `67108864` (64 MB) | S3 block cache size |
| `--compaction-interval` | `300000` (5 min) | S3 compaction interval in ms |

## Deploying a Multi-Broker Cluster Locally (with MinIO)

The included `docker-compose.yml` spins up a **3-broker cluster** with **MinIO** as the S3 backend — everything you need for local development and testing.

### Quick Start

```bash
# Build the Docker image and start the cluster
docker-compose up -d

# Verify all services are running
docker-compose ps

# Watch broker logs
docker-compose logs -f broker1 broker2 broker3

# Stop and clean up
docker-compose down -v
```

Or use the Makefile:

```bash
make docker-up      # start cluster
make docker-logs    # tail logs
make docker-down    # stop and clean up
```

### What Gets Started

| Service | Container | Ports | Description |
|---------|-----------|-------|-------------|
| **MinIO** | `zmq-minio` | `9000` (S3 API), `9001` (Console) | S3-compatible object storage |
| **minio-init** | *(one-shot)* | — | Creates the `automq` bucket |
| **Broker 1** | `zmq-broker-1` | `9092` (Kafka), `9090` (metrics) | Node 0 |
| **Broker 2** | `zmq-broker-2` | `9093` (Kafka), `9091` (metrics) | Node 1 |
| **Broker 3** | `zmq-broker-3` | `9094` (Kafka), `9095` (metrics) | Node 2 |

### Cluster Topology

```
                        ┌─────────────────────────────────────┐
                        │           MinIO (S3)                │
                        │   http://localhost:9000              │
                        │   Console: http://localhost:9001     │
                        │   User: minioadmin / minioadmin      │
                        │   Bucket: automq                    │
                        └───────┬──────────┬──────────┬───────┘
                                │          │          │
                    ┌───────────▼──┐ ┌─────▼──────┐ ┌─▼───────────┐
                    │  Broker 1    │ │  Broker 2   │ │  Broker 3   │
                    │  node-id: 0  │ │  node-id: 1 │ │  node-id: 2 │
                    │  :9092       │ │  :9093      │ │  :9094      │
                    │  :9090 (met) │ │  :9091 (met)│ │  :9095 (met)│
                    └──────────────┘ └─────────────┘ └─────────────┘
```

### Connecting with Kafka Clients

Once the cluster is up, connect with any standard Kafka client:

```bash
# Using kcat (formerly kafkacat)
echo "hello zmq" | kcat -P -b localhost:9092 -t test-topic
kcat -C -b localhost:9092 -t test-topic -e

# Using the official Kafka CLI tools
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic --partitions 3
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

### Accessing MinIO Console

Open [http://localhost:9001](http://localhost:9001) in your browser to view the MinIO console and inspect the `automq` bucket contents.

- **Username:** `minioadmin`
- **Password:** `minioadmin`

### Health & Metrics

Each broker exposes HTTP endpoints for monitoring:

```bash
# Health check
curl http://localhost:9090/health

# Readiness probe
curl http://localhost:9090/ready

# Prometheus metrics
curl http://localhost:9090/metrics
```

### Running Without Docker (Manual Multi-Broker)

You can also run a cluster directly on your host (requires 3 terminal windows):

```bash
# Build first
zig build

# Terminal 1 — Broker 0
./zig-out/bin/zmq 9092 --node-id 0 \
  --voters 0@localhost:9092,1@localhost:9093,2@localhost:9094 \
  --data-dir /tmp/zmq-node0

# Terminal 2 — Broker 1
./zig-out/bin/zmq 9093 --node-id 1 \
  --voters 0@localhost:9092,1@localhost:9093,2@localhost:9094 \
  --data-dir /tmp/zmq-node1

# Terminal 3 — Broker 2
./zig-out/bin/zmq 9094 --node-id 2 \
  --voters 0@localhost:9092,1@localhost:9093,2@localhost:9094 \
  --data-dir /tmp/zmq-node2
```

## Testing

```bash
# Run all unit tests
zig build test --summary all
# or
make test

# Run E2E integration tests against a running Docker cluster
make e2e

# Run gated S3 broker-process crash/replacement harness against MinIO
ZMQ_RUN_PROCESS_CRASH_TESTS=1 make s3-crash

# Run performance benchmarks
make bench
```

The E2E test suite (`tests/e2e_test.py`) and cluster validation test (`tests/cluster_validation_test.py`) cover:

- Leader election verification
- Produce 1,000 messages across 3 partitions
- Fetch all messages and verify count
- Consumer group lifecycle (JoinGroup, SyncGroup, Heartbeat)
- Offset commit and fetch
- Broker failure and recovery
- Topic creation and deletion
- Metadata consistency across brokers

## Production Readiness

ZMQ includes production-grade features across five dimensions. All code is AI-generated using Claude.

### Security
- **TLS**: Hostname verification (SSL_set1_host), certificate chain validation, expiry checking, 30s handshake timeout
- **mTLS**: Client certificate principal extraction from subject DN for ACL authorization
- **SASL/PLAIN**: Password authentication with PBKDF2-hashed credential storage
- **SCRAM-SHA-256**: Full RFC 5802 multi-round authentication exchange
- **OAUTHBEARER**: JWT claim extraction (issuer, audience, expiry validation)
- **ACL Authorization**: Kafka-compliant model (allow/deny, resource types, prefix matching, super-users)

### Data Durability
- **CRC32C checksums** on all S3 objects (v2 format) with backward-compatible v1 reading
- **S3 GET retry** with exponential backoff (100ms, 200ms, 400ms)
- **ObjectManager metadata persistence** via binary snapshot (survives broker restart)
- **Graceful shutdown**: SIGTERM drains connections (30s), flushes WAL to S3, takes Raft snapshot

### Data Expiration (AutoMQ parity — all 5 layers)
- **Layer 1**: Automatic time-based and size-based retention enforcement (every 60s)
- **Layer 2**: Compaction cleanup of expired S3 objects (every 5 min)
- **Layer 3**: S3 object lifecycle state machine (prepared → committed → mark_destroyed), 10-minute consumer-safe deletion buffer, 60-minute prepared object TTL
- **Layer 4**: WAL segment cleanup after S3 upload confirmation
- **Layer 5**: Dual-buffer prepared object registry for KRaft snapshot safety

### Observability
- **45 Prometheus metric types** at `/metrics` (request latency, error rates, consumer lag, Raft consensus, compaction, cache hit/miss)
- **Per-API error counters** (`kafka_server_api_errors_total{api,error_code}`)
- **Consumer lag gauge** (`kafka_consumer_lag{group,topic,partition}`)
- **Health/Ready probes**: `/health` (liveness), `/ready` (503 until startup completes)
- **Structured JSON logging** (NDJSON format with ISO 8601 timestamps and correlation IDs)

### High Availability
- **Graceful shutdown**: SIGTERM → drain connections → flush WAL → Raft snapshot → exit
- **Double-signal force quit**: Second SIGTERM kills immediately
- **Request rejection during shutdown**: Returns NOT_LEADER_OR_FOLLOWER (error 6) to redirect clients

## Configuration File

The broker supports Kafka-style `key=value` properties files. See [`config/server.properties`](config/server.properties) for a full reference with all available options. CLI flags always take precedence over config file values.

```bash
./zig-out/bin/zmq 9092 --config config/server.properties
```

## Project Structure

```
.
├── build.zig               # Zig build system — 8 modules + executable
├── Dockerfile              # Multi-stage Docker build (Zig 0.13.0 → slim runtime)
├── docker-compose.yml      # 3-broker cluster + MinIO
├── Makefile                # Developer shortcuts
├── config/
│   └── server.properties   # Reference configuration file
├── src/
│   ├── main.zig            # Entry point — CLI parsing, server bootstrap
│   ├── core/               # Byte buffers, varint, CRC32c, UUID
│   ├── allocators/         # Arena, pool, slab memory allocators
│   ├── concurrency/        # Future, ConcurrentMap, Channel, Scheduler
│   ├── compression/        # Gzip, LZ4, Zstd, Snappy codecs
│   ├── protocol/           # Kafka wire protocol (230+ message schemas)
│   │   ├── generated/      # Auto-generated request/response structs
│   │   ├── schemas/        # JSON API schemas (input to codegen)
│   │   └── codegen/        # Python code generator
│   ├── network/            # TCP server (io_uring/epoll), TLS, metrics server
│   ├── storage/            # WAL, LogCache, S3BlockCache, S3 client
│   ├── raft/               # KRaft consensus, election, replication
│   ├── broker/             # Request handlers, purgatory, partitions
│   ├── security/           # SASL (PLAIN, SCRAM-SHA-256, OAUTHBEARER), TLS, ACL, JWT
│   ├── coordinator/        # Group & transaction coordinators
│   ├── streams/            # Stream processing primitives
│   ├── connect/            # Connector framework
│   ├── metrics/            # Prometheus metrics
│   ├── config/             # Config file parser
│   └── tools/              # Admin CLI tools
├── tests/
│   ├── e2e_test.py         # 9-scenario E2E integration test suite
│   ├── cluster_validation_test.py # 16-phase cluster validation (wire protocol, stress, observability)
│   └── production_readiness_test.zig # Cross-cutting integration tests
├── benchmarks/
│   ├── main.zig            # Performance benchmarks
│   └── benchmark_compare.py# Java vs Zig comparison tool
├── KeyDesign.md            # Detailed design document
└── LICENSE                 # Apache License 2.0
```

## Useful Makefile Targets

```
  build           Build the broker binary (debug)
  release         Build optimized release binary
  test            Run all unit tests
  run             Run the broker on port 9092
  run-s3          Run with MinIO S3 backend
  run-cluster     Run 3-node cluster (requires 3 terminals)
  e2e             Run E2E tests with MinIO (requires Docker)
  s3-crash        Run gated S3 broker process crash/restart test (requires MinIO)
  bench           Run performance benchmarks
  docker          Build Docker image
  docker-up       Start 3-broker cluster with MinIO
  docker-down     Stop the cluster
  docker-logs     View broker logs
  codegen         Regenerate protocol structs from JSON schemas
  fmt             Format all Zig source files
  clean           Clean build artifacts
  loc             Count lines of code
  status          Show project status
```

## License

This project is licensed under the [Apache License 2.0](LICENSE).

## Acknowledgments

- [AutoMQ](https://github.com/AutoMQ/automq) — the original Java implementation this project is based on
- [Apache Kafka](https://kafka.apache.org/) — the protocol and API specification
- [Zig](https://ziglang.org/) — the programming language

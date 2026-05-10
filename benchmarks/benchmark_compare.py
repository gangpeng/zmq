#!/usr/bin/env python3
"""
Benchmark comparison: ZMQ vs Apache Kafka vs AutoMQ

Runs the same Kafka wire-protocol benchmarks against all three systems using
Docker Compose, then prints a side-by-side comparison table.

All systems use:
  - 3-node cluster (combined controller+broker mode)
  - Broker exposed on host port 19092
  - Same Kafka protocol requests, same iteration counts

Storage backends:
  - ZMQ:          MinIO S3
  - Apache Kafka: Local disk (vanilla KRaft, no S3)
  - AutoMQ:       MinIO S3

Usage:
  # Full 3-way comparison (manages Docker lifecycle automatically):
  python3 benchmarks/benchmark_compare.py

  # Run only against a single target (cluster must already be up on port 19092):
  python3 benchmarks/benchmark_compare.py --target zmq
  python3 benchmarks/benchmark_compare.py --target kafka
  python3 benchmarks/benchmark_compare.py --target automq

  # Run a subset of targets:
  python3 benchmarks/benchmark_compare.py --target zmq,kafka
  python3 benchmarks/benchmark_compare.py --target zmq,automq

  # Release trend gate against a previous benchmarks/results.json artifact:
  ZMQ_BENCH_COMPARE_REQUIRED_TARGETS=zmq,kafka,automq \
  ZMQ_BENCH_COMPARE_REQUIRE_TREND=1 \
  ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json \
  python3 benchmarks/benchmark_compare.py --require-enabled
"""

import socket
import struct
import time
import sys
import subprocess
import os
import json
import argparse
import tempfile
import math
import io
import contextlib

BROKER_PORT = 19092
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZMQ_COMPOSE = os.path.join(PROJECT_DIR, "docker-compose.yml")
KAFKA_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "kafka-compose.yml")
AUTOMQ_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "automq-compose.yml")
RESULTS_ARTIFACT = os.path.join("benchmarks", "results.json")
RESULTS_FILE = os.path.join(PROJECT_DIR, RESULTS_ARTIFACT)

# All supported targets in display order
ALL_TARGETS = ["zmq", "kafka", "automq"]
TARGET_LABELS = {
    "zmq": "ZMQ (Zig)",
    "kafka": "Apache Kafka",
    "automq": "AutoMQ (Java)",
}
TARGET_SHORT_LABELS = {
    "zmq": "ZMQ",
    "kafka": "Kafka",
    "automq": "AutoMQ",
}
TARGET_COMPOSE = {
    "zmq": ZMQ_COMPOSE,
    "kafka": KAFKA_COMPOSE,
    "automq": AUTOMQ_COMPOSE,
}

# Benchmark parameters — identical for all systems
ITERATIONS = {
    "api_versions": 5000,
    "produce_single": 5000,
    "produce_fresh": 2000,
    "fetch": 3000,
    "metadata": 3000,
}
WARMUP = {
    "api_versions": 100,
    "produce_single": 100,
    "produce_fresh": 50,
    "fetch": 100,
    "metadata": 100,
}

BENCHMARKS = [
    ("api_versions", "ApiVersions"),
    ("produce_single", "Produce (reuse)"),
    ("produce_fresh", "Produce (fresh)"),
    ("fetch", "Fetch"),
    ("metadata", "Metadata"),
]

DEFAULT_GATE_THRESHOLDS = {
    "min_throughput_ratio": 0.05,
    "max_p50_latency_ratio": 20.0,
    "max_p99_latency_ratio": 20.0,
    "max_error_rate": 0.0,
    "min_trend_throughput_ratio": 0.90,
    "max_trend_p50_latency_ratio": 1.25,
    "max_trend_p99_latency_ratio": 1.25,
}

PLACEHOLDER_ENV_VALUES = {"...", "placeholder", "required", "tbd", "todo"}
BOOL_TRUE_VALUES = {"1", "true", "yes", "on"}
BOOL_FALSE_VALUES = {"0", "false", "no", "off"}

def placeholder_env_value(value):
    stripped = str(value or "").strip()
    lowered = stripped.lower()
    angle_start = stripped.find("<")
    has_angle_placeholder = (
        angle_start >= 0
        and stripped.find(">", angle_start + 1) > angle_start + 1
    )
    return (
        lowered in PLACEHOLDER_ENV_VALUES
        or lowered.startswith("/path/to/")
        or has_angle_placeholder
    )

def parse_targets(raw):
    """Parse a benchmark target argument into the display-order target list."""
    raw = (raw or "").strip()
    if raw == "all":
        return list(ALL_TARGETS)

    return parse_target_list(raw, "benchmark target list")

def parse_target_list(raw, context):
    blank_target = False
    targets = []
    for item in raw.split(","):
        target = item.strip()
        if not target:
            blank_target = True
            continue
        targets.append(target)
    if not targets:
        raise ValueError(f"{context} must contain at least one target")
    if blank_target:
        raise ValueError(f"{context} must not contain blank target values")
    placeholders = [target for target in targets if placeholder_env_value(target)]
    if placeholders:
        raise ValueError(
            f"{context} must not use placeholder target values: "
            + ", ".join(placeholders)
        )
    duplicates = sorted(
        target
        for target in set(targets)
        if targets.count(target) > 1
    )
    if duplicates:
        raise ValueError(
            f"{context} must not contain duplicate target values: "
            + ", ".join(duplicates)
        )
    for target in targets:
        if target not in ALL_TARGETS:
            raise ValueError(f"Unknown target '{target}'. Valid targets: {', '.join(ALL_TARGETS)}")
    return targets

def validate_required_release_targets(targets):
    if "zmq" not in targets:
        raise ValueError("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS must include zmq")
    if not any(target in targets for target in ("kafka", "automq")):
        raise ValueError(
            "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS must include kafka or automq"
        )

def required_targets_from_env(require_release_targets=False):
    raw = os.environ.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")
    if raw is None:
        if require_release_targets:
            raise ValueError(
                "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS must include zmq plus "
                "kafka or automq when comparative benchmark gates are enforced"
            )
        return []
    if not str(raw).strip():
        raise ValueError("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS must not be blank")
    targets = parse_target_list(raw, "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")
    if require_release_targets:
        validate_required_release_targets(targets)
    return targets

def missing_required_targets(selected_targets, required_targets):
    selected = set(selected_targets)
    return [target for target in required_targets if target not in selected]

# ── Kafka wire protocol helpers ──
# Uses lowest versions compatible with all three targets:
#   - Produce v0: supported by ZMQ (0-11), Kafka 4.2 (0-13), AutoMQ (0-11)
#   - Fetch v4:   Kafka 4.2 dropped v0-v3 (min=4); ZMQ (0-17), AutoMQ (0-17)
#   - CreateTopics v2: Kafka 4.2 dropped v0-v1 (min=2); ZMQ (0-7), AutoMQ (0-7)
#   - Metadata v1: all support it
#   - ApiVersions v0: all support it

def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(min(65536, n - len(buf)))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf

def kafka_request_reuse(sock, api_key, api_version, corr_id, body=b''):
    client_id = b'bench'
    hdr = struct.pack('>hhih', api_key, api_version, corr_id, len(client_id)) + client_id
    frame = struct.pack('>I', len(hdr + body)) + hdr + body
    sock.sendall(frame)
    size_buf = _recv_exact(sock, 4)
    sz = struct.unpack('>I', size_buf)[0]
    return _recv_exact(sock, sz)

def kafka_request_fresh(port, api_key, api_version, corr_id, body=b''):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect(('127.0.0.1', port))
    resp = kafka_request_reuse(s, api_key, api_version, corr_id, body)
    s.close()
    return resp

def create_topic(sock, corr_id, name, partitions=3):
    """CreateTopics v2 — compatible with Kafka 4.2+ (which dropped v0-v1)."""
    name_b = name.encode()
    # num_topics=1
    body = struct.pack('>i', 1)
    # topic name (string16)
    body += struct.pack('>h', len(name_b)) + name_b
    # num_partitions, replication_factor
    body += struct.pack('>ih', partitions, 1)
    # num_assignments=0 (empty array, non-nullable in v2)
    body += struct.pack('>i', 0)
    # num_configs=0
    body += struct.pack('>i', 0)
    # timeout_ms=30000, validate_only=false
    body += struct.pack('>i', 30000) + struct.pack('>?', False)
    kafka_request_reuse(sock, 19, 2, corr_id, body)

def current_time_ms():
    """Kafka-visible record timestamps use wall-clock epoch milliseconds."""
    return int(time.time() * 1000)

def produce_body(topic, partition, msg):
    """Produce v3 body with RecordBatch v2 format.

    Produce v3+ is required because Kafka 4.2 rejects v0-v2 despite reporting
    them as supported in ApiVersions.  v3 adds transactional_id (set to null).
    The record payload uses RecordBatch (magic=2) with CRC32C.
    """
    topic_b = topic.encode()
    if isinstance(msg, str):
        msg = msg.encode()

    # ── Build a single Record (inside the batch) ──
    record = bytearray()
    record.append(0)            # attributes
    record.append(0)            # timestampDelta (varint 0)
    record.append(0)            # offsetDelta (varint 0)
    record.append(0x01)         # keyLength = -1 zigzag-varint
    # (no key bytes)
    _encode_varint_into(record, len(msg))  # valueLength
    record.extend(msg)
    record.append(0)            # headersCount (varint 0)

    record_with_len = bytearray()
    _encode_varint_into(record_with_len, len(record))
    record_with_len.extend(record)

    # ── Build RecordBatch header (after baseOffset + batchLength) ──
    now_ms = current_time_ms()
    batch_body = bytearray()
    batch_body.extend(struct.pack('>i', 0))     # partitionLeaderEpoch
    batch_body.append(2)                         # magic = 2 (RecordBatch)
    # CRC placeholder — 4 bytes, filled below
    crc_offset = len(batch_body)
    batch_body.extend(b'\x00\x00\x00\x00')
    # Everything after CRC is included in the checksum
    crc_start = len(batch_body)
    batch_body.extend(struct.pack('>h', 0))     # attributes
    batch_body.extend(struct.pack('>i', 0))     # lastOffsetDelta
    batch_body.extend(struct.pack('>q', now_ms)) # firstTimestamp
    batch_body.extend(struct.pack('>q', now_ms)) # maxTimestamp
    batch_body.extend(struct.pack('>q', -1))    # producerId
    batch_body.extend(struct.pack('>h', -1))    # producerEpoch
    batch_body.extend(struct.pack('>i', -1))    # baseSequence
    batch_body.extend(struct.pack('>i', 1))     # numRecords
    batch_body.extend(record_with_len)

    # Compute CRC32C over everything after the CRC field
    crc = _crc32c(bytes(batch_body[crc_start:]))
    struct.pack_into('>I', batch_body, crc_offset, crc)

    # Full record set: baseOffset(8) + batchLength(4) + batch_body
    records = struct.pack('>q', 0) + struct.pack('>i', len(batch_body)) + bytes(batch_body)

    # ── Produce v3 request body ──
    body = struct.pack('>h', -1)                 # transactionalId = null
    body += struct.pack('>hi', -1, 30000)        # acks=-1, timeout=30s
    body += struct.pack('>i', 1)                 # num_topics
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>i', 1)                 # num_partitions
    body += struct.pack('>i', partition)
    body += struct.pack('>i', len(records)) + records
    return body

# Produce version (v3 is minimum that works with Kafka 4.2)
PRODUCE_VERSION = 3

def _encode_varint_into(buf, value):
    """Encode a signed int as zigzag varint, appending to buf."""
    # Zigzag encode
    value = (value << 1) ^ (value >> 31)
    while value & ~0x7F:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value & 0x7F)

def _crc32c(data):
    """Compute CRC-32C (Castagnoli). Uses crcmod if available, else pure Python."""
    try:
        import crcmod
        fn = crcmod.predefined.mkCrcFun('crc-32c')
        return fn(data) & 0xFFFFFFFF
    except ImportError:
        pass
    # Pure-Python fallback (slow but correct)
    crc = 0xFFFFFFFF
    poly = 0x82F63B78
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
    return crc ^ 0xFFFFFFFF

def fetch_body(topic, partition, offset, max_bytes=1048576):
    """Fetch v4 body — compatible with Kafka 4.2+ (which dropped v0-v3)."""
    topic_b = topic.encode()
    # replica_id=-1, max_wait_ms=100, min_bytes=1, max_bytes
    body = struct.pack('>iiii', -1, 100, 1, max_bytes)
    # isolation_level=0 (READ_UNCOMMITTED) — added in v4
    body += struct.pack('>b', 0)
    # num_topics=1
    body += struct.pack('>i', 1)
    # topic name (string16)
    body += struct.pack('>h', len(topic_b)) + topic_b
    # num_partitions=1
    body += struct.pack('>i', 1)
    # partition, fetch_offset, partition_max_bytes
    body += struct.pack('>iqi', partition, offset, max_bytes)
    return body

# Fetch version used in benchmarks (v4 is minimum for Kafka 4.2 compat)
FETCH_VERSION = 4

# ── Benchmark runner ──

def bench(name, fn, iterations, warmup=100):
    """Run warmup, then measure iterations. Returns dict with throughput + latencies."""
    for i in range(warmup):
        try:
            fn(i)
        except Exception:
            pass

    latencies = []
    errors = 0
    t0 = time.monotonic()
    for i in range(iterations):
        start = time.monotonic()
        try:
            fn(warmup + i)
            latencies.append(time.monotonic() - start)
        except Exception:
            errors += 1
    elapsed = time.monotonic() - t0

    if not latencies:
        print(f"    {name}: FAILED ({errors} errors)")
        return {
            "throughput": 0,
            "p50": 0,
            "p99": 0,
            "p999": 0,
            "errors": errors,
            "successes": 0,
            "requests": iterations,
        }

    throughput = len(latencies) / elapsed
    latencies_ms = sorted([l * 1000 for l in latencies])
    p50 = latencies_ms[len(latencies_ms) // 2]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]
    p999 = latencies_ms[int(len(latencies_ms) * 0.999)]

    suffix = f"  ({errors} errors)" if errors else ""
    print(f"    {name}: {throughput:,.0f} req/s  p50={p50:.2f}ms  p99={p99:.2f}ms{suffix}")
    return {
        "throughput": throughput,
        "p50": p50,
        "p99": p99,
        "p999": p999,
        "errors": errors,
        "successes": len(latencies),
        "requests": iterations,
    }

def wait_for_broker(port, timeout=120):
    """Wait until the broker responds to ApiVersions."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            s = socket.socket()
            s.settimeout(3)
            s.connect(('127.0.0.1', port))
            kafka_request_reuse(s, 18, 0, 1)
            s.close()
            return True
        except Exception:
            time.sleep(1)
    return False

def run_benchmarks(label):
    """Run the full benchmark suite. Returns results dict or None on failure."""
    storage = "local disk" if "Kafka" in label else "MinIO S3"
    print(f"\n{'=' * 60}")
    print(f"  {label} Benchmark — 3-Node Cluster + {storage}")
    print(f"{'=' * 60}")

    print(f"  Waiting for broker on port {BROKER_PORT}...", end="", flush=True)
    if not wait_for_broker(BROKER_PORT):
        print(f" FAILED")
        return None
    print(f" OK")

    # Create topic (ignore errors — may already exist)
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))
    try:
        create_topic(sock, 1, "bench-topic", 3)
    except Exception:
        pass
    sock.close()
    time.sleep(1)

    results = {}

    # 1. ApiVersions
    print(f"\n  [1/5] ApiVersions (connection reuse, {ITERATIONS['api_versions']} iters)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))
    def api_versions_fn(i):
        kafka_request_reuse(sock, 18, 0, i)
    results["api_versions"] = bench("ApiVersions", api_versions_fn,
                                     ITERATIONS["api_versions"], WARMUP["api_versions"])
    sock.close()

    # 2. Produce (connection reuse)
    print(f"\n  [2/5] Produce — single msg, conn reuse ({ITERATIONS['produce_single']} iters)")
    sock = socket.socket()
    sock.settimeout(30)
    sock.connect(('127.0.0.1', BROKER_PORT))
    def produce_fn(i):
        body = produce_body("bench-topic", i % 3, f"msg-{i:08d}")
        kafka_request_reuse(sock, 0, PRODUCE_VERSION, i, body)
    results["produce_single"] = bench("Produce (reuse)", produce_fn,
                                       ITERATIONS["produce_single"], WARMUP["produce_single"])
    sock.close()

    # 3. Produce (fresh connection)
    print(f"\n  [3/5] Produce — single msg, fresh conn ({ITERATIONS['produce_fresh']} iters)")
    def produce_fresh_fn(i):
        body = produce_body("bench-topic", i % 3, f"fresh-{i:08d}")
        kafka_request_fresh(BROKER_PORT, 0, PRODUCE_VERSION, i, body)
    results["produce_fresh"] = bench("Produce (fresh)", produce_fresh_fn,
                                      ITERATIONS["produce_fresh"], WARMUP["produce_fresh"])

    # 4. Fetch
    print(f"\n  [4/5] Fetch — conn reuse ({ITERATIONS['fetch']} iters)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))
    def fetch_fn(i):
        body = fetch_body("bench-topic", i % 3, 0)
        kafka_request_reuse(sock, 1, FETCH_VERSION, i, body)
    results["fetch"] = bench("Fetch", fetch_fn,
                              ITERATIONS["fetch"], WARMUP["fetch"])
    sock.close()

    # 5. Metadata
    print(f"\n  [5/5] Metadata — conn reuse ({ITERATIONS['metadata']} iters)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))
    def metadata_fn(i):
        body = struct.pack('>i', -1)
        kafka_request_reuse(sock, 3, 1, i, body)
    results["metadata"] = bench("Metadata", metadata_fn,
                                 ITERATIONS["metadata"], WARMUP["metadata"])
    sock.close()

    return results

def compose_up(compose_file, label, wait_secs=10):
    """Start a Docker Compose cluster."""
    print(f"\n>>> Starting {label} cluster...")
    r = subprocess.run(
        ["docker", "compose", "-f", compose_file, "up", "-d"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    if r.returncode != 0:
        print(f"  docker compose up failed: {r.stderr[:300]}")
        return False
    print(f"  Containers started, waiting {wait_secs}s for initialization...")
    time.sleep(wait_secs)
    return True

def compose_down(compose_file, label):
    """Stop and clean a Docker Compose cluster."""
    print(f"\n>>> Stopping {label} cluster...")
    subprocess.run(
        ["docker", "compose", "-f", compose_file, "down", "-v"],
        capture_output=True, cwd=PROJECT_DIR
    )

def _ratio_str(val_a, val_b, higher_is_better=True):
    """Format a ratio with arrow marker. Returns (ratio_text, marker)."""
    if val_b == 0:
        return "   inf  ", "▲" if higher_is_better else "▼"
    ratio = val_a / val_b
    if higher_is_better:
        marker = "▲" if ratio >= 1.0 else "▼"
    else:
        marker = "▲" if ratio <= 1.0 else "▼"
    return f"{ratio:>6.2f}x", marker

def env_float(name, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    stripped = str(raw).strip()
    if not stripped:
        raise ValueError(f"{name} must not be blank")
    if placeholder_env_value(stripped):
        raise ValueError(f"{name} must not use a placeholder value")
    try:
        value = float(stripped)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {stripped!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {stripped!r}")
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value

def env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    stripped = str(raw).strip()
    if not stripped:
        raise ValueError(f"{name} must not be blank")
    lowered = stripped.lower()
    if placeholder_env_value(stripped):
        raise ValueError(f"{name} must not use a placeholder value")
    if lowered in BOOL_TRUE_VALUES:
        return True
    if lowered in BOOL_FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be true or false")

def comparison_gate_thresholds():
    return {
        "min_throughput_ratio": env_float(
            "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO",
            DEFAULT_GATE_THRESHOLDS["min_throughput_ratio"],
        ),
        "max_p50_latency_ratio": env_float(
            "ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO",
            DEFAULT_GATE_THRESHOLDS["max_p50_latency_ratio"],
        ),
        "max_p99_latency_ratio": env_float(
            "ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO",
            DEFAULT_GATE_THRESHOLDS["max_p99_latency_ratio"],
        ),
        "max_error_rate": env_float(
            "ZMQ_BENCH_COMPARE_MAX_ERROR_RATE",
            DEFAULT_GATE_THRESHOLDS["max_error_rate"],
        ),
        "min_trend_throughput_ratio": env_float(
            "ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO",
            DEFAULT_GATE_THRESHOLDS["min_trend_throughput_ratio"],
        ),
        "max_trend_p50_latency_ratio": env_float(
            "ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO",
            DEFAULT_GATE_THRESHOLDS["max_trend_p50_latency_ratio"],
        ),
        "max_trend_p99_latency_ratio": env_float(
            "ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO",
            DEFAULT_GATE_THRESHOLDS["max_trend_p99_latency_ratio"],
        ),
    }

def non_negative_count(result, target_label, benchmark_label, field_name):
    value = result.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        return None, (
            f"{target_label} {benchmark_label} {field_name} "
            "is missing or non-integral"
        )
    if value < 0:
        return None, f"{target_label} {benchmark_label} {field_name} is negative"
    return value, None

def result_error_rate(result, target_label, benchmark_label):
    failures = []
    errors, failure = non_negative_count(result, target_label, benchmark_label, "errors")
    if failure:
        failures.append(failure)
        errors = None
    requests, failure = non_negative_count(result, target_label, benchmark_label, "requests")
    if failure:
        failures.append(failure)
        requests = None
    successes, failure = non_negative_count(result, target_label, benchmark_label, "successes")
    if failure:
        failures.append(failure)
        successes = None

    if failures:
        return None, failures

    denominator = requests if requests > 0 else errors + successes
    if denominator <= 0:
        return None, [
            f"{target_label} {benchmark_label} request count is zero; "
            "error-rate comparison is invalid"
        ]
    return errors / denominator, []

def numeric_metric(result, target_label, benchmark_label, metric_name, source_label):
    value = result.get(metric_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, (
            f"{source_label} {target_label} {benchmark_label} {metric_name} "
            "is missing or non-numeric"
        )
    try:
        metric = float(value)
    except OverflowError:
        return None, (
            f"{source_label} {target_label} {benchmark_label} {metric_name} "
            "is non-finite"
        )
    if not math.isfinite(metric):
        return None, (
            f"{source_label} {target_label} {benchmark_label} {metric_name} "
            "is non-finite"
        )
    if metric < 0:
        return None, (
            f"{source_label} {target_label} {benchmark_label} {metric_name} "
            "is negative"
        )
    return metric, None

def trend_baseline_required():
    return env_bool("ZMQ_BENCH_COMPARE_REQUIRE_TREND", False)

def trend_baseline_path_from_env():
    return os.environ.get("ZMQ_BENCH_COMPARE_TREND_BASELINE", "").strip()

def project_path(path):
    if os.path.isabs(path):
        return os.path.realpath(path)
    return os.path.realpath(os.path.join(PROJECT_DIR, path))

def trend_baseline_points_at_current_results(path):
    return project_path(path) == os.path.realpath(RESULTS_FILE)

def trend_baseline_uses_placeholder(path):
    return placeholder_env_value(path)

def reject_nonstandard_json_constant(value):
    raise ValueError(f"non-standard JSON constant {value!r} is not allowed in strict JSON")

def reject_duplicate_json_object_keys(pairs):
    parsed = {}
    for key, value in pairs:
        if key in parsed:
            raise ValueError(f"duplicate JSON object key {key!r} is not allowed in strict JSON")
        parsed[key] = value
    return parsed

def validate_trend_baseline_artifact_metadata(baseline):
    metadata = baseline.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE must contain benchmark artifact "
            "metadata"
        )
    artifact_results = artifact_payload_result_map(
        baseline,
        "ZMQ_BENCH_COMPARE_TREND_BASELINE",
    )
    validate_artifact_metadata(
        metadata,
        artifact_results,
        "ZMQ_BENCH_COMPARE_TREND_BASELINE benchmark artifact",
        require_zmq=True,
    )

def artifact_payload_result_map(payload, context):
    if not isinstance(payload, dict):
        raise ValueError(f"{context} must contain a JSON object")
    allowed_metadata_keys = {"metadata", "timestamp"}
    unknown_keys = sorted(
        key
        for key in payload
        if key not in allowed_metadata_keys and key not in ALL_TARGETS
    )
    if unknown_keys:
        raise ValueError(
            f"{context} contains unknown top-level artifact keys: "
            + ", ".join(unknown_keys)
        )

    artifact_results = {
        target: payload[target]
        for target in ALL_TARGETS
        if target in payload
    }
    validate_artifact_results_map(artifact_results)
    return artifact_results

def validate_artifact_metadata(metadata, artifact_results, context, require_zmq=False):
    if not isinstance(metadata, dict):
        raise ValueError(f"{context} metadata must be an object")
    if metadata.get("schema_version") != 1:
        raise ValueError(f"{context} metadata schema_version must be 1")

    targets_with_results = artifact_target_list(
        metadata.get("targets_with_results"),
        f"{context} metadata targets_with_results",
    )
    expected_targets = [
        target for target in ALL_TARGETS if target in artifact_results
    ]
    if require_zmq and "zmq" not in targets_with_results:
        raise ValueError(
            f"{context} metadata targets_with_results must include zmq"
        )
    if targets_with_results != expected_targets:
        raise ValueError(
            f"{context} metadata targets_with_results must match result "
            "targets: expected "
            + ", ".join(expected_targets)
            + "; got "
            + ", ".join(targets_with_results)
        )

    selected_targets = artifact_target_list(
        metadata.get("selected_targets"),
        f"{context} metadata selected_targets",
    )
    required_targets = artifact_target_list(
        metadata.get("required_targets"),
        f"{context} metadata required_targets",
    )
    missing_selected_results = [
        target for target in targets_with_results if target not in selected_targets
    ]
    if missing_selected_results:
        raise ValueError(
            f"{context} metadata selected_targets must include result targets: "
            + ", ".join(missing_selected_results)
        )
    required_not_selected = [
        target for target in required_targets if target not in selected_targets
    ]
    if required_not_selected:
        raise ValueError(
            f"{context} metadata required_targets must be selected targets: "
            + ", ".join(required_not_selected)
        )

    validate_artifact_target_labels(
        metadata.get("target_labels"),
        f"{context} metadata target_labels",
    )
    validate_artifact_profile_int_map(
        metadata.get("iterations"),
        ITERATIONS,
        f"{context} metadata iterations",
    )
    validate_artifact_profile_int_map(
        metadata.get("warmup"),
        WARMUP,
        f"{context} metadata warmup",
    )
    validate_artifact_thresholds(
        metadata.get("thresholds"),
        f"{context} metadata thresholds",
    )
    validate_artifact_bool_metadata(
        metadata.get("gates_enforced"),
        f"{context} metadata gates_enforced",
    )
    trend_required = validate_artifact_bool_metadata(
        metadata.get("trend_required"),
        f"{context} metadata trend_required",
    )
    validate_artifact_trend_baseline_metadata(
        metadata.get("trend_baseline"),
        trend_required,
        f"{context} metadata trend_baseline",
    )

def load_trend_baseline_from_env(require_trend=False):
    path = trend_baseline_path_from_env()
    if not path:
        if require_trend:
            raise ValueError(
                "ZMQ_BENCH_COMPARE_REQUIRE_TREND=1 requires "
                "ZMQ_BENCH_COMPARE_TREND_BASELINE"
            )
        return None

    if trend_baseline_uses_placeholder(path):
        raise ValueError(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE must not use a placeholder path"
        )

    if trend_baseline_points_at_current_results(path):
        raise ValueError(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE must point at a prior benchmark "
            "artifact, not the current benchmarks/results.json output"
        )

    resolved_path = project_path(path)
    try:
        with open(resolved_path) as f:
            baseline = json.load(
                f,
                parse_constant=reject_nonstandard_json_constant,
                object_pairs_hook=reject_duplicate_json_object_keys,
            )
    except OSError as exc:
        raise ValueError(
            f"could not read ZMQ_BENCH_COMPARE_TREND_BASELINE {path!r} "
            f"(resolved to {resolved_path!r}): {exc}"
        ) from exc
    except ValueError as exc:
        raise ValueError(
            f"invalid strict JSON in ZMQ_BENCH_COMPARE_TREND_BASELINE {path!r}: {exc}"
        ) from exc

    if not isinstance(baseline, dict):
        raise ValueError("ZMQ_BENCH_COMPARE_TREND_BASELINE must contain a JSON object")
    validate_trend_baseline_artifact_metadata(baseline)
    return baseline

def artifact_target_list(values, context):
    if values is None:
        return []
    if isinstance(values, str) or not isinstance(values, (list, tuple)):
        raise ValueError(f"{context} must be a list of benchmark targets")

    blank_target = False
    targets = []
    for item in values:
        if not isinstance(item, str):
            raise ValueError(f"{context} target values must be strings")
        target = item.strip()
        if not target:
            blank_target = True
            continue
        targets.append(target)

    if blank_target:
        raise ValueError(f"{context} must not contain blank target values")
    placeholders = [target for target in targets if placeholder_env_value(target)]
    if placeholders:
        raise ValueError(
            f"{context} must not use placeholder target values: "
            + ", ".join(placeholders)
        )
    duplicates = sorted(
        target
        for target in set(targets)
        if targets.count(target) > 1
    )
    if duplicates:
        raise ValueError(
            f"{context} must not contain duplicate target values: "
            + ", ".join(duplicates)
        )
    unknown = [target for target in targets if target not in ALL_TARGETS]
    if unknown:
        raise ValueError(
            f"{context} contains unknown target values: "
            + ", ".join(unknown)
        )
    return targets

def validate_artifact_target_labels(labels, context):
    if not isinstance(labels, dict):
        raise ValueError(f"{context} must be an object")
    unknown = sorted(set(labels) - set(ALL_TARGETS))
    if unknown:
        raise ValueError(f"{context} contains unknown targets: " + ", ".join(unknown))
    missing = [target for target in ALL_TARGETS if target not in labels]
    if missing:
        raise ValueError(f"{context} missing targets: " + ", ".join(missing))
    for target in ALL_TARGETS:
        label = labels.get(target)
        if not isinstance(label, str):
            raise ValueError(f"{context} {target} must be a string")
        if label != TARGET_LABELS[target]:
            raise ValueError(
                f"{context} {target} must be {TARGET_LABELS[target]!r}; "
                f"got {label!r}"
            )

def validate_artifact_profile_int_map(values, expected, context):
    if not isinstance(values, dict):
        raise ValueError(f"{context} must be an object")
    unknown = sorted(set(values) - set(expected))
    if unknown:
        raise ValueError(f"{context} contains unknown keys: " + ", ".join(unknown))
    missing = [key for key in expected if key not in values]
    if missing:
        raise ValueError(f"{context} missing keys: " + ", ".join(missing))
    for key, expected_value in expected.items():
        value = values.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{context} {key} must be an integer")
        if value != expected_value:
            raise ValueError(
                f"{context} {key} must match the benchmark profile: "
                f"expected {expected_value}; got {value}"
            )

def validate_artifact_thresholds(thresholds, context):
    if not isinstance(thresholds, dict):
        raise ValueError(f"{context} must be an object")
    unknown = sorted(set(thresholds) - set(DEFAULT_GATE_THRESHOLDS))
    if unknown:
        raise ValueError(f"{context} contains unknown keys: " + ", ".join(unknown))
    missing = [key for key in DEFAULT_GATE_THRESHOLDS if key not in thresholds]
    if missing:
        raise ValueError(f"{context} missing keys: " + ", ".join(missing))

    validated = {}
    for key in DEFAULT_GATE_THRESHOLDS:
        value = thresholds.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{context} {key} must be numeric")
        metric = float(value)
        if not math.isfinite(metric):
            raise ValueError(f"{context} {key} must be finite")
        if metric < 0:
            raise ValueError(f"{context} {key} must be non-negative")
        validated[key] = metric
    return validated

def validate_artifact_bool_metadata(value, context):
    if not isinstance(value, bool):
        raise ValueError(f"{context} must be a boolean")
    return value

def validate_artifact_trend_baseline_metadata(value, trend_required, context):
    if value is None:
        if trend_required:
            raise ValueError(f"{context} is required when trend_required is true")
        return
    if not isinstance(value, str):
        raise ValueError(f"{context} must be a string or null")
    if not value.strip():
        raise ValueError(f"{context} must not be blank")
    if placeholder_env_value(value):
        raise ValueError(f"{context} must not use a placeholder path")
    if trend_baseline_points_at_current_results(value):
        raise ValueError(
            f"{context} must point at a prior benchmark artifact, not "
            "benchmarks/results.json"
        )

def validate_artifact_results_map(all_results):
    if not isinstance(all_results, dict):
        raise ValueError("benchmark result artifact results must be an object")
    benchmark_keys = {key for key, _ in BENCHMARKS}
    for target, result in all_results.items():
        if not isinstance(target, str):
            raise ValueError("benchmark result artifact target names must be strings")
        if target not in ALL_TARGETS:
            raise ValueError(
                f"benchmark result artifact contains unknown target {target!r}"
            )
        if not isinstance(result, dict):
            raise ValueError(
                f"benchmark result artifact target {target!r} result must be an object"
            )
        unknown_benchmarks = sorted(set(result) - benchmark_keys)
        if unknown_benchmarks:
            raise ValueError(
                f"benchmark result artifact target {target!r} contains unknown "
                "benchmark result keys: "
                + ", ".join(unknown_benchmarks)
            )
        for key, label in BENCHMARKS:
            benchmark_result = result.get(key)
            if benchmark_result is None:
                raise ValueError(
                    f"benchmark result artifact target {target!r} missing "
                    f"{label} result"
                )
            failures = benchmark_result_object_failures(
                benchmark_result,
                TARGET_LABELS[target],
                label,
                "benchmark result artifact",
            )
            if failures:
                raise ValueError("; ".join(failures))


def benchmark_result_object_failures(result, target_label, benchmark_label, source_label):
    if not isinstance(result, dict):
        return [f"{source_label} {target_label} {benchmark_label} result must be an object"]

    failures = []
    _, rate_failures = result_error_rate(
        result,
        f"{source_label} {target_label}",
        benchmark_label,
    )
    failures.extend(rate_failures)
    for metric in ("throughput", "p50", "p99"):
        value, failure = numeric_metric(
            result,
            target_label,
            benchmark_label,
            metric,
            source_label,
        )
        if failure:
            failures.append(failure)
            continue
        if value <= 0:
            failures.append(
                f"{source_label} {target_label} {benchmark_label} {metric} is zero"
            )
    return failures

def evaluate_trend_gates(all_results, trend_baseline, thresholds=None):
    """Return release-gate failures for current ZMQ results versus prior ZMQ trend data."""
    if trend_baseline is None:
        return []

    thresholds = thresholds or comparison_gate_thresholds()
    current_zmq = all_results.get("zmq")
    if not isinstance(current_zmq, dict):
        return ["ZMQ result is required for comparative benchmark trend gates"]

    baseline_zmq = trend_baseline.get("zmq")
    if not isinstance(baseline_zmq, dict):
        return ["trend baseline missing ZMQ results"]

    min_throughput_ratio = thresholds.get(
        "min_trend_throughput_ratio",
        DEFAULT_GATE_THRESHOLDS["min_trend_throughput_ratio"],
    )
    max_p50_ratio = thresholds.get(
        "max_trend_p50_latency_ratio",
        DEFAULT_GATE_THRESHOLDS["max_trend_p50_latency_ratio"],
    )
    max_p99_ratio = thresholds.get(
        "max_trend_p99_latency_ratio",
        DEFAULT_GATE_THRESHOLDS["max_trend_p99_latency_ratio"],
    )

    failures = []
    for key, label in BENCHMARKS:
        current = current_zmq.get(key)
        baseline = baseline_zmq.get(key)
        if not isinstance(current, dict):
            failures.append(f"ZMQ missing {label} result for trend gate")
            continue
        if not isinstance(baseline, dict):
            failures.append(f"trend baseline missing {label} ZMQ result")
            continue

        baseline_throughput, failure = numeric_metric(
            baseline,
            "ZMQ",
            label,
            "throughput",
            "trend baseline",
        )
        if failure:
            failures.append(failure)
            baseline_throughput = None
        current_throughput, failure = numeric_metric(
            current,
            "ZMQ",
            label,
            "throughput",
            "current result",
        )
        if failure:
            failures.append(failure)
            current_throughput = None

        if baseline_throughput is None or current_throughput is None:
            pass
        elif current_throughput <= 0:
            failures.append(f"ZMQ {label} throughput is zero; trend comparison is invalid")
        elif baseline_throughput <= 0:
            failures.append(f"trend baseline {label} ZMQ throughput is zero; trend comparison is invalid")
        else:
            throughput_ratio = current_throughput / baseline_throughput
            if throughput_ratio < min_throughput_ratio:
                failures.append(
                    f"{label} ZMQ trend throughput ratio {throughput_ratio:.2f}x below "
                    f"{min_throughput_ratio:.2f}x"
                )

        for metric, max_ratio in [
            ("p50", max_p50_ratio),
            ("p99", max_p99_ratio),
        ]:
            baseline_latency, failure = numeric_metric(
                baseline,
                "ZMQ",
                label,
                metric,
                "trend baseline",
            )
            if failure:
                failures.append(failure)
                baseline_latency = None
            current_latency, failure = numeric_metric(
                current,
                "ZMQ",
                label,
                metric,
                "current result",
            )
            if failure:
                failures.append(failure)
                current_latency = None

            if baseline_latency is None or current_latency is None:
                continue
            if baseline_latency <= 0:
                failures.append(f"trend baseline {label} ZMQ {metric} is zero; trend comparison is invalid")
                continue
            if current_latency <= 0:
                failures.append(f"ZMQ {label} {metric} is zero; trend comparison is invalid")
                continue
            latency_ratio = current_latency / baseline_latency
            if latency_ratio > max_ratio:
                failures.append(
                    f"{label} ZMQ trend {metric} latency ratio {latency_ratio:.2f}x exceeds "
                    f"{max_ratio:.2f}x"
                )

    return failures

def evaluate_comparison_gates(
    all_results,
    thresholds=None,
    require_baseline=False,
    required_targets=None,
    selected_targets=None,
    trend_baseline=None,
):
    """Return release-gate failures for ZMQ-vs-baseline comparative results."""
    thresholds = thresholds or comparison_gate_thresholds()
    required_targets = required_targets or []
    selected_targets = selected_targets or []
    failures = []
    trend_failures = evaluate_trend_gates(all_results, trend_baseline, thresholds)

    missing_selected = set()
    for target in selected_targets:
        if target not in all_results:
            missing_selected.add(target)
            failures.append(f"selected benchmark target {TARGET_LABELS[target]} did not produce results")

    for target in required_targets:
        if target not in all_results and target not in missing_selected:
            failures.append(f"required benchmark target {TARGET_LABELS[target]} did not produce results")

    malformed_targets = [
        target
        for target in ALL_TARGETS
        if target in all_results and not isinstance(all_results[target], dict)
    ]
    for target in malformed_targets:
        failures.append(f"{TARGET_LABELS[target]} benchmark result must be an object")

    if "zmq" not in all_results or not isinstance(all_results.get("zmq"), dict):
        if require_baseline:
            failures.append("ZMQ result is required for comparative benchmark gates")
        return failures + trend_failures

    baselines = [
        target
        for target in ALL_TARGETS
        if target != "zmq" and isinstance(all_results.get(target), dict)
    ]
    if not baselines:
        if require_baseline:
            failures.append("at least one Kafka or AutoMQ baseline result is required")
        return failures + trend_failures

    max_error_rate = thresholds["max_error_rate"]
    for target in ["zmq"] + baselines:
        for key, label in BENCHMARKS:
            result = all_results[target].get(key)
            if result is None:
                failures.append(f"{TARGET_LABELS[target]} missing {label} result")
                continue
            if not isinstance(result, dict):
                failures.append(f"{TARGET_LABELS[target]} {label} result must be an object")
                continue
            error_rate, rate_failures = result_error_rate(
                result,
                TARGET_LABELS[target],
                label,
            )
            if rate_failures:
                failures.extend(rate_failures)
                continue
            if error_rate > max_error_rate:
                failures.append(
                    f"{TARGET_LABELS[target]} {label} error rate {error_rate:.2%} exceeds "
                    f"{max_error_rate:.2%}"
                )

    for baseline in baselines:
        for key, label in BENCHMARKS:
            zmq = all_results["zmq"].get(key)
            other = all_results[baseline].get(key)
            if not isinstance(zmq, dict) or not isinstance(other, dict):
                continue

            zmq_throughput, failure = numeric_metric(
                zmq,
                TARGET_LABELS["zmq"],
                label,
                "throughput",
                "comparison result",
            )
            if failure:
                failures.append(failure)
                zmq_throughput = None
            other_throughput, failure = numeric_metric(
                other,
                TARGET_LABELS[baseline],
                label,
                "throughput",
                "comparison result",
            )
            if failure:
                failures.append(failure)
                other_throughput = None

            if zmq_throughput is None or other_throughput is None:
                pass
            elif zmq_throughput <= 0:
                failures.append(f"ZMQ (Zig) {label} throughput is zero; comparison is invalid")
            elif other_throughput <= 0:
                failures.append(f"{TARGET_LABELS[baseline]} {label} throughput is zero; comparison is invalid")
            else:
                throughput_ratio = zmq_throughput / other_throughput
                if throughput_ratio < thresholds["min_throughput_ratio"]:
                    failures.append(
                        f"{label} ZMQ/{baseline} throughput ratio {throughput_ratio:.2f}x below "
                        f"{thresholds['min_throughput_ratio']:.2f}x"
                    )

            for metric, threshold_name in [
                ("p50", "max_p50_latency_ratio"),
                ("p99", "max_p99_latency_ratio"),
            ]:
                zmq_latency, failure = numeric_metric(
                    zmq,
                    TARGET_LABELS["zmq"],
                    label,
                    metric,
                    "comparison result",
                )
                if failure:
                    failures.append(failure)
                    zmq_latency = None
                other_latency, failure = numeric_metric(
                    other,
                    TARGET_LABELS[baseline],
                    label,
                    metric,
                    "comparison result",
                )
                if failure:
                    failures.append(failure)
                    other_latency = None

                if zmq_latency is None or other_latency is None:
                    continue
                if other_latency <= 0:
                    failures.append(f"{TARGET_LABELS[baseline]} {label} {metric} is zero; comparison is invalid")
                    continue
                if zmq_latency <= 0:
                    failures.append(f"ZMQ (Zig) {label} {metric} is zero; comparison is invalid")
                    continue
                latency_ratio = zmq_latency / other_latency
                max_ratio = thresholds[threshold_name]
                if latency_ratio > max_ratio:
                    failures.append(
                        f"{label} ZMQ/{baseline} {metric} latency ratio {latency_ratio:.2f}x exceeds "
                        f"{max_ratio:.2f}x"
                    )

    return failures + trend_failures

def print_gate_result(
    failures,
    thresholds,
    trend_enabled=False,
    trend_baseline_path=None,
):
    print("\n" + "=" * 72)
    print("  COMPARATIVE BENCHMARK GATE")
    print("=" * 72)
    print(
        "  thresholds: "
        f"throughput_ratio>={thresholds['min_throughput_ratio']:.2f}x, "
        f"p50_ratio<={thresholds['max_p50_latency_ratio']:.2f}x, "
        f"p99_ratio<={thresholds['max_p99_latency_ratio']:.2f}x, "
        f"error_rate<={thresholds['max_error_rate']:.2%}"
    )
    if trend_enabled:
        print(
            "  trend thresholds: "
            f"throughput_ratio>={thresholds['min_trend_throughput_ratio']:.2f}x, "
            f"p50_ratio<={thresholds['max_trend_p50_latency_ratio']:.2f}x, "
            f"p99_ratio<={thresholds['max_trend_p99_latency_ratio']:.2f}x"
        )
        if trend_baseline_path:
            print(f"  trend baseline: {trend_baseline_path}")
    if not failures:
        print("  result: pass")
        return
    print("  result: fail")
    for failure in failures:
        print(f"  - {failure}")

def should_write_results_artifact(enforce_gates, gate_failures):
    return (not enforce_gates) or not gate_failures

def marker_bool(value):
    return "true" if value else "false"

def marker_csv(values):
    return ",".join(values) if values else "-"

def benchmark_profile_text(values):
    return ",".join(f"{key}:{values[key]}" for key, _label in BENCHMARKS)

def comparative_profile_marker(
    all_results,
    selected_targets,
    required_targets,
    gates_enforced,
    trend_required,
    trend_baseline_path,
):
    targets_with_results = [
        target for target in ALL_TARGETS if target in all_results
    ]
    trend_baseline = trend_baseline_path if trend_baseline_path else "-"
    return (
        "ok: comparative benchmark profile "
        f"selected={marker_csv(selected_targets)} "
        f"required={marker_csv(required_targets)} "
        f"results_targets={marker_csv(targets_with_results)} "
        f"results={RESULTS_ARTIFACT} "
        f"gates_enforced={marker_bool(gates_enforced)} "
        f"trend_required={marker_bool(trend_required)} "
        f"trend_baseline={trend_baseline} "
        f"iterations={benchmark_profile_text(ITERATIONS)} "
        f"warmup={benchmark_profile_text(WARMUP)} "
        "source=command"
    )

def print_comparison(all_results):
    """Print side-by-side comparison table for 2 or 3 systems."""
    targets = [t for t in ALL_TARGETS if t in all_results]
    if len(targets) < 2:
        return

    labels = [TARGET_LABELS[t] for t in targets]

    # Build ratio column headers: ZMQ vs each other target
    ratio_pairs = []
    if "zmq" in all_results:
        for t in targets:
            if t != "zmq":
                ratio_pairs.append(("zmq", t))

    print("\n" + "=" * (60 + 14 * len(targets) + 14 * len(ratio_pairs)))
    title_parts = " vs ".join(labels)
    print(f"  COMPARISON: {title_parts}")
    print("=" * (60 + 14 * len(targets) + 14 * len(ratio_pairs)))

    # Header
    hdr = f"  {'Benchmark':<22} {'Metric':<6}"
    for t in targets:
        hdr += f" {TARGET_SHORT_LABELS[t]:>12}"
    for a, b in ratio_pairs:
        hdr += f" {TARGET_SHORT_LABELS[a]+'/'+TARGET_SHORT_LABELS[b]:>14}"
    print(f"\n{hdr}")

    sep = f"  {'─'*22} {'─'*6}"
    for _ in targets:
        sep += f" {'─'*12}"
    for _ in ratio_pairs:
        sep += f" {'─'*14}"
    print(sep)

    for key, label in BENCHMARKS:
        results_for_key = {t: all_results[t].get(key, {}) for t in targets}

        for metric, metric_label, higher_is_better in [
            ("throughput", "tput", True),
            ("p50", "p50", False),
            ("p99", "p99", False),
        ]:
            row_label = label if metric == "throughput" else ""
            row = f"  {row_label:<22} {metric_label:<6}"

            for t in targets:
                val = results_for_key[t].get(metric, 0)
                if metric == "throughput":
                    row += f" {val:>10,.0f}/s"
                else:
                    row += f" {val:>10.2f}ms"

            for a, b in ratio_pairs:
                val_a = results_for_key[a].get(metric, 0)
                val_b = results_for_key[b].get(metric, 0)
                ratio_text, marker = _ratio_str(val_a, val_b, higher_is_better)
                row += f"  {ratio_text} {marker}"

            print(row)

        print()

    width = 60 + 14 * len(targets) + 14 * len(ratio_pairs)
    print("─" * width)
    print("  ▲ = ZMQ wins (higher throughput or lower latency)")
    print("  ▼ = Other system wins")
    print("  Ratio: throughput = ZMQ/other  (>1 = ZMQ faster)")
    print("         latency   = ZMQ/other  (<1 = ZMQ faster)")
    print("─" * width)

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark: ZMQ vs Apache Kafka vs AutoMQ",
        epilog="Examples:\n"
               "  %(prog)s                        # 3-way comparison\n"
               "  %(prog)s --target kafka          # Kafka only\n"
               "  %(prog)s --target zmq,kafka      # ZMQ vs Kafka\n"
               "  %(prog)s --target zmq,automq     # ZMQ vs AutoMQ (original)\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--target",
        default="all",
        help="Comma-separated list of targets: zmq, kafka, automq, or 'all' (default: all)",
    )
    parser.add_argument(
        "--require-enabled",
        action="store_true",
        help="Skip unless ZMQ_RUN_BENCH_COMPARE=1 is set; used by the build step",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run deterministic parser/formatting checks without Docker or brokers",
    )
    args = parser.parse_args()

    if args.self_test:
        return self_test()

    try:
        if args.require_enabled and not env_bool("ZMQ_RUN_BENCH_COMPARE", False):
            print("skip: set ZMQ_RUN_BENCH_COMPARE=1 to run comparative benchmark gate")
            return 0
        enforce_gates = args.require_enabled or env_bool(
            "ZMQ_BENCH_COMPARE_ENFORCE_GATES",
            False,
        )
        gate_thresholds = comparison_gate_thresholds()
        required_targets = required_targets_from_env(
            require_release_targets=enforce_gates,
        )
        require_trend = trend_baseline_required()
        trend_baseline = load_trend_baseline_from_env(require_trend=require_trend)
        trend_baseline_path = (
            trend_baseline_path_from_env()
            if trend_baseline is not None
            else None
        )
    except ValueError as exc:
        print(f"  ERROR: {exc}")
        return 1

    # Parse target list
    try:
        targets = parse_targets(args.target)
    except ValueError as exc:
        print(f"  ERROR: {exc}")
        return 1
    missing_targets = missing_required_targets(targets, required_targets)
    if missing_targets:
        labels = ", ".join(TARGET_LABELS[target] for target in missing_targets)
        print(f"  ERROR: required benchmark target(s) not selected: {labels}")
        return 1

    manage_docker = len(targets) > 1
    all_results = {}

    for target in targets:
        label = TARGET_LABELS[target]
        compose_file = TARGET_COMPOSE[target]

        if manage_docker:
            compose_down(compose_file, label)
            time.sleep(2)
            wait_secs = 15
            if not compose_up(compose_file, label, wait_secs=wait_secs):
                print(f"  FATAL: {label} cluster failed to start")
                return 1
            # JVM-based systems need more startup time
            if target in ("kafka", "automq"):
                print(f"  {label} JVM startup takes ~30-60s, waiting...")

        results = run_benchmarks(label)
        if results:
            all_results[target] = results

        if manage_docker:
            compose_down(compose_file, label)
            time.sleep(5)

    # Print individual results when only one target was run
    if len(all_results) == 1:
        for target, results in all_results.items():
            label = TARGET_LABELS[target]
            print(f"\n{'=' * 60}")
            print(f"  {label} RESULTS")
            print(f"{'=' * 60}")
            print(f"  {'Benchmark':<25} {'Throughput':>12} {'p50':>8} {'p99':>8}")
            print(f"  {'-'*25} {'-'*12} {'-'*8} {'-'*8}")
            for name, r in results.items():
                print(f"  {name:<25} {r['throughput']:>10,.0f}/s {r['p50']:>6.2f}ms {r['p99']:>6.2f}ms")

    # Print comparison when multiple targets were run
    if len(all_results) >= 2:
        print_comparison(all_results)

    gate_failures = evaluate_comparison_gates(
        all_results,
        thresholds=gate_thresholds,
        require_baseline=args.require_enabled,
        required_targets=required_targets,
        selected_targets=targets,
        trend_baseline=trend_baseline,
    )
    exit_code = 0
    if enforce_gates:
        print_gate_result(
            gate_failures,
            gate_thresholds,
            trend_enabled=trend_baseline is not None,
            trend_baseline_path=trend_baseline_path,
        )
        if gate_failures:
            exit_code = 1

    saved = write_results_file_if_allowed(
        all_results,
        selected_targets=targets,
        required_targets=required_targets,
        thresholds=gate_thresholds,
        trend_required=require_trend,
        trend_baseline_path=trend_baseline_path,
        gates_enforced=enforce_gates,
        gate_failures=gate_failures,
    )
    if saved is not None:
        print(f"\n  Results saved to {RESULTS_ARTIFACT}")
        print(
            "  "
            + comparative_profile_marker(
                all_results,
                selected_targets=targets,
                required_targets=required_targets,
                gates_enforced=enforce_gates,
                trend_required=require_trend,
                trend_baseline_path=trend_baseline_path,
            )
        )
    else:
        print(
            "\n  Results not saved because the comparative benchmark gate failed"
        )

    return exit_code

def saved_results_payload(
    all_results,
    timestamp=None,
    selected_targets=None,
    required_targets=None,
    thresholds=None,
    trend_required=False,
    trend_baseline_path=None,
    gates_enforced=False,
):
    validate_artifact_results_map(all_results)
    targets_with_results = [
        target for target in ALL_TARGETS if target in all_results
    ]
    selected_targets = artifact_target_list(
        targets_with_results if selected_targets is None else selected_targets,
        "benchmark result artifact selected_targets",
    )
    required_targets = artifact_target_list(
        required_targets,
        "benchmark result artifact required_targets",
    )
    artifact_thresholds = validate_artifact_thresholds(
        DEFAULT_GATE_THRESHOLDS if thresholds is None else thresholds,
        "benchmark result artifact metadata thresholds",
    )
    validate_artifact_bool_metadata(
        gates_enforced,
        "benchmark result artifact metadata gates_enforced",
    )
    validate_artifact_bool_metadata(
        trend_required,
        "benchmark result artifact metadata trend_required",
    )
    validate_artifact_trend_baseline_metadata(
        trend_baseline_path,
        trend_required,
        "benchmark result artifact metadata trend_baseline",
    )
    saved = {"timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}
    if timestamp is not None:
        saved["timestamp"] = timestamp
    saved["metadata"] = {
        "schema_version": 1,
        "targets_with_results": targets_with_results,
        "selected_targets": selected_targets,
        "required_targets": required_targets,
        "target_labels": {
            target: TARGET_LABELS[target]
            for target in ALL_TARGETS
        },
        "iterations": dict(ITERATIONS),
        "warmup": dict(WARMUP),
        "thresholds": artifact_thresholds,
        "gates_enforced": gates_enforced,
        "trend_required": trend_required,
        "trend_baseline": trend_baseline_path,
    }
    for target in ALL_TARGETS:
        if target in all_results:
            saved[target] = all_results[target]
    validate_artifact_metadata(
        saved["metadata"],
        all_results,
        "benchmark result artifact",
    )
    return saved

def write_results_file(
    all_results,
    results_file=RESULTS_FILE,
    selected_targets=None,
    required_targets=None,
    thresholds=None,
    trend_required=False,
    trend_baseline_path=None,
    gates_enforced=False,
):
    saved = saved_results_payload(
        all_results,
        selected_targets=selected_targets,
        required_targets=required_targets,
        thresholds=thresholds,
        trend_required=trend_required,
        trend_baseline_path=trend_baseline_path,
        gates_enforced=gates_enforced,
    )
    encoded = json.dumps(saved, indent=2, allow_nan=False) + "\n"
    results_dir = os.path.dirname(os.path.abspath(results_file)) or "."
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            delete=False,
            dir=results_dir,
            prefix=".results-",
            suffix=".json",
        ) as f:
            tmp_path = f.name
            f.write(encoded)
        os.replace(tmp_path, results_file)
        tmp_path = None
    finally:
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    return saved

def write_results_file_if_allowed(
    all_results,
    results_file=RESULTS_FILE,
    selected_targets=None,
    required_targets=None,
    thresholds=None,
    trend_required=False,
    trend_baseline_path=None,
    gates_enforced=False,
    gate_failures=None,
):
    if not should_write_results_artifact(gates_enforced, gate_failures or []):
        return None
    return write_results_file(
        all_results,
        results_file=results_file,
        selected_targets=selected_targets,
        required_targets=required_targets,
        thresholds=thresholds,
        trend_required=trend_required,
        trend_baseline_path=trend_baseline_path,
        gates_enforced=gates_enforced,
    )

def self_test():
    if RESULTS_ARTIFACT != "benchmarks/results.json":
        raise AssertionError("benchmark results artifact display label drifted")
    if parse_targets("all") != ALL_TARGETS:
        raise AssertionError("all target parsing failed")
    if parse_targets("zmq,kafka") != ["zmq", "kafka"]:
        raise AssertionError("subset target parsing failed")

    try:
        parse_targets("both")
        raise AssertionError("ambiguous target alias was accepted")
    except ValueError:
        pass
    try:
        parse_targets("zmq,unknown")
        raise AssertionError("invalid target parsing did not fail")
    except ValueError:
        pass
    try:
        parse_targets(",,,")
        raise AssertionError("empty target parsing did not fail")
    except ValueError:
        pass

    faster, faster_marker = _ratio_str(2.0, 1.0, True)
    if "2.00x" not in faster or faster_marker != "▲":
        raise AssertionError("throughput ratio formatting failed")
    lower_latency, latency_marker = _ratio_str(0.5, 1.0, False)
    if "0.50x" not in lower_latency or latency_marker != "▲":
        raise AssertionError("latency ratio formatting failed")

    timestamp_before = current_time_ms() - 1000
    timestamp_body = produce_body("timestamp-self-test", 0, b"x")
    timestamp_after = current_time_ms() + 1000
    pos = 0
    transactional_id_len = struct.unpack_from(">h", timestamp_body, pos)[0]
    if transactional_id_len != -1:
        raise AssertionError("Produce v3 timestamp fixture transaction id drifted")
    pos += 2
    pos += 2 + 4  # acks + timeout_ms
    topics_len = struct.unpack_from(">i", timestamp_body, pos)[0]
    pos += 4
    if topics_len != 1:
        raise AssertionError("Produce v3 timestamp fixture topic count drifted")
    topic_len = struct.unpack_from(">h", timestamp_body, pos)[0]
    pos += 2 + topic_len
    partitions_len = struct.unpack_from(">i", timestamp_body, pos)[0]
    pos += 4
    if partitions_len != 1:
        raise AssertionError("Produce v3 timestamp fixture partition count drifted")
    pos += 4  # partition index
    records_len = struct.unpack_from(">i", timestamp_body, pos)[0]
    pos += 4
    records = timestamp_body[pos : pos + records_len]
    first_timestamp_ms = struct.unpack_from(">q", records, 27)[0]
    max_timestamp_ms = struct.unpack_from(">q", records, 35)[0]
    if first_timestamp_ms != max_timestamp_ms:
        raise AssertionError("Produce v3 timestamp fixture timestamp mismatch")
    if not (timestamp_before <= first_timestamp_ms <= timestamp_after):
        raise AssertionError("Produce v3 record timestamp must use wall-clock epoch milliseconds")

    thresholds = {
        "min_throughput_ratio": 0.50,
        "max_p50_latency_ratio": 2.00,
        "max_p99_latency_ratio": 3.00,
        "max_error_rate": 0.00,
        "min_trend_throughput_ratio": 0.75,
        "max_trend_p50_latency_ratio": 2.50,
        "max_trend_p99_latency_ratio": 4.00,
    }
    passing_result = {
        "api_versions": {"throughput": 80, "p50": 2, "p99": 6, "errors": 0, "requests": 10, "successes": 10},
        "produce_single": {"throughput": 80, "p50": 2, "p99": 6, "errors": 0, "requests": 10, "successes": 10},
        "produce_fresh": {"throughput": 80, "p50": 2, "p99": 6, "errors": 0, "requests": 10, "successes": 10},
        "fetch": {"throughput": 80, "p50": 2, "p99": 6, "errors": 0, "requests": 10, "successes": 10},
        "metadata": {"throughput": 80, "p50": 2, "p99": 6, "errors": 0, "requests": 10, "successes": 10},
    }
    baseline_result = {
        "api_versions": {"throughput": 100, "p50": 1, "p99": 2, "errors": 0, "requests": 10, "successes": 10},
        "produce_single": {"throughput": 100, "p50": 1, "p99": 2, "errors": 0, "requests": 10, "successes": 10},
        "produce_fresh": {"throughput": 100, "p50": 1, "p99": 2, "errors": 0, "requests": 10, "successes": 10},
        "fetch": {"throughput": 100, "p50": 1, "p99": 2, "errors": 0, "requests": 10, "successes": 10},
        "metadata": {"throughput": 100, "p50": 1, "p99": 2, "errors": 0, "requests": 10, "successes": 10},
    }
    trend_artifact = saved_results_payload(
        {"zmq": baseline_result},
        timestamp="2026-01-01 00:00:00",
        selected_targets=["zmq", "kafka"],
        required_targets=["zmq", "kafka"],
        thresholds=thresholds,
        trend_required=True,
        trend_baseline_path="benchmarks/results-before-previous.json",
        gates_enforced=True,
    )
    trend_baseline = {"zmq": baseline_result}
    if evaluate_comparison_gates(
        {"zmq": passing_result, "kafka": baseline_result},
        thresholds,
        True,
        trend_baseline=trend_baseline,
    ):
        raise AssertionError("passing comparison gate failed")
    required_failures = evaluate_comparison_gates(
        {"zmq": passing_result, "kafka": baseline_result},
        thresholds,
        True,
        required_targets=["zmq", "kafka", "automq"],
    )
    if not any("AutoMQ" in failure and "did not produce results" in failure for failure in required_failures):
        raise AssertionError("missing required benchmark target was not reported")
    selected_failures = evaluate_comparison_gates(
        {"zmq": passing_result},
        thresholds,
        selected_targets=["zmq", "automq"],
    )
    if not any("selected benchmark target AutoMQ" in failure for failure in selected_failures):
        raise AssertionError("missing selected benchmark target was not reported")
    malformed_target_failures = evaluate_comparison_gates(
        {"zmq": [], "kafka": baseline_result},
        thresholds,
        True,
    )
    if not any("ZMQ (Zig) benchmark result must be an object" in failure for failure in malformed_target_failures):
        raise AssertionError("malformed benchmark target result was not reported")

    comparison_output = io.StringIO()
    with contextlib.redirect_stdout(comparison_output):
        print_comparison({
            "zmq": passing_result,
            "kafka": baseline_result,
            "automq": baseline_result,
        })
    comparison_lines = [
        line.strip()
        for line in comparison_output.getvalue().splitlines()
        if line.strip()
    ]
    comparison_header = next(
        (
            line
            for line in comparison_lines
            if line.startswith("Benchmark") and "Metric" in line
        ),
        "",
    )
    comparison_header_columns = comparison_header.split()
    if comparison_header_columns[:5] != [
        "Benchmark",
        "Metric",
        "ZMQ",
        "Kafka",
        "AutoMQ",
    ]:
        raise AssertionError("comparative table header target labels drifted")
    if (
        "ZMQ/Kafka" not in comparison_header_columns
        or "ZMQ/AutoMQ" not in comparison_header_columns
    ):
        raise AssertionError("comparative table ratio labels drifted")
    expected_profile_marker = (
        "ok: comparative benchmark profile "
        "selected=zmq,kafka,automq "
        "required=zmq,kafka,automq "
        "results_targets=zmq,kafka,automq "
        "results=benchmarks/results.json "
        "gates_enforced=true "
        "trend_required=true "
        "trend_baseline=benchmarks/results-previous.json "
        "iterations=api_versions:5000,produce_single:5000,produce_fresh:2000,fetch:3000,metadata:3000 "
        "warmup=api_versions:100,produce_single:100,produce_fresh:50,fetch:100,metadata:100 "
        "source=command"
    )
    observed_profile_marker = comparative_profile_marker(
        {
            "zmq": passing_result,
            "kafka": baseline_result,
            "automq": baseline_result,
        },
        selected_targets=["zmq", "kafka", "automq"],
        required_targets=["zmq", "kafka", "automq"],
        gates_enforced=True,
        trend_required=True,
        trend_baseline_path="benchmarks/results-previous.json",
    )
    if observed_profile_marker != expected_profile_marker:
        raise AssertionError("comparative profile marker formatting drifted")

    malformed_current_result = {"zmq": dict(passing_result), "kafka": baseline_result}
    malformed_current_result["zmq"]["fetch"] = dict(passing_result["fetch"])
    malformed_current_result["zmq"]["fetch"]["p50"] = "fast"
    malformed_current_failures = evaluate_comparison_gates(
        malformed_current_result,
        thresholds,
        True,
    )
    if not any(
        "comparison result ZMQ (Zig) Fetch p50" in failure
        and "non-numeric" in failure
        for failure in malformed_current_failures
    ):
        raise AssertionError("malformed current benchmark metric was not reported")

    nonfinite_current_result = {"zmq": dict(passing_result), "kafka": baseline_result}
    nonfinite_current_result["zmq"]["fetch"] = dict(passing_result["fetch"])
    nonfinite_current_result["zmq"]["fetch"]["p50"] = float("nan")
    nonfinite_current_failures = evaluate_comparison_gates(
        nonfinite_current_result,
        thresholds,
        True,
    )
    if not any(
        "comparison result ZMQ (Zig) Fetch p50" in failure
        and "non-finite" in failure
        for failure in nonfinite_current_failures
    ):
        raise AssertionError("non-finite current benchmark metric was not reported")

    zero_latency_result = {"zmq": dict(passing_result), "kafka": baseline_result}
    zero_latency_result["zmq"]["metadata"] = dict(passing_result["metadata"])
    zero_latency_result["zmq"]["metadata"]["p99"] = 0
    zero_latency_failures = evaluate_comparison_gates(
        zero_latency_result,
        thresholds,
        True,
    )
    if not any("ZMQ (Zig) Metadata p99 is zero" in failure for failure in zero_latency_failures):
        raise AssertionError("zero current benchmark latency was not reported")

    malformed_count_result = {"zmq": dict(passing_result), "kafka": baseline_result}
    malformed_count_result["zmq"]["api_versions"] = dict(passing_result["api_versions"])
    malformed_count_result["zmq"]["api_versions"]["errors"] = "0"
    malformed_count_failures = evaluate_comparison_gates(
        malformed_count_result,
        thresholds,
        True,
    )
    if not any(
        "ZMQ (Zig) ApiVersions errors" in failure
        and "non-integral" in failure
        for failure in malformed_count_failures
    ):
        raise AssertionError("malformed benchmark error count was not reported")

    failing_result = dict(passing_result)
    failing_result["fetch"] = {
        "throughput": 10,
        "p50": 5,
        "p99": 20,
        "errors": 1,
        "requests": 10,
        "successes": 9,
    }
    failures = evaluate_comparison_gates({"zmq": failing_result, "kafka": baseline_result}, thresholds, True)
    if not failures:
        raise AssertionError("failing comparison gate passed")
    if not any("throughput ratio" in failure for failure in failures):
        raise AssertionError("throughput regression was not reported")
    if not any("error rate" in failure for failure in failures):
        raise AssertionError("error-rate regression was not reported")
    if not evaluate_comparison_gates({"zmq": passing_result}, thresholds, True):
        raise AssertionError("missing baseline was not reported")
    trend_failures = evaluate_trend_gates({"zmq": failing_result}, trend_baseline, thresholds)
    if not any("trend throughput ratio" in failure for failure in trend_failures):
        raise AssertionError("trend throughput regression was not reported")
    if not any("trend p50 latency ratio" in failure for failure in trend_failures):
        raise AssertionError("trend latency regression was not reported")
    if not evaluate_trend_gates({"kafka": baseline_result}, trend_baseline, thresholds):
        raise AssertionError("missing ZMQ trend target was not reported")
    if not evaluate_trend_gates({"zmq": passing_result}, {}, thresholds):
        raise AssertionError("missing ZMQ trend baseline was not reported")
    malformed_trend_baseline = {"zmq": dict(baseline_result)}
    malformed_trend_baseline["zmq"]["fetch"] = dict(baseline_result["fetch"])
    malformed_trend_baseline["zmq"]["fetch"]["throughput"] = "fast"
    malformed_failures = evaluate_trend_gates(
        {"zmq": passing_result},
        malformed_trend_baseline,
        thresholds,
    )
    if not any(
        "trend baseline ZMQ Fetch throughput" in failure
        and "non-numeric" in failure
        for failure in malformed_failures
    ):
        raise AssertionError("malformed trend baseline throughput was not reported")
    nonfinite_trend_baseline = {"zmq": dict(baseline_result)}
    nonfinite_trend_baseline["zmq"]["fetch"] = dict(baseline_result["fetch"])
    nonfinite_trend_baseline["zmq"]["fetch"]["throughput"] = float("inf")
    nonfinite_failures = evaluate_trend_gates(
        {"zmq": passing_result},
        nonfinite_trend_baseline,
        thresholds,
    )
    if not any(
        "trend baseline ZMQ Fetch throughput" in failure
        and "non-finite" in failure
        for failure in nonfinite_failures
    ):
        raise AssertionError("non-finite trend baseline throughput was not reported")
    negative_trend_baseline = {"zmq": dict(baseline_result)}
    negative_trend_baseline["zmq"]["metadata"] = dict(baseline_result["metadata"])
    negative_trend_baseline["zmq"]["metadata"]["p99"] = -1
    negative_failures = evaluate_trend_gates(
        {"zmq": passing_result},
        negative_trend_baseline,
        thresholds,
    )
    if not any(
        "trend baseline ZMQ Metadata p99" in failure
        and "negative" in failure
        for failure in negative_failures
    ):
        raise AssertionError("negative trend baseline latency was not reported")

    old_env = os.environ.copy()
    trend_path = None
    relative_trend_path = None
    strict_trend_path = None
    duplicate_trend_path = None
    strict_results_path = None
    try:
        os.environ["ZMQ_RUN_BENCH_COMPARE"] = "placeholder"
        try:
            env_bool("ZMQ_RUN_BENCH_COMPARE", False)
            raise AssertionError("placeholder comparative benchmark run gate was accepted")
        except ValueError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_RUN_BENCH_COMPARE"] = "maybe"
        try:
            env_bool("ZMQ_RUN_BENCH_COMPARE", False)
            raise AssertionError("invalid comparative benchmark run gate was accepted")
        except ValueError as exc:
            if "true or false" not in str(exc):
                raise
        os.environ["ZMQ_RUN_BENCH_COMPARE"] = "   "
        try:
            env_bool("ZMQ_RUN_BENCH_COMPARE", False)
            raise AssertionError("blank comparative benchmark run gate was accepted")
        except ValueError as exc:
            if "blank" not in str(exc):
                raise
        os.environ["ZMQ_RUN_BENCH_COMPARE"] = "on"
        if not env_bool("ZMQ_RUN_BENCH_COMPARE", False):
            raise AssertionError("truthy comparative benchmark run gate was not accepted")
        os.environ.pop("ZMQ_RUN_BENCH_COMPARE", None)

        os.environ["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "placeholder"
        try:
            env_bool("ZMQ_BENCH_COMPARE_ENFORCE_GATES", False)
            raise AssertionError("placeholder benchmark enforce-gates flag was accepted")
        except ValueError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "maybe"
        try:
            env_bool("ZMQ_BENCH_COMPARE_ENFORCE_GATES", False)
            raise AssertionError("invalid benchmark enforce-gates flag was accepted")
        except ValueError as exc:
            if "true or false" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = ""
        try:
            env_bool("ZMQ_BENCH_COMPARE_ENFORCE_GATES", False)
            raise AssertionError("blank benchmark enforce-gates flag was accepted")
        except ValueError as exc:
            if "blank" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "yes"
        if not env_bool("ZMQ_BENCH_COMPARE_ENFORCE_GATES", False):
            raise AssertionError("truthy benchmark enforce-gates flag was not accepted")
        os.environ.pop("ZMQ_BENCH_COMPARE_ENFORCE_GATES", None)

        os.environ["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = "0.25"
        os.environ["ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO"] = "4"
        os.environ["ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO"] = "8"
        os.environ["ZMQ_BENCH_COMPARE_MAX_ERROR_RATE"] = "0.01"
        os.environ["ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO"] = "0.80"
        os.environ["ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO"] = "1.50"
        os.environ["ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO"] = "1.75"
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "kafka,automq"
        os.environ["ZMQ_BENCH_COMPARE_REQUIRE_TREND"] = "true"
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            json.dump(trend_artifact, f, allow_nan=False)
            trend_path = f.name
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = trend_path
        env_thresholds = comparison_gate_thresholds()
        if env_thresholds["min_throughput_ratio"] != 0.25 or env_thresholds["max_error_rate"] != 0.01:
            raise AssertionError("environment threshold parsing failed")
        if env_thresholds["min_trend_throughput_ratio"] != 0.80:
            raise AssertionError("environment trend threshold parsing failed")
        for bad_threshold in ("nan", "inf", "-inf"):
            os.environ["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = bad_threshold
            try:
                comparison_gate_thresholds()
                raise AssertionError("non-finite threshold parsing did not fail")
            except ValueError as exc:
                if "finite" not in str(exc):
                    raise
        os.environ["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = "placeholder"
        try:
            comparison_gate_thresholds()
            raise AssertionError("placeholder threshold parsing did not fail")
        except ValueError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = "   "
        try:
            comparison_gate_thresholds()
            raise AssertionError("blank threshold parsing did not fail")
        except ValueError as exc:
            if "blank" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = "-0.1"
        try:
            comparison_gate_thresholds()
            raise AssertionError("negative threshold parsing did not fail")
        except ValueError as exc:
            if "non-negative" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = "0.25"
        if required_targets_from_env() != ["kafka", "automq"]:
            raise AssertionError("environment required target parsing failed")
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "   "
        try:
            required_targets_from_env()
            raise AssertionError("blank required target list was accepted")
        except ValueError as exc:
            if "blank" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "zmq,,kafka"
        try:
            required_targets_from_env()
            raise AssertionError("embedded blank required target was accepted")
        except ValueError as exc:
            if "blank target" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "placeholder"
        try:
            required_targets_from_env()
            raise AssertionError("placeholder required target list was accepted")
        except ValueError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "<target>"
        try:
            required_targets_from_env()
            raise AssertionError("angle-bracket placeholder required target list was accepted")
        except ValueError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "zmq,kafka,kafka"
        try:
            required_targets_from_env()
            raise AssertionError("duplicate required target was accepted")
        except ValueError as exc:
            if "duplicate target" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "kafka,automq"
        try:
            required_targets_from_env(require_release_targets=True)
            raise AssertionError("required target list without ZMQ was accepted")
        except ValueError as exc:
            if "must include zmq" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "zmq"
        try:
            required_targets_from_env(require_release_targets=True)
            raise AssertionError("required target list without baseline was accepted")
        except ValueError as exc:
            if "kafka or automq" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "all"
        try:
            required_targets_from_env(require_release_targets=True)
            raise AssertionError("required target alias was accepted")
        except ValueError as exc:
            if "Unknown target 'all'" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "ZMQ,kafka"
        try:
            required_targets_from_env(require_release_targets=True)
            raise AssertionError("uppercase required target was accepted")
        except ValueError as exc:
            if "Unknown target 'ZMQ'" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "zmq,kafka"
        if required_targets_from_env(require_release_targets=True) != ["zmq", "kafka"]:
            raise AssertionError("release required target parsing failed")
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "kafka,automq"
        if missing_required_targets(["zmq", "kafka"], required_targets_from_env()) != ["automq"]:
            raise AssertionError("required target selection validation failed")
        os.environ.pop("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS", None)
        try:
            required_targets_from_env(require_release_targets=True)
            raise AssertionError("missing release required target list was accepted")
        except ValueError as exc:
            if "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "kafka,automq"
        if not trend_baseline_required():
            raise AssertionError("trend requirement parsing failed")
        os.environ["ZMQ_BENCH_COMPARE_REQUIRE_TREND"] = "placeholder"
        try:
            trend_baseline_required()
            raise AssertionError("placeholder trend requirement flag was accepted")
        except ValueError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRE_TREND"] = "maybe"
        try:
            trend_baseline_required()
            raise AssertionError("invalid trend requirement flag was accepted")
        except ValueError as exc:
            if "true or false" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_REQUIRE_TREND"] = "true"
        loaded_trend = load_trend_baseline_from_env(require_trend=True)
        if loaded_trend.get("zmq", {}).get("fetch", {}).get("throughput") != 100:
            raise AssertionError("trend baseline loading failed")
        with tempfile.NamedTemporaryFile(
            "w",
            delete=False,
            dir=os.path.join(PROJECT_DIR, "benchmarks"),
            prefix=".trend-self-",
            suffix=".json",
        ) as f:
            json.dump(trend_artifact, f, allow_nan=False)
            relative_trend_path = f.name
        relative_trend_env = os.path.relpath(relative_trend_path, PROJECT_DIR)
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = relative_trend_env
        old_cwd = os.getcwd()
        try:
            os.chdir(tempfile.gettempdir())
            loaded_relative_trend = load_trend_baseline_from_env(require_trend=True)
        finally:
            os.chdir(old_cwd)
        if loaded_relative_trend.get("zmq", {}).get("fetch", {}).get("throughput") != 100:
            raise AssertionError("relative trend baseline loading was not project-rooted")
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = trend_path
        artifact = saved_results_payload(
            {"zmq": passing_result},
            timestamp="2026-01-01 00:00:00",
            selected_targets=["zmq", "kafka"],
            required_targets=["zmq", "kafka"],
            thresholds=thresholds,
            trend_required=True,
            trend_baseline_path="benchmarks/results-previous.json",
            gates_enforced=True,
        )
        artifact_metadata = artifact.get("metadata", {})
        if artifact_metadata.get("schema_version") != 1:
            raise AssertionError("benchmark result artifact metadata schema missing")
        if artifact_metadata.get("targets_with_results") != ["zmq"]:
            raise AssertionError("benchmark result artifact target metadata missing")
        if artifact_metadata.get("trend_baseline") != "benchmarks/results-previous.json":
            raise AssertionError("benchmark result artifact trend baseline metadata missing")
        if artifact_metadata.get("thresholds", {}).get("min_trend_throughput_ratio") != 0.75:
            raise AssertionError("benchmark result artifact threshold metadata missing")
        if artifact_metadata.get("target_labels") != TARGET_LABELS:
            raise AssertionError("benchmark result artifact target-label metadata missing")
        if artifact_metadata.get("iterations") != ITERATIONS:
            raise AssertionError("benchmark result artifact iteration metadata missing")
        if artifact_metadata.get("warmup") != WARMUP:
            raise AssertionError("benchmark result artifact warmup metadata missing")
        missing_metadata_artifact = {"zmq": baseline_result}
        try:
            validate_trend_baseline_artifact_metadata(missing_metadata_artifact)
            raise AssertionError("trend baseline artifact metadata missing was accepted")
        except ValueError as exc:
            if "metadata" not in str(exc):
                raise
        wrong_schema_artifact = dict(trend_artifact)
        wrong_schema_artifact["metadata"] = dict(trend_artifact["metadata"])
        wrong_schema_artifact["metadata"]["schema_version"] = 2
        try:
            validate_trend_baseline_artifact_metadata(wrong_schema_artifact)
            raise AssertionError("trend baseline artifact schema drift was accepted")
        except ValueError as exc:
            if "schema_version" not in str(exc):
                raise
        missing_zmq_metadata_artifact = dict(trend_artifact)
        missing_zmq_metadata_artifact["metadata"] = dict(trend_artifact["metadata"])
        missing_zmq_metadata_artifact["metadata"]["targets_with_results"] = ["kafka"]
        try:
            validate_trend_baseline_artifact_metadata(missing_zmq_metadata_artifact)
            raise AssertionError("trend baseline artifact metadata without ZMQ was accepted")
        except ValueError as exc:
            if "must include zmq" not in str(exc):
                raise
        mismatched_targets_metadata_artifact = dict(trend_artifact)
        mismatched_targets_metadata_artifact["metadata"] = dict(trend_artifact["metadata"])
        mismatched_targets_metadata_artifact["metadata"]["targets_with_results"] = [
            "zmq",
            "kafka",
        ]
        try:
            validate_trend_baseline_artifact_metadata(mismatched_targets_metadata_artifact)
            raise AssertionError("mismatched trend baseline artifact target metadata was accepted")
        except ValueError as exc:
            if "must match result targets" not in str(exc):
                raise
        missing_selected_result_artifact = dict(trend_artifact)
        missing_selected_result_artifact["metadata"] = dict(trend_artifact["metadata"])
        missing_selected_result_artifact["metadata"]["selected_targets"] = ["kafka"]
        try:
            validate_trend_baseline_artifact_metadata(missing_selected_result_artifact)
            raise AssertionError("trend baseline artifact result target outside selected targets was accepted")
        except ValueError as exc:
            if "selected_targets must include result targets" not in str(exc):
                raise
        required_outside_selected_artifact = dict(trend_artifact)
        required_outside_selected_artifact["metadata"] = dict(trend_artifact["metadata"])
        required_outside_selected_artifact["metadata"]["selected_targets"] = ["zmq"]
        required_outside_selected_artifact["metadata"]["required_targets"] = [
            "zmq",
            "kafka",
        ]
        try:
            validate_trend_baseline_artifact_metadata(required_outside_selected_artifact)
            raise AssertionError("trend baseline artifact required target outside selected targets was accepted")
        except ValueError as exc:
            if "required_targets must be selected targets" not in str(exc):
                raise
        unknown_top_level_artifact = dict(trend_artifact)
        unknown_top_level_artifact["stray"] = baseline_result
        try:
            validate_trend_baseline_artifact_metadata(unknown_top_level_artifact)
            raise AssertionError("trend baseline artifact unknown top-level key was accepted")
        except ValueError as exc:
            if "unknown top-level artifact keys" not in str(exc):
                raise
        missing_label_artifact = dict(trend_artifact)
        missing_label_artifact["metadata"] = dict(trend_artifact["metadata"])
        missing_label_artifact["metadata"]["target_labels"] = {
            target: label
            for target, label in TARGET_LABELS.items()
            if target != "automq"
        }
        try:
            validate_trend_baseline_artifact_metadata(missing_label_artifact)
            raise AssertionError("trend baseline artifact missing target label was accepted")
        except ValueError as exc:
            if "target_labels missing targets" not in str(exc):
                raise
        wrong_label_artifact = dict(trend_artifact)
        wrong_label_artifact["metadata"] = dict(trend_artifact["metadata"])
        wrong_label_artifact["metadata"]["target_labels"] = dict(TARGET_LABELS)
        wrong_label_artifact["metadata"]["target_labels"]["kafka"] = "Kafka"
        try:
            validate_trend_baseline_artifact_metadata(wrong_label_artifact)
            raise AssertionError("trend baseline artifact mismatched target label was accepted")
        except ValueError as exc:
            if "target_labels kafka must be" not in str(exc):
                raise
        drifted_iterations_artifact = dict(trend_artifact)
        drifted_iterations_artifact["metadata"] = dict(trend_artifact["metadata"])
        drifted_iterations_artifact["metadata"]["iterations"] = dict(ITERATIONS)
        drifted_iterations_artifact["metadata"]["iterations"]["fetch"] += 1
        try:
            validate_trend_baseline_artifact_metadata(drifted_iterations_artifact)
            raise AssertionError("trend baseline artifact mismatched iterations were accepted")
        except ValueError as exc:
            if "iterations fetch must match" not in str(exc):
                raise
        missing_threshold_artifact = dict(trend_artifact)
        missing_threshold_artifact["metadata"] = dict(trend_artifact["metadata"])
        missing_threshold_artifact["metadata"]["thresholds"] = dict(thresholds)
        missing_threshold_artifact["metadata"]["thresholds"].pop("max_error_rate")
        try:
            validate_trend_baseline_artifact_metadata(missing_threshold_artifact)
            raise AssertionError("trend baseline artifact missing threshold was accepted")
        except ValueError as exc:
            if "metadata thresholds missing keys" not in str(exc):
                raise
        nonfinite_threshold_artifact = dict(trend_artifact)
        nonfinite_threshold_artifact["metadata"] = dict(trend_artifact["metadata"])
        nonfinite_threshold_artifact["metadata"]["thresholds"] = dict(thresholds)
        nonfinite_threshold_artifact["metadata"]["thresholds"]["max_error_rate"] = float("inf")
        try:
            validate_trend_baseline_artifact_metadata(nonfinite_threshold_artifact)
            raise AssertionError("trend baseline artifact non-finite threshold was accepted")
        except ValueError as exc:
            if "max_error_rate must be finite" not in str(exc):
                raise
        nonboolean_gate_artifact = dict(trend_artifact)
        nonboolean_gate_artifact["metadata"] = dict(trend_artifact["metadata"])
        nonboolean_gate_artifact["metadata"]["gates_enforced"] = "true"
        try:
            validate_trend_baseline_artifact_metadata(nonboolean_gate_artifact)
            raise AssertionError("trend baseline artifact non-boolean gate flag was accepted")
        except ValueError as exc:
            if "gates_enforced must be a boolean" not in str(exc):
                raise
        missing_required_trend_baseline_artifact = dict(trend_artifact)
        missing_required_trend_baseline_artifact["metadata"] = dict(
            trend_artifact["metadata"]
        )
        missing_required_trend_baseline_artifact["metadata"]["trend_baseline"] = None
        try:
            validate_trend_baseline_artifact_metadata(missing_required_trend_baseline_artifact)
            raise AssertionError("trend baseline artifact missing required trend-baseline path was accepted")
        except ValueError as exc:
            if "trend_baseline is required" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                selected_targets="zmq,kafka",
            )
            raise AssertionError("string benchmark result artifact selected targets were accepted")
        except ValueError as exc:
            if "list of benchmark targets" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                selected_targets=["zmq", "zmq"],
            )
            raise AssertionError("duplicate benchmark result artifact selected targets were accepted")
        except ValueError as exc:
            if "duplicate target" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                required_targets=["zmq", "unknown"],
            )
            raise AssertionError("unknown benchmark result artifact required target was accepted")
        except ValueError as exc:
            if "unknown target" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                selected_targets=["kafka"],
            )
            raise AssertionError("benchmark result artifact selected targets missing result target was accepted")
        except ValueError as exc:
            if "selected_targets must include result targets" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                selected_targets=["zmq"],
                required_targets=["zmq", "kafka"],
            )
            raise AssertionError("benchmark result artifact required target outside selected targets was accepted")
        except ValueError as exc:
            if "required_targets must be selected targets" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                gates_enforced="true",
            )
            raise AssertionError("non-boolean benchmark result artifact gate flag was accepted")
        except ValueError as exc:
            if "gates_enforced must be a boolean" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                trend_required=True,
            )
            raise AssertionError("trend-required benchmark result artifact without trend baseline was accepted")
        except ValueError as exc:
            if "trend_baseline is required" not in str(exc):
                raise
        try:
            saved_results_payload(
                {"zmq": passing_result},
                trend_baseline_path=123,
            )
            raise AssertionError("non-string benchmark result artifact trend baseline was accepted")
        except ValueError as exc:
            if "trend_baseline must be a string or null" not in str(exc):
                raise
        try:
            saved_results_payload(["zmq"])
            raise AssertionError("non-object benchmark result artifact map was accepted")
        except ValueError as exc:
            if "results must be an object" not in str(exc):
                raise
        try:
            saved_results_payload({"unknown": passing_result})
            raise AssertionError("unknown benchmark result artifact result target was accepted")
        except ValueError as exc:
            if "unknown target" not in str(exc):
                raise
        try:
            saved_results_payload({"zmq": []})
            raise AssertionError("non-object benchmark result artifact target result was accepted")
        except ValueError as exc:
            if "result must be an object" not in str(exc):
                raise
        try:
            saved_results_payload({"zmq": {"fetch": passing_result["fetch"]}})
            raise AssertionError("benchmark result artifact missing benchmark row was accepted")
        except ValueError as exc:
            if "missing ApiVersions result" not in str(exc):
                raise
        unknown_benchmark_artifact = {"zmq": dict(passing_result)}
        unknown_benchmark_artifact["zmq"]["extra"] = dict(passing_result["fetch"])
        try:
            saved_results_payload(unknown_benchmark_artifact)
            raise AssertionError("unknown benchmark result artifact benchmark key was accepted")
        except ValueError as exc:
            if "unknown benchmark result keys" not in str(exc):
                raise
        malformed_metric_artifact = {"zmq": dict(passing_result)}
        malformed_metric_artifact["zmq"]["fetch"] = dict(passing_result["fetch"])
        malformed_metric_artifact["zmq"]["fetch"]["p50"] = "fast"
        try:
            saved_results_payload(malformed_metric_artifact)
            raise AssertionError("malformed benchmark result artifact metric was accepted")
        except ValueError as exc:
            if "benchmark result artifact ZMQ (Zig) Fetch p50" not in str(exc):
                raise
        zero_metric_artifact = {"zmq": dict(passing_result)}
        zero_metric_artifact["zmq"]["metadata"] = dict(passing_result["metadata"])
        zero_metric_artifact["zmq"]["metadata"]["throughput"] = 0
        try:
            saved_results_payload(zero_metric_artifact)
            raise AssertionError("zero benchmark result artifact metric was accepted")
        except ValueError as exc:
            if "benchmark result artifact ZMQ (Zig) Metadata throughput is zero" not in str(exc):
                raise
        malformed_count_artifact = {"zmq": dict(passing_result)}
        malformed_count_artifact["zmq"]["api_versions"] = dict(
            passing_result["api_versions"]
        )
        malformed_count_artifact["zmq"]["api_versions"]["errors"] = "0"
        try:
            saved_results_payload(malformed_count_artifact)
            raise AssertionError("malformed benchmark result artifact count was accepted")
        except ValueError as exc:
            if "benchmark result artifact ZMQ (Zig) ApiVersions errors" not in str(exc):
                raise
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write('{"zmq":{"fetch":{"throughput":NaN}}}')
            strict_trend_path = f.name
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = strict_trend_path
        try:
            load_trend_baseline_from_env(require_trend=True)
            raise AssertionError("non-standard JSON trend baseline was accepted")
        except ValueError as exc:
            message = str(exc)
            if "strict JSON" not in message or "non-standard JSON constant" not in message:
                raise
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write('{"zmq":{"fetch":{"throughput":100},"fetch":{"throughput":99}}}')
            duplicate_trend_path = f.name
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = duplicate_trend_path
        try:
            load_trend_baseline_from_env(require_trend=True)
            raise AssertionError("duplicate-key JSON trend baseline was accepted")
        except ValueError as exc:
            message = str(exc)
            if "strict JSON" not in message or "duplicate JSON object key" not in message:
                raise
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = trend_path
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write("previous benchmark artifact\n")
            strict_results_path = f.name
        nonstandard_metric_artifact = {"zmq": dict(passing_result)}
        nonstandard_metric_artifact["zmq"]["fetch"] = dict(passing_result["fetch"])
        nonstandard_metric_artifact["zmq"]["fetch"]["throughput"] = float("nan")
        try:
            write_results_file(nonstandard_metric_artifact, strict_results_path)
            raise AssertionError("non-standard JSON benchmark result was written")
        except ValueError as exc:
            if "non-finite" not in str(exc) and "JSON compliant" not in str(exc):
                raise
        with open(strict_results_path) as f:
            if f.read() != "previous benchmark artifact\n":
                raise AssertionError(
                    "non-standard JSON benchmark result clobbered existing artifact"
                )
        malformed_metric_artifact["zmq"]["fetch"]["p50"] = "fast"
        try:
            write_results_file(malformed_metric_artifact, strict_results_path)
            raise AssertionError("malformed benchmark result artifact was written")
        except ValueError as exc:
            if "benchmark result artifact ZMQ (Zig) Fetch p50" not in str(exc):
                raise
        with open(strict_results_path) as f:
            if f.read() != "previous benchmark artifact\n":
                raise AssertionError(
                    "malformed benchmark result artifact clobbered existing artifact"
                )
        if not should_write_results_artifact(False, ["gate failed"]):
            raise AssertionError("non-enforced failing benchmark result artifact was not writable")
        if not should_write_results_artifact(True, []):
            raise AssertionError("passing enforced benchmark result artifact was not writable")
        if should_write_results_artifact(True, ["gate failed"]):
            raise AssertionError("failing enforced benchmark result artifact was writable")
        skipped_artifact = write_results_file_if_allowed(
            {"zmq": passing_result},
            strict_results_path,
            selected_targets=["zmq"],
            thresholds=thresholds,
            gates_enforced=True,
            gate_failures=["gate failed"],
        )
        if skipped_artifact is not None:
            raise AssertionError("failing enforced benchmark result artifact was written")
        with open(strict_results_path) as f:
            if f.read() != "previous benchmark artifact\n":
                raise AssertionError(
                    "failing enforced benchmark result clobbered existing artifact"
                )
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = "/path/to/prior-results.json"
        try:
            load_trend_baseline_from_env(require_trend=True)
            raise AssertionError("placeholder trend baseline path was accepted")
        except ValueError as exc:
            if "placeholder path" not in str(exc):
                raise
        os.environ["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = "benchmarks/results.json"
        try:
            load_trend_baseline_from_env(require_trend=True)
            raise AssertionError("current results artifact was accepted as trend baseline")
        except ValueError as exc:
            if "prior benchmark artifact" not in str(exc):
                raise
        os.environ.pop("ZMQ_BENCH_COMPARE_TREND_BASELINE", None)
        try:
            load_trend_baseline_from_env(require_trend=True)
            raise AssertionError("missing required trend baseline was accepted")
        except ValueError as exc:
            if "ZMQ_BENCH_COMPARE_TREND_BASELINE" not in str(exc):
                raise
    finally:
        os.environ.clear()
        os.environ.update(old_env)
        if trend_path:
            try:
                os.unlink(trend_path)
            except OSError:
                pass
        if strict_trend_path:
            try:
                os.unlink(strict_trend_path)
            except OSError:
                pass
        if duplicate_trend_path:
            try:
                os.unlink(duplicate_trend_path)
            except OSError:
                pass
        if relative_trend_path:
            try:
                os.unlink(relative_trend_path)
            except OSError:
                pass
        if strict_results_path:
            try:
                os.unlink(strict_results_path)
            except OSError:
                pass

    print("ok: comparative benchmark self-test")
    return 0

if __name__ == "__main__":
    sys.exit(main())

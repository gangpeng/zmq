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
"""

import socket
import struct
import time
import sys
import subprocess
import os
import json
import argparse

BROKER_PORT = 19092
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZMQ_COMPOSE = os.path.join(PROJECT_DIR, "docker-compose.yml")
KAFKA_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "kafka-compose.yml")
AUTOMQ_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "automq-compose.yml")

# All supported targets in display order
ALL_TARGETS = ["zmq", "kafka", "automq"]
TARGET_LABELS = {
    "zmq": "ZMQ (Zig)",
    "kafka": "Apache Kafka",
    "automq": "AutoMQ (Java)",
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
    now_ms = int(time.monotonic() * 1000)
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
        return {"throughput": 0, "p50": 0, "p99": 0, "p999": 0, "errors": errors}

    throughput = len(latencies) / elapsed
    latencies_ms = sorted([l * 1000 for l in latencies])
    p50 = latencies_ms[len(latencies_ms) // 2]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]
    p999 = latencies_ms[int(len(latencies_ms) * 0.999)]

    suffix = f"  ({errors} errors)" if errors else ""
    print(f"    {name}: {throughput:,.0f} req/s  p50={p50:.2f}ms  p99={p99:.2f}ms{suffix}")
    return {"throughput": throughput, "p50": p50, "p99": p99, "p999": p999, "errors": errors}

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

def print_comparison(all_results):
    """Print side-by-side comparison table for 2 or 3 systems."""
    targets = [t for t in ALL_TARGETS if t in all_results]
    if len(targets) < 2:
        return

    labels = [TARGET_LABELS[t] for t in targets]
    short_labels = {
        "zmq": "ZMQ",
        "kafka": "Kafka",
        "automq": "AutoMQ",
    }

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

    benchmarks = [
        ("api_versions", "ApiVersions"),
        ("produce_single", "Produce (reuse)"),
        ("produce_fresh", "Produce (fresh)"),
        ("fetch", "Fetch"),
        ("metadata", "Metadata"),
    ]

    # Header
    hdr = f"  {'Benchmark':<22} {'Metric':<6}"
    for t in targets:
        hdr += f" {short_labels[t]:>12}"
    for a, b in ratio_pairs:
        hdr += f" {short_labels[a]+'/'+short_labels[b]:>14}"
    print(f"\n{hdr}")

    sep = f"  {'─'*22} {'─'*6}"
    for _ in targets:
        sep += f" {'─'*12}"
    for _ in ratio_pairs:
        sep += f" {'─'*14}"
    print(sep)

    for key, label in benchmarks:
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
    args = parser.parse_args()

    # Parse target list
    if args.target == "all" or args.target == "both":
        targets = list(ALL_TARGETS)
    else:
        targets = [t.strip() for t in args.target.split(",")]
        for t in targets:
            if t not in ALL_TARGETS:
                print(f"  ERROR: Unknown target '{t}'. Valid targets: {', '.join(ALL_TARGETS)}")
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

    # Save results
    results_file = os.path.join(PROJECT_DIR, "benchmarks", "results.json")
    saved = {"timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}
    for target in ALL_TARGETS:
        if target in all_results:
            saved[target] = all_results[target]
    with open(results_file, "w") as f:
        json.dump(saved, f, indent=2)
    print(f"\n  Results saved to {results_file}")

    return 0

if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Benchmark comparison: ZMQ vs AutoMQ

Runs the same Kafka wire-protocol benchmarks against both systems using
Docker Compose, then prints a side-by-side comparison table.

Both systems use:
  - 3-node cluster (combined controller+broker mode)
  - MinIO S3 backend
  - Broker exposed on host port 19092
  - Same Kafka protocol v0 requests, same iteration counts

Usage:
  # Full comparison (manages Docker lifecycle automatically):
  python3 benchmarks/benchmark_compare.py

  # Run only against ZMQ (cluster must already be up on port 19092):
  python3 benchmarks/benchmark_compare.py --target zmq

  # Run only against AutoMQ (cluster must already be up on port 19092):
  python3 benchmarks/benchmark_compare.py --target automq
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
AUTOMQ_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "automq-compose.yml")

# Benchmark parameters — identical for both systems
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

# ── Kafka wire protocol helpers (v0, non-flexible) ──

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
    name_b = name.encode()
    body = struct.pack('>i', 1) + struct.pack('>h', len(name_b)) + name_b
    body += struct.pack('>ih', partitions, 1) + struct.pack('>iii', 0, 0, 30000)
    kafka_request_reuse(sock, 19, 0, corr_id, body)

def produce_body(topic, partition, msg):
    topic_b = topic.encode()
    body = struct.pack('>hi', -1, 30000) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>ii', 1, partition)
    body += struct.pack('>i', len(msg)) + msg
    return body

def fetch_body(topic, partition, offset, max_bytes=1048576):
    topic_b = topic.encode()
    # max_wait_ms=100 (low for benchmarking), min_bytes=1
    body = struct.pack('>iii', -1, 100, 1) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>i', 1) + struct.pack('>iqi', partition, offset, max_bytes)
    return body

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
    print(f"\n{'=' * 60}")
    print(f"  {label} Benchmark — 3-Node Cluster + MinIO S3")
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
        body = produce_body("bench-topic", i % 3, f"msg-{i:08d}".encode())
        kafka_request_reuse(sock, 0, 0, i, body)
    results["produce_single"] = bench("Produce (reuse)", produce_fn,
                                       ITERATIONS["produce_single"], WARMUP["produce_single"])
    sock.close()

    # 3. Produce (fresh connection)
    print(f"\n  [3/5] Produce — single msg, fresh conn ({ITERATIONS['produce_fresh']} iters)")
    def produce_fresh_fn(i):
        body = produce_body("bench-topic", i % 3, f"fresh-{i:08d}".encode())
        kafka_request_fresh(BROKER_PORT, 0, 0, i, body)
    results["produce_fresh"] = bench("Produce (fresh)", produce_fresh_fn,
                                      ITERATIONS["produce_fresh"], WARMUP["produce_fresh"])

    # 4. Fetch
    print(f"\n  [4/5] Fetch — conn reuse ({ITERATIONS['fetch']} iters)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))
    def fetch_fn(i):
        body = fetch_body("bench-topic", i % 3, 0)
        kafka_request_reuse(sock, 1, 0, i, body)
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

def print_comparison(zmq_results, automq_results):
    """Print side-by-side comparison table."""
    print("\n" + "=" * 85)
    print("  COMPARISON: ZMQ (Zig) vs AutoMQ (Java)")
    print("=" * 85)

    benchmarks = [
        ("api_versions", "ApiVersions"),
        ("produce_single", "Produce (reuse)"),
        ("produce_fresh", "Produce (fresh)"),
        ("fetch", "Fetch"),
        ("metadata", "Metadata"),
    ]

    # Header
    print(f"\n  {'Benchmark':<22} {'Metric':<6} {'ZMQ':>12} {'AutoMQ':>12} {'ZMQ/AutoMQ':>12}")
    print(f"  {'─'*22} {'─'*6} {'─'*12} {'─'*12} {'─'*12}")

    for key, label in benchmarks:
        z = zmq_results.get(key, {})
        a = automq_results.get(key, {})

        # Throughput row
        zt = z.get("throughput", 0)
        at = a.get("throughput", 0)
        ratio = zt / at if at > 0 else float('inf')
        marker = "▲" if ratio >= 1.0 else "▼"
        print(f"  {label:<22} {'tput':<6} {zt:>10,.0f}/s {at:>10,.0f}/s {ratio:>8.2f}x {marker}")

        # p50 row
        zp = z.get("p50", 0)
        ap = a.get("p50", 0)
        ratio_p = zp / ap if ap > 0 else float('inf')
        marker_p = "▲" if ratio_p <= 1.0 else "▼"
        print(f"  {'':<22} {'p50':<6} {zp:>10.2f}ms {ap:>10.2f}ms {ratio_p:>8.2f}x {marker_p}")

        # p99 row
        zp99 = z.get("p99", 0)
        ap99 = a.get("p99", 0)
        ratio_p99 = zp99 / ap99 if ap99 > 0 else float('inf')
        marker_99 = "▲" if ratio_p99 <= 1.0 else "▼"
        print(f"  {'':<22} {'p99':<6} {zp99:>10.2f}ms {ap99:>10.2f}ms {ratio_p99:>8.2f}x {marker_99}")
        print()

    print("─" * 85)
    print("  ▲ = ZMQ wins (higher throughput or lower latency)")
    print("  ▼ = AutoMQ wins")
    print("  Ratio: throughput = ZMQ/AutoMQ (>1 = ZMQ faster)")
    print("         latency   = ZMQ/AutoMQ (<1 = ZMQ faster)")
    print("─" * 85)

def main():
    parser = argparse.ArgumentParser(description="Benchmark: ZMQ vs AutoMQ")
    parser.add_argument("--target", choices=["zmq", "automq", "both"], default="both",
                       help="Which system to benchmark (default: both)")
    args = parser.parse_args()

    zmq_results = None
    automq_results = None

    if args.target in ("zmq", "both"):
        if args.target == "both":
            compose_down(ZMQ_COMPOSE, "ZMQ")
            time.sleep(2)
            if not compose_up(ZMQ_COMPOSE, "ZMQ", wait_secs=15):
                print("  FATAL: ZMQ cluster failed to start")
                return 1
        zmq_results = run_benchmarks("ZMQ (Zig)")
        if args.target == "both":
            compose_down(ZMQ_COMPOSE, "ZMQ")
            time.sleep(5)

    if args.target in ("automq", "both"):
        if args.target == "both":
            compose_down(AUTOMQ_COMPOSE, "AutoMQ")
            time.sleep(2)
            if not compose_up(AUTOMQ_COMPOSE, "AutoMQ", wait_secs=15):
                print("  FATAL: AutoMQ cluster failed to start")
                return 1
            # AutoMQ JVM needs more startup time
            print("  AutoMQ JVM startup takes ~30-60s, waiting...")
        automq_results = run_benchmarks("AutoMQ (Java)")
        if args.target == "both":
            compose_down(AUTOMQ_COMPOSE, "AutoMQ")

    # Print individual results
    for label, results in [("ZMQ", zmq_results), ("AutoMQ", automq_results)]:
        if results and not (zmq_results and automq_results):
            print(f"\n{'=' * 60}")
            print(f"  {label} RESULTS")
            print(f"{'=' * 60}")
            print(f"  {'Benchmark':<25} {'Throughput':>12} {'p50':>8} {'p99':>8}")
            print(f"  {'-'*25} {'-'*12} {'-'*8} {'-'*8}")
            for name, r in results.items():
                print(f"  {name:<25} {r['throughput']:>10,.0f}/s {r['p50']:>6.2f}ms {r['p99']:>6.2f}ms")

    # Print comparison
    if zmq_results and automq_results:
        print_comparison(zmq_results, automq_results)

    # Save results
    results_file = os.path.join(PROJECT_DIR, "benchmarks", "results.json")
    saved = {"timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}
    if zmq_results:
        saved["zmq"] = zmq_results
    if automq_results:
        saved["automq"] = automq_results
    with open(results_file, "w") as f:
        json.dump(saved, f, indent=2)
    print(f"\n  Results saved to {results_file}")

    return 0

if __name__ == "__main__":
    sys.exit(main())

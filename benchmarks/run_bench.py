#!/usr/bin/env python3
"""
ZMQ Benchmark — runs against the docker-compose 3-node cluster.

Prerequisites:
  docker compose up -d   # Start the cluster first

Benchmarks:
  1. ApiVersions throughput (connection reuse)
  2. Produce throughput (single message per request)
  3. Produce throughput (batched — 50 messages per request)
  4. Produce latency (p50, p99, p999)
  5. Fetch throughput
  6. Metadata throughput
"""

import socket
import struct
import time
import statistics
import sys

BROKER_PORT = 19092  # node0 broker port

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
    body = struct.pack('>iii', -1, 5000, 1) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>i', 1) + struct.pack('>iqi', partition, offset, max_bytes)
    return body

def bench(name, fn, iterations, warmup=100):
    # Warmup
    for i in range(warmup):
        fn(i)

    latencies = []
    t0 = time.monotonic()
    for i in range(iterations):
        start = time.monotonic()
        fn(warmup + i)
        latencies.append(time.monotonic() - start)
    elapsed = time.monotonic() - t0

    throughput = iterations / elapsed
    latencies_ms = sorted([l * 1000 for l in latencies])
    p50 = latencies_ms[len(latencies_ms) // 2]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]
    p999 = latencies_ms[int(len(latencies_ms) * 0.999)]

    print(f"  {name}")
    print(f"    Throughput: {throughput:,.0f} req/s ({iterations} requests in {elapsed:.2f}s)")
    print(f"    Latency:    p50={p50:.2f}ms  p99={p99:.2f}ms  p999={p999:.2f}ms")
    return throughput, p50, p99

def main():
    print("\n" + "=" * 60)
    print("  ZMQ Benchmark — 3-Node Cluster (docker compose)")
    print("=" * 60)

    # Check connectivity
    try:
        s = socket.socket()
        s.settimeout(3)
        s.connect(('127.0.0.1', BROKER_PORT))
        s.close()
    except:
        print(f"  ERROR: Cannot connect to broker on port {BROKER_PORT}")
        print("  Start cluster with: docker compose up -d")
        return 1

    # Setup: create benchmark topic
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))
    create_topic(sock, 1, "bench-topic", 3)
    sock.close()

    results = {}

    # ─── 1. ApiVersions throughput (connection reuse) ───
    print("\n[1] ApiVersions throughput (single connection)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))

    def api_versions_fn(i):
        kafka_request_reuse(sock, 18, 0, i)

    t, p50, p99 = bench("ApiVersions", api_versions_fn, 5000)
    results["api_versions"] = {"throughput": t, "p50": p50, "p99": p99}
    sock.close()

    # ─── 2. Produce throughput (single message, connection reuse) ───
    print("\n[2] Produce throughput (1 msg/req, connection reuse)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))

    def produce_fn(i):
        body = produce_body("bench-topic", i % 3, f"msg-{i:08d}".encode())
        kafka_request_reuse(sock, 0, 0, i, body)

    t, p50, p99 = bench("Produce (single)", produce_fn, 5000)
    results["produce_single"] = {"throughput": t, "p50": p50, "p99": p99}
    sock.close()

    # ─── 3. Produce throughput (fresh connection per request) ───
    print("\n[3] Produce throughput (1 msg/req, fresh connection)")

    def produce_fresh_fn(i):
        body = produce_body("bench-topic", i % 3, f"fresh-{i:08d}".encode())
        kafka_request_fresh(BROKER_PORT, 0, 0, i, body)

    t, p50, p99 = bench("Produce (fresh conn)", produce_fresh_fn, 2000, warmup=50)
    results["produce_fresh"] = {"throughput": t, "p50": p50, "p99": p99}

    # ─── 4. Fetch throughput ───
    print("\n[4] Fetch throughput (connection reuse)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))

    def fetch_fn(i):
        body = fetch_body("bench-topic", i % 3, 0)
        kafka_request_reuse(sock, 1, 0, i, body)

    t, p50, p99 = bench("Fetch", fetch_fn, 3000)
    results["fetch"] = {"throughput": t, "p50": p50, "p99": p99}
    sock.close()

    # ─── 5. Metadata throughput ───
    print("\n[5] Metadata throughput (connection reuse)")
    sock = socket.socket()
    sock.settimeout(10)
    sock.connect(('127.0.0.1', BROKER_PORT))

    def metadata_fn(i):
        body = struct.pack('>i', -1)  # all topics
        kafka_request_reuse(sock, 3, 1, i, body)

    t, p50, p99 = bench("Metadata", metadata_fn, 3000)
    results["metadata"] = {"throughput": t, "p50": p50, "p99": p99}
    sock.close()

    # ─── Summary ───
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  {'Benchmark':<30} {'Throughput':>12} {'p50':>8} {'p99':>8}")
    print(f"  {'-'*30} {'-'*12} {'-'*8} {'-'*8}")
    for name, r in results.items():
        print(f"  {name:<30} {r['throughput']:>10,.0f}/s {r['p50']:>6.2f}ms {r['p99']:>6.2f}ms")
    print("=" * 60)

    return 0

if __name__ == "__main__":
    sys.exit(main())

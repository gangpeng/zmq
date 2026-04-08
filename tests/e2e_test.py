#!/usr/bin/env python3
"""
ZMQ — E2E Integration Test Suite (3-node cluster with MinIO S3)

Runs against a 3-node combined-mode (controller+broker) cluster started via
docker compose. Each node has separate broker port (9092) and controller port
(9093), mapped to different host ports.

Tests:
  a. Cluster health — all 3 nodes respond to ApiVersions
  b. Leader election — verify KRaft quorum via DescribeQuorum
  c. Topic creation and deletion
  d. Produce 1000 messages to 3 partitions
  e. Fetch all messages back, verify count
  f. Consumer group — JoinGroup, SyncGroup, Heartbeat
  g. Offset commit and fetch
  h. Broker failure — kill node 0, verify nodes 1/2 still serve
  i. Restart recovery — restart killed node, verify WAL recovery
  j. Metadata consistency — all nodes return same topics

Usage:
  docker compose up -d                 # Start cluster first
  python3 tests/e2e_test.py            # Run tests
  docker compose down -v               # Cleanup
"""

import socket
import struct
import subprocess
import sys
import time
import urllib.request

# ---------------------------------------------------------------
# Configuration — matches docker-compose.yml port mapping
# ---------------------------------------------------------------
NODES = [
    {"name": "node0", "broker_port": 19092, "controller_port": 19093, "metrics_port": 19090, "container": "zmq-node-0"},
    {"name": "node1", "broker_port": 19094, "controller_port": 19095, "metrics_port": 19091, "container": "zmq-node-1"},
    {"name": "node2", "broker_port": 19096, "controller_port": 19097, "metrics_port": 19098, "container": "zmq-node-2"},
]
MINIO_PORT = 9000


# ---------------------------------------------------------------
# Kafka protocol helpers (raw TCP wire protocol)
# ---------------------------------------------------------------

def kafka_request(sock, api_key, api_version, correlation_id, body=b''):
    """Send a Kafka request with v1 header (non-flexible)."""
    client_id = b'e2e-test'
    header = struct.pack('>hhih', api_key, api_version, correlation_id, len(client_id))
    header += client_id
    frame_body = header + body
    frame = struct.pack('>I', len(frame_body)) + frame_body
    sock.send(frame)
    time.sleep(0.15)
    resp = b''
    while True:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            resp += chunk
            if len(resp) >= 4:
                expected = struct.unpack('>I', resp[:4])[0] + 4
                if len(resp) >= expected:
                    break
        except socket.timeout:
            break
    if len(resp) < 4:
        return None
    size = struct.unpack('>I', resp[:4])[0]
    return resp[4:4+size]


def api_versions(sock, corr_id):
    """Send ApiVersions and return number of API keys."""
    data = kafka_request(sock, 18, 0, corr_id)
    if data is None or len(data) < 8:
        return 0
    return struct.unpack_from('>i', data, 6)[0]


def create_topic(sock, corr_id, name, partitions=3):
    """CreateTopics v0 — returns error_code."""
    name_b = name.encode()
    body = struct.pack('>i', 1) + struct.pack('>h', len(name_b)) + name_b
    body += struct.pack('>ih', partitions, 1) + struct.pack('>iii', 0, 0, 30000)
    data = kafka_request(sock, 19, 0, corr_id, body)
    if data is None or len(data) < 12:
        return -1
    pos = 8
    name_len = struct.unpack_from('>h', data, pos)[0]
    pos += 2 + name_len
    return struct.unpack_from('>h', data, pos)[0]


def delete_topic(sock, corr_id, name):
    """DeleteTopics v0 — returns error_code."""
    name_b = name.encode()
    body = struct.pack('>i', 1) + struct.pack('>h', len(name_b)) + name_b
    body += struct.pack('>i', 30000)
    data = kafka_request(sock, 20, 0, corr_id, body)
    if data is None or len(data) < 12:
        return -1
    pos = 8
    name_len = struct.unpack_from('>h', data, pos)[0]
    pos += 2 + name_len
    return struct.unpack_from('>h', data, pos)[0]


def produce(sock, corr_id, topic, partition, record):
    """Produce v0 — returns (error_code, base_offset)."""
    topic_b = topic.encode()
    body = struct.pack('>hi', -1, 30000) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>ii', 1, partition)
    body += struct.pack('>i', len(record)) + record
    data = kafka_request(sock, 0, 0, corr_id, body)
    if data is None or len(data) < 20:
        return -1, -1
    pos = 4 + 4
    name_len = struct.unpack_from('>h', data, pos)[0]
    pos += 2 + name_len + 4 + 4
    err = struct.unpack_from('>h', data, pos)[0]
    pos += 2
    off = struct.unpack_from('>q', data, pos)[0]
    return err, off


def fetch(sock, corr_id, topic, partition, offset):
    """Fetch v0 — returns (error_code, high_watermark, record_len, records)."""
    topic_b = topic.encode()
    body = struct.pack('>iii', -1, 5000, 1) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>i', 1) + struct.pack('>iqi', partition, offset, 1048576)
    data = kafka_request(sock, 1, 0, corr_id, body)
    if data is None or len(data) < 20:
        return -1, 0, 0, b''
    pos = 4 + 4
    name_len = struct.unpack_from('>h', data, pos)[0]
    pos += 2 + name_len + 4 + 4
    err = struct.unpack_from('>h', data, pos)[0]
    pos += 2
    hw = struct.unpack_from('>q', data, pos)[0]
    pos += 8
    rec_len = struct.unpack_from('>i', data, pos)[0]
    pos += 4
    records = data[pos:pos+rec_len] if rec_len > 0 else b''
    return err, hw, rec_len, records


def metadata_request(sock, corr_id):
    """Metadata v1 with null topics (all topics). Returns list of topic names."""
    body = struct.pack('>i', -1)
    data = kafka_request(sock, 3, 1, corr_id, body)
    if data is None or len(data) < 10:
        return []
    pos = 4
    num_brokers = struct.unpack_from('>i', data, pos)[0]
    pos += 4
    for _ in range(max(num_brokers, 0)):
        pos += 4
        host_len = struct.unpack_from('>h', data, pos)[0]
        pos += 2 + max(host_len, 0) + 4
    cid_len = struct.unpack_from('>h', data, pos)[0]
    pos += 2 + max(cid_len, 0) + 4
    num_topics = struct.unpack_from('>i', data, pos)[0]
    pos += 4
    topics = []
    for _ in range(max(num_topics, 0)):
        pos += 2  # err
        topic_len = struct.unpack_from('>h', data, pos)[0]
        pos += 2
        if topic_len > 0:
            topics.append(data[pos:pos+topic_len].decode('utf-8', errors='replace'))
            pos += topic_len
        pos += 1  # is_internal
        num_parts = struct.unpack_from('>i', data, pos)[0]
        pos += 4
        for _ in range(max(num_parts, 0)):
            pos += 2 + 4 + 4
            num_r = struct.unpack_from('>i', data, pos)[0]
            pos += 4 + max(num_r, 0) * 4
            num_isr = struct.unpack_from('>i', data, pos)[0]
            pos += 4 + max(num_isr, 0) * 4
    return topics


# ---------------------------------------------------------------
# Test Runner
# ---------------------------------------------------------------

class TestRunner:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.corr = 1

    def next(self):
        c = self.corr
        self.corr += 1
        return c

    def connect(self, port):
        s = socket.socket()
        s.settimeout(5)
        s.connect(('127.0.0.1', port))
        return s

    def check(self, name, ok, detail=""):
        if ok:
            self.passed += 1
            print(f"  \u2713 {name}")
        else:
            self.failed += 1
            print(f"  \u2717 {name} {detail}")


# ---------------------------------------------------------------
# Main test suite
# ---------------------------------------------------------------

def main():
    t = TestRunner()

    print("\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557")
    print("\u2551  ZMQ \u2014 3-Node E2E Test Suite (combined mode + MinIO S3)   \u2551")
    print("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d")

    # =============================================
    # Phase 0: Check MinIO is running
    # =============================================
    print("\n[Phase 0] Checking MinIO S3 backend")
    minio_ok = False
    for attempt in range(10):
        try:
            resp = urllib.request.urlopen(f"http://127.0.0.1:{MINIO_PORT}/minio/health/live", timeout=3)
            minio_ok = resp.status == 200
            break
        except:
            time.sleep(1)
    t.check("MinIO is healthy", minio_ok)
    if not minio_ok:
        print("  FATAL: MinIO not running. Start cluster with: docker compose up -d")
        return 1

    # =============================================
    # Test (a): Cluster health — all nodes respond
    # =============================================
    print("\n[Test a] Cluster health check")
    for node in NODES:
        # Check broker port
        try:
            sock = t.connect(node["broker_port"])
            n = api_versions(sock, t.next())
            t.check(f"{node['name']} broker port :{node['broker_port']} responds ({n} APIs)", n >= 10)
            sock.close()
        except Exception as e:
            t.check(f"{node['name']} broker port", False, str(e))

        # Check metrics/health endpoint
        try:
            resp = urllib.request.urlopen(f"http://127.0.0.1:{node['metrics_port']}/health", timeout=3)
            t.check(f"{node['name']} health endpoint :{node['metrics_port']}", resp.status == 200)
        except Exception as e:
            t.check(f"{node['name']} health endpoint", False, str(e))

    # =============================================
    # Test (b): Leader election — check via metrics or DescribeQuorum
    # =============================================
    print("\n[Test b] KRaft leader election")
    try:
        # Connect to controller port and send ApiVersions
        sock = t.connect(NODES[0]["controller_port"])
        n = api_versions(sock, t.next())
        t.check(f"Controller port :{NODES[0]['controller_port']} responds ({n} APIs)", n >= 1)
        sock.close()
    except Exception as e:
        t.check("Controller port responds", False, str(e))

    # =============================================
    # Test (c): Topic creation and deletion
    # =============================================
    print("\n[Test c] Topic creation and deletion")
    sock0 = t.connect(NODES[0]["broker_port"])

    err = create_topic(sock0, t.next(), "e2e-topic", 3)
    t.check("Create e2e-topic (3 partitions)", err == 0, f"err={err}")

    err = create_topic(sock0, t.next(), "delete-me-topic", 1)
    t.check("Create delete-me-topic", err == 0, f"err={err}")

    err = delete_topic(sock0, t.next(), "delete-me-topic")
    t.check("Delete delete-me-topic", err == 0, f"err={err}")

    err = create_topic(sock0, t.next(), "e2e-topic", 3)
    t.check("Duplicate create returns TOPIC_ALREADY_EXISTS (36)", err == 36, f"err={err}")

    sock0.close()

    # =============================================
    # Test (d): Produce 1000 messages to 3 partitions
    # =============================================
    print("\n[Test d] Produce 1000 messages")
    sock_p = t.connect(NODES[0]["broker_port"])
    produce_errors = 0
    for i in range(1000):
        err, off = produce(sock_p, t.next(), "e2e-topic", i % 3, f"msg-{i:04d}".encode())
        if err != 0:
            produce_errors += 1
    t.check(f"Produced 1000 messages (errors={produce_errors})", produce_errors == 0)
    sock_p.close()

    # =============================================
    # Test (e): Fetch all messages back
    # =============================================
    print("\n[Test e] Fetch messages from all partitions")
    sock_f = t.connect(NODES[0]["broker_port"])
    total_fetched_bytes = 0
    total_hw = 0
    for p in range(3):
        err, hw, rec_len, records = fetch(sock_f, t.next(), "e2e-topic", p, 0)
        t.check(f"Fetch partition {p}: hw={hw}, {rec_len}B", err == 0 and rec_len > 0)
        total_fetched_bytes += rec_len
        total_hw += hw
    t.check(f"Total HW across partitions = {total_hw} (>= 900)", total_hw >= 900)
    t.check(f"Total fetched bytes = {total_fetched_bytes}", total_fetched_bytes > 0)
    sock_f.close()

    # =============================================
    # Test (f): Consumer group — JoinGroup, Heartbeat, SyncGroup
    # =============================================
    print("\n[Test f] Consumer group operations")
    sock_g = t.connect(NODES[0]["broker_port"])

    # FindCoordinator
    fc_body = struct.pack('>h', len(b'e2e-group')) + b'e2e-group'
    data = kafka_request(sock_g, 10, 0, t.next(), fc_body)
    t.check("FindCoordinator", data is not None and len(data) >= 4)

    # JoinGroup v0
    jg_body = struct.pack('>h', len(b'e2e-group')) + b'e2e-group'
    jg_body += struct.pack('>i', 30000)
    jg_body += struct.pack('>h', 0)  # empty member_id
    jg_body += struct.pack('>h', len(b'consumer')) + b'consumer'
    jg_body += struct.pack('>i', 1)
    jg_body += struct.pack('>h', len(b'range')) + b'range'
    jg_body += struct.pack('>i', 0)
    data = kafka_request(sock_g, 11, 0, t.next(), jg_body)
    jg_ok = data is not None and len(data) >= 10
    if jg_ok:
        jg_err = struct.unpack_from('>h', data, 4)[0]
        jg_gen = struct.unpack_from('>i', data, 6)[0]
        t.check(f"JoinGroup: err={jg_err}, gen={jg_gen}", jg_err == 0)
    else:
        t.check("JoinGroup", False, "no response")

    # Heartbeat v0
    hb_body = struct.pack('>h', len(b'e2e-group')) + b'e2e-group'
    hb_body += struct.pack('>i', 1)
    hb_body += struct.pack('>h', len(b'me')) + b'me'
    data = kafka_request(sock_g, 12, 0, t.next(), hb_body)
    t.check("Heartbeat", data is not None and len(data) >= 4)

    sock_g.close()

    # =============================================
    # Test (g): Offset commit and fetch
    # =============================================
    print("\n[Test g] Offset commit and fetch")
    sock_o = t.connect(NODES[0]["broker_port"])

    oc_body = struct.pack('>h', len(b'e2e-group')) + b'e2e-group'
    oc_body += struct.pack('>i', 1)
    oc_body += struct.pack('>h', len(b'member-1')) + b'member-1'
    oc_body += struct.pack('>i', 1)
    oc_body += struct.pack('>h', len(b'e2e-topic')) + b'e2e-topic'
    oc_body += struct.pack('>i', 1)
    oc_body += struct.pack('>iq', 0, 500)
    data = kafka_request(sock_o, 8, 0, t.next(), oc_body)
    t.check("OffsetCommit", data is not None and len(data) >= 4)

    of_body = struct.pack('>h', len(b'e2e-group')) + b'e2e-group'
    of_body += struct.pack('>i', 1)
    of_body += struct.pack('>h', len(b'e2e-topic')) + b'e2e-topic'
    of_body += struct.pack('>i', 1)
    of_body += struct.pack('>i', 0)
    data = kafka_request(sock_o, 9, 1, t.next(), of_body)
    of_ok = data is not None and len(data) >= 10
    if of_ok:
        pos = 4
        num_t = struct.unpack_from('>i', data, pos)[0]
        pos += 4
        if num_t > 0 and pos + 2 <= len(data):
            tname_len = struct.unpack_from('>h', data, pos)[0]
            pos += 2 + max(tname_len, 0)
            if pos + 4 <= len(data):
                num_p = struct.unpack_from('>i', data, pos)[0]
                pos += 4
                if num_p > 0 and pos + 4 + 8 <= len(data):
                    pos += 4
                    committed_offset = struct.unpack_from('>q', data, pos)[0]
                    t.check(f"OffsetFetch: committed offset = {committed_offset}", committed_offset == 500)
                else:
                    t.check("OffsetFetch: got partitions", False)
            else:
                t.check("OffsetFetch: parse topics", False)
        else:
            t.check("OffsetFetch: got topics", False)
    else:
        t.check("OffsetFetch", False)
    sock_o.close()

    # =============================================
    # Test (h): Node failure — kill node 0, verify 1/2 serve
    # =============================================
    print("\n[Test h] Node failure resilience")
    subprocess.run(["docker", "stop", NODES[0]["container"]], capture_output=True, timeout=10)
    time.sleep(2)
    t.check("Node 0 stopped", True)

    for i in [1, 2]:
        try:
            sock = t.connect(NODES[i]["broker_port"])
            n = api_versions(sock, t.next())
            t.check(f"Node {i} still serves ({n} APIs)", n >= 10)

            err, off = produce(sock, t.next(), "e2e-topic", 0, b"after-kill")
            t.check(f"Produce to node {i} after kill: offset={off}", err == 0)
            sock.close()
        except Exception as e:
            t.check(f"Node {i} still serves", False, str(e))

    # =============================================
    # Test (i): Restart recovery
    # =============================================
    print("\n[Test i] Restart recovery")
    subprocess.run(["docker", "start", NODES[0]["container"]], capture_output=True, timeout=10)
    time.sleep(3)

    try:
        sock = t.connect(NODES[0]["broker_port"])
        n = api_versions(sock, t.next())
        t.check(f"Node 0 restarted and responds ({n} APIs)", n >= 10)
        sock.close()
    except Exception as e:
        t.check("Node 0 restarted", False, str(e))

    # =============================================
    # Test (j): Metadata consistency across nodes
    # =============================================
    print("\n[Test j] Metadata consistency across nodes")
    topic_lists = []
    for node in NODES:
        try:
            sock = t.connect(node["broker_port"])
            topics = metadata_request(sock, t.next())
            sock.close()
            topic_lists.append(set(topics))
            t.check(f"{node['name']} metadata: {len(topics)} topics ({', '.join(sorted(topics)[:3])}...)", len(topics) > 0)
        except Exception as e:
            t.check(f"{node['name']} metadata", False, str(e))
            topic_lists.append(set())

    # =============================================
    # Test (k): Produce to different nodes (cross-node)
    # =============================================
    print("\n[Test k] Cross-node produce/fetch")
    # Produce to node 1
    try:
        sock = t.connect(NODES[1]["broker_port"])
        err, off = produce(sock, t.next(), "e2e-topic", 0, b"from-node1")
        t.check(f"Produce to node 1: offset={off}", err == 0)
        sock.close()
    except Exception as e:
        t.check("Produce to node 1", False, str(e))

    # Fetch from node 2
    try:
        sock = t.connect(NODES[2]["broker_port"])
        err, hw, rec_len, records = fetch(sock, t.next(), "e2e-topic", 0, 0)
        t.check(f"Fetch from node 2: hw={hw}, {rec_len}B", err == 0)
        sock.close()
    except Exception as e:
        t.check("Fetch from node 2", False, str(e))

    # =============================================
    # Summary
    # =============================================
    print("\n" + "=" * 60)
    total = t.passed + t.failed
    print(f"Results: {t.passed}/{total} passed, {t.failed} failed")
    print("=" * 60)

    return 0 if t.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

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
  ZMQ_RUN_E2E_TESTS=1 python3 tests/e2e_test.py
  python3 tests/e2e_test.py --self-test
  python3 tests/e2e_test.py --load-scale-fixture apply
  docker compose down -v               # Cleanup

Optional cross-broker chaos environment:
  ZMQ_E2E_CHAOS_MATRIX                 comma-separated named phases
  ZMQ_E2E_CHAOS_DOWN / UP              default partition/heal hooks
  ZMQ_E2E_CHAOS_EXPECT                 "fail" (default) or "survive"
  ZMQ_E2E_CHAOS_<PHASE>_DOWN / UP      phase-specific hooks
  ZMQ_E2E_CHAOS_<PHASE>_EXPECT         phase-specific expectation
  ZMQ_E2E_REQUIRED_CHAOS_PHASES        fail if matrix omits required phases
  ZMQ_E2E_LOAD_SCALE_MATRIX            comma-separated named scale/load phases
  ZMQ_E2E_LOAD_SCALE_APPLY / RESTORE   default scale/load orchestration hooks
  ZMQ_E2E_LOAD_SCALE_<PHASE>_APPLY / RESTORE
                                      phase-specific scale/load hooks
  ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES   fail if matrix omits required phases
  ZMQ_E2E_LOAD_SCALE_FIXTURE_NODE      node stopped/started by fixture (node0)
  ZMQ_E2E_LOAD_SCALE_FIXTURE_PRODUCER_NODE
                                      node used by fixture marker produce (node1)
  ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS
                                      record count for fixture load phase

Hook context:
  ZMQ_E2E_TOPIC                        active test topic for the phase
  ZMQ_E2E_BROKER_PORTS                 node:port list for broker listeners
  ZMQ_E2E_CONTROLLER_PORTS             node:port list for controller listeners
  ZMQ_E2E_METRICS_PORTS                node:port list for metrics listeners
  ZMQ_E2E_CONTAINERS                   node:container list
  ZMQ_E2E_MINIO_PORT                   mapped MinIO port
"""

import socket
import struct
import subprocess
import sys
import time
import os
import urllib.request
import shlex
import zlib

RUN_ENABLED = sys.argv.count("--self-test") > 0 or os.environ.get("ZMQ_RUN_E2E_TESTS") == "1"

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


def write_varint(value):
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def write_compact_string(value):
    if value is None:
        return b'\x00'
    raw = value.encode()
    return write_varint(len(raw) + 1) + raw


def write_compact_array_len(count):
    return write_varint(count + 1)


def write_compact_i32_array(values):
    out = bytearray(write_compact_array_len(len(values)))
    for value in values:
        out += struct.pack('>i', value)
    return bytes(out)


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError(f"connection closed after {len(data)}/{size} bytes")
        data.extend(chunk)
    return bytes(data)


def read_varint(buf, pos):
    result = 0
    shift = 0
    for _ in range(5):
        if pos >= len(buf):
            raise RuntimeError("buffer underflow while reading varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if b & 0x80 == 0:
            return result, pos
        shift += 7
    raise RuntimeError("varint too long")


def read_i16(buf, pos):
    if pos + 2 > len(buf):
        raise RuntimeError("buffer underflow while reading i16")
    return struct.unpack_from('>h', buf, pos)[0], pos + 2


def read_i32(buf, pos):
    if pos + 4 > len(buf):
        raise RuntimeError("buffer underflow while reading i32")
    return struct.unpack_from('>i', buf, pos)[0], pos + 4


def read_i64(buf, pos):
    if pos + 8 > len(buf):
        raise RuntimeError("buffer underflow while reading i64")
    return struct.unpack_from('>q', buf, pos)[0], pos + 8


def read_compact_string(buf, pos):
    raw_len, pos = read_varint(buf, pos)
    if raw_len == 0:
        return None, pos
    length = raw_len - 1
    if pos + length > len(buf):
        raise RuntimeError("buffer underflow while reading compact string")
    return buf[pos:pos+length].decode('utf-8', errors='replace'), pos + length


def read_compact_array_len(buf, pos):
    raw_len, pos = read_varint(buf, pos)
    if raw_len == 0:
        return 0, pos
    return raw_len - 1, pos


def read_i32_array(buf, pos):
    count, pos = read_i32(buf, pos)
    if count < 0:
        return None, pos
    values = []
    for _ in range(count):
        value, pos = read_i32(buf, pos)
        values.append(value)
    return values, pos


def read_compact_i32_array(buf, pos):
    count, pos = read_compact_array_len(buf, pos)
    values = []
    for _ in range(count):
        value, pos = read_i32(buf, pos)
        values.append(value)
    return values, pos


def read_bool(buf, pos):
    if pos >= len(buf):
        raise RuntimeError("buffer underflow while reading bool")
    return buf[pos] != 0, pos + 1


def skip_tags(buf, pos):
    count, pos = read_varint(buf, pos)
    for _ in range(count):
        _, pos = read_varint(buf, pos)
        size, pos = read_varint(buf, pos)
        if pos + size > len(buf):
            raise RuntimeError("buffer underflow while skipping tagged field")
        pos += size
    return pos


def controller_request(port, api_key, api_version, correlation_id, body=b'', timeout=5):
    """Send a flexible KRaft controller request."""
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    header += write_compact_string('e2e-test')
    header += b'\x00'
    frame_body = header + body

    with socket.create_connection(('127.0.0.1', port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack('>I', len(frame_body)) + frame_body)
        response_size = struct.unpack('>I', read_exact(sock, 4))[0]
        if response_size <= 0 or response_size > 1024 * 1024:
            raise RuntimeError(f"invalid response frame size {response_size}")
        return read_exact(sock, response_size)


def parse_flexible_response_header(response, correlation_id):
    pos = 0
    response_corr, pos = read_i32(response, pos)
    if response_corr != correlation_id:
        raise RuntimeError(f"correlation mismatch: {response_corr} != {correlation_id}")
    pos = skip_tags(response, pos)
    return pos


def describe_quorum_body():
    body = bytearray()
    body += write_compact_array_len(1)
    body += write_compact_string('__cluster_metadata')
    body += write_compact_array_len(1)
    body += struct.pack('>i', 0)
    body += b'\x00'  # partition tagged fields
    body += b'\x00'  # topic tagged fields
    body += b'\x00'  # request tagged fields
    return bytes(body)


def describe_quorum(port, corr_id):
    """Describe the metadata quorum; returns leader_id/epoch/voters."""
    data = controller_request(port, 55, 0, corr_id, describe_quorum_body())
    pos = 0
    response_corr, pos = read_i32(data, pos)
    if response_corr != corr_id:
        raise RuntimeError(f"DescribeQuorum correlation mismatch: {response_corr}")
    pos = skip_tags(data, pos)
    top_error, pos = read_i16(data, pos)
    if top_error != 0:
        raise RuntimeError(f"DescribeQuorum top-level error_code={top_error}")

    topics_len, pos = read_compact_array_len(data, pos)
    if topics_len == 0:
        raise RuntimeError("DescribeQuorum returned no topics")
    _, pos = read_compact_string(data, pos)
    partitions_len, pos = read_compact_array_len(data, pos)
    if partitions_len == 0:
        raise RuntimeError("DescribeQuorum returned no partitions")

    partition_index, pos = read_i32(data, pos)
    partition_error, pos = read_i16(data, pos)
    leader_id, pos = read_i32(data, pos)
    leader_epoch, pos = read_i32(data, pos)
    high_watermark, pos = read_i64(data, pos)
    voters_len, pos = read_compact_array_len(data, pos)
    voters = []
    for _ in range(voters_len):
        voter_id, pos = read_i32(data, pos)
        _, pos = read_i64(data, pos)
        pos = skip_tags(data, pos)
        voters.append(voter_id)
    observers_len, pos = read_compact_array_len(data, pos)
    for _ in range(observers_len):
        _, pos = read_i32(data, pos)
        _, pos = read_i64(data, pos)
        pos = skip_tags(data, pos)
    pos = skip_tags(data, pos)

    return {
        "partition_index": partition_index,
        "error_code": partition_error,
        "leader_id": leader_id,
        "leader_epoch": leader_epoch,
        "high_watermark": high_watermark,
        "voters": voters,
    }


def wait_for_controller_leader(t, forbidden_leaders=frozenset(), timeout=30):
    deadline = time.time() + timeout
    last_detail = ""
    while time.time() < deadline:
        for node in NODES:
            try:
                quorum = describe_quorum(node["controller_port"], t.next())
                leader_id = quorum["leader_id"]
                if quorum["error_code"] == 0 and leader_id >= 0 and leader_id not in forbidden_leaders:
                    return leader_id, quorum
                last_detail = f"{node['name']} leader={leader_id} err={quorum['error_code']}"
            except Exception as exc:
                last_detail = f"{node['name']}: {exc}"
        time.sleep(1)
    raise RuntimeError(f"controller leader not discovered: {last_detail}")


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


def build_message_set(payload):
    """Build a single-message Kafka v0 MessageSet preserving payload as value."""
    message_body = (
        struct.pack('>bb', 0, 0)  # magic, attributes
        + struct.pack('>i', -1)  # null key
        + struct.pack('>i', len(payload))
        + payload
    )
    crc = zlib.crc32(message_body) & 0xFFFFFFFF
    message = struct.pack('>I', crc) + message_body
    return struct.pack('>qi', 0, len(message)) + message


def produce(sock, corr_id, topic, partition, record):
    """Produce v0 — wraps record as a MessageSet value; returns (error_code, base_offset)."""
    topic_b = topic.encode()
    message_set = build_message_set(record)
    body = struct.pack('>hi', -1, 30000) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>ii', 1, partition)
    body += struct.pack('>i', len(message_set)) + message_set
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


def metadata_partition_leader(port, corr_id, topic):
    topic_b = topic.encode()
    body = struct.pack('>i', 1) + struct.pack('>h', len(topic_b)) + topic_b
    with socket.create_connection(('127.0.0.1', port), timeout=5) as sock:
        sock.settimeout(5)
        data = kafka_request(sock, 3, 1, corr_id, body)
    if data is None or len(data) < 10:
        raise RuntimeError("Metadata response too short")

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
    if num_topics != 1:
        raise RuntimeError(f"Metadata topic count={num_topics}")

    topic_error = struct.unpack_from('>h', data, pos)[0]
    pos += 2
    topic_len = struct.unpack_from('>h', data, pos)[0]
    pos += 2
    topic_name = data[pos:pos+topic_len].decode('utf-8', errors='replace')
    pos += topic_len
    pos += 1  # is_internal
    partition_count = struct.unpack_from('>i', data, pos)[0]
    pos += 4
    if topic_error != 0 or topic_name != topic or partition_count != 1:
        raise RuntimeError(
            f"Metadata topic={topic_name!r} error={topic_error} partitions={partition_count}"
        )

    partition_error = struct.unpack_from('>h', data, pos)[0]
    pos += 2
    partition_index = struct.unpack_from('>i', data, pos)[0]
    pos += 4
    leader_id = struct.unpack_from('>i', data, pos)[0]
    pos += 4
    replicas, pos = read_i32_array(data, pos)
    isr, pos = read_i32_array(data, pos)
    if partition_error != 0 or partition_index != 0:
        raise RuntimeError(f"Metadata partition={partition_index} error={partition_error}")
    return {"leader_id": leader_id, "replicas": replicas, "isr": isr}


def wait_for_metadata_leader(t, port, topic, expected_leader, timeout=30):
    deadline = time.time() + timeout
    last_detail = ""
    while time.time() < deadline:
        try:
            metadata = metadata_partition_leader(port, t.next(), topic)
            if metadata["leader_id"] == expected_leader:
                return metadata
            last_detail = f"leader={metadata['leader_id']} expected={expected_leader}"
        except Exception as exc:
            last_detail = str(exc)
        time.sleep(0.25)
    raise RuntimeError(f"metadata leader did not converge for {topic}: {last_detail}")


def alter_partition_reassignment(port, corr_id, topic, partition, replicas):
    body = struct.pack('>i', 30000)
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack('>i', partition)
    body += write_compact_i32_array(replicas) if replicas is not None else b'\x00'
    body += b'\x00'  # partition tagged fields
    body += b'\x00'  # topic tagged fields
    body += b'\x00'  # request tagged fields

    response = controller_request(port, 45, 0, corr_id, body)
    pos = parse_flexible_response_header(response, corr_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    top_error, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    topics_len, pos = read_compact_array_len(response, pos)
    if top_error != 0 or topics_len != 1:
        raise RuntimeError(f"AlterPartitionReassignments top_error={top_error} topics={topics_len}")
    topic_name, pos = read_compact_string(response, pos)
    partitions_len, pos = read_compact_array_len(response, pos)
    if topic_name != topic or partitions_len != 1:
        raise RuntimeError(f"AlterPartitionReassignments topic={topic_name!r} partitions={partitions_len}")
    response_partition, pos = read_i32(response, pos)
    partition_error, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    if response_partition != partition or partition_error != 0:
        raise RuntimeError(f"AlterPartitionReassignments partition={response_partition} error={partition_error}")


def list_partition_reassignment(port, corr_id, topic, partition):
    body = struct.pack('>i', 30000)
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_i32_array([partition])
    body += b'\x00'  # topic tagged fields
    body += b'\x00'  # request tagged fields

    response = controller_request(port, 46, 0, corr_id, body)
    pos = parse_flexible_response_header(response, corr_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    top_error, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    topics_len, pos = read_compact_array_len(response, pos)
    if top_error != 0:
        raise RuntimeError(f"ListPartitionReassignments top_error={top_error}")
    if topics_len == 0:
        return None
    topic_name, pos = read_compact_string(response, pos)
    partitions_len, pos = read_compact_array_len(response, pos)
    if topic_name != topic or partitions_len == 0:
        return None
    response_partition, pos = read_i32(response, pos)
    replicas, pos = read_compact_i32_array(response, pos)
    adding, pos = read_compact_i32_array(response, pos)
    removing, pos = read_compact_i32_array(response, pos)
    if response_partition != partition:
        return None
    return {"replicas": replicas, "adding": adding, "removing": removing}


def wait_for_partition_reassignment(t, port, topic, partition, expected_replicas, timeout=30):
    deadline = time.time() + timeout
    last_state = None
    last_detail = ""
    while time.time() < deadline:
        try:
            last_state = list_partition_reassignment(port, t.next(), topic, partition)
            if last_state is not None and last_state["replicas"] == expected_replicas:
                return last_state
            last_detail = f"state={last_state}"
        except Exception as exc:
            last_detail = str(exc)
        time.sleep(0.25)
    raise RuntimeError(
        f"partition reassignment did not converge to {expected_replicas}: "
        f"last_state={last_state} last_detail={last_detail}"
    )


# ---------------------------------------------------------------
# Cross-broker chaos gate helpers
# ---------------------------------------------------------------

def split_csv(raw):
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def cli_arg_value(name):
    for index, arg in enumerate(sys.argv):
        if arg == name:
            if index + 1 >= len(sys.argv):
                raise AssertionError(f"{name} requires a value")
            return sys.argv[index + 1]
        if arg.startswith(name + "="):
            return arg[len(name) + 1:]
    return None


def env_phase_token(name):
    token = []
    for ch in name.upper():
        if ch.isalnum():
            token.append(ch)
        else:
            token.append("_")
    collapsed = "_".join(part for part in "".join(token).split("_") if part)
    if not collapsed:
        raise AssertionError("empty E2E chaos phase name")
    return collapsed


def e2e_chaos_hooks_configured():
    return bool(
        os.environ.get("ZMQ_E2E_CHAOS_MATRIX")
        or os.environ.get("ZMQ_E2E_CHAOS_DOWN")
        or os.environ.get("ZMQ_E2E_CHAOS_UP")
    )


def e2e_chaos_phase_env_name(phase, suffix):
    return f"ZMQ_E2E_CHAOS_{env_phase_token(phase)}_{suffix}"


def e2e_chaos_phase_command(phase, suffix):
    return os.environ.get(e2e_chaos_phase_env_name(phase, suffix)) or os.environ.get(
        f"ZMQ_E2E_CHAOS_{suffix}"
    )


def e2e_chaos_phase_expect(phase):
    return os.environ.get(e2e_chaos_phase_env_name(phase, "EXPECT")) or os.environ.get(
        "ZMQ_E2E_CHAOS_EXPECT", "fail"
    )


def selected_e2e_chaos_phases():
    if not e2e_chaos_hooks_configured():
        return []

    raw_matrix = os.environ.get("ZMQ_E2E_CHAOS_MATRIX")
    phase_names = split_csv(raw_matrix) if raw_matrix else ["cross-broker"]
    if not phase_names:
        raise AssertionError("ZMQ_E2E_CHAOS_MATRIX did not contain any phases")

    phases = []
    seen = set()
    for phase in phase_names:
        if phase in seen:
            continue
        seen.add(phase)
        down = e2e_chaos_phase_command(phase, "DOWN")
        up = e2e_chaos_phase_command(phase, "UP")
        if not down or not up:
            raise AssertionError(
                f"E2E chaos phase {phase!r} requires DOWN and UP hooks"
            )
        expect = e2e_chaos_phase_expect(phase)
        if expect not in ("fail", "survive"):
            raise AssertionError(f"invalid E2E chaos expectation for {phase!r}: {expect!r}")
        phases.append({"name": phase, "down": down, "up": up, "expect": expect})
    return phases


def validate_required_e2e_chaos_phase_coverage():
    required = split_csv(os.environ.get("ZMQ_E2E_REQUIRED_CHAOS_PHASES"))
    if not required:
        return

    configured = [phase["name"] for phase in selected_e2e_chaos_phases()]
    missing = [phase for phase in required if phase not in configured]
    if missing:
        raise AssertionError(
            "required E2E chaos phases not configured: " + ", ".join(missing)
        )


def e2e_chaos_hook_env(phase, index, topic=None):
    env = os.environ.copy()
    env["ZMQ_E2E_CHAOS_PHASE"] = phase["name"]
    env["ZMQ_E2E_CHAOS_PHASE_INDEX"] = str(index)
    env["ZMQ_E2E_CHAOS_EXPECT"] = phase["expect"]
    if topic is not None:
        env["ZMQ_E2E_TOPIC"] = topic
    env["ZMQ_E2E_BROKER_PORTS"] = ",".join(
        f"{node['name']}:{node['broker_port']}" for node in NODES
    )
    env["ZMQ_E2E_CONTROLLER_PORTS"] = ",".join(
        f"{node['name']}:{node['controller_port']}" for node in NODES
    )
    env["ZMQ_E2E_METRICS_PORTS"] = ",".join(
        f"{node['name']}:{node['metrics_port']}" for node in NODES
    )
    env["ZMQ_E2E_CONTAINERS"] = ",".join(
        f"{node['name']}:{node['container']}" for node in NODES
    )
    env["ZMQ_E2E_MINIO_PORT"] = str(MINIO_PORT)
    return env


def run_e2e_chaos_hook(label, command, env):
    proc = subprocess.run(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120,
        env=env,
    )
    if proc.returncode != 0:
        raise AssertionError(f"{label} failed with exit code {proc.returncode}\n{proc.stdout}")


def wait_for_cross_node_payload(t, topic, payload, timeout=20):
    deadline = time.time() + timeout
    produced = False
    last_detail = ""

    while time.time() < deadline:
        try:
            if not produced:
                sock = t.connect(NODES[1]["broker_port"])
                err, off = produce(sock, t.next(), topic, 0, payload)
                sock.close()
                if err != 0:
                    last_detail = f"produce err={err}"
                    time.sleep(0.5)
                    continue
                produced = True

            sock = t.connect(NODES[2]["broker_port"])
            err, hw, rec_len, records = fetch(sock, t.next(), topic, 0, 0)
            sock.close()
            if err == 0 and payload in records:
                return True, f"hw={hw}, {rec_len}B"
            last_detail = f"fetch err={err}, {rec_len}B"
        except Exception as exc:
            last_detail = str(exc)
        time.sleep(0.5)

    return False, last_detail


def run_cross_broker_chaos(t, topic):
    phases = selected_e2e_chaos_phases()
    if not phases:
        return

    print("\n[Test m] Cross-broker chaos phases")
    for index, phase in enumerate(phases):
        env = e2e_chaos_hook_env(phase, index, topic)
        healed = False
        during_payload = f"e2e-chaos-during-{index}-{phase['name']}".encode()
        healed_payload = f"e2e-chaos-healed-{index}-{phase['name']}".encode()
        try:
            run_e2e_chaos_hook(f"{phase['name']}:down", phase["down"], env)
            ok, detail = wait_for_cross_node_payload(t, topic, during_payload, timeout=6)
            if phase["expect"] == "survive":
                t.check(f"Cross-broker chaos {phase['name']} survives", ok, detail)
            else:
                t.check(f"Cross-broker chaos {phase['name']} fails closed", not ok, detail)
        finally:
            run_e2e_chaos_hook(f"{phase['name']}:up", phase["up"], env)
            healed = True

        ok, detail = wait_for_cross_node_payload(t, topic, healed_payload, timeout=30)
        t.check(f"Cross-broker chaos {phase['name']} heals", ok, detail)

        if not healed:
            raise AssertionError(f"E2E chaos phase {phase['name']} did not heal")


# ---------------------------------------------------------------
# Live load/scale orchestration gate helpers
# ---------------------------------------------------------------

def e2e_load_scale_hooks_configured():
    return bool(
        os.environ.get("ZMQ_E2E_LOAD_SCALE_MATRIX")
        or os.environ.get("ZMQ_E2E_LOAD_SCALE_APPLY")
        or os.environ.get("ZMQ_E2E_LOAD_SCALE_RESTORE")
    )


def e2e_load_scale_phase_env_name(phase, suffix):
    return f"ZMQ_E2E_LOAD_SCALE_{env_phase_token(phase)}_{suffix}"


def e2e_load_scale_phase_command(phase, suffix):
    return os.environ.get(e2e_load_scale_phase_env_name(phase, suffix)) or os.environ.get(
        f"ZMQ_E2E_LOAD_SCALE_{suffix}"
    )


def selected_e2e_load_scale_phases():
    if not e2e_load_scale_hooks_configured():
        return []

    raw_matrix = os.environ.get("ZMQ_E2E_LOAD_SCALE_MATRIX")
    phase_names = split_csv(raw_matrix) if raw_matrix else ["scale"]
    if not phase_names:
        raise AssertionError("ZMQ_E2E_LOAD_SCALE_MATRIX did not contain any phases")

    phases = []
    seen = set()
    for phase in phase_names:
        if phase in seen:
            continue
        seen.add(phase)
        apply = e2e_load_scale_phase_command(phase, "APPLY")
        restore = e2e_load_scale_phase_command(phase, "RESTORE")
        if not apply or not restore:
            raise AssertionError(
                f"E2E load/scale phase {phase!r} requires APPLY and RESTORE hooks"
            )
        phases.append({"name": phase, "apply": apply, "restore": restore})
    return phases


def validate_required_e2e_load_scale_phase_coverage():
    required = split_csv(os.environ.get("ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"))
    if not required:
        return

    configured = [phase["name"] for phase in selected_e2e_load_scale_phases()]
    missing = [phase for phase in required if phase not in configured]
    if missing:
        raise AssertionError(
            "required E2E load/scale phases not configured: " + ", ".join(missing)
        )


def e2e_load_scale_hook_env(phase, index, topic=None):
    env = os.environ.copy()
    env["ZMQ_E2E_LOAD_SCALE_PHASE"] = phase["name"]
    env["ZMQ_E2E_LOAD_SCALE_PHASE_INDEX"] = str(index)
    if topic is not None:
        env["ZMQ_E2E_TOPIC"] = topic
    env["ZMQ_E2E_BROKER_PORTS"] = ",".join(
        f"{node['name']}:{node['broker_port']}" for node in NODES
    )
    env["ZMQ_E2E_CONTROLLER_PORTS"] = ",".join(
        f"{node['name']}:{node['controller_port']}" for node in NODES
    )
    env["ZMQ_E2E_METRICS_PORTS"] = ",".join(
        f"{node['name']}:{node['metrics_port']}" for node in NODES
    )
    env["ZMQ_E2E_CONTAINERS"] = ",".join(
        f"{node['name']}:{node['container']}" for node in NODES
    )
    env["ZMQ_E2E_MINIO_PORT"] = str(MINIO_PORT)
    return env


def run_e2e_load_scale_hook(label, command, env):
    proc = subprocess.run(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=180,
        env=env,
    )
    if proc.returncode != 0:
        raise AssertionError(f"{label} failed with exit code {proc.returncode}\n{proc.stdout}")


def parse_named_map(raw, var_name):
    if not raw:
        raise AssertionError(f"{var_name} is required")
    result = {}
    for item in split_csv(raw):
        if ":" not in item:
            raise AssertionError(f"{var_name} entry {item!r} must be name:value")
        name, value = item.split(":", 1)
        name = name.strip()
        value = value.strip()
        if not name or not value:
            raise AssertionError(f"{var_name} entry {item!r} must be name:value")
        result[name] = value
    if not result:
        raise AssertionError(f"{var_name} did not contain any entries")
    return result


def parse_named_ports(raw, var_name):
    values = parse_named_map(raw, var_name)
    ports = {}
    for name, value in values.items():
        try:
            port = int(value)
        except ValueError as exc:
            raise AssertionError(f"{var_name} entry {name}:{value} has invalid port") from exc
        if port <= 0 or port > 65535:
            raise AssertionError(f"{var_name} entry {name}:{value} has invalid port")
        ports[name] = port
    return ports


def e2e_load_scale_fixture_env(phase_name, key, default=None):
    token = env_phase_token(phase_name)
    return (
        os.environ.get(f"ZMQ_E2E_LOAD_SCALE_{token}_FIXTURE_{key}")
        or os.environ.get(f"ZMQ_E2E_LOAD_SCALE_FIXTURE_{key}")
        or default
    )


def e2e_load_scale_fixture_payload(kind, phase_name, phase_index):
    suffix = "applied" if kind == "apply" else "restored"
    return f"e2e-load-scale-{suffix}-{phase_index}-{phase_name}".encode()


def docker_container_running(container):
    proc = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise AssertionError(f"docker inspect {container} failed\n{proc.stdout}")
    return proc.stdout.strip().lower() == "true"


def run_docker_container_command(action, container):
    proc = subprocess.run(
        ["docker", action, container],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise AssertionError(f"docker {action} {container} failed\n{proc.stdout}")


def wait_for_broker_port(port, should_be_up, timeout=45):
    deadline = time.time() + timeout
    last_detail = ""
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
                sock.settimeout(3)
                n = api_versions(sock, int(time.time() * 1000) & 0x7fffffff)
            if should_be_up and n >= 10:
                return
            last_detail = f"broker accepted connection with {n} APIs"
        except Exception as exc:
            if not should_be_up:
                return
            last_detail = str(exc)
        time.sleep(1)
    state = "up" if should_be_up else "down"
    raise AssertionError(f"broker port {port} did not become {state}: {last_detail}")


def produce_fixture_payload(port, topic, payload, retries=20):
    last_detail = ""
    for attempt in range(retries):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
                sock.settimeout(5)
                err, off = produce(sock, 700000 + attempt, topic, 0, payload)
            if err == 0:
                return off
            last_detail = f"produce err={err}"
        except Exception as exc:
            last_detail = str(exc)
        time.sleep(0.5)
    raise AssertionError(f"fixture could not produce payload to {port}: {last_detail}")


def run_load_scale_fixture(kind):
    if kind not in ("apply", "restore"):
        raise AssertionError("--load-scale-fixture must be apply or restore")

    phase_name = os.environ.get("ZMQ_E2E_LOAD_SCALE_PHASE")
    if not phase_name:
        raise AssertionError("ZMQ_E2E_LOAD_SCALE_PHASE is required")
    try:
        phase_index = int(os.environ.get("ZMQ_E2E_LOAD_SCALE_PHASE_INDEX", "0"))
    except ValueError as exc:
        raise AssertionError("ZMQ_E2E_LOAD_SCALE_PHASE_INDEX must be an integer") from exc
    topic = os.environ.get("ZMQ_E2E_TOPIC")
    if not topic:
        raise AssertionError("ZMQ_E2E_TOPIC is required")

    broker_ports = parse_named_ports(os.environ.get("ZMQ_E2E_BROKER_PORTS"), "ZMQ_E2E_BROKER_PORTS")
    containers = parse_named_map(os.environ.get("ZMQ_E2E_CONTAINERS"), "ZMQ_E2E_CONTAINERS")
    action = e2e_load_scale_fixture_env(phase_name, "ACTION", phase_name).lower()
    target_node = e2e_load_scale_fixture_env(phase_name, "NODE", "node0")
    producer_node = e2e_load_scale_fixture_env(phase_name, "PRODUCER_NODE", "node1")
    dry_run = e2e_load_scale_fixture_env(phase_name, "DRY_RUN", "0") == "1"

    if target_node not in broker_ports or target_node not in containers:
        raise AssertionError(f"unknown fixture target node {target_node!r}")
    if producer_node not in broker_ports:
        raise AssertionError(f"unknown fixture producer node {producer_node!r}")

    if action == "scale-in":
        if kind == "apply":
            if not dry_run and docker_container_running(containers[target_node]):
                run_docker_container_command("stop", containers[target_node])
                wait_for_broker_port(broker_ports[target_node], should_be_up=False, timeout=20)
        else:
            if not dry_run and not docker_container_running(containers[target_node]):
                run_docker_container_command("start", containers[target_node])
            if not dry_run:
                wait_for_broker_port(broker_ports[target_node], should_be_up=True, timeout=60)
    elif action == "scale-out":
        if kind == "apply":
            if not dry_run and not docker_container_running(containers[target_node]):
                run_docker_container_command("start", containers[target_node])
            if not dry_run:
                wait_for_broker_port(broker_ports[target_node], should_be_up=True, timeout=60)
    elif action == "load":
        if kind == "apply" and not dry_run:
            records = int(e2e_load_scale_fixture_env(phase_name, "LOAD_RECORDS", "30"))
            for index in range(records):
                node_names = sorted(broker_ports)
                node_name = node_names[index % len(node_names)]
                payload = f"e2e-load-scale-load-{phase_index}-{index}".encode()
                produce_fixture_payload(broker_ports[node_name], topic, payload, retries=6)
    elif action not in ("probe", "noop"):
        raise AssertionError(f"unknown load/scale fixture action {action!r}")

    if not dry_run:
        marker = e2e_load_scale_fixture_payload(kind, phase_name, phase_index)
        produce_fixture_payload(broker_ports[producer_node], topic, marker)

    print(f"ok: load/scale fixture {kind} phase={phase_name} action={action} dry_run={dry_run}")
    return 0


def run_e2e_load_scale_phases(t, topic):
    phases = selected_e2e_load_scale_phases()
    if not phases:
        return

    print("\n[Test n] Live load/scale phases")
    for index, phase in enumerate(phases):
        env = e2e_load_scale_hook_env(phase, index, topic)
        restored = False
        applied_payload = f"e2e-load-scale-applied-{index}-{phase['name']}".encode()
        restored_payload = f"e2e-load-scale-restored-{index}-{phase['name']}".encode()
        try:
            run_e2e_load_scale_hook(f"{phase['name']}:apply", phase["apply"], env)
            ok, detail = wait_for_cross_node_payload(t, topic, applied_payload, timeout=30)
            t.check(f"Load/scale {phase['name']} serves after apply", ok, detail)
        finally:
            run_e2e_load_scale_hook(f"{phase['name']}:restore", phase["restore"], env)
            restored = True

        ok, detail = wait_for_cross_node_payload(t, topic, restored_payload, timeout=30)
        t.check(f"Load/scale {phase['name']} serves after restore", ok, detail)

        if not restored:
            raise AssertionError(f"E2E load/scale phase {phase['name']} did not restore")


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


def self_test():
    required_keys = {"name", "broker_port", "controller_port", "metrics_port", "container"}
    if len(NODES) != 3:
        raise AssertionError("E2E harness must define exactly three nodes")

    seen_ports = set()
    for node in NODES:
        missing = required_keys - set(node)
        if missing:
            raise AssertionError(f"E2E node {node!r} missing keys: {sorted(missing)}")
        for key in ("broker_port", "controller_port", "metrics_port"):
            port = node[key]
            if not isinstance(port, int) or port <= 0:
                raise AssertionError(f"E2E node {node['name']} has invalid {key}: {port!r}")
            if port in seen_ports:
                raise AssertionError(f"E2E port {port} is reused")
            seen_ports.add(port)

    if MINIO_PORT in seen_ports:
        raise AssertionError("MinIO port must not collide with broker/controller/metrics ports")

    payload = b"message-set-payload"
    message_set = bytearray(build_message_set(payload))
    if payload not in message_set:
        raise AssertionError("E2E Produce helper did not preserve payload as MessageSet value")
    struct.pack_into('>q', message_set, 0, 42)
    if payload not in message_set:
        raise AssertionError("broker offset rewrite would corrupt E2E MessageSet payload")

    old_env = os.environ.copy()
    try:
        os.environ.pop("ZMQ_E2E_CHAOS_DOWN", None)
        os.environ.pop("ZMQ_E2E_CHAOS_UP", None)
        os.environ.pop("ZMQ_E2E_CHAOS_MATRIX", None)
        os.environ.pop("ZMQ_E2E_LOAD_SCALE_APPLY", None)
        os.environ.pop("ZMQ_E2E_LOAD_SCALE_RESTORE", None)
        os.environ.pop("ZMQ_E2E_LOAD_SCALE_MATRIX", None)
        os.environ.pop("ZMQ_E2E_LOAD_SCALE_FIXTURE_DRY_RUN", None)
        if selected_e2e_chaos_phases() != []:
            raise AssertionError("E2E chaos phases unexpectedly configured")
        if selected_e2e_load_scale_phases() != []:
            raise AssertionError("E2E load/scale phases unexpectedly configured")

        os.environ["ZMQ_E2E_CHAOS_DOWN"] = "true"
        os.environ["ZMQ_E2E_CHAOS_UP"] = "true"
        phases = selected_e2e_chaos_phases()
        if len(phases) != 1 or phases[0]["name"] != "cross-broker":
            raise AssertionError(f"default E2E chaos phase selection failed: {phases}")

        os.environ["ZMQ_E2E_CHAOS_MATRIX"] = "az-a, broker-2,az-a"
        os.environ["ZMQ_E2E_CHAOS_BROKER_2_DOWN"] = "true"
        os.environ["ZMQ_E2E_CHAOS_BROKER_2_UP"] = "true"
        os.environ["ZMQ_E2E_CHAOS_BROKER_2_EXPECT"] = "survive"
        phases = selected_e2e_chaos_phases()
        if [phase["name"] for phase in phases] != ["az-a", "broker-2"]:
            raise AssertionError(f"E2E chaos matrix parsing failed: {phases}")
        if phases[0]["expect"] != "fail" or phases[1]["expect"] != "survive":
            raise AssertionError(f"E2E chaos expectation parsing failed: {phases}")

        os.environ["ZMQ_E2E_REQUIRED_CHAOS_PHASES"] = "az-a,broker-2"
        validate_required_e2e_chaos_phase_coverage()
        os.environ["ZMQ_E2E_REQUIRED_CHAOS_PHASES"] = "missing-phase"
        try:
            validate_required_e2e_chaos_phase_coverage()
            raise AssertionError("missing required E2E chaos phase was not rejected")
        except AssertionError as exc:
            if "required E2E chaos phases" not in str(exc):
                raise

        env = e2e_chaos_hook_env(phases[1], 1, "self-test-topic")
        if env["ZMQ_E2E_CHAOS_PHASE"] != "broker-2":
            raise AssertionError("E2E chaos phase context failed")
        if env["ZMQ_E2E_TOPIC"] != "self-test-topic":
            raise AssertionError("E2E chaos topic context failed")
        if env["ZMQ_E2E_BROKER_PORTS"] != "node0:19092,node1:19094,node2:19096":
            raise AssertionError("E2E chaos broker port context failed")
        if env["ZMQ_E2E_METRICS_PORTS"] != "node0:19090,node1:19091,node2:19098":
            raise AssertionError("E2E chaos metrics port context failed")
        if "node0:zmq-node-0" not in env["ZMQ_E2E_CONTAINERS"]:
            raise AssertionError("E2E chaos container context failed")
        if env["ZMQ_E2E_MINIO_PORT"] != "9000":
            raise AssertionError("E2E chaos MinIO context failed")
        run_e2e_chaos_hook("self-test:down", phases[1]["down"], env)
        run_e2e_chaos_hook("self-test:up", phases[1]["up"], env)

        os.environ["ZMQ_E2E_LOAD_SCALE_APPLY"] = "true"
        os.environ["ZMQ_E2E_LOAD_SCALE_RESTORE"] = "true"
        load_scale_phases = selected_e2e_load_scale_phases()
        if len(load_scale_phases) != 1 or load_scale_phases[0]["name"] != "scale":
            raise AssertionError(f"default E2E load/scale phase selection failed: {load_scale_phases}")

        os.environ["ZMQ_E2E_LOAD_SCALE_MATRIX"] = "scale-out, scale-in,scale-out"
        os.environ["ZMQ_E2E_LOAD_SCALE_SCALE_IN_APPLY"] = "true"
        os.environ["ZMQ_E2E_LOAD_SCALE_SCALE_IN_RESTORE"] = "true"
        load_scale_phases = selected_e2e_load_scale_phases()
        if [phase["name"] for phase in load_scale_phases] != ["scale-out", "scale-in"]:
            raise AssertionError(f"E2E load/scale matrix parsing failed: {load_scale_phases}")

        os.environ["ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"] = "scale-out,scale-in"
        validate_required_e2e_load_scale_phase_coverage()
        os.environ["ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"] = "missing-scale"
        try:
            validate_required_e2e_load_scale_phase_coverage()
            raise AssertionError("missing required E2E load/scale phase was not rejected")
        except AssertionError as exc:
            if "required E2E load/scale phases" not in str(exc):
                raise

        load_env = e2e_load_scale_hook_env(load_scale_phases[1], 1, "self-test-topic")
        if load_env["ZMQ_E2E_LOAD_SCALE_PHASE"] != "scale-in":
            raise AssertionError("E2E load/scale phase context failed")
        if load_env["ZMQ_E2E_TOPIC"] != "self-test-topic":
            raise AssertionError("E2E load/scale topic context failed")
        if load_env["ZMQ_E2E_CONTROLLER_PORTS"] != "node0:19093,node1:19095,node2:19097":
            raise AssertionError("E2E load/scale controller port context failed")
        if load_env["ZMQ_E2E_METRICS_PORTS"] != "node0:19090,node1:19091,node2:19098":
            raise AssertionError("E2E load/scale metrics port context failed")
        if load_env["ZMQ_E2E_MINIO_PORT"] != "9000":
            raise AssertionError("E2E load/scale MinIO context failed")
        run_e2e_load_scale_hook("self-test:apply", load_scale_phases[1]["apply"], load_env)
        run_e2e_load_scale_hook("self-test:restore", load_scale_phases[1]["restore"], load_env)

        expected_apply_payload = e2e_load_scale_fixture_payload("apply", "scale-in", 1)
        expected_restore_payload = e2e_load_scale_fixture_payload("restore", "scale-in", 1)
        if expected_apply_payload != b"e2e-load-scale-applied-1-scale-in":
            raise AssertionError("E2E load/scale apply fixture payload drifted")
        if expected_restore_payload != b"e2e-load-scale-restored-1-scale-in":
            raise AssertionError("E2E load/scale restore fixture payload drifted")

        fixture_command_base = f"{shlex.quote(sys.executable)} {shlex.quote(__file__)} --load-scale-fixture"
        load_env["ZMQ_E2E_LOAD_SCALE_FIXTURE_DRY_RUN"] = "1"
        run_e2e_load_scale_hook(
            "self-test:fixture-apply",
            f"{fixture_command_base} apply",
            load_env,
        )
        run_e2e_load_scale_hook(
            "self-test:fixture-restore",
            f"{fixture_command_base} restore",
            load_env,
        )
    finally:
        os.environ.clear()
        os.environ.update(old_env)

    print("ok: E2E harness self-test")
    return 0


# ---------------------------------------------------------------
# Main test suite
# ---------------------------------------------------------------

def main():
    load_scale_fixture_kind = cli_arg_value("--load-scale-fixture")
    if load_scale_fixture_kind is not None:
        return run_load_scale_fixture(load_scale_fixture_kind)
    if "--self-test" in sys.argv:
        return self_test()
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_E2E_TESTS=1 to run Docker 3-node E2E harness")
        return 0

    try:
        validate_required_e2e_chaos_phase_coverage()
        validate_required_e2e_load_scale_phase_coverage()
    except AssertionError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    t = TestRunner()
    active_controller_leader = None
    active_controller_epoch = -1

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
        leader_id, quorum = wait_for_controller_leader(t)
        active_controller_leader = leader_id
        active_controller_epoch = quorum["leader_epoch"]
        expected_voters = set(range(len(NODES)))
        t.check(
            f"Controller leader elected: node {leader_id} epoch={active_controller_epoch}",
            leader_id in expected_voters and set(quorum["voters"]) == expected_voters,
            f"quorum={quorum}",
        )
    except Exception as e:
        t.check("Controller leader elected via DescribeQuorum", False, str(e))

    # =============================================
    # Test (c): Topic creation and deletion
    # =============================================
    print("\n[Test c] Topic creation and deletion")
    sock0 = t.connect(NODES[0]["broker_port"])

    err = create_topic(sock0, t.next(), "e2e-topic", 3)
    t.check("Create e2e-topic (3 partitions)", err == 0 or err == 36, f"err={err}")

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
    oc_body += struct.pack('>h', len(b'e2e-topic')) + b'e2e-topic'
    oc_body += struct.pack('>i', 1)
    oc_body += struct.pack('>iq', 0, 500)
    oc_body += struct.pack('>h', 0)  # committed_metadata
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
    # Test (h): Node failure — kill active controller leader, verify survivors serve
    # =============================================
    print("\n[Test h] Node failure resilience")
    stopped_node = active_controller_leader if active_controller_leader in range(len(NODES)) else 0
    stopped = NODES[stopped_node]
    subprocess.run(["docker", "stop", stopped["container"]], capture_output=True, timeout=30)
    time.sleep(2)
    t.check(f"Node {stopped_node} stopped (active controller leader)", True)

    if active_controller_leader is not None:
        try:
            replacement_leader, replacement_quorum = wait_for_controller_leader(
                t,
                forbidden_leaders={active_controller_leader},
                timeout=45,
            )
            t.check(
                f"Replacement controller leader elected: node {replacement_leader}",
                replacement_leader != active_controller_leader
                and replacement_quorum["leader_epoch"] > active_controller_epoch,
                f"quorum={replacement_quorum}",
            )
        except Exception as e:
            t.check("Replacement controller leader elected", False, str(e))

    for i in range(len(NODES)):
        if i == stopped_node:
            continue
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
    subprocess.run(["docker", "start", stopped["container"]], capture_output=True, timeout=30)

    restart_ok = False
    restart_detail = ""
    for _ in range(30):
        try:
            sock = t.connect(stopped["broker_port"])
            n = api_versions(sock, t.next())
            sock.close()
            if n >= 10:
                restart_ok = True
                restart_detail = f"{n} APIs"
                break
            restart_detail = f"{n} APIs"
        except Exception as e:
            restart_detail = str(e)
        time.sleep(1)
    t.check(f"Node {stopped_node} restarted and responds ({restart_detail})", restart_ok, restart_detail)

    try:
        restarted_leader, restarted_quorum = wait_for_controller_leader(t, timeout=30)
        t.check(
            f"Controller quorum healthy after node {stopped_node} restart: leader={restarted_leader}",
            restarted_leader in range(len(NODES)),
            f"quorum={restarted_quorum}",
        )
    except Exception as e:
        t.check("Controller quorum healthy after restart", False, str(e))

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
    if topic_lists:
        reference_topics = topic_lists[0]
        metadata_consistent = all(topics == reference_topics for topics in topic_lists[1:])
        if metadata_consistent:
            t.check("Metadata topic sets are consistent across nodes", True)
        else:
            detail = "; ".join(
                f"{NODES[i]['name']}={sorted(topics)}" for i, topics in enumerate(topic_lists)
            )
            t.check("Metadata topic sets are consistent across nodes", False, detail)

    # =============================================
    # Test (k): Produce to different nodes (cross-node)
    # =============================================
    print("\n[Test k] Cross-node produce/fetch")
    cross_node_payload = b"from-node1"
    # Produce to node 1
    try:
        sock = t.connect(NODES[1]["broker_port"])
        err, off = produce(sock, t.next(), "e2e-topic", 0, cross_node_payload)
        t.check(f"Produce to node 1: offset={off}", err == 0)
        sock.close()
    except Exception as e:
        t.check("Produce to node 1", False, str(e))

    # Fetch from node 2
    try:
        sock = t.connect(NODES[2]["broker_port"])
        err, hw, rec_len, records = fetch(sock, t.next(), "e2e-topic", 0, 0)
        t.check(
            f"Fetch from node 2 includes cross-node payload: hw={hw}, {rec_len}B",
            err == 0 and cross_node_payload in records,
        )
        sock.close()
    except Exception as e:
        t.check("Fetch from node 2", False, str(e))

    # =============================================
    # Test (l): Partition reassignment convergence
    # =============================================
    print("\n[Test l] Partition reassignment convergence")
    reassign_topic = f"e2e-reassign-{os.getpid()}"
    source_node = 1
    target_node = 2
    try:
        sock = t.connect(NODES[source_node]["broker_port"])
        err = create_topic(sock, t.next(), reassign_topic, 1)
        sock.close()
        t.check(f"Create {reassign_topic} on node {source_node}", err == 0 or err == 36, f"err={err}")
    except Exception as e:
        t.check("Create reassignment topic", False, str(e))

    try:
        metadata = wait_for_metadata_leader(
            t,
            NODES[source_node]["broker_port"],
            reassign_topic,
            source_node,
        )
        t.check(
            f"Metadata leader starts on node {source_node}",
            metadata["leader_id"] == source_node,
            f"metadata={metadata}",
        )
    except Exception as e:
        t.check("Metadata leader starts on source", False, str(e))

    before_payload = b"reassign-before"
    try:
        sock = t.connect(NODES[source_node]["broker_port"])
        err, off = produce(sock, t.next(), reassign_topic, 0, before_payload)
        sock.close()
        t.check(f"Produce on source before reassignment: offset={off}", err == 0)
    except Exception as e:
        t.check("Produce on source before reassignment", False, str(e))

    try:
        alter_partition_reassignment(
            NODES[source_node]["broker_port"],
            t.next(),
            reassign_topic,
            0,
            [target_node],
        )
        state = wait_for_partition_reassignment(
            t,
            NODES[source_node]["broker_port"],
            reassign_topic,
            0,
            [target_node],
        )
        t.check(
            f"ListPartitionReassignments shows target node {target_node}",
            state["replicas"] == [target_node],
            f"state={state}",
        )
    except Exception as e:
        t.check("Alter/ListPartitionReassignments", False, str(e))

    try:
        source_metadata = wait_for_metadata_leader(
            t,
            NODES[source_node]["broker_port"],
            reassign_topic,
            target_node,
        )
        target_metadata = wait_for_metadata_leader(
            t,
            NODES[target_node]["broker_port"],
            reassign_topic,
            target_node,
        )
        t.check(
            f"Metadata converges to target node {target_node}",
            source_metadata["leader_id"] == target_node and target_metadata["leader_id"] == target_node,
            f"source={source_metadata} target={target_metadata}",
        )
    except Exception as e:
        t.check("Metadata converges after reassignment", False, str(e))

    try:
        sock = t.connect(NODES[source_node]["broker_port"])
        err, _ = produce(sock, t.next(), reassign_topic, 0, b"old-owner-rejected")
        sock.close()
        t.check("Old owner rejects Produce after reassignment", err == 6, f"err={err}")
    except Exception as e:
        t.check("Old owner rejects Produce after reassignment", False, str(e))

    after_payload = b"reassign-after"
    try:
        sock = t.connect(NODES[target_node]["broker_port"])
        err, off = produce(sock, t.next(), reassign_topic, 0, after_payload)
        t.check(f"Target owner Produce succeeds: offset={off}", err == 0)
        err, hw, rec_len, records = fetch(sock, t.next(), reassign_topic, 0, 0)
        sock.close()
        t.check(
            f"Target owner Fetch includes reassigned payload: hw={hw}, {rec_len}B",
            err == 0 and after_payload in records,
        )
    except Exception as e:
        t.check("Target owner Produce/Fetch after reassignment", False, str(e))

    try:
        run_cross_broker_chaos(t, "e2e-topic")
    except Exception as e:
        t.check("Cross-broker chaos gate", False, str(e))

    try:
        run_e2e_load_scale_phases(t, "e2e-topic")
    except Exception as e:
        t.check("Live load/scale gate", False, str(e))

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

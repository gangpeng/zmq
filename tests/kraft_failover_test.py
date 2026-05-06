#!/usr/bin/env python3
"""
Gated multi-process KRaft controller failover harness.

The default build step is intentionally cheap and deterministic: it skips unless
explicitly enabled. When enabled, this starts three controller-only ZMQ
processes, waits for a controller leader via DescribeQuorum, kills that leader,
then verifies the remaining controllers elect a replacement leader.

Run:

    ZMQ_RUN_KRAFT_FAILOVER_TESTS=1 zig build test-kraft-failover

Optional environment:
    ZMQ_BIN                         ./zig-out/bin/zmq
    ZMQ_KRAFT_CONTROLLER_PORT_BASE  39093
    ZMQ_KRAFT_BROKER_PORT           39092
    ZMQ_KRAFT_NETWORK_DOWN          command run to inject controller/broker partition
    ZMQ_KRAFT_NETWORK_UP            command run to heal controller/broker partition
    ZMQ_KRAFT_NETWORK_EXPECT        "fail" (default) or "survive" for Produce during partition
    ZMQ_KRAFT_NETWORK_MATRIX        comma-separated scheduled partition phases
    ZMQ_KRAFT_NETWORK_<PHASE>_DOWN  optional phase-specific down hook
    ZMQ_KRAFT_NETWORK_<PHASE>_UP    optional phase-specific heal hook
    ZMQ_KRAFT_NETWORK_<PHASE>_EXPECT optional phase-specific "fail" or "survive"
    ZMQ_KRAFT_REQUIRED_NETWORK_PHASES
                                     fail if scheduled phases omit any required name
"""

import json
import os
import shlex
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time


RUN_ENABLED = os.environ.get("ZMQ_RUN_KRAFT_FAILOVER_TESTS") == "1"
ZMQ_BIN = os.environ.get("ZMQ_BIN", "./zig-out/bin/zmq")
PORT_BASE = int(os.environ.get("ZMQ_KRAFT_CONTROLLER_PORT_BASE", "39093"))
BROKER_PORT = int(os.environ.get("ZMQ_KRAFT_BROKER_PORT", "39092"))
CLUSTER_ID = f"zmq-kraft-failover-{os.getpid()}-{int(time.time())}"
ERROR_GROUP_ID_NOT_FOUND = 69
ERROR_UNKNOWN_MEMBER_ID = 25
ERROR_FENCED_MEMBER_EPOCH = 110


class TestError(Exception):
    pass


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


def write_signed_varint(value):
    return write_varint((value << 1) ^ (value >> 31))


def write_signed_varlong(value):
    return write_varint((value << 1) ^ (value >> 63))


def read_varint(buf, pos):
    result = 0
    shift = 0
    for _ in range(5):
        if pos >= len(buf):
            raise TestError("buffer underflow while reading varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if b & 0x80 == 0:
            return result, pos
        shift += 7
    raise TestError("varint too long")


def write_string(value):
    raw = value.encode("utf-8")
    return struct.pack(">h", len(raw)) + raw


def write_compact_string(value):
    if value is None:
        return b"\x00"
    raw = value.encode("utf-8")
    return write_varint(len(raw) + 1) + raw


def write_compact_bytes(value):
    if value is None:
        return b"\x00"
    return write_varint(len(value) + 1) + value


def write_bytes(value):
    if value is None:
        return struct.pack(">i", -1)
    return struct.pack(">i", len(value)) + value


def write_compact_array_len(count):
    return write_varint(count + 1)


def write_compact_i32_array(values):
    out = bytearray(write_compact_array_len(len(values)))
    for value in values:
        out += struct.pack(">i", value)
    return bytes(out)


def write_automq_node_tags(tags):
    out = bytearray(write_compact_array_len(len(tags)))
    for key, value in tags:
        out += write_compact_string(key)
        out += write_compact_string(value)
        out += b"\x00"  # tag tagged fields
    return bytes(out)


def write_automq_stream_tags(tags):
    return write_automq_node_tags(tags)


def read_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise TestError(f"connection closed after {len(data)}/{size} bytes")
        data.extend(chunk)
    return bytes(data)


def read_i16(buf, pos):
    if pos + 2 > len(buf):
        raise TestError("buffer underflow while reading i16")
    return struct.unpack_from(">h", buf, pos)[0], pos + 2


def read_i32(buf, pos):
    if pos + 4 > len(buf):
        raise TestError("buffer underflow while reading i32")
    return struct.unpack_from(">i", buf, pos)[0], pos + 4


def read_i64(buf, pos):
    if pos + 8 > len(buf):
        raise TestError("buffer underflow while reading i64")
    return struct.unpack_from(">q", buf, pos)[0], pos + 8


def read_string(buf, pos):
    length, pos = read_i16(buf, pos)
    if length < 0:
        return None, pos
    if pos + length > len(buf):
        raise TestError("buffer underflow while reading string")
    return buf[pos : pos + length].decode("utf-8", errors="replace"), pos + length


def read_bytes(buf, pos):
    length, pos = read_i32(buf, pos)
    if length < 0:
        return None, pos
    if pos + length > len(buf):
        raise TestError("buffer underflow while reading bytes")
    return buf[pos : pos + length], pos + length


def read_compact_string(buf, pos):
    raw_len, pos = read_varint(buf, pos)
    if raw_len == 0:
        return None, pos
    length = raw_len - 1
    if pos + length > len(buf):
        raise TestError("buffer underflow while reading compact string")
    return buf[pos : pos + length].decode("utf-8", errors="replace"), pos + length


def read_compact_bytes(buf, pos):
    raw_len, pos = read_varint(buf, pos)
    if raw_len == 0:
        return None, pos
    length = raw_len - 1
    if pos + length > len(buf):
        raise TestError("buffer underflow while reading compact bytes")
    return buf[pos : pos + length], pos + length


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
        raise TestError("buffer underflow while reading bool")
    return buf[pos] != 0, pos + 1


def skip_tags(buf, pos):
    count, pos = read_varint(buf, pos)
    for _ in range(count):
        _, pos = read_varint(buf, pos)
        size, pos = read_varint(buf, pos)
        if pos + size > len(buf):
            raise TestError("buffer underflow while skipping tagged field")
        pos += size
    return pos


def controller_request(port, api_key, api_version, correlation_id, body=b"", timeout=5):
    if api_key == 55:
        header = struct.pack(">hhi", api_key, api_version, correlation_id)
        header += write_compact_string("kraft-failover-test")
        header += b"\x00"
    else:
        header = struct.pack(">hhi", api_key, api_version, correlation_id)
        header += write_string("kraft-failover-test")
    frame_body = header + body

    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack(">I", len(frame_body)) + frame_body)
        response_size = struct.unpack(">I", read_exact(sock, 4))[0]
        if response_size <= 0 or response_size > 1024 * 1024:
            raise TestError(f"invalid response frame size {response_size}")
        return read_exact(sock, response_size)


def flexible_kafka_request(port, api_key, api_version, correlation_id, body=b"", timeout=5):
    header = struct.pack(">hhi", api_key, api_version, correlation_id)
    header += write_compact_string("kraft-failover-test")
    header += b"\x00"
    frame_body = header + body

    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack(">I", len(frame_body)) + frame_body)
        response_size = struct.unpack(">I", read_exact(sock, 4))[0]
        if response_size <= 0 or response_size > 1024 * 1024:
            raise TestError(f"invalid flexible response frame size {response_size}")
        return read_exact(sock, response_size)


def automq_request(port, api_key, correlation_id, body=b"", timeout=10, api_version=0):
    header = struct.pack(">hhi", api_key, api_version, correlation_id)
    header += write_compact_string("automq-failover-test")
    header += b"\x00"
    frame_body = header + body

    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack(">I", len(frame_body)) + frame_body)
        response_size = struct.unpack(">I", read_exact(sock, 4))[0]
        if response_size <= 0 or response_size > 1024 * 1024:
            raise TestError(f"invalid AutoMQ response frame size {response_size}")
        return read_exact(sock, response_size)


def parse_flexible_response_header(response, expected_correlation_id):
    pos = 0
    correlation_id, pos = read_i32(response, pos)
    if correlation_id != expected_correlation_id:
        raise TestError(
            f"AutoMQ correlation mismatch: expected={expected_correlation_id} got={correlation_id}"
        )
    pos = skip_tags(response, pos)
    return pos


def api_versions_count(port):
    response = controller_request(port, 18, 0, 100)
    pos = 0
    correlation_id, pos = read_i32(response, pos)
    if correlation_id != 100:
        raise TestError(f"ApiVersions correlation mismatch: {correlation_id}")
    error_code, pos = read_i16(response, pos)
    if error_code != 0:
        raise TestError(f"ApiVersions error_code={error_code}")
    count, pos = read_i32(response, pos)
    return count


def create_topic(port, name, correlation_id):
    body = struct.pack(">i", 1)
    body += write_string(name)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">h", 1)  # replication factor
    body += struct.pack(">i", 0)  # replica assignment count
    body += struct.pack(">i", 0)  # configs count
    body += struct.pack(">i", 30000)
    response = controller_request(port, 19, 0, correlation_id, body)
    payload = response[4:]
    if len(payload) < 8:
        raise TestError("CreateTopics response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(name_len, 0)
    error_code = struct.unpack_from(">h", payload, pos)[0]
    if error_code not in (0, 36):  # NONE or TOPIC_ALREADY_EXISTS
        raise TestError(f"CreateTopics error_code={error_code}")


def metadata_partition_leader(port, topic, correlation_id):
    body = struct.pack(">i", 1)
    body += write_string(topic)
    body += b"\x00"  # allow_auto_topic_creation=false
    response = controller_request(port, 3, 4, correlation_id, body)

    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"Metadata correlation mismatch: {response_correlation}")
    _, pos = read_i32(response, pos)  # throttle_time_ms

    brokers_len, pos = read_i32(response, pos)
    if brokers_len < 0:
        raise TestError(f"Metadata invalid broker count={brokers_len}")
    for _ in range(brokers_len):
        _, pos = read_i32(response, pos)  # node_id
        _, pos = read_string(response, pos)  # host
        _, pos = read_i32(response, pos)  # port
        _, pos = read_string(response, pos)  # rack

    _, pos = read_string(response, pos)  # cluster_id
    _, pos = read_i32(response, pos)  # controller_id

    topics_len, pos = read_i32(response, pos)
    if topics_len != 1:
        raise TestError(f"Metadata topic count={topics_len}")
    topic_error, pos = read_i16(response, pos)
    topic_name, pos = read_string(response, pos)
    _, pos = read_bool(response, pos)  # is_internal
    partitions_len, pos = read_i32(response, pos)
    if topic_error != 0:
        raise TestError(f"Metadata topic={topic_name!r} error_code={topic_error}")
    if partitions_len != 1:
        raise TestError(f"Metadata partitions count={partitions_len}")
    partition_error, pos = read_i16(response, pos)
    partition_index, pos = read_i32(response, pos)
    leader_id, pos = read_i32(response, pos)
    replicas, pos = read_i32_array(response, pos)
    isr, pos = read_i32_array(response, pos)
    if partition_error != 0 or partition_index != 0:
        raise TestError(
            f"Metadata partition={partition_index} error_code={partition_error}"
        )
    return {
        "leader_id": leader_id,
        "replicas": replicas,
        "isr": isr,
    }


def wait_for_metadata_leader(port, topic, expected_leader, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 3100
    last_error = None
    last_metadata = None
    while time.time() < deadline:
        try:
            last_metadata = metadata_partition_leader(port, topic, correlation_id)
            if last_metadata["leader_id"] == expected_leader:
                return last_metadata
            raise TestError(
                f"leader_id={last_metadata['leader_id']} expected={expected_leader}"
            )
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"Metadata leader for {topic!r} did not converge to {expected_leader}: "
        f"last_metadata={last_metadata} last_error={last_error}"
    )


def wait_for_topic(port, name):
    deadline = time.time() + 20
    correlation_id = 3000
    last_error = None
    while time.time() < deadline:
        try:
            create_topic(port, name, correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.25)
    raise TestError(f"topic {name!r} was not created: {last_error}")


def produce(port, topic, payload, correlation_id):
    body = struct.pack(">h", 1)  # acks
    body += struct.pack(">i", 30000)
    body += struct.pack(">i", 1)
    body += write_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += struct.pack(">i", len(payload)) + payload

    response = controller_request(port, 0, 0, correlation_id, body)
    payload_body = response[4:]
    if len(payload_body) < 24:
        raise TestError("Produce response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload_body, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload_body, pos)[0]
    if partitions != 1:
        raise TestError(f"Produce partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", payload_body, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", payload_body, pos)[0]
    pos += 2
    base_offset = struct.unpack_from(">q", payload_body, pos)[0]
    if partition != 0 or error_code != 0:
        raise TestError(f"Produce partition={partition} error_code={error_code}")
    return base_offset


def produce_error_code(port, topic, payload, correlation_id):
    body = struct.pack(">h", 1)  # acks
    body += struct.pack(">i", 30000)
    body += struct.pack(">i", 1)
    body += write_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += struct.pack(">i", len(payload)) + payload

    response = controller_request(port, 0, 0, correlation_id, body)
    payload_body = response[4:]
    if len(payload_body) < 24:
        raise TestError("Produce response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload_body, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload_body, pos)[0]
    if partitions != 1:
        raise TestError(f"Produce partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", payload_body, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", payload_body, pos)[0]
    if partition != 0:
        raise TestError(f"Produce partition={partition} error_code={error_code}")
    return error_code


def build_record_value(payload, offset_delta):
    body = bytearray()
    body += b"\x00"  # attributes
    body += write_signed_varlong(0)  # timestamp_delta
    body += write_signed_varint(offset_delta)
    body += write_signed_varint(-1)  # null key
    body += write_signed_varint(len(payload))
    body += payload
    body += write_signed_varint(0)  # headers
    return write_signed_varint(len(body)) + bytes(body)


def build_record_batch(
    payloads,
    producer_id,
    producer_epoch,
    base_sequence,
    timestamp_ms=None,
    attributes=0,
):
    if isinstance(payloads, (bytes, bytearray)):
        payloads = [bytes(payloads)]
    if not payloads:
        raise TestError("record batch requires at least one payload")
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)

    records = b"".join(
        build_record_value(payload, offset_delta)
        for offset_delta, payload in enumerate(payloads)
    )
    batch_length = 49 + len(records)
    header = bytearray()
    header += struct.pack(">q", 0)  # base_offset, assigned by broker storage
    header += struct.pack(">i", batch_length)
    header += struct.pack(">i", 0)  # partition_leader_epoch
    header += struct.pack(">b", 2)  # magic
    header += struct.pack(">I", 0)  # CRC placeholder; broker treats zero as unchecked
    header += struct.pack(">h", attributes)
    header += struct.pack(">i", len(payloads) - 1)
    header += struct.pack(">q", timestamp_ms)
    header += struct.pack(">q", timestamp_ms)
    header += struct.pack(">q", producer_id)
    header += struct.pack(">h", producer_epoch)
    header += struct.pack(">i", base_sequence)
    header += struct.pack(">i", len(payloads))
    return bytes(header) + records


def parse_produce_v9_response(response, correlation_id, topic):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"Produce v9 correlation mismatch: {response_correlation}")
    pos = skip_tags(response, pos)
    topics, pos = read_compact_array_len(response, pos)
    if topics != 1:
        raise TestError(f"Produce v9 topic response count={topics}")
    topic_name, pos = read_compact_string(response, pos)
    partitions, pos = read_compact_array_len(response, pos)
    if topic_name != topic or partitions != 1:
        raise TestError(f"Produce v9 topic={topic_name!r} partitions={partitions}")
    partition, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    base_offset, pos = read_i64(response, pos)
    _, pos = read_i64(response, pos)  # log_append_time_ms
    _, pos = read_i64(response, pos)  # log_start_offset
    record_errors, pos = read_compact_array_len(response, pos)
    for _ in range(record_errors):
        _, pos = read_i32(response, pos)
        _, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
    error_message, pos = read_compact_string(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    pos = skip_tags(response, pos)
    if partition != 0:
        raise TestError(f"Produce v9 partition={partition} error_code={error_code}")
    if pos != len(response):
        raise TestError(f"Produce v9 response trailing bytes: {len(response) - pos}")
    return {
        "error_code": error_code,
        "base_offset": base_offset,
        "error_message": error_message,
    }


def produce_record_batch_result(port, topic, record_batch, correlation_id):
    body = write_compact_string(None)  # transactional_id
    body += struct.pack(">h", 1)  # acks
    body += struct.pack(">i", 30000)
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack(">i", 0)
    body += write_compact_bytes(record_batch)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields

    header = struct.pack(">hhi", 0, 9, correlation_id)
    header += write_compact_string("kraft-idempotent-producer")
    header += b"\x00"
    frame_body = header + body

    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(struct.pack(">I", len(frame_body)) + frame_body)
        response_size = struct.unpack(">I", read_exact(sock, 4))[0]
        if response_size <= 0 or response_size > 1024 * 1024:
            raise TestError(f"invalid Produce v9 response frame size {response_size}")
        response = read_exact(sock, response_size)

    return parse_produce_v9_response(response, correlation_id, topic)


def wait_for_record_batch_result(port, topic, record_batch, expected_error, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7100
    last_error = None
    last_result = None
    while time.time() < deadline:
        try:
            last_result = produce_record_batch_result(
                port,
                topic,
                record_batch,
                correlation_id,
            )
            if last_result["error_code"] == expected_error:
                return last_result
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"Produce v9 to {topic!r} did not return {expected_error}: "
        f"last_result={last_result} last_error={last_error}"
    )


def wait_for_produce_error(port, topic, payload, expected_error, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 4300
    last_error = None
    last_code = None
    while time.time() < deadline:
        try:
            last_code = produce_error_code(port, topic, payload, correlation_id)
            if last_code == expected_error:
                return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"Produce to {topic!r} did not return {expected_error}: "
        f"last_code={last_code} last_error={last_error}"
    )


def fetch_records(port, topic, offset, correlation_id):
    body = struct.pack(">i", -1)  # replica_id
    body += struct.pack(">i", 5000)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 1)
    body += write_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += struct.pack(">q", offset)
    body += struct.pack(">i", 1024 * 1024)

    response = controller_request(port, 1, 0, correlation_id, body)
    payload_body = response[4:]
    if len(payload_body) < 30:
        raise TestError("Fetch response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload_body, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload_body, pos)[0]
    if partitions != 1:
        raise TestError(f"Fetch partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", payload_body, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", payload_body, pos)[0]
    pos += 2
    high_watermark = struct.unpack_from(">q", payload_body, pos)[0]
    pos += 8
    record_len = struct.unpack_from(">i", payload_body, pos)[0]
    pos += 4
    if partition != 0 or error_code != 0:
        raise TestError(f"Fetch partition={partition} error_code={error_code}")
    if record_len < 0 or pos + record_len > len(payload_body):
        raise TestError(f"Fetch invalid record_len={record_len}")
    return high_watermark, payload_body[pos : pos + record_len]


def alter_partition_reassignment(port, topic, partition, replicas, correlation_id):
    body = struct.pack(">i", 30000)
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack(">i", partition)
    body += write_compact_i32_array(replicas) if replicas is not None else b"\x00"
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 45, correlation_id, body, api_version=0)
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    top_error, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    topics_len, pos = read_compact_array_len(response, pos)
    if top_error != 0 or topics_len != 1:
        raise TestError(f"AlterPartitionReassignments top_error={top_error} topics={topics_len}")
    topic_name, pos = read_compact_string(response, pos)
    partitions_len, pos = read_compact_array_len(response, pos)
    if topic_name != topic or partitions_len != 1:
        raise TestError(
            f"AlterPartitionReassignments topic={topic_name!r} partitions={partitions_len}"
        )
    response_partition, pos = read_i32(response, pos)
    partition_error, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    if response_partition != partition:
        raise TestError(f"AlterPartitionReassignments partition={response_partition}")
    if partition_error != 0:
        raise TestError(f"AlterPartitionReassignments partition_error={partition_error}")


def list_partition_reassignment(port, topic, partition, correlation_id):
    body = struct.pack(">i", 30000)
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_i32_array([partition])
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 46, correlation_id, body, api_version=0)
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    top_error, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    topics_len, pos = read_compact_array_len(response, pos)
    if top_error != 0:
        raise TestError(f"ListPartitionReassignments top_error={top_error}")
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
    return {
        "replicas": replicas,
        "adding": adding,
        "removing": removing,
    }


def wait_for_partition_reassignment(port, topic, partition, expected_replicas, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 4600
    last_error = None
    last_state = None
    while time.time() < deadline:
        try:
            last_state = list_partition_reassignment(port, topic, partition, correlation_id)
            if last_state is not None and last_state["replicas"] == expected_replicas:
                return last_state
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"partition reassignment for {topic}-{partition} did not converge to "
        f"{expected_replicas}: last_state={last_state} last_error={last_error}"
    )


def wait_for_produce(port, topic, payload, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 4000
    last_error = None
    while time.time() < deadline:
        try:
            return produce(port, topic, payload, correlation_id)
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"produce did not succeed within {timeout}s: {last_error}")


def wait_for_payloads(port, topic, payloads, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6000
    last_error = None
    last_high_watermark = None
    last_records = b""
    while time.time() < deadline:
        try:
            high_watermark, records = fetch_records(port, topic, 0, correlation_id)
            last_high_watermark = high_watermark
            last_records = records
            if all(payload in records for payload in payloads):
                return records
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    missing = [payload for payload in payloads if payload not in last_records]
    if last_error is not None:
        raise TestError(
            f"missing payloads after fetch retry: {missing!r}; "
            f"last_high_watermark={last_high_watermark}; last_error={last_error}"
        )
    raise TestError(
        f"missing payloads after fetch retry: {missing!r}; "
        f"last_high_watermark={last_high_watermark}"
    )


def wait_for_payload_counts(port, topic, expected_counts, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7200
    last_error = None
    last_counts = {}
    last_high_watermark = None
    while time.time() < deadline:
        try:
            high_watermark, records = fetch_records(port, topic, 0, correlation_id)
            last_high_watermark = high_watermark
            last_counts = {
                payload: records.count(payload)
                for payload in expected_counts
            }
            if all(last_counts[payload] == count for payload, count in expected_counts.items()):
                return {"high_watermark": high_watermark, "counts": last_counts}
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"payload counts for {topic!r} did not converge: expected={expected_counts} "
        f"last_counts={last_counts} last_high_watermark={last_high_watermark} "
        f"last_error={last_error}"
    )


def parse_offset_commit_response(response, correlation_id, expected_topic):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"OffsetCommit correlation mismatch: {response_correlation}")
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topics, pos = read_i32(response, pos)
    if topics != 1:
        raise TestError(f"OffsetCommit topic response count={topics}")
    topic_name, pos = read_string(response, pos)
    partitions, pos = read_i32(response, pos)
    if topic_name != expected_topic or partitions != 1:
        raise TestError(
            f"OffsetCommit topic={topic_name!r} partitions={partitions}"
        )
    partition, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    if partition != 0:
        raise TestError(f"OffsetCommit partition={partition} error_code={error_code}")
    return error_code


def commit_offset(port, group, topic, offset, correlation_id):
    body = write_string(group)
    body += struct.pack(">i", -1)  # generation_id: simple commit
    body += write_string("")  # member_id
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)  # partition_index
    body += struct.pack(">q", offset)
    body += write_string("kraft-failover")

    response = controller_request(port, 8, 5, correlation_id, body)
    error_code = parse_offset_commit_response(response, correlation_id, topic)
    if error_code != 0:
        raise TestError(f"OffsetCommit error_code={error_code}")


def parse_offset_fetch_response_status(response, correlation_id, expected_topic):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"OffsetFetch correlation mismatch: {response_correlation}")
    topics, pos = read_i32(response, pos)
    if topics != 1:
        raise TestError(f"OffsetFetch topic response count={topics}")
    topic_name, pos = read_string(response, pos)
    partitions, pos = read_i32(response, pos)
    if topic_name != expected_topic or partitions != 1:
        raise TestError(f"OffsetFetch topic={topic_name!r} partitions={partitions}")
    partition, pos = read_i32(response, pos)
    committed, pos = read_i64(response, pos)
    metadata, pos = read_string(response, pos)
    error_code, pos = read_i16(response, pos)
    return {
        "partition": partition,
        "offset": committed,
        "metadata": metadata,
        "error_code": error_code,
    }


def parse_offset_fetch_response(response, correlation_id, expected_topic):
    result = parse_offset_fetch_response_status(
        response, correlation_id, expected_topic
    )
    if result["partition"] != 0 or result["error_code"] != 0:
        raise TestError(
            f"OffsetFetch partition={result['partition']} "
            f"error_code={result['error_code']}"
        )
    return result


def fetch_committed_offset(port, group, topic, correlation_id):
    body = write_string(group)
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)

    response = controller_request(port, 9, 1, correlation_id, body)
    return parse_offset_fetch_response(response, correlation_id, topic)["offset"]


def fetch_committed_offset_status(port, group, topic, correlation_id):
    body = write_string(group)
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)

    response = controller_request(port, 9, 1, correlation_id, body)
    return parse_offset_fetch_response_status(response, correlation_id, topic)


def wait_for_committed_offset(port, group, topic, expected_offset, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6200
    last_offset = None
    last_error = None
    while time.time() < deadline:
        try:
            last_offset = fetch_committed_offset(port, group, topic, correlation_id)
            if last_offset == expected_offset:
                return last_offset
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"committed offset did not converge: expected={expected_offset} "
        f"last_offset={last_offset} last_error={last_error}"
    )


def wait_for_committed_offset_error(
    port, group, topic, expected_error, expected_offset=-1, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 6250
    last_result = None
    last_error = None
    while time.time() < deadline:
        try:
            last_result = fetch_committed_offset_status(
                port, group, topic, correlation_id
            )
            if (
                last_result["partition"] == 0
                and last_result["offset"] == expected_offset
                and last_result["error_code"] == expected_error
            ):
                return last_result
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"committed offset error did not converge: expected_error={expected_error} "
        f"expected_offset={expected_offset} last_result={last_result} "
        f"last_error={last_error}"
    )


def wait_for_offset_commit(port, group, topic, expected_offset, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6300
    last_error = None
    while time.time() < deadline:
        try:
            commit_offset(port, group, topic, expected_offset, correlation_id)
            return wait_for_committed_offset(
                port,
                group,
                topic,
                expected_offset,
                timeout=max(1, int(deadline - time.time())),
            )
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"OffsetCommit did not persist offset {expected_offset}: {last_error}"
    )


def parse_offset_fetch_grouped_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    group_count, pos = read_compact_array_len(response, pos)
    groups = []
    for _ in range(group_count):
        group_id, pos = read_compact_string(response, pos)
        topic_count, pos = read_compact_array_len(response, pos)
        topics = []
        for _ in range(topic_count):
            topic_name, pos = read_compact_string(response, pos)
            partition_count, pos = read_compact_array_len(response, pos)
            partitions = []
            for _ in range(partition_count):
                partition_index, pos = read_i32(response, pos)
                committed_offset, pos = read_i64(response, pos)
                committed_leader_epoch, pos = read_i32(response, pos)
                metadata, pos = read_compact_string(response, pos)
                error_code, pos = read_i16(response, pos)
                pos = skip_tags(response, pos)
                partitions.append(
                    {
                        "partition": partition_index,
                        "offset": committed_offset,
                        "leader_epoch": committed_leader_epoch,
                        "metadata": metadata,
                        "error_code": error_code,
                    }
                )
            pos = skip_tags(response, pos)
            topics.append({"name": topic_name, "partitions": partitions})
        error_code, pos = read_i16(response, pos)
        pos = skip_tags(response, pos)
        groups.append(
            {
                "group_id": group_id,
                "topics": topics,
                "error_code": error_code,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"OffsetFetch v8 response trailing bytes: {len(response) - pos}"
        )
    return groups


def offset_fetch_grouped(
    port, group_requests, correlation_id, require_stable=False, api_version=8
):
    body = bytearray(write_compact_array_len(len(group_requests)))
    for group_request in group_requests:
        body += write_compact_string(group_request["group_id"])
        if api_version >= 9:
            body += write_compact_string(group_request.get("member_id"))
            body += struct.pack(">i", group_request.get("member_epoch", -1))
        topics = group_request.get("topics")
        if topics is None:
            body += b"\x00"  # null topics fetches all committed offsets.
        else:
            body += write_compact_array_len(len(topics))
            for topic_request in topics:
                body += write_compact_string(topic_request["name"])
                body += write_compact_i32_array(topic_request["partitions"])
                body += b"\x00"  # topic tagged fields
        body += b"\x00"  # group tagged fields
    body += b"\x01" if require_stable else b"\x00"
    body += b"\x00"  # request tagged fields

    response = flexible_kafka_request(port, 9, api_version, correlation_id, bytes(body))
    return parse_offset_fetch_grouped_response(response, correlation_id)


def assert_offset_fetch_grouped(
    port, group_requests, expected_groups, correlation_id, api_version=8
):
    groups = offset_fetch_grouped(
        port, group_requests, correlation_id, api_version=api_version
    )
    api_label = f"OffsetFetch v{api_version}"
    if len(groups) != len(expected_groups):
        raise TestError(
            f"{api_label} group count={len(groups)} expected={len(expected_groups)} "
            f"groups={groups}"
        )

    for group_idx, (group, expected_group) in enumerate(zip(groups, expected_groups)):
        if group["group_id"] != expected_group["group_id"]:
            raise TestError(
                f"{api_label} group[{group_idx}] id={group['group_id']!r} "
                f"expected={expected_group['group_id']!r}"
            )
        expected_group_error = expected_group.get("error_code", 0)
        if group["error_code"] != expected_group_error:
            raise TestError(
                f"{api_label} group[{group_idx}] error={group['error_code']} "
                f"expected={expected_group_error} group={group}"
            )

        expected_topics = expected_group.get("topics", [])
        if len(group["topics"]) != len(expected_topics):
            raise TestError(
                f"{api_label} group[{group_idx}] topic count={len(group['topics'])} "
                f"expected={len(expected_topics)} group={group}"
            )
        for topic_idx, (actual_topic, expected_topic) in enumerate(
            zip(group["topics"], expected_topics)
        ):
            if actual_topic["name"] != expected_topic["name"]:
                raise TestError(
                    f"{api_label} group[{group_idx}] topic[{topic_idx}] "
                    f"name={actual_topic['name']!r} expected={expected_topic['name']!r}"
                )
            expected_partitions = expected_topic.get("partitions", [])
            if len(actual_topic["partitions"]) != len(expected_partitions):
                raise TestError(
                    f"{api_label} group[{group_idx}] topic[{topic_idx}] "
                    f"partition count={len(actual_topic['partitions'])} "
                    f"expected={len(expected_partitions)} topic={actual_topic}"
                )
            for partition_idx, (actual_partition, expected_partition) in enumerate(
                zip(actual_topic["partitions"], expected_partitions)
            ):
                for key in ("partition", "offset", "error_code"):
                    if actual_partition[key] != expected_partition[key]:
                        raise TestError(
                            f"{api_label} group[{group_idx}] topic[{topic_idx}] "
                            f"partition[{partition_idx}] {key}={actual_partition[key]} "
                            f"expected={expected_partition[key]} "
                            f"partition={actual_partition}"
                        )
                if "metadata" in expected_partition and (
                    actual_partition["metadata"] != expected_partition["metadata"]
                ):
                    raise TestError(
                        f"{api_label} group[{group_idx}] topic[{topic_idx}] "
                        f"partition[{partition_idx}] metadata="
                        f"{actual_partition['metadata']!r} "
                        f"expected={expected_partition['metadata']!r}"
                    )


def wait_for_offset_fetch_grouped_checkpoint(
    port,
    group,
    topic,
    committed_offset,
    offset_delete_group,
    delete_groups_group,
    txn_offset_group,
    txn_offset_committed_offset,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8050
    last_error = None
    group_requests = [
        {
            "group_id": group,
            "topics": [{"name": topic, "partitions": [0]}],
        },
        {
            "group_id": group,
            "topics": None,
        },
        {
            "group_id": offset_delete_group,
            "topics": [{"name": topic, "partitions": [0]}],
        },
        {
            "group_id": delete_groups_group,
            "topics": [{"name": topic, "partitions": [0]}],
        },
        {
            "group_id": txn_offset_group,
            "topics": [{"name": topic, "partitions": [0]}],
        },
    ]
    committed_topic = {
        "name": topic,
        "partitions": [
            {
                "partition": 0,
                "offset": committed_offset,
                "metadata": "kraft-failover",
                "error_code": 0,
            }
        ],
    }
    expected_groups = [
        {
            "group_id": group,
            "topics": [committed_topic],
        },
        {
            "group_id": group,
            "topics": [committed_topic],
        },
        {
            "group_id": offset_delete_group,
            "topics": [
                {
                    "name": topic,
                    "partitions": [
                        {
                            "partition": 0,
                            "offset": -1,
                            "metadata": None,
                            "error_code": 0,
                        }
                    ],
                }
            ],
        },
        {
            "group_id": delete_groups_group,
            "topics": [],
            "error_code": ERROR_GROUP_ID_NOT_FOUND,
        },
        {
            "group_id": txn_offset_group,
            "topics": [
                {
                    "name": topic,
                    "partitions": [
                        {
                            "partition": 0,
                            "offset": txn_offset_committed_offset,
                            "metadata": "kraft-failover-txn",
                            "error_code": 0,
                        }
                    ],
                }
            ],
        },
    ]

    while time.time() < deadline:
        try:
            assert_offset_fetch_grouped(
                port, group_requests, expected_groups, correlation_id
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"OffsetFetch v8 grouped checkpoint did not recover: {last_error}")


def parse_offset_commit_flexible_response(response, correlation_id, expected_topic):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topic_count, pos = read_compact_array_len(response, pos)
    if topic_count != 1:
        raise TestError(f"OffsetCommit v9 topic response count={topic_count}")
    topic_name, pos = read_compact_string(response, pos)
    partition_count, pos = read_compact_array_len(response, pos)
    if topic_name != expected_topic or partition_count != 1:
        raise TestError(
            f"OffsetCommit v9 topic={topic_name!r} partitions={partition_count}"
        )
    partition_index, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"OffsetCommit v9 response trailing bytes: {len(response) - pos}"
        )
    if partition_index != 0:
        raise TestError(
            f"OffsetCommit v9 partition={partition_index} error_code={error_code}"
        )
    return error_code


def offset_commit_v9(
    port,
    group_id,
    member_id,
    member_epoch,
    topic,
    offset,
    metadata,
    correlation_id,
):
    body = bytearray()
    body += write_compact_string(group_id)
    body += struct.pack(">i", member_epoch)
    body += write_compact_string(member_id)
    body += write_compact_string(None)  # group_instance_id
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack(">iqi", 0, offset, -1)
    body += write_compact_string(metadata)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields

    response = flexible_kafka_request(port, 8, 9, correlation_id, bytes(body))
    return parse_offset_commit_flexible_response(response, correlation_id, topic)


def wait_for_offset_commit_v9_member_checkpoint(
    port,
    group_state,
    topic,
    expected_offset,
    expected_metadata,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8250
    last_error = None
    group_id = group_state["group_id"]
    member_id = group_state["member_id"]
    member_epoch = group_state["member_epoch"]

    while time.time() < deadline:
        try:
            valid_error = offset_commit_v9(
                port,
                group_id,
                member_id,
                member_epoch,
                topic,
                expected_offset,
                expected_metadata,
                correlation_id,
            )
            if valid_error != 0:
                raise TestError(f"OffsetCommit v9 valid member error={valid_error}")
            missing_error = offset_commit_v9(
                port,
                group_id,
                f"{member_id}-missing",
                member_epoch,
                topic,
                expected_offset + 100,
                "missing-kip848-member",
                correlation_id + 1,
            )
            if missing_error != ERROR_UNKNOWN_MEMBER_ID:
                raise TestError(
                    f"OffsetCommit v9 missing member error={missing_error}"
                )
            stale_error = offset_commit_v9(
                port,
                group_id,
                member_id,
                member_epoch + 1,
                topic,
                expected_offset + 200,
                "stale-kip848-member-epoch",
                correlation_id + 2,
            )
            if stale_error != ERROR_FENCED_MEMBER_EPOCH:
                raise TestError(f"OffsetCommit v9 stale epoch error={stale_error}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 3
        time.sleep(0.25)
    raise TestError(
        f"OffsetCommit v9 member checkpoint did not recover for {group_id!r}: "
        f"{last_error}"
    )


def wait_for_offset_fetch_v9_member_checkpoint(
    port,
    group_state,
    topic,
    expected_offset=-1,
    expected_metadata=None,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8150
    last_error = None
    group_id = group_state["group_id"]
    member_id = group_state["member_id"]
    member_epoch = group_state["member_epoch"]
    topic_request = {"name": topic, "partitions": [0]}
    empty_offset_topic = {
        "name": topic,
        "partitions": [
            {
                "partition": 0,
                "offset": expected_offset,
                "metadata": expected_metadata,
                "error_code": 0,
            }
        ],
    }
    group_requests = [
        {
            "group_id": group_id,
            "member_id": member_id,
            "member_epoch": member_epoch,
            "topics": [topic_request],
        },
        {
            "group_id": group_id,
            "member_id": None,
            "member_epoch": -1,
            "topics": [topic_request],
        },
        {
            "group_id": group_id,
            "member_id": f"{member_id}-missing",
            "member_epoch": member_epoch,
            "topics": [topic_request],
        },
        {
            "group_id": group_id,
            "member_id": member_id,
            "member_epoch": member_epoch + 1,
            "topics": [topic_request],
        },
    ]
    expected_groups = [
        {
            "group_id": group_id,
            "topics": [empty_offset_topic],
        },
        {
            "group_id": group_id,
            "topics": [empty_offset_topic],
        },
        {
            "group_id": group_id,
            "topics": [],
            "error_code": ERROR_UNKNOWN_MEMBER_ID,
        },
        {
            "group_id": group_id,
            "topics": [],
            "error_code": ERROR_FENCED_MEMBER_EPOCH,
        },
    ]

    while time.time() < deadline:
        try:
            assert_offset_fetch_grouped(
                port,
                group_requests,
                expected_groups,
                correlation_id,
                api_version=9,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"OffsetFetch v9 member checkpoint did not recover for {group_id!r}: "
        f"{last_error}"
    )


def parse_offset_delete_response(response, correlation_id, expected_topic):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"OffsetDelete correlation mismatch: {response_correlation}")
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topics, pos = read_i32(response, pos)
    if topics != 1:
        raise TestError(f"OffsetDelete topic response count={topics}")
    topic_name, pos = read_string(response, pos)
    partitions, pos = read_i32(response, pos)
    if topic_name != expected_topic or partitions != 1:
        raise TestError(f"OffsetDelete topic={topic_name!r} partitions={partitions}")
    partition, pos = read_i32(response, pos)
    partition_error, pos = read_i16(response, pos)
    if pos != len(response):
        raise TestError(f"OffsetDelete response trailing bytes: {len(response) - pos}")
    if top_error != 0:
        raise TestError(f"OffsetDelete top-level error_code={top_error}")
    if partition != 0:
        raise TestError(
            f"OffsetDelete partition={partition} error_code={partition_error}"
        )
    return partition_error


def delete_offset(port, group, topic, correlation_id):
    body = write_string(group)
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)  # partition_index

    response = controller_request(port, 47, 0, correlation_id, body)
    error_code = parse_offset_delete_response(response, correlation_id, topic)
    if error_code != 0:
        raise TestError(f"OffsetDelete error_code={error_code}")


def wait_for_offset_delete(port, group, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6400
    last_error = None
    while time.time() < deadline:
        try:
            delete_offset(port, group, topic, correlation_id)
            return wait_for_committed_offset(
                port,
                group,
                topic,
                -1,
                timeout=max(1, int(deadline - time.time())),
            )
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"OffsetDelete did not remove offset for {group!r}: {last_error}")


def parse_init_producer_id_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"InitProducerId correlation mismatch: {response_correlation}")
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    producer_id, pos = read_i64(response, pos)
    producer_epoch, pos = read_i16(response, pos)
    if error_code != 0:
        raise TestError(f"InitProducerId error_code={error_code}")
    if producer_id < 0 or producer_epoch < 0:
        raise TestError(
            f"InitProducerId invalid producer identity id={producer_id} epoch={producer_epoch}"
        )
    return {"producer_id": producer_id, "producer_epoch": producer_epoch}


def init_producer_id(port, transactional_id, correlation_id):
    body = write_string(transactional_id)
    body += struct.pack(">i", 60000)  # transaction_timeout_ms
    response = controller_request(port, 22, 0, correlation_id, body)
    return parse_init_producer_id_response(response, correlation_id)


def wait_for_init_producer_id(port, transactional_id, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7300
    last_error = None
    while time.time() < deadline:
        try:
            return init_producer_id(port, transactional_id, correlation_id)
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"InitProducerId for {transactional_id!r} did not recover: {last_error}")


def parse_add_partitions_to_txn_response(response, correlation_id, expected_topic):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"AddPartitionsToTxn correlation mismatch: {response_correlation}")
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topics, pos = read_i32(response, pos)
    if topics != 1:
        raise TestError(f"AddPartitionsToTxn topic response count={topics}")
    topic_name, pos = read_string(response, pos)
    partitions, pos = read_i32(response, pos)
    if topic_name != expected_topic or partitions != 1:
        raise TestError(
            f"AddPartitionsToTxn topic={topic_name!r} partitions={partitions}"
        )
    partition, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    if partition != 0:
        raise TestError(
            f"AddPartitionsToTxn partition={partition} error_code={error_code}"
        )
    if error_code != 0:
        raise TestError(f"AddPartitionsToTxn error_code={error_code}")


def add_partitions_to_txn(port, transactional_id, producer_id, producer_epoch, topic, correlation_id):
    body = write_string(transactional_id)
    body += struct.pack(">q", producer_id)
    body += struct.pack(">h", producer_epoch)
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)

    response = controller_request(port, 24, 0, correlation_id, body)
    parse_add_partitions_to_txn_response(response, correlation_id, topic)


def parse_end_txn_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"EndTxn correlation mismatch: {response_correlation}")
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    if error_code != 0:
        raise TestError(f"EndTxn error_code={error_code}")


def end_txn(port, transactional_id, producer_id, producer_epoch, committed, correlation_id):
    body = write_string(transactional_id)
    body += struct.pack(">q", producer_id)
    body += struct.pack(">h", producer_epoch)
    body += b"\x01" if committed else b"\x00"

    response = controller_request(port, 26, 0, correlation_id, body)
    parse_end_txn_response(response, correlation_id)


def parse_add_offsets_to_txn_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(
            f"AddOffsetsToTxn correlation mismatch: {response_correlation}"
        )
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    if pos != len(response):
        raise TestError(
            f"AddOffsetsToTxn response trailing bytes: {len(response) - pos}"
        )
    if error_code != 0:
        raise TestError(f"AddOffsetsToTxn error_code={error_code}")


def add_offsets_to_txn(
    port, transactional_id, producer_id, producer_epoch, group_id, correlation_id
):
    body = write_string(transactional_id)
    body += struct.pack(">q", producer_id)
    body += struct.pack(">h", producer_epoch)
    body += write_string(group_id)

    response = controller_request(port, 25, 0, correlation_id, body)
    parse_add_offsets_to_txn_response(response, correlation_id)


def parse_txn_offset_commit_response(response, correlation_id, expected_topic):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(
            f"TxnOffsetCommit correlation mismatch: {response_correlation}"
        )
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topics, pos = read_i32(response, pos)
    if topics != 1:
        raise TestError(f"TxnOffsetCommit topic response count={topics}")
    topic_name, pos = read_string(response, pos)
    partitions, pos = read_i32(response, pos)
    if topic_name != expected_topic or partitions != 1:
        raise TestError(
            f"TxnOffsetCommit topic={topic_name!r} partitions={partitions}"
        )
    partition, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    if pos != len(response):
        raise TestError(
            f"TxnOffsetCommit response trailing bytes: {len(response) - pos}"
        )
    if partition != 0:
        raise TestError(
            f"TxnOffsetCommit partition={partition} error_code={error_code}"
        )
    if error_code != 0:
        raise TestError(f"TxnOffsetCommit error_code={error_code}")


def txn_offset_commit(
    port,
    transactional_id,
    group_id,
    producer_id,
    producer_epoch,
    topic,
    offset,
    correlation_id,
):
    body = write_string(transactional_id)
    body += write_string(group_id)
    body += struct.pack(">q", producer_id)
    body += struct.pack(">h", producer_epoch)
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)  # partition_index
    body += struct.pack(">q", offset)
    body += write_string("kraft-failover-txn")

    response = controller_request(port, 28, 0, correlation_id, body)
    parse_txn_offset_commit_response(response, correlation_id, topic)


def begin_transaction(port, transactional_id, topic, correlation_id):
    identity = init_producer_id(port, transactional_id, correlation_id)
    add_partitions_to_txn(
        port,
        transactional_id,
        identity["producer_id"],
        identity["producer_epoch"],
        topic,
        correlation_id + 1,
    )
    return {
        "transactional_id": transactional_id,
        "producer_id": identity["producer_id"],
        "producer_epoch": identity["producer_epoch"],
        "topic": topic,
    }


def begin_offset_transaction(port, transactional_id, group_id, topic, offset, correlation_id):
    identity = init_producer_id(port, transactional_id, correlation_id)
    add_offsets_to_txn(
        port,
        transactional_id,
        identity["producer_id"],
        identity["producer_epoch"],
        group_id,
        correlation_id + 1,
    )
    txn_offset_commit(
        port,
        transactional_id,
        group_id,
        identity["producer_id"],
        identity["producer_epoch"],
        topic,
        offset,
        correlation_id + 2,
    )
    return {
        "transactional_id": transactional_id,
        "producer_id": identity["producer_id"],
        "producer_epoch": identity["producer_epoch"],
        "group_id": group_id,
        "topic": topic,
        "offset": offset,
    }


def wait_for_transaction_begin(port, transactional_id, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6400
    last_error = None
    while time.time() < deadline:
        try:
            return begin_transaction(port, transactional_id, topic, correlation_id)
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(f"transaction {transactional_id!r} did not begin: {last_error}")


def wait_for_offset_transaction_begin(
    port, transactional_id, group_id, topic, offset, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7000
    last_error = None
    while time.time() < deadline:
        try:
            txn = begin_offset_transaction(
                port, transactional_id, group_id, topic, offset, correlation_id
            )
            wait_for_committed_offset(
                port,
                group_id,
                topic,
                offset,
                timeout=max(1, int(deadline - time.time())),
            )
            return txn
        except Exception as exc:
            last_error = exc
        correlation_id += 3
        time.sleep(0.25)
    raise TestError(
        f"transactional offset commit {transactional_id!r} did not begin: {last_error}"
    )


def parse_list_transactions_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    unknown_filters_count, pos = read_compact_array_len(response, pos)
    unknown_filters = []
    for _ in range(unknown_filters_count):
        value, pos = read_compact_string(response, pos)
        unknown_filters.append(value)
    transaction_count, pos = read_compact_array_len(response, pos)
    transactions = []
    for _ in range(transaction_count):
        transactional_id, pos = read_compact_string(response, pos)
        producer_id, pos = read_i64(response, pos)
        transaction_state, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        transactions.append(
            {
                "transactional_id": transactional_id,
                "producer_id": producer_id,
                "transaction_state": transaction_state,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ListTransactions response trailing bytes: {len(response) - pos}"
        )
    if error_code != 0:
        raise TestError(f"ListTransactions error_code={error_code}")
    if unknown_filters:
        raise TestError(f"ListTransactions unknown filters={unknown_filters!r}")
    return transactions


def list_transactions(port, producer_id, state, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(state)
    body += write_compact_array_len(1)
    body += struct.pack(">q", producer_id)
    body += struct.pack(">q", -1)  # duration_filter
    body += b"\x00"  # request tagged fields

    response = flexible_kafka_request(port, 66, 1, correlation_id, body)
    return parse_list_transactions_response(response, correlation_id)


def parse_describe_transactions_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    transaction_count, pos = read_compact_array_len(response, pos)
    transactions = []
    for _ in range(transaction_count):
        error_code, pos = read_i16(response, pos)
        transactional_id, pos = read_compact_string(response, pos)
        transaction_state, pos = read_compact_string(response, pos)
        transaction_timeout_ms, pos = read_i32(response, pos)
        transaction_start_time_ms, pos = read_i64(response, pos)
        producer_id, pos = read_i64(response, pos)
        producer_epoch, pos = read_i16(response, pos)
        topic_count, pos = read_compact_array_len(response, pos)
        topics = []
        for _ in range(topic_count):
            topic_name, pos = read_compact_string(response, pos)
            partitions, pos = read_compact_i32_array(response, pos)
            pos = skip_tags(response, pos)
            topics.append({"topic": topic_name, "partitions": partitions})
        pos = skip_tags(response, pos)
        transactions.append(
            {
                "error_code": error_code,
                "transactional_id": transactional_id,
                "transaction_state": transaction_state,
                "transaction_timeout_ms": transaction_timeout_ms,
                "transaction_start_time_ms": transaction_start_time_ms,
                "producer_id": producer_id,
                "producer_epoch": producer_epoch,
                "topics": topics,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeTransactions response trailing bytes: {len(response) - pos}"
        )
    return transactions


def describe_transaction(port, transactional_id, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(transactional_id)
    body += b"\x00"  # request tagged fields

    response = flexible_kafka_request(port, 65, 0, correlation_id, body)
    transactions = parse_describe_transactions_response(response, correlation_id)
    if len(transactions) != 1:
        raise TestError(f"DescribeTransactions count={len(transactions)}")
    return transactions[0]


def assert_transaction_introspection(port, txn, expected_state, expected_topic, correlation_id):
    described = describe_transaction(port, txn["transactional_id"], correlation_id)
    if described["error_code"] != 0:
        raise TestError(
            f"DescribeTransactions {txn['transactional_id']!r} error_code="
            f"{described['error_code']}"
        )
    if described["producer_id"] != txn["producer_id"]:
        raise TestError(
            f"DescribeTransactions producer_id={described['producer_id']} "
            f"expected={txn['producer_id']}"
        )
    if described["producer_epoch"] != txn["producer_epoch"]:
        raise TestError(
            f"DescribeTransactions producer_epoch={described['producer_epoch']} "
            f"expected={txn['producer_epoch']}"
        )
    if described["transaction_state"] != expected_state:
        raise TestError(
            f"DescribeTransactions state={described['transaction_state']!r} "
            f"expected={expected_state!r}"
        )
    if expected_topic is not None:
        matching_topic = next(
            (topic for topic in described["topics"] if topic["topic"] == expected_topic),
            None,
        )
        if matching_topic is None or 0 not in matching_topic["partitions"]:
            raise TestError(
                f"DescribeTransactions missing topic {expected_topic!r}: {described}"
            )

    listed = list_transactions(
        port,
        txn["producer_id"],
        expected_state,
        correlation_id + 1,
    )
    matching = [
        item
        for item in listed
        if item["transactional_id"] == txn["transactional_id"]
        and item["producer_id"] == txn["producer_id"]
        and item["transaction_state"] == expected_state
    ]
    if not matching:
        raise TestError(
            f"ListTransactions missing {txn['transactional_id']!r} "
            f"state={expected_state!r}: {listed}"
        )


def wait_for_transaction_introspection(
    port, txn, expected_state, expected_topic=None, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7200
    last_error = None
    while time.time() < deadline:
        try:
            assert_transaction_introspection(
                port, txn, expected_state, expected_topic, correlation_id
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(
        f"transaction introspection {txn['transactional_id']!r} did not reach "
        f"{expected_state!r}: {last_error}"
    )


def wait_for_transaction_end(port, txn, committed=True, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6600
    last_error = None
    while time.time() < deadline:
        try:
            end_txn(
                port,
                txn["transactional_id"],
                txn["producer_id"],
                txn["producer_epoch"],
                committed,
                correlation_id,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"transaction {txn['transactional_id']!r} did not end cleanly: {last_error}"
    )


def parse_join_group_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"JoinGroup correlation mismatch: {response_correlation}")
    error_code, pos = read_i16(response, pos)
    generation_id, pos = read_i32(response, pos)
    protocol_name, pos = read_string(response, pos)
    leader, pos = read_string(response, pos)
    member_id, pos = read_string(response, pos)
    member_count, pos = read_i32(response, pos)
    members = []
    for _ in range(member_count):
        response_member_id, pos = read_string(response, pos)
        metadata, pos = read_bytes(response, pos)
        members.append({"member_id": response_member_id, "metadata": metadata})
    if error_code != 0:
        raise TestError(f"JoinGroup error_code={error_code}")
    if generation_id < 0 or not member_id:
        raise TestError(
            f"JoinGroup invalid generation/member generation={generation_id} member={member_id!r}"
        )
    return {
        "generation_id": generation_id,
        "protocol_name": protocol_name,
        "leader": leader,
        "member_id": member_id,
        "members": members,
    }


def join_group(port, group_id, correlation_id):
    body = write_string(group_id)
    body += struct.pack(">i", 30000)  # session_timeout_ms
    body += write_string("")  # dynamic member
    body += write_string("consumer")
    body += struct.pack(">i", 1)  # supported protocols
    body += write_string("range")
    body += write_bytes(b"range-metadata")

    response = controller_request(port, 11, 0, correlation_id, body)
    result = parse_join_group_response(response, correlation_id)
    result["group_id"] = group_id
    return result


def parse_sync_group_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"SyncGroup correlation mismatch: {response_correlation}")
    error_code, pos = read_i16(response, pos)
    assignment, pos = read_bytes(response, pos)
    if error_code != 0:
        raise TestError(f"SyncGroup error_code={error_code}")
    return assignment


def sync_group(port, group_state, correlation_id):
    assignment = b"kraft-failover-assignment"
    body = write_string(group_state["group_id"])
    body += struct.pack(">i", group_state["generation_id"])
    body += write_string(group_state["member_id"])
    body += struct.pack(">i", 1)  # assignments
    body += write_string(group_state["member_id"])
    body += write_bytes(assignment)

    response = controller_request(port, 14, 0, correlation_id, body)
    returned_assignment = parse_sync_group_response(response, correlation_id)
    if returned_assignment != assignment:
        raise TestError(f"SyncGroup assignment mismatch: {returned_assignment!r}")


def parse_heartbeat_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"Heartbeat correlation mismatch: {response_correlation}")
    error_code, pos = read_i16(response, pos)
    if error_code != 0:
        raise TestError(f"Heartbeat error_code={error_code}")


def heartbeat_group(port, group_state, correlation_id):
    body = write_string(group_state["group_id"])
    body += struct.pack(">i", group_state["generation_id"])
    body += write_string(group_state["member_id"])

    response = controller_request(port, 12, 0, correlation_id, body)
    parse_heartbeat_response(response, correlation_id)


def parse_describe_groups_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"DescribeGroups correlation mismatch: {response_correlation}")
    group_count, pos = read_i32(response, pos)
    groups = []
    for _ in range(group_count):
        error_code, pos = read_i16(response, pos)
        group_id, pos = read_string(response, pos)
        group_state, pos = read_string(response, pos)
        protocol_type, pos = read_string(response, pos)
        protocol_data, pos = read_string(response, pos)
        member_count, pos = read_i32(response, pos)
        members = []
        for _ in range(member_count):
            member_id, pos = read_string(response, pos)
            client_id, pos = read_string(response, pos)
            client_host, pos = read_string(response, pos)
            member_metadata, pos = read_bytes(response, pos)
            member_assignment, pos = read_bytes(response, pos)
            members.append(
                {
                    "member_id": member_id,
                    "client_id": client_id,
                    "client_host": client_host,
                    "member_metadata": member_metadata,
                    "member_assignment": member_assignment,
                }
            )
        groups.append(
            {
                "error_code": error_code,
                "group_id": group_id,
                "group_state": group_state,
                "protocol_type": protocol_type,
                "protocol_data": protocol_data,
                "members": members,
            }
        )
    if pos != len(response):
        raise TestError(f"DescribeGroups response trailing bytes: {len(response) - pos}")
    return groups


def describe_group(port, group_id, correlation_id):
    body = struct.pack(">i", 1)
    body += write_string(group_id)
    response = controller_request(port, 15, 0, correlation_id, body)
    groups = parse_describe_groups_response(response, correlation_id)
    if len(groups) != 1:
        raise TestError(f"DescribeGroups count={len(groups)}")
    return groups[0]


def assert_group_description(port, group_state, correlation_id):
    described = describe_group(port, group_state["group_id"], correlation_id)
    if described["error_code"] != 0:
        raise TestError(
            f"DescribeGroups {group_state['group_id']!r} error_code="
            f"{described['error_code']}"
        )
    if described["group_state"] != "Stable":
        raise TestError(f"DescribeGroups state={described['group_state']!r}")
    if described["protocol_type"] != "consumer" or described["protocol_data"] != "range":
        raise TestError(f"DescribeGroups protocol mismatch: {described}")
    matching_member = next(
        (
            member
            for member in described["members"]
            if member["member_id"] == group_state["member_id"]
        ),
        None,
    )
    if matching_member is None:
        raise TestError(f"DescribeGroups missing member: {described}")
    if matching_member["member_metadata"] != b"range-metadata":
        raise TestError(f"DescribeGroups member metadata mismatch: {matching_member}")
    if matching_member["member_assignment"] != b"kraft-failover-assignment":
        raise TestError(f"DescribeGroups member assignment mismatch: {matching_member}")


def wait_for_group_description(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7300
    last_error = None
    while time.time() < deadline:
        try:
            assert_group_description(port, group_state, correlation_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"group description {group_state['group_id']!r} was not stable: {last_error}"
    )


def parse_consumer_group_assignment(response, pos):
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        topic_name, pos = read_compact_string(response, pos)
        partitions, pos = read_compact_i32_array(response, pos)
        pos = skip_tags(response, pos)
        topics.append(
            {
                "topic_id": topic_id,
                "topic_name": topic_name,
                "partitions": partitions,
            }
        )
    pos = skip_tags(response, pos)
    return {"topic_partitions": topics}, pos


def parse_consumer_group_describe_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    group_count, pos = read_compact_array_len(response, pos)
    groups = []
    for _ in range(group_count):
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        group_id, pos = read_compact_string(response, pos)
        group_state, pos = read_compact_string(response, pos)
        group_epoch, pos = read_i32(response, pos)
        assignment_epoch, pos = read_i32(response, pos)
        assignor_name, pos = read_compact_string(response, pos)
        member_count, pos = read_compact_array_len(response, pos)
        members = []
        for _ in range(member_count):
            member_id, pos = read_compact_string(response, pos)
            instance_id, pos = read_compact_string(response, pos)
            rack_id, pos = read_compact_string(response, pos)
            member_epoch, pos = read_i32(response, pos)
            client_id, pos = read_compact_string(response, pos)
            client_host, pos = read_compact_string(response, pos)
            subscribed_count, pos = read_compact_array_len(response, pos)
            subscribed_topics = []
            for _ in range(subscribed_count):
                topic_name, pos = read_compact_string(response, pos)
                subscribed_topics.append(topic_name)
            subscribed_regex, pos = read_compact_string(response, pos)
            assignment, pos = parse_consumer_group_assignment(response, pos)
            target_assignment, pos = parse_consumer_group_assignment(response, pos)
            pos = skip_tags(response, pos)
            members.append(
                {
                    "member_id": member_id,
                    "instance_id": instance_id,
                    "rack_id": rack_id,
                    "member_epoch": member_epoch,
                    "client_id": client_id,
                    "client_host": client_host,
                    "subscribed_topics": subscribed_topics,
                    "subscribed_regex": subscribed_regex,
                    "assignment": assignment,
                    "target_assignment": target_assignment,
                }
            )
        authorized_operations, pos = read_i32(response, pos)
        pos = skip_tags(response, pos)
        groups.append(
            {
                "error_code": error_code,
                "error_message": error_message,
                "group_id": group_id,
                "group_state": group_state,
                "group_epoch": group_epoch,
                "assignment_epoch": assignment_epoch,
                "assignor_name": assignor_name,
                "members": members,
                "authorized_operations": authorized_operations,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ConsumerGroupDescribe response trailing bytes: {len(response) - pos}"
        )
    return groups


def consumer_group_describe(port, group_id, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(group_id)
    body += b"\x00"  # include_authorized_operations=false
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 69, 0, correlation_id, body)
    groups = parse_consumer_group_describe_response(response, correlation_id)
    if len(groups) != 1:
        raise TestError(f"ConsumerGroupDescribe count={len(groups)}")
    return groups[0]


def assert_consumer_group_description(port, group_state, correlation_id):
    described = consumer_group_describe(port, group_state["group_id"], correlation_id)
    if described["error_code"] != 0:
        raise TestError(
            f"ConsumerGroupDescribe {group_state['group_id']!r} error_code="
            f"{described['error_code']} message={described['error_message']!r}"
        )
    if described["group_state"] != "Stable":
        raise TestError(f"ConsumerGroupDescribe state={described['group_state']!r}")
    if described["group_epoch"] != group_state["generation_id"]:
        raise TestError(
            f"ConsumerGroupDescribe group_epoch={described['group_epoch']} "
            f"expected={group_state['generation_id']}"
        )
    if described["assignment_epoch"] != group_state["generation_id"]:
        raise TestError(
            f"ConsumerGroupDescribe assignment_epoch={described['assignment_epoch']} "
            f"expected={group_state['generation_id']}"
        )
    if described["assignor_name"] != "range":
        raise TestError(f"ConsumerGroupDescribe assignor mismatch: {described}")
    matching_member = next(
        (
            member
            for member in described["members"]
            if member["member_id"] == group_state["member_id"]
        ),
        None,
    )
    if matching_member is None:
        raise TestError(f"ConsumerGroupDescribe missing member: {described}")
    if matching_member["member_epoch"] != group_state["generation_id"]:
        raise TestError(
            f"ConsumerGroupDescribe member_epoch={matching_member['member_epoch']} "
            f"expected={group_state['generation_id']}"
        )


def wait_for_consumer_group_description(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7400
    last_error = None
    while time.time() < deadline:
        try:
            assert_consumer_group_description(port, group_state, correlation_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"consumer group description {group_state['group_id']!r} was not stable: "
        f"{last_error}"
    )


def parse_list_groups_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    group_count, pos = read_compact_array_len(response, pos)
    groups = []
    for _ in range(group_count):
        group_id, pos = read_compact_string(response, pos)
        protocol_type, pos = read_compact_string(response, pos)
        group_state, pos = read_compact_string(response, pos)
        group_type, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        groups.append(
            {
                "group_id": group_id,
                "protocol_type": protocol_type,
                "group_state": group_state,
                "group_type": group_type,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"ListGroups response trailing bytes: {len(response) - pos}")
    return {"error_code": error_code, "groups": groups}


def list_groups(port, states, group_types, correlation_id):
    body = write_compact_array_len(len(states))
    for state in states:
        body += write_compact_string(state)
    body += write_compact_array_len(len(group_types))
    for group_type in group_types:
        body += write_compact_string(group_type)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 16, 5, correlation_id, body)
    return parse_list_groups_response(response, correlation_id)


def assert_list_groups_contains(port, group_state, correlation_id):
    listed = list_groups(port, ["Stable"], ["classic"], correlation_id)
    if listed["error_code"] != 0:
        raise TestError(f"ListGroups error_code={listed['error_code']}")
    matching_group = next(
        (
            group
            for group in listed["groups"]
            if group["group_id"] == group_state["group_id"]
        ),
        None,
    )
    if matching_group is None:
        raise TestError(f"ListGroups missing {group_state['group_id']!r}: {listed}")
    if matching_group["protocol_type"] != "consumer":
        raise TestError(f"ListGroups protocol mismatch: {matching_group}")
    if matching_group["group_state"] != "Stable":
        raise TestError(f"ListGroups state mismatch: {matching_group}")
    if matching_group["group_type"] != "classic":
        raise TestError(f"ListGroups type mismatch: {matching_group}")


def wait_for_list_groups(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7500
    last_error = None
    while time.time() < deadline:
        try:
            assert_list_groups_contains(port, group_state, correlation_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ListGroups did not retain {group_state['group_id']!r}: {last_error}"
    )


def parse_find_coordinator_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    coordinator_count, pos = read_compact_array_len(response, pos)
    coordinators = []
    for _ in range(coordinator_count):
        key, pos = read_compact_string(response, pos)
        node_id, pos = read_i32(response, pos)
        host, pos = read_compact_string(response, pos)
        coordinator_port, pos = read_i32(response, pos)
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        coordinators.append(
            {
                "key": key,
                "node_id": node_id,
                "host": host,
                "port": coordinator_port,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"FindCoordinator response trailing bytes: {len(response) - pos}"
        )
    return coordinators


def find_coordinator(port, coordinator_key, key_type, correlation_id):
    body = struct.pack(">b", key_type)
    body += write_compact_array_len(1)
    body += write_compact_string(coordinator_key)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 10, 4, correlation_id, body)
    coordinators = parse_find_coordinator_response(response, correlation_id)
    if len(coordinators) != 1:
        raise TestError(f"FindCoordinator count={len(coordinators)}")
    return coordinators[0]


def assert_coordinator(port, coordinator_key, key_type, correlation_id):
    coordinator = find_coordinator(port, coordinator_key, key_type, correlation_id)
    if coordinator["key"] != coordinator_key:
        raise TestError(
            f"FindCoordinator key mismatch: expected={coordinator_key!r} "
            f"got={coordinator['key']!r}"
        )
    if coordinator["error_code"] != 0:
        raise TestError(
            f"FindCoordinator {coordinator_key!r} key_type={key_type} "
            f"error_code={coordinator['error_code']} "
            f"message={coordinator['error_message']!r}"
        )
    if coordinator["node_id"] != 100:
        raise TestError(f"FindCoordinator node mismatch: {coordinator}")
    if coordinator["host"] != "localhost":
        raise TestError(f"FindCoordinator host mismatch: {coordinator}")
    if coordinator["port"] != BROKER_PORT:
        raise TestError(f"FindCoordinator port mismatch: {coordinator}")


def wait_for_coordinator_discovery(
    port, group_id, transactional_id, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7600
    last_error = None
    while time.time() < deadline:
        try:
            assert_coordinator(port, group_id, 0, correlation_id)
            assert_coordinator(port, transactional_id, 1, correlation_id + 1)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(
        f"FindCoordinator did not recover for group={group_id!r} "
        f"transactional_id={transactional_id!r}: {last_error}"
    )


def parse_consumer_group_heartbeat_assignment(response, pos):
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading heartbeat topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        partitions, pos = read_compact_i32_array(response, pos)
        pos = skip_tags(response, pos)
        topics.append({"topic_id": topic_id, "partitions": partitions})
    pos = skip_tags(response, pos)
    return {"topic_partitions": topics}, pos


def parse_consumer_group_heartbeat_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    member_id, pos = read_compact_string(response, pos)
    member_epoch, pos = read_i32(response, pos)
    heartbeat_interval_ms, pos = read_i32(response, pos)
    assignment_present, pos = read_varint(response, pos)
    assignment = None
    if assignment_present != 0:
        assignment, pos = parse_consumer_group_heartbeat_assignment(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ConsumerGroupHeartbeat response trailing bytes: {len(response) - pos}"
        )
    return {
        "error_code": error_code,
        "error_message": error_message,
        "member_id": member_id,
        "member_epoch": member_epoch,
        "heartbeat_interval_ms": heartbeat_interval_ms,
        "assignment": assignment,
    }


def write_consumer_group_heartbeat_topic_partitions(topic_partitions):
    if topic_partitions is None:
        return b"\x00"
    out = bytearray(write_compact_array_len(len(topic_partitions)))
    for topic in topic_partitions:
        topic_id = topic["topic_id"]
        if len(topic_id) != 16:
            raise TestError(f"invalid heartbeat topic id length {len(topic_id)}")
        out += topic_id
        out += write_compact_i32_array(topic["partitions"])
        out += b"\x00"  # topic tagged fields
    return bytes(out)


def consumer_group_heartbeat(
    port,
    group_id,
    member_id,
    member_epoch,
    correlation_id,
    subscribed_topics=None,
    server_assignor=None,
    topic_partitions=None,
):
    body = write_compact_string(group_id)
    body += write_compact_string(member_id)
    body += struct.pack(">i", member_epoch)
    body += write_compact_string(None)  # instance_id
    body += write_compact_string(None)  # rack_id
    body += struct.pack(">i", 30000 if member_epoch == 0 else -1)
    if subscribed_topics is None:
        body += b"\x00"
    else:
        body += write_compact_array_len(len(subscribed_topics))
        for subscribed_topic in subscribed_topics:
            body += write_compact_string(subscribed_topic)
    body += write_compact_string(server_assignor)
    body += write_consumer_group_heartbeat_topic_partitions(topic_partitions)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 68, 0, correlation_id, body)
    return parse_consumer_group_heartbeat_response(response, correlation_id)


def assert_consumer_group_heartbeat_assignment(response, group_state):
    if response["error_code"] != 0:
        raise TestError(
            f"ConsumerGroupHeartbeat {group_state['group_id']!r} error_code="
            f"{response['error_code']} message={response['error_message']!r}"
        )
    if response["member_id"] != group_state["member_id"]:
        raise TestError(f"ConsumerGroupHeartbeat member mismatch: {response}")
    if response["member_epoch"] < group_state["member_epoch"]:
        raise TestError(f"ConsumerGroupHeartbeat epoch regressed: {response}")
    if response["heartbeat_interval_ms"] != 3000:
        raise TestError(f"ConsumerGroupHeartbeat interval mismatch: {response}")
    assignment = response["assignment"]
    if assignment is None:
        raise TestError(f"ConsumerGroupHeartbeat missing assignment: {response}")
    matching_topic = next(
        (
            topic
            for topic in assignment["topic_partitions"]
            if topic["topic_id"] == group_state["topic_id"]
        ),
        None,
    )
    if matching_topic is None:
        raise TestError(f"ConsumerGroupHeartbeat missing topic assignment: {response}")
    if matching_topic["partitions"] != [0]:
        raise TestError(f"ConsumerGroupHeartbeat partition mismatch: {response}")
    group_state["member_epoch"] = response["member_epoch"]


def wait_for_consumer_group_heartbeat_join(port, group_id, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7700
    member_id = f"{group_id}-member"
    last_error = None
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_id,
                member_id,
                0,
                correlation_id,
                subscribed_topics=[topic],
                server_assignor="range",
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"join error_code={response['error_code']} "
                    f"message={response['error_message']!r}"
                )
            assignment = response["assignment"]
            if assignment is None or not assignment["topic_partitions"]:
                raise TestError(f"join missing assignment: {response}")
            topic_assignment = assignment["topic_partitions"][0]
            group_state = {
                "group_id": group_id,
                "member_id": response["member_id"],
                "member_epoch": response["member_epoch"],
                "topic_id": topic_assignment["topic_id"],
            }
            assert_consumer_group_heartbeat_assignment(response, group_state)
            return group_state
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"ConsumerGroupHeartbeat group {group_id!r} did not join: {last_error}")


def wait_for_consumer_group_heartbeat(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7800
    last_error = None
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                group_state["member_epoch"],
                correlation_id,
            )
            assert_consumer_group_heartbeat_assignment(response, group_state)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def assert_kip848_consumer_group_description(port, group_state, topic, correlation_id):
    described = consumer_group_describe(port, group_state["group_id"], correlation_id)
    if described["error_code"] != 0:
        raise TestError(
            f"KIP-848 ConsumerGroupDescribe {group_state['group_id']!r} "
            f"error_code={described['error_code']} "
            f"message={described['error_message']!r}"
        )
    if described["group_state"] not in ("PreparingRebalance", "Stable"):
        raise TestError(f"KIP-848 ConsumerGroupDescribe state={described['group_state']!r}")
    if described["group_epoch"] != group_state["member_epoch"]:
        raise TestError(
            f"KIP-848 ConsumerGroupDescribe group_epoch={described['group_epoch']} "
            f"expected={group_state['member_epoch']}"
        )
    if described["assignment_epoch"] != group_state["member_epoch"]:
        raise TestError(
            f"KIP-848 ConsumerGroupDescribe assignment_epoch="
            f"{described['assignment_epoch']} expected={group_state['member_epoch']}"
        )
    if described["assignor_name"] != "range":
        raise TestError(f"KIP-848 ConsumerGroupDescribe assignor mismatch: {described}")
    matching_member = next(
        (
            member
            for member in described["members"]
            if member["member_id"] == group_state["member_id"]
        ),
        None,
    )
    if matching_member is None:
        raise TestError(f"KIP-848 ConsumerGroupDescribe missing member: {described}")
    if matching_member["member_epoch"] != group_state["member_epoch"]:
        raise TestError(
            f"KIP-848 ConsumerGroupDescribe member_epoch="
            f"{matching_member['member_epoch']} expected={group_state['member_epoch']}"
        )
    if topic not in matching_member["subscribed_topics"]:
        raise TestError(
            f"KIP-848 ConsumerGroupDescribe subscriptions mismatch: {matching_member}"
        )
    for assignment_name in ("assignment", "target_assignment"):
        assignment = matching_member[assignment_name]
        matching_topic = next(
            (
                described_topic
                for described_topic in assignment["topic_partitions"]
                if described_topic["topic_id"] == group_state["topic_id"]
            ),
            None,
        )
        if matching_topic is None:
            raise TestError(
                f"KIP-848 ConsumerGroupDescribe missing {assignment_name}: "
                f"{matching_member}"
            )
        if matching_topic["topic_name"] != topic:
            raise TestError(
                f"KIP-848 ConsumerGroupDescribe topic name mismatch: {matching_topic}"
            )
        if matching_topic["partitions"] != [0]:
            raise TestError(
                f"KIP-848 ConsumerGroupDescribe partitions mismatch: {matching_topic}"
            )


def wait_for_kip848_consumer_group_description(
    port, group_state, topic, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7900
    last_error = None
    while time.time() < deadline:
        try:
            assert_kip848_consumer_group_description(
                port, group_state, topic, correlation_id
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"KIP-848 ConsumerGroupDescribe did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def parse_leave_group_response(response, correlation_id):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"LeaveGroup correlation mismatch: {response_correlation}")
    error_code, pos = read_i16(response, pos)
    if pos != len(response):
        raise TestError(f"LeaveGroup response trailing bytes: {len(response) - pos}")
    if error_code != 0:
        raise TestError(f"LeaveGroup error_code={error_code}")


def leave_group(port, group_state, correlation_id):
    body = write_string(group_state["group_id"])
    body += write_string(group_state["member_id"])

    response = controller_request(port, 13, 0, correlation_id, body)
    parse_leave_group_response(response, correlation_id)


def wait_for_group_stable(port, group_id, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6700
    last_error = None
    while time.time() < deadline:
        try:
            group_state = join_group(port, group_id, correlation_id)
            sync_group(port, group_state, correlation_id + 1)
            heartbeat_group(port, group_state, correlation_id + 2)
            return group_state
        except Exception as exc:
            last_error = exc
        correlation_id += 3
        time.sleep(0.25)
    raise TestError(f"consumer group {group_id!r} did not become stable: {last_error}")


def wait_for_group_heartbeat(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6900
    last_error = None
    while time.time() < deadline:
        try:
            heartbeat_group(port, group_state, correlation_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"consumer group {group_state['group_id']!r} heartbeat did not recover: {last_error}"
    )


def parse_delete_groups_response(response, correlation_id, expected_group):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"DeleteGroups correlation mismatch: {response_correlation}")
    _, pos = read_i32(response, pos)  # throttle_time_ms
    results, pos = read_i32(response, pos)
    if results != 1:
        raise TestError(f"DeleteGroups result count={results}")
    group_id, pos = read_string(response, pos)
    error_code, pos = read_i16(response, pos)
    if pos != len(response):
        raise TestError(f"DeleteGroups response trailing bytes: {len(response) - pos}")
    if group_id != expected_group:
        raise TestError(f"DeleteGroups group={group_id!r}")
    return error_code


def delete_group(port, group, correlation_id):
    body = struct.pack(">i", 1)
    body += write_string(group)

    response = controller_request(port, 42, 0, correlation_id, body)
    error_code = parse_delete_groups_response(response, correlation_id, group)
    if error_code != 0:
        raise TestError(f"DeleteGroups error_code={error_code}")


def wait_for_group_delete(port, group, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 6950
    deleted = False
    last_error = None
    while time.time() < deadline:
        try:
            if not deleted:
                delete_group(port, group, correlation_id)
                deleted = True
            return wait_for_committed_offset_error(
                port,
                group,
                topic,
                ERROR_GROUP_ID_NOT_FOUND,
                timeout=max(1, int(deadline - time.time())),
            )
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"DeleteGroups did not delete {group!r}: {last_error}")


def network_hooks_configured():
    return bool(
        os.environ.get("ZMQ_KRAFT_NETWORK_MATRIX")
        or os.environ.get("ZMQ_KRAFT_NETWORK_DOWN")
        or os.environ.get("ZMQ_KRAFT_NETWORK_UP")
    )


def network_phase_env_name(phase, suffix):
    normalized = []
    for ch in phase.upper():
        if ch.isalnum():
            normalized.append(ch)
        else:
            normalized.append("_")
    name = "_".join(part for part in "".join(normalized).split("_") if part)
    if not name:
        raise TestError(f"invalid empty KRaft network matrix phase {phase!r}")
    return f"ZMQ_KRAFT_NETWORK_{name}_{suffix}"


def network_phase_command(phase, suffix):
    return os.environ.get(network_phase_env_name(phase, suffix)) or os.environ.get(
        f"ZMQ_KRAFT_NETWORK_{suffix}"
    )


def network_phase_expect(phase):
    return os.environ.get(network_phase_env_name(phase, "EXPECT")) or os.environ.get(
        "ZMQ_KRAFT_NETWORK_EXPECT", "fail"
    )


def split_csv(raw):
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def selected_network_partition_phases():
    if not network_hooks_configured():
        return []

    raw_matrix = os.environ.get("ZMQ_KRAFT_NETWORK_MATRIX")
    if raw_matrix:
        names = split_csv(raw_matrix)
        if not names:
            raise TestError("ZMQ_KRAFT_NETWORK_MATRIX did not contain any phases")
    else:
        names = ["controller-broker"]

    phases = []
    for name in names:
        down = network_phase_command(name, "DOWN")
        up = network_phase_command(name, "UP")
        if not down or not up:
            raise TestError(
                "KRaft network partition gate requires DOWN and UP hooks for "
                f"phase {name!r}"
            )
        expect = network_phase_expect(name)
        if expect not in ("fail", "survive"):
            raise TestError(f"invalid KRaft network expectation for {name!r}: {expect!r}")
        phases.append({"name": name, "down": down, "up": up, "expect": expect})
    return phases


def validate_required_network_phase_coverage():
    required_phases = split_csv(os.environ.get("ZMQ_KRAFT_REQUIRED_NETWORK_PHASES"))
    if not required_phases:
        return

    configured_phases = [phase["name"] for phase in selected_network_partition_phases()]
    missing_phases = [phase for phase in required_phases if phase not in configured_phases]
    if missing_phases:
        raise TestError(
            "required KRaft network phases not configured: "
            + ", ".join(missing_phases)
        )


def hook_context_env(processes, broker, leader_id):
    env = os.environ.copy()
    env["ZMQ_KRAFT_ACTIVE_LEADER_ID"] = str(leader_id)
    env["ZMQ_KRAFT_CONTROLLER_PORTS"] = ",".join(
        f"{node_id}:{info['port']}" for node_id, info in sorted(processes.items())
    )
    env["ZMQ_KRAFT_CONTROLLER_PIDS"] = ",".join(
        f"{node_id}:{info['proc'].pid}" for node_id, info in sorted(processes.items())
    )
    if broker is not None:
        env["ZMQ_KRAFT_BROKER_PORT"] = str(broker["port"])
        env["ZMQ_KRAFT_BROKER_PID"] = str(broker["proc"].pid)
    return env


def run_network_hook(label, command, env):
    proc = subprocess.run(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
        env=env,
    )
    if proc.returncode != 0:
        raise TestError(f"{label} failed with exit code {proc.returncode}\n{proc.stdout}")


def run_network_partition_phase(processes, broker, topic, expected_payloads, leader_id, phase, phase_index):
    hook_env = hook_context_env(processes, broker, leader_id)
    hook_env["ZMQ_KRAFT_NETWORK_PHASE"] = phase["name"]
    hook_env["ZMQ_KRAFT_NETWORK_PHASE_INDEX"] = str(phase_index)
    hook_env["ZMQ_KRAFT_NETWORK_EXPECT"] = phase["expect"]
    payload = f"r-network-{phase_index}-{phase['name']}".encode("utf-8")
    healed = False
    survived = False
    try:
        run_network_hook(f"{phase['name']}:down", phase["down"], hook_env)
        try:
            wait_for_produce(broker["port"], topic, payload, timeout=8)
            survived = True
        except Exception:
            survived = False
        if phase["expect"] == "fail" and survived:
            raise TestError(f"network partition phase {phase['name']!r} unexpectedly succeeded")
        if phase["expect"] == "survive" and not survived:
            raise TestError(f"network partition phase {phase['name']!r} unexpectedly failed")
        if survived:
            expected_payloads.append(payload)
    finally:
        run_network_hook(f"{phase['name']}:up", phase["up"], hook_env)
        healed = True

    healed_leader, _ = wait_for_leader(processes)
    wait_for_all_alive_to_report(processes, healed_leader)
    wait_for_broker_ready(broker["proc"], broker["port"], broker["log_path"])
    wait_for_payloads(broker["port"], topic, expected_payloads)
    expected_payloads.append(f"r-network-healed-{phase_index}-{phase['name']}".encode("utf-8"))
    wait_for_produce(broker["port"], topic, expected_payloads[-1])
    wait_for_payloads(broker["port"], topic, expected_payloads)
    return {
        "phase": phase["name"],
        "leader_id": healed_leader,
        "expect": phase["expect"],
        "survived": survived,
        "healed": healed,
    }


def run_network_partition_matrix(processes, broker, topic, expected_payloads, leader_id):
    phases = selected_network_partition_phases()
    if not phases:
        return None

    results = []
    current_leader_id = leader_id
    for phase_index, phase in enumerate(phases):
        result = run_network_partition_phase(
            processes,
            broker,
            topic,
            expected_payloads,
            current_leader_id,
            phase,
            phase_index,
        )
        current_leader_id = result["leader_id"]
        results.append(result)
    return results


def automq_put_kv(port, key, value, correlation_id, overwrite=True):
    body = write_compact_array_len(1)
    body += write_compact_string(key)
    body += write_compact_bytes(value)
    body += b"\x01" if overwrite else b"\x00"
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 510, correlation_id, body)
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(f"PutKVs top_error={top_error} response_count={response_count}")
    item_error, pos = read_i16(response, pos)
    item_value, pos = read_compact_bytes(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    if item_error != 0:
        raise TestError(f"PutKVs item_error={item_error}")
    return item_value


def automq_get_kv_response(port, key, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(key)
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 509, correlation_id, body)
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(f"GetKVs top_error={top_error} response_count={response_count}")
    item_error, pos = read_i16(response, pos)
    value, pos = read_compact_bytes(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    return {"error_code": item_error, "value": value}


def automq_get_kv(port, key, correlation_id):
    item = automq_get_kv_response(port, key, correlation_id)
    item_error = item["error_code"]
    if item_error != 0:
        raise TestError(f"GetKVs item_error={item_error}")
    return item["value"]


def automq_delete_kv(port, key, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(key)
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 511, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(f"DeleteKVs top_error={top_error} response_count={response_count}")
    item_error, pos = read_i16(response, pos)
    value, pos = read_compact_bytes(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    if item_error != 0:
        raise TestError(f"DeleteKVs item_error={item_error}")
    return value


def automq_create_stream(port, node_id, correlation_id, tags=None):
    api_version = 1 if tags is not None else 0
    tags = tags or []
    body = struct.pack(">iq", node_id, 1)
    body += write_compact_array_len(1)
    body += struct.pack(">i", node_id)
    if api_version >= 1:
        body += write_automq_stream_tags(tags)
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(
        port, 501, correlation_id, body, timeout=15, api_version=api_version
    )
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(f"CreateStreams top_error={top_error} response_count={response_count}")
    item_error, pos = read_i16(response, pos)
    stream_id, pos = read_i64(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    if item_error != 0:
        raise TestError(f"CreateStreams item_error={item_error}")
    if stream_id < 0:
        raise TestError(f"CreateStreams invalid stream_id={stream_id}")
    return stream_id


def automq_open_stream(port, node_id, stream_id, stream_epoch, correlation_id, tags=None):
    api_version = 1 if tags is not None else 0
    tags = tags or []
    body = struct.pack(">iq", node_id, 1)
    body += write_compact_array_len(1)
    body += struct.pack(">qq", stream_id, stream_epoch)
    if api_version >= 1:
        body += write_automq_stream_tags(tags)
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(
        port, 502, correlation_id, body, timeout=15, api_version=api_version
    )
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(f"OpenStreams top_error={top_error} response_count={response_count}")
    item_error, pos = read_i16(response, pos)
    start_offset, pos = read_i64(response, pos)
    next_offset, pos = read_i64(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    if item_error != 0:
        raise TestError(f"OpenStreams item_error={item_error}")
    return {"start_offset": start_offset, "next_offset": next_offset}


def automq_close_stream(port, node_id, stream_id, stream_epoch, correlation_id):
    return automq_single_stream_error_response(
        port,
        503,
        correlation_id,
        struct.pack(">iq", node_id, 1)
        + write_compact_array_len(1)
        + struct.pack(">qq", stream_id, stream_epoch)
        + b"\x00"
        + b"\x00",
        "CloseStreams",
    )


def automq_delete_stream(port, node_id, stream_id, stream_epoch, correlation_id):
    return automq_single_stream_error_response(
        port,
        504,
        correlation_id,
        struct.pack(">iq", node_id, 1)
        + write_compact_array_len(1)
        + struct.pack(">qq", stream_id, stream_epoch)
        + b"\x00"
        + b"\x00",
        "DeleteStreams",
    )


def automq_prepare_s3_object(port, node_id, correlation_id):
    body = struct.pack(">iiq", node_id, 1, 60_000)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 505, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    object_id, pos = read_i64(response, pos)
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"PrepareS3Object error_code={error_code}")
    if object_id < 0:
        raise TestError(f"PrepareS3Object invalid object_id={object_id}")
    return object_id


def automq_commit_stream_object(
    port,
    node_id,
    stream_id,
    object_id,
    start_offset,
    end_offset,
    stream_epoch,
    correlation_id,
):
    body = struct.pack(">i", node_id)
    body += struct.pack(">q", 1)  # node_epoch
    body += struct.pack(">q", object_id)
    body += struct.pack(">q", 128)  # object_size
    body += struct.pack(">q", stream_id)
    body += struct.pack(">q", start_offset)
    body += struct.pack(">q", end_offset)
    body += write_compact_array_len(0)  # source_object_ids
    body += struct.pack(">q", stream_epoch)
    body += struct.pack(">i", 0)  # attributes
    body += write_compact_array_len(0)  # operations
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 507, correlation_id, body, timeout=15, api_version=1)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"CommitStreamObject error_code={error_code}")


def automq_commit_stream_set_object(
    port,
    node_id,
    stream_id,
    object_id,
    start_offset,
    end_offset,
    stream_epoch,
    correlation_id,
):
    body = struct.pack(">i", node_id)
    body += struct.pack(">q", 1)  # node_epoch
    body += struct.pack(">q", object_id)
    body += struct.pack(">q", object_id)  # order_id
    body += struct.pack(">q", 256)  # object_size
    body += write_compact_array_len(1)
    body += struct.pack(">qqqq", stream_id, stream_epoch, start_offset, end_offset)
    body += b"\x00"  # object_stream_range tagged fields
    body += write_compact_array_len(0)  # stream_objects
    body += write_compact_array_len(0)  # compacted_object_ids
    body += b"\x00"  # failover_mode
    body += struct.pack(">i", 0)  # attributes
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 506, correlation_id, body, timeout=15, api_version=1)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    _, pos = read_i32(response, pos)  # attributes
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"CommitStreamSetObject error_code={error_code}")


def automq_get_opening_streams(port, node_id, correlation_id, failover_mode=False):
    body = struct.pack(">iq", node_id, 1)
    body += b"\x01" if failover_mode else b"\x00"
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 508, correlation_id, body)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    if error_code != 0:
        raise TestError(f"GetOpeningStreams error_code={error_code}")
    stream_count, pos = read_compact_array_len(response, pos)
    streams = []
    for _ in range(stream_count):
        stream_id, pos = read_i64(response, pos)
        epoch, pos = read_i64(response, pos)
        start_offset, pos = read_i64(response, pos)
        end_offset, pos = read_i64(response, pos)
        pos = skip_tags(response, pos)
        streams.append(
            {
                "stream_id": stream_id,
                "epoch": epoch,
                "start_offset": start_offset,
                "end_offset": end_offset,
            }
        )
    pos = skip_tags(response, pos)
    return streams


def automq_trim_stream(port, node_id, stream_id, stream_epoch, new_start_offset, correlation_id):
    return automq_single_stream_error_response(
        port,
        512,
        correlation_id,
        struct.pack(">iq", node_id, 1)
        + write_compact_array_len(1)
        + struct.pack(">qqq", stream_id, stream_epoch, new_start_offset)
        + b"\x00"
        + b"\x00",
        "TrimStreams",
    )


def automq_single_stream_error_response(port, api_key, correlation_id, body, api_name):
    response = automq_request(port, api_key, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(f"{api_name} top_error={top_error} response_count={response_count}")
    item_error, pos = read_i16(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    if item_error != 0:
        raise TestError(f"{api_name} item_error={item_error}")


def parse_automq_describe_stream_response(response, correlation_id, stream_id):
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    stream_count, pos = read_compact_array_len(response, pos)
    if top_error != 0:
        raise TestError(f"DescribeStreams top_error={top_error}")

    streams = []
    for _ in range(stream_count):
        described_stream_id, pos = read_i64(response, pos)
        described_node_id, pos = read_i32(response, pos)
        state, pos = read_compact_string(response, pos)
        if pos + 16 > len(response):
            raise TestError("DescribeStreams response truncated in topic_id")
        pos += 16
        _, pos = read_compact_string(response, pos)  # topic_name
        partition_index, pos = read_i32(response, pos)
        epoch, pos = read_i64(response, pos)
        start_offset, pos = read_i64(response, pos)
        end_offset, pos = read_i64(response, pos)
        tag_count, pos = read_compact_array_len(response, pos)
        tags = []
        for _ in range(tag_count):
            tag_key, pos = read_compact_string(response, pos)
            tag_value, pos = read_compact_string(response, pos)
            tags.append((tag_key, tag_value))
            pos = skip_tags(response, pos)
        pos = skip_tags(response, pos)
        streams.append(
            {
                "stream_id": described_stream_id,
                "node_id": described_node_id,
                "state": state,
                "partition_index": partition_index,
                "epoch": epoch,
                "start_offset": start_offset,
                "end_offset": end_offset,
                "tags": tags,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeStreams response has trailing bytes: pos={pos} len={len(response)}"
        )
    for stream in streams:
        if stream["stream_id"] == stream_id:
            return stream
    raise TestError(f"DescribeStreams did not include stream_id={stream_id}; streams={streams}")


def automq_describe_stream(port, stream_id, correlation_id):
    body = write_compact_array_len(0)  # topic_partitions
    body += struct.pack(">i", -1)  # node_id
    body += struct.pack(">q", stream_id)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 601, correlation_id, body)
    return parse_automq_describe_stream_response(response, correlation_id, stream_id)


def automq_register_node(port, node_id, node_epoch, wal_config, correlation_id, tags=None):
    tags = tags or []
    body = struct.pack(">iq", node_id, node_epoch)
    body += write_compact_string(wal_config)
    body += write_automq_node_tags(tags)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 513, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"AutomqRegisterNode error_code={error_code}")


def parse_automq_get_nodes_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    if error_code != 0:
        raise TestError(f"AutomqGetNodes error_code={error_code}")
    node_count, pos = read_compact_array_len(response, pos)
    nodes = []
    for _ in range(node_count):
        described_node_id, pos = read_i32(response, pos)
        node_epoch, pos = read_i64(response, pos)
        wal_config, pos = read_compact_string(response, pos)
        state, pos = read_compact_string(response, pos)
        has_opening_streams, pos = read_bool(response, pos)
        tag_count, pos = read_compact_array_len(response, pos)
        tags = []
        for _ in range(tag_count):
            tag_key, pos = read_compact_string(response, pos)
            tag_value, pos = read_compact_string(response, pos)
            tags.append((tag_key, tag_value))
            pos = skip_tags(response, pos)
        pos = skip_tags(response, pos)
        nodes.append(
            {
                "node_id": described_node_id,
                "node_epoch": node_epoch,
                "wal_config": wal_config,
                "state": state,
                "has_opening_streams": has_opening_streams,
                "tags": tags,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"AutomqGetNodes response has trailing bytes: pos={pos} len={len(response)}"
        )
    return nodes


def automq_get_node(port, node_id, correlation_id):
    body = write_compact_array_len(1)
    body += struct.pack(">i", node_id)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 514, correlation_id, body)
    nodes = parse_automq_get_nodes_response(response, correlation_id)
    for node in nodes:
        if node["node_id"] == node_id:
            return node
    raise TestError(f"AutomqGetNodes did not include node_id={node_id}; nodes={nodes}")


def automq_update_license(port, license_value, correlation_id):
    body = write_compact_string(license_value)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 517, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    _, pos = read_compact_string(response, pos)  # error_message
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"UpdateLicense error_code={error_code}")


def automq_describe_license(port, correlation_id):
    response = automq_request(port, 518, correlation_id, b"\x00")
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    _, pos = read_compact_string(response, pos)  # error_message
    license_value, pos = read_compact_string(response, pos)
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"DescribeLicense error_code={error_code}")
    return license_value


def automq_get_next_node_id(port, cluster_id, correlation_id):
    body = write_compact_string(cluster_id)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 600, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    node_id, pos = read_i32(response, pos)
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"GetNextNodeId error_code={error_code}")
    return node_id


def automq_get_partition_snapshot(port, session_id, session_epoch, correlation_id):
    body = struct.pack(">ii", session_id, session_epoch)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 516, correlation_id, body)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_session_id, pos = read_i32(response, pos)
    response_session_epoch, pos = read_i32(response, pos)
    if error_code != 0:
        raise TestError(f"AutomqGetPartitionSnapshot error_code={error_code}")
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        if pos + 16 > len(response):
            raise TestError("AutomqGetPartitionSnapshot truncated topic_id")
        topic_id = response[pos : pos + 16]
        pos += 16
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            operation, pos = read_i16(response, pos)
            has_log_metadata, pos = read_varint(response, pos)
            if has_log_metadata != 0:
                raise TestError("unexpected non-empty log_metadata in partition snapshot")
            has_first_unstable, pos = read_varint(response, pos)
            if has_first_unstable != 0:
                raise TestError("unexpected non-empty first_unstable_offset in partition snapshot")
            has_log_end, pos = read_varint(response, pos)
            log_end_offset = None
            if has_log_end != 0:
                message_offset, pos = read_i64(response, pos)
                _, pos = read_i32(response, pos)  # relative_position_in_segment
                pos = skip_tags(response, pos)
                log_end_offset = message_offset
            stream_count, pos = read_compact_array_len(response, pos)
            streams = []
            for _ in range(stream_count):
                stream_id, pos = read_i64(response, pos)
                end_offset, pos = read_i64(response, pos)
                pos = skip_tags(response, pos)
                streams.append({"stream_id": stream_id, "end_offset": end_offset})
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "leader_epoch": leader_epoch,
                    "operation": operation,
                    "log_end_offset": log_end_offset,
                    "streams": streams,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"topic_id": topic_id, "partitions": partitions})
    pos = skip_tags(response, pos)
    return {
        "session_id": response_session_id,
        "session_epoch": response_session_epoch,
        "topics": topics,
    }


def automq_export_cluster_manifest(port, correlation_id):
    response = automq_request(port, 519, correlation_id, b"\x00")
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    manifest, pos = read_compact_string(response, pos)
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"ExportClusterManifest error_code={error_code}")
    if manifest is None:
        raise TestError("ExportClusterManifest returned null manifest")
    try:
        return json.loads(manifest)
    except json.JSONDecodeError as exc:
        raise TestError(f"ExportClusterManifest returned invalid JSON: {manifest!r}") from exc


def automq_update_group(port, link_id, group_id, promoted, correlation_id):
    body = write_compact_string(link_id)
    body += write_compact_string(group_id)
    body += b"\x01" if promoted else b"\x00"
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 602, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    response_group_id, pos = read_compact_string(response, pos)
    error_code, pos = read_i16(response, pos)
    _, pos = read_compact_string(response, pos)  # error_message
    _, pos = read_i32(response, pos)  # throttle_time_ms
    pos = skip_tags(response, pos)
    if response_group_id != group_id:
        raise TestError(f"AutomqUpdateGroup group_id mismatch: {response_group_id!r}")
    if error_code != 0:
        raise TestError(f"AutomqUpdateGroup error_code={error_code}")


def automq_zone_router(port, metadata, route_epoch, correlation_id, api_version=1):
    body = write_compact_bytes(metadata)
    if api_version >= 1:
        body += struct.pack(">q", route_epoch)
        body += struct.pack(">h", api_version)
    body += b"\x00"  # request tagged fields

    response = automq_request(
        port,
        515,
        correlation_id,
        body,
        timeout=15,
        api_version=api_version,
    )
    pos = parse_flexible_response_header(response, correlation_id)
    top_error, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    response_count, pos = read_compact_array_len(response, pos)
    if top_error != 0 or response_count != 1:
        raise TestError(
            f"AutomqZoneRouter top_error={top_error} response_count={response_count}"
        )
    data, pos = read_compact_bytes(response, pos)
    pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)
    return data


def wait_for_automq_put_kv(port, key, value, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 7000
    last_error = None
    while time.time() < deadline:
        try:
            return automq_put_kv(port, key, value, correlation_id)
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ PutKVs did not succeed within {timeout}s: {last_error}")


def wait_for_automq_kv(port, key, expected_value, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 8000
    last_error = None
    last_value = None
    while time.time() < deadline:
        try:
            last_value = automq_get_kv(port, key, correlation_id)
            if last_value == expected_value:
                return last_value
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetKVs did not return expected value for {key!r}: "
        f"last_value={last_value!r} last_error={last_error}"
    )


def wait_for_automq_kv_missing(port, key, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 8500
    last_error = None
    last_item = None
    while time.time() < deadline:
        try:
            last_item = automq_get_kv_response(port, key, correlation_id)
            if last_item["error_code"] != 0:
                return last_item["error_code"]
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetKVs did not report missing key {key!r}: "
        f"last_item={last_item!r} last_error={last_error}"
    )


def wait_for_automq_delete_kv(port, key, expected_value, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 8700
    last_error = None
    last_value = None
    while time.time() < deadline:
        try:
            last_value = automq_delete_kv(port, key, correlation_id)
            if last_value == expected_value:
                return last_value
            raise TestError(f"expected deleted value {expected_value!r}, got {last_value!r}")
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(
        f"AutoMQ DeleteKVs did not delete {key!r}: "
        f"last_value={last_value!r} last_error={last_error}"
    )


def wait_for_automq_create_stream(port, node_id, tags=None, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 9000
    last_error = None
    while time.time() < deadline:
        try:
            return automq_create_stream(port, node_id, correlation_id, tags=tags)
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ CreateStreams did not succeed within {timeout}s: {last_error}")


def wait_for_automq_open_stream(
    port, node_id, stream_id, stream_epoch, tags=None, timeout=45
):
    deadline = time.time() + timeout
    correlation_id = 9300
    last_error = None
    while time.time() < deadline:
        try:
            return automq_open_stream(
                port, node_id, stream_id, stream_epoch, correlation_id, tags=tags
            )
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ OpenStreams did not succeed within {timeout}s: {last_error}")


def wait_for_automq_prepare_s3_object(port, node_id, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 9500
    last_error = None
    while time.time() < deadline:
        try:
            return automq_prepare_s3_object(port, node_id, correlation_id)
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ PrepareS3Object did not succeed within {timeout}s: {last_error}")


def wait_for_automq_commit_stream_object(
    port,
    node_id,
    stream_id,
    object_id,
    start_offset,
    end_offset,
    stream_epoch,
    timeout=45,
):
    deadline = time.time() + timeout
    correlation_id = 9700
    last_error = None
    while time.time() < deadline:
        try:
            automq_commit_stream_object(
                port,
                node_id,
                stream_id,
                object_id,
                start_offset,
                end_offset,
                stream_epoch,
                correlation_id,
            )
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ CommitStreamObject did not succeed within {timeout}s: {last_error}")


def wait_for_automq_commit_stream_set_object(
    port,
    node_id,
    stream_id,
    object_id,
    start_offset,
    end_offset,
    stream_epoch,
    timeout=45,
):
    deadline = time.time() + timeout
    correlation_id = 9800
    last_error = None
    while time.time() < deadline:
        try:
            automq_commit_stream_set_object(
                port,
                node_id,
                stream_id,
                object_id,
                start_offset,
                end_offset,
                stream_epoch,
                correlation_id,
            )
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(
        f"AutoMQ CommitStreamSetObject did not succeed within {timeout}s: {last_error}"
    )


def wait_for_automq_stream(
    port,
    stream_id,
    expected_state=None,
    expected_epoch=None,
    expected_start_offset=None,
    expected_end_offset=None,
    expected_tags=None,
    timeout=45,
):
    deadline = time.time() + timeout
    correlation_id = 10000
    last_error = None
    last_stream = None
    while time.time() < deadline:
        try:
            stream = automq_describe_stream(port, stream_id, correlation_id)
            last_stream = stream
            if (
                stream["stream_id"] == stream_id
                and (expected_state is None or stream["state"] == expected_state)
                and (expected_epoch is None or stream["epoch"] == expected_epoch)
                and (
                    expected_start_offset is None
                    or stream["start_offset"] == expected_start_offset
                )
                and (expected_end_offset is None or stream["end_offset"] == expected_end_offset)
                and (expected_tags is None or stream["tags"] == list(expected_tags))
            ):
                return stream
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ DescribeStreams did not return expected stream {stream_id}: "
        f"last_stream={last_stream!r} last_error={last_error}"
    )


def wait_for_automq_stream_missing(port, stream_id, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 10500
    last_error = None
    while time.time() < deadline:
        try:
            automq_describe_stream(port, stream_id, correlation_id)
        except Exception as exc:
            last_error = exc
            return
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ DescribeStreams still returned deleted stream {stream_id}: "
        f"last_error={last_error}"
    )


def wait_for_automq_close_stream(port, node_id, stream_id, stream_epoch, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 10600
    last_error = None
    while time.time() < deadline:
        try:
            automq_close_stream(port, node_id, stream_id, stream_epoch, correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ CloseStreams did not succeed within {timeout}s: {last_error}")


def wait_for_automq_delete_stream(port, node_id, stream_id, stream_epoch, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 10700
    last_error = None
    while time.time() < deadline:
        try:
            automq_delete_stream(port, node_id, stream_id, stream_epoch, correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ DeleteStreams did not succeed within {timeout}s: {last_error}")


def wait_for_automq_trim_stream(
    port,
    node_id,
    stream_id,
    stream_epoch,
    new_start_offset,
    timeout=45,
):
    deadline = time.time() + timeout
    correlation_id = 10800
    last_error = None
    while time.time() < deadline:
        try:
            automq_trim_stream(
                port,
                node_id,
                stream_id,
                stream_epoch,
                new_start_offset,
                correlation_id,
            )
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ TrimStreams did not succeed within {timeout}s: {last_error}")


def wait_for_automq_opening_stream(
    port,
    node_id,
    stream_id,
    expected_epoch=None,
    expected_start_offset=None,
    expected_end_offset=None,
    timeout=45,
):
    deadline = time.time() + timeout
    correlation_id = 10900
    last_error = None
    last_streams = []
    while time.time() < deadline:
        try:
            last_streams = automq_get_opening_streams(port, node_id, correlation_id)
            for stream in last_streams:
                if (
                    stream["stream_id"] == stream_id
                    and (expected_epoch is None or stream["epoch"] == expected_epoch)
                    and (
                        expected_start_offset is None
                        or stream["start_offset"] == expected_start_offset
                    )
                    and (expected_end_offset is None or stream["end_offset"] == expected_end_offset)
                ):
                    return stream
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetOpeningStreams did not return stream {stream_id}: "
        f"last_streams={last_streams!r} last_error={last_error}"
    )


def wait_for_automq_opening_stream_missing(port, node_id, stream_id, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 10950
    last_error = None
    last_streams = []
    while time.time() < deadline:
        try:
            last_streams = automq_get_opening_streams(port, node_id, correlation_id)
            if all(stream["stream_id"] != stream_id for stream in last_streams):
                return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetOpeningStreams still returned stream {stream_id}: "
        f"last_streams={last_streams!r} last_error={last_error}"
    )


def wait_for_automq_register_node(
    port, node_id, node_epoch, wal_config, tags=None, timeout=45
):
    deadline = time.time() + timeout
    correlation_id = 11000
    last_error = None
    while time.time() < deadline:
        try:
            automq_register_node(port, node_id, node_epoch, wal_config, correlation_id, tags=tags)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ RegisterNode did not succeed within {timeout}s: {last_error}")


def wait_for_automq_node(
    port, node_id, expected_epoch, expected_wal_config, expected_tags=None, timeout=45
):
    deadline = time.time() + timeout
    correlation_id = 12000
    last_error = None
    last_node = None
    while time.time() < deadline:
        try:
            last_node = automq_get_node(port, node_id, correlation_id)
            if (
                last_node["node_epoch"] == expected_epoch
                and last_node["wal_config"] == expected_wal_config
                and (expected_tags is None or last_node["tags"] == list(expected_tags))
            ):
                return last_node
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetNodes did not return expected node {node_id}: "
        f"last_node={last_node!r} last_error={last_error}"
    )


def wait_for_automq_update_license(port, license_value, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 13000
    last_error = None
    while time.time() < deadline:
        try:
            automq_update_license(port, license_value, correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ UpdateLicense did not succeed within {timeout}s: {last_error}")


def wait_for_automq_license(port, expected_license, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 14000
    last_error = None
    last_license = None
    while time.time() < deadline:
        try:
            last_license = automq_describe_license(port, correlation_id)
            if last_license == expected_license:
                return last_license
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ DescribeLicense did not return expected license: "
        f"last_license={last_license!r} last_error={last_error}"
    )


def wait_for_automq_next_node_id(port, cluster_id, expected_node_id=None, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 15000
    last_error = None
    last_node_id = None
    while time.time() < deadline:
        try:
            last_node_id = automq_get_next_node_id(port, cluster_id, correlation_id)
            if expected_node_id is None or last_node_id == expected_node_id:
                return last_node_id
            raise TestError(f"expected node_id={expected_node_id}, got {last_node_id}")
        except Exception as exc:
            last_error = exc
            if last_node_id is not None:
                break
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetNextNodeId did not return expected id {expected_node_id}: "
        f"last_node_id={last_node_id} last_error={last_error}"
    )


def wait_for_automq_partition_snapshot(port, session_id, session_epoch, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 15500
    last_error = None
    last_snapshot = None
    while time.time() < deadline:
        try:
            last_snapshot = automq_get_partition_snapshot(
                port,
                session_id,
                session_epoch,
                correlation_id,
            )
            if (
                last_snapshot["session_id"] == session_id
                and last_snapshot["session_epoch"] == session_epoch + 1
            ):
                return last_snapshot
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ GetPartitionSnapshot did not return expected session: "
        f"last_snapshot={last_snapshot!r} last_error={last_error}"
    )


def wait_for_automq_manifest_streams(port, minimum_streams, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 15800
    last_error = None
    last_manifest = None
    while time.time() < deadline:
        try:
            last_manifest = automq_export_cluster_manifest(port, correlation_id)
            if last_manifest.get("streams", -1) >= minimum_streams:
                return last_manifest
            raise TestError(
                f"expected at least {minimum_streams} streams, got {last_manifest.get('streams')}"
            )
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ ExportClusterManifest did not report at least {minimum_streams} streams: "
        f"last_manifest={last_manifest!r} last_error={last_error}"
    )


def wait_for_automq_manifest_groups(port, expected_groups, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 15900
    last_error = None
    last_manifest = None
    while time.time() < deadline:
        try:
            last_manifest = automq_export_cluster_manifest(port, correlation_id)
            if last_manifest.get("groups") == expected_groups:
                return last_manifest
            raise TestError(
                f"expected groups={expected_groups}, got {last_manifest.get('groups')}"
            )
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ ExportClusterManifest did not report groups={expected_groups}: "
        f"last_manifest={last_manifest!r} last_error={last_error}"
    )


def wait_for_automq_update_group(port, link_id, group_id, promoted, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 15950
    last_error = None
    while time.time() < deadline:
        try:
            automq_update_group(port, link_id, group_id, promoted, correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ UpdateGroup did not succeed within {timeout}s: {last_error}")


def wait_for_automq_zone_router_update(port, metadata, route_epoch, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 16000
    last_error = None
    last_data = None
    while time.time() < deadline:
        try:
            last_data = automq_zone_router(port, metadata, route_epoch, correlation_id)
            if last_data == metadata:
                return last_data
            raise TestError(f"expected router data {metadata!r}, got {last_data!r}")
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(
        f"AutoMQ ZoneRouter did not update metadata: "
        f"last_data={last_data!r} last_error={last_error}"
    )


def wait_for_automq_zone_router(port, expected_metadata, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 17000
    last_error = None
    last_data = None
    while time.time() < deadline:
        try:
            last_data = automq_zone_router(port, None, 0, correlation_id)
            if last_data == expected_metadata:
                return last_data
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.5)
    raise TestError(
        f"AutoMQ ZoneRouter did not return expected metadata: "
        f"last_data={last_data!r} last_error={last_error}"
    )


def describe_quorum_body():
    body = bytearray()
    body += write_compact_array_len(1)
    body += write_compact_string("__cluster_metadata")
    body += write_compact_array_len(1)
    body += struct.pack(">i", 0)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields
    return bytes(body)


def describe_quorum(port, correlation_id):
    response = controller_request(port, 55, 0, correlation_id, describe_quorum_body())
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"DescribeQuorum correlation mismatch: {response_correlation}")
    pos = skip_tags(response, pos)
    top_error, pos = read_i16(response, pos)
    if top_error != 0:
        raise TestError(f"DescribeQuorum top-level error_code={top_error}")

    topics_len, pos = read_compact_array_len(response, pos)
    if topics_len == 0:
        raise TestError("DescribeQuorum returned no topics")
    topic_name, pos = read_compact_string(response, pos)
    partitions_len, pos = read_compact_array_len(response, pos)
    if partitions_len == 0:
        raise TestError(f"DescribeQuorum topic {topic_name!r} returned no partitions")

    partition_index, pos = read_i32(response, pos)
    partition_error, pos = read_i16(response, pos)
    leader_id, pos = read_i32(response, pos)
    leader_epoch, pos = read_i32(response, pos)
    high_watermark, pos = read_i64(response, pos)
    voters_len, pos = read_compact_array_len(response, pos)
    voters = []
    for _ in range(voters_len):
        replica_id, pos = read_i32(response, pos)
        _, pos = read_i64(response, pos)
        pos = skip_tags(response, pos)
        voters.append(replica_id)
    observers_len, pos = read_compact_array_len(response, pos)
    for _ in range(observers_len):
        _, pos = read_i32(response, pos)
        _, pos = read_i64(response, pos)
        pos = skip_tags(response, pos)
    pos = skip_tags(response, pos)

    return {
        "partition_index": partition_index,
        "error_code": partition_error,
        "leader_id": leader_id,
        "leader_epoch": leader_epoch,
        "high_watermark": high_watermark,
        "voters": voters,
    }


def tail(path, limit=12000):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - limit), os.SEEK_SET)
            return f.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def wait_for_ready(proc, port, log_path):
    deadline = time.time() + 30
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise TestError(f"controller on {port} exited early with code {proc.returncode}\n{tail(log_path)}")
        try:
            if api_versions_count(port) > 0:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise TestError(f"controller on {port} did not become ready: {last_error}\n{tail(log_path)}")


def wait_for_broker_ready(proc, port, log_path):
    deadline = time.time() + 30
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise TestError(f"broker on {port} exited early with code {proc.returncode}\n{tail(log_path)}")
        try:
            if api_versions_count(port) > 0:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise TestError(f"broker on {port} did not become ready: {last_error}\n{tail(log_path)}")


def wait_for_leader(processes, forbidden_leaders=frozenset(), timeout=45):
    deadline = time.time() + timeout
    correlation = 1000
    last_error = None
    while time.time() < deadline:
        for node_id, info in processes.items():
            proc = info["proc"]
            if proc.poll() is not None:
                continue
            try:
                quorum = describe_quorum(info["port"], correlation)
                correlation += 1
                leader_id = quorum["leader_id"]
                if quorum["error_code"] == 0 and leader_id >= 0 and leader_id not in forbidden_leaders:
                    return leader_id, quorum
            except Exception as exc:
                last_error = exc
        time.sleep(0.25)
    raise TestError(f"leader was not discovered within {timeout}s: {last_error}")


def wait_for_all_alive_to_report(processes, expected_leader, timeout=20):
    deadline = time.time() + timeout
    correlation = 2000
    last_seen = {}
    while time.time() < deadline:
        ok = True
        for node_id, info in processes.items():
            if info["proc"].poll() is not None:
                continue
            try:
                quorum = describe_quorum(info["port"], correlation)
                correlation += 1
                last_seen[node_id] = quorum["leader_id"]
                if quorum["leader_id"] != expected_leader:
                    ok = False
            except Exception as exc:
                last_seen[node_id] = f"error: {exc}"
                ok = False
        if ok:
            return
        time.sleep(0.25)
    raise TestError(f"controllers did not converge on leader {expected_leader}; last_seen={last_seen}")


def start_controller(tmp, node_id, port, voters):
    data_dir = os.path.join(tmp, f"controller-{node_id}")
    log_path = os.path.join(tmp, f"controller-{node_id}.log")
    os.makedirs(data_dir, exist_ok=True)
    log_file = open(log_path, "ab", buffering=0)
    args = [
        ZMQ_BIN,
        "--node-id",
        str(node_id),
        "--process-roles",
        "controller",
        "--controller-port",
        str(port),
        "--port",
        str(port + 1000),
        "--data-dir",
        data_dir,
        "--cluster-id",
        CLUSTER_ID,
        "--voters",
        voters,
        "--workers",
        "1",
    ]
    proc = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
    proc._zmq_log_file = log_file
    return {"proc": proc, "port": port, "log_path": log_path}


def start_broker(tmp, voters):
    data_dir = os.path.join(tmp, "broker-100")
    log_path = os.path.join(tmp, "broker-100.log")
    os.makedirs(data_dir, exist_ok=True)
    log_file = open(log_path, "ab", buffering=0)
    args = [
        ZMQ_BIN,
        "--node-id",
        "100",
        "--process-roles",
        "broker",
        "--port",
        str(BROKER_PORT),
        "--metrics-port",
        str(BROKER_PORT + 1000),
        "--data-dir",
        data_dir,
        "--cluster-id",
        CLUSTER_ID,
        "--voters",
        voters,
        "--advertised-host",
        "localhost",
        "--workers",
        "1",
    ]
    proc = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
    proc._zmq_log_file = log_file
    return {"proc": proc, "port": BROKER_PORT, "log_path": log_path}


def start_combined_node(tmp, node_id, controller_port, broker_port, voters):
    data_dir = os.path.join(tmp, f"automq-combined-{node_id}")
    log_path = os.path.join(tmp, f"automq-combined-{node_id}.log")
    os.makedirs(data_dir, exist_ok=True)
    log_file = open(log_path, "ab", buffering=0)
    args = [
        ZMQ_BIN,
        "--node-id",
        str(node_id),
        "--process-roles",
        "broker,controller",
        "--controller-port",
        str(controller_port),
        "--port",
        str(broker_port),
        "--metrics-port",
        str(broker_port + 2000),
        "--data-dir",
        data_dir,
        "--cluster-id",
        f"{CLUSTER_ID}-automq",
        "--voters",
        voters,
        "--advertised-host",
        "localhost",
        "--workers",
        "1",
    ]
    proc = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
    proc._zmq_log_file = log_file
    return {
        "proc": proc,
        "port": controller_port,
        "broker_port": broker_port,
        "log_path": log_path,
    }


def stop_process(proc, crash=False):
    if proc is None:
        return
    try:
        if proc.poll() is None:
            if crash:
                proc.kill()
            else:
                proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
    finally:
        log_file = getattr(proc, "_zmq_log_file", None)
        if log_file is not None:
            log_file.close()


def run_live_reassignment_convergence(processes, source_id):
    source_port = processes[source_id]["broker_port"]
    target_id = next(node_id for node_id in sorted(processes) if node_id != source_id)
    target_port = processes[target_id]["broker_port"]
    topic = f"kraft-reassign-{os.getpid()}-{source_id}-{target_id}-{int(time.time())}"

    wait_for_topic(source_port, topic)
    wait_for_metadata_leader(source_port, topic, source_id)

    before_payload = b"ra"
    wait_for_produce(source_port, topic, before_payload)

    alter_partition_reassignment(source_port, topic, 0, [target_id], 4500)
    wait_for_partition_reassignment(source_port, topic, 0, [target_id])
    wait_for_metadata_leader(source_port, topic, target_id)
    wait_for_metadata_leader(target_port, topic, target_id)

    wait_for_produce_error(source_port, topic, b"old-owner-rejected", 6)
    target_offset = wait_for_produce(target_port, topic, b"rb")
    wait_for_payloads(target_port, topic, [b"rb"])

    return {
        "topic": topic,
        "source_id": source_id,
        "target_id": target_id,
        "target_offset": target_offset,
    }


def run_automq_metadata_failover_scenario(tmp):
    controller_base = PORT_BASE + 200
    broker_base = BROKER_PORT + 1200
    processes = {}
    try:
        controller_ports = {node_id: controller_base + node_id for node_id in range(3)}
        broker_ports = {node_id: broker_base + node_id for node_id in range(3)}
        voters = ",".join(
            f"{node_id}@127.0.0.1:{port}" for node_id, port in controller_ports.items()
        )

        for node_id in sorted(controller_ports):
            processes[node_id] = start_combined_node(
                tmp, node_id, controller_ports[node_id], broker_ports[node_id], voters
            )
        for info in processes.values():
            wait_for_ready(info["proc"], info["port"], info["log_path"])
            wait_for_broker_ready(info["proc"], info["broker_port"], info["log_path"])

        leader_id, initial = wait_for_leader(processes)
        if leader_id not in processes:
            raise TestError(f"AutoMQ scenario discovered unexpected leader {leader_id}")

        leader_broker_port = processes[leader_id]["broker_port"]
        key = f"automq.failover.{os.getpid()}.{int(time.time())}"
        value_before = b"before-controller-failover"
        wait_for_automq_put_kv(leader_broker_port, key, value_before)
        wait_for_automq_kv(leader_broker_port, key, value_before)

        delete_key = f"{key}.delete"
        delete_value = b"delete-after-controller-failover"
        wait_for_automq_put_kv(leader_broker_port, delete_key, delete_value)
        wait_for_automq_kv(leader_broker_port, delete_key, delete_value)

        zone_router_before = (
            f'{{"route":"before-controller-failover","leader":{leader_id}}}'.encode("utf-8")
        )
        zone_router_epoch_before = 100 + leader_id
        wait_for_automq_zone_router_update(
            leader_broker_port,
            zone_router_before,
            zone_router_epoch_before,
        )
        wait_for_automq_zone_router(leader_broker_port, zone_router_before)

        group_id = f"automq-group-{os.getpid()}-{leader_id}"
        link_id = f"automq-link-{leader_id}"
        wait_for_automq_update_group(leader_broker_port, link_id, group_id, True)
        wait_for_automq_manifest_groups(leader_broker_port, 1)

        stream_owner_node_id = leader_id
        stream_tags_before = [
            ("purpose", "failover"),
            ("owner", f"node-{leader_id}"),
        ]
        stream_tags_after = [
            ("purpose", "failover"),
            ("phase", "reopened"),
        ]
        stream_tags_cleared = []
        stream_id = wait_for_automq_create_stream(
            leader_broker_port,
            leader_id,
            tags=stream_tags_before,
        )
        stream_object_id = wait_for_automq_prepare_s3_object(
            leader_broker_port,
            stream_owner_node_id,
        )
        wait_for_automq_commit_stream_object(
            leader_broker_port,
            stream_owner_node_id,
            stream_id,
            stream_object_id,
            0,
            10,
            1,
        )
        wait_for_automq_stream(
            leader_broker_port,
            stream_id,
            "OPENED",
            1,
            0,
            10,
            expected_tags=stream_tags_before,
        )
        wait_for_automq_opening_stream(
            leader_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=1,
            expected_start_offset=0,
            expected_end_offset=10,
        )

        deleted_stream_id = wait_for_automq_create_stream(
            leader_broker_port,
            stream_owner_node_id,
        )
        wait_for_automq_stream(leader_broker_port, deleted_stream_id, "OPENED", 1, 0, 0)
        wait_for_automq_manifest_streams(leader_broker_port, 2)
        wait_for_automq_partition_snapshot(leader_broker_port, 1, 0)

        registered_node_id = 700 + leader_id
        registered_node_epoch = 42
        registered_wal_config = f"wal://automq-node-{registered_node_id}"
        registered_node_tags = [
            ("rack", f"rack-{leader_id}"),
            ("zone", "primary"),
        ]
        registered_node_tags_cleared = []
        registered_node_epoch_after = registered_node_epoch
        wait_for_automq_register_node(
            leader_broker_port,
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
            tags=registered_node_tags,
        )
        wait_for_automq_node(
            leader_broker_port,
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
            expected_tags=registered_node_tags,
        )

        license_value = f"license-{os.getpid()}-{int(time.time())}"
        wait_for_automq_update_license(leader_broker_port, license_value)
        wait_for_automq_license(leader_broker_port, license_value)

        cluster_id = f"{CLUSTER_ID}-automq"
        first_allocated_node_id = wait_for_automq_next_node_id(leader_broker_port, cluster_id)
        if first_allocated_node_id < registered_node_id + 1:
            raise TestError(
                f"GetNextNodeId did not advance beyond registered node: "
                f"allocated={first_allocated_node_id} registered={registered_node_id}"
            )

        for node_id, info in processes.items():
            if node_id == leader_id:
                continue
            wait_for_automq_kv(info["broker_port"], key, value_before)
            wait_for_automq_kv(info["broker_port"], delete_key, delete_value)
            wait_for_automq_zone_router(info["broker_port"], zone_router_before)
            wait_for_automq_manifest_groups(info["broker_port"], 1)
            wait_for_automq_stream(
                info["broker_port"],
                stream_id,
                "OPENED",
                1,
                0,
                10,
                expected_tags=stream_tags_before,
            )
            wait_for_automq_opening_stream(
                info["broker_port"],
                stream_owner_node_id,
                stream_id,
                expected_epoch=1,
                expected_start_offset=0,
                expected_end_offset=10,
            )
            wait_for_automq_stream(info["broker_port"], deleted_stream_id, "OPENED", 1, 0, 0)
            wait_for_automq_manifest_streams(info["broker_port"], 2)
            wait_for_automq_partition_snapshot(info["broker_port"], 1, 0)
            wait_for_automq_node(
                info["broker_port"],
                registered_node_id,
                registered_node_epoch,
                registered_wal_config,
                expected_tags=registered_node_tags,
            )
            wait_for_automq_license(info["broker_port"], license_value)

        reassignment_result = run_live_reassignment_convergence(processes, leader_id)

        stop_process(processes[leader_id]["proc"], crash=True)
        replacement_leader, after = wait_for_leader(processes, forbidden_leaders={leader_id})
        if after["leader_epoch"] <= initial["leader_epoch"]:
            raise TestError(f"AutoMQ failover leader epoch did not advance: before={initial} after={after}")
        wait_for_all_alive_to_report(processes, replacement_leader)

        replacement_broker_port = processes[replacement_leader]["broker_port"]
        wait_for_automq_kv(replacement_broker_port, key, value_before)
        wait_for_automq_kv(replacement_broker_port, delete_key, delete_value)
        wait_for_automq_zone_router(replacement_broker_port, zone_router_before)
        wait_for_automq_manifest_groups(replacement_broker_port, 1)
        wait_for_automq_stream(
            replacement_broker_port,
            stream_id,
            "OPENED",
            1,
            0,
            10,
            expected_tags=stream_tags_before,
        )
        wait_for_automq_opening_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=1,
            expected_start_offset=0,
            expected_end_offset=10,
        )
        wait_for_automq_stream(replacement_broker_port, deleted_stream_id, "OPENED", 1, 0, 0)
        wait_for_automq_manifest_streams(replacement_broker_port, 2)
        wait_for_automq_partition_snapshot(replacement_broker_port, 1, 0)
        wait_for_automq_node(
            replacement_broker_port,
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
            expected_tags=registered_node_tags,
        )
        wait_for_automq_license(replacement_broker_port, license_value)
        wait_for_automq_next_node_id(
            replacement_broker_port,
            cluster_id,
            expected_node_id=first_allocated_node_id + 1,
        )
        registered_node_epoch_after = registered_node_epoch + 1
        wait_for_automq_register_node(
            replacement_broker_port,
            registered_node_id,
            registered_node_epoch_after,
            registered_wal_config,
            tags=registered_node_tags_cleared,
        )
        wait_for_automq_node(
            replacement_broker_port,
            registered_node_id,
            registered_node_epoch_after,
            registered_wal_config,
            expected_tags=registered_node_tags_cleared,
        )

        value_after = b"after-controller-failover"
        wait_for_automq_put_kv(replacement_broker_port, key, value_after)
        wait_for_automq_kv(replacement_broker_port, key, value_after)

        wait_for_automq_delete_kv(replacement_broker_port, delete_key, delete_value)
        wait_for_automq_kv_missing(replacement_broker_port, delete_key)

        zone_router_after = (
            f'{{"route":"after-controller-failover","leader":{replacement_leader}}}'.encode("utf-8")
        )
        zone_router_epoch_after = zone_router_epoch_before + 100
        wait_for_automq_zone_router_update(
            replacement_broker_port,
            zone_router_after,
            zone_router_epoch_after,
        )
        wait_for_automq_zone_router(replacement_broker_port, zone_router_after)

        wait_for_automq_update_group(replacement_broker_port, link_id, group_id, False)
        wait_for_automq_manifest_groups(replacement_broker_port, 0)

        stream_set_object_id = wait_for_automq_prepare_s3_object(
            replacement_broker_port,
            stream_owner_node_id,
        )
        wait_for_automq_commit_stream_set_object(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            stream_set_object_id,
            10,
            20,
            1,
        )
        wait_for_automq_stream(
            replacement_broker_port,
            stream_id,
            "OPENED",
            1,
            0,
            20,
            expected_tags=stream_tags_before,
        )
        wait_for_automq_opening_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=1,
            expected_start_offset=0,
            expected_end_offset=20,
        )
        wait_for_automq_manifest_streams(replacement_broker_port, 2)

        wait_for_automq_close_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            1,
        )
        wait_for_automq_stream(
            replacement_broker_port,
            stream_id,
            "CLOSED",
            1,
            0,
            20,
            expected_tags=stream_tags_before,
        )
        wait_for_automq_opening_stream_missing(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
        )
        wait_for_automq_open_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            2,
            tags=stream_tags_after,
        )
        wait_for_automq_stream(
            replacement_broker_port,
            stream_id,
            "OPENED",
            2,
            0,
            20,
            expected_tags=stream_tags_after,
        )
        wait_for_automq_trim_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            2,
            5,
        )
        wait_for_automq_stream(
            replacement_broker_port,
            stream_id,
            "OPENED",
            2,
            5,
            20,
            expected_tags=stream_tags_after,
        )
        wait_for_automq_opening_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=2,
            expected_start_offset=5,
            expected_end_offset=20,
        )
        wait_for_automq_open_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            2,
            tags=stream_tags_cleared,
        )
        wait_for_automq_stream(
            replacement_broker_port,
            stream_id,
            "OPENED",
            2,
            5,
            20,
            expected_tags=stream_tags_cleared,
        )
        wait_for_automq_opening_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=2,
            expected_start_offset=5,
            expected_end_offset=20,
        )
        wait_for_automq_delete_stream(
            replacement_broker_port,
            stream_owner_node_id,
            deleted_stream_id,
            1,
        )
        wait_for_automq_stream_missing(replacement_broker_port, deleted_stream_id)
        wait_for_automq_opening_stream_missing(
            replacement_broker_port,
            stream_owner_node_id,
            deleted_stream_id,
        )
        wait_for_automq_manifest_streams(replacement_broker_port, 1)
        wait_for_automq_partition_snapshot(replacement_broker_port, 2, 0)

        shutil.rmtree(os.path.join(tmp, f"automq-combined-{leader_id}"), ignore_errors=True)
        processes[leader_id] = start_combined_node(
            tmp,
            leader_id,
            controller_ports[leader_id],
            broker_ports[leader_id],
            voters,
        )
        wait_for_ready(
            processes[leader_id]["proc"],
            processes[leader_id]["port"],
            processes[leader_id]["log_path"],
        )
        wait_for_broker_ready(
            processes[leader_id]["proc"],
            processes[leader_id]["broker_port"],
            processes[leader_id]["log_path"],
        )
        wait_for_all_alive_to_report(processes, replacement_leader)
        wait_for_automq_kv(processes[leader_id]["broker_port"], key, value_after)
        wait_for_automq_kv_missing(processes[leader_id]["broker_port"], delete_key)
        wait_for_automq_zone_router(processes[leader_id]["broker_port"], zone_router_after)
        wait_for_automq_manifest_groups(processes[leader_id]["broker_port"], 0)
        wait_for_automq_stream(
            processes[leader_id]["broker_port"],
            stream_id,
            "OPENED",
            2,
            5,
            20,
            expected_tags=stream_tags_cleared,
        )
        wait_for_automq_opening_stream(
            processes[leader_id]["broker_port"],
            stream_owner_node_id,
            stream_id,
            expected_epoch=2,
            expected_start_offset=5,
            expected_end_offset=20,
        )
        wait_for_automq_stream_missing(processes[leader_id]["broker_port"], deleted_stream_id)
        wait_for_automq_opening_stream_missing(
            processes[leader_id]["broker_port"],
            stream_owner_node_id,
            deleted_stream_id,
        )
        wait_for_automq_manifest_streams(processes[leader_id]["broker_port"], 1)
        wait_for_automq_partition_snapshot(processes[leader_id]["broker_port"], 2, 0)
        wait_for_automq_node(
            processes[leader_id]["broker_port"],
            registered_node_id,
            registered_node_epoch_after,
            registered_wal_config,
            expected_tags=registered_node_tags_cleared,
        )
        wait_for_automq_license(processes[leader_id]["broker_port"], license_value)

        return {
            "old_leader": leader_id,
            "new_leader": replacement_leader,
            "stream_id": stream_id,
            "deleted_stream_id": deleted_stream_id,
            "stream_set_object_id": stream_set_object_id,
            "registered_node_id": registered_node_id,
            "zone_router_epoch": zone_router_epoch_after,
            "old_leader_fresh_rejoin": True,
            "reassignment_topic": reassignment_result["topic"],
            "reassignment_target": reassignment_result["target_id"],
            "reassignment_target_offset": reassignment_result["target_offset"],
            "epoch": after["leader_epoch"],
        }
    finally:
        for info in processes.values():
            stop_process(info.get("proc"))


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_KRAFT_FAILOVER_TESTS=1 to run KRaft failover harness")
        return 0
    if not os.path.exists(ZMQ_BIN):
        raise TestError(f"broker binary not found: {ZMQ_BIN}")
    validate_required_network_phase_coverage()

    tmp = tempfile.mkdtemp(prefix="zmq-kraft-failover-")
    processes = {}
    broker = None
    try:
        ports = {node_id: PORT_BASE + node_id for node_id in range(3)}
        voters = ",".join(f"{node_id}@127.0.0.1:{port}" for node_id, port in ports.items())

        for node_id, port in ports.items():
            processes[node_id] = start_controller(tmp, node_id, port, voters)
        for node_id, info in processes.items():
            wait_for_ready(info["proc"], info["port"], info["log_path"])

        leader_id, initial = wait_for_leader(processes)
        if leader_id not in processes:
            raise TestError(f"discovered leader {leader_id}, expected one of {sorted(processes)}")
        if sorted(initial["voters"]) != [0, 1, 2]:
            raise TestError(f"unexpected voter set from DescribeQuorum: {initial['voters']}")

        broker = start_broker(tmp, voters)
        wait_for_broker_ready(broker["proc"], broker["port"], broker["log_path"])
        topic = f"kraft-failover-{os.getpid()}-{int(time.time())}"
        group = f"kraft-failover-group-{os.getpid()}-{int(time.time())}"
        offset_delete_group = f"{group}-offset-delete"
        delete_groups_group = f"{group}-delete-groups"
        txn_offset_group = f"{group}-txn-offset"
        txn_topic = f"{topic}-txn"
        idempotent_topic = f"{topic}-idempotent"
        expected_payloads = []
        wait_for_topic(broker["port"], topic)
        wait_for_topic(broker["port"], txn_topic)
        wait_for_topic(broker["port"], idempotent_topic)
        expected_payloads.append(b"r0")
        first_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        committed_offset = first_offset + 1
        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_offset_commit(broker["port"], group, topic, committed_offset)
        offset_delete_group_state = wait_for_group_stable(
            broker["port"],
            offset_delete_group,
        )
        wait_for_offset_commit(
            broker["port"],
            offset_delete_group,
            topic,
            committed_offset,
        )
        wait_for_offset_delete(broker["port"], offset_delete_group, topic)
        delete_groups_state = wait_for_group_stable(
            broker["port"],
            delete_groups_group,
        )
        leave_group(broker["port"], delete_groups_state, 6850)
        wait_for_offset_commit(
            broker["port"],
            delete_groups_group,
            topic,
            committed_offset,
        )
        wait_for_group_delete(broker["port"], delete_groups_group, topic)
        txn_offset_committed_offset = committed_offset
        txn_offset_txn = wait_for_offset_transaction_begin(
            broker["port"],
            f"{group}-txn-offset",
            txn_offset_group,
            topic,
            txn_offset_committed_offset,
        )
        idempotent_transactional_id = f"{group}-idempotent"
        idempotent_identity = wait_for_init_producer_id(
            broker["port"],
            idempotent_transactional_id,
        )
        controller_failover_txn = wait_for_transaction_begin(
            broker["port"],
            f"{group}-controller-failover",
            txn_topic,
        )
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "Ongoing", txn_topic
        )
        abort_failover_txn = wait_for_transaction_begin(
            broker["port"],
            f"{group}-abort-failover",
            txn_topic,
        )
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "Ongoing", txn_topic
        )
        classic_group_state = wait_for_group_stable(
            broker["port"],
            f"{group}-classic",
        )
        wait_for_group_description(broker["port"], classic_group_state)
        wait_for_consumer_group_description(broker["port"], classic_group_state)
        wait_for_list_groups(broker["port"], classic_group_state)
        wait_for_coordinator_discovery(
            broker["port"],
            classic_group_state["group_id"],
            controller_failover_txn["transactional_id"],
        )
        kip848_group_state = wait_for_consumer_group_heartbeat_join(
            broker["port"],
            f"{group}-kip848",
            topic,
        )
        kip848_committed_offset = committed_offset
        kip848_offset_metadata = "kraft-failover-kip848"
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_offset_commit_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )

        network_partition_result = run_network_partition_matrix(
            processes, broker, topic, expected_payloads, leader_id
        )
        if network_partition_result is not None:
            leader_id, initial = wait_for_leader(processes)
        wait_for_committed_offset(broker["port"], group, topic, committed_offset)
        wait_for_committed_offset(broker["port"], offset_delete_group, topic, -1)
        wait_for_committed_offset_error(
            broker["port"], delete_groups_group, topic, ERROR_GROUP_ID_NOT_FOUND
        )
        wait_for_committed_offset(
            broker["port"], txn_offset_group, topic, txn_offset_committed_offset
        )
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )
        wait_for_group_heartbeat(broker["port"], offset_delete_group_state)
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "Ongoing", txn_topic
        )
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "Ongoing", txn_topic
        )
        wait_for_group_heartbeat(broker["port"], classic_group_state)
        wait_for_group_description(broker["port"], classic_group_state)
        wait_for_consumer_group_description(broker["port"], classic_group_state)
        wait_for_list_groups(broker["port"], classic_group_state)
        wait_for_coordinator_discovery(
            broker["port"],
            classic_group_state["group_id"],
            controller_failover_txn["transactional_id"],
        )
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_offset_commit_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )

        stop_process(processes[leader_id]["proc"], crash=True)
        replacement_leader, after = wait_for_leader(processes, forbidden_leaders={leader_id})
        alive = {node_id for node_id, info in processes.items() if info["proc"].poll() is None}
        if replacement_leader not in alive:
            raise TestError(f"replacement leader {replacement_leader} is not alive; alive={sorted(alive)}")
        if after["leader_epoch"] <= initial["leader_epoch"]:
            raise TestError(f"leader epoch did not advance: before={initial} after={after}")

        wait_for_all_alive_to_report(processes, replacement_leader)
        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_committed_offset(broker["port"], group, topic, committed_offset)
        wait_for_committed_offset(broker["port"], offset_delete_group, topic, -1)
        wait_for_committed_offset_error(
            broker["port"], delete_groups_group, topic, ERROR_GROUP_ID_NOT_FOUND
        )
        wait_for_committed_offset(
            broker["port"], txn_offset_group, topic, txn_offset_committed_offset
        )
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )
        wait_for_group_heartbeat(broker["port"], offset_delete_group_state)
        wait_for_transaction_end(broker["port"], txn_offset_txn)
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "Ongoing", txn_topic
        )
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "Ongoing", txn_topic
        )
        wait_for_transaction_end(broker["port"], abort_failover_txn, committed=False)
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "CompleteAbort"
        )
        wait_for_transaction_end(broker["port"], controller_failover_txn)
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "CompleteCommit"
        )
        wait_for_group_heartbeat(broker["port"], classic_group_state)
        wait_for_group_description(broker["port"], classic_group_state)
        wait_for_consumer_group_description(broker["port"], classic_group_state)
        wait_for_list_groups(broker["port"], classic_group_state)
        wait_for_coordinator_discovery(
            broker["port"],
            classic_group_state["group_id"],
            controller_failover_txn["transactional_id"],
        )
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_offset_commit_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        expected_payloads.append(b"r1")
        second_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        if second_offset <= first_offset:
            raise TestError(f"broker did not continue after failover: {second_offset} <= {first_offset}")
        wait_for_payloads(broker["port"], topic, expected_payloads)
        committed_offset = second_offset + 1
        wait_for_offset_commit(broker["port"], group, topic, committed_offset)
        post_failover_txn = wait_for_transaction_begin(
            broker["port"],
            f"{group}-post-failover",
            txn_topic,
        )
        wait_for_transaction_end(broker["port"], post_failover_txn)

        shutil.rmtree(os.path.join(tmp, f"controller-{leader_id}"), ignore_errors=True)
        processes[leader_id] = start_controller(tmp, leader_id, ports[leader_id], voters)
        wait_for_ready(
            processes[leader_id]["proc"],
            processes[leader_id]["port"],
            processes[leader_id]["log_path"],
        )
        wait_for_all_alive_to_report(processes, replacement_leader)
        rejoined_quorum = describe_quorum(processes[leader_id]["port"], 5100 + leader_id)
        if rejoined_quorum["leader_id"] != replacement_leader:
            raise TestError(
                f"restarted old leader {leader_id} did not rejoin leader "
                f"{replacement_leader}: {rejoined_quorum}"
            )

        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_committed_offset(broker["port"], group, topic, committed_offset)
        wait_for_committed_offset(broker["port"], offset_delete_group, topic, -1)
        wait_for_committed_offset_error(
            broker["port"], delete_groups_group, topic, ERROR_GROUP_ID_NOT_FOUND
        )
        wait_for_committed_offset(
            broker["port"], txn_offset_group, topic, txn_offset_committed_offset
        )
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )
        wait_for_group_heartbeat(broker["port"], offset_delete_group_state)
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "CompleteCommit"
        )
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "CompleteAbort"
        )
        wait_for_group_heartbeat(broker["port"], classic_group_state)
        wait_for_group_description(broker["port"], classic_group_state)
        wait_for_consumer_group_description(broker["port"], classic_group_state)
        wait_for_list_groups(broker["port"], classic_group_state)
        wait_for_coordinator_discovery(
            broker["port"],
            classic_group_state["group_id"],
            controller_failover_txn["transactional_id"],
        )
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_offset_commit_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        expected_payloads.append(b"r2")
        third_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        if third_offset <= second_offset:
            raise TestError(
                f"broker did not continue after old leader rejoin: {third_offset} <= {second_offset}"
            )
        wait_for_payloads(broker["port"], topic, expected_payloads)
        committed_offset = third_offset + 1
        wait_for_offset_commit(broker["port"], group, topic, committed_offset)

        alive = {node_id for node_id, info in processes.items() if info["proc"].poll() is None}
        restart_controller_id = next(
            node_id for node_id in sorted(alive)
            if node_id != replacement_leader and node_id != leader_id
        )
        stop_process(processes[restart_controller_id]["proc"])
        processes[restart_controller_id] = start_controller(
            tmp, restart_controller_id, ports[restart_controller_id], voters
        )
        wait_for_ready(
            processes[restart_controller_id]["proc"],
            processes[restart_controller_id]["port"],
            processes[restart_controller_id]["log_path"],
        )
        wait_for_all_alive_to_report(processes, replacement_leader)
        restarted_quorum = describe_quorum(
            processes[restart_controller_id]["port"], 5000 + restart_controller_id
        )
        if restarted_quorum["leader_id"] != replacement_leader:
            raise TestError(
                f"restarted controller {restart_controller_id} did not rejoin leader "
                f"{replacement_leader}: {restarted_quorum}"
            )

        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_committed_offset(broker["port"], group, topic, committed_offset)
        wait_for_committed_offset(broker["port"], offset_delete_group, topic, -1)
        wait_for_committed_offset_error(
            broker["port"], delete_groups_group, topic, ERROR_GROUP_ID_NOT_FOUND
        )
        wait_for_committed_offset(
            broker["port"], txn_offset_group, topic, txn_offset_committed_offset
        )
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )
        wait_for_group_heartbeat(broker["port"], offset_delete_group_state)
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "CompleteCommit"
        )
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "CompleteAbort"
        )
        wait_for_group_heartbeat(broker["port"], classic_group_state)
        wait_for_group_description(broker["port"], classic_group_state)
        wait_for_consumer_group_description(broker["port"], classic_group_state)
        wait_for_list_groups(broker["port"], classic_group_state)
        wait_for_coordinator_discovery(
            broker["port"],
            classic_group_state["group_id"],
            controller_failover_txn["transactional_id"],
        )
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_offset_commit_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        expected_payloads.append(b"r3")
        fourth_offset = wait_for_produce(
            broker["port"], topic, expected_payloads[-1]
        )
        if fourth_offset <= third_offset:
            raise TestError(
                f"broker did not continue after controller restart: {fourth_offset} <= {third_offset}"
            )
        wait_for_payloads(broker["port"], topic, expected_payloads)
        committed_offset = fourth_offset + 1
        wait_for_offset_commit(broker["port"], group, topic, committed_offset)
        broker_restart_txn = wait_for_transaction_begin(
            broker["port"],
            f"{group}-broker-restart",
            txn_topic,
        )
        idempotent_payload_0 = b"idempotent-seq-0"
        idempotent_payload_1 = b"idempotent-seq-1"
        idempotent_stale_payload = b"idempotent-stale-epoch"
        idempotent_batch_0 = build_record_batch(
            idempotent_payload_0,
            idempotent_identity["producer_id"],
            idempotent_identity["producer_epoch"],
            0,
        )
        idempotent_first = wait_for_record_batch_result(
            broker["port"],
            idempotent_topic,
            idempotent_batch_0,
            0,
        )
        if idempotent_first["base_offset"] < 0:
            raise TestError(f"initial idempotent produce returned {idempotent_first}")
        wait_for_payload_counts(
            broker["port"],
            idempotent_topic,
            {idempotent_payload_0: 1},
        )

        stop_process(broker["proc"])
        broker = start_broker(tmp, voters)
        wait_for_broker_ready(broker["proc"], broker["port"], broker["log_path"])
        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_committed_offset(broker["port"], group, topic, committed_offset)
        wait_for_committed_offset(broker["port"], offset_delete_group, topic, -1)
        wait_for_committed_offset_error(
            broker["port"], delete_groups_group, topic, ERROR_GROUP_ID_NOT_FOUND
        )
        wait_for_committed_offset(
            broker["port"], txn_offset_group, topic, txn_offset_committed_offset
        )
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )
        wait_for_group_heartbeat(broker["port"], offset_delete_group_state)
        wait_for_transaction_introspection(
            broker["port"], controller_failover_txn, "CompleteCommit"
        )
        wait_for_transaction_introspection(
            broker["port"], abort_failover_txn, "CompleteAbort"
        )
        wait_for_transaction_introspection(
            broker["port"], broker_restart_txn, "Ongoing", txn_topic
        )
        wait_for_transaction_end(broker["port"], broker_restart_txn)
        wait_for_group_heartbeat(broker["port"], classic_group_state)
        wait_for_group_description(broker["port"], classic_group_state)
        wait_for_consumer_group_description(broker["port"], classic_group_state)
        wait_for_list_groups(broker["port"], classic_group_state)
        wait_for_coordinator_discovery(
            broker["port"],
            classic_group_state["group_id"],
            controller_failover_txn["transactional_id"],
        )
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_offset_commit_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        wait_for_offset_fetch_v9_member_checkpoint(
            broker["port"],
            kip848_group_state,
            topic,
            kip848_committed_offset,
            kip848_offset_metadata,
        )
        duplicate_idempotent = wait_for_record_batch_result(
            broker["port"],
            idempotent_topic,
            idempotent_batch_0,
            0,
        )
        if duplicate_idempotent["base_offset"] != -1:
            raise TestError(
                f"duplicate idempotent batch appended after broker restart: {duplicate_idempotent}"
            )
        wait_for_payload_counts(
            broker["port"],
            idempotent_topic,
            {idempotent_payload_0: 1},
        )
        idempotent_batch_1 = build_record_batch(
            idempotent_payload_1,
            idempotent_identity["producer_id"],
            idempotent_identity["producer_epoch"],
            1,
        )
        idempotent_second = wait_for_record_batch_result(
            broker["port"],
            idempotent_topic,
            idempotent_batch_1,
            0,
        )
        if idempotent_second["base_offset"] <= idempotent_first["base_offset"]:
            raise TestError(
                f"idempotent producer did not advance after restart: "
                f"{idempotent_second} <= {idempotent_first}"
            )
        wait_for_payload_counts(
            broker["port"],
            idempotent_topic,
            {idempotent_payload_0: 1, idempotent_payload_1: 1},
        )
        bumped_identity = wait_for_init_producer_id(
            broker["port"],
            idempotent_transactional_id,
        )
        if (
            bumped_identity["producer_id"] != idempotent_identity["producer_id"]
            or bumped_identity["producer_epoch"] <= idempotent_identity["producer_epoch"]
        ):
            raise TestError(
                f"InitProducerId did not bump producer epoch: "
                f"before={idempotent_identity} after={bumped_identity}"
            )
        stale_epoch_batch = build_record_batch(
            idempotent_stale_payload,
            idempotent_identity["producer_id"],
            idempotent_identity["producer_epoch"],
            2,
        )
        stale_epoch_result = wait_for_record_batch_result(
            broker["port"],
            idempotent_topic,
            stale_epoch_batch,
            47,
        )
        if stale_epoch_result["base_offset"] != -1:
            raise TestError(f"stale epoch batch returned offset: {stale_epoch_result}")
        next_epoch_batch = build_record_batch(
            b"idempotent-next-epoch",
            bumped_identity["producer_id"],
            bumped_identity["producer_epoch"],
            0,
        )
        next_epoch_result = wait_for_record_batch_result(
            broker["port"],
            idempotent_topic,
            next_epoch_batch,
            0,
        )
        if next_epoch_result["base_offset"] <= idempotent_second["base_offset"]:
            raise TestError(
                f"bumped idempotent producer did not append: "
                f"{next_epoch_result} <= {idempotent_second}"
            )
        wait_for_payload_counts(
            broker["port"],
            idempotent_topic,
            {
                idempotent_payload_0: 1,
                idempotent_payload_1: 1,
                idempotent_stale_payload: 0,
                b"idempotent-next-epoch": 1,
            },
        )
        expected_payloads.append(b"r4")
        fifth_offset = wait_for_produce(
            broker["port"], topic, expected_payloads[-1]
        )
        if fifth_offset <= fourth_offset:
            raise TestError(
                f"broker did not continue after broker restart: {fifth_offset} <= {fourth_offset}"
            )
        wait_for_payloads(broker["port"], topic, expected_payloads)
        committed_offset = fifth_offset + 1
        wait_for_offset_commit(broker["port"], group, topic, committed_offset)
        wait_for_offset_fetch_grouped_checkpoint(
            broker["port"],
            group,
            topic,
            committed_offset,
            offset_delete_group,
            delete_groups_group,
            txn_offset_group,
            txn_offset_committed_offset,
        )

        automq_result = run_automq_metadata_failover_scenario(tmp)

        print(
            "ok: KRaft controller failover harness passed "
            f"(old_leader={leader_id}, new_leader={replacement_leader}, "
            f"restarted_controller={restart_controller_id}, "
            f"old_leader_rejoined=true, old_leader_fresh_rejoin=true, "
            f"epoch={after['leader_epoch']}, "
            f"automq_old_leader={automq_result['old_leader']}, "
            f"automq_new_leader={automq_result['new_leader']}, "
            f"automq_stream_id={automq_result['stream_id']}, "
            f"automq_deleted_stream_id={automq_result['deleted_stream_id']}, "
            f"automq_stream_set_object_id={automq_result['stream_set_object_id']}, "
            f"automq_node_id={automq_result['registered_node_id']}, "
            f"automq_zone_router_epoch={automq_result['zone_router_epoch']}, "
            f"reassignment_topic={automq_result['reassignment_topic']}, "
            f"reassignment_target={automq_result['reassignment_target']}, "
            f"reassignment_target_offset={automq_result['reassignment_target_offset']}, "
            f"committed_offset={committed_offset}, "
            f"transactions_checked=5, "
            f"transaction_introspection_checked=true, "
            f"transaction_abort_checked=true, "
            f"txn_offset_commit_checked=true, "
            f"offset_fetch_v8_grouped_checked=true, "
            f"idempotent_producer_fencing=true, "
            f"delete_groups_checked=true, "
            f"classic_group_heartbeats=true, "
            f"group_describe_checked=true, "
            f"consumer_group_describe_checked=true, "
            f"list_groups_checked=true, "
            f"find_coordinator_checked=true, "
            f"consumer_group_heartbeat_checked=true, "
            f"kip848_describe_checked=true, "
            f"offset_commit_v9_member_checked=true, "
            f"offset_fetch_v9_member_checked=true, "
            f"network_partition={network_partition_result}, "
            f"automq_old_leader_fresh_rejoin={automq_result['old_leader_fresh_rejoin']})"
        )
        return 0
    finally:
        if broker is not None:
            stop_process(broker.get("proc"))
        for info in processes.values():
            stop_process(info.get("proc"))
        shutil.rmtree(tmp, ignore_errors=True)


def self_test():
    class DummyProc:
        def __init__(self, pid):
            self.pid = pid

    old_env = os.environ.copy()
    try:
        os.environ.pop("ZMQ_KRAFT_NETWORK_DOWN", None)
        os.environ.pop("ZMQ_KRAFT_NETWORK_UP", None)
        if network_hooks_configured():
            raise TestError("network hooks unexpectedly configured")

        os.environ["ZMQ_KRAFT_NETWORK_DOWN"] = "true"
        os.environ["ZMQ_KRAFT_NETWORK_UP"] = "true"
        if not network_hooks_configured():
            raise TestError("network hooks were not detected")
        phases = selected_network_partition_phases()
        if len(phases) != 1 or phases[0]["name"] != "controller-broker":
            raise TestError(f"default network phase selection failed: {phases}")

        os.environ["ZMQ_KRAFT_NETWORK_MATRIX"] = "leader-isolation, broker-link"
        os.environ["ZMQ_KRAFT_NETWORK_BROKER_LINK_EXPECT"] = "survive"
        os.environ["ZMQ_KRAFT_NETWORK_BROKER_LINK_DOWN"] = "true"
        os.environ["ZMQ_KRAFT_NETWORK_BROKER_LINK_UP"] = "true"
        phases = selected_network_partition_phases()
        if [phase["name"] for phase in phases] != ["leader-isolation", "broker-link"]:
            raise TestError(f"network matrix phase parsing failed: {phases}")
        if phases[0]["expect"] != "fail" or phases[1]["expect"] != "survive":
            raise TestError(f"network matrix expectation parsing failed: {phases}")
        os.environ["ZMQ_KRAFT_REQUIRED_NETWORK_PHASES"] = "leader-isolation,broker-link"
        validate_required_network_phase_coverage()
        os.environ["ZMQ_KRAFT_REQUIRED_NETWORK_PHASES"] = "missing-phase"
        try:
            validate_required_network_phase_coverage()
            raise TestError("missing required KRaft network phase was not rejected")
        except TestError as exc:
            if "required KRaft network phases" not in str(exc):
                raise

        processes = {
            0: {"proc": DummyProc(1000), "port": 39093},
            1: {"proc": DummyProc(1001), "port": 39094},
            2: {"proc": DummyProc(1002), "port": 39095},
        }
        broker = {"proc": DummyProc(2000), "port": 39092}
        env = hook_context_env(processes, broker, 1)
        if env["ZMQ_KRAFT_ACTIVE_LEADER_ID"] != "1":
            raise TestError("hook leader context failed")
        if env["ZMQ_KRAFT_CONTROLLER_PORTS"] != "0:39093,1:39094,2:39095":
            raise TestError("hook controller port context failed")
        if env["ZMQ_KRAFT_BROKER_PID"] != "2000":
            raise TestError("hook broker pid context failed")
        env["ZMQ_KRAFT_NETWORK_PHASE"] = "leader-isolation"
        env["ZMQ_KRAFT_NETWORK_PHASE_INDEX"] = "0"
        env["ZMQ_KRAFT_NETWORK_EXPECT"] = "fail"
        run_network_hook("self-test:down", phases[0]["down"], env)
        run_network_hook("self-test:up", phases[0]["up"], env)

        commit_fixture = struct.pack(">ii", 42, 0)
        commit_fixture += struct.pack(">i", 1)
        commit_fixture += write_string("offset-self-test")
        commit_fixture += struct.pack(">iih", 1, 0, 0)
        if parse_offset_commit_response(commit_fixture, 42, "offset-self-test") != 0:
            raise TestError("OffsetCommit fixture parser failed")
        commit_v9_fixture = struct.pack(">i", 242)
        commit_v9_fixture += b"\x00"  # response header tagged fields
        commit_v9_fixture += struct.pack(">i", 0)
        commit_v9_fixture += write_compact_array_len(1)
        commit_v9_fixture += write_compact_string("offset-v9-self-test")
        commit_v9_fixture += write_compact_array_len(1)
        commit_v9_fixture += struct.pack(">ih", 0, ERROR_FENCED_MEMBER_EPOCH)
        commit_v9_fixture += b"\x00"  # partition tagged fields
        commit_v9_fixture += b"\x00"  # topic tagged fields
        commit_v9_fixture += b"\x00"  # response tagged fields
        if (
            parse_offset_commit_flexible_response(
                commit_v9_fixture, 242, "offset-v9-self-test"
            )
            != ERROR_FENCED_MEMBER_EPOCH
        ):
            raise TestError("OffsetCommit v9 fixture parser failed")

        fetch_fixture = struct.pack(">i", 43)
        fetch_fixture += struct.pack(">i", 1)
        fetch_fixture += write_string("offset-self-test")
        fetch_fixture += struct.pack(">iiq", 1, 0, 7)
        fetch_fixture += write_string("kraft-failover")
        fetch_fixture += struct.pack(">h", 0)
        fetched = parse_offset_fetch_response(fetch_fixture, 43, "offset-self-test")
        if fetched["offset"] != 7 or fetched["metadata"] != "kraft-failover":
            raise TestError(f"OffsetFetch fixture parser failed: {fetched}")
        missing_fetch_fixture = struct.pack(">i", 143)
        missing_fetch_fixture += struct.pack(">i", 1)
        missing_fetch_fixture += write_string("offset-missing-self-test")
        missing_fetch_fixture += struct.pack(">iiq", 1, 0, -1)
        missing_fetch_fixture += write_string("")
        missing_fetch_fixture += struct.pack(">h", ERROR_GROUP_ID_NOT_FOUND)
        missing_fetch = parse_offset_fetch_response_status(
            missing_fetch_fixture, 143, "offset-missing-self-test"
        )
        if (
            missing_fetch["offset"] != -1
            or missing_fetch["error_code"] != ERROR_GROUP_ID_NOT_FOUND
        ):
            raise TestError(f"OffsetFetch error fixture parser failed: {missing_fetch}")
        grouped_fetch_fixture = struct.pack(">i", 243)
        grouped_fetch_fixture += b"\x00"  # response header tagged fields
        grouped_fetch_fixture += struct.pack(">i", 0)
        grouped_fetch_fixture += write_compact_array_len(2)
        grouped_fetch_fixture += write_compact_string("grouped-offset-self-test")
        grouped_fetch_fixture += write_compact_array_len(1)
        grouped_fetch_fixture += write_compact_string("offset-self-test")
        grouped_fetch_fixture += write_compact_array_len(1)
        grouped_fetch_fixture += struct.pack(">iqi", 0, 9, -1)
        grouped_fetch_fixture += write_compact_string("kraft-failover")
        grouped_fetch_fixture += struct.pack(">h", 0)
        grouped_fetch_fixture += b"\x00"  # partition tagged fields
        grouped_fetch_fixture += b"\x00"  # topic tagged fields
        grouped_fetch_fixture += struct.pack(">h", 0)
        grouped_fetch_fixture += b"\x00"  # group tagged fields
        grouped_fetch_fixture += write_compact_string("missing-grouped-offset-self-test")
        grouped_fetch_fixture += write_compact_array_len(0)
        grouped_fetch_fixture += struct.pack(">h", ERROR_GROUP_ID_NOT_FOUND)
        grouped_fetch_fixture += b"\x00"  # group tagged fields
        grouped_fetch_fixture += b"\x00"  # response tagged fields
        grouped_fetch = parse_offset_fetch_grouped_response(
            grouped_fetch_fixture, 243
        )
        if (
            len(grouped_fetch) != 2
            or grouped_fetch[0]["group_id"] != "grouped-offset-self-test"
            or grouped_fetch[0]["topics"][0]["partitions"][0]["offset"] != 9
            or grouped_fetch[0]["topics"][0]["partitions"][0]["metadata"]
            != "kraft-failover"
            or grouped_fetch[1]["group_id"] != "missing-grouped-offset-self-test"
            or grouped_fetch[1]["topics"]
            or grouped_fetch[1]["error_code"] != ERROR_GROUP_ID_NOT_FOUND
        ):
            raise TestError(
                f"OffsetFetch v8 grouped fixture parser failed: {grouped_fetch}"
            )
        v9_member_fetch_fixture = struct.pack(">i", 244)
        v9_member_fetch_fixture += b"\x00"  # response header tagged fields
        v9_member_fetch_fixture += struct.pack(">i", 0)
        v9_member_fetch_fixture += write_compact_array_len(2)
        for group_id, error_code in (
            ("v9-unknown-member-self-test", ERROR_UNKNOWN_MEMBER_ID),
            ("v9-fenced-member-self-test", ERROR_FENCED_MEMBER_EPOCH),
        ):
            v9_member_fetch_fixture += write_compact_string(group_id)
            v9_member_fetch_fixture += write_compact_array_len(0)
            v9_member_fetch_fixture += struct.pack(">h", error_code)
            v9_member_fetch_fixture += b"\x00"  # group tagged fields
        v9_member_fetch_fixture += b"\x00"  # response tagged fields
        v9_member_fetch = parse_offset_fetch_grouped_response(
            v9_member_fetch_fixture, 244
        )
        if (
            len(v9_member_fetch) != 2
            or v9_member_fetch[0]["error_code"] != ERROR_UNKNOWN_MEMBER_ID
            or v9_member_fetch[1]["error_code"] != ERROR_FENCED_MEMBER_EPOCH
            or v9_member_fetch[0]["topics"]
            or v9_member_fetch[1]["topics"]
        ):
            raise TestError(
                f"OffsetFetch v9 member fixture parser failed: {v9_member_fetch}"
            )

        delete_fixture = struct.pack(">ihi", 144, 0, 0)
        delete_fixture += struct.pack(">i", 1)
        delete_fixture += write_string("offset-delete-self-test")
        delete_fixture += struct.pack(">iih", 1, 0, 0)
        if parse_offset_delete_response(delete_fixture, 144, "offset-delete-self-test") != 0:
            raise TestError("OffsetDelete fixture parser failed")

        delete_groups_fixture = struct.pack(">ii", 145, 0)
        delete_groups_fixture += struct.pack(">i", 1)
        delete_groups_fixture += write_string("delete-groups-self-test")
        delete_groups_fixture += struct.pack(">h", 0)
        if parse_delete_groups_response(delete_groups_fixture, 145, "delete-groups-self-test") != 0:
            raise TestError("DeleteGroups fixture parser failed")

        init_fixture = struct.pack(">iihqh", 44, 0, 0, 1000, 0)
        identity = parse_init_producer_id_response(init_fixture, 44)
        if identity["producer_id"] != 1000 or identity["producer_epoch"] != 0:
            raise TestError(f"InitProducerId fixture parser failed: {identity}")

        add_txn_fixture = struct.pack(">ii", 45, 0)
        add_txn_fixture += struct.pack(">i", 1)
        add_txn_fixture += write_string("txn-self-test")
        add_txn_fixture += struct.pack(">iih", 1, 0, 0)
        parse_add_partitions_to_txn_response(add_txn_fixture, 45, "txn-self-test")

        add_offsets_fixture = struct.pack(">iih", 51, 0, 0)
        parse_add_offsets_to_txn_response(add_offsets_fixture, 51)

        txn_offset_fixture = struct.pack(">ii", 52, 0)
        txn_offset_fixture += struct.pack(">i", 1)
        txn_offset_fixture += write_string("txn-offset-self-test")
        txn_offset_fixture += struct.pack(">iih", 1, 0, 0)
        parse_txn_offset_commit_response(txn_offset_fixture, 52, "txn-offset-self-test")

        list_txn_fixture = struct.pack(">i", 53)
        list_txn_fixture += b"\x00"  # response header tagged fields
        list_txn_fixture += struct.pack(">ih", 0, 0)
        list_txn_fixture += write_compact_array_len(0)
        list_txn_fixture += write_compact_array_len(1)
        list_txn_fixture += write_compact_string("introspection-self-test")
        list_txn_fixture += struct.pack(">q", 1001)
        list_txn_fixture += write_compact_string("Ongoing")
        list_txn_fixture += b"\x00"  # transaction tagged fields
        list_txn_fixture += b"\x00"  # response tagged fields
        listed = parse_list_transactions_response(list_txn_fixture, 53)
        if (
            len(listed) != 1
            or listed[0]["transactional_id"] != "introspection-self-test"
            or listed[0]["producer_id"] != 1001
            or listed[0]["transaction_state"] != "Ongoing"
        ):
            raise TestError(f"ListTransactions fixture parser failed: {listed}")

        describe_txn_fixture = struct.pack(">i", 54)
        describe_txn_fixture += b"\x00"  # response header tagged fields
        describe_txn_fixture += struct.pack(">i", 0)
        describe_txn_fixture += write_compact_array_len(1)
        describe_txn_fixture += struct.pack(">h", 0)
        describe_txn_fixture += write_compact_string("introspection-self-test")
        describe_txn_fixture += write_compact_string("Ongoing")
        describe_txn_fixture += struct.pack(">iqqh", 60000, 123456, 1001, 0)
        describe_txn_fixture += write_compact_array_len(1)
        describe_txn_fixture += write_compact_string("introspection-topic")
        describe_txn_fixture += write_compact_i32_array([0])
        describe_txn_fixture += b"\x00"  # topic tagged fields
        describe_txn_fixture += b"\x00"  # transaction tagged fields
        describe_txn_fixture += b"\x00"  # response tagged fields
        described = parse_describe_transactions_response(describe_txn_fixture, 54)
        if (
            len(described) != 1
            or described[0]["transactional_id"] != "introspection-self-test"
            or described[0]["producer_id"] != 1001
            or described[0]["transaction_state"] != "Ongoing"
            or described[0]["topics"][0]["topic"] != "introspection-topic"
            or described[0]["topics"][0]["partitions"] != [0]
        ):
            raise TestError(f"DescribeTransactions fixture parser failed: {described}")

        end_txn_fixture = struct.pack(">iih", 46, 0, 0)
        parse_end_txn_response(end_txn_fixture, 46)

        join_fixture = struct.pack(">ihi", 47, 0, 3)
        join_fixture += write_string("range")
        join_fixture += write_string("member-1")
        join_fixture += write_string("member-1")
        join_fixture += struct.pack(">i", 1)
        join_fixture += write_string("member-1")
        join_fixture += write_bytes(b"metadata")
        joined = parse_join_group_response(join_fixture, 47)
        if joined["generation_id"] != 3 or joined["member_id"] != "member-1":
            raise TestError(f"JoinGroup fixture parser failed: {joined}")

        sync_fixture = struct.pack(">ih", 48, 0)
        sync_fixture += write_bytes(b"assignment")
        if parse_sync_group_response(sync_fixture, 48) != b"assignment":
            raise TestError("SyncGroup fixture parser failed")

        heartbeat_fixture = struct.pack(">ih", 49, 0)
        parse_heartbeat_response(heartbeat_fixture, 49)

        describe_group_fixture = struct.pack(">i", 55)
        describe_group_fixture += struct.pack(">i", 1)
        describe_group_fixture += struct.pack(">h", 0)
        describe_group_fixture += write_string("describe-group-self-test")
        describe_group_fixture += write_string("Stable")
        describe_group_fixture += write_string("consumer")
        describe_group_fixture += write_string("range")
        describe_group_fixture += struct.pack(">i", 1)
        describe_group_fixture += write_string("member-1")
        describe_group_fixture += write_string("zmq-client")
        describe_group_fixture += write_string("/127.0.0.1")
        describe_group_fixture += write_bytes(b"range-metadata")
        describe_group_fixture += write_bytes(b"kraft-failover-assignment")
        described_groups = parse_describe_groups_response(describe_group_fixture, 55)
        if (
            len(described_groups) != 1
            or described_groups[0]["group_id"] != "describe-group-self-test"
            or described_groups[0]["group_state"] != "Stable"
            or described_groups[0]["members"][0]["member_id"] != "member-1"
            or described_groups[0]["members"][0]["member_assignment"]
            != b"kraft-failover-assignment"
        ):
            raise TestError(f"DescribeGroups fixture parser failed: {described_groups}")

        consumer_describe_topic_id = bytes(reversed(range(16)))
        consumer_group_describe_fixture = struct.pack(">i", 56)
        consumer_group_describe_fixture += b"\x00"  # response header tagged fields
        consumer_group_describe_fixture += struct.pack(">i", 0)
        consumer_group_describe_fixture += write_compact_array_len(1)
        consumer_group_describe_fixture += struct.pack(">h", 0)
        consumer_group_describe_fixture += write_compact_string(None)
        consumer_group_describe_fixture += write_compact_string(
            "consumer-group-describe-self-test"
        )
        consumer_group_describe_fixture += write_compact_string("Stable")
        consumer_group_describe_fixture += struct.pack(">ii", 3, 3)
        consumer_group_describe_fixture += write_compact_string("range")
        consumer_group_describe_fixture += write_compact_array_len(1)
        consumer_group_describe_fixture += write_compact_string("member-1")
        consumer_group_describe_fixture += write_compact_string(None)
        consumer_group_describe_fixture += write_compact_string(None)
        consumer_group_describe_fixture += struct.pack(">i", 3)
        consumer_group_describe_fixture += write_compact_string("zmq-client")
        consumer_group_describe_fixture += write_compact_string("/127.0.0.1")
        consumer_group_describe_fixture += write_compact_array_len(1)
        consumer_group_describe_fixture += write_compact_string("describe-topic")
        consumer_group_describe_fixture += write_compact_string(None)
        consumer_group_describe_fixture += write_compact_array_len(1)
        consumer_group_describe_fixture += consumer_describe_topic_id
        consumer_group_describe_fixture += write_compact_string("describe-topic")
        consumer_group_describe_fixture += write_compact_i32_array([0])
        consumer_group_describe_fixture += b"\x00"  # assignment topic tagged fields
        consumer_group_describe_fixture += b"\x00"  # assignment tagged fields
        consumer_group_describe_fixture += write_compact_array_len(1)
        consumer_group_describe_fixture += consumer_describe_topic_id
        consumer_group_describe_fixture += write_compact_string("describe-topic")
        consumer_group_describe_fixture += write_compact_i32_array([0])
        consumer_group_describe_fixture += b"\x00"  # target assignment topic tagged fields
        consumer_group_describe_fixture += b"\x00"  # target assignment tagged fields
        consumer_group_describe_fixture += b"\x00"  # member tagged fields
        consumer_group_describe_fixture += struct.pack(">i", -2147483648)
        consumer_group_describe_fixture += b"\x00"  # group tagged fields
        consumer_group_describe_fixture += b"\x00"  # response tagged fields
        consumer_described = parse_consumer_group_describe_response(
            consumer_group_describe_fixture, 56
        )
        if (
            len(consumer_described) != 1
            or consumer_described[0]["group_id"] != "consumer-group-describe-self-test"
            or consumer_described[0]["group_state"] != "Stable"
            or consumer_described[0]["group_epoch"] != 3
            or consumer_described[0]["assignor_name"] != "range"
            or consumer_described[0]["members"][0]["member_epoch"] != 3
            or consumer_described[0]["members"][0]["subscribed_topics"]
            != ["describe-topic"]
            or consumer_described[0]["members"][0]["assignment"]["topic_partitions"][0][
                "topic_id"
            ]
            != consumer_describe_topic_id
            or consumer_described[0]["members"][0]["assignment"]["topic_partitions"][0][
                "partitions"
            ]
            != [0]
        ):
            raise TestError(
                f"ConsumerGroupDescribe fixture parser failed: {consumer_described}"
            )

        list_groups_fixture = struct.pack(">i", 57)
        list_groups_fixture += b"\x00"  # response header tagged fields
        list_groups_fixture += struct.pack(">ih", 0, 0)
        list_groups_fixture += write_compact_array_len(1)
        list_groups_fixture += write_compact_string("list-groups-self-test")
        list_groups_fixture += write_compact_string("consumer")
        list_groups_fixture += write_compact_string("Stable")
        list_groups_fixture += write_compact_string("classic")
        list_groups_fixture += b"\x00"  # group tagged fields
        list_groups_fixture += b"\x00"  # response tagged fields
        listed_groups = parse_list_groups_response(list_groups_fixture, 57)
        if (
            listed_groups["error_code"] != 0
            or len(listed_groups["groups"]) != 1
            or listed_groups["groups"][0]["group_id"] != "list-groups-self-test"
            or listed_groups["groups"][0]["group_state"] != "Stable"
            or listed_groups["groups"][0]["group_type"] != "classic"
        ):
            raise TestError(f"ListGroups fixture parser failed: {listed_groups}")

        find_coordinator_fixture = struct.pack(">i", 58)
        find_coordinator_fixture += b"\x00"  # response header tagged fields
        find_coordinator_fixture += struct.pack(">i", 0)
        find_coordinator_fixture += write_compact_array_len(2)
        for coordinator_key in (
            "find-coordinator-group-self-test",
            "find-coordinator-txn-self-test",
        ):
            find_coordinator_fixture += write_compact_string(coordinator_key)
            find_coordinator_fixture += struct.pack(">i", 100)
            find_coordinator_fixture += write_compact_string("localhost")
            find_coordinator_fixture += struct.pack(">ih", BROKER_PORT, 0)
            find_coordinator_fixture += write_compact_string(None)
            find_coordinator_fixture += b"\x00"  # coordinator tagged fields
        find_coordinator_fixture += b"\x00"  # response tagged fields
        coordinators = parse_find_coordinator_response(find_coordinator_fixture, 58)
        if (
            len(coordinators) != 2
            or coordinators[0]["key"] != "find-coordinator-group-self-test"
            or coordinators[0]["node_id"] != 100
            or coordinators[0]["host"] != "localhost"
            or coordinators[0]["port"] != BROKER_PORT
            or coordinators[1]["key"] != "find-coordinator-txn-self-test"
            or coordinators[1]["error_code"] != 0
        ):
            raise TestError(f"FindCoordinator fixture parser failed: {coordinators}")

        heartbeat_topic_id = bytes(range(16))
        consumer_group_heartbeat_fixture = struct.pack(">i", 59)
        consumer_group_heartbeat_fixture += b"\x00"  # response header tagged fields
        consumer_group_heartbeat_fixture += struct.pack(">ih", 0, 0)
        consumer_group_heartbeat_fixture += write_compact_string(None)
        consumer_group_heartbeat_fixture += write_compact_string("kip848-member")
        consumer_group_heartbeat_fixture += struct.pack(">ii", 1, 3000)
        consumer_group_heartbeat_fixture += write_varint(1)  # assignment present
        consumer_group_heartbeat_fixture += write_compact_array_len(1)
        consumer_group_heartbeat_fixture += heartbeat_topic_id
        consumer_group_heartbeat_fixture += write_compact_i32_array([0])
        consumer_group_heartbeat_fixture += b"\x00"  # topic tagged fields
        consumer_group_heartbeat_fixture += b"\x00"  # assignment tagged fields
        consumer_group_heartbeat_fixture += b"\x00"  # response tagged fields
        heartbeat = parse_consumer_group_heartbeat_response(
            consumer_group_heartbeat_fixture, 59
        )
        if (
            heartbeat["error_code"] != 0
            or heartbeat["member_id"] != "kip848-member"
            or heartbeat["member_epoch"] != 1
            or heartbeat["heartbeat_interval_ms"] != 3000
            or heartbeat["assignment"]["topic_partitions"][0]["topic_id"]
            != heartbeat_topic_id
            or heartbeat["assignment"]["topic_partitions"][0]["partitions"] != [0]
        ):
            raise TestError(
                f"ConsumerGroupHeartbeat fixture parser failed: {heartbeat}"
            )

        leave_fixture = struct.pack(">ih", 50, 0)
        parse_leave_group_response(leave_fixture, 50)

        batch_fixture = build_record_batch(
            b"idempotent-fixture",
            1000,
            2,
            5,
            timestamp_ms=123456,
        )
        if len(batch_fixture) < 61:
            raise TestError("record batch fixture too short")
        if struct.unpack_from(">i", batch_fixture, 8)[0] != len(batch_fixture) - 12:
            raise TestError("record batch fixture length mismatch")
        if struct.unpack_from(">b", batch_fixture, 16)[0] != 2:
            raise TestError("record batch fixture magic mismatch")
        if struct.unpack_from(">q", batch_fixture, 43)[0] != 1000:
            raise TestError("record batch fixture producer id mismatch")
        if struct.unpack_from(">h", batch_fixture, 51)[0] != 2:
            raise TestError("record batch fixture producer epoch mismatch")
        if struct.unpack_from(">i", batch_fixture, 53)[0] != 5:
            raise TestError("record batch fixture base sequence mismatch")

        produce_v9_fixture = struct.pack(">i", 50)
        produce_v9_fixture += b"\x00"  # response header tagged fields
        produce_v9_fixture += write_compact_array_len(1)
        produce_v9_fixture += write_compact_string("idempotent-self-test")
        produce_v9_fixture += write_compact_array_len(1)
        produce_v9_fixture += struct.pack(">ihqqq", 0, 47, -1, -1, -1)
        produce_v9_fixture += write_compact_array_len(0)
        produce_v9_fixture += write_compact_string(None)
        produce_v9_fixture += b"\x00"  # partition tagged fields
        produce_v9_fixture += b"\x00"  # topic tagged fields
        produce_v9_fixture += struct.pack(">i", 0)
        produce_v9_fixture += b"\x00"  # response tagged fields
        produce_v9 = parse_produce_v9_response(
            produce_v9_fixture,
            50,
            "idempotent-self-test",
        )
        if produce_v9["error_code"] != 47 or produce_v9["base_offset"] != -1:
            raise TestError(f"Produce v9 fixture parser failed: {produce_v9}")

        automq_nodes_fixture = struct.pack(">i", 51)
        automq_nodes_fixture += b"\x00"  # response header tagged fields
        automq_nodes_fixture += struct.pack(">hi", 0, 0)
        automq_nodes_fixture += write_compact_array_len(1)
        automq_nodes_fixture += struct.pack(">iq", 7, 42)
        automq_nodes_fixture += write_compact_string("wal://fixture-node")
        automq_nodes_fixture += write_compact_string("ACTIVE")
        automq_nodes_fixture += b"\x01"
        automq_nodes_fixture += write_automq_node_tags(
            [("rack", "az-a"), ("role", "broker")]
        )
        automq_nodes_fixture += b"\x00"  # node tagged fields
        automq_nodes_fixture += b"\x00"  # response tagged fields
        automq_nodes = parse_automq_get_nodes_response(automq_nodes_fixture, 51)
        if automq_nodes != [
            {
                "node_id": 7,
                "node_epoch": 42,
                "wal_config": "wal://fixture-node",
                "state": "ACTIVE",
                "has_opening_streams": True,
                "tags": [("rack", "az-a"), ("role", "broker")],
            }
        ]:
            raise TestError(f"AutomqGetNodes tag fixture parser failed: {automq_nodes}")

        describe_stream_id = 1234
        describe_stream_tags = [("purpose", "self-test"), ("phase", "parser")]
        describe_stream_fixture = struct.pack(">i", 52)
        describe_stream_fixture += b"\x00"  # response header tagged fields
        describe_stream_fixture += struct.pack(">hi", 0, 0)
        describe_stream_fixture += write_compact_array_len(1)
        describe_stream_fixture += struct.pack(">qi", describe_stream_id, 7)
        describe_stream_fixture += write_compact_string("OPENED")
        describe_stream_fixture += b"\x00" * 16
        describe_stream_fixture += write_compact_string(None)
        describe_stream_fixture += struct.pack(">iqqq", -1, 2, 5, 20)
        describe_stream_fixture += write_automq_stream_tags(describe_stream_tags)
        describe_stream_fixture += b"\x00"  # stream metadata tagged fields
        describe_stream_fixture += b"\x00"  # response tagged fields
        described_stream = parse_automq_describe_stream_response(
            describe_stream_fixture,
            52,
            describe_stream_id,
        )
        if described_stream["tags"] != describe_stream_tags:
            raise TestError(
                f"DescribeStreams tag fixture parser failed: {described_stream}"
            )

        print("ok: KRaft failover harness self-test")
        return 0
    finally:
        os.environ.clear()
        os.environ.update(old_env)


if __name__ == "__main__":
    try:
        if "--self-test" in sys.argv:
            sys.exit(self_test())
        sys.exit(main())
    except TestError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)

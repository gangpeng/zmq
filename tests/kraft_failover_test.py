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
ERROR_NONE = 0
ERROR_UNSUPPORTED_VERSION = 35
ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3
ERROR_TOPIC_ALREADY_EXISTS = 36
ERROR_GROUP_ID_NOT_FOUND = 69
ERROR_SNAPSHOT_NOT_FOUND = 98
ERROR_RESOURCE_NOT_FOUND = 91
ERROR_INVALID_UPDATE_VERSION = 95
ERROR_BROKER_ID_NOT_REGISTERED = 102
ERROR_UNKNOWN_CONTROLLER_ID = 116
ERROR_INVALID_REGISTRATION = 119
ERROR_UNKNOWN_MEMBER_ID = 25
ERROR_INVALID_REQUEST = 42
ERROR_FENCED_MEMBER_EPOCH = 110
ERROR_UNSUPPORTED_ASSIGNOR = 112
ACL_RESOURCE_TYPE_ANY = 1
ACL_RESOURCE_TYPE_TOPIC = 2
ACL_PATTERN_TYPE_MATCH = 2
ACL_PATTERN_TYPE_LITERAL = 3
ACL_OPERATION_ALL = 2
ACL_OPERATION_DESCRIBE = 8
ACL_PERMISSION_ALLOW = 3
CONTROLLER_API_VERSIONS = {
    18: (0, 4),
    52: (0, 1),
    53: (0, 1),
    54: (0, 1),
    55: (0, 2),
    59: (0, 1),
    62: (0, 2),
    63: (0, 1),
    64: (0, 0),
    67: (0, 0),
    70: (0, 0),
    80: (0, 0),
    81: (0, 0),
    82: (0, 0),
}


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


WYHASH_SECRET = (
    0xA0761D6478BD642F,
    0xE7037ED1A0B428DB,
    0x8EBC6AF09C88C6E3,
    0x589965CC75374CC3,
)
UINT64_MASK = (1 << 64) - 1


def _wy_read(data, offset, size):
    return int.from_bytes(data[offset : offset + size], "little")


def _wy_mix(a, b):
    product = (a & UINT64_MASK) * (b & UINT64_MASK)
    return ((product & UINT64_MASK) ^ (product >> 64)) & UINT64_MASK


def wyhash_hash(seed, data):
    seed &= UINT64_MASK
    state = [seed ^ _wy_mix(seed ^ WYHASH_SECRET[0], WYHASH_SECRET[1])] * 3
    length = len(data)

    if length <= 16:
        if length >= 4:
            end = length - 4
            quarter = (length >> 3) << 2
            a = (_wy_read(data, 0, 4) << 32) | _wy_read(data, quarter, 4)
            b = (_wy_read(data, end, 4) << 32) | _wy_read(data, end - quarter, 4)
        elif length > 0:
            a = (data[0] << 16) | (data[length >> 1] << 8) | data[length - 1]
            b = 0
        else:
            a = 0
            b = 0
    else:
        offset = 0
        if length >= 48:
            while offset + 48 < length:
                for idx in range(3):
                    a_part = _wy_read(data, offset + 8 * (2 * idx), 8)
                    b_part = _wy_read(data, offset + 8 * (2 * idx + 1), 8)
                    state[idx] = _wy_mix(
                        a_part ^ WYHASH_SECRET[idx + 1],
                        b_part ^ state[idx],
                    )
                offset += 48
            state[0] = (state[0] ^ state[1] ^ state[2]) & UINT64_MASK

        tail_data = data[offset:]
        tail_offset = 0
        while tail_offset + 16 < len(tail_data):
            state[0] = _wy_mix(
                _wy_read(tail_data, tail_offset, 8) ^ WYHASH_SECRET[1],
                _wy_read(tail_data, tail_offset + 8, 8) ^ state[0],
            )
            tail_offset += 16
        a = _wy_read(data, length - 16, 8)
        b = _wy_read(data, length - 8, 8)

    a ^= WYHASH_SECRET[1]
    b ^= state[0]
    product = (a & UINT64_MASK) * (b & UINT64_MASK)
    a = product & UINT64_MASK
    b = (product >> 64) & UINT64_MASK
    return _wy_mix(a ^ WYHASH_SECRET[0] ^ length, b ^ WYHASH_SECRET[1])


def derive_replica_directory_id(path):
    raw = path.encode("utf-8")
    first = wyhash_hash(0x5A6D715F6C6F6731, raw)
    second = wyhash_hash(0x5A6D715F6C6F6732, raw)
    directory_id = bytearray(first.to_bytes(8, "big") + second.to_bytes(8, "big"))
    directory_id[6] = (directory_id[6] & 0x0F) | 0x40
    directory_id[8] = (directory_id[8] & 0x3F) | 0x80
    if all(byte == 0 for byte in directory_id):
        directory_id[15] = 1
    return bytes(directory_id)


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


def read_u16(buf, pos):
    if pos + 2 > len(buf):
        raise TestError("buffer underflow while reading u16")
    return struct.unpack_from(">H", buf, pos)[0], pos + 2


def read_i8(buf, pos):
    if pos + 1 > len(buf):
        raise TestError("buffer underflow while reading i8")
    return struct.unpack_from(">b", buf, pos)[0], pos + 1


def read_i32(buf, pos):
    if pos + 4 > len(buf):
        raise TestError("buffer underflow while reading i32")
    return struct.unpack_from(">i", buf, pos)[0], pos + 4


def read_i64(buf, pos):
    if pos + 8 > len(buf):
        raise TestError("buffer underflow while reading i64")
    return struct.unpack_from(">q", buf, pos)[0], pos + 8


def read_f64(buf, pos):
    if pos + 8 > len(buf):
        raise TestError("buffer underflow while reading f64")
    return struct.unpack_from(">d", buf, pos)[0], pos + 8


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


def read_uuid(buf, pos):
    if pos + 16 > len(buf):
        raise TestError("buffer underflow while reading uuid")
    return buf[pos : pos + 16], pos + 16


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


def parse_delete_topics_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    response_count, pos = read_compact_array_len(response, pos)
    responses = []
    for _ in range(response_count):
        name, pos = read_compact_string(response, pos)
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading DeleteTopics topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        responses.append(
            {
                "name": name,
                "topic_id": topic_id,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"DeleteTopics response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "responses": responses}


def delete_topic(port, topic, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += b"\x00" * 16  # topic_id omitted; delete by name
    body += b"\x00"  # topic tagged fields
    body += struct.pack(">i", 30000)  # timeout_ms
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 20, 6, correlation_id, body)
    return parse_delete_topics_response(response, correlation_id)


def require_delete_topics_result(response, topic, expected_error_codes, label):
    if isinstance(expected_error_codes, int):
        expected_error_codes = (expected_error_codes,)
    if response["throttle_time_ms"] != 0:
        raise TestError(f"{label} throttle mismatch: {response}")
    responses = response["responses"]
    if len(responses) != 1:
        raise TestError(f"{label} response count mismatch: {response}")
    item = responses[0]
    if (
        item["name"] != topic
        or item["error_code"] not in expected_error_codes
        or item["error_message"] is not None
    ):
        raise TestError(f"{label} result mismatch: {response}")


def parse_create_topics_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        name, pos = read_compact_string(response, pos)
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading CreateTopics topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        num_partitions, pos = read_i32(response, pos)
        replication_factor, pos = read_i16(response, pos)
        config_count, pos = read_compact_array_len(response, pos)
        configs = []
        for _ in range(config_count):
            config_name, pos = read_compact_string(response, pos)
            config_value, pos = read_compact_string(response, pos)
            read_only, pos = read_bool(response, pos)
            config_source, pos = read_i8(response, pos)
            is_sensitive, pos = read_bool(response, pos)
            pos = skip_tags(response, pos)
            configs.append(
                {
                    "name": config_name,
                    "value": config_value,
                    "read_only": read_only,
                    "config_source": config_source,
                    "is_sensitive": is_sensitive,
                }
            )
        pos = skip_tags(response, pos)
        topics.append(
            {
                "name": name,
                "topic_id": topic_id,
                "error_code": error_code,
                "error_message": error_message,
                "num_partitions": num_partitions,
                "replication_factor": replication_factor,
                "configs": configs,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"CreateTopics response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "topics": topics}


def create_topic_with_configs(
    port,
    topic,
    partition_count,
    replication_factor,
    configs,
    validate_only,
    correlation_id,
):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += struct.pack(">ih", partition_count, replication_factor)
    body += write_compact_array_len(0)  # assignments
    body += write_compact_array_len(len(configs))
    for name, value in configs:
        body += write_compact_string(name)
        body += write_compact_string(value)
        body += b"\x00"  # config tagged fields
    body += b"\x00"  # topic tagged fields
    body += struct.pack(">i", 30000)  # timeout_ms
    body += b"\x01" if validate_only else b"\x00"
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 19, 7, correlation_id, body)
    return parse_create_topics_response(response, correlation_id)


def require_create_topics_result(
    response,
    topic,
    partition_count,
    replication_factor,
    expected_error_codes,
    label,
):
    if isinstance(expected_error_codes, int):
        expected_error_codes = (expected_error_codes,)
    if response["throttle_time_ms"] != 0:
        raise TestError(f"{label} throttle mismatch: {response}")
    topics = response["topics"]
    if len(topics) != 1:
        raise TestError(f"{label} topic count mismatch: {response}")
    item = topics[0]
    if (
        item["name"] != topic
        or item["error_code"] not in expected_error_codes
        or item["num_partitions"] != partition_count
        or item["replication_factor"] != replication_factor
    ):
        raise TestError(f"{label} result mismatch: {response}")
    if item["error_code"] == ERROR_NONE and item["error_message"] is not None:
        raise TestError(f"{label} unexpected error message: {response}")


def parse_allocate_producer_ids_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    producer_id_start, pos = read_i64(response, pos)
    producer_id_len, pos = read_i32(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"AllocateProducerIds response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "producer_id_start": producer_id_start,
        "producer_id_len": producer_id_len,
    }


def allocate_producer_ids(port, broker_id, broker_epoch, correlation_id):
    body = struct.pack(">iq", broker_id, broker_epoch)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 67, 0, correlation_id, body)
    return parse_allocate_producer_ids_response(response, correlation_id)


def wait_for_allocate_producer_ids_checkpoint(
    port,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9040)
    last_error = None
    while time.time() < deadline:
        try:
            allocated = allocate_producer_ids(port, 100, -1, correlation_id)
            if (
                allocated["throttle_time_ms"] != 0
                or allocated["error_code"] != ERROR_NONE
                or allocated["producer_id_start"] < 0
                or allocated["producer_id_len"] <= 0
            ):
                raise TestError(f"AllocateProducerIds invalid response: {allocated}")
            previous_next = state.get("next_producer_id")
            if (
                previous_next is not None
                and allocated["producer_id_start"] < previous_next
            ):
                raise TestError(
                    f"AllocateProducerIds reused PID range during {label}: "
                    f"previous_next={previous_next} response={allocated}"
                )
            state["next_producer_id"] = (
                allocated["producer_id_start"] + allocated["producer_id_len"]
            )
            state["correlation_id"] = correlation_id + 1
            return allocated
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"AllocateProducerIds did not recover during {label}: {last_error}")


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


def expected_topic_end_offset(first_offset, payloads):
    return first_offset + len(payloads)


def parse_list_offsets_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            error_code, pos = read_i16(response, pos)
            timestamp, pos = read_i64(response, pos)
            offset, pos = read_i64(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": error_code,
                    "timestamp": timestamp,
                    "offset": offset,
                    "leader_epoch": leader_epoch,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"ListOffsets response trailing bytes: {len(response) - pos}")
    return topics


def list_offsets(port, topic, timestamp, correlation_id):
    body = struct.pack(">i", -1)  # replica_id
    body += struct.pack(">b", 0)  # isolation_level=read_uncommitted
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack(">iiq", 0, -1, timestamp)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 2, 6, correlation_id, body)
    return parse_list_offsets_response(response, correlation_id)


def parse_offset_for_leader_epoch_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            error_code, pos = read_i16(response, pos)
            partition, pos = read_i32(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            end_offset, pos = read_i64(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition": partition,
                    "error_code": error_code,
                    "leader_epoch": leader_epoch,
                    "end_offset": end_offset,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"OffsetForLeaderEpoch response trailing bytes: {len(response) - pos}"
        )
    return topics


def offset_for_leader_epoch(port, topic, correlation_id):
    body = struct.pack(">i", -1)  # replica_id
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack(">iii", 0, -1, 0)  # partition, current epoch, lookup epoch
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 23, 4, correlation_id, body)
    return parse_offset_for_leader_epoch_response(response, correlation_id)


def require_single_list_offsets_partition(topics, topic):
    if len(topics) != 1 or topics[0]["name"] != topic:
        raise TestError(f"ListOffsets topic mismatch: {topics}")
    partitions = topics[0]["partitions"]
    if len(partitions) != 1:
        raise TestError(f"ListOffsets partition count mismatch: {topics}")
    partition = partitions[0]
    if partition["partition_index"] != 0 or partition["error_code"] != 0:
        raise TestError(f"ListOffsets partition error: {topics}")
    return partition


def require_single_offset_for_leader_epoch_partition(topics, topic):
    if len(topics) != 1 or topics[0]["name"] != topic:
        raise TestError(f"OffsetForLeaderEpoch topic mismatch: {topics}")
    partitions = topics[0]["partitions"]
    if len(partitions) != 1:
        raise TestError(f"OffsetForLeaderEpoch partition count mismatch: {topics}")
    partition = partitions[0]
    if partition["partition"] != 0 or partition["error_code"] != 0:
        raise TestError(f"OffsetForLeaderEpoch partition error: {topics}")
    return partition


def wait_for_log_position_checkpoint(
    port, topic, expected_start_offset, expected_end_offset, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8200
    last_error = None
    while time.time() < deadline:
        try:
            earliest = require_single_list_offsets_partition(
                list_offsets(port, topic, -2, correlation_id),
                topic,
            )
            latest = require_single_list_offsets_partition(
                list_offsets(port, topic, -1, correlation_id + 1),
                topic,
            )
            epoch = require_single_offset_for_leader_epoch_partition(
                offset_for_leader_epoch(port, topic, correlation_id + 2),
                topic,
            )
            if earliest["offset"] != expected_start_offset:
                raise TestError(
                    f"ListOffsets earliest={earliest} expected={expected_start_offset}"
                )
            if latest["offset"] != expected_end_offset:
                raise TestError(
                    f"ListOffsets latest={latest} expected={expected_end_offset}"
                )
            if epoch["end_offset"] != expected_end_offset:
                raise TestError(
                    f"OffsetForLeaderEpoch end={epoch} expected={expected_end_offset}"
                )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 3
        time.sleep(0.25)
    raise TestError(
        f"log position APIs did not recover for {topic!r}: {last_error}"
    )


def parse_delete_records_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            low_watermark, pos = read_i64(response, pos)
            error_code, pos = read_i16(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "low_watermark": low_watermark,
                    "error_code": error_code,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"DeleteRecords response trailing bytes: {len(response) - pos}")
    return topics


def delete_records(port, topic, offset, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_array_len(1)
    body += struct.pack(">iq", 0, offset)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += struct.pack(">i", 30000)  # timeout_ms
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 21, 2, correlation_id, body)
    return parse_delete_records_response(response, correlation_id)


def require_single_delete_records_partition(topics, topic):
    if len(topics) != 1 or topics[0]["name"] != topic:
        raise TestError(f"DeleteRecords topic mismatch: {topics}")
    partitions = topics[0]["partitions"]
    if len(partitions) != 1:
        raise TestError(f"DeleteRecords partition count mismatch: {topics}")
    partition = partitions[0]
    if partition["partition_index"] != 0 or partition["error_code"] != 0:
        raise TestError(f"DeleteRecords partition error: {topics}")
    return partition


def wait_for_delete_records_checkpoint(
    port, topic, expected_low_watermark, expected_end_offset, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8300
    last_error = None
    while time.time() < deadline:
        try:
            partition = require_single_delete_records_partition(
                delete_records(port, topic, expected_low_watermark, correlation_id),
                topic,
            )
            if partition["low_watermark"] != expected_low_watermark:
                raise TestError(
                    f"DeleteRecords low watermark={partition} "
                    f"expected={expected_low_watermark}"
                )
            wait_for_log_position_checkpoint(
                port,
                topic,
                expected_low_watermark,
                expected_end_offset,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"DeleteRecords did not recover for {topic!r}: {last_error}"
    )


def parse_create_partitions_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        topic_name, pos = read_compact_string(response, pos)
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        results.append(
            {
                "name": topic_name,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"CreatePartitions response trailing bytes: {len(response) - pos}"
        )
    return {"throttle_time_ms": throttle_time_ms, "results": results}


def create_partitions(port, topic, count, validate_only, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += struct.pack(">i", count)
    body += b"\x00"  # null assignments
    body += b"\x00"  # topic tagged fields
    body += struct.pack(">i", 30000)
    body += b"\x01" if validate_only else b"\x00"
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 37, 2, correlation_id, body)
    return parse_create_partitions_response(response, correlation_id)


def require_create_partitions_success(response, topic):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"CreatePartitions throttle mismatch: {response}")
    results = response["results"]
    if len(results) != 1 or results[0]["name"] != topic:
        raise TestError(f"CreatePartitions topic mismatch: {response}")
    result = results[0]
    if result["error_code"] != 0 or result["error_message"] is not None:
        raise TestError(f"CreatePartitions result mismatch: {response}")


def wait_for_create_partitions_mutation(port, topic, count, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8330
    last_error = None
    while time.time() < deadline:
        try:
            response = create_partitions(port, topic, count, False, correlation_id)
            require_create_partitions_success(response, topic)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"CreatePartitions did not expand {topic!r}: {last_error}")


def wait_for_create_partitions_validate_only_checkpoint(
    port, topic, count, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8360
    last_error = None
    while time.time() < deadline:
        try:
            response = create_partitions(port, topic, count, True, correlation_id)
            require_create_partitions_success(response, topic)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"CreatePartitions validate-only did not recover for {topic!r}: {last_error}"
    )


def read_tagged_fields(buf, pos):
    count, pos = read_varint(buf, pos)
    fields = []
    seen = set()
    for _ in range(count):
        tag, pos = read_varint(buf, pos)
        size, pos = read_varint(buf, pos)
        if tag in seen:
            raise TestError(f"duplicate tagged field {tag}")
        seen.add(tag)
        if pos + size > len(buf):
            raise TestError("buffer underflow while reading tagged field")
        fields.append((tag, buf[pos : pos + size]))
        pos += size
    return fields, pos


def parse_update_features_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        feature, pos = read_compact_string(response, pos)
        result_error_code, pos = read_i16(response, pos)
        result_error_message, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        results.append(
            {
                "feature": feature,
                "error_code": result_error_code,
                "error_message": result_error_message,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"UpdateFeatures response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
        "results": results,
    }


def update_features(port, feature, max_version_level, validate_only, correlation_id):
    body = struct.pack(">i", 30000)
    body += write_compact_array_len(1)
    body += write_compact_string(feature)
    body += struct.pack(">h", max_version_level)
    body += struct.pack(">b", 1)  # upgrade_type=upgrade-only
    body += b"\x00"  # feature tagged fields
    body += b"\x01" if validate_only else b"\x00"
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 57, 1, correlation_id, body)
    return parse_update_features_response(response, correlation_id)


def require_update_features_success(response, feature):
    if (
        response["throttle_time_ms"] != 0
        or response["error_code"] != 0
        or response["error_message"] is not None
    ):
        raise TestError(f"UpdateFeatures top-level mismatch: {response}")
    results = response["results"]
    if len(results) != 1 or results[0]["feature"] != feature:
        raise TestError(f"UpdateFeatures feature mismatch: {response}")
    if results[0]["error_code"] != 0 or results[0]["error_message"] is not None:
        raise TestError(f"UpdateFeatures result mismatch: {response}")


def parse_api_versions_features_response(response, correlation_id):
    pos = 0
    response_correlation_id, pos = read_i32(response, pos)
    if response_correlation_id != correlation_id:
        raise TestError(
            f"ApiVersions correlation mismatch: {response_correlation_id}"
        )
    error_code, pos = read_i16(response, pos)
    api_count, pos = read_compact_array_len(response, pos)
    apis = []
    for _ in range(api_count):
        api_key, pos = read_i16(response, pos)
        min_version, pos = read_i16(response, pos)
        max_version, pos = read_i16(response, pos)
        pos = skip_tags(response, pos)
        apis.append(
            {
                "api_key": api_key,
                "min_version": min_version,
                "max_version": max_version,
            }
        )
    throttle_time_ms, pos = read_i32(response, pos)

    supported_features = []
    finalized_features_epoch = -1
    finalized_features = []
    zk_migration_ready = False
    fields, pos = read_tagged_fields(response, pos)
    for tag, data in fields:
        tag_pos = 0
        if tag == 0:
            feature_count, tag_pos = read_compact_array_len(data, tag_pos)
            for _ in range(feature_count):
                name, tag_pos = read_compact_string(data, tag_pos)
                min_version, tag_pos = read_i16(data, tag_pos)
                max_version, tag_pos = read_i16(data, tag_pos)
                tag_pos = skip_tags(data, tag_pos)
                supported_features.append(
                    {
                        "name": name,
                        "min_version": min_version,
                        "max_version": max_version,
                    }
                )
        elif tag == 1:
            finalized_features_epoch, tag_pos = read_i64(data, tag_pos)
        elif tag == 2:
            feature_count, tag_pos = read_compact_array_len(data, tag_pos)
            for _ in range(feature_count):
                name, tag_pos = read_compact_string(data, tag_pos)
                max_version_level, tag_pos = read_i16(data, tag_pos)
                min_version_level, tag_pos = read_i16(data, tag_pos)
                tag_pos = skip_tags(data, tag_pos)
                finalized_features.append(
                    {
                        "name": name,
                        "max_version_level": max_version_level,
                        "min_version_level": min_version_level,
                    }
                )
        elif tag == 3:
            zk_migration_ready, tag_pos = read_bool(data, tag_pos)
        else:
            tag_pos = len(data)
        if tag_pos != len(data):
            raise TestError(
                f"ApiVersions tagged field {tag} trailing bytes: {len(data) - tag_pos}"
            )

    if pos != len(response):
        raise TestError(f"ApiVersions response trailing bytes: {len(response) - pos}")
    return {
        "error_code": error_code,
        "apis": apis,
        "throttle_time_ms": throttle_time_ms,
        "supported_features": supported_features,
        "finalized_features_epoch": finalized_features_epoch,
        "finalized_features": finalized_features,
        "zk_migration_ready": zk_migration_ready,
    }


def api_versions_v3(port, correlation_id):
    body = write_compact_string("kraft-failover-test")
    body += write_compact_string("1.0")
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 18, 3, correlation_id, body)
    return parse_api_versions_features_response(response, correlation_id)


def require_controller_api_versions(response):
    if response["throttle_time_ms"] != 0 or response["error_code"] != ERROR_NONE:
        raise TestError(f"Controller ApiVersions top-level mismatch: {response}")
    actual = {
        item["api_key"]: (item["min_version"], item["max_version"])
        for item in response["apis"]
    }
    if actual != CONTROLLER_API_VERSIONS:
        raise TestError(
            f"Controller ApiVersions mismatch: expected={CONTROLLER_API_VERSIONS} "
            f"actual={actual}"
        )
    for unexpected in (71, 72):
        if unexpected in actual:
            raise TestError(
                f"Controller ApiVersions advertised telemetry key {unexpected}"
            )


def wait_for_controller_api_versions_checkpoint(
    port,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9340)
    last_error = None
    while time.time() < deadline:
        try:
            response = api_versions_v3(port, correlation_id)
            require_controller_api_versions(response)
            state["correlation_id"] = correlation_id + 1
            return response
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"Controller ApiVersions did not recover during {label}: {last_error}"
    )


def parse_controller_small_error_response(response, correlation_id, response_name):
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    if pos != len(response):
        raise TestError(
            f"{response_name} response trailing bytes: {len(response) - pos}"
        )
    return {"error_code": error_code}


def controller_small_error_request(port, api_key, api_version, correlation_id):
    response = flexible_kafka_request(port, api_key, api_version, correlation_id)
    return parse_controller_small_error_response(
        response,
        correlation_id,
        f"controller api_key={api_key} v={api_version}",
    )


def require_controller_unsupported_response(response, api_key, api_version, label):
    if response["error_code"] != ERROR_UNSUPPORTED_VERSION:
        raise TestError(
            f"controller unsupported response mismatch during {label}: "
            f"api_key={api_key} api_version={api_version} response={response}"
        )


def wait_for_controller_unsupported_checkpoint(
    port,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9740)
    unsupported_cases = [
        (api_key, versions[1] + 1)
        for api_key, versions in sorted(CONTROLLER_API_VERSIONS.items())
        if api_key != 18
    ]
    unsupported_cases.extend([(71, 0), (72, 0)])
    last_error = None
    while time.time() < deadline:
        try:
            for index, (api_key, api_version) in enumerate(unsupported_cases):
                response = controller_small_error_request(
                    port,
                    api_key,
                    api_version,
                    correlation_id + index,
                )
                require_controller_unsupported_response(
                    response,
                    api_key,
                    api_version,
                    label,
                )
            state["correlation_id"] = correlation_id + len(unsupported_cases)
            return {"cases": unsupported_cases}
        except Exception as exc:
            last_error = exc
        correlation_id += len(unsupported_cases)
        time.sleep(0.25)
    raise TestError(
        f"controller unsupported API probes did not recover during {label}: "
        f"{last_error}"
    )


def raft_voter_directory_id(voter_id):
    if voter_id < 0:
        raise TestError(f"invalid negative voter_id={voter_id}")
    return voter_id.to_bytes(16, "big", signed=False)


def write_raft_voter_listener(name, host, port):
    if port <= 0 or port > 65535:
        raise TestError(f"invalid raft voter listener port={port}")
    body = bytearray()
    body += write_compact_string(name)
    body += write_compact_string(host)
    body += struct.pack(">H", port)
    body += b"\x00"  # listener tagged fields
    return bytes(body)


def parse_raft_voter_response(
    response,
    correlation_id,
    response_name,
    has_error_message=True,
):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message = None
    if has_error_message:
        error_message, pos = read_compact_string(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"{response_name} response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
    }


def require_raft_voter_error(response, expected_error_code, response_name, label):
    if response["throttle_time_ms"] != 0:
        raise TestError(
            f"{response_name} throttle mismatch during {label}: {response}"
        )
    if response["error_code"] != expected_error_code:
        raise TestError(
            f"{response_name} error mismatch during {label}: "
            f"expected={expected_error_code} response={response}"
        )


def add_raft_voter_empty_listeners(port, voter_id, correlation_id):
    body = bytearray()
    body += write_compact_string(CLUSTER_ID)
    body += struct.pack(">ii", 1000, voter_id)
    body += raft_voter_directory_id(voter_id)
    body += write_compact_array_len(0)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 80, 0, correlation_id, bytes(body))
    return parse_raft_voter_response(response, correlation_id, "AddRaftVoter")


def remove_raft_voter_unknown(port, voter_id, correlation_id):
    body = bytearray()
    body += write_compact_string(CLUSTER_ID)
    body += struct.pack(">i", voter_id)
    body += raft_voter_directory_id(voter_id)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 81, 0, correlation_id, bytes(body))
    return parse_raft_voter_response(response, correlation_id, "RemoveRaftVoter")


def update_raft_voter(
    port,
    voter_id,
    listener_port,
    min_supported_version,
    max_supported_version,
    correlation_id,
):
    body = bytearray()
    body += write_compact_string(CLUSTER_ID)
    body += struct.pack(">i", voter_id)
    body += raft_voter_directory_id(voter_id)
    body += write_compact_array_len(1)
    body += write_raft_voter_listener("CONTROLLER", "127.0.0.1", listener_port)
    body += struct.pack(">hh", min_supported_version, max_supported_version)
    body += b"\x00"  # KRaftVersionFeature tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 82, 0, correlation_id, bytes(body))
    return parse_raft_voter_response(
        response,
        correlation_id,
        "UpdateRaftVoter",
        has_error_message=False,
    )


def require_dynamic_voter_quorum_unchanged(quorum, expected_ports, label):
    expected_node_ids = sorted(expected_ports)
    if quorum["partition_error_code"] != ERROR_NONE:
        raise TestError(
            f"DescribeQuorum after dynamic voter probes failed during {label}: {quorum}"
        )
    if sorted(quorum["voters"]) != expected_node_ids:
        raise TestError(
            f"dynamic voter probes changed voter set during {label}: "
            f"expected={expected_node_ids} quorum={quorum}"
        )
    node_ports = {}
    for node in quorum["nodes"]:
        controller_listeners = [
            listener
            for listener in node["listeners"]
            if listener["name"] == "CONTROLLER"
        ]
        if len(controller_listeners) != 1:
            raise TestError(
                f"dynamic voter probes changed node listeners during {label}: "
                f"{node}"
            )
        node_ports[node["node_id"]] = controller_listeners[0]["port"]
    if node_ports != expected_ports:
        raise TestError(
            f"dynamic voter probes changed endpoints during {label}: "
            f"expected={expected_ports} actual={node_ports}"
        )


def wait_for_dynamic_raft_voter_negative_checkpoint(
    port,
    expected_ports,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9440)
    existing_voter_id = sorted(expected_ports)[0]
    unknown_voter_id = max(expected_ports) + 50000
    last_error = None
    while time.time() < deadline:
        try:
            add_response = add_raft_voter_empty_listeners(
                port,
                unknown_voter_id,
                correlation_id,
            )
            require_raft_voter_error(
                add_response,
                ERROR_INVALID_REQUEST,
                "AddRaftVoter",
                label,
            )

            remove_response = remove_raft_voter_unknown(
                port,
                unknown_voter_id,
                correlation_id + 1,
            )
            require_raft_voter_error(
                remove_response,
                ERROR_RESOURCE_NOT_FOUND,
                "RemoveRaftVoter",
                label,
            )

            update_unknown = update_raft_voter(
                port,
                unknown_voter_id,
                65535,
                0,
                0,
                correlation_id + 2,
            )
            require_raft_voter_error(
                update_unknown,
                ERROR_RESOURCE_NOT_FOUND,
                "UpdateRaftVoter unknown voter",
                label,
            )

            update_invalid_feature = update_raft_voter(
                port,
                existing_voter_id,
                expected_ports[existing_voter_id],
                2,
                1,
                correlation_id + 3,
            )
            require_raft_voter_error(
                update_invalid_feature,
                ERROR_INVALID_UPDATE_VERSION,
                "UpdateRaftVoter invalid feature",
                label,
            )

            quorum = describe_quorum_v2(port, correlation_id + 4)
            require_dynamic_voter_quorum_unchanged(quorum, expected_ports, label)
            state["correlation_id"] = correlation_id + 5
            return {
                "add_empty_listeners": add_response,
                "remove_unknown": remove_response,
                "update_unknown": update_unknown,
                "update_invalid_feature": update_invalid_feature,
                "quorum": quorum,
            }
        except Exception as exc:
            last_error = exc
        correlation_id += 5
        time.sleep(0.25)
    raise TestError(
        f"dynamic Raft voter negative probes did not recover during {label}: "
        f"{last_error}"
    )


def parse_broker_heartbeat_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    is_caught_up, pos = read_bool(response, pos)
    is_fenced, pos = read_bool(response, pos)
    should_shut_down, pos = read_bool(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"BrokerHeartbeat response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "is_caught_up": is_caught_up,
        "is_fenced": is_fenced,
        "should_shut_down": should_shut_down,
    }


def parse_unregister_broker_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"UnregisterBroker response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
    }


def broker_heartbeat_unknown(port, broker_id, correlation_id):
    offline_log_dirs_tag = write_compact_array_len(1)
    offline_log_dirs_tag += raft_voter_directory_id(broker_id)

    body = bytearray()
    body += struct.pack(">iqq", broker_id, -1, 0)
    body += b"\x00"  # want_fence=false
    body += b"\x00"  # want_shut_down=false
    body += write_varint(1)  # tagged field count
    body += write_varint(0)  # offline_log_dirs tag
    body += write_varint(len(offline_log_dirs_tag))
    body += offline_log_dirs_tag
    response = flexible_kafka_request(port, 63, 1, correlation_id, bytes(body))
    return parse_broker_heartbeat_response(response, correlation_id)


def unregister_broker_unknown(port, broker_id, correlation_id):
    body = struct.pack(">i", broker_id)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 64, 0, correlation_id, body)
    return parse_unregister_broker_response(response, correlation_id)


def require_broker_lifecycle_negative_response(response, response_name, label):
    if response["throttle_time_ms"] != 0:
        raise TestError(
            f"{response_name} throttle mismatch during {label}: {response}"
        )
    if response["error_code"] != ERROR_BROKER_ID_NOT_REGISTERED:
        raise TestError(
            f"{response_name} error mismatch during {label}: {response}"
        )


def wait_for_broker_lifecycle_negative_checkpoint(
    port,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9540)
    broker_id = state.get("broker_id", 60100)
    last_error = None
    while time.time() < deadline:
        try:
            heartbeat = broker_heartbeat_unknown(port, broker_id, correlation_id)
            require_broker_lifecycle_negative_response(
                heartbeat,
                "BrokerHeartbeat",
                label,
            )
            if (
                heartbeat["is_caught_up"]
                or not heartbeat["is_fenced"]
                or heartbeat["should_shut_down"]
            ):
                raise TestError(
                    f"BrokerHeartbeat unknown broker state mismatch during {label}: "
                    f"{heartbeat}"
                )

            unregister = unregister_broker_unknown(
                port,
                broker_id,
                correlation_id + 1,
            )
            require_broker_lifecycle_negative_response(
                unregister,
                "UnregisterBroker",
                label,
            )

            state["correlation_id"] = correlation_id + 2
            state["broker_id"] = broker_id
            return {
                "heartbeat": heartbeat,
                "unregister": unregister,
            }
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(
        f"broker lifecycle negative probes did not recover during {label}: "
        f"{last_error}"
    )


def write_controller_registration_listener(name, host, port, security_protocol=0):
    if port < 0 or port > 65535:
        raise TestError(f"invalid controller listener port={port}")
    body = bytearray()
    body += write_compact_string(name)
    body += write_compact_string(host)
    body += struct.pack(">Hh", port, security_protocol)
    body += b"\x00"  # listener tagged fields
    return bytes(body)


def write_controller_registration_feature(name, min_version, max_version):
    body = bytearray()
    body += write_compact_string(name)
    body += struct.pack(">hh", min_version, max_version)
    body += b"\x00"  # feature tagged fields
    return bytes(body)


def parse_controller_registration_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ControllerRegistration response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
    }


def controller_registration(
    port,
    controller_id,
    listener_host,
    listener_port,
    features,
    correlation_id,
):
    body = bytearray()
    body += struct.pack(">i", controller_id)
    body += raft_voter_directory_id(controller_id + 1)
    body += b"\x00"  # zk_migration_ready=false
    body += write_compact_array_len(1)
    body += write_controller_registration_listener(
        "CONTROLLER",
        listener_host,
        listener_port,
    )
    body += write_compact_array_len(len(features))
    for feature in features:
        body += write_controller_registration_feature(
            feature["name"],
            feature["min_version"],
            feature["max_version"],
        )
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 70, 0, correlation_id, bytes(body))
    return parse_controller_registration_response(response, correlation_id)


def require_controller_registration_error(
    response,
    expected_error_code,
    response_name,
    label,
):
    if response["throttle_time_ms"] != 0:
        raise TestError(
            f"{response_name} throttle mismatch during {label}: {response}"
        )
    if response["error_code"] != expected_error_code:
        raise TestError(
            f"{response_name} error mismatch during {label}: "
            f"expected={expected_error_code} response={response}"
        )


def wait_for_controller_registration_negative_checkpoint(
    port,
    expected_ports,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9640)
    existing_controller_id = sorted(expected_ports)[0]
    unknown_controller_id = max(expected_ports) + 60100
    last_error = None
    while time.time() < deadline:
        try:
            unknown = controller_registration(
                port,
                unknown_controller_id,
                "127.0.0.1",
                65535,
                [],
                correlation_id,
            )
            require_controller_registration_error(
                unknown,
                ERROR_UNKNOWN_CONTROLLER_ID,
                "ControllerRegistration unknown controller",
                label,
            )

            invalid_feature = controller_registration(
                port,
                existing_controller_id,
                "127.0.0.1",
                expected_ports[existing_controller_id],
                [{"name": "kraft.version", "min_version": 2, "max_version": 1}],
                correlation_id + 1,
            )
            require_controller_registration_error(
                invalid_feature,
                ERROR_INVALID_REGISTRATION,
                "ControllerRegistration invalid feature",
                label,
            )

            invalid_listener = controller_registration(
                port,
                existing_controller_id,
                "",
                expected_ports[existing_controller_id],
                [],
                correlation_id + 2,
            )
            require_controller_registration_error(
                invalid_listener,
                ERROR_INVALID_REGISTRATION,
                "ControllerRegistration invalid listener",
                label,
            )

            quorum = describe_quorum_v2(port, correlation_id + 3)
            require_dynamic_voter_quorum_unchanged(quorum, expected_ports, label)
            state["correlation_id"] = correlation_id + 4
            return {
                "unknown": unknown,
                "invalid_feature": invalid_feature,
                "invalid_listener": invalid_listener,
                "quorum": quorum,
            }
        except Exception as exc:
            last_error = exc
        correlation_id += 4
        time.sleep(0.25)
    raise TestError(
        f"controller registration negative probes did not recover during {label}: "
        f"{last_error}"
    )


def require_finalized_feature_visible(response, feature, max_version_level):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"ApiVersions top-level mismatch: {response}")
    supported = [
        item
        for item in response["supported_features"]
        if item["name"] == feature
        and item["min_version"] <= max_version_level <= item["max_version"]
    ]
    if not supported:
        raise TestError(f"ApiVersions supported feature missing: {response}")
    if response["finalized_features_epoch"] < 0:
        raise TestError(f"ApiVersions finalized epoch missing: {response}")
    matches = [
        item for item in response["finalized_features"] if item["name"] == feature
    ]
    if len(matches) != 1:
        raise TestError(f"ApiVersions finalized feature mismatch: {response}")
    finalized = matches[0]
    if (
        finalized["max_version_level"] != max_version_level
        or finalized["min_version_level"] != max_version_level
    ):
        raise TestError(f"ApiVersions finalized level mismatch: {response}")


def wait_for_update_features_mutation(
    port, feature, max_version_level, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8780
    last_error = None
    while time.time() < deadline:
        try:
            response = update_features(
                port,
                feature,
                max_version_level,
                False,
                correlation_id,
            )
            require_update_features_success(response, feature)
            correlation_id += 1
            require_finalized_feature_visible(
                api_versions_v3(port, correlation_id),
                feature,
                max_version_level,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"UpdateFeatures did not finalize {feature!r}: {last_error}")


def wait_for_finalized_features_checkpoint(
    port, feature, max_version_level, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8810
    last_error = None
    while time.time() < deadline:
        try:
            response = update_features(
                port,
                feature,
                max_version_level,
                True,
                correlation_id,
            )
            require_update_features_success(response, feature)
            correlation_id += 1
            require_finalized_feature_visible(
                api_versions_v3(port, correlation_id),
                feature,
                max_version_level,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"Finalized feature checkpoint did not recover for {feature!r}: {last_error}"
    )


def parse_create_acls_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        results.append(
            {
                "error_code": error_code,
                "error_message": error_message,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"CreateAcls response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "results": results}


def acl_fields_body(binding):
    body = struct.pack(">b", binding["resource_type"])
    body += write_compact_string(binding["resource_name"])
    body += struct.pack(">b", binding["pattern_type"])
    body += write_compact_string(binding["principal"])
    body += write_compact_string(binding["host"])
    body += struct.pack(">bb", binding["operation"], binding["permission_type"])
    return body


def write_acl_binding(binding):
    return acl_fields_body(binding) + b"\x00"  # ACL tagged fields


def create_acls(port, bindings, correlation_id):
    body = write_compact_array_len(len(bindings))
    for binding in bindings:
        body += write_acl_binding(binding)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 30, 2, correlation_id, body)
    return parse_create_acls_response(response, correlation_id)


def require_create_acls_success(response, expected_count):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"CreateAcls throttle mismatch: {response}")
    if len(response["results"]) != expected_count:
        raise TestError(f"CreateAcls result count mismatch: {response}")
    for result in response["results"]:
        if result["error_code"] != 0 or result["error_message"] is not None:
            raise TestError(f"CreateAcls result mismatch: {response}")


def parse_describe_acls_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    resource_count, pos = read_compact_array_len(response, pos)
    resources = []
    for _ in range(resource_count):
        resource_type, pos = read_i8(response, pos)
        resource_name, pos = read_compact_string(response, pos)
        pattern_type, pos = read_i8(response, pos)
        acl_count, pos = read_compact_array_len(response, pos)
        acls = []
        for _ in range(acl_count):
            principal, pos = read_compact_string(response, pos)
            host, pos = read_compact_string(response, pos)
            operation, pos = read_i8(response, pos)
            permission_type, pos = read_i8(response, pos)
            pos = skip_tags(response, pos)
            acls.append(
                {
                    "principal": principal,
                    "host": host,
                    "operation": operation,
                    "permission_type": permission_type,
                }
            )
        pos = skip_tags(response, pos)
        resources.append(
            {
                "resource_type": resource_type,
                "resource_name": resource_name,
                "pattern_type": pattern_type,
                "acls": acls,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeAcls response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
        "resources": resources,
    }


def acl_filter_body(binding):
    return acl_fields_body(binding) + b"\x00"  # filter tagged fields


def describe_acls(port, binding_filter, correlation_id):
    body = acl_fields_body(binding_filter)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 29, 2, correlation_id, body)
    return parse_describe_acls_response(response, correlation_id)


def acl_matches(binding, resource, acl):
    return (
        resource["resource_type"] == binding["resource_type"]
        and resource["resource_name"] == binding["resource_name"]
        and resource["pattern_type"] == binding["pattern_type"]
        and acl["principal"] == binding["principal"]
        and acl["host"] == binding["host"]
        and acl["operation"] == binding["operation"]
        and acl["permission_type"] == binding["permission_type"]
    )


def require_describe_acls_success(response):
    if (
        response["throttle_time_ms"] != 0
        or response["error_code"] != 0
        or response["error_message"] is not None
    ):
        raise TestError(f"DescribeAcls top-level mismatch: {response}")


def require_acl_visible(response, binding):
    require_describe_acls_success(response)
    matches = [
        (resource, acl)
        for resource in response["resources"]
        for acl in resource["acls"]
        if acl_matches(binding, resource, acl)
    ]
    if not matches:
        raise TestError(f"DescribeAcls ACL missing: {response}")


def require_acl_absent(response, binding):
    require_describe_acls_success(response)
    matches = [
        (resource, acl)
        for resource in response["resources"]
        for acl in resource["acls"]
        if acl_matches(binding, resource, acl)
    ]
    if matches:
        raise TestError(f"DescribeAcls deleted ACL still visible: {response}")


def parse_delete_acls_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    filter_results = []
    for _ in range(result_count):
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        match_count, pos = read_compact_array_len(response, pos)
        matching_acls = []
        for _ in range(match_count):
            match_error_code, pos = read_i16(response, pos)
            match_error_message, pos = read_compact_string(response, pos)
            resource_type, pos = read_i8(response, pos)
            resource_name, pos = read_compact_string(response, pos)
            pattern_type, pos = read_i8(response, pos)
            principal, pos = read_compact_string(response, pos)
            host, pos = read_compact_string(response, pos)
            operation, pos = read_i8(response, pos)
            permission_type, pos = read_i8(response, pos)
            pos = skip_tags(response, pos)
            matching_acls.append(
                {
                    "error_code": match_error_code,
                    "error_message": match_error_message,
                    "resource_type": resource_type,
                    "resource_name": resource_name,
                    "pattern_type": pattern_type,
                    "principal": principal,
                    "host": host,
                    "operation": operation,
                    "permission_type": permission_type,
                }
            )
        pos = skip_tags(response, pos)
        filter_results.append(
            {
                "error_code": error_code,
                "error_message": error_message,
                "matching_acls": matching_acls,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"DeleteAcls response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "filter_results": filter_results}


def delete_acls(port, binding_filter, correlation_id):
    body = write_compact_array_len(1)
    body += acl_filter_body(binding_filter)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 31, 2, correlation_id, body)
    return parse_delete_acls_response(response, correlation_id)


def require_delete_acls_success(response, binding):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"DeleteAcls throttle mismatch: {response}")
    results = response["filter_results"]
    if len(results) != 1:
        raise TestError(f"DeleteAcls result count mismatch: {response}")
    result = results[0]
    if result["error_code"] != 0 or result["error_message"] is not None:
        raise TestError(f"DeleteAcls result mismatch: {response}")
    if not result["matching_acls"]:
        raise TestError(f"DeleteAcls did not match ACL: {response}")
    for acl in result["matching_acls"]:
        if acl["error_code"] != 0 or acl["error_message"] is not None:
            raise TestError(f"DeleteAcls matching ACL error: {response}")
        resource = {
            "resource_type": acl["resource_type"],
            "resource_name": acl["resource_name"],
            "pattern_type": acl["pattern_type"],
        }
        if not acl_matches(binding, resource, acl):
            raise TestError(f"DeleteAcls matching ACL mismatch: {response}")


def wait_for_acl_admin_seed(port, broad_allow_acl, deleted_acl, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8840
    last_error = None
    while time.time() < deadline:
        try:
            created = create_acls(
                port,
                [broad_allow_acl, deleted_acl],
                correlation_id,
            )
            require_create_acls_success(created, 2)
            correlation_id += 1
            require_acl_visible(
                describe_acls(port, broad_allow_acl, correlation_id),
                broad_allow_acl,
            )
            correlation_id += 1
            deleted = delete_acls(port, deleted_acl, correlation_id)
            require_delete_acls_success(deleted, deleted_acl)
            correlation_id += 1
            require_acl_absent(
                describe_acls(port, deleted_acl, correlation_id),
                deleted_acl,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"ACL admin seed did not recover: {last_error}")


def wait_for_acl_admin_checkpoint(port, broad_allow_acl, deleted_acl, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8880
    last_error = None
    while time.time() < deadline:
        try:
            require_acl_visible(
                describe_acls(port, broad_allow_acl, correlation_id),
                broad_allow_acl,
            )
            correlation_id += 1
            require_acl_absent(
                describe_acls(port, deleted_acl, correlation_id),
                deleted_acl,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"ACL admin checkpoint did not recover: {last_error}")


def parse_alter_client_quotas_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    entry_count, pos = read_compact_array_len(response, pos)
    entries = []
    for _ in range(entry_count):
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        entity_count, pos = read_compact_array_len(response, pos)
        entity = []
        for _ in range(entity_count):
            entity_type, pos = read_compact_string(response, pos)
            entity_name, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
            entity.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name,
                }
            )
        pos = skip_tags(response, pos)
        entries.append(
            {
                "error_code": error_code,
                "error_message": error_message,
                "entity": entity,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"AlterClientQuotas response trailing bytes: {len(response) - pos}"
        )
    return {"throttle_time_ms": throttle_time_ms, "entries": entries}


def alter_client_quotas(port, client_id, quota_ops, validate_only, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_array_len(1)
    body += write_compact_string("client-id")
    body += write_compact_string(client_id)
    body += b"\x00"  # entity tagged fields
    body += write_compact_array_len(len(quota_ops))
    for key, value, remove in quota_ops:
        body += write_compact_string(key)
        body += struct.pack(">d", value)
        body += b"\x01" if remove else b"\x00"
        body += b"\x00"  # op tagged fields
    body += b"\x00"  # entry tagged fields
    body += b"\x01" if validate_only else b"\x00"
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 49, 1, correlation_id, body)
    return parse_alter_client_quotas_response(response, correlation_id)


def require_alter_client_quota_success(response, client_id):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"AlterClientQuotas throttle mismatch: {response}")
    entries = response["entries"]
    if len(entries) != 1:
        raise TestError(f"AlterClientQuotas entry count mismatch: {response}")
    entry = entries[0]
    if entry["error_code"] != 0 or entry["error_message"] is not None:
        raise TestError(f"AlterClientQuotas entry mismatch: {response}")
    entity = entry["entity"]
    if (
        len(entity) != 1
        or entity[0]["entity_type"] != "client-id"
        or entity[0]["entity_name"] != client_id
    ):
        raise TestError(f"AlterClientQuotas entity mismatch: {response}")


def wait_for_alter_client_quotas_mutation(port, client_id, quota_ops, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8500
    last_error = None
    while time.time() < deadline:
        try:
            response = alter_client_quotas(
                port,
                client_id,
                quota_ops,
                False,
                correlation_id,
            )
            require_alter_client_quota_success(response, client_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"AlterClientQuotas did not mutate {client_id!r}: {last_error}")


def parse_describe_client_quotas_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    entry_count, pos = read_compact_array_len(response, pos)
    entries = []
    for _ in range(entry_count):
        entity_count, pos = read_compact_array_len(response, pos)
        entity = []
        for _ in range(entity_count):
            entity_type, pos = read_compact_string(response, pos)
            entity_name, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
            entity.append(
                {
                    "entity_type": entity_type,
                    "entity_name": entity_name,
                }
            )
        value_count, pos = read_compact_array_len(response, pos)
        values = []
        for _ in range(value_count):
            key, pos = read_compact_string(response, pos)
            value, pos = read_f64(response, pos)
            pos = skip_tags(response, pos)
            values.append({"key": key, "value": value})
        pos = skip_tags(response, pos)
        entries.append({"entity": entity, "values": values})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeClientQuotas response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
        "entries": entries,
    }


def describe_client_quotas(port, client_id, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string("client-id")
    body += b"\x00"  # exact name match
    body += write_compact_string(client_id)
    body += b"\x00"  # component tagged fields
    body += b"\x01"  # strict
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 48, 1, correlation_id, body)
    return parse_describe_client_quotas_response(response, correlation_id)


def require_describe_client_quota_values(response, client_id, expected_values):
    if (
        response["throttle_time_ms"] != 0
        or response["error_code"] != 0
        or response["error_message"] is not None
    ):
        raise TestError(f"DescribeClientQuotas top-level mismatch: {response}")
    entries = response["entries"]
    if len(entries) != 1:
        raise TestError(f"DescribeClientQuotas entry count mismatch: {response}")
    entity = entries[0]["entity"]
    if (
        len(entity) != 1
        or entity[0]["entity_type"] != "client-id"
        or entity[0]["entity_name"] != client_id
    ):
        raise TestError(f"DescribeClientQuotas entity mismatch: {response}")
    values = {item["key"]: item["value"] for item in entries[0]["values"]}
    if set(values) != set(expected_values):
        raise TestError(f"DescribeClientQuotas value keys mismatch: {response}")
    for key, expected in expected_values.items():
        if abs(values[key] - expected) > 0.000001:
            raise TestError(
                f"DescribeClientQuotas value mismatch for {key}: "
                f"{values[key]} != {expected}"
            )


def require_describe_client_quota_absent(response):
    if (
        response["throttle_time_ms"] != 0
        or response["error_code"] != 0
        or response["error_message"] is not None
    ):
        raise TestError(f"DescribeClientQuotas empty top-level mismatch: {response}")
    if response["entries"]:
        raise TestError(f"DescribeClientQuotas expected no entries: {response}")


def wait_for_client_quotas_checkpoint(
    port,
    client_id,
    expected_values,
    validate_only_client_id,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8540
    last_error = None
    validate_only_ops = [("producer_byte_rate", 9876.0, False)]
    while time.time() < deadline:
        try:
            response = describe_client_quotas(port, client_id, correlation_id)
            require_describe_client_quota_values(response, client_id, expected_values)
            correlation_id += 1
            validate_response = alter_client_quotas(
                port,
                validate_only_client_id,
                validate_only_ops,
                True,
                correlation_id,
            )
            require_alter_client_quota_success(
                validate_response,
                validate_only_client_id,
            )
            correlation_id += 1
            absent_response = describe_client_quotas(
                port,
                validate_only_client_id,
                correlation_id,
            )
            require_describe_client_quota_absent(absent_response)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"Client quota checkpoint did not recover: {last_error}")


def parse_alter_user_scram_credentials_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        user, pos = read_compact_string(response, pos)
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        results.append(
            {
                "user": user,
                "error_code": error_code,
                "error_message": error_message,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"AlterUserScramCredentials response trailing bytes: {len(response) - pos}"
        )
    return {"throttle_time_ms": throttle_time_ms, "results": results}


def alter_user_scram_credentials_upsert(
    port,
    user,
    salt,
    salted_password,
    iterations,
    correlation_id,
):
    body = write_compact_array_len(0)
    body += write_compact_array_len(1)
    body += write_compact_string(user)
    body += b"\x01"  # SCRAM-SHA-256
    body += struct.pack(">i", iterations)
    body += write_compact_bytes(salt)
    body += write_compact_bytes(salted_password)
    body += b"\x00"  # upsertion tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 51, 0, correlation_id, body)
    return parse_alter_user_scram_credentials_response(response, correlation_id)


def require_alter_user_scram_credentials_success(response, user):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"AlterUserScramCredentials throttle mismatch: {response}")
    results = response["results"]
    if len(results) != 1:
        raise TestError(f"AlterUserScramCredentials result count mismatch: {response}")
    result = results[0]
    if result["user"] != user:
        raise TestError(f"AlterUserScramCredentials user mismatch: {response}")
    if result["error_code"] != 0 or result["error_message"] is not None:
        raise TestError(f"AlterUserScramCredentials result mismatch: {response}")


def wait_for_alter_user_scram_credentials_upsert(
    port,
    user,
    salt,
    salted_password,
    iterations,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8580
    last_error = None
    while time.time() < deadline:
        try:
            response = alter_user_scram_credentials_upsert(
                port,
                user,
                salt,
                salted_password,
                iterations,
                correlation_id,
            )
            require_alter_user_scram_credentials_success(response, user)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"AlterUserScramCredentials did not upsert {user!r}: {last_error}"
    )


def parse_describe_user_scram_credentials_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        user, pos = read_compact_string(response, pos)
        user_error_code, pos = read_i16(response, pos)
        user_error_message, pos = read_compact_string(response, pos)
        credential_count, pos = read_compact_array_len(response, pos)
        credential_infos = []
        for _ in range(credential_count):
            mechanism, pos = read_i8(response, pos)
            iterations, pos = read_i32(response, pos)
            pos = skip_tags(response, pos)
            credential_infos.append(
                {
                    "mechanism": mechanism,
                    "iterations": iterations,
                }
            )
        pos = skip_tags(response, pos)
        results.append(
            {
                "user": user,
                "error_code": user_error_code,
                "error_message": user_error_message,
                "credential_infos": credential_infos,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeUserScramCredentials response trailing bytes: "
            f"{len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
        "results": results,
    }


def describe_user_scram_credentials(port, user, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(user)
    body += b"\x00"  # user tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 50, 0, correlation_id, body)
    return parse_describe_user_scram_credentials_response(response, correlation_id)


def require_describe_user_scram_credentials(
    response,
    user,
    expected_iterations,
):
    if (
        response["throttle_time_ms"] != 0
        or response["error_code"] != 0
        or response["error_message"] is not None
    ):
        raise TestError(
            f"DescribeUserScramCredentials top-level mismatch: {response}"
        )
    results = response["results"]
    if len(results) != 1:
        raise TestError(
            f"DescribeUserScramCredentials result count mismatch: {response}"
        )
    result = results[0]
    if (
        result["user"] != user
        or result["error_code"] != 0
        or result["error_message"] is not None
    ):
        raise TestError(f"DescribeUserScramCredentials result mismatch: {response}")
    credential_infos = result["credential_infos"]
    if credential_infos != [{"mechanism": 1, "iterations": expected_iterations}]:
        raise TestError(
            f"DescribeUserScramCredentials credential mismatch: {response}"
        )


def wait_for_user_scram_credentials_checkpoint(
    port,
    user,
    expected_iterations,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8620
    last_error = None
    while time.time() < deadline:
        try:
            response = describe_user_scram_credentials(port, user, correlation_id)
            require_describe_user_scram_credentials(
                response,
                user,
                expected_iterations,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"SCRAM credential checkpoint did not recover: {last_error}")


def parse_get_telemetry_subscriptions_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    if pos + 16 > len(response):
        raise TestError("buffer underflow while reading client instance id")
    client_instance_id = response[pos : pos + 16]
    pos += 16
    subscription_id, pos = read_i32(response, pos)
    compression_count, pos = read_compact_array_len(response, pos)
    accepted_compression_types = []
    for _ in range(compression_count):
        compression_type, pos = read_i8(response, pos)
        accepted_compression_types.append(compression_type)
    push_interval_ms, pos = read_i32(response, pos)
    telemetry_max_bytes, pos = read_i32(response, pos)
    delta_temporality, pos = read_bool(response, pos)
    metric_count, pos = read_compact_array_len(response, pos)
    requested_metrics = []
    for _ in range(metric_count):
        metric, pos = read_compact_string(response, pos)
        requested_metrics.append(metric)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"GetTelemetrySubscriptions response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "client_instance_id": client_instance_id,
        "subscription_id": subscription_id,
        "accepted_compression_types": accepted_compression_types,
        "push_interval_ms": push_interval_ms,
        "telemetry_max_bytes": telemetry_max_bytes,
        "delta_temporality": delta_temporality,
        "requested_metrics": requested_metrics,
    }


def get_telemetry_subscriptions(port, client_instance_id, correlation_id):
    body = client_instance_id
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 71, 0, correlation_id, body)
    return parse_get_telemetry_subscriptions_response(response, correlation_id)


def require_telemetry_subscription(response, client_instance_id):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"GetTelemetrySubscriptions top-level mismatch: {response}")
    if response["client_instance_id"] != client_instance_id:
        raise TestError(f"GetTelemetrySubscriptions client id mismatch: {response}")
    if response["subscription_id"] != 1:
        raise TestError(f"GetTelemetrySubscriptions subscription mismatch: {response}")
    if response["accepted_compression_types"] != [0]:
        raise TestError(f"GetTelemetrySubscriptions compression mismatch: {response}")
    if response["push_interval_ms"] <= 0 or response["telemetry_max_bytes"] <= 0:
        raise TestError(f"GetTelemetrySubscriptions limits mismatch: {response}")
    if response["delta_temporality"]:
        raise TestError(f"GetTelemetrySubscriptions delta mismatch: {response}")
    if response["requested_metrics"] != [""]:
        raise TestError(f"GetTelemetrySubscriptions metrics mismatch: {response}")


def parse_push_telemetry_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"PushTelemetry response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "error_code": error_code}


def push_telemetry(port, client_instance_id, subscription_id, metrics, correlation_id):
    body = client_instance_id
    body += struct.pack(">i", subscription_id)
    body += b"\x00"  # terminating=false
    body += b"\x00"  # compression_type=none
    body += write_compact_bytes(metrics)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 72, 0, correlation_id, body)
    return parse_push_telemetry_response(response, correlation_id)


def require_push_telemetry_success(response):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"PushTelemetry mismatch: {response}")


def parse_list_client_metrics_resources_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    resource_count, pos = read_compact_array_len(response, pos)
    resources = []
    for _ in range(resource_count):
        name, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        resources.append(name)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ListClientMetricsResources response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "resources": resources,
    }


def list_client_metrics_resources(port, correlation_id):
    response = flexible_kafka_request(port, 74, 0, correlation_id, b"\x00")
    return parse_list_client_metrics_resources_response(response, correlation_id)


def require_client_metrics_resources(response, client_instance_id):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"ListClientMetricsResources top-level mismatch: {response}")
    resources = response["resources"]
    expected_resource = f"client:{client_instance_id.hex()}"
    if "default" not in resources or expected_resource not in resources:
        raise TestError(f"ListClientMetricsResources resources mismatch: {response}")


def wait_for_client_telemetry_checkpoint(
    port,
    client_instance_id,
    metrics,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8660
    last_error = None
    while time.time() < deadline:
        try:
            subscription = get_telemetry_subscriptions(
                port,
                client_instance_id,
                correlation_id,
            )
            require_telemetry_subscription(subscription, client_instance_id)
            correlation_id += 1
            push_response = push_telemetry(
                port,
                client_instance_id,
                subscription["subscription_id"],
                metrics,
                correlation_id,
            )
            require_push_telemetry_success(push_response)
            correlation_id += 1
            resources = list_client_metrics_resources(port, correlation_id)
            require_client_metrics_resources(resources, client_instance_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"Client telemetry checkpoint did not recover: {last_error}")


def parse_create_delegation_token_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    principal_type, pos = read_compact_string(response, pos)
    principal_name, pos = read_compact_string(response, pos)
    requester_principal_type, pos = read_compact_string(response, pos)
    requester_principal_name, pos = read_compact_string(response, pos)
    issue_timestamp_ms, pos = read_i64(response, pos)
    expiry_timestamp_ms, pos = read_i64(response, pos)
    max_timestamp_ms, pos = read_i64(response, pos)
    token_id, pos = read_compact_string(response, pos)
    hmac, pos = read_compact_bytes(response, pos)
    throttle_time_ms, pos = read_i32(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"CreateDelegationToken response trailing bytes: {len(response) - pos}"
        )
    return {
        "error_code": error_code,
        "principal_type": principal_type,
        "principal_name": principal_name,
        "requester_principal_type": requester_principal_type,
        "requester_principal_name": requester_principal_name,
        "issue_timestamp_ms": issue_timestamp_ms,
        "expiry_timestamp_ms": expiry_timestamp_ms,
        "max_timestamp_ms": max_timestamp_ms,
        "token_id": token_id,
        "hmac": hmac,
        "throttle_time_ms": throttle_time_ms,
    }


def create_delegation_token(port, max_lifetime_ms, correlation_id):
    body = write_compact_string(None)  # owner_principal_type=request principal
    body += write_compact_string(None)  # owner_principal_name=request principal
    body += write_compact_array_len(0)
    body += struct.pack(">q", max_lifetime_ms)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 38, 3, correlation_id, body)
    return parse_create_delegation_token_response(response, correlation_id)


def require_create_delegation_token_success(response, owner_name):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"CreateDelegationToken top-level mismatch: {response}")
    if response["principal_type"] != "User" or response["principal_name"] != owner_name:
        raise TestError(f"CreateDelegationToken owner mismatch: {response}")
    if (
        response["requester_principal_type"] != "User"
        or response["requester_principal_name"] != owner_name
    ):
        raise TestError(f"CreateDelegationToken requester mismatch: {response}")
    if not response["token_id"] or len(response["hmac"] or b"") != 32:
        raise TestError(f"CreateDelegationToken token material mismatch: {response}")
    if not (
        response["issue_timestamp_ms"]
        <= response["expiry_timestamp_ms"]
        <= response["max_timestamp_ms"]
    ):
        raise TestError(f"CreateDelegationToken timestamp mismatch: {response}")


def wait_for_create_delegation_token(port, owner_name, max_lifetime_ms, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8700
    last_error = None
    while time.time() < deadline:
        try:
            response = create_delegation_token(
                port,
                max_lifetime_ms,
                correlation_id,
            )
            require_create_delegation_token_success(response, owner_name)
            return response
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"CreateDelegationToken did not create token: {last_error}")


def parse_delegation_token_lifecycle_response(response, correlation_id, label):
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    expiry_timestamp_ms, pos = read_i64(response, pos)
    throttle_time_ms, pos = read_i32(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"{label} response trailing bytes: {len(response) - pos}")
    return {
        "error_code": error_code,
        "expiry_timestamp_ms": expiry_timestamp_ms,
        "throttle_time_ms": throttle_time_ms,
    }


def renew_delegation_token(port, hmac, renew_period_ms, correlation_id):
    body = write_compact_bytes(hmac)
    body += struct.pack(">q", renew_period_ms)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 39, 2, correlation_id, body)
    return parse_delegation_token_lifecycle_response(
        response,
        correlation_id,
        "RenewDelegationToken",
    )


def expire_delegation_token(port, hmac, expiry_period_ms, correlation_id):
    body = write_compact_bytes(hmac)
    body += struct.pack(">q", expiry_period_ms)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 40, 2, correlation_id, body)
    return parse_delegation_token_lifecycle_response(
        response,
        correlation_id,
        "ExpireDelegationToken",
    )


def require_delegation_token_lifecycle_success(response, label):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"{label} mismatch: {response}")
    if response["expiry_timestamp_ms"] <= 0:
        raise TestError(f"{label} expiry mismatch: {response}")


def parse_describe_delegation_token_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    token_count, pos = read_compact_array_len(response, pos)
    tokens = []
    for _ in range(token_count):
        principal_type, pos = read_compact_string(response, pos)
        principal_name, pos = read_compact_string(response, pos)
        requester_principal_type, pos = read_compact_string(response, pos)
        requester_principal_name, pos = read_compact_string(response, pos)
        issue_timestamp, pos = read_i64(response, pos)
        expiry_timestamp, pos = read_i64(response, pos)
        max_timestamp, pos = read_i64(response, pos)
        token_id, pos = read_compact_string(response, pos)
        hmac, pos = read_compact_bytes(response, pos)
        renewer_count, pos = read_compact_array_len(response, pos)
        renewers = []
        for _ in range(renewer_count):
            renewer_type, pos = read_compact_string(response, pos)
            renewer_name, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
            renewers.append(
                {
                    "principal_type": renewer_type,
                    "principal_name": renewer_name,
                }
            )
        pos = skip_tags(response, pos)
        tokens.append(
            {
                "principal_type": principal_type,
                "principal_name": principal_name,
                "requester_principal_type": requester_principal_type,
                "requester_principal_name": requester_principal_name,
                "issue_timestamp": issue_timestamp,
                "expiry_timestamp": expiry_timestamp,
                "max_timestamp": max_timestamp,
                "token_id": token_id,
                "hmac": hmac,
                "renewers": renewers,
            }
        )
    throttle_time_ms, pos = read_i32(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeDelegationToken response trailing bytes: {len(response) - pos}"
        )
    return {
        "error_code": error_code,
        "tokens": tokens,
        "throttle_time_ms": throttle_time_ms,
    }


def describe_delegation_token(port, owner_name, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string("User")
    body += write_compact_string(owner_name)
    body += b"\x00"  # owner tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 41, 3, correlation_id, body)
    return parse_describe_delegation_token_response(response, correlation_id)


def require_delegation_token_visible(response, owner_name, token_id, hmac):
    if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
        raise TestError(f"DescribeDelegationToken top-level mismatch: {response}")
    matches = [
        token
        for token in response["tokens"]
        if token["token_id"] == token_id and token["hmac"] == hmac
    ]
    if len(matches) != 1:
        raise TestError(f"DescribeDelegationToken token mismatch: {response}")
    token = matches[0]
    if token["principal_type"] != "User" or token["principal_name"] != owner_name:
        raise TestError(f"DescribeDelegationToken owner mismatch: {response}")
    if (
        token["requester_principal_type"] != "User"
        or token["requester_principal_name"] != owner_name
    ):
        raise TestError(f"DescribeDelegationToken requester mismatch: {response}")
    if token["renewers"]:
        raise TestError(f"DescribeDelegationToken renewer mismatch: {response}")
    if not (
        token["issue_timestamp"]
        <= token["expiry_timestamp"]
        <= token["max_timestamp"]
    ):
        raise TestError(f"DescribeDelegationToken timestamp mismatch: {response}")


def wait_for_delegation_token_checkpoint(
    port,
    owner_name,
    token_id,
    hmac,
    lifetime_ms,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8740
    last_error = None
    while time.time() < deadline:
        try:
            renewed = renew_delegation_token(
                port,
                hmac,
                lifetime_ms,
                correlation_id,
            )
            require_delegation_token_lifecycle_success(
                renewed,
                "RenewDelegationToken",
            )
            correlation_id += 1
            expiry = expire_delegation_token(
                port,
                hmac,
                lifetime_ms,
                correlation_id,
            )
            require_delegation_token_lifecycle_success(
                expiry,
                "ExpireDelegationToken",
            )
            correlation_id += 1
            described = describe_delegation_token(port, owner_name, correlation_id)
            require_delegation_token_visible(described, owner_name, token_id, hmac)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"Delegation token checkpoint did not recover: {last_error}")


def parse_describe_configs_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        resource_type, pos = read_i8(response, pos)
        resource_name, pos = read_compact_string(response, pos)
        config_count, pos = read_compact_array_len(response, pos)
        configs = []
        for _ in range(config_count):
            name, pos = read_compact_string(response, pos)
            value, pos = read_compact_string(response, pos)
            read_only, pos = read_bool(response, pos)
            config_source, pos = read_i8(response, pos)
            is_sensitive, pos = read_bool(response, pos)
            synonym_count, pos = read_compact_array_len(response, pos)
            synonyms = []
            for _ in range(synonym_count):
                synonym_name, pos = read_compact_string(response, pos)
                synonym_value, pos = read_compact_string(response, pos)
                synonym_source, pos = read_i8(response, pos)
                pos = skip_tags(response, pos)
                synonyms.append(
                    {
                        "name": synonym_name,
                        "value": synonym_value,
                        "source": synonym_source,
                    }
                )
            config_type, pos = read_i8(response, pos)
            documentation, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
            configs.append(
                {
                    "name": name,
                    "value": value,
                    "read_only": read_only,
                    "config_source": config_source,
                    "is_sensitive": is_sensitive,
                    "synonyms": synonyms,
                    "config_type": config_type,
                    "documentation": documentation,
                }
            )
        pos = skip_tags(response, pos)
        results.append(
            {
                "error_code": error_code,
                "error_message": error_message,
                "resource_type": resource_type,
                "resource_name": resource_name,
                "configs": configs,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"DescribeConfigs response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "results": results}


def describe_topic_configs(port, topic, correlation_id):
    return describe_topic_selected_configs(
        port,
        topic,
        ["cleanup.policy", "min.insync.replicas"],
        correlation_id,
    )


def describe_topic_selected_configs(port, topic, config_names, correlation_id):
    body = write_compact_array_len(1)
    body += struct.pack(">b", 2)  # resource_type=TOPIC
    body += write_compact_string(topic)
    body += write_compact_array_len(len(config_names))
    for config_name in config_names:
        body += write_compact_string(config_name)
    body += b"\x00"  # resource tagged fields
    body += b"\x00"  # include_synonyms=false
    body += b"\x01"  # include_documentation=true
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 32, 4, correlation_id, body)
    return parse_describe_configs_response(response, correlation_id)


def wait_for_describe_configs_checkpoint(port, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8450
    last_error = None
    while time.time() < deadline:
        try:
            response = describe_topic_configs(port, topic, correlation_id)
            results = response["results"]
            if response["throttle_time_ms"] != 0:
                raise TestError(f"DescribeConfigs throttle mismatch: {response}")
            if len(results) != 1:
                raise TestError(f"DescribeConfigs result count mismatch: {response}")
            result = results[0]
            if (
                result["error_code"] != 0
                or result["resource_type"] != 2
                or result["resource_name"] != topic
            ):
                raise TestError(f"DescribeConfigs resource mismatch: {response}")
            configs = {item["name"]: item for item in result["configs"]}
            cleanup = configs.get("cleanup.policy")
            min_isr = configs.get("min.insync.replicas")
            if cleanup is None or cleanup["value"] != "delete":
                raise TestError(f"DescribeConfigs cleanup mismatch: {response}")
            if min_isr is None or min_isr["value"] != "1":
                raise TestError(f"DescribeConfigs min ISR mismatch: {response}")
            if cleanup["documentation"] is None or min_isr["documentation"] is None:
                raise TestError(f"DescribeConfigs documentation missing: {response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"DescribeConfigs did not recover for {topic!r}: {last_error}")


def parse_alter_configs_response(response, correlation_id, label):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    response_count, pos = read_compact_array_len(response, pos)
    responses = []
    for _ in range(response_count):
        error_code, pos = read_i16(response, pos)
        error_message, pos = read_compact_string(response, pos)
        resource_type, pos = read_i8(response, pos)
        resource_name, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        responses.append(
            {
                "error_code": error_code,
                "error_message": error_message,
                "resource_type": resource_type,
                "resource_name": resource_name,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"{label} response trailing bytes: {len(response) - pos}")
    return {"throttle_time_ms": throttle_time_ms, "responses": responses}


def alter_configs(port, topic, configs, validate_only, correlation_id):
    body = write_compact_array_len(1)
    body += b"\x02"  # resource_type=TOPIC
    body += write_compact_string(topic)
    body += write_compact_array_len(len(configs))
    for name, value in configs:
        body += write_compact_string(name)
        body += write_compact_string(value)
        body += b"\x00"  # config tagged fields
    body += b"\x00"  # resource tagged fields
    body += b"\x01" if validate_only else b"\x00"
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 33, 2, correlation_id, body)
    return parse_alter_configs_response(response, correlation_id, "AlterConfigs")


def incremental_alter_configs(port, topic, configs, validate_only, correlation_id):
    body = write_compact_array_len(1)
    body += b"\x02"  # resource_type=TOPIC
    body += write_compact_string(topic)
    body += write_compact_array_len(len(configs))
    for name, operation, value in configs:
        body += write_compact_string(name)
        body += struct.pack(">b", operation)
        body += write_compact_string(value)
        body += b"\x00"  # config tagged fields
    body += b"\x00"  # resource tagged fields
    body += b"\x01" if validate_only else b"\x00"
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 44, 1, correlation_id, body)
    return parse_alter_configs_response(
        response,
        correlation_id,
        "IncrementalAlterConfigs",
    )


def require_alter_configs_success(response, topic, label):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"{label} throttle mismatch: {response}")
    responses = response["responses"]
    if len(responses) != 1:
        raise TestError(f"{label} response count mismatch: {response}")
    item = responses[0]
    if (
        item["error_code"] != 0
        or item["error_message"] is not None
        or item["resource_type"] != 2
        or item["resource_name"] != topic
    ):
        raise TestError(f"{label} resource mismatch: {response}")


def require_topic_config_values(response, topic, expected_values):
    if response["throttle_time_ms"] != 0:
        raise TestError(f"DescribeConfigs throttle mismatch: {response}")
    results = response["results"]
    if len(results) != 1:
        raise TestError(f"DescribeConfigs result count mismatch: {response}")
    result = results[0]
    if (
        result["error_code"] != 0
        or result["resource_type"] != 2
        or result["resource_name"] != topic
    ):
        raise TestError(f"DescribeConfigs resource mismatch: {response}")
    configs = {item["name"]: item for item in result["configs"]}
    for name, expected_value in expected_values.items():
        config = configs.get(name)
        if config is None or config["value"] != expected_value:
            raise TestError(
                f"DescribeConfigs {name} mismatch: expected={expected_value!r} "
                f"response={response}"
            )
        if config["documentation"] is None:
            raise TestError(f"DescribeConfigs {name} documentation missing: {response}")


def wait_for_config_admin_seed(
    port,
    topic,
    alter_configs_values,
    incremental_configs_values,
    final_values,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8910
    last_error = None
    while time.time() < deadline:
        try:
            altered = alter_configs(
                port,
                topic,
                alter_configs_values,
                False,
                correlation_id,
            )
            require_alter_configs_success(altered, topic, "AlterConfigs")
            correlation_id += 1
            incremented = incremental_alter_configs(
                port,
                topic,
                incremental_configs_values,
                False,
                correlation_id,
            )
            require_alter_configs_success(
                incremented,
                topic,
                "IncrementalAlterConfigs",
            )
            correlation_id += 1
            described = describe_topic_selected_configs(
                port,
                topic,
                list(final_values.keys()),
                correlation_id,
            )
            require_topic_config_values(described, topic, final_values)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"Config admin seed did not recover for {topic!r}: {last_error}")


def wait_for_config_admin_checkpoint(
    port,
    topic,
    alter_configs_values,
    incremental_configs_values,
    final_values,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8950
    last_error = None
    while time.time() < deadline:
        try:
            altered = alter_configs(
                port,
                topic,
                alter_configs_values,
                True,
                correlation_id,
            )
            require_alter_configs_success(altered, topic, "AlterConfigs")
            correlation_id += 1
            incremented = incremental_alter_configs(
                port,
                topic,
                incremental_configs_values,
                True,
                correlation_id,
            )
            require_alter_configs_success(
                incremented,
                topic,
                "IncrementalAlterConfigs",
            )
            correlation_id += 1
            described = describe_topic_selected_configs(
                port,
                topic,
                list(final_values.keys()),
                correlation_id,
            )
            require_topic_config_values(described, topic, final_values)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"Config admin checkpoint did not recover for {topic!r}: {last_error}"
    )


def wait_for_create_topics_seed(
    port,
    topic,
    configs,
    expected_values,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 8990
    last_error = None
    while time.time() < deadline:
        try:
            created = create_topic_with_configs(
                port,
                topic,
                1,
                1,
                configs,
                False,
                correlation_id,
            )
            require_create_topics_result(
                created,
                topic,
                1,
                1,
                (ERROR_NONE, ERROR_TOPIC_ALREADY_EXISTS),
                "CreateTopics",
            )
            described = describe_topic_selected_configs(
                port,
                topic,
                list(expected_values.keys()),
                correlation_id + 1,
            )
            require_topic_config_values(described, topic, expected_values)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(f"CreateTopics seed did not recover for {topic!r}: {last_error}")


def wait_for_create_topics_checkpoint(
    port,
    topic,
    configs,
    expected_values,
    validate_only_topic,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = 9020
    last_error = None
    while time.time() < deadline:
        try:
            described = describe_topic_selected_configs(
                port,
                topic,
                list(expected_values.keys()),
                correlation_id,
            )
            require_topic_config_values(described, topic, expected_values)
            created = create_topic_with_configs(
                port,
                validate_only_topic,
                1,
                1,
                configs,
                True,
                correlation_id + 1,
            )
            require_create_topics_result(
                created,
                validate_only_topic,
                1,
                1,
                ERROR_NONE,
                "CreateTopics validate-only",
            )
            validate_only_described = describe_topic_partitions(
                port,
                validate_only_topic,
                correlation_id + 2,
            )
            require_deleted_topic_absent(validate_only_described, validate_only_topic)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 3
        time.sleep(0.25)
    raise TestError(
        f"CreateTopics checkpoint did not recover for {topic!r}: {last_error}"
    )


def parse_describe_log_dirs_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        result_error, pos = read_i16(response, pos)
        log_dir, pos = read_compact_string(response, pos)
        topic_count, pos = read_compact_array_len(response, pos)
        topics = []
        for _ in range(topic_count):
            topic_name, pos = read_compact_string(response, pos)
            partition_count, pos = read_compact_array_len(response, pos)
            partitions = []
            for _ in range(partition_count):
                partition_index, pos = read_i32(response, pos)
                partition_size, pos = read_i64(response, pos)
                offset_lag, pos = read_i64(response, pos)
                is_future_key, pos = read_bool(response, pos)
                pos = skip_tags(response, pos)
                partitions.append(
                    {
                        "partition_index": partition_index,
                        "partition_size": partition_size,
                        "offset_lag": offset_lag,
                        "is_future_key": is_future_key,
                    }
                )
            pos = skip_tags(response, pos)
            topics.append({"name": topic_name, "partitions": partitions})
        total_bytes, pos = read_i64(response, pos)
        usable_bytes, pos = read_i64(response, pos)
        pos = skip_tags(response, pos)
        results.append(
            {
                "error_code": result_error,
                "log_dir": log_dir,
                "topics": topics,
                "total_bytes": total_bytes,
                "usable_bytes": usable_bytes,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"DescribeLogDirs response trailing bytes: {len(response) - pos}")
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "results": results,
    }


def describe_log_dirs(port, topic, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_i32_array([0])
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 35, 4, correlation_id, body)
    return parse_describe_log_dirs_response(response, correlation_id)


def wait_for_describe_log_dirs_checkpoint(port, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8550
    last_error = None
    while time.time() < deadline:
        try:
            response = describe_log_dirs(port, topic, correlation_id)
            if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
                raise TestError(f"DescribeLogDirs top-level mismatch: {response}")
            results = response["results"]
            if len(results) != 1 or results[0]["error_code"] != 0:
                raise TestError(f"DescribeLogDirs result mismatch: {response}")
            result = results[0]
            if not result["log_dir"]:
                raise TestError(f"DescribeLogDirs missing log dir: {response}")
            topics = result["topics"]
            if len(topics) != 1 or topics[0]["name"] != topic:
                raise TestError(f"DescribeLogDirs topic mismatch: {response}")
            partitions = topics[0]["partitions"]
            if len(partitions) != 1:
                raise TestError(f"DescribeLogDirs partition count mismatch: {response}")
            partition = partitions[0]
            if partition["partition_index"] != 0:
                raise TestError(f"DescribeLogDirs partition index mismatch: {response}")
            if partition["partition_size"] < 1:
                raise TestError(f"DescribeLogDirs partition size mismatch: {response}")
            if partition["offset_lag"] != 0 or partition["is_future_key"]:
                raise TestError(f"DescribeLogDirs future/lag mismatch: {response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"DescribeLogDirs did not recover for {topic!r}: {last_error}")


def parse_alter_replica_log_dirs_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            error_code, pos = read_i16(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": error_code,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"AlterReplicaLogDirs response trailing bytes: {len(response) - pos}"
        )
    return {"throttle_time_ms": throttle_time_ms, "topics": topics}


def alter_replica_log_dirs(port, topic, path, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(path)
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_i32_array([0])
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # dir tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 34, 2, correlation_id, body)
    return parse_alter_replica_log_dirs_response(response, correlation_id)


def wait_for_alter_replica_log_dirs_checkpoint(port, topic, path, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8580
    last_error = None
    while time.time() < deadline:
        try:
            response = alter_replica_log_dirs(port, topic, path, correlation_id)
            if response["throttle_time_ms"] != 0:
                raise TestError(f"AlterReplicaLogDirs throttle mismatch: {response}")
            topics = response["topics"]
            if len(topics) != 1 or topics[0]["name"] != topic:
                raise TestError(f"AlterReplicaLogDirs topic mismatch: {response}")
            partitions = topics[0]["partitions"]
            if len(partitions) != 1:
                raise TestError(
                    f"AlterReplicaLogDirs partition count mismatch: {response}"
                )
            partition = partitions[0]
            if partition["partition_index"] != 0 or partition["error_code"] != 0:
                raise TestError(f"AlterReplicaLogDirs partition error: {response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"AlterReplicaLogDirs did not recover for {topic!r}: {last_error}"
    )


def parse_assign_replicas_to_dirs_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    directory_count, pos = read_compact_array_len(response, pos)
    directories = []
    for _ in range(directory_count):
        if pos + 16 > len(response):
            raise TestError("AssignReplicasToDirs response truncated in directory id")
        directory_id = response[pos : pos + 16]
        pos += 16
        topic_count, pos = read_compact_array_len(response, pos)
        topics = []
        for _ in range(topic_count):
            if pos + 16 > len(response):
                raise TestError("AssignReplicasToDirs response truncated in topic id")
            topic_id = response[pos : pos + 16]
            pos += 16
            partition_count, pos = read_compact_array_len(response, pos)
            partitions = []
            for _ in range(partition_count):
                partition_index, pos = read_i32(response, pos)
                partition_error, pos = read_i16(response, pos)
                pos = skip_tags(response, pos)
                partitions.append(
                    {
                        "partition_index": partition_index,
                        "error_code": partition_error,
                    }
                )
            pos = skip_tags(response, pos)
            topics.append({"topic_id": topic_id, "partitions": partitions})
        pos = skip_tags(response, pos)
        directories.append({"id": directory_id, "topics": topics})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"AssignReplicasToDirs response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "directories": directories,
    }


def assign_replicas_to_dirs(
    port, broker_id, broker_epoch, directory_id, topic_id, partition_index, correlation_id
):
    body = struct.pack(">iq", broker_id, broker_epoch)
    body += write_compact_array_len(1)
    body += directory_id
    body += write_compact_array_len(1)
    body += topic_id
    body += write_compact_array_len(1)
    body += struct.pack(">i", partition_index)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # directory tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 73, 0, correlation_id, body)
    return parse_assign_replicas_to_dirs_response(response, correlation_id)


def latest_broker_epoch(log_path):
    marker = "broker_epoch="
    text = tail(log_path, limit=32000)
    epoch = None
    offset = 0
    while True:
        idx = text.find(marker, offset)
        if idx < 0:
            return epoch
        start = idx + len(marker)
        end = start
        while end < len(text) and text[end].isdigit():
            end += 1
        if end > start:
            epoch = int(text[start:end])
        offset = end


def wait_for_assign_replicas_to_dirs_checkpoint(
    port, topic, data_dir, log_path, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8610
    directory_id = derive_replica_directory_id(data_dir)
    last_error = None
    while time.time() < deadline:
        try:
            broker_epoch = latest_broker_epoch(log_path)
            if broker_epoch is None:
                raise TestError("broker epoch not yet visible in broker log")
            topic_response = describe_topic_partitions(port, topic, correlation_id)
            correlation_id += 1
            topics = topic_response["topics"]
            if len(topics) != 1 or topics[0]["name"] != topic:
                raise TestError(f"AssignReplicasToDirs topic lookup mismatch: {topic_response}")
            topic_result = topics[0]
            if topic_result["error_code"] != 0 or len(topic_result["topic_id"]) != 16:
                raise TestError(f"AssignReplicasToDirs topic lookup error: {topic_response}")

            response = assign_replicas_to_dirs(
                port,
                100,
                broker_epoch,
                directory_id,
                topic_result["topic_id"],
                0,
                correlation_id,
            )
            if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
                raise TestError(f"AssignReplicasToDirs top-level mismatch: {response}")
            directories = response["directories"]
            if len(directories) != 1 or directories[0]["id"] != directory_id:
                raise TestError(f"AssignReplicasToDirs directory mismatch: {response}")
            topics = directories[0]["topics"]
            if len(topics) != 1 or topics[0]["topic_id"] != topic_result["topic_id"]:
                raise TestError(f"AssignReplicasToDirs response topic mismatch: {response}")
            partitions = topics[0]["partitions"]
            if len(partitions) != 1:
                raise TestError(f"AssignReplicasToDirs partition count mismatch: {response}")
            partition = partitions[0]
            if partition["partition_index"] != 0 or partition["error_code"] != 0:
                raise TestError(f"AssignReplicasToDirs partition error: {response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"AssignReplicasToDirs did not recover for {topic!r}: {last_error}")


def parse_elect_leaders_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_id, pos = read_i32(response, pos)
            partition_error, pos = read_i16(response, pos)
            error_message, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_id": partition_id,
                    "error_code": partition_error,
                    "error_message": error_message,
                }
            )
        pos = skip_tags(response, pos)
        results.append({"topic": topic_name, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"ElectLeaders response trailing bytes: {len(response) - pos}")
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "results": results,
    }


def elect_leaders(port, topic, correlation_id):
    body = b"\x00"  # preferred election
    body += write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_i32_array([0])
    body += b"\x00"  # topic tagged fields
    body += struct.pack(">i", 30000)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 43, 2, correlation_id, body)
    return parse_elect_leaders_response(response, correlation_id)


def wait_for_elect_leaders_checkpoint(port, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8640
    last_error = None
    while time.time() < deadline:
        try:
            response = elect_leaders(port, topic, correlation_id)
            if response["throttle_time_ms"] != 0 or response["error_code"] != 0:
                raise TestError(f"ElectLeaders top-level mismatch: {response}")
            results = response["results"]
            if len(results) != 1 or results[0]["topic"] != topic:
                raise TestError(f"ElectLeaders topic mismatch: {response}")
            partitions = results[0]["partitions"]
            if len(partitions) != 1:
                raise TestError(f"ElectLeaders partition count mismatch: {response}")
            partition = partitions[0]
            if (
                partition["partition_id"] != 0
                or partition["error_code"] != 0
                or partition["error_message"] is not None
            ):
                raise TestError(f"ElectLeaders partition mismatch: {response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"ElectLeaders did not recover for {topic!r}: {last_error}")


def parse_nullable_compact_i32_array(buf, pos):
    raw_len, pos = read_varint(buf, pos)
    if raw_len == 0:
        return None, pos
    values = []
    for _ in range(raw_len - 1):
        value, pos = read_i32(buf, pos)
        values.append(value)
    return values, pos


def parse_describe_topic_partitions_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        error_code, pos = read_i16(response, pos)
        topic_name, pos = read_compact_string(response, pos)
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading topic uuid")
        topic_id = response[pos : pos + 16]
        pos += 16
        is_internal, pos = read_bool(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_error, pos = read_i16(response, pos)
            partition_index, pos = read_i32(response, pos)
            leader_id, pos = read_i32(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            replica_nodes, pos = read_compact_i32_array(response, pos)
            isr_nodes, pos = read_compact_i32_array(response, pos)
            eligible_leader_replicas, pos = parse_nullable_compact_i32_array(
                response, pos
            )
            last_known_elr, pos = parse_nullable_compact_i32_array(response, pos)
            offline_replicas, pos = read_compact_i32_array(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "error_code": partition_error,
                    "partition_index": partition_index,
                    "leader_id": leader_id,
                    "leader_epoch": leader_epoch,
                    "replica_nodes": replica_nodes,
                    "isr_nodes": isr_nodes,
                    "eligible_leader_replicas": eligible_leader_replicas,
                    "last_known_elr": last_known_elr,
                    "offline_replicas": offline_replicas,
                }
            )
        topic_authorized_operations, pos = read_i32(response, pos)
        pos = skip_tags(response, pos)
        topics.append(
            {
                "error_code": error_code,
                "name": topic_name,
                "topic_id": topic_id,
                "is_internal": is_internal,
                "partitions": partitions,
                "topic_authorized_operations": topic_authorized_operations,
            }
        )
    cursor_present, pos = read_varint(response, pos)
    next_cursor = None
    if cursor_present != 0:
        cursor_topic, pos = read_compact_string(response, pos)
        cursor_partition, pos = read_i32(response, pos)
        pos = skip_tags(response, pos)
        next_cursor = {
            "topic_name": cursor_topic,
            "partition_index": cursor_partition,
        }
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeTopicPartitions response trailing bytes: {len(response) - pos}"
        )
    return {"topics": topics, "next_cursor": next_cursor}


def describe_topic_partitions(port, topic, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += b"\x00"  # topic tagged fields
    body += struct.pack(">i", 10)  # response_partition_limit
    body += b"\x00"  # null cursor
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 75, 0, correlation_id, body)
    return parse_describe_topic_partitions_response(response, correlation_id)


def wait_for_describe_topic_partitions_checkpoint(
    port, topic, expected_leader_id, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8400
    last_error = None
    while time.time() < deadline:
        try:
            response = describe_topic_partitions(port, topic, correlation_id)
            topics = response["topics"]
            if response["next_cursor"] is not None:
                raise TestError(f"DescribeTopicPartitions unexpected cursor: {response}")
            if len(topics) != 1 or topics[0]["name"] != topic:
                raise TestError(f"DescribeTopicPartitions topic mismatch: {response}")
            topic_result = topics[0]
            if topic_result["error_code"] != 0:
                raise TestError(f"DescribeTopicPartitions topic error: {response}")
            partitions = topic_result["partitions"]
            if len(partitions) != 1:
                raise TestError(
                    f"DescribeTopicPartitions partition count mismatch: {response}"
                )
            partition = partitions[0]
            if partition["partition_index"] != 0 or partition["error_code"] != 0:
                raise TestError(
                    f"DescribeTopicPartitions partition error: {response}"
                )
            if partition["leader_id"] != expected_leader_id:
                raise TestError(
                    f"DescribeTopicPartitions leader={partition} "
                    f"expected={expected_leader_id}"
                )
            if expected_leader_id not in partition["replica_nodes"]:
                raise TestError(f"DescribeTopicPartitions replicas={response}")
            if expected_leader_id not in partition["isr_nodes"]:
                raise TestError(f"DescribeTopicPartitions isr={response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"DescribeTopicPartitions did not recover for {topic!r}: {last_error}"
    )


def wait_for_describe_topic_partitions_count_checkpoint(
    port, topic, expected_leader_id, expected_partition_count, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8425
    last_error = None
    while time.time() < deadline:
        try:
            response = describe_topic_partitions(port, topic, correlation_id)
            topics = response["topics"]
            if response["next_cursor"] is not None:
                raise TestError(f"DescribeTopicPartitions unexpected cursor: {response}")
            if len(topics) != 1 or topics[0]["name"] != topic:
                raise TestError(f"DescribeTopicPartitions topic mismatch: {response}")
            topic_result = topics[0]
            if topic_result["error_code"] != 0:
                raise TestError(f"DescribeTopicPartitions topic error: {response}")
            partitions = topic_result["partitions"]
            if len(partitions) != expected_partition_count:
                raise TestError(
                    f"DescribeTopicPartitions partition count mismatch: {response}"
                )
            seen = set()
            for partition in partitions:
                if partition["error_code"] != 0:
                    raise TestError(f"DescribeTopicPartitions partition error: {response}")
                if partition["leader_id"] != expected_leader_id:
                    raise TestError(
                        f"DescribeTopicPartitions leader={partition} "
                        f"expected={expected_leader_id}"
                    )
                if expected_leader_id not in partition["replica_nodes"]:
                    raise TestError(f"DescribeTopicPartitions replicas={response}")
                if expected_leader_id not in partition["isr_nodes"]:
                    raise TestError(f"DescribeTopicPartitions isr={response}")
                seen.add(partition["partition_index"])
            if seen != set(range(expected_partition_count)):
                raise TestError(f"DescribeTopicPartitions indexes={seen}: {response}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"DescribeTopicPartitions count did not recover for {topic!r}: {last_error}"
    )


def require_deleted_topic_absent(response, topic):
    if response["next_cursor"] is not None:
        raise TestError(f"Deleted topic DescribeTopicPartitions cursor: {response}")
    topics = response["topics"]
    if len(topics) != 1 or topics[0]["name"] != topic:
        raise TestError(f"Deleted topic DescribeTopicPartitions mismatch: {response}")
    topic_result = topics[0]
    if topic_result["error_code"] != ERROR_UNKNOWN_TOPIC_OR_PARTITION:
        raise TestError(f"Deleted topic still visible: {response}")
    if topic_result["partitions"]:
        raise TestError(f"Deleted topic partitions still visible: {response}")


def wait_for_delete_topics_seed(port, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8460
    last_error = None
    while time.time() < deadline:
        try:
            deleted = delete_topic(port, topic, correlation_id)
            require_delete_topics_result(
                deleted,
                topic,
                (ERROR_NONE, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                "DeleteTopics",
            )
            described = describe_topic_partitions(port, topic, correlation_id + 1)
            require_deleted_topic_absent(described, topic)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(f"DeleteTopics seed did not recover for {topic!r}: {last_error}")


def wait_for_deleted_topic_checkpoint(port, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8480
    last_error = None
    while time.time() < deadline:
        try:
            described = describe_topic_partitions(port, topic, correlation_id)
            require_deleted_topic_absent(described, topic)
            deleted = delete_topic(port, topic, correlation_id + 1)
            require_delete_topics_result(
                deleted,
                topic,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                "DeleteTopics deleted-topic checkpoint",
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(
        f"Deleted topic checkpoint did not recover for {topic!r}: {last_error}"
    )


def parse_describe_cluster_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    endpoint_type, pos = read_i8(response, pos)
    cluster_id, pos = read_compact_string(response, pos)
    controller_id, pos = read_i32(response, pos)
    broker_count, pos = read_compact_array_len(response, pos)
    brokers = []
    for _ in range(broker_count):
        broker_id, pos = read_i32(response, pos)
        host, pos = read_compact_string(response, pos)
        broker_port, pos = read_i32(response, pos)
        rack, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        brokers.append(
            {
                "broker_id": broker_id,
                "host": host,
                "port": broker_port,
                "rack": rack,
            }
        )
    cluster_authorized_operations, pos = read_i32(response, pos)
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"DescribeCluster response trailing bytes: {len(response) - pos}")
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "error_message": error_message,
        "endpoint_type": endpoint_type,
        "cluster_id": cluster_id,
        "controller_id": controller_id,
        "brokers": brokers,
        "cluster_authorized_operations": cluster_authorized_operations,
    }


def describe_cluster(
    port, endpoint_type, include_cluster_authorized_operations, correlation_id
):
    body = b"\x01" if include_cluster_authorized_operations else b"\x00"
    body += struct.pack(">b", endpoint_type)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 60, 1, correlation_id, body)
    return parse_describe_cluster_response(response, correlation_id)


def require_describe_cluster_checkpoint(
    response,
    expected_endpoint_type,
    expected_node_id,
    expected_port,
    expected_cluster_id,
    expected_authorized_operations,
):
    if response["error_code"] != 0:
        raise TestError(f"DescribeCluster error response: {response}")
    if response["endpoint_type"] != expected_endpoint_type:
        raise TestError(
            f"DescribeCluster endpoint={response} expected={expected_endpoint_type}"
        )
    if response["cluster_id"] != expected_cluster_id:
        raise TestError(
            f"DescribeCluster cluster_id={response} expected={expected_cluster_id!r}"
        )
    if response["controller_id"] != expected_node_id:
        raise TestError(
            f"DescribeCluster controller_id={response} expected={expected_node_id}"
        )
    if response["cluster_authorized_operations"] != expected_authorized_operations:
        raise TestError(
            f"DescribeCluster authorized ops={response} "
            f"expected={expected_authorized_operations}"
        )
    brokers = response["brokers"]
    if len(brokers) != 1:
        raise TestError(f"DescribeCluster broker count mismatch: {response}")
    broker = brokers[0]
    if (
        broker["broker_id"] != expected_node_id
        or broker["host"] != "localhost"
        or broker["port"] != expected_port
        or broker["rack"] is not None
    ):
        raise TestError(f"DescribeCluster broker mismatch: {response}")


def wait_for_describe_cluster_checkpoint(
    port, expected_node_id, expected_cluster_id, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8500
    last_error = None
    while time.time() < deadline:
        try:
            broker_response = describe_cluster(port, 1, False, correlation_id)
            require_describe_cluster_checkpoint(
                broker_response,
                1,
                expected_node_id,
                port,
                expected_cluster_id,
                -2147483648,
            )
            controller_response = describe_cluster(port, 2, True, correlation_id + 1)
            require_describe_cluster_checkpoint(
                controller_response,
                2,
                expected_node_id,
                port,
                expected_cluster_id,
                0,
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(f"DescribeCluster did not recover: {last_error}")


def parse_describe_producers_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            error_code, pos = read_i16(response, pos)
            error_message, pos = read_compact_string(response, pos)
            producer_count, pos = read_compact_array_len(response, pos)
            active_producers = []
            for _ in range(producer_count):
                producer_id, pos = read_i64(response, pos)
                producer_epoch, pos = read_i32(response, pos)
                last_sequence, pos = read_i32(response, pos)
                last_timestamp, pos = read_i64(response, pos)
                coordinator_epoch, pos = read_i32(response, pos)
                current_txn_start_offset, pos = read_i64(response, pos)
                pos = skip_tags(response, pos)
                active_producers.append(
                    {
                        "producer_id": producer_id,
                        "producer_epoch": producer_epoch,
                        "last_sequence": last_sequence,
                        "last_timestamp": last_timestamp,
                        "coordinator_epoch": coordinator_epoch,
                        "current_txn_start_offset": current_txn_start_offset,
                    }
                )
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": error_code,
                    "error_message": error_message,
                    "active_producers": active_producers,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeProducers response trailing bytes: {len(response) - pos}"
        )
    return topics


def describe_producers(port, topic, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(topic)
    body += write_compact_i32_array([0])
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 61, 0, correlation_id, body)
    return parse_describe_producers_response(response, correlation_id)


def wait_for_describe_producers_checkpoint(
    port, topic, identity, min_last_sequence, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 8150
    last_error = None
    while time.time() < deadline:
        try:
            topics = describe_producers(port, topic, correlation_id)
            if len(topics) != 1 or topics[0]["name"] != topic:
                raise TestError(f"DescribeProducers topic mismatch: {topics}")
            partitions = topics[0]["partitions"]
            if len(partitions) != 1:
                raise TestError(f"DescribeProducers partition count mismatch: {topics}")
            partition = partitions[0]
            if partition["partition_index"] != 0 or partition["error_code"] != 0:
                raise TestError(f"DescribeProducers partition error: {topics}")
            producer = next(
                (
                    item
                    for item in partition["active_producers"]
                    if item["producer_id"] == identity["producer_id"]
                ),
                None,
            )
            if producer is None:
                raise TestError(f"DescribeProducers missing producer: {topics}")
            if producer["producer_epoch"] != identity["producer_epoch"]:
                raise TestError(f"DescribeProducers epoch mismatch: {topics}")
            if producer["last_sequence"] < min_last_sequence:
                raise TestError(f"DescribeProducers sequence mismatch: {topics}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"DescribeProducers did not recover for {topic!r}: {last_error}"
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
    rack_id=None,
    instance_id=None,
):
    body = write_compact_string(group_id)
    body += write_compact_string(member_id)
    body += struct.pack(">i", member_epoch)
    body += write_compact_string(instance_id)
    body += write_compact_string(rack_id)
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


def wait_for_consumer_group_heartbeat_static_join(
    port, group_id, topic, instance_id, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7750
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
                instance_id=instance_id,
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"static join error_code={response['error_code']} "
                    f"message={response['error_message']!r}"
                )
            assignment = response["assignment"]
            if assignment is None or not assignment["topic_partitions"]:
                raise TestError(f"static join missing assignment: {response}")
            topic_assignment = assignment["topic_partitions"][0]
            group_state = {
                "group_id": group_id,
                "member_id": response["member_id"],
                "member_epoch": response["member_epoch"],
                "topic_id": topic_assignment["topic_id"],
                "instance_id": instance_id,
            }
            assert_consumer_group_heartbeat_assignment(response, group_state)
            return group_state
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat static group {group_id!r} did not join: {last_error}"
    )


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


def wait_for_consumer_group_heartbeat_owned_assignment(
    port, group_state, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7810
    last_error = None
    owned = [
        {
            "topic_id": group_state["topic_id"],
            "partitions": [0],
        }
    ]
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                group_state["member_epoch"],
                correlation_id,
                topic_partitions=owned,
            )
            assert_consumer_group_heartbeat_assignment(response, group_state)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat owned assignment did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_consumer_group_heartbeat_static_rejoin(
    port, group_state, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7820
    last_error = None
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                "",
                -2,
                correlation_id,
                instance_id=group_state["instance_id"],
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ConsumerGroupHeartbeat static rejoin error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if response["member_id"] != group_state["member_id"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat static rejoin member mismatch: {response}"
                )
            if response["member_epoch"] != group_state["member_epoch"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat static rejoin epoch mismatch: {response}"
                )
            assert_consumer_group_heartbeat_assignment(response, group_state)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat static rejoin did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_consumer_group_heartbeat_rack_update(
    port, group_state, rack_id, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7830
    last_error = None
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                group_state["member_epoch"],
                correlation_id,
                rack_id=rack_id,
            )
            assert_consumer_group_heartbeat_assignment(response, group_state)
            group_state["rack_id"] = rack_id
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat rack update did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_consumer_group_heartbeat_subscription_update(
    port, group_state, topic, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7840
    last_error = None
    previous_epoch = group_state["member_epoch"]
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                group_state["member_epoch"],
                correlation_id,
                subscribed_topics=[topic],
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ConsumerGroupHeartbeat subscription update error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if response["member_id"] != group_state["member_id"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat subscription update member mismatch: "
                    f"{response}"
                )
            if response["member_epoch"] <= previous_epoch:
                raise TestError(
                    f"ConsumerGroupHeartbeat subscription update epoch did not "
                    f"advance: {response}"
                )
            assignment = response["assignment"]
            if assignment is None or not assignment["topic_partitions"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat subscription update missing assignment: "
                    f"{response}"
                )
            topic_assignment = assignment["topic_partitions"][0]
            group_state["member_epoch"] = response["member_epoch"]
            group_state["topic_id"] = topic_assignment["topic_id"]
            assert_consumer_group_heartbeat_assignment(response, group_state)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat subscription update did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def assert_consumer_group_heartbeat_negative_join(
    port,
    group_id,
    topic,
    expected_error,
    correlation_id,
    subscribed_topics,
    server_assignor,
    message_fragment=None,
):
    response = consumer_group_heartbeat(
        port,
        group_id,
        f"{group_id}-member",
        0,
        correlation_id,
        subscribed_topics=subscribed_topics,
        server_assignor=server_assignor,
    )
    if response["error_code"] != expected_error:
        raise TestError(
            f"ConsumerGroupHeartbeat negative join {group_id!r} error_code="
            f"{response['error_code']} expected={expected_error} "
            f"message={response['error_message']!r}"
        )
    if message_fragment is not None:
        error_message = response["error_message"] or ""
        if message_fragment not in error_message:
            raise TestError(
                f"ConsumerGroupHeartbeat negative join {group_id!r} "
                f"message={response['error_message']!r} missing "
                f"{message_fragment!r}"
            )
    if response["heartbeat_interval_ms"] != 0 or response["assignment"] is not None:
        raise TestError(
            f"ConsumerGroupHeartbeat negative join returned active state: {response}"
        )

    described = consumer_group_describe(port, group_id, correlation_id + 100)
    if described["error_code"] != ERROR_GROUP_ID_NOT_FOUND:
        raise TestError(
            f"ConsumerGroupHeartbeat negative join materialized group {group_id!r}: "
            f"{described}"
        )
    if described["members"]:
        raise TestError(
            f"ConsumerGroupHeartbeat negative join returned members for "
            f"{group_id!r}: {described}"
        )


def wait_for_consumer_group_heartbeat_negative_joins(
    port, group_prefix, topic, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7860
    last_error = None
    duplicate_group = f"{group_prefix}-duplicate-subscription"
    unsupported_group = f"{group_prefix}-unsupported-assignor"
    while time.time() < deadline:
        try:
            assert_consumer_group_heartbeat_negative_join(
                port,
                duplicate_group,
                topic,
                ERROR_INVALID_REQUEST,
                correlation_id,
                subscribed_topics=[topic, topic],
                server_assignor="range",
                message_fragment="invalid ConsumerGroupHeartbeat subscription",
            )
            assert_consumer_group_heartbeat_negative_join(
                port,
                unsupported_group,
                topic,
                ERROR_UNSUPPORTED_ASSIGNOR,
                correlation_id + 1,
                subscribed_topics=[topic],
                server_assignor="roundrobin",
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 2
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat negative joins did not recover for "
        f"{group_prefix!r}: {last_error}"
    )


def wait_for_consumer_group_heartbeat_leave(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7850
    last_error = None
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                -1,
                correlation_id,
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ConsumerGroupHeartbeat leave error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if response["member_id"] != group_state["member_id"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat leave member mismatch: {response}"
                )
            if response["member_epoch"] != -1:
                raise TestError(
                    f"ConsumerGroupHeartbeat leave epoch mismatch: {response}"
                )
            if response["heartbeat_interval_ms"] != 0 or response["assignment"] is not None:
                raise TestError(
                    f"ConsumerGroupHeartbeat leave returned active state: {response}"
                )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat leave did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_consumer_group_heartbeat_unknown_member(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7875
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
            if response["error_code"] != ERROR_UNKNOWN_MEMBER_ID:
                raise TestError(
                    f"ConsumerGroupHeartbeat old member error="
                    f"{response['error_code']} response={response}"
                )
            if response["member_id"] != group_state["member_id"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat old member id mismatch: {response}"
                )
            if response["heartbeat_interval_ms"] != 0 or response["assignment"] is not None:
                raise TestError(
                    f"ConsumerGroupHeartbeat old member returned active state: {response}"
                )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat old member did not fence for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_consumer_group_heartbeat_rejoin(
    port, group_state, topic, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7890
    last_error = None
    while time.time() < deadline:
        try:
            response = consumer_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                0,
                correlation_id,
                subscribed_topics=[topic],
                server_assignor="range",
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ConsumerGroupHeartbeat rejoin error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if response["member_id"] != group_state["member_id"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat rejoin member mismatch: {response}"
                )
            if response["member_epoch"] <= group_state["member_epoch"]:
                raise TestError(
                    f"ConsumerGroupHeartbeat rejoin epoch did not advance: "
                    f"{response}"
                )
            assignment = response["assignment"]
            if assignment is None or not assignment["topic_partitions"]:
                raise TestError(f"ConsumerGroupHeartbeat rejoin missing assignment: {response}")
            topic_assignment = assignment["topic_partitions"][0]
            rejoined_state = {
                "group_id": group_state["group_id"],
                "member_id": response["member_id"],
                "member_epoch": response["member_epoch"],
                "topic_id": topic_assignment["topic_id"],
            }
            assert_consumer_group_heartbeat_assignment(response, rejoined_state)
            return rejoined_state
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ConsumerGroupHeartbeat rejoin did not recover for "
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
    if group_state.get("instance_id") is not None:
        if matching_member["instance_id"] != group_state["instance_id"]:
            raise TestError(
                f"KIP-848 ConsumerGroupDescribe instance_id="
                f"{matching_member['instance_id']!r} "
                f"expected={group_state['instance_id']!r}"
            )
    if group_state.get("rack_id") is not None:
        if matching_member["rack_id"] != group_state["rack_id"]:
            raise TestError(
                f"KIP-848 ConsumerGroupDescribe rack_id="
                f"{matching_member['rack_id']!r} expected={group_state['rack_id']!r}"
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


def wait_for_kip848_static_member_checkpoint(port, group_state, topic):
    wait_for_consumer_group_heartbeat_static_rejoin(port, group_state)
    wait_for_kip848_consumer_group_description(port, group_state, topic)


def wait_for_kip848_subscription_checkpoint(port, group_state, topic):
    wait_for_consumer_group_heartbeat(port, group_state)
    wait_for_consumer_group_heartbeat_owned_assignment(port, group_state)
    wait_for_kip848_consumer_group_description(port, group_state, topic)


def wait_for_kip848_negative_checkpoint(port, group_prefix, topic):
    wait_for_consumer_group_heartbeat_negative_joins(port, group_prefix, topic)


def parse_share_group_heartbeat_response(response, correlation_id):
    return parse_consumer_group_heartbeat_response(response, correlation_id)


def share_group_heartbeat(
    port,
    group_id,
    member_id,
    member_epoch,
    correlation_id,
    subscribed_topics=None,
    rack_id=None,
):
    body = write_compact_string(group_id)
    body += write_compact_string(member_id)
    body += struct.pack(">i", member_epoch)
    body += write_compact_string(rack_id)
    if subscribed_topics is None:
        body += b"\x00"
    else:
        body += write_compact_array_len(len(subscribed_topics))
        for subscribed_topic in subscribed_topics:
            body += write_compact_string(subscribed_topic)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 76, 0, correlation_id, body)
    return parse_share_group_heartbeat_response(response, correlation_id)


def assert_share_group_heartbeat_assignment(response, group_state):
    if response["error_code"] != 0:
        raise TestError(
            f"ShareGroupHeartbeat {group_state['group_id']!r} error_code="
            f"{response['error_code']} message={response['error_message']!r}"
        )
    if response["member_id"] != group_state["member_id"]:
        raise TestError(f"ShareGroupHeartbeat member mismatch: {response}")
    if response["member_epoch"] < group_state["member_epoch"]:
        raise TestError(f"ShareGroupHeartbeat epoch regressed: {response}")
    if response["heartbeat_interval_ms"] != 3000:
        raise TestError(f"ShareGroupHeartbeat interval mismatch: {response}")
    assignment = response["assignment"]
    if assignment is None:
        raise TestError(f"ShareGroupHeartbeat missing assignment: {response}")
    matching_topic = next(
        (
            topic
            for topic in assignment["topic_partitions"]
            if topic["topic_id"] == group_state["topic_id"]
        ),
        None,
    )
    if matching_topic is None:
        raise TestError(f"ShareGroupHeartbeat missing topic assignment: {response}")
    if matching_topic["partitions"] != [0]:
        raise TestError(f"ShareGroupHeartbeat partition mismatch: {response}")
    group_state["member_epoch"] = response["member_epoch"]


def wait_for_share_group_heartbeat_join(port, group_id, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7940
    member_id = f"{group_id}-member"
    last_error = None
    while time.time() < deadline:
        try:
            response = share_group_heartbeat(
                port,
                group_id,
                member_id,
                0,
                correlation_id,
                subscribed_topics=[topic],
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ShareGroupHeartbeat join error_code={response['error_code']} "
                    f"message={response['error_message']!r}"
                )
            assignment = response["assignment"]
            if assignment is None or not assignment["topic_partitions"]:
                raise TestError(f"ShareGroupHeartbeat join missing assignment: {response}")
            topic_assignment = assignment["topic_partitions"][0]
            group_state = {
                "group_id": group_id,
                "member_id": response["member_id"],
                "member_epoch": response["member_epoch"],
                "topic_id": topic_assignment["topic_id"],
            }
            assert_share_group_heartbeat_assignment(response, group_state)
            return group_state
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"ShareGroupHeartbeat group {group_id!r} did not join: {last_error}")


def wait_for_share_group_heartbeat(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7950
    last_error = None
    while time.time() < deadline:
        try:
            response = share_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                group_state["member_epoch"],
                correlation_id,
            )
            assert_share_group_heartbeat_assignment(response, group_state)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareGroupHeartbeat did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_share_group_heartbeat_rack_update(
    port, group_state, rack_id, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7960
    last_error = None
    while time.time() < deadline:
        try:
            response = share_group_heartbeat(
                port,
                group_state["group_id"],
                group_state["member_id"],
                group_state["member_epoch"],
                correlation_id,
                rack_id=rack_id,
            )
            assert_share_group_heartbeat_assignment(response, group_state)
            group_state["rack_id"] = rack_id
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareGroupHeartbeat rack update did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def parse_share_group_describe_response(response, correlation_id):
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
            rack_id, pos = read_compact_string(response, pos)
            member_epoch, pos = read_i32(response, pos)
            client_id, pos = read_compact_string(response, pos)
            client_host, pos = read_compact_string(response, pos)
            subscribed_count, pos = read_compact_array_len(response, pos)
            subscribed_topics = []
            for _ in range(subscribed_count):
                topic_name, pos = read_compact_string(response, pos)
                subscribed_topics.append(topic_name)
            assignment, pos = parse_consumer_group_assignment(response, pos)
            pos = skip_tags(response, pos)
            members.append(
                {
                    "member_id": member_id,
                    "rack_id": rack_id,
                    "member_epoch": member_epoch,
                    "client_id": client_id,
                    "client_host": client_host,
                    "subscribed_topics": subscribed_topics,
                    "assignment": assignment,
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
            f"ShareGroupDescribe response trailing bytes: {len(response) - pos}"
        )
    return groups


def share_group_describe(port, group_id, correlation_id):
    body = write_compact_array_len(1)
    body += write_compact_string(group_id)
    body += b"\x00"  # include_authorized_operations=false
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 77, 0, correlation_id, body)
    groups = parse_share_group_describe_response(response, correlation_id)
    if len(groups) != 1:
        raise TestError(f"ShareGroupDescribe count={len(groups)}")
    return groups[0]


def assert_share_group_description(port, group_state, topic, correlation_id):
    described = share_group_describe(port, group_state["group_id"], correlation_id)
    if described["error_code"] != 0:
        raise TestError(
            f"ShareGroupDescribe {group_state['group_id']!r} "
            f"error_code={described['error_code']} "
            f"message={described['error_message']!r}"
        )
    if described["group_state"] != "Stable":
        raise TestError(f"ShareGroupDescribe state={described['group_state']!r}")
    if described["group_epoch"] != group_state["member_epoch"]:
        raise TestError(
            f"ShareGroupDescribe group_epoch={described['group_epoch']} "
            f"expected={group_state['member_epoch']}"
        )
    if described["assignment_epoch"] != group_state["member_epoch"]:
        raise TestError(
            f"ShareGroupDescribe assignment_epoch={described['assignment_epoch']} "
            f"expected={group_state['member_epoch']}"
        )
    if described["assignor_name"] != "range":
        raise TestError(f"ShareGroupDescribe assignor mismatch: {described}")
    matching_member = next(
        (
            member
            for member in described["members"]
            if member["member_id"] == group_state["member_id"]
        ),
        None,
    )
    if matching_member is None:
        raise TestError(f"ShareGroupDescribe missing member: {described}")
    if matching_member["member_epoch"] != group_state["member_epoch"]:
        raise TestError(
            f"ShareGroupDescribe member_epoch={matching_member['member_epoch']} "
            f"expected={group_state['member_epoch']}"
        )
    if group_state.get("rack_id") is not None:
        if matching_member["rack_id"] != group_state["rack_id"]:
            raise TestError(
                f"ShareGroupDescribe rack_id={matching_member['rack_id']!r} "
                f"expected={group_state['rack_id']!r}"
            )
    if topic not in matching_member["subscribed_topics"]:
        raise TestError(f"ShareGroupDescribe subscriptions mismatch: {matching_member}")
    assignment = matching_member["assignment"]
    matching_topic = next(
        (
            described_topic
            for described_topic in assignment["topic_partitions"]
            if described_topic["topic_id"] == group_state["topic_id"]
        ),
        None,
    )
    if matching_topic is None:
        raise TestError(f"ShareGroupDescribe missing assignment: {matching_member}")
    if matching_topic["topic_name"] != topic:
        raise TestError(f"ShareGroupDescribe topic name mismatch: {matching_topic}")
    if matching_topic["partitions"] != [0]:
        raise TestError(f"ShareGroupDescribe partitions mismatch: {matching_topic}")


def wait_for_share_group_description(port, group_state, topic, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7970
    last_error = None
    while time.time() < deadline:
        try:
            assert_share_group_description(port, group_state, topic, correlation_id)
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareGroupDescribe did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_share_group_checkpoint(port, group_state, topic):
    wait_for_share_group_heartbeat(port, group_state)
    wait_for_share_group_description(port, group_state, topic)


def write_share_fetch_topics(topic_partitions):
    out = bytearray(write_compact_array_len(len(topic_partitions)))
    for topic in topic_partitions:
        topic_id = topic["topic_id"]
        if len(topic_id) != 16:
            raise TestError(f"invalid share fetch topic id length {len(topic_id)}")
        out += topic_id
        partitions = topic.get("partitions", [])
        out += write_compact_array_len(len(partitions))
        for partition in partitions:
            out += struct.pack(
                ">ii",
                partition.get("partition_index", 0),
                partition.get("partition_max_bytes", 0),
            )
            acknowledgement_batches = partition.get("acknowledgement_batches", [])
            out += write_compact_array_len(len(acknowledgement_batches))
            for batch in acknowledgement_batches:
                out += struct.pack(">qq", batch["first_offset"], batch["last_offset"])
                acknowledge_types = batch.get("acknowledge_types", [])
                out += write_compact_array_len(len(acknowledge_types))
                for acknowledge_type in acknowledge_types:
                    out += struct.pack(">b", acknowledge_type)
                out += b"\x00"  # acknowledgement batch tagged fields
            out += b"\x00"  # partition tagged fields
        out += b"\x00"  # topic tagged fields
    return bytes(out)


def parse_share_fetch_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    response_count, pos = read_compact_array_len(response, pos)
    responses = []
    for _ in range(response_count):
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading ShareFetch topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            partition_error, pos = read_i16(response, pos)
            partition_message, pos = read_compact_string(response, pos)
            acknowledge_error, pos = read_i16(response, pos)
            acknowledge_message, pos = read_compact_string(response, pos)
            leader_id, pos = read_i32(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            pos = skip_tags(response, pos)
            records, pos = read_compact_bytes(response, pos)
            acquired_count, pos = read_compact_array_len(response, pos)
            acquired_records = []
            for _ in range(acquired_count):
                first_offset, pos = read_i64(response, pos)
                last_offset, pos = read_i64(response, pos)
                delivery_count, pos = read_i16(response, pos)
                pos = skip_tags(response, pos)
                acquired_records.append(
                    {
                        "first_offset": first_offset,
                        "last_offset": last_offset,
                        "delivery_count": delivery_count,
                    }
                )
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": partition_error,
                    "error_message": partition_message,
                    "acknowledge_error_code": acknowledge_error,
                    "acknowledge_error_message": acknowledge_message,
                    "leader_id": leader_id,
                    "leader_epoch": leader_epoch,
                    "records": records,
                    "acquired_records": acquired_records,
                }
            )
        pos = skip_tags(response, pos)
        responses.append({"topic_id": topic_id, "partitions": partitions})
    node_endpoint_count, pos = read_compact_array_len(response, pos)
    node_endpoints = []
    for _ in range(node_endpoint_count):
        node_id, pos = read_i32(response, pos)
        host, pos = read_compact_string(response, pos)
        endpoint_port, pos = read_i32(response, pos)
        rack, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        node_endpoints.append(
            {
                "node_id": node_id,
                "host": host,
                "port": endpoint_port,
                "rack": rack,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(f"ShareFetch response trailing bytes: {len(response) - pos}")
    return {
        "error_code": error_code,
        "error_message": error_message,
        "responses": responses,
        "node_endpoints": node_endpoints,
    }


def share_fetch(
    port,
    group_state,
    share_session_epoch,
    correlation_id,
    topic_partitions=None,
    max_bytes=1024,
):
    if topic_partitions is None:
        topic_partitions = []
    body = write_compact_string(group_state["group_id"])
    body += write_compact_string(group_state["member_id"])
    body += struct.pack(">i", share_session_epoch)
    body += struct.pack(">iii", 1, 0, max_bytes)
    body += write_share_fetch_topics(topic_partitions)
    body += write_compact_array_len(0)  # forgotten_topics_data
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 78, 0, correlation_id, body)
    return parse_share_fetch_response(response, correlation_id)


def wait_for_share_fetch_open(
    port, group_state, expected_payload, timeout=30
):
    deadline = time.time() + timeout
    correlation_id = 7980
    last_error = None
    topic_partitions = [
        {
            "topic_id": group_state["topic_id"],
            "partitions": [
                {
                    "partition_index": 0,
                    "partition_max_bytes": 1024,
                }
            ],
        }
    ]
    while time.time() < deadline:
        try:
            response = share_fetch(
                port,
                group_state,
                0,
                correlation_id,
                topic_partitions=topic_partitions,
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ShareFetch open error_code={response['error_code']} "
                    f"message={response['error_message']!r}"
                )
            if len(response["responses"]) != 1:
                raise TestError(f"ShareFetch open topic count mismatch: {response}")
            topic = response["responses"][0]
            if topic["topic_id"] != group_state["topic_id"]:
                raise TestError(f"ShareFetch open topic id mismatch: {response}")
            if len(topic["partitions"]) != 1:
                raise TestError(f"ShareFetch open partition count mismatch: {response}")
            partition = topic["partitions"][0]
            if (
                partition["partition_index"] != 0
                or partition["error_code"] != 0
                or partition["acknowledge_error_code"] != 0
            ):
                raise TestError(f"ShareFetch open partition error: {response}")
            records = partition["records"] or b""
            if expected_payload not in records:
                raise TestError(
                    f"ShareFetch open records missing {expected_payload!r}: "
                    f"{records!r}"
                )
            if not partition["acquired_records"]:
                raise TestError(f"ShareFetch open missing acquired records: {response}")
            acquired = partition["acquired_records"][0]
            if acquired["first_offset"] > acquired["last_offset"]:
                raise TestError(f"ShareFetch open invalid acquired range: {response}")
            group_state["share_fetch_acquired"] = acquired
            group_state["share_session_epoch"] = 0
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareFetch session did not open for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_share_fetch_session_checkpoint(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 7990
    last_error = None
    next_epoch = group_state["share_session_epoch"] + 1
    while time.time() < deadline:
        try:
            response = share_fetch(
                port,
                group_state,
                next_epoch,
                correlation_id,
                topic_partitions=[],
                max_bytes=0,
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ShareFetch session epoch {next_epoch} error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if response["responses"]:
                raise TestError(
                    f"ShareFetch session checkpoint returned partitions: {response}"
                )
            group_state["share_session_epoch"] = next_epoch
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareFetch session epoch {next_epoch} did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def write_share_acknowledge_topics(topic_partitions):
    out = bytearray(write_compact_array_len(len(topic_partitions)))
    for topic in topic_partitions:
        topic_id = topic["topic_id"]
        if len(topic_id) != 16:
            raise TestError(f"invalid ShareAcknowledge topic id length {len(topic_id)}")
        out += topic_id
        partitions = topic.get("partitions", [])
        out += write_compact_array_len(len(partitions))
        for partition in partitions:
            out += struct.pack(">i", partition.get("partition_index", 0))
            acknowledgement_batches = partition.get("acknowledgement_batches", [])
            out += write_compact_array_len(len(acknowledgement_batches))
            for batch in acknowledgement_batches:
                out += struct.pack(">qq", batch["first_offset"], batch["last_offset"])
                acknowledge_types = batch.get("acknowledge_types", [])
                out += write_compact_array_len(len(acknowledge_types))
                for acknowledge_type in acknowledge_types:
                    out += struct.pack(">b", acknowledge_type)
                out += b"\x00"  # acknowledgement batch tagged fields
            out += b"\x00"  # partition tagged fields
        out += b"\x00"  # topic tagged fields
    return bytes(out)


def parse_share_acknowledge_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    error_code, pos = read_i16(response, pos)
    error_message, pos = read_compact_string(response, pos)
    response_count, pos = read_compact_array_len(response, pos)
    responses = []
    for _ in range(response_count):
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading ShareAcknowledge topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            partition_error, pos = read_i16(response, pos)
            partition_message, pos = read_compact_string(response, pos)
            leader_id, pos = read_i32(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            pos = skip_tags(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": partition_error,
                    "error_message": partition_message,
                    "leader_id": leader_id,
                    "leader_epoch": leader_epoch,
                }
            )
        pos = skip_tags(response, pos)
        responses.append({"topic_id": topic_id, "partitions": partitions})
    node_endpoint_count, pos = read_compact_array_len(response, pos)
    node_endpoints = []
    for _ in range(node_endpoint_count):
        node_id, pos = read_i32(response, pos)
        host, pos = read_compact_string(response, pos)
        endpoint_port, pos = read_i32(response, pos)
        rack, pos = read_compact_string(response, pos)
        pos = skip_tags(response, pos)
        node_endpoints.append(
            {
                "node_id": node_id,
                "host": host,
                "port": endpoint_port,
                "rack": rack,
            }
        )
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ShareAcknowledge response trailing bytes: {len(response) - pos}"
        )
    return {
        "error_code": error_code,
        "error_message": error_message,
        "responses": responses,
        "node_endpoints": node_endpoints,
    }


def share_acknowledge(
    port,
    group_state,
    share_session_epoch,
    correlation_id,
    topic_partitions=None,
):
    if topic_partitions is None:
        topic_partitions = []
    body = write_compact_string(group_state["group_id"])
    body += write_compact_string(group_state["member_id"])
    body += struct.pack(">i", share_session_epoch)
    body += write_share_acknowledge_topics(topic_partitions)
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 79, 0, correlation_id, body)
    return parse_share_acknowledge_response(response, correlation_id)


def wait_for_share_acknowledge_acquired(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8000
    last_error = None
    acquired = group_state.get("share_fetch_acquired")
    if acquired is None:
        raise TestError("ShareAcknowledge requested before ShareFetch acquisition")
    ack_count = acquired["last_offset"] - acquired["first_offset"] + 1
    if ack_count <= 0 or ack_count > 1000:
        raise TestError(f"ShareAcknowledge acquired range is not sane: {acquired}")
    next_epoch = group_state["share_session_epoch"] + 1
    topic_partitions = [
        {
            "topic_id": group_state["topic_id"],
            "partitions": [
                {
                    "partition_index": 0,
                    "acknowledgement_batches": [
                        {
                            "first_offset": acquired["first_offset"],
                            "last_offset": acquired["last_offset"],
                            "acknowledge_types": [1] * ack_count,
                        }
                    ],
                }
            ],
        }
    ]
    while time.time() < deadline:
        try:
            response = share_acknowledge(
                port,
                group_state,
                next_epoch,
                correlation_id,
                topic_partitions=topic_partitions,
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ShareAcknowledge acquired range error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if len(response["responses"]) != 1:
                raise TestError(
                    f"ShareAcknowledge acquired topic count mismatch: {response}"
                )
            topic = response["responses"][0]
            if topic["topic_id"] != group_state["topic_id"]:
                raise TestError(f"ShareAcknowledge acquired topic id mismatch: {response}")
            if len(topic["partitions"]) != 1:
                raise TestError(
                    f"ShareAcknowledge acquired partition count mismatch: {response}"
                )
            partition = topic["partitions"][0]
            if partition["partition_index"] != 0 or partition["error_code"] != 0:
                raise TestError(f"ShareAcknowledge acquired partition error: {response}")
            group_state["share_acknowledged_acquired"] = dict(acquired)
            group_state["share_session_epoch"] = next_epoch
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareAcknowledge acquired range did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def wait_for_share_acknowledge_session_checkpoint(port, group_state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8010
    last_error = None
    next_epoch = group_state["share_session_epoch"] + 1
    while time.time() < deadline:
        try:
            response = share_acknowledge(
                port,
                group_state,
                next_epoch,
                correlation_id,
                topic_partitions=[],
            )
            if response["error_code"] != 0:
                raise TestError(
                    f"ShareAcknowledge session epoch {next_epoch} error_code="
                    f"{response['error_code']} message={response['error_message']!r}"
                )
            if response["responses"]:
                raise TestError(
                    f"ShareAcknowledge session checkpoint returned partitions: "
                    f"{response}"
                )
            group_state["share_session_epoch"] = next_epoch
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ShareAcknowledge session epoch {next_epoch} did not recover for "
        f"{group_state['group_id']!r}: {last_error}"
    )


def write_share_state_topics(topic_partitions, partition_writer):
    out = bytearray(write_compact_array_len(len(topic_partitions)))
    for topic in topic_partitions:
        topic_id = topic["topic_id"]
        if len(topic_id) != 16:
            raise TestError(f"invalid share-state topic id length {len(topic_id)}")
        out += topic_id
        partitions = topic.get("partitions", [])
        out += write_compact_array_len(len(partitions))
        for partition in partitions:
            out += partition_writer(partition)
            out += b"\x00"  # partition tagged fields
        out += b"\x00"  # topic tagged fields
    return bytes(out)


def write_initialize_share_group_state_topics(topic_partitions):
    def write_partition(partition):
        return struct.pack(
            ">iiq",
            partition.get("partition", 0),
            partition["state_epoch"],
            partition["start_offset"],
        )

    return write_share_state_topics(topic_partitions, write_partition)


def write_read_share_group_state_topics(topic_partitions):
    def write_partition(partition):
        return struct.pack(
            ">ii",
            partition.get("partition", 0),
            partition.get("leader_epoch", 0),
        )

    return write_share_state_topics(topic_partitions, write_partition)


def write_write_share_group_state_topics(topic_partitions):
    def write_partition(partition):
        out = bytearray(
            struct.pack(
                ">iiiq",
                partition.get("partition", 0),
                partition["state_epoch"],
                partition.get("leader_epoch", 0),
                partition["start_offset"],
            )
        )
        state_batches = partition.get("state_batches", [])
        out += write_compact_array_len(len(state_batches))
        for batch in state_batches:
            out += struct.pack(
                ">qqbh",
                batch["first_offset"],
                batch["last_offset"],
                batch["delivery_state"],
                batch["delivery_count"],
            )
            out += b"\x00"  # state batch tagged fields
        return bytes(out)

    return write_share_state_topics(topic_partitions, write_partition)


def write_delete_share_group_state_topics(topic_partitions):
    def write_partition(partition):
        return struct.pack(">i", partition.get("partition", 0))

    return write_share_state_topics(topic_partitions, write_partition)


def parse_share_state_result_response(response, correlation_id, response_name):
    pos = parse_flexible_response_header(response, correlation_id)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        if pos + 16 > len(response):
            raise TestError(
                f"buffer underflow while reading {response_name} topic id"
            )
        topic_id = response[pos : pos + 16]
        pos += 16
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition, pos = read_i32(response, pos)
            error_code, pos = read_i16(response, pos)
            error_message, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition": partition,
                    "error_code": error_code,
                    "error_message": error_message,
                }
            )
        pos = skip_tags(response, pos)
        results.append({"topic_id": topic_id, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"{response_name} response trailing bytes: {len(response) - pos}"
        )
    return results


def parse_read_share_group_state_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        if pos + 16 > len(response):
            raise TestError("buffer underflow while reading ReadShareGroupState topic id")
        topic_id = response[pos : pos + 16]
        pos += 16
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition, pos = read_i32(response, pos)
            error_code, pos = read_i16(response, pos)
            error_message, pos = read_compact_string(response, pos)
            state_epoch, pos = read_i32(response, pos)
            start_offset, pos = read_i64(response, pos)
            batch_count, pos = read_compact_array_len(response, pos)
            state_batches = []
            for _ in range(batch_count):
                first_offset, pos = read_i64(response, pos)
                last_offset, pos = read_i64(response, pos)
                delivery_state, pos = read_i8(response, pos)
                delivery_count, pos = read_i16(response, pos)
                pos = skip_tags(response, pos)
                state_batches.append(
                    {
                        "first_offset": first_offset,
                        "last_offset": last_offset,
                        "delivery_state": delivery_state,
                        "delivery_count": delivery_count,
                    }
                )
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition": partition,
                    "error_code": error_code,
                    "error_message": error_message,
                    "state_epoch": state_epoch,
                    "start_offset": start_offset,
                    "state_batches": state_batches,
                }
            )
        pos = skip_tags(response, pos)
        results.append({"topic_id": topic_id, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ReadShareGroupState response trailing bytes: {len(response) - pos}"
        )
    return results


def parse_read_share_group_state_summary_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    result_count, pos = read_compact_array_len(response, pos)
    results = []
    for _ in range(result_count):
        if pos + 16 > len(response):
            raise TestError(
                "buffer underflow while reading ReadShareGroupStateSummary topic id"
            )
        topic_id = response[pos : pos + 16]
        pos += 16
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition, pos = read_i32(response, pos)
            error_code, pos = read_i16(response, pos)
            error_message, pos = read_compact_string(response, pos)
            state_epoch, pos = read_i32(response, pos)
            start_offset, pos = read_i64(response, pos)
            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition": partition,
                    "error_code": error_code,
                    "error_message": error_message,
                    "state_epoch": state_epoch,
                    "start_offset": start_offset,
                }
            )
        pos = skip_tags(response, pos)
        results.append({"topic_id": topic_id, "partitions": partitions})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"ReadShareGroupStateSummary response trailing bytes: "
            f"{len(response) - pos}"
        )
    return results


def share_state_single_topic(state, partition):
    return [{"topic_id": state["topic_id"], "partitions": [partition]}]


def initialize_share_group_state(port, state, correlation_id):
    body = write_compact_string(state["group_id"])
    body += write_initialize_share_group_state_topics(
        share_state_single_topic(
            state,
            {
                "partition": state["partition"],
                "state_epoch": state["state_epoch"],
                "start_offset": state["start_offset"],
            },
        )
    )
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 83, 0, correlation_id, body)
    return parse_share_state_result_response(
        response, correlation_id, "InitializeShareGroupState"
    )


def read_share_group_state(port, state, correlation_id):
    body = write_compact_string(state["group_id"])
    body += write_read_share_group_state_topics(
        share_state_single_topic(
            state,
            {"partition": state["partition"], "leader_epoch": 0},
        )
    )
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 84, 0, correlation_id, body)
    return parse_read_share_group_state_response(response, correlation_id)


def write_share_group_state(port, state, correlation_id):
    body = write_compact_string(state["group_id"])
    body += write_write_share_group_state_topics(
        share_state_single_topic(
            state,
            {
                "partition": state["partition"],
                "state_epoch": state["state_epoch"],
                "leader_epoch": 0,
                "start_offset": state["start_offset"],
                "state_batches": state.get("state_batches", []),
            },
        )
    )
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 85, 0, correlation_id, body)
    return parse_share_state_result_response(
        response, correlation_id, "WriteShareGroupState"
    )


def delete_share_group_state(port, state, correlation_id):
    body = write_compact_string(state["group_id"])
    body += write_delete_share_group_state_topics(
        share_state_single_topic(state, {"partition": state["partition"]})
    )
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 86, 0, correlation_id, body)
    return parse_share_state_result_response(
        response, correlation_id, "DeleteShareGroupState"
    )


def read_share_group_state_summary(port, state, correlation_id):
    body = write_compact_string(state["group_id"])
    body += write_read_share_group_state_topics(
        share_state_single_topic(
            state,
            {"partition": state["partition"], "leader_epoch": 0},
        )
    )
    body += b"\x00"  # request tagged fields
    response = flexible_kafka_request(port, 87, 0, correlation_id, body)
    return parse_read_share_group_state_summary_response(response, correlation_id)


def assert_share_state_partition_result(results, state, response_name):
    if len(results) != 1:
        raise TestError(f"{response_name} topic count mismatch: {results}")
    topic = results[0]
    if topic["topic_id"] != state["topic_id"]:
        raise TestError(f"{response_name} topic id mismatch: {results}")
    if len(topic["partitions"]) != 1:
        raise TestError(f"{response_name} partition count mismatch: {results}")
    partition = topic["partitions"][0]
    if partition["partition"] != state["partition"]:
        raise TestError(f"{response_name} partition mismatch: {results}")
    if partition["error_code"] != 0:
        raise TestError(f"{response_name} partition error: {results}")
    return partition


def wait_for_share_state_initialized(port, state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8100
    last_error = None
    while time.time() < deadline:
        try:
            results = initialize_share_group_state(port, state, correlation_id)
            assert_share_state_partition_result(
                results, state, "InitializeShareGroupState"
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"InitializeShareGroupState did not recover for "
        f"{state['group_id']!r}: {last_error}"
    )


def wait_for_share_state_written(port, state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8110
    last_error = None
    while time.time() < deadline:
        try:
            results = write_share_group_state(port, state, correlation_id)
            assert_share_state_partition_result(
                results, state, "WriteShareGroupState"
            )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"WriteShareGroupState did not recover for "
        f"{state['group_id']!r}: {last_error}"
    )


def wait_for_share_state_read_checkpoint(port, state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8120
    last_error = None
    while time.time() < deadline:
        try:
            results = read_share_group_state(port, state, correlation_id)
            partition = assert_share_state_partition_result(
                results, state, "ReadShareGroupState"
            )
            if partition["state_epoch"] != state["state_epoch"]:
                raise TestError(f"ReadShareGroupState epoch mismatch: {results}")
            if partition["start_offset"] != state["start_offset"]:
                raise TestError(f"ReadShareGroupState start offset mismatch: {results}")
            if partition["state_batches"] != state.get("state_batches", []):
                raise TestError(f"ReadShareGroupState batches mismatch: {results}")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ReadShareGroupState did not recover for "
        f"{state['group_id']!r}: {last_error}"
    )


def wait_for_share_state_summary_checkpoint(port, state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8130
    last_error = None
    while time.time() < deadline:
        try:
            results = read_share_group_state_summary(port, state, correlation_id)
            partition = assert_share_state_partition_result(
                results, state, "ReadShareGroupStateSummary"
            )
            if partition["state_epoch"] != state["state_epoch"]:
                raise TestError(
                    f"ReadShareGroupStateSummary epoch mismatch: {results}"
                )
            if partition["start_offset"] != state["start_offset"]:
                raise TestError(
                    f"ReadShareGroupStateSummary start offset mismatch: {results}"
                )
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"ReadShareGroupStateSummary did not recover for "
        f"{state['group_id']!r}: {last_error}"
    )


def wait_for_share_state_deleted(port, state, timeout=30):
    deadline = time.time() + timeout
    correlation_id = 8140
    last_error = None
    while time.time() < deadline:
        try:
            results = delete_share_group_state(port, state, correlation_id)
            assert_share_state_partition_result(results, state, "DeleteShareGroupState")
            return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"DeleteShareGroupState did not recover for "
        f"{state['group_id']!r}: {last_error}"
    )


def wait_for_share_state_checkpoint(port, state):
    wait_for_share_state_read_checkpoint(port, state)
    wait_for_share_state_summary_checkpoint(port, state)


def wait_for_share_state_deleted_checkpoint(port, state, timeout=30):
    deleted_state = dict(state)
    deleted_state["state_epoch"] = 0
    deleted_state["start_offset"] = -1
    deleted_state["state_batches"] = []
    wait_for_share_state_read_checkpoint(port, deleted_state, timeout=timeout)
    wait_for_share_state_summary_checkpoint(port, deleted_state, timeout=timeout)


def wait_for_share_state_live_probe(port, group_id, topic_id, first_offset):
    initialized_state = {
        "group_id": group_id,
        "topic_id": topic_id,
        "partition": 0,
        "state_epoch": 1,
        "start_offset": first_offset,
        "state_batches": [],
    }
    written_state = {
        "group_id": group_id,
        "topic_id": topic_id,
        "partition": 0,
        "state_epoch": 2,
        "start_offset": first_offset + 1,
        "state_batches": [
            {
                "first_offset": first_offset,
                "last_offset": first_offset,
                "delivery_state": 2,
                "delivery_count": 1,
            }
        ],
    }
    wait_for_share_state_initialized(port, initialized_state)
    wait_for_share_state_written(port, written_state)
    wait_for_share_state_checkpoint(port, written_state)
    return written_state


def wait_for_deleted_share_state_live_probe(port, group_id, topic_id, first_offset):
    state = {
        "group_id": group_id,
        "topic_id": topic_id,
        "partition": 0,
        "state_epoch": 1,
        "start_offset": first_offset,
        "state_batches": [],
    }
    wait_for_share_state_initialized(port, state)
    wait_for_share_state_deleted(port, state)
    wait_for_share_state_deleted_checkpoint(port, state)
    return state


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


def parse_describe_quorum_response(response, correlation_id, api_version):
    pos = 0
    response_correlation, pos = read_i32(response, pos)
    if response_correlation != correlation_id:
        raise TestError(f"DescribeQuorum correlation mismatch: {response_correlation}")
    pos = skip_tags(response, pos)
    top_error, pos = read_i16(response, pos)
    top_error_message = None
    if api_version >= 2:
        top_error_message, pos = read_compact_string(response, pos)
    if top_error != 0:
        raise TestError(
            f"DescribeQuorum top-level error_code={top_error} "
            f"message={top_error_message!r}"
        )

    topics_len, pos = read_compact_array_len(response, pos)
    if topics_len == 0:
        raise TestError("DescribeQuorum returned no topics")
    topics = []
    for _ in range(topics_len):
        topic_name, pos = read_compact_string(response, pos)
        partitions_len, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partitions_len):
            partition_index, pos = read_i32(response, pos)
            partition_error, pos = read_i16(response, pos)
            partition_error_message = None
            if api_version >= 2:
                partition_error_message, pos = read_compact_string(response, pos)
            leader_id, pos = read_i32(response, pos)
            leader_epoch, pos = read_i32(response, pos)
            high_watermark, pos = read_i64(response, pos)

            voters_len, pos = read_compact_array_len(response, pos)
            voters = []
            for _ in range(voters_len):
                replica_id, pos = read_i32(response, pos)
                replica_directory_id = None
                if api_version >= 2:
                    replica_directory_id, pos = read_uuid(response, pos)
                log_end_offset, pos = read_i64(response, pos)
                last_fetch_timestamp = -1
                last_caught_up_timestamp = -1
                if api_version >= 1:
                    last_fetch_timestamp, pos = read_i64(response, pos)
                    last_caught_up_timestamp, pos = read_i64(response, pos)
                pos = skip_tags(response, pos)
                voters.append(
                    {
                        "replica_id": replica_id,
                        "replica_directory_id": replica_directory_id,
                        "log_end_offset": log_end_offset,
                        "last_fetch_timestamp": last_fetch_timestamp,
                        "last_caught_up_timestamp": last_caught_up_timestamp,
                    }
                )

            observers_len, pos = read_compact_array_len(response, pos)
            observers = []
            for _ in range(observers_len):
                replica_id, pos = read_i32(response, pos)
                replica_directory_id = None
                if api_version >= 2:
                    replica_directory_id, pos = read_uuid(response, pos)
                log_end_offset, pos = read_i64(response, pos)
                last_fetch_timestamp = -1
                last_caught_up_timestamp = -1
                if api_version >= 1:
                    last_fetch_timestamp, pos = read_i64(response, pos)
                    last_caught_up_timestamp, pos = read_i64(response, pos)
                pos = skip_tags(response, pos)
                observers.append(
                    {
                        "replica_id": replica_id,
                        "replica_directory_id": replica_directory_id,
                        "log_end_offset": log_end_offset,
                        "last_fetch_timestamp": last_fetch_timestamp,
                        "last_caught_up_timestamp": last_caught_up_timestamp,
                    }
                )

            pos = skip_tags(response, pos)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": partition_error,
                    "error_message": partition_error_message,
                    "leader_id": leader_id,
                    "leader_epoch": leader_epoch,
                    "high_watermark": high_watermark,
                    "current_voters": voters,
                    "observers": observers,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})

    nodes = []
    if api_version >= 2:
        nodes_len, pos = read_compact_array_len(response, pos)
        for _ in range(nodes_len):
            node_id, pos = read_i32(response, pos)
            listeners_len, pos = read_compact_array_len(response, pos)
            listeners = []
            for _ in range(listeners_len):
                name, pos = read_compact_string(response, pos)
                host, pos = read_compact_string(response, pos)
                listener_port, pos = read_u16(response, pos)
                pos = skip_tags(response, pos)
                listeners.append(
                    {"name": name, "host": host, "port": listener_port}
                )
            pos = skip_tags(response, pos)
            nodes.append({"node_id": node_id, "listeners": listeners})
    pos = skip_tags(response, pos)
    if pos != len(response):
        raise TestError(
            f"DescribeQuorum response trailing bytes: {len(response) - pos}"
        )

    if not topics or not topics[0]["partitions"]:
        raise TestError("DescribeQuorum returned no partitions")
    first_partition = topics[0]["partitions"][0]

    return {
        "top_error_code": top_error,
        "top_error_message": top_error_message,
        "error_code": first_partition["error_code"],
        "error_message": first_partition["error_message"],
        "topics": topics,
        "nodes": nodes,
        "partition_index": first_partition["partition_index"],
        "partition_error_code": first_partition["error_code"],
        "leader_id": first_partition["leader_id"],
        "leader_epoch": first_partition["leader_epoch"],
        "high_watermark": first_partition["high_watermark"],
        "voters": [
            voter["replica_id"] for voter in first_partition["current_voters"]
        ],
    }


def describe_quorum(port, correlation_id):
    response = controller_request(port, 55, 0, correlation_id, describe_quorum_body())
    return parse_describe_quorum_response(response, correlation_id, 0)


def describe_quorum_v2(port, correlation_id):
    response = controller_request(port, 55, 2, correlation_id, describe_quorum_body())
    return parse_describe_quorum_response(response, correlation_id, 2)


def wait_for_describe_quorum_v2_checkpoint(
    port,
    expected_ports,
    state,
    label,
    timeout=30,
):
    expected_node_ids = sorted(expected_ports)
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9140)
    last_error = None
    while time.time() < deadline:
        try:
            quorum = describe_quorum_v2(port, correlation_id)
            if quorum["partition_error_code"] != ERROR_NONE:
                raise TestError(
                    f"DescribeQuorum v2 partition error during {label}: {quorum}"
                )
            if sorted(quorum["voters"]) != expected_node_ids:
                raise TestError(
                    f"DescribeQuorum v2 voter set changed during {label}: "
                    f"{quorum['voters']}"
                )

            node_ports = {}
            for node in quorum["nodes"]:
                controller_listeners = [
                    listener
                    for listener in node["listeners"]
                    if listener["name"] == "CONTROLLER"
                ]
                if len(controller_listeners) != 1:
                    raise TestError(
                        f"DescribeQuorum v2 node {node['node_id']} listeners "
                        f"unexpected during {label}: {node['listeners']}"
                    )
                listener = controller_listeners[0]
                if listener["host"] != "127.0.0.1":
                    raise TestError(
                        f"DescribeQuorum v2 node {node['node_id']} host "
                        f"unexpected during {label}: {listener}"
                    )
                node_ports[node["node_id"]] = listener["port"]
            if node_ports != expected_ports:
                raise TestError(
                    f"DescribeQuorum v2 endpoints changed during {label}: "
                    f"expected={expected_ports} actual={node_ports}"
                )

            state["correlation_id"] = correlation_id + 1
            return quorum
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"DescribeQuorum v2 did not recover during {label}: {last_error}")


def fetch_snapshot_v1_body(end_offset, epoch, position):
    body = bytearray()
    body += struct.pack(">ii", 100, 128)  # replica_id, max_bytes
    body += write_compact_array_len(1)
    body += write_compact_string("__cluster_metadata")
    body += write_compact_array_len(1)
    body += struct.pack(">iiqi", 0, -1, end_offset, epoch)
    body += b"\x00"  # snapshot_id tagged fields
    body += struct.pack(">q", position)
    body += b"\x00"  # partition tagged fields
    body += b"\x00"  # topic tagged fields
    body += b"\x00"  # request tagged fields
    return bytes(body)


def parse_current_leader_tag(data):
    pos = 0
    leader_id, pos = read_i32(data, pos)
    leader_epoch, pos = read_i32(data, pos)
    pos = skip_tags(data, pos)
    if pos != len(data):
        raise TestError(
            f"FetchSnapshot current_leader tag trailing bytes: {len(data) - pos}"
        )
    return {"leader_id": leader_id, "leader_epoch": leader_epoch}


def parse_fetch_snapshot_node_endpoints_tag(data):
    pos = 0
    endpoint_count, pos = read_compact_array_len(data, pos)
    endpoints = []
    for _ in range(endpoint_count):
        node_id, pos = read_i32(data, pos)
        host, pos = read_compact_string(data, pos)
        endpoint_port, pos = read_u16(data, pos)
        pos = skip_tags(data, pos)
        endpoints.append({"node_id": node_id, "host": host, "port": endpoint_port})
    if pos != len(data):
        raise TestError(
            f"FetchSnapshot node_endpoints tag trailing bytes: {len(data) - pos}"
        )
    return endpoints


def parse_fetch_snapshot_response(response, correlation_id):
    pos = parse_flexible_response_header(response, correlation_id)
    throttle_time_ms, pos = read_i32(response, pos)
    error_code, pos = read_i16(response, pos)
    topic_count, pos = read_compact_array_len(response, pos)
    topics = []
    for _ in range(topic_count):
        topic_name, pos = read_compact_string(response, pos)
        partition_count, pos = read_compact_array_len(response, pos)
        partitions = []
        for _ in range(partition_count):
            partition_index, pos = read_i32(response, pos)
            partition_error, pos = read_i16(response, pos)
            snapshot_end_offset, pos = read_i64(response, pos)
            snapshot_epoch, pos = read_i32(response, pos)
            pos = skip_tags(response, pos)
            size, pos = read_i64(response, pos)
            position, pos = read_i64(response, pos)
            records, pos = read_compact_bytes(response, pos)
            current_leader = None
            fields, pos = read_tagged_fields(response, pos)
            for tag, data in fields:
                if tag == 0:
                    current_leader = parse_current_leader_tag(data)
            partitions.append(
                {
                    "partition_index": partition_index,
                    "error_code": partition_error,
                    "snapshot_end_offset": snapshot_end_offset,
                    "snapshot_epoch": snapshot_epoch,
                    "size": size,
                    "position": position,
                    "records": records,
                    "current_leader": current_leader,
                }
            )
        pos = skip_tags(response, pos)
        topics.append({"name": topic_name, "partitions": partitions})

    node_endpoints = []
    fields, pos = read_tagged_fields(response, pos)
    for tag, data in fields:
        if tag == 0:
            node_endpoints = parse_fetch_snapshot_node_endpoints_tag(data)
    if pos != len(response):
        raise TestError(
            f"FetchSnapshot response trailing bytes: {len(response) - pos}"
        )
    return {
        "throttle_time_ms": throttle_time_ms,
        "error_code": error_code,
        "topics": topics,
        "node_endpoints": node_endpoints,
    }


def fetch_snapshot_v1(port, end_offset, epoch, position, correlation_id):
    response = flexible_kafka_request(
        port,
        59,
        1,
        correlation_id,
        fetch_snapshot_v1_body(end_offset, epoch, position),
    )
    return parse_fetch_snapshot_response(response, correlation_id)


def wait_for_fetch_snapshot_checkpoint(
    port,
    expected_leader_id,
    expected_leader_port,
    state,
    label,
    timeout=30,
):
    deadline = time.time() + timeout
    correlation_id = state.get("correlation_id", 9240)
    snapshot_end_offset = 987654321
    snapshot_epoch = 77
    snapshot_position = 12
    last_error = None
    while time.time() < deadline:
        try:
            response = fetch_snapshot_v1(
                port,
                snapshot_end_offset,
                snapshot_epoch,
                snapshot_position,
                correlation_id,
            )
            if (
                response["throttle_time_ms"] != 0
                or response["error_code"] != ERROR_NONE
                or len(response["topics"]) != 1
                or response["topics"][0]["name"] != "__cluster_metadata"
                or len(response["topics"][0]["partitions"]) != 1
            ):
                raise TestError(f"FetchSnapshot v1 invalid response: {response}")
            partition = response["topics"][0]["partitions"][0]
            if (
                partition["partition_index"] != 0
                or partition["error_code"] != ERROR_SNAPSHOT_NOT_FOUND
                or partition["snapshot_end_offset"] != snapshot_end_offset
                or partition["snapshot_epoch"] != snapshot_epoch
                or partition["position"] != snapshot_position
                or partition["records"] is not None
            ):
                raise TestError(
                    f"FetchSnapshot v1 unexpected partition during {label}: "
                    f"{partition}"
                )
            current_leader = partition["current_leader"]
            if (
                current_leader is None
                or current_leader["leader_id"] != expected_leader_id
                or current_leader["leader_epoch"] < 0
            ):
                raise TestError(
                    f"FetchSnapshot v1 leader tag mismatch during {label}: "
                    f"{current_leader}"
                )
            if response["node_endpoints"] != [
                {
                    "node_id": expected_leader_id,
                    "host": "127.0.0.1",
                    "port": expected_leader_port,
                }
            ]:
                raise TestError(
                    f"FetchSnapshot v1 endpoint mismatch during {label}: "
                    f"{response['node_endpoints']}"
                )
            state["correlation_id"] = correlation_id + 1
            return response
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(f"FetchSnapshot v1 did not recover during {label}: {last_error}")


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
    return {
        "proc": proc,
        "port": BROKER_PORT,
        "log_path": log_path,
        "data_dir": data_dir,
    }


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
        controller_api_versions_state = {"correlation_id": 9340}
        wait_for_controller_api_versions_checkpoint(
            processes[leader_id]["port"],
            controller_api_versions_state,
            "initial leader",
        )
        controller_unsupported_state = {"correlation_id": 9740}
        wait_for_controller_unsupported_checkpoint(
            processes[leader_id]["port"],
            controller_unsupported_state,
            "initial leader",
        )
        describe_quorum_state = {"correlation_id": 9140}
        wait_for_describe_quorum_v2_checkpoint(
            processes[leader_id]["port"],
            ports,
            describe_quorum_state,
            "initial leader",
        )
        fetch_snapshot_state = {"correlation_id": 9240}
        wait_for_fetch_snapshot_checkpoint(
            processes[leader_id]["port"],
            leader_id,
            ports[leader_id],
            fetch_snapshot_state,
            "initial leader",
        )
        dynamic_voter_state = {"correlation_id": 9440}
        wait_for_dynamic_raft_voter_negative_checkpoint(
            processes[leader_id]["port"],
            ports,
            dynamic_voter_state,
            "initial leader",
        )
        broker_lifecycle_state = {"correlation_id": 9540, "broker_id": 60100}
        wait_for_broker_lifecycle_negative_checkpoint(
            processes[leader_id]["port"],
            broker_lifecycle_state,
            "initial leader",
        )
        controller_registration_state = {"correlation_id": 9640}
        wait_for_controller_registration_negative_checkpoint(
            processes[leader_id]["port"],
            ports,
            controller_registration_state,
            "initial leader",
        )

        broker = start_broker(tmp, voters)
        wait_for_broker_ready(broker["proc"], broker["port"], broker["log_path"])
        producer_id_state = {"correlation_id": 9040}
        wait_for_allocate_producer_ids_checkpoint(
            processes[leader_id]["port"],
            producer_id_state,
            "initial leader",
        )
        topic = f"kraft-failover-{os.getpid()}-{int(time.time())}"
        group = f"kraft-failover-group-{os.getpid()}-{int(time.time())}"
        offset_delete_group = f"{group}-offset-delete"
        delete_groups_group = f"{group}-delete-groups"
        txn_offset_group = f"{group}-txn-offset"
        txn_topic = f"{topic}-txn"
        idempotent_topic = f"{topic}-idempotent"
        delete_records_topic = f"{topic}-delete-records"
        delete_topics_topic = f"{topic}-delete-topics"
        create_topics_topic = f"{topic}-create-topics"
        create_topics_validate_only_topic = f"{topic}-create-topics-validate-only"
        create_partitions_topic = f"{topic}-create-partitions"
        config_admin_topic = f"{topic}-config-admin"
        quota_client_id = f"{group}-quota-client"
        validate_only_quota_client_id = f"{group}-quota-validate-only"
        quota_values = {
            "producer_byte_rate": 1234.0,
            "consumer_byte_rate": 4321.0,
            "request_percentage": 12.5,
        }
        quota_ops = [
            ("producer_byte_rate", quota_values["producer_byte_rate"], False),
            ("consumer_byte_rate", quota_values["consumer_byte_rate"], False),
            ("request_percentage", quota_values["request_percentage"], False),
        ]
        scram_user = f"{group}-scram-user"
        scram_iterations = 8192
        scram_salt = bytes([0x11] * 32)
        scram_salted_password = bytes([0x22] * 32)
        telemetry_client_instance_id = bytes([0x33] * 16)
        telemetry_metrics = b"\x08\x01"
        delegation_token_owner = "kraft-failover-test"
        delegation_token_lifetime_ms = 60 * 60 * 1000
        finalized_feature_name = "metadata.version"
        finalized_feature_level = 1
        broad_allow_acl = {
            "resource_type": ACL_RESOURCE_TYPE_ANY,
            "resource_name": "*",
            "pattern_type": ACL_PATTERN_TYPE_MATCH,
            "principal": "*",
            "host": "*",
            "operation": ACL_OPERATION_ALL,
            "permission_type": ACL_PERMISSION_ALLOW,
        }
        deleted_acl = {
            "resource_type": ACL_RESOURCE_TYPE_TOPIC,
            "resource_name": f"{topic}-deleted-acl",
            "pattern_type": ACL_PATTERN_TYPE_LITERAL,
            "principal": "User:kraft-failover-deleted-acl",
            "host": "*",
            "operation": ACL_OPERATION_DESCRIBE,
            "permission_type": ACL_PERMISSION_ALLOW,
        }
        alter_configs_values = [
            ("cleanup.policy", "compact"),
            ("min.insync.replicas", "1"),
            ("segment.bytes", "131072"),
        ]
        incremental_configs_values = [
            ("cleanup.policy", 0, "compact,delete"),
            ("segment.bytes", 0, "262144"),
        ]
        final_config_values = {
            "cleanup.policy": "compact,delete",
            "min.insync.replicas": "1",
            "segment.bytes": "262144",
        }
        create_topics_configs = [
            ("cleanup.policy", "compact"),
            ("min.insync.replicas", "1"),
            ("segment.bytes", "393216"),
            ("compression.type", "lz4"),
        ]
        create_topics_config_values = {
            "cleanup.policy": "compact",
            "min.insync.replicas": "1",
            "segment.bytes": "393216",
            "compression.type": "lz4",
        }
        kip848_subscription_topic = f"{topic}-kip848-subscription"
        kip848_negative_group_prefix = f"{group}-kip848-negative"
        expected_payloads = []
        wait_for_topic(broker["port"], topic)
        wait_for_topic(broker["port"], txn_topic)
        wait_for_topic(broker["port"], idempotent_topic)
        wait_for_topic(broker["port"], delete_records_topic)
        wait_for_topic(broker["port"], delete_topics_topic)
        wait_for_topic(broker["port"], create_partitions_topic)
        wait_for_topic(broker["port"], config_admin_topic)
        wait_for_topic(broker["port"], kip848_subscription_topic)
        expected_payloads.append(b"r0")
        first_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        committed_offset = first_offset + 1
        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        delete_records_first_offset = wait_for_produce(
            broker["port"],
            delete_records_topic,
            b"delete-records-before-trim",
        )
        delete_records_second_offset = wait_for_produce(
            broker["port"],
            delete_records_topic,
            b"delete-records-after-trim",
        )
        if delete_records_second_offset <= delete_records_first_offset:
            raise TestError(
                f"DeleteRecords probe did not advance: "
                f"{delete_records_second_offset} <= {delete_records_first_offset}"
            )
        delete_records_low_watermark = delete_records_first_offset + 1
        delete_records_end_offset = delete_records_second_offset + 1
        wait_for_produce(
            broker["port"],
            delete_topics_topic,
            b"delete-topics-before-delete",
        )

        def wait_for_delete_records_probe():
            wait_for_delete_records_checkpoint(
                broker["port"],
                delete_records_topic,
                delete_records_low_watermark,
                delete_records_end_offset,
            )

        def wait_for_delete_topics_probe():
            wait_for_deleted_topic_checkpoint(
                broker["port"],
                delete_topics_topic,
            )

        def wait_for_create_topics_probe():
            wait_for_create_topics_checkpoint(
                broker["port"],
                create_topics_topic,
                create_topics_configs,
                create_topics_config_values,
                create_topics_validate_only_topic,
            )

        def wait_for_topic_partitions_probe():
            wait_for_describe_topic_partitions_checkpoint(
                broker["port"],
                topic,
                100,
            )

        def wait_for_create_partitions_probe():
            wait_for_create_partitions_validate_only_checkpoint(
                broker["port"],
                create_partitions_topic,
                3,
            )
            wait_for_describe_topic_partitions_count_checkpoint(
                broker["port"],
                create_partitions_topic,
                100,
                2,
            )

        def wait_for_client_quotas_probe():
            wait_for_client_quotas_checkpoint(
                broker["port"],
                quota_client_id,
                quota_values,
                validate_only_quota_client_id,
            )

        def wait_for_scram_credentials_probe():
            wait_for_user_scram_credentials_checkpoint(
                broker["port"],
                scram_user,
                scram_iterations,
            )

        def wait_for_client_telemetry_probe():
            wait_for_client_telemetry_checkpoint(
                broker["port"],
                telemetry_client_instance_id,
                telemetry_metrics,
            )

        def wait_for_delegation_token_probe():
            wait_for_delegation_token_checkpoint(
                broker["port"],
                delegation_token_owner,
                delegation_token["token_id"],
                delegation_token["hmac"],
                delegation_token_lifetime_ms,
            )

        def wait_for_finalized_features_probe():
            wait_for_finalized_features_checkpoint(
                broker["port"],
                finalized_feature_name,
                finalized_feature_level,
            )

        def wait_for_acl_admin_probe():
            wait_for_acl_admin_checkpoint(
                broker["port"],
                broad_allow_acl,
                deleted_acl,
            )

        def wait_for_config_admin_probe():
            wait_for_config_admin_checkpoint(
                broker["port"],
                config_admin_topic,
                alter_configs_values,
                incremental_configs_values,
                final_config_values,
            )

        def wait_for_cluster_visibility_probes():
            wait_for_topic_partitions_probe()
            wait_for_delete_topics_probe()
            wait_for_create_topics_probe()
            wait_for_create_partitions_probe()
            wait_for_client_quotas_probe()
            wait_for_scram_credentials_probe()
            wait_for_client_telemetry_probe()
            wait_for_delegation_token_probe()
            wait_for_finalized_features_probe()
            wait_for_acl_admin_probe()
            wait_for_config_admin_probe()
            wait_for_describe_configs_checkpoint(
                broker["port"],
                topic,
            )
            wait_for_describe_log_dirs_checkpoint(
                broker["port"],
                topic,
            )
            wait_for_alter_replica_log_dirs_checkpoint(
                broker["port"],
                topic,
                broker["data_dir"],
            )
            wait_for_assign_replicas_to_dirs_checkpoint(
                broker["port"],
                topic,
                broker["data_dir"],
                broker["log_path"],
            )
            wait_for_elect_leaders_checkpoint(
                broker["port"],
                topic,
            )
            wait_for_describe_cluster_checkpoint(
                broker["port"],
                100,
                CLUSTER_ID,
            )

        wait_for_topic_partitions_probe()
        wait_for_delete_records_probe()
        wait_for_delete_topics_seed(
            broker["port"],
            delete_topics_topic,
        )
        wait_for_create_topics_seed(
            broker["port"],
            create_topics_topic,
            create_topics_configs,
            create_topics_config_values,
        )
        wait_for_create_partitions_mutation(
            broker["port"],
            create_partitions_topic,
            2,
        )
        wait_for_alter_client_quotas_mutation(
            broker["port"],
            quota_client_id,
            quota_ops,
        )
        wait_for_alter_user_scram_credentials_upsert(
            broker["port"],
            scram_user,
            scram_salt,
            scram_salted_password,
            scram_iterations,
        )
        delegation_token = wait_for_create_delegation_token(
            broker["port"],
            delegation_token_owner,
            delegation_token_lifetime_ms,
        )
        wait_for_update_features_mutation(
            broker["port"],
            finalized_feature_name,
            finalized_feature_level,
        )
        wait_for_acl_admin_seed(
            broker["port"],
            broad_allow_acl,
            deleted_acl,
        )
        wait_for_config_admin_seed(
            broker["port"],
            config_admin_topic,
            alter_configs_values,
            incremental_configs_values,
            final_config_values,
        )
        wait_for_cluster_visibility_probes()
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
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        kip848_static_group_state = wait_for_consumer_group_heartbeat_static_join(
            broker["port"],
            f"{group}-kip848-static",
            topic,
            "instance-kip848-failover",
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        kip848_subscription_group_state = wait_for_consumer_group_heartbeat_join(
            broker["port"],
            f"{group}-kip848-subscription",
            topic,
        )
        wait_for_consumer_group_heartbeat_subscription_update(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        share_group_state = wait_for_share_group_heartbeat_join(
            broker["port"],
            f"{group}-share",
            topic,
        )
        wait_for_share_group_heartbeat_rack_update(
            broker["port"],
            share_group_state,
            "rack-share-failover",
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_open(
            broker["port"],
            share_group_state,
            expected_payloads[0],
        )
        wait_for_share_acknowledge_acquired(
            broker["port"],
            share_group_state,
        )
        share_state_probe = wait_for_share_state_live_probe(
            broker["port"],
            f"{group}-share-state",
            share_group_state["topic_id"],
            first_offset,
        )
        deleted_share_state_probe = wait_for_deleted_share_state_live_probe(
            broker["port"],
            f"{group}-share-state-deleted",
            share_group_state["topic_id"],
            first_offset,
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
        old_kip848_group_state = dict(kip848_group_state)
        wait_for_consumer_group_heartbeat_leave(
            broker["port"],
            old_kip848_group_state,
        )
        wait_for_consumer_group_heartbeat_unknown_member(
            broker["port"],
            old_kip848_group_state,
        )
        kip848_group_state = wait_for_consumer_group_heartbeat_rejoin(
            broker["port"],
            old_kip848_group_state,
            topic,
        )
        wait_for_consumer_group_heartbeat(broker["port"], kip848_group_state)
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_consumer_group_heartbeat_rack_update(
            broker["port"],
            kip848_group_state,
            "rack-kip848-failover",
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_acknowledge_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_state_checkpoint(broker["port"], share_state_probe)
        wait_for_share_state_deleted_checkpoint(
            broker["port"],
            deleted_share_state_probe,
        )
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
            wait_for_controller_api_versions_checkpoint(
                processes[leader_id]["port"],
                controller_api_versions_state,
                "network partition matrix",
            )
            wait_for_controller_unsupported_checkpoint(
                processes[leader_id]["port"],
                controller_unsupported_state,
                "network partition matrix",
            )
            wait_for_describe_quorum_v2_checkpoint(
                processes[leader_id]["port"],
                ports,
                describe_quorum_state,
                "network partition matrix",
            )
            wait_for_fetch_snapshot_checkpoint(
                processes[leader_id]["port"],
                leader_id,
                ports[leader_id],
                fetch_snapshot_state,
                "network partition matrix",
            )
            wait_for_allocate_producer_ids_checkpoint(
                processes[leader_id]["port"],
                producer_id_state,
                "network partition matrix",
            )
            wait_for_dynamic_raft_voter_negative_checkpoint(
                processes[leader_id]["port"],
                ports,
                dynamic_voter_state,
                "network partition matrix",
            )
            wait_for_broker_lifecycle_negative_checkpoint(
                processes[leader_id]["port"],
                broker_lifecycle_state,
                "network partition matrix",
            )
            wait_for_controller_registration_negative_checkpoint(
                processes[leader_id]["port"],
                ports,
                controller_registration_state,
                "network partition matrix",
            )
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_acknowledge_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_state_checkpoint(broker["port"], share_state_probe)
        wait_for_share_state_deleted_checkpoint(
            broker["port"],
            deleted_share_state_probe,
        )
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
        wait_for_controller_api_versions_checkpoint(
            processes[replacement_leader]["port"],
            controller_api_versions_state,
            "controller leader failover",
        )
        wait_for_controller_unsupported_checkpoint(
            processes[replacement_leader]["port"],
            controller_unsupported_state,
            "controller leader failover",
        )
        wait_for_describe_quorum_v2_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            describe_quorum_state,
            "controller leader failover",
        )
        wait_for_fetch_snapshot_checkpoint(
            processes[replacement_leader]["port"],
            replacement_leader,
            ports[replacement_leader],
            fetch_snapshot_state,
            "controller leader failover",
        )
        wait_for_allocate_producer_ids_checkpoint(
            processes[replacement_leader]["port"],
            producer_id_state,
            "controller leader failover",
        )
        wait_for_dynamic_raft_voter_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            dynamic_voter_state,
            "controller leader failover",
        )
        wait_for_broker_lifecycle_negative_checkpoint(
            processes[replacement_leader]["port"],
            broker_lifecycle_state,
            "controller leader failover",
        )
        wait_for_controller_registration_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            controller_registration_state,
            "controller leader failover",
        )
        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_acknowledge_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_state_checkpoint(broker["port"], share_state_probe)
        wait_for_share_state_deleted_checkpoint(
            broker["port"],
            deleted_share_state_probe,
        )
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_controller_api_versions_checkpoint(
            processes[replacement_leader]["port"],
            controller_api_versions_state,
            "old leader fresh rejoin",
        )
        wait_for_controller_unsupported_checkpoint(
            processes[replacement_leader]["port"],
            controller_unsupported_state,
            "old leader fresh rejoin",
        )
        wait_for_describe_quorum_v2_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            describe_quorum_state,
            "old leader fresh rejoin",
        )
        wait_for_fetch_snapshot_checkpoint(
            processes[replacement_leader]["port"],
            replacement_leader,
            ports[replacement_leader],
            fetch_snapshot_state,
            "old leader fresh rejoin",
        )
        wait_for_allocate_producer_ids_checkpoint(
            processes[replacement_leader]["port"],
            producer_id_state,
            "old leader fresh rejoin",
        )
        wait_for_dynamic_raft_voter_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            dynamic_voter_state,
            "old leader fresh rejoin",
        )
        wait_for_broker_lifecycle_negative_checkpoint(
            processes[replacement_leader]["port"],
            broker_lifecycle_state,
            "old leader fresh rejoin",
        )
        wait_for_controller_registration_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            controller_registration_state,
            "old leader fresh rejoin",
        )

        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_acknowledge_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_state_checkpoint(broker["port"], share_state_probe)
        wait_for_share_state_deleted_checkpoint(
            broker["port"],
            deleted_share_state_probe,
        )
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_controller_api_versions_checkpoint(
            processes[replacement_leader]["port"],
            controller_api_versions_state,
            "surviving controller restart",
        )
        wait_for_controller_unsupported_checkpoint(
            processes[replacement_leader]["port"],
            controller_unsupported_state,
            "surviving controller restart",
        )
        wait_for_describe_quorum_v2_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            describe_quorum_state,
            "surviving controller restart",
        )
        wait_for_fetch_snapshot_checkpoint(
            processes[replacement_leader]["port"],
            replacement_leader,
            ports[replacement_leader],
            fetch_snapshot_state,
            "surviving controller restart",
        )
        wait_for_allocate_producer_ids_checkpoint(
            processes[replacement_leader]["port"],
            producer_id_state,
            "surviving controller restart",
        )
        wait_for_dynamic_raft_voter_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            dynamic_voter_state,
            "surviving controller restart",
        )
        wait_for_broker_lifecycle_negative_checkpoint(
            processes[replacement_leader]["port"],
            broker_lifecycle_state,
            "surviving controller restart",
        )
        wait_for_controller_registration_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            controller_registration_state,
            "surviving controller restart",
        )

        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_acknowledge_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_state_checkpoint(broker["port"], share_state_probe)
        wait_for_share_state_deleted_checkpoint(
            broker["port"],
            deleted_share_state_probe,
        )
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
        wait_for_offset_commit(broker["port"], group, topic, committed_offset)
        broker_restart_txn = wait_for_transaction_begin(
            broker["port"],
            f"{group}-broker-restart",
            txn_topic,
        )

        stop_process(broker["proc"])
        broker = start_broker(tmp, voters)
        wait_for_broker_ready(broker["proc"], broker["port"], broker["log_path"])
        wait_for_controller_api_versions_checkpoint(
            processes[replacement_leader]["port"],
            controller_api_versions_state,
            "broker restart",
        )
        wait_for_controller_unsupported_checkpoint(
            processes[replacement_leader]["port"],
            controller_unsupported_state,
            "broker restart",
        )
        wait_for_describe_quorum_v2_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            describe_quorum_state,
            "broker restart",
        )
        wait_for_fetch_snapshot_checkpoint(
            processes[replacement_leader]["port"],
            replacement_leader,
            ports[replacement_leader],
            fetch_snapshot_state,
            "broker restart",
        )
        wait_for_allocate_producer_ids_checkpoint(
            processes[replacement_leader]["port"],
            producer_id_state,
            "broker restart",
        )
        wait_for_dynamic_raft_voter_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            dynamic_voter_state,
            "broker restart",
        )
        wait_for_broker_lifecycle_negative_checkpoint(
            processes[replacement_leader]["port"],
            broker_lifecycle_state,
            "broker restart",
        )
        wait_for_controller_registration_negative_checkpoint(
            processes[replacement_leader]["port"],
            ports,
            controller_registration_state,
            "broker restart",
        )
        wait_for_payloads(broker["port"], topic, expected_payloads)
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
        wait_for_consumer_group_heartbeat_owned_assignment(
            broker["port"],
            kip848_group_state,
        )
        wait_for_kip848_consumer_group_description(
            broker["port"],
            kip848_group_state,
            topic,
        )
        wait_for_kip848_static_member_checkpoint(
            broker["port"],
            kip848_static_group_state,
            topic,
        )
        wait_for_kip848_subscription_checkpoint(
            broker["port"],
            kip848_subscription_group_state,
            kip848_subscription_topic,
        )
        wait_for_kip848_negative_checkpoint(
            broker["port"],
            kip848_negative_group_prefix,
            topic,
        )
        wait_for_share_group_checkpoint(
            broker["port"],
            share_group_state,
            topic,
        )
        wait_for_share_fetch_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_acknowledge_session_checkpoint(
            broker["port"],
            share_group_state,
        )
        wait_for_share_state_checkpoint(broker["port"], share_state_probe)
        wait_for_share_state_deleted_checkpoint(
            broker["port"],
            deleted_share_state_probe,
        )
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            0,
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
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            idempotent_identity,
            1,
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
        wait_for_describe_producers_checkpoint(
            broker["port"],
            idempotent_topic,
            bumped_identity,
            0,
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
        wait_for_log_position_checkpoint(
            broker["port"],
            topic,
            first_offset,
            expected_topic_end_offset(first_offset, expected_payloads),
        )
        wait_for_delete_records_probe()
        wait_for_cluster_visibility_probes()
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
            f"allocate_producer_ids_checked=true, "
            f"describe_quorum_v2_checked=true, "
            f"fetch_snapshot_v1_checked=true, "
            f"controller_api_versions_checked=true, "
            f"controller_unsupported_checked=true, "
            f"dynamic_raft_voter_negative_checked=true, "
            f"broker_lifecycle_negative_checked=true, "
            f"controller_registration_negative_checked=true, "
            f"committed_offset={committed_offset}, "
            f"transactions_checked=5, "
            f"transaction_introspection_checked=true, "
            f"transaction_abort_checked=true, "
            f"txn_offset_commit_checked=true, "
            f"offset_fetch_v8_grouped_checked=true, "
            f"log_position_apis_checked=true, "
            f"delete_records_checked=true, "
            f"delete_topics_checked=true, "
            f"create_topics_checked=true, "
            f"create_partitions_checked=true, "
            f"client_quotas_checked=true, "
            f"scram_credentials_checked=true, "
            f"client_telemetry_checked=true, "
            f"delegation_tokens_checked=true, "
            f"finalized_features_checked=true, "
            f"acl_admin_checked=true, "
            f"config_admin_checked=true, "
            f"describe_topic_partitions_checked=true, "
            f"describe_configs_checked=true, "
            f"describe_log_dirs_checked=true, "
            f"alter_replica_log_dirs_checked=true, "
            f"assign_replicas_to_dirs_checked=true, "
            f"elect_leaders_checked=true, "
            f"describe_cluster_checked=true, "
            f"idempotent_producer_fencing=true, "
            f"describe_producers_checked=true, "
            f"delete_groups_checked=true, "
            f"classic_group_heartbeats=true, "
            f"group_describe_checked=true, "
            f"consumer_group_describe_checked=true, "
            f"list_groups_checked=true, "
            f"find_coordinator_checked=true, "
            f"share_group_heartbeat_checked=true, "
            f"share_group_describe_checked=true, "
            f"share_fetch_session_checked=true, "
            f"share_acknowledge_checked=true, "
            f"share_state_apis_checked=true, "
            f"consumer_group_heartbeat_checked=true, "
            f"kip848_describe_checked=true, "
            f"kip848_rejoin_checked=true, "
            f"kip848_rack_checked=true, "
            f"kip848_owned_assignment_checked=true, "
            f"kip848_subscription_update_checked=true, "
            f"kip848_negative_join_checked=true, "
            f"kip848_static_rejoin_checked=true, "
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
        if wyhash_hash(2, b"abc") != 0x32DD92E4B2915153:
            raise TestError("wyhash test vector failed")
        directory_id = derive_replica_directory_id("/tmp/zmq-self-test-dir")
        if len(directory_id) != 16 or directory_id[6] >> 4 != 4:
            raise TestError(f"replica directory id derivation failed: {directory_id!r}")
        if directory_id[8] & 0xC0 != 0x80:
            raise TestError(f"replica directory variant derivation failed: {directory_id!r}")

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

        delete_topics_fixture = struct.pack(">i", 146)
        delete_topics_fixture += b"\x00"  # response header tagged fields
        delete_topics_fixture += struct.pack(">i", 0)
        delete_topics_fixture += write_compact_array_len(1)
        delete_topics_fixture += write_compact_string("delete-topics-self-test")
        delete_topics_fixture += bytes(range(16))
        delete_topics_fixture += struct.pack(">h", ERROR_NONE)
        delete_topics_fixture += write_compact_string(None)
        delete_topics_fixture += b"\x00"  # topic tagged fields
        delete_topics_fixture += b"\x00"  # response tagged fields
        deleted_topics = parse_delete_topics_response(delete_topics_fixture, 146)
        require_delete_topics_result(
            deleted_topics,
            "delete-topics-self-test",
            ERROR_NONE,
            "DeleteTopics fixture",
        )

        create_topics_fixture = struct.pack(">i", 147)
        create_topics_fixture += b"\x00"  # response header tagged fields
        create_topics_fixture += struct.pack(">i", 0)
        create_topics_fixture += write_compact_array_len(1)
        create_topics_fixture += write_compact_string("create-topics-self-test")
        create_topics_fixture += bytes(range(16))
        create_topics_fixture += struct.pack(">h", ERROR_NONE)
        create_topics_fixture += write_compact_string(None)
        create_topics_fixture += struct.pack(">ih", 1, 1)
        create_topics_fixture += write_compact_array_len(1)
        create_topics_fixture += write_compact_string("cleanup.policy")
        create_topics_fixture += write_compact_string("compact")
        create_topics_fixture += b"\x00"  # read_only=false
        create_topics_fixture += struct.pack(">b", 5)
        create_topics_fixture += b"\x00"  # is_sensitive=false
        create_topics_fixture += b"\x00"  # config tagged fields
        create_topics_fixture += b"\x00"  # topic tagged fields
        create_topics_fixture += b"\x00"  # response tagged fields
        created_topics = parse_create_topics_response(create_topics_fixture, 147)
        require_create_topics_result(
            created_topics,
            "create-topics-self-test",
            1,
            1,
            ERROR_NONE,
            "CreateTopics fixture",
        )
        if created_topics["topics"][0]["configs"][0]["value"] != "compact":
            raise TestError(f"CreateTopics fixture parser failed: {created_topics}")

        allocate_pids_fixture = struct.pack(">i", 148)
        allocate_pids_fixture += b"\x00"  # response header tagged fields
        allocate_pids_fixture += struct.pack(">ihqi", 0, ERROR_NONE, 5000, 1000)
        allocate_pids_fixture += b"\x00"  # response tagged fields
        allocated_pids = parse_allocate_producer_ids_response(
            allocate_pids_fixture,
            148,
        )
        if (
            allocated_pids["producer_id_start"] != 5000
            or allocated_pids["producer_id_len"] != 1000
            or allocated_pids["error_code"] != ERROR_NONE
        ):
            raise TestError(
                f"AllocateProducerIds fixture parser failed: {allocated_pids}"
            )

        init_fixture = struct.pack(">iihqh", 44, 0, 0, 1000, 0)
        identity = parse_init_producer_id_response(init_fixture, 44)
        if identity["producer_id"] != 1000 or identity["producer_epoch"] != 0:
            raise TestError(f"InitProducerId fixture parser failed: {identity}")

        list_offsets_fixture = struct.pack(">i", 160)
        list_offsets_fixture += b"\x00"  # response header tagged fields
        list_offsets_fixture += struct.pack(">i", 0)
        list_offsets_fixture += write_compact_array_len(1)
        list_offsets_fixture += write_compact_string("offset-self-test")
        list_offsets_fixture += write_compact_array_len(1)
        list_offsets_fixture += struct.pack(">ihqqi", 0, 0, -1, 5, 3)
        list_offsets_fixture += b"\x00"  # partition tagged fields
        list_offsets_fixture += b"\x00"  # topic tagged fields
        list_offsets_fixture += b"\x00"  # response tagged fields
        listed_offsets = parse_list_offsets_response(list_offsets_fixture, 160)
        if (
            listed_offsets[0]["name"] != "offset-self-test"
            or listed_offsets[0]["partitions"][0]["offset"] != 5
            or listed_offsets[0]["partitions"][0]["leader_epoch"] != 3
        ):
            raise TestError(f"ListOffsets fixture parser failed: {listed_offsets}")

        leader_epoch_fixture = struct.pack(">i", 161)
        leader_epoch_fixture += b"\x00"  # response header tagged fields
        leader_epoch_fixture += struct.pack(">i", 0)
        leader_epoch_fixture += write_compact_array_len(1)
        leader_epoch_fixture += write_compact_string("epoch-self-test")
        leader_epoch_fixture += write_compact_array_len(1)
        leader_epoch_fixture += struct.pack(">hiiq", 0, 0, 3, 5)
        leader_epoch_fixture += b"\x00"  # partition tagged fields
        leader_epoch_fixture += b"\x00"  # topic tagged fields
        leader_epoch_fixture += b"\x00"  # response tagged fields
        epoch_offsets = parse_offset_for_leader_epoch_response(
            leader_epoch_fixture, 161
        )
        if (
            epoch_offsets[0]["name"] != "epoch-self-test"
            or epoch_offsets[0]["partitions"][0]["end_offset"] != 5
            or epoch_offsets[0]["partitions"][0]["leader_epoch"] != 3
        ):
            raise TestError(
                f"OffsetForLeaderEpoch fixture parser failed: {epoch_offsets}"
            )

        delete_records_fixture = struct.pack(">i", 163)
        delete_records_fixture += b"\x00"  # response header tagged fields
        delete_records_fixture += struct.pack(">i", 0)
        delete_records_fixture += write_compact_array_len(1)
        delete_records_fixture += write_compact_string("delete-records-self-test")
        delete_records_fixture += write_compact_array_len(1)
        delete_records_fixture += struct.pack(">iqh", 0, 4, 0)
        delete_records_fixture += b"\x00"  # partition tagged fields
        delete_records_fixture += b"\x00"  # topic tagged fields
        delete_records_fixture += b"\x00"  # response tagged fields
        deleted_records = parse_delete_records_response(delete_records_fixture, 163)
        if (
            deleted_records[0]["name"] != "delete-records-self-test"
            or deleted_records[0]["partitions"][0]["low_watermark"] != 4
            or deleted_records[0]["partitions"][0]["error_code"] != 0
        ):
            raise TestError(
                f"DeleteRecords fixture parser failed: {deleted_records}"
            )

        create_partitions_fixture = struct.pack(">i", 165)
        create_partitions_fixture += b"\x00"  # response header tagged fields
        create_partitions_fixture += struct.pack(">i", 0)
        create_partitions_fixture += write_compact_array_len(1)
        create_partitions_fixture += write_compact_string("create-parts-self-test")
        create_partitions_fixture += struct.pack(">h", 0)
        create_partitions_fixture += write_compact_string(None)
        create_partitions_fixture += b"\x00"  # topic tagged fields
        create_partitions_fixture += b"\x00"  # response tagged fields
        created_partitions = parse_create_partitions_response(
            create_partitions_fixture, 165
        )
        if (
            created_partitions["results"][0]["name"] != "create-parts-self-test"
            or created_partitions["results"][0]["error_code"] != 0
            or created_partitions["results"][0]["error_message"] is not None
        ):
            raise TestError(
                f"CreatePartitions fixture parser failed: {created_partitions}"
            )

        alter_quota_fixture = struct.pack(">i", 166)
        alter_quota_fixture += b"\x00"  # response header tagged fields
        alter_quota_fixture += struct.pack(">i", 0)
        alter_quota_fixture += write_compact_array_len(1)
        alter_quota_fixture += struct.pack(">h", 0)
        alter_quota_fixture += write_compact_string(None)
        alter_quota_fixture += write_compact_array_len(1)
        alter_quota_fixture += write_compact_string("client-id")
        alter_quota_fixture += write_compact_string("quota-self-test")
        alter_quota_fixture += b"\x00"  # entity tagged fields
        alter_quota_fixture += b"\x00"  # entry tagged fields
        alter_quota_fixture += b"\x00"  # response tagged fields
        altered_quota = parse_alter_client_quotas_response(alter_quota_fixture, 166)
        require_alter_client_quota_success(altered_quota, "quota-self-test")

        describe_quota_fixture = struct.pack(">i", 167)
        describe_quota_fixture += b"\x00"  # response header tagged fields
        describe_quota_fixture += struct.pack(">ih", 0, 0)
        describe_quota_fixture += write_compact_string(None)
        describe_quota_fixture += write_compact_array_len(1)
        describe_quota_fixture += write_compact_array_len(1)
        describe_quota_fixture += write_compact_string("client-id")
        describe_quota_fixture += write_compact_string("quota-self-test")
        describe_quota_fixture += b"\x00"  # entity tagged fields
        describe_quota_fixture += write_compact_array_len(2)
        describe_quota_fixture += write_compact_string("producer_byte_rate")
        describe_quota_fixture += struct.pack(">d", 1234.0)
        describe_quota_fixture += b"\x00"  # value tagged fields
        describe_quota_fixture += write_compact_string("consumer_byte_rate")
        describe_quota_fixture += struct.pack(">d", 4321.0)
        describe_quota_fixture += b"\x00"  # value tagged fields
        describe_quota_fixture += b"\x00"  # entry tagged fields
        describe_quota_fixture += b"\x00"  # response tagged fields
        described_quota = parse_describe_client_quotas_response(
            describe_quota_fixture, 167
        )
        require_describe_client_quota_values(
            described_quota,
            "quota-self-test",
            {"producer_byte_rate": 1234.0, "consumer_byte_rate": 4321.0},
        )

        alter_scram_fixture = struct.pack(">i", 168)
        alter_scram_fixture += b"\x00"  # response header tagged fields
        alter_scram_fixture += struct.pack(">i", 0)
        alter_scram_fixture += write_compact_array_len(1)
        alter_scram_fixture += write_compact_string("scram-self-test")
        alter_scram_fixture += struct.pack(">h", 0)
        alter_scram_fixture += write_compact_string(None)
        alter_scram_fixture += b"\x00"  # result tagged fields
        alter_scram_fixture += b"\x00"  # response tagged fields
        altered_scram = parse_alter_user_scram_credentials_response(
            alter_scram_fixture, 168
        )
        require_alter_user_scram_credentials_success(altered_scram, "scram-self-test")

        describe_scram_fixture = struct.pack(">i", 169)
        describe_scram_fixture += b"\x00"  # response header tagged fields
        describe_scram_fixture += struct.pack(">ih", 0, 0)
        describe_scram_fixture += write_compact_string(None)
        describe_scram_fixture += write_compact_array_len(1)
        describe_scram_fixture += write_compact_string("scram-self-test")
        describe_scram_fixture += struct.pack(">h", 0)
        describe_scram_fixture += write_compact_string(None)
        describe_scram_fixture += write_compact_array_len(1)
        describe_scram_fixture += b"\x01"
        describe_scram_fixture += struct.pack(">i", 8192)
        describe_scram_fixture += b"\x00"  # credential tagged fields
        describe_scram_fixture += b"\x00"  # result tagged fields
        describe_scram_fixture += b"\x00"  # response tagged fields
        described_scram = parse_describe_user_scram_credentials_response(
            describe_scram_fixture, 169
        )
        require_describe_user_scram_credentials(
            described_scram,
            "scram-self-test",
            8192,
        )

        telemetry_client_id = bytes([0x33] * 16)
        get_telemetry_fixture = struct.pack(">i", 170)
        get_telemetry_fixture += b"\x00"  # response header tagged fields
        get_telemetry_fixture += struct.pack(">ih", 0, 0)
        get_telemetry_fixture += telemetry_client_id
        get_telemetry_fixture += struct.pack(">i", 1)
        get_telemetry_fixture += write_compact_array_len(1)
        get_telemetry_fixture += b"\x00"
        get_telemetry_fixture += struct.pack(">ii", 60000, 1048576)
        get_telemetry_fixture += b"\x00"
        get_telemetry_fixture += write_compact_array_len(1)
        get_telemetry_fixture += write_compact_string("")
        get_telemetry_fixture += b"\x00"  # response tagged fields
        telemetry_subscription = parse_get_telemetry_subscriptions_response(
            get_telemetry_fixture, 170
        )
        require_telemetry_subscription(telemetry_subscription, telemetry_client_id)

        push_telemetry_fixture = struct.pack(">i", 171)
        push_telemetry_fixture += b"\x00"  # response header tagged fields
        push_telemetry_fixture += struct.pack(">ih", 0, 0)
        push_telemetry_fixture += b"\x00"  # response tagged fields
        pushed_telemetry = parse_push_telemetry_response(push_telemetry_fixture, 171)
        require_push_telemetry_success(pushed_telemetry)

        list_metrics_fixture = struct.pack(">i", 172)
        list_metrics_fixture += b"\x00"  # response header tagged fields
        list_metrics_fixture += struct.pack(">ih", 0, 0)
        list_metrics_fixture += write_compact_array_len(2)
        list_metrics_fixture += write_compact_string("default")
        list_metrics_fixture += b"\x00"  # resource tagged fields
        list_metrics_fixture += write_compact_string(f"client:{telemetry_client_id.hex()}")
        list_metrics_fixture += b"\x00"  # resource tagged fields
        list_metrics_fixture += b"\x00"  # response tagged fields
        listed_metrics = parse_list_client_metrics_resources_response(
            list_metrics_fixture, 172
        )
        require_client_metrics_resources(listed_metrics, telemetry_client_id)

        token_hmac = bytes([0x44] * 32)
        create_token_fixture = struct.pack(">i", 173)
        create_token_fixture += b"\x00"  # response header tagged fields
        create_token_fixture += struct.pack(">h", 0)
        create_token_fixture += write_compact_string("User")
        create_token_fixture += write_compact_string("token-self-test")
        create_token_fixture += write_compact_string("User")
        create_token_fixture += write_compact_string("token-self-test")
        create_token_fixture += struct.pack(">qqq", 1000, 2000, 2000)
        create_token_fixture += write_compact_string("token-id-self-test")
        create_token_fixture += write_compact_bytes(token_hmac)
        create_token_fixture += struct.pack(">i", 0)
        create_token_fixture += b"\x00"  # response tagged fields
        created_token = parse_create_delegation_token_response(
            create_token_fixture, 173
        )
        require_create_delegation_token_success(created_token, "token-self-test")

        renew_token_fixture = struct.pack(">i", 174)
        renew_token_fixture += b"\x00"  # response header tagged fields
        renew_token_fixture += struct.pack(">hqi", 0, 2000, 0)
        renew_token_fixture += b"\x00"  # response tagged fields
        renewed_token = parse_delegation_token_lifecycle_response(
            renew_token_fixture,
            174,
            "RenewDelegationToken",
        )
        require_delegation_token_lifecycle_success(
            renewed_token,
            "RenewDelegationToken",
        )

        describe_token_fixture = struct.pack(">i", 175)
        describe_token_fixture += b"\x00"  # response header tagged fields
        describe_token_fixture += struct.pack(">h", 0)
        describe_token_fixture += write_compact_array_len(1)
        describe_token_fixture += write_compact_string("User")
        describe_token_fixture += write_compact_string("token-self-test")
        describe_token_fixture += write_compact_string("User")
        describe_token_fixture += write_compact_string("token-self-test")
        describe_token_fixture += struct.pack(">qqq", 1000, 2000, 2000)
        describe_token_fixture += write_compact_string("token-id-self-test")
        describe_token_fixture += write_compact_bytes(token_hmac)
        describe_token_fixture += write_compact_array_len(0)
        describe_token_fixture += b"\x00"  # token tagged fields
        describe_token_fixture += struct.pack(">i", 0)
        describe_token_fixture += b"\x00"  # response tagged fields
        described_token = parse_describe_delegation_token_response(
            describe_token_fixture, 175
        )
        require_delegation_token_visible(
            described_token,
            "token-self-test",
            "token-id-self-test",
            token_hmac,
        )

        update_features_fixture = struct.pack(">i", 176)
        update_features_fixture += b"\x00"  # response header tagged fields
        update_features_fixture += struct.pack(">ih", 0, 0)
        update_features_fixture += write_compact_string(None)
        update_features_fixture += write_compact_array_len(1)
        update_features_fixture += write_compact_string("metadata.version")
        update_features_fixture += struct.pack(">h", 0)
        update_features_fixture += write_compact_string(None)
        update_features_fixture += b"\x00"  # result tagged fields
        update_features_fixture += b"\x00"  # response tagged fields
        updated_features = parse_update_features_response(
            update_features_fixture, 176
        )
        require_update_features_success(updated_features, "metadata.version")

        supported_feature_tag = write_compact_array_len(1)
        supported_feature_tag += write_compact_string("metadata.version")
        supported_feature_tag += struct.pack(">hh", 1, 1)
        supported_feature_tag += b"\x00"  # supported feature tagged fields
        finalized_feature_tag = write_compact_array_len(1)
        finalized_feature_tag += write_compact_string("metadata.version")
        finalized_feature_tag += struct.pack(">hh", 1, 1)
        finalized_feature_tag += b"\x00"  # finalized feature tagged fields
        api_versions_v3_fixture = struct.pack(">i", 177)
        api_versions_v3_fixture += struct.pack(">h", 0)
        api_versions_v3_fixture += write_compact_array_len(1)
        api_versions_v3_fixture += struct.pack(">hhh", 57, 0, 1)
        api_versions_v3_fixture += b"\x00"  # ApiVersion tagged fields
        api_versions_v3_fixture += struct.pack(">i", 0)
        api_versions_v3_fixture += write_varint(3)
        api_versions_v3_fixture += write_varint(0)
        api_versions_v3_fixture += write_varint(len(supported_feature_tag))
        api_versions_v3_fixture += supported_feature_tag
        api_versions_v3_fixture += write_varint(1)
        api_versions_v3_fixture += write_varint(8)
        api_versions_v3_fixture += struct.pack(">q", 2)
        api_versions_v3_fixture += write_varint(2)
        api_versions_v3_fixture += write_varint(len(finalized_feature_tag))
        api_versions_v3_fixture += finalized_feature_tag
        api_versions_v3 = parse_api_versions_features_response(
            api_versions_v3_fixture, 177
        )
        require_finalized_feature_visible(api_versions_v3, "metadata.version", 1)

        controller_api_versions_fixture = struct.pack(">i", 187)
        controller_api_versions_fixture += struct.pack(">h", ERROR_NONE)
        controller_api_versions_fixture += write_compact_array_len(
            len(CONTROLLER_API_VERSIONS)
        )
        for api_key, versions in sorted(CONTROLLER_API_VERSIONS.items()):
            min_version, max_version = versions
            controller_api_versions_fixture += struct.pack(
                ">hhh", api_key, min_version, max_version
            )
            controller_api_versions_fixture += b"\x00"  # ApiVersion tagged fields
        controller_api_versions_fixture += struct.pack(">i", 0)
        controller_api_versions_fixture += b"\x00"  # response tagged fields
        controller_api_versions = parse_api_versions_features_response(
            controller_api_versions_fixture,
            187,
        )
        require_controller_api_versions(controller_api_versions)

        controller_unsupported_fixture = struct.pack(">i", 193)
        controller_unsupported_fixture += b"\x00"  # response header tagged fields
        controller_unsupported_fixture += struct.pack(
            ">h",
            ERROR_UNSUPPORTED_VERSION,
        )
        controller_unsupported = parse_controller_small_error_response(
            controller_unsupported_fixture,
            193,
            "controller unsupported fixture",
        )
        require_controller_unsupported_response(
            controller_unsupported,
            71,
            0,
            "self-test",
        )

        add_voter_fixture = struct.pack(">i", 188)
        add_voter_fixture += b"\x00"  # response header tagged fields
        add_voter_fixture += struct.pack(">ih", 0, ERROR_INVALID_REQUEST)
        add_voter_fixture += write_compact_string(None)
        add_voter_fixture += b"\x00"  # response tagged fields
        add_voter_response = parse_raft_voter_response(
            add_voter_fixture,
            188,
            "AddRaftVoter",
        )
        require_raft_voter_error(
            add_voter_response,
            ERROR_INVALID_REQUEST,
            "AddRaftVoter fixture",
            "self-test",
        )

        update_voter_fixture = struct.pack(">i", 189)
        update_voter_fixture += b"\x00"  # response header tagged fields
        update_voter_fixture += struct.pack(">ih", 0, ERROR_INVALID_UPDATE_VERSION)
        update_voter_fixture += b"\x00"  # response tagged fields
        update_voter_response = parse_raft_voter_response(
            update_voter_fixture,
            189,
            "UpdateRaftVoter",
            has_error_message=False,
        )
        require_raft_voter_error(
            update_voter_response,
            ERROR_INVALID_UPDATE_VERSION,
            "UpdateRaftVoter fixture",
            "self-test",
        )

        broker_heartbeat_fixture = struct.pack(">i", 190)
        broker_heartbeat_fixture += b"\x00"  # response header tagged fields
        broker_heartbeat_fixture += struct.pack(
            ">ih???",
            0,
            ERROR_BROKER_ID_NOT_REGISTERED,
            False,
            True,
            False,
        )
        broker_heartbeat_fixture += b"\x00"  # response tagged fields
        broker_heartbeat_response = parse_broker_heartbeat_response(
            broker_heartbeat_fixture,
            190,
        )
        require_broker_lifecycle_negative_response(
            broker_heartbeat_response,
            "BrokerHeartbeat fixture",
            "self-test",
        )
        if (
            broker_heartbeat_response["is_caught_up"]
            or not broker_heartbeat_response["is_fenced"]
            or broker_heartbeat_response["should_shut_down"]
        ):
            raise TestError(
                f"BrokerHeartbeat fixture parser failed: {broker_heartbeat_response}"
            )

        unregister_broker_fixture = struct.pack(">i", 191)
        unregister_broker_fixture += b"\x00"  # response header tagged fields
        unregister_broker_fixture += struct.pack(
            ">ih",
            0,
            ERROR_BROKER_ID_NOT_REGISTERED,
        )
        unregister_broker_fixture += write_compact_string(None)
        unregister_broker_fixture += b"\x00"  # response tagged fields
        unregister_broker_response = parse_unregister_broker_response(
            unregister_broker_fixture,
            191,
        )
        require_broker_lifecycle_negative_response(
            unregister_broker_response,
            "UnregisterBroker fixture",
            "self-test",
        )

        controller_registration_fixture = struct.pack(">i", 192)
        controller_registration_fixture += b"\x00"  # response header tagged fields
        controller_registration_fixture += struct.pack(
            ">ih",
            0,
            ERROR_UNKNOWN_CONTROLLER_ID,
        )
        controller_registration_fixture += write_compact_string(None)
        controller_registration_fixture += b"\x00"  # response tagged fields
        controller_registration_response = parse_controller_registration_response(
            controller_registration_fixture,
            192,
        )
        require_controller_registration_error(
            controller_registration_response,
            ERROR_UNKNOWN_CONTROLLER_ID,
            "ControllerRegistration fixture",
            "self-test",
        )

        acl_fixture = {
            "resource_type": ACL_RESOURCE_TYPE_TOPIC,
            "resource_name": "acl-self-test",
            "pattern_type": ACL_PATTERN_TYPE_LITERAL,
            "principal": "User:acl-self-test",
            "host": "*",
            "operation": ACL_OPERATION_DESCRIBE,
            "permission_type": ACL_PERMISSION_ALLOW,
        }
        create_acls_fixture = struct.pack(">i", 178)
        create_acls_fixture += b"\x00"  # response header tagged fields
        create_acls_fixture += struct.pack(">i", 0)
        create_acls_fixture += write_compact_array_len(1)
        create_acls_fixture += struct.pack(">h", 0)
        create_acls_fixture += write_compact_string(None)
        create_acls_fixture += b"\x00"  # result tagged fields
        create_acls_fixture += b"\x00"  # response tagged fields
        created_acls = parse_create_acls_response(create_acls_fixture, 178)
        require_create_acls_success(created_acls, 1)

        describe_acls_fixture = struct.pack(">i", 179)
        describe_acls_fixture += b"\x00"  # response header tagged fields
        describe_acls_fixture += struct.pack(">ih", 0, 0)
        describe_acls_fixture += write_compact_string(None)
        describe_acls_fixture += write_compact_array_len(1)
        describe_acls_fixture += struct.pack(">b", acl_fixture["resource_type"])
        describe_acls_fixture += write_compact_string(acl_fixture["resource_name"])
        describe_acls_fixture += struct.pack(">b", acl_fixture["pattern_type"])
        describe_acls_fixture += write_compact_array_len(1)
        describe_acls_fixture += write_compact_string(acl_fixture["principal"])
        describe_acls_fixture += write_compact_string(acl_fixture["host"])
        describe_acls_fixture += struct.pack(
            ">bb",
            acl_fixture["operation"],
            acl_fixture["permission_type"],
        )
        describe_acls_fixture += b"\x00"  # ACL tagged fields
        describe_acls_fixture += b"\x00"  # resource tagged fields
        describe_acls_fixture += b"\x00"  # response tagged fields
        described_acls = parse_describe_acls_response(describe_acls_fixture, 179)
        require_acl_visible(described_acls, acl_fixture)

        delete_acls_fixture = struct.pack(">i", 180)
        delete_acls_fixture += b"\x00"  # response header tagged fields
        delete_acls_fixture += struct.pack(">i", 0)
        delete_acls_fixture += write_compact_array_len(1)
        delete_acls_fixture += struct.pack(">h", 0)
        delete_acls_fixture += write_compact_string(None)
        delete_acls_fixture += write_compact_array_len(1)
        delete_acls_fixture += struct.pack(">h", 0)
        delete_acls_fixture += write_compact_string(None)
        delete_acls_fixture += struct.pack(">b", acl_fixture["resource_type"])
        delete_acls_fixture += write_compact_string(acl_fixture["resource_name"])
        delete_acls_fixture += struct.pack(">b", acl_fixture["pattern_type"])
        delete_acls_fixture += write_compact_string(acl_fixture["principal"])
        delete_acls_fixture += write_compact_string(acl_fixture["host"])
        delete_acls_fixture += struct.pack(
            ">bb",
            acl_fixture["operation"],
            acl_fixture["permission_type"],
        )
        delete_acls_fixture += b"\x00"  # matching ACL tagged fields
        delete_acls_fixture += b"\x00"  # filter result tagged fields
        delete_acls_fixture += b"\x00"  # response tagged fields
        deleted_acls = parse_delete_acls_response(delete_acls_fixture, 180)
        require_delete_acls_success(deleted_acls, acl_fixture)

        absent_acls_fixture = struct.pack(">i", 181)
        absent_acls_fixture += b"\x00"  # response header tagged fields
        absent_acls_fixture += struct.pack(">ih", 0, 0)
        absent_acls_fixture += write_compact_string(None)
        absent_acls_fixture += write_compact_array_len(0)
        absent_acls_fixture += b"\x00"  # response tagged fields
        absent_acls = parse_describe_acls_response(absent_acls_fixture, 181)
        require_acl_absent(absent_acls, acl_fixture)

        describe_configs_fixture = struct.pack(">i", 166)
        describe_configs_fixture += b"\x00"  # response header tagged fields
        describe_configs_fixture += struct.pack(">i", 0)
        describe_configs_fixture += write_compact_array_len(1)
        describe_configs_fixture += struct.pack(">h", 0)
        describe_configs_fixture += write_compact_string(None)
        describe_configs_fixture += b"\x02"  # resource_type=TOPIC
        describe_configs_fixture += write_compact_string("cfg-self-test")
        describe_configs_fixture += write_compact_array_len(2)
        describe_configs_fixture += write_compact_string("cleanup.policy")
        describe_configs_fixture += write_compact_string("delete")
        describe_configs_fixture += b"\x00"  # read_only=false
        describe_configs_fixture += b"\x05"  # config_source=DEFAULT_CONFIG
        describe_configs_fixture += b"\x00"  # is_sensitive=false
        describe_configs_fixture += write_compact_array_len(0)
        describe_configs_fixture += b"\x06"  # config_type=LIST
        describe_configs_fixture += write_compact_string("Cleanup policy")
        describe_configs_fixture += b"\x00"  # config tagged fields
        describe_configs_fixture += write_compact_string("min.insync.replicas")
        describe_configs_fixture += write_compact_string("1")
        describe_configs_fixture += b"\x00"  # read_only=false
        describe_configs_fixture += b"\x05"  # config_source=DEFAULT_CONFIG
        describe_configs_fixture += b"\x00"  # is_sensitive=false
        describe_configs_fixture += write_compact_array_len(0)
        describe_configs_fixture += b"\x02"  # config_type=INT
        describe_configs_fixture += write_compact_string("Minimum ISR")
        describe_configs_fixture += b"\x00"  # config tagged fields
        describe_configs_fixture += b"\x00"  # result tagged fields
        describe_configs_fixture += b"\x00"  # response tagged fields
        described_configs = parse_describe_configs_response(
            describe_configs_fixture, 166
        )
        if (
            described_configs["results"][0]["resource_name"] != "cfg-self-test"
            or described_configs["results"][0]["configs"][0]["value"] != "delete"
            or described_configs["results"][0]["configs"][1]["value"] != "1"
        ):
            raise TestError(
                f"DescribeConfigs fixture parser failed: {described_configs}"
            )

        alter_configs_fixture = struct.pack(">i", 182)
        alter_configs_fixture += b"\x00"  # response header tagged fields
        alter_configs_fixture += struct.pack(">i", 0)
        alter_configs_fixture += write_compact_array_len(1)
        alter_configs_fixture += struct.pack(">h", 0)
        alter_configs_fixture += write_compact_string(None)
        alter_configs_fixture += b"\x02"  # resource_type=TOPIC
        alter_configs_fixture += write_compact_string("alter-cfg-self-test")
        alter_configs_fixture += b"\x00"  # resource response tagged fields
        alter_configs_fixture += b"\x00"  # response tagged fields
        altered_configs = parse_alter_configs_response(
            alter_configs_fixture,
            182,
            "AlterConfigs",
        )
        require_alter_configs_success(
            altered_configs,
            "alter-cfg-self-test",
            "AlterConfigs",
        )

        incremental_configs_fixture = struct.pack(">i", 183)
        incremental_configs_fixture += b"\x00"  # response header tagged fields
        incremental_configs_fixture += struct.pack(">i", 0)
        incremental_configs_fixture += write_compact_array_len(1)
        incremental_configs_fixture += struct.pack(">h", 0)
        incremental_configs_fixture += write_compact_string(None)
        incremental_configs_fixture += b"\x02"  # resource_type=TOPIC
        incremental_configs_fixture += write_compact_string("inc-cfg-self-test")
        incremental_configs_fixture += b"\x00"  # resource response tagged fields
        incremental_configs_fixture += b"\x00"  # response tagged fields
        incremented_configs = parse_alter_configs_response(
            incremental_configs_fixture,
            183,
            "IncrementalAlterConfigs",
        )
        require_alter_configs_success(
            incremented_configs,
            "inc-cfg-self-test",
            "IncrementalAlterConfigs",
        )

        describe_log_dirs_fixture = struct.pack(">i", 167)
        describe_log_dirs_fixture += b"\x00"  # response header tagged fields
        describe_log_dirs_fixture += struct.pack(">ih", 0, 0)
        describe_log_dirs_fixture += write_compact_array_len(1)
        describe_log_dirs_fixture += struct.pack(">h", 0)
        describe_log_dirs_fixture += write_compact_string("/tmp/zmq-log")
        describe_log_dirs_fixture += write_compact_array_len(1)
        describe_log_dirs_fixture += write_compact_string("log-dir-self-test")
        describe_log_dirs_fixture += write_compact_array_len(1)
        describe_log_dirs_fixture += struct.pack(">iqq", 0, 7, 0)
        describe_log_dirs_fixture += b"\x00"  # is_future_key=false
        describe_log_dirs_fixture += b"\x00"  # partition tagged fields
        describe_log_dirs_fixture += b"\x00"  # topic tagged fields
        describe_log_dirs_fixture += struct.pack(">qq", -1, -1)
        describe_log_dirs_fixture += b"\x00"  # result tagged fields
        describe_log_dirs_fixture += b"\x00"  # response tagged fields
        described_log_dirs = parse_describe_log_dirs_response(
            describe_log_dirs_fixture, 167
        )
        if (
            described_log_dirs["results"][0]["topics"][0]["name"]
            != "log-dir-self-test"
            or described_log_dirs["results"][0]["topics"][0]["partitions"][0][
                "partition_size"
            ]
            != 7
        ):
            raise TestError(
                f"DescribeLogDirs fixture parser failed: {described_log_dirs}"
            )

        alter_log_dirs_fixture = struct.pack(">i", 168)
        alter_log_dirs_fixture += b"\x00"  # response header tagged fields
        alter_log_dirs_fixture += struct.pack(">i", 0)
        alter_log_dirs_fixture += write_compact_array_len(1)
        alter_log_dirs_fixture += write_compact_string("alter-log-dir-self-test")
        alter_log_dirs_fixture += write_compact_array_len(1)
        alter_log_dirs_fixture += struct.pack(">ih", 0, 0)
        alter_log_dirs_fixture += b"\x00"  # partition tagged fields
        alter_log_dirs_fixture += b"\x00"  # topic tagged fields
        alter_log_dirs_fixture += b"\x00"  # response tagged fields
        altered_log_dirs = parse_alter_replica_log_dirs_response(
            alter_log_dirs_fixture, 168
        )
        if (
            altered_log_dirs["topics"][0]["name"] != "alter-log-dir-self-test"
            or altered_log_dirs["topics"][0]["partitions"][0]["partition_index"] != 0
            or altered_log_dirs["topics"][0]["partitions"][0]["error_code"] != 0
        ):
            raise TestError(
                f"AlterReplicaLogDirs fixture parser failed: {altered_log_dirs}"
            )

        assign_dir_id = bytes(range(16))
        assign_topic_id = bytes(reversed(range(16)))
        assign_dirs_fixture = struct.pack(">i", 169)
        assign_dirs_fixture += b"\x00"  # response header tagged fields
        assign_dirs_fixture += struct.pack(">ih", 0, 0)
        assign_dirs_fixture += write_compact_array_len(1)
        assign_dirs_fixture += assign_dir_id
        assign_dirs_fixture += write_compact_array_len(1)
        assign_dirs_fixture += assign_topic_id
        assign_dirs_fixture += write_compact_array_len(1)
        assign_dirs_fixture += struct.pack(">ih", 0, 0)
        assign_dirs_fixture += b"\x00"  # partition tagged fields
        assign_dirs_fixture += b"\x00"  # topic tagged fields
        assign_dirs_fixture += b"\x00"  # directory tagged fields
        assign_dirs_fixture += b"\x00"  # response tagged fields
        assigned_dirs = parse_assign_replicas_to_dirs_response(
            assign_dirs_fixture, 169
        )
        if (
            assigned_dirs["directories"][0]["id"] != assign_dir_id
            or assigned_dirs["directories"][0]["topics"][0]["topic_id"]
            != assign_topic_id
            or assigned_dirs["directories"][0]["topics"][0]["partitions"][0][
                "error_code"
            ]
            != 0
        ):
            raise TestError(
                f"AssignReplicasToDirs fixture parser failed: {assigned_dirs}"
            )

        elect_leaders_fixture = struct.pack(">i", 170)
        elect_leaders_fixture += b"\x00"  # response header tagged fields
        elect_leaders_fixture += struct.pack(">ih", 0, 0)
        elect_leaders_fixture += write_compact_array_len(1)
        elect_leaders_fixture += write_compact_string("elect-self-test")
        elect_leaders_fixture += write_compact_array_len(1)
        elect_leaders_fixture += struct.pack(">ih", 0, 0)
        elect_leaders_fixture += write_compact_string(None)
        elect_leaders_fixture += b"\x00"  # partition tagged fields
        elect_leaders_fixture += b"\x00"  # topic tagged fields
        elect_leaders_fixture += b"\x00"  # response tagged fields
        elected = parse_elect_leaders_response(elect_leaders_fixture, 170)
        if (
            elected["results"][0]["topic"] != "elect-self-test"
            or elected["results"][0]["partitions"][0]["partition_id"] != 0
            or elected["results"][0]["partitions"][0]["error_code"] != 0
        ):
            raise TestError(f"ElectLeaders fixture parser failed: {elected}")

        topic_partitions_fixture = struct.pack(">i", 164)
        topic_partitions_fixture += b"\x00"  # response header tagged fields
        topic_partitions_fixture += struct.pack(">i", 0)
        topic_partitions_fixture += write_compact_array_len(1)
        topic_partitions_fixture += struct.pack(">h", 0)
        topic_partitions_fixture += write_compact_string("dtp-self-test")
        topic_partitions_fixture += bytes(range(16))
        topic_partitions_fixture += b"\x00"  # is_internal=false
        topic_partitions_fixture += write_compact_array_len(1)
        topic_partitions_fixture += struct.pack(">hiii", 0, 0, 100, 3)
        topic_partitions_fixture += write_compact_i32_array([100])
        topic_partitions_fixture += write_compact_i32_array([100])
        topic_partitions_fixture += b"\x00"  # eligible_leader_replicas=null
        topic_partitions_fixture += b"\x00"  # last_known_elr=null
        topic_partitions_fixture += write_compact_i32_array([])
        topic_partitions_fixture += b"\x00"  # partition tagged fields
        topic_partitions_fixture += struct.pack(">i", -2147483648)
        topic_partitions_fixture += b"\x00"  # topic tagged fields
        topic_partitions_fixture += b"\x00"  # next_cursor=null
        topic_partitions_fixture += b"\x00"  # response tagged fields
        topic_partitions = parse_describe_topic_partitions_response(
            topic_partitions_fixture, 164
        )
        if (
            topic_partitions["topics"][0]["name"] != "dtp-self-test"
            or topic_partitions["topics"][0]["partitions"][0]["leader_id"] != 100
            or topic_partitions["topics"][0]["partitions"][0]["replica_nodes"]
            != [100]
            or topic_partitions["next_cursor"] is not None
        ):
            raise TestError(
                f"DescribeTopicPartitions fixture parser failed: {topic_partitions}"
            )

        deleted_topic_partitions_fixture = struct.pack(">i", 184)
        deleted_topic_partitions_fixture += b"\x00"  # response header tagged fields
        deleted_topic_partitions_fixture += struct.pack(">i", 0)
        deleted_topic_partitions_fixture += write_compact_array_len(1)
        deleted_topic_partitions_fixture += struct.pack(
            ">h",
            ERROR_UNKNOWN_TOPIC_OR_PARTITION,
        )
        deleted_topic_partitions_fixture += write_compact_string(
            "deleted-dtp-self-test"
        )
        deleted_topic_partitions_fixture += b"\x00" * 16
        deleted_topic_partitions_fixture += b"\x00"  # is_internal=false
        deleted_topic_partitions_fixture += write_compact_array_len(0)
        deleted_topic_partitions_fixture += struct.pack(">i", -2147483648)
        deleted_topic_partitions_fixture += b"\x00"  # topic tagged fields
        deleted_topic_partitions_fixture += b"\x00"  # next_cursor=null
        deleted_topic_partitions_fixture += b"\x00"  # response tagged fields
        deleted_topic_partitions = parse_describe_topic_partitions_response(
            deleted_topic_partitions_fixture,
            184,
        )
        require_deleted_topic_absent(
            deleted_topic_partitions,
            "deleted-dtp-self-test",
        )

        describe_quorum_fixture = struct.pack(">i", 185)
        describe_quorum_fixture += b"\x00"  # response header tagged fields
        describe_quorum_fixture += struct.pack(">h", ERROR_NONE)
        describe_quorum_fixture += write_compact_string(None)
        describe_quorum_fixture += write_compact_array_len(1)
        describe_quorum_fixture += write_compact_string("__cluster_metadata")
        describe_quorum_fixture += write_compact_array_len(1)
        describe_quorum_fixture += struct.pack(">ih", 0, ERROR_NONE)
        describe_quorum_fixture += write_compact_string(None)
        describe_quorum_fixture += struct.pack(">iiq", 1, 7, 42)
        describe_quorum_fixture += write_compact_array_len(2)
        for replica_id, port in ((0, 63093), (1, 63094)):
            describe_quorum_fixture += struct.pack(">i", replica_id)
            describe_quorum_fixture += bytes([replica_id + 1]) * 16
            describe_quorum_fixture += struct.pack(">qqq", 42, -1, -1)
            describe_quorum_fixture += b"\x00"  # voter tagged fields
        describe_quorum_fixture += write_compact_array_len(0)
        describe_quorum_fixture += b"\x00"  # partition tagged fields
        describe_quorum_fixture += b"\x00"  # topic tagged fields
        describe_quorum_fixture += write_compact_array_len(2)
        for node_id, port in ((0, 63093), (1, 63094)):
            describe_quorum_fixture += struct.pack(">i", node_id)
            describe_quorum_fixture += write_compact_array_len(1)
            describe_quorum_fixture += write_compact_string("CONTROLLER")
            describe_quorum_fixture += write_compact_string("127.0.0.1")
            describe_quorum_fixture += struct.pack(">H", port)
            describe_quorum_fixture += b"\x00"  # listener tagged fields
            describe_quorum_fixture += b"\x00"  # node tagged fields
        describe_quorum_fixture += b"\x00"  # response tagged fields
        described_quorum = parse_describe_quorum_response(
            describe_quorum_fixture,
            185,
            2,
        )
        if (
            described_quorum["leader_id"] != 1
            or described_quorum["leader_epoch"] != 7
            or sorted(described_quorum["voters"]) != [0, 1]
            or described_quorum["nodes"][1]["listeners"][0]["port"] != 63094
            or described_quorum["topics"][0]["partitions"][0]["current_voters"][0][
                "replica_directory_id"
            ]
            != bytes([1]) * 16
        ):
            raise TestError(
                f"DescribeQuorum v2 fixture parser failed: {described_quorum}"
            )

        fetch_snapshot_fixture = struct.pack(">i", 186)
        fetch_snapshot_fixture += b"\x00"  # response header tagged fields
        fetch_snapshot_fixture += struct.pack(">ih", 0, ERROR_NONE)
        fetch_snapshot_fixture += write_compact_array_len(1)
        fetch_snapshot_fixture += write_compact_string("__cluster_metadata")
        fetch_snapshot_fixture += write_compact_array_len(1)
        fetch_snapshot_fixture += struct.pack(">ihqi", 0, ERROR_SNAPSHOT_NOT_FOUND, 7, 2)
        fetch_snapshot_fixture += b"\x00"  # snapshot_id tagged fields
        fetch_snapshot_fixture += struct.pack(">qq", 0, 12)
        fetch_snapshot_fixture += write_compact_bytes(None)
        current_leader_tag = struct.pack(">ii", 1, 3) + b"\x00"
        fetch_snapshot_fixture += write_varint(1)
        fetch_snapshot_fixture += write_varint(0)
        fetch_snapshot_fixture += write_varint(len(current_leader_tag))
        fetch_snapshot_fixture += current_leader_tag
        fetch_snapshot_fixture += b"\x00"  # topic tagged fields
        node_endpoints_tag = bytearray()
        node_endpoints_tag += write_compact_array_len(1)
        node_endpoints_tag += struct.pack(">i", 1)
        node_endpoints_tag += write_compact_string("127.0.0.1")
        node_endpoints_tag += struct.pack(">H", 63094)
        node_endpoints_tag += b"\x00"  # node endpoint tagged fields
        fetch_snapshot_fixture += write_varint(1)
        fetch_snapshot_fixture += write_varint(0)
        fetch_snapshot_fixture += write_varint(len(node_endpoints_tag))
        fetch_snapshot_fixture += node_endpoints_tag
        fetched_snapshot = parse_fetch_snapshot_response(
            fetch_snapshot_fixture,
            186,
        )
        if (
            fetched_snapshot["error_code"] != ERROR_NONE
            or fetched_snapshot["topics"][0]["partitions"][0]["error_code"]
            != ERROR_SNAPSHOT_NOT_FOUND
            or fetched_snapshot["topics"][0]["partitions"][0]["current_leader"][
                "leader_id"
            ]
            != 1
            or fetched_snapshot["node_endpoints"][0]["port"] != 63094
        ):
            raise TestError(
                f"FetchSnapshot v1 fixture parser failed: {fetched_snapshot}"
            )

        describe_cluster_fixture = struct.pack(">i", 165)
        describe_cluster_fixture += b"\x00"  # response header tagged fields
        describe_cluster_fixture += struct.pack(">ih", 0, 0)
        describe_cluster_fixture += write_compact_string(None)
        describe_cluster_fixture += b"\x02"  # endpoint_type=controllers
        describe_cluster_fixture += write_compact_string("cluster-self-test")
        describe_cluster_fixture += struct.pack(">i", 100)
        describe_cluster_fixture += write_compact_array_len(1)
        describe_cluster_fixture += struct.pack(">i", 100)
        describe_cluster_fixture += write_compact_string("localhost")
        describe_cluster_fixture += struct.pack(">i", 39092)
        describe_cluster_fixture += write_compact_string(None)
        describe_cluster_fixture += b"\x00"  # broker tagged fields
        describe_cluster_fixture += struct.pack(">i", 0)
        describe_cluster_fixture += b"\x00"  # response tagged fields
        described_cluster = parse_describe_cluster_response(
            describe_cluster_fixture, 165
        )
        if (
            described_cluster["endpoint_type"] != 2
            or described_cluster["cluster_id"] != "cluster-self-test"
            or described_cluster["controller_id"] != 100
            or described_cluster["brokers"][0]["port"] != 39092
            or described_cluster["cluster_authorized_operations"] != 0
        ):
            raise TestError(
                f"DescribeCluster fixture parser failed: {described_cluster}"
            )

        describe_producers_fixture = struct.pack(">i", 162)
        describe_producers_fixture += b"\x00"  # response header tagged fields
        describe_producers_fixture += struct.pack(">i", 0)
        describe_producers_fixture += write_compact_array_len(1)
        describe_producers_fixture += write_compact_string("producer-self-test")
        describe_producers_fixture += write_compact_array_len(1)
        describe_producers_fixture += struct.pack(">ih", 0, 0)
        describe_producers_fixture += write_compact_string(None)
        describe_producers_fixture += write_compact_array_len(1)
        describe_producers_fixture += struct.pack(">qiiqiq", 1000, 0, 7, -1, 0, -1)
        describe_producers_fixture += b"\x00"  # producer tagged fields
        describe_producers_fixture += b"\x00"  # partition tagged fields
        describe_producers_fixture += b"\x00"  # topic tagged fields
        describe_producers_fixture += b"\x00"  # response tagged fields
        described_producers = parse_describe_producers_response(
            describe_producers_fixture, 162
        )
        if (
            described_producers[0]["name"] != "producer-self-test"
            or described_producers[0]["partitions"][0]["active_producers"][0][
                "producer_id"
            ]
            != 1000
            or described_producers[0]["partitions"][0]["active_producers"][0][
                "last_sequence"
            ]
            != 7
        ):
            raise TestError(
                f"DescribeProducers fixture parser failed: {described_producers}"
            )

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

        share_heartbeat = parse_share_group_heartbeat_response(
            consumer_group_heartbeat_fixture, 59
        )
        if (
            share_heartbeat["error_code"] != 0
            or share_heartbeat["member_id"] != "kip848-member"
            or share_heartbeat["assignment"]["topic_partitions"][0]["partitions"]
            != [0]
        ):
            raise TestError(
                f"ShareGroupHeartbeat fixture parser failed: {share_heartbeat}"
            )

        share_describe_topic_id = bytes(range(16, 32))
        share_group_describe_fixture = struct.pack(">i", 156)
        share_group_describe_fixture += b"\x00"  # response header tagged fields
        share_group_describe_fixture += struct.pack(">i", 0)
        share_group_describe_fixture += write_compact_array_len(1)
        share_group_describe_fixture += struct.pack(">h", 0)
        share_group_describe_fixture += write_compact_string(None)
        share_group_describe_fixture += write_compact_string("share-describe-self-test")
        share_group_describe_fixture += write_compact_string("Stable")
        share_group_describe_fixture += struct.pack(">ii", 2, 2)
        share_group_describe_fixture += write_compact_string("range")
        share_group_describe_fixture += write_compact_array_len(1)
        share_group_describe_fixture += write_compact_string("share-member-1")
        share_group_describe_fixture += write_compact_string("rack-share")
        share_group_describe_fixture += struct.pack(">i", 2)
        share_group_describe_fixture += write_compact_string("zmq-client")
        share_group_describe_fixture += write_compact_string("/127.0.0.1")
        share_group_describe_fixture += write_compact_array_len(1)
        share_group_describe_fixture += write_compact_string("share-topic")
        share_group_describe_fixture += write_compact_array_len(1)
        share_group_describe_fixture += share_describe_topic_id
        share_group_describe_fixture += write_compact_string("share-topic")
        share_group_describe_fixture += write_compact_i32_array([0])
        share_group_describe_fixture += b"\x00"  # assignment topic tagged fields
        share_group_describe_fixture += b"\x00"  # assignment tagged fields
        share_group_describe_fixture += b"\x00"  # member tagged fields
        share_group_describe_fixture += struct.pack(">i", -2147483648)
        share_group_describe_fixture += b"\x00"  # group tagged fields
        share_group_describe_fixture += b"\x00"  # response tagged fields
        share_described = parse_share_group_describe_response(
            share_group_describe_fixture, 156
        )
        if (
            len(share_described) != 1
            or share_described[0]["group_id"] != "share-describe-self-test"
            or share_described[0]["members"][0]["rack_id"] != "rack-share"
            or share_described[0]["members"][0]["subscribed_topics"]
            != ["share-topic"]
            or share_described[0]["members"][0]["assignment"]["topic_partitions"][0][
                "topic_id"
            ]
            != share_describe_topic_id
        ):
            raise TestError(
                f"ShareGroupDescribe fixture parser failed: {share_described}"
            )

        share_fetch_fixture = struct.pack(">i", 157)
        share_fetch_fixture += b"\x00"  # response header tagged fields
        share_fetch_fixture += struct.pack(">ih", 0, 0)
        share_fetch_fixture += write_compact_string(None)
        share_fetch_fixture += write_compact_array_len(1)
        share_fetch_fixture += share_describe_topic_id
        share_fetch_fixture += write_compact_array_len(1)
        share_fetch_fixture += struct.pack(">ih", 0, 0)
        share_fetch_fixture += write_compact_string(None)
        share_fetch_fixture += struct.pack(">h", 0)
        share_fetch_fixture += write_compact_string(None)
        share_fetch_fixture += struct.pack(">ii", 1, 0)
        share_fetch_fixture += b"\x00"  # current_leader tagged fields
        share_fetch_fixture += write_compact_bytes(b"r0")
        share_fetch_fixture += write_compact_array_len(1)
        share_fetch_fixture += struct.pack(">qqh", 0, 0, 1)
        share_fetch_fixture += b"\x00"  # acquired record tagged fields
        share_fetch_fixture += b"\x00"  # partition tagged fields
        share_fetch_fixture += b"\x00"  # topic tagged fields
        share_fetch_fixture += write_compact_array_len(0)
        share_fetch_fixture += b"\x00"  # response tagged fields
        share_fetched = parse_share_fetch_response(share_fetch_fixture, 157)
        if (
            share_fetched["error_code"] != 0
            or share_fetched["responses"][0]["topic_id"] != share_describe_topic_id
            or share_fetched["responses"][0]["partitions"][0]["records"] != b"r0"
            or share_fetched["responses"][0]["partitions"][0]["acquired_records"][0][
                "last_offset"
            ]
            != 0
        ):
            raise TestError(f"ShareFetch fixture parser failed: {share_fetched}")

        share_ack_fixture = struct.pack(">i", 158)
        share_ack_fixture += b"\x00"  # response header tagged fields
        share_ack_fixture += struct.pack(">ih", 0, 0)
        share_ack_fixture += write_compact_string(None)
        share_ack_fixture += write_compact_array_len(1)
        share_ack_fixture += share_describe_topic_id
        share_ack_fixture += write_compact_array_len(1)
        share_ack_fixture += struct.pack(">ih", 0, 0)
        share_ack_fixture += write_compact_string(None)
        share_ack_fixture += struct.pack(">ii", 1, 0)
        share_ack_fixture += b"\x00"  # current_leader tagged fields
        share_ack_fixture += b"\x00"  # partition tagged fields
        share_ack_fixture += b"\x00"  # topic tagged fields
        share_ack_fixture += write_compact_array_len(0)
        share_ack_fixture += b"\x00"  # response tagged fields
        share_acked = parse_share_acknowledge_response(share_ack_fixture, 158)
        if (
            share_acked["error_code"] != 0
            or share_acked["responses"][0]["topic_id"] != share_describe_topic_id
            or share_acked["responses"][0]["partitions"][0]["partition_index"] != 0
            or share_acked["responses"][0]["partitions"][0]["error_code"] != 0
        ):
            raise TestError(f"ShareAcknowledge fixture parser failed: {share_acked}")

        share_state_result_fixture = struct.pack(">i", 159)
        share_state_result_fixture += b"\x00"  # response header tagged fields
        share_state_result_fixture += write_compact_array_len(1)
        share_state_result_fixture += share_describe_topic_id
        share_state_result_fixture += write_compact_array_len(1)
        share_state_result_fixture += struct.pack(">ih", 0, 0)
        share_state_result_fixture += write_compact_string(None)
        share_state_result_fixture += b"\x00"  # partition tagged fields
        share_state_result_fixture += b"\x00"  # topic tagged fields
        share_state_result_fixture += b"\x00"  # response tagged fields
        share_state_result = parse_share_state_result_response(
            share_state_result_fixture, 159, "InitializeShareGroupState"
        )
        if (
            share_state_result[0]["topic_id"] != share_describe_topic_id
            or share_state_result[0]["partitions"][0]["partition"] != 0
            or share_state_result[0]["partitions"][0]["error_code"] != 0
        ):
            raise TestError(
                f"Share state result fixture parser failed: {share_state_result}"
            )

        share_state_read_fixture = struct.pack(">i", 160)
        share_state_read_fixture += b"\x00"  # response header tagged fields
        share_state_read_fixture += write_compact_array_len(1)
        share_state_read_fixture += share_describe_topic_id
        share_state_read_fixture += write_compact_array_len(1)
        share_state_read_fixture += struct.pack(">ih", 0, 0)
        share_state_read_fixture += write_compact_string(None)
        share_state_read_fixture += struct.pack(">iq", 2, 1)
        share_state_read_fixture += write_compact_array_len(1)
        share_state_read_fixture += struct.pack(">qqbh", 0, 0, 2, 1)
        share_state_read_fixture += b"\x00"  # state batch tagged fields
        share_state_read_fixture += b"\x00"  # partition tagged fields
        share_state_read_fixture += b"\x00"  # topic tagged fields
        share_state_read_fixture += b"\x00"  # response tagged fields
        share_state_read = parse_read_share_group_state_response(
            share_state_read_fixture, 160
        )
        if (
            share_state_read[0]["partitions"][0]["state_epoch"] != 2
            or share_state_read[0]["partitions"][0]["start_offset"] != 1
            or share_state_read[0]["partitions"][0]["state_batches"][0][
                "delivery_state"
            ]
            != 2
        ):
            raise TestError(
                f"ReadShareGroupState fixture parser failed: {share_state_read}"
            )

        share_state_summary_fixture = struct.pack(">i", 161)
        share_state_summary_fixture += b"\x00"  # response header tagged fields
        share_state_summary_fixture += write_compact_array_len(1)
        share_state_summary_fixture += share_describe_topic_id
        share_state_summary_fixture += write_compact_array_len(1)
        share_state_summary_fixture += struct.pack(">ih", 0, 0)
        share_state_summary_fixture += write_compact_string(None)
        share_state_summary_fixture += struct.pack(">iq", 2, 1)
        share_state_summary_fixture += b"\x00"  # partition tagged fields
        share_state_summary_fixture += b"\x00"  # topic tagged fields
        share_state_summary_fixture += b"\x00"  # response tagged fields
        share_state_summary = parse_read_share_group_state_summary_response(
            share_state_summary_fixture, 161
        )
        if (
            share_state_summary[0]["partitions"][0]["state_epoch"] != 2
            or share_state_summary[0]["partitions"][0]["start_offset"] != 1
        ):
            raise TestError(
                f"ReadShareGroupStateSummary fixture parser failed: "
                f"{share_state_summary}"
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

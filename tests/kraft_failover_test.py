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
"""

import os
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


def write_compact_array_len(count):
    return write_varint(count + 1)


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


def automq_create_stream(port, node_id, correlation_id):
    body = struct.pack(">iq", node_id, 1)
    body += write_compact_array_len(1)
    body += struct.pack(">i", node_id)
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 501, correlation_id, body, timeout=15)
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


def automq_open_stream(port, node_id, stream_id, stream_epoch, correlation_id):
    body = struct.pack(">iq", node_id, 1)
    body += write_compact_array_len(1)
    body += struct.pack(">qq", stream_id, stream_epoch)
    body += b"\x00"  # item tagged fields
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 502, correlation_id, body, timeout=15)
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


def automq_describe_stream(port, stream_id, correlation_id):
    body = write_compact_array_len(0)  # topic_partitions
    body += struct.pack(">i", -1)  # node_id
    body += struct.pack(">q", stream_id)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 601, correlation_id, body)
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
        for _ in range(tag_count):
            _, pos = read_compact_string(response, pos)
            _, pos = read_compact_string(response, pos)
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
            }
        )
    pos = skip_tags(response, pos)
    for stream in streams:
        if stream["stream_id"] == stream_id:
            return stream
    raise TestError(f"DescribeStreams did not include stream_id={stream_id}; streams={streams}")


def automq_register_node(port, node_id, node_epoch, wal_config, correlation_id):
    body = struct.pack(">iq", node_id, node_epoch)
    body += write_compact_string(wal_config)
    body += write_compact_array_len(0)  # tags
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 513, correlation_id, body, timeout=15)
    pos = parse_flexible_response_header(response, correlation_id)
    error_code, pos = read_i16(response, pos)
    _, pos = read_i32(response, pos)  # throttle_time_ms
    pos = skip_tags(response, pos)
    if error_code != 0:
        raise TestError(f"AutomqRegisterNode error_code={error_code}")


def automq_get_node(port, node_id, correlation_id):
    body = write_compact_array_len(1)
    body += struct.pack(">i", node_id)
    body += b"\x00"  # request tagged fields

    response = automq_request(port, 514, correlation_id, body)
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
        for _ in range(tag_count):
            _, pos = read_compact_string(response, pos)
            _, pos = read_compact_string(response, pos)
            pos = skip_tags(response, pos)
        pos = skip_tags(response, pos)
        nodes.append(
            {
                "node_id": described_node_id,
                "node_epoch": node_epoch,
                "wal_config": wal_config,
                "state": state,
                "has_opening_streams": has_opening_streams,
            }
        )
    pos = skip_tags(response, pos)
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


def wait_for_automq_create_stream(port, node_id, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 9000
    last_error = None
    while time.time() < deadline:
        try:
            return automq_create_stream(port, node_id, correlation_id)
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ CreateStreams did not succeed within {timeout}s: {last_error}")


def wait_for_automq_open_stream(port, node_id, stream_id, stream_epoch, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 9300
    last_error = None
    while time.time() < deadline:
        try:
            return automq_open_stream(port, node_id, stream_id, stream_epoch, correlation_id)
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


def wait_for_automq_stream(
    port,
    stream_id,
    expected_state=None,
    expected_epoch=None,
    expected_start_offset=None,
    expected_end_offset=None,
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


def wait_for_automq_register_node(port, node_id, node_epoch, wal_config, timeout=45):
    deadline = time.time() + timeout
    correlation_id = 11000
    last_error = None
    while time.time() < deadline:
        try:
            automq_register_node(port, node_id, node_epoch, wal_config, correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.5)
    raise TestError(f"AutoMQ RegisterNode did not succeed within {timeout}s: {last_error}")


def wait_for_automq_node(port, node_id, expected_epoch, expected_wal_config, timeout=45):
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

        stream_owner_node_id = leader_id
        stream_id = wait_for_automq_create_stream(leader_broker_port, leader_id)
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
        wait_for_automq_stream(leader_broker_port, stream_id, "OPENED", 1, 0, 10)
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

        registered_node_id = 700 + leader_id
        registered_node_epoch = 42
        registered_wal_config = f"wal://automq-node-{registered_node_id}"
        wait_for_automq_register_node(
            leader_broker_port,
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
        )
        wait_for_automq_node(
            leader_broker_port,
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
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
            wait_for_automq_stream(info["broker_port"], stream_id, "OPENED", 1, 0, 10)
            wait_for_automq_opening_stream(
                info["broker_port"],
                stream_owner_node_id,
                stream_id,
                expected_epoch=1,
                expected_start_offset=0,
                expected_end_offset=10,
            )
            wait_for_automq_stream(info["broker_port"], deleted_stream_id, "OPENED", 1, 0, 0)
            wait_for_automq_node(
                info["broker_port"],
                registered_node_id,
                registered_node_epoch,
                registered_wal_config,
            )
            wait_for_automq_license(info["broker_port"], license_value)

        stop_process(processes[leader_id]["proc"], crash=True)
        replacement_leader, after = wait_for_leader(processes, forbidden_leaders={leader_id})
        if after["leader_epoch"] <= initial["leader_epoch"]:
            raise TestError(f"AutoMQ failover leader epoch did not advance: before={initial} after={after}")
        wait_for_all_alive_to_report(processes, replacement_leader)

        replacement_broker_port = processes[replacement_leader]["broker_port"]
        wait_for_automq_kv(replacement_broker_port, key, value_before)
        wait_for_automq_kv(replacement_broker_port, delete_key, delete_value)
        wait_for_automq_zone_router(replacement_broker_port, zone_router_before)
        wait_for_automq_stream(replacement_broker_port, stream_id, "OPENED", 1, 0, 10)
        wait_for_automq_opening_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=1,
            expected_start_offset=0,
            expected_end_offset=10,
        )
        wait_for_automq_stream(replacement_broker_port, deleted_stream_id, "OPENED", 1, 0, 0)
        wait_for_automq_node(
            replacement_broker_port,
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
        )
        wait_for_automq_license(replacement_broker_port, license_value)
        wait_for_automq_next_node_id(
            replacement_broker_port,
            cluster_id,
            expected_node_id=first_allocated_node_id + 1,
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

        wait_for_automq_close_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            1,
        )
        wait_for_automq_stream(replacement_broker_port, stream_id, "CLOSED", 1, 0, 10)
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
        )
        wait_for_automq_stream(replacement_broker_port, stream_id, "OPENED", 2, 0, 10)
        wait_for_automq_trim_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            2,
            5,
        )
        wait_for_automq_stream(replacement_broker_port, stream_id, "OPENED", 2, 5, 10)
        wait_for_automq_opening_stream(
            replacement_broker_port,
            stream_owner_node_id,
            stream_id,
            expected_epoch=2,
            expected_start_offset=5,
            expected_end_offset=10,
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
        wait_for_automq_stream(
            processes[leader_id]["broker_port"],
            stream_id,
            "OPENED",
            2,
            5,
            10,
        )
        wait_for_automq_opening_stream(
            processes[leader_id]["broker_port"],
            stream_owner_node_id,
            stream_id,
            expected_epoch=2,
            expected_start_offset=5,
            expected_end_offset=10,
        )
        wait_for_automq_stream_missing(processes[leader_id]["broker_port"], deleted_stream_id)
        wait_for_automq_opening_stream_missing(
            processes[leader_id]["broker_port"],
            stream_owner_node_id,
            deleted_stream_id,
        )
        wait_for_automq_node(
            processes[leader_id]["broker_port"],
            registered_node_id,
            registered_node_epoch,
            registered_wal_config,
        )
        wait_for_automq_license(processes[leader_id]["broker_port"], license_value)

        return {
            "old_leader": leader_id,
            "new_leader": replacement_leader,
            "stream_id": stream_id,
            "deleted_stream_id": deleted_stream_id,
            "registered_node_id": registered_node_id,
            "zone_router_epoch": zone_router_epoch_after,
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
        expected_payloads = []
        wait_for_topic(broker["port"], topic)
        expected_payloads.append(b"r0")
        first_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        wait_for_payloads(broker["port"], topic, expected_payloads)

        stop_process(processes[leader_id]["proc"], crash=True)
        replacement_leader, after = wait_for_leader(processes, forbidden_leaders={leader_id})
        alive = {node_id for node_id, info in processes.items() if info["proc"].poll() is None}
        if replacement_leader not in alive:
            raise TestError(f"replacement leader {replacement_leader} is not alive; alive={sorted(alive)}")
        if after["leader_epoch"] <= initial["leader_epoch"]:
            raise TestError(f"leader epoch did not advance: before={initial} after={after}")

        wait_for_all_alive_to_report(processes, replacement_leader)
        wait_for_payloads(broker["port"], topic, expected_payloads)
        expected_payloads.append(b"r1")
        second_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        if second_offset <= first_offset:
            raise TestError(f"broker did not continue after failover: {second_offset} <= {first_offset}")
        wait_for_payloads(broker["port"], topic, expected_payloads)

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
        expected_payloads.append(b"r2")
        third_offset = wait_for_produce(broker["port"], topic, expected_payloads[-1])
        if third_offset <= second_offset:
            raise TestError(
                f"broker did not continue after old leader rejoin: {third_offset} <= {second_offset}"
            )
        wait_for_payloads(broker["port"], topic, expected_payloads)

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
        expected_payloads.append(b"r3")
        fourth_offset = wait_for_produce(
            broker["port"], topic, expected_payloads[-1]
        )
        if fourth_offset <= third_offset:
            raise TestError(
                f"broker did not continue after controller restart: {fourth_offset} <= {third_offset}"
            )
        wait_for_payloads(broker["port"], topic, expected_payloads)

        stop_process(broker["proc"])
        broker = start_broker(tmp, voters)
        wait_for_broker_ready(broker["proc"], broker["port"], broker["log_path"])
        wait_for_payloads(broker["port"], topic, expected_payloads)
        expected_payloads.append(b"r4")
        fifth_offset = wait_for_produce(
            broker["port"], topic, expected_payloads[-1]
        )
        if fifth_offset <= fourth_offset:
            raise TestError(
                f"broker did not continue after broker restart: {fifth_offset} <= {fourth_offset}"
            )
        wait_for_payloads(broker["port"], topic, expected_payloads)

        automq_result = run_automq_metadata_failover_scenario(tmp)

        print(
            "ok: KRaft controller failover harness passed "
            f"(old_leader={leader_id}, new_leader={replacement_leader}, "
            f"restarted_controller={restart_controller_id}, "
            f"old_leader_rejoined=true, epoch={after['leader_epoch']}, "
            f"automq_old_leader={automq_result['old_leader']}, "
            f"automq_new_leader={automq_result['new_leader']}, "
            f"automq_stream_id={automq_result['stream_id']}, "
            f"automq_deleted_stream_id={automq_result['deleted_stream_id']}, "
            f"automq_node_id={automq_result['registered_node_id']}, "
            f"automq_zone_router_epoch={automq_result['zone_router_epoch']})"
        )
        return 0
    finally:
        if broker is not None:
            stop_process(broker.get("proc"))
        for info in processes.values():
            stop_process(info.get("proc"))
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except TestError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)

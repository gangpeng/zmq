#!/usr/bin/env python3
"""
Gated S3 WAL broker-process crash/restart harness.

This test exercises the real broker binary instead of in-process Zig structs:
it starts a broker against MinIO/S3, produces acknowledged data, kills the
process, restarts a replacement broker with a fresh local data directory, then
fetches the data back from S3 and appends new data.

Run only when external infrastructure is intentionally available:

    ZMQ_RUN_PROCESS_CRASH_TESTS=1 zig build test-s3-process-crash

Optional environment:
    ZMQ_BIN                    ./zig-out/bin/zmq
    ZMQ_S3_ENDPOINT            127.0.0.1
    ZMQ_S3_PORT                9000
    ZMQ_S3_BUCKET              unique bucket per run
    ZMQ_S3_ACCESS_KEY          minioadmin
    ZMQ_S3_SECRET_KEY          minioadmin
    ZMQ_S3_SCHEME              http
    ZMQ_S3_REGION              us-east-1
    ZMQ_S3_PATH_STYLE          true
    ZMQ_S3_TLS_CA_FILE         optional CA file for HTTPS providers
    ZMQ_S3_SKIP_MINIO_HEALTH   0; set to 1 for non-MinIO providers
    ZMQ_TEST_BROKER_PORT       29092
    ZMQ_TEST_CONTROLLER_PORT   29093
    ZMQ_TEST_METRICS_PORT      29090
"""

import os
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import zlib


ZMQ_BIN = os.environ.get("ZMQ_BIN", "./zig-out/bin/zmq")
S3_ENDPOINT = os.environ.get("ZMQ_S3_ENDPOINT", "127.0.0.1")
S3_PORT = 9000
S3_BUCKET = os.environ.get("ZMQ_S3_BUCKET", f"zmq-crash-{os.getpid()}-{int(time.time())}")
S3_ACCESS_KEY = os.environ.get("ZMQ_S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("ZMQ_S3_SECRET_KEY", "minioadmin")
S3_SCHEME = os.environ.get("ZMQ_S3_SCHEME", "http")
S3_REGION = os.environ.get("ZMQ_S3_REGION", "us-east-1")
S3_PATH_STYLE = os.environ.get("ZMQ_S3_PATH_STYLE", "true")
S3_TLS_CA_FILE = os.environ.get("ZMQ_S3_TLS_CA_FILE")
S3_SKIP_MINIO_HEALTH = os.environ.get("ZMQ_S3_SKIP_MINIO_HEALTH", "0")
BROKER_PORT = 29092
CONTROLLER_PORT = 29093
METRICS_PORT = 29090
CURRENT_LOG_PATH = None
LAST_LOG_TAIL = ""
PLACEHOLDER_SETTING_VALUES = {
    "...",
    "placeholder",
    "required",
    "tbd",
    "todo",
}
BOOL_TRUE_VALUES = {"1", "true", "yes", "on"}
BOOL_FALSE_VALUES = {"0", "false", "no", "off"}


class TestError(Exception):
    pass


def setting_uses_placeholder(value):
    stripped = str(value).strip()
    lowered = stripped.lower()
    angle_start = stripped.find("<")
    has_angle_placeholder = (
        angle_start >= 0
        and stripped.find(">", angle_start + 1) > angle_start + 1
    )
    return (
        lowered in PLACEHOLDER_SETTING_VALUES
        or lowered.startswith("/path/to/")
        or has_angle_placeholder
    )


def require_non_placeholder_setting(name, value):
    stripped = "" if value is None else str(value).strip()
    if not stripped:
        raise TestError(f"{name} is required")
    if setting_uses_placeholder(stripped):
        raise TestError(f"{name} must not use a placeholder value")
    return stripped


def require_positive_int_setting(name, value):
    stripped = require_non_placeholder_setting(name, value)
    try:
        parsed = int(stripped)
    except ValueError as exc:
        raise TestError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise TestError(f"{name} must be positive")
    if parsed > 65535:
        raise TestError(f"{name} must be a TCP port")
    return parsed


def require_bool_setting(name, value):
    stripped = require_non_placeholder_setting(name, value)
    lowered = stripped.lower()
    if lowered in BOOL_TRUE_VALUES:
        return True
    if lowered in BOOL_FALSE_VALUES:
        return False
    raise TestError(f"{name} must be true or false")


def require_bool_text_setting(name, value):
    return "true" if require_bool_setting(name, value) else "false"


def run_gate_enabled(name):
    return require_bool_setting(name, os.environ.get(name, "0"))


def validate_s3_config():
    global S3_ENDPOINT, S3_PORT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY
    global S3_SCHEME, S3_REGION, S3_PATH_STYLE, S3_TLS_CA_FILE
    global S3_SKIP_MINIO_HEALTH

    S3_ENDPOINT = require_non_placeholder_setting("ZMQ_S3_ENDPOINT", S3_ENDPOINT)
    S3_PORT = require_positive_int_setting(
        "ZMQ_S3_PORT",
        os.environ.get("ZMQ_S3_PORT", str(S3_PORT)),
    )
    S3_BUCKET = require_non_placeholder_setting("ZMQ_S3_BUCKET", S3_BUCKET)
    S3_ACCESS_KEY = require_non_placeholder_setting("ZMQ_S3_ACCESS_KEY", S3_ACCESS_KEY)
    S3_SECRET_KEY = require_non_placeholder_setting("ZMQ_S3_SECRET_KEY", S3_SECRET_KEY)
    S3_SCHEME = require_non_placeholder_setting("ZMQ_S3_SCHEME", S3_SCHEME)
    if S3_SCHEME not in ("http", "https"):
        raise TestError("ZMQ_S3_SCHEME must be http or https")
    S3_REGION = require_non_placeholder_setting("ZMQ_S3_REGION", S3_REGION)
    S3_PATH_STYLE = require_bool_text_setting("ZMQ_S3_PATH_STYLE", S3_PATH_STYLE)
    S3_SKIP_MINIO_HEALTH = require_bool_setting(
        "ZMQ_S3_SKIP_MINIO_HEALTH",
        S3_SKIP_MINIO_HEALTH,
    )
    if S3_TLS_CA_FILE is not None:
        S3_TLS_CA_FILE = require_non_placeholder_setting(
            "ZMQ_S3_TLS_CA_FILE",
            S3_TLS_CA_FILE,
        )


def validate_process_ports():
    global BROKER_PORT, CONTROLLER_PORT, METRICS_PORT

    BROKER_PORT = require_positive_int_setting(
        "ZMQ_TEST_BROKER_PORT",
        os.environ.get("ZMQ_TEST_BROKER_PORT", str(BROKER_PORT)),
    )
    CONTROLLER_PORT = require_positive_int_setting(
        "ZMQ_TEST_CONTROLLER_PORT",
        os.environ.get("ZMQ_TEST_CONTROLLER_PORT", str(CONTROLLER_PORT)),
    )
    METRICS_PORT = require_positive_int_setting(
        "ZMQ_TEST_METRICS_PORT",
        os.environ.get("ZMQ_TEST_METRICS_PORT", str(METRICS_PORT)),
    )


def recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise TestError(f"connection closed after {len(data)}/{size} bytes")
        data.extend(chunk)
    return bytes(data)


def write_string(value):
    raw = value.encode("utf-8")
    return struct.pack(">h", len(raw)) + raw


def kafka_request(api_key, api_version, correlation_id, body=b"", timeout=10):
    client_id = b"s3-crash-test"
    header = struct.pack(">hhi", api_key, api_version, correlation_id)
    header += struct.pack(">h", len(client_id)) + client_id
    frame_body = header + body

    with socket.create_connection(("127.0.0.1", BROKER_PORT), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack(">I", len(frame_body)) + frame_body)
        response_size = struct.unpack(">I", recv_exact(sock, 4))[0]
        return recv_exact(sock, response_size)


def api_versions_count():
    response = kafka_request(18, 0, 1)
    if len(response) < 10:
        raise TestError("ApiVersions response too short")
    correlation_id = struct.unpack_from(">i", response, 0)[0]
    if correlation_id != 1:
        raise TestError(f"ApiVersions correlation mismatch: {correlation_id}")
    error_code = struct.unpack_from(">h", response, 4)[0]
    if error_code != 0:
        raise TestError(f"ApiVersions error_code={error_code}")
    return struct.unpack_from(">i", response, 6)[0]


def create_topic(name, partitions=1):
    body = struct.pack(">i", 1)
    body += write_string(name)
    body += struct.pack(">i", partitions)
    body += struct.pack(">h", 1)
    body += struct.pack(">i", 0)  # replica assignment count
    body += struct.pack(">i", 0)  # configs count
    body += struct.pack(">i", 30000)
    response = kafka_request(19, 0, 2, body)
    body = response[4:]
    if len(body) < 8:
        raise TestError("CreateTopics response too short")
    pos = 4
    name_len = struct.unpack_from(">h", body, pos)[0]
    pos += 2 + max(name_len, 0)
    error_code = struct.unpack_from(">h", body, pos)[0]
    if error_code not in (0, 36):  # NONE or TOPIC_ALREADY_EXISTS
        raise TestError(f"CreateTopics error_code={error_code}")


def build_message_set(payload):
    message_body = (
        struct.pack(">bb", 0, 0)  # magic, attributes
        + struct.pack(">i", -1)  # null key
        + struct.pack(">i", len(payload))
        + payload
    )
    crc = zlib.crc32(message_body) & 0xFFFFFFFF
    message = struct.pack(">I", crc) + message_body
    return struct.pack(">qi", 0, len(message)) + message


def produce(topic, payload, correlation_id):
    message_set = build_message_set(payload)
    body = struct.pack(">h", 1)  # acks
    body += struct.pack(">i", 30000)
    body += struct.pack(">i", 1)
    body += write_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += struct.pack(">i", len(message_set)) + message_set

    response = kafka_request(0, 0, correlation_id, body)
    body = response[4:]
    if len(body) < 24:
        raise TestError("Produce response too short")
    pos = 4
    name_len = struct.unpack_from(">h", body, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", body, pos)[0]
    if partitions != 1:
        raise TestError(f"Produce partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", body, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", body, pos)[0]
    pos += 2
    base_offset = struct.unpack_from(">q", body, pos)[0]
    if partition != 0 or error_code != 0:
        raise TestError(f"Produce partition={partition} error_code={error_code}")
    return base_offset


def fetch_records(topic, offset, correlation_id):
    body = struct.pack(">i", -1)  # replica_id
    body += struct.pack(">i", 5000)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 1)
    body += write_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += struct.pack(">q", offset)
    body += struct.pack(">i", 1024 * 1024)

    response = kafka_request(1, 0, correlation_id, body)
    body = response[4:]
    if len(body) < 30:
        raise TestError("Fetch response too short")
    pos = 4
    name_len = struct.unpack_from(">h", body, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", body, pos)[0]
    if partitions != 1:
        raise TestError(f"Fetch partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", body, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", body, pos)[0]
    pos += 2
    high_watermark = struct.unpack_from(">q", body, pos)[0]
    pos += 8
    record_len = struct.unpack_from(">i", body, pos)[0]
    pos += 4
    if partition != 0 or error_code != 0:
        raise TestError(f"Fetch partition={partition} error_code={error_code}")
    if record_len < 0 or pos + record_len > len(body):
        raise TestError(f"Fetch invalid record_len={record_len}")
    return high_watermark, body[pos : pos + record_len]


def wait_for_broker(proc, log_path):
    deadline = time.monotonic() + 30
    last_error = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise TestError(f"broker exited early with code {proc.returncode}\n{tail(log_path)}")
        try:
            if api_versions_count() > 0:
                return
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)
    raise TestError(f"broker did not become ready: {last_error}\n{tail(log_path)}")


def wait_for_payload(topic, payloads):
    deadline = time.monotonic() + 20
    correlation_id = 100
    last_records = b""
    while time.monotonic() < deadline:
        _, records = fetch_records(topic, 0, correlation_id)
        correlation_id += 1
        last_records = records
        if all(payload in records for payload in payloads):
            return records
        time.sleep(0.25)
    missing = [payload for payload in payloads if payload not in last_records]
    raise TestError(f"missing payloads after fetch retry: {missing!r}")


def commit_offset(group, topic, offset, correlation_id):
    body = write_string(group)
    body += struct.pack(">i", -1)  # generation_id: simple commit
    body += write_string("")  # member_id
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)  # partition_index
    body += struct.pack(">q", offset)
    body += write_string("s3-process-crash")

    response = kafka_request(8, 5, correlation_id, body)
    payload = response[4:]
    if len(payload) < 18:
        raise TestError("OffsetCommit response too short")
    pos = 4  # throttle_time_ms
    topics = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    if topics != 1:
        raise TestError(f"OffsetCommit topic response count={topics}")
    name_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    if partitions != 1:
        raise TestError(f"OffsetCommit partition response count={partitions}")
    partition = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", payload, pos)[0]
    if partition != 0 or error_code != 0:
        raise TestError(f"OffsetCommit partition={partition} error_code={error_code}")


def fetch_committed_offset(group, topic, correlation_id):
    body = write_string(group)
    body += struct.pack(">i", 1)  # topics
    body += write_string(topic)
    body += struct.pack(">i", 1)  # partitions
    body += struct.pack(">i", 0)

    response = kafka_request(9, 1, correlation_id, body)
    payload = response[4:]
    if len(payload) < 26:
        raise TestError("OffsetFetch response too short")
    pos = 0
    topics = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    if topics != 1:
        raise TestError(f"OffsetFetch topic response count={topics}")
    name_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    if partitions != 1:
        raise TestError(f"OffsetFetch partition response count={partitions}")
    partition = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    committed = struct.unpack_from(">q", payload, pos)[0]
    pos += 8
    metadata_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(metadata_len, 0)
    error_code = struct.unpack_from(">h", payload, pos)[0]
    if partition != 0 or error_code != 0:
        raise TestError(f"OffsetFetch partition={partition} error_code={error_code}")
    return committed


def wait_for_committed_offset(group, topic, expected_offset):
    deadline = time.monotonic() + 20
    correlation_id = 200
    last_offset = None
    last_error = None
    while time.monotonic() < deadline:
        try:
            last_offset = fetch_committed_offset(group, topic, correlation_id)
            if last_offset == expected_offset:
                return
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    raise TestError(
        f"committed offset did not recover: expected={expected_offset} "
        f"last_offset={last_offset} last_error={last_error}"
    )


def minio_health_url():
    endpoint = S3_ENDPOINT.rstrip("/")
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.port is not None:
            return f"{endpoint}/minio/health/live"
        return f"{endpoint}:{S3_PORT}/minio/health/live"
    scheme = os.environ.get("ZMQ_S3_SCHEME", "http")
    return f"{scheme}://{endpoint}:{S3_PORT}/minio/health/live"


def require_minio():
    if S3_SKIP_MINIO_HEALTH:
        return
    try:
        with urllib.request.urlopen(minio_health_url(), timeout=3) as response:
            if response.status != 200:
                raise TestError(f"MinIO health status={response.status}")
    except Exception as exc:
        raise TestError(f"MinIO is not healthy at {minio_health_url()}: {exc}") from exc


def tail(path, limit=12000):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - limit), os.SEEK_SET)
            return f.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def start_broker(data_dir, log_path):
    global CURRENT_LOG_PATH
    CURRENT_LOG_PATH = log_path
    log_file = open(log_path, "ab", buffering=0)
    args = [
        ZMQ_BIN,
        str(BROKER_PORT),
        "--node-id",
        "0",
        "--process-roles",
        "controller,broker",
        "--controller-port",
        str(CONTROLLER_PORT),
        "--metrics-port",
        str(METRICS_PORT),
        "--data-dir",
        data_dir,
        "--s3-endpoint",
        S3_ENDPOINT,
        "--s3-port",
        str(S3_PORT),
        "--s3-bucket",
        S3_BUCKET,
        "--s3-access-key",
        S3_ACCESS_KEY,
        "--s3-secret-key",
        S3_SECRET_KEY,
        "--s3-scheme",
        S3_SCHEME,
        "--s3-region",
        S3_REGION,
        "--s3-path-style",
        S3_PATH_STYLE,
        "--cluster-id",
        "zmq-s3-process-crash",
        "--advertised-host",
        "localhost",
        "--s3-wal-batch-size",
        "1",
        "--s3-wal-flush-interval",
        "1",
        "--s3-wal-flush-mode",
        "sync",
        "--compaction-interval",
        "3600000",
    ]
    if S3_TLS_CA_FILE:
        args.extend(["--s3-ca-file", S3_TLS_CA_FILE])
    proc = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
    proc._zmq_log_file = log_file
    proc._zmq_log_path = log_path
    wait_for_broker(proc, log_path)
    return proc


def self_test():
    global S3_ENDPOINT, S3_PORT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY
    global S3_SCHEME, S3_REGION, S3_PATH_STYLE, S3_TLS_CA_FILE
    global S3_SKIP_MINIO_HEALTH
    global BROKER_PORT, CONTROLLER_PORT, METRICS_PORT

    payload = b"message-set-self-test"
    records = build_message_set(payload)
    if len(records) < 30:
        raise AssertionError("message set fixture is too short")

    base_offset, message_len = struct.unpack_from(">qi", records, 0)
    if base_offset != 0:
        raise AssertionError(f"message set base offset drifted: {base_offset}")
    if message_len != len(records) - 12:
        raise AssertionError(
            f"message length mismatch: {message_len} vs {len(records) - 12}"
        )

    message = records[12:]
    expected_crc = struct.unpack_from(">I", message, 0)[0]
    actual_crc = zlib.crc32(message[4:]) & 0xFFFFFFFF
    if expected_crc != actual_crc:
        raise AssertionError(f"message CRC mismatch: {expected_crc:#x} vs {actual_crc:#x}")

    magic, attributes = struct.unpack_from(">bb", message, 4)
    key_len = struct.unpack_from(">i", message, 6)[0]
    value_len = struct.unpack_from(">i", message, 10)[0]
    value = message[14 : 14 + value_len]
    if magic != 0 or attributes != 0 or key_len != -1 or value != payload:
        raise AssertionError("message set fixture layout drifted")

    old_env = os.environ.copy()
    old_settings = (
        S3_ENDPOINT,
        S3_PORT,
        S3_BUCKET,
        S3_ACCESS_KEY,
        S3_SECRET_KEY,
        S3_SCHEME,
        S3_REGION,
        S3_PATH_STYLE,
        S3_TLS_CA_FILE,
        S3_SKIP_MINIO_HEALTH,
        BROKER_PORT,
        CONTROLLER_PORT,
        METRICS_PORT,
    )
    try:
        os.environ["ZMQ_RUN_PROCESS_CRASH_TESTS"] = "placeholder"
        try:
            run_gate_enabled("ZMQ_RUN_PROCESS_CRASH_TESTS")
            raise AssertionError("placeholder S3 process-crash run gate was accepted")
        except TestError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_RUN_PROCESS_CRASH_TESTS"] = "   "
        try:
            run_gate_enabled("ZMQ_RUN_PROCESS_CRASH_TESTS")
            raise AssertionError("blank S3 process-crash run gate was accepted")
        except TestError as exc:
            if "ZMQ_RUN_PROCESS_CRASH_TESTS" not in str(exc):
                raise
        os.environ["ZMQ_RUN_PROCESS_CRASH_TESTS"] = "maybe"
        try:
            run_gate_enabled("ZMQ_RUN_PROCESS_CRASH_TESTS")
            raise AssertionError("invalid S3 process-crash run gate was accepted")
        except TestError as exc:
            if "true or false" not in str(exc):
                raise
        os.environ["ZMQ_RUN_PROCESS_CRASH_TESTS"] = "on"
        if not run_gate_enabled("ZMQ_RUN_PROCESS_CRASH_TESTS"):
            raise AssertionError("truthy S3 process-crash run gate was not accepted")
        os.environ.pop("ZMQ_RUN_PROCESS_CRASH_TESTS", None)

        os.environ["ZMQ_TEST_BROKER_PORT"] = "placeholder"
        try:
            validate_process_ports()
            raise AssertionError("placeholder S3 process-crash broker port was accepted")
        except TestError as exc:
            if "ZMQ_TEST_BROKER_PORT" not in str(exc) or "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_TEST_BROKER_PORT"] = "not-a-port"
        try:
            validate_process_ports()
            raise AssertionError("malformed S3 process-crash broker port was accepted")
        except TestError as exc:
            if "ZMQ_TEST_BROKER_PORT" not in str(exc) or "integer" not in str(exc):
                raise
        os.environ["ZMQ_TEST_BROKER_PORT"] = "0"
        try:
            validate_process_ports()
            raise AssertionError("non-positive S3 process-crash broker port was accepted")
        except TestError as exc:
            if "ZMQ_TEST_BROKER_PORT" not in str(exc) or "positive" not in str(exc):
                raise
        os.environ["ZMQ_TEST_BROKER_PORT"] = "29192"
        validate_process_ports()
        if BROKER_PORT != 29192:
            raise AssertionError("S3 process-crash broker port did not parse")
        os.environ.pop("ZMQ_TEST_BROKER_PORT", None)

        os.environ.pop("ZMQ_S3_PORT", None)
        S3_ENDPOINT = "placeholder"
        try:
            validate_s3_config()
            raise AssertionError("placeholder S3 process-crash endpoint was accepted")
        except TestError as exc:
            if "ZMQ_S3_ENDPOINT" not in str(exc):
                raise

        S3_ENDPOINT = "<host>"
        try:
            validate_s3_config()
            raise AssertionError("angle-bracket placeholder S3 process-crash endpoint was accepted")
        except TestError as exc:
            if "ZMQ_S3_ENDPOINT" not in str(exc) or "placeholder" not in str(exc):
                raise

        S3_ENDPOINT = "   "
        try:
            validate_s3_config()
            raise AssertionError("blank S3 process-crash endpoint was accepted")
        except TestError as exc:
            if "ZMQ_S3_ENDPOINT" not in str(exc):
                raise

        S3_ENDPOINT = "127.0.0.1"
        os.environ["ZMQ_S3_PORT"] = "   "
        try:
            validate_s3_config()
            raise AssertionError("blank S3 process-crash port was accepted")
        except TestError as exc:
            if "ZMQ_S3_PORT" not in str(exc):
                raise
        os.environ.pop("ZMQ_S3_PORT", None)

        S3_PORT = 0
        try:
            validate_s3_config()
            raise AssertionError("non-positive S3 process-crash port was accepted")
        except TestError as exc:
            if "ZMQ_S3_PORT" not in str(exc):
                raise

        S3_PORT = 9000
        S3_BUCKET = "required"
        try:
            validate_s3_config()
            raise AssertionError("placeholder S3 process-crash bucket was accepted")
        except TestError as exc:
            if "ZMQ_S3_BUCKET" not in str(exc):
                raise

        S3_BUCKET = "zmq-crash-self-test"
        S3_ACCESS_KEY = "   "
        try:
            validate_s3_config()
            raise AssertionError("blank S3 process-crash access key was accepted")
        except TestError as exc:
            if "ZMQ_S3_ACCESS_KEY" not in str(exc):
                raise

        S3_ACCESS_KEY = "minioadmin"
        S3_SECRET_KEY = "   "
        try:
            validate_s3_config()
            raise AssertionError("blank S3 process-crash secret key was accepted")
        except TestError as exc:
            if "ZMQ_S3_SECRET_KEY" not in str(exc):
                raise

        S3_SECRET_KEY = "minioadmin"
        S3_SCHEME = "ftp"
        try:
            validate_s3_config()
            raise AssertionError("invalid S3 process-crash scheme was accepted")
        except TestError as exc:
            if "ZMQ_S3_SCHEME" not in str(exc):
                raise

        S3_SCHEME = "http"
        S3_REGION = "   "
        try:
            validate_s3_config()
            raise AssertionError("blank S3 process-crash region was accepted")
        except TestError as exc:
            if "ZMQ_S3_REGION" not in str(exc):
                raise

        S3_REGION = "us-east-1"
        S3_PATH_STYLE = "maybe"
        try:
            validate_s3_config()
            raise AssertionError("invalid S3 process-crash path-style was accepted")
        except TestError as exc:
            if "ZMQ_S3_PATH_STYLE" not in str(exc):
                raise

        S3_PATH_STYLE = "yes"
        validate_s3_config()
        if S3_PATH_STYLE != "true":
            raise AssertionError("truthy S3 process-crash path-style flag did not parse")
        S3_PATH_STYLE = "off"
        validate_s3_config()
        if S3_PATH_STYLE != "false":
            raise AssertionError("false S3 process-crash path-style flag did not parse")

        S3_PATH_STYLE = "true"
        S3_SKIP_MINIO_HEALTH = "placeholder"
        try:
            validate_s3_config()
            raise AssertionError("placeholder S3 process-crash skip-health flag was accepted")
        except TestError as exc:
            if "ZMQ_S3_SKIP_MINIO_HEALTH" not in str(exc):
                raise

        S3_SKIP_MINIO_HEALTH = "sometimes"
        try:
            validate_s3_config()
            raise AssertionError("invalid S3 process-crash skip-health flag was accepted")
        except TestError as exc:
            if "ZMQ_S3_SKIP_MINIO_HEALTH" not in str(exc) or "true or false" not in str(exc):
                raise

        S3_SKIP_MINIO_HEALTH = "yes"
        validate_s3_config()
        if S3_SKIP_MINIO_HEALTH is not True:
            raise AssertionError("truthy S3 process-crash skip-health flag did not parse")
        S3_SKIP_MINIO_HEALTH = "0"
        validate_s3_config()
        if S3_SKIP_MINIO_HEALTH is not False:
            raise AssertionError("false S3 process-crash skip-health flag did not parse")

        S3_TLS_CA_FILE = "   "
        try:
            validate_s3_config()
            raise AssertionError("blank S3 process-crash TLS CA file was accepted")
        except TestError as exc:
            if "ZMQ_S3_TLS_CA_FILE" not in str(exc):
                raise
        S3_TLS_CA_FILE = None

        S3_ENDPOINT = "127.0.0.1"
        S3_PORT = 9000
        if minio_health_url() != "http://127.0.0.1:9000/minio/health/live":
            raise AssertionError(
                f"default MinIO health URL drifted: {minio_health_url()}"
            )

        S3_ENDPOINT = "http://s3.example.test"
        S3_PORT = 19000
        if minio_health_url() != "http://s3.example.test:19000/minio/health/live":
            raise AssertionError(
                f"scheme-qualified MinIO health URL drifted: {minio_health_url()}"
            )
    finally:
        (
            S3_ENDPOINT,
            S3_PORT,
            S3_BUCKET,
            S3_ACCESS_KEY,
            S3_SECRET_KEY,
            S3_SCHEME,
            S3_REGION,
            S3_PATH_STYLE,
            S3_TLS_CA_FILE,
            S3_SKIP_MINIO_HEALTH,
            BROKER_PORT,
            CONTROLLER_PORT,
            METRICS_PORT,
        ) = old_settings
        os.environ.clear()
        os.environ.update(old_env)

    summary = process_crash_summary("topic-self-test", "group-self-test", 0, 1, 2)
    expected_fragments = (
        "ok: S3 process crash/replacement harness passed ",
        "bucket=",
        "topic=topic-self-test",
        "group=group-self-test",
        "killed_broker=true",
        "fresh_data_dir=true",
        "first_offset=0",
        "committed_offset=1",
        "replacement_offset=2",
        "recovered_payloads=2",
        "source=command",
    )
    for fragment in expected_fragments:
        if fragment not in summary:
            raise AssertionError(f"process crash summary missing {fragment!r}")

    print("ok: S3 process crash harness self-test")
    return 0


def stop_broker(proc, crash=False):
    global LAST_LOG_TAIL
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
        log_path = getattr(proc, "_zmq_log_path", None)
        if log_path:
            LAST_LOG_TAIL = tail(log_path)
        log_file = getattr(proc, "_zmq_log_file", None)
        if log_file is not None:
            log_file.close()


def process_crash_summary(topic, group, first_offset, committed_offset, second_offset):
    return (
        "ok: S3 process crash/replacement harness passed "
        f"(bucket={S3_BUCKET}, topic={topic}, group={group}, "
        "killed_broker=true, fresh_data_dir=true, "
        f"first_offset={first_offset}, committed_offset={committed_offset}, "
        f"replacement_offset={second_offset}, recovered_payloads=2) source=command"
    )


def main():
    if "--self-test" in sys.argv:
        return self_test()

    if not run_gate_enabled("ZMQ_RUN_PROCESS_CRASH_TESTS"):
        print("skip: set ZMQ_RUN_PROCESS_CRASH_TESTS=1 to run S3 process crash harness")
        return 0
    if not os.path.exists(ZMQ_BIN):
        raise TestError(f"broker binary not found: {ZMQ_BIN}")

    validate_s3_config()
    validate_process_ports()
    require_minio()
    tmp = tempfile.mkdtemp(prefix="zmq-s3-crash-")
    proc = None
    topic = f"s3-crash-{os.getpid()}-{int(time.time())}"
    group = f"s3-crash-group-{os.getpid()}-{int(time.time())}"
    first = b"before-kill"
    second = b"after-replacement"
    try:
        data_a = os.path.join(tmp, "broker-a")
        data_b = os.path.join(tmp, "broker-b")
        log_a = os.path.join(tmp, "broker-a.log")
        log_b = os.path.join(tmp, "broker-b.log")

        proc = start_broker(data_a, log_a)
        create_topic(topic)
        first_offset = produce(topic, first, 10)
        if first_offset != 0:
            raise TestError(f"expected first offset 0, got {first_offset}")
        wait_for_payload(topic, [first])
        commit_offset(group, topic, 1, 30)
        wait_for_committed_offset(group, topic, 1)

        stop_broker(proc, crash=True)
        proc = None
        shutil.rmtree(data_a, ignore_errors=True)

        proc = start_broker(data_b, log_b)
        wait_for_payload(topic, [first])
        wait_for_committed_offset(group, topic, 1)
        second_offset = produce(topic, second, 20)
        if second_offset <= first_offset:
            raise TestError(f"replacement did not advance offsets: {second_offset} <= {first_offset}")
        wait_for_payload(topic, [first, second])
        print(process_crash_summary(topic, group, first_offset, 1, second_offset))
        return 0
    finally:
        stop_broker(proc)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except TestError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        if LAST_LOG_TAIL:
            print("broker log tail:", file=sys.stderr)
            print(LAST_LOG_TAIL, file=sys.stderr)
        elif CURRENT_LOG_PATH:
            print("broker log tail:", file=sys.stderr)
            print(tail(CURRENT_LOG_PATH), file=sys.stderr)
        sys.exit(1)

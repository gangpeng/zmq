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


def env_int(name, default):
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


RUN_ENABLED = os.environ.get("ZMQ_RUN_PROCESS_CRASH_TESTS") == "1"
ZMQ_BIN = os.environ.get("ZMQ_BIN", "./zig-out/bin/zmq")
S3_ENDPOINT = os.environ.get("ZMQ_S3_ENDPOINT", "127.0.0.1")
S3_PORT = env_int("ZMQ_S3_PORT", 9000)
S3_BUCKET = os.environ.get("ZMQ_S3_BUCKET", f"zmq-crash-{os.getpid()}-{int(time.time())}")
S3_ACCESS_KEY = os.environ.get("ZMQ_S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("ZMQ_S3_SECRET_KEY", "minioadmin")
S3_SCHEME = os.environ.get("ZMQ_S3_SCHEME", "http")
S3_REGION = os.environ.get("ZMQ_S3_REGION", "us-east-1")
S3_PATH_STYLE = os.environ.get("ZMQ_S3_PATH_STYLE", "true")
S3_TLS_CA_FILE = os.environ.get("ZMQ_S3_TLS_CA_FILE")
S3_SKIP_MINIO_HEALTH = os.environ.get("ZMQ_S3_SKIP_MINIO_HEALTH", "0").lower() in ("1", "true", "yes")
BROKER_PORT = env_int("ZMQ_TEST_BROKER_PORT", 29092)
CONTROLLER_PORT = env_int("ZMQ_TEST_CONTROLLER_PORT", 29093)
METRICS_PORT = env_int("ZMQ_TEST_METRICS_PORT", 29090)
CURRENT_LOG_PATH = None
LAST_LOG_TAIL = ""


class TestError(Exception):
    pass


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
    deadline = time.time() + 30
    last_error = None
    while time.time() < deadline:
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
    deadline = time.time() + 20
    correlation_id = 100
    last_records = b""
    while time.time() < deadline:
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
    deadline = time.time() + 20
    correlation_id = 200
    last_offset = None
    last_error = None
    while time.time() < deadline:
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


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_PROCESS_CRASH_TESTS=1 to run S3 process crash harness")
        return 0
    if not os.path.exists(ZMQ_BIN):
        raise TestError(f"broker binary not found: {ZMQ_BIN}")

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
        print("ok: S3 process crash/replacement harness passed")
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

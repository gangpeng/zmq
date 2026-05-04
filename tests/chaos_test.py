#!/usr/bin/env python3
"""
Gated broker chaos harness.

The default build/test suite stays deterministic; this harness runs only when
explicitly enabled. Safe local scenarios require no Docker or cloud services:

    ZMQ_RUN_CHAOS_TESTS=1 zig build test-chaos

Optional environment:
    ZMQ_BIN                      ./zig-out/bin/zmq
    ZMQ_CHAOS_SCENARIOS          comma-separated scenario names, or "all"
    ZMQ_CHAOS_REQUIRED_SCENARIOS fail if selected scenarios omit any required names
    ZMQ_CHAOS_BROKER_PORT        fixed broker port instead of an ephemeral port
    ZMQ_CHAOS_CONTROLLER_PORT    fixed controller port
    ZMQ_CHAOS_METRICS_PORT       fixed metrics port
    ZMQ_CHAOS_NETWORK_DOWN       command run to inject a network partition
    ZMQ_CHAOS_NETWORK_UP         command run to heal a network partition
    ZMQ_CHAOS_NETWORK_EXPECT     "fail" (default) or "survive" for probe during partition
    ZMQ_CHAOS_NETWORK_MATRIX     comma-separated named network partition phases
    ZMQ_CHAOS_NETWORK_<PHASE>_{DOWN,UP,EXPECT}
                                 per-phase network partition hooks/expectation
    ZMQ_CHAOS_REQUIRED_NETWORK_PHASES
                                 fail if matrix omits any required phase
    ZMQ_CHAOS_S3_DOWN            command run to inject live S3 provider outage
    ZMQ_CHAOS_S3_UP              command run to heal live S3 provider outage
    ZMQ_CHAOS_S3_* or ZMQ_S3_*   live S3 settings for live-s3-outage scenario

Safe default scenarios:
    sigkill-restart              kill -9 and restart from the same local WAL
    slow-partial-client          hold half-open/truncated client frames
    clock-skewed-records         accept a far-future client timestamp without crashing
    s3-outage                    fail closed when sync S3 WAL cannot reach object storage

The network-partition scenario is available for CI jobs that provide explicit
traffic-control hooks through ZMQ_CHAOS_NETWORK_DOWN/UP.

The live-s3-outage scenario is available for CI jobs that provide an actual
S3-compatible endpoint plus explicit outage/heal hooks through
ZMQ_CHAOS_S3_DOWN/UP.
"""

import os
import shlex
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time


RUN_ENABLED = os.environ.get("ZMQ_RUN_CHAOS_TESTS") == "1"
ZMQ_BIN = os.environ.get("ZMQ_BIN", "./zig-out/bin/zmq")
DEFAULT_SCENARIOS = [
    "sigkill-restart",
    "slow-partial-client",
    "clock-skewed-records",
    "s3-outage",
]
ALIASES = {
    "sigkill": "sigkill-restart",
    "partial-client": "slow-partial-client",
    "clock-skew": "clock-skewed-records",
    "s3": "s3-outage",
    "network": "network-partition",
    "live-s3": "live-s3-outage",
    "s3-live": "live-s3-outage",
}


class TestError(Exception):
    pass


def env_int(name, default):
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def split_csv(raw):
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_phase_token(name):
    token = []
    for ch in name.upper():
        if ch.isalnum():
            token.append(ch)
        else:
            token.append("_")
    collapsed = "_".join(part for part in "".join(token).split("_") if part)
    if not collapsed:
        raise TestError("empty chaos matrix phase name")
    return collapsed


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def choose_port(name):
    configured = os.environ.get(name)
    if configured:
        return env_int(name, 0)
    return free_port()


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


def write_bytes(value):
    return struct.pack(">i", len(value)) + value


def kafka_request(port, api_key, api_version, correlation_id, body=b"", timeout=10):
    client_id = b"zmq-chaos-test"
    header = struct.pack(">hhi", api_key, api_version, correlation_id)
    header += struct.pack(">h", len(client_id)) + client_id
    frame_body = header + body

    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(struct.pack(">I", len(frame_body)) + frame_body)
        response_size = struct.unpack(">I", recv_exact(sock, 4))[0]
        if response_size <= 0 or response_size > 16 * 1024 * 1024:
            raise TestError(f"invalid response frame size {response_size}")
        return recv_exact(sock, response_size)


def api_versions_count(port):
    response = kafka_request(port, 18, 0, 1)
    if len(response) < 10:
        raise TestError("ApiVersions response too short")
    correlation_id = struct.unpack_from(">i", response, 0)[0]
    if correlation_id != 1:
        raise TestError(f"ApiVersions correlation mismatch: {correlation_id}")
    error_code = struct.unpack_from(">h", response, 4)[0]
    if error_code != 0:
        raise TestError(f"ApiVersions error_code={error_code}")
    return struct.unpack_from(">i", response, 6)[0]


def create_topic(port, name, partitions=1, correlation_id=2):
    body = struct.pack(">i", 1)
    body += write_string(name)
    body += struct.pack(">i", partitions)
    body += struct.pack(">h", 1)
    body += struct.pack(">i", 0)  # replica assignment count
    body += struct.pack(">i", 0)  # configs count
    body += struct.pack(">i", 30000)
    response = kafka_request(port, 19, 0, correlation_id, body)
    payload = response[4:]
    if len(payload) < 8:
        raise TestError("CreateTopics response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(name_len, 0)
    error_code = struct.unpack_from(">h", payload, pos)[0]
    if error_code not in (0, 36):  # NONE or TOPIC_ALREADY_EXISTS
        raise TestError(f"CreateTopics error_code={error_code}")


def wait_for_topic(port, name, timeout=20):
    deadline = time.time() + timeout
    correlation_id = 2
    last_error = None
    while time.time() < deadline:
        try:
            create_topic(port, name, correlation_id=correlation_id)
            return
        except Exception as exc:
            last_error = exc
            correlation_id += 1
            time.sleep(0.25)
    raise TestError(f"topic {name!r} was not created: {last_error}")


def produce_result(port, topic, records, correlation_id):
    body = struct.pack(">h", 1)  # acks
    body += struct.pack(">i", 30000)
    body += struct.pack(">i", 1)
    body += write_string(topic)
    body += struct.pack(">i", 1)
    body += struct.pack(">i", 0)
    body += write_bytes(records)

    response = kafka_request(port, 0, 0, correlation_id, body)
    payload = response[4:]
    if len(payload) < 24:
        raise TestError("Produce response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload, pos)[0]
    if partitions != 1:
        raise TestError(f"Produce partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", payload, pos)[0]
    pos += 2
    base_offset = struct.unpack_from(">q", payload, pos)[0]
    if partition != 0:
        raise TestError(f"Produce returned partition={partition}")
    return error_code, base_offset


def produce(port, topic, records, correlation_id):
    error_code, base_offset = produce_result(port, topic, records, correlation_id)
    if error_code != 0:
        raise TestError(f"Produce error_code={error_code}")
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

    response = kafka_request(port, 1, 0, correlation_id, body)
    payload = response[4:]
    if len(payload) < 30:
        raise TestError("Fetch response too short")
    pos = 4
    name_len = struct.unpack_from(">h", payload, pos)[0]
    pos += 2 + max(name_len, 0)
    partitions = struct.unpack_from(">i", payload, pos)[0]
    if partitions != 1:
        raise TestError(f"Fetch partition response count={partitions}")
    pos += 4
    partition = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    error_code = struct.unpack_from(">h", payload, pos)[0]
    pos += 2
    high_watermark = struct.unpack_from(">q", payload, pos)[0]
    pos += 8
    record_len = struct.unpack_from(">i", payload, pos)[0]
    pos += 4
    if partition != 0 or error_code != 0:
        raise TestError(f"Fetch partition={partition} error_code={error_code}")
    if record_len < 0 or pos + record_len > len(payload):
        raise TestError(f"Fetch invalid record_len={record_len}")
    return high_watermark, payload[pos : pos + record_len]


def wait_for_payload(port, topic, payloads, timeout=20):
    deadline = time.time() + timeout
    correlation_id = 100
    last_records = b""
    last_error = None
    while time.time() < deadline:
        try:
            _, records = fetch_records(port, topic, 0, correlation_id)
            last_records = records
            if all(payload in records for payload in payloads):
                return records
        except Exception as exc:
            last_error = exc
        correlation_id += 1
        time.sleep(0.25)
    missing = [payload for payload in payloads if payload not in last_records]
    raise TestError(f"missing payloads after fetch retry: {missing!r}; last_error={last_error}")


def build_record_batch(value, timestamp_ms):
    record = b"\x00"  # attributes
    record += b"\x00"  # timestamp delta
    record += b"\x00"  # offset delta
    record += b"\x02"  # null key marker, accepted by existing wire tests
    record += struct.pack("B", len(value) * 2)
    record += value
    record += b"\x00"  # headers count

    batch_body = struct.pack(">i", 0)  # partition_leader_epoch
    batch_body += struct.pack("B", 2)  # magic
    batch_body += struct.pack(">I", 0)  # crc placeholder
    batch_body += struct.pack(">h", 0)  # attributes
    batch_body += struct.pack(">i", 0)  # last_offset_delta
    batch_body += struct.pack(">q", timestamp_ms)
    batch_body += struct.pack(">q", timestamp_ms)
    batch_body += struct.pack(">q", -1)  # producer_id
    batch_body += struct.pack(">h", -1)  # producer_epoch
    batch_body += struct.pack(">i", -1)  # base_sequence
    batch_body += struct.pack(">i", 1)  # record_count
    batch_body += struct.pack("B", len(record))
    batch_body += record

    return struct.pack(">q", 0) + struct.pack(">i", len(batch_body)) + batch_body


def current_time_ms():
    return int(time.time() * 1000)


def setting_with_fallback(prefix, suffix, fallback_prefix=None, default=None):
    value = os.environ.get(f"{prefix}{suffix}")
    if value is not None and value != "":
        return value
    if fallback_prefix:
        value = os.environ.get(f"{fallback_prefix}{suffix}")
        if value is not None and value != "":
            return value
    return default


def live_s3_config_from_env():
    endpoint = setting_with_fallback("ZMQ_CHAOS_S3_", "ENDPOINT", "ZMQ_S3_", None)
    if not endpoint:
        raise TestError("live-s3-outage requires ZMQ_CHAOS_S3_ENDPOINT or ZMQ_S3_ENDPOINT")
    return {
        "endpoint": endpoint,
        "port": setting_with_fallback("ZMQ_CHAOS_S3_", "PORT", "ZMQ_S3_", "9000"),
        "bucket": setting_with_fallback("ZMQ_CHAOS_S3_", "BUCKET", "ZMQ_S3_", f"zmq-chaos-{os.getpid()}"),
        "access_key": setting_with_fallback("ZMQ_CHAOS_S3_", "ACCESS_KEY", "ZMQ_S3_", "minioadmin"),
        "secret_key": setting_with_fallback("ZMQ_CHAOS_S3_", "SECRET_KEY", "ZMQ_S3_", "minioadmin"),
        "scheme": setting_with_fallback("ZMQ_CHAOS_S3_", "SCHEME", "ZMQ_S3_", None),
        "region": setting_with_fallback("ZMQ_CHAOS_S3_", "REGION", "ZMQ_S3_", None),
        "path_style": setting_with_fallback("ZMQ_CHAOS_S3_", "PATH_STYLE", "ZMQ_S3_", None),
        "tls_ca_file": setting_with_fallback("ZMQ_CHAOS_S3_", "TLS_CA_FILE", "ZMQ_S3_", None),
    }


def append_s3_args(args, config):
    args += [
        "--s3-endpoint",
        config["endpoint"],
        "--s3-port",
        str(config["port"]),
        "--s3-bucket",
        config["bucket"],
        "--s3-access-key",
        config["access_key"],
        "--s3-secret-key",
        config["secret_key"],
        "--s3-wal-batch-size",
        "1",
        "--s3-wal-flush-interval",
        "1",
        "--s3-wal-flush-mode",
        "sync",
    ]
    optional_flags = [
        ("scheme", "--s3-scheme"),
        ("region", "--s3-region"),
        ("path_style", "--s3-path-style"),
        ("tls_ca_file", "--s3-tls-ca-file"),
    ]
    for key, flag in optional_flags:
        value = config.get(key)
        if value is not None and value != "":
            args += [flag, str(value)]


def tail(path, limit=12000):
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - limit), os.SEEK_SET)
            return f.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def wait_for_broker(proc, port, log_path, timeout=40):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        if proc.poll() is not None:
            raise TestError(f"broker exited early with code {proc.returncode}\n{tail(log_path)}")
        try:
            if api_versions_count(port) > 0:
                return
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)
    raise TestError(f"broker did not become ready: {last_error}\n{tail(log_path)}")


def launch_broker(data_dir, log_path, s3_port=None, s3_config=None):
    broker_port = choose_port("ZMQ_CHAOS_BROKER_PORT")
    controller_port = choose_port("ZMQ_CHAOS_CONTROLLER_PORT")
    metrics_port = choose_port("ZMQ_CHAOS_METRICS_PORT")
    log_file = open(log_path, "ab", buffering=0)
    cluster_id = f"zmq-chaos-{os.getpid()}-{int(time.time())}"
    args = [
        ZMQ_BIN,
        str(broker_port),
        "--node-id",
        "0",
        "--process-roles",
        "controller,broker",
        "--controller-port",
        str(controller_port),
        "--metrics-port",
        str(metrics_port),
        "--data-dir",
        data_dir,
        "--cluster-id",
        cluster_id,
        "--advertised-host",
        "localhost",
        "--workers",
        "1",
        "--compaction-interval",
        "3600000",
    ]
    if s3_config is not None:
        append_s3_args(args, s3_config)
    elif s3_port is not None:
        append_s3_args(args, {
            "endpoint": "127.0.0.1",
            "port": s3_port,
            "bucket": f"zmq-chaos-{os.getpid()}",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
        })
    proc = subprocess.Popen(args, stdout=log_file, stderr=subprocess.STDOUT)
    proc._zmq_log_file = log_file
    proc._zmq_port = broker_port
    return proc, broker_port


def start_broker(data_dir, log_path, s3_port=None, s3_config=None):
    proc, broker_port = launch_broker(data_dir, log_path, s3_port=s3_port, s3_config=s3_config)
    wait_for_broker(proc, broker_port, log_path)
    return proc, broker_port


def stop_broker(proc, crash=False):
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


def scenario_sigkill_restart(tmp):
    data_dir = os.path.join(tmp, "sigkill-data")
    log_a = os.path.join(tmp, "sigkill-a.log")
    log_b = os.path.join(tmp, "sigkill-b.log")
    topic = f"chaos-sigkill-{os.getpid()}"
    first = b"before-sigkill"
    second = b"after-restart"
    proc = None
    try:
        proc, port = start_broker(data_dir, log_a)
        wait_for_topic(port, topic)
        first_offset = produce(port, topic, build_record_batch(first, current_time_ms()), 10)
        if first_offset != 0:
            raise TestError(f"expected first offset 0, got {first_offset}")
        wait_for_payload(port, topic, [first])

        stop_broker(proc, crash=True)
        proc = None
        time.sleep(0.25)

        proc, port = start_broker(data_dir, log_b)
        wait_for_payload(port, topic, [first])
        second_offset = produce(port, topic, build_record_batch(second, current_time_ms()), 11)
        if second_offset <= first_offset:
            raise TestError(f"restart did not advance offsets: {second_offset} <= {first_offset}")
        wait_for_payload(port, topic, [first, second])
        print("ok: chaos sigkill-restart")
    finally:
        stop_broker(proc)


def scenario_slow_partial_client(tmp):
    data_dir = os.path.join(tmp, "partial-client-data")
    log_path = os.path.join(tmp, "partial-client.log")
    proc = None
    try:
        proc, port = start_broker(data_dir, log_path)

        slow = socket.create_connection(("127.0.0.1", port), timeout=5)
        slow.settimeout(5)
        try:
            slow.sendall(b"\x00\x00")
            time.sleep(0.5)
            if api_versions_count(port) <= 0:
                raise TestError("broker stopped serving while a partial frame was open")
            slow.sendall(b"\x00\x20\x00\x12")
            time.sleep(0.25)
        finally:
            slow.close()

        truncated = socket.create_connection(("127.0.0.1", port), timeout=5)
        try:
            truncated.sendall(struct.pack(">I", 64) + b"\x00\x12\x00")
        finally:
            truncated.close()
        time.sleep(0.5)

        if proc.poll() is not None:
            raise TestError(f"broker exited after partial client frame\n{tail(log_path)}")
        if api_versions_count(port) <= 0:
            raise TestError("broker did not recover after truncated client frame")
        print("ok: chaos slow-partial-client")
    finally:
        stop_broker(proc)


def scenario_clock_skewed_records(tmp):
    data_dir = os.path.join(tmp, "clock-skew-data")
    log_path = os.path.join(tmp, "clock-skew.log")
    topic = f"chaos-clock-skew-{os.getpid()}"
    payload = b"future-client-timestamp"
    future_ms = current_time_ms() + 30 * 24 * 60 * 60 * 1000
    proc = None
    try:
        proc, port = start_broker(data_dir, log_path)
        wait_for_topic(port, topic)
        produce(port, topic, build_record_batch(payload, future_ms), 20)
        wait_for_payload(port, topic, [payload])
        if api_versions_count(port) <= 0:
            raise TestError("broker stopped serving after skewed timestamp batch")
        print("ok: chaos clock-skewed-records")
    finally:
        stop_broker(proc)


def scenario_s3_outage(tmp):
    data_dir = os.path.join(tmp, "s3-outage-data")
    log_path = os.path.join(tmp, "s3-outage.log")
    topic = f"chaos-s3-outage-{os.getpid()}"
    outage_port = free_port()
    payload = b"must-not-ack-when-s3-is-down"
    proc = None
    try:
        proc, port = launch_broker(data_dir, log_path, s3_port=outage_port)
        deadline = time.time() + 45
        while time.time() < deadline and proc.poll() is None:
            try:
                if api_versions_count(port) > 0:
                    break
            except Exception:
                time.sleep(0.25)

        if proc.poll() is not None:
            log_tail = tail(log_path)
            if proc.returncode == 0:
                raise TestError(f"S3 outage startup failed with success exit code\n{log_tail}")
            if "Failed to open storage" not in log_tail:
                raise TestError(f"S3 outage startup failed without storage error\n{log_tail}")
            print("ok: chaos s3-outage failed closed during startup")
            return

        error_code, base_offset = produce_result(port, topic, payload, 30)
        if error_code == 0 or base_offset >= 0:
            raise TestError(f"S3 outage produce was acknowledged: error={error_code} offset={base_offset}")
        if api_versions_count(port) <= 0:
            raise TestError("broker stopped serving after S3 WAL outage")
        print(f"ok: chaos s3-outage rejected produce with error_code={error_code}")
    finally:
        stop_broker(proc)


def run_hook(env_name):
    raw = os.environ.get(env_name)
    if not raw:
        raise TestError(f"{env_name} is required for network-partition scenario")
    proc = subprocess.run(
        shlex.split(raw),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise TestError(f"{env_name} failed with exit code {proc.returncode}\n{proc.stdout}")


def network_partition_phases():
    phases = split_csv(os.environ.get("ZMQ_CHAOS_NETWORK_MATRIX"))
    if not phases:
        return [None]
    seen = []
    for phase in phases:
        if phase not in seen:
            seen.append(phase)
    return seen


def network_phase_env(phase, suffix):
    if phase is None:
        return f"ZMQ_CHAOS_NETWORK_{suffix}"
    specific = f"ZMQ_CHAOS_NETWORK_{env_phase_token(phase)}_{suffix}"
    if os.environ.get(specific):
        return specific
    return f"ZMQ_CHAOS_NETWORK_{suffix}"


def network_phase_expect(phase):
    if phase is None:
        return os.environ.get("ZMQ_CHAOS_NETWORK_EXPECT", "fail")
    specific = f"ZMQ_CHAOS_NETWORK_{env_phase_token(phase)}_EXPECT"
    return os.environ.get(specific, os.environ.get("ZMQ_CHAOS_NETWORK_EXPECT", "fail"))


def scenario_network_partition(tmp):
    data_dir = os.path.join(tmp, "network-partition-data")
    log_path = os.path.join(tmp, "network-partition.log")
    topic = f"chaos-network-{os.getpid()}"
    payload = b"before-network-partition"
    proc = None
    healed = False
    try:
        proc, port = start_broker(data_dir, log_path)
        wait_for_topic(port, topic)
        produce(port, topic, build_record_batch(payload, current_time_ms()), 40)
        wait_for_payload(port, topic, [payload])

        for phase in network_partition_phases():
            down_env = network_phase_env(phase, "DOWN")
            up_env = network_phase_env(phase, "UP")
            run_hook(down_env)
            try:
                expect = network_phase_expect(phase)
                try:
                    api_versions_count(port)
                    probe_succeeded = True
                except Exception:
                    probe_succeeded = False
                if expect == "fail" and probe_succeeded:
                    raise TestError("network partition probe unexpectedly succeeded")
                if expect == "survive" and not probe_succeeded:
                    raise TestError("network partition probe unexpectedly failed")
            finally:
                run_hook(up_env)
                healed = True

            wait_for_broker(proc, port, log_path)
            wait_for_payload(port, topic, [payload])
            if phase is not None:
                print(f"ok: chaos network-partition phase {phase}")

        print("ok: chaos network-partition")
    finally:
        if not healed and os.environ.get("ZMQ_CHAOS_NETWORK_UP"):
            try:
                run_hook("ZMQ_CHAOS_NETWORK_UP")
            except TestError as exc:
                print(f"WARN: failed to heal network partition: {exc}", file=sys.stderr)
        stop_broker(proc)


def scenario_live_s3_outage(tmp):
    data_dir = os.path.join(tmp, "live-s3-outage-data")
    log_path = os.path.join(tmp, "live-s3-outage.log")
    topic = f"chaos-live-s3-{os.getpid()}"
    before = b"before-live-s3-outage"
    during = b"during-live-s3-outage"
    after = b"after-live-s3-outage"
    proc = None
    healed = False
    s3_config = live_s3_config_from_env()
    try:
        proc, port = start_broker(data_dir, log_path, s3_config=s3_config)
        wait_for_topic(port, topic)
        produce(port, topic, build_record_batch(before, current_time_ms()), 50)
        wait_for_payload(port, topic, [before])

        run_hook("ZMQ_CHAOS_S3_DOWN")
        try:
            try:
                error_code, base_offset = produce_result(port, topic, build_record_batch(during, current_time_ms()), 51)
                if error_code == 0 or base_offset >= 0:
                    raise TestError(
                        f"live S3 outage produce was acknowledged: error={error_code} offset={base_offset}"
                    )
            except (OSError, socket.timeout, TestError) as exc:
                if "acknowledged" in str(exc):
                    raise
        finally:
            run_hook("ZMQ_CHAOS_S3_UP")
            healed = True

        wait_for_broker(proc, port, log_path)
        produce(port, topic, build_record_batch(after, current_time_ms()), 52)
        wait_for_payload(port, topic, [before, after])
        print("ok: chaos live-s3-outage")
    finally:
        if not healed and os.environ.get("ZMQ_CHAOS_S3_UP"):
            try:
                run_hook("ZMQ_CHAOS_S3_UP")
            except TestError as exc:
                print(f"WARN: failed to heal S3 outage: {exc}", file=sys.stderr)
        stop_broker(proc)


SCENARIO_FUNCS = {
    "sigkill-restart": scenario_sigkill_restart,
    "slow-partial-client": scenario_slow_partial_client,
    "clock-skewed-records": scenario_clock_skewed_records,
    "s3-outage": scenario_s3_outage,
    "network-partition": scenario_network_partition,
    "live-s3-outage": scenario_live_s3_outage,
}


def selected_scenarios():
    raw = os.environ.get("ZMQ_CHAOS_SCENARIOS")
    names = DEFAULT_SCENARIOS if not raw else split_csv(raw)
    if len(names) == 1 and names[0] == "all":
        names = list(DEFAULT_SCENARIOS)
        if os.environ.get("ZMQ_CHAOS_NETWORK_DOWN") or os.environ.get("ZMQ_CHAOS_NETWORK_UP") or os.environ.get("ZMQ_CHAOS_NETWORK_MATRIX"):
            names.append("network-partition")
        if os.environ.get("ZMQ_CHAOS_S3_DOWN") or os.environ.get("ZMQ_CHAOS_S3_UP"):
            names.append("live-s3-outage")

    resolved = []
    for name in names:
        canonical = ALIASES.get(name, name)
        if canonical not in SCENARIO_FUNCS:
            raise TestError(f"unknown chaos scenario: {name}")
        if canonical not in resolved:
            resolved.append(canonical)
    return resolved


def validate_required_coverage(scenarios):
    required_scenarios = [ALIASES.get(name, name) for name in split_csv(os.environ.get("ZMQ_CHAOS_REQUIRED_SCENARIOS"))]
    missing_scenarios = [name for name in required_scenarios if name not in scenarios]
    if missing_scenarios:
        raise TestError(f"required chaos scenarios not selected: {', '.join(missing_scenarios)}")

    required_phases = split_csv(os.environ.get("ZMQ_CHAOS_REQUIRED_NETWORK_PHASES"))
    if required_phases:
        if "network-partition" not in scenarios:
            raise TestError("required network partition phases need network-partition scenario")
        configured_phases = [phase for phase in network_partition_phases() if phase is not None]
        missing_phases = [phase for phase in required_phases if phase not in configured_phases]
        if missing_phases:
            raise TestError(f"required chaos network phases not configured: {', '.join(missing_phases)}")


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_CHAOS_TESTS=1 to run broker chaos harness")
        return 0
    if not os.path.exists(ZMQ_BIN):
        raise TestError(f"broker binary not found: {ZMQ_BIN}")

    scenarios = selected_scenarios()
    validate_required_coverage(scenarios)
    tmp = tempfile.mkdtemp(prefix="zmq-chaos-")
    try:
        for scenario in scenarios:
            SCENARIO_FUNCS[scenario](tmp)
        print(f"ok: chaos harness passed for {', '.join(scenarios)}")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def self_test():
    old_env = os.environ.copy()
    try:
        os.environ.pop("ZMQ_CHAOS_SCENARIOS", None)
        if selected_scenarios() != DEFAULT_SCENARIOS:
            raise TestError("default scenario selection failed")
        os.environ["ZMQ_CHAOS_SCENARIOS"] = "sigkill,partial-client,clock-skew,s3"
        expected = [
            "sigkill-restart",
            "slow-partial-client",
            "clock-skewed-records",
            "s3-outage",
        ]
        if selected_scenarios() != expected:
            raise TestError("scenario alias selection failed")
        os.environ["ZMQ_CHAOS_SCENARIOS"] = "live-s3,network"
        if selected_scenarios() != ["live-s3-outage", "network-partition"]:
            raise TestError("live S3/network scenario alias selection failed")
        os.environ["ZMQ_CHAOS_SCENARIOS"] = "all"
        os.environ["ZMQ_CHAOS_NETWORK_MATRIX"] = "az-a,broker-2,az-a"
        os.environ["ZMQ_CHAOS_NETWORK_AZ_A_DOWN"] = "true"
        os.environ["ZMQ_CHAOS_NETWORK_AZ_A_UP"] = "true"
        os.environ["ZMQ_CHAOS_NETWORK_BROKER_2_DOWN"] = "true"
        os.environ["ZMQ_CHAOS_NETWORK_BROKER_2_UP"] = "true"
        os.environ["ZMQ_CHAOS_NETWORK_BROKER_2_EXPECT"] = "survive"
        os.environ["ZMQ_CHAOS_S3_DOWN"] = "true"
        os.environ["ZMQ_CHAOS_S3_UP"] = "true"
        all_scenarios = selected_scenarios()
        if "network-partition" not in all_scenarios or "live-s3-outage" not in all_scenarios:
            raise TestError("all scenario selection did not include hooked chaos scenarios")
        if network_partition_phases() != ["az-a", "broker-2"]:
            raise TestError("network partition phase parsing failed")
        if network_phase_env("broker-2", "DOWN") != "ZMQ_CHAOS_NETWORK_BROKER_2_DOWN":
            raise TestError("network phase hook selection failed")
        if network_phase_expect("broker-2") != "survive":
            raise TestError("network phase expect selection failed")
        os.environ["ZMQ_CHAOS_REQUIRED_SCENARIOS"] = "sigkill,network"
        os.environ["ZMQ_CHAOS_REQUIRED_NETWORK_PHASES"] = "az-a,broker-2"
        validate_required_coverage(all_scenarios)
        os.environ["ZMQ_CHAOS_REQUIRED_NETWORK_PHASES"] = "missing-phase"
        try:
            validate_required_coverage(all_scenarios)
            raise TestError("missing required network phase was not rejected")
        except TestError as exc:
            if "missing required network phase" in str(exc):
                raise
        os.environ["ZMQ_CHAOS_S3_ENDPOINT"] = "s3.example.test"
        os.environ["ZMQ_CHAOS_S3_PORT"] = "443"
        os.environ["ZMQ_CHAOS_S3_BUCKET"] = "zmq-chaos"
        os.environ["ZMQ_CHAOS_S3_ACCESS_KEY"] = "ak"
        os.environ["ZMQ_CHAOS_S3_SECRET_KEY"] = "sk"
        os.environ["ZMQ_CHAOS_S3_SCHEME"] = "https"
        os.environ["ZMQ_CHAOS_S3_REGION"] = "us-west-2"
        os.environ["ZMQ_CHAOS_S3_PATH_STYLE"] = "false"
        os.environ["ZMQ_CHAOS_S3_TLS_CA_FILE"] = "/tmp/ca.pem"
        s3_config = live_s3_config_from_env()
        if s3_config["endpoint"] != "s3.example.test" or s3_config["scheme"] != "https":
            raise TestError("live S3 config parsing failed")
        args = []
        append_s3_args(args, s3_config)
        for expected_arg in [
            "--s3-endpoint",
            "s3.example.test",
            "--s3-scheme",
            "https",
            "--s3-region",
            "us-west-2",
            "--s3-path-style",
            "false",
            "--s3-tls-ca-file",
            "/tmp/ca.pem",
        ]:
            if expected_arg not in args:
                raise TestError(f"live S3 broker args missing {expected_arg}")
        if write_string("abc") != b"\x00\x03abc":
            raise TestError("string encoding self-test failed")
        batch = build_record_batch(b"x", 123456789)
        if len(batch) < 61 or batch[16] != 2:
            raise TestError("record batch header self-test failed")
        record_count = struct.unpack_from(">i", batch, 57)[0]
        if record_count != 1:
            raise TestError(f"record count self-test failed: {record_count}")
        print("ok: chaos harness self-test")
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

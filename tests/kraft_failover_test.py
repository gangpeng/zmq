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


def read_compact_array_len(buf, pos):
    raw_len, pos = read_varint(buf, pos)
    if raw_len == 0:
        return 0, pos
    return raw_len - 1, pos


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


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_KRAFT_FAILOVER_TESTS=1 to run KRaft failover harness")
        return 0
    if not os.path.exists(ZMQ_BIN):
        raise TestError(f"broker binary not found: {ZMQ_BIN}")

    tmp = tempfile.mkdtemp(prefix="zmq-kraft-failover-")
    processes = {}
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

        stop_process(processes[leader_id]["proc"], crash=True)
        replacement_leader, after = wait_for_leader(processes, forbidden_leaders={leader_id})
        alive = {node_id for node_id, info in processes.items() if info["proc"].poll() is None}
        if replacement_leader not in alive:
            raise TestError(f"replacement leader {replacement_leader} is not alive; alive={sorted(alive)}")
        if after["leader_epoch"] <= initial["leader_epoch"]:
            raise TestError(f"leader epoch did not advance: before={initial} after={after}")

        wait_for_all_alive_to_report(processes, replacement_leader)
        print(
            "ok: KRaft controller failover harness passed "
            f"(old_leader={leader_id}, new_leader={replacement_leader}, epoch={after['leader_epoch']})"
        )
        return 0
    finally:
        for info in processes.values():
            stop_process(info.get("proc"))
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except TestError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)

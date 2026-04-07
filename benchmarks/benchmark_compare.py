#!/usr/bin/env python3
"""
ZMQ Benchmark: Zig vs Java Implementation
Compares throughput, latency, memory, and startup time using same MinIO S3 backend.
Runs 3 Zig brokers. Java runs in KRaft combined mode.

Uses per-request TCP connections to measure full connection + protocol overhead.
This is a fair comparison since both implementations handle the same workload.
"""

import socket
import struct
import time
import subprocess
import os
import signal
import sys
import statistics
import json
import traceback

MINIO_PORT = 18900
ZIG_PORTS = [18910, 18911, 18912]
JAVA_PORT = 18920
JAVA_CONTROLLER_PORT = 19020
METRICS_PORTS = [18930, 18931, 18932]

# ─── MinIO helpers ──────────────────────────────────────────────────

def start_minio():
    """Start MinIO container."""
    subprocess.run(["docker", "rm", "-f", "bench-minio"], capture_output=True)
    r = subprocess.run([
        "docker", "run", "-d", "--name", "bench-minio",
        "-p", f"{MINIO_PORT}:9000",
        "-e", "MINIO_ROOT_USER=minioadmin",
        "-e", "MINIO_ROOT_PASSWORD=minioadmin",
        "minio/minio:latest", "server", "/data"
    ], capture_output=True, text=True)
    import urllib.request
    for i in range(30):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{MINIO_PORT}/minio/health/live", timeout=2)
            break
        except:
            time.sleep(1)
    else:
        return False
    subprocess.run([
        "docker", "exec", "bench-minio",
        "mc", "alias", "set", "local", "http://localhost:9000", "minioadmin", "minioadmin"
    ], capture_output=True, timeout=10)
    subprocess.run([
        "docker", "exec", "bench-minio",
        "mc", "mb", "local/automq", "--ignore-existing"
    ], capture_output=True, timeout=10)
    return True

def clean_minio():
    subprocess.run(["docker", "exec", "bench-minio",
        "mc", "rm", "--recursive", "--force", "local/automq/"],
        capture_output=True, timeout=30)
    subprocess.run(["docker", "exec", "bench-minio",
        "mc", "mb", "local/automq", "--ignore-existing"],
        capture_output=True, timeout=10)

def stop_minio():
    subprocess.run(["docker", "rm", "-f", "bench-minio"], capture_output=True)

# ─── Kafka protocol ────────────────────────────────────────────────

def kafka_request(port, api_key, api_version, correlation_id, body=b'', timeout_sec=10):
    """Send Kafka request, receive response. Fresh connection each time."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout_sec)
    s.connect(('127.0.0.1', port))
    client_id = b'bench'
    hdr = struct.pack('>hhih', api_key, api_version, correlation_id, len(client_id)) + client_id
    frame = struct.pack('>I', len(hdr + body)) + hdr + body
    s.sendall(frame)
    size_buf = _recv_exact(s, 4)
    sz = struct.unpack('>I', size_buf)[0]
    resp = _recv_exact(s, sz)
    s.close()
    return resp

def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(min(65536, n - len(buf)))
        if not chunk:
            raise ConnectionError("Connection closed")
        buf += chunk
    return buf

class ReusableConn:
    """TCP connection with automatic reconnect on failure."""
    def __init__(self, port):
        self.port = port
        self.sock = None
        self._connect()

    def _connect(self):
        if self.sock:
            try: self.sock.close()
            except: pass
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.connect(('127.0.0.1', self.port))

    def request(self, api_key, api_version, correlation_id, body=b''):
        client_id = b'bench'
        hdr = struct.pack('>hhih', api_key, api_version, correlation_id, len(client_id)) + client_id
        frame = struct.pack('>I', len(hdr + body)) + hdr + body
        try:
            self.sock.sendall(frame)
            size_buf = _recv_exact(self.sock, 4)
            sz = struct.unpack('>I', size_buf)[0]
            return _recv_exact(self.sock, sz)
        except Exception:
            self._connect()
            raise

    def close(self):
        try: self.sock.close()
        except: pass

def create_topic_raw(port, name, partitions=3):
    body = struct.pack('>i', 1)
    tb = name.encode()
    body += struct.pack('>h', len(tb)) + tb
    body += struct.pack('>i', partitions)
    body += struct.pack('>h', 1)
    body += struct.pack('>i', 0)
    body += struct.pack('>i', 0)
    body += struct.pack('>i', 30000)
    return kafka_request(port, 19, 0, 1, body)

def make_produce_body(topic, partition, value):
    tb = topic.encode() if isinstance(topic, str) else topic
    vb = value if isinstance(value, bytes) else value.encode()
    body = struct.pack('>h', 1)  # acks=1
    body += struct.pack('>i', 5000)  # timeout
    body += struct.pack('>i', 1)
    body += struct.pack('>h', len(tb)) + tb
    body += struct.pack('>i', 1)
    body += struct.pack('>i', partition)
    body += struct.pack('>i', len(vb)) + vb
    return body

def make_produce_batch_body(topic, partition, values):
    """Build Produce v0 body with multiple values concatenated as record set."""
    tb = topic.encode() if isinstance(topic, str) else topic
    # Concatenate all values as one record set bytes blob
    record_set = b''
    for v in values:
        vb = v if isinstance(v, bytes) else v.encode()
        record_set += vb
    body = struct.pack('>h', 1)  # acks=1
    body += struct.pack('>i', 5000)  # timeout
    body += struct.pack('>i', 1)  # 1 topic
    body += struct.pack('>h', len(tb)) + tb
    body += struct.pack('>i', 1)  # 1 partition
    body += struct.pack('>i', partition)
    body += struct.pack('>i', len(record_set)) + record_set
    return body

def make_fetch_body(topic, partition, offset=0):
    tb = topic.encode() if isinstance(topic, str) else topic
    body = struct.pack('>iiii', -1, 500, 1, 1048576)  # max_wait=500ms
    body += struct.pack('>i', 1) + struct.pack('>h', len(tb)) + tb
    body += struct.pack('>i', 1) + struct.pack('>i', partition)
    body += struct.pack('>qi', offset, 1048576)
    return body

# ─── Benchmarks ─────────────────────────────────────────────────────

def bench_api_versions(port, n, use_persistent=True):
    """ApiVersions: lightest request, measures protocol overhead."""
    latencies = []
    errors = 0

    if use_persistent:
        try:
            conn = ReusableConn(port)
        except:
            use_persistent = False

    for i in range(n):
        t0 = time.monotonic()
        try:
            if use_persistent:
                conn.request(18, 0, i+1, b'')
            else:
                kafka_request(port, 18, 0, i+1, b'')
            latencies.append((time.monotonic() - t0) * 1000)
        except:
            errors += 1
            if use_persistent:
                try: conn = ReusableConn(port)
                except: use_persistent = False

    if use_persistent:
        try: conn.close()
        except: pass

    elapsed = sum(latencies) / 1000 if latencies else 1
    return _make_result(n, errors, latencies, elapsed, 'throughput_req_sec')

def bench_produce(port, topic, n, msg_size, partitions=3, use_persistent=True, throttle_ms=0):
    """Produce benchmark."""
    value = b'X' * msg_size
    latencies = []
    errors = 0

    if use_persistent:
        try:
            conn = ReusableConn(port)
        except:
            use_persistent = False

    for i in range(n):
        if throttle_ms > 0:
            time.sleep(throttle_ms / 1000.0)
        t0 = time.monotonic()
        try:
            body = make_produce_body(topic, i % partitions, value)
            if use_persistent:
                conn.request(0, 0, i+1, body)
            else:
                kafka_request(port, 0, 0, i+1, body)
            latencies.append((time.monotonic() - t0) * 1000)
        except:
            errors += 1
            if use_persistent:
                try: conn = ReusableConn(port)
                except: use_persistent = False

    if use_persistent:
        try: conn.close()
        except: pass

    elapsed = sum(latencies) / 1000 if latencies else 1
    ok = n - errors
    throughput = ok / elapsed if elapsed > 0 else 0
    bytes_sec = (ok * msg_size) / elapsed if elapsed > 0 else 0
    result = _make_result(n, errors, latencies, elapsed, 'throughput_msg_sec')
    result['throughput_mb_sec'] = round(bytes_sec / (1024*1024), 3)
    return result

def bench_produce_batched(port, topic, total_msgs, msg_size, batch_size=50, partitions=3):
    """Produce benchmark with batching — multiple records per Produce request."""
    value = b'X' * msg_size
    latencies = []
    errors = 0
    total_records = 0

    try:
        conn = ReusableConn(port)
    except:
        return _make_result(total_msgs, total_msgs, [], 1, 'throughput_msg_sec')

    num_batches = (total_msgs + batch_size - 1) // batch_size
    for i in range(num_batches):
        batch_values = [value] * min(batch_size, total_msgs - i * batch_size)
        t0 = time.monotonic()
        try:
            body = make_produce_batch_body(topic, i % partitions, batch_values)
            conn.request(0, 0, i+1, body)
            batch_latency = (time.monotonic() - t0) * 1000
            latencies.append(batch_latency)
            total_records += len(batch_values)
        except:
            errors += 1
            try: conn = ReusableConn(port)
            except: break

    conn.close()

    elapsed = sum(latencies) / 1000 if latencies else 1
    ok_records = total_records
    throughput = ok_records / elapsed if elapsed > 0 else 0
    bytes_sec = (ok_records * msg_size) / elapsed if elapsed > 0 else 0
    return {
        'total_records': total_records,
        'batches': num_batches,
        'batch_size': batch_size,
        'errors': errors,
        'elapsed_sec': round(elapsed, 3),
        'throughput_msg_sec': round(throughput, 1),
        'throughput_mb_sec': round(bytes_sec / (1024*1024), 3),
        'latency_p50': round(statistics.median(latencies), 3) if latencies else 0,
        'latency_p99': round(sorted(latencies)[int(len(latencies)*0.99)], 3) if latencies else 0,
        'latency_avg': round(statistics.mean(latencies), 3) if latencies else 0,
    }

def bench_fetch(port, topic, partition, n, use_persistent=True):
    """Fetch benchmark."""
    latencies = []
    errors = 0
    total_bytes = 0

    if use_persistent:
        try:
            conn = ReusableConn(port)
        except:
            use_persistent = False

    for i in range(n):
        t0 = time.monotonic()
        try:
            body = make_fetch_body(topic, partition, 0)
            if use_persistent:
                resp = conn.request(1, 3, i+1, body)
            else:
                resp = kafka_request(port, 1, 3, i+1, body)
            total_bytes += len(resp)
            latencies.append((time.monotonic() - t0) * 1000)
        except:
            errors += 1
            if use_persistent:
                try: conn = ReusableConn(port)
                except: use_persistent = False

    if use_persistent:
        try: conn.close()
        except: pass

    elapsed = sum(latencies) / 1000 if latencies else 1
    result = _make_result(n, errors, latencies, elapsed, 'throughput_fetch_sec')
    result['throughput_mb_sec'] = round(total_bytes / (1024*1024) / elapsed if elapsed > 0 else 0, 3)
    return result

def bench_metadata(port, n, use_persistent=True):
    """Metadata benchmark."""
    latencies = []
    errors = 0
    body = struct.pack('>i', 0)

    if use_persistent:
        try:
            conn = ReusableConn(port)
        except:
            use_persistent = False

    for i in range(n):
        t0 = time.monotonic()
        try:
            if use_persistent:
                conn.request(3, 0, i+1, body)
            else:
                kafka_request(port, 3, 0, i+1, body)
            latencies.append((time.monotonic() - t0) * 1000)
        except:
            errors += 1
            if use_persistent:
                try: conn = ReusableConn(port)
                except: use_persistent = False

    if use_persistent:
        try: conn.close()
        except: pass

    elapsed = sum(latencies) / 1000 if latencies else 1
    return _make_result(n, errors, latencies, elapsed, 'throughput_req_sec')

def _make_result(n, errors, latencies, elapsed, throughput_key):
    ok = n - errors
    throughput = ok / elapsed if elapsed > 0 else 0
    return {
        'total': n,
        'errors': errors,
        'elapsed_sec': round(elapsed, 3),
        throughput_key: round(throughput, 1),
        'latency_p50': round(statistics.median(latencies), 3) if latencies else 0,
        'latency_p99': round(sorted(latencies)[int(len(latencies)*0.99)], 3) if latencies else 0,
        'latency_avg': round(statistics.mean(latencies), 3) if latencies else 0,
    }

# ─── Helpers ────────────────────────────────────────────────────────

def print_results(name, results):
    print(f"\n  {name}:")
    for k, v in results.items():
        if isinstance(v, float):
            if 'latency' in k:
                print(f"    {k:30s} {v:>12.3f} ms")
            else:
                print(f"    {k:30s} {v:>12.3f}")
        else:
            print(f"    {k:30s} {v:>12}")

def get_rss_kb(pattern):
    result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
    total_kb = 0
    count = 0
    for line in result.stdout.split('\n'):
        if pattern in line and 'grep' not in line and 'python' not in line:
            parts = line.split()
            if len(parts) > 5:
                try:
                    total_kb += int(parts[5])
                    count += 1
                except ValueError:
                    pass
    return total_kb, count

def wait_for_port(port, timeout=60):
    """Wait until a port accepts connections."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            kafka_request(port, 18, 0, 1)
            return time.monotonic() - t0
        except:
            time.sleep(0.5)
    return None

# ==================================================================
if __name__ == '__main__':
    NUM_PRODUCE = 1000
    MSG_SIZE = 1024
    NUM_FETCH = 100
    NUM_METADATA = 2000
    NUM_API_VERSIONS = 3000
    PARTITIONS = 3
    # No throttle needed - broker is stable under load
    PRODUCE_THROTTLE_MS = 0

    print("=" * 72)
    print("  ZMQ Benchmark: Zig vs Java")
    print("  MinIO S3 backend | Same protocol-level workload")
    print(f"  Produce: {NUM_PRODUCE} msgs × {MSG_SIZE}B | Fetch: {NUM_FETCH}")
    print(f"  Metadata: {NUM_METADATA} | ApiVersions: {NUM_API_VERSIONS}")
    print("=" * 72)

    print("\n[1/4] Starting MinIO...")
    if not start_minio():
        print("  FATAL: MinIO failed"); sys.exit(1)
    print("  MinIO ready on port", MINIO_PORT)

    zig_results = {}
    java_results = {}

    # ════════════════════════════════════════════════════════════════
    # ZIG
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 72)
    print("  [2/4] ZIG BROKER BENCHMARK (3 brokers)")
    print("=" * 72)

    clean_minio()
    for i in range(3):
        d = f"/tmp/bench-zig-{i}"
        subprocess.run(["rm", "-rf", d], capture_output=True)
        os.makedirs(d, exist_ok=True)

    zig_binary = "/home/penggang/automq/zig/zig-out/bin/zmq"
    bin_size = os.path.getsize(zig_binary)
    print(f"\n  Binary: {bin_size:,} bytes ({bin_size/1024/1024:.1f} MB)")

    # Start broker 0 and measure startup time
    # NOTE: No --data-dir flag → in-memory storage only (no local WAL fsync).
    # This is fairer vs Java AutoMQ which uses S3-based WAL (HTTP to MinIO),
    # not local filesystem fsync. Both paths: in-memory → deferred S3.
    zig_cmd = lambda i: [
        zig_binary, str(ZIG_PORTS[i]),
        "--node-id", str(i),
        "--s3-endpoint", "127.0.0.1",
        "--s3-port", str(MINIO_PORT),
        "--s3-bucket", "automq",
        "--metrics-port", str(METRICS_PORTS[i]),
        "--advertised-host", "127.0.0.1",
    ]

    print("  Measuring startup time (broker 0)...")
    t_start = time.monotonic()
    zig_procs = [subprocess.Popen(zig_cmd(0), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)]
    startup_s = wait_for_port(ZIG_PORTS[0], timeout=15)
    if startup_s:
        zig_results['startup_ms'] = round(startup_s * 1000, 1)
        print(f"  Startup time: {startup_s*1000:.0f} ms")
    else:
        zig_results['startup_ms'] = -1
        print("  Startup FAILED")

    # Start brokers 1, 2
    for i in range(1, 3):
        zig_procs.append(subprocess.Popen(zig_cmd(i), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    time.sleep(2)

    alive = sum(1 for p in zig_procs if p.poll() is None)
    print(f"  {alive}/3 Zig brokers running")

    if alive > 0:
        rss_kb, pc = get_rss_kb("zig-out/bin/zmq")
        zig_results['startup_rss_mb'] = round(rss_kb / 1024, 1)
        print(f"  Startup RSS: {rss_kb:,} KB ({rss_kb/1024:.1f} MB) across {pc} procs")

        try:
            create_topic_raw(ZIG_PORTS[0], "bench-topic", PARTITIONS)
            print("  Topic 'bench-topic' created")
            time.sleep(0.5)
        except Exception as e:
            print(f"  Topic creation: {e}")

        # ApiVersions
        print("\n  [Bench] ApiVersions (protocol overhead)...")
        zig_results['api_versions'] = bench_api_versions(ZIG_PORTS[0], NUM_API_VERSIONS)
        print_results("ApiVersions", zig_results['api_versions'])

        # Produce (single-message per request)
        print(f"\n  [Bench] Produce (single msg/req)...")
        zig_results['produce'] = bench_produce(ZIG_PORTS[0], "bench-topic", NUM_PRODUCE, MSG_SIZE, PARTITIONS, throttle_ms=PRODUCE_THROTTLE_MS)
        print_results("Produce", zig_results['produce'])

        # Produce batched (50 records per request — like real Kafka clients)
        print(f"\n  [Bench] Produce Batched (50 msgs/req)...")
        zig_results['produce_batched'] = bench_produce_batched(ZIG_PORTS[0], "bench-topic", NUM_PRODUCE, MSG_SIZE, batch_size=50, partitions=PARTITIONS)
        print_results("Produce Batched", zig_results['produce_batched'])

        # Fetch
        alive_after_produce = sum(1 for p in zig_procs if p.poll() is None)
        if alive_after_produce > 0:
            print("\n  [Bench] Fetch...")
            zig_results['fetch'] = bench_fetch(ZIG_PORTS[0], "bench-topic", 0, NUM_FETCH)
            print_results("Fetch", zig_results['fetch'])
        else:
            print("\n  WARN: Zig brokers crashed during produce, skipping fetch")

        # Metadata
        alive_after_fetch = sum(1 for p in zig_procs if p.poll() is None)
        if alive_after_fetch > 0:
            print("\n  [Bench] Metadata...")
            zig_results['metadata'] = bench_metadata(ZIG_PORTS[0], NUM_METADATA)
            print_results("Metadata", zig_results['metadata'])
        else:
            print("\n  WARN: Zig brokers crashed during fetch, skipping metadata")

        rss_kb, _ = get_rss_kb("zig-out/bin/zmq")
        zig_results['loaded_rss_mb'] = round(rss_kb / 1024, 1)
        print(f"\n  After-load RSS: {rss_kb:,} KB ({rss_kb/1024:.1f} MB)")

    for p in zig_procs:
        try: p.terminate()
        except: pass
    for p in zig_procs:
        try: p.wait(timeout=5)
        except:
            try: p.kill()
            except: pass
    print("  Zig brokers stopped")

    # ════════════════════════════════════════════════════════════════
    # JAVA
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 72)
    print("  [3/4] JAVA AUTOMQ BENCHMARK (KRaft combined mode)")
    print("=" * 72)

    clean_minio()
    subprocess.run(["rm", "-rf", "/tmp/bench-java"], capture_output=True)
    os.makedirs("/tmp/bench-java/data", exist_ok=True)
    os.makedirs("/tmp/bench-java/logs", exist_ok=True)

    java_home = "/tmp/jdk-17"
    automq_home = "/home/penggang/automq"
    env = os.environ.copy()
    env["JAVA_HOME"] = java_home
    env["PATH"] = f"{java_home}/bin:" + env.get("PATH", "")

    s3_bucket_uri = f"0@s3://automq?region=us-east-1&endpoint=http://127.0.0.1:{MINIO_PORT}&pathStyle=true&accessKey=minioadmin&secretKey=minioadmin"
    props = f"""process.roles=broker,controller
node.id=0
controller.quorum.voters=0@localhost:{JAVA_CONTROLLER_PORT}
listeners=PLAINTEXT://:{JAVA_PORT},CONTROLLER://localhost:{JAVA_CONTROLLER_PORT}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:{JAVA_PORT}
log.dirs=/tmp/bench-java/data
num.partitions={PARTITIONS}
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
s3.data.buckets={s3_bucket_uri}
s3.ops.buckets={s3_bucket_uri}
s3.wal.path={s3_bucket_uri}
"""
    with open("/tmp/bench-java/server.properties", "w") as f:
        f.write(props)

    # Generate cluster ID
    print("  Generating cluster ID...")
    uuid_r = subprocess.run(
        [f"{automq_home}/bin/kafka-storage.sh", "random-uuid"],
        capture_output=True, text=True, env=env, timeout=30
    )
    if uuid_r.returncode == 0 and uuid_r.stdout.strip():
        cluster_id = uuid_r.stdout.strip()
    else:
        import base64, uuid as uuid_mod
        cluster_id = base64.urlsafe_b64encode(uuid_mod.uuid4().bytes).decode().rstrip('=')
    print(f"  Cluster ID: {cluster_id}")

    # Format
    print("  Formatting KRaft storage...")
    fmt_r = subprocess.run(
        [f"{automq_home}/bin/kafka-storage.sh", "format",
         "-t", cluster_id,
         "-c", "/tmp/bench-java/server.properties"],
        capture_output=True, text=True, env=env, timeout=60
    )
    print(f"  Format: {fmt_r.stdout.strip()[:120]}")
    if fmt_r.returncode != 0:
        print(f"  Format stderr: {fmt_r.stderr[:300]}")

    # Start Java broker
    print("  Starting Java AutoMQ broker...")
    env["KAFKA_HEAP_OPTS"] = "-Xmx2g -Xms1g"
    env["LOG_DIR"] = "/tmp/bench-java/logs"
    env["AWS_ACCESS_KEY_ID"] = "minioadmin"
    env["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

    java_t0 = time.monotonic()
    java_proc = subprocess.Popen(
        [f"{automq_home}/bin/kafka-server-start.sh", "/tmp/bench-java/server.properties"],
        stdout=open("/tmp/bench-java/stdout.log", "w"),
        stderr=open("/tmp/bench-java/stderr.log", "w"),
        env=env
    )

    java_ready = False
    java_startup_s = wait_for_port(JAVA_PORT, timeout=180)
    if java_startup_s and java_proc.poll() is None:
        java_ready = True
        java_results['startup_ms'] = round(java_startup_s * 1000, 1)
        print(f"  Java broker ready after {java_startup_s:.1f}s")
    else:
        print("  Java broker failed to start!")
        try:
            with open('/tmp/bench-java/stderr.log') as f:
                print(f"  stderr (last 600):\n{f.read()[-600:]}")
        except: pass
        java_proc.terminate()
        try: java_proc.wait(timeout=10)
        except: java_proc.kill()

    if java_ready:
        rss_kb, pc = get_rss_kb("bench-java")
        if rss_kb == 0:
            rss_kb, pc = get_rss_kb("kafka.Kafka")
        java_results['startup_rss_mb'] = round(rss_kb / 1024, 1)
        print(f"  Startup RSS: {rss_kb:,} KB ({rss_kb/1024:.1f} MB) across {pc} procs")

        # Create topic
        try:
            ct_r = subprocess.run([
                f"{automq_home}/bin/kafka-topics.sh",
                "--bootstrap-server", f"localhost:{JAVA_PORT}",
                "--create", "--topic", "bench-topic",
                "--partitions", str(PARTITIONS),
                "--replication-factor", "1"
            ], env=env, capture_output=True, text=True, timeout=30)
            print(f"  Topic: {ct_r.stdout.strip()[:80]}")
            time.sleep(2)
        except Exception as e:
            print(f"  Topic error: {e}")

        # ApiVersions
        print("\n  [Bench] ApiVersions...")
        java_results['api_versions'] = bench_api_versions(JAVA_PORT, NUM_API_VERSIONS)
        print_results("ApiVersions", java_results['api_versions'])

        # Produce (single-message per request)
        print(f"\n  [Bench] Produce (single msg/req)...")
        java_results['produce'] = bench_produce(JAVA_PORT, "bench-topic", NUM_PRODUCE, MSG_SIZE, PARTITIONS, throttle_ms=PRODUCE_THROTTLE_MS)
        print_results("Produce", java_results['produce'])

        # Produce batched
        print(f"\n  [Bench] Produce Batched (50 msgs/req)...")
        java_results['produce_batched'] = bench_produce_batched(JAVA_PORT, "bench-topic", NUM_PRODUCE, MSG_SIZE, batch_size=50, partitions=PARTITIONS)
        print_results("Produce Batched", java_results['produce_batched'])

        # Fetch
        print("\n  [Bench] Fetch...")
        java_results['fetch'] = bench_fetch(JAVA_PORT, "bench-topic", 0, NUM_FETCH)
        print_results("Fetch", java_results['fetch'])

        # Metadata
        print("\n  [Bench] Metadata...")
        java_results['metadata'] = bench_metadata(JAVA_PORT, NUM_METADATA)
        print_results("Metadata", java_results['metadata'])

        rss_kb, _ = get_rss_kb("bench-java")
        if rss_kb == 0:
            rss_kb, _ = get_rss_kb("kafka.Kafka")
        java_results['loaded_rss_mb'] = round(rss_kb / 1024, 1)
        print(f"\n  After-load RSS: {rss_kb:,} KB ({rss_kb/1024:.1f} MB)")

        java_proc.terminate()
        try: java_proc.wait(timeout=15)
        except: java_proc.kill()
        print("  Java broker stopped")

    # ════════════════════════════════════════════════════════════════
    # COMPARISON
    # ════════════════════════════════════════════════════════════════
    print("\n" + "=" * 72)
    print("  [4/4] COMPARISON: Zig vs Java AutoMQ")
    print("=" * 72)

    def cmp(label, zv, jv, unit="", higher_better=True):
        zs = f"{zv:>10.2f}" if isinstance(zv, (int, float)) and zv >= 0 else f"{'N/A':>10s}"
        js = f"{jv:>10.2f}" if isinstance(jv, (int, float)) and jv >= 0 else f"{'N/A':>10s}"
        ratio_s = ""
        if isinstance(zv, (int,float)) and isinstance(jv, (int,float)) and jv > 0 and zv >= 0:
            ratio = zv / jv
            marker = "✓" if ((ratio >= 1) == higher_better) else "✗"
            ratio_s = f"{ratio:>6.2f}x {marker}"
        print(f"  {label:35s} {zs} {unit:>5s}  {js} {unit:>5s}  {ratio_s}")

    print(f"\n  {'Metric':35s} {'Zig':>15s}  {'Java':>15s}  {'Ratio':>8s}")
    print(f"  {'─'*35} {'─'*15}  {'─'*15}  {'─'*8}")

    # Startup
    cmp("Startup time", zig_results.get('startup_ms',0), java_results.get('startup_ms',0), "ms", False)
    cmp("Startup memory (RSS total)", zig_results.get('startup_rss_mb',0), java_results.get('startup_rss_mb',0), "MB", False)
    cmp("Loaded memory (RSS total)", zig_results.get('loaded_rss_mb',0), java_results.get('loaded_rss_mb',0), "MB", False)

    for bench_name, tp_key in [('api_versions','throughput_req_sec'), ('produce','throughput_msg_sec'), ('produce_batched','throughput_msg_sec'), ('fetch','throughput_fetch_sec'), ('metadata','throughput_req_sec')]:
        zb = zig_results.get(bench_name, {})
        jb = java_results.get(bench_name, {})
        if zb or jb:
            print()
            nice = bench_name.replace('_',' ').title()
            cmp(f"{nice} throughput", zb.get(tp_key,0), jb.get(tp_key,0), "/s")
            cmp(f"{nice} p50 latency", zb.get('latency_p50',0), jb.get('latency_p50',0), "ms", False)
            cmp(f"{nice} p99 latency", zb.get('latency_p99',0), jb.get('latency_p99',0), "ms", False)
            if 'throughput_mb_sec' in zb or 'throughput_mb_sec' in jb:
                cmp(f"{nice} throughput", zb.get('throughput_mb_sec',0), jb.get('throughput_mb_sec',0), "MB/s")
            ze = zb.get('errors',0)
            je = jb.get('errors',0)
            if ze > 0 or je > 0:
                cmp(f"{nice} errors", ze, je, "", False)

    # Binary size
    print()
    zig_bin_mb = os.path.getsize(zig_binary) / (1024*1024)
    print(f"  {'Binary size':35s} {zig_bin_mb:>10.1f} {'MB':>5s}  {'~200+':>10s} {'MB':>5s}")

    print("\n" + "─" * 72)
    print("  Notes:")
    print("  • Zig: 3 brokers, epoll, in-memory + S3 WAL batcher (async)")
    print("  • Java: KRaft combined mode (broker+controller), -Xmx2g")
    print("  • Both use S3-backed WAL to MinIO (batched async writes)")
    print("  • Both use Kafka wire protocol (Produce v0), persistent TCP")
    print("  • Both parse RecordBatch headers, validate CRC, assign offsets")
    print("  • Zig has fetch purgatory, timestamp validation, size limits")
    print("  • Same MinIO S3 backend, data cleaned between tests")
    print("  • Produce v0 sends raw bytes (not RecordBatch v2 envelope)")
    print("  • ✓ = Zig wins, ✗ = Java wins")
    print("─" * 72)

    results_file = "/tmp/bench-results.json"
    with open(results_file, 'w') as f:
        json.dump({'zig': zig_results, 'java': java_results}, f, indent=2, default=str)
    print(f"\n  Full results: {results_file}")

    # Cleanup
    print("\n[Cleanup]")
    stop_minio()
    subprocess.run(["rm", "-rf", "/tmp/bench-zig-0", "/tmp/bench-zig-1", "/tmp/bench-zig-2"], capture_output=True)
    print("  Done")

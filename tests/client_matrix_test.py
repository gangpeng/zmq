#!/usr/bin/env python3
"""
Gated real-client compatibility matrix.

This harness intentionally depends on external Kafka clients. It is skipped by
default so normal local/CI Zig tests remain deterministic.

Run against a running ZMQ broker:

    ZMQ_RUN_CLIENT_MATRIX=1 zig build test-client-matrix

Optional environment:
    ZMQ_CLIENT_MATRIX_BOOTSTRAP   localhost:9092
    ZMQ_CLIENT_MATRIX_TOOLS       auto | kcat,kafka-cli,kafka-python,confluent-kafka

Supported probes:
    kcat             kcat -L
    kafka-cli        kafka-broker-api-versions.sh and kafka-topics.sh --list
    kafka-python     kafka.KafkaAdminClient metadata/list-topics probe
    confluent-kafka  confluent_kafka.admin.AdminClient metadata probe
"""

import importlib.util
import os
import shutil
import subprocess
import sys


RUN_ENABLED = os.environ.get("ZMQ_RUN_CLIENT_MATRIX") == "1"
BOOTSTRAP = os.environ.get("ZMQ_CLIENT_MATRIX_BOOTSTRAP", "localhost:9092")
TOOLS = os.environ.get("ZMQ_CLIENT_MATRIX_TOOLS", "auto")


class MatrixError(Exception):
    pass


def run(cmd, timeout=30):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise MatrixError(f"{cmd[0]} failed with exit code {proc.returncode}\n{proc.stdout}")
    return proc.stdout


def have(exe):
    return shutil.which(exe) is not None


def have_module(module):
    return importlib.util.find_spec(module) is not None


def selected_tools():
    if TOOLS == "auto":
        tools = []
        if have("kcat"):
            tools.append("kcat")
        if have("kafka-broker-api-versions.sh") and have("kafka-topics.sh"):
            tools.append("kafka-cli")
        if have_module("kafka"):
            tools.append("kafka-python")
        if have_module("confluent_kafka"):
            tools.append("confluent-kafka")
        return tools
    return [tool.strip() for tool in TOOLS.split(",") if tool.strip()]


def test_kcat():
    if not have("kcat"):
        raise MatrixError("kcat selected but not found in PATH")
    out = run(["kcat", "-L", "-b", BOOTSTRAP])
    if "Metadata for" not in out and "broker" not in out.lower():
        raise MatrixError(f"kcat metadata output did not look valid\n{out}")
    print("ok: kcat metadata probe")


def test_kafka_cli():
    if not have("kafka-broker-api-versions.sh"):
        raise MatrixError("kafka-broker-api-versions.sh selected but not found in PATH")
    if not have("kafka-topics.sh"):
        raise MatrixError("kafka-topics.sh selected but not found in PATH")

    api_out = run(["kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP], timeout=45)
    if "Produce" not in api_out and "ApiVersions" not in api_out:
        raise MatrixError(f"kafka-broker-api-versions output did not include expected APIs\n{api_out}")

    run(["kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--list"], timeout=45)
    print("ok: kafka CLI API/topic probes")


def test_kafka_python():
    if not have_module("kafka"):
        raise MatrixError("kafka-python selected but import 'kafka' is not available")

    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP,
        client_id="zmq-client-matrix-kafka-python",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
    )
    try:
        topics = admin.list_topics()
        if topics is None:
            raise MatrixError("kafka-python list_topics returned None")
    finally:
        admin.close()
    print("ok: kafka-python metadata/list-topics probe")


def test_confluent_kafka():
    if not have_module("confluent_kafka"):
        raise MatrixError("confluent-kafka selected but import 'confluent_kafka' is not available")

    from confluent_kafka.admin import AdminClient

    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-kafka",
        "socket.timeout.ms": 10000,
    })
    metadata = admin.list_topics(timeout=10)
    if not metadata.brokers:
        raise MatrixError("confluent-kafka metadata response did not include brokers")
    print("ok: confluent-kafka metadata probe")


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_CLIENT_MATRIX=1 to run real-client matrix")
        return 0

    tools = selected_tools()
    if not tools:
        raise MatrixError(
            "no supported Kafka client tools found; install kcat or Kafka CLI, "
            "or set ZMQ_CLIENT_MATRIX_TOOLS explicitly"
        )

    for tool in tools:
        if tool == "kcat":
            test_kcat()
        elif tool == "kafka-cli":
            test_kafka_cli()
        elif tool == "kafka-python":
            test_kafka_python()
        elif tool == "confluent-kafka":
            test_confluent_kafka()
        else:
            raise MatrixError(f"unknown client matrix tool: {tool}")

    print(f"ok: client matrix passed for {', '.join(tools)} against {BOOTSTRAP}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except MatrixError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)

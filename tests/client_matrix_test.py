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
    kcat             metadata plus produce/fetch round trip
    kafka-cli        API/topic probes plus console produce/fetch round trip
    kafka-python     AdminClient metadata plus producer/consumer round trip
    confluent-kafka  AdminClient metadata plus producer/consumer round trip
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


def run(cmd, timeout=30, input_text=None):
    proc = subprocess.run(
        cmd,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise MatrixError(f"{cmd[0]} failed with exit code {proc.returncode}\n{proc.stdout}")
    return proc.stdout


def have(exe):
    return shutil.which(exe) is not None


def have_module(module):
    return importlib.util.find_spec(module) is not None


def matrix_topic(tool):
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in tool)
    return f"zmq_client_matrix_{sanitized}_{os.getpid()}"


def matrix_payload(tool):
    return f"zmq-client-matrix-{tool}-{os.getpid()}"


def require_output_contains(output, payload, tool):
    if payload not in output:
        raise MatrixError(f"{tool} fetch output did not include produced payload {payload!r}\n{output}")


def selected_tools():
    if TOOLS == "auto":
        tools = []
        if have("kcat"):
            tools.append("kcat")
        if (
            have("kafka-broker-api-versions.sh")
            and have("kafka-topics.sh")
            and have("kafka-console-producer.sh")
            and have("kafka-console-consumer.sh")
        ):
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

    topic = matrix_topic("kcat")
    payload = matrix_payload("kcat")
    run(["kcat", "-P", "-b", BOOTSTRAP, "-t", topic], input_text=payload + "\n", timeout=45)
    fetched = run(["kcat", "-C", "-b", BOOTSTRAP, "-t", topic, "-o", "beginning", "-c", "1", "-e"], timeout=45)
    require_output_contains(fetched, payload, "kcat")
    print("ok: kcat metadata and produce/fetch probes")


def test_kafka_cli():
    if not have("kafka-broker-api-versions.sh"):
        raise MatrixError("kafka-broker-api-versions.sh selected but not found in PATH")
    if not have("kafka-topics.sh"):
        raise MatrixError("kafka-topics.sh selected but not found in PATH")
    if not have("kafka-console-producer.sh"):
        raise MatrixError("kafka-console-producer.sh selected but not found in PATH")
    if not have("kafka-console-consumer.sh"):
        raise MatrixError("kafka-console-consumer.sh selected but not found in PATH")

    api_out = run(["kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP], timeout=45)
    if "Produce" not in api_out and "ApiVersions" not in api_out:
        raise MatrixError(f"kafka-broker-api-versions output did not include expected APIs\n{api_out}")

    run(["kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--list"], timeout=45)

    topic = matrix_topic("kafka-cli")
    payload = matrix_payload("kafka-cli")
    run(
        ["kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP, "--topic", topic],
        input_text=payload + "\n",
        timeout=45,
    )
    fetched = run(
        [
            "kafka-console-consumer.sh",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "10000",
        ],
        timeout=45,
    )
    require_output_contains(fetched, payload, "kafka-cli")
    print("ok: kafka CLI API/topic and produce/fetch probes")


def test_kafka_python():
    if not have_module("kafka"):
        raise MatrixError("kafka-python selected but import 'kafka' is not available")

    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer

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

    topic = matrix_topic("kafka-python")
    payload = matrix_payload("kafka-python").encode()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        client_id="zmq-client-matrix-kafka-python-producer",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
    )
    try:
        producer.send(topic, payload).get(timeout=10)
        producer.flush(timeout=10)
    finally:
        producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        client_id="zmq-client-matrix-kafka-python-consumer",
        group_id=f"zmq-client-matrix-kafka-python-{os.getpid()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
    )
    try:
        for message in consumer:
            if message.value == payload:
                print("ok: kafka-python metadata and produce/fetch probes")
                return
        raise MatrixError("kafka-python consumer did not fetch produced payload")
    finally:
        consumer.close()


def test_confluent_kafka():
    if not have_module("confluent_kafka"):
        raise MatrixError("confluent-kafka selected but import 'confluent_kafka' is not available")

    from confluent_kafka import Consumer, Producer
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-kafka",
        "socket.timeout.ms": 10000,
    })
    metadata = admin.list_topics(timeout=10)
    if not metadata.brokers:
        raise MatrixError("confluent-kafka metadata response did not include brokers")

    topic = matrix_topic("confluent-kafka")
    payload = matrix_payload("confluent-kafka").encode()
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-producer",
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
    })
    producer.produce(topic, payload)
    if producer.flush(10) != 0:
        raise MatrixError("confluent-kafka producer did not flush produced payload")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-consumer",
        "group.id": f"zmq-client-matrix-confluent-{os.getpid()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "socket.timeout.ms": 10000,
    })
    try:
        consumer.subscribe([topic])
        deadline = 10
        while deadline > 0:
            message = consumer.poll(1.0)
            deadline -= 1
            if message is None:
                continue
            if message.error():
                raise MatrixError(f"confluent-kafka consumer error: {message.error()}")
            if message.value() == payload:
                print("ok: confluent-kafka metadata and produce/fetch probes")
                return
        raise MatrixError("confluent-kafka consumer did not fetch produced payload")
    finally:
        consumer.close()


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

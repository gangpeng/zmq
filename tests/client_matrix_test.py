#!/usr/bin/env python3
"""
Gated real-client compatibility matrix.

This harness intentionally depends on external Kafka clients. It is skipped by
default so normal local/CI Zig tests remain deterministic.

Run against a running ZMQ broker:

    ZMQ_RUN_CLIENT_MATRIX=1 zig build test-client-matrix

Optional environment:
    ZMQ_CLIENT_MATRIX_BOOTSTRAP   localhost:9092
    ZMQ_CLIENT_MATRIX_TOOLS       auto | kcat,kafka-cli

Supported probes:
    kcat       kcat -L
    kafka-cli  kafka-broker-api-versions.sh and kafka-topics.sh --list
"""

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


def selected_tools():
    if TOOLS == "auto":
        tools = []
        if have("kcat"):
            tools.append("kcat")
        if have("kafka-broker-api-versions.sh") and have("kafka-topics.sh"):
            tools.append("kafka-cli")
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

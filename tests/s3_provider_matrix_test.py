#!/usr/bin/env python3
"""
Gated live S3-compatible provider matrix.

The existing Zig MinIO tests validate live object, multipart, S3 WAL rebuild,
and PartitionStore resume behavior for one S3 endpoint. This wrapper runs that
same suite once per named provider profile so CI can cover MinIO plus additional
S3-compatible providers without changing the deterministic default test suite.

Run:
    ZMQ_RUN_S3_PROVIDER_MATRIX=1 ZMQ_S3_PROVIDER_PROFILES=minio zig build test-s3-provider-matrix

Global environment:
    ZMQ_S3_PROVIDER_PROFILES    Comma-separated profile names. Defaults to minio.
    ZMQ_S3_PROVIDER_ZIG         Zig executable. Defaults to zig.

Per-profile overrides:
    For profile "aws_us_east_1", set ZMQ_S3_AWS_US_EAST_1_ENDPOINT,
    ZMQ_S3_AWS_US_EAST_1_PORT, ZMQ_S3_AWS_US_EAST_1_BUCKET,
    ZMQ_S3_AWS_US_EAST_1_ACCESS_KEY, ZMQ_S3_AWS_US_EAST_1_SECRET_KEY.
    Optional ZMQ_S3_<PROFILE>_REGION and ZMQ_S3_<PROFILE>_PATH_STYLE are passed
    through for providers whose client configuration uses them.
"""

import os
import subprocess
import sys


RUN_ENABLED = os.environ.get("ZMQ_RUN_S3_PROVIDER_MATRIX") == "1"
ZIG = os.environ.get("ZMQ_S3_PROVIDER_ZIG", os.environ.get("ZIG", "zig"))


class MatrixError(Exception):
    pass


def run(cmd, timeout=300, env=None):
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        env=env,
    )
    if proc.returncode != 0:
        raise MatrixError(f"{cmd[0]} failed with exit code {proc.returncode}\n{proc.stdout}")
    return proc.stdout


def profile_names():
    raw = os.environ.get("ZMQ_S3_PROVIDER_PROFILES", "minio")
    names = [name.strip() for name in raw.split(",") if name.strip()]
    return names if names else ["minio"]


def profile_key(profile, suffix):
    sanitized = "".join(ch.upper() if ch.isalnum() else "_" for ch in profile)
    return f"ZMQ_S3_{sanitized}_{suffix}"


def profile_setting(profile, suffix, fallback):
    return os.environ.get(profile_key(profile, suffix), os.environ.get(f"ZMQ_S3_{suffix}", fallback))


def provider_env(profile):
    env = os.environ.copy()
    env["ZMQ_RUN_MINIO_TESTS"] = "1"
    env["ZMQ_S3_ENDPOINT"] = profile_setting(profile, "ENDPOINT", "127.0.0.1")
    env["ZMQ_S3_PORT"] = profile_setting(profile, "PORT", "9000")
    env["ZMQ_S3_BUCKET"] = profile_setting(profile, "BUCKET", "zmq-minio-it")
    env["ZMQ_S3_ACCESS_KEY"] = profile_setting(profile, "ACCESS_KEY", "minioadmin")
    env["ZMQ_S3_SECRET_KEY"] = profile_setting(profile, "SECRET_KEY", "minioadmin")

    region = profile_setting(profile, "REGION", None)
    if region:
        env["ZMQ_S3_REGION"] = region
    path_style = profile_setting(profile, "PATH_STYLE", None)
    if path_style:
        env["ZMQ_S3_PATH_STYLE"] = path_style
    return env


def run_profile(profile):
    env = provider_env(profile)
    run([ZIG, "build", "test-minio", "--summary", "all"], timeout=600, env=env)
    print(
        "ok: S3 provider profile "
        f"{profile} endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} bucket={env['ZMQ_S3_BUCKET']}"
    )


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_S3_PROVIDER_MATRIX=1 to run live S3 provider matrix")
        return 0

    profiles = profile_names()
    for profile in profiles:
        run_profile(profile)
    print(f"ok: S3 provider matrix passed for {', '.join(profiles)}")
    return 0


def self_test():
    old_env = os.environ.copy()
    try:
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "s3.amazonaws.com"
        os.environ["ZMQ_S3_AWS_US_EAST_1_PORT"] = "443"
        os.environ["ZMQ_S3_AWS_US_EAST_1_BUCKET"] = "zmq-parity"
        os.environ["ZMQ_S3_AWS_US_EAST_1_REGION"] = "us-east-1"

        names = profile_names()
        if names != ["minio", "aws_us_east_1"]:
            raise MatrixError(f"profile parsing failed: {names}")

        env = provider_env("aws_us_east_1")
        if env["ZMQ_S3_ENDPOINT"] != "s3.amazonaws.com":
            raise MatrixError("profile endpoint override failed")
        if env["ZMQ_S3_PORT"] != "443":
            raise MatrixError("profile port override failed")
        if env["ZMQ_S3_BUCKET"] != "zmq-parity":
            raise MatrixError("profile bucket override failed")
        if env["ZMQ_S3_REGION"] != "us-east-1":
            raise MatrixError("profile region override failed")

        print("ok: S3 provider matrix self-test")
        return 0
    finally:
        os.environ.clear()
        os.environ.update(old_env)


if __name__ == "__main__":
    try:
        if "--self-test" in sys.argv:
            sys.exit(self_test())
        sys.exit(main())
    except MatrixError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)

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
    ZMQ_S3_PROVIDER_REQUIRED_PROFILES comma-separated profile names that must be present.
    ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES comma-separated profile names that must run outage gates.
    ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES
                               comma-separated profile names that must run the ListObjectsV2 pagination gate.
    ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES
                               comma-separated profile names that must run the multipart edge gate.
    ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES
                               comma-separated profile names that must run a provider-specific
                               multipart fault-injection command.
    ZMQ_S3_PROVIDER_ZIG         Zig executable. Defaults to zig.

Per-profile overrides:
    For profile "aws_us_east_1", set ZMQ_S3_AWS_US_EAST_1_ENDPOINT,
    ZMQ_S3_AWS_US_EAST_1_PORT, ZMQ_S3_AWS_US_EAST_1_BUCKET,
    ZMQ_S3_AWS_US_EAST_1_ACCESS_KEY, ZMQ_S3_AWS_US_EAST_1_SECRET_KEY.
    Optional ZMQ_S3_<PROFILE>_SCHEME, ZMQ_S3_<PROFILE>_REGION,
    ZMQ_S3_<PROFILE>_PATH_STYLE, ZMQ_S3_<PROFILE>_TLS_CA_FILE,
    ZMQ_S3_<PROFILE>_SKIP_ENSURE_BUCKET, and
    ZMQ_S3_<PROFILE>_SKIP_MINIO_HEALTH are passed through for HTTPS and
    non-path-style providers.

Per-profile gates:
    ZMQ_S3_<PROFILE>_REQUIRE_LIST_PAGINATION=1 enables a live 1005-object
    ListObjectsV2 pagination gate for providers in that profile.
    ZMQ_S3_<PROFILE>_REQUIRE_MULTIPART_EDGE=1 enables a live uneven three-part
    multipart upload/get verification gate for providers in that profile.
    ZMQ_S3_<PROFILE>_RUN_MULTIPART_FAULT=1 runs a provider-specific multipart
    fault-injection command after the live MinIO/S3 suite. It requires
    ZMQ_S3_<PROFILE>_MULTIPART_FAULT_CMD.
    ZMQ_S3_<PROFILE>_RUN_PROCESS_CRASH=1 also runs the broker-process
    crash/replacement harness with that provider's S3 settings.
    ZMQ_S3_<PROFILE>_RUN_LIVE_OUTAGE=1 also runs the live-S3 chaos outage
    harness. It requires ZMQ_S3_<PROFILE>_OUTAGE_DOWN and
    ZMQ_S3_<PROFILE>_OUTAGE_UP commands that inject and heal provider access.
"""

import os
import shlex
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


def run_command_string(label, command, timeout=900, env=None):
    proc = subprocess.run(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        env=env,
    )
    if proc.returncode != 0:
        raise MatrixError(f"{label} failed with exit code {proc.returncode}\n{proc.stdout}")
    return proc.stdout


def profile_names():
    raw = os.environ.get("ZMQ_S3_PROVIDER_PROFILES", "minio")
    names = [name.strip() for name in raw.split(",") if name.strip()]
    return names if names else ["minio"]


def configured_names(env_name):
    raw = os.environ.get(env_name, "")
    return [name.strip() for name in raw.split(",") if name.strip()]


def profile_key(profile, suffix):
    sanitized = "".join(ch.upper() if ch.isalnum() else "_" for ch in profile)
    return f"ZMQ_S3_{sanitized}_{suffix}"


def profile_setting(profile, suffix, fallback):
    return os.environ.get(profile_key(profile, suffix), os.environ.get(f"ZMQ_S3_{suffix}", fallback))


def truthy(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def provider_env(profile):
    env = os.environ.copy()
    env["ZMQ_RUN_MINIO_TESTS"] = "1"
    env["ZMQ_S3_ENDPOINT"] = profile_setting(profile, "ENDPOINT", "127.0.0.1")
    env["ZMQ_S3_PORT"] = profile_setting(profile, "PORT", "9000")
    env["ZMQ_S3_BUCKET"] = profile_setting(profile, "BUCKET", "zmq-minio-it")
    env["ZMQ_S3_ACCESS_KEY"] = profile_setting(profile, "ACCESS_KEY", "minioadmin")
    env["ZMQ_S3_SECRET_KEY"] = profile_setting(profile, "SECRET_KEY", "minioadmin")

    scheme = profile_setting(profile, "SCHEME", None)
    if scheme:
        env["ZMQ_S3_SCHEME"] = scheme
    region = profile_setting(profile, "REGION", None)
    if region:
        env["ZMQ_S3_REGION"] = region
    path_style = profile_setting(profile, "PATH_STYLE", None)
    if path_style:
        env["ZMQ_S3_PATH_STYLE"] = path_style
    tls_ca_file = profile_setting(profile, "TLS_CA_FILE", None)
    if tls_ca_file:
        env["ZMQ_S3_TLS_CA_FILE"] = tls_ca_file
    skip_ensure_bucket = profile_setting(profile, "SKIP_ENSURE_BUCKET", None)
    if skip_ensure_bucket:
        env["ZMQ_S3_SKIP_ENSURE_BUCKET"] = skip_ensure_bucket
    skip_minio_health = profile_setting(profile, "SKIP_MINIO_HEALTH", None)
    if skip_minio_health:
        env["ZMQ_S3_SKIP_MINIO_HEALTH"] = skip_minio_health
    require_list_pagination = profile_setting(profile, "REQUIRE_LIST_PAGINATION", None)
    if require_list_pagination:
        env["ZMQ_S3_REQUIRE_LIST_PAGINATION"] = require_list_pagination
    require_multipart_edge = profile_setting(profile, "REQUIRE_MULTIPART_EDGE", None)
    if require_multipart_edge:
        env["ZMQ_S3_REQUIRE_MULTIPART_EDGE"] = require_multipart_edge
    return env


def provider_chaos_env(profile, env):
    chaos_env = env.copy()
    chaos_env["ZMQ_RUN_CHAOS_TESTS"] = "1"
    chaos_env["ZMQ_CHAOS_SCENARIOS"] = "live-s3-outage"
    for suffix in (
        "ENDPOINT",
        "PORT",
        "BUCKET",
        "ACCESS_KEY",
        "SECRET_KEY",
        "SCHEME",
        "REGION",
        "PATH_STYLE",
        "TLS_CA_FILE",
    ):
        value = env.get(f"ZMQ_S3_{suffix}")
        if value:
            chaos_env[f"ZMQ_CHAOS_S3_{suffix}"] = value

    outage_down = profile_setting(profile, "OUTAGE_DOWN", None)
    outage_up = profile_setting(profile, "OUTAGE_UP", None)
    if not outage_down or not outage_up:
        raise MatrixError(
            f"profile {profile} RUN_LIVE_OUTAGE requires "
            f"{profile_key(profile, 'OUTAGE_DOWN')} and {profile_key(profile, 'OUTAGE_UP')}"
        )
    chaos_env["ZMQ_CHAOS_S3_DOWN"] = outage_down
    chaos_env["ZMQ_CHAOS_S3_UP"] = outage_up
    return chaos_env


def provider_multipart_fault_env(profile, env):
    fault_env = env.copy()
    fault_env["ZMQ_S3_MULTIPART_FAULT_PROFILE"] = profile
    fault_env["ZMQ_S3_MULTIPART_FAULT_ENDPOINT"] = env["ZMQ_S3_ENDPOINT"]
    fault_env["ZMQ_S3_MULTIPART_FAULT_PORT"] = env["ZMQ_S3_PORT"]
    fault_env["ZMQ_S3_MULTIPART_FAULT_BUCKET"] = env["ZMQ_S3_BUCKET"]
    return fault_env


def profile_enabled(profile, suffix):
    return truthy(profile_setting(profile, suffix, "0"))


def validate_required_profiles(profiles):
    profile_set = set(profiles)
    required = configured_names("ZMQ_S3_PROVIDER_REQUIRED_PROFILES")
    missing = [profile for profile in required if profile not in profile_set]
    if missing:
        raise MatrixError(
            "required S3 provider profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing)
        )

    required_outage = configured_names("ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES")
    missing_outage_profiles = [profile for profile in required_outage if profile not in profile_set]
    if missing_outage_profiles:
        raise MatrixError(
            "required S3 outage profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing_outage_profiles)
        )
    disabled = [
        profile for profile in required_outage
        if not profile_enabled(profile, "RUN_LIVE_OUTAGE")
    ]
    if disabled:
        raise MatrixError(
            "required S3 outage profiles must set RUN_LIVE_OUTAGE=1: "
            + ", ".join(disabled)
        )
    missing_hooks = [
        profile for profile in required_outage
        if not profile_setting(profile, "OUTAGE_DOWN", None)
        or not profile_setting(profile, "OUTAGE_UP", None)
    ]
    if missing_hooks:
        raise MatrixError(
            "required S3 outage profiles must set OUTAGE_DOWN and OUTAGE_UP hooks: "
            + ", ".join(missing_hooks)
        )

    required_list_pagination = configured_names("ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES")
    missing_list_pagination_profiles = [
        profile for profile in required_list_pagination if profile not in profile_set
    ]
    if missing_list_pagination_profiles:
        raise MatrixError(
            "required S3 list-pagination profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing_list_pagination_profiles)
        )
    disabled_list_pagination = [
        profile for profile in required_list_pagination
        if not profile_enabled(profile, "REQUIRE_LIST_PAGINATION")
    ]
    if disabled_list_pagination:
        raise MatrixError(
            "required S3 list-pagination profiles must set REQUIRE_LIST_PAGINATION=1: "
            + ", ".join(disabled_list_pagination)
        )

    required_multipart_edge = configured_names("ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES")
    missing_multipart_edge_profiles = [profile for profile in required_multipart_edge if profile not in profile_set]
    if missing_multipart_edge_profiles:
        raise MatrixError(
            "required S3 multipart-edge profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing_multipart_edge_profiles)
        )
    disabled_multipart_edge = [
        profile for profile in required_multipart_edge
        if not profile_enabled(profile, "REQUIRE_MULTIPART_EDGE")
    ]
    if disabled_multipart_edge:
        raise MatrixError(
            "required S3 multipart-edge profiles must set REQUIRE_MULTIPART_EDGE=1: "
            + ", ".join(disabled_multipart_edge)
        )

    required_multipart_fault = configured_names("ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES")
    missing_multipart_fault_profiles = [profile for profile in required_multipart_fault if profile not in profile_set]
    if missing_multipart_fault_profiles:
        raise MatrixError(
            "required S3 multipart-fault profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing_multipart_fault_profiles)
        )
    disabled_multipart_fault = [
        profile for profile in required_multipart_fault
        if not profile_enabled(profile, "RUN_MULTIPART_FAULT")
    ]
    if disabled_multipart_fault:
        raise MatrixError(
            "required S3 multipart-fault profiles must set RUN_MULTIPART_FAULT=1: "
            + ", ".join(disabled_multipart_fault)
        )
    missing_multipart_fault_cmd = [
        profile for profile in required_multipart_fault
        if not profile_setting(profile, "MULTIPART_FAULT_CMD", None)
    ]
    if missing_multipart_fault_cmd:
        raise MatrixError(
            "required S3 multipart-fault profiles must set MULTIPART_FAULT_CMD: "
            + ", ".join(missing_multipart_fault_cmd)
        )


def run_profile(profile):
    env = provider_env(profile)
    run([ZIG, "build", "test-minio", "--summary", "all"], timeout=600, env=env)
    if profile_enabled(profile, "RUN_PROCESS_CRASH"):
        process_env = env.copy()
        process_env["ZMQ_RUN_PROCESS_CRASH_TESTS"] = "1"
        run([ZIG, "build", "test-s3-process-crash", "--summary", "all"], timeout=900, env=process_env)
    if profile_enabled(profile, "RUN_LIVE_OUTAGE"):
        run([ZIG, "build", "test-chaos", "--summary", "all"], timeout=900, env=provider_chaos_env(profile, env))
    if profile_enabled(profile, "RUN_MULTIPART_FAULT"):
        command = profile_setting(profile, "MULTIPART_FAULT_CMD", None)
        if not command:
            raise MatrixError(
                f"profile {profile} RUN_MULTIPART_FAULT requires "
                f"{profile_key(profile, 'MULTIPART_FAULT_CMD')}"
            )
        run_command_string(
            f"S3 multipart fault profile {profile}",
            command,
            timeout=900,
            env=provider_multipart_fault_env(profile, env),
        )
    print(
        "ok: S3 provider profile "
        f"{profile} endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} bucket={env['ZMQ_S3_BUCKET']}"
    )


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_S3_PROVIDER_MATRIX=1 to run live S3 provider matrix")
        return 0

    profiles = profile_names()
    validate_required_profiles(profiles)
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
        os.environ["ZMQ_S3_AWS_US_EAST_1_SCHEME"] = "https"
        os.environ["ZMQ_S3_AWS_US_EAST_1_PATH_STYLE"] = "false"
        os.environ["ZMQ_S3_AWS_US_EAST_1_SKIP_ENSURE_BUCKET"] = "1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_SKIP_MINIO_HEALTH"] = "1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_REQUIRE_LIST_PAGINATION"] = "1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE"] = "1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT"] = "1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN"] = "tc qdisc add dev lo root netem loss 100%"
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"] = "tc qdisc del dev lo root"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio,aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES"] = "aws_us_east_1"

        names = profile_names()
        if names != ["minio", "aws_us_east_1"]:
            raise MatrixError(f"profile parsing failed: {names}")
        validate_required_profiles(names)

        env = provider_env("aws_us_east_1")
        if env["ZMQ_S3_ENDPOINT"] != "s3.amazonaws.com":
            raise MatrixError("profile endpoint override failed")
        if env["ZMQ_S3_PORT"] != "443":
            raise MatrixError("profile port override failed")
        if env["ZMQ_S3_BUCKET"] != "zmq-parity":
            raise MatrixError("profile bucket override failed")
        if env["ZMQ_S3_REGION"] != "us-east-1":
            raise MatrixError("profile region override failed")
        if env["ZMQ_S3_SCHEME"] != "https":
            raise MatrixError("profile scheme override failed")
        if env["ZMQ_S3_PATH_STYLE"] != "false":
            raise MatrixError("profile path-style override failed")
        if env["ZMQ_S3_SKIP_ENSURE_BUCKET"] != "1":
            raise MatrixError("profile skip-ensure-bucket override failed")
        if env["ZMQ_S3_SKIP_MINIO_HEALTH"] != "1":
            raise MatrixError("profile skip-minio-health override failed")
        if env["ZMQ_S3_REQUIRE_LIST_PAGINATION"] != "1":
            raise MatrixError("profile pagination gate override failed")
        if env["ZMQ_S3_REQUIRE_MULTIPART_EDGE"] != "1":
            raise MatrixError("profile multipart-edge gate override failed")
        if not profile_enabled("aws_us_east_1", "RUN_MULTIPART_FAULT"):
            raise MatrixError("profile multipart-fault gate override failed")
        if not profile_enabled("aws_us_east_1", "RUN_PROCESS_CRASH"):
            raise MatrixError("profile process-crash gate override failed")
        if not profile_enabled("aws_us_east_1", "RUN_LIVE_OUTAGE"):
            raise MatrixError("profile live-outage gate override failed")
        chaos_env = provider_chaos_env("aws_us_east_1", env)
        if chaos_env["ZMQ_CHAOS_SCENARIOS"] != "live-s3-outage":
            raise MatrixError("profile live-outage scenario override failed")
        if chaos_env["ZMQ_CHAOS_S3_ENDPOINT"] != "s3.amazonaws.com":
            raise MatrixError("profile live-outage endpoint pass-through failed")
        if chaos_env["ZMQ_CHAOS_S3_DOWN"] != "tc qdisc add dev lo root netem loss 100%":
            raise MatrixError("profile live-outage down hook override failed")
        fault_env = provider_multipart_fault_env("aws_us_east_1", env)
        if fault_env["ZMQ_S3_MULTIPART_FAULT_PROFILE"] != "aws_us_east_1":
            raise MatrixError("profile multipart-fault context failed")
        if fault_env["ZMQ_S3_MULTIPART_FAULT_BUCKET"] != "zmq-parity":
            raise MatrixError("profile multipart-fault bucket pass-through failed")
        run_command_string(
            "multipart-fault self-test",
            profile_setting("aws_us_east_1", "MULTIPART_FAULT_CMD", None),
            env=fault_env,
        )

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES"] = "minio,aws_us_east_1"
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required outage profile did not fail validation")
        except MatrixError as exc:
            if "RUN_LIVE_OUTAGE" not in str(exc):
                raise

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES"] = "minio,aws_us_east_1"
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required list-pagination profile did not fail validation")
        except MatrixError as exc:
            if "REQUIRE_LIST_PAGINATION" not in str(exc):
                raise

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES"] = "minio,aws_us_east_1"
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required multipart-edge profile did not fail validation")
        except MatrixError as exc:
            if "REQUIRE_MULTIPART_EDGE" not in str(exc):
                raise

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES"] = "minio,aws_us_east_1"
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required multipart-fault profile did not fail validation")
        except MatrixError as exc:
            if "RUN_MULTIPART_FAULT" not in str(exc):
                raise

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

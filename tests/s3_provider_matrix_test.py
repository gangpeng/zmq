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
    ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES
                               comma-separated profile names that must run broker
                               process-crash/replacement gates.
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
    Per-profile values override global ZMQ_S3_* fallbacks. Optional
    ZMQ_S3_<PROFILE>_SCHEME, ZMQ_S3_<PROFILE>_REGION,
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
    Multipart fault commands receive ZMQ_S3_MULTIPART_FAULT_* context for the
    selected endpoint, bucket, credentials, scheme, region, path-style mode,
    and TLS CA file so the injected fault targets the same provider profile
    that passed the live S3 suite. Required commands must emit
    `ok: S3 multipart fault profile <profile> ... injected=true recovered=true source=command`
    with the selected provider endpoint, bucket, scheme, region, and path-style
    values before the matrix prints its release marker.
    ZMQ_S3_<PROFILE>_RUN_PROCESS_CRASH=1 also runs the broker-process
    crash/replacement harness with that provider's S3 settings.
    ZMQ_S3_<PROFILE>_RUN_LIVE_OUTAGE=1 also runs the live-S3 chaos outage
    harness. It requires ZMQ_S3_<PROFILE>_OUTAGE_DOWN and
    ZMQ_S3_<PROFILE>_OUTAGE_UP commands that inject and heal provider access.
    The wrapped chaos output must emit the selected provider summary line and
    outage recovery line with source=command before the matrix emits its
    provider outage release marker.
"""

import os
import shlex
import subprocess
import sys


ZIG = os.environ.get("ZMQ_S3_PROVIDER_ZIG", os.environ.get("ZIG", "zig"))
PLACEHOLDER_SETTING_VALUES = {"...", "placeholder", "required", "tbd", "todo"}
BOOL_TRUE_VALUES = {"1", "true", "yes", "on"}
BOOL_FALSE_VALUES = {"0", "false", "no", "off"}
DEFAULT_PROVIDER_PROFILE = "minio"
EXPLICIT_PROVIDER_SETTING_SUFFIXES = (
    "ENDPOINT",
    "PORT",
    "BUCKET",
    "ACCESS_KEY",
    "SECRET_KEY",
    "SCHEME",
    "REGION",
    "PATH_STYLE",
)
PROCESS_CRASH_SUMMARY_FIELDS = (
    "bucket",
    "topic",
    "group",
    "killed_broker",
    "fresh_data_dir",
    "first_offset",
    "committed_offset",
    "replacement_offset",
    "recovered_payloads",
)


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
    try:
        words = shlex.split(command or "")
    except ValueError as exc:
        raise MatrixError(f"{label} command is malformed: {exc}") from exc
    if not words:
        raise MatrixError(f"{label} command must contain at least one word")
    try:
        proc = subprocess.run(
            words,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            env=env,
        )
    except OSError as exc:
        raise MatrixError(f"{label} command could not start: {exc}") from exc
    if proc.returncode != 0:
        raise MatrixError(f"{label} failed with exit code {proc.returncode}\n{proc.stdout}")
    return proc.stdout


def process_crash_summary_fields(output):
    prefix = "ok: S3 process crash/replacement harness passed"
    detail_prefix = prefix + " ("
    source_suffix = ") source=command"
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.startswith(prefix):
            continue
        if not stripped.startswith(detail_prefix) or not stripped.endswith(source_suffix):
            return None
        fields = {}
        for token in stripped[len(detail_prefix) : -len(source_suffix)].split(","):
            if "=" not in token:
                return None
            key, value = token.strip().split("=", 1)
            if not key or not value:
                return None
            if key in fields:
                return None
            fields[key] = value
        if set(fields) != set(PROCESS_CRASH_SUMMARY_FIELDS):
            return None
        return fields
    return None


def process_crash_summary_int(fields, name, profile):
    value = fields.get(name)
    if value is None:
        raise MatrixError(
            f"profile {profile} process-crash output missing {name}"
        )
    try:
        return int(value, 10)
    except ValueError as exc:
        raise MatrixError(
            f"profile {profile} process-crash output field {name} must be an integer"
        ) from exc


def require_process_crash_evidence(output, profile, env):
    fields = process_crash_summary_fields(output)
    if fields is None:
        raise MatrixError(
            f"profile {profile} process-crash output missing detailed evidence: "
            "ok: S3 process crash/replacement harness passed "
            "(bucket=<bucket>, topic=<topic>, group=<group>, "
            "killed_broker=true, fresh_data_dir=true, first_offset=0, "
            "committed_offset=1, replacement_offset=<offset>, recovered_payloads=2) "
            "source=command"
        )
    expected_bucket = env["ZMQ_S3_BUCKET"]
    if fields.get("bucket") != expected_bucket:
        raise MatrixError(
            f"profile {profile} process-crash output bucket must match "
            f"selected provider bucket {expected_bucket}"
        )
    for key in ("topic", "group"):
        value = fields.get(key)
        if not value or setting_uses_placeholder(value):
            raise MatrixError(
                f"profile {profile} process-crash output missing non-placeholder {key}"
            )
    for key in ("killed_broker", "fresh_data_dir"):
        if fields.get(key) != "true":
            raise MatrixError(
                f"profile {profile} process-crash output must report {key}=true"
            )
    first_offset = process_crash_summary_int(fields, "first_offset", profile)
    committed_offset = process_crash_summary_int(fields, "committed_offset", profile)
    replacement_offset = process_crash_summary_int(fields, "replacement_offset", profile)
    recovered_payloads = process_crash_summary_int(fields, "recovered_payloads", profile)
    if first_offset != 0:
        raise MatrixError(
            f"profile {profile} process-crash output must report first_offset=0"
        )
    if committed_offset != 1:
        raise MatrixError(
            f"profile {profile} process-crash output must report committed_offset=1"
        )
    if replacement_offset <= first_offset:
        raise MatrixError(
            f"profile {profile} process-crash output replacement_offset must "
            "be greater than first_offset"
        )
    if recovered_payloads != 2:
        raise MatrixError(
            f"profile {profile} process-crash output must report recovered_payloads=2"
        )
    return fields


def process_crash_detail_marker(profile, fields):
    return (
        f"ok: S3 provider process-crash detail profile {profile} "
        f"bucket={fields['bucket']} topic={fields['topic']} "
        f"group={fields['group']} killed_broker={fields['killed_broker']} "
        f"fresh_data_dir={fields['fresh_data_dir']} "
        f"first_offset={fields['first_offset']} "
        f"committed_offset={fields['committed_offset']} "
        f"replacement_offset={fields['replacement_offset']} "
        f"recovered_payloads={fields['recovered_payloads']} source=command"
    )


def outage_provider_evidence_marker(profile, env):
    scheme, region, path_style = provider_summary_settings(env)
    return (
        "ok: chaos live-s3-outage provider "
        f"endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} "
        f"bucket={env['ZMQ_S3_BUCKET']} scheme={scheme} region={region} "
        f"path_style={path_style} source=command"
    )


def outage_detail_marker(profile, env):
    scheme, region, path_style = provider_summary_settings(env)
    return (
        f"ok: S3 provider outage detail profile {profile} "
        f"endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} "
        f"bucket={env['ZMQ_S3_BUCKET']} scheme={scheme} region={region} "
        f"path_style={path_style} down=true healed=true "
        "fail_closed=true recovered=true source=command"
    )


def require_outage_evidence(output, profile, env):
    provider_marker = outage_provider_evidence_marker(profile, env)
    if not any(line.strip() == provider_marker for line in output.splitlines()):
        raise MatrixError(
            f"profile {profile} live outage output missing provider evidence: "
            f"{provider_marker}"
        )
    marker = (
        "ok: chaos live-s3-outage down=true healed=true fail_closed=true "
        "recovered=true source=command"
    )
    if not any(line.strip() == marker for line in output.splitlines()):
        raise MatrixError(
            f"profile {profile} live outage output missing evidence: {marker}"
        )


def multipart_fault_evidence_marker(profile, env):
    scheme, region, path_style = provider_summary_settings(env)
    return (
        f"ok: S3 multipart fault profile {profile} "
        f"endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} "
        f"bucket={env['ZMQ_S3_BUCKET']} scheme={scheme} region={region} "
        f"path_style={path_style} injected=true recovered=true source=command"
    )


def require_multipart_fault_evidence(output, profile, env):
    marker = multipart_fault_evidence_marker(profile, env)
    for line in output.splitlines():
        if line.strip() == marker:
            return marker
    raise MatrixError(
        f"profile {profile} multipart-fault command output missing evidence: "
        f"{marker}"
    )


def list_value_uses_placeholder(value):
    stripped = str(value or "").strip()
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


def reject_placeholder_list_values(env_name, values):
    placeholders = [value for value in values if list_value_uses_placeholder(value)]
    if placeholders:
        raise MatrixError(
            f"{env_name} must not use placeholder values: "
            + ", ".join(placeholders)
        )


def parse_configured_names(env_name, raw):
    blank_name = False
    names = []
    for item in raw.split(","):
        name = item.strip()
        if not name:
            blank_name = True
            continue
        names.append(name)
    if not names:
        raise MatrixError(
            f"{env_name} must contain at least one comma-separated value"
        )
    if blank_name:
        raise MatrixError(f"{env_name} must not contain blank comma-separated values")
    reject_placeholder_list_values(env_name, names)
    duplicates = sorted(name for name in set(names) if names.count(name) > 1)
    if duplicates:
        raise MatrixError(
            f"{env_name} must not contain duplicate comma-separated values: "
            + ", ".join(duplicates)
        )
    return names


def profile_names():
    raw = os.environ.get("ZMQ_S3_PROVIDER_PROFILES")
    if raw is None:
        return [DEFAULT_PROVIDER_PROFILE]
    names = parse_configured_names("ZMQ_S3_PROVIDER_PROFILES", raw)
    validate_profile_tokens_unique(names)
    return names


def configured_names(env_name):
    raw = os.environ.get(env_name)
    if raw is None:
        return []
    return parse_configured_names(env_name, raw)


def profile_env_token(profile):
    return "".join(ch.upper() if ch.isalnum() else "_" for ch in profile)


def validate_profile_tokens_unique(profiles):
    by_token = {}
    for profile in profiles:
        token = profile_env_token(profile)
        previous = by_token.get(token)
        if previous is not None and previous != profile:
            raise MatrixError(
                f"S3 provider profile names {previous!r} and {profile!r} "
                f"map to the same environment token {token}"
            )
        by_token[token] = profile


def profile_key(profile, suffix):
    return f"ZMQ_S3_{profile_env_token(profile)}_{suffix}"


def profile_setting(profile, suffix, fallback):
    return profile_setting_source(profile, suffix, fallback)[1]


def profile_setting_source_detail(profile, suffix, fallback):
    specific_name = profile_key(profile, suffix)
    if specific_name in os.environ:
        return specific_name, os.environ[specific_name], True
    global_name = f"ZMQ_S3_{suffix}"
    if global_name in os.environ:
        return global_name, os.environ[global_name], True
    return global_name, fallback, False


def profile_setting_source(profile, suffix, fallback):
    name, value, _configured = profile_setting_source_detail(profile, suffix, fallback)
    return name, value


def require_explicit_provider_settings(profile):
    if profile == DEFAULT_PROVIDER_PROFILE:
        return
    missing = []
    for suffix in EXPLICIT_PROVIDER_SETTING_SUFFIXES:
        _name, _value, configured = profile_setting_source_detail(profile, suffix, None)
        if not configured:
            missing.append(f"{profile_key(profile, suffix)} or ZMQ_S3_{suffix}")
    if missing:
        raise MatrixError(
            f"profile {profile} requires explicit S3 provider settings: "
            + ", ".join(missing)
        )


def setting_uses_placeholder(value):
    if value is None:
        return False
    stripped = str(value).strip()
    if not stripped:
        return True
    return list_value_uses_placeholder(stripped)


def require_non_placeholder_setting(name, value):
    if setting_uses_placeholder(value):
        raise MatrixError(f"{name} must not be blank or use a placeholder value")


def require_positive_int_setting(name, value):
    require_non_placeholder_setting(name, value)
    try:
        parsed = int(str(value).strip(), 10)
    except ValueError as exc:
        raise MatrixError(f"{name} must be a positive integer") from exc
    if parsed <= 0:
        raise MatrixError(f"{name} must be a positive integer")


def strict_bool_text(name, value, default=None):
    if value is None:
        return default
    stripped = str(value).strip()
    if not stripped or list_value_uses_placeholder(stripped):
        raise MatrixError(f"{name} must not be blank or use a placeholder value")
    lowered = stripped.lower()
    if lowered in BOOL_TRUE_VALUES:
        return "true"
    if lowered in BOOL_FALSE_VALUES:
        return "false"
    raise MatrixError(f"{name} must be true or false")


def profile_bool_setting(profile, suffix, default=None):
    name, value = profile_setting_source(profile, suffix, default)
    return strict_bool_text(name, value, default)


def run_gate_enabled(name):
    return strict_bool_text(name, os.environ.get(name), False) == "true"


def s3_bool_text(value, default):
    return strict_bool_text("S3 provider PATH_STYLE", value, default)


def provider_summary_settings(env):
    scheme = env.get("ZMQ_S3_SCHEME", "http").strip().lower()
    if scheme not in ("http", "https"):
        raise MatrixError("S3 provider SCHEME must be http or https")
    region = env.get("ZMQ_S3_REGION", "us-east-1").strip()
    require_non_placeholder_setting("ZMQ_S3_REGION", region)
    path_style = s3_bool_text(env.get("ZMQ_S3_PATH_STYLE"), "true")
    return scheme, region, path_style


def validate_provider_env(profile, env):
    for suffix in ("ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET_KEY"):
        require_non_placeholder_setting(
            f"{profile_key(profile, suffix)} or ZMQ_S3_{suffix}",
            env.get(f"ZMQ_S3_{suffix}"),
        )
    require_positive_int_setting(
        f"{profile_key(profile, 'PORT')} or ZMQ_S3_PORT",
        env.get("ZMQ_S3_PORT"),
    )
    provider_summary_settings(env)


def provider_env(profile):
    require_explicit_provider_settings(profile)
    env = os.environ.copy()
    env["ZMQ_RUN_MINIO_TESTS"] = "1"
    env["ZMQ_S3_ENDPOINT"] = profile_setting(profile, "ENDPOINT", "127.0.0.1")
    env["ZMQ_S3_PORT"] = profile_setting(profile, "PORT", "9000")
    env["ZMQ_S3_BUCKET"] = profile_setting(profile, "BUCKET", "zmq-minio-it")
    env["ZMQ_S3_ACCESS_KEY"] = profile_setting(profile, "ACCESS_KEY", "minioadmin")
    env["ZMQ_S3_SECRET_KEY"] = profile_setting(profile, "SECRET_KEY", "minioadmin")

    scheme = profile_setting(profile, "SCHEME", None)
    if scheme is not None:
        env["ZMQ_S3_SCHEME"] = scheme
    region = profile_setting(profile, "REGION", None)
    if region is not None:
        env["ZMQ_S3_REGION"] = region
    path_style = profile_bool_setting(profile, "PATH_STYLE", None)
    if path_style is not None:
        env["ZMQ_S3_PATH_STYLE"] = path_style
    tls_ca_name, tls_ca_file = profile_setting_source(profile, "TLS_CA_FILE", None)
    if tls_ca_file is not None:
        require_non_placeholder_setting(tls_ca_name, tls_ca_file)
        env["ZMQ_S3_TLS_CA_FILE"] = tls_ca_file
    skip_ensure_bucket = profile_bool_setting(profile, "SKIP_ENSURE_BUCKET", None)
    if skip_ensure_bucket is not None:
        env["ZMQ_S3_SKIP_ENSURE_BUCKET"] = skip_ensure_bucket
    skip_minio_health = profile_bool_setting(profile, "SKIP_MINIO_HEALTH", None)
    if skip_minio_health is not None:
        env["ZMQ_S3_SKIP_MINIO_HEALTH"] = skip_minio_health
    require_list_pagination = profile_bool_setting(profile, "REQUIRE_LIST_PAGINATION", None)
    if require_list_pagination is not None:
        env["ZMQ_S3_REQUIRE_LIST_PAGINATION"] = require_list_pagination
    require_multipart_edge = profile_bool_setting(profile, "REQUIRE_MULTIPART_EDGE", None)
    if require_multipart_edge is not None:
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

    outage_down_name, outage_down = profile_setting_source(
        profile,
        "OUTAGE_DOWN",
        None,
    )
    outage_up_name, outage_up = profile_setting_source(
        profile,
        "OUTAGE_UP",
        None,
    )
    if outage_down is None or outage_up is None:
        raise MatrixError(
            f"profile {profile} RUN_LIVE_OUTAGE requires "
            f"{profile_key(profile, 'OUTAGE_DOWN')} and {profile_key(profile, 'OUTAGE_UP')}"
        )
    require_non_placeholder_setting(outage_down_name, outage_down)
    require_non_placeholder_setting(outage_up_name, outage_up)
    chaos_env["ZMQ_CHAOS_S3_DOWN"] = outage_down
    chaos_env["ZMQ_CHAOS_S3_UP"] = outage_up
    return chaos_env


def provider_multipart_fault_env(profile, env):
    fault_env = env.copy()
    fault_env["ZMQ_S3_MULTIPART_FAULT_PROFILE"] = profile
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
            fault_env[f"ZMQ_S3_MULTIPART_FAULT_{suffix}"] = value
    return fault_env


def profile_enabled(profile, suffix):
    return profile_bool_setting(profile, suffix, "false") == "true"


def validate_required_profiles(profiles):
    profile_set = set(profiles)
    required = configured_names("ZMQ_S3_PROVIDER_REQUIRED_PROFILES")
    required_set = set(required)
    missing = [profile for profile in required if profile not in profile_set]
    if missing:
        raise MatrixError(
            "required S3 provider profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing)
        )

    required_outage = configured_names("ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES")
    validate_required_profile_subset("required S3 outage profiles", required_outage, required_set)
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
    missing_hooks = []
    placeholder_hooks = []
    for profile in required_outage:
        _down_name, down_hook = profile_setting_source(
            profile,
            "OUTAGE_DOWN",
            None,
        )
        _up_name, up_hook = profile_setting_source(profile, "OUTAGE_UP", None)
        if down_hook is None or up_hook is None:
            missing_hooks.append(profile)
        elif setting_uses_placeholder(down_hook) or setting_uses_placeholder(up_hook):
            placeholder_hooks.append(profile)
    if missing_hooks:
        raise MatrixError(
            "required S3 outage profiles must set OUTAGE_DOWN and OUTAGE_UP hooks: "
            + ", ".join(missing_hooks)
        )
    if placeholder_hooks:
        raise MatrixError(
            "required S3 outage profiles must set non-placeholder "
            "OUTAGE_DOWN and OUTAGE_UP hooks: "
            + ", ".join(placeholder_hooks)
        )

    required_process_crash = configured_names("ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES")
    validate_required_profile_subset(
        "required S3 process-crash profiles",
        required_process_crash,
        required_set,
    )
    missing_process_crash_profiles = [
        profile for profile in required_process_crash if profile not in profile_set
    ]
    if missing_process_crash_profiles:
        raise MatrixError(
            "required S3 process-crash profiles missing from ZMQ_S3_PROVIDER_PROFILES: "
            + ", ".join(missing_process_crash_profiles)
        )
    disabled_process_crash = [
        profile for profile in required_process_crash
        if not profile_enabled(profile, "RUN_PROCESS_CRASH")
    ]
    if disabled_process_crash:
        raise MatrixError(
            "required S3 process-crash profiles must set RUN_PROCESS_CRASH=1: "
            + ", ".join(disabled_process_crash)
        )

    required_list_pagination = configured_names("ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES")
    validate_required_profile_subset(
        "required S3 list-pagination profiles",
        required_list_pagination,
        required_set,
    )
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
    validate_required_profile_subset(
        "required S3 multipart-edge profiles",
        required_multipart_edge,
        required_set,
    )
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
    validate_required_profile_subset(
        "required S3 multipart-fault profiles",
        required_multipart_fault,
        required_set,
    )
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
    missing_multipart_fault_cmd = []
    placeholder_multipart_fault_cmd = []
    for profile in required_multipart_fault:
        _cmd_name, command = profile_setting_source(
            profile,
            "MULTIPART_FAULT_CMD",
            None,
        )
        if command is None:
            missing_multipart_fault_cmd.append(profile)
        elif setting_uses_placeholder(command):
            placeholder_multipart_fault_cmd.append(profile)
    if missing_multipart_fault_cmd:
        raise MatrixError(
            "required S3 multipart-fault profiles must set MULTIPART_FAULT_CMD: "
            + ", ".join(missing_multipart_fault_cmd)
        )
    if placeholder_multipart_fault_cmd:
        raise MatrixError(
            "required S3 multipart-fault profiles must set non-placeholder "
            "MULTIPART_FAULT_CMD: "
            + ", ".join(placeholder_multipart_fault_cmd)
        )


def validate_required_profile_subset(label, profiles, required_set):
    if not profiles or not required_set:
        return
    outside = [profile for profile in profiles if profile not in required_set]
    if outside:
        raise MatrixError(
            f"{label} must also be listed in ZMQ_S3_PROVIDER_REQUIRED_PROFILES: "
            + ", ".join(outside)
        )


def run_profile(profile):
    env = provider_env(profile)
    validate_provider_env(profile, env)
    run([ZIG, "build", "test-minio", "--summary", "all"], timeout=600, env=env)
    print(f"ok: S3 provider live-suite profile {profile} command_started=true completed=true source=command")
    if profile_enabled(profile, "REQUIRE_LIST_PAGINATION"):
        print(f"ok: S3 provider list-pagination profile {profile} required=true completed=true source=command")
    if profile_enabled(profile, "REQUIRE_MULTIPART_EDGE"):
        print(f"ok: S3 provider multipart-edge profile {profile} required=true completed=true source=command")
    if profile_enabled(profile, "RUN_PROCESS_CRASH"):
        process_env = env.copy()
        process_env["ZMQ_RUN_PROCESS_CRASH_TESTS"] = "1"
        process_output = run([ZIG, "build", "test-s3-process-crash", "--summary", "all"], timeout=900, env=process_env)
        process_fields = require_process_crash_evidence(process_output, profile, env)
        print(process_crash_detail_marker(profile, process_fields))
        print(f"ok: S3 provider process-crash profile {profile} killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command")
    if profile_enabled(profile, "RUN_LIVE_OUTAGE"):
        outage_output = run([ZIG, "build", "test-chaos", "--summary", "all"], timeout=900, env=provider_chaos_env(profile, env))
        require_outage_evidence(outage_output, profile, env)
        print(outage_detail_marker(profile, env))
        print(f"ok: S3 provider outage profile {profile} down=true healed=true fail_closed=true recovered=true source=command")
    if profile_enabled(profile, "RUN_MULTIPART_FAULT"):
        command_name, command = profile_setting_source(
            profile,
            "MULTIPART_FAULT_CMD",
            None,
        )
        if command is None:
            raise MatrixError(
                f"profile {profile} RUN_MULTIPART_FAULT requires "
                f"{profile_key(profile, 'MULTIPART_FAULT_CMD')}"
            )
        require_non_placeholder_setting(
            command_name,
            command,
        )
        fault_output = run_command_string(
            f"S3 multipart fault profile {profile}",
            command,
            timeout=900,
            env=provider_multipart_fault_env(profile, env),
        )
        fault_marker = require_multipart_fault_evidence(fault_output, profile, env)
        print(fault_marker)
        print(f"ok: S3 provider multipart-fault profile {profile} command_started=true completed=true injected=true recovered=true source=command")
    scheme, region, path_style = provider_summary_settings(env)
    print(
        "ok: S3 provider profile "
        f"{profile} endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} "
        f"bucket={env['ZMQ_S3_BUCKET']} scheme={scheme} region={region} "
        f"path_style={path_style} source=command"
    )


def main():
    if not run_gate_enabled("ZMQ_RUN_S3_PROVIDER_MATRIX"):
        print("skip: set ZMQ_RUN_S3_PROVIDER_MATRIX=1 to run live S3 provider matrix")
        return 0

    profiles = profile_names()
    validate_required_profiles(profiles)
    for profile in profiles:
        run_profile(profile)
    print(f"ok: S3 provider matrix passed for {', '.join(profiles)} source=command")
    return 0


def self_test():
    old_env = os.environ.copy()
    try:
        os.environ["ZMQ_RUN_S3_PROVIDER_MATRIX"] = "placeholder"
        try:
            run_gate_enabled("ZMQ_RUN_S3_PROVIDER_MATRIX")
            raise MatrixError("placeholder S3 provider run gate was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_RUN_S3_PROVIDER_MATRIX"] = "   "
        try:
            run_gate_enabled("ZMQ_RUN_S3_PROVIDER_MATRIX")
            raise MatrixError("blank S3 provider run gate was accepted")
        except MatrixError as exc:
            if "blank" not in str(exc):
                raise
        os.environ["ZMQ_RUN_S3_PROVIDER_MATRIX"] = "maybe"
        try:
            run_gate_enabled("ZMQ_RUN_S3_PROVIDER_MATRIX")
            raise MatrixError("invalid S3 provider run gate was accepted")
        except MatrixError as exc:
            if "true or false" not in str(exc):
                raise
        os.environ["ZMQ_RUN_S3_PROVIDER_MATRIX"] = "on"
        if not run_gate_enabled("ZMQ_RUN_S3_PROVIDER_MATRIX"):
            raise MatrixError("truthy S3 provider run gate was not accepted")
        os.environ.pop("ZMQ_RUN_S3_PROVIDER_MATRIX", None)

        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"
        os.environ["ZMQ_S3_ENDPOINT"] = "global-s3.example.test"
        os.environ["ZMQ_S3_PORT"] = "9443"
        os.environ["ZMQ_S3_BUCKET"] = "global-zmq-parity"
        os.environ["ZMQ_S3_ACCESS_KEY"] = "global-akid"
        os.environ["ZMQ_S3_SECRET_KEY"] = "global-secret"
        os.environ["ZMQ_S3_REGION"] = "us-west-2"
        os.environ["ZMQ_S3_SCHEME"] = "https"
        os.environ["ZMQ_S3_PATH_STYLE"] = "true"
        os.environ["ZMQ_S3_TLS_CA_FILE"] = "/tmp/global-s3-ca.pem"
        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "s3.amazonaws.com"
        os.environ["ZMQ_S3_AWS_US_EAST_1_PORT"] = "443"
        os.environ["ZMQ_S3_AWS_US_EAST_1_BUCKET"] = "zmq-parity"
        os.environ["ZMQ_S3_AWS_US_EAST_1_ACCESS_KEY"] = "akid"
        os.environ["ZMQ_S3_AWS_US_EAST_1_SECRET_KEY"] = "secret"
        os.environ["ZMQ_S3_AWS_US_EAST_1_REGION"] = "us-east-1"
        os.environ["ZMQ_S3_AWS_US_EAST_1_SCHEME"] = "https"
        os.environ["ZMQ_S3_AWS_US_EAST_1_PATH_STYLE"] = "false"
        os.environ["ZMQ_S3_AWS_US_EAST_1_TLS_CA_FILE"] = "/tmp/aws-ca.pem"
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
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES"] = "aws_us_east_1"

        names = profile_names()
        if names != ["minio", "aws_us_east_1"]:
            raise MatrixError(f"profile parsing failed: {names}")
        validate_required_profiles(names)

        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "   "
        try:
            profile_names()
            raise MatrixError("blank S3 provider profile list was accepted")
        except MatrixError as exc:
            if "at least one comma-separated value" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"

        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio,,aws_us_east_1"
        try:
            profile_names()
            raise MatrixError("embedded blank S3 provider profile was accepted")
        except MatrixError as exc:
            if "blank comma-separated" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"

        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = ",,,"
        try:
            profile_names()
            raise MatrixError("empty S3 provider profile list was accepted")
        except MatrixError as exc:
            if "at least one comma-separated value" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio,aws_us_east_1,minio"
        try:
            profile_names()
            raise MatrixError("duplicate S3 provider profile was accepted")
        except MatrixError as exc:
            if "duplicate comma-separated" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "aws-us-east-1,aws_us_east_1"
        try:
            profile_names()
            raise MatrixError("colliding S3 provider profile names were accepted")
        except MatrixError as exc:
            if "same environment token" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"

        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "placeholder"
        try:
            profile_names()
            raise MatrixError("placeholder S3 provider profile was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"

        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "<profile>"
        try:
            profile_names()
            raise MatrixError("angle-bracket placeholder S3 provider profile was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_PROFILES"] = "minio, aws_us_east_1"

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = ",,,"
        try:
            validate_required_profiles(names)
            raise MatrixError("empty required S3 provider list was accepted")
        except MatrixError as exc:
            if "at least one comma-separated value" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio,aws_us_east_1"

        for env_name in (
            "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
        ):
            old_value = os.environ[env_name]
            os.environ[env_name] = "   "
            try:
                validate_required_profiles(names)
                raise MatrixError(f"blank {env_name} list was accepted")
            except MatrixError as exc:
                if "at least one comma-separated value" not in str(exc):
                    raise
            os.environ[env_name] = "minio,,aws_us_east_1"
            try:
                validate_required_profiles(names)
                raise MatrixError(f"embedded blank {env_name} list was accepted")
            except MatrixError as exc:
                if "blank comma-separated" not in str(exc):
                    raise
            os.environ[env_name] = "minio,aws_us_east_1,minio"
            try:
                validate_required_profiles(names)
                raise MatrixError(f"duplicate {env_name} list was accepted")
            except MatrixError as exc:
                if "duplicate comma-separated" not in str(exc):
                    raise
            os.environ[env_name] = old_value

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio,placeholder"
        try:
            validate_required_profiles(names)
            raise MatrixError("placeholder required S3 provider profile was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio,aws_us_east_1"

        inherited_env = provider_env("minio")
        if inherited_env["ZMQ_S3_ENDPOINT"] != "global-s3.example.test":
            raise MatrixError("global S3 provider endpoint fallback failed")
        if inherited_env["ZMQ_S3_PORT"] != "9443":
            raise MatrixError("global S3 provider port fallback failed")
        if inherited_env["ZMQ_S3_BUCKET"] != "global-zmq-parity":
            raise MatrixError("global S3 provider bucket fallback failed")
        if inherited_env["ZMQ_S3_ACCESS_KEY"] != "global-akid":
            raise MatrixError("global S3 provider access-key fallback failed")
        if inherited_env["ZMQ_S3_SECRET_KEY"] != "global-secret":
            raise MatrixError("global S3 provider secret-key fallback failed")
        if inherited_env["ZMQ_S3_REGION"] != "us-west-2":
            raise MatrixError("global S3 provider region fallback failed")
        if inherited_env["ZMQ_S3_SCHEME"] != "https":
            raise MatrixError("global S3 provider scheme fallback failed")
        if inherited_env["ZMQ_S3_PATH_STYLE"] != "true":
            raise MatrixError("global S3 provider path-style fallback failed")
        if inherited_env["ZMQ_S3_TLS_CA_FILE"] != "/tmp/global-s3-ca.pem":
            raise MatrixError("global S3 provider TLS CA fallback failed")

        global_only_env = provider_env("global_only")
        if global_only_env["ZMQ_S3_ENDPOINT"] != "global-s3.example.test":
            raise MatrixError("global-only S3 provider endpoint fallback failed")
        if global_only_env["ZMQ_S3_ACCESS_KEY"] != "global-akid":
            raise MatrixError("global-only S3 provider credential fallback failed")
        if global_only_env["ZMQ_S3_SCHEME"] != "https":
            raise MatrixError("global-only S3 provider scheme fallback failed")
        if global_only_env["ZMQ_S3_PATH_STYLE"] != "true":
            raise MatrixError("global-only S3 provider path-style fallback failed")
        validate_provider_env("global_only", global_only_env)

        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = ""
        try:
            validate_provider_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("blank S3 provider endpoint used global fallback")
        except MatrixError as exc:
            if "ENDPOINT" not in str(exc) or "blank" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "s3.amazonaws.com"

        os.environ["ZMQ_S3_AWS_US_EAST_1_SCHEME"] = ""
        try:
            validate_provider_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("blank S3 provider scheme used global fallback")
        except MatrixError as exc:
            if "SCHEME" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_SCHEME"] = "https"

        os.environ["ZMQ_S3_RUN_LIVE_OUTAGE"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE"] = ""
        try:
            profile_enabled("aws_us_east_1", "RUN_LIVE_OUTAGE")
            raise MatrixError("blank S3 provider outage enable used global fallback")
        except MatrixError as exc:
            if "RUN_LIVE_OUTAGE" not in str(exc) or "blank" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE"] = "true"
        os.environ.pop("ZMQ_S3_RUN_LIVE_OUTAGE", None)

        os.environ["ZMQ_S3_OUTAGE_UP"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"] = ""
        try:
            provider_chaos_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("blank S3 provider outage hook used global fallback")
        except MatrixError as exc:
            if "OUTAGE_UP" not in str(exc) or "blank" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"] = "tc qdisc del dev lo root"
        os.environ.pop("ZMQ_S3_OUTAGE_UP", None)

        os.environ["ZMQ_S3_RUN_MULTIPART_FAULT"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT"] = ""
        try:
            profile_enabled("aws_us_east_1", "RUN_MULTIPART_FAULT")
            raise MatrixError("blank S3 provider multipart-fault enable used global fallback")
        except MatrixError as exc:
            if "RUN_MULTIPART_FAULT" not in str(exc) or "blank" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT"] = "1"
        os.environ.pop("ZMQ_S3_RUN_MULTIPART_FAULT", None)

        os.environ["ZMQ_S3_MULTIPART_FAULT_CMD"] = "true"
        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = ""
        try:
            validate_required_profiles(names)
            raise MatrixError("blank S3 provider multipart-fault command used global fallback")
        except MatrixError as exc:
            if "MULTIPART_FAULT_CMD" not in str(exc) or "non-placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = "true"
        os.environ.pop("ZMQ_S3_MULTIPART_FAULT_CMD", None)

        saved_explicit_settings = {}
        for suffix in EXPLICIT_PROVIDER_SETTING_SUFFIXES:
            for env_name in (f"ZMQ_S3_{suffix}", profile_key("aws_us_east_1", suffix)):
                saved_explicit_settings[env_name] = os.environ.pop(env_name, None)
        try:
            provider_env("aws_us_east_1")
            raise MatrixError("missing explicit non-minio S3 provider settings was accepted")
        except MatrixError as exc:
            if "requires explicit S3 provider settings" not in str(exc):
                raise
        finally:
            for env_name, value in saved_explicit_settings.items():
                if value is not None:
                    os.environ[env_name] = value

        env = provider_env("aws_us_east_1")
        if env["ZMQ_S3_ENDPOINT"] != "s3.amazonaws.com":
            raise MatrixError("profile endpoint override failed")
        if env["ZMQ_S3_PORT"] != "443":
            raise MatrixError("profile port override failed")
        if env["ZMQ_S3_BUCKET"] != "zmq-parity":
            raise MatrixError("profile bucket override failed")
        if env["ZMQ_S3_ACCESS_KEY"] != "akid":
            raise MatrixError("profile access-key override failed")
        if env["ZMQ_S3_SECRET_KEY"] != "secret":
            raise MatrixError("profile secret-key override failed")
        if env["ZMQ_S3_REGION"] != "us-east-1":
            raise MatrixError("profile region override failed")
        if env["ZMQ_S3_SCHEME"] != "https":
            raise MatrixError("profile scheme override failed")
        if env["ZMQ_S3_PATH_STYLE"] != "false":
            raise MatrixError("profile path-style override failed")
        if env["ZMQ_S3_TLS_CA_FILE"] != "/tmp/aws-ca.pem":
            raise MatrixError("profile TLS CA override failed")
        validate_provider_env("aws_us_east_1", env)
        if env["ZMQ_S3_SKIP_ENSURE_BUCKET"] != "true":
            raise MatrixError("profile skip-ensure-bucket override failed")
        if env["ZMQ_S3_SKIP_MINIO_HEALTH"] != "true":
            raise MatrixError("profile skip-minio-health override failed")
        if env["ZMQ_S3_REQUIRE_LIST_PAGINATION"] != "true":
            raise MatrixError("profile pagination gate override failed")
        if env["ZMQ_S3_REQUIRE_MULTIPART_EDGE"] != "true":
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
        if fault_env["ZMQ_S3_MULTIPART_FAULT_ACCESS_KEY"] != "akid":
            raise MatrixError("profile multipart-fault access-key pass-through failed")
        if fault_env["ZMQ_S3_MULTIPART_FAULT_SECRET_KEY"] != "secret":
            raise MatrixError("profile multipart-fault secret-key pass-through failed")
        if fault_env["ZMQ_S3_MULTIPART_FAULT_SCHEME"] != "https":
            raise MatrixError("profile multipart-fault scheme pass-through failed")
        if fault_env["ZMQ_S3_MULTIPART_FAULT_REGION"] != "us-east-1":
            raise MatrixError("profile multipart-fault region pass-through failed")
        if fault_env["ZMQ_S3_MULTIPART_FAULT_PATH_STYLE"] != "false":
            raise MatrixError("profile multipart-fault path-style pass-through failed")
        if fault_env["ZMQ_S3_MULTIPART_FAULT_TLS_CA_FILE"] != "/tmp/aws-ca.pem":
            raise MatrixError("profile multipart-fault TLS CA pass-through failed")

        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "placeholder"
        try:
            validate_provider_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("placeholder S3 provider endpoint was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "s3.amazonaws.com"

        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "<host>"
        try:
            validate_provider_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("angle-bracket placeholder S3 provider endpoint was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_ENDPOINT"] = "s3.amazonaws.com"

        os.environ["ZMQ_S3_AWS_US_EAST_1_PORT"] = "0"
        try:
            validate_provider_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("non-positive S3 provider port was accepted")
        except MatrixError as exc:
            if "positive integer" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_PORT"] = "443"

        os.environ["ZMQ_S3_AWS_US_EAST_1_REGION"] = "required"
        try:
            validate_provider_env("aws_us_east_1", provider_env("aws_us_east_1"))
            raise MatrixError("placeholder S3 provider region was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_REGION"] = "us-east-1"

        os.environ["ZMQ_S3_AWS_US_EAST_1_TLS_CA_FILE"] = ""
        try:
            provider_env("aws_us_east_1")
            raise MatrixError("blank S3 provider TLS CA was accepted")
        except MatrixError as exc:
            if "TLS_CA_FILE" not in str(exc) or "blank" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_TLS_CA_FILE"] = "/tmp/aws-ca.pem"

        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"] = "placeholder"
        try:
            provider_chaos_env("aws_us_east_1", env)
            raise MatrixError("placeholder S3 provider outage hook was accepted")
        except MatrixError as exc:
            if "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"] = "tc qdisc del dev lo root"

        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN"] = "placeholder"
        try:
            validate_required_profiles(names)
            raise MatrixError("placeholder required outage hooks did not fail validation")
        except MatrixError as exc:
            if "non-placeholder OUTAGE_DOWN and OUTAGE_UP" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN"] = "tc qdisc add dev lo root netem loss 100%"

        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = "placeholder"
        try:
            validate_required_profiles(names)
            raise MatrixError("placeholder multipart-fault command was accepted")
        except MatrixError as exc:
            if "non-placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = "true"

        os.environ["ZMQ_S3_AWS_US_EAST_1_SCHEME"] = "ftp"
        try:
            provider_summary_settings(provider_env("aws_us_east_1"))
            raise MatrixError("invalid S3 provider scheme was accepted")
        except MatrixError as exc:
            if "SCHEME" not in str(exc) or "http or https" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_SCHEME"] = "https"

        os.environ["ZMQ_S3_AWS_US_EAST_1_PATH_STYLE"] = "sometimes"
        try:
            provider_summary_settings(provider_env("aws_us_east_1"))
            raise MatrixError("invalid S3 provider path-style was accepted")
        except MatrixError as exc:
            if "PATH_STYLE" not in str(exc) or "true or false" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_PATH_STYLE"] = "false"

        os.environ["ZMQ_S3_AWS_US_EAST_1_SKIP_ENSURE_BUCKET"] = "sometimes"
        try:
            provider_env("aws_us_east_1")
            raise MatrixError("invalid S3 provider skip-ensure-bucket flag was accepted")
        except MatrixError as exc:
            if "SKIP_ENSURE_BUCKET" not in str(exc) or "true or false" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_SKIP_ENSURE_BUCKET"] = "1"

        for env_name, action, assertion_message in (
            (
                "ZMQ_S3_AWS_US_EAST_1_SKIP_MINIO_HEALTH",
                lambda: provider_env("aws_us_east_1"),
                "invalid S3 provider skip-minio-health flag was accepted",
            ),
            (
                "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE",
                lambda: provider_env("aws_us_east_1"),
                "invalid S3 provider multipart-edge gate flag was accepted",
            ),
            (
                "ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE",
                lambda: profile_enabled("aws_us_east_1", "RUN_LIVE_OUTAGE"),
                "invalid S3 provider live-outage gate flag was accepted",
            ),
            (
                "ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT",
                lambda: profile_enabled("aws_us_east_1", "RUN_MULTIPART_FAULT"),
                "invalid S3 provider multipart-fault gate flag was accepted",
            ),
        ):
            os.environ[env_name] = "sometimes"
            try:
                action()
                raise MatrixError(assertion_message)
            except MatrixError as exc:
                if env_name not in str(exc) or "true or false" not in str(exc):
                    raise
            os.environ[env_name] = "1"

        os.environ["ZMQ_S3_AWS_US_EAST_1_REQUIRE_LIST_PAGINATION"] = "placeholder"
        try:
            provider_env("aws_us_east_1")
            raise MatrixError("placeholder S3 provider pagination gate flag was accepted")
        except MatrixError as exc:
            if "REQUIRE_LIST_PAGINATION" not in str(exc) or "placeholder" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_REQUIRE_LIST_PAGINATION"] = "1"

        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH"] = "maybe"
        try:
            profile_enabled("aws_us_east_1", "RUN_PROCESS_CRASH")
            raise MatrixError("invalid S3 provider process-crash gate flag was accepted")
        except MatrixError as exc:
            if "RUN_PROCESS_CRASH" not in str(exc) or "true or false" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH"] = "true"

        fault_marker = multipart_fault_evidence_marker("aws_us_east_1", env)
        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = (
            f"{shlex.quote(sys.executable)} -c "
            f"{shlex.quote(f'print({fault_marker!r})')}"
        )
        fault_output = run_command_string(
            "multipart-fault self-test",
            profile_setting("aws_us_east_1", "MULTIPART_FAULT_CMD", None),
            env=fault_env,
        )
        require_multipart_fault_evidence(fault_output, "aws_us_east_1", env)
        if multipart_fault_evidence_marker("aws_us_east_1", env) != fault_marker:
            raise MatrixError("multipart-fault detail marker did not match selected provider")
        try:
            require_multipart_fault_evidence(
                "ok: S3 multipart fault profile aws_us_east_1",
                "aws_us_east_1",
                env,
            )
            raise MatrixError("bare multipart-fault output evidence was accepted")
        except MatrixError as exc:
            if "multipart-fault command output missing evidence" not in str(exc):
                raise
        try:
            require_multipart_fault_evidence(
                fault_marker.replace("bucket=zmq-parity", "bucket=wrong-bucket"),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("mismatched multipart-fault output evidence was accepted")
        except MatrixError as exc:
            if "multipart-fault command output missing evidence" not in str(exc):
                raise
        try:
            run_command_string("blank multipart-fault self-test", "   ", env=fault_env)
            raise MatrixError("blank multipart-fault command was accepted")
        except MatrixError as exc:
            if "at least one word" not in str(exc):
                raise
        try:
            run_command_string(
                "malformed multipart-fault self-test",
                "'unterminated",
                env=fault_env,
            )
            raise MatrixError("malformed multipart-fault command was accepted")
        except MatrixError as exc:
            if "malformed" not in str(exc):
                raise
        try:
            run_command_string(
                "unstartable multipart-fault self-test",
                "__zmq_missing_hook_command__",
                env=fault_env,
            )
            raise MatrixError("unstartable multipart-fault command was accepted")
        except MatrixError as exc:
            if "could not start" not in str(exc):
                raise
        process_crash_marker = (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-parity, topic=zmq-process-crash, "
            "group=zmq-process-crash-group, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=2, recovered_payloads=2) source=command"
        )
        process_crash_fields = require_process_crash_evidence(
            process_crash_marker,
            "aws_us_east_1",
            env,
        )
        expected_process_crash_detail_marker = (
            "ok: S3 provider process-crash detail profile aws_us_east_1 "
            "bucket=zmq-parity topic=zmq-process-crash "
            "group=zmq-process-crash-group killed_broker=true "
            "fresh_data_dir=true first_offset=0 committed_offset=1 "
            "replacement_offset=2 recovered_payloads=2 source=command"
        )
        if (
            process_crash_detail_marker("aws_us_east_1", process_crash_fields)
            != expected_process_crash_detail_marker
        ):
            raise MatrixError("process-crash detail marker did not match validated fields")
        try:
            require_process_crash_evidence(
                "ok: S3 process crash/replacement harness passed",
                "aws_us_east_1",
                env,
            )
            raise MatrixError("bare process-crash output evidence was accepted")
        except MatrixError as exc:
            if "detailed evidence" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace(" source=command", ""),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("process-crash output without source=command was accepted")
        except MatrixError as exc:
            if "source=command" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace("source=command", "source=wrapper"),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("process-crash output with wrapper source was accepted")
        except MatrixError as exc:
            if "source=command" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace("bucket=zmq-parity", "bucket=wrong-bucket"),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("mismatched process-crash bucket was accepted")
        except MatrixError as exc:
            if "selected provider bucket" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace(
                    "topic=zmq-process-crash",
                    "topic=wrong-topic, topic=zmq-process-crash",
                ),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("duplicate process-crash output field was accepted")
        except MatrixError as exc:
            if "detailed evidence" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace(
                    "recovered_payloads=2) source=command",
                    "recovered_payloads=2, unchecked=true) source=command",
                ),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("unknown process-crash output field was accepted")
        except MatrixError as exc:
            if "detailed evidence" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace("replacement_offset=2", "replacement_offset=0"),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("stale process-crash replacement offset was accepted")
        except MatrixError as exc:
            if "greater than first_offset" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace("committed_offset=1", "committed_offset=2"),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("wrong process-crash committed offset was accepted")
        except MatrixError as exc:
            if "committed_offset=1" not in str(exc):
                raise
        try:
            require_process_crash_evidence(
                process_crash_marker.replace("recovered_payloads=2", "recovered_payloads=1"),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("wrong process-crash recovered payload count was accepted")
        except MatrixError as exc:
            if "recovered_payloads=2" not in str(exc):
                raise
        outage_provider_marker = outage_provider_evidence_marker("aws_us_east_1", env)
        outage_detail_output_marker = (
            "ok: chaos live-s3-outage down=true healed=true "
            "fail_closed=true recovered=true source=command"
        )
        require_outage_evidence(
            f"{outage_provider_marker}\n{outage_detail_output_marker}",
            "aws_us_east_1",
            env,
        )
        expected_outage_detail_marker = (
            "ok: S3 provider outage detail profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-parity "
            "scheme=https region=us-east-1 path_style=false "
            "down=true healed=true fail_closed=true recovered=true source=command"
        )
        if outage_detail_marker("aws_us_east_1", env) != expected_outage_detail_marker:
            raise MatrixError("outage detail marker did not match selected provider")
        try:
            require_outage_evidence(outage_detail_output_marker, "aws_us_east_1", env)
            raise MatrixError("outage output without provider evidence was accepted")
        except MatrixError as exc:
            if "provider evidence" not in str(exc):
                raise
        try:
            require_outage_evidence(
                (
                    "ok: chaos live-s3-outage provider "
                    "endpoint=s3.amazonaws.com:9000 bucket=zmq-parity "
                    "scheme=https region=us-east-1 path_style=false\n"
                    f"{outage_detail_output_marker}"
                ),
                "aws_us_east_1",
                env,
            )
            raise MatrixError("mismatched outage provider evidence was accepted")
        except MatrixError as exc:
            if "s3.amazonaws.com:443" not in str(exc):
                raise
        try:
            require_outage_evidence(
                f"{outage_provider_marker}\nok: chaos live-s3-outage",
                "aws_us_east_1",
                env,
            )
            raise MatrixError("bare outage output evidence was accepted")
        except MatrixError as exc:
            if "fail_closed=true recovered=true" not in str(exc):
                raise

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio"
        try:
            validate_required_profiles(names)
            raise MatrixError("required sub-profile outside provider set was accepted")
        except MatrixError as exc:
            if "ZMQ_S3_PROVIDER_REQUIRED_PROFILES" not in str(exc):
                raise
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio,aws_us_east_1"

        os.environ.pop("ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN", None)
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required outage hooks did not fail validation")
        except MatrixError as exc:
            if "OUTAGE_DOWN and OUTAGE_UP" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN"] = "tc qdisc add dev lo root netem loss 100%"

        os.environ.pop("ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD", None)
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required multipart-fault command did not fail validation")
        except MatrixError as exc:
            if "MULTIPART_FAULT_CMD" not in str(exc):
                raise
        os.environ["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = "true"

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES"] = "minio,aws_us_east_1"
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required outage profile did not fail validation")
        except MatrixError as exc:
            if "RUN_LIVE_OUTAGE" not in str(exc):
                raise

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES"] = "aws_us_east_1"
        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES"] = "minio,aws_us_east_1"
        try:
            validate_required_profiles(names)
            raise MatrixError("missing required process-crash profile did not fail validation")
        except MatrixError as exc:
            if "RUN_PROCESS_CRASH" not in str(exc):
                raise

        os.environ["ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES"] = "aws_us_east_1"
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

#!/usr/bin/env python3
"""
Validate AutoMQ parity release evidence.

The release criteria require more than green local tests: a release decision
must name the exact commit, include command output for every required gate, pin
the environment-specific coverage variables, and account for unsupported or
partial surfaces. This checker validates that evidence manifest without needing
live S3 providers or external clients.

Run:
    ZMQ_RELEASE_EVIDENCE=/path/to/release-evidence.json python3 tests/release_evidence_test.py

Manifest shape:
    {
      "commit": "40 hex chars",
      "environment": {"ZMQ_KRAFT_REQUIRED_NETWORK_PHASES": "...", ...},
      "commands": [
        {"command": "ZMQ_RUN_CHAOS_TESTS=1 /tmp/zig-aarch64-linux-0.16.0/zig build test-chaos --summary all",
         "exit_code": 0,
         "output": "... captured command output ..."}
      ],
      "unsupported_or_partial_surfaces": [
        {"surface": "...", "status": "...", "evidence": "..."}
      ],
      "known_data_loss_bug": false,
      "advertised_stub_api": false,
      "untriaged_durability_failure": false,
      "automq_complete": false
    }
"""

import json
import base64
import math
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RELEASE_CRITERIA_PATH = os.path.join(PROJECT_DIR, "docs", "RELEASE_CRITERIA.md")
BUILD_ZIG_PATH = os.path.join(PROJECT_DIR, "build.zig")
BENCHMARK_MAIN_PATH = os.path.join(PROJECT_DIR, "benchmarks", "main.zig")
BENCHMARK_COMPARE_PATH = os.path.join(PROJECT_DIR, "benchmarks", "benchmark_compare.py")
CHAOS_TEST_PATH = os.path.join(PROJECT_DIR, "tests", "chaos_test.py")
CLIENT_MATRIX_TEST_PATH = os.path.join(PROJECT_DIR, "tests", "client_matrix_test.py")
E2E_TEST_PATH = os.path.join(PROJECT_DIR, "tests", "e2e_test.py")
KRAFT_FAILOVER_TEST_PATH = os.path.join(PROJECT_DIR, "tests", "kraft_failover_test.py")
S3_PROCESS_CRASH_TEST_PATH = os.path.join(
    PROJECT_DIR,
    "tests",
    "s3_process_crash_test.py",
)
S3_PROVIDER_MATRIX_TEST_PATH = os.path.join(
    PROJECT_DIR,
    "tests",
    "s3_provider_matrix_test.py",
)
RELEASE_ZIG = "/tmp/zig-aarch64-linux-0.16.0/zig"
BENCHMARK_RESULTS_ARTIFACT = "benchmarks/results.json"
ZIG_BUILD_SUMMARY_RE = re.compile(
    r"Build Summary:\s+([1-9][0-9]*)/([1-9][0-9]*) steps succeeded"
    r"(?:;\s+([0-9][0-9]*)/([0-9][0-9]*) tests passed(?:.*)?)?"
)
KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS = (
    "network_partition=[",
    "automq_stream_id=",
    "automq_deleted_stream_id=",
    "automq_stream_set_object_id=",
    "automq_node_id=",
    "automq_zone_router_epoch=",
    "old_leader=",
    "new_leader=",
    "restarted_controller=",
    "epoch=",
    "automq_old_leader=",
    "automq_new_leader=",
    "old_leader_rejoined=true",
    "old_leader_fresh_rejoin=true",
    "automq_old_leader_fresh_rejoin=true",
    "allocate_producer_ids_checked=true",
    "allocate_producer_ids_follower_rejection_checked=true",
    "describe_quorum_v2_checked=true",
    "fetch_snapshot_v1_checked=true",
    "all_controller_fetch_snapshot_v1_checked=true",
    "controller_api_versions_checked=true",
    "all_controller_api_versions_checked=true",
    "controller_unsupported_checked=true",
    "all_controller_unsupported_checked=true",
    "controller_unsupported_cases=[",
    "dynamic_raft_voter_negative_checked=true",
    "dynamic_raft_voter_follower_rejection_checked=true",
    "all_controller_describe_quorum_v2_checked=true",
    "broker_lifecycle_negative_checked=true",
    "broker_lifecycle_follower_rejection_checked=true",
    "controller_registration_negative_checked=true",
    "controller_registration_follower_rejection_checked=true",
    "broker_registration_follower_rejection_checked=true",
    "broker_non_broker_api_rejection_checked=true",
    "broker_non_broker_api_rejection_cases=[",
    "committed_offset=",
    "transactions_checked=5",
    "transaction_introspection_checked=true",
    "transaction_abort_checked=true",
    "txn_offset_commit_checked=true",
    "offset_fetch_v8_grouped_checked=true",
    "log_position_apis_checked=true",
    "delete_records_checked=true",
    "delete_topics_checked=true",
    "create_topics_checked=true",
    "create_partitions_checked=true",
    "client_quotas_checked=true",
    "scram_credentials_checked=true",
    "client_telemetry_checked=true",
    "delegation_tokens_checked=true",
    "finalized_features_checked=true",
    "acl_admin_checked=true",
    "config_admin_checked=true",
    "describe_topic_partitions_checked=true",
    "describe_configs_checked=true",
    "describe_log_dirs_checked=true",
    "alter_replica_log_dirs_checked=true",
    "assign_replicas_to_dirs_checked=true",
    "elect_leaders_checked=true",
    "describe_cluster_checked=true",
    "idempotent_producer_fencing=true",
    "describe_producers_checked=true",
    "delete_groups_checked=true",
    "classic_group_heartbeats=true",
    "group_describe_checked=true",
    "consumer_group_describe_checked=true",
    "list_groups_checked=true",
    "find_coordinator_checked=true",
    "share_group_heartbeat_checked=true",
    "share_group_describe_checked=true",
    "consumer_group_heartbeat_checked=true",
    "share_fetch_session_checked=true",
    "share_acknowledge_checked=true",
    "share_state_apis_checked=true",
    "kip848_describe_checked=true",
    "kip848_rejoin_checked=true",
    "kip848_rack_checked=true",
    "kip848_owned_assignment_checked=true",
    "kip848_subscription_update_checked=true",
    "kip848_negative_join_checked=true",
    "kip848_static_rejoin_checked=true",
    "offset_commit_v9_member_checked=true",
    "offset_fetch_v9_member_checked=true",
    "reassignment_topic=",
    "reassignment_target=",
    "reassignment_target_offset=",
    "reassignment_old_owner_rejected=true",
    "reassignment_target_fetch_verified=true",
)
KRAFT_CONTROLLER_UNSUPPORTED_REQUIRED_CASES = (
    "4:0",
    "4:7",
    "5:0",
    "5:4",
    "6:0",
    "6:8",
    "7:0",
    "7:3",
    "71:0",
    "72:0",
)
KRAFT_BROKER_NON_BROKER_REQUIRED_CASES = (
    "56:3",
    "58:0",
    "59:1",
    "62:4",
    "63:1",
    "64:0",
    "67:0",
    "70:0",
    "80:0",
    "81:0",
    "82:0",
)
KRAFT_FAILOVER_SUMMARY_FIELDS = tuple(
    marker.split("=", 1)[0]
    for marker in KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS
)
S3_PROCESS_CRASH_SUMMARY_FIELDS = (
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

def reject_nonstandard_json_constant(value):
    raise ValueError(f"non-standard JSON constant {value!r} is not allowed in strict JSON")


def reject_duplicate_json_object_keys(pairs):
    parsed = {}
    for key, value in pairs:
        if key in parsed:
            raise ValueError(f"duplicate JSON object key {key!r} is not allowed in strict JSON")
        parsed[key] = value
    return parsed


def load_release_evidence_manifest(path):
    if placeholder_env_value(path):
        raise ValueError("ZMQ_RELEASE_EVIDENCE must not use a placeholder path")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(
                f,
                parse_constant=reject_nonstandard_json_constant,
                object_pairs_hook=reject_duplicate_json_object_keys,
            )
    except OSError as exc:
        raise ValueError(f"could not read ZMQ_RELEASE_EVIDENCE {path!r}: {exc}") from exc
    except ValueError as exc:
        raise ValueError(f"invalid strict JSON in ZMQ_RELEASE_EVIDENCE {path!r}: {exc}") from exc


REQUIRED_COMMANDS = [
    {
        "label": "default Zig test suite",
        "required": [f"{RELEASE_ZIG} build test --summary all"],
    },
    {
        "label": "protocol static audit",
        "required": [f"{RELEASE_ZIG} build test-protocol-static-audit --summary all"],
        "output_markers": ["ok: protocol static audit"],
    },
    {
        "label": "observability static audit",
        "required": [
            f"{RELEASE_ZIG} build test-observability-static-audit --summary all"
        ],
        "output_markers": ["ok: observability static audit"],
    },
    {
        "label": "build static audit",
        "required": [f"{RELEASE_ZIG} build test-build-static-audit --summary all"],
        "output_markers": ["ok: build static audit"],
    },
    {
        "label": "root compose config validation",
        "required": [
            "docker compose -f docker-compose.yml config --quiet",
            "echo ok: root compose config",
        ],
        "output_markers": ["ok: root compose config"],
    },
    {
        "label": "Kafka benchmark compose config validation",
        "required": [
            "docker compose -f benchmarks/kafka-compose.yml config --quiet",
            "echo ok: kafka compose config",
        ],
        "output_markers": ["ok: kafka compose config"],
    },
    {
        "label": "AutoMQ benchmark compose config validation",
        "required": [
            "docker compose -f benchmarks/automq-compose.yml config --quiet",
            "echo ok: automq compose config",
        ],
        "output_markers": ["ok: automq compose config"],
    },
    {
        "label": "broker chaos harness",
        "required": [
            "ZMQ_RUN_CHAOS_TESTS=1",
            f"{RELEASE_ZIG} build test-chaos --summary all",
        ],
        "command_env_assignments": [
            "ZMQ_CHAOS_REQUIRED_SCENARIOS",
            "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
            "ZMQ_CHAOS_NETWORK_MATRIX",
        ],
        "output_markers": [
            "ok: chaos network-partition source=command",
            "ok: chaos harness passed for",
        ],
        "skip_markers": ["skip: set ZMQ_RUN_CHAOS_TESTS=1"],
    },
    {
        "label": "external client matrix",
        "required": [
            "ZMQ_RUN_CLIENT_MATRIX=1",
            f"{RELEASE_ZIG} build test-client-matrix --summary all",
        ],
        "command_env_assignments": [
            "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES",
            "ZMQ_CLIENT_MATRIX_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
            "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
            "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
        ],
        "output_markers": [
            "ok: client matrix profile",
            "ok: client matrix passed",
        ],
        "skip_markers": ["skip: set ZMQ_RUN_CLIENT_MATRIX=1"],
    },
    {
        "label": "MinIO/S3 integration gate",
        "required": [
            "ZMQ_RUN_MINIO_TESTS=1",
            "ZMQ_S3_REQUIRE_MULTIPART_EDGE=1",
            "ZMQ_S3_REQUIRE_LIST_PAGINATION=1",
            f"{RELEASE_ZIG} build test-minio --summary all",
        ],
        "output_markers": ["8/8 tests passed"],
        "skip_markers": ["skipped"],
    },
    {
        "label": "S3 process-crash replacement gate",
        "required": [
            "ZMQ_RUN_PROCESS_CRASH_TESTS=1",
            f"{RELEASE_ZIG} build test-s3-process-crash --summary all",
        ],
        "output_markers": ["ok: S3 process crash/replacement harness passed"],
        "skip_markers": ["skip: set ZMQ_RUN_PROCESS_CRASH_TESTS=1"],
    },
    {
        "label": "S3 provider matrix",
        "required": [
            "ZMQ_RUN_S3_PROVIDER_MATRIX=1",
            f"{RELEASE_ZIG} build test-s3-provider-matrix --summary all",
        ],
        "command_env_assignments": [
            "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
            "ZMQ_S3_PROVIDER_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
        ],
        "output_markers": [
            "ok: S3 provider live-suite profile",
            "ok: S3 provider profile",
            "ok: S3 provider matrix passed",
        ],
        "skip_markers": ["skip: set ZMQ_RUN_S3_PROVIDER_MATRIX=1"],
    },
    {
        "label": "KRaft failover gate",
        "required": [
            "ZMQ_RUN_KRAFT_FAILOVER_TESTS=1",
            f"{RELEASE_ZIG} build test-kraft-failover --summary all",
        ],
        "command_env_assignments": [
            "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
            "ZMQ_KRAFT_NETWORK_MATRIX",
        ],
        "output_markers": [
            "ok: KRaft controller failover harness passed",
        ]
        + list(KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS),
        "skip_markers": ["skip: set ZMQ_RUN_KRAFT_FAILOVER_TESTS=1"],
    },
    {
        "label": "Docker E2E gate",
        "required": [
            "ZMQ_RUN_E2E_TESTS=1",
            f"{RELEASE_ZIG} build test-e2e --summary all",
        ],
        "output_markers": [
            "3-Node E2E Test Suite",
            "[Test m] Cross-broker chaos phases",
            "[Test n] Live load/scale phases",
            "Results:",
        ],
        "skip_markers": ["skip: set ZMQ_RUN_E2E_TESTS=1"],
    },
    {
        "label": "local benchmark gate",
        "required": [f"{RELEASE_ZIG} build bench --summary all"],
        "forbidden": ["ZMQ_RUN_BENCH_LIVE_S3=1"],
        "output_markers": [
            "=== Benchmarks complete ===",
            "ok: local benchmark gate source=command",
            "S3 WAL request volume",
            "PartitionStore memory",
        ],
    },
    {
        "label": "live-S3 benchmark gate",
        "required": [
            "ZMQ_RUN_BENCH_LIVE_S3=1",
            f"{RELEASE_ZIG} build bench --summary all",
        ],
        "command_env_assignments": [
            "ZMQ_S3_ENDPOINT",
            "ZMQ_S3_PORT",
            "ZMQ_S3_BUCKET",
            "ZMQ_S3_SCHEME",
            "ZMQ_S3_REGION",
            "ZMQ_S3_PATH_STYLE",
        ],
        "output_markers": [
            "=== Benchmarks complete ===",
            "ok: live-S3 benchmark gate source=command",
            "Live S3 provider",
            "Live S3 put",
            "Live S3 get",
            "Live S3 request volume",
        ],
        "skip_markers": ["Live S3 provider benchmark skipped"],
    },
    {
        "label": "comparative benchmark gate",
        "required": [
            "ZMQ_RUN_BENCH_COMPARE=1",
            "ZMQ_BENCH_COMPARE_REQUIRE_TREND=1",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE",
            f"{RELEASE_ZIG} build bench-compare --summary all",
        ],
        "command_env_assignments": [
            "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE",
        ],
        "output_markers": [
            "COMPARISON:",
            "Benchmark",
            "ApiVersions",
            "Produce (reuse)",
            "Produce (fresh)",
            "Fetch",
            "Metadata",
            "COMPARATIVE BENCHMARK GATE",
            "thresholds:",
            "result: pass",
            "ok: comparative benchmark profile",
        ],
        "skip_markers": ["skip: set ZMQ_RUN_BENCH_COMPARE=1"],
    },
]

EXACT_ONCE_OUTPUT_MARKERS_BY_LABEL = {
    "protocol static audit": ("ok: protocol static audit",),
    "observability static audit": ("ok: observability static audit",),
    "build static audit": ("ok: build static audit",),
    "root compose config validation": ("ok: root compose config",),
    "Kafka benchmark compose config validation": ("ok: kafka compose config",),
    "AutoMQ benchmark compose config validation": ("ok: automq compose config",),
}

REQUIRED_ENV_VARS = [
    "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
    "ZMQ_CHAOS_REQUIRED_SCENARIOS",
    "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
    "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
    "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
    "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
    "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
    "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
    "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
    "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
    "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
    "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES",
    "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
    "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
    "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
    "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
    "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
    "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
    "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
    "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS",
    "ZMQ_BENCH_COMPARE_REQUIRE_TREND",
    "ZMQ_BENCH_COMPARE_TREND_BASELINE",
]

COMMA_SEPARATED_ENV_VARS = [
    name
    for name in REQUIRED_ENV_VARS
    if name
    not in ("ZMQ_BENCH_COMPARE_REQUIRE_TREND", "ZMQ_BENCH_COMPARE_TREND_BASELINE")
]

BENCHMARK_THRESHOLD_ENV_VARS = [
    "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO",
    "ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO",
    "ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO",
    "ZMQ_BENCH_COMPARE_MAX_ERROR_RATE",
    "ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO",
    "ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO",
    "ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO",
]

POSITIVE_INTEGER_ENV_VARS = {
    "ZMQ_BENCH_LIVE_S3_ITERATIONS",
    "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES",
}

E2E_LOAD_SCALE_FIXTURE_ACTIONS = {"scale-in", "scale-out", "load", "probe", "noop"}

DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS = {
    "min_throughput_ratio": 0.05,
    "max_p50_latency_ratio": 20.0,
    "max_p99_latency_ratio": 20.0,
    "max_error_rate": 0.0,
    "min_trend_throughput_ratio": 0.90,
    "max_trend_p50_latency_ratio": 1.25,
    "max_trend_p99_latency_ratio": 1.25,
}

COMPARATIVE_BENCHMARK_THRESHOLD_ENV = (
    ("ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO", "min_throughput_ratio"),
    ("ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO", "max_p50_latency_ratio"),
    ("ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO", "max_p99_latency_ratio"),
    ("ZMQ_BENCH_COMPARE_MAX_ERROR_RATE", "max_error_rate"),
    ("ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO", "min_trend_throughput_ratio"),
    ("ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO", "max_trend_p50_latency_ratio"),
    ("ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO", "max_trend_p99_latency_ratio"),
)

COVERAGE_SELECTOR_REQUIREMENTS = [
    {
        "selector": "ZMQ_CHAOS_NETWORK_MATRIX",
        "required": "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
        "label": "chaos network phases",
        "token_style": "collapsed",
    },
    {
        "selector": "ZMQ_KRAFT_NETWORK_MATRIX",
        "required": "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
        "label": "KRaft network phases",
        "token_style": "collapsed",
    },
    {
        "selector": "ZMQ_E2E_CHAOS_MATRIX",
        "required": "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
        "label": "E2E chaos phases",
        "token_style": "collapsed",
    },
    {
        "selector": "ZMQ_E2E_LOAD_SCALE_MATRIX",
        "required": "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
        "label": "E2E load/scale phases",
        "token_style": "collapsed",
        "fixture": "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
    },
    {
        "selector": "ZMQ_S3_PROVIDER_PROFILES",
        "required": "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
        "label": "S3 provider profiles",
        "token_style": "literal",
    },
    {
        "selector": "ZMQ_CLIENT_MATRIX_PROFILES",
        "required": "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES",
        "label": "client matrix profiles",
        "token_style": "literal",
    },
]

PHASE_HOOK_PROVENANCE_REQUIREMENTS = [
    {
        "required": "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
        "prefix": "ZMQ_CHAOS_NETWORK",
        "label": "chaos network phase",
        "suffixes": ("DOWN", "UP"),
        "token_style": "collapsed",
    },
    {
        "required": "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
        "prefix": "ZMQ_KRAFT_NETWORK",
        "label": "KRaft network phase",
        "suffixes": ("DOWN", "UP"),
        "token_style": "collapsed",
    },
    {
        "required": "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
        "prefix": "ZMQ_E2E_CHAOS",
        "label": "E2E chaos phase",
        "suffixes": ("DOWN", "UP"),
        "token_style": "collapsed",
    },
    {
        "required": "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
        "prefix": "ZMQ_E2E_LOAD_SCALE",
        "label": "E2E load/scale phase",
        "suffixes": ("APPLY", "RESTORE"),
        "token_style": "collapsed",
        "fixture": "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
    },
]

PROFILE_HOOK_PROVENANCE_REQUIREMENTS = [
    {
        "required": "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
        "prefix": "ZMQ_S3",
        "label": "S3 outage profile",
        "suffixes": ("OUTAGE_DOWN", "OUTAGE_UP"),
        "token_style": "literal",
    },
    {
        "required": "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
        "prefix": "ZMQ_S3",
        "label": "S3 multipart-fault profile",
        "suffixes": ("MULTIPART_FAULT_CMD",),
        "token_style": "literal",
    },
]

S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS = [
    (
        "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
        "RUN_LIVE_OUTAGE",
        "S3 outage profile",
    ),
    (
        "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
        "RUN_PROCESS_CRASH",
        "S3 process-crash profile",
    ),
    (
        "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
        "REQUIRE_LIST_PAGINATION",
        "S3 list-pagination profile",
    ),
    (
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
        "REQUIRE_MULTIPART_EDGE",
        "S3 multipart-edge profile",
    ),
    (
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
        "RUN_MULTIPART_FAULT",
        "S3 multipart-fault profile",
    ),
]

PLACEHOLDER_ENV_VALUES = {
    "...",
    "placeholder",
    "required",
    "tbd",
    "todo",
}
BOOL_TRUE_VALUES = {"1", "true", "yes", "on"}
BOOL_FALSE_VALUES = {"0", "false", "no", "off"}
BOOLEAN_ENV_VARS = {
    "ZMQ_BENCH_COMPARE_ENFORCE_GATES",
    "ZMQ_BENCH_COMPARE_REQUIRE_TREND",
    "ZMQ_RUN_BENCH_COMPARE",
    "ZMQ_RUN_BENCH_LIVE_S3",
    "ZMQ_RUN_CHAOS_TESTS",
    "ZMQ_RUN_CLIENT_MATRIX",
    "ZMQ_RUN_E2E_TESTS",
    "ZMQ_RUN_KRAFT_FAILOVER_TESTS",
    "ZMQ_RUN_MINIO_TESTS",
    "ZMQ_RUN_PROCESS_CRASH_TESTS",
    "ZMQ_RUN_S3_PROVIDER_MATRIX",
    "ZMQ_CLIENT_MATRIX_ENABLE_GO",
    "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
}
CLIENT_PROFILE_BOOL_SUFFIXES = ("ENABLE_GO",)
E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES = ("FIXTURE_DRY_RUN", "FIXTURE_PRESTOP")
S3_BOOL_SUFFIXES = (
    "PATH_STYLE",
    "SKIP_ENSURE_BUCKET",
    "SKIP_MINIO_HEALTH",
    "REQUIRE_LIST_PAGINATION",
    "REQUIRE_MULTIPART_EDGE",
    "RUN_LIVE_OUTAGE",
    "RUN_MULTIPART_FAULT",
    "RUN_PROCESS_CRASH",
)
S3_STRING_SUFFIXES = (
    "ENDPOINT",
    "BUCKET",
    "ACCESS_KEY",
    "SECRET_KEY",
    "REGION",
    "SCHEME",
    "TLS_CA_FILE",
)

BLOCKING_FLAGS = [
    "known_data_loss_bug",
    "advertised_stub_api",
    "untriaged_durability_failure",
]

RELEASE_EVIDENCE_FIELDS = (
    "commit",
    "environment",
    "commands",
    "unsupported_or_partial_surfaces",
    "known_data_loss_bug",
    "advertised_stub_api",
    "untriaged_durability_failure",
    "automq_complete",
)

COMMAND_ENTRY_FIELDS = (
    "command",
    "exit_code",
    "output",
)

ENV_ASSIGNMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SHELL_COMMAND_SEPARATORS = {"&&", "||", ";"}
SUCCESS_SHELL_COMMAND_SEPARATOR = "&&"
DISALLOWED_SHELL_OPERATOR_TOKENS = {
    "&",
    "&>",
    "&>>",
    "|",
    "|&",
    ">",
    ">>",
    "<",
    "<<",
    "<<<",
    "<>",
    "<&",
    ">&",
    ">|",
    "(",
    ")",
    "{",
    "}",
}
DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS = ("$(", "`")
DISALLOWED_COMMAND_LINE_BREAKS = ("\n", "\r")
DISALLOWED_COMMAND_QUOTE_CHARS = ("'", '"')
DISALLOWED_COMMAND_ESCAPE_CHARS = ("\\",)
ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS = (
    "echo ok: root compose config",
    "echo ok: kafka compose config",
    "echo ok: automq compose config",
)
ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS = (
    (
        "docker compose -f docker-compose.yml config --quiet",
        "echo ok: root compose config",
    ),
    (
        "docker compose -f benchmarks/kafka-compose.yml config --quiet",
        "echo ok: kafka compose config",
    ),
    (
        "docker compose -f benchmarks/automq-compose.yml config --quiet",
        "echo ok: automq compose config",
    ),
)
FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS = (
    "Build Summary:",
    " tests passed",
    "test success",
    "bench success",
    "bench-compare success",
    "ok:",
    "COMPARISON:",
    "COMPARATIVE BENCHMARK GATE",
    "thresholds:",
    "trend thresholds:",
    "trend baseline:",
    "result: pass",
    "8/8 tests passed",
    "3-Node E2E Test Suite",
    "Results:",
    "S3 WAL request volume",
    "Live S3 provider",
    "Live S3 request volume",
    *KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS,
)

CHAOS_SCENARIO_ALIASES = {
    "sigkill": "sigkill-restart",
    "partial-client": "slow-partial-client",
    "clock-skew": "clock-skewed-records",
    "s3": "s3-outage",
    "network": "network-partition",
    "live-s3": "live-s3-outage",
    "s3-live": "live-s3-outage",
}

REQUIRED_CHAOS_SCENARIOS = [
    "sigkill-restart",
    "slow-partial-client",
    "clock-skewed-records",
    "s3-outage",
    "network-partition",
]

CHAOS_SCENARIO_MARKERS = {
    "sigkill-restart": (
        "ok: chaos sigkill-restart killed=true restarted=true "
        "recovered_payloads=2 first_offset=0"
    ),
    "slow-partial-client": (
        "ok: chaos slow-partial-client "
        "partial_frame=true truncated_frame=true survived=true"
    ),
    "clock-skewed-records": (
        "ok: chaos clock-skewed-records "
        "future_timestamp=true fetched=true serving=true"
    ),
    "s3-outage": "ok: chaos s3-outage",
    "network-partition": "ok: chaos network-partition source=command",
    "live-s3-outage": (
        "ok: chaos live-s3-outage "
        "down=true healed=true fail_closed=true recovered=true source=command"
    ),
}

REQUIRED_CLIENT_TOOLS = [
    "kcat",
    "kafka-cli",
    "kafka-python",
    "confluent-kafka",
    "java-kafka",
    "go-kafka",
]

REQUIRED_CLIENT_SEMANTICS = [
    "basic",
    "admin",
    "groups",
    "rebalance",
    "transactions",
    "security",
    "security-negative",
]

CLIENT_SECURITY_PROTOCOLS = {
    "PLAINTEXT",
    "SASL_PLAINTEXT",
    "SSL",
    "SASL_SSL",
}

CLIENT_SASL_MECHANISMS = {
    "PLAIN",
    "SCRAM-SHA-256",
    "OAUTHBEARER",
}

CLIENT_SECURITY_TOOLS = {
    "kcat",
    "kafka-cli",
    "kafka-python",
    "confluent-kafka",
    "java-kafka",
}

CLIENT_REBALANCE_TOOLS = {
    "kafka-python",
    "confluent-kafka",
    "java-kafka",
}

CLIENT_TRANSACTION_TOOLS = {
    "confluent-kafka",
    "java-kafka",
}

CLIENT_PYTHON_TOOLS = {
    "kafka-python",
    "confluent-kafka",
}

CLIENT_UNPINNED_VERSION_LABELS = {
    "auto",
    "default",
    "latest",
}

CLIENT_TOOL_OUTPUT_MARKERS = {
    "kcat": "ok: kcat probes",
    "kafka-cli": "ok: kafka CLI probes",
    "kafka-python": "ok: kafka-python probes",
    "confluent-kafka": "ok: confluent-kafka probes",
    "java-kafka": "ok: java-kafka probes",
    "go-kafka": "ok: go-kafka probes",
}

COMPARATIVE_TARGET_LABELS = {
    "zmq": "ZMQ (Zig)",
    "kafka": "Apache Kafka",
    "automq": "AutoMQ (Java)",
}

COMPARATIVE_TABLE_TARGET_HEADERS = {
    "zmq": "ZMQ",
    "kafka": "Kafka",
    "automq": "AutoMQ",
}

COMPARATIVE_TABLE_ROW_MARKERS = (
    "ApiVersions",
    "Produce (reuse)",
    "Produce (fresh)",
    "Fetch",
    "Metadata",
)

COMPARATIVE_BENCHMARK_PROFILE_ITERATIONS = {
    "api_versions": 5000,
    "produce_single": 5000,
    "produce_fresh": 2000,
    "fetch": 3000,
    "metadata": 3000,
}
COMPARATIVE_BENCHMARK_PROFILE_WARMUP = {
    "api_versions": 100,
    "produce_single": 100,
    "produce_fresh": 50,
    "fetch": 100,
    "metadata": 100,
}
COMPARATIVE_PROFILE_MARKER_PREFIX = "ok: comparative benchmark profile "
COMPARATIVE_PROFILE_MARKER_STEM = COMPARATIVE_PROFILE_MARKER_PREFIX.strip()
COMPARATIVE_PROFILE_MARKER_KEYS = (
    "selected",
    "required",
    "results_targets",
    "results",
    "gates_enforced",
    "trend_required",
    "trend_baseline",
    "iterations",
    "warmup",
    "source",
)

COMPARATIVE_TABLE_METRICS = ("tput", "p50", "p99")
COMPARATIVE_MEASUREMENT_RE = {
    "tput": re.compile(r"(?<![\w.,+-])([0-9][0-9,]*(?:\.[0-9]+)?)/s\b"),
    "p50": re.compile(r"(?<![\w.,+-])([0-9][0-9,]*(?:\.[0-9]+)?)ms\b"),
    "p99": re.compile(r"(?<![\w.,+-])([0-9][0-9,]*(?:\.[0-9]+)?)ms\b"),
}
COMPARATIVE_RATIO_RE = re.compile(
    r"(?<![\w.,+-])([0-9][0-9,]*(?:\.[0-9]+)?)x\b"
)
COMPARATIVE_RATIO_MARKERS = {"\u25b2", "\u25bc"}

BENCHMARK_OUTPUT_LINE_MARKERS = {
    "=== Benchmarks complete ===",
    "S3 WAL request volume",
    "PartitionStore memory",
    "Live S3 provider",
    "Live S3 put",
    "Live S3 get",
    "Live S3 request volume",
}

KRAFT_DETAIL_OUTPUT_MARKERS = set(KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS)

E2E_OUTPUT_LINE_MARKERS = {
    "3-Node E2E Test Suite",
    "[Test m] Cross-broker chaos phases",
    "[Test n] Live load/scale phases",
    "Results:",
}

E2E_EXACT_ONCE_OUTPUT_LINE_MARKERS = (
    "3-Node E2E Test Suite",
    "[Test m] Cross-broker chaos phases",
    "[Test n] Live load/scale phases",
    "Results:",
)

REQUIRED_UNSUPPORTED_SURFACES = [
    {
        "label": "ZooKeeper-era inter-broker API keys 4-7",
        "surface_fragments": [
            "ZooKeeper-era inter-broker API keys 4-7",
        ],
        "fragments": [
            "ZooKeeper-era inter-broker API keys 4-7",
            "broker and controller ApiVersions",
            "neither port",
            "generated-only",
            "direct broker/controller probes",
            "fail closed",
        ],
        "status_label": "generated-only, not-advertised, or fail-closed",
        "status_markers": [
            "generated-only",
            "generated only",
            "not advertised",
            "fail closed",
            "fail-closed",
            "unsupported",
        ],
    },
    {
        "label": "broker-only stateless replacement",
        "surface_fragments": [
            "broker-only stateless replacement",
        ],
        "fragments": [
            "broker-only stateless replacement",
            "local cache/state assumptions",
            "S3/quorum replay paths",
        ],
        "status_label": "partial or blocked",
        "status_markers": [
            "partial",
            "blocked",
            "blocker",
        ],
    },
    {
        "label": "external client/security/OAuth live matrix",
        "surface_fragments": [
            "external-client",
            "secured-client",
            "OAuth profile execution",
        ],
        "fragments": [
            "external-client",
            "secured-client",
            "OAuth profile execution",
        ],
        "status_label": "release-CI-required or blocked",
        "status_markers": [
            "release-ci-required",
            "release ci required",
            "ci required",
            "blocked",
            "blocker",
            "must run",
        ],
    },
    {
        "label": "cross-broker chaos live matrix",
        "surface_fragments": [
            "cross-broker chaos",
            "multi-broker chaos",
        ],
        "fragments": [
            "scheduled cross-broker chaos",
            "multi-broker chaos",
        ],
        "status_label": "release-CI-required or blocked",
        "status_markers": [
            "release-ci-required",
            "release ci required",
            "ci required",
            "blocked",
            "blocker",
            "must run",
        ],
    },
    {
        "label": "Docker E2E load/scale live orchestration",
        "surface_fragments": [
            "Docker E2E load/scale",
            "live orchestration",
        ],
        "fragments": [
            "E2E load/scale",
            "live orchestration",
        ],
        "status_label": "release-CI-required or blocked",
        "status_markers": [
            "release-ci-required",
            "release ci required",
            "ci required",
            "blocked",
            "blocker",
            "must run",
        ],
    },
    {
        "label": "KRaft failover network matrix",
        "surface_fragments": [
            "KRaft failover",
            "network",
        ],
        "fragments": [
            "KRaft failover",
            "network matrices",
        ],
        "status_label": "release-CI-required or blocked",
        "status_markers": [
            "release-ci-required",
            "release ci required",
            "ci required",
            "blocked",
            "blocker",
            "must run",
        ],
    },
    {
        "label": "live S3 provider outage and multipart-fault profile execution",
        "surface_fragments": [
            "live",
            "provider outage",
            "multipart-fault profile execution",
        ],
        "fragments": [
            "live provider outage",
            "multipart-fault profile execution",
        ],
        "status_label": "release-CI-required or blocked",
        "status_markers": [
            "release-ci-required",
            "release ci required",
            "ci required",
            "blocked",
            "blocker",
            "must run",
        ],
    },
    {
        "label": "comparative Kafka/AutoMQ performance profile/trend gates",
        "surface_fragments": [
            "comparative Kafka/AutoMQ performance",
            "profile/trend gates",
        ],
        "fragments": [
            "comparative Kafka/AutoMQ performance",
            "profile/trend gates",
        ],
        "status_label": "release-CI-required or blocked",
        "status_markers": [
            "release-ci-required",
            "release ci required",
            "ci required",
            "blocked",
            "blocker",
            "must run",
        ],
    },
]

REQUIRED_UNSUPPORTED_SURFACE_FIELDS = ("surface", "status", "evidence")
OPTIONAL_UNSUPPORTED_SURFACE_FIELDS = ("id", "mitigation", "notes")
UNSUPPORTED_SURFACE_FIELDS = (
    *REQUIRED_UNSUPPORTED_SURFACE_FIELDS,
    *OPTIONAL_UNSUPPORTED_SURFACE_FIELDS,
)
UNSUPPORTED_SURFACE_TEXT_FIELDS = (
    "id",
    "surface",
    "status",
    "evidence",
    "mitigation",
    "notes",
)

UNSUPPORTED_SURFACE_STATUS_MARKERS = (
    "unsupported",
    "not advertised",
    "fail closed",
    "fail-closed",
    "generated-only",
    "partial",
    "blocked",
    "blocker",
    "release-ci-required",
    "release ci required",
    "ci required",
    "must run",
)


def validate_object_fields(obj, allowed_fields, label):
    failures = []
    allowed = set(allowed_fields)
    for field in obj:
        if not isinstance(field, str):
            failures.append(f"{label} field name {field!r} must be a string")
        elif field not in allowed:
            failures.append(f"{label} contains unexpected field {field!r}")
    return failures


def manifest_bool_value(manifest, name, failures, default=False):
    if name not in manifest:
        return default
    value = manifest.get(name)
    if isinstance(value, bool):
        return value
    failures.append(f"release evidence manifest field {name} must be a JSON boolean")
    return default


def shell_tokens(command):
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    return list(lexer)


def is_env_assignment_token(token):
    return ENV_ASSIGNMENT_RE.fullmatch(token) is not None


def is_env_name_token(token):
    return ENV_NAME_RE.fullmatch(token) is not None


def split_command_segments_with_separators(tokens):
    segments = []
    separators = []
    current = []
    for token in tokens:
        if token in SHELL_COMMAND_SEPARATORS:
            if not current:
                raise ValueError("empty shell command segment")
            if current:
                segments.append(current)
            current = []
            separators.append(token)
            continue
        current.append(token)
    if separators and not current:
        raise ValueError("trailing shell command separator")
    if current:
        segments.append(current)
    if len(separators) != max(0, len(segments) - 1):
        raise ValueError("malformed shell command separators")
    return segments, separators


def split_command_segments(tokens):
    segments, _separators = split_command_segments_with_separators(tokens)
    return segments


def validate_shell_command_separators(command, index):
    try:
        _segments, separators = split_command_segments_with_separators(
            shell_tokens(command)
        )
    except ValueError as exc:
        return [f"command entry {index} has invalid shell command syntax: {exc}"]

    failures = []
    for separator in separators:
        if separator != SUCCESS_SHELL_COMMAND_SEPARATOR:
            failures.append(
                f"command entry {index} uses non-success shell separator "
                f"{separator!r}; release gate command chains must use &&"
            )
    return failures


def validate_shell_command_single_line(command, index):
    for line_break in DISALLOWED_COMMAND_LINE_BREAKS:
        if line_break in command:
            return [
                f"command entry {index} contains a line break; release gate "
                "command strings must be single-line direct invocations"
            ]
    return []


def validate_shell_command_unquoted(command, index):
    for quote in DISALLOWED_COMMAND_QUOTE_CHARS:
        if quote in command:
            return [
                f"command entry {index} uses shell quote character {quote!r}; "
                "release gate command strings must be unquoted direct invocations"
            ]
    return []


def validate_shell_command_unescaped(command, index):
    for escape in DISALLOWED_COMMAND_ESCAPE_CHARS:
        if escape in command:
            return [
                f"command entry {index} uses shell escape character {escape!r}; "
                "release gate command strings must not use backslash escapes"
            ]
    return []


def command_segment_invocations(segments):
    return [segment_invocation(segment) for segment in segments]


def allowed_multi_segment_command_chain(segments):
    invocations = command_segment_invocations(segments)
    for chain in ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS:
        try:
            expected = [shell_tokens(fragment) for fragment in chain]
        except ValueError:
            continue
        if invocations == expected:
            return True
    return False


def validate_shell_command_segment_shape(command, index):
    try:
        segments = split_command_segments(shell_tokens(command))
    except ValueError:
        return []

    if len(segments) > 1 and not allowed_multi_segment_command_chain(segments):
        return [
            f"command entry {index} uses unexpected extra shell command segments; "
            "only documented compose config commands may use multi-segment "
            "release gate chains"
        ]
    return []


def validate_disallowed_shell_operators(command, index):
    failures = []
    for fragment in DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS:
        if fragment in command:
            failures.append(
                f"command entry {index} uses command substitution {fragment!r}; "
                "release gate commands must be direct invocations"
            )

    try:
        tokens = shell_tokens(command)
    except ValueError as exc:
        return failures + [
            f"command entry {index} has invalid shell command syntax: {exc}"
        ]

    for token in tokens:
        if token in DISALLOWED_SHELL_OPERATOR_TOKENS:
            failures.append(
                f"command entry {index} uses disallowed shell operator {token!r}; "
                "release gate commands must not pipe, redirect, background, "
                "or wrap gate execution"
            )
            break
    return failures


def validate_duplicate_command_env_assignments(command, index):
    try:
        segments = split_command_segments(shell_tokens(command))
    except ValueError:
        return []

    failures = []
    for segment_index, segment in enumerate(segments):
        names = []
        for assignment in segment_env_assignments(segment):
            name, _value = assignment.split("=", 1)
            names.append(name)
        duplicates = sorted(
            name
            for name in set(names)
            if names.count(name) > 1
        )
        if duplicates:
            failures.append(
                f"command entry {index} repeats environment assignment(s) "
                f"in command segment {segment_index}: "
                + ", ".join(duplicates)
            )
    return failures


def validate_command_does_not_embed_output_markers(command, index):
    normalized = command
    for allowed in ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS:
        normalized = normalized.replace(allowed, "")
    lowered = normalized.lower()
    for marker in FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS:
        if marker.lower() in lowered:
            return [
                f"command entry {index} embeds output marker text {marker!r}; "
                "release output markers must come from captured command output"
            ]
    return []


def segment_env_assignments(segment):
    assignments = []
    for token in segment:
        if not is_env_assignment_token(token):
            break
        assignments.append(token)
    return assignments


def segment_invocation(segment):
    env_count = len(segment_env_assignments(segment))
    return segment[env_count:]


def token_sequence_present(tokens, expected):
    if not expected or len(expected) > len(tokens):
        return False
    limit = len(tokens) - len(expected) + 1
    for start in range(limit):
        if tokens[start : start + len(expected)] == expected:
            return True
    return False


def command_has_forbidden(tokens, forbidden):
    for fragment in forbidden or []:
        try:
            forbidden_tokens = shell_tokens(fragment)
        except ValueError:
            return True
        if token_sequence_present(tokens, forbidden_tokens):
            return True
    return False


def classify_required_fragments(required):
    env_assignments = []
    env_names = []
    invocations = []
    for fragment in required:
        tokens = shell_tokens(fragment)
        if len(tokens) == 1 and is_env_assignment_token(tokens[0]):
            env_assignments.append(tokens[0])
        elif len(tokens) == 1 and is_env_name_token(tokens[0]):
            env_names.append(tokens[0])
        else:
            invocations.append(tokens)
    return env_assignments, env_names, invocations


def segment_has_required_env(segment, env_assignments):
    assignments = segment_env_assignments(segment)
    last_by_name = {}
    for assignment in assignments:
        name, _value = assignment.split("=", 1)
        last_by_name[name] = assignment
    for required in env_assignments:
        name, _value = required.split("=", 1)
        if last_by_name.get(name) != required:
            return False
    return True


def match_invocation_indexes(segments, invocations):
    match_indexes = []
    search_from = 0
    for invocation in invocations:
        matched_index = None
        for index in range(search_from, len(segments)):
            if segment_invocation(segments[index]) == invocation:
                matched_index = index
                break
        if matched_index is None:
            return None
        match_indexes.append(matched_index)
        search_from = matched_index + 1
    return match_indexes


def command_matches(command, required, forbidden=None):
    try:
        tokens = shell_tokens(command)
        env_assignments, _env_names, invocations = classify_required_fragments(required)
        segments = split_command_segments(tokens)
    except ValueError:
        return False

    if command_has_forbidden(tokens, forbidden):
        return False

    if not invocations:
        return any(
            segment_has_required_env(segment, env_assignments)
            for segment in segments
        )

    if len(segments) != len(invocations):
        return False

    match_indexes = match_invocation_indexes(segments, invocations)
    if match_indexes is None:
        return False

    if env_assignments:
        return segment_has_required_env(
            segments[match_indexes[-1]],
            env_assignments,
        )

    return True


def split_csv(raw):
    if not isinstance(raw, str) or not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def csv_value_failures(name, raw, context):
    if not isinstance(raw, str):
        return [f"{context} {name} must be a string"]

    stripped = raw.strip()
    if not stripped:
        return [f"{context} {name} must not be blank"]

    failures = []
    if any(not item.strip() for item in raw.split(",")):
        failures.append(
            f"{context} {name} must not contain blank comma-separated values"
        )

    values = split_csv(raw)
    if not values:
        failures.append(
            f"{context} {name} must contain at least one comma-separated value"
        )

    placeholders = [item for item in values if placeholder_env_value(item)]
    if placeholders:
        failures.append(
            f"{context} {name} uses placeholder values: "
            + ", ".join(placeholders)
        )
    duplicates = sorted(
        value
        for value in set(values)
        if values.count(value) > 1
    )
    if duplicates:
        failures.append(
            f"{context} {name} must not contain duplicate "
            "comma-separated values: " + ", ".join(duplicates)
        )
    return failures


def output_csv_values(raw, label):
    values = split_csv(raw)
    failures = []
    if any(not item.strip() for item in raw.split(",")):
        failures.append(
            f"release evidence {label} must not contain blank comma-separated values"
        )
    if not values:
        failures.append(f"release evidence {label} must list at least one value")
    duplicates = sorted(
        value
        for value in set(values)
        if values.count(value) > 1
    )
    if duplicates:
        failures.append(
            f"release evidence {label} must not contain duplicate "
            "comma-separated values: " + ", ".join(duplicates)
        )
    placeholders = [item for item in values if placeholder_env_value(item)]
    if placeholders:
        failures.append(
            f"release evidence {label} uses placeholder values: "
            + ", ".join(placeholders)
        )
    return values, failures


def canonical_chaos_scenario(name):
    return CHAOS_SCENARIO_ALIASES.get(name, name)


def placeholder_env_value(value):
    stripped = str(value).strip()
    lowered = stripped.lower()
    angle_start = stripped.find("<")
    has_angle_placeholder = (
        angle_start >= 0
        and stripped.find(">", angle_start + 1) > angle_start + 1
    )
    return (
        lowered in PLACEHOLDER_ENV_VALUES
        or lowered.startswith("/path/to/")
        or has_angle_placeholder
    )


def bool_environment_names(environment):
    names = {name for name in BOOLEAN_ENV_VARS if name in environment}
    for name in environment:
        if name.startswith("ZMQ_CLIENT_MATRIX_") and any(
            name.endswith(f"_{suffix}") for suffix in CLIENT_PROFILE_BOOL_SUFFIXES
        ):
            names.add(name)
        if name.startswith("ZMQ_E2E_LOAD_SCALE_") and any(
            name.endswith(f"_{suffix}") for suffix in E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES
        ):
            names.add(name)
        if name.startswith("ZMQ_S3_") and any(
            name == f"ZMQ_S3_{suffix}" or name.endswith(f"_{suffix}")
            for suffix in S3_BOOL_SUFFIXES
        ):
            names.add(name)
    return sorted(names)


def bool_environment_value(environment, name, failures=None, default=False):
    value = environment.get(name)
    if value is None:
        return default
    if not isinstance(value, str):
        if failures is not None:
            failures.append(
                f"release evidence environment variable {name} must be a boolean string"
            )
        return None
    stripped = value.strip()
    if not stripped:
        if failures is not None:
            failures.append(
                f"release evidence environment variable {name} must not be blank"
            )
        return None
    if placeholder_env_value(stripped):
        if failures is not None:
            failures.append(
                f"release evidence environment variable {name} uses placeholder value"
            )
        return None
    lowered = stripped.lower()
    if lowered in BOOL_TRUE_VALUES:
        return True
    if lowered in BOOL_FALSE_VALUES:
        return False
    if failures is not None:
        failures.append(
            f"release evidence environment variable {name} must be true or false"
        )
    return None


def validate_boolean_environment(environment):
    failures = []
    for name in bool_environment_names(environment):
        bool_environment_value(environment, name, failures)
    return failures


def s3_string_environment_names(environment):
    names = set()
    for name in environment:
        if not name.startswith("ZMQ_S3_"):
            continue
        if any(
            name == f"ZMQ_S3_{suffix}" or name.endswith(f"_{suffix}")
            for suffix in S3_STRING_SUFFIXES
        ):
            names.add(name)
    return sorted(names)


def validate_s3_string_environment(environment):
    failures = []
    for name in s3_string_environment_names(environment):
        value = environment.get(name)
        if not isinstance(value, str):
            failures.append(
                f"release evidence environment variable {name} must be a string"
            )
            continue
        stripped = value.strip()
        if not stripped:
            failures.append(
                f"release evidence environment variable {name} must not be blank"
            )
            continue
        if placeholder_env_value(stripped):
            failures.append(
                f"release evidence environment variable {name} uses placeholder value"
            )
            continue
        if name == "ZMQ_S3_SCHEME" or name.endswith("_SCHEME"):
            if stripped not in ("http", "https"):
                failures.append(
                    f"release evidence environment variable {name} must be http or https"
                )
    return failures


def validate_environment_names_and_values(environment):
    failures = []
    for name, value in environment.items():
        if not isinstance(name, str) or ENV_NAME_RE.fullmatch(name) is None:
            failures.append(
                f"release evidence environment variable name {name!r} "
                "must be a valid shell variable name"
            )
            continue
        if not isinstance(value, str):
            failures.append(
                f"release evidence environment variable {name} must be a string"
            )
            continue
        stripped = value.strip()
        if not stripped:
            failures.append(
                f"release evidence environment variable {name} must not be blank"
            )
            continue
        if placeholder_env_value(stripped):
            failures.append(
                f"release evidence environment variable {name} uses placeholder value"
            )
    return failures


def coverage_env_token(value, style):
    raw = "".join(ch.upper() if ch.isalnum() else "_" for ch in value)
    if style == "collapsed":
        return "_".join(part for part in raw.split("_") if part)
    return raw


def validate_coverage_token_collisions(values, label, style):
    failures = []
    by_token = {}
    for value in values:
        token = coverage_env_token(value, style)
        if not token:
            failures.append(
                f"release evidence {label} value {value!r} normalizes to "
                "an empty environment-variable token"
            )
            continue
        previous = by_token.get(token)
        if previous is not None and previous != value:
            failures.append(
                f"release evidence {label} values {previous!r} and {value!r} "
                f"normalize to the same environment-variable token {token}"
            )
        by_token[token] = value
    return failures


def validate_coverage_selector_provenance(environment):
    failures = []
    for requirement in COVERAGE_SELECTOR_REQUIREMENTS:
        required_name = requirement["required"]
        selector_name = requirement["selector"]
        label = requirement["label"]
        token_style = requirement["token_style"]
        required_values = split_csv(environment.get(required_name))
        if not required_values:
            continue

        failures.extend(
            validate_coverage_token_collisions(
                required_values,
                required_name,
                token_style,
            )
        )

        selector_raw = environment.get(selector_name)
        if selector_name in environment:
            failures.extend(
                csv_value_failures(
                    selector_name,
                    selector_raw,
                    "release evidence coverage selector",
                )
            )
        selector_values = split_csv(selector_raw) if isinstance(selector_raw, str) else []
        if not selector_values:
            fixture_name = requirement.get("fixture")
            if fixture_name and bool_environment_value(environment, fixture_name) is True:
                continue
            failures.append(
                f"release evidence missing coverage selector {selector_name} "
                f"for {label}"
            )
            continue

        placeholders = [value for value in selector_values if placeholder_env_value(value)]
        if placeholders:
            failures.append(
                f"release evidence coverage selector {selector_name} "
                "uses placeholder values: " + ", ".join(placeholders)
            )
        failures.extend(
            validate_coverage_token_collisions(
                selector_values,
                selector_name,
                token_style,
            )
        )

        selected = set(selector_values)
        missing = [value for value in required_values if value not in selected]
        if missing:
            failures.append(
                f"release evidence coverage selector {selector_name} must include "
                f"required values from {required_name}: " + ", ".join(missing)
            )
    return failures


def first_present_environment_value(environment, names):
    for name in names:
        if name in environment:
            return name, environment.get(name)
    return None, None


def validate_hook_command_value(env_name, value, label):
    failures = []
    if not isinstance(value, str):
        failures.append(
            f"release evidence hook command {env_name} for {label} must be a string"
        )
        return failures
    stripped = value.strip()
    if not stripped:
        failures.append(
            f"release evidence hook command {env_name} for {label} must not be blank"
        )
        return failures
    if placeholder_env_value(stripped):
        failures.append(
            f"release evidence hook command {env_name} for {label} uses placeholder value"
        )
        return failures
    try:
        words = shlex.split(stripped)
    except ValueError as exc:
        failures.append(
            f"release evidence hook command {env_name} for {label} is malformed: {exc}"
        )
        return failures
    if not words:
        failures.append(
            f"release evidence hook command {env_name} for {label} "
            "must contain at least one word"
        )
    return failures


def validate_hook_command_provenance(environment, names, label):
    env_name, value = first_present_environment_value(environment, names)
    if value is None:
        return [
            f"release evidence missing hook command for {label}: "
            + " or ".join(names)
        ]
    return validate_hook_command_value(env_name, value, label)


def profile_setting_environment_value(environment, prefix, profile, suffix):
    token = coverage_env_token(profile, "literal")
    return first_present_environment_value(
        environment,
        (
            f"{prefix}_{token}_{suffix}",
            f"{prefix}_{suffix}",
        ),
    )


def client_profile_setting_names(profile, suffix):
    if profile == "default":
        return (f"ZMQ_CLIENT_MATRIX_{suffix}",)
    token = coverage_env_token(profile, "literal")
    return (
        f"ZMQ_CLIENT_MATRIX_{token}_{suffix}",
        f"ZMQ_CLIENT_MATRIX_{suffix}",
    )


def client_profile_setting_environment_value(environment, profile, suffix):
    return first_present_environment_value(
        environment,
        client_profile_setting_names(profile, suffix),
    )


def require_client_profile_setting(environment, profile, suffix, label):
    names = client_profile_setting_names(profile, suffix)
    env_name, value = first_present_environment_value(environment, names)
    if value is None:
        return (
            None,
            None,
            [
                f"release evidence missing client profile setting {suffix} "
                f"for {label} {profile}: " + " or ".join(names)
            ],
        )

    if not isinstance(value, str):
        return (
            env_name,
            value,
            [
                f"release evidence client profile setting {env_name} "
                f"for {label} {profile} must be a string"
            ],
        )

    stripped = value.strip()
    if not stripped:
        return (
            env_name,
            value,
            [
                f"release evidence client profile setting {env_name} "
                f"for {label} {profile} must not be blank"
            ],
        )

    if placeholder_env_value(stripped):
        return (
            env_name,
            value,
            [
                f"release evidence client profile setting {env_name} "
                f"for {label} {profile} uses placeholder value"
            ],
        )

    return env_name, value, []


def validate_bootstrap_server_text(label, server):
    failures = []
    if not server:
        return [f"{label} bootstrap entries must not be blank"]
    if any(ch.isspace() for ch in server):
        failures.append(f"{label} bootstrap entry {server!r} must not contain whitespace")

    if server.startswith("["):
        end = server.find("]")
        if end <= 1 or end + 1 >= len(server) or server[end + 1] != ":":
            failures.append(f"{label} bootstrap entry {server!r} must be host:port")
            return failures
        host = server[1:end]
        port_text = server[end + 2:]
    else:
        if server.count(":") != 1:
            failures.append(f"{label} bootstrap entry {server!r} must be host:port")
            return failures
        host, port_text = server.rsplit(":", 1)

    if not host.strip():
        failures.append(f"{label} bootstrap entry {server!r} must include a host")
    if "/" in host:
        failures.append(
            f"{label} bootstrap entry {server!r} must not include a URL scheme or path"
        )
    if not port_text.strip():
        failures.append(f"{label} bootstrap entry {server!r} must include a port")
    elif not port_text.isdigit():
        failures.append(f"{label} bootstrap entry {server!r} port must be numeric")
    else:
        port = int(port_text)
        if port <= 0 or port > 65535:
            failures.append(
                f"{label} bootstrap entry {server!r} port must be between 1 and 65535"
            )
    return failures


def validate_bootstrap_servers_text(label, value):
    stripped = value.strip()
    if not stripped:
        return [f"{label} must not be blank"]
    if placeholder_env_value(stripped):
        return [f"{label} uses placeholder value"]
    servers = [server.strip() for server in stripped.split(",")]
    if not servers or any(not server for server in servers):
        return [f"{label} must contain comma-separated host:port bootstrap entries"]
    failures = []
    for server in servers:
        failures.extend(validate_bootstrap_server_text(label, server))
    return failures


def client_profile_setting_is_concrete(environment, profile, suffix):
    _env_name, value = client_profile_setting_environment_value(
        environment,
        profile,
        suffix,
    )
    return (
        isinstance(value, str)
        and bool(value.strip())
        and not placeholder_env_value(value.strip())
    )


def client_profile_expected_bootstrap(environment, profile, failures=None):
    env_name, value, setting_failures = require_client_profile_setting(
        environment,
        profile,
        "BOOTSTRAP",
        "client matrix profile",
    )
    if failures is not None:
        failures.extend(setting_failures)
    if setting_failures:
        return None
    bootstrap_failures = validate_bootstrap_servers_text(
        f"release evidence client profile setting {env_name} for client matrix profile {profile}",
        value,
    )
    if failures is not None:
        failures.extend(bootstrap_failures)
    if bootstrap_failures:
        return None
    return value.strip()


def parse_client_profile_tools(raw):
    tools = split_csv(raw)
    if not tools:
        return set(), ["must contain at least one comma-separated value"]
    duplicates = sorted(tool for tool in set(tools) if tools.count(tool) > 1)
    if duplicates:
        return set(tools), [
            "must not contain duplicate comma-separated values: "
            + ", ".join(duplicates)
        ]
    lowered = [tool.lower() for tool in tools]
    if lowered == ["auto"] or "auto" in lowered:
        return set(), ["must explicitly list selected tools, not auto"]
    unknown = sorted({tool for tool in tools if tool not in CLIENT_TOOL_OUTPUT_MARKERS})
    if unknown:
        return set(tools), ["has unknown tools: " + ", ".join(unknown)]
    return set(tools), []


def parse_client_profile_semantics(raw):
    items = split_csv(raw)
    if not items:
        return set(), ["must contain at least one comma-separated value"]

    semantics = set()
    tokens = []
    known = set(REQUIRED_CLIENT_SEMANTICS)
    for item in items:
        semantic = item.lower()
        if semantic in ("auto", "default"):
            return set(), ["must explicitly list semantic probes"]
        tokens.append(semantic)
        if semantic == "all":
            semantics.update(REQUIRED_CLIENT_SEMANTICS)
            continue
        if semantic not in known:
            return semantics, [f"has unknown semantic probe: {item}"]
        semantics.add(semantic)

    if not semantics:
        return set(), ["must contain at least one comma-separated value"]
    duplicates = sorted(token for token in set(tokens) if tokens.count(token) > 1)
    if duplicates:
        return semantics, [
            "must not contain duplicate comma-separated values: "
            + ", ".join(duplicates)
        ]
    semantics.add("basic")
    return semantics, []


def validate_client_profile_tools(environment, profile):
    env_name, value, failures = require_client_profile_setting(
        environment,
        profile,
        "TOOLS",
        "client matrix profile",
    )
    if failures:
        return set(), failures

    tools, parse_failures = parse_client_profile_tools(value)
    return tools, [
        f"release evidence client profile setting {env_name} for {profile} {failure}"
        for failure in parse_failures
    ]


def validate_client_profile_semantics(environment, profile):
    env_name, value, failures = require_client_profile_setting(
        environment,
        profile,
        "SEMANTICS",
        "client matrix profile",
    )
    if failures:
        return set(), failures

    semantics, parse_failures = parse_client_profile_semantics(value)
    return semantics, [
        f"release evidence client profile setting {env_name} for {profile} {failure}"
        for failure in parse_failures
    ]


def validate_client_profile_tool_semantic_compatibility(profile, tools, semantics):
    failures = []
    for tool in sorted(tools):
        if "rebalance" in semantics and tool not in CLIENT_REBALANCE_TOOLS:
            failures.append(
                f"release evidence client profile {profile} selects {tool} "
                "with rebalance semantics, but that tool has no rebalance probe"
            )
        if "transactions" in semantics and tool not in CLIENT_TRANSACTION_TOOLS:
            failures.append(
                f"release evidence client profile {profile} selects {tool} "
                "with transactions semantics, but that tool has no "
                "transactional probe"
            )
        if (
            ("security" in semantics or "security-negative" in semantics)
            and tool not in CLIENT_SECURITY_TOOLS
        ):
            failures.append(
                f"release evidence client profile {profile} selects {tool} "
                "with security semantics, but that tool has no security "
                "interop probe"
            )
    return failures


def validate_client_profile_version(environment, profile):
    env_name, value, failures = require_client_profile_setting(
        environment,
        profile,
        "VERSION",
        "versioned client matrix profile",
    )
    if failures:
        return failures
    if value.strip().lower() in CLIENT_UNPINNED_VERSION_LABELS:
        return [
            f"release evidence client profile setting {env_name} for "
            f"versioned client matrix profile {profile} must pin an exact "
            "client/library version"
        ]
    return []


def client_profile_expected_version(environment, profile):
    if profile not in split_csv(
        environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES")
    ):
        return None
    _env_name, value, failures = require_client_profile_setting(
        environment,
        profile,
        "VERSION",
        "versioned client matrix profile",
    )
    if failures or value.strip().lower() in CLIENT_UNPINNED_VERSION_LABELS:
        return None
    return value.strip()


def validate_client_profile_tool_settings(environment, profile, tools):
    failures = []
    if CLIENT_PYTHON_TOOLS.intersection(tools):
        _env_name, _value, setting_failures = require_client_profile_setting(
            environment,
            profile,
            "PYTHON",
            "Python client matrix profile",
        )
        failures.extend(setting_failures)
    if "java-kafka" in tools:
        _env_name, _value, setting_failures = require_client_profile_setting(
            environment,
            profile,
            "JAVA_CLASSPATH",
            "java-kafka client matrix profile",
        )
        failures.extend(setting_failures)
    if "go-kafka" in tools:
        env_name, value, setting_failures = require_client_profile_setting(
            environment,
            profile,
            "GO_MODULE",
            "go-kafka client matrix profile",
        )
        failures.extend(setting_failures)
        if not setting_failures:
            module = value.strip()
            module_version = module.rsplit("@", 1)
            unpinned = (
                len(module_version) != 2
                or not module_version[1].strip()
                or module_version[1].strip().lower() == "latest"
            )
        if not setting_failures and unpinned:
            failures.append(
                f"release evidence client profile setting {env_name} for "
                f"go-kafka client matrix profile {profile} must pin an exact "
                "Go module version"
            )
    return failures


def validate_client_security_context(environment, profile, tools, semantics):
    failures = []
    if not tools:
        failures.append(
            f"release evidence secured-client profile {profile} must select "
            "one or more tools"
        )
    unsupported_tools = sorted(tool for tool in tools if tool not in CLIENT_SECURITY_TOOLS)
    if unsupported_tools:
        failures.append(
            f"release evidence secured-client profile {profile} uses tools "
            "without security interop coverage: " + ", ".join(unsupported_tools)
        )
    if "security" not in semantics and "security-negative" not in semantics:
        failures.append(
            f"release evidence secured-client profile {profile} must enable "
            "security or security-negative semantics"
        )

    env_name, protocol, setting_failures = require_client_profile_setting(
        environment,
        profile,
        "SECURITY_PROTOCOL",
        "secured-client profile",
    )
    failures.extend(setting_failures)
    context = {"protocol": None, "mechanism": None}
    if setting_failures:
        return context, failures

    protocol_upper = protocol.strip().upper()
    context["protocol"] = protocol_upper
    if protocol_upper not in CLIENT_SECURITY_PROTOCOLS:
        failures.append(
            f"release evidence client profile setting {env_name} for "
            f"secured-client profile {profile} has unknown security protocol "
            f"{protocol!r}"
        )
    elif protocol_upper == "PLAINTEXT":
        failures.append(
            f"release evidence secured-client profile {profile} must use "
            "SASL_PLAINTEXT, SSL, or SASL_SSL rather than PLAINTEXT"
        )

    if protocol_upper in ("SASL_PLAINTEXT", "SASL_SSL"):
        mechanism_name, mechanism, mechanism_failures = require_client_profile_setting(
            environment,
            profile,
            "SASL_MECHANISM",
            "secured-client profile",
        )
        failures.extend(mechanism_failures)
        if not mechanism_failures:
            mechanism_upper = mechanism.strip().upper()
            context["mechanism"] = mechanism_upper
            if mechanism_upper not in CLIENT_SASL_MECHANISMS:
                failures.append(
                    f"release evidence client profile setting {mechanism_name} "
                    f"for secured-client profile {profile} has unknown SASL "
                    f"mechanism {mechanism!r}"
                )
            elif mechanism_upper != "OAUTHBEARER":
                for suffix in ("SASL_USERNAME", "SASL_PASSWORD"):
                    _name, _value, credential_failures = require_client_profile_setting(
                        environment,
                        profile,
                        suffix,
                        "secured-client profile",
                    )
                    failures.extend(credential_failures)

    if protocol_upper in ("SSL", "SASL_SSL"):
        _name, _value, tls_failures = require_client_profile_setting(
            environment,
            profile,
            "SSL_CA_LOCATION",
            "secured-client profile",
        )
        failures.extend(tls_failures)

    return context, failures


def oauth_positive_fixture_suffix(tool):
    if tool in ("kafka-python", "confluent-kafka"):
        return "OAUTH_TOKEN"
    if tool in ("kafka-cli", "java-kafka"):
        return "OAUTH_JAAS_CONFIG"
    if tool == "kcat":
        return "OAUTHBEARER_CONFIG"
    return None


def oauth_negative_fixture_suffix(tool):
    if tool in ("kafka-python", "confluent-kafka"):
        return "BAD_OAUTH_TOKEN"
    if tool in ("kafka-cli", "java-kafka"):
        return "BAD_OAUTH_JAAS_CONFIG"
    if tool == "kcat":
        return "BAD_OAUTHBEARER_CONFIG"
    return None


def decode_client_matrix_jwt_payload(token):
    parts = token.split(".")
    if len(parts) != 3 or not parts[1]:
        raise ValueError("OAuth token fixture is not a compact JWT")
    padded_payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        payload_bytes = base64.urlsafe_b64decode(padded_payload.encode("ascii"))
        payload = json.loads(
            payload_bytes.decode("utf-8"),
            parse_constant=reject_nonstandard_json_constant,
            object_pairs_hook=reject_duplicate_json_object_keys,
        )
    except Exception as exc:
        raise ValueError(f"OAuth token fixture has an invalid JWT payload: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("OAuth token fixture JWT payload must be a JSON object")
    return payload


def client_matrix_numeric_date_claim(payload, name):
    value = payload.get(name)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def raw_oauth_token_positive_configured(token):
    try:
        payload = decode_client_matrix_jwt_payload(token)
    except ValueError:
        return False
    subject = payload.get("sub")
    exp = client_matrix_numeric_date_claim(payload, "exp")
    nbf = client_matrix_numeric_date_claim(payload, "nbf")
    now = int(time.time())
    return (
        isinstance(subject, str)
        and bool(subject)
        and exp is not None
        and exp > now
        and (nbf is None or nbf <= now)
    )


def raw_oauth_token_negative_configured(token):
    try:
        payload = decode_client_matrix_jwt_payload(token)
    except ValueError:
        return True
    subject = payload.get("sub")
    exp = client_matrix_numeric_date_claim(payload, "exp")
    nbf = client_matrix_numeric_date_claim(payload, "nbf")
    now = int(time.time())
    return (
        not isinstance(subject, str)
        or not subject
        or exp is None
        or exp <= now
        or (nbf is not None and nbf > now)
    )


def parse_oauth_jaas_options(config):
    lexer = shlex.shlex(config or "", posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        tokens = list(lexer)
    except ValueError as exc:
        raise ValueError(f"OAuth JAAS fixture is malformed: {exc}") from exc

    cleaned = []
    saw_terminator = False
    for token in tokens:
        if token == ";":
            saw_terminator = True
            continue
        if saw_terminator:
            raise ValueError(
                "OAuth JAAS fixture has tokens after the terminating semicolon"
            )
        if token.endswith(";"):
            saw_terminator = True
            token = token[:-1]
        if token:
            cleaned.append(token)

    if not saw_terminator:
        raise ValueError("OAuth JAAS fixture is missing its terminating semicolon")

    module_index = None
    for index, token in enumerate(cleaned):
        if token.endswith("OAuthBearerLoginModule"):
            module_index = index
            break
    if module_index is None:
        raise ValueError("OAuth JAAS fixture is missing OAuthBearerLoginModule")
    if module_index + 1 >= len(cleaned) or cleaned[module_index + 1] != "required":
        raise ValueError("OAuth JAAS fixture must use a required OAuthBearerLoginModule")

    options = {}
    for token in cleaned[module_index + 2:]:
        if "=" not in token:
            raise ValueError(f"OAuth JAAS fixture has an invalid option token: {token!r}")
        key, value = token.split("=", 1)
        if not key:
            raise ValueError("OAuth JAAS fixture has an empty option key")
        options[key] = value
    return options


def jaas_numeric_claim(options, name):
    value = options.get(f"unsecuredLoginNumberClaim_{name}")
    if value is None:
        return None
    try:
        return int(value, 10)
    except ValueError:
        return None


def oauth_jaas_positive_configured(config):
    try:
        options = parse_oauth_jaas_options(config)
    except ValueError:
        return False
    subject = options.get("unsecuredLoginStringClaim_sub")
    exp = jaas_numeric_claim(options, "exp")
    nbf = jaas_numeric_claim(options, "nbf")
    now = int(time.time())
    return bool(subject) and exp is not None and exp > now and (nbf is None or nbf <= now)


def oauth_jaas_negative_configured(config):
    try:
        options = parse_oauth_jaas_options(config)
    except ValueError:
        return True
    subject = options.get("unsecuredLoginStringClaim_sub")
    exp = jaas_numeric_claim(options, "exp")
    nbf = jaas_numeric_claim(options, "nbf")
    now = int(time.time())
    return (
        not subject
        or exp is None
        or exp <= now
        or (nbf is not None and nbf > now)
    )


def parse_librdkafka_oauthbearer_config(config):
    if not config:
        raise ValueError("librdkafka OAUTHBEARER fixture must not be empty")

    parsed = {}
    index = 0
    length = len(config)
    standard_prefixes = (
        "principalClaimName=",
        "principal=",
        "scopeClaimName=",
        "scope=",
        "lifeSeconds=",
    )
    while index < length:
        if config[index] == " ":
            index += 1
            continue

        matched = False
        for prefix in standard_prefixes:
            if not config.startswith(prefix, index):
                continue
            matched = True
            key = prefix[:-1]
            if key in parsed:
                raise ValueError(f"librdkafka OAUTHBEARER fixture has duplicate {key}")
            value_start = index + len(prefix)
            value_end = config.find(" ", value_start)
            if value_end == -1:
                value_end = length
            value = config[value_start:value_end]
            if not value:
                raise ValueError(f"librdkafka OAUTHBEARER fixture has empty {key}")
            if '"' in value:
                raise ValueError(f"librdkafka OAUTHBEARER fixture has a quote in {key}")
            parsed[key] = value
            index = value_end
            break
        if matched:
            continue

        if config.startswith("extension_", index):
            key_start = index + len("extension_")
            key_end = config.find("=", key_start)
            if key_end == -1:
                raise ValueError(
                    "librdkafka OAUTHBEARER fixture has malformed extension"
                )
            key = config[key_start:key_end]
            value_start = key_end + 1
            value_end = config.find(" ", value_start)
            if value_end == -1:
                value_end = length
            value = config[value_start:value_end]
            if not key:
                raise ValueError(
                    "librdkafka OAUTHBEARER fixture has an empty extension key"
                )
            if key == "auth" or not key.isalpha():
                raise ValueError(
                    "librdkafka OAUTHBEARER fixture has an invalid extension key"
                )
            if any((ord(ch) < 0x21 or ord(ch) > 0x7e) for ch in value):
                raise ValueError(
                    "librdkafka OAUTHBEARER fixture has an invalid extension value"
                )
            index = value_end
            continue

        raise ValueError("librdkafka OAUTHBEARER fixture has an unrecognized token")

    principal = parsed.get("principal")
    if not principal:
        raise ValueError("librdkafka OAUTHBEARER fixture must include principal")
    principal_claim_name = parsed.get("principalClaimName", "sub")
    if principal_claim_name != "sub":
        raise ValueError(
            "librdkafka OAUTHBEARER fixture must emit the broker-supported sub claim"
        )
    life_seconds_text = parsed.get("lifeSeconds")
    if life_seconds_text is not None:
        try:
            life_seconds = int(life_seconds_text, 10)
        except ValueError as exc:
            raise ValueError(
                "librdkafka OAUTHBEARER fixture has non-integral lifeSeconds"
            ) from exc
        if life_seconds <= 0 or life_seconds > 2147483647:
            raise ValueError(
                "librdkafka OAUTHBEARER fixture has out-of-range lifeSeconds"
            )
    return parsed


def librdkafka_oauthbearer_positive_configured(config):
    try:
        parse_librdkafka_oauthbearer_config(config)
        return True
    except ValueError:
        return False


def librdkafka_oauthbearer_negative_configured(config):
    try:
        parse_librdkafka_oauthbearer_config(config)
    except ValueError:
        return True
    return False


def validate_client_oauth_fixture_value(env_name, value, positive, label, profile):
    suffix = env_name.split("_", 4)[-1]
    if suffix.endswith("OAUTH_TOKEN"):
        valid = (
            raw_oauth_token_positive_configured(value)
            if positive
            else raw_oauth_token_negative_configured(value)
        )
    elif suffix.endswith("OAUTH_JAAS_CONFIG"):
        valid = (
            oauth_jaas_positive_configured(value)
            if positive
            else oauth_jaas_negative_configured(value)
        )
    elif suffix.endswith("OAUTHBEARER_CONFIG"):
        valid = (
            librdkafka_oauthbearer_positive_configured(value)
            if positive
            else librdkafka_oauthbearer_negative_configured(value)
        )
    else:
        return []

    if valid:
        return []
    expected = "positive OAuth fixture" if positive else "negative OAuth fixture"
    return [
        f"release evidence client profile setting {env_name} for {label} "
        f"{profile} must be a {expected}"
    ]


def validate_client_oauth_positive_fixtures(environment, profile, tools, label):
    failures = []
    for tool in sorted(tools):
        suffix = oauth_positive_fixture_suffix(tool)
        if suffix is None:
            failures.append(
                f"release evidence {label} {profile} selects {tool}, which "
                "does not have OAuth fixture coverage"
            )
            continue
        env_name, value, setting_failures = require_client_profile_setting(
            environment,
            profile,
            suffix,
            label,
        )
        failures.extend(setting_failures)
        if not setting_failures:
            failures.extend(
                validate_client_oauth_fixture_value(
                    env_name,
                    value,
                    True,
                    label,
                    profile,
                )
            )
    return failures


def validate_client_oauth_negative_fixtures(environment, profile, tools, label):
    failures = []
    for tool in sorted(tools):
        suffix = oauth_negative_fixture_suffix(tool)
        if suffix is None:
            failures.append(
                f"release evidence {label} {profile} selects {tool}, which "
                "does not have OAuth-negative fixture coverage"
            )
            continue
        env_name, value, setting_failures = require_client_profile_setting(
            environment,
            profile,
            suffix,
            label,
        )
        failures.extend(setting_failures)
        if not setting_failures:
            failures.extend(
                validate_client_oauth_fixture_value(
                    env_name,
                    value,
                    False,
                    label,
                    profile,
                )
            )
    return failures


def client_profile_has_generic_negative_vector(environment, profile, context):
    protocol = context.get("protocol")
    mechanism = context.get("mechanism")
    if (
        protocol in ("SASL_PLAINTEXT", "SASL_SSL")
        and mechanism != "OAUTHBEARER"
        and client_profile_setting_is_concrete(environment, profile, "SASL_PASSWORD")
    ):
        return True
    if (
        protocol in ("SSL", "SASL_SSL")
        and client_profile_setting_is_concrete(environment, profile, "BAD_SSL_CA_LOCATION")
    ):
        return True
    return client_profile_setting_is_concrete(environment, profile, "ACL_DENIED_TOPIC")


def client_profile_has_oauth_negative_vector(environment, profile, tool):
    suffix = oauth_negative_fixture_suffix(tool)
    return bool(suffix and client_profile_setting_is_concrete(environment, profile, suffix))


def required_client_profile_names(environment):
    profiles = []
    for name in (
        "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
    ):
        for profile in split_csv(environment.get(name)):
            add_unique_marker(profiles, profile)
    return profiles


def validate_client_profile_provenance(environment):
    failures = []
    required_profiles = split_csv(
        environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES")
    )
    if not required_profiles:
        return failures

    profile_tools = {}
    profile_semantics = {}
    for profile in required_client_profile_names(environment):
        tools, tool_failures = validate_client_profile_tools(environment, profile)
        semantics, semantic_failures = validate_client_profile_semantics(
            environment,
            profile,
        )
        failures.extend(tool_failures)
        failures.extend(semantic_failures)
        failures.extend(
            validate_client_profile_tool_semantic_compatibility(
                profile,
                tools,
                semantics,
            )
        )
        client_profile_expected_bootstrap(environment, profile, failures)
        failures.extend(validate_client_profile_tool_settings(environment, profile, tools))
        profile_tools[profile] = tools
        profile_semantics[profile] = semantics

    required_tools = split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS"))
    covered_tools = set()
    for profile in required_profiles:
        covered_tools.update(profile_tools.get(profile, set()))
    missing_tools = [tool for tool in required_tools if tool not in covered_tools]
    if missing_tools:
        failures.append(
            "release evidence client profile settings must cover required "
            "tools from ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS: "
            + ", ".join(missing_tools)
        )

    required_semantics = set(
        split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS"))
    )
    covered_semantics = set()
    for profile in required_profiles:
        covered_semantics.update(profile_semantics.get(profile, set()))
    missing_semantics = [
        semantic
        for semantic in REQUIRED_CLIENT_SEMANTICS
        if semantic in required_semantics and semantic not in covered_semantics
    ]
    if missing_semantics:
        failures.append(
            "release evidence client profile settings must cover required "
            "semantics from ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS: "
            + ", ".join(missing_semantics)
        )

    for profile in split_csv(
        environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES")
    ):
        failures.extend(validate_client_profile_version(environment, profile))

    security_profiles = []
    for name in (
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
    ):
        for profile in split_csv(environment.get(name)):
            add_unique_marker(security_profiles, profile)

    security_contexts = {}
    for profile in security_profiles:
        context, context_failures = validate_client_security_context(
            environment,
            profile,
            profile_tools.get(profile, set()),
            profile_semantics.get(profile, set()),
        )
        security_contexts[profile] = context
        failures.extend(context_failures)
        if context.get("mechanism") == "OAUTHBEARER":
            failures.extend(
                validate_client_oauth_positive_fixtures(
                    environment,
                    profile,
                    profile_tools.get(profile, set()),
                    "secured-client OAuth profile",
                )
            )

    for profile in split_csv(
        environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES")
    ):
        tools = profile_tools.get(profile, set())
        semantics = profile_semantics.get(profile, set())
        context = security_contexts.get(profile, {})
        if "security-negative" not in semantics:
            failures.append(
                f"release evidence negative-security profile {profile} must "
                "enable security-negative semantics"
            )
        generic_negative = client_profile_has_generic_negative_vector(
            environment,
            profile,
            context,
        )
        missing_negative = [
            tool
            for tool in sorted(tools)
            if not generic_negative
            and not client_profile_has_oauth_negative_vector(environment, profile, tool)
        ]
        if missing_negative:
            failures.append(
                f"release evidence negative-security profile {profile} must "
                "record a compatible negative fixture for selected tools: "
                + ", ".join(missing_negative)
            )

    for profile in split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES")):
        context = security_contexts.get(profile, {})
        semantics = profile_semantics.get(profile, set())
        if context.get("mechanism") != "OAUTHBEARER":
            failures.append(
                f"release evidence OAuth profile {profile} must set "
                "SASL_MECHANISM=OAUTHBEARER"
            )
        if "security" not in semantics:
            failures.append(
                f"release evidence OAuth profile {profile} must enable "
                "security semantics"
            )
        failures.extend(
            validate_client_oauth_positive_fixtures(
                environment,
                profile,
                profile_tools.get(profile, set()),
                "OAuth profile",
            )
        )

    for profile in split_csv(
        environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES")
    ):
        context = security_contexts.get(profile, {})
        semantics = profile_semantics.get(profile, set())
        if context.get("mechanism") != "OAUTHBEARER":
            failures.append(
                f"release evidence OAuth-negative profile {profile} must set "
                "SASL_MECHANISM=OAUTHBEARER"
            )
        if "security-negative" not in semantics:
            failures.append(
                f"release evidence OAuth-negative profile {profile} must "
                "enable security-negative semantics"
            )
        failures.extend(
            validate_client_oauth_negative_fixtures(
                environment,
                profile,
                profile_tools.get(profile, set()),
                "OAuth-negative profile",
            )
        )

    return failures


def validate_s3_profile_enable_provenance(environment):
    failures = []
    for required_name, suffix, label in S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS:
        for profile in split_csv(environment.get(required_name)):
            env_name, value = profile_setting_environment_value(
                environment,
                "ZMQ_S3",
                profile,
                suffix,
            )
            if value is None:
                failures.append(
                    f"release evidence missing {suffix}=1 provenance for "
                    f"{label} {profile}"
                )
                continue
            enabled = bool_environment_value({env_name: value}, env_name, failures)
            if enabled is None:
                continue
            if not enabled:
                failures.append(
                    f"release evidence {env_name} for {label} {profile} "
                    "must be truthy"
                )
    return failures


def s3_profile_enable_command_env_names(environment):
    names = []
    seen = set()
    for required_name, suffix, _label in S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS:
        for profile in split_csv(environment.get(required_name)):
            env_name, value = profile_setting_environment_value(
                environment,
                "ZMQ_S3",
                profile,
                suffix,
            )
            if not value or env_name in seen:
                continue
            names.append(env_name)
            seen.add(env_name)
    return names


def validate_s3_provider_matrix_command_provenance(command, environment, required):
    failures = []
    for env_name in s3_profile_enable_command_env_names(environment):
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                env_name,
                environment,
                "S3 provider matrix",
            )
        )
    return failures


def validate_live_hook_provenance(environment):
    failures = []
    required_chaos_scenarios = [
        canonical_chaos_scenario(name)
        for name in split_csv(environment.get("ZMQ_CHAOS_REQUIRED_SCENARIOS"))
    ]
    if "live-s3-outage" in required_chaos_scenarios:
        for suffix in ("DOWN", "UP"):
            failures.extend(
                validate_hook_command_provenance(
                    environment,
                    (f"ZMQ_CHAOS_S3_{suffix}",),
                    "chaos live-S3 outage",
                )
            )

    for requirement in PHASE_HOOK_PROVENANCE_REQUIREMENTS:
        fixture_name = requirement.get("fixture")
        if fixture_name and bool_environment_value(environment, fixture_name) is True:
            if requirement["prefix"] == "ZMQ_E2E_LOAD_SCALE":
                for phase in split_csv(environment.get(requirement["required"])):
                    for suffix in requirement["suffixes"]:
                        env_name, value = e2e_load_scale_phase_hook_setting(
                            environment,
                            phase,
                            suffix,
                        )
                        if value is not None:
                            failures.extend(
                                validate_hook_command_value(
                                    env_name,
                                    value,
                                    f"{requirement['label']} {phase}",
                                )
                            )
                continue
            continue
        for phase in split_csv(environment.get(requirement["required"])):
            token = coverage_env_token(phase, requirement["token_style"])
            for suffix in requirement["suffixes"]:
                failures.extend(
                    validate_hook_command_provenance(
                        environment,
                        (
                            f"{requirement['prefix']}_{token}_{suffix}",
                            f"{requirement['prefix']}_{suffix}",
                        ),
                        f"{requirement['label']} {phase}",
                    )
                )

    for requirement in PROFILE_HOOK_PROVENANCE_REQUIREMENTS:
        for profile in split_csv(environment.get(requirement["required"])):
            token = coverage_env_token(profile, requirement["token_style"])
            for suffix in requirement["suffixes"]:
                failures.extend(
                    validate_hook_command_provenance(
                        environment,
                        (
                            f"{requirement['prefix']}_{token}_{suffix}",
                            f"{requirement['prefix']}_{suffix}",
                        ),
                        f"{requirement['label']} {profile}",
                    )
                )
    return failures


def project_path(value):
    if os.path.isabs(value):
        return os.path.realpath(value)
    return os.path.realpath(os.path.join(PROJECT_DIR, value))


def benchmark_trend_baseline_points_at_current_results(value):
    if not isinstance(value, str) or not value.strip():
        return False
    return project_path(value.strip()) == project_path(BENCHMARK_RESULTS_ARTIFACT)


def finite_non_negative_float_failure(name, raw, context):
    if not isinstance(raw, str) or not raw.strip():
        return f"{context} {name} must be a finite non-negative float"
    try:
        value = float(raw)
    except ValueError:
        return f"{context} {name} must be a finite non-negative float"
    if not math.isfinite(value):
        return f"{context} {name} must be a finite non-negative float"
    if value < 0:
        return f"{context} {name} must be a finite non-negative float"
    return None


def integer_environment_rule(name):
    if name in POSITIVE_INTEGER_ENV_VARS:
        return (1, None, "positive integer")
    if (
        name == "ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS"
        or (
            name.startswith("ZMQ_E2E_LOAD_SCALE_")
            and name.endswith("_FIXTURE_LOAD_RECORDS")
        )
    ):
        return (1, None, "positive integer")
    if name.endswith("_PORT") or name.endswith("_PORT_BASE"):
        return (1, 65535, "positive TCP port")
    if name.endswith("_PHASE_INDEX"):
        return (0, None, "non-negative integer")
    return None


def integer_environment_failure(name, raw, context):
    rule = integer_environment_rule(name)
    if rule is None:
        return None
    minimum, maximum, label = rule
    if not isinstance(raw, str):
        return f"{context} {name} must be an integer string"
    stripped = raw.strip()
    if not stripped:
        return f"{context} {name} must not be blank"
    if placeholder_env_value(stripped):
        return f"{context} {name} uses placeholder value"
    try:
        parsed = int(stripped)
    except ValueError:
        return f"{context} {name} must be an integer"
    if parsed < minimum:
        return f"{context} {name} must be a {label}"
    if maximum is not None and parsed > maximum:
        return f"{context} {name} must be a {label}"
    return None


def validate_integer_environment(environment):
    failures = []
    for name, value in environment.items():
        if integer_environment_rule(name) is None:
            continue
        failure = integer_environment_failure(
            name,
            value,
            "release evidence environment variable",
        )
        if failure:
            failures.append(failure)
    return failures


def e2e_load_scale_fixture_action_name(name):
    return (
        name == "ZMQ_E2E_LOAD_SCALE_FIXTURE_ACTION"
        or (
            name.startswith("ZMQ_E2E_LOAD_SCALE_")
            and name.endswith("_FIXTURE_ACTION")
        )
    )


def validate_e2e_load_scale_fixture_environment(environment):
    failures = []
    for name, value in environment.items():
        if not e2e_load_scale_fixture_action_name(name):
            continue
        if not isinstance(value, str):
            failures.append(
                f"release evidence environment variable {name} must be a string"
            )
            continue
        stripped = value.strip()
        if not stripped:
            failures.append(
                f"release evidence environment variable {name} must not be blank"
            )
            continue
        if placeholder_env_value(stripped):
            failures.append(
                f"release evidence environment variable {name} uses placeholder value"
            )
            continue
        if stripped.lower() not in E2E_LOAD_SCALE_FIXTURE_ACTIONS:
            failures.append(
                f"release evidence environment variable {name} must be one of "
                + ", ".join(sorted(E2E_LOAD_SCALE_FIXTURE_ACTIONS))
            )
    return failures


def validate_benchmark_threshold_environment(environment):
    failures = []
    for name in BENCHMARK_THRESHOLD_ENV_VARS:
        if name not in environment:
            continue
        failure = finite_non_negative_float_failure(
            name,
            environment.get(name),
            "release evidence environment variable",
        )
        if failure:
            failures.append(failure)
    return failures


def validate_benchmark_threshold_command_assignments(command, index):
    failures = []
    try:
        segments = split_command_segments(shell_tokens(command))
    except ValueError:
        return failures

    prefixes = tuple(name + "=" for name in BENCHMARK_THRESHOLD_ENV_VARS)
    for segment in segments:
        for assignment in segment_env_assignments(segment):
            if not assignment.startswith(prefixes):
                continue
            name, raw = assignment.split("=", 1)
            failure = finite_non_negative_float_failure(
                name,
                raw,
                f"command entry {index} assignment",
            )
            if failure:
                failures.append(failure)
    return failures


def validate_integer_command_assignments(command, index):
    failures = []
    try:
        segments = split_command_segments(shell_tokens(command))
    except ValueError:
        return failures

    for segment in segments:
        for assignment in segment_env_assignments(segment):
            name, raw = assignment.split("=", 1)
            if integer_environment_rule(name) is None:
                continue
            failure = integer_environment_failure(
                name,
                raw,
                f"command entry {index} assignment",
            )
            if failure:
                failures.append(failure)
    return failures


def command_env_assignment_for_requirement(command, required, name):
    try:
        tokens = shell_tokens(command)
        _env_assignments, _env_names, invocations = classify_required_fragments(required)
        segments = split_command_segments(tokens)
    except ValueError:
        return None

    match_indexes = match_invocation_indexes(segments, invocations)
    if not match_indexes:
        return None

    prefix = name + "="
    value = None
    for token in segment_env_assignments(segments[match_indexes[-1]]):
        if token.startswith(prefix):
            value = token[len(prefix) :]
    return value


def validate_command_assignment_matches_manifest(command, required, name, environment, label):
    command_value = command_env_assignment_for_requirement(
        command or "",
        required,
        name,
    )
    manifest_value = environment.get(name)
    if not command_value:
        return [
            f"release evidence command for {label} must include non-empty "
            f"{name}= assignment"
        ]
    if placeholder_env_value(command_value):
        return [
            f"release evidence command for {label} uses placeholder {name} value"
        ]
    if not isinstance(manifest_value, str) or not manifest_value.strip():
        return [
            f"release evidence manifest environment for {label} must record "
            f"non-empty {name}"
        ]
    if command_value != manifest_value.strip():
        return [
            f"release evidence command for {label} uses {name}={command_value!r}, "
            f"but manifest environment records {manifest_value.strip()!r}"
        ]
    return []


def validate_required_command_env_assignments(command, required, environment, label):
    failures = []
    env_assignments, _env_names, _invocations = classify_required_fragments(required)
    seen = set()
    for assignment in env_assignments:
        name, _required_value = assignment.split("=", 1)
        if name in seen:
            continue
        seen.add(name)
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                name,
                environment,
                label,
            )
        )
    return failures


def command_zig_build_step(command):
    try:
        tokens = shell_tokens(command)
        segments = split_command_segments(tokens)
    except ValueError:
        return None

    for segment in segments:
        invocation = segment_invocation(segment)
        if (
            len(invocation) >= 3
            and invocation[0] == RELEASE_ZIG
            and invocation[1] == "build"
        ):
            return invocation[2]
    return None


def requirement_zig_build_step(requirement):
    for fragment in requirement["required"]:
        try:
            tokens = shell_tokens(fragment)
        except ValueError:
            continue
        if len(tokens) >= 3 and tokens[0] == RELEASE_ZIG and tokens[1] == "build":
            return tokens[2]
    return None


def validate_environment(environment):
    failures = []
    failures.extend(validate_environment_names_and_values(environment))

    for name in REQUIRED_ENV_VARS:
        value = environment.get(name)
        if not isinstance(value, str) or not value.strip():
            failures.append(f"release evidence missing required environment variable {name}")
            continue
        if placeholder_env_value(value):
            failures.append(
                f"release evidence environment variable {name} uses placeholder value"
            )

    for name in COMMA_SEPARATED_ENV_VARS:
        if name in environment:
            failures.extend(
                csv_value_failures(
                    name,
                    environment.get(name),
                    "release evidence environment variable",
                )
            )

    failures.extend(validate_benchmark_threshold_environment(environment))
    failures.extend(validate_integer_environment(environment))
    failures.extend(validate_e2e_load_scale_fixture_environment(environment))
    failures.extend(validate_coverage_selector_provenance(environment))
    failures.extend(validate_live_hook_provenance(environment))
    failures.extend(validate_s3_profile_enable_provenance(environment))
    failures.extend(validate_client_profile_provenance(environment))
    failures.extend(validate_boolean_environment(environment))
    failures.extend(validate_s3_string_environment(environment))

    trend_required = bool_environment_value(
        environment,
        "ZMQ_BENCH_COMPARE_REQUIRE_TREND",
    )
    if trend_required is not True:
        failures.append("release evidence must set ZMQ_BENCH_COMPARE_REQUIRE_TREND=1")
    trend_baseline = environment.get("ZMQ_BENCH_COMPARE_TREND_BASELINE")
    if benchmark_trend_baseline_points_at_current_results(trend_baseline):
        failures.append(
            "release evidence ZMQ_BENCH_COMPARE_TREND_BASELINE must point at "
            "a prior benchmark artifact, not the current benchmarks/results.json output"
        )

    targets = split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"))
    unknown_targets = [
        target for target in targets if target not in COMPARATIVE_TARGET_LABELS
    ]
    if unknown_targets:
        failures.append(
            "release evidence has unknown comparative benchmark required targets: "
            + ", ".join(unknown_targets)
        )
    duplicate_targets = sorted(
        target
        for target in set(targets)
        if targets.count(target) > 1
    )
    if duplicate_targets:
        failures.append(
            "release evidence comparative benchmark required targets must not "
            "contain duplicate values: " + ", ".join(duplicate_targets)
        )
    if targets and "zmq" not in targets:
        failures.append("release evidence comparative benchmark targets must include zmq")
    if targets and not any(target in targets for target in ("kafka", "automq")):
        failures.append(
            "release evidence comparative benchmark targets must include kafka or automq"
        )

    chaos_scenarios = [
        canonical_chaos_scenario(name)
        for name in split_csv(environment.get("ZMQ_CHAOS_REQUIRED_SCENARIOS"))
    ]
    unknown_chaos_scenarios = [
        scenario
        for scenario in chaos_scenarios
        if scenario not in CHAOS_SCENARIO_MARKERS
    ]
    if unknown_chaos_scenarios:
        failures.append(
            "release evidence has unknown required chaos scenarios: "
            + ", ".join(unknown_chaos_scenarios)
        )
    missing_chaos_scenarios = [
        scenario
        for scenario in REQUIRED_CHAOS_SCENARIOS
        if scenario not in chaos_scenarios
    ]
    if missing_chaos_scenarios:
        failures.append(
            "release evidence chaos scenarios must include: "
            + ", ".join(missing_chaos_scenarios)
        )
    if (
        split_csv(environment.get("ZMQ_CHAOS_REQUIRED_NETWORK_PHASES"))
        and "network-partition" not in chaos_scenarios
    ):
        failures.append(
            "release evidence chaos network phases require network-partition scenario"
        )

    client_tools = set(split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS")))
    unknown_client_tools = [
        tool for tool in client_tools if tool not in CLIENT_TOOL_OUTPUT_MARKERS
    ]
    if unknown_client_tools:
        failures.append(
            "release evidence has unknown client matrix required tools: "
            + ", ".join(sorted(unknown_client_tools))
        )
    missing_client_tools = [
        tool for tool in REQUIRED_CLIENT_TOOLS if tool not in client_tools
    ]
    if missing_client_tools:
        failures.append(
            "release evidence client matrix tools must include: "
            + ", ".join(missing_client_tools)
        )

    client_semantics = set(
        split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS"))
    )
    known_client_semantics = set(REQUIRED_CLIENT_SEMANTICS)
    unknown_client_semantics = [
        semantic for semantic in client_semantics if semantic not in known_client_semantics
    ]
    if unknown_client_semantics:
        failures.append(
            "release evidence has unknown client matrix required semantics: "
            + ", ".join(sorted(unknown_client_semantics))
        )
    missing_client_semantics = [
        semantic
        for semantic in REQUIRED_CLIENT_SEMANTICS
        if semantic not in client_semantics
    ]
    if missing_client_semantics:
        failures.append(
            "release evidence client matrix semantics must include: "
            + ", ".join(missing_client_semantics)
        )

    provider_profiles = set(split_csv(environment.get("ZMQ_S3_PROVIDER_REQUIRED_PROFILES")))
    for name in (
        "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
    ):
        missing = [
            profile
            for profile in split_csv(environment.get(name))
            if profile not in provider_profiles
        ]
        if missing:
            failures.append(
                f"release evidence {name} includes profiles not required in "
                "ZMQ_S3_PROVIDER_REQUIRED_PROFILES: "
                + ", ".join(missing)
            )

    client_profiles = set(split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES")))
    for name in (
        "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
    ):
        missing = [
            profile
            for profile in split_csv(environment.get(name))
            if profile not in client_profiles
        ]
        if missing:
            failures.append(
                f"release evidence {name} includes profiles not required in "
                "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES: "
                + ", ".join(missing)
            )

    load_scale_phases = set(
        split_csv(environment.get("ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"))
    )
    missing_load_scale = [
        phase for phase in ("load", "scale-in", "scale-out")
        if phase not in load_scale_phases
    ]
    if missing_load_scale:
        failures.append(
            "release evidence E2E load/scale phases must include: "
            + ", ".join(missing_load_scale)
        )

    e2e_chaos_phases = split_csv(environment.get("ZMQ_E2E_REQUIRED_CHAOS_PHASES"))
    if not any("cross" in phase and "broker" in phase for phase in e2e_chaos_phases):
        failures.append("release evidence E2E chaos phases must include cross-broker coverage")

    return failures


def validate_command_entry(entry, index):
    failures = []
    if not isinstance(entry, dict):
        return [f"command entry {index} must be an object"]
    failures.extend(
        validate_object_fields(entry, COMMAND_ENTRY_FIELDS, f"command entry {index}")
    )

    command = entry.get("command")
    if not isinstance(command, str) or not command.strip():
        failures.append(f"command entry {index} missing command string")
    elif "/path/to/" in command or placeholder_env_value(command):
        failures.append(f"command entry {index} uses placeholder value in command string")
    else:
        failures.extend(validate_shell_command_single_line(command, index))
        failures.extend(validate_shell_command_unquoted(command, index))
        failures.extend(validate_shell_command_unescaped(command, index))
        failures.extend(validate_shell_command_separators(command, index))
        failures.extend(validate_shell_command_segment_shape(command, index))
        failures.extend(validate_disallowed_shell_operators(command, index))
        failures.extend(validate_duplicate_command_env_assignments(command, index))
        failures.extend(validate_command_does_not_embed_output_markers(command, index))
        failures.extend(validate_benchmark_threshold_command_assignments(command, index))
        failures.extend(validate_integer_command_assignments(command, index))

    if entry.get("exit_code") != 0:
        failures.append(f"command entry {index} did not exit successfully")

    output = entry.get("output")
    if not isinstance(output, str) or not output.strip():
        failures.append(f"command entry {index} missing captured output")
    elif isinstance(command, str) and f"{RELEASE_ZIG} build " in command:
        zig_step = command_zig_build_step(command)
        if zig_build_summary_failure_present(output):
            failures.append(
                f"command entry {index} contains unsuccessful Zig Build Summary output"
            )
        successful_summaries = zig_build_summary_success_count(output)
        if successful_summaries == 0:
            failures.append(
                f"command entry {index} missing successful Zig Build Summary output"
            )
        elif successful_summaries > 1:
            failures.append(
                f"command entry {index} contains multiple successful Zig Build Summary outputs"
            )
        if not zig_step or not zig_success_line_present(output, zig_step):
            failures.append(
                f"command entry {index} missing Zig success line for invoked build step"
            )

    return failures


def unsupported_surface_text(entry):
    if not isinstance(entry, dict):
        return ""

    parts = []
    for key in UNSUPPORTED_SURFACE_TEXT_FIELDS:
        value = entry.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, list):
            parts.extend(item for item in value if isinstance(item, str))
    return " ".join(parts)


def unsupported_surface_status_is_explicit(status):
    lowered = status.strip().lower()
    return any(marker in lowered for marker in UNSUPPORTED_SURFACE_STATUS_MARKERS)


def unsupported_surface_entry_matches(entry, surface):
    return all(fragment in entry for fragment in surface["fragments"])


def unsupported_surface_name_matches(entry, surface):
    if not isinstance(entry, dict):
        return False
    name = entry.get("surface")
    if not isinstance(name, str):
        return False
    return all(fragment in name for fragment in surface["surface_fragments"])


def unsupported_surface_status_matches(entry, surface):
    if not isinstance(entry, dict):
        return False
    status = entry.get("status")
    if not isinstance(status, str):
        return False
    lowered = status.strip().lower()
    return any(marker in lowered for marker in surface["status_markers"])


def validate_optional_unsupported_surface_field(index, field, value):
    failures = []
    if isinstance(value, str):
        if not value.strip():
            failures.append(
                f"unsupported_or_partial_surfaces entry {index} optional field "
                f"{field} must not be blank"
            )
        elif placeholder_env_value(value):
            failures.append(
                f"unsupported_or_partial_surfaces entry {index} optional field "
                f"{field} uses placeholder value"
            )
        return failures

    if isinstance(value, list):
        if not value:
            failures.append(
                f"unsupported_or_partial_surfaces entry {index} optional field "
                f"{field} must not be an empty list"
            )
            return failures
        for item_index, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                failures.append(
                    f"unsupported_or_partial_surfaces entry {index} optional field "
                    f"{field} item {item_index} must be a non-empty string"
                )
            elif placeholder_env_value(item):
                failures.append(
                    f"unsupported_or_partial_surfaces entry {index} optional field "
                    f"{field} item {item_index} uses placeholder value"
                )
        return failures

    failures.append(
        f"unsupported_or_partial_surfaces entry {index} optional field "
        f"{field} must be a string or list of strings"
    )
    return failures


def validate_unsupported_surfaces(unsupported):
    failures = []
    entries = []
    for index, entry in enumerate(unsupported):
        if not isinstance(entry, dict):
            failures.append(
                f"unsupported_or_partial_surfaces entry {index} must be an object "
                "with surface, status, and evidence fields"
            )
            entries.append("")
            continue
        failures.extend(
            validate_object_fields(
                entry,
                UNSUPPORTED_SURFACE_FIELDS,
                f"unsupported_or_partial_surfaces entry {index}",
            )
        )

        for field in REQUIRED_UNSUPPORTED_SURFACE_FIELDS:
            value = entry.get(field)
            if not isinstance(value, str) or not value.strip():
                failures.append(
                    f"unsupported_or_partial_surfaces entry {index} "
                    f"missing required field {field}"
                )
            elif placeholder_env_value(value):
                failures.append(
                    f"unsupported_or_partial_surfaces entry {index} "
                    f"uses placeholder {field} value"
                )
            elif field == "status" and not unsupported_surface_status_is_explicit(value):
                failures.append(
                    f"unsupported_or_partial_surfaces entry {index} status "
                    "must explicitly mark the surface as unsupported, partial, "
                    "blocked, fail-closed, not advertised, or release-CI-required"
                )

        for field in OPTIONAL_UNSUPPORTED_SURFACE_FIELDS:
            if field in entry:
                failures.extend(
                    validate_optional_unsupported_surface_field(
                        index,
                        field,
                        entry.get(field),
                    )
                )

        text = unsupported_surface_text(entry)
        entries.append(text)
        if not text.strip():
            failures.append(
                f"unsupported_or_partial_surfaces entry {index} must describe a surface"
            )

    matched_entry_indexes = set()
    for surface in REQUIRED_UNSUPPORTED_SURFACES:
        matching_indexes = [
            index
            for index, entry in enumerate(entries)
            if unsupported_surface_entry_matches(entry, surface)
        ]
        if len(matching_indexes) > 1:
            failures.append(
                "release evidence contains duplicate unsupported/partial surface "
                f"accounting for {surface['label']}: "
                + ", ".join(str(index) for index in matching_indexes)
            )
        matched_index = None
        for index in matching_indexes:
            if index in matched_entry_indexes:
                continue
            matched_index = index
            break
        if matched_index is None:
            failures.append(
                "release evidence missing unsupported/partial surface accounting for "
                f"{surface['label']}"
            )
        else:
            if not unsupported_surface_status_matches(
                unsupported[matched_index],
                surface,
            ):
                failures.append(
                    "unsupported_or_partial_surfaces entry "
                    f"{matched_index} status for {surface['label']} must mark "
                    f"the surface as {surface['status_label']}"
                )
            if not unsupported_surface_name_matches(
                unsupported[matched_index],
                surface,
            ):
                failures.append(
                    "unsupported_or_partial_surfaces entry "
                    f"{matched_index} surface field for {surface['label']} "
                    "must name the known surface; evidence, mitigation, and "
                    "notes cannot be the only matching fields"
                )
            matched_entry_indexes.add(matched_index)

    unmatched_indexes = sorted(set(range(len(entries))) - matched_entry_indexes)
    for index in unmatched_indexes:
        if entries[index].strip():
            failures.append(
                "release evidence contains unsupported/partial surface accounting "
                f"outside the verifier catalog at entry {index}"
            )

    return failures


def add_unique_marker(markers, marker):
    if marker not in markers:
        markers.append(marker)


def output_lines(output):
    return [line.strip() for line in output.splitlines()]


def benchmark_lines_before_completion(output):
    lines = []
    for line in output_lines(output):
        if line == "=== Benchmarks complete ===":
            return lines
        lines.append(line)
    return []


def zig_build_summary_line_success(line):
    match = ZIG_BUILD_SUMMARY_RE.fullmatch(line)
    if not match or match.group(1) != match.group(2):
        return False
    if match.group(3) is not None and match.group(3) != match.group(4):
        return False
    return True


def zig_build_summary_failure_present(output):
    for line in output_lines(output):
        if line.startswith("Build Summary:") and not zig_build_summary_line_success(line):
            return True
    return False


def zig_build_summary_success_present(output):
    return zig_build_summary_success_count(output) > 0


def zig_build_summary_success_count(output):
    return sum(1 for line in output_lines(output) if zig_build_summary_line_success(line))


def zig_success_line_present(output, zig_step):
    expected = f"{zig_step} success".lower()
    for line in output_lines(output):
        lowered = line.lower()
        if lowered == expected and "failure" not in lowered and "failed" not in lowered:
            return True
    return False


def output_line_marker_present(output, marker):
    for line in output_lines(output):
        if line == marker:
            return True
        if line.startswith(marker + " ") or line.startswith(marker + " ("):
            return True
    return False


def output_template_marker_present(output, marker):
    if "<id>" not in marker:
        return None
    if marker.startswith("ok: KRaft network partition phase "):
        pattern = re.escape(marker).replace(re.escape("<id>"), r"[0-9]+")
        return any(re.fullmatch(pattern, line) is not None for line in output_lines(output))
    return None


def line_marker_present(lines, marker):
    for line in lines:
        if line == marker:
            return True
        if line.startswith(marker + " ") or line.startswith(marker + " ("):
            return True
    return False


def e2e_title_output_marker_line_matches(line, marker):
    if output_line_marker_present(line, marker):
        return True
    if marker not in line or not line.startswith(("\u2551", "|")):
        return False
    title = line.strip("\u2551| ").strip()
    if not title.startswith("ZMQ "):
        return False
    title = title[len("ZMQ ") :].lstrip("-\u2014: ")
    return (
        title == marker
        or title.startswith(marker + " ")
        or title.startswith(marker + " (")
    )


def e2e_output_marker_lines(output, marker):
    if marker == "3-Node E2E Test Suite":
        return [
            line
            for line in output_lines(output)
            if e2e_title_output_marker_line_matches(line, marker)
        ]
    return [
        line
        for line in output_lines(output)
        if line == marker
        or line.startswith(marker + " ")
        or line.startswith(marker + " (")
    ]


def e2e_output_marker_present(output, marker):
    return bool(e2e_output_marker_lines(output, marker))


def skip_marker_present(output, marker):
    if marker.startswith("skip:"):
        return output_line_marker_present(output, marker)
    if marker == "skipped":
        for line in output_lines(output):
            if line == marker or line.startswith("skip:"):
                return True
            if re.search(r"\([1-9][0-9]* skipped\)", line):
                return True
        return False
    return output_line_marker_present(output, marker)


def comparative_benchmark_gate_indexes(lines):
    return [
        index
        for index, line in enumerate(lines)
        if line == "COMPARATIVE BENCHMARK GATE"
    ]


def comparative_benchmark_gate_index(lines):
    indexes = comparative_benchmark_gate_indexes(lines)
    return indexes[0] if indexes else None


def comparative_benchmark_comparison_indexes(lines):
    gate_index = comparative_benchmark_gate_index(lines)
    end_index = gate_index if gate_index is not None else len(lines)
    return [
        index
        for index, line in enumerate(lines[:end_index])
        if line.startswith("COMPARISON:")
    ]


def comparative_benchmark_comparison_index(lines):
    indexes = comparative_benchmark_comparison_indexes(lines)
    return indexes[0] if indexes else None


def comparative_expected_comparison_line():
    return "COMPARISON: " + " vs ".join(COMPARATIVE_TARGET_LABELS.values())


def comparative_benchmark_table_section(output):
    lines = output_lines(output)
    comparison_index = comparative_benchmark_comparison_index(lines)
    if comparison_index is None:
        return []
    gate_index = comparative_benchmark_gate_index(lines)
    end_index = gate_index if gate_index is not None else len(lines)
    return lines[comparison_index + 1 : end_index]


def comparative_output_marker_present(output, marker):
    lines = output_lines(output)
    comparison_index = comparative_benchmark_comparison_index(lines)
    if marker in COMPARATIVE_TARGET_LABELS.values():
        return comparison_index is not None and marker in lines[comparison_index]

    if marker == "COMPARISON:":
        return comparison_index is not None

    if marker == "Benchmark":
        return any(
            line.startswith("Benchmark") and re.search(r"\bMetric\b", line)
            for line in comparative_benchmark_table_section(output)
        )

    if marker in COMPARATIVE_TABLE_ROW_MARKERS:
        row_re = re.compile(r"^" + re.escape(marker) + r"\s+tput\b")
        return any(
            row_re.search(line) is not None
            for line in comparative_benchmark_table_section(output)
        )

    if marker == "COMPARATIVE BENCHMARK GATE":
        return output_line_marker_present(output, marker)

    return None


def output_marker_present(output, marker):
    comparative_match = comparative_output_marker_present(output, marker)
    if comparative_match is not None:
        return comparative_match
    template_match = output_template_marker_present(output, marker)
    if template_match is not None:
        return template_match
    if marker == "8/8 tests passed":
        return any(
            line == marker or (line.startswith("Build Summary:") and marker in line)
            for line in output_lines(output)
        )
    if marker in KRAFT_DETAIL_OUTPUT_MARKERS:
        return any(
            line.startswith("ok: KRaft controller failover harness passed ")
            and marker in line
            for line in output_lines(output)
        )
    if marker == "ok: chaos network-partition source=command":
        return any(line == marker for line in output_lines(output))
    if marker in E2E_OUTPUT_LINE_MARKERS:
        return e2e_output_marker_present(output, marker)
    if marker in BENCHMARK_OUTPUT_LINE_MARKERS:
        return output_line_marker_present(output, marker)
    if marker.startswith("ok:"):
        return output_line_marker_present(output, marker)
    if marker == "result: pass":
        return any(line == marker for line in output_lines(output))
    if marker in ("COMPARISON:", "thresholds:", "trend thresholds:", "trend baseline:"):
        return output_line_marker_present(output, marker)
    return False


def minio_test_count_lines(output):
    return [
        line
        for line in output_lines(output)
        if line == "8/8 tests passed"
        or (line.startswith("Build Summary:") and "8/8 tests passed" in line)
    ]


def validate_minio_test_count_output(output):
    if len(minio_test_count_lines(output)) > 1:
        return [
            "release evidence MinIO 8/8 tests passed output marker must "
            "appear exactly once"
        ]
    return []


def exact_once_output_marker_lines(output, marker):
    return [
        line
        for line in output_lines(output)
        if line == marker
        or line.startswith(marker + " ")
        or line.startswith(marker + " (")
    ]


def exact_once_requirement_output_marker_failures(label, output):
    failures = []
    label_line = f"ok: {label}"
    for marker in EXACT_ONCE_OUTPUT_MARKERS_BY_LABEL.get(label, ()):
        marker_lines = exact_once_output_marker_lines(output, marker)
        exact_lines = [line for line in marker_lines if line == marker]
        invalid_non_exact_lines = [
            line
            for line in marker_lines
            if line != marker and line != label_line
        ]
        if (
            len(exact_lines) > 1
            or invalid_non_exact_lines
            or (marker_lines and len(exact_lines) != 1)
        ):
            failures.append(
                f"release evidence {label} output marker must appear "
                f"exactly once as its own stripped line: {marker}"
            )
    return failures


def client_matrix_lines_before_summary(output):
    lines = []
    for line in output_lines(output):
        if (
            line.startswith("ok: client matrix passed for ")
            and line.endswith(" profile(s) source=command")
        ):
            return lines
        lines.append(line)
    return []


def output_summary_candidate_lines(output, marker, suffix=None):
    if suffix is not None:
        return [
            line
            for line in output_lines(output)
            if line.startswith(marker) and line.endswith(suffix)
        ]
    return [
        line
        for line in output_lines(output)
        if line == marker or line.startswith(marker + " ")
    ]


def exact_output_line_count(output, marker):
    return sum(1 for line in output_lines(output) if line == marker)


def exact_summary_output_line_failures(output, marker, description):
    marker_lines = output_summary_candidate_lines(output, marker)
    exact_count = exact_output_line_count(output, marker)
    if len(marker_lines) > 1 or (marker_lines and exact_count != 1):
        return [
            f"release evidence {description} output marker must appear "
            "exactly once as its own stripped line"
        ]
    return []


def suffixed_summary_output_line_failures(output, prefix, suffix, description):
    prefixed_lines = [
        line
        for line in output_lines(output)
        if line.startswith(prefix)
    ]
    valid_lines = [
        line
        for line in prefixed_lines
        if line.endswith(suffix)
    ]
    if len(prefixed_lines) > 1 or (prefixed_lines and len(valid_lines) != 1):
        return [
            f"release evidence {description} output marker must appear "
            "exactly once with source=command as its own stripped line"
        ]
    return []


def client_probe_semantic_tokens(output):
    tokens = set()
    source_suffix = ") source=command"
    for marker in CLIENT_TOOL_OUTPUT_MARKERS.values():
        prefix = marker + " ("
        for line in client_matrix_lines_before_summary(output):
            if not line.startswith(prefix) or not line.endswith(source_suffix):
                continue
            values, _failures = output_csv_values(
                line[len(prefix) : -len(source_suffix)],
                "client tool probe semantics",
            )
            tokens.update(values)
    return tokens


def client_probe_semantic_present(output, semantic):
    return semantic in client_probe_semantic_tokens(output)


def client_probe_semantics_by_tool(output):
    semantics_by_tool = {}
    source_suffix = ") source=command"
    for tool, marker in CLIENT_TOOL_OUTPUT_MARKERS.items():
        prefix = marker + " ("
        tool_semantics = set()
        for line in output_lines(output):
            if not line.startswith(prefix) or not line.endswith(source_suffix):
                continue
            values, _failures = output_csv_values(
                line[len(prefix) : -len(source_suffix)],
                f"client tool probe semantics for {tool}",
            )
            tool_semantics.update(values)
        semantics_by_tool[tool] = tool_semantics
    return semantics_by_tool


def client_profile_tools_for_semantic(environment, semantic):
    tools = set()
    for profile in split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES")):
        profile_tools, tool_failures = validate_client_profile_tools(environment, profile)
        profile_semantics, semantic_failures = validate_client_profile_semantics(
            environment,
            profile,
        )
        if tool_failures or semantic_failures or semantic not in profile_semantics:
            continue
        tools.update(profile_tools)
    return tools


def client_profile_semantic_output_present(output, environment, semantic):
    eligible_profiles = []
    for profile in split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES")):
        profile_tools, tool_failures = validate_client_profile_tools(environment, profile)
        profile_semantics, semantic_failures = validate_client_profile_semantics(
            environment,
            profile,
        )
        if tool_failures or semantic_failures or semantic not in profile_semantics:
            continue
        eligible_profiles.append((profile, profile_tools))
    if not eligible_profiles:
        return True
    profile_blocks = client_profile_probe_blocks(output, environment)
    return any(
        any(
            semantic in block.get(tool, set())
            for block in profile_blocks.get(profile, [])
            for tool in profile_tools
        )
        for profile, profile_tools in eligible_profiles
    )


def parse_client_tool_probe_line(line):
    source_suffix = ") source=command"
    for tool, marker in CLIENT_TOOL_OUTPUT_MARKERS.items():
        prefix = marker + " ("
        if not line.startswith(prefix):
            continue
        if not line.endswith(source_suffix):
            return tool, set(), [
                "client tool probe marker must use "
                f"source=command line shape for {tool}"
            ]
        semantics, failures = output_csv_values(
            line[len(prefix) : -len(source_suffix)],
            f"client tool probe semantics for {tool}",
        )
        return tool, set(semantics), failures
    return None, set(), []


def parse_client_profile_output_line(line, profile):
    base = f"ok: client matrix profile {profile}"
    prefix = base + " passed for "
    source_suffix = " source=command"
    if line != base and not line.startswith(base + " "):
        return None, False
    if not line.startswith(prefix):
        return None, True
    rest = line[len(prefix) :]
    if not rest.endswith(source_suffix):
        return None, True
    rest = rest[: -len(source_suffix)]
    if " against " not in rest:
        return None, True
    tools_raw, bootstrap = rest.split(" against ", 1)
    version = None
    if " version=" in bootstrap:
        bootstrap, version = bootstrap.rsplit(" version=", 1)
        version = version.strip()
    tools, tool_failures = output_csv_values(
        tools_raw,
        f"client profile {profile} output tools",
    )
    bootstrap = bootstrap.strip()
    if tool_failures or not tools or not bootstrap or version == "":
        return None, True
    if validate_bootstrap_servers_text("client profile output marker", bootstrap):
        return None, True
    return (set(tools), bootstrap, version), False


def parse_client_security_detail_line(line, profile):
    pattern = re.compile(
        r"^ok: client security detail profile "
        + re.escape(profile)
        + r" tool=(\S+) protocol=(\S+) mechanism=(\S+) "
        + r"oauth=(true|false) positive=(true|false) "
        + r"security_negative=(true|false) oauth_negative=(true|false) "
        + r"sasl_negative=(true|false) tls_negative=(true|false) "
        + r"acl_negative=(true|false) source=(\S+)$"
    )
    marker_prefix = f"ok: client security detail profile {profile}"
    match = pattern.match(line)
    if match:
        return {
            "tool": match.group(1),
            "protocol": match.group(2),
            "mechanism": match.group(3),
            "oauth": match.group(4),
            "positive": match.group(5),
            "security_negative": match.group(6),
            "oauth_negative": match.group(7),
            "sasl_negative": match.group(8),
            "tls_negative": match.group(9),
            "acl_negative": match.group(10),
            "source": match.group(11),
        }, False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def client_security_bool_text(value):
    return "true" if value else "false"


def client_security_detail_expected(environment, profile, tool, context, semantics):
    protocol = context.get("protocol")
    mechanism = context.get("mechanism") or "none"
    oauth = mechanism == "OAUTHBEARER"
    negative_enabled = "security-negative" in semantics
    sasl_negative = (
        negative_enabled
        and protocol in ("SASL_PLAINTEXT", "SASL_SSL")
        and mechanism != "OAUTHBEARER"
        and client_profile_setting_is_concrete(environment, profile, "SASL_PASSWORD")
    )
    tls_negative = (
        negative_enabled
        and protocol in ("SSL", "SASL_SSL")
        and client_profile_setting_is_concrete(environment, profile, "BAD_SSL_CA_LOCATION")
    )
    acl_negative = (
        negative_enabled
        and client_profile_setting_is_concrete(environment, profile, "ACL_DENIED_TOPIC")
    )
    oauth_negative = (
        negative_enabled
        and oauth
        and client_profile_has_oauth_negative_vector(environment, profile, tool)
    )
    security_negative = (
        negative_enabled
        and (sasl_negative or tls_negative or acl_negative or oauth_negative)
    )
    return {
        "tool": tool,
        "protocol": protocol,
        "mechanism": mechanism,
        "oauth": client_security_bool_text(oauth),
        "positive": "true",
        "security_negative": client_security_bool_text(security_negative),
        "oauth_negative": client_security_bool_text(oauth_negative),
        "sasl_negative": client_security_bool_text(sasl_negative),
        "tls_negative": client_security_bool_text(tls_negative),
        "acl_negative": client_security_bool_text(acl_negative),
        "source": "command",
    }


def client_security_detail_valid(detail, expected):
    return all(detail.get(name) == value for name, value in expected.items())


def client_security_detail_marker_text(profile, detail):
    return (
        f"ok: client security detail profile {profile} "
        f"tool={detail['tool']} protocol={detail['protocol']} "
        f"mechanism={detail['mechanism']} oauth={detail['oauth']} "
        f"positive={detail['positive']} "
        f"security_negative={detail['security_negative']} "
        f"oauth_negative={detail['oauth_negative']} "
        f"sasl_negative={detail['sasl_negative']} "
        f"tls_negative={detail['tls_negative']} "
        f"acl_negative={detail['acl_negative']} "
        f"source={detail['source']}"
    )


def client_profile_output_details(output, profile):
    details = []
    malformed = False
    for line in client_matrix_lines_before_summary(output):
        detail, line_malformed = parse_client_profile_output_line(line, profile)
        malformed = malformed or line_malformed
        if detail is not None:
            details.append(detail)
    return details, malformed


def client_profile_output_blocks(output, environment):
    profile_names = sorted(
        required_client_profile_names(environment),
        key=len,
        reverse=True,
    )
    blocks = {}
    current = {}
    current_probe_counts = {}
    current_probe_failures = {}
    current_security_details = {}
    current_security_detail_malformed = {}
    for line in client_matrix_lines_before_summary(output):
        tool, semantics, probe_failures = parse_client_tool_probe_line(line)
        if tool is not None:
            current.setdefault(tool, set()).update(semantics)
            current_probe_counts[tool] = current_probe_counts.get(tool, 0) + 1
            if probe_failures:
                current_probe_failures.setdefault(tool, []).extend(probe_failures)
            continue
        for profile in profile_names:
            security_detail, security_detail_malformed = (
                parse_client_security_detail_line(line, profile)
            )
            if security_detail is not None:
                current_security_details.setdefault(profile, []).append(security_detail)
                break
            if security_detail_malformed:
                current_security_detail_malformed[profile] = True
                break
            detail, malformed = parse_client_profile_output_line(line, profile)
            if detail is not None:
                tools, bootstrap, version = detail
                blocks.setdefault(profile, []).append({
                    "tools": set(tools),
                    "bootstrap": bootstrap,
                    "version": version,
                    "probes": {
                        tool_name: set(tool_semantics)
                        for tool_name, tool_semantics in current.items()
                    },
                    "probe_counts": dict(current_probe_counts),
                    "probe_failures": {
                        tool_name: list(tool_failures)
                        for tool_name, tool_failures in current_probe_failures.items()
                    },
                    "security_details": list(
                        current_security_details.get(profile, [])
                    ),
                    "security_detail_malformed": (
                        current_security_detail_malformed.get(profile, False)
                    ),
                })
                current = {}
                current_probe_counts = {}
                current_probe_failures = {}
                current_security_details[profile] = []
                current_security_detail_malformed[profile] = False
                break
            if malformed:
                break
    return blocks


def client_profile_probe_blocks(output, environment):
    return {
        profile: [block["probes"] for block in blocks]
        for profile, blocks in client_profile_output_blocks(output, environment).items()
    }


def validate_client_profile_output_markers(output, environment):
    failures = []
    for profile in required_client_profile_names(environment):
        details, malformed = client_profile_output_details(output, profile)
        expected_tools, tool_failures = validate_client_profile_tools(
            environment,
            profile,
        )
        expected_bootstrap = client_profile_expected_bootstrap(environment, profile)
        expected_version = client_profile_expected_version(environment, profile)
        if not details:
            version_suffix = " version=<version>" if expected_version is not None else ""
            failures.append(
                "release evidence missing passed client profile output marker "
                "for external client matrix: "
                f"ok: client matrix profile {profile} passed for <tools> "
                f"against <bootstrap>{version_suffix} source=command"
            )
            continue
        if malformed:
            failures.append(
                "release evidence client profile output marker for external "
                f"client matrix {profile} must use the "
                "passed-for/against/source=command line shape"
            )
        if len(details) > 1:
            failures.append(
                "release evidence client profile output marker for external "
                f"client matrix {profile} must not repeat before the final "
                "client matrix summary"
            )
        if tool_failures:
            continue
        if not any(tools == expected_tools for tools, _bootstrap, _version in details):
            observed = [
                ",".join(sorted(tools))
                for tools, _bootstrap, _version in details
            ]
            failures.append(
                "release evidence client profile output marker for external "
                f"client matrix {profile} must list selected tools "
                f"{','.join(sorted(expected_tools))}: got "
                + ", ".join(observed)
            )
        matching_tool_bootstraps = [
            (bootstrap, version)
            for tools, bootstrap, version in details
            if tools == expected_tools
        ]
        if matching_tool_bootstraps:
            matching_bootstraps = [
                bootstrap for bootstrap, _version in matching_tool_bootstraps
            ]
            if expected_bootstrap is not None:
                if expected_bootstrap not in matching_bootstraps:
                    failures.append(
                        "release evidence client profile output marker for external "
                        f"client matrix {profile} must match selected bootstrap "
                        f"{expected_bootstrap!r}: got "
                        + ", ".join(matching_bootstraps)
                    )
            elif not any(
                not placeholder_env_value(bootstrap)
                for bootstrap in matching_bootstraps
            ):
                failures.append(
                    "release evidence client profile output marker for external "
                    f"client matrix {profile} must include a non-placeholder "
                    "bootstrap after against"
                )
            if expected_version is not None:
                matching_versions = [
                    version
                    for bootstrap, version in matching_tool_bootstraps
                    if (
                        bootstrap == expected_bootstrap
                        if expected_bootstrap is not None
                        else not placeholder_env_value(bootstrap)
                    )
                ]
                if expected_version not in matching_versions:
                    observed_versions = [
                        version if version is not None else "<missing>"
                        for version in matching_versions
                    ] or ["<none>"]
                    failures.append(
                        "release evidence client profile output marker for external "
                        f"client matrix {profile} must report "
                        f"version={expected_version}: got "
                        + ", ".join(observed_versions)
                    )
    return failures


def validate_client_profile_scoped_probe_markers(output, environment):
    failures = []
    blocks = client_profile_output_blocks(output, environment)
    security_detail_required_profiles = set()
    for env_name in (
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
    ):
        security_detail_required_profiles.update(split_csv(environment.get(env_name)))
    for profile in required_client_profile_names(environment):
        expected_tools, tool_failures = validate_client_profile_tools(
            environment,
            profile,
        )
        expected_semantics, semantic_failures = validate_client_profile_semantics(
            environment,
            profile,
        )
        expected_bootstrap = client_profile_expected_bootstrap(environment, profile)
        expected_version = client_profile_expected_version(environment, profile)
        if tool_failures or semantic_failures:
            continue
        profile_blocks = blocks.get(profile, [])
        if not profile_blocks:
            continue
        matching_blocks = [
            block
            for block in profile_blocks
            if (
                block["tools"] == expected_tools
                and (
                    block["bootstrap"] == expected_bootstrap
                    if expected_bootstrap is not None
                    else not placeholder_env_value(block["bootstrap"])
                )
                and (
                    expected_version is None
                    or block.get("version") == expected_version
                )
            )
        ]
        if not matching_blocks:
            continue
        if not any(
            expected_tools.issubset(block["probes"].keys())
            for block in profile_blocks
        ):
            failures.append(
                "release evidence missing profile-scoped client tool probe "
                f"markers before profile pass marker for external client "
                f"matrix {profile}: " + ", ".join(sorted(expected_tools))
            )
        if not any(
            expected_tools.issubset(block["probes"].keys())
            for block in matching_blocks
        ):
            failures.append(
                "release evidence missing same-block client tool probe markers "
                "before the matching profile pass marker for external client "
                f"matrix {profile}: " + ", ".join(sorted(expected_tools))
            )
        for tool in sorted(expected_tools):
            if any(
                block.get("probe_counts", {}).get(tool, 0) > 1
                for block in matching_blocks
            ):
                failures.append(
                    "release evidence client tool probe marker must not repeat "
                    "before the matching profile pass marker for external "
                    f"client matrix {profile}: {tool}"
                )
        malformed_probe_failures = []
        for block in matching_blocks:
            for tool in sorted(expected_tools):
                malformed_probe_failures.extend(
                    block.get("probe_failures", {}).get(tool, [])
                )
        for failure in sorted(set(malformed_probe_failures)):
            failures.append(failure)
        for semantic in sorted(expected_semantics):
            if not any(
                any(
                    semantic in block["probes"].get(tool, set())
                    for tool in expected_tools
                )
                for block in matching_blocks
            ):
                failures.append(
                    "release evidence missing client semantic token on a "
                    "profile-scoped tool marker before the matching profile pass marker for "
                    f"external client matrix {profile}: {semantic}"
                )
        if profile not in security_detail_required_profiles:
            continue
        context, context_failures = validate_client_security_context(
            environment,
            profile,
            expected_tools,
            expected_semantics,
        )
        if context_failures:
            continue
        if any(
            block.get("security_detail_malformed", False)
            for block in matching_blocks
        ):
            failures.append(
                "release evidence client security detail marker for external "
                f"client matrix {profile} must use the "
                "profile/tool/protocol/mechanism/result line shape"
            )
        for tool in sorted(expected_tools):
            expected_detail = client_security_detail_expected(
                environment,
                profile,
                tool,
                context,
                expected_semantics,
            )
            if any(
                sum(
                    1
                    for detail in block.get("security_details", [])
                    if detail.get("tool") == tool
                ) > 1
                for block in matching_blocks
            ):
                failures.append(
                    "release evidence client security detail marker must not "
                    "repeat before the matching profile pass marker for "
                    f"external client matrix {profile}: {tool}"
                )
            if not any(
                any(
                    client_security_detail_valid(detail, expected_detail)
                    for detail in block.get("security_details", [])
                )
                for block in matching_blocks
            ):
                failures.append(
                    "release evidence missing same-block client security "
                    "detail marker before the matching profile pass marker "
                    f"for external client matrix {profile}: "
                    + client_security_detail_marker_text(profile, expected_detail)
                )
    return failures


def client_matrix_summary_profiles(output):
    prefix = "ok: client matrix passed for "
    suffix = " profile(s) source=command"
    for line in output_lines(output):
        if line.startswith(prefix) and line.endswith(suffix):
            return output_csv_values(
                line[len(prefix) : -len(suffix)],
                "client matrix summary profiles",
            )
    return None, []


def validate_client_matrix_summary_output(output, environment):
    selected_profiles = split_csv(environment.get("ZMQ_CLIENT_MATRIX_PROFILES"))
    if not selected_profiles:
        return []
    failures = []
    summary_prefix = "ok: client matrix passed for "
    summary_suffix = " profile(s) source=command"
    failures.extend(
        suffixed_summary_output_line_failures(
            output,
            summary_prefix,
            summary_suffix,
            "client matrix summary",
        )
    )
    summary_lines = output_summary_candidate_lines(
        output,
        summary_prefix,
        summary_suffix,
    )
    summary_profiles, summary_failures = client_matrix_summary_profiles(output)
    if summary_profiles is None:
        failures.append(
            "release evidence missing client matrix summary output marker: "
            "ok: client matrix passed for <profiles> profile(s) source=command"
        )
        return failures
    failures.extend(summary_failures)
    if summary_profiles != selected_profiles:
        failures.append(
            "release evidence client matrix summary must list selected "
            "profiles from ZMQ_CLIENT_MATRIX_PROFILES: expected "
            + ", ".join(selected_profiles)
            + "; got "
            + ", ".join(summary_profiles)
        )
    return failures


def split_summary_field_tokens(payload):
    tokens = []
    current = []
    bracket_depth = 0
    for char in payload:
        if char == "[":
            bracket_depth += 1
        elif char == "]" and bracket_depth > 0:
            bracket_depth -= 1
        if char == "," and bracket_depth == 0:
            tokens.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    tokens.append("".join(current).strip())
    return tokens


def parse_summary_key_value_fields(payload):
    fields = {}
    duplicates = []
    for token in split_summary_field_tokens(payload):
        if not token or "=" not in token:
            return None, duplicates
        key, value = token.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (
            not key
            or not value
            or re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key) is None
        ):
            return None, duplicates
        if key in fields:
            if key not in duplicates:
                duplicates.append(key)
            continue
        fields[key] = value
    return fields, duplicates


def parenthesized_summary_payload(line):
    start = line.find("(")
    end = line.rfind(")")
    if start == -1 or end <= start:
        return None
    return line[start + 1 : end]


def append_phase_detail(details, duplicate_phases, phase, detail):
    if phase in details:
        duplicate_phases.add(phase)
    details.setdefault(phase, []).append(detail)


def s3_process_crash_summary_lines(output):
    return output_summary_candidate_lines(
        output,
        "ok: S3 process crash/replacement harness passed (",
        ") source=command",
    )


def s3_process_crash_summary_details(output):
    prefix = "ok: S3 process crash/replacement harness passed"
    source_suffix = ") source=command"
    detail_prefix = prefix + " ("
    lines = s3_process_crash_summary_lines(output)
    if not lines:
        return None
    line = lines[0]
    fields, duplicates = parse_summary_key_value_fields(
        line[len(detail_prefix) : -len(source_suffix)]
    )
    if fields is None:
        return None
    fields["__duplicate_fields"] = duplicates
    return fields


def s3_process_crash_summary_int(details, name, failures):
    value = details.get(name)
    if value is None:
        failures.append(f"release evidence S3 process-crash summary missing {name}")
        return None
    try:
        return int(value, 10)
    except ValueError:
        failures.append(
            "release evidence S3 process-crash summary field "
            f"{name} must be an integer"
        )
        return None


def validate_s3_process_crash_summary_output(output):
    failures = []
    failures.extend(
        suffixed_summary_output_line_failures(
            output,
            "ok: S3 process crash/replacement harness passed",
            ") source=command",
            "S3 process-crash summary",
        )
    )
    summary_lines = s3_process_crash_summary_lines(output)
    if not summary_lines:
        failures.append(
            "release evidence missing detailed S3 process-crash summary output "
            "marker: ok: S3 process crash/replacement harness passed "
            "(bucket=<bucket>, topic=<topic>, group=<group>, "
            "killed_broker=true, fresh_data_dir=true, first_offset=0, "
            "committed_offset=1, replacement_offset=<offset>, recovered_payloads=2) "
            "source=command"
        )
        return failures

    details = s3_process_crash_summary_details(output)
    if details is None:
        failures.append(
            "release evidence S3 process-crash summary must use "
            "comma-separated key=value fields"
        )
        return failures
    duplicate_fields = details.get("__duplicate_fields", [])
    if duplicate_fields:
        failures.append(
            "release evidence S3 process-crash summary must not repeat fields: "
            + ", ".join(duplicate_fields)
        )
    unknown_fields = sorted(
        set(details) - set(S3_PROCESS_CRASH_SUMMARY_FIELDS) - {"__duplicate_fields"}
    )
    if unknown_fields:
        failures.append(
            "release evidence S3 process-crash summary must not include "
            "unknown fields: " + ", ".join(unknown_fields)
        )
    for name in ("bucket", "topic", "group"):
        value = details.get(name)
        if value is None or placeholder_env_value(value):
            failures.append(
                "release evidence S3 process-crash summary must include "
                f"non-placeholder {name}"
            )
    for name in ("killed_broker", "fresh_data_dir"):
        if details.get(name) != "true":
            failures.append(
                "release evidence S3 process-crash summary must report "
                f"{name}=true"
            )

    first_offset = s3_process_crash_summary_int(details, "first_offset", failures)
    committed_offset = s3_process_crash_summary_int(
        details,
        "committed_offset",
        failures,
    )
    replacement_offset = s3_process_crash_summary_int(
        details,
        "replacement_offset",
        failures,
    )
    recovered_payloads = s3_process_crash_summary_int(
        details,
        "recovered_payloads",
        failures,
    )

    if first_offset is not None and first_offset != 0:
        failures.append(
            "release evidence S3 process-crash summary must report first_offset=0"
        )
    if committed_offset is not None and committed_offset != 1:
        failures.append(
            "release evidence S3 process-crash summary must report committed_offset=1"
        )
    if (
        first_offset is not None
        and replacement_offset is not None
        and replacement_offset <= first_offset
    ):
        failures.append(
            "release evidence S3 process-crash summary must report "
            "replacement_offset greater than first_offset"
        )
    if recovered_payloads is not None and recovered_payloads != 2:
        failures.append(
            "release evidence S3 process-crash summary must report recovered_payloads=2"
        )
    return failures


def s3_provider_matrix_lines_before_summary(output):
    lines = []
    for line in output_lines(output):
        if (
            line.startswith("ok: S3 provider matrix passed for ")
            and line.endswith(" source=command")
        ):
            return lines
        lines.append(line)
    return []


def s3_provider_matrix_summary_profiles(output):
    prefix = "ok: S3 provider matrix passed for "
    source_suffix = " source=command"
    for line in output_lines(output):
        if line.startswith(prefix) and line.endswith(source_suffix):
            return output_csv_values(
                line[len(prefix) : -len(source_suffix)],
                "S3 provider matrix summary profiles",
            )
    return None, []


S3_PROVIDER_SCOPED_MARKER_TEMPLATES = (
    (
        "live-suite",
        "ok: S3 provider live-suite profile {profile} "
        "command_started=true completed=true source=command",
    ),
    (
        "outage",
        "ok: S3 provider outage profile {profile} "
        "down=true healed=true fail_closed=true recovered=true source=command",
    ),
    (
        "process-crash",
        "ok: S3 provider process-crash profile {profile} "
        "killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
    ),
    (
        "list-pagination",
        "ok: S3 provider list-pagination profile {profile} "
        "required=true completed=true source=command",
    ),
    (
        "multipart-edge",
        "ok: S3 provider multipart-edge profile {profile} "
        "required=true completed=true source=command",
    ),
    (
        "multipart-fault",
        "ok: S3 provider multipart-fault profile {profile} "
        "command_started=true completed=true injected=true recovered=true source=command",
    ),
)


def parse_s3_provider_profile_output_line(line, profile):
    pattern = re.compile(
        r"^ok: S3 provider profile "
        + re.escape(profile)
        + r" endpoint=(\S+) bucket=(\S+) scheme=(\S+) region=(\S+) "
        + r"path_style=(true|false) source=(command)$"
    )
    marker_prefix = f"ok: S3 provider profile {profile}"
    match = pattern.match(line)
    if match:
        return (
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
            match.group(5),
        ), False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def parse_s3_provider_scoped_marker_line(line, profile):
    for marker, template in S3_PROVIDER_SCOPED_MARKER_TEMPLATES:
        if line == template.format(profile=profile):
            return marker
    return None


def parse_s3_provider_process_crash_detail_line(line, profile):
    pattern = re.compile(
        r"^ok: S3 provider process-crash detail profile "
        + re.escape(profile)
        + r" bucket=(\S+) topic=(\S+) group=(\S+) "
        + r"killed_broker=(true|false) fresh_data_dir=(true|false) "
        + r"first_offset=(-?\d+) committed_offset=(-?\d+) "
        + r"replacement_offset=(-?\d+) recovered_payloads=(-?\d+) "
        + r"source=(command)$"
    )
    marker_prefix = f"ok: S3 provider process-crash detail profile {profile}"
    match = pattern.match(line)
    if match:
        return {
            "bucket": match.group(1),
            "topic": match.group(2),
            "group": match.group(3),
            "killed_broker": match.group(4),
            "fresh_data_dir": match.group(5),
            "first_offset": match.group(6),
            "committed_offset": match.group(7),
            "replacement_offset": match.group(8),
            "recovered_payloads": match.group(9),
            "source": match.group(10),
        }, False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def parse_s3_provider_outage_detail_line(line, profile):
    pattern = re.compile(
        r"^ok: S3 provider outage detail profile "
        + re.escape(profile)
        + r" endpoint=(\S+) bucket=(\S+) scheme=(\S+) region=(\S+) "
        + r"path_style=(true|false) down=(true|false) healed=(true|false) "
        + r"fail_closed=(true|false) recovered=(true|false) source=(command)$"
    )
    marker_prefix = f"ok: S3 provider outage detail profile {profile}"
    match = pattern.match(line)
    if match:
        return {
            "endpoint": match.group(1),
            "bucket": match.group(2),
            "scheme": match.group(3),
            "region": match.group(4),
            "path_style": match.group(5),
            "down": match.group(6),
            "healed": match.group(7),
            "fail_closed": match.group(8),
            "recovered": match.group(9),
            "source": match.group(10),
        }, False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def s3_provider_outage_detail_valid(detail, expected):
    (
        expected_endpoint,
        expected_bucket,
        expected_scheme,
        expected_region,
        expected_path_style,
    ) = expected
    return (
        detail.get("endpoint") == expected_endpoint
        and detail.get("bucket") == expected_bucket
        and detail.get("scheme") == expected_scheme
        and detail.get("region") == expected_region
        and detail.get("path_style") == expected_path_style
        and detail.get("down") == "true"
        and detail.get("healed") == "true"
        and detail.get("fail_closed") == "true"
        and detail.get("recovered") == "true"
        and detail.get("source") == "command"
    )


def parse_s3_provider_multipart_fault_detail_line(line, profile):
    pattern = re.compile(
        r"^ok: S3 multipart fault profile "
        + re.escape(profile)
        + r" endpoint=(\S+) bucket=(\S+) scheme=(\S+) region=(\S+) "
        + r"path_style=(true|false) injected=(true|false) "
        + r"recovered=(true|false) source=(command)$"
    )
    marker_prefix = f"ok: S3 multipart fault profile {profile}"
    match = pattern.match(line)
    if match:
        return {
            "endpoint": match.group(1),
            "bucket": match.group(2),
            "scheme": match.group(3),
            "region": match.group(4),
            "path_style": match.group(5),
            "injected": match.group(6),
            "recovered": match.group(7),
            "source": match.group(8),
        }, False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def s3_provider_multipart_fault_detail_valid(detail, expected):
    (
        expected_endpoint,
        expected_bucket,
        expected_scheme,
        expected_region,
        expected_path_style,
    ) = expected
    return (
        detail.get("endpoint") == expected_endpoint
        and detail.get("bucket") == expected_bucket
        and detail.get("scheme") == expected_scheme
        and detail.get("region") == expected_region
        and detail.get("path_style") == expected_path_style
        and detail.get("injected") == "true"
        and detail.get("recovered") == "true"
        and detail.get("source") == "command"
    )


def s3_provider_process_crash_detail_int(detail, name):
    try:
        return int(detail[name], 10)
    except (KeyError, ValueError):
        return None


def s3_provider_process_crash_detail_valid(detail, expected_bucket):
    if detail.get("bucket") != expected_bucket:
        return False
    for name in ("topic", "group"):
        value = detail.get(name)
        if value is None or placeholder_env_value(value):
            return False
    for name in ("killed_broker", "fresh_data_dir"):
        if detail.get(name) != "true":
            return False
    first_offset = s3_provider_process_crash_detail_int(detail, "first_offset")
    committed_offset = s3_provider_process_crash_detail_int(detail, "committed_offset")
    replacement_offset = s3_provider_process_crash_detail_int(
        detail,
        "replacement_offset",
    )
    recovered_payloads = s3_provider_process_crash_detail_int(
        detail,
        "recovered_payloads",
    )
    return (
        first_offset == 0
        and committed_offset == 1
        and replacement_offset is not None
        and replacement_offset > first_offset
        and recovered_payloads == 2
        and detail.get("source") == "command"
    )


def s3_provider_profile_output_details(output, profile):
    details = []
    malformed = False
    for line in s3_provider_matrix_lines_before_summary(output):
        detail, line_malformed = parse_s3_provider_profile_output_line(line, profile)
        if detail:
            details.append(detail)
        elif line_malformed:
            malformed = True
    return details, malformed


def s3_provider_profile_output_blocks(output, environment):
    profile_names = split_csv(environment.get("ZMQ_S3_PROVIDER_PROFILES"))
    current_markers = {}
    current_marker_counts = {}
    current_outage_details = {}
    current_outage_detail_malformed = {}
    current_multipart_fault_details = {}
    current_multipart_fault_detail_malformed = {}
    current_process_crash_details = {}
    current_process_crash_detail_malformed = {}
    blocks = {}
    for line in s3_provider_matrix_lines_before_summary(output):
        for profile in profile_names:
            marker = parse_s3_provider_scoped_marker_line(line, profile)
            if marker is not None:
                current_markers.setdefault(profile, set()).add(marker)
                marker_counts = current_marker_counts.setdefault(profile, {})
                marker_counts[marker] = marker_counts.get(marker, 0) + 1
                break
            outage_detail, outage_detail_malformed = (
                parse_s3_provider_outage_detail_line(line, profile)
            )
            if outage_detail is not None:
                current_outage_details.setdefault(profile, []).append(outage_detail)
                break
            if outage_detail_malformed:
                current_outage_detail_malformed[profile] = True
                break
            multipart_fault_detail, multipart_fault_detail_malformed = (
                parse_s3_provider_multipart_fault_detail_line(line, profile)
            )
            if multipart_fault_detail is not None:
                current_multipart_fault_details.setdefault(profile, []).append(
                    multipart_fault_detail
                )
                break
            if multipart_fault_detail_malformed:
                current_multipart_fault_detail_malformed[profile] = True
                break
            process_crash_detail, process_crash_detail_malformed = (
                parse_s3_provider_process_crash_detail_line(line, profile)
            )
            if process_crash_detail is not None:
                current_process_crash_details.setdefault(profile, []).append(
                    process_crash_detail
                )
                break
            if process_crash_detail_malformed:
                current_process_crash_detail_malformed[profile] = True
                break
            detail, malformed = parse_s3_provider_profile_output_line(line, profile)
            if detail is not None:
                endpoint, bucket, scheme, region, path_style = detail
                blocks.setdefault(profile, []).append({
                    "endpoint": endpoint,
                    "bucket": bucket,
                    "scheme": scheme,
                    "region": region,
                    "path_style": path_style,
                    "markers": set(current_markers.get(profile, set())),
                    "marker_counts": dict(current_marker_counts.get(profile, {})),
                    "outage_details": list(
                        current_outage_details.get(profile, [])
                    ),
                    "outage_detail_malformed": (
                        current_outage_detail_malformed.get(profile, False)
                    ),
                    "multipart_fault_details": list(
                        current_multipart_fault_details.get(profile, [])
                    ),
                    "multipart_fault_detail_malformed": (
                        current_multipart_fault_detail_malformed.get(profile, False)
                    ),
                    "process_crash_details": list(
                        current_process_crash_details.get(profile, [])
                    ),
                    "process_crash_detail_malformed": (
                        current_process_crash_detail_malformed.get(profile, False)
                    ),
                })
                current_markers[profile] = set()
                current_marker_counts[profile] = {}
                current_outage_details[profile] = []
                current_outage_detail_malformed[profile] = False
                current_multipart_fault_details[profile] = []
                current_multipart_fault_detail_malformed[profile] = False
                current_process_crash_details[profile] = []
                current_process_crash_detail_malformed[profile] = False
                break
            if malformed:
                break
    return blocks


def s3_provider_bool_text(value, default):
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in BOOL_TRUE_VALUES:
        return "true"
    if lowered in BOOL_FALSE_VALUES:
        return "false"
    return default


def s3_provider_profile_expected_settings(environment, profile, failures):
    values = {}
    for suffix in ("ENDPOINT", "PORT", "BUCKET"):
        env_name, value = profile_setting_environment_value(
            environment,
            "ZMQ_S3",
            profile,
            suffix,
        )
        if value is None:
            failures.append(
                f"release evidence missing S3 provider profile setting {suffix} "
                f"for S3 provider matrix {profile}"
            )
            return None
        if not isinstance(value, str):
            failures.append(
                f"release evidence S3 provider profile setting {env_name} "
                f"for S3 provider matrix {profile} must be a string"
            )
            return None
        if placeholder_env_value(value):
            failures.append(
                f"release evidence S3 provider profile setting {env_name} "
                f"for S3 provider matrix {profile} uses placeholder value"
            )
            return None
        stripped = value.strip()
        if not stripped:
            failures.append(
                f"release evidence S3 provider profile setting {env_name} "
                f"for S3 provider matrix {profile} must not be blank"
            )
            return None
        values[suffix] = stripped

    try:
        port = int(values["PORT"], 10)
    except ValueError:
        failures.append(
            "release evidence S3 provider profile setting PORT for "
            f"S3 provider matrix {profile} must be an integer"
        )
        return None
    if port <= 0:
        failures.append(
            "release evidence S3 provider profile setting PORT for "
            f"S3 provider matrix {profile} must be positive"
        )
        return None

    settings = {
        "endpoint": f"{values['ENDPOINT']}:{port}",
        "bucket": values["BUCKET"],
    }
    for suffix, key, default in (
        ("SCHEME", "scheme", "http"),
        ("REGION", "region", "us-east-1"),
        ("PATH_STYLE", "path_style", "true"),
    ):
        env_name, value = profile_setting_environment_value(
            environment,
            "ZMQ_S3",
            profile,
            suffix,
        )
        if value is None:
            if profile != "minio":
                failures.append(
                    "release evidence missing explicit S3 provider profile "
                    f"setting {suffix} for non-minio S3 provider matrix {profile}"
                )
                return None
            settings[key] = default
            continue
        if not isinstance(value, str):
            failures.append(
                f"release evidence S3 provider profile setting {env_name} "
                f"for S3 provider matrix {profile} must be a string"
            )
            return None
        stripped = value.strip()
        if not stripped:
            failures.append(
                f"release evidence S3 provider profile setting {env_name} "
                f"for S3 provider matrix {profile} must not be blank"
            )
            return None
        if placeholder_env_value(stripped):
            failures.append(
                f"release evidence S3 provider profile setting {env_name} "
                f"for S3 provider matrix {profile} uses placeholder value"
            )
            return None
        if suffix == "SCHEME":
            scheme = stripped.lower()
            if scheme not in ("http", "https"):
                failures.append(
                    f"release evidence S3 provider profile setting {env_name} "
                    f"for S3 provider matrix {profile} must be http or https"
                )
                return None
            settings[key] = scheme
        elif suffix == "PATH_STYLE":
            path_style = s3_provider_bool_text(stripped, None)
            if path_style is None:
                failures.append(
                    f"release evidence S3 provider profile setting {env_name} "
                    f"for S3 provider matrix {profile} must be true or false"
                )
                return None
            settings[key] = path_style
        else:
            settings[key] = stripped

    return (
        settings["endpoint"],
        settings["bucket"],
        settings["scheme"],
        settings["region"],
        settings["path_style"],
    )


def validate_s3_provider_profile_output_markers(output, environment):
    failures = []
    for profile in split_csv(environment.get("ZMQ_S3_PROVIDER_PROFILES")):
        details, malformed = s3_provider_profile_output_details(output, profile)
        if not details:
            failures.append(
                "release evidence missing S3 provider profile output marker "
                "for S3 provider matrix: "
                f"ok: S3 provider profile {profile} "
                "endpoint=<endpoint>:<port> bucket=<bucket> "
                "scheme=<scheme> region=<region> "
                "path_style=<true|false> source=command"
            )
            continue
        if malformed:
            failures.append(
                "release evidence S3 provider profile output marker for "
                f"S3 provider matrix {profile} must use the "
                "endpoint/bucket/scheme/region/path_style/source line shape"
            )
        if len(details) > 1:
            failures.append(
                "release evidence S3 provider profile output marker for "
                f"S3 provider matrix {profile} must not repeat before the "
                "final S3 provider matrix summary"
            )
        if not any(
            ":" in endpoint
            and not placeholder_env_value(endpoint)
            and not placeholder_env_value(bucket)
            and not placeholder_env_value(scheme)
            and not placeholder_env_value(region)
            for endpoint, bucket, scheme, region, path_style in details
        ):
            failures.append(
                "release evidence S3 provider profile output marker for "
                f"S3 provider matrix {profile} must include non-placeholder "
                "endpoint=<endpoint>:<port>, bucket=<bucket>, "
                "scheme=<scheme>, and region=<region>"
            )
        expected = s3_provider_profile_expected_settings(
            environment,
            profile,
            failures,
        )
        if expected is not None and expected not in details:
            (
                expected_endpoint,
                expected_bucket,
                expected_scheme,
                expected_region,
                expected_path_style,
            ) = expected
            failures.append(
                "release evidence S3 provider profile output marker for "
                f"S3 provider matrix {profile} must match selected "
                f"endpoint={expected_endpoint} bucket={expected_bucket} "
                f"scheme={expected_scheme} region={expected_region} "
                f"path_style={expected_path_style}"
            )
    return failures


def s3_provider_block_marker_count(block, marker):
    return block.get("marker_counts", {}).get(marker, 0)


def validate_s3_provider_profile_scoped_markers(output, environment):
    failures = []
    blocks = s3_provider_profile_output_blocks(output, environment)
    for profile in split_csv(environment.get("ZMQ_S3_PROVIDER_PROFILES")):
        profile_blocks = blocks.get(profile, [])
        if not profile_blocks:
            continue
        expected = s3_provider_profile_expected_settings(
            environment,
            profile,
            [],
        )
        if expected is None:
            continue
        (
            expected_endpoint,
            expected_bucket,
            expected_scheme,
            expected_region,
            expected_path_style,
        ) = expected
        matching_blocks = [
            block
            for block in profile_blocks
            if (
                block["endpoint"] == expected_endpoint
                and block["bucket"] == expected_bucket
                and block["scheme"] == expected_scheme
                and block["region"] == expected_region
                and block["path_style"] == expected_path_style
            )
        ]
        if not matching_blocks:
            continue
        if not any("live-suite" in block["markers"] for block in matching_blocks):
            failures.append(
                "release evidence missing same-block S3 provider live-suite "
                "marker before the matching provider-settings profile output "
                "marker for "
                f"S3 provider matrix {profile}"
            )
        if any(
            s3_provider_block_marker_count(block, "live-suite") > 1
            for block in matching_blocks
        ):
            failures.append(
                "release evidence S3 provider live-suite marker must not "
                "repeat before the matching provider-settings profile output "
                f"marker for S3 provider matrix {profile}"
            )
    for env_name, marker in (
        ("ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES", "outage"),
        ("ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES", "process-crash"),
        ("ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES", "list-pagination"),
        ("ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES", "multipart-edge"),
        ("ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES", "multipart-fault"),
    ):
        for profile in split_csv(environment.get(env_name)):
            expected = s3_provider_profile_expected_settings(
                environment,
                profile,
                [],
            )
            if expected is None:
                continue
            (
                expected_endpoint,
                expected_bucket,
                expected_scheme,
                expected_region,
                expected_path_style,
            ) = expected
            matching_blocks = [
                block
                for block in blocks.get(profile, [])
                if (
                    block["endpoint"] == expected_endpoint
                    and block["bucket"] == expected_bucket
                    and block["scheme"] == expected_scheme
                    and block["region"] == expected_region
                    and block["path_style"] == expected_path_style
                )
            ]
            if not any(marker in block["markers"] for block in matching_blocks):
                failures.append(
                    "release evidence missing same-block S3 provider "
                    f"{marker} marker before the matching provider-settings "
                    f"profile output marker for S3 provider matrix {profile}"
                )
                continue
            if any(
                s3_provider_block_marker_count(block, marker) > 1
                for block in matching_blocks
            ):
                failures.append(
                    "release evidence S3 provider "
                    f"{marker} marker must not repeat before the matching "
                    "provider-settings profile output marker for "
                    f"S3 provider matrix {profile}"
                )
            if marker == "outage":
                outage_blocks = [
                    block
                    for block in matching_blocks
                    if "outage" in block["markers"]
                ]
                if any(
                    len(block.get("outage_details", [])) > 1
                    for block in outage_blocks
                ):
                    failures.append(
                        "release evidence S3 provider outage detail marker "
                        "must not repeat before the matching provider-settings "
                        f"profile output marker for S3 provider matrix {profile}"
                    )
                if any(
                    block.get("outage_detail_malformed", False)
                    for block in outage_blocks
                ):
                    failures.append(
                        "release evidence S3 provider outage detail marker "
                        f"for S3 provider matrix {profile} must use the "
                        "provider/bucket/scheme/region/path_style/result "
                        "source=command line shape"
                    )
                if not any(
                    any(
                        s3_provider_outage_detail_valid(detail, expected)
                        for detail in block.get("outage_details", [])
                    )
                    for block in outage_blocks
                ):
                    failures.append(
                        "release evidence missing same-block S3 provider "
                        "outage detail marker before the matching "
                        "provider-settings profile output marker for "
                        f"S3 provider matrix {profile}: "
                        "ok: S3 provider outage detail profile "
                        f"{profile} endpoint={expected_endpoint} "
                        f"bucket={expected_bucket} scheme={expected_scheme} "
                        f"region={expected_region} "
                        f"path_style={expected_path_style} down=true "
                        "healed=true fail_closed=true recovered=true "
                        "source=command"
                    )
            if marker == "process-crash":
                process_crash_blocks = [
                    block
                    for block in matching_blocks
                    if "process-crash" in block["markers"]
                ]
                if any(
                    len(block.get("process_crash_details", [])) > 1
                    for block in process_crash_blocks
                ):
                    failures.append(
                        "release evidence S3 provider process-crash detail "
                        "marker must not repeat before the matching "
                        "provider-settings profile output marker for "
                        f"S3 provider matrix {profile}"
                    )
                if any(
                    block.get("process_crash_detail_malformed", False)
                    for block in process_crash_blocks
                ):
                    failures.append(
                        "release evidence S3 provider process-crash detail "
                        f"marker for S3 provider matrix {profile} must use "
                        "the provider/bucket/topic/group/offset "
                        "source=command line shape"
                    )
                if not any(
                    any(
                        s3_provider_process_crash_detail_valid(
                            detail,
                            expected_bucket,
                        )
                        for detail in block.get("process_crash_details", [])
                    )
                    for block in process_crash_blocks
                ):
                    failures.append(
                        "release evidence missing same-block S3 provider "
                        "process-crash detail marker before the matching "
                        "provider-settings profile output marker for "
                        f"S3 provider matrix {profile}: "
                        "ok: S3 provider process-crash detail profile "
                        f"{profile} bucket={expected_bucket} topic=<topic> "
                        "group=<group> killed_broker=true "
                        "fresh_data_dir=true first_offset=0 "
                        "committed_offset=1 replacement_offset=<offset> "
                        "recovered_payloads=2 source=command"
                    )
            if marker == "multipart-fault":
                multipart_fault_blocks = [
                    block
                    for block in matching_blocks
                    if "multipart-fault" in block["markers"]
                ]
                if any(
                    len(block.get("multipart_fault_details", [])) > 1
                    for block in multipart_fault_blocks
                ):
                    failures.append(
                        "release evidence S3 provider multipart-fault detail "
                        "marker must not repeat before the matching "
                        "provider-settings profile output marker for "
                        f"S3 provider matrix {profile}"
                    )
                if any(
                    block.get("multipart_fault_detail_malformed", False)
                    for block in multipart_fault_blocks
                ):
                    failures.append(
                        "release evidence S3 provider multipart-fault detail "
                        f"marker for S3 provider matrix {profile} must use "
                        "the provider/bucket/scheme/region/path_style/result "
                        "source=command line shape"
                    )
                if not any(
                    any(
                        s3_provider_multipart_fault_detail_valid(
                            detail,
                            expected,
                        )
                        for detail in block.get("multipart_fault_details", [])
                    )
                    for block in multipart_fault_blocks
                ):
                    failures.append(
                        "release evidence missing same-block S3 provider "
                        "multipart-fault detail marker before the matching "
                        "provider-settings profile output marker for "
                        f"S3 provider matrix {profile}: "
                        "ok: S3 multipart fault profile "
                        f"{profile} endpoint={expected_endpoint} "
                        f"bucket={expected_bucket} scheme={expected_scheme} "
                        f"region={expected_region} "
                        f"path_style={expected_path_style} injected=true "
                        "recovered=true source=command"
                    )
    return failures


def validate_s3_provider_matrix_summary_output(output, environment):
    selected_profiles = split_csv(environment.get("ZMQ_S3_PROVIDER_PROFILES"))
    if not selected_profiles:
        return []
    failures = []
    summary_prefix = "ok: S3 provider matrix passed for "
    summary_suffix = " source=command"
    failures.extend(
        suffixed_summary_output_line_failures(
            output,
            summary_prefix,
            summary_suffix,
            "S3 provider matrix summary",
        )
    )
    summary_lines = output_summary_candidate_lines(
        output,
        summary_prefix,
        summary_suffix,
    )
    summary_profiles, summary_failures = s3_provider_matrix_summary_profiles(output)
    if summary_profiles is None:
        failures.append(
            "release evidence missing S3 provider matrix summary output marker: "
            "ok: S3 provider matrix passed for <profiles> source=command"
        )
        return failures
    failures.extend(summary_failures)
    if summary_profiles != selected_profiles:
        failures.append(
            "release evidence S3 provider matrix summary must list selected "
            "profiles from ZMQ_S3_PROVIDER_PROFILES: expected "
            + ", ".join(selected_profiles)
            + "; got "
            + ", ".join(summary_profiles)
        )
    return failures


def ordered_unique(values):
    seen = set()
    unique = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def e2e_chaos_summary_phases(environment):
    return ordered_unique(split_csv(environment.get("ZMQ_E2E_CHAOS_MATRIX")))


def e2e_load_scale_summary_phases(environment):
    matrix_phases = ordered_unique(
        split_csv(environment.get("ZMQ_E2E_LOAD_SCALE_MATRIX"))
    )
    if matrix_phases:
        return matrix_phases
    if bool_environment_value(environment, "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE") is True:
        return ordered_unique(
            split_csv(environment.get("ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"))
        )
    return []


def validate_e2e_command_provenance(command, environment, required):
    failures = []
    label = "Docker E2E gate"
    for env_name in (
        "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
        "ZMQ_E2E_CHAOS_MATRIX",
        "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
    ):
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                env_name,
                environment,
                label,
            )
        )

    load_scale_matrix = environment.get("ZMQ_E2E_LOAD_SCALE_MATRIX")
    if isinstance(load_scale_matrix, str) and load_scale_matrix.strip():
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                "ZMQ_E2E_LOAD_SCALE_MATRIX",
                environment,
                label,
            )
        )
    elif bool_environment_value(environment, "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE") is not True:
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                "ZMQ_E2E_LOAD_SCALE_MATRIX",
                environment,
                label,
            )
        )

    if bool_environment_value(environment, "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE") is True:
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
                environment,
                label,
            )
        )
    return failures


def validate_e2e_output_line_markers(output):
    failures = []
    for marker in E2E_EXACT_ONCE_OUTPUT_LINE_MARKERS:
        if len(e2e_output_marker_lines(output, marker)) > 1:
            failures.append(
                "release evidence Docker E2E output line marker must appear "
                f"exactly once: {marker}"
            )
    return failures


def validate_comparative_benchmark_command_provenance(command, environment, required):
    failures = []
    for env_name in ("ZMQ_BENCH_COMPARE_ENFORCE_GATES", *BENCHMARK_THRESHOLD_ENV_VARS):
        value = environment.get(env_name)
        if not isinstance(value, str) or not value.strip():
            continue
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                env_name,
                environment,
                "comparative benchmark gate",
            )
        )
    return failures


def validate_live_s3_benchmark_command_provenance(command, environment, required):
    failures = []
    for env_name in sorted(POSITIVE_INTEGER_ENV_VARS):
        value = environment.get(env_name)
        if not isinstance(value, str) or not value.strip():
            continue
        failures.extend(
            validate_command_assignment_matches_manifest(
                command,
                required,
                env_name,
                environment,
                "live-S3 benchmark gate",
            )
        )
    return failures


def e2e_load_scale_phase_hook_setting(environment, phase, suffix):
    token = coverage_env_token(phase, "collapsed")
    return first_present_environment_value(
        environment,
        (
            f"ZMQ_E2E_LOAD_SCALE_{token}_{suffix}",
            f"ZMQ_E2E_LOAD_SCALE_{suffix}",
        ),
    )


def e2e_load_scale_expected_phase_source(environment, phase, suffix):
    _env_name, value = e2e_load_scale_phase_hook_setting(environment, phase, suffix)
    if value is not None:
        return "hook"
    if bool_environment_value(environment, "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE") is True:
        return "fixture"
    return "hook"


def e2e_load_scale_expected_fixture_action(environment, phase, failures):
    token = coverage_env_token(phase, "collapsed")
    env_name, value = first_present_environment_value(
        environment,
        (
            f"ZMQ_E2E_LOAD_SCALE_{token}_FIXTURE_ACTION",
            "ZMQ_E2E_LOAD_SCALE_FIXTURE_ACTION",
        ),
    )
    if value is None:
        env_name = f"default fixture action for E2E load/scale phase {phase}"
        value = phase
    if not isinstance(value, str):
        failures.append(f"release evidence {env_name} must be a string")
        return None
    stripped = value.strip()
    if not stripped:
        failures.append(f"release evidence {env_name} must not be blank")
        return None
    if placeholder_env_value(stripped):
        failures.append(
            f"release evidence {env_name} uses placeholder value"
        )
        return None
    action = stripped.lower()
    if action not in E2E_LOAD_SCALE_FIXTURE_ACTIONS:
        failures.append(
            f"release evidence {env_name} must be one of "
            + ", ".join(sorted(E2E_LOAD_SCALE_FIXTURE_ACTIONS))
        )
        return None
    return action


def e2e_load_scale_expected_fixture_load_records(environment, phase, failures):
    token = coverage_env_token(phase, "collapsed")
    env_name, value = first_present_environment_value(
        environment,
        (
            f"ZMQ_E2E_LOAD_SCALE_{token}_FIXTURE_LOAD_RECORDS",
            "ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS",
        ),
    )
    if value is None:
        return 30
    if not isinstance(value, str):
        failures.append(f"release evidence {env_name} must be an integer string")
        return None
    stripped = value.strip()
    if not stripped:
        failures.append(f"release evidence {env_name} must not be blank")
        return None
    if placeholder_env_value(stripped):
        failures.append(f"release evidence {env_name} uses placeholder value")
        return None
    try:
        records = int(stripped)
    except ValueError:
        failures.append(f"release evidence {env_name} must be an integer")
        return None
    if records <= 0:
        failures.append(f"release evidence {env_name} must be a positive integer")
        return None
    return records


def e2e_phase_summary_output_phases(output, label):
    prefix = f"ok: E2E {label} passed for "
    suffix = " phase(s) source=command"
    for line in output_lines(output):
        if line.startswith(prefix) and line.endswith(suffix):
            return output_csv_values(
                line[len(prefix) : -len(suffix)],
                f"Docker E2E {label} summary phases",
            )
    return None, []


def validate_e2e_phase_summary_output(output, environment):
    failures = []
    for label, expected, env_name in (
        ("chaos", e2e_chaos_summary_phases(environment), "ZMQ_E2E_CHAOS_MATRIX"),
        (
            "load/scale",
            e2e_load_scale_summary_phases(environment),
            "ZMQ_E2E_LOAD_SCALE_MATRIX",
        ),
    ):
        if not expected:
            continue
        summary_prefix = f"ok: E2E {label} passed for "
        summary_suffix = " phase(s) source=command"
        failures.extend(
            suffixed_summary_output_line_failures(
                output,
                summary_prefix,
                summary_suffix,
                f"Docker E2E {label} summary",
            )
        )
        summary_lines = output_summary_candidate_lines(
            output,
            summary_prefix,
            summary_suffix,
        )
        observed, summary_failures = e2e_phase_summary_output_phases(output, label)
        if observed is None:
            failures.append(
                "release evidence missing Docker E2E "
                f"{label} summary output marker: "
                f"ok: E2E {label} passed for <phases> phase(s) source=command"
            )
        elif summary_failures:
            failures.extend(summary_failures)
        elif observed != expected:
            if (
                label == "load/scale"
                and bool_environment_value(
                    environment,
                    "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
                )
                is True
            ):
                env_name = (
                    "ZMQ_E2E_LOAD_SCALE_MATRIX or fixture-inferred "
                    "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"
                )
            failures.append(
                f"release evidence Docker E2E {label} summary must list "
                f"selected phases from {env_name}: expected "
                + ", ".join(expected)
                + "; got "
                + ", ".join(observed)
            )
    return failures


def e2e_chaos_phase_expected_result(environment, phase, failures):
    token = coverage_env_token(phase, "collapsed")
    env_name, value = first_present_environment_value(
        environment,
        (
            f"ZMQ_E2E_CHAOS_{token}_EXPECT",
            "ZMQ_E2E_CHAOS_EXPECT",
        ),
    )
    if value is None:
        return "fail"
    stripped = value.strip()
    if not stripped:
        failures.append(
            f"release evidence E2E chaos expectation {env_name} must not be blank"
        )
        return None
    if placeholder_env_value(stripped):
        failures.append(
            f"release evidence E2E chaos expectation {env_name} uses placeholder value"
        )
        return None
    if stripped not in ("fail", "survive"):
        failures.append(
            f"release evidence E2E chaos expectation {env_name} must be fail or survive"
        )
        return None
    return stripped


def e2e_chaos_phase_expected_observed(expectation):
    return "survived" if expectation == "survive" else "failed"


def validate_e2e_chaos_phase_detail_output(output, environment):
    expected = e2e_chaos_summary_phases(environment)
    if not expected:
        return []

    phase_lines = []
    for line in output_lines(output):
        if line.startswith("ok: E2E chaos passed for "):
            break
        phase_lines.append(line)

    details = {}
    malformed = set()
    duplicate_phases = set()
    pattern = re.compile(
        r"^ok: E2E chaos phase (\S+) down=true "
        r"observed=(failed|survived) healed=true recovered=true "
        r"expect=(fail|survive) source=command$"
    )
    prefix = "ok: E2E chaos phase "
    for line in phase_lines:
        match = pattern.match(line)
        if match:
            append_phase_detail(
                details,
                duplicate_phases,
                match.group(1),
                {
                    "observed": match.group(2),
                    "expect": match.group(3),
                },
            )
        elif line.startswith(prefix):
            phase = line[len(prefix) :].split()[0] if line[len(prefix) :] else ""
            if phase:
                malformed.add(phase)

    failures = []
    for phase in expected:
        phase_expected = e2e_chaos_phase_expected_result(environment, phase, failures)
        if phase_expected is None:
            continue
        expected_observed = e2e_chaos_phase_expected_observed(phase_expected)
        if phase in duplicate_phases:
            failures.append(
                "release evidence Docker E2E chaos phase detail marker must "
                f"not repeat phase {phase}"
            )
            continue
        observed_values = details.get(phase, [])
        detail = observed_values[0] if observed_values else None
        if (
            detail is not None
            and detail["expect"] == phase_expected
            and detail["observed"] == expected_observed
        ):
            continue
        if phase in malformed:
            failures.append(
                "release evidence Docker E2E chaos phase marker must include "
                f"down=true observed={expected_observed} healed=true "
                f"recovered=true expect={phase_expected} source=command "
                f"for phase {phase}"
            )
        elif detail is not None:
            if detail["expect"] != phase_expected:
                failures.append(
                    "release evidence Docker E2E chaos phase marker for phase "
                    f"{phase} must report expect={phase_expected}; "
                    f"got {detail['expect']}"
                )
            if detail["observed"] != expected_observed:
                failures.append(
                    "release evidence Docker E2E chaos phase marker for phase "
                    f"{phase} must report observed={expected_observed}; "
                    f"got {detail['observed']}"
                )
        else:
            failures.append(
                "release evidence missing Docker E2E chaos phase detail marker "
                "before the E2E chaos summary line: "
                f"ok: E2E chaos phase {phase} down=true "
                f"observed={expected_observed} healed=true recovered=true "
                f"expect={phase_expected} source=command"
            )
    return failures


def validate_e2e_load_scale_phase_detail_output(output, environment):
    expected = e2e_load_scale_summary_phases(environment)
    if not expected:
        return []

    phase_lines = []
    for line in output_lines(output):
        if line.startswith("ok: E2E load/scale passed for "):
            break
        phase_lines.append(line)

    details = {}
    malformed = set()
    duplicate_phases = set()
    pattern = re.compile(
        r"^ok: E2E load/scale phase (\S+) applied=true restored=true "
        r"marker_payloads=hook-owned apply_source=(hook|fixture) "
        r"restore_source=(hook|fixture) source=command"
        r"(?: action=(scale-in|scale-out|load|probe|noop)"
        r"(?: load_records=([1-9][0-9]*))?)?$"
    )
    prefix = "ok: E2E load/scale phase "
    for line in phase_lines:
        match = pattern.match(line)
        if match:
            append_phase_detail(
                details,
                duplicate_phases,
                match.group(1),
                {
                    "apply_source": match.group(2),
                    "restore_source": match.group(3),
                    "action": match.group(4),
                    "load_records": int(match.group(5)) if match.group(5) else None,
                },
            )
        elif line.startswith(prefix):
            phase = line[len(prefix) :].split()[0] if line[len(prefix) :] else ""
            if phase:
                malformed.add(phase)

    failures = []
    for phase in expected:
        expected_apply_source = e2e_load_scale_expected_phase_source(
            environment,
            phase,
            "APPLY",
        )
        expected_restore_source = e2e_load_scale_expected_phase_source(
            environment,
            phase,
            "RESTORE",
        )
        expected_action = None
        if "fixture" in (expected_apply_source, expected_restore_source):
            expected_action = e2e_load_scale_expected_fixture_action(
                environment,
                phase,
                failures,
            )
        observed = details.get(phase)
        if phase in duplicate_phases:
            failures.append(
                "release evidence Docker E2E load/scale phase detail marker "
                f"must not repeat phase {phase}"
            )
            continue
        observed = observed[0] if observed else None
        if observed is not None:
            if (
                observed["apply_source"] != expected_apply_source
                or observed["restore_source"] != expected_restore_source
            ):
                failures.append(
                    "release evidence Docker E2E load/scale phase marker for "
                    f"phase {phase} must report apply_source={expected_apply_source} "
                    f"restore_source={expected_restore_source}; got "
                    f"apply_source={observed['apply_source']} "
                    f"restore_source={observed['restore_source']}"
                )
            if expected_action is not None and observed["action"] != expected_action:
                failures.append(
                    "release evidence Docker E2E load/scale phase marker for "
                    f"phase {phase} must report fixture action={expected_action}"
                )
            if expected_action == "load":
                expected_records = e2e_load_scale_expected_fixture_load_records(
                    environment,
                    phase,
                    failures,
                )
                if (
                    expected_records is not None
                    and observed["load_records"] != expected_records
                ):
                    failures.append(
                        "release evidence Docker E2E load/scale phase marker for "
                        f"phase {phase} must report load_records={expected_records}"
                    )
            elif observed["load_records"] is not None:
                failures.append(
                    "release evidence Docker E2E load/scale phase marker for "
                    f"phase {phase} must only report load_records for fixture "
                    "action=load"
                )
            if expected_action is None and observed["action"] is not None:
                failures.append(
                    "release evidence Docker E2E load/scale phase marker for "
                    f"phase {phase} must not report fixture action when hooks "
                    "provided both apply and restore markers"
                )
            continue
        if phase in malformed:
            failures.append(
                "release evidence Docker E2E load/scale phase marker must "
                "include applied=true restored=true marker_payloads=hook-owned "
                f"apply_source={expected_apply_source} "
                f"restore_source={expected_restore_source} source=command "
                f"for phase {phase}"
            )
        else:
            failures.append(
                "release evidence missing Docker E2E load/scale phase detail "
                "marker before the E2E load/scale summary line: "
                f"ok: E2E load/scale phase {phase} "
                "applied=true restored=true marker_payloads=hook-owned "
                f"apply_source={expected_apply_source} "
                f"restore_source={expected_restore_source} source=command"
            )
    return failures


def e2e_lines_after_phase_summaries(output, environment):
    lines = output_lines(output)
    summary_indexes = []
    for label, expected in (
        ("chaos", e2e_chaos_summary_phases(environment)),
        ("load/scale", e2e_load_scale_summary_phases(environment)),
    ):
        if not expected:
            continue
        summary = (
            f"ok: E2E {label} passed for {', '.join(expected)} "
            "phase(s) source=command"
        )
        try:
            summary_indexes.append(lines.index(summary))
        except ValueError:
            return []
    if not summary_indexes:
        return lines
    return lines[max(summary_indexes) + 1 :]


def e2e_final_result_lines(output, environment):
    return [
        line
        for line in e2e_lines_after_phase_summaries(output, environment)
        if line == "Results:" or line.startswith("Results: ")
    ]


def e2e_final_results(output, environment):
    for line in e2e_final_result_lines(output, environment):
        match = re.fullmatch(r"Results: ([0-9]+)/([0-9]+) passed, ([0-9]+) failed", line)
        if match:
            return tuple(int(group) for group in match.groups())
    return None


def validate_e2e_final_results_output(output, environment):
    failures = []
    result_lines = e2e_final_result_lines(output, environment)
    if len(result_lines) > 1:
        failures.append(
            "release evidence Docker E2E final results line must appear "
            "exactly once after required E2E phase summaries"
        )
    results = e2e_final_results(output, environment)
    if results is None:
        failures.append(
            "release evidence missing Docker E2E final results line after "
            "required E2E phase summaries: "
            "Results: <passed>/<total> passed, 0 failed"
        )
        return failures
    passed, total, failed = results
    if total <= 0 or passed != total or failed != 0:
        failures.append(
            "release evidence Docker E2E final results must report all "
            f"tests passed and 0 failed: got {passed}/{total} passed, "
            f"{failed} failed"
        )
    return failures


def local_benchmark_s3_request_volumes(output):
    pattern = re.compile(
        r"^S3 WAL request volume\s+puts=([0-9]+)\s+lists=([0-9]+)"
        r"\s+requests/MiB=([0-9]+(?:\.[0-9]+)?)$"
    )
    volumes = []
    for line in benchmark_lines_before_completion(output):
        match = pattern.match(line)
        if match:
            volumes.append((
                int(match.group(1)),
                int(match.group(2)),
                float(match.group(3)),
            ))
    return volumes


def local_benchmark_s3_request_volume(output):
    volumes = local_benchmark_s3_request_volumes(output)
    return volumes[0] if volumes else None


def local_benchmark_memory_summaries(output):
    pattern = re.compile(
        r"^PartitionStore memory\s+([0-9]+(?:\.[0-9]+)?)/s"
        r"\s+retained=([0-9]+) KiB\s+peak=([0-9]+) KiB"
        r"\s+max_current=([0-9]+) KiB$"
    )
    summaries = []
    for line in benchmark_lines_before_completion(output):
        match = pattern.match(line)
        if match:
            summaries.append((
                float(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
                int(match.group(4)),
            ))
    return summaries


def local_benchmark_memory_summary(output):
    summaries = local_benchmark_memory_summaries(output)
    return summaries[0] if summaries else None


def validate_local_benchmark_summary_output(output):
    failures = []
    failures.extend(
        exact_summary_output_line_failures(
            output,
            "=== Benchmarks complete ===",
            "local benchmark completion",
        )
    )
    failures.extend(
        exact_summary_output_line_failures(
            output,
            "ok: local benchmark gate source=command",
            "local benchmark summary",
        )
    )
    request_volumes = local_benchmark_s3_request_volumes(output)
    request_volume = request_volumes[0] if request_volumes else None
    if len(request_volumes) > 1:
        failures.append(
            "release evidence local benchmark S3 WAL request-volume marker "
            "must appear exactly once before the benchmark completion marker"
        )
    if request_volume is None:
        failures.append(
            "release evidence missing detailed local benchmark S3 WAL "
            "request-volume marker before the benchmark completion marker: "
            "S3 WAL request volume puts=<puts> lists=<lists> "
            "requests/MiB=<value>"
        )
    else:
        puts, lists, requests_per_mib = request_volume
        if puts <= 0 or lists < 0 or requests_per_mib <= 0 or not math.isfinite(requests_per_mib):
            failures.append(
                "release evidence local benchmark S3 WAL request-volume "
                "marker must include positive finite puts and requests/MiB"
            )

    memory_summaries = local_benchmark_memory_summaries(output)
    memory_summary = memory_summaries[0] if memory_summaries else None
    if len(memory_summaries) > 1:
        failures.append(
            "release evidence local benchmark memory marker must appear "
            "exactly once before the benchmark completion marker"
        )
    if memory_summary is None:
        failures.append(
            "release evidence missing detailed local benchmark memory marker: "
            "PartitionStore memory <rate>/s retained=<retained> KiB "
            "peak=<peak> KiB max_current=<max_current> KiB before "
            "the benchmark completion marker"
        )
    else:
        rate, retained, peak, max_current = memory_summary
        if (
            rate <= 0
            or not math.isfinite(rate)
            or retained < 0
            or peak < retained
            or max_current < retained
        ):
            failures.append(
                "release evidence local benchmark memory marker must include "
                "positive finite rate and coherent retained/peak/max_current values"
            )
    return failures


def validate_live_s3_benchmark_summary_output(output):
    failures = []
    failures.extend(
        exact_summary_output_line_failures(
            output,
            "=== Benchmarks complete ===",
            "live-S3 benchmark completion",
        )
    )
    failures.extend(
        exact_summary_output_line_failures(
            output,
            "ok: live-S3 benchmark gate source=command",
            "live-S3 benchmark summary",
        )
    )
    return failures


def live_s3_benchmark_request_volumes(output):
    pattern = re.compile(
        r"^Live S3 request volume\s+puts=([0-9]+)\s+gets=([0-9]+)"
        r"\s+requests/MiB=([0-9]+(?:\.[0-9]+)?)$"
    )
    volumes = []
    for line in benchmark_lines_before_completion(output):
        match = pattern.match(line)
        if match:
            volumes.append((
                int(match.group(1)),
                int(match.group(2)),
                float(match.group(3)),
            ))
    return volumes


def live_s3_benchmark_request_volume(output):
    volumes = live_s3_benchmark_request_volumes(output)
    return volumes[0] if volumes else None


def live_s3_benchmark_put_summaries(output):
    pattern = re.compile(
        r"^Live S3 put\s+([0-9]+(?:\.[0-9]+)?) MiB/s"
        r"\s+p99=\s*([0-9]+(?:\.[0-9]+)?) ms\s+objects=([0-9]+)$"
    )
    summaries = []
    for line in benchmark_lines_before_completion(output):
        match = pattern.match(line)
        if match:
            summaries.append((
                float(match.group(1)),
                float(match.group(2)),
                int(match.group(3)),
            ))
    return summaries


def live_s3_benchmark_put_summary(output):
    summaries = live_s3_benchmark_put_summaries(output)
    return summaries[0] if summaries else None


def live_s3_benchmark_get_summaries(output):
    pattern = re.compile(
        r"^Live S3 get\s+([0-9]+(?:\.[0-9]+)?) MiB/s"
        r"\s+p99=\s*([0-9]+(?:\.[0-9]+)?) ms"
        r"\s+requests/MiB=([0-9]+(?:\.[0-9]+)?)$"
    )
    summaries = []
    for line in benchmark_lines_before_completion(output):
        match = pattern.match(line)
        if match:
            summaries.append((
                float(match.group(1)),
                float(match.group(2)),
                float(match.group(3)),
            ))
    return summaries


def live_s3_benchmark_get_summary(output):
    summaries = live_s3_benchmark_get_summaries(output)
    return summaries[0] if summaries else None


def parse_live_s3_benchmark_provider_line(line):
    pattern = re.compile(
        r"^Live S3 provider endpoint=(\S+) bucket=(\S+) "
        r"scheme=(\S+) region=(\S+) path_style=(true|false)$"
    )
    marker_prefix = "Live S3 provider"
    match = pattern.match(line)
    if match:
        return (
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
            match.group(5),
        ), False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def live_s3_benchmark_provider_details(output):
    details = []
    malformed = False
    for line in benchmark_lines_before_completion(output):
        detail, line_malformed = parse_live_s3_benchmark_provider_line(line)
        if detail:
            details.append(detail)
        elif line_malformed:
            malformed = True
    return details, malformed


def live_s3_benchmark_setting_value(environment, command, required, suffix):
    env_name = f"ZMQ_S3_{suffix}"
    value = command_env_assignment_for_requirement(command or "", required, env_name)
    if value:
        return env_name, value
    if isinstance(environment, dict):
        env_value = environment.get(env_name)
        if isinstance(env_value, str) and env_value.strip():
            return env_name, env_value
    return env_name, None


def live_s3_benchmark_scheme_text(value, default):
    if value is None:
        return default
    lowered = str(value).strip().lower()
    if lowered in ("http", "https"):
        return lowered
    return default


def live_s3_benchmark_expected_settings(
    environment,
    command,
    required,
    failures,
):
    values = {}
    for suffix in ("ENDPOINT", "PORT", "BUCKET"):
        env_name, value = live_s3_benchmark_setting_value(
            environment,
            command,
            required,
            suffix,
        )
        if value is None:
            failures.append(
                f"release evidence missing live-S3 benchmark setting {suffix}"
            )
            return None
        if placeholder_env_value(value):
            failures.append(
                f"release evidence live-S3 benchmark setting {env_name} "
                "uses placeholder value"
            )
            return None
        values[suffix] = value.strip()

    try:
        port = int(values["PORT"], 10)
    except ValueError:
        failures.append(
            "release evidence live-S3 benchmark setting PORT must be an integer"
        )
        return None
    if port <= 0:
        failures.append(
            "release evidence live-S3 benchmark setting PORT must be positive"
        )
        return None

    settings = {
        "endpoint": f"{values['ENDPOINT']}:{port}",
        "bucket": values["BUCKET"],
    }
    for suffix, key, default in (
        ("SCHEME", "scheme", "http"),
        ("REGION", "region", "us-east-1"),
        ("PATH_STYLE", "path_style", "true"),
    ):
        env_name, value = live_s3_benchmark_setting_value(
            environment,
            command,
            required,
            suffix,
        )
        if value is None:
            settings[key] = default
            continue
        stripped = value.strip()
        if placeholder_env_value(stripped):
            failures.append(
                f"release evidence live-S3 benchmark setting {env_name} "
                "uses placeholder value"
            )
            return None
        if suffix == "SCHEME":
            scheme = live_s3_benchmark_scheme_text(stripped, None)
            if scheme is None:
                failures.append(
                    f"release evidence live-S3 benchmark setting {env_name} "
                    "must be http or https"
                )
                return None
            settings[key] = scheme
        elif suffix == "PATH_STYLE":
            path_style = s3_provider_bool_text(stripped, None)
            if path_style is None:
                failures.append(
                    f"release evidence live-S3 benchmark setting {env_name} "
                    "must be true or false"
                )
                return None
            settings[key] = path_style
        else:
            settings[key] = stripped

    return (
        settings["endpoint"],
        settings["bucket"],
        settings["scheme"],
        settings["region"],
        settings["path_style"],
    )


def validate_live_s3_benchmark_provider_output(
    output,
    environment,
    command,
    required,
):
    failures = []
    details, malformed = live_s3_benchmark_provider_details(output)
    if not details:
        failures.append(
            "release evidence missing detailed live-S3 benchmark provider marker: "
            "Live S3 provider endpoint=<endpoint>:<port> bucket=<bucket> "
            "scheme=<scheme> region=<region> path_style=<true|false> "
            "before the benchmark completion marker"
        )
        return failures
    if malformed:
        failures.append(
            "release evidence live-S3 benchmark provider marker must use the "
            "endpoint/bucket/scheme/region/path_style line shape"
        )
    if len(details) > 1:
        failures.append(
            "release evidence live-S3 benchmark provider marker must appear "
            "exactly once before the benchmark completion marker"
        )
    if not any(
        ":" in endpoint
        and not placeholder_env_value(endpoint)
        and not placeholder_env_value(bucket)
        and not placeholder_env_value(scheme)
        and not placeholder_env_value(region)
        for endpoint, bucket, scheme, region, path_style in details
    ):
        failures.append(
            "release evidence live-S3 benchmark provider marker must include "
            "non-placeholder endpoint=<endpoint>:<port>, bucket=<bucket>, "
            "scheme=<scheme>, and region=<region>"
        )

    expected = live_s3_benchmark_expected_settings(
        environment,
        command,
        required,
        failures,
    )
    if expected is not None and expected not in details:
        (
            expected_endpoint,
            expected_bucket,
            expected_scheme,
            expected_region,
            expected_path_style,
        ) = expected
        failures.append(
            "release evidence live-S3 benchmark provider marker must match "
            f"selected endpoint={expected_endpoint} bucket={expected_bucket} "
            f"scheme={expected_scheme} region={expected_region} "
            f"path_style={expected_path_style}"
        )
    return failures


def validate_live_s3_benchmark_operation_summary_output(output):
    failures = []
    put_summaries = live_s3_benchmark_put_summaries(output)
    put_summary = put_summaries[0] if put_summaries else None
    if len(put_summaries) > 1:
        failures.append(
            "release evidence live-S3 benchmark put marker must appear "
            "exactly once before the benchmark completion marker"
        )
    if put_summary is None:
        failures.append(
            "release evidence missing detailed live-S3 benchmark put marker: "
            "Live S3 put <MiB/s> MiB/s p99=<ms> ms objects=<objects> "
            "before the benchmark completion marker"
        )
    else:
        put_mib_per_sec, put_p99_ms, objects = put_summary
        if (
            put_mib_per_sec <= 0
            or not math.isfinite(put_mib_per_sec)
            or put_p99_ms < 0
            or not math.isfinite(put_p99_ms)
            or objects <= 0
        ):
            failures.append(
                "release evidence live-S3 benchmark put marker must include "
                "positive finite throughput, finite p99, and positive object count"
            )

    get_summaries = live_s3_benchmark_get_summaries(output)
    get_summary = get_summaries[0] if get_summaries else None
    if len(get_summaries) > 1:
        failures.append(
            "release evidence live-S3 benchmark get marker must appear "
            "exactly once before the benchmark completion marker"
        )
    if get_summary is None:
        failures.append(
            "release evidence missing detailed live-S3 benchmark get marker: "
            "Live S3 get <MiB/s> MiB/s p99=<ms> ms requests/MiB=<value> "
            "before the benchmark completion marker"
        )
    else:
        get_mib_per_sec, get_p99_ms, requests_per_mib = get_summary
        if (
            get_mib_per_sec <= 0
            or not math.isfinite(get_mib_per_sec)
            or get_p99_ms < 0
            or not math.isfinite(get_p99_ms)
            or requests_per_mib <= 0
            or not math.isfinite(requests_per_mib)
        ):
            failures.append(
                "release evidence live-S3 benchmark get marker must include "
                "positive finite throughput, finite p99, and positive requests/MiB"
            )
    return failures


def validate_live_s3_benchmark_request_volume_output(output):
    volumes = live_s3_benchmark_request_volumes(output)
    volume = volumes[0] if volumes else None
    if len(volumes) > 1:
        return [
            "release evidence live-S3 benchmark request-volume marker must "
            "appear exactly once before the benchmark completion marker"
        ]
    if volume is None:
        return [
            "release evidence missing detailed live-S3 benchmark request-volume "
            "marker: Live S3 request volume puts=<puts> gets=<gets> "
            "requests/MiB=<value> before the benchmark completion marker"
        ]
    puts, gets, requests_per_mib = volume
    if puts <= 0 or gets <= 0 or requests_per_mib <= 0 or not math.isfinite(requests_per_mib):
        return [
            "release evidence live-S3 benchmark request-volume marker must "
            "include positive finite puts, gets, and requests/MiB values"
        ]
    return []


def comparative_benchmark_thresholds(environment):
    thresholds = dict(DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS)
    if not isinstance(environment, dict):
        return thresholds
    for env_name, threshold_name in COMPARATIVE_BENCHMARK_THRESHOLD_ENV:
        raw = environment.get(env_name)
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if math.isfinite(value) and value >= 0:
            thresholds[threshold_name] = value
    return thresholds


def comparative_benchmark_gate_section(output):
    lines = output_lines(output)
    gate_index = comparative_benchmark_gate_index(lines)
    if gate_index is None:
        return []
    section = []
    for line in lines[gate_index + 1 :]:
        if line.startswith("Results saved to "):
            return section
        section.append(line)
        if line.startswith("result:"):
            return section
    return section


def comparative_benchmark_full_gate_section(output):
    lines = output_lines(output)
    gate_index = comparative_benchmark_gate_index(lines)
    if gate_index is None:
        return []
    section = []
    for line in lines[gate_index + 1 :]:
        if line.startswith("Results saved to "):
            return section
        section.append(line)
    return section


def comparative_benchmark_results_artifact_indexes(lines):
    return [
        index
        for index, line in enumerate(lines)
        if line.startswith("Results saved to ")
    ]


def validate_comparative_benchmark_results_artifact_output(output):
    failures = []
    lines = output_lines(output)
    artifact_line = f"Results saved to {BENCHMARK_RESULTS_ARTIFACT}"
    artifact_indexes = comparative_benchmark_results_artifact_indexes(lines)
    if len(artifact_indexes) != 1:
        failures.append(
            "release evidence comparative benchmark results artifact line must "
            f"appear exactly once as {artifact_line!r}"
        )
        return failures

    artifact_index = artifact_indexes[0]
    if lines[artifact_index] != artifact_line:
        failures.append(
            "release evidence comparative benchmark results artifact line must "
            f"be {artifact_line!r}; got {lines[artifact_index]!r}"
        )

    gate_index = comparative_benchmark_gate_index(lines)
    previous_index = artifact_index - 1
    while previous_index >= 0 and lines[previous_index] == "":
        previous_index -= 1
    if (
        gate_index is None
        or artifact_index <= gate_index
        or previous_index <= gate_index
        or lines[previous_index] != "result: pass"
    ):
        failures.append(
            "release evidence comparative benchmark results artifact line must "
            "appear after the COMPARATIVE BENCHMARK GATE result: pass line"
        )
    return failures


def comparative_benchmark_gate_line(output, prefix):
    for line in comparative_benchmark_gate_section(output):
        if line.startswith(prefix):
            return line
    return None


def comparative_thresholds_line(thresholds):
    return (
        "thresholds: "
        f"throughput_ratio>={thresholds['min_throughput_ratio']:.2f}x, "
        f"p50_ratio<={thresholds['max_p50_latency_ratio']:.2f}x, "
        f"p99_ratio<={thresholds['max_p99_latency_ratio']:.2f}x, "
        f"error_rate<={thresholds['max_error_rate']:.2%}"
    )


def comparative_trend_thresholds_line(thresholds):
    return (
        "trend thresholds: "
        f"throughput_ratio>={thresholds['min_trend_throughput_ratio']:.2f}x, "
        f"p50_ratio<={thresholds['max_trend_p50_latency_ratio']:.2f}x, "
        f"p99_ratio<={thresholds['max_trend_p99_latency_ratio']:.2f}x"
    )


def comparative_trend_baseline_line(environment):
    if not isinstance(environment, dict):
        return None
    value = environment.get("ZMQ_BENCH_COMPARE_TREND_BASELINE")
    if not isinstance(value, str) or not value.strip():
        return None
    return f"trend baseline: {value.strip()}"


def comparative_profile_map_text(values):
    return ",".join(f"{key}:{values[key]}" for key in values)


def comparative_profile_marker_indexes(lines):
    return [
        index
        for index, line in enumerate(lines)
        if line == COMPARATIVE_PROFILE_MARKER_STEM
        or line.startswith(COMPARATIVE_PROFILE_MARKER_PREFIX)
    ]


def parse_comparative_profile_marker(line):
    failures = []
    if line == COMPARATIVE_PROFILE_MARKER_STEM:
        return {}, [
            "release evidence comparative benchmark profile marker must use "
            "key=value fields"
        ]
    if not line.startswith(COMPARATIVE_PROFILE_MARKER_PREFIX):
        return {}, [
            "release evidence comparative benchmark profile marker must start "
            f"with {COMPARATIVE_PROFILE_MARKER_PREFIX!r}"
        ]

    fields = {}
    for token in line[len(COMPARATIVE_PROFILE_MARKER_PREFIX) :].split():
        if "=" not in token:
            failures.append(
                "release evidence comparative benchmark profile marker must use "
                f"key=value fields; got {token!r}"
            )
            continue
        key, value = token.split("=", 1)
        if key not in COMPARATIVE_PROFILE_MARKER_KEYS:
            failures.append(
                "release evidence comparative benchmark profile marker contains "
                f"unknown field {key!r}"
            )
            continue
        if key in fields:
            failures.append(
                "release evidence comparative benchmark profile marker repeats "
                f"field {key!r}"
            )
            continue
        if not value:
            failures.append(
                "release evidence comparative benchmark profile marker field "
                f"{key!r} must not be blank"
            )
            continue
        fields[key] = value

    missing = [
        key for key in COMPARATIVE_PROFILE_MARKER_KEYS if key not in fields
    ]
    if missing:
        failures.append(
            "release evidence comparative benchmark profile marker missing "
            "fields: " + ", ".join(missing)
        )
    return fields, failures


def comparative_marker_csv(value):
    return value.split(",") if value != "-" else []


def validate_comparative_benchmark_profile_marker_output(output, environment):
    failures = []
    lines = output_lines(output)
    marker_indexes = comparative_profile_marker_indexes(lines)
    if len(marker_indexes) != 1:
        failures.append(
            "release evidence comparative benchmark profile marker must appear "
            "exactly once as an ok: comparative benchmark profile line"
        )
        return failures

    marker_index = marker_indexes[0]
    fields, marker_failures = parse_comparative_profile_marker(lines[marker_index])
    failures.extend(marker_failures)
    if marker_failures:
        return failures

    artifact_indexes = comparative_benchmark_results_artifact_indexes(lines)
    if artifact_indexes and marker_index <= artifact_indexes[0]:
        failures.append(
            "release evidence comparative benchmark profile marker must appear "
            "after the comparative benchmark results artifact line"
        )

    expected_selected = ",".join(COMPARATIVE_TARGET_LABELS)
    expected_required = ",".join(
        split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"))
    )
    expected_trend_baseline = (
        environment.get("ZMQ_BENCH_COMPARE_TREND_BASELINE", "").strip()
        if isinstance(environment, dict)
        else ""
    )
    expected_values = {
        "selected": expected_selected,
        "required": expected_required,
        "results_targets": expected_selected,
        "results": BENCHMARK_RESULTS_ARTIFACT,
        "gates_enforced": "true",
        "trend_required": "true",
        "trend_baseline": expected_trend_baseline,
        "iterations": comparative_profile_map_text(
            COMPARATIVE_BENCHMARK_PROFILE_ITERATIONS
        ),
        "warmup": comparative_profile_map_text(
            COMPARATIVE_BENCHMARK_PROFILE_WARMUP
        ),
        "source": "command",
    }
    for key, expected in expected_values.items():
        if fields.get(key) == expected:
            continue
        failures.append(
            "release evidence comparative benchmark profile marker field "
            f"{key} must be {expected!r}; got {fields.get(key)!r}"
        )

    selected_targets = comparative_marker_csv(fields.get("selected", ""))
    required_targets = comparative_marker_csv(fields.get("required", ""))
    result_targets = comparative_marker_csv(fields.get("results_targets", ""))
    if any(placeholder_env_value(target) for target in selected_targets):
        failures.append(
            "release evidence comparative benchmark profile marker selected "
            "targets must not contain placeholder values"
        )
    if any(placeholder_env_value(target) for target in required_targets):
        failures.append(
            "release evidence comparative benchmark profile marker required "
            "targets must not contain placeholder values"
        )
    if any(placeholder_env_value(target) for target in result_targets):
        failures.append(
            "release evidence comparative benchmark profile marker result "
            "targets must not contain placeholder values"
        )
    return failures


def comparative_required_target_count(environment):
    if not isinstance(environment, dict):
        return 0
    return len(ordered_unique([
        target
        for target in split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"))
        if target in COMPARATIVE_TARGET_LABELS
    ]))


def positive_measurement_count(line, metric):
    count = 0
    for raw in COMPARATIVE_MEASUREMENT_RE[metric].findall(line):
        try:
            value = float(raw.replace(",", ""))
        except ValueError:
            continue
        if math.isfinite(value) and value > 0:
            count += 1
    return count


def positive_finite_metric_cell(cell, metric):
    pattern = COMPARATIVE_MEASUREMENT_RE.get(metric)
    if pattern is None:
        return False
    match = pattern.fullmatch(cell)
    if match is None:
        return False
    try:
        value = float(match.group(1).replace(",", ""))
    except ValueError:
        return False
    return math.isfinite(value) and value > 0


def positive_finite_ratio_cell(cell):
    match = COMPARATIVE_RATIO_RE.fullmatch(cell)
    if match is None:
        return False
    try:
        value = float(match.group(1).replace(",", ""))
    except ValueError:
        return False
    return math.isfinite(value) and value > 0


def comparative_benchmark_metric_row_payloads(output):
    payloads = {}
    active_benchmark = None
    for line in comparative_benchmark_table_section(output):
        if not line:
            active_benchmark = None
            continue
        matched_benchmark = None
        for benchmark in COMPARATIVE_TABLE_ROW_MARKERS:
            match = re.match(
                r"^" + re.escape(benchmark) + r"\s+tput\b\s*(.*)$",
                line,
            )
            if match is None:
                continue
            matched_benchmark = benchmark
            active_benchmark = benchmark
            payloads.setdefault((benchmark, "tput"), []).append(
                match.group(1).strip()
            )
            break
        if matched_benchmark is not None:
            continue
        if active_benchmark is None:
            continue
        metric_match = re.match(r"^(p50|p99)\b\s*(.*)$", line)
        if metric_match is None:
            continue
        metric = metric_match.group(1)
        if metric not in COMPARATIVE_TABLE_METRICS:
            continue
        payloads.setdefault((active_benchmark, metric), []).append(
            metric_match.group(2).strip()
        )
    return payloads


def comparative_metric_row_cells(payload, target_count, ratio_count):
    tokens = payload.split()
    target_cells = tokens[:target_count]
    index = len(target_cells)
    ratio_cells = []
    while index < len(tokens) and len(ratio_cells) < ratio_count:
        ratio_cells.append(tokens[index])
        index += 1
        if index < len(tokens) and tokens[index] in COMPARATIVE_RATIO_MARKERS:
            index += 1
    return target_cells, ratio_cells, tokens[index:]


def comparative_benchmark_metric_measurements(output):
    return {
        key: positive_measurement_count(payloads[0], key[1])
        for key, payloads in comparative_benchmark_metric_row_payloads(output).items()
        if payloads
    }


def comparative_benchmark_metric_row_counts(output):
    return {
        key: len(payloads)
        for key, payloads in comparative_benchmark_metric_row_payloads(output).items()
    }


def comparative_benchmark_metric_rows(output):
    return set(comparative_benchmark_metric_measurements(output))


def comparative_benchmark_table_target_columns(header):
    columns = header.split()
    if len(columns) < 3 or columns[0] != "Benchmark" or columns[1] != "Metric":
        return []
    target_columns = []
    for column in columns[2:]:
        if "/" in column:
            break
        target_columns.append(column)
    return target_columns


def comparative_benchmark_table_ratio_columns(header):
    columns = header.split()
    if len(columns) < 3 or columns[0] != "Benchmark" or columns[1] != "Metric":
        return []
    target_count = len(comparative_benchmark_table_target_columns(header))
    return columns[2 + target_count :]


def comparative_known_table_target_columns():
    return tuple(COMPARATIVE_TABLE_TARGET_HEADERS.values())


def comparative_known_table_ratio_columns():
    zmq_header = COMPARATIVE_TABLE_TARGET_HEADERS["zmq"]
    return tuple(
        f"{zmq_header}/{header}"
        for target, header in COMPARATIVE_TABLE_TARGET_HEADERS.items()
        if target != "zmq"
    )


def comparative_required_table_target_columns(environment):
    if not isinstance(environment, dict):
        return []
    required_targets = set(split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")))
    return [
        header
        for target, header in COMPARATIVE_TABLE_TARGET_HEADERS.items()
        if target in required_targets
    ]


def comparative_required_target_labels(environment):
    if not isinstance(environment, dict):
        return []
    required_targets = set(split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")))
    return [
        label
        for target, label in COMPARATIVE_TARGET_LABELS.items()
        if target in required_targets
    ]


def comparative_required_table_ratio_columns(environment):
    if not isinstance(environment, dict):
        return []
    required_targets = set(split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")))
    zmq_header = COMPARATIVE_TABLE_TARGET_HEADERS["zmq"]
    if "zmq" not in required_targets:
        return []
    return [
        f"{zmq_header}/{header}"
        for target, header in COMPARATIVE_TABLE_TARGET_HEADERS.items()
        if target != "zmq" and target in required_targets
    ]


def validate_comparative_benchmark_metric_row_cells(
    output,
    table_target_columns,
    table_ratio_columns,
):
    failures = []
    if not table_target_columns and not table_ratio_columns:
        return failures
    row_payloads = comparative_benchmark_metric_row_payloads(output)
    for benchmark in COMPARATIVE_TABLE_ROW_MARKERS:
        for metric in COMPARATIVE_TABLE_METRICS:
            payloads = row_payloads.get((benchmark, metric), [])
            for payload in payloads:
                target_cells, ratio_cells, extra_cells = comparative_metric_row_cells(
                    payload,
                    len(table_target_columns),
                    len(table_ratio_columns),
                )
                if len(target_cells) != len(table_target_columns):
                    failures.append(
                        "release evidence comparative benchmark table row "
                        f"{benchmark} {metric} must include exactly "
                        f"{len(table_target_columns)} target measurement cells "
                        "matching the table header target columns"
                    )
                for column, cell in zip(table_target_columns, target_cells):
                    if positive_finite_metric_cell(cell, metric):
                        continue
                    failures.append(
                        "release evidence comparative benchmark table row "
                        f"{benchmark} {metric} target column {column!r} must "
                        "contain a positive finite target measurement cell; "
                        f"got {cell!r}"
                    )
                if len(ratio_cells) != len(table_ratio_columns):
                    failures.append(
                        "release evidence comparative benchmark table row "
                        f"{benchmark} {metric} must include exactly "
                        f"{len(table_ratio_columns)} ratio cells matching the "
                        "table header ratio columns"
                    )
                for column, cell in zip(table_ratio_columns, ratio_cells):
                    if positive_finite_ratio_cell(cell):
                        continue
                    failures.append(
                        "release evidence comparative benchmark table row "
                        f"{benchmark} {metric} ratio column {column!r} must "
                        "contain a positive finite ratio cell; "
                        f"got {cell!r}"
                    )
                if extra_cells:
                    failures.append(
                        "release evidence comparative benchmark table row "
                        f"{benchmark} {metric} must not include extra cells "
                        "after target and ratio columns: "
                        f"{' '.join(extra_cells)!r}"
                    )
    return failures


def validate_comparative_benchmark_summary_output(output, environment):
    failures = []
    thresholds = comparative_benchmark_thresholds(environment)
    lines = output_lines(output)
    failures.extend(
        exact_summary_output_line_failures(
            output,
            "COMPARATIVE BENCHMARK GATE",
            "comparative benchmark gate banner",
        )
    )
    failures.extend(validate_comparative_benchmark_results_artifact_output(output))
    failures.extend(
        validate_comparative_benchmark_profile_marker_output(output, environment)
    )

    gate_indexes = comparative_benchmark_gate_indexes(lines)
    if len(gate_indexes) > 1:
        failures.append(
            "release evidence comparative benchmark output must include exactly "
            "one COMPARATIVE BENCHMARK GATE marker"
        )

    comparison_indexes = comparative_benchmark_comparison_indexes(lines)
    if len(comparison_indexes) > 1:
        failures.append(
            "release evidence comparative benchmark output must include exactly "
            "one COMPARISON line before the COMPARATIVE BENCHMARK GATE"
        )
    if comparison_indexes:
        comparison_line = lines[comparison_indexes[0]]
        expected_comparison_line = comparative_expected_comparison_line()
        if comparison_line != expected_comparison_line:
            failures.append(
                "release evidence comparative benchmark COMPARISON line must "
                f"exactly match selected target labels: expected "
                f"{expected_comparison_line!r}; got {comparison_line!r}"
            )
        required_target_labels = comparative_required_target_labels(environment)
        observed_required_label_order = [
            label
            for label, index in sorted(
                (
                    (label, comparison_line.find(label))
                    for label in required_target_labels
                ),
                key=lambda item: item[1],
            )
            if index >= 0
        ]
        if required_target_labels and observed_required_label_order != required_target_labels:
            failures.append(
                "release evidence comparative benchmark COMPARISON line target "
                "labels must follow the comparative target catalogue order: "
                f"expected {', '.join(required_target_labels)!r}; got "
                f"{', '.join(observed_required_label_order)!r}"
            )
        for target in split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")):
            label = COMPARATIVE_TARGET_LABELS.get(target)
            if label is not None and comparison_line.count(label) != 1:
                failures.append(
                    "release evidence comparative benchmark COMPARISON line must "
                    f"include target label {label!r} exactly once"
                )

    table_target_columns = []
    table_ratio_columns = []
    table_headers = [
        line
        for line in comparative_benchmark_table_section(output)
        if line.startswith("Benchmark") and re.search(r"\bMetric\b", line)
    ]
    if len(table_headers) > 1:
        failures.append(
            "release evidence comparative benchmark table header must appear "
            "exactly once before the COMPARATIVE BENCHMARK GATE"
        )
    if table_headers:
        table_target_columns = comparative_benchmark_table_target_columns(
            table_headers[0],
        )
        unknown_table_target_columns = [
            column
            for column in ordered_unique(table_target_columns)
            if column not in comparative_known_table_target_columns()
        ]
        if unknown_table_target_columns:
            failures.append(
                "release evidence comparative benchmark table header must not "
                "include unknown target columns before ratio columns: "
                + ", ".join(unknown_table_target_columns)
            )
        required_table_target_columns = comparative_required_table_target_columns(
            environment,
        )
        observed_required_order = [
            column
            for column in table_target_columns
            if column in required_table_target_columns
        ]
        if (
            required_table_target_columns
            and observed_required_order != required_table_target_columns
        ):
            failures.append(
                "release evidence comparative benchmark table header target "
                "columns must follow the comparative target catalogue order "
                "before ratio columns: expected "
                f"{', '.join(required_table_target_columns)!r}; got "
                f"{', '.join(observed_required_order)!r}"
            )
        table_ratio_columns = comparative_benchmark_table_ratio_columns(
            table_headers[0],
        )
        unknown_table_ratio_columns = [
            column
            for column in ordered_unique(table_ratio_columns)
            if column not in comparative_known_table_ratio_columns()
        ]
        if unknown_table_ratio_columns:
            failures.append(
                "release evidence comparative benchmark table header must not "
                "include unknown ratio columns after target columns: "
                + ", ".join(unknown_table_ratio_columns)
            )
        required_table_ratio_columns = comparative_required_table_ratio_columns(
            environment,
        )
        observed_required_ratio_order = [
            column
            for column in table_ratio_columns
            if column in required_table_ratio_columns
        ]
        if (
            required_table_ratio_columns
            and observed_required_ratio_order != required_table_ratio_columns
        ):
            failures.append(
                "release evidence comparative benchmark table header ratio "
                "columns must follow the comparative target catalogue order "
                "after target columns: expected "
                f"{', '.join(required_table_ratio_columns)!r}; got "
                f"{', '.join(observed_required_ratio_order)!r}"
            )
        for target in split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")):
            header = COMPARATIVE_TABLE_TARGET_HEADERS.get(target)
            if header is not None and table_target_columns.count(header) != 1:
                failures.append(
                    "release evidence comparative benchmark table header must "
                    f"include target column {header!r} exactly once before "
                    "ratio columns"
                )
        for ratio in required_table_ratio_columns:
            if table_ratio_columns.count(ratio) != 1:
                failures.append(
                    "release evidence comparative benchmark table header must "
                    f"include ratio column {ratio!r} exactly once after target "
                    "columns"
                )

    failures.extend(
        validate_comparative_benchmark_metric_row_cells(
            output,
            table_target_columns,
            table_ratio_columns,
        )
    )

    gate_line_counts = {}
    for prefix in ("thresholds:", "trend thresholds:", "trend baseline:", "result:"):
        gate_line_counts[prefix] = sum(
            1
            for line in comparative_benchmark_full_gate_section(output)
            if line.startswith(prefix)
        )
    if gate_line_counts["thresholds:"] > 1:
        failures.append(
            "release evidence comparative benchmark thresholds line must appear "
            "exactly once inside the COMPARATIVE BENCHMARK GATE section"
        )
    if gate_line_counts["result:"] > 1:
        failures.append(
            "release evidence comparative benchmark gate result line must appear "
            "exactly once inside the COMPARATIVE BENCHMARK GATE section"
        )

    threshold_line = comparative_benchmark_gate_line(output, "thresholds:")
    expected_thresholds = comparative_thresholds_line(thresholds)
    if threshold_line is None:
        failures.append(
            "release evidence missing detailed comparative benchmark thresholds "
            "inside COMPARATIVE BENCHMARK GATE section"
        )
    elif threshold_line != expected_thresholds:
        failures.append(
            "release evidence comparative benchmark thresholds must match the "
            f"selected gate environment: expected {expected_thresholds!r}; "
            f"got {threshold_line!r}"
        )

    if bool_environment_value(environment, "ZMQ_BENCH_COMPARE_REQUIRE_TREND") is True:
        if gate_line_counts["trend thresholds:"] > 1:
            failures.append(
                "release evidence comparative benchmark trend thresholds line "
                "must appear exactly once inside the COMPARATIVE BENCHMARK GATE "
                "section"
            )
        if gate_line_counts["trend baseline:"] > 1:
            failures.append(
                "release evidence comparative benchmark trend baseline line "
                "must appear exactly once inside the COMPARATIVE BENCHMARK GATE "
                "section"
            )
        trend_line = comparative_benchmark_gate_line(output, "trend thresholds:")
        expected_trend = comparative_trend_thresholds_line(thresholds)
        if trend_line is None:
            failures.append(
                "release evidence missing detailed comparative benchmark trend "
                "thresholds inside COMPARATIVE BENCHMARK GATE section"
            )
        elif trend_line != expected_trend:
            failures.append(
                "release evidence comparative benchmark trend thresholds must "
                "match the selected gate environment: expected "
                f"{expected_trend!r}; got {trend_line!r}"
            )
        baseline_line = comparative_benchmark_gate_line(output, "trend baseline:")
        expected_baseline = comparative_trend_baseline_line(environment)
        if baseline_line is None:
            failures.append(
                "release evidence missing comparative benchmark trend baseline "
                "inside COMPARATIVE BENCHMARK GATE section"
            )
        elif expected_baseline is not None and baseline_line != expected_baseline:
            failures.append(
                "release evidence comparative benchmark trend baseline must "
                "match the selected gate environment: expected "
                f"{expected_baseline!r}; got {baseline_line!r}"
            )

    result_line = comparative_benchmark_gate_line(output, "result:")
    if result_line != "result: pass":
        got = result_line if result_line is not None else "<missing>"
        failures.append(
            "release evidence comparative benchmark gate result must be "
            "'result: pass' inside the COMPARATIVE BENCHMARK GATE section; "
            f"got {got!r}"
        )

    required_target_count = comparative_required_target_count(environment)
    metric_measurements = comparative_benchmark_metric_measurements(output)
    metric_row_counts = comparative_benchmark_metric_row_counts(output)
    metric_rows = set(metric_measurements)
    for benchmark in COMPARATIVE_TABLE_ROW_MARKERS:
        for metric in COMPARATIVE_TABLE_METRICS:
            count = metric_row_counts.get((benchmark, metric), 0)
            if count > 1:
                failures.append(
                    "release evidence comparative benchmark table row "
                    f"{benchmark} {metric} must appear exactly once before the "
                    "COMPARATIVE BENCHMARK GATE"
                )
            if (benchmark, metric) not in metric_rows:
                failures.append(
                    "release evidence comparative benchmark table missing "
                    f"{benchmark} {metric} metric row"
                )
            elif (
                required_target_count > 0
                and metric_measurements[(benchmark, metric)] < required_target_count
            ):
                failures.append(
                    "release evidence comparative benchmark table row "
                    f"{benchmark} {metric} must include at least "
                    f"{required_target_count} positive finite target "
                    "measurements"
                )
    return failures


def kraft_network_summary_phases(output):
    for line in kraft_failover_summary_lines(output):
        match = re.search(r"\bnetwork_partition=\[([^\]]*)\]", line)
        if match:
            return output_csv_values(
                match.group(1),
                "KRaft network partition summary phases",
            )
    return None, []


def kraft_failover_summary_lines(output):
    return output_summary_candidate_lines(
        output,
        "ok: KRaft controller failover harness passed ",
        " source=command",
    )


def kraft_failover_summary_line(output):
    lines = kraft_failover_summary_lines(output)
    return lines[0] if lines else None


def kraft_failover_summary_fields(line):
    payload = parenthesized_summary_payload(line)
    if payload is None:
        return None, []
    return parse_summary_key_value_fields(payload)


def kraft_failover_summary_field(line, name):
    fields, _ = kraft_failover_summary_fields(line)
    if fields is None:
        return None
    return fields.get(name)


def kraft_failover_summary_int(line, name, failures):
    value = kraft_failover_summary_field(line, name)
    if value is None or placeholder_env_value(value):
        failures.append(
            "release evidence KRaft failover summary must include "
            f"non-placeholder integer {name}"
        )
        return None
    try:
        parsed = int(value, 10)
    except ValueError:
        failures.append(
            f"release evidence KRaft failover summary field {name} "
            "must be an integer"
        )
        return None
    if parsed < 0:
        failures.append(
            f"release evidence KRaft failover summary field {name} "
            "must be non-negative"
        )
    return parsed


def validate_kraft_api_case_summary(fields, field_name, label, required_cases, failures):
    if fields is None:
        return

    value = fields.get(field_name)
    if value is None:
        failures.append(
            "release evidence KRaft failover summary must include "
            f"{field_name}=[<api_key>:<version>,...]"
        )
        return
    if not (value.startswith("[") and value.endswith("]")):
        failures.append(
            "release evidence KRaft failover summary "
            f"{field_name} must be bracketed"
        )
        return

    cases, case_failures = output_csv_values(value[1:-1], label)
    failures.extend(case_failures)
    malformed = [
        case
        for case in cases
        if re.match(r"^[0-9]+:[0-9]+$", case) is None
    ]
    if malformed:
        failures.append(
            f"release evidence {label} must use "
            "<api_key>:<version> entries; got " + ", ".join(malformed)
        )
    missing = [
        case
        for case in required_cases
        if case not in cases
    ]
    if missing:
        failures.append(
            f"release evidence {label} must include "
            + ", ".join(missing)
        )


def validate_kraft_controller_unsupported_cases(fields, failures):
    validate_kraft_api_case_summary(
        fields,
        "controller_unsupported_cases",
        "KRaft controller unsupported cases",
        KRAFT_CONTROLLER_UNSUPPORTED_REQUIRED_CASES,
        failures,
    )


def validate_kraft_broker_non_broker_cases(fields, failures):
    validate_kraft_api_case_summary(
        fields,
        "broker_non_broker_api_rejection_cases",
        "KRaft broker non-broker API rejection cases",
        KRAFT_BROKER_NON_BROKER_REQUIRED_CASES,
        failures,
    )


def validate_kraft_network_summary_output(output, environment):
    selected_phases = split_csv(environment.get("ZMQ_KRAFT_NETWORK_MATRIX"))
    if not selected_phases:
        return []
    summary_phases, summary_failures = kraft_network_summary_phases(output)
    if summary_phases is None:
        return [
            "release evidence missing KRaft network partition summary on "
            "harness output marker: network_partition=[<phases>]"
        ]
    if summary_failures:
        return summary_failures
    if summary_phases != selected_phases:
        got = ", ".join(summary_phases) if summary_phases else "<none>"
        return [
            "release evidence KRaft network partition summary must list "
            "selected phases from ZMQ_KRAFT_NETWORK_MATRIX: expected "
            + ", ".join(selected_phases)
            + "; got "
            + got
        ]
    return []


def kraft_network_phase_expected_result(environment, phase, failures):
    token = coverage_env_token(phase, "collapsed")
    env_name, value = first_present_environment_value(
        environment,
        (
            f"ZMQ_KRAFT_NETWORK_{token}_EXPECT",
            "ZMQ_KRAFT_NETWORK_EXPECT",
        ),
    )
    if value is None:
        return "fail"
    stripped = value.strip()
    if not stripped:
        failures.append(
            f"release evidence KRaft network expectation {env_name} must not be blank"
        )
        return None
    if placeholder_env_value(stripped):
        failures.append(
            f"release evidence KRaft network expectation {env_name} uses placeholder value"
        )
        return None
    if stripped not in ("fail", "survive"):
        failures.append(
            f"release evidence KRaft network expectation {env_name} must be fail or survive"
        )
        return None
    return stripped


def kraft_network_phase_expected_observed(expectation):
    return "survived" if expectation == "survive" else "failed"


def validate_kraft_network_phase_detail_output(output, environment):
    selected_phases = split_csv(environment.get("ZMQ_KRAFT_NETWORK_MATRIX"))
    if not selected_phases:
        return []

    phase_lines = []
    for line in output_lines(output):
        if line.startswith("ok: KRaft controller failover harness passed "):
            break
        phase_lines.append(line)

    details = {}
    malformed = set()
    duplicate_phases = set()
    pattern = re.compile(
        r"^ok: KRaft network partition phase (\S+) down=true "
        r"observed=(failed|survived) healed=true healed_leader=([0-9]+) "
        r"healed_fetch=true expect=(fail|survive) source=command$"
    )
    prefix = "ok: KRaft network partition phase "
    for line in phase_lines:
        match = pattern.match(line)
        if match:
            append_phase_detail(
                details,
                duplicate_phases,
                match.group(1),
                {
                    "observed": match.group(2),
                    "healed_leader": match.group(3),
                    "expect": match.group(4),
                },
            )
        elif line.startswith(prefix):
            phase = line[len(prefix) :].split()[0] if line[len(prefix) :] else ""
            if phase:
                malformed.add(phase)

    failures = []
    for phase in selected_phases:
        phase_expected = kraft_network_phase_expected_result(
            environment,
            phase,
            failures,
        )
        if phase_expected is None:
            continue
        expected_observed = kraft_network_phase_expected_observed(phase_expected)
        if phase in duplicate_phases:
            failures.append(
                "release evidence KRaft network partition phase detail marker "
                f"must not repeat phase {phase}"
            )
            continue
        observed_values = details.get(phase, [])
        detail = observed_values[0] if observed_values else None
        if (
            detail is not None
            and detail["expect"] == phase_expected
            and detail["observed"] == expected_observed
        ):
            continue
        if phase in malformed:
            failures.append(
                "release evidence KRaft network partition phase marker must include "
                f"down=true observed={expected_observed} healed=true "
                f"healed_leader=<id> healed_fetch=true expect={phase_expected} "
                f"source=command for phase {phase}"
            )
        elif detail is not None:
            if detail["expect"] != phase_expected:
                failures.append(
                    "release evidence KRaft network partition phase marker for phase "
                    f"{phase} must report expect={phase_expected}; "
                    f"got {detail['expect']}"
                )
            if detail["observed"] != expected_observed:
                failures.append(
                    "release evidence KRaft network partition phase marker for phase "
                    f"{phase} must report observed={expected_observed}; "
                    f"got {detail['observed']}"
                )
        else:
            failures.append(
                "release evidence missing KRaft network partition phase detail "
                "marker before the KRaft failover summary line: "
                f"ok: KRaft network partition phase {phase} down=true "
                f"observed={expected_observed} healed=true healed_leader=<id> "
                f"healed_fetch=true expect={phase_expected} source=command"
            )
    return failures


def validate_kraft_reassignment_summary_output(output):
    failures = []
    failures.extend(
        suffixed_summary_output_line_failures(
            output,
            "ok: KRaft controller failover harness passed ",
            " source=command",
            "KRaft failover summary",
        )
    )
    summary_lines = kraft_failover_summary_lines(output)
    if not summary_lines:
        failures.append(
            "release evidence missing KRaft failover summary output marker: "
            "ok: KRaft controller failover harness passed "
            "(reassignment_topic=<topic>, reassignment_target=<broker>, "
            "reassignment_target_offset=<offset>, "
            "reassignment_old_owner_rejected=true, "
            "reassignment_target_fetch_verified=true) source=command"
        )
        return failures

    line = summary_lines[0]
    fields, duplicate_fields = kraft_failover_summary_fields(line)
    if fields is None:
        failures.append(
            "release evidence KRaft failover summary must use "
            "comma-separated key=value fields"
        )
    elif duplicate_fields:
        failures.append(
            "release evidence KRaft failover summary must not repeat fields: "
            + ", ".join(duplicate_fields)
        )
    if fields is not None:
        unknown_fields = sorted(set(fields) - set(KRAFT_FAILOVER_SUMMARY_FIELDS))
        if unknown_fields:
            failures.append(
                "release evidence KRaft failover summary must not include "
                "unknown fields: " + ", ".join(unknown_fields)
            )
    validate_kraft_controller_unsupported_cases(fields, failures)
    validate_kraft_broker_non_broker_cases(fields, failures)
    for name in (
        "old_leader",
        "new_leader",
        "restarted_controller",
        "epoch",
        "automq_old_leader",
        "automq_new_leader",
        "automq_stream_id",
        "automq_deleted_stream_id",
        "automq_stream_set_object_id",
        "automq_node_id",
        "automq_zone_router_epoch",
        "committed_offset",
    ):
        kraft_failover_summary_int(line, name, failures)
    transactions_checked = kraft_failover_summary_int(
        line,
        "transactions_checked",
        failures,
    )
    if transactions_checked is not None and transactions_checked != 5:
        failures.append(
            "release evidence KRaft failover summary must report "
            "transactions_checked=5"
        )

    topic = kraft_failover_summary_field(line, "reassignment_topic")
    if topic is None or placeholder_env_value(topic):
        failures.append(
            "release evidence KRaft failover summary must include "
            "non-placeholder reassignment_topic"
        )
    for name in ("reassignment_target", "reassignment_target_offset"):
        value = kraft_failover_summary_field(line, name)
        if value is None or placeholder_env_value(value):
            failures.append(
                f"release evidence KRaft failover summary must include {name}"
            )
            continue
        try:
            parsed = int(value, 10)
        except ValueError:
            failures.append(
                f"release evidence KRaft failover summary field {name} "
                "must be an integer"
            )
            continue
        if parsed < 0:
            failures.append(
                f"release evidence KRaft failover summary field {name} "
                "must be non-negative"
            )
    for name in (
        "old_leader_rejoined",
        "old_leader_fresh_rejoin",
        "automq_old_leader_fresh_rejoin",
        "allocate_producer_ids_checked",
        "allocate_producer_ids_follower_rejection_checked",
        "describe_quorum_v2_checked",
        "fetch_snapshot_v1_checked",
        "all_controller_fetch_snapshot_v1_checked",
        "controller_api_versions_checked",
        "all_controller_api_versions_checked",
        "controller_unsupported_checked",
        "all_controller_unsupported_checked",
        "dynamic_raft_voter_negative_checked",
        "dynamic_raft_voter_follower_rejection_checked",
        "all_controller_describe_quorum_v2_checked",
        "broker_lifecycle_negative_checked",
        "broker_lifecycle_follower_rejection_checked",
        "controller_registration_negative_checked",
        "controller_registration_follower_rejection_checked",
        "broker_registration_follower_rejection_checked",
        "broker_non_broker_api_rejection_checked",
        "transaction_introspection_checked",
        "transaction_abort_checked",
        "txn_offset_commit_checked",
        "offset_fetch_v8_grouped_checked",
        "log_position_apis_checked",
        "delete_records_checked",
        "delete_topics_checked",
        "create_topics_checked",
        "create_partitions_checked",
        "client_quotas_checked",
        "scram_credentials_checked",
        "client_telemetry_checked",
        "delegation_tokens_checked",
        "finalized_features_checked",
        "acl_admin_checked",
        "config_admin_checked",
        "describe_topic_partitions_checked",
        "describe_configs_checked",
        "describe_log_dirs_checked",
        "alter_replica_log_dirs_checked",
        "assign_replicas_to_dirs_checked",
        "elect_leaders_checked",
        "describe_cluster_checked",
        "idempotent_producer_fencing",
        "describe_producers_checked",
        "delete_groups_checked",
        "classic_group_heartbeats",
        "group_describe_checked",
        "consumer_group_describe_checked",
        "list_groups_checked",
        "find_coordinator_checked",
        "share_group_heartbeat_checked",
        "share_group_describe_checked",
        "consumer_group_heartbeat_checked",
        "share_fetch_session_checked",
        "share_acknowledge_checked",
        "share_state_apis_checked",
        "kip848_describe_checked",
        "kip848_rejoin_checked",
        "kip848_rack_checked",
        "kip848_owned_assignment_checked",
        "kip848_subscription_update_checked",
        "kip848_negative_join_checked",
        "kip848_static_rejoin_checked",
        "offset_commit_v9_member_checked",
        "offset_fetch_v9_member_checked",
        "reassignment_old_owner_rejected",
        "reassignment_target_fetch_verified",
    ):
        if kraft_failover_summary_field(line, name) != "true":
            failures.append(
                f"release evidence KRaft failover summary must report {name}=true"
            )
    return failures


def parse_chaos_live_s3_provider_line(line):
    pattern = re.compile(
        r"^ok: chaos live-s3-outage provider "
        r"endpoint=(\S+) bucket=(\S+) scheme=(\S+) region=(\S+) "
        r"path_style=(true|false) source=(\S+)$"
    )
    marker_prefix = "ok: chaos live-s3-outage provider"
    match = pattern.match(line)
    if match:
        return (
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
            match.group(5),
            match.group(6),
        ), False
    if line == marker_prefix or line.startswith(marker_prefix + " "):
        return None, True
    return None, False


def chaos_live_s3_provider_details(output, lines=None):
    details = []
    malformed = False
    if lines is None:
        lines = output_lines(output)
    for line in lines:
        detail, line_malformed = parse_chaos_live_s3_provider_line(line)
        if detail:
            details.append(detail)
        elif line_malformed:
            malformed = True
    return details, malformed


def chaos_live_s3_setting_value(environment, suffix):
    return first_present_environment_value(
        environment,
        (
            f"ZMQ_CHAOS_S3_{suffix}",
            f"ZMQ_S3_{suffix}",
        ),
    )


def require_chaos_live_s3_setting(environment, suffix, failures):
    env_name, value = chaos_live_s3_setting_value(environment, suffix)
    if value is None:
        failures.append(
            f"release evidence missing live-S3 chaos setting {suffix}: "
            f"ZMQ_CHAOS_S3_{suffix} or ZMQ_S3_{suffix}"
        )
        return None, None
    if not isinstance(value, str):
        failures.append(
            f"release evidence live-S3 chaos setting {env_name} must be a string"
        )
        return env_name, None
    stripped = value.strip()
    if not stripped:
        failures.append(
            f"release evidence live-S3 chaos setting {env_name} must not be blank"
        )
        return env_name, None
    if placeholder_env_value(stripped):
        failures.append(
            f"release evidence live-S3 chaos setting {env_name} uses placeholder value"
        )
        return env_name, None
    return env_name, stripped


def validate_optional_chaos_live_s3_setting(environment, suffix, failures):
    env_name, value = chaos_live_s3_setting_value(environment, suffix)
    if value is None:
        return
    if not isinstance(value, str):
        failures.append(
            f"release evidence live-S3 chaos setting {env_name} must be a string"
        )
        return
    stripped = value.strip()
    if not stripped:
        failures.append(
            f"release evidence live-S3 chaos setting {env_name} must not be blank"
        )
    elif placeholder_env_value(stripped):
        failures.append(
            f"release evidence live-S3 chaos setting {env_name} uses placeholder value"
        )


def chaos_live_s3_expected_settings(environment, failures):
    values = {}
    for suffix in (
        "ENDPOINT",
        "PORT",
        "BUCKET",
        "ACCESS_KEY",
        "SECRET_KEY",
        "SCHEME",
        "REGION",
        "PATH_STYLE",
    ):
        env_name, value = require_chaos_live_s3_setting(
            environment,
            suffix,
            failures,
        )
        if value is None:
            return None
        values[suffix] = (env_name, value)

    validate_optional_chaos_live_s3_setting(environment, "TLS_CA_FILE", failures)

    port_name, port_text = values["PORT"]
    try:
        port = int(port_text, 10)
    except ValueError:
        failures.append(
            f"release evidence live-S3 chaos setting {port_name} must be an integer"
        )
        return None
    if port <= 0 or port > 65535:
        failures.append(
            f"release evidence live-S3 chaos setting {port_name} "
            "must be a positive TCP port"
        )
        return None

    scheme_name, scheme_text = values["SCHEME"]
    scheme = scheme_text.lower()
    if scheme not in ("http", "https"):
        failures.append(
            f"release evidence live-S3 chaos setting {scheme_name} "
            "must be http or https"
        )
        return None

    path_style_name, path_style_text = values["PATH_STYLE"]
    path_style = s3_provider_bool_text(path_style_text, None)
    if path_style is None:
        failures.append(
            f"release evidence live-S3 chaos setting {path_style_name} "
            "must be true or false"
        )
        return None

    return (
        f"{values['ENDPOINT'][1]}:{port}",
        values["BUCKET"][1],
        scheme,
        values["REGION"][1],
        path_style,
    )


def validate_chaos_live_s3_command_provenance(command, environment, required):
    failures = []
    for suffix in ("ENDPOINT", "PORT", "BUCKET", "SCHEME", "REGION", "PATH_STYLE"):
        env_name, manifest_value = chaos_live_s3_setting_value(environment, suffix)
        if not isinstance(manifest_value, str) or not manifest_value.strip():
            continue

        command_value = command_env_assignment_for_requirement(
            command or "",
            required,
            env_name,
        )
        if not command_value:
            failures.append(
                "release evidence command for broker chaos harness must include "
                f"non-empty {env_name}= assignment for live-S3 outage"
            )
        elif placeholder_env_value(command_value):
            failures.append(
                "release evidence command for broker chaos harness uses "
                f"placeholder {env_name} value for live-S3 outage"
            )
        elif command_value != manifest_value.strip():
            failures.append(
                "release evidence command for broker chaos harness uses "
                f"{env_name}={command_value!r}, but manifest environment "
                f"records {manifest_value.strip()!r}"
            )
    return failures


def validate_chaos_live_s3_provider_output(output, environment, lines=None):
    failures = []
    details, malformed = chaos_live_s3_provider_details(output, lines)
    if not details:
        failures.append(
            "release evidence missing chaos live-S3 provider marker: "
            "ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> "
            "bucket=<bucket> scheme=<scheme> region=<region> "
            "path_style=<true|false> source=command before the broker chaos "
            "harness summary line"
        )
        return failures
    if malformed:
        failures.append(
            "release evidence chaos live-S3 provider marker must use the "
            "endpoint/bucket/scheme/region/path_style/source line shape"
        )
    if len(details) > 1:
        failures.append(
            "release evidence chaos live-S3 provider marker must appear exactly "
            "once before the broker chaos harness summary line"
        )
    if not any(
        ":" in endpoint
        and not placeholder_env_value(endpoint)
        and not placeholder_env_value(bucket)
        and not placeholder_env_value(scheme)
        and not placeholder_env_value(region)
        and source == "command"
        for endpoint, bucket, scheme, region, path_style, source in details
    ):
        failures.append(
            "release evidence chaos live-S3 provider marker must include "
            "non-placeholder endpoint=<endpoint>:<port>, bucket=<bucket>, "
            "scheme=<scheme>, region=<region>, and source=command"
        )

    expected = chaos_live_s3_expected_settings(environment, failures)
    expected_detail = expected + ("command",) if expected is not None else None
    if expected_detail is not None and expected_detail not in details:
        (
            expected_endpoint,
            expected_bucket,
            expected_scheme,
            expected_region,
            expected_path_style,
            expected_source,
        ) = expected_detail
        failures.append(
            "release evidence chaos live-S3 provider marker must match "
            f"selected endpoint={expected_endpoint} bucket={expected_bucket} "
            f"scheme={expected_scheme} region={expected_region} "
            f"path_style={expected_path_style} source={expected_source}"
        )
    return failures


def chaos_harness_summary_scenarios(output):
    prefix = "ok: chaos harness passed for "
    suffix = " source=command"
    for line in output_lines(output):
        if line.startswith(prefix) and line.endswith(suffix):
            values, failures = output_csv_values(
                line[len(prefix) : -len(suffix)],
                "broker chaos harness summary scenarios",
            )
            return [
                canonical_chaos_scenario(name)
                for name in values
            ], failures
    return None, []


def chaos_lines_before_harness_summary(output):
    lines = []
    for line in output_lines(output):
        if (
            line.startswith("ok: chaos harness passed for ")
            and line.endswith(" source=command")
        ):
            return lines
        lines.append(line)
    return lines


def validate_chaos_harness_summary_output(output, environment):
    required_scenarios = [
        canonical_chaos_scenario(name)
        for name in split_csv(environment.get("ZMQ_CHAOS_REQUIRED_SCENARIOS"))
    ]
    if not required_scenarios:
        return []
    failures = []
    summary_prefix = "ok: chaos harness passed for "
    summary_suffix = " source=command"
    failures.extend(
        suffixed_summary_output_line_failures(
            output,
            summary_prefix,
            summary_suffix,
            "broker chaos harness summary",
        )
    )
    summary_lines = output_summary_candidate_lines(
        output,
        summary_prefix,
        summary_suffix,
    )
    summary_scenarios, summary_failures = chaos_harness_summary_scenarios(output)
    if summary_scenarios is None:
        failures.append(
            "release evidence missing broker chaos harness summary output "
            "marker: ok: chaos harness passed for <scenarios> source=command"
        )
        return failures
    if summary_failures:
        failures.extend(summary_failures)
        return failures
    missing = [
        scenario
        for scenario in required_scenarios
        if scenario not in summary_scenarios
    ]
    if missing:
        failures.append(
            "release evidence broker chaos harness summary must include "
            "required scenarios from ZMQ_CHAOS_REQUIRED_SCENARIOS: missing "
            + ", ".join(missing)
        )
        return failures
    extra = [
        scenario
        for scenario in summary_scenarios
        if scenario not in required_scenarios
    ]
    if extra:
        failures.append(
            "release evidence broker chaos harness summary must not list "
            "scenarios outside ZMQ_CHAOS_REQUIRED_SCENARIOS: "
            + ", ".join(extra)
        )
        return failures
    unknown = [
        scenario
        for scenario in summary_scenarios
        if scenario not in CHAOS_SCENARIO_MARKERS
    ]
    if unknown:
        failures.append(
            "release evidence broker chaos harness summary lists unknown "
            "scenarios: "
            + ", ".join(unknown)
        )
        return failures
    return failures


def chaos_exact_detail_lines(lines, scenario):
    prefix = f"ok: chaos {scenario}"
    return [
        line
        for line in lines
        if line == prefix or line.startswith(prefix + " ")
    ]


def validate_exact_chaos_detail_marker(lines, scenario, marker, failures):
    detail_lines = chaos_exact_detail_lines(lines, scenario)
    if len(detail_lines) > 1:
        failures.append(
            f"release evidence chaos {scenario} detail marker must not repeat "
            "before the broker chaos harness summary line"
        )
    if line_marker_present(lines, marker):
        return
    if detail_lines:
        failures.append(
            f"release evidence chaos {scenario} detail marker must exactly match: "
            + marker
            + " before the broker chaos harness summary line"
        )
    else:
        failures.append(
            f"release evidence missing chaos {scenario} detail marker: "
            + marker
            + " before the broker chaos harness summary line"
        )


def validate_chaos_scenario_detail_output(output, environment):
    required_scenarios = [
        canonical_chaos_scenario(name)
        for name in split_csv(environment.get("ZMQ_CHAOS_REQUIRED_SCENARIOS"))
    ]
    if not required_scenarios:
        return []

    lines = chaos_lines_before_harness_summary(output)
    failures = []

    if "sigkill-restart" in required_scenarios:
        prefix = "ok: chaos sigkill-restart"
        pattern = re.compile(
            r"^ok: chaos sigkill-restart killed=true restarted=true "
            r"recovered_payloads=2 first_offset=0 second_offset=([0-9]+) "
            r"source=command$"
        )
        matches = [pattern.match(line) for line in lines if line.startswith(prefix)]
        if len(matches) > 1:
            failures.append(
                "release evidence chaos sigkill-restart detail marker must not "
                "repeat before the broker chaos harness summary line"
            )
        good = False
        malformed = False
        for match in matches:
            if match is None:
                malformed = True
                continue
            if int(match.group(1)) > 0:
                good = True
            else:
                malformed = True
        if not good:
            if malformed:
                failures.append(
                    "release evidence chaos sigkill-restart marker must include "
                    "killed=true restarted=true recovered_payloads=2 first_offset=0 "
                    "positive second_offset, and source=command"
                )
            else:
                failures.append(
                    "release evidence missing chaos sigkill-restart detail marker "
                    "before the broker chaos harness summary line: "
                    "ok: chaos sigkill-restart killed=true restarted=true "
                    "recovered_payloads=2 first_offset=0 "
                    "second_offset=<positive> source=command"
                )

    if "slow-partial-client" in required_scenarios:
        marker = (
            "ok: chaos slow-partial-client "
            "partial_frame=true truncated_frame=true survived=true "
            "source=command"
        )
        validate_exact_chaos_detail_marker(
            lines,
            "slow-partial-client",
            marker,
            failures,
        )

    if "clock-skewed-records" in required_scenarios:
        marker = (
            "ok: chaos clock-skewed-records "
            "future_timestamp=true fetched=true serving=true source=command"
        )
        validate_exact_chaos_detail_marker(
            lines,
            "clock-skewed-records",
            marker,
            failures,
        )

    if "s3-outage" in required_scenarios:
        startup_marker = "ok: chaos s3-outage startup_fail_closed=true source=command"
        rejected_pattern = re.compile(
            r"^ok: chaos s3-outage rejected=true error_code=([0-9]+) "
            r"base_offset_negative=true serving=true source=command$"
        )
        s3_lines = [line for line in lines if line.startswith("ok: chaos s3-outage")]
        if len(s3_lines) > 1:
            failures.append(
                "release evidence chaos s3-outage detail marker must not repeat "
                "before the broker chaos harness summary line"
            )
        accepted = False
        malformed = False
        for line in s3_lines:
            if line == startup_marker:
                accepted = True
                continue
            match = rejected_pattern.match(line)
            if match is None:
                malformed = True
                continue
            if int(match.group(1)) != 0:
                accepted = True
            else:
                malformed = True
        if not accepted:
            if malformed:
                failures.append(
                    "release evidence chaos s3-outage marker must report either "
                    "startup_fail_closed=true or rejected=true error_code=<nonzero> "
                    "base_offset_negative=true serving=true source=command"
                )
            else:
                failures.append(
                    "release evidence missing chaos s3-outage detail marker "
                    "before the broker chaos harness summary line: "
                    "ok: chaos s3-outage rejected=true error_code=<nonzero> "
                    "base_offset_negative=true serving=true source=command"
                )

    if "live-s3-outage" in required_scenarios:
        marker = (
            "ok: chaos live-s3-outage "
            "down=true healed=true fail_closed=true recovered=true source=command"
        )
        live_s3_detail_lines = [
            line
            for line in lines
            if line == "ok: chaos live-s3-outage"
            or (
                line.startswith("ok: chaos live-s3-outage ")
                and not line.startswith("ok: chaos live-s3-outage provider ")
            )
        ]
        if len(live_s3_detail_lines) > 1:
            failures.append(
                "release evidence chaos live-s3-outage detail marker must not "
                "repeat before the broker chaos harness summary line"
            )
        if not line_marker_present(lines, marker):
            failures.append(
                "release evidence missing chaos live-s3-outage detail marker: "
                + marker
                + " before the broker chaos harness summary line"
            )
        failures.extend(
            validate_chaos_live_s3_provider_output(output, environment, lines)
        )

    return failures


def chaos_network_phase_expected_result(environment, phase, failures):
    token = coverage_env_token(phase, "collapsed")
    env_name, value = first_present_environment_value(
        environment,
        (
            f"ZMQ_CHAOS_NETWORK_{token}_EXPECT",
            "ZMQ_CHAOS_NETWORK_EXPECT",
        ),
    )
    if value is None:
        return "fail"
    stripped = value.strip()
    if not stripped:
        failures.append(
            f"release evidence chaos network expectation {env_name} must not be blank"
        )
        return None
    if placeholder_env_value(stripped):
        failures.append(
            f"release evidence chaos network expectation {env_name} uses placeholder value"
        )
        return None
    if stripped not in ("fail", "survive"):
        failures.append(
            f"release evidence chaos network expectation {env_name} must be fail or survive"
        )
        return None
    return stripped


def chaos_network_phase_expected_observed(expectation):
    return "survived" if expectation == "survive" else "failed"


def validate_chaos_network_phase_detail_output(output, environment):
    failures = []
    failures.extend(
        exact_summary_output_line_failures(
            output,
            "ok: chaos network-partition source=command",
            "chaos network-partition scenario summary",
        )
    )

    expected = split_csv(environment.get("ZMQ_CHAOS_REQUIRED_NETWORK_PHASES"))
    if not expected:
        return failures

    phase_lines = []
    for line in output_lines(output):
        if line == "ok: chaos network-partition source=command":
            break
        phase_lines.append(line)

    details = {}
    malformed = set()
    duplicate_phases = set()
    pattern = re.compile(
        r"^ok: chaos network-partition phase (\S+) down=true "
        r"observed=(failed|survived) healed=true recovered=true "
        r"expect=(fail|survive) source=command$"
    )
    prefix = "ok: chaos network-partition phase "
    for line in phase_lines:
        match = pattern.match(line)
        if match:
            append_phase_detail(
                details,
                duplicate_phases,
                match.group(1),
                {
                    "observed": match.group(2),
                    "expect": match.group(3),
                },
            )
        elif line.startswith(prefix):
            phase = line[len(prefix) :].split()[0] if line[len(prefix) :] else ""
            if phase:
                malformed.add(phase)

    for phase in expected:
        phase_expected = chaos_network_phase_expected_result(
            environment,
            phase,
            failures,
        )
        if phase_expected is None:
            continue
        expected_observed = chaos_network_phase_expected_observed(phase_expected)
        if phase in duplicate_phases:
            failures.append(
                "release evidence chaos network-partition phase detail marker "
                f"must not repeat phase {phase}"
            )
            continue
        observed_values = details.get(phase, [])
        detail = observed_values[0] if observed_values else None
        if (
            detail is not None
            and detail["expect"] == phase_expected
            and detail["observed"] == expected_observed
        ):
            continue
        if phase in malformed:
            failures.append(
                "release evidence chaos network-partition phase marker must include "
                f"down=true observed={expected_observed} healed=true "
                f"recovered=true expect={phase_expected} source=command "
                f"for phase {phase}"
            )
        elif detail is not None:
            if detail["expect"] != phase_expected:
                failures.append(
                    "release evidence chaos network-partition phase marker for phase "
                    f"{phase} must report expect={phase_expected}; "
                    f"got {detail['expect']}"
                )
            if detail["observed"] != expected_observed:
                failures.append(
                    "release evidence chaos network-partition phase marker for phase "
                    f"{phase} must report observed={expected_observed}; "
                    f"got {detail['observed']}"
                )
        else:
            failures.append(
                "release evidence missing chaos network-partition phase detail "
                "marker before the chaos network-partition scenario marker: "
                f"ok: chaos network-partition phase {phase} down=true "
                f"observed={expected_observed} healed=true recovered=true "
                f"expect={phase_expected} source=command"
            )
    return failures


def chaos_network_phase_required_marker(environment, phase):
    phase_failures = []
    phase_expected = chaos_network_phase_expected_result(
        environment,
        phase,
        phase_failures,
    )
    if phase_expected is None:
        return f"ok: chaos network-partition phase {phase}"
    expected_observed = chaos_network_phase_expected_observed(phase_expected)
    return (
        f"ok: chaos network-partition phase {phase} down=true "
        f"observed={expected_observed} healed=true recovered=true "
        f"expect={phase_expected} source=command"
    )


def kraft_network_phase_required_marker(environment, phase):
    phase_failures = []
    phase_expected = kraft_network_phase_expected_result(
        environment,
        phase,
        phase_failures,
    )
    if phase_expected is None:
        return f"ok: KRaft network partition phase {phase}"
    expected_observed = kraft_network_phase_expected_observed(phase_expected)
    return (
        f"ok: KRaft network partition phase {phase} down=true "
        f"observed={expected_observed} healed=true healed_leader=<id> "
        f"healed_fetch=true expect={phase_expected} source=command"
    )


def e2e_chaos_phase_required_marker(environment, phase):
    phase_failures = []
    phase_expected = e2e_chaos_phase_expected_result(
        environment,
        phase,
        phase_failures,
    )
    if phase_expected is None:
        return f"ok: E2E chaos phase {phase}"
    expected_observed = e2e_chaos_phase_expected_observed(phase_expected)
    return (
        f"ok: E2E chaos phase {phase} down=true "
        f"observed={expected_observed} healed=true recovered=true "
        f"expect={phase_expected} source=command"
    )


def e2e_load_scale_phase_required_marker(environment, phase):
    expected_apply_source = e2e_load_scale_expected_phase_source(
        environment,
        phase,
        "APPLY",
    )
    expected_restore_source = e2e_load_scale_expected_phase_source(
        environment,
        phase,
        "RESTORE",
    )
    marker = (
        f"ok: E2E load/scale phase {phase} applied=true restored=true "
        "marker_payloads=hook-owned "
        f"apply_source={expected_apply_source} "
        f"restore_source={expected_restore_source} source=command"
    )
    if "fixture" not in (expected_apply_source, expected_restore_source):
        return marker

    phase_failures = []
    expected_action = e2e_load_scale_expected_fixture_action(
        environment,
        phase,
        phase_failures,
    )
    if expected_action is None:
        return marker
    marker += f" action={expected_action}"
    if expected_action == "load":
        expected_records = e2e_load_scale_expected_fixture_load_records(
            environment,
            phase,
            phase_failures,
        )
        if expected_records is not None:
            marker += f" load_records={expected_records}"
    return marker


def required_environment_output_markers(label, environment):
    markers = []
    if not isinstance(environment, dict):
        return markers

    if label == "broker chaos harness":
        for scenario in split_csv(environment.get("ZMQ_CHAOS_REQUIRED_SCENARIOS")):
            marker = CHAOS_SCENARIO_MARKERS.get(canonical_chaos_scenario(scenario))
            if marker:
                add_unique_marker(markers, marker)
        for phase in split_csv(environment.get("ZMQ_CHAOS_REQUIRED_NETWORK_PHASES")):
            add_unique_marker(markers, chaos_network_phase_required_marker(environment, phase))

    elif label == "external client matrix":
        for tool in split_csv(environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS")):
            marker = CLIENT_TOOL_OUTPUT_MARKERS.get(tool)
            if marker:
                add_unique_marker(markers, marker)
        for profile in required_client_profile_names(environment):
            add_unique_marker(markers, f"ok: client matrix profile {profile}")

    elif label == "S3 provider matrix":
        for profile in split_csv(environment.get("ZMQ_S3_PROVIDER_REQUIRED_PROFILES")):
            add_unique_marker(
                markers,
                (
                    f"ok: S3 provider live-suite profile {profile} "
                    "command_started=true completed=true source=command"
                ),
            )
            add_unique_marker(markers, f"ok: S3 provider profile {profile}")
        for name, marker_template in (
            (
                "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
                "ok: S3 provider outage profile {profile} "
                "down=true healed=true fail_closed=true recovered=true source=command",
            ),
            (
                "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
                "ok: S3 provider process-crash profile {profile} "
                "killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
            ),
            (
                "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
                "ok: S3 provider list-pagination profile {profile} "
                "required=true completed=true source=command",
            ),
            (
                "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
                "ok: S3 provider multipart-edge profile {profile} "
                "required=true completed=true source=command",
            ),
            (
                "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
                "ok: S3 provider multipart-fault profile {profile} "
                "command_started=true completed=true injected=true recovered=true source=command",
            ),
        ):
            for profile in split_csv(environment.get(name)):
                add_unique_marker(markers, marker_template.format(profile=profile))

    elif label == "KRaft failover gate":
        for phase in split_csv(environment.get("ZMQ_KRAFT_REQUIRED_NETWORK_PHASES")):
            add_unique_marker(markers, kraft_network_phase_required_marker(environment, phase))

    elif label == "Docker E2E gate":
        for phase in split_csv(environment.get("ZMQ_E2E_REQUIRED_CHAOS_PHASES")):
            add_unique_marker(markers, e2e_chaos_phase_required_marker(environment, phase))
        for phase in split_csv(environment.get("ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES")):
            add_unique_marker(
                markers,
                e2e_load_scale_phase_required_marker(environment, phase),
            )

    elif label == "comparative benchmark gate":
        for target in split_csv(environment.get("ZMQ_BENCH_COMPARE_REQUIRED_TARGETS")):
            marker = COMPARATIVE_TARGET_LABELS.get(target)
            if marker:
                add_unique_marker(markers, marker)
        if bool_environment_value(environment, "ZMQ_BENCH_COMPARE_REQUIRE_TREND") is True:
            add_unique_marker(markers, "trend thresholds:")
            add_unique_marker(markers, "trend baseline:")

    return markers


def validate_release_evidence(
    manifest,
    current_commit=None,
    tracked_worktree_dirty=None,
):
    failures = []
    if not isinstance(manifest, dict):
        return ["release evidence must be a JSON object"]
    failures.extend(
        validate_object_fields(
            manifest,
            RELEASE_EVIDENCE_FIELDS,
            "release evidence manifest",
        )
    )

    commit = manifest.get("commit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        failures.append("release evidence must include exact 40-hex commit")
    elif current_commit is not None and commit != current_commit:
        failures.append(
            "release evidence commit does not match current checkout: "
            f"{commit} != {current_commit}"
        )

    if tracked_worktree_dirty is True:
        failures.append("release evidence must be validated from a clean tracked worktree")

    environment = manifest.get("environment")
    if not isinstance(environment, dict):
        failures.append("release evidence must include environment object")
        environment = {}
    failures.extend(validate_environment(environment))

    commands = manifest.get("commands")
    if not isinstance(commands, list):
        failures.append("release evidence must include commands list")
        commands = []

    successful_commands = []
    for index, entry in enumerate(commands):
        entry_failures = validate_command_entry(entry, index)
        failures.extend(entry_failures)
        if not entry_failures:
            successful_commands.append((index, entry["command"], entry["output"]))

    used_indices = set()
    for requirement in REQUIRED_COMMANDS:
        matching_indices = [
            index
            for index, command, _output in successful_commands
            if command_matches(
                command,
                requirement["required"],
                requirement.get("forbidden"),
            )
        ]
        if len(matching_indices) > 1:
            failures.append(
                "release evidence contains duplicate successful command entries "
                f"for {requirement['label']}: "
                + ", ".join(str(index) for index in matching_indices)
            )

        match = None
        match_command = None
        match_output = None
        for index, command, output in successful_commands:
            if index in used_indices:
                continue
            if command_matches(
                command,
                requirement["required"],
                requirement.get("forbidden"),
            ):
                match = index
                match_command = command
                match_output = output
                break
        if match is None:
            failures.append(
                f"release evidence missing successful output for {requirement['label']}"
            )
        else:
            for marker in requirement.get("skip_markers", []):
                if skip_marker_present(match_output, marker):
                    failures.append(
                        "release evidence captured skip output for "
                        f"{requirement['label']}: {marker}"
                    )
            for marker in requirement.get("output_markers", []):
                if not output_marker_present(match_output, marker):
                    failures.append(
                        "release evidence missing output marker for "
                        f"{requirement['label']}: {marker}"
                    )
            for marker in required_environment_output_markers(
                requirement["label"],
                environment,
            ):
                if not output_marker_present(match_output, marker):
                    failures.append(
                        "release evidence missing required coverage output marker for "
                        f"{requirement['label']}: {marker}"
                    )
            failures.extend(
                exact_once_requirement_output_marker_failures(
                    requirement["label"],
                    match_output,
                )
            )
            if requirement["label"] == "broker chaos harness":
                failures.extend(
                    validate_chaos_harness_summary_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_chaos_scenario_detail_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_chaos_network_phase_detail_output(
                        match_output,
                        environment,
                    )
                )
                if "live-s3-outage" in [
                    canonical_chaos_scenario(name)
                    for name in split_csv(
                        environment.get("ZMQ_CHAOS_REQUIRED_SCENARIOS")
                    )
                ]:
                    failures.extend(
                        validate_chaos_live_s3_command_provenance(
                            match_command,
                            environment,
                            requirement["required"],
                        )
                    )
            if requirement["label"] == "external client matrix":
                for semantic in split_csv(
                    environment.get("ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS")
                ):
                    if not client_probe_semantic_present(match_output, semantic):
                        failures.append(
                            "release evidence missing exact client semantic token for "
                            f"{requirement['label']}: {semantic}"
                        )
                    elif not client_profile_semantic_output_present(
                        match_output,
                        environment,
                        semantic,
                    ):
                        failures.append(
                            "release evidence missing client semantic token on "
                            "a profile-selected tool marker for "
                            f"{requirement['label']}: {semantic}"
                        )
                failures.extend(
                    validate_client_profile_output_markers(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_client_profile_scoped_probe_markers(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_client_matrix_summary_output(
                        match_output,
                        environment,
                    )
                )
            if requirement["label"] == "S3 process-crash replacement gate":
                failures.extend(validate_s3_process_crash_summary_output(match_output))
            if requirement["label"] == "MinIO/S3 integration gate":
                failures.extend(validate_minio_test_count_output(match_output))
            if requirement["label"] == "S3 provider matrix":
                failures.extend(
                    validate_s3_provider_matrix_command_provenance(
                        match_command,
                        environment,
                        requirement["required"],
                    )
                )
                failures.extend(
                    validate_s3_provider_profile_output_markers(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_s3_provider_profile_scoped_markers(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_s3_provider_matrix_summary_output(
                        match_output,
                        environment,
                    )
                )
            if requirement["label"] == "KRaft failover gate":
                failures.extend(
                    validate_kraft_network_summary_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_kraft_network_phase_detail_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_kraft_reassignment_summary_output(match_output)
                )
            if requirement["label"] == "Docker E2E gate":
                failures.extend(
                    validate_e2e_command_provenance(
                        match_command,
                        environment,
                        requirement["required"],
                    )
                )
                failures.extend(validate_e2e_output_line_markers(match_output))
                failures.extend(
                    validate_e2e_phase_summary_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_e2e_chaos_phase_detail_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(
                    validate_e2e_load_scale_phase_detail_output(
                        match_output,
                        environment,
                    )
                )
                failures.extend(validate_e2e_final_results_output(match_output, environment))
            if requirement["label"] == "local benchmark gate":
                failures.extend(validate_local_benchmark_summary_output(match_output))
            if requirement["label"] == "live-S3 benchmark gate":
                failures.extend(validate_live_s3_benchmark_summary_output(match_output))
                failures.extend(
                    validate_live_s3_benchmark_command_provenance(
                        match_command,
                        environment,
                        requirement["required"],
                    )
                )
                failures.extend(
                    validate_live_s3_benchmark_provider_output(
                        match_output,
                        environment,
                        match_command,
                        requirement["required"],
                    )
                )
                failures.extend(
                    validate_live_s3_benchmark_operation_summary_output(match_output)
                )
                failures.extend(
                    validate_live_s3_benchmark_request_volume_output(match_output)
                )
            if requirement["label"] == "comparative benchmark gate":
                failures.extend(
                    validate_comparative_benchmark_command_provenance(
                        match_command,
                        environment,
                        requirement["required"],
                    )
                )
                failures.extend(
                    validate_comparative_benchmark_summary_output(
                        match_output,
                        environment,
                    )
                )
            failures.extend(
                validate_required_command_env_assignments(
                    match_command,
                    requirement["required"],
                    environment,
                    requirement["label"],
                )
            )
            for env_name in requirement.get("command_env_assignments", []):
                command_value = command_env_assignment_for_requirement(
                    match_command or "",
                    requirement["required"],
                    env_name,
                )
                manifest_value = environment.get(env_name)
                if not command_value:
                    failures.append(
                        f"release evidence command for {requirement['label']} "
                        f"must include non-empty {env_name}= assignment"
                    )
                elif placeholder_env_value(command_value):
                    failures.append(
                        f"release evidence command for {requirement['label']} "
                        f"uses placeholder {env_name} value"
                    )
                elif (
                    env_name == "ZMQ_BENCH_COMPARE_TREND_BASELINE"
                    and benchmark_trend_baseline_points_at_current_results(command_value)
                ):
                    failures.append(
                        f"release evidence command for {requirement['label']} "
                        f"{env_name} must point at a prior benchmark artifact, "
                        "not the current benchmarks/results.json output"
                    )
                elif not isinstance(manifest_value, str) or not manifest_value.strip():
                    failures.append(
                        f"release evidence manifest environment for "
                        f"{requirement['label']} must record non-empty {env_name}"
                    )
                else:
                    if command_value != manifest_value.strip():
                        failures.append(
                            f"release evidence command for {requirement['label']} "
                            f"uses {env_name}={command_value!r}, but manifest "
                            f"environment records {manifest_value.strip()!r}"
                        )
            used_indices.add(match)

    if "automq_complete" not in manifest:
        failures.append("release evidence missing automq_complete=false")
    automq_complete = manifest_bool_value(manifest, "automq_complete", failures)
    if automq_complete and REQUIRED_UNSUPPORTED_SURFACES:
        failures.append(
            "AutoMQ-complete evidence is blocked while the verifier catalog "
            "still lists unsupported or partial surfaces"
        )

    unsupported = manifest.get("unsupported_or_partial_surfaces")
    if not isinstance(unsupported, list):
        failures.append("release evidence must include unsupported_or_partial_surfaces list")
    else:
        if automq_complete and unsupported:
            failures.append("AutoMQ-complete evidence cannot list unsupported or partial surfaces")
        failures.extend(validate_unsupported_surfaces(unsupported))

    for flag in BLOCKING_FLAGS:
        if flag not in manifest:
            failures.append(
                f"release evidence missing blocking flag {flag}=false"
            )
            continue
        if manifest_bool_value(manifest, flag, failures):
            failures.append(f"release evidence has blocking flag set: {flag}")

    return failures


def validate_release_evidence_for_checkout(
    manifest,
    current_commit,
    tracked_worktree_dirty,
):
    failures = []
    if current_commit is None:
        failures.append("release evidence could not determine current git commit")
    if tracked_worktree_dirty is None:
        failures.append(
            "release evidence could not determine tracked worktree cleanliness"
        )
    failures.extend(
        validate_release_evidence(
            manifest,
            current_commit=current_commit,
            tracked_worktree_dirty=tracked_worktree_dirty,
        )
    )
    return failures


def normalized_contains(haystack, needle):
    return " ".join(needle.split()) in " ".join(haystack.split())


def required_commands_block(criteria):
    marker = "## Required Commands"
    start = criteria.find(marker)
    if start < 0:
        return ""
    fence_start = criteria.find("```sh", start)
    if fence_start < 0:
        return ""
    block_start = criteria.find("\n", fence_start)
    fence_end = criteria.find("```", block_start + 1)
    if block_start < 0 or fence_end < 0:
        return ""
    return criteria[block_start:fence_end]


def required_command_lines(criteria):
    return [
        line.strip()
        for line in required_commands_block(criteria).splitlines()
        if line.strip()
    ]


def markdown_section(criteria, heading):
    start = criteria.find(heading)
    if start < 0:
        return ""
    body_start = criteria.find("\n", start)
    if body_start < 0:
        return ""
    next_heading = re.search(r"(?m)^## ", criteria[body_start + 1 :])
    if next_heading is None:
        return criteria[body_start + 1 :]
    return criteria[body_start + 1 : body_start + 1 + next_heading.start()]


def known_unsupported_surface_bullets(criteria):
    section = markdown_section(criteria, "## Known Unsupported Or Partial Surfaces")
    bullets = []
    current = []
    for line in section.splitlines():
        if line.startswith("- "):
            if current:
                bullets.append(" ".join(current))
            current = [line[2:].strip()]
            continue
        if current:
            stripped = line.strip()
            if stripped:
                current.append(stripped)
    if current:
        bullets.append(" ".join(current))
    return bullets


def assert_known_unsupported_surfaces_match_validator(criteria):
    bullets = known_unsupported_surface_bullets(criteria)
    if len(bullets) != len(REQUIRED_UNSUPPORTED_SURFACES):
        raise AssertionError(
            "release criteria Known Unsupported Or Partial Surfaces must list "
            f"exactly {len(REQUIRED_UNSUPPORTED_SURFACES)} top-level surfaces, "
            f"found {len(bullets)}"
        )

    matched_bullets = set()
    for surface in REQUIRED_UNSUPPORTED_SURFACES:
        matches = [
            index
            for index, bullet in enumerate(bullets)
            if unsupported_surface_entry_matches(bullet, surface)
        ]
        if not matches:
            raise AssertionError(
                "release criteria Known Unsupported Or Partial Surfaces missing "
                f"top-level bullet for {surface['label']}"
            )
        if len(matches) > 1:
            raise AssertionError(
                "release criteria Known Unsupported Or Partial Surfaces has "
                f"duplicate bullets for {surface['label']}: "
                + ", ".join(str(index) for index in matches)
            )
        if matches[0] in matched_bullets:
            raise AssertionError(
                "release criteria Known Unsupported Or Partial Surfaces bullet "
                f"{matches[0]} matches multiple verifier surfaces"
            )
        bullet = bullets[matches[0]]
        if not any(marker in bullet.lower() for marker in surface["status_markers"]):
            raise AssertionError(
                "release criteria Known Unsupported Or Partial Surfaces bullet "
                f"for {surface['label']} must mark the surface as "
                f"{surface['status_label']}"
            )
        matched_bullets.add(matches[0])

    unmatched = sorted(set(range(len(bullets))) - matched_bullets)
    if unmatched:
        raise AssertionError(
            "release criteria Known Unsupported Or Partial Surfaces has "
            "unmatched top-level bullets: "
            + ", ".join(str(index) for index in unmatched)
        )


def validate_required_command_block_line(command, index):
    failures = []
    failures.extend(validate_shell_command_single_line(command, index))
    failures.extend(validate_shell_command_unquoted(command, index))
    failures.extend(validate_shell_command_unescaped(command, index))
    failures.extend(validate_shell_command_separators(command, index))
    failures.extend(validate_shell_command_segment_shape(command, index))
    failures.extend(validate_disallowed_shell_operators(command, index))
    failures.extend(validate_duplicate_command_env_assignments(command, index))
    failures.extend(validate_command_does_not_embed_output_markers(command, index))
    failures.extend(validate_benchmark_threshold_command_assignments(command, index))
    failures.extend(validate_integer_command_assignments(command, index))
    return failures


def assert_required_command_block_matches_validator(criteria):
    lines = required_command_lines(criteria)
    if len(lines) != len(REQUIRED_COMMANDS):
        raise AssertionError(
            "release criteria required command block must list exactly "
            f"{len(REQUIRED_COMMANDS)} command lines, found {len(lines)}"
        )

    for index, requirement in enumerate(REQUIRED_COMMANDS):
        line = lines[index]
        line_failures = validate_required_command_block_line(line, index)
        if line_failures:
            raise AssertionError(
                "release criteria required command block line "
                f"{index + 1} violates release command syntax: "
                + "; ".join(line_failures)
            )
        if not command_matches(
            line,
            requirement["required"],
            requirement.get("forbidden"),
        ):
            raise AssertionError(
                "release criteria required command block line "
                f"{index + 1} must match {requirement['label']}"
            )


def current_git_commit():
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--verify", "HEAD"],
            cwd=PROJECT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    commit = proc.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        return None
    return commit


def tracked_worktree_dirty():
    for args in (
        ["git", "diff", "--quiet", "--"],
        ["git", "diff", "--cached", "--quiet", "--"],
    ):
        try:
            proc = subprocess.run(
                args,
                cwd=PROJECT_DIR,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            return None
        if proc.returncode == 1:
            return True
        if proc.returncode != 0:
            return None
    return False


def assert_release_criteria_contract_documented():
    with open(RELEASE_CRITERIA_PATH, "r", encoding="utf-8") as f:
        criteria = f.read()

    for fragment in (
        "# AutoMQ Parity Release Criteria",
        "## Required Gates",
        "## Required Commands",
        "## Known Unsupported Or Partial Surfaces",
        "## Release Decision",
        "`Protocol`",
        "`Durability`",
        "`Stateless`",
        "`MultiNode`",
        "`Security`",
        "`Observability`",
        "`Performance`",
        "`Chaos`",
        "Runtime elapsed-time gates must use monotonic clocks",
        "outbound TLS hostname verification",
    ):
        if not normalized_contains(criteria, fragment):
            raise AssertionError(
                f"release criteria missing required fragment {fragment!r}"
            )

    for requirement in REQUIRED_COMMANDS:
        for fragment in requirement["required"]:
            if not normalized_contains(criteria, fragment):
                raise AssertionError(
                    f"release criteria missing command fragment {fragment!r}"
                )
        for marker in requirement.get("output_markers", []):
            if not normalized_contains(criteria, marker):
                raise AssertionError(
                    f"release criteria missing output marker fragment {marker!r}"
                )

    for name in REQUIRED_ENV_VARS:
        if not normalized_contains(criteria, name):
            raise AssertionError(
                f"release criteria missing required environment variable {name}"
            )

    for surface in REQUIRED_UNSUPPORTED_SURFACES:
        for fragment in surface["fragments"]:
            if not normalized_contains(criteria, fragment):
                raise AssertionError(
                    f"release criteria missing unsupported surface fragment {fragment!r}"
                )
    assert_known_unsupported_surfaces_match_validator(criteria)

    for fragment in (
        "concrete non-placeholder values",
        "comma-separated coverage variables must parse to at least one value",
        "explicitly blank selector values are rejected",
        "selector/provenance",
        "`ZMQ_CHAOS_NETWORK_MATRIX`",
        "`ZMQ_KRAFT_NETWORK_MATRIX`",
        "`ZMQ_E2E_CHAOS_MATRIX`",
        "`ZMQ_E2E_LOAD_SCALE_MATRIX`",
        "`ZMQ_S3_PROVIDER_PROFILES`",
        "`ZMQ_CLIENT_MATRIX_PROFILES`",
        "Required values must be subsets of those selector variables",
        "same environment-variable token",
        "hook command variables",
        "`ZMQ_CHAOS_NETWORK_<PHASE>_{DOWN,UP}`",
        "`ZMQ_KRAFT_NETWORK_<PHASE>_{DOWN,UP}`",
        "`ZMQ_E2E_CHAOS_<PHASE>_{DOWN,UP}`",
        "`ZMQ_E2E_LOAD_SCALE_<PHASE>_{APPLY,RESTORE}`",
        "`ZMQ_S3_<PROFILE>_{OUTAGE_DOWN,OUTAGE_UP}`",
        "`ZMQ_S3_<PROFILE>_MULTIPART_FAULT_CMD`",
        "angle-bracket placeholders",
        "`<host>`",
        "`<port>`",
        "`<bucket>`",
        "non-placeholder, nonblank, and parseable",
        "truthy enable toggles",
        "`ZMQ_S3_<PROFILE>_RUN_LIVE_OUTAGE`",
        "`ZMQ_S3_<PROFILE>_RUN_PROCESS_CRASH`",
        "`ZMQ_S3_<PROFILE>_REQUIRE_LIST_PAGINATION`",
        "`ZMQ_S3_<PROFILE>_REQUIRE_MULTIPART_EDGE`",
        "`ZMQ_S3_<PROFILE>_RUN_MULTIPART_FAULT`",
        "same selected enable assignments",
        "untracked shell environment",
        "Required client profile evidence",
        "`ZMQ_CLIENT_MATRIX_<PROFILE>_TOOLS`",
        "`ZMQ_CLIENT_MATRIX_<PROFILE>_SEMANTICS`",
        "`ZMQ_CLIENT_MATRIX_<PROFILE>_VERSION`",
        "Java `JAVA_CLASSPATH`",
        "non-`@latest` `GO_MODULE`",
        "Secured and OAuth-required profiles",
        "`SECURITY_PROTOCOL`",
        "`OAUTH_TOKEN`",
        "`OAUTH_JAAS_CONFIG`",
        "`OAUTHBEARER_CONFIG`",
        "`BAD_OAUTH_TOKEN`",
        "`BAD_OAUTH_JAAS_CONFIG`",
        "`BAD_OAUTHBEARER_CONFIG`",
        "Profile semantics must also match the selected tools",
        "security semantics cannot be claimed",
        "not expose those probes",
        "Release evidence also validates",
        "tool-specific OAuth fixture semantics",
        "malformed positive fixtures",
        "claimed as negative coverage",
        "placeholder paths",
        "`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS` must include `zmq` plus at least one",
        "comparative benchmark command must include",
        "`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`",
        "match the manifest environment",
        "`ZMQ_BENCH_COMPARE_ENFORCE_GATES`",
        "same gate-control assignment",
        "Captured environment variables must be strings",
        "valid shell variable names",
        "blank or placeholder values",
        "`unsupported_or_partial_surfaces` entries that account for every surface listed",
        "object with non-empty `surface`, `status`, and `evidence` fields",
        "Top-level manifest, command entry, and unsupported-surface objects",
        "closed schemas",
        "unknown fields are rejected",
        "bare strings and placeholder values are rejected",
        "Optional unsupported-surface accounting fields",
        "non-empty strings or lists of non-empty strings",
        "Optional accounting lists must be non-empty",
        "placeholder optional accounting fields",
        "broker and controller ApiVersions omit them",
        "neither port has a dispatch/no-op path",
        "Each required surface must be covered by a distinct object",
        "catch-all entries cannot satisfy multiple known surfaces",
        "top-level Known Unsupported Or Partial Surfaces bullet list",
        "Duplicate objects for the same known surface are rejected",
        "Each `status` must explicitly",
        "unsupported, partial, blocked, fail-closed/not-advertised",
        "vague completion-style statuses are rejected",
        "status class must match the surface",
        "broker-only stateless replacement must remain partial/blocked",
        "live CI matrix/performance",
        "release-CI-required or blocked",
        "`known_data_loss_bug=false`",
        "`advertised_stub_api=false`",
        "`untriaged_durability_failure=false`",
        "`automq_complete=false`",
        "checked against the verifier catalog",
        "validated from the same clean tracked checkout",
        "release evidence manifest must be strict JSON",
        "rejected before schema validation",
        "duplicate JSON object keys are rejected",
        "Protocol static audit evidence also pins strict schema codegen JSON parsing",
        "before generated Zig protocol schemas are written",
        "codegen scripts must exit nonzero on schema parse errors",
        "cannot determine the current git commit",
        "tracked worktree cleanliness",
        "token-aware command validation",
        "same shell command segment",
        "Command strings must be single-line and unquoted",
        "CR/LF line breaks",
        "newline command separators",
        "shell quote characters",
        "quoted assignment words cannot masquerade as active gate environment",
        "Backslash escapes are rejected",
        "escaped assignment words",
        "cannot satisfy required gate environment",
        "Required command environment assignments",
        "must also be recorded in the manifest environment",
        "Repeated environment assignments are rejected",
        "cannot contain contradictory provenance",
        "Duplicate successful command entries",
        "same required gate",
        "success-dependent `&&` separators only",
        "only documented compose config commands may use multi-segment",
        "`;` and `||` cannot connect or trail",
        "pipes, backgrounding, redirection, subshell grouping, and command substitution",
        "including Bash `&>`/`&>>` combined redirects",
        "release gate commands must be direct invocations",
        "quoted/echoed command text cannot satisfy",
        "command strings must not embed release output marker text",
        "markers must come from captured command output",
        "fenced command block itself is parsed",
        "same token-aware command-shape checks",
        "gated harness skip message",
        "Every captured Zig build output",
        "successful `Build Summary: N/N steps succeeded`",
        "matching `N/N tests",
        "passed` counts",
        "must not contain any unsuccessful `Build Summary:` line",
        "non-negated build",
        "success line matching the invoked Zig build step",
        "`bench-compare success`",
        "Build static audit evidence also pins Docker compose release contracts",
        "`docker-compose.yml`, `benchmarks/kafka-compose.yml`, and",
        "`apache/kafka:4.0.2`",
        "`automqinc/automq:1.6.5`",
        "`minio/minio:RELEASE.2025-09-07T16-13-09Z`",
        "`minio/mc:RELEASE.2025-08-13T08-35-41Z`",
        "must not use `:latest`",
        "line-aware output marker matching",
        "stripped output lines or line prefixes",
        "rather than arbitrary substrings",
        "Captured skip markers are also line-aware",
        "Zig `Build Summary:` skip count",
        "Docker E2E section markers are line-aware",
        "`3-Node E2E Test Suite`",
        "`Results:`",
        "Docker E2E phase summaries must",
        "Results: <passed>/<total> passed, 0 failed",
        "`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1`",
        "`ZMQ_E2E_LOAD_SCALE_APPLY_MARKER`",
        "`ZMQ_E2E_LOAD_SCALE_RESTORE_MARKER`",
        "hook-owned evidence",
        "load_records=<count>",
        "Local and live-S3 benchmark markers are also line-aware",
        "ok: local benchmark gate source=command",
        "ok: live-S3 benchmark gate source=command",
        "`S3 WAL request volume`",
        "`Live S3 provider`",
        "`Live S3 request volume`",
        "local benchmark summary must appear exactly once",
        "live-S3 benchmark summary must appear exactly once",
        "S3 WAL request volume puts=<puts> lists=<lists>",
        "PartitionStore memory <rate>/s retained=<retained>",
        "Live S3 provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>",
        "command/env-selected `ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
        "live-S3 benchmark command must include",
        "`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
        "manifest environment must record the same values",
        "`ZMQ_BENCH_LIVE_S3_{ITERATIONS,PAYLOAD_BYTES}`",
        "matching positive-integer assignments",
        "`SCHEME` parsing as `http` or `https`",
        "`PATH_STYLE` parsing as `true` or `false`",
        "Live S3 put <MiB/s> MiB/s p99=<ms> ms objects=<objects>",
        "Live S3 get <MiB/s> MiB/s p99=<ms> ms requests/MiB=<value>",
        "Live S3 request volume puts=<puts> gets=<gets>",
        "request-count context",
        "Comparative benchmark table markers are also line-aware",
        "target labels must",
        "appear on the `COMPARISON:` line",
        "`Benchmark` marker must be a table header",
        "throughput (`tput`) row",
        "concrete `tput`, `p50`, and `p99`",
        "positive finite target measurements",
        "inside the `COMPARATIVE BENCHMARK GATE`",
        "gate section result",
        "within `ZMQ_S3_PROVIDER_REQUIRED_PROFILES`",
        "`ZMQ_S3_<PROFILE>_{RUN_LIVE_OUTAGE,RUN_PROCESS_CRASH,REQUIRE_LIST_PAGINATION,REQUIRE_MULTIPART_EDGE,RUN_MULTIPART_FAULT}`",
        "documented global fallback enable assignments",
        "within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`",
        "OAuth raw JWT fixtures must be strict JSON",
        "rejected before client execution",
        "`load`, `scale-in`, and `scale-out`",
        "cross-broker coverage",
        "`kcat`, `kafka-cli`, `kafka-python`, `confluent-kafka`, `java-kafka`, and `go-kafka`",
        "`basic`, `admin`, `groups`, `rebalance`, `transactions`, `security`, and `security-negative`",
        "per-required client tool probe markers",
        "ok: <client> probes (<semantics>) source=command",
        "ok: kafka-python probes",
        "ok: confluent-kafka probes",
        "passed for <tools> against <bootstrap> version=<version> source=command",
        "profile-selected tools",
        "selected bootstrap",
        "profile-scoped tool",
        "before the corresponding profile pass marker",
        "same profile block",
        "matching passed-for tools/bootstrap/version/source line",
        "client tool probe markers now require `source=command`",
        "markers plus required client security detail",
        "stale or contradictory semantic or security-negative",
        "client profile pass marker must be unique",
        "contradictory bootstrap/tool evidence",
        "ok: client matrix passed for <profiles> profile(s) source=command",
        "exactly matching `ZMQ_CLIENT_MATRIX_PROFILES`",
        "client matrix summary must appear exactly once",
        "ok: S3 provider matrix passed for <profiles> source=command",
        "exactly matching `ZMQ_S3_PROVIDER_PROFILES`",
        "S3 provider matrix summary must appear exactly once",
        "`ZMQ_S3_<PROFILE>_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
        "`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
        "non-`minio` provider profiles must set explicit profile/global S3 settings",
        "profile/global endpoint",
        "effective scheme/region/path-style settings",
        "`SCHEME` must parse as `http` or `https`",
        "`PATH_STYLE` must parse as",
        "exact semantic tokens inside client probe marker",
        "for every semantic named by `ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS`",
        "recognized profile-selected",
        "required client-tool probe markers",
        "tools whose profile did not enable that semantic",
        "per-required coverage markers",
        "network_partition=[<phases>]",
        "exactly matching `ZMQ_KRAFT_NETWORK_MATRIX`",
        "old_leader_rejoined=true",
        "old_leader_fresh_rejoin=true",
        "automq_old_leader_fresh_rejoin=true",
        "automq_stream_id=",
        "automq_deleted_stream_id=",
        "automq_stream_set_object_id=",
        "automq_node_id=",
        "automq_zone_router_epoch=",
        "must parse as non-placeholder non-negative integers",
        "controller_api_versions_checked=true",
        "all_controller_api_versions_checked=true",
        "controller_unsupported_checked=true",
        "all_controller_unsupported_checked=true",
        "dynamic_raft_voter_negative_checked=true",
        "broker_lifecycle_negative_checked=true",
        "controller_registration_negative_checked=true",
        "transactions_checked=5",
        "must parse as exactly `5`",
        "transaction_abort_checked=true",
        "txn_offset_commit_checked=true",
        "idempotent_producer_fencing=true",
        "consumer_group_heartbeat_checked=true",
        "share_fetch_session_checked=true",
        "share_state_apis_checked=true",
        "placeholder, empty result, or detached marker line",
        "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2",
        "second_offset=<positive> source=command",
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
        "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command",
        "ok: chaos s3-outage",
        "base_offset_negative=true serving=true source=command",
        "ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command",
        "ok: chaos network-partition source=command",
        "ok: chaos harness passed for <scenarios> source=command",
        "`ZMQ_CHAOS_REQUIRED_SCENARIOS` entry",
        "broker chaos harness summary must appear exactly once",
        "scenario summary must appear as its own stripped line",
        "markers cannot satisfy the scenario summary",
        "scenario detail markers must be unique per required scenario",
        "phase detail markers must be unique per phase",
        "repeated or contradictory",
        "ok: client matrix profile",
        "ok: S3 process crash/replacement harness passed (bucket=<bucket>",
        "S3 process-crash summary marker must appear exactly once",
        "killed_broker=true",
        "fresh_data_dir=true",
        "replacement_offset=<offset>",
        "recovered_payloads=2",
        "source=command",
        "replacement_offset` greater than `first_offset`",
        "ok: S3 provider live-suite profile",
        "ok: S3 provider profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command",
        "matching provider-settings profile",
        "unique within that profile block",
        "contradictory endpoint, bucket, outage, crash-recovery, or multipart-fault",
        "provider-settings profile marker must be unique",
        "contradictory endpoint/bucket evidence",
        "underlying chaos output includes the matching `ok: chaos live-s3-outage provider ...`",
        "ok: S3 provider outage detail profile",
        "fail_closed=true recovered=true source=command",
        "ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command",
        "underlying process-crash output includes the detailed",
        "bucket=<bucket>` matching the selected provider bucket",
        "ok: S3 provider process-crash detail profile",
        "recovered_payloads=2 source=command",
        "ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
        "ok: S3 provider list-pagination profile ... required=true completed=true source=command",
        "ok: S3 provider multipart-edge profile ... required=true completed=true source=command",
        "ok: S3 provider multipart-fault profile ... command_started=true completed=true injected=true recovered=true source=command",
        "ok: S3 multipart fault profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> injected=true recovered=true source=command",
        "ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command",
        "ok: E2E chaos phase",
        "down=true observed=<failed|survived> healed=true recovered=true",
        "expect=<fail|survive> source=command",
        "ok: E2E load/scale phase",
        "applied=true restored=true",
        "marker_payloads=hook-owned",
        "apply_source=<hook|fixture>",
        "restore_source=<hook|fixture> source=command",
        "ok: E2E chaos passed for <phases> phase(s) source=command",
        "ok: E2E load/scale passed for <phases> phase(s) source=command",
        "Docker E2E phase summaries must appear exactly once",
        "MinIO `8/8 tests passed` marker",
        "successful Zig `Build Summary:` line",
        "`ok: KRaft controller failover harness passed ... source=command` line",
        "KRaft failover summary must appear exactly once",
        "old_leader_rejoined=true",
        "old_leader_fresh_rejoin=true",
        "automq_old_leader_fresh_rejoin=true",
        "automq_stream_id=",
        "automq_deleted_stream_id=",
        "automq_stream_set_object_id=",
        "automq_node_id=",
        "automq_zone_router_epoch=",
        "must parse as non-placeholder non-negative integers",
        "controller_api_versions_checked=true",
        "all_controller_api_versions_checked=true",
        "controller_unsupported_checked=true",
        "all_controller_unsupported_checked=true",
        "dynamic_raft_voter_negative_checked=true",
        "broker_lifecycle_negative_checked=true",
        "controller_registration_negative_checked=true",
        "transactions_checked=5",
        "must parse as exactly `5`",
        "transaction_abort_checked=true",
        "txn_offset_commit_checked=true",
        "idempotent_producer_fencing=true",
        "consumer_group_heartbeat_checked=true",
        "share_fetch_session_checked=true",
        "share_state_apis_checked=true",
        "reassignment_topic=<topic>",
        "reassignment_old_owner_rejected=true",
        "reassignment_target_fetch_verified=true",
        "labels for every target named by `ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`",
        "profile marker is a closed key=value schema",
        "every required field must appear exactly once",
        "fields must not be blank",
        "unknown fields are rejected",
        "`trend thresholds:`",
        "`trend baseline:`",
        "Trend baseline artifacts must remain strict structured benchmark JSON",
        "non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity`",
        "rejected while parsing archived baselines",
        "writing current `benchmarks/results.json`",
        "Current result artifacts also record",
        "trend baseline must not resolve to the current `benchmarks/results.json` output path",
        "relative trend baseline paths resolve from the project",
        "numeric finite non-negative `throughput`, `p50`, and",
        "threshold variables must be nonblank, non-placeholder strings",
        "finite non-negative floats instead of falling back to defaults",
        "missing, non-numeric, non-finite, negative, or zero trend",
        "Current comparative result rows are validated",
        "non-numeric or non-finite throughput/latency metrics",
        "non-integral error/request/success counts",
        "zero throughput/latency values fail the gate",
        "Required target columns must stay in the same relative order as the",
        "comparative target catalogue",
        "Table target columns are limited to the known target headers",
        "Required ZMQ-to-baseline ratio columns",
        "`ZMQ/Kafka` and `ZMQ/AutoMQ`",
        "ratio columns are limited to known ZMQ-to-baseline pairs",
        "after target columns",
        "same comparative target catalogue order",
        "`COMPARISON:` line target labels",
        "must also follow the comparative target catalogue order",
    ):
        if not normalized_contains(criteria, fragment):
            raise AssertionError(f"release criteria missing evidence contract {fragment!r}")

    for forbidden in ("TBD", "TODO"):
        if forbidden in criteria:
            raise AssertionError(f"release criteria must not contain {forbidden}")
    if "/path/to/" in required_commands_block(criteria):
        raise AssertionError(
            "release criteria required command block must not contain placeholder paths"
        )
    if re.search(r"<[^>\n]+>", required_commands_block(criteria)) is not None:
        raise AssertionError(
            "release criteria required command block must not contain angle-bracket placeholders"
        )
    assert_required_command_block_matches_validator(criteria)


def assert_required_build_steps_defined():
    with open(BUILD_ZIG_PATH, "r", encoding="utf-8") as f:
        build_zig = f.read()

    defined_steps = set(re.findall(r'\bb\.step\("([^"]+)"', build_zig))
    required_steps = set()
    for requirement in REQUIRED_COMMANDS:
        for fragment in requirement["required"]:
            match = re.search(r"\bbuild\s+([A-Za-z0-9_-]+)\s+--summary\s+all", fragment)
            if match:
                required_steps.add(match.group(1))

    missing = sorted(required_steps - defined_steps)
    if missing:
        raise AssertionError(
            "release criteria require undefined build steps: " + ", ".join(missing)
        )


def assert_file_contains(path, label, fragments):
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    missing = [fragment for fragment in fragments if fragment not in text]
    if missing:
        raise AssertionError(
            f"{label} missing release evidence marker fragments: "
            + ", ".join(missing)
        )


def assert_live_harness_marker_contracts():
    assert_file_contains(
        CHAOS_TEST_PATH,
        "broker chaos harness",
        (
            "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2",
            "source=command",
            "ok: chaos slow-partial-client partial_frame=true",
            "ok: chaos clock-skewed-records future_timestamp=true",
            "ok: chaos s3-outage",
            "ok: chaos live-s3-outage provider endpoint=",
            "ok: chaos live-s3-outage down=true healed=true",
            "def network_partition_phase_marker(",
            "ok: chaos network-partition phase {phase} down=true ",
            "observed={observed} healed=true recovered=true expect={expect}",
            "source=command",
            "ok: chaos network-partition source=command",
            "ok: chaos harness passed for {', '.join(scenarios)} source=command",
        ),
    )
    assert_file_contains(
        CLIENT_MATRIX_TEST_PATH,
        "client matrix harness",
        (
            "ok: kcat probes ({semantics_csv()}) source=command",
            "ok: kafka CLI probes ({semantics_csv()}) source=command",
            "ok: kafka-python probes ({semantics_csv()}) source=command",
            "ok: confluent-kafka probes ({semantics_csv()}) source=command",
            "ok: java-kafka probes ({semantics_csv()}) source=command",
            "ok: go-kafka probes ({semantics_csv()}) source=command",
            "ok: client matrix profile",
            "ok: client matrix passed",
        ),
    )
    assert_file_contains(
        S3_PROVIDER_MATRIX_TEST_PATH,
        "S3 provider matrix harness",
        (
            "ok: S3 provider live-suite profile",
            "ok: S3 provider profile",
            "def provider_summary_settings(",
            "endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} ",
            "bucket={env['ZMQ_S3_BUCKET']} scheme={scheme} region={region} ",
            "path_style={path_style}",
            "def outage_detail_marker(",
            "ok: S3 provider outage detail profile {profile}",
            "fail_closed=true recovered=true source=command",
            "ok: S3 provider outage profile {profile} down=true healed=true fail_closed=true recovered=true source=command",
            "def process_crash_detail_marker(",
            "ok: S3 provider process-crash detail profile {profile}",
            "recovered_payloads={fields['recovered_payloads']} source=command",
            "ok: S3 provider process-crash profile {profile} killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
            "ok: S3 provider list-pagination profile {profile} required=true completed=true source=command",
            "ok: S3 provider multipart-edge profile {profile} required=true completed=true source=command",
            "ok: S3 provider multipart-fault profile {profile} command_started=true completed=true injected=true recovered=true source=command",
            "def require_multipart_fault_evidence(",
            "ok: S3 multipart fault profile {profile}",
            "source=command",
            "return marker",
            "fault_marker = require_multipart_fault_evidence(",
            "ok: S3 provider matrix passed",
            "ok: S3 provider matrix passed for {', '.join(profiles)} source=command",
        ),
    )
    assert_file_contains(
        S3_PROCESS_CRASH_TEST_PATH,
        "S3 process-crash harness",
        (
            "def process_crash_summary(",
            "ok: S3 process crash/replacement harness passed",
            "killed_broker=true",
            "fresh_data_dir=true",
            "recovered_payloads=2",
        ),
    )
    assert_file_contains(
        KRAFT_FAILOVER_TEST_PATH,
        "KRaft failover harness",
        (
            "def network_partition_phase_marker(",
            "ok: KRaft network partition phase {result['phase']} down=true ",
            "observed={observed} healed={healed} ",
            "healed_leader={result['leader_id']} healed_fetch={healed_fetch} ",
            "expect={result['expect']} source=command",
            "ok: KRaft controller failover harness passed",
            "network_partition=",
            "old_leader_rejoined=true",
            "old_leader_fresh_rejoin=true",
            "automq_stream_id=",
            "automq_deleted_stream_id=",
            "automq_stream_set_object_id=",
            "{'true' if automq_result['old_leader_fresh_rejoin'] else 'false'}) ",
            "source=command",
            "automq_node_id=",
            "automq_zone_router_epoch=",
            "controller_api_versions_checked=true",
            "all_controller_api_versions_checked=true",
            "controller_unsupported_checked=true",
            "all_controller_unsupported_checked=true",
            "dynamic_raft_voter_negative_checked=true",
            "broker_lifecycle_negative_checked=true",
            "controller_registration_negative_checked=true",
            "transactions_checked=5",
            "transaction_abort_checked=true",
            "txn_offset_commit_checked=true",
            "idempotent_producer_fencing=true",
            "consumer_group_heartbeat_checked=true",
            "share_fetch_session_checked=true",
            "share_state_apis_checked=true",
            "reassignment_topic=",
            "reassignment_target=",
            "reassignment_target_offset=",
            "reassignment_old_owner_rejected=true",
            "reassignment_target_fetch_verified=true",
            "automq_old_leader_fresh_rejoin=",
        ),
    )
    assert_file_contains(
        E2E_TEST_PATH,
        "Docker E2E harness",
        (
            "def e2e_chaos_phase_marker(",
            "ok: E2E chaos phase {phase['name']} down=true ",
            "observed={observed} healed={healed_text} recovered={recovered_text} ",
            "expect={phase['expect']} source=command",
            "ok: E2E load/scale phase",
            "applied=true restored=true",
            "marker_payloads=hook-owned",
            "restore_source={phase['restore_source']} source=command",
            "ZMQ_E2E_LOAD_SCALE_APPLY_MARKER",
            "ZMQ_E2E_LOAD_SCALE_RESTORE_MARKER",
            "wait_for_existing_cross_node_payload(",
            "ok: E2E chaos passed for {', '.join(phase['name'] for phase in phases)} phase(s) source=command",
            "ok: E2E load/scale passed for {', '.join(phase['name'] for phase in phases)} phase(s) source=command",
            "[Test m] Cross-broker chaos phases",
            "[Test n] Live load/scale phases",
            "Results:",
        ),
    )
    assert_file_contains(
        BENCHMARK_MAIN_PATH,
        "Zig benchmark harness",
        (
            "S3 WAL request volume",
            "PartitionStore memory",
            "Live S3 provider endpoint={s}:{d} bucket={s} scheme={s} region={s} path_style={s}",
            "Live S3 put",
            "Live S3 get",
            "Live S3 request volume",
            "Live S3 request volume   puts={d} gets={d} requests/MiB={d:.2}",
        ),
    )
    assert_file_contains(
        BENCHMARK_COMPARE_PATH,
        "comparative benchmark harness",
        (
            "ZMQ (Zig)",
            "Apache Kafka",
            "AutoMQ (Java)",
            "COMPARISON:",
            "Benchmark",
            "Produce (fresh)",
            "thresholds:",
            "trend thresholds:",
            "COMPARATIVE BENCHMARK GATE",
            "result: pass",
        ),
    )


def sample_manifest():
    environment = {
        "ZMQ_RUN_CHAOS_TESTS": "1",
        "ZMQ_RUN_CLIENT_MATRIX": "1",
        "ZMQ_RUN_MINIO_TESTS": "1",
        "ZMQ_S3_REQUIRE_MULTIPART_EDGE": "1",
        "ZMQ_S3_REQUIRE_LIST_PAGINATION": "1",
        "ZMQ_RUN_PROCESS_CRASH_TESTS": "1",
        "ZMQ_RUN_S3_PROVIDER_MATRIX": "1",
        "ZMQ_RUN_KRAFT_FAILOVER_TESTS": "1",
        "ZMQ_RUN_E2E_TESTS": "1",
        "ZMQ_RUN_BENCH_LIVE_S3": "1",
        "ZMQ_RUN_BENCH_COMPARE": "1",
        "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES": "leader-isolation,broker-link",
        "ZMQ_KRAFT_NETWORK_MATRIX": "leader-isolation,broker-link",
        "ZMQ_KRAFT_NETWORK_BROKER_LINK_EXPECT": "survive",
        "ZMQ_CHAOS_REQUIRED_SCENARIOS": (
            "sigkill-restart,slow-partial-client,clock-skewed-records,"
            "s3-outage,network-partition"
        ),
        "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES": "broker-link",
        "ZMQ_CHAOS_NETWORK_MATRIX": "broker-link",
        "ZMQ_CHAOS_NETWORK_BROKER_LINK_DOWN": "true",
        "ZMQ_CHAOS_NETWORK_BROKER_LINK_UP": "true",
        "ZMQ_E2E_REQUIRED_CHAOS_PHASES": "cross-broker",
        "ZMQ_E2E_CHAOS_MATRIX": "cross-broker",
        "ZMQ_E2E_CHAOS_CROSS_BROKER_DOWN": "true",
        "ZMQ_E2E_CHAOS_CROSS_BROKER_UP": "true",
        "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES": "load,scale-in,scale-out",
        "ZMQ_E2E_LOAD_SCALE_MATRIX": "load,scale-in,scale-out",
        "ZMQ_E2E_LOAD_SCALE_APPLY": "true",
        "ZMQ_E2E_LOAD_SCALE_RESTORE": "true",
        "ZMQ_KRAFT_NETWORK_DOWN": "true",
        "ZMQ_KRAFT_NETWORK_UP": "true",
        "ZMQ_S3_PROVIDER_REQUIRED_PROFILES": "minio,aws_us_east_1",
        "ZMQ_S3_PROVIDER_PROFILES": "minio,aws_us_east_1",
        "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES": "aws_us_east_1",
        "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES": "aws_us_east_1",
        "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES": "aws_us_east_1",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES": "aws_us_east_1",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES": "aws_us_east_1",
        "ZMQ_S3_MINIO_ENDPOINT": "127.0.0.1",
        "ZMQ_S3_MINIO_PORT": "9000",
        "ZMQ_S3_MINIO_BUCKET": "zmq-minio-it",
        "ZMQ_S3_AWS_US_EAST_1_ENDPOINT": "s3.amazonaws.com",
        "ZMQ_S3_AWS_US_EAST_1_PORT": "443",
        "ZMQ_S3_AWS_US_EAST_1_BUCKET": "zmq-aws-it",
        "ZMQ_S3_AWS_US_EAST_1_SCHEME": "https",
        "ZMQ_S3_AWS_US_EAST_1_REGION": "us-east-1",
        "ZMQ_S3_AWS_US_EAST_1_PATH_STYLE": "false",
        "ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE": "1",
        "ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH": "1",
        "ZMQ_S3_AWS_US_EAST_1_REQUIRE_LIST_PAGINATION": "1",
        "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE": "1",
        "ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT": "1",
        "ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN": "true",
        "ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP": "true",
        "ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD": "ci-s3-multipart-fault aws_us_east_1",
        "ZMQ_S3_ENDPOINT": "s3-bench.example.test",
        "ZMQ_S3_PORT": "9443",
        "ZMQ_S3_BUCKET": "zmq-live-bench",
        "ZMQ_S3_SCHEME": "http",
        "ZMQ_S3_REGION": "us-east-1",
        "ZMQ_S3_PATH_STYLE": "true",
        "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS": (
            "kcat,kafka-cli,kafka-python,confluent-kafka,java-kafka,go-kafka"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS": (
            "basic,admin,groups,rebalance,transactions,security,security-negative"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,"
            "confluent_2_3,java_3_7,go_1_21"
        ),
        "ZMQ_CLIENT_MATRIX_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,"
            "confluent_2_3,java_3_7,go_1_21"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,"
            "confluent_2_3,java_3_7,go_1_21"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,confluent_2_3,java_3_7"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,confluent_2_3,java_3_7"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,confluent_2_3,java_3_7"
        ),
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES": (
            "kcat_sec,kafka_cli_sec,kafka_python_sec,confluent_2_3,java_3_7"
        ),
        "ZMQ_CLIENT_MATRIX_BOOTSTRAP": "localhost:9092",
        "ZMQ_CLIENT_MATRIX_PYTHON": "/usr/bin/python3",
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS": "kcat",
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SEMANTICS": (
            "basic,security,security-negative"
        ),
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_VERSION": "kcat-1.7.1",
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SASL_MECHANISM": "OAUTHBEARER",
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_OAUTHBEARER_CONFIG": (
            "principal=matrix-user lifeSeconds=3600"
        ),
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_BAD_OAUTHBEARER_CONFIG": (
            "principalClaimName=azp principal=matrix-user lifeSeconds=3600"
        ),
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_TOOLS": "kafka-cli",
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_SEMANTICS": (
            "basic,admin,security,security-negative"
        ),
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_VERSION": "apache-kafka-cli-3.7.1",
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_SASL_MECHANISM": "OAUTHBEARER",
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_OAUTH_JAAS_CONFIG": (
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
            "required unsecuredLoginStringClaim_sub=matrix-user "
            "unsecuredLoginNumberClaim_exp=9999999999;"
        ),
        "ZMQ_CLIENT_MATRIX_KAFKA_CLI_SEC_BAD_OAUTH_JAAS_CONFIG": (
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
            "required unsecuredLoginStringClaim_sub=matrix-user "
            "unsecuredLoginNumberClaim_exp=1000;"
        ),
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_TOOLS": "kafka-python",
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_SEMANTICS": (
            "basic,admin,groups,security,security-negative"
        ),
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_VERSION": "kafka-python-2.0.2",
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_SASL_MECHANISM": "OAUTHBEARER",
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_OAUTH_TOKEN": (
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6OTk5OTk5OTk5OX0."
        ),
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_BAD_OAUTH_TOKEN": (
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6MTAwMH0."
        ),
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_TOOLS": "confluent-kafka",
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_SEMANTICS": (
            "basic,admin,groups,rebalance,transactions,security,security-negative"
        ),
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_VERSION": "confluent-kafka-2.3.0",
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_SASL_MECHANISM": "OAUTHBEARER",
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_OAUTH_TOKEN": (
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6OTk5OTk5OTk5OX0."
        ),
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_BAD_OAUTH_TOKEN": (
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6MTAwMH0."
        ),
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_TOOLS": "java-kafka",
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_SEMANTICS": (
            "basic,admin,rebalance,transactions,security,security-negative"
        ),
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_VERSION": "apache-kafka-clients-3.7.1",
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_JAVA_CLASSPATH": "/opt/kafka-3.7/libs/*",
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_SASL_MECHANISM": "OAUTHBEARER",
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_OAUTH_JAAS_CONFIG": (
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
            "required unsecuredLoginStringClaim_sub=matrix-user "
            "unsecuredLoginNumberClaim_exp=9999999999;"
        ),
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_BAD_OAUTH_JAAS_CONFIG": (
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
            "required unsecuredLoginStringClaim_sub=matrix-user "
            "unsecuredLoginNumberClaim_exp=1000;"
        ),
        "ZMQ_CLIENT_MATRIX_GO_1_21_TOOLS": "go-kafka",
        "ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS": "basic,admin,groups",
        "ZMQ_CLIENT_MATRIX_GO_1_21_VERSION": "segmentio-kafka-go-v0.4.47",
        "ZMQ_CLIENT_MATRIX_GO_1_21_GO_MODULE": (
            "github.com/segmentio/kafka-go@v0.4.47"
        ),
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS": "zmq,kafka,automq",
        "ZMQ_BENCH_COMPARE_REQUIRE_TREND": "1",
        "ZMQ_BENCH_COMPARE_TREND_BASELINE": "benchmarks/results-previous.json",
    }
    commands = []
    for requirement in REQUIRED_COMMANDS:
        command = sample_requirement_command(requirement, environment)
        commands.append({
            "command": command,
            "exit_code": 0,
            "output": sample_command_output(requirement),
        })
    return {
        "commit": "a" * 40,
        "environment": environment,
        "commands": commands,
        "unsupported_or_partial_surfaces": [
            {
                "surface": "ZooKeeper-era inter-broker API keys 4-7",
                "status": "generated-only in KRaft mode and not advertised",
                "evidence": (
                    "broker and controller ApiVersions omit them, neither port "
                    "has a dispatch/no-op path, and direct broker/controller "
                    "probes fail closed before body decode"
                ),
            },
            {
                "surface": "broker-only stateless replacement",
                "status": "partial coverage",
                "evidence": (
                    "local cache/state assumptions remain outside the covered "
                    "S3/quorum replay paths"
                ),
            },
            {
                "surface": (
                    "external-client, secured-client, and OAuth profile execution "
                    "live matrix"
                ),
                "status": "release-CI-required",
                "evidence": (
                    "external-client, secured-client, and OAuth profile execution "
                    "must run in the live matrix before release"
                ),
            },
            {
                "surface": "cross-broker chaos and multi-broker chaos live matrix",
                "status": "release-CI-required",
                "evidence": (
                    "scheduled cross-broker chaos and broader multi-broker chaos "
                    "must run in release CI"
                ),
            },
            {
                "surface": "Docker E2E load/scale live orchestration",
                "status": "release-CI-required",
                "evidence": "E2E load/scale live orchestration must run in release CI",
            },
            {
                "surface": "KRaft failover network matrix",
                "status": "release-CI-required",
                "evidence": "KRaft failover network matrices must run in release CI",
            },
            {
                "surface": "live S3 provider outage and multipart-fault profile execution",
                "status": "release-CI-required",
                "evidence": (
                    "scheduled live provider outage and multipart-fault profile execution "
                    "must run against release provider profiles"
                ),
            },
            {
                "surface": "comparative Kafka/AutoMQ performance profile/trend gates",
                "status": "release-CI-required",
                "evidence": (
                    "comparative Kafka/AutoMQ performance profile/trend gates "
                    "must run against archived baselines"
                ),
            },
        ],
        "known_data_loss_bug": False,
        "advertised_stub_api": False,
        "untriaged_durability_failure": False,
        "automq_complete": False,
    }


def sample_requirement_command(requirement, environment):
    command_fragments = list(requirement["required"])
    if requirement["label"] == "comparative benchmark gate":
        baseline = environment["ZMQ_BENCH_COMPARE_TREND_BASELINE"]
        targets = environment["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"]
        gate_assignments = [
            f"ZMQ_BENCH_COMPARE_ENFORCE_GATES={environment['ZMQ_BENCH_COMPARE_ENFORCE_GATES']}"
        ] if (
            isinstance(environment.get("ZMQ_BENCH_COMPARE_ENFORCE_GATES"), str)
            and environment["ZMQ_BENCH_COMPARE_ENFORCE_GATES"].strip()
        ) else []
        threshold_assignments = [
            f"{env_name}={environment[env_name]}"
            for env_name in BENCHMARK_THRESHOLD_ENV_VARS
            if isinstance(environment.get(env_name), str)
            and environment[env_name].strip()
        ]
        command_fragments = [
            (
                f"ZMQ_BENCH_COMPARE_TREND_BASELINE={baseline}"
                if fragment == "ZMQ_BENCH_COMPARE_TREND_BASELINE"
                else fragment
            )
            for fragment in command_fragments
        ]
        command_fragments.insert(0, f"ZMQ_BENCH_COMPARE_REQUIRED_TARGETS={targets}")
        command_fragments = gate_assignments + threshold_assignments + command_fragments
    if requirement["label"] == "live-S3 benchmark gate":
        optional_live_s3_benchmark_env = [
            env_name
            for env_name in sorted(POSITIVE_INTEGER_ENV_VARS)
            if isinstance(environment.get(env_name), str)
            and environment[env_name].strip()
        ]
        command_fragments = [
            f"ZMQ_S3_ENDPOINT={environment['ZMQ_S3_ENDPOINT']}",
            f"ZMQ_S3_PORT={environment['ZMQ_S3_PORT']}",
            f"ZMQ_S3_BUCKET={environment['ZMQ_S3_BUCKET']}",
            f"ZMQ_S3_SCHEME={environment['ZMQ_S3_SCHEME']}",
            f"ZMQ_S3_REGION={environment['ZMQ_S3_REGION']}",
            f"ZMQ_S3_PATH_STYLE={environment['ZMQ_S3_PATH_STYLE']}",
            *[
                f"{env_name}={environment[env_name]}"
                for env_name in optional_live_s3_benchmark_env
            ],
            *command_fragments,
        ]
    if requirement["label"] == "S3 provider matrix":
        provider_matrix_command_env = (
            "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
            "ZMQ_S3_PROVIDER_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
        )
        command_fragments = [
            f"{env_name}={environment[env_name]}"
            for env_name in provider_matrix_command_env
        ] + [
            f"{env_name}={environment[env_name]}"
            for env_name in s3_profile_enable_command_env_names(environment)
        ] + command_fragments
    if requirement["label"] == "external client matrix":
        client_matrix_command_env = (
            "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES",
            "ZMQ_CLIENT_MATRIX_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
            "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
            "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
            "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
        )
        command_fragments = [
            f"{env_name}={environment[env_name]}"
            for env_name in client_matrix_command_env
        ] + command_fragments
    if requirement["label"] == "broker chaos harness":
        chaos_command_env = (
            "ZMQ_CHAOS_REQUIRED_SCENARIOS",
            "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
            "ZMQ_CHAOS_NETWORK_MATRIX",
        )
        command_fragments = [
            f"{env_name}={environment[env_name]}"
            for env_name in chaos_command_env
        ] + command_fragments
    if requirement["label"] == "KRaft failover gate":
        kraft_command_env = (
            "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
            "ZMQ_KRAFT_NETWORK_MATRIX",
        )
        command_fragments = [
            f"{env_name}={environment[env_name]}"
            for env_name in kraft_command_env
        ] + command_fragments
    if requirement["label"] == "Docker E2E gate":
        e2e_command_env = [
            "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
            "ZMQ_E2E_CHAOS_MATRIX",
            "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
        ]
        if (
            isinstance(environment.get("ZMQ_E2E_LOAD_SCALE_MATRIX"), str)
            and environment["ZMQ_E2E_LOAD_SCALE_MATRIX"].strip()
        ):
            e2e_command_env.append("ZMQ_E2E_LOAD_SCALE_MATRIX")
        elif bool_environment_value(environment, "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE") is not True:
            e2e_command_env.append("ZMQ_E2E_LOAD_SCALE_MATRIX")
        if bool_environment_value(environment, "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE") is True:
            e2e_command_env.append("ZMQ_E2E_LOAD_SCALE_USE_FIXTURE")
        command_fragments = [
            f"{env_name}={environment[env_name]}"
            for env_name in e2e_command_env
        ] + command_fragments
    if any(fragment.startswith("echo ") for fragment in command_fragments):
        return " && ".join(command_fragments)
    return " ".join(command_fragments)


def sample_command_output(requirement):
    if requirement["label"] == "S3 process-crash replacement gate":
        return "\n".join(
            [
                "ok: S3 process-crash replacement gate",
                "Build Summary: 1/1 steps succeeded",
                "test-s3-process-crash success",
                (
                    "ok: S3 process crash/replacement harness passed "
                    "(bucket=zmq-crash-release, topic=s3-crash-release, "
                    "group=s3-crash-group-release, killed_broker=true, "
                    "fresh_data_dir=true, first_offset=0, committed_offset=1, "
                    "replacement_offset=1, recovered_payloads=2) source=command"
                ),
            ]
        )

    if requirement["label"] == "KRaft failover gate":
        lines = [
            "ok: KRaft failover gate",
            "Build Summary: 1/1 steps succeeded",
            "test-kraft-failover success",
        ]
        lines.extend(SAMPLE_ENVIRONMENT_OUTPUT_MARKERS.get(requirement["label"], []))
        lines.append(
            (
                "ok: KRaft controller failover harness passed "
                "(network_partition=[leader-isolation,broker-link], "
                "old_leader=1, new_leader=2, restarted_controller=1, "
                "epoch=3, automq_old_leader=1, automq_new_leader=2, "
                "old_leader_rejoined=true, old_leader_fresh_rejoin=true, "
                "automq_stream_id=21, automq_deleted_stream_id=22, "
                "automq_stream_set_object_id=42, "
                "automq_node_id=1, automq_zone_router_epoch=3, "
                "allocate_producer_ids_checked=true, "
                "allocate_producer_ids_follower_rejection_checked=true, "
                "describe_quorum_v2_checked=true, "
                "fetch_snapshot_v1_checked=true, "
                "all_controller_fetch_snapshot_v1_checked=true, "
                "controller_api_versions_checked=true, "
                "all_controller_api_versions_checked=true, "
                "controller_unsupported_checked=true, "
                "all_controller_unsupported_checked=true, "
                "controller_unsupported_cases=[52:2,53:2,54:2,55:3,"
                "59:2,62:3,63:2,64:1,67:1,70:1,80:1,81:1,82:1,"
                "4:0,4:7,5:0,5:4,6:0,6:8,7:0,7:3,71:0,72:0], "
                "dynamic_raft_voter_negative_checked=true, "
                "dynamic_raft_voter_follower_rejection_checked=true, "
                "all_controller_describe_quorum_v2_checked=true, "
                "broker_lifecycle_negative_checked=true, "
                "broker_lifecycle_follower_rejection_checked=true, "
                "controller_registration_negative_checked=true, "
                "controller_registration_follower_rejection_checked=true, "
                "broker_registration_follower_rejection_checked=true, "
                "broker_non_broker_api_rejection_checked=true, "
                "broker_non_broker_api_rejection_cases=[56:3,58:0,59:1,"
                "62:4,63:1,64:0,67:0,70:0,80:0,81:0,82:0], "
                "committed_offset=1, "
                "transactions_checked=5, "
                "transaction_introspection_checked=true, "
                "transaction_abort_checked=true, "
                "txn_offset_commit_checked=true, "
                "offset_fetch_v8_grouped_checked=true, "
                "log_position_apis_checked=true, "
                "delete_records_checked=true, "
                "delete_topics_checked=true, "
                "create_topics_checked=true, "
                "create_partitions_checked=true, "
                "client_quotas_checked=true, "
                "scram_credentials_checked=true, "
                "client_telemetry_checked=true, "
                "delegation_tokens_checked=true, "
                "finalized_features_checked=true, "
                "acl_admin_checked=true, "
                "config_admin_checked=true, "
                "describe_topic_partitions_checked=true, "
                "describe_configs_checked=true, "
                "describe_log_dirs_checked=true, "
                "alter_replica_log_dirs_checked=true, "
                "assign_replicas_to_dirs_checked=true, "
                "elect_leaders_checked=true, "
                "describe_cluster_checked=true, "
                "idempotent_producer_fencing=true, "
                "describe_producers_checked=true, "
                "delete_groups_checked=true, "
                "classic_group_heartbeats=true, "
                "group_describe_checked=true, "
                "consumer_group_describe_checked=true, "
                "list_groups_checked=true, "
                "find_coordinator_checked=true, "
                "share_group_heartbeat_checked=true, "
                "share_group_describe_checked=true, "
                "consumer_group_heartbeat_checked=true, "
                "share_fetch_session_checked=true, "
                "share_acknowledge_checked=true, "
                "share_state_apis_checked=true, "
                "kip848_describe_checked=true, "
                "kip848_rejoin_checked=true, "
                "kip848_rack_checked=true, "
                "kip848_owned_assignment_checked=true, "
                "kip848_subscription_update_checked=true, "
                "kip848_negative_join_checked=true, "
                "kip848_static_rejoin_checked=true, "
                "offset_commit_v9_member_checked=true, "
                "offset_fetch_v9_member_checked=true, "
                "reassignment_topic=kraft-reassign-release, "
                "reassignment_target=1, reassignment_target_offset=1, "
                "reassignment_old_owner_rejected=true, "
                "reassignment_target_fetch_verified=true, "
                "automq_old_leader_fresh_rejoin=true) source=command"
            )
        )
        return "\n".join(lines)

    if requirement["label"] == "local benchmark gate":
        return "\n".join(
            [
                "Build Summary: 1/1 steps succeeded",
                "bench success",
                "S3 WAL request volume    puts=200 lists=0 requests/MiB=251.70",
                "PartitionStore memory         80964/s  retained=514 KiB  peak=518 KiB  max_current=514 KiB",
                "=== Benchmarks complete ===",
                "ok: local benchmark gate source=command",
            ]
        )

    if requirement["label"] == "live-S3 benchmark gate":
        return "\n".join(
            [
                "Build Summary: 1/1 steps succeeded",
                "bench success",
                (
                    "Live S3 provider endpoint=s3-bench.example.test:9443 "
                    "bucket=zmq-live-bench scheme=http region=us-east-1 "
                    "path_style=true"
                ),
                "Live S3 put                12.50 MiB/s  p99=   10.00 ms  objects=20",
                "Live S3 get                14.00 MiB/s  p99=    8.00 ms  requests/MiB=40.00",
                "Live S3 request volume   puts=20 gets=20 requests/MiB=40.00",
                "=== Benchmarks complete ===",
                "ok: local benchmark gate source=command",
                "ok: live-S3 benchmark gate source=command",
            ]
        )

    if requirement["label"] == "comparative benchmark gate":
        return "\n".join(
            [
                "ok: comparative benchmark gate",
                "Build Summary: 1/1 steps succeeded",
                "bench-compare success",
                "  COMPARISON: ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)",
                "",
                "  Benchmark              Metric          ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
                "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x          1.25x",
                "                          p50           1.20ms       1.40ms       1.50ms          0.86x          0.80x",
                "                          p99           4.00ms       4.50ms       5.00ms          0.89x          0.80x",
                "  Produce (reuse)        tput        8,000/s      7,000/s      6,000/s          1.14x          1.33x",
                "                          p50           2.00ms       2.20ms       2.40ms          0.91x          0.83x",
                "                          p99           8.00ms       9.00ms      10.00ms          0.89x          0.80x",
                "  Produce (fresh)        tput        2,000/s      1,800/s      1,700/s          1.11x          1.18x",
                "                          p50           4.00ms       4.50ms       5.00ms          0.89x          0.80x",
                "                          p99          15.00ms      18.00ms      20.00ms          0.83x          0.75x",
                "  Fetch                  tput        7,000/s      6,500/s      6,000/s          1.08x          1.17x",
                "                          p50           1.50ms       1.80ms       2.00ms          0.83x          0.75x",
                "                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x",
                "  Metadata               tput        9,000/s      8,000/s      7,500/s          1.12x          1.20x",
                "                          p50           1.00ms       1.20ms       1.30ms          0.83x          0.77x",
                "                          p99           3.50ms       4.00ms       4.50ms          0.88x          0.78x",
                "  COMPARATIVE BENCHMARK GATE",
                "  thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%",
                "  trend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x",
                "  trend baseline: benchmarks/results-previous.json",
                "  result: pass",
                "  Results saved to benchmarks/results.json",
                (
                    "  ok: comparative benchmark profile "
                    "selected=zmq,kafka,automq "
                    "required=zmq,kafka,automq "
                    "results_targets=zmq,kafka,automq "
                    "results=benchmarks/results.json "
                    "gates_enforced=true "
                    "trend_required=true "
                    "trend_baseline=benchmarks/results-previous.json "
                    "iterations=api_versions:5000,produce_single:5000,produce_fresh:2000,fetch:3000,metadata:3000 "
                    "warmup=api_versions:100,produce_single:100,produce_fresh:50,fetch:100,metadata:100 "
                    "source=command"
                ),
            ]
        )

    if requirement["label"] == "broker chaos harness":
        return "\n".join(
            [
                "ok: broker chaos harness",
                "Build Summary: 1/1 steps succeeded",
                "test-chaos success",
                *SAMPLE_ENVIRONMENT_OUTPUT_MARKERS.get(requirement["label"], []),
            ]
        )

    label_marker = f"ok: {requirement['label']}"
    lines = []
    if label_marker not in requirement.get("output_markers", []):
        lines.append(label_marker)
    lines.append("Build Summary: 1/1 steps succeeded")
    success_label = requirement_zig_build_step(requirement) or requirement["label"]
    lines.append(f"{success_label} success")
    output_markers = list(requirement.get("output_markers", []))
    if requirement["label"] == "Docker E2E gate":
        output_markers = [marker for marker in output_markers if marker != "Results:"]
    lines.extend(output_markers)
    lines.extend(SAMPLE_ENVIRONMENT_OUTPUT_MARKERS.get(requirement["label"], []))
    return "\n".join(lines)


SAMPLE_ENVIRONMENT_OUTPUT_MARKERS = {
    "broker chaos harness": [
        "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=1 source=command",
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
        "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command",
        "ok: chaos s3-outage rejected=true error_code=56 base_offset_negative=true serving=true source=command",
        "ok: chaos network-partition phase broker-link down=true observed=failed healed=true recovered=true expect=fail source=command",
        "ok: chaos network-partition source=command",
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition source=command"
        ),
    ],
    "external client matrix": [
        "ok: kcat probes (basic,security,security-negative) source=command",
        (
            "ok: client security detail profile kcat_sec "
            "tool=kcat protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER "
            "oauth=true positive=true security_negative=true "
            "oauth_negative=true sasl_negative=false tls_negative=false "
            "acl_negative=false source=command"
        ),
        "ok: client matrix profile kcat_sec passed for kcat against localhost:9092 version=kcat-1.7.1 source=command",
        "ok: kafka CLI probes (basic,admin,security,security-negative) source=command",
        (
            "ok: client security detail profile kafka_cli_sec "
            "tool=kafka-cli protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER "
            "oauth=true positive=true security_negative=true "
            "oauth_negative=true sasl_negative=false tls_negative=false "
            "acl_negative=false source=command"
        ),
        "ok: client matrix profile kafka_cli_sec passed for kafka-cli against localhost:9092 version=apache-kafka-cli-3.7.1 source=command",
        "ok: kafka-python probes (basic,admin,groups,security,security-negative) source=command",
        (
            "ok: client security detail profile kafka_python_sec "
            "tool=kafka-python protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER "
            "oauth=true positive=true security_negative=true "
            "oauth_negative=true sasl_negative=false tls_negative=false "
            "acl_negative=false source=command"
        ),
        "ok: client matrix profile kafka_python_sec passed for kafka-python against localhost:9092 version=kafka-python-2.0.2 source=command",
        "ok: confluent-kafka probes (basic,admin,groups,rebalance,transactions,security,security-negative) source=command",
        (
            "ok: client security detail profile confluent_2_3 "
            "tool=confluent-kafka protocol=SASL_PLAINTEXT "
            "mechanism=OAUTHBEARER oauth=true positive=true "
            "security_negative=true oauth_negative=true "
            "sasl_negative=false tls_negative=false acl_negative=false "
            "source=command"
        ),
        "ok: client matrix profile confluent_2_3 passed for confluent-kafka against localhost:9092 version=confluent-kafka-2.3.0 source=command",
        "ok: java-kafka probes (basic,admin,rebalance,transactions,security,security-negative) source=command",
        (
            "ok: client security detail profile java_3_7 "
            "tool=java-kafka protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER "
            "oauth=true positive=true security_negative=true "
            "oauth_negative=true sasl_negative=false tls_negative=false "
            "acl_negative=false source=command"
        ),
        "ok: client matrix profile java_3_7 passed for java-kafka against localhost:9092 version=apache-kafka-clients-3.7.1 source=command",
        "ok: go-kafka probes (basic,admin,groups) source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
    ],
    "S3 provider matrix": [
        "ok: S3 provider live-suite profile minio command_started=true completed=true source=command",
        (
            "ok: S3 provider profile minio endpoint=127.0.0.1:9000 "
            "bucket=zmq-minio-it scheme=http region=us-east-1 "
            "path_style=true source=command"
        ),
        "ok: S3 provider live-suite profile aws_us_east_1 command_started=true completed=true source=command",
        (
            "ok: S3 provider outage detail profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false "
            "down=true healed=true fail_closed=true recovered=true "
            "source=command"
        ),
        "ok: S3 provider outage profile aws_us_east_1 down=true healed=true fail_closed=true recovered=true source=command",
        (
            "ok: S3 provider process-crash detail profile aws_us_east_1 "
            "bucket=zmq-aws-it topic=zmq-process-crash "
            "group=zmq-process-crash-group killed_broker=true "
            "fresh_data_dir=true first_offset=0 committed_offset=1 "
            "replacement_offset=2 recovered_payloads=2 source=command"
        ),
        "ok: S3 provider process-crash profile aws_us_east_1 killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
        "ok: S3 provider list-pagination profile aws_us_east_1 required=true completed=true source=command",
        "ok: S3 provider multipart-edge profile aws_us_east_1 required=true completed=true source=command",
        (
            "ok: S3 multipart fault profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false "
            "injected=true recovered=true source=command"
        ),
        "ok: S3 provider multipart-fault profile aws_us_east_1 command_started=true completed=true injected=true recovered=true source=command",
        (
            "ok: S3 provider profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false source=command"
        ),
        "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
    ],
    "KRaft failover gate": [
        "ok: KRaft network partition phase leader-isolation down=true observed=failed healed=true healed_leader=1 healed_fetch=true expect=fail source=command",
        "ok: KRaft network partition phase broker-link down=true observed=survived healed=true healed_leader=2 healed_fetch=true expect=survive source=command",
    ],
    "Docker E2E gate": [
        "ok: E2E chaos phase cross-broker down=true observed=failed healed=true recovered=true expect=fail source=command",
        "ok: E2E chaos passed for cross-broker phase(s) source=command",
        "ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale phase scale-in applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command",
        "Results: 53/53 passed, 0 failed",
    ],
    "comparative benchmark gate": [
        "ZMQ (Zig)",
        "Apache Kafka",
        "AutoMQ (Java)",
        "trend thresholds:",
    ],
}


def self_test():
    assert_release_criteria_contract_documented()
    assert_required_build_steps_defined()
    assert_live_harness_marker_contracts()

    with open(RELEASE_CRITERIA_PATH, "r", encoding="utf-8") as f:
        criteria = f.read()

    extra_known_surface = criteria.replace(
        "## Release Decision",
        (
            "- New undocumented release blocker must be accounted for in the "
            "release-evidence verifier.\n\n"
            "## Release Decision"
        ),
        1,
    )
    try:
        assert_known_unsupported_surfaces_match_validator(extra_known_surface)
        raise AssertionError("extra known unsupported surface bullet was accepted")
    except AssertionError as exc:
        if "must list exactly" not in str(exc):
            raise

    missing_known_surface = re.sub(
        (
            r"(?ms)^- ZooKeeper-era inter-broker API keys 4-7"
            r".*?(?=^- Broader broker-only stateless replacement)"
        ),
        "",
        criteria,
        count=1,
    )
    if missing_known_surface == criteria:
        raise AssertionError("known unsupported surface bullet removal fixture did not apply")
    try:
        assert_known_unsupported_surfaces_match_validator(missing_known_surface)
        raise AssertionError("missing known unsupported surface bullet was accepted")
    except AssertionError as exc:
        if "must list exactly" not in str(exc):
            raise

    zookeeper_surface_match = re.search(
        (
            r"(?ms)^- ZooKeeper-era inter-broker API keys 4-7"
            r".*?(?=^- Broader broker-only stateless replacement)"
        ),
        criteria,
    )
    broker_surface_match = re.search(
        (
            r"(?ms)^- Broader broker-only stateless replacement"
            r".*?(?=^- The external client/security/OAuth live matrix)"
        ),
        criteria,
    )
    if zookeeper_surface_match is None or broker_surface_match is None:
        raise AssertionError("known unsupported surface duplicate fixture did not apply")
    duplicate_known_surface = (
        criteria[: broker_surface_match.start()]
        + zookeeper_surface_match.group(0)
        + criteria[broker_surface_match.end() :]
    )
    try:
        assert_known_unsupported_surfaces_match_validator(duplicate_known_surface)
        raise AssertionError("duplicate known unsupported surface bullet was accepted")
    except AssertionError as exc:
        if "duplicate bullets" not in str(exc):
            raise

    misclassified_known_surface = criteria.replace(
        "Broader broker-only stateless replacement remains partial because ",
        "Broader broker-only stateless replacement is not advertised although ",
        1,
    )
    if misclassified_known_surface == criteria:
        raise AssertionError("known unsupported surface status fixture did not apply")
    try:
        assert_known_unsupported_surfaces_match_validator(misclassified_known_surface)
        raise AssertionError(
            "misclassified known unsupported surface bullet status was accepted"
        )
    except AssertionError as exc:
        if "must mark the surface as partial or blocked" not in str(exc):
            raise

    mismatched_command_block = criteria.replace(
        f"{RELEASE_ZIG} build test-observability-static-audit --summary all",
        f"{RELEASE_ZIG} build test-observability-static-audit --summary compact",
        1,
    )
    try:
        assert_required_command_block_matches_validator(mismatched_command_block)
        raise AssertionError("release criteria required command block mismatch was accepted")
    except AssertionError as exc:
        message = str(exc)
        if "line 3" not in message or "observability static audit" not in message:
            raise

    duplicate_assignment_command_block = criteria.replace(
        "ZMQ_S3_ENDPOINT=s3.release.internal ",
        "ZMQ_S3_ENDPOINT=stale.release.internal "
        "ZMQ_S3_ENDPOINT=s3.release.internal ",
        1,
    )
    try:
        assert_required_command_block_matches_validator(
            duplicate_assignment_command_block
        )
        raise AssertionError(
            "release criteria required command block duplicate assignment was accepted"
        )
    except AssertionError as exc:
        message = str(exc)
        if "line 16" not in message or "repeats environment assignment" not in message:
            raise

    strict_manifest_path = None
    try:
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write('{"commit": NaN}')
            strict_manifest_path = f.name
        try:
            load_release_evidence_manifest(strict_manifest_path)
            raise AssertionError("non-standard JSON release evidence manifest was accepted")
        except ValueError as exc:
            message = str(exc)
            if "strict JSON" not in message or "non-standard JSON constant" not in message:
                raise
    finally:
        if strict_manifest_path:
            try:
                os.unlink(strict_manifest_path)
            except OSError:
                pass

    duplicate_manifest_path = None
    try:
        with tempfile.NamedTemporaryFile("w", delete=False) as f:
            f.write('{"commit": "a", "commit": "b"}')
            duplicate_manifest_path = f.name
        try:
            load_release_evidence_manifest(duplicate_manifest_path)
            raise AssertionError("duplicate-key release evidence manifest was accepted")
        except ValueError as exc:
            message = str(exc)
            if "strict JSON" not in message or "duplicate JSON object key" not in message:
                raise
    finally:
        if duplicate_manifest_path:
            try:
                os.unlink(duplicate_manifest_path)
            except OSError:
                pass

    manifest = sample_manifest()
    failures = validate_release_evidence(manifest)
    if failures:
        raise AssertionError(f"passing release evidence failed: {failures}")

    failures = validate_release_evidence([])
    if not any("must be a JSON object" in failure for failure in failures):
        raise AssertionError("non-object release evidence manifest was accepted")

    malformed_commit = sample_manifest()
    malformed_commit["commit"] = "abc"
    failures = validate_release_evidence(malformed_commit)
    if not any("exact 40-hex commit" in failure for failure in failures):
        raise AssertionError("malformed release evidence commit was accepted")

    unexpected_manifest_field = sample_manifest()
    unexpected_manifest_field["release_status"] = "complete"
    failures = validate_release_evidence(unexpected_manifest_field)
    if not any(
        "release evidence manifest contains unexpected field 'release_status'"
        in failure
        for failure in failures
    ):
        raise AssertionError("unknown release evidence manifest field was accepted")

    missing_environment = sample_manifest()
    missing_environment.pop("environment")
    failures = validate_release_evidence(missing_environment)
    if not any("must include environment object" in failure for failure in failures):
        raise AssertionError("missing release evidence environment object was accepted")

    non_object_environment = sample_manifest()
    non_object_environment["environment"] = []
    failures = validate_release_evidence(non_object_environment)
    if not any("must include environment object" in failure for failure in failures):
        raise AssertionError("non-object release evidence environment was accepted")

    invalid_environment_name = sample_manifest()
    invalid_environment_name["environment"]["ZMQ INVALID"] = "1"
    failures = validate_release_evidence(invalid_environment_name)
    if not any("ZMQ INVALID" in failure and "valid shell variable name" in failure for failure in failures):
        raise AssertionError("invalid release evidence environment variable name was accepted")

    json_boolean_environment = sample_manifest()
    json_boolean_environment["environment"]["ZMQ_EXTRA_BOOL"] = True
    failures = validate_release_evidence(json_boolean_environment)
    if not any("ZMQ_EXTRA_BOOL" in failure and "must be a string" in failure for failure in failures):
        raise AssertionError("JSON boolean release evidence environment value was accepted")

    blank_environment_value = sample_manifest()
    blank_environment_value["environment"]["ZMQ_EXTRA_BLANK"] = " "
    failures = validate_release_evidence(blank_environment_value)
    if not any("ZMQ_EXTRA_BLANK" in failure and "must not be blank" in failure for failure in failures):
        raise AssertionError("blank release evidence environment value was accepted")

    non_list_commands = sample_manifest()
    non_list_commands["commands"] = {}
    failures = validate_release_evidence(non_list_commands)
    if not any("must include commands list" in failure for failure in failures):
        raise AssertionError("non-list release evidence commands were accepted")

    unexpected_command_field = sample_manifest()
    unexpected_command_field["commands"][0]["duration_ms"] = 123
    failures = validate_release_evidence(unexpected_command_field)
    if not any(
        "command entry 0 contains unexpected field 'duration_ms'" in failure
        for failure in failures
    ):
        raise AssertionError("unknown release evidence command field was accepted")

    failed_command_exit = sample_manifest()
    failed_command_exit["commands"][0]["exit_code"] = 1
    failures = validate_release_evidence(failed_command_exit)
    if not any("did not exit successfully" in failure for failure in failures):
        raise AssertionError("failed release evidence command exit code was accepted")

    failures = validate_release_evidence(
        manifest,
        current_commit=manifest["commit"],
        tracked_worktree_dirty=False,
    )
    if failures:
        raise AssertionError(f"passing current-checkout release evidence failed: {failures}")

    failures = validate_release_evidence_for_checkout(
        manifest,
        current_commit=manifest["commit"],
        tracked_worktree_dirty=False,
    )
    if failures:
        raise AssertionError(f"passing strict-checkout release evidence failed: {failures}")
    unknown_checkout = sample_manifest()
    failures = validate_release_evidence_for_checkout(
        unknown_checkout,
        current_commit=None,
        tracked_worktree_dirty=None,
    )
    if not any("current git commit" in failure for failure in failures):
        raise AssertionError("unknown current git commit was not reported")
    if not any("worktree cleanliness" in failure for failure in failures):
        raise AssertionError("unknown tracked worktree status was not reported")

    wrong_commit = sample_manifest()
    failures = validate_release_evidence(
        wrong_commit,
        current_commit="b" * 40,
        tracked_worktree_dirty=False,
    )
    if not any("does not match current checkout" in failure for failure in failures):
        raise AssertionError("commit mismatch was not reported")

    dirty_worktree = sample_manifest()
    failures = validate_release_evidence(
        dirty_worktree,
        current_commit=dirty_worktree["commit"],
        tracked_worktree_dirty=True,
    )
    if not any("clean tracked worktree" in failure for failure in failures):
        raise AssertionError("dirty tracked worktree was not reported")

    missing_command = sample_manifest()
    missing_command["commands"] = missing_command["commands"][:-1]
    failures = validate_release_evidence(missing_command)
    if not any("comparative benchmark gate" in failure for failure in failures):
        raise AssertionError("missing required command was not reported")

    duplicate_required_command = sample_manifest()
    duplicate_required_command["commands"].insert(
        1,
        dict(duplicate_required_command["commands"][0]),
    )
    failures = validate_release_evidence(duplicate_required_command)
    if not any(
        "duplicate successful command entries" in failure
        and "default Zig test suite" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate required command entry was accepted")

    missing_static_audit = sample_manifest()
    missing_static_audit["commands"] = [
        entry
        for entry in missing_static_audit["commands"]
        if "test-observability-static-audit" not in entry["command"]
    ]
    failures = validate_release_evidence(missing_static_audit)
    if not any("observability static audit" in failure for failure in failures):
        raise AssertionError("missing observability static audit was not reported")

    missing_protocol_audit = sample_manifest()
    missing_protocol_audit["commands"] = [
        entry
        for entry in missing_protocol_audit["commands"]
        if "test-protocol-static-audit" not in entry["command"]
    ]
    failures = validate_release_evidence(missing_protocol_audit)
    if not any("protocol static audit" in failure for failure in failures):
        raise AssertionError("missing protocol static audit was not reported")

    missing_build_audit = sample_manifest()
    missing_build_audit["commands"] = [
        entry
        for entry in missing_build_audit["commands"]
        if "test-build-static-audit" not in entry["command"]
    ]
    failures = validate_release_evidence(missing_build_audit)
    if not any("build static audit" in failure for failure in failures):
        raise AssertionError("missing build static audit was not reported")

    bad_static_audit_output = sample_manifest()
    protocol_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "protocol static audit"
    )
    bad_static_audit_output["commands"][protocol_command_index]["output"] = (
        "Build Summary: 2/2 steps succeeded\n"
        "test-protocol-static-audit success\n"
        "passed"
    )
    failures = validate_release_evidence(bad_static_audit_output)
    if not any("ok: protocol static audit" in failure for failure in failures):
        raise AssertionError("missing static audit output marker was not reported")

    prefixed_static_audit_output = sample_manifest()
    prefixed_static_audit_output["commands"][protocol_command_index]["output"] = (
        "Build Summary: 2/2 steps succeeded\n"
        "test-protocol-static-audit success\n"
        "not ok: protocol static audit"
    )
    failures = validate_release_evidence(prefixed_static_audit_output)
    if not any("ok: protocol static audit" in failure for failure in failures):
        raise AssertionError("prefixed static audit output marker was accepted")

    suffixed_static_audit_output = sample_manifest()
    suffixed_static_audit_output["commands"][protocol_command_index]["output"] = (
        suffixed_static_audit_output["commands"][protocol_command_index][
            "output"
        ].replace(
            "ok: protocol static audit",
            "ok: protocol static audit source=wrapper",
            1,
        )
    )
    failures = validate_release_evidence(suffixed_static_audit_output)
    if not any(
        "protocol static audit output marker must appear exactly once "
        "as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed static audit output marker was accepted")

    duplicate_static_audit_output = sample_manifest()
    duplicate_static_audit_output["commands"][protocol_command_index]["output"] = (
        duplicate_static_audit_output["commands"][protocol_command_index][
            "output"
        ].replace(
            "ok: protocol static audit",
            "ok: protocol static audit\nok: protocol static audit",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_static_audit_output)
    if not any(
        "protocol static audit output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate static audit output marker was accepted")

    missing_compose_echo = sample_manifest()
    root_compose_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "root compose config validation"
    )
    missing_compose_echo["commands"][root_compose_command_index]["command"] = (
        "docker compose -f docker-compose.yml config --quiet"
    )
    failures = validate_release_evidence(missing_compose_echo)
    if not any("root compose config validation" in failure for failure in failures):
        raise AssertionError("missing compose config echo command was not reported")

    bad_compose_output = sample_manifest()
    bad_compose_output["commands"][root_compose_command_index]["output"] = (
        "docker compose config succeeded"
    )
    failures = validate_release_evidence(bad_compose_output)
    if not any("ok: root compose config" in failure for failure in failures):
        raise AssertionError("missing compose config output marker was not reported")

    suffixed_compose_output = sample_manifest()
    suffixed_compose_output["commands"][root_compose_command_index]["output"] = (
        suffixed_compose_output["commands"][root_compose_command_index][
            "output"
        ].replace(
            "ok: root compose config",
            "ok: root compose config source=wrapper",
            1,
        )
    )
    failures = validate_release_evidence(suffixed_compose_output)
    if not any(
        "root compose config validation output marker must appear exactly once "
        "as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed compose config output marker was accepted")

    duplicate_compose_output = sample_manifest()
    duplicate_compose_output["commands"][root_compose_command_index]["output"] = (
        duplicate_compose_output["commands"][root_compose_command_index][
            "output"
        ].replace(
            "ok: root compose config",
            "ok: root compose config\nok: root compose config",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_compose_output)
    if not any(
        "root compose config validation output marker must appear exactly once"
        in failure
        for failure in failures
    ):
        raise AssertionError("duplicate compose config output marker was accepted")

    bad_benchmark_output = sample_manifest()
    local_benchmark_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "local benchmark gate"
    )
    bad_benchmark_output["commands"][local_benchmark_command_index]["output"] = (
        "Build Summary: 3/3 steps succeeded\nbench success"
    )
    failures = validate_release_evidence(bad_benchmark_output)
    if not any("S3 WAL request volume" in failure for failure in failures):
        raise AssertionError("missing local benchmark output marker was not reported")

    missing_source_local_benchmark_summary = sample_manifest()
    missing_source_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ] = missing_source_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "ok: local benchmark gate source=command",
        "ok: local benchmark gate",
    )
    failures = validate_release_evidence(missing_source_local_benchmark_summary)
    if not any("ok: local benchmark gate source=command" in failure for failure in failures):
        raise AssertionError("local benchmark summary without source=command was accepted")

    mismatched_source_local_benchmark_summary = sample_manifest()
    mismatched_source_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ] = mismatched_source_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "ok: local benchmark gate source=command",
        "ok: local benchmark gate source=wrapper",
    )
    failures = validate_release_evidence(mismatched_source_local_benchmark_summary)
    if not any("ok: local benchmark gate source=command" in failure for failure in failures):
        raise AssertionError("local benchmark summary with wrapper source was accepted")

    suffixed_local_benchmark_summary = sample_manifest()
    suffixed_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ] = suffixed_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "ok: local benchmark gate source=command",
        "ok: local benchmark gate source=command wrapper=1",
    )
    failures = validate_release_evidence(suffixed_local_benchmark_summary)
    if not any(
        "local benchmark summary output marker must appear exactly once "
        "as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed local benchmark summary marker was accepted")

    duplicate_local_benchmark_summary = sample_manifest()
    duplicate_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ] = duplicate_local_benchmark_summary["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "ok: local benchmark gate source=command",
        "ok: local benchmark gate source=command\nok: local benchmark gate source=command",
    )
    failures = validate_release_evidence(duplicate_local_benchmark_summary)
    if not any(
        "local benchmark summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate local benchmark summary marker was accepted")

    suffixed_local_benchmark_completion = sample_manifest()
    suffixed_local_benchmark_completion["commands"][local_benchmark_command_index][
        "output"
    ] = suffixed_local_benchmark_completion["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "=== Benchmarks complete ===",
        "=== Benchmarks complete === wrapper=1",
        1,
    )
    failures = validate_release_evidence(suffixed_local_benchmark_completion)
    if not any(
        "local benchmark completion output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed local benchmark completion marker was accepted")

    prefixed_benchmark_output = sample_manifest()
    prefixed_benchmark_output["commands"][local_benchmark_command_index]["output"] = (
        prefixed_benchmark_output["commands"][local_benchmark_command_index]["output"].replace(
            "S3 WAL request volume",
            "previous S3 WAL request volume",
            1,
        )
    )
    failures = validate_release_evidence(prefixed_benchmark_output)
    if not any("S3 WAL request volume" in failure for failure in failures):
        raise AssertionError("embedded local benchmark output marker was accepted")

    bare_local_benchmark_request_volume = sample_manifest()
    bare_local_benchmark_request_volume["commands"][local_benchmark_command_index][
        "output"
    ] = bare_local_benchmark_request_volume["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "S3 WAL request volume    puts=200 lists=0 requests/MiB=251.70",
        "S3 WAL request volume",
    )
    failures = validate_release_evidence(bare_local_benchmark_request_volume)
    if not any("local benchmark S3 WAL request-volume" in failure for failure in failures):
        raise AssertionError("bare local benchmark request-volume marker was accepted")

    bare_local_benchmark_memory = sample_manifest()
    bare_local_benchmark_memory["commands"][local_benchmark_command_index][
        "output"
    ] = bare_local_benchmark_memory["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "PartitionStore memory         80964/s  retained=514 KiB  peak=518 KiB  max_current=514 KiB",
        "PartitionStore memory",
    )
    failures = validate_release_evidence(bare_local_benchmark_memory)
    if not any("local benchmark memory marker" in failure for failure in failures):
        raise AssertionError("bare local benchmark memory marker was accepted")

    detached_local_benchmark_details = sample_manifest()
    detached_local_benchmark_details["commands"][local_benchmark_command_index][
        "output"
    ] = "\n".join(
        [
            "Build Summary: 1/1 steps succeeded",
            "bench success",
            "=== Benchmarks complete ===",
            "ok: local benchmark gate source=command",
            "S3 WAL request volume    puts=200 lists=0 requests/MiB=251.70",
            "PartitionStore memory         80964/s  retained=514 KiB  peak=518 KiB  max_current=514 KiB",
        ]
    )
    failures = validate_release_evidence(detached_local_benchmark_details)
    if not any("before the benchmark completion marker" in failure for failure in failures):
        raise AssertionError("detached local benchmark detail markers were accepted")

    duplicate_local_benchmark_request_volume = sample_manifest()
    duplicate_local_benchmark_request_volume["commands"][local_benchmark_command_index][
        "output"
    ] = duplicate_local_benchmark_request_volume["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "S3 WAL request volume    puts=200 lists=0 requests/MiB=251.70",
        (
            "S3 WAL request volume    puts=200 lists=0 requests/MiB=251.70\n"
            "S3 WAL request volume    puts=200 lists=0 requests/MiB=251.70"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_local_benchmark_request_volume)
    if not any(
        "local benchmark S3 WAL request-volume marker" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate local benchmark request-volume marker was accepted")

    duplicate_local_benchmark_memory = sample_manifest()
    duplicate_local_benchmark_memory["commands"][local_benchmark_command_index][
        "output"
    ] = duplicate_local_benchmark_memory["commands"][local_benchmark_command_index][
        "output"
    ].replace(
        "PartitionStore memory         80964/s  retained=514 KiB  peak=518 KiB  max_current=514 KiB",
        (
            "PartitionStore memory         80964/s  retained=514 KiB  peak=518 KiB  max_current=514 KiB\n"
            "PartitionStore memory         80964/s  retained=514 KiB  peak=518 KiB  max_current=514 KiB"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_local_benchmark_memory)
    if not any(
        "local benchmark memory marker" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate local benchmark memory marker was accepted")

    live_s3_benchmark_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "live-S3 benchmark gate"
    )
    missing_source_live_s3_benchmark_summary = sample_manifest()
    missing_source_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ] = missing_source_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ].replace(
        "ok: live-S3 benchmark gate source=command",
        "ok: live-S3 benchmark gate",
    )
    failures = validate_release_evidence(missing_source_live_s3_benchmark_summary)
    if not any("ok: live-S3 benchmark gate source=command" in failure for failure in failures):
        raise AssertionError("live-S3 benchmark summary without source=command was accepted")

    mismatched_source_live_s3_benchmark_summary = sample_manifest()
    mismatched_source_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ] = mismatched_source_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ].replace(
        "ok: live-S3 benchmark gate source=command",
        "ok: live-S3 benchmark gate source=wrapper",
    )
    failures = validate_release_evidence(mismatched_source_live_s3_benchmark_summary)
    if not any("ok: live-S3 benchmark gate source=command" in failure for failure in failures):
        raise AssertionError("live-S3 benchmark summary with wrapper source was accepted")

    suffixed_live_s3_benchmark_summary = sample_manifest()
    suffixed_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ] = suffixed_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ].replace(
        "ok: live-S3 benchmark gate source=command",
        "ok: live-S3 benchmark gate source=command wrapper=1",
    )
    failures = validate_release_evidence(suffixed_live_s3_benchmark_summary)
    if not any(
        "live-S3 benchmark summary output marker must appear exactly once "
        "as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed live-S3 benchmark summary marker was accepted")

    duplicate_live_s3_benchmark_summary = sample_manifest()
    duplicate_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ] = duplicate_live_s3_benchmark_summary["commands"][live_s3_benchmark_command_index][
        "output"
    ].replace(
        "ok: live-S3 benchmark gate source=command",
        "ok: live-S3 benchmark gate source=command\nok: live-S3 benchmark gate source=command",
    )
    failures = validate_release_evidence(duplicate_live_s3_benchmark_summary)
    if not any(
        "live-S3 benchmark summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate live-S3 benchmark summary marker was accepted")

    suffixed_live_s3_benchmark_completion = sample_manifest()
    suffixed_live_s3_benchmark_completion["commands"][live_s3_benchmark_command_index][
        "output"
    ] = suffixed_live_s3_benchmark_completion["commands"][
        live_s3_benchmark_command_index
    ]["output"].replace(
        "=== Benchmarks complete ===",
        "=== Benchmarks complete === wrapper=1",
        1,
    )
    failures = validate_release_evidence(suffixed_live_s3_benchmark_completion)
    if not any(
        "live-S3 benchmark completion output marker must appear exactly once"
        in failure
        for failure in failures
    ):
        raise AssertionError("suffixed live-S3 benchmark completion marker was accepted")

    live_s3_benchmark_provider_marker = (
        "Live S3 provider endpoint=s3-bench.example.test:9443 "
        "bucket=zmq-live-bench scheme=http region=us-east-1 path_style=true"
    )
    bare_live_s3_provider = sample_manifest()
    bare_live_s3_provider["commands"][live_s3_benchmark_command_index]["output"] = (
        bare_live_s3_provider["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            live_s3_benchmark_provider_marker,
            "Live S3 provider",
        )
    )
    failures = validate_release_evidence(bare_live_s3_provider)
    if not any("live-S3 benchmark provider marker" in failure for failure in failures):
        raise AssertionError("bare live-S3 benchmark provider marker was accepted")

    legacy_live_s3_provider = sample_manifest()
    legacy_live_s3_provider["commands"][live_s3_benchmark_command_index]["output"] = (
        legacy_live_s3_provider["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            live_s3_benchmark_provider_marker,
            "Live S3 provider endpoint=s3-bench.example.test:9443 bucket=zmq-live-bench",
        )
    )
    failures = validate_release_evidence(legacy_live_s3_provider)
    if not any("path_style" in failure for failure in failures):
        raise AssertionError("legacy live-S3 benchmark provider marker was accepted")

    missing_live_s3_provider_endpoint = sample_manifest()
    missing_live_s3_provider_endpoint["commands"][live_s3_benchmark_command_index][
        "command"
    ] = missing_live_s3_provider_endpoint["commands"][live_s3_benchmark_command_index][
        "command"
    ].replace(
        "ZMQ_S3_ENDPOINT=s3-bench.example.test ",
        "",
    )
    missing_live_s3_provider_endpoint["environment"].pop("ZMQ_S3_ENDPOINT")
    failures = validate_release_evidence(missing_live_s3_provider_endpoint)
    if not any("ZMQ_S3_ENDPOINT" in failure for failure in failures):
        raise AssertionError("missing live-S3 benchmark endpoint provenance was accepted")

    missing_live_s3_manifest_endpoint = sample_manifest()
    missing_live_s3_manifest_endpoint["environment"].pop("ZMQ_S3_ENDPOINT")
    failures = validate_release_evidence(missing_live_s3_manifest_endpoint)
    if not any(
        "manifest environment" in failure and "ZMQ_S3_ENDPOINT" in failure
        for failure in failures
    ):
        raise AssertionError("missing live-S3 manifest endpoint provenance was accepted")

    for env_name, assertion_message in (
        (
            "ZMQ_S3_ENDPOINT",
            "missing live-S3 benchmark endpoint command assignment was accepted",
        ),
        (
            "ZMQ_S3_PORT",
            "missing live-S3 benchmark port command assignment was accepted",
        ),
        (
            "ZMQ_S3_BUCKET",
            "missing live-S3 benchmark bucket command assignment was accepted",
        ),
        (
            "ZMQ_S3_SCHEME",
            "missing live-S3 benchmark scheme command assignment was accepted",
        ),
        (
            "ZMQ_S3_REGION",
            "missing live-S3 benchmark region command assignment was accepted",
        ),
        (
            "ZMQ_S3_PATH_STYLE",
            "missing live-S3 benchmark path-style command assignment was accepted",
        ),
    ):
        missing_live_s3_command_assignment = sample_manifest()
        live_s3_command_value = missing_live_s3_command_assignment["environment"][
            env_name
        ]
        missing_live_s3_command_assignment["commands"][
            live_s3_benchmark_command_index
        ]["command"] = missing_live_s3_command_assignment["commands"][
            live_s3_benchmark_command_index
        ][
            "command"
        ].replace(
            f"{env_name}={live_s3_command_value} ",
            "",
        )
        failures = validate_release_evidence(missing_live_s3_command_assignment)
        if not any(
            "live-S3 benchmark gate" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    missing_live_s3_scheme_assignment = sample_manifest()
    missing_live_s3_scheme_assignment["commands"][live_s3_benchmark_command_index][
        "command"
    ] = missing_live_s3_scheme_assignment["commands"][live_s3_benchmark_command_index][
        "command"
    ].replace(
        "ZMQ_S3_SCHEME=http ",
        "",
    )
    failures = validate_release_evidence(missing_live_s3_scheme_assignment)
    if not any("non-empty ZMQ_S3_SCHEME=" in failure for failure in failures):
        raise AssertionError("missing live-S3 scheme command assignment was accepted")

    mismatched_live_s3_provider_output = sample_manifest()
    mismatched_live_s3_provider_output["commands"][live_s3_benchmark_command_index][
        "output"
    ] = mismatched_live_s3_provider_output["commands"][live_s3_benchmark_command_index][
        "output"
    ].replace(
        live_s3_benchmark_provider_marker,
        (
            "Live S3 provider endpoint=s3-bench.example.test:9000 "
            "bucket=zmq-live-bench scheme=http region=us-east-1 "
            "path_style=true"
        ),
    )
    failures = validate_release_evidence(mismatched_live_s3_provider_output)
    if not any("must match selected endpoint" in failure for failure in failures):
        raise AssertionError("mismatched live-S3 benchmark provider output was accepted")

    live_s3_provider_settings_provenance = sample_manifest()
    live_s3_provider_settings_provenance["environment"]["ZMQ_S3_SCHEME"] = "https"
    live_s3_provider_settings_provenance["environment"]["ZMQ_S3_REGION"] = "us-west-2"
    live_s3_provider_settings_provenance["environment"]["ZMQ_S3_PATH_STYLE"] = "false"
    live_s3_provider_settings_provenance["environment"]["ZMQ_S3_MINIO_SCHEME"] = "http"
    live_s3_provider_settings_provenance["environment"]["ZMQ_S3_MINIO_REGION"] = "us-east-1"
    live_s3_provider_settings_provenance["environment"]["ZMQ_S3_MINIO_PATH_STYLE"] = "true"
    live_s3_provider_settings_provenance["commands"][live_s3_benchmark_command_index][
        "command"
    ] = live_s3_provider_settings_provenance["commands"][live_s3_benchmark_command_index][
        "command"
    ].replace(
        "ZMQ_S3_SCHEME=http ZMQ_S3_REGION=us-east-1 ZMQ_S3_PATH_STYLE=true",
        "ZMQ_S3_SCHEME=https ZMQ_S3_REGION=us-west-2 ZMQ_S3_PATH_STYLE=false",
    )
    live_s3_provider_settings_provenance["commands"][live_s3_benchmark_command_index][
        "output"
    ] = live_s3_provider_settings_provenance["commands"][
        live_s3_benchmark_command_index
    ][
        "output"
    ].replace(
        live_s3_benchmark_provider_marker,
        (
            "Live S3 provider endpoint=s3-bench.example.test:9443 "
            "bucket=zmq-live-bench scheme=https region=us-west-2 "
            "path_style=false"
        ),
    )
    failures = validate_release_evidence(live_s3_provider_settings_provenance)
    if failures:
        raise AssertionError(
            "live-S3 benchmark settings provenance was rejected: "
            + "; ".join(failures)
        )

    live_s3_benchmark_size_provenance = sample_manifest()
    live_s3_benchmark_size_provenance["environment"][
        "ZMQ_BENCH_LIVE_S3_ITERATIONS"
    ] = "32"
    live_s3_benchmark_size_provenance["environment"][
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES"
    ] = "131072"
    live_s3_benchmark_size_provenance["commands"][live_s3_benchmark_command_index][
        "command"
    ] = sample_requirement_command(
        REQUIRED_COMMANDS[live_s3_benchmark_command_index],
        live_s3_benchmark_size_provenance["environment"],
    )
    failures = validate_release_evidence(live_s3_benchmark_size_provenance)
    if failures:
        raise AssertionError(
            "live-S3 benchmark iteration/payload-size command provenance was rejected: "
            + "; ".join(failures)
        )

    missing_live_s3_iterations_command = sample_manifest()
    missing_live_s3_iterations_command["environment"][
        "ZMQ_BENCH_LIVE_S3_ITERATIONS"
    ] = "32"
    failures = validate_release_evidence(missing_live_s3_iterations_command)
    if not any(
        "live-S3 benchmark gate" in failure
        and "ZMQ_BENCH_LIVE_S3_ITERATIONS" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing live-S3 benchmark iteration command assignment was accepted"
        )

    missing_live_s3_payload_command = sample_manifest()
    missing_live_s3_payload_command["environment"][
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES"
    ] = "131072"
    failures = validate_release_evidence(missing_live_s3_payload_command)
    if not any(
        "live-S3 benchmark gate" in failure
        and "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing live-S3 benchmark payload-size command assignment was accepted"
        )

    mismatched_live_s3_iterations_command = sample_manifest()
    mismatched_live_s3_iterations_command["environment"][
        "ZMQ_BENCH_LIVE_S3_ITERATIONS"
    ] = "32"
    mismatched_live_s3_iterations_command["commands"][live_s3_benchmark_command_index][
        "command"
    ] = sample_requirement_command(
        REQUIRED_COMMANDS[live_s3_benchmark_command_index],
        mismatched_live_s3_iterations_command["environment"],
    ).replace(
        "ZMQ_BENCH_LIVE_S3_ITERATIONS=32",
        "ZMQ_BENCH_LIVE_S3_ITERATIONS=16",
    )
    failures = validate_release_evidence(mismatched_live_s3_iterations_command)
    if not any(
        "ZMQ_BENCH_LIVE_S3_ITERATIONS" in failure
        and "manifest environment records" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched live-S3 benchmark iteration command assignment was accepted"
        )

    mismatched_live_s3_payload_command = sample_manifest()
    mismatched_live_s3_payload_command["environment"][
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES"
    ] = "131072"
    mismatched_live_s3_payload_command["commands"][live_s3_benchmark_command_index][
        "command"
    ] = sample_requirement_command(
        REQUIRED_COMMANDS[live_s3_benchmark_command_index],
        mismatched_live_s3_payload_command["environment"],
    ).replace(
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES=131072",
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES=65536",
    )
    failures = validate_release_evidence(mismatched_live_s3_payload_command)
    if not any(
        "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES" in failure
        and "manifest environment records" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched live-S3 benchmark payload-size command assignment was accepted"
        )

    invalid_live_s3_scheme_provenance = sample_manifest()
    invalid_live_s3_scheme_provenance["environment"]["ZMQ_S3_SCHEME"] = "ftp"
    failures = validate_release_evidence(invalid_live_s3_scheme_provenance)
    if not any("ZMQ_S3_SCHEME" in failure and "http or https" in failure for failure in failures):
        raise AssertionError("invalid live-S3 benchmark scheme provenance was accepted")

    invalid_live_s3_path_style_provenance = sample_manifest()
    invalid_live_s3_path_style_provenance["environment"]["ZMQ_S3_PATH_STYLE"] = "sometimes"
    failures = validate_release_evidence(invalid_live_s3_path_style_provenance)
    if not any("ZMQ_S3_PATH_STYLE" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid live-S3 benchmark path-style provenance was accepted")

    mismatched_live_s3_provider_settings_output = sample_manifest()
    mismatched_live_s3_provider_settings_output["environment"]["ZMQ_S3_SCHEME"] = "https"
    mismatched_live_s3_provider_settings_output["commands"][
        live_s3_benchmark_command_index
    ]["command"] = mismatched_live_s3_provider_settings_output["commands"][
        live_s3_benchmark_command_index
    ]["command"].replace(
        "ZMQ_S3_SCHEME=http",
        "ZMQ_S3_SCHEME=https",
    )
    failures = validate_release_evidence(mismatched_live_s3_provider_settings_output)
    if not any("scheme=https" in failure for failure in failures):
        raise AssertionError(
            "mismatched live-S3 benchmark settings output was accepted"
        )

    prefixed_live_s3_benchmark_output = sample_manifest()
    prefixed_live_s3_benchmark_output["commands"][live_s3_benchmark_command_index]["output"] = (
        prefixed_live_s3_benchmark_output["commands"][live_s3_benchmark_command_index]["output"].replace(
            "Live S3 request volume",
            "previous Live S3 request volume",
            1,
        )
    )
    failures = validate_release_evidence(prefixed_live_s3_benchmark_output)
    if not any("Live S3 request volume" in failure for failure in failures):
        raise AssertionError("embedded live-S3 benchmark output marker was accepted")

    bare_live_s3_request_volume = sample_manifest()
    bare_live_s3_request_volume["commands"][live_s3_benchmark_command_index]["output"] = (
        bare_live_s3_request_volume["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            "Live S3 request volume   puts=20 gets=20 requests/MiB=40.00",
            "Live S3 request volume",
        )
    )
    failures = validate_release_evidence(bare_live_s3_request_volume)
    if not any("live-S3 benchmark request-volume" in failure for failure in failures):
        raise AssertionError("bare live-S3 benchmark request-volume marker was accepted")

    bare_live_s3_put = sample_manifest()
    bare_live_s3_put["commands"][live_s3_benchmark_command_index]["output"] = (
        bare_live_s3_put["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            "Live S3 put                12.50 MiB/s  p99=   10.00 ms  objects=20",
            "Live S3 put",
        )
    )
    failures = validate_release_evidence(bare_live_s3_put)
    if not any("live-S3 benchmark put marker" in failure for failure in failures):
        raise AssertionError("bare live-S3 benchmark put marker was accepted")

    bare_live_s3_get = sample_manifest()
    bare_live_s3_get["commands"][live_s3_benchmark_command_index]["output"] = (
        bare_live_s3_get["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            "Live S3 get                14.00 MiB/s  p99=    8.00 ms  requests/MiB=40.00",
            "Live S3 get",
        )
    )
    failures = validate_release_evidence(bare_live_s3_get)
    if not any("live-S3 benchmark get marker" in failure for failure in failures):
        raise AssertionError("bare live-S3 benchmark get marker was accepted")

    detached_live_s3_benchmark_details = sample_manifest()
    live_s3_detail_markers = [
        live_s3_benchmark_provider_marker,
        "Live S3 put                12.50 MiB/s  p99=   10.00 ms  objects=20",
        "Live S3 get                14.00 MiB/s  p99=    8.00 ms  requests/MiB=40.00",
        "Live S3 request volume   puts=20 gets=20 requests/MiB=40.00",
    ]
    detached_live_s3_output = detached_live_s3_benchmark_details["commands"][
        live_s3_benchmark_command_index
    ]["output"]
    for marker in live_s3_detail_markers:
        detached_live_s3_output = detached_live_s3_output.replace(marker + "\n", "")
    detached_live_s3_output = detached_live_s3_output.replace(
        "=== Benchmarks complete ===",
        "=== Benchmarks complete ===\n" + "\n".join(live_s3_detail_markers),
    )
    detached_live_s3_benchmark_details["commands"][live_s3_benchmark_command_index][
        "output"
    ] = detached_live_s3_output
    failures = validate_release_evidence(detached_live_s3_benchmark_details)
    if not any("before the benchmark completion marker" in failure for failure in failures):
        raise AssertionError("detached live-S3 benchmark detail markers were accepted")

    duplicate_live_s3_provider = sample_manifest()
    duplicate_live_s3_provider["commands"][live_s3_benchmark_command_index]["output"] = (
        duplicate_live_s3_provider["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            live_s3_benchmark_provider_marker,
            live_s3_benchmark_provider_marker + "\n" + live_s3_benchmark_provider_marker,
            1,
        )
    )
    failures = validate_release_evidence(duplicate_live_s3_provider)
    if not any(
        "live-S3 benchmark provider marker" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate live-S3 benchmark provider marker was accepted")

    duplicate_live_s3_put = sample_manifest()
    duplicate_live_s3_put["commands"][live_s3_benchmark_command_index]["output"] = (
        duplicate_live_s3_put["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            "Live S3 put                12.50 MiB/s  p99=   10.00 ms  objects=20",
            (
                "Live S3 put                12.50 MiB/s  p99=   10.00 ms  objects=20\n"
                "Live S3 put                12.50 MiB/s  p99=   10.00 ms  objects=20"
            ),
            1,
        )
    )
    failures = validate_release_evidence(duplicate_live_s3_put)
    if not any(
        "live-S3 benchmark put marker" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate live-S3 benchmark put marker was accepted")

    duplicate_live_s3_get = sample_manifest()
    duplicate_live_s3_get["commands"][live_s3_benchmark_command_index]["output"] = (
        duplicate_live_s3_get["commands"][live_s3_benchmark_command_index][
            "output"
        ].replace(
            "Live S3 get                14.00 MiB/s  p99=    8.00 ms  requests/MiB=40.00",
            (
                "Live S3 get                14.00 MiB/s  p99=    8.00 ms  requests/MiB=40.00\n"
                "Live S3 get                14.00 MiB/s  p99=    8.00 ms  requests/MiB=40.00"
            ),
            1,
        )
    )
    failures = validate_release_evidence(duplicate_live_s3_get)
    if not any(
        "live-S3 benchmark get marker" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate live-S3 benchmark get marker was accepted")

    duplicate_live_s3_request_volume = sample_manifest()
    duplicate_live_s3_request_volume["commands"][live_s3_benchmark_command_index][
        "output"
    ] = duplicate_live_s3_request_volume["commands"][live_s3_benchmark_command_index][
        "output"
    ].replace(
        "Live S3 request volume   puts=20 gets=20 requests/MiB=40.00",
        (
            "Live S3 request volume   puts=20 gets=20 requests/MiB=40.00\n"
            "Live S3 request volume   puts=20 gets=20 requests/MiB=40.00"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_live_s3_request_volume)
    if not any(
        "live-S3 benchmark request-volume marker" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate live-S3 benchmark request-volume marker was accepted")

    missing_comparative_target_output = sample_manifest()
    comparative_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "comparative benchmark gate"
    )
    comparative_profile_marker = (
        "  ok: comparative benchmark profile "
        "selected=zmq,kafka,automq "
        "required=zmq,kafka,automq "
        "results_targets=zmq,kafka,automq "
        "results=benchmarks/results.json "
        "gates_enforced=true "
        "trend_required=true "
        "trend_baseline=benchmarks/results-previous.json "
        "iterations=api_versions:5000,produce_single:5000,produce_fresh:2000,fetch:3000,metadata:3000 "
        "warmup=api_versions:100,produce_single:100,produce_fresh:50,fetch:100,metadata:100 "
        "source=command"
    )
    missing_comparative_target_output["commands"][comparative_command_index]["output"] = (
        missing_comparative_target_output["commands"][comparative_command_index]["output"].replace(
            "AutoMQ (Java)",
            "",
        )
    )
    failures = validate_release_evidence(missing_comparative_target_output)
    if not any("AutoMQ (Java)" in failure for failure in failures):
        raise AssertionError("missing comparative benchmark target output was not reported")

    missing_comparative_benchmark_row = sample_manifest()
    missing_comparative_benchmark_row["commands"][comparative_command_index]["output"] = (
        missing_comparative_benchmark_row["commands"][comparative_command_index]["output"].replace(
            "Produce (fresh)",
            "",
        )
    )
    failures = validate_release_evidence(missing_comparative_benchmark_row)
    if not any("Produce (fresh)" in failure for failure in failures):
        raise AssertionError("missing comparative benchmark row was not reported")

    embedded_comparative_table_header = sample_manifest()
    embedded_comparative_table_header["commands"][comparative_command_index]["output"] = (
        embedded_comparative_table_header["commands"][comparative_command_index]["output"].replace(
            "  Benchmark              Metric",
            "  Previous Benchmark Metric",
            1,
        )
    )
    failures = validate_release_evidence(embedded_comparative_table_header)
    if not any("Benchmark" in failure for failure in failures):
        raise AssertionError("embedded comparative benchmark table header was accepted")

    detached_comparative_table_header = sample_manifest()
    detached_comparative_table_header["commands"][comparative_command_index]["output"] = (
        detached_comparative_table_header["commands"][comparative_command_index][
            "output"
        ].replace(
            "  Benchmark              Metric          ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ\n",
            "",
            1,
        )
        + "\nBenchmark Metric"
    )
    failures = validate_release_evidence(detached_comparative_table_header)
    if not any("Benchmark" in failure for failure in failures):
        raise AssertionError("detached comparative benchmark table header was accepted")

    missing_comparative_table_target_column = sample_manifest()
    missing_comparative_table_target_column["commands"][comparative_command_index][
        "output"
    ] = missing_comparative_table_target_column["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        Other       ZMQ/Kafka      ZMQ/AutoMQ",
        1,
    )
    failures = validate_release_evidence(missing_comparative_table_target_column)
    if not any(
        "comparative benchmark table header" in failure
        and "target column 'AutoMQ'" in failure
        for failure in failures
    ):
        raise AssertionError("missing comparative benchmark table target column was accepted")

    duplicate_comparative_table_target_column = sample_manifest()
    duplicate_comparative_table_target_column["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_table_target_column["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        Kafka       ZMQ/Kafka      ZMQ/AutoMQ",
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_table_target_column)
    if not any(
        "comparative benchmark table header" in failure
        and "target column 'Kafka'" in failure
        for failure in failures
    ):
        raise AssertionError(
            "duplicate comparative benchmark table target column was accepted"
        )

    unknown_comparative_table_target_column = sample_manifest()
    unknown_comparative_table_target_column["commands"][comparative_command_index][
        "output"
    ] = unknown_comparative_table_target_column["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        AutoMQ      Other       ZMQ/Kafka      ZMQ/AutoMQ",
        1,
    )
    failures = validate_release_evidence(unknown_comparative_table_target_column)
    if not any(
        "unknown target columns" in failure and "Other" in failure
        for failure in failures
    ):
        raise AssertionError(
            "unknown comparative benchmark table target column was accepted"
        )

    reordered_comparative_table_target_columns = sample_manifest()
    reordered_comparative_table_target_columns["commands"][comparative_command_index][
        "output"
    ] = reordered_comparative_table_target_columns["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        AutoMQ      Kafka        ZMQ/Kafka      ZMQ/AutoMQ",
        1,
    )
    failures = validate_release_evidence(reordered_comparative_table_target_columns)
    if not any(
        "comparative benchmark table header target columns" in failure
        and "comparative target catalogue order" in failure
        for failure in failures
    ):
        raise AssertionError(
            "reordered comparative benchmark table target columns were accepted"
        )

    missing_comparative_table_ratio_column = sample_manifest()
    missing_comparative_table_ratio_column["commands"][comparative_command_index][
        "output"
    ] = missing_comparative_table_ratio_column["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/Other",
        1,
    )
    failures = validate_release_evidence(missing_comparative_table_ratio_column)
    if not any(
        "comparative benchmark table header" in failure
        and "ratio column 'ZMQ/AutoMQ'" in failure
        for failure in failures
    ):
        raise AssertionError("missing comparative benchmark table ratio column was accepted")

    duplicate_comparative_table_ratio_column = sample_manifest()
    duplicate_comparative_table_ratio_column["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_table_ratio_column["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/Kafka",
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_table_ratio_column)
    if not any(
        "comparative benchmark table header" in failure
        and "ratio column 'ZMQ/Kafka'" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark table ratio column was accepted")

    unknown_comparative_table_ratio_column = sample_manifest()
    unknown_comparative_table_ratio_column["commands"][comparative_command_index][
        "output"
    ] = unknown_comparative_table_ratio_column["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ     ZMQ/Other",
        1,
    )
    failures = validate_release_evidence(unknown_comparative_table_ratio_column)
    if not any(
        "unknown ratio columns" in failure and "ZMQ/Other" in failure
        for failure in failures
    ):
        raise AssertionError(
            "unknown comparative benchmark table ratio column was accepted"
        )

    reordered_comparative_table_ratio_columns = sample_manifest()
    reordered_comparative_table_ratio_columns["commands"][comparative_command_index][
        "output"
    ] = reordered_comparative_table_ratio_columns["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        "ZMQ        Kafka        AutoMQ      ZMQ/AutoMQ     ZMQ/Kafka",
        1,
    )
    failures = validate_release_evidence(reordered_comparative_table_ratio_columns)
    if not any(
        "comparative benchmark table header ratio columns" in failure
        and "comparative target catalogue order" in failure
        for failure in failures
    ):
        raise AssertionError(
            "reordered comparative benchmark table ratio columns were accepted"
        )

    embedded_comparative_row_label = sample_manifest()
    embedded_comparative_row_label["commands"][comparative_command_index]["output"] = (
        embedded_comparative_row_label["commands"][comparative_command_index]["output"].replace(
            "  Produce (fresh)        tput",
            "  Previous Produce (fresh) tput",
            1,
        )
    )
    failures = validate_release_evidence(embedded_comparative_row_label)
    if not any("Produce (fresh)" in failure for failure in failures):
        raise AssertionError("embedded comparative benchmark row label was accepted")

    embedded_comparative_target_label = sample_manifest()
    embedded_comparative_target_label["commands"][comparative_command_index]["output"] = (
        embedded_comparative_target_label["commands"][comparative_command_index]["output"].replace(
            "ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)",
            "ZMQ (Zig) vs Apache Kafka vs AutoMQ",
            1,
        )
        + "\nAutoMQ (Java)"
    )
    failures = validate_release_evidence(embedded_comparative_target_label)
    if not any("AutoMQ (Java)" in failure for failure in failures):
        raise AssertionError("embedded comparative target label was accepted")

    detached_comparative_target_label = sample_manifest()
    detached_comparative_target_label["commands"][comparative_command_index]["output"] = (
        detached_comparative_target_label["commands"][comparative_command_index][
            "output"
        ].replace(
            "ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)",
            "ZMQ (Zig) vs Apache Kafka vs AutoMQ",
            1,
        )
        + "\nCOMPARISON: ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)"
    )
    failures = validate_release_evidence(detached_comparative_target_label)
    if not any("AutoMQ (Java)" in failure for failure in failures):
        raise AssertionError("detached comparative target label was accepted")

    suffixed_comparative_comparison = sample_manifest()
    suffixed_comparative_comparison["commands"][comparative_command_index]["output"] = (
        suffixed_comparative_comparison["commands"][comparative_command_index][
            "output"
        ].replace(
            "COMPARISON: ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)",
            "COMPARISON: ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java) wrapper=1",
            1,
        )
    )
    failures = validate_release_evidence(suffixed_comparative_comparison)
    if not any(
        "comparative benchmark COMPARISON line" in failure
        and "exactly match selected target labels" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed comparative benchmark comparison line was accepted")

    reordered_comparative_target_labels = sample_manifest()
    reordered_comparative_target_labels["commands"][comparative_command_index][
        "output"
    ] = reordered_comparative_target_labels["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)",
        "ZMQ (Zig) vs AutoMQ (Java) vs Apache Kafka",
        1,
    )
    failures = validate_release_evidence(reordered_comparative_target_labels)
    if not any(
        "COMPARISON line target labels" in failure
        and "comparative target catalogue order" in failure
        for failure in failures
    ):
        raise AssertionError("reordered comparative target labels were accepted")

    embedded_comparative_pass = sample_manifest()
    embedded_comparative_pass["commands"][comparative_command_index]["output"] = (
        embedded_comparative_pass["commands"][comparative_command_index]["output"].replace(
            "result: pass",
            "previous result: pass",
        )
    )
    failures = validate_release_evidence(embedded_comparative_pass)
    if not any("result: pass" in failure for failure in failures):
        raise AssertionError("embedded comparative pass marker was accepted")

    bare_comparative_thresholds = sample_manifest()
    bare_comparative_thresholds["commands"][comparative_command_index]["output"] = (
        bare_comparative_thresholds["commands"][comparative_command_index][
            "output"
        ].replace(
            "thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%",
            "thresholds:",
            1,
        )
    )
    failures = validate_release_evidence(bare_comparative_thresholds)
    if not any("comparative benchmark thresholds" in failure for failure in failures):
        raise AssertionError("bare comparative benchmark thresholds were accepted")

    detached_comparative_thresholds = sample_manifest()
    detached_comparative_thresholds["commands"][comparative_command_index]["output"] = (
        detached_comparative_thresholds["commands"][comparative_command_index][
            "output"
        ].replace(
            "  thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%\n",
            "",
            1,
        )
        + "\nthresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%"
    )
    failures = validate_release_evidence(detached_comparative_thresholds)
    if not any("comparative benchmark thresholds" in failure for failure in failures):
        raise AssertionError("detached comparative benchmark thresholds were accepted")

    mismatched_comparative_trend_thresholds = sample_manifest()
    mismatched_comparative_trend_thresholds["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_trend_thresholds["commands"][comparative_command_index][
        "output"
    ].replace(
        "trend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x",
        "trend thresholds: throughput_ratio>=0.10x, p50_ratio<=9.99x, p99_ratio<=9.99x",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_trend_thresholds)
    if not any("comparative benchmark trend thresholds" in failure for failure in failures):
        raise AssertionError("mismatched comparative benchmark trend thresholds were accepted")

    detached_comparative_trend_thresholds = sample_manifest()
    detached_comparative_trend_thresholds["commands"][comparative_command_index][
        "output"
    ] = detached_comparative_trend_thresholds["commands"][comparative_command_index][
        "output"
    ].replace(
        "  trend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x\n",
        "",
        1,
    ) + "\ntrend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x"
    failures = validate_release_evidence(detached_comparative_trend_thresholds)
    if not any("comparative benchmark trend thresholds" in failure for failure in failures):
        raise AssertionError("detached comparative benchmark trend thresholds were accepted")

    missing_comparative_trend_baseline = sample_manifest()
    missing_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ] = missing_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ].replace(
        "  trend baseline: benchmarks/results-previous.json\n",
        "",
        1,
    )
    failures = validate_release_evidence(missing_comparative_trend_baseline)
    if not any("comparative benchmark trend baseline" in failure for failure in failures):
        raise AssertionError("missing comparative benchmark trend baseline was accepted")

    mismatched_comparative_trend_baseline = sample_manifest()
    mismatched_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ].replace(
        "trend baseline: benchmarks/results-previous.json",
        "trend baseline: benchmarks/other-results.json",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_trend_baseline)
    if not any("comparative benchmark trend baseline" in failure for failure in failures):
        raise AssertionError("mismatched comparative benchmark trend baseline was accepted")

    detached_comparative_trend_baseline = sample_manifest()
    detached_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ] = detached_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ].replace(
        "  trend baseline: benchmarks/results-previous.json\n",
        "",
        1,
    ) + "\ntrend baseline: benchmarks/results-previous.json"
    failures = validate_release_evidence(detached_comparative_trend_baseline)
    if not any("comparative benchmark trend baseline" in failure for failure in failures):
        raise AssertionError("detached comparative benchmark trend baseline was accepted")

    detached_comparative_pass = sample_manifest()
    detached_comparative_pass["commands"][comparative_command_index]["output"] = (
        detached_comparative_pass["commands"][comparative_command_index][
            "output"
        ].replace(
            "result: pass",
            "result: fail",
            1,
        )
        + "\nresult: pass"
    )
    failures = validate_release_evidence(detached_comparative_pass)
    if not any("comparative benchmark gate result" in failure for failure in failures):
        raise AssertionError("detached comparative benchmark pass marker was accepted")

    missing_comparative_latency_row = sample_manifest()
    missing_comparative_latency_row["commands"][comparative_command_index]["output"] = (
        missing_comparative_latency_row["commands"][comparative_command_index][
            "output"
        ].replace(
            "                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x\n",
            "",
            1,
        )
    )
    failures = validate_release_evidence(missing_comparative_latency_row)
    if not any("Fetch p99 metric row" in failure for failure in failures):
        raise AssertionError("missing comparative benchmark latency row was accepted")

    detached_comparative_latency_row = sample_manifest()
    detached_comparative_latency_row["commands"][comparative_command_index]["output"] = (
        detached_comparative_latency_row["commands"][comparative_command_index][
            "output"
        ].replace(
            "                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x\n",
            "",
            1,
        )
        + "\n  Fetch                  tput        7,000/s      6,500/s      6,000/s          1.08x          1.17x"
        + "\n                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x"
    )
    failures = validate_release_evidence(detached_comparative_latency_row)
    if not any("Fetch p99 metric row" in failure for failure in failures):
        raise AssertionError("detached comparative benchmark metric row was accepted")

    malformed_comparative_measurement = sample_manifest()
    malformed_comparative_measurement["commands"][comparative_command_index]["output"] = (
        malformed_comparative_measurement["commands"][comparative_command_index][
            "output"
        ].replace(
            "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x          1.25x",
            "  ApiVersions            tput          fast      9,000/s      8,000/s          1.11x          1.25x",
            1,
        )
    )
    failures = validate_release_evidence(malformed_comparative_measurement)
    if not any(
        "ApiVersions tput" in failure
        and "positive finite target measurements" in failure
        for failure in failures
    ):
        raise AssertionError("malformed comparative benchmark measurement was accepted")

    zero_comparative_measurement = sample_manifest()
    zero_comparative_measurement["commands"][comparative_command_index]["output"] = (
        zero_comparative_measurement["commands"][comparative_command_index][
            "output"
        ].replace(
            "                          p50           1.20ms       1.40ms       1.50ms          0.86x          0.80x",
            "                          p50           0.00ms       1.40ms       1.50ms          0.86x          0.80x",
            1,
        )
    )
    failures = validate_release_evidence(zero_comparative_measurement)
    if not any(
        "ApiVersions p50" in failure
        and "positive finite target measurements" in failure
        for failure in failures
    ):
        raise AssertionError("zero comparative benchmark measurement was accepted")

    interleaved_comparative_row_cells = sample_manifest()
    interleaved_comparative_row_cells["commands"][comparative_command_index][
        "output"
    ] = interleaved_comparative_row_cells["commands"][comparative_command_index][
        "output"
    ].replace(
        "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x          1.25x",
        "  ApiVersions            tput       10,000/s      9,000/s      1.11x       8,000/s          1.25x",
        1,
    )
    failures = validate_release_evidence(interleaved_comparative_row_cells)
    if not any(
        "ApiVersions tput target column 'AutoMQ'" in failure
        and "positive finite target measurement cell" in failure
        for failure in failures
    ):
        raise AssertionError("interleaved comparative benchmark row cells were accepted")

    missing_comparative_ratio_cell = sample_manifest()
    missing_comparative_ratio_cell["commands"][comparative_command_index][
        "output"
    ] = missing_comparative_ratio_cell["commands"][comparative_command_index][
        "output"
    ].replace(
        "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x          1.25x",
        "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x",
        1,
    )
    failures = validate_release_evidence(missing_comparative_ratio_cell)
    if not any(
        "ApiVersions tput" in failure and "exactly 2 ratio cells" in failure
        for failure in failures
    ):
        raise AssertionError("missing comparative benchmark ratio cell was accepted")

    malformed_comparative_ratio_cell = sample_manifest()
    malformed_comparative_ratio_cell["commands"][comparative_command_index][
        "output"
    ] = malformed_comparative_ratio_cell["commands"][comparative_command_index][
        "output"
    ].replace(
        "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x          1.25x",
        "  ApiVersions            tput       10,000/s      9,000/s      8,000/s          1.11x          fast",
        1,
    )
    failures = validate_release_evidence(malformed_comparative_ratio_cell)
    if not any(
        "ApiVersions tput ratio column 'ZMQ/AutoMQ'" in failure
        and "positive finite ratio cell" in failure
        for failure in failures
    ):
        raise AssertionError("malformed comparative benchmark ratio cell was accepted")

    zero_comparative_ratio_cell = sample_manifest()
    zero_comparative_ratio_cell["commands"][comparative_command_index][
        "output"
    ] = zero_comparative_ratio_cell["commands"][comparative_command_index][
        "output"
    ].replace(
        "                          p50           1.20ms       1.40ms       1.50ms          0.86x          0.80x",
        "                          p50           1.20ms       1.40ms       1.50ms          0.86x          0.00x",
        1,
    )
    failures = validate_release_evidence(zero_comparative_ratio_cell)
    if not any(
        "ApiVersions p50 ratio column 'ZMQ/AutoMQ'" in failure
        and "positive finite ratio cell" in failure
        for failure in failures
    ):
        raise AssertionError("zero comparative benchmark ratio cell was accepted")

    duplicate_comparative_comparison = sample_manifest()
    duplicate_comparative_comparison["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_comparison["commands"][comparative_command_index][
        "output"
    ].replace(
        "  COMPARATIVE BENCHMARK GATE",
        (
            "  COMPARISON: ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)\n"
            "  COMPARATIVE BENCHMARK GATE"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_comparison)
    if not any("exactly one COMPARISON" in failure for failure in failures):
        raise AssertionError("duplicate comparative benchmark comparison line was accepted")

    suffixed_comparative_gate_banner = sample_manifest()
    suffixed_comparative_gate_banner["commands"][comparative_command_index][
        "output"
    ] = suffixed_comparative_gate_banner["commands"][comparative_command_index][
        "output"
    ].replace(
        "  COMPARATIVE BENCHMARK GATE",
        "  COMPARATIVE BENCHMARK GATE\n  COMPARATIVE BENCHMARK GATE wrapper=1",
        1,
    )
    failures = validate_release_evidence(suffixed_comparative_gate_banner)
    if not any(
        "comparative benchmark gate banner" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed comparative benchmark gate marker was accepted")

    duplicate_comparative_target_label = sample_manifest()
    duplicate_comparative_target_label["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_target_label["commands"][comparative_command_index][
        "output"
    ].replace(
        "ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java)",
        "ZMQ (Zig) vs Apache Kafka vs AutoMQ (Java) vs AutoMQ (Java)",
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_target_label)
    if not any(
        "AutoMQ (Java)" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative target label was accepted")

    duplicate_comparative_table_header = sample_manifest()
    duplicate_comparative_table_header["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_table_header["commands"][comparative_command_index][
        "output"
    ].replace(
        "  Benchmark              Metric          ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ",
        (
            "  Benchmark              Metric          ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ\n"
            "  Benchmark              Metric          ZMQ        Kafka        AutoMQ      ZMQ/Kafka      ZMQ/AutoMQ"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_table_header)
    if not any("table header" in failure and "exactly once" in failure for failure in failures):
        raise AssertionError("duplicate comparative benchmark table header was accepted")

    duplicate_comparative_metric_row = sample_manifest()
    duplicate_comparative_metric_row["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_metric_row["commands"][comparative_command_index][
        "output"
    ].replace(
        "                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x",
        (
            "                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x\n"
            "                          p99           6.00ms       7.00ms       8.00ms          0.86x          0.75x"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_metric_row)
    if not any(
        "Fetch p99" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark metric row was accepted")

    duplicate_comparative_thresholds = sample_manifest()
    duplicate_comparative_thresholds["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_thresholds["commands"][comparative_command_index][
        "output"
    ].replace(
        "  thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%",
        (
            "  thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%\n"
            "  thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, p99_ratio<=20.00x, error_rate<=0.00%"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_thresholds)
    if not any(
        "comparative benchmark thresholds line" in failure and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark thresholds line was accepted")

    duplicate_comparative_trend_thresholds = sample_manifest()
    duplicate_comparative_trend_thresholds["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_trend_thresholds["commands"][comparative_command_index][
        "output"
    ].replace(
        "  trend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x",
        (
            "  trend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x\n"
            "  trend thresholds: throughput_ratio>=0.90x, p50_ratio<=1.25x, p99_ratio<=1.25x"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_trend_thresholds)
    if not any(
        "comparative benchmark trend thresholds line" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark trend thresholds line was accepted")

    duplicate_comparative_trend_baseline = sample_manifest()
    duplicate_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_trend_baseline["commands"][comparative_command_index][
        "output"
    ].replace(
        "  trend baseline: benchmarks/results-previous.json",
        (
            "  trend baseline: benchmarks/results-previous.json\n"
            "  trend baseline: benchmarks/results-previous.json"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_trend_baseline)
    if not any(
        "comparative benchmark trend baseline line" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark trend baseline line was accepted")

    duplicate_comparative_gate_result = sample_manifest()
    duplicate_comparative_gate_result["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_gate_result["commands"][comparative_command_index][
        "output"
    ].replace(
        "  result: pass",
        "  result: pass\n  result: pass",
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_gate_result)
    if not any(
        "comparative benchmark gate result line" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark gate result line was accepted")

    missing_comparative_results_artifact = sample_manifest()
    missing_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ] = missing_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ].replace(
        "  Results saved to benchmarks/results.json",
        "",
        1,
    )
    failures = validate_release_evidence(missing_comparative_results_artifact)
    if not any(
        "comparative benchmark results artifact line" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing comparative benchmark results artifact line was accepted"
        )

    mismatched_comparative_results_artifact = sample_manifest()
    mismatched_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ].replace(
        "Results saved to benchmarks/results.json",
        "Results saved to benchmarks/other-results.json",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_results_artifact)
    if not any(
        "comparative benchmark results artifact line" in failure
        and "benchmarks/results.json" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched comparative benchmark results artifact path was accepted"
        )

    detached_comparative_results_artifact = sample_manifest()
    detached_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ] = (
        detached_comparative_results_artifact["commands"][comparative_command_index][
            "output"
        ]
        .replace(
            "  Results saved to benchmarks/results.json",
            "",
            1,
        )
        .replace(
            "  COMPARATIVE BENCHMARK GATE",
            (
                "  Results saved to benchmarks/results.json\n"
                "  COMPARATIVE BENCHMARK GATE"
            ),
            1,
        )
    )
    failures = validate_release_evidence(detached_comparative_results_artifact)
    if not any(
        "comparative benchmark results artifact line" in failure
        and "after the COMPARATIVE BENCHMARK GATE" in failure
        for failure in failures
    ):
        raise AssertionError(
            "detached comparative benchmark results artifact line was accepted"
        )

    duplicate_comparative_results_artifact = sample_manifest()
    duplicate_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_results_artifact["commands"][comparative_command_index][
        "output"
    ].replace(
        "  Results saved to benchmarks/results.json",
        (
            "  Results saved to benchmarks/results.json\n"
            "  Results saved to benchmarks/results.json"
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_results_artifact)
    if not any(
        "comparative benchmark results artifact line" in failure
        and "exactly once" in failure
        for failure in failures
    ):
        raise AssertionError(
            "duplicate comparative benchmark results artifact line was accepted"
        )

    missing_comparative_profile_marker = sample_manifest()
    missing_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ] = missing_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ].replace(
        comparative_profile_marker,
        "",
        1,
    )
    failures = validate_release_evidence(missing_comparative_profile_marker)
    if not any(
        "comparative benchmark profile marker" in failure
        for failure in failures
    ):
        raise AssertionError("missing comparative benchmark profile marker was accepted")

    bare_comparative_profile_marker = sample_manifest()
    bare_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ] = bare_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ].replace(
        comparative_profile_marker,
        "  ok: comparative benchmark profile",
        1,
    )
    failures = validate_release_evidence(bare_comparative_profile_marker)
    if not any(
        "comparative benchmark profile marker" in failure
        and "key=value fields" in failure
        for failure in failures
    ):
        raise AssertionError("bare comparative benchmark profile marker was accepted")

    unknown_comparative_profile_field = sample_manifest()
    unknown_comparative_profile_field["commands"][comparative_command_index][
        "output"
    ] = unknown_comparative_profile_field["commands"][comparative_command_index][
        "output"
    ].replace(
        "source=command",
        "wrapper=1 source=command",
        1,
    )
    failures = validate_release_evidence(unknown_comparative_profile_field)
    if not any(
        "comparative benchmark profile marker contains unknown field" in failure
        and "wrapper" in failure
        for failure in failures
    ):
        raise AssertionError(
            "unknown comparative benchmark profile marker field was accepted"
        )

    duplicate_comparative_profile_field = sample_manifest()
    duplicate_comparative_profile_field["commands"][comparative_command_index][
        "output"
    ] = duplicate_comparative_profile_field["commands"][comparative_command_index][
        "output"
    ].replace(
        "source=command",
        "source=command source=command",
        1,
    )
    failures = validate_release_evidence(duplicate_comparative_profile_field)
    if not any(
        "comparative benchmark profile marker repeats field" in failure
        and "source" in failure
        for failure in failures
    ):
        raise AssertionError(
            "duplicate comparative benchmark profile marker field was accepted"
        )

    blank_comparative_profile_field = sample_manifest()
    blank_comparative_profile_field["commands"][comparative_command_index][
        "output"
    ] = blank_comparative_profile_field["commands"][comparative_command_index][
        "output"
    ].replace(
        "trend_baseline=benchmarks/results-previous.json",
        "trend_baseline=",
        1,
    )
    failures = validate_release_evidence(blank_comparative_profile_field)
    if not any(
        "comparative benchmark profile marker field 'trend_baseline'" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank comparative benchmark profile marker field was accepted"
        )

    wrapper_comparative_profile_marker = sample_manifest()
    wrapper_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ] = wrapper_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ].replace("source=command", "source=wrapper", 1)
    failures = validate_release_evidence(wrapper_comparative_profile_marker)
    if not any(
        "comparative benchmark profile marker field source" in failure
        and "command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "wrapper comparative benchmark profile marker was accepted"
        )

    mismatched_comparative_profile_required = sample_manifest()
    mismatched_comparative_profile_required["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_profile_required["commands"][
        comparative_command_index
    ]["output"].replace(
        "required=zmq,kafka,automq",
        "required=zmq,kafka",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_profile_required)
    if not any(
        "comparative benchmark profile marker field required" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched comparative benchmark profile required targets were accepted"
        )

    mismatched_comparative_profile_selected = sample_manifest()
    mismatched_comparative_profile_selected["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_profile_selected["commands"][
        comparative_command_index
    ]["output"].replace(
        "selected=zmq,kafka,automq",
        "selected=zmq,kafka",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_profile_selected)
    if not any(
        "comparative benchmark profile marker field selected" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched comparative benchmark profile selected targets were accepted"
        )

    mismatched_comparative_profile_results = sample_manifest()
    mismatched_comparative_profile_results["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_profile_results["commands"][
        comparative_command_index
    ]["output"].replace(
        "results_targets=zmq,kafka,automq",
        "results_targets=zmq,kafka",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_profile_results)
    if not any(
        "comparative benchmark profile marker field results_targets" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched comparative benchmark profile result targets were accepted"
        )

    mismatched_comparative_profile_iterations = sample_manifest()
    mismatched_comparative_profile_iterations["commands"][comparative_command_index][
        "output"
    ] = mismatched_comparative_profile_iterations["commands"][
        comparative_command_index
    ]["output"].replace(
        "iterations=api_versions:5000,produce_single:5000,produce_fresh:2000,fetch:3000,metadata:3000",
        "iterations=api_versions:1,produce_single:1,produce_fresh:1,fetch:1,metadata:1",
        1,
    )
    failures = validate_release_evidence(mismatched_comparative_profile_iterations)
    if not any(
        "comparative benchmark profile marker field iterations" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched comparative benchmark profile iterations were accepted"
        )

    detached_comparative_profile_marker = sample_manifest()
    detached_comparative_profile_marker["commands"][comparative_command_index][
        "output"
    ] = (
        detached_comparative_profile_marker["commands"][comparative_command_index][
            "output"
        ]
        .replace(
            comparative_profile_marker,
            "",
            1,
        )
        .replace(
            "  Results saved to benchmarks/results.json",
            comparative_profile_marker + "\n  Results saved to benchmarks/results.json",
            1,
        )
    )
    failures = validate_release_evidence(detached_comparative_profile_marker)
    if not any(
        "comparative benchmark profile marker" in failure
        and "after the comparative benchmark results artifact line" in failure
        for failure in failures
    ):
        raise AssertionError(
            "detached comparative benchmark profile marker was accepted"
        )

    bad_live_harness_output = sample_manifest()
    client_matrix_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "external client matrix"
    )
    bad_live_harness_output["commands"][client_matrix_command_index]["output"] = (
        "Build Summary: 3/3 steps succeeded\n"
        "test-client-matrix success"
    )
    failures = validate_release_evidence(bad_live_harness_output)
    if not any("ok: client matrix passed" in failure for failure in failures):
        raise AssertionError("missing live harness output marker was not reported")

    missing_client_matrix_required_profiles_command = sample_manifest()
    client_profiles_value = missing_client_matrix_required_profiles_command[
        "environment"
    ]["ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES"]
    missing_client_matrix_required_profiles_command["commands"][
        client_matrix_command_index
    ]["command"] = missing_client_matrix_required_profiles_command["commands"][
        client_matrix_command_index
    ][
        "command"
    ].replace(
        f"ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES={client_profiles_value} ",
        "",
    )
    failures = validate_release_evidence(
        missing_client_matrix_required_profiles_command
    )
    if not any(
        "external client matrix" in failure
        and "ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing client matrix required-profile command assignment was accepted"
        )

    missing_client_matrix_selected_profiles_command = sample_manifest()
    client_selected_profiles_value = missing_client_matrix_selected_profiles_command[
        "environment"
    ]["ZMQ_CLIENT_MATRIX_PROFILES"]
    missing_client_matrix_selected_profiles_command["commands"][
        client_matrix_command_index
    ]["command"] = missing_client_matrix_selected_profiles_command["commands"][
        client_matrix_command_index
    ][
        "command"
    ].replace(
        f"ZMQ_CLIENT_MATRIX_PROFILES={client_selected_profiles_value} ",
        "",
    )
    failures = validate_release_evidence(
        missing_client_matrix_selected_profiles_command
    )
    if not any(
        "external client matrix" in failure
        and "ZMQ_CLIENT_MATRIX_PROFILES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing client matrix selected-profile command assignment was accepted"
        )

    for env_name, assertion_message in (
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
            "missing client matrix required-tool command assignment was accepted",
        ),
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
            "missing client matrix required-semantic command assignment was accepted",
        ),
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES",
            "missing client matrix required-versioned-profile command assignment was accepted",
        ),
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES",
            "missing client matrix required-security-profile command assignment was accepted",
        ),
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES",
            "missing client matrix required-security-negative-profile command assignment was accepted",
        ),
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES",
            "missing client matrix required-oauth-profile command assignment was accepted",
        ),
        (
            "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES",
            "missing client matrix required-oauth-negative-profile command assignment was accepted",
        ),
    ):
        missing_client_matrix_command_assignment = sample_manifest()
        client_matrix_value = missing_client_matrix_command_assignment["environment"][
            env_name
        ]
        missing_client_matrix_command_assignment["commands"][client_matrix_command_index][
            "command"
        ] = missing_client_matrix_command_assignment["commands"][client_matrix_command_index][
            "command"
        ].replace(
            f"{env_name}={client_matrix_value} ",
            "",
        )
        failures = validate_release_evidence(missing_client_matrix_command_assignment)
        if not any(
            "external client matrix" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    mismatched_client_matrix_tools_command = sample_manifest()
    client_tools_value = mismatched_client_matrix_tools_command["environment"][
        "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS"
    ]
    mismatched_client_matrix_tools_command["commands"][client_matrix_command_index][
        "command"
    ] = mismatched_client_matrix_tools_command["commands"][
        client_matrix_command_index
    ][
        "command"
    ].replace(
        f"ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS={client_tools_value}",
        "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS=kcat,kafka-python",
    )
    failures = validate_release_evidence(mismatched_client_matrix_tools_command)
    if not any(
        "external client matrix" in failure
        and "ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched client matrix tool command assignment was accepted"
        )

    mismatched_client_matrix_semantics_command = sample_manifest()
    client_semantics_value = mismatched_client_matrix_semantics_command[
        "environment"
    ]["ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS"]
    mismatched_client_matrix_semantics_command["commands"][
        client_matrix_command_index
    ]["command"] = mismatched_client_matrix_semantics_command["commands"][
        client_matrix_command_index
    ][
        "command"
    ].replace(
        f"ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS={client_semantics_value}",
        "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS=basic,admin,groups",
    )
    failures = validate_release_evidence(mismatched_client_matrix_semantics_command)
    if not any(
        "external client matrix" in failure
        and "ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS" in failure
        and "security-negative" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched client matrix semantic command assignment was accepted"
        )

    bare_client_matrix_summary = sample_manifest()
    bare_client_matrix_summary["commands"][client_matrix_command_index]["output"] = (
        bare_client_matrix_summary["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
            "ok: client matrix passed",
        )
    )
    failures = validate_release_evidence(bare_client_matrix_summary)
    if not any("client matrix summary" in failure for failure in failures):
        raise AssertionError("bare client matrix summary marker was accepted")

    missing_client_matrix_summary_source = sample_manifest()
    missing_client_matrix_summary_source["commands"][client_matrix_command_index]["output"] = (
        missing_client_matrix_summary_source["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s)",
        )
    )
    failures = validate_release_evidence(missing_client_matrix_summary_source)
    if not any(
        "client matrix summary" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("client matrix summary without source=command was accepted")

    suffixed_client_matrix_summary = sample_manifest()
    suffixed_client_matrix_summary["commands"][client_matrix_command_index]["output"] = (
        suffixed_client_matrix_summary["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command wrapper=1",
        )
    )
    failures = validate_release_evidence(suffixed_client_matrix_summary)
    if not any(
        "client matrix summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed client matrix summary marker was accepted")

    mismatched_client_matrix_summary = sample_manifest()
    mismatched_client_matrix_summary["commands"][client_matrix_command_index]["output"] = (
        mismatched_client_matrix_summary["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec profile(s) source=command",
        )
    )
    failures = validate_release_evidence(mismatched_client_matrix_summary)
    if not any(
        "ZMQ_CLIENT_MATRIX_PROFILES" in failure and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched client matrix summary profiles were accepted")

    blank_client_matrix_summary = sample_manifest()
    blank_client_matrix_summary["commands"][client_matrix_command_index]["output"] = (
        blank_client_matrix_summary["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
            "ok: client matrix passed for kcat_sec, kafka_cli_sec,, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
        )
    )
    failures = validate_release_evidence(blank_client_matrix_summary)
    if not any("client matrix summary profiles" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank client matrix summary profile was accepted")

    duplicate_client_matrix_summary = sample_manifest()
    duplicate_client_matrix_summary["commands"][client_matrix_command_index]["output"] = (
        duplicate_client_matrix_summary["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, confluent_2_3, java_3_7, go_1_21 profile(s) source=command",
            (
                "ok: client matrix passed for kcat_sec, kafka_cli_sec, "
                "kafka_python_sec, confluent_2_3, java_3_7, go_1_21 "
                "profile(s) source=command\n"
                "ok: client matrix passed for kcat_sec, kafka_cli_sec, "
                "kafka_python_sec, confluent_2_3, java_3_7, go_1_21 "
                "profile(s) source=command"
            ),
        )
    )
    failures = validate_release_evidence(duplicate_client_matrix_summary)
    if not any(
        "client matrix summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate client matrix summary marker was accepted")

    missing_client_profile_marker = sample_manifest()
    missing_client_profile_marker["commands"][client_matrix_command_index]["output"] = (
        missing_client_profile_marker["commands"][client_matrix_command_index]["output"].replace(
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
            "",
        )
    )
    failures = validate_release_evidence(missing_client_profile_marker)
    if not any("ok: client matrix profile go_1_21" in failure for failure in failures):
        raise AssertionError("missing required client profile output marker was not reported")

    bare_client_profile_marker = sample_manifest()
    bare_client_profile_marker["commands"][client_matrix_command_index]["output"] = (
        bare_client_profile_marker["commands"][client_matrix_command_index]["output"].replace(
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
            "ok: client matrix profile go_1_21",
        )
    )
    failures = validate_release_evidence(bare_client_profile_marker)
    if not any(
        "passed client profile output marker" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("bare client profile output marker was accepted")

    missing_client_profile_source_marker = sample_manifest()
    missing_client_profile_source_marker["commands"][client_matrix_command_index]["output"] = (
        missing_client_profile_source_marker["commands"][client_matrix_command_index]["output"].replace(
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47",
        )
    )
    failures = validate_release_evidence(missing_client_profile_source_marker)
    if not any(
        "source=command" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("client profile output marker without source=command was accepted")

    mismatched_client_profile_tools = sample_manifest()
    mismatched_client_profile_tools["commands"][client_matrix_command_index]["output"] = (
        mismatched_client_profile_tools["commands"][client_matrix_command_index][
            "output"
        ].replace(
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
            "ok: client matrix profile go_1_21 passed for kafka-python against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        )
    )
    failures = validate_release_evidence(mismatched_client_profile_tools)
    if not any(
        "selected tools" in failure
        and "go_1_21" in failure
        and "go-kafka" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched client profile output tools were accepted")

    missing_client_bootstrap_provenance = sample_manifest()
    missing_client_bootstrap_provenance["environment"].pop("ZMQ_CLIENT_MATRIX_BOOTSTRAP")
    failures = validate_release_evidence(missing_client_bootstrap_provenance)
    if not any(
        "BOOTSTRAP" in failure
        and "client matrix profile go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("missing client bootstrap provenance was accepted")

    invalid_client_bootstrap_provenance = sample_manifest()
    invalid_client_bootstrap_provenance["environment"]["ZMQ_CLIENT_MATRIX_BOOTSTRAP"] = "localhost"
    failures = validate_release_evidence(invalid_client_bootstrap_provenance)
    if not any(
        "BOOTSTRAP" in failure
        and "client matrix profile go_1_21" in failure
        and "host:port" in failure
        for failure in failures
    ):
        raise AssertionError("malformed client bootstrap provenance was accepted")

    mismatched_client_profile_bootstrap = sample_manifest()
    mismatched_client_profile_bootstrap["commands"][
        client_matrix_command_index
    ]["output"] = mismatched_client_profile_bootstrap["commands"][
        client_matrix_command_index
    ]["output"].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka against 127.0.0.1:19092 version=segmentio-kafka-go-v0.4.47 source=command",
    )
    failures = validate_release_evidence(mismatched_client_profile_bootstrap)
    if not any(
        "selected bootstrap" in failure
        and "go_1_21" in failure
        and "localhost:9092" in failure
            for failure in failures
        ):
            raise AssertionError("mismatched client profile bootstrap output was accepted")

    missing_client_profile_version_marker = sample_manifest()
    missing_client_profile_version_marker["commands"][
        client_matrix_command_index
    ]["output"] = missing_client_profile_version_marker["commands"][
        client_matrix_command_index
    ]["output"].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 source=command",
    )
    failures = validate_release_evidence(missing_client_profile_version_marker)
    if not any(
        "version=segmentio-kafka-go-v0.4.47" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("missing client profile version marker was accepted")

    mismatched_client_profile_version_marker = sample_manifest()
    mismatched_client_profile_version_marker["commands"][
        client_matrix_command_index
    ]["output"] = mismatched_client_profile_version_marker["commands"][
        client_matrix_command_index
    ]["output"].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.46 source=command",
    )
    failures = validate_release_evidence(mismatched_client_profile_version_marker)
    if not any(
        "version=segmentio-kafka-go-v0.4.47" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched client profile version marker was accepted")

    duplicate_client_profile_marker = sample_manifest()
    duplicate_client_profile_marker["commands"][client_matrix_command_index][
        "output"
    ] = duplicate_client_profile_marker["commands"][client_matrix_command_index][
        "output"
    ].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        (
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command\n"
            "ok: client matrix profile go_1_21 passed for go-kafka against 127.0.0.1:19092 version=segmentio-kafka-go-v0.4.47 source=command"
        ),
    )
    failures = validate_release_evidence(duplicate_client_profile_marker)
    if not any("client matrix go_1_21 must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate client profile output marker was accepted")

    malformed_client_profile_bootstrap = sample_manifest()
    malformed_client_profile_bootstrap["commands"][
        client_matrix_command_index
    ]["output"] = malformed_client_profile_bootstrap["commands"][
        client_matrix_command_index
    ]["output"].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost version=segmentio-kafka-go-v0.4.47 source=command",
    )
    failures = validate_release_evidence(malformed_client_profile_bootstrap)
    if not any(
        "client profile output marker" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("malformed client profile bootstrap output was accepted")

    blank_client_profile_tools = sample_manifest()
    blank_client_profile_tools["commands"][
        client_matrix_command_index
    ]["output"] = blank_client_profile_tools["commands"][
        client_matrix_command_index
    ]["output"].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka,, against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
    )
    failures = validate_release_evidence(blank_client_profile_tools)
    if not any(
        "client profile output marker" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("embedded blank client profile tool was accepted")

    duplicate_client_profile_tools = sample_manifest()
    duplicate_client_profile_tools["commands"][
        client_matrix_command_index
    ]["output"] = duplicate_client_profile_tools["commands"][
        client_matrix_command_index
    ]["output"].replace(
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
        "ok: client matrix profile go_1_21 passed for go-kafka,go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command",
    )
    failures = validate_release_evidence(duplicate_client_profile_tools)
    if not any(
        "client profile output marker" in failure
        and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate client profile tool was accepted")

    unscoped_client_profile_tool_marker = sample_manifest()
    unscoped_client_profile_tool_marker["commands"][
        client_matrix_command_index
    ]["output"] = unscoped_client_profile_tool_marker["commands"][
        client_matrix_command_index
    ]["output"].replace(
        (
            "ok: go-kafka probes (basic,admin,groups) source=command\n"
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command"
        ),
        (
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command\n"
            "ok: go-kafka probes (basic,admin,groups) source=command"
        ),
    )
    failures = validate_release_evidence(unscoped_client_profile_tool_marker)
    if not any(
        "profile-scoped client tool probe" in failure
        and "go_1_21" in failure
        and "go-kafka" in failure
        for failure in failures
    ):
        raise AssertionError("unscoped client profile tool marker was accepted")

    split_client_profile_probe_block = sample_manifest()
    split_client_profile_probe_block["commands"][
        client_matrix_command_index
    ]["output"] = split_client_profile_probe_block["commands"][
        client_matrix_command_index
    ]["output"].replace(
        (
            "ok: go-kafka probes (basic,admin,groups) source=command\n"
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command"
        ),
        (
            "ok: go-kafka probes (basic,admin,groups) source=command\n"
            "ok: client matrix profile go_1_21 passed for kafka-python against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command\n"
            "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command"
        ),
    )
    failures = validate_release_evidence(split_client_profile_probe_block)
    if not any(
        "same-block client tool probe markers" in failure
        and "go_1_21" in failure
        and "go-kafka" in failure
        for failure in failures
    ):
        raise AssertionError("split client profile probe/pass block was accepted")

    detached_client_profile_block = sample_manifest()
    client_matrix_summary = (
        "ok: client matrix passed for kcat_sec, kafka_cli_sec, kafka_python_sec, "
        "confluent_2_3, java_3_7, go_1_21 profile(s) source=command"
    )
    go_client_profile_block = (
        "ok: go-kafka probes (basic,admin,groups) source=command\n"
        "ok: client matrix profile go_1_21 passed for go-kafka against localhost:9092 version=segmentio-kafka-go-v0.4.47 source=command"
    )
    detached_client_profile_block["commands"][client_matrix_command_index][
        "output"
    ] = detached_client_profile_block["commands"][client_matrix_command_index][
        "output"
    ].replace(
        go_client_profile_block,
        "",
        1,
    ).replace(
        client_matrix_summary,
        client_matrix_summary + "\n" + go_client_profile_block,
        1,
    )
    failures = validate_release_evidence(detached_client_profile_block)
    if not any(
        "passed client profile output marker" in failure and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("detached client profile block was accepted")

    missing_client_tool_marker = sample_manifest()
    missing_client_tool_marker["commands"][client_matrix_command_index]["output"] = (
        missing_client_tool_marker["commands"][client_matrix_command_index]["output"].replace(
            "ok: go-kafka probes (basic,admin,groups) source=command",
            "",
        )
    )
    failures = validate_release_evidence(missing_client_tool_marker)
    if not any("ok: go-kafka probes" in failure for failure in failures):
        raise AssertionError("missing required client tool output marker was not reported")

    missing_client_tool_probe_source = sample_manifest()
    missing_client_tool_probe_source["commands"][client_matrix_command_index]["output"] = (
        missing_client_tool_probe_source["commands"][client_matrix_command_index]["output"].replace(
            "ok: go-kafka probes (basic,admin,groups) source=command",
            "ok: go-kafka probes (basic,admin,groups)",
        )
    )
    failures = validate_release_evidence(missing_client_tool_probe_source)
    if not any(
        "source=command" in failure
        and "go-kafka" in failure
        for failure in failures
    ):
        raise AssertionError("client tool probe marker without source=command was accepted")

    blank_client_tool_probe_semantic = sample_manifest()
    blank_client_tool_probe_semantic["commands"][client_matrix_command_index][
        "output"
    ] = blank_client_tool_probe_semantic["commands"][client_matrix_command_index][
        "output"
    ].replace(
        "ok: go-kafka probes (basic,admin,groups) source=command",
        "ok: go-kafka probes (basic,,admin,groups) source=command",
    )
    failures = validate_release_evidence(blank_client_tool_probe_semantic)
    if not any("client tool probe semantics" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank client tool probe semantic was accepted")

    duplicate_client_tool_probe_semantic = sample_manifest()
    duplicate_client_tool_probe_semantic["commands"][client_matrix_command_index][
        "output"
    ] = duplicate_client_tool_probe_semantic["commands"][client_matrix_command_index][
        "output"
    ].replace(
        "ok: go-kafka probes (basic,admin,groups) source=command",
        "ok: go-kafka probes (basic,admin,groups,groups) source=command",
    )
    failures = validate_release_evidence(duplicate_client_tool_probe_semantic)
    if not any("client tool probe semantics" in failure and "duplicate" in failure for failure in failures):
        raise AssertionError("duplicate client tool probe semantic was accepted")

    duplicate_client_tool_probe_marker = sample_manifest()
    duplicate_client_tool_probe_marker["commands"][client_matrix_command_index][
        "output"
    ] = duplicate_client_tool_probe_marker["commands"][client_matrix_command_index][
        "output"
    ].replace(
        "ok: go-kafka probes (basic,admin,groups) source=command",
        "ok: go-kafka probes (basic) source=command\nok: go-kafka probes (basic,admin,groups) source=command",
    )
    failures = validate_release_evidence(duplicate_client_tool_probe_marker)
    if not any("client tool probe marker must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate client tool probe marker was accepted")

    missing_client_semantic_marker = sample_manifest()
    missing_client_semantic_marker["commands"][client_matrix_command_index]["output"] = (
        missing_client_semantic_marker["commands"][client_matrix_command_index]["output"].replace(
            "security-negative",
            "security-denied",
        )
    )
    failures = validate_release_evidence(missing_client_semantic_marker)
    if not any("security-negative" in failure for failure in failures):
        raise AssertionError("missing required client semantic output marker was not reported")

    missing_exact_client_semantic_marker = sample_manifest()
    missing_exact_client_semantic_marker["commands"][client_matrix_command_index]["output"] = (
        missing_exact_client_semantic_marker["commands"][client_matrix_command_index]["output"].replace(
            "security,security-negative",
            "security-negative",
        )
    )
    failures = validate_release_evidence(missing_exact_client_semantic_marker)
    if not any(
        "exact client semantic token" in failure and "security" in failure
        for failure in failures
    ):
        raise AssertionError("missing exact client semantic token was not reported")

    fake_client_semantic_marker = sample_manifest()
    fake_client_semantic_marker["commands"][client_matrix_command_index]["output"] = (
        fake_client_semantic_marker["commands"][client_matrix_command_index]["output"]
        .replace(",transactions", "")
        + "\nok: fake-client probes (transactions)"
    )
    failures = validate_release_evidence(fake_client_semantic_marker)
    if not any("transactions" in failure for failure in failures):
        raise AssertionError("unrecognized client probe semantic marker was accepted")

    misattributed_client_semantic_marker = sample_manifest()
    misattributed_client_semantic_marker["commands"][client_matrix_command_index]["output"] = (
        misattributed_client_semantic_marker["commands"][client_matrix_command_index]["output"]
        .replace(",transactions", "")
        .replace(
            "ok: kcat probes (basic,security,security-negative) source=command",
            "ok: kcat probes (basic,transactions,security,security-negative) source=command",
        )
    )
    failures = validate_release_evidence(misattributed_client_semantic_marker)
    if not any(
        "profile-selected tool marker" in failure and "transactions" in failure
        for failure in failures
    ):
        raise AssertionError("misattributed client semantic marker was accepted")

    kcat_security_detail_marker = (
        "ok: client security detail profile kcat_sec "
        "tool=kcat protocol=SASL_PLAINTEXT mechanism=OAUTHBEARER "
        "oauth=true positive=true security_negative=true "
        "oauth_negative=true sasl_negative=false tls_negative=false "
        "acl_negative=false source=command"
    )
    missing_client_security_detail = sample_manifest()
    missing_client_security_detail["commands"][client_matrix_command_index]["output"] = (
        missing_client_security_detail["commands"][client_matrix_command_index][
            "output"
        ].replace(kcat_security_detail_marker, "")
    )
    failures = validate_release_evidence(missing_client_security_detail)
    if not any(
        "client security detail marker" in failure
        and "kcat_sec" in failure
        and "kcat" in failure
        for failure in failures
    ):
        raise AssertionError("missing client security detail marker was accepted")

    bare_client_security_detail = sample_manifest()
    bare_client_security_detail["commands"][client_matrix_command_index]["output"] = (
        bare_client_security_detail["commands"][client_matrix_command_index][
            "output"
        ].replace(
            kcat_security_detail_marker,
            "ok: client security detail profile kcat_sec",
        )
    )
    failures = validate_release_evidence(bare_client_security_detail)
    if not any(
        "client security detail marker" in failure
        and "line shape" in failure
        and "kcat_sec" in failure
        for failure in failures
    ):
        raise AssertionError("bare client security detail marker was accepted")

    legacy_client_security_detail = sample_manifest()
    legacy_client_security_detail["commands"][client_matrix_command_index][
        "output"
    ] = legacy_client_security_detail["commands"][client_matrix_command_index][
        "output"
    ].replace(
        kcat_security_detail_marker,
        kcat_security_detail_marker.replace(" source=command", ""),
    )
    failures = validate_release_evidence(legacy_client_security_detail)
    if not any(
        "client security detail marker" in failure
        and "line shape" in failure
        and "kcat_sec" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing client security detail command source was accepted"
        )

    mismatched_client_security_detail = sample_manifest()
    mismatched_client_security_detail["commands"][
        client_matrix_command_index
    ]["output"] = mismatched_client_security_detail["commands"][
        client_matrix_command_index
    ]["output"].replace(
        kcat_security_detail_marker,
        kcat_security_detail_marker.replace("oauth_negative=true", "oauth_negative=false"),
    )
    failures = validate_release_evidence(mismatched_client_security_detail)
    if not any(
        "client security detail marker" in failure
        and "oauth_negative=true" in failure
        and "kcat_sec" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched client security detail marker was accepted")

    mismatched_client_security_detail_source = sample_manifest()
    mismatched_client_security_detail_source["commands"][client_matrix_command_index][
        "output"
    ] = mismatched_client_security_detail_source["commands"][
        client_matrix_command_index
    ]["output"].replace(
        kcat_security_detail_marker,
        kcat_security_detail_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_client_security_detail_source)
    if not any(
        "client security detail marker" in failure
        and "source=command" in failure
        and "kcat_sec" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched client security detail command source was accepted"
        )

    duplicate_client_security_detail = sample_manifest()
    duplicate_client_security_detail["commands"][client_matrix_command_index][
        "output"
    ] = duplicate_client_security_detail["commands"][client_matrix_command_index][
        "output"
    ].replace(
        kcat_security_detail_marker,
        (
            kcat_security_detail_marker.replace(
                "oauth_negative=true",
                "oauth_negative=false",
            )
            + "\n"
            + kcat_security_detail_marker
        ),
    )
    failures = validate_release_evidence(duplicate_client_security_detail)
    if not any("client security detail marker must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate client security detail marker was accepted")

    detached_client_security_detail = sample_manifest()
    kcat_profile_marker = (
        "ok: client matrix profile kcat_sec passed for kcat against localhost:9092 version=kcat-1.7.1 source=command"
    )
    detached_client_security_detail["commands"][client_matrix_command_index][
        "output"
    ] = detached_client_security_detail["commands"][client_matrix_command_index][
        "output"
    ].replace(
        kcat_security_detail_marker + "\n",
        "",
        1,
    ).replace(
        client_matrix_summary,
        client_matrix_summary + "\n" + kcat_security_detail_marker + "\n" + kcat_profile_marker,
        1,
    )
    failures = validate_release_evidence(detached_client_security_detail)
    if not any(
        "client security detail marker" in failure and "kcat_sec" in failure
        for failure in failures
    ):
        raise AssertionError("detached client security detail marker was accepted")

    missing_chaos_scenario_marker = sample_manifest()
    chaos_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "broker chaos harness"
    )

    missing_chaos_scenario_command = sample_manifest()
    chaos_scenarios_value = missing_chaos_scenario_command["environment"][
        "ZMQ_CHAOS_REQUIRED_SCENARIOS"
    ]
    missing_chaos_scenario_command["commands"][chaos_command_index][
        "command"
    ] = missing_chaos_scenario_command["commands"][chaos_command_index][
        "command"
    ].replace(
        f"ZMQ_CHAOS_REQUIRED_SCENARIOS={chaos_scenarios_value} ",
        "",
    )
    failures = validate_release_evidence(missing_chaos_scenario_command)
    if not any(
        "broker chaos harness" in failure
        and "ZMQ_CHAOS_REQUIRED_SCENARIOS" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing chaos required-scenarios command assignment was accepted"
        )

    for env_name, assertion_message in (
        (
            "ZMQ_CHAOS_REQUIRED_NETWORK_PHASES",
            "missing chaos required-network-phases command assignment was accepted",
        ),
        (
            "ZMQ_CHAOS_NETWORK_MATRIX",
            "missing chaos network-matrix command assignment was accepted",
        ),
    ):
        missing_chaos_network_command = sample_manifest()
        chaos_network_value = missing_chaos_network_command["environment"][env_name]
        missing_chaos_network_command["commands"][chaos_command_index][
            "command"
        ] = missing_chaos_network_command["commands"][chaos_command_index][
            "command"
        ].replace(
            f"{env_name}={chaos_network_value} ",
            "",
        )
        failures = validate_release_evidence(missing_chaos_network_command)
        if not any(
            "broker chaos harness" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    bare_chaos_harness_summary = sample_manifest()
    bare_chaos_harness_summary["commands"][chaos_command_index]["output"] = (
        bare_chaos_harness_summary["commands"][chaos_command_index]["output"].replace(
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
            "ok: chaos harness passed",
        )
    )
    failures = validate_release_evidence(bare_chaos_harness_summary)
    if not any("chaos harness summary" in failure for failure in failures):
        raise AssertionError("bare chaos harness summary marker was accepted")

    missing_source_chaos_harness_summary = sample_manifest()
    missing_source_chaos_harness_summary["commands"][chaos_command_index][
        "output"
    ] = missing_source_chaos_harness_summary["commands"][chaos_command_index][
        "output"
    ].replace(
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition source=command"
        ),
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition"
        ),
    )
    failures = validate_release_evidence(missing_source_chaos_harness_summary)
    if not any(
        "chaos harness summary" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("chaos harness summary without source=command was accepted")

    mismatched_source_chaos_harness_summary = sample_manifest()
    mismatched_source_chaos_harness_summary["commands"][chaos_command_index][
        "output"
    ] = mismatched_source_chaos_harness_summary["commands"][chaos_command_index][
        "output"
    ].replace(
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition source=command"
        ),
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition source=wrapper"
        ),
    )
    failures = validate_release_evidence(mismatched_source_chaos_harness_summary)
    if not any(
        "chaos harness summary" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("chaos harness summary with wrapper source was accepted")

    suffixed_chaos_harness_summary = sample_manifest()
    suffixed_chaos_harness_summary["commands"][chaos_command_index][
        "output"
    ] = suffixed_chaos_harness_summary["commands"][chaos_command_index][
        "output"
    ].replace(
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition source=command"
        ),
        (
            "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
            "clock-skewed-records, s3-outage, network-partition source=command wrapper=1"
        ),
    )
    failures = validate_release_evidence(suffixed_chaos_harness_summary)
    if not any(
        "broker chaos harness summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed chaos harness summary marker was accepted")

    mismatched_chaos_harness_summary = sample_manifest()
    mismatched_chaos_harness_summary["commands"][chaos_command_index]["output"] = (
        mismatched_chaos_harness_summary["commands"][chaos_command_index]["output"].replace(
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
            (
                "ok: chaos harness passed for sigkill-restart, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
        )
    )
    failures = validate_release_evidence(mismatched_chaos_harness_summary)
    if not any(
        "ZMQ_CHAOS_REQUIRED_SCENARIOS" in failure and "slow-partial-client" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched chaos harness summary scenarios were accepted")

    unrequired_chaos_harness_summary = sample_manifest()
    unrequired_chaos_harness_summary["commands"][chaos_command_index]["output"] = (
        unrequired_chaos_harness_summary["commands"][chaos_command_index]["output"].replace(
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition, "
                "live-s3-outage source=command"
            ),
        )
    )
    failures = validate_release_evidence(unrequired_chaos_harness_summary)
    if not any("outside ZMQ_CHAOS_REQUIRED_SCENARIOS" in failure and "live-s3-outage" in failure for failure in failures):
        raise AssertionError("unrequired chaos harness summary scenario was accepted")

    blank_chaos_harness_summary = sample_manifest()
    blank_chaos_harness_summary["commands"][chaos_command_index]["output"] = (
        blank_chaos_harness_summary["commands"][chaos_command_index]["output"].replace(
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records,, s3-outage, network-partition source=command"
            ),
        )
    )
    failures = validate_release_evidence(blank_chaos_harness_summary)
    if not any("chaos harness summary scenarios" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank chaos harness summary scenario was accepted")

    duplicate_chaos_harness_summary = sample_manifest()
    duplicate_chaos_harness_summary["commands"][chaos_command_index]["output"] = (
        duplicate_chaos_harness_summary["commands"][chaos_command_index][
            "output"
        ].replace(
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
            (
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command\n"
                "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
                "clock-skewed-records, s3-outage, network-partition source=command"
            ),
        )
    )
    failures = validate_release_evidence(duplicate_chaos_harness_summary)
    if not any(
        "broker chaos harness summary output marker must appear exactly once"
        in failure
        for failure in failures
    ):
        raise AssertionError("duplicate chaos harness summary marker was accepted")

    bare_chaos_sigkill_marker = sample_manifest()
    bare_chaos_sigkill_marker["commands"][chaos_command_index]["output"] = (
        bare_chaos_sigkill_marker["commands"][chaos_command_index]["output"].replace(
            "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=1 source=command",
            "ok: chaos sigkill-restart",
        )
    )
    failures = validate_release_evidence(bare_chaos_sigkill_marker)
    if not any("second_offset" in failure for failure in failures):
        raise AssertionError("bare chaos sigkill-restart marker was accepted")

    unverified_chaos_sigkill_marker = sample_manifest()
    unverified_chaos_sigkill_marker["commands"][chaos_command_index]["output"] = (
        unverified_chaos_sigkill_marker["commands"][chaos_command_index][
            "output"
        ].replace(
            "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=1 source=command",
            "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2 first_offset=0 second_offset=0 source=command",
        )
    )
    failures = validate_release_evidence(unverified_chaos_sigkill_marker)
    if not any("positive second_offset" in failure for failure in failures):
        raise AssertionError("unverified chaos sigkill-restart marker was accepted")

    bare_chaos_slow_partial_marker = sample_manifest()
    bare_chaos_slow_partial_marker["commands"][chaos_command_index]["output"] = (
        bare_chaos_slow_partial_marker["commands"][chaos_command_index][
            "output"
        ].replace(
            "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
            "ok: chaos slow-partial-client",
        )
    )
    failures = validate_release_evidence(bare_chaos_slow_partial_marker)
    if not any(
        "partial_frame=true truncated_frame=true survived=true" in failure
        for failure in failures
    ):
        raise AssertionError("bare chaos slow-partial-client marker was accepted")

    missing_chaos_detail_command_source = sample_manifest()
    missing_chaos_detail_command_source["commands"][chaos_command_index][
        "output"
    ] = missing_chaos_detail_command_source["commands"][chaos_command_index][
        "output"
    ].replace(
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true",
    )
    failures = validate_release_evidence(missing_chaos_detail_command_source)
    if not any(
        "source=command" in failure and "slow-partial-client" in failure
        for failure in failures
    ):
        raise AssertionError("missing chaos detail command source was accepted")

    mismatched_chaos_detail_command_source = sample_manifest()
    mismatched_chaos_detail_command_source["commands"][chaos_command_index][
        "output"
    ] = mismatched_chaos_detail_command_source["commands"][chaos_command_index][
        "output"
    ].replace(
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=wrapper",
    )
    failures = validate_release_evidence(mismatched_chaos_detail_command_source)
    if not any(
        "source=command" in failure and "slow-partial-client" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched chaos detail command source was accepted")

    bare_chaos_clock_marker = sample_manifest()
    bare_chaos_clock_marker["commands"][chaos_command_index]["output"] = (
        bare_chaos_clock_marker["commands"][chaos_command_index]["output"].replace(
            "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command",
            "ok: chaos clock-skewed-records",
        )
    )
    failures = validate_release_evidence(bare_chaos_clock_marker)
    if not any(
        "future_timestamp=true fetched=true serving=true" in failure
        for failure in failures
    ):
        raise AssertionError("bare chaos clock-skewed-records marker was accepted")

    bare_chaos_s3_outage_marker = sample_manifest()
    bare_chaos_s3_outage_marker["commands"][chaos_command_index]["output"] = (
        bare_chaos_s3_outage_marker["commands"][chaos_command_index]["output"].replace(
            "ok: chaos s3-outage rejected=true error_code=56 base_offset_negative=true serving=true source=command",
            "ok: chaos s3-outage",
        )
    )
    failures = validate_release_evidence(bare_chaos_s3_outage_marker)
    if not any("base_offset_negative=true serving=true" in failure for failure in failures):
        raise AssertionError("bare chaos s3-outage marker was accepted")

    unverified_chaos_s3_outage_marker = sample_manifest()
    unverified_chaos_s3_outage_marker["commands"][chaos_command_index]["output"] = (
        unverified_chaos_s3_outage_marker["commands"][chaos_command_index][
            "output"
        ].replace(
            "ok: chaos s3-outage rejected=true error_code=56 base_offset_negative=true serving=true source=command",
            "ok: chaos s3-outage rejected=true error_code=0 base_offset_negative=true serving=true source=command",
        )
    )
    failures = validate_release_evidence(unverified_chaos_s3_outage_marker)
    if not any("error_code=<nonzero>" in failure for failure in failures):
        raise AssertionError("unverified chaos s3-outage marker was accepted")

    detached_chaos_scenario_detail = sample_manifest()
    detached_chaos_detail_marker = (
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true "
        "survived=true source=command"
    )
    detached_chaos_summary = (
        "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
        "clock-skewed-records, s3-outage, network-partition source=command"
    )
    detached_chaos_output = detached_chaos_scenario_detail["commands"][
        chaos_command_index
    ]["output"].replace(detached_chaos_detail_marker + "\n", "")
    detached_chaos_output = detached_chaos_output.replace(
        detached_chaos_summary,
        detached_chaos_summary + "\n" + detached_chaos_detail_marker,
    )
    detached_chaos_scenario_detail["commands"][chaos_command_index][
        "output"
    ] = detached_chaos_output
    failures = validate_release_evidence(detached_chaos_scenario_detail)
    if not any(
        "before the broker chaos harness summary line" in failure
        and "slow-partial-client" in failure
        for failure in failures
    ):
        raise AssertionError("detached chaos scenario detail marker was accepted")

    duplicate_chaos_scenario_detail = sample_manifest()
    duplicate_chaos_scenario_detail["commands"][chaos_command_index]["output"] = (
        duplicate_chaos_scenario_detail["commands"][chaos_command_index][
            "output"
        ].replace(
            detached_chaos_detail_marker,
            detached_chaos_detail_marker + "\n" + detached_chaos_detail_marker,
        )
    )
    failures = validate_release_evidence(duplicate_chaos_scenario_detail)
    if not any(
        "chaos slow-partial-client detail marker must not repeat" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate chaos scenario detail marker was accepted")

    live_s3_chaos_marker = sample_manifest()
    live_s3_chaos_marker["environment"]["ZMQ_CHAOS_REQUIRED_SCENARIOS"] += (
        ",live-s3-outage"
    )
    live_s3_chaos_marker["environment"].update({
        "ZMQ_CHAOS_S3_ENDPOINT": "s3-chaos.example.test",
        "ZMQ_CHAOS_S3_PORT": "9443",
        "ZMQ_CHAOS_S3_BUCKET": "zmq-chaos-release",
        "ZMQ_CHAOS_S3_ACCESS_KEY": "chaos-ak",
        "ZMQ_CHAOS_S3_SECRET_KEY": "chaos-sk",
        "ZMQ_CHAOS_S3_SCHEME": "https",
        "ZMQ_CHAOS_S3_REGION": "us-west-2",
        "ZMQ_CHAOS_S3_PATH_STYLE": "false",
        "ZMQ_CHAOS_S3_DOWN": "true",
        "ZMQ_CHAOS_S3_UP": "true",
    })
    live_s3_chaos_marker["commands"][chaos_command_index]["command"] = (
        "ZMQ_CHAOS_S3_ENDPOINT=s3-chaos.example.test "
        "ZMQ_CHAOS_S3_PORT=9443 "
        "ZMQ_CHAOS_S3_BUCKET=zmq-chaos-release "
        "ZMQ_CHAOS_S3_SCHEME=https "
        "ZMQ_CHAOS_S3_REGION=us-west-2 "
        "ZMQ_CHAOS_S3_PATH_STYLE=false "
        + live_s3_chaos_marker["commands"][chaos_command_index]["command"]
        .replace(
            (
                "ZMQ_CHAOS_REQUIRED_SCENARIOS="
                "sigkill-restart,slow-partial-client,clock-skewed-records,"
                "s3-outage,network-partition"
            ),
            (
                "ZMQ_CHAOS_REQUIRED_SCENARIOS="
                "sigkill-restart,slow-partial-client,clock-skewed-records,"
                "s3-outage,network-partition,live-s3-outage"
            ),
        )
    )
    live_s3_chaos_summary = (
        "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
        "clock-skewed-records, s3-outage, network-partition source=command"
    )
    live_s3_chaos_summary_with_live = (
        "ok: chaos harness passed for sigkill-restart, slow-partial-client, "
        "clock-skewed-records, s3-outage, network-partition, live-s3-outage "
        "source=command"
    )
    live_s3_chaos_detail_markers = [
        (
            "ok: chaos live-s3-outage provider "
            "endpoint=s3-chaos.example.test:9443 bucket=zmq-chaos-release "
            "scheme=https region=us-west-2 path_style=false source=command"
        ),
        (
            "ok: chaos live-s3-outage down=true healed=true "
            "fail_closed=true recovered=true source=command"
        ),
    ]
    live_s3_chaos_marker["commands"][chaos_command_index]["output"] = (
        live_s3_chaos_marker["commands"][chaos_command_index]["output"].replace(
            live_s3_chaos_summary,
            "\n".join(live_s3_chaos_detail_markers)
            + "\n"
            + live_s3_chaos_summary_with_live,
        )
    )
    failures = validate_release_evidence(live_s3_chaos_marker)
    if failures:
        raise AssertionError(
            "live-S3 chaos detail marker fixture was rejected: "
            + "; ".join(failures)
        )

    fallback_live_s3_chaos_marker = live_s3_chaos_marker.copy()
    fallback_live_s3_chaos_marker["environment"] = dict(
        live_s3_chaos_marker["environment"]
    )
    fallback_live_s3_chaos_marker["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    for suffix in ("ENDPOINT", "PORT", "BUCKET", "SCHEME", "REGION", "PATH_STYLE"):
        fallback_live_s3_chaos_marker["environment"].pop(f"ZMQ_CHAOS_S3_{suffix}")
    fallback_live_s3_chaos_marker["commands"][chaos_command_index][
        "command"
    ] = fallback_live_s3_chaos_marker["commands"][chaos_command_index][
        "command"
    ].replace(
        "ZMQ_CHAOS_S3_ENDPOINT=s3-chaos.example.test "
        "ZMQ_CHAOS_S3_PORT=9443 "
        "ZMQ_CHAOS_S3_BUCKET=zmq-chaos-release "
        "ZMQ_CHAOS_S3_SCHEME=https "
        "ZMQ_CHAOS_S3_REGION=us-west-2 "
        "ZMQ_CHAOS_S3_PATH_STYLE=false ",
        "ZMQ_S3_ENDPOINT=s3-bench.example.test "
        "ZMQ_S3_PORT=9443 "
        "ZMQ_S3_BUCKET=zmq-live-bench "
        "ZMQ_S3_SCHEME=http "
        "ZMQ_S3_REGION=us-east-1 "
        "ZMQ_S3_PATH_STYLE=true ",
    )
    fallback_live_s3_chaos_marker["commands"][chaos_command_index][
        "output"
    ] = fallback_live_s3_chaos_marker["commands"][chaos_command_index][
        "output"
    ].replace(
        (
            "ok: chaos live-s3-outage provider "
            "endpoint=s3-chaos.example.test:9443 bucket=zmq-chaos-release "
            "scheme=https region=us-west-2 path_style=false source=command"
        ),
        (
            "ok: chaos live-s3-outage provider "
            "endpoint=s3-bench.example.test:9443 bucket=zmq-live-bench "
            "scheme=http region=us-east-1 path_style=true source=command"
        ),
    )
    failures = validate_release_evidence(fallback_live_s3_chaos_marker)
    if failures:
        raise AssertionError(
            "live-S3 chaos fallback command provenance was rejected: "
            + "; ".join(failures)
        )

    blank_chaos_live_s3_endpoint = fallback_live_s3_chaos_marker.copy()
    blank_chaos_live_s3_endpoint["environment"] = dict(
        fallback_live_s3_chaos_marker["environment"]
    )
    blank_chaos_live_s3_endpoint["environment"]["ZMQ_CHAOS_S3_ENDPOINT"] = ""
    blank_chaos_live_s3_endpoint["commands"] = [
        dict(command) for command in fallback_live_s3_chaos_marker["commands"]
    ]
    failures = validate_release_evidence(blank_chaos_live_s3_endpoint)
    if not any(
        "ZMQ_CHAOS_S3_ENDPOINT" in failure and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError("blank chaos live-S3 endpoint fallback was accepted")

    blank_chaos_live_s3_tls_ca = fallback_live_s3_chaos_marker.copy()
    blank_chaos_live_s3_tls_ca["environment"] = dict(
        fallback_live_s3_chaos_marker["environment"]
    )
    blank_chaos_live_s3_tls_ca["environment"]["ZMQ_S3_TLS_CA_FILE"] = (
        "/tmp/fallback-ca.pem"
    )
    blank_chaos_live_s3_tls_ca["environment"]["ZMQ_CHAOS_S3_TLS_CA_FILE"] = ""
    blank_chaos_live_s3_tls_ca["commands"] = [
        dict(command) for command in fallback_live_s3_chaos_marker["commands"]
    ]
    failures = validate_release_evidence(blank_chaos_live_s3_tls_ca)
    if not any(
        "ZMQ_CHAOS_S3_TLS_CA_FILE" in failure and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError("blank chaos live-S3 TLS CA fallback was accepted")

    bare_live_s3_chaos_marker = live_s3_chaos_marker.copy()
    bare_live_s3_chaos_marker["environment"] = dict(live_s3_chaos_marker["environment"])
    bare_live_s3_chaos_marker["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    bare_live_s3_chaos_marker["commands"][chaos_command_index]["output"] = (
        bare_live_s3_chaos_marker["commands"][chaos_command_index]["output"].replace(
            (
                "ok: chaos live-s3-outage down=true healed=true "
                "fail_closed=true recovered=true source=command"
            ),
            "ok: chaos live-s3-outage",
        )
    )
    failures = validate_release_evidence(bare_live_s3_chaos_marker)
    if not any("down=true healed=true fail_closed=true recovered=true" in failure for failure in failures):
        raise AssertionError("bare chaos live-s3-outage marker was accepted")

    missing_live_s3_provider_marker = live_s3_chaos_marker.copy()
    missing_live_s3_provider_marker["environment"] = dict(live_s3_chaos_marker["environment"])
    missing_live_s3_provider_marker["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    missing_live_s3_provider_marker["commands"][chaos_command_index]["output"] = (
        "\n".join(
            line
            for line in missing_live_s3_provider_marker["commands"][chaos_command_index][
                "output"
            ].splitlines()
            if not line.startswith("ok: chaos live-s3-outage provider")
        )
    )
    failures = validate_release_evidence(missing_live_s3_provider_marker)
    if not any("chaos live-S3 provider marker" in failure for failure in failures):
        raise AssertionError("missing chaos live-S3 provider marker was accepted")

    mismatched_live_s3_provider_marker = live_s3_chaos_marker.copy()
    mismatched_live_s3_provider_marker["environment"] = dict(live_s3_chaos_marker["environment"])
    mismatched_live_s3_provider_marker["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    mismatched_live_s3_provider_marker["commands"][chaos_command_index]["output"] = (
        mismatched_live_s3_provider_marker["commands"][chaos_command_index][
            "output"
        ].replace("bucket=zmq-chaos-release", "bucket=wrong-chaos-bucket")
    )
    failures = validate_release_evidence(mismatched_live_s3_provider_marker)
    if not any("chaos live-S3 provider marker must match" in failure for failure in failures):
        raise AssertionError("mismatched chaos live-S3 provider marker was accepted")

    mismatched_live_s3_provider_source = live_s3_chaos_marker.copy()
    mismatched_live_s3_provider_source["environment"] = dict(
        live_s3_chaos_marker["environment"]
    )
    mismatched_live_s3_provider_source["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    mismatched_live_s3_provider_source["commands"][chaos_command_index][
        "output"
    ] = mismatched_live_s3_provider_source["commands"][chaos_command_index][
        "output"
    ].replace(
        (
            "ok: chaos live-s3-outage provider "
            "endpoint=s3-chaos.example.test:9443 bucket=zmq-chaos-release "
            "scheme=https region=us-west-2 path_style=false source=command"
        ),
        (
            "ok: chaos live-s3-outage provider "
            "endpoint=s3-chaos.example.test:9443 bucket=zmq-chaos-release "
            "scheme=https region=us-west-2 path_style=false source=wrapper"
        ),
    )
    failures = validate_release_evidence(mismatched_live_s3_provider_source)
    if not any("source=command" in failure for failure in failures):
        raise AssertionError("mismatched chaos live-S3 provider source was accepted")

    duplicate_live_s3_provider_marker = live_s3_chaos_marker.copy()
    duplicate_live_s3_provider_marker["environment"] = dict(
        live_s3_chaos_marker["environment"]
    )
    duplicate_live_s3_provider_marker["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    duplicate_live_s3_provider_marker["commands"][chaos_command_index][
        "output"
    ] = duplicate_live_s3_provider_marker["commands"][chaos_command_index][
        "output"
    ].replace(
        live_s3_chaos_detail_markers[0],
        live_s3_chaos_detail_markers[0] + "\n" + live_s3_chaos_detail_markers[0],
    )
    failures = validate_release_evidence(duplicate_live_s3_provider_marker)
    if not any(
        "chaos live-S3 provider marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate chaos live-S3 provider marker was accepted")

    missing_live_s3_hook = live_s3_chaos_marker.copy()
    missing_live_s3_hook["environment"] = dict(live_s3_chaos_marker["environment"])
    missing_live_s3_hook["environment"].pop("ZMQ_CHAOS_S3_DOWN")
    failures = validate_release_evidence(missing_live_s3_hook)
    if not any("ZMQ_CHAOS_S3_DOWN" in failure for failure in failures):
        raise AssertionError("missing chaos live-S3 hook provenance was accepted")

    missing_live_s3_command_provenance = live_s3_chaos_marker.copy()
    missing_live_s3_command_provenance["environment"] = dict(
        live_s3_chaos_marker["environment"]
    )
    missing_live_s3_command_provenance["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    missing_live_s3_command_provenance["commands"][chaos_command_index][
        "command"
    ] = missing_live_s3_command_provenance["commands"][chaos_command_index][
        "command"
    ].replace(
        "ZMQ_CHAOS_S3_SCHEME=https ",
        "",
    )
    failures = validate_release_evidence(missing_live_s3_command_provenance)
    if not any(
        "broker chaos harness" in failure and "ZMQ_CHAOS_S3_SCHEME" in failure
        for failure in failures
    ):
        raise AssertionError("missing chaos live-S3 command provenance was accepted")

    invalid_live_s3_path_style = live_s3_chaos_marker.copy()
    invalid_live_s3_path_style["environment"] = dict(live_s3_chaos_marker["environment"])
    invalid_live_s3_path_style["environment"]["ZMQ_CHAOS_S3_PATH_STYLE"] = "sometimes"
    failures = validate_release_evidence(invalid_live_s3_path_style)
    if not any("ZMQ_CHAOS_S3_PATH_STYLE" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid chaos live-S3 path-style provenance was accepted")

    detached_live_s3_chaos_detail = live_s3_chaos_marker.copy()
    detached_live_s3_chaos_detail["environment"] = dict(live_s3_chaos_marker["environment"])
    detached_live_s3_chaos_detail["commands"] = [
        dict(command) for command in live_s3_chaos_marker["commands"]
    ]
    detached_live_s3_output = detached_live_s3_chaos_detail["commands"][
        chaos_command_index
    ]["output"]
    for marker in live_s3_chaos_detail_markers:
        detached_live_s3_output = detached_live_s3_output.replace(marker + "\n", "")
    detached_live_s3_output = detached_live_s3_output.replace(
        live_s3_chaos_summary_with_live,
        live_s3_chaos_summary_with_live + "\n" + "\n".join(live_s3_chaos_detail_markers),
    )
    detached_live_s3_chaos_detail["commands"][chaos_command_index][
        "output"
    ] = detached_live_s3_output
    failures = validate_release_evidence(detached_live_s3_chaos_detail)
    if not any(
        "before the broker chaos harness summary line" in failure
        and "live-S3" in failure
        for failure in failures
    ):
        raise AssertionError("detached chaos live-S3 detail marker was accepted")

    missing_chaos_scenario_marker["commands"][chaos_command_index]["output"] = (
        missing_chaos_scenario_marker["commands"][chaos_command_index]["output"].replace(
            "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
            "",
        )
    )
    failures = validate_release_evidence(missing_chaos_scenario_marker)
    if not any(
        "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command"
        in failure
        for failure in failures
    ):
        raise AssertionError("missing chaos required scenario output marker was not reported")

    chaos_network_phase_marker = (
        "ok: chaos network-partition phase broker-link down=true "
        "observed=failed healed=true recovered=true expect=fail source=command"
    )
    chaos_network_summary_marker = "ok: chaos network-partition source=command"
    stale_chaos_network_phase_marker = (
        "ok: chaos network-partition phase broker-link down=true "
        "healed=true expect=fail source=command"
    )

    missing_chaos_phase_marker = sample_manifest()
    missing_chaos_phase_marker["commands"][chaos_command_index]["output"] = (
        missing_chaos_phase_marker["commands"][chaos_command_index]["output"].replace(
            chaos_network_phase_marker,
            "",
        )
    )
    failures = validate_release_evidence(missing_chaos_phase_marker)
    if not any("ok: chaos network-partition phase broker-link" in failure for failure in failures):
        raise AssertionError("missing chaos required phase output marker was not reported")

    bare_chaos_network_phase_detail = sample_manifest()
    bare_chaos_network_phase_detail["commands"][chaos_command_index]["output"] = (
        bare_chaos_network_phase_detail["commands"][chaos_command_index]["output"].replace(
            chaos_network_phase_marker,
            "ok: chaos network-partition phase broker-link",
        )
    )
    failures = validate_release_evidence(bare_chaos_network_phase_detail)
    if not any(
        "down=true observed=failed" in failure and "recovered=true" in failure
        for failure in failures
    ):
        raise AssertionError("bare chaos network-partition phase detail marker was accepted")

    stale_chaos_network_phase_detail = sample_manifest()
    stale_chaos_network_phase_detail["commands"][chaos_command_index]["output"] = (
        stale_chaos_network_phase_detail["commands"][chaos_command_index][
            "output"
        ].replace(chaos_network_phase_marker, stale_chaos_network_phase_marker)
    )
    failures = validate_release_evidence(stale_chaos_network_phase_detail)
    if not any(
        "down=true observed=failed" in failure and "recovered=true" in failure
        for failure in failures
    ):
        raise AssertionError(
            "stale chaos network-partition phase detail marker was accepted"
        )

    mismatched_chaos_network_phase_source = sample_manifest()
    mismatched_chaos_network_phase_source["commands"][chaos_command_index][
        "output"
    ] = mismatched_chaos_network_phase_source["commands"][chaos_command_index][
        "output"
    ].replace(
        chaos_network_phase_marker,
        (
            "ok: chaos network-partition phase broker-link down=true "
            "observed=failed healed=true recovered=true expect=fail "
            "source=wrapper"
        ),
    )
    failures = validate_release_evidence(mismatched_chaos_network_phase_source)
    if not any(
        "source=command" in failure and "broker-link" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched chaos network phase source was accepted")

    mismatched_chaos_network_phase_expect = sample_manifest()
    mismatched_chaos_network_phase_expect["commands"][chaos_command_index]["output"] = (
        mismatched_chaos_network_phase_expect["commands"][chaos_command_index][
            "output"
        ].replace(
            chaos_network_phase_marker,
            (
                "ok: chaos network-partition phase broker-link down=true "
                "observed=failed healed=true recovered=true expect=survive "
                "source=command"
            ),
        )
    )
    failures = validate_release_evidence(mismatched_chaos_network_phase_expect)
    if not any("expect=fail" in failure for failure in failures):
        raise AssertionError("mismatched chaos network-partition expectation was accepted")

    mismatched_chaos_network_phase_observed = sample_manifest()
    mismatched_chaos_network_phase_observed["commands"][chaos_command_index][
        "output"
    ] = mismatched_chaos_network_phase_observed["commands"][chaos_command_index][
        "output"
    ].replace(
        chaos_network_phase_marker,
        (
            "ok: chaos network-partition phase broker-link down=true "
            "observed=survived healed=true recovered=true expect=fail "
            "source=command"
        ),
    )
    failures = validate_release_evidence(mismatched_chaos_network_phase_observed)
    if not any("observed=failed" in failure for failure in failures):
        raise AssertionError(
            "mismatched chaos network-partition observed result was accepted"
        )

    duplicate_chaos_network_phase_detail = sample_manifest()
    duplicate_chaos_network_phase_detail["commands"][chaos_command_index]["output"] = (
        duplicate_chaos_network_phase_detail["commands"][chaos_command_index][
            "output"
        ].replace(
            chaos_network_phase_marker,
            (
                "ok: chaos network-partition phase broker-link down=true "
                "observed=survived healed=true recovered=true expect=survive "
                "source=command\n"
                "ok: chaos network-partition phase broker-link down=true "
                "observed=failed healed=true recovered=true expect=fail "
                "source=command"
            ),
        )
    )
    failures = validate_release_evidence(duplicate_chaos_network_phase_detail)
    if not any("must not repeat phase broker-link" in failure for failure in failures):
        raise AssertionError(
            "duplicate chaos network-partition phase detail marker was accepted"
        )

    detached_chaos_network_phase_detail = sample_manifest()
    detached_chaos_output = detached_chaos_network_phase_detail["commands"][
        chaos_command_index
    ]["output"].replace(chaos_network_phase_marker + "\n", "")
    detached_chaos_output = detached_chaos_output.replace(
        chaos_network_summary_marker,
        chaos_network_summary_marker + "\n" + chaos_network_phase_marker,
    )
    detached_chaos_network_phase_detail["commands"][chaos_command_index][
        "output"
    ] = detached_chaos_output
    failures = validate_release_evidence(detached_chaos_network_phase_detail)
    if not any(
        "before the chaos network-partition scenario marker" in failure
        and "broker-link" in failure
        for failure in failures
    ):
        raise AssertionError(
            "detached chaos network-partition phase detail marker was accepted"
        )

    missing_network_summary_marker = sample_manifest()
    missing_network_summary_marker["commands"][chaos_command_index]["output"] = "\n".join(
        line
        for line in missing_network_summary_marker["commands"][chaos_command_index][
            "output"
        ].splitlines()
        if line != chaos_network_summary_marker
    )
    failures = validate_release_evidence(missing_network_summary_marker)
    if not any("ok: chaos network-partition" in failure for failure in failures):
        raise AssertionError("network phase marker satisfied chaos scenario summary")

    missing_source_network_summary_marker = sample_manifest()
    missing_source_network_summary_marker["commands"][chaos_command_index][
        "output"
    ] = missing_source_network_summary_marker["commands"][chaos_command_index][
        "output"
    ].replace(
        chaos_network_summary_marker,
        "ok: chaos network-partition",
    )
    failures = validate_release_evidence(missing_source_network_summary_marker)
    if not any(
        "ok: chaos network-partition source=command" in failure
        for failure in failures
    ):
        raise AssertionError("chaos network summary without source=command was accepted")

    mismatched_source_network_summary_marker = sample_manifest()
    mismatched_source_network_summary_marker["commands"][chaos_command_index][
        "output"
    ] = mismatched_source_network_summary_marker["commands"][chaos_command_index][
        "output"
    ].replace(
        chaos_network_summary_marker,
        "ok: chaos network-partition source=wrapper",
    )
    failures = validate_release_evidence(mismatched_source_network_summary_marker)
    if not any(
        "ok: chaos network-partition source=command" in failure
        for failure in failures
    ):
        raise AssertionError("chaos network summary with wrapper source was accepted")

    suffixed_network_summary_marker = sample_manifest()
    suffixed_network_summary_marker["commands"][chaos_command_index]["output"] = (
        suffixed_network_summary_marker["commands"][chaos_command_index][
            "output"
        ].replace(
            chaos_network_summary_marker,
            chaos_network_summary_marker + " wrapper=1",
        )
    )
    failures = validate_release_evidence(suffixed_network_summary_marker)
    if not any(
        "chaos network-partition scenario summary output marker must appear "
        "exactly once as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed chaos network summary marker was accepted")

    duplicate_network_summary_marker = sample_manifest()
    duplicate_network_summary_marker["commands"][chaos_command_index]["output"] = (
        duplicate_network_summary_marker["commands"][chaos_command_index][
            "output"
        ].replace(
            chaos_network_summary_marker,
            chaos_network_summary_marker + "\n" + chaos_network_summary_marker,
        )
    )
    failures = validate_release_evidence(duplicate_network_summary_marker)
    if not any(
        "chaos network-partition scenario summary output marker must appear exactly once"
        in failure
        for failure in failures
    ):
        raise AssertionError("duplicate chaos network summary marker was accepted")

    missing_kraft_network_matrix_output = sample_manifest()
    kraft_failover_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "KRaft failover gate"
    )

    for env_name, assertion_message in (
        (
            "ZMQ_KRAFT_REQUIRED_NETWORK_PHASES",
            "missing KRaft required-network-phases command assignment was accepted",
        ),
        (
            "ZMQ_KRAFT_NETWORK_MATRIX",
            "missing KRaft network-matrix command assignment was accepted",
        ),
    ):
        missing_kraft_network_command = sample_manifest()
        kraft_network_value = missing_kraft_network_command["environment"][env_name]
        missing_kraft_network_command["commands"][kraft_failover_command_index][
            "command"
        ] = missing_kraft_network_command["commands"][kraft_failover_command_index][
            "command"
        ].replace(
            f"{env_name}={kraft_network_value} ",
            "",
        )
        failures = validate_release_evidence(missing_kraft_network_command)
        if not any(
            "KRaft failover gate" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    mismatched_kraft_network_command = sample_manifest()
    mismatched_kraft_network_command["commands"][kraft_failover_command_index][
        "command"
    ] = mismatched_kraft_network_command["commands"][kraft_failover_command_index][
        "command"
    ].replace(
        "ZMQ_KRAFT_NETWORK_MATRIX=leader-isolation,broker-link",
        "ZMQ_KRAFT_NETWORK_MATRIX=leader-isolation",
    )
    failures = validate_release_evidence(mismatched_kraft_network_command)
    if not any(
        "KRaft failover gate" in failure
        and "ZMQ_KRAFT_NETWORK_MATRIX" in failure
        and "broker-link" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched KRaft network command assignment was accepted"
        )

    missing_kraft_network_matrix_output["commands"][kraft_failover_command_index]["output"] = (
        "Build Summary: 3/3 steps succeeded\n"
        "test-kraft-failover success\n"
        "ok: KRaft controller failover harness passed\n"
        "network_partition=None\n"
        "automq_stream_set_object_id=42"
    )
    failures = validate_release_evidence(missing_kraft_network_matrix_output)
    if not any("network_partition=[" in failure for failure in failures):
        raise AssertionError("missing KRaft network partition matrix output was not reported")

    embedded_kraft_network_matrix_output = sample_manifest()
    embedded_kraft_network_matrix_output["commands"][kraft_failover_command_index]["output"] = (
        embedded_kraft_network_matrix_output["commands"][kraft_failover_command_index]["output"].replace(
            "network_partition=[leader-isolation,broker-link], ",
            "",
            1,
        )
        + "\nnetwork_partition=[leader-isolation,broker-link]"
    )
    failures = validate_release_evidence(embedded_kraft_network_matrix_output)
    if not any("network_partition=[" in failure for failure in failures):
        raise AssertionError("detached KRaft network partition marker was accepted")

    mismatched_kraft_network_matrix_output = sample_manifest()
    mismatched_kraft_network_matrix_output["commands"][kraft_failover_command_index][
        "output"
    ] = mismatched_kraft_network_matrix_output["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "network_partition=[leader-isolation,broker-link], ",
        "network_partition=[leader-isolation], ",
    )
    failures = validate_release_evidence(mismatched_kraft_network_matrix_output)
    if not any(
        "ZMQ_KRAFT_NETWORK_MATRIX" in failure and "broker-link" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched KRaft network partition summary phases were accepted"
        )

    empty_kraft_network_matrix_output = sample_manifest()
    empty_kraft_network_matrix_output["commands"][kraft_failover_command_index][
        "output"
    ] = empty_kraft_network_matrix_output["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "network_partition=[leader-isolation,broker-link], ",
        "network_partition=[], ",
    )
    failures = validate_release_evidence(empty_kraft_network_matrix_output)
    if not any(
        "KRaft network partition summary phases" in failure
        and "at least one" in failure
        for failure in failures
    ):
        raise AssertionError(
            "empty KRaft network partition summary phases were accepted"
        )

    blank_kraft_network_matrix_output = sample_manifest()
    blank_kraft_network_matrix_output["commands"][kraft_failover_command_index][
        "output"
    ] = blank_kraft_network_matrix_output["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "network_partition=[leader-isolation,broker-link], ",
        "network_partition=[leader-isolation,,broker-link], ",
    )
    failures = validate_release_evidence(blank_kraft_network_matrix_output)
    if not any("KRaft network partition summary phases" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank KRaft network partition summary phase was accepted")

    kraft_leader_phase_marker = (
        "ok: KRaft network partition phase leader-isolation down=true "
        "observed=failed healed=true healed_leader=1 healed_fetch=true "
        "expect=fail source=command"
    )
    kraft_broker_phase_marker = (
        "ok: KRaft network partition phase broker-link down=true "
        "observed=survived healed=true healed_leader=2 healed_fetch=true "
        "expect=survive source=command"
    )

    bare_kraft_network_phase_detail = sample_manifest()
    bare_kraft_network_phase_detail["commands"][kraft_failover_command_index][
        "output"
    ] = bare_kraft_network_phase_detail["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_leader_phase_marker,
        "ok: KRaft network partition phase leader-isolation",
    )
    failures = validate_release_evidence(bare_kraft_network_phase_detail)
    if not any("observed=failed" in failure for failure in failures):
        raise AssertionError("bare KRaft network partition phase detail marker was accepted")

    stale_kraft_network_phase_detail = sample_manifest()
    stale_kraft_network_phase_detail["commands"][kraft_failover_command_index][
        "output"
    ] = stale_kraft_network_phase_detail["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_leader_phase_marker,
        (
            "ok: KRaft network partition phase leader-isolation down=true "
            "healed=true expect=fail source=command"
        ),
    )
    failures = validate_release_evidence(stale_kraft_network_phase_detail)
    if not any("healed_leader=<id>" in failure for failure in failures):
        raise AssertionError("stale KRaft network partition phase marker was accepted")

    mismatched_kraft_network_phase_source = sample_manifest()
    mismatched_kraft_network_phase_source["commands"][kraft_failover_command_index][
        "output"
    ] = mismatched_kraft_network_phase_source["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_leader_phase_marker,
        kraft_leader_phase_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_kraft_network_phase_source)
    if not any(
        "source=command" in failure and "leader-isolation" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched KRaft network phase source was accepted")

    mismatched_kraft_network_phase_expect = sample_manifest()
    mismatched_kraft_network_phase_expect["commands"][kraft_failover_command_index][
        "output"
    ] = mismatched_kraft_network_phase_expect["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_broker_phase_marker,
        kraft_broker_phase_marker.replace("expect=survive", "expect=fail"),
    )
    failures = validate_release_evidence(mismatched_kraft_network_phase_expect)
    if not any("expect=survive" in failure for failure in failures):
        raise AssertionError("mismatched KRaft network partition expectation was accepted")

    mismatched_kraft_network_phase_observed = sample_manifest()
    mismatched_kraft_network_phase_observed["commands"][kraft_failover_command_index][
        "output"
    ] = mismatched_kraft_network_phase_observed["commands"][
        kraft_failover_command_index
    ]["output"].replace(
        kraft_broker_phase_marker,
        kraft_broker_phase_marker.replace("observed=survived", "observed=failed"),
    )
    failures = validate_release_evidence(mismatched_kraft_network_phase_observed)
    if not any("observed=survived" in failure for failure in failures):
        raise AssertionError("mismatched KRaft network partition observed result was accepted")

    duplicate_kraft_network_phase_detail = sample_manifest()
    duplicate_kraft_network_phase_detail["commands"][kraft_failover_command_index][
        "output"
    ] = duplicate_kraft_network_phase_detail["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_broker_phase_marker,
        (
            f"{kraft_broker_phase_marker.replace('expect=survive', 'expect=fail')}\n"
            f"{kraft_broker_phase_marker}"
        ),
    )
    failures = validate_release_evidence(duplicate_kraft_network_phase_detail)
    if not any("must not repeat phase broker-link" in failure for failure in failures):
        raise AssertionError(
            "duplicate KRaft network partition phase detail marker was accepted"
        )

    detached_kraft_network_phase_detail = sample_manifest()
    detached_output = detached_kraft_network_phase_detail["commands"][
        kraft_failover_command_index
    ]["output"]
    kraft_phase_markers = [
        kraft_leader_phase_marker,
        kraft_broker_phase_marker,
    ]
    for marker in kraft_phase_markers:
        detached_output = detached_output.replace(marker + "\n", "")
    kraft_summary = next(
        line
        for line in detached_output.splitlines()
        if line.startswith("ok: KRaft controller failover harness passed ")
    )
    detached_output = detached_output.replace(
        kraft_summary,
        kraft_summary + "\n" + "\n".join(kraft_phase_markers),
    )
    detached_kraft_network_phase_detail["commands"][
        kraft_failover_command_index
    ]["output"] = detached_output
    failures = validate_release_evidence(detached_kraft_network_phase_detail)
    if not any(
        "before the KRaft failover summary line" in failure
        and "leader-isolation" in failure
        for failure in failures
    ):
        raise AssertionError(
            "detached KRaft network partition phase detail marker was accepted"
        )

    missing_source_kraft_failover_summary = sample_manifest()
    missing_source_kraft_failover_summary["commands"][kraft_failover_command_index][
        "output"
    ] = missing_source_kraft_failover_summary["commands"][
        kraft_failover_command_index
    ][
        "output"
    ].replace(
        kraft_summary,
        kraft_summary.replace(" source=command", ""),
    )
    failures = validate_release_evidence(missing_source_kraft_failover_summary)
    if not any(
        "KRaft failover summary output marker" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("KRaft failover summary without source=command was accepted")

    mismatched_source_kraft_failover_summary = sample_manifest()
    mismatched_source_kraft_failover_summary["commands"][kraft_failover_command_index][
        "output"
    ] = mismatched_source_kraft_failover_summary["commands"][
        kraft_failover_command_index
    ][
        "output"
    ].replace(
        kraft_summary,
        kraft_summary.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_source_kraft_failover_summary)
    if not any(
        "KRaft failover summary output marker" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("KRaft failover summary with wrapper source was accepted")

    suffixed_kraft_failover_summary = sample_manifest()
    suffixed_kraft_failover_summary["commands"][kraft_failover_command_index][
        "output"
    ] = suffixed_kraft_failover_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_summary,
        kraft_summary + " wrapper=1",
    )
    failures = validate_release_evidence(suffixed_kraft_failover_summary)
    if not any(
        "KRaft failover summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed KRaft failover summary marker was accepted")

    duplicate_kraft_failover_summary = sample_manifest()
    duplicate_kraft_failover_summary["commands"][kraft_failover_command_index][
        "output"
    ] = duplicate_kraft_failover_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        kraft_summary,
        kraft_summary + "\n" + kraft_summary,
    )
    failures = validate_release_evidence(duplicate_kraft_failover_summary)
    if not any(
        "KRaft failover summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate KRaft failover summary marker was accepted")

    missing_kraft_reassignment_summary = sample_manifest()
    missing_kraft_reassignment_summary["commands"][kraft_failover_command_index][
        "output"
    ] = missing_kraft_reassignment_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "reassignment_topic=kraft-reassign-release, ",
        "",
    )
    failures = validate_release_evidence(missing_kraft_reassignment_summary)
    if not any("reassignment_topic" in failure for failure in failures):
        raise AssertionError("missing KRaft reassignment summary was accepted")

    detached_kraft_reassignment_summary = sample_manifest()
    detached_kraft_reassignment_summary["commands"][kraft_failover_command_index][
        "output"
    ] = (
        detached_kraft_reassignment_summary["commands"][kraft_failover_command_index][
            "output"
        ].replace(
            "reassignment_target=1, ",
            "",
        )
        + "\nreassignment_target=1"
    )
    failures = validate_release_evidence(detached_kraft_reassignment_summary)
    if not any("reassignment_target" in failure for failure in failures):
        raise AssertionError("detached KRaft reassignment summary was accepted")

    unverified_kraft_reassignment_summary = sample_manifest()
    unverified_kraft_reassignment_summary["commands"][kraft_failover_command_index][
        "output"
    ] = unverified_kraft_reassignment_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "reassignment_old_owner_rejected=true",
        "reassignment_old_owner_rejected=false",
    )
    failures = validate_release_evidence(unverified_kraft_reassignment_summary)
    if not any("reassignment_old_owner_rejected=true" in failure for failure in failures):
        raise AssertionError("unverified KRaft reassignment summary was accepted")

    missing_kraft_coordinator_summary = sample_manifest()
    missing_kraft_coordinator_summary["commands"][kraft_failover_command_index][
        "output"
    ] = missing_kraft_coordinator_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "transactions_checked=5, ",
        "",
    )
    failures = validate_release_evidence(missing_kraft_coordinator_summary)
    if not any("transactions_checked=5" in failure for failure in failures):
        raise AssertionError("missing KRaft coordinator summary was accepted")

    unverified_kraft_coordinator_summary = sample_manifest()
    unverified_kraft_coordinator_summary["commands"][kraft_failover_command_index][
        "output"
    ] = unverified_kraft_coordinator_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "idempotent_producer_fencing=true",
        "idempotent_producer_fencing=false",
    )
    failures = validate_release_evidence(unverified_kraft_coordinator_summary)
    if not any("idempotent_producer_fencing=true" in failure for failure in failures):
        raise AssertionError("unverified KRaft coordinator summary was accepted")

    unverified_kraft_follower_rejection_summary = sample_manifest()
    unverified_kraft_follower_rejection_summary["commands"][kraft_failover_command_index][
        "output"
    ] = unverified_kraft_follower_rejection_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "broker_registration_follower_rejection_checked=true",
        "broker_registration_follower_rejection_checked=false",
    )
    failures = validate_release_evidence(unverified_kraft_follower_rejection_summary)
    if not any("broker_registration_follower_rejection_checked=true" in failure for failure in failures):
        raise AssertionError("unverified KRaft follower rejection summary was accepted")

    unverified_kraft_admin_summary = sample_manifest()
    unverified_kraft_admin_summary["commands"][kraft_failover_command_index][
        "output"
    ] = unverified_kraft_admin_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "create_topics_checked=true",
        "create_topics_checked=false",
    )
    failures = validate_release_evidence(unverified_kraft_admin_summary)
    if not any("create_topics_checked=true" in failure for failure in failures):
        raise AssertionError("unverified KRaft admin summary was accepted")

    unverified_kraft_group_summary = sample_manifest()
    unverified_kraft_group_summary["commands"][kraft_failover_command_index][
        "output"
    ] = unverified_kraft_group_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "kip848_rejoin_checked=true",
        "kip848_rejoin_checked=false",
    )
    failures = validate_release_evidence(unverified_kraft_group_summary)
    if not any("kip848_rejoin_checked=true" in failure for failure in failures):
        raise AssertionError("unverified KRaft group summary was accepted")

    missing_kraft_controller_unsupported_cases = sample_manifest()
    missing_kraft_controller_unsupported_cases["commands"][kraft_failover_command_index][
        "output"
    ] = missing_kraft_controller_unsupported_cases["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        (
            "controller_unsupported_cases=[52:2,53:2,54:2,55:3,"
            "59:2,62:3,63:2,64:1,67:1,70:1,80:1,81:1,82:1,"
            "4:0,4:7,5:0,5:4,6:0,6:8,7:0,7:3,71:0,72:0], "
        ),
        "",
    )
    failures = validate_release_evidence(missing_kraft_controller_unsupported_cases)
    if not any("controller_unsupported_cases" in failure for failure in failures):
        raise AssertionError("missing KRaft controller unsupported cases summary was accepted")

    incomplete_kraft_controller_unsupported_cases = sample_manifest()
    incomplete_kraft_controller_unsupported_cases["commands"][kraft_failover_command_index][
        "output"
    ] = incomplete_kraft_controller_unsupported_cases["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "4:7,",
        "",
    )
    failures = validate_release_evidence(incomplete_kraft_controller_unsupported_cases)
    if not any("KRaft controller unsupported cases" in failure and "4:7" in failure for failure in failures):
        raise AssertionError("incomplete KRaft controller unsupported cases summary was accepted")

    missing_kraft_broker_non_broker_cases = sample_manifest()
    missing_kraft_broker_non_broker_cases["commands"][kraft_failover_command_index][
        "output"
    ] = missing_kraft_broker_non_broker_cases["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        (
            "broker_non_broker_api_rejection_cases=[56:3,58:0,59:1,"
            "62:4,63:1,64:0,67:0,70:0,80:0,81:0,82:0], "
        ),
        "",
    )
    failures = validate_release_evidence(missing_kraft_broker_non_broker_cases)
    if not any("broker_non_broker_api_rejection_cases" in failure for failure in failures):
        raise AssertionError("missing KRaft broker non-broker cases summary was accepted")

    incomplete_kraft_broker_non_broker_cases = sample_manifest()
    incomplete_kraft_broker_non_broker_cases["commands"][kraft_failover_command_index][
        "output"
    ] = incomplete_kraft_broker_non_broker_cases["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "82:0",
        "",
    )
    failures = validate_release_evidence(incomplete_kraft_broker_non_broker_cases)
    if not any("KRaft broker non-broker API rejection cases" in failure and "82:0" in failure for failure in failures):
        raise AssertionError("incomplete KRaft broker non-broker cases summary was accepted")

    placeholder_kraft_automq_summary = sample_manifest()
    placeholder_kraft_automq_summary["commands"][kraft_failover_command_index][
        "output"
    ] = placeholder_kraft_automq_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "automq_stream_id=21",
        "automq_stream_id=/path/to/stream",
    )
    failures = validate_release_evidence(placeholder_kraft_automq_summary)
    if not any("non-placeholder integer automq_stream_id" in failure for failure in failures):
        raise AssertionError("placeholder KRaft AutoMQ summary was accepted")

    negative_kraft_automq_summary = sample_manifest()
    negative_kraft_automq_summary["commands"][kraft_failover_command_index][
        "output"
    ] = negative_kraft_automq_summary["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "automq_node_id=1",
        "automq_node_id=-1",
    )
    failures = validate_release_evidence(negative_kraft_automq_summary)
    if not any("automq_node_id" in failure and "non-negative" in failure for failure in failures):
        raise AssertionError("negative KRaft AutoMQ summary id was accepted")

    mismatched_kraft_transaction_count = sample_manifest()
    mismatched_kraft_transaction_count["commands"][kraft_failover_command_index][
        "output"
    ] = mismatched_kraft_transaction_count["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "transactions_checked=5",
        "transactions_checked=4",
    )
    failures = validate_release_evidence(mismatched_kraft_transaction_count)
    if not any("transactions_checked=5" in failure for failure in failures):
        raise AssertionError("mismatched KRaft transaction count was accepted")

    duplicate_kraft_summary_field = sample_manifest()
    duplicate_kraft_summary_field["commands"][kraft_failover_command_index][
        "output"
    ] = duplicate_kraft_summary_field["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "transactions_checked=5",
        "transactions_checked=5, transactions_checked=4",
    )
    failures = validate_release_evidence(duplicate_kraft_summary_field)
    if not any("KRaft failover summary must not repeat fields" in failure for failure in failures):
        raise AssertionError("duplicate KRaft summary field was accepted")

    unknown_kraft_summary_field = sample_manifest()
    unknown_kraft_summary_field["commands"][kraft_failover_command_index][
        "output"
    ] = unknown_kraft_summary_field["commands"][kraft_failover_command_index][
        "output"
    ].replace(
        "automq_old_leader_fresh_rejoin=true) source=command",
        "automq_old_leader_fresh_rejoin=true, unchecked=true) source=command",
    )
    failures = validate_release_evidence(unknown_kraft_summary_field)
    if not any("KRaft failover summary must not include unknown fields" in failure for failure in failures):
        raise AssertionError("unknown KRaft summary field was accepted")

    provider_matrix_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "S3 provider matrix"
    )
    live_s3_benchmark_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "live-S3 benchmark gate"
    )
    minio_s3_provider_marker = (
        "ok: S3 provider profile minio endpoint=127.0.0.1:9000 "
        "bucket=zmq-minio-it scheme=http region=us-east-1 "
        "path_style=true source=command"
    )
    aws_s3_provider_marker = (
        "ok: S3 provider profile aws_us_east_1 "
        "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
        "scheme=https region=us-east-1 path_style=false source=command"
    )
    s3_provider_matrix_summary = "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command"
    aws_s3_multipart_fault_marker = (
        "ok: S3 provider multipart-fault profile aws_us_east_1 "
        "command_started=true completed=true injected=true recovered=true "
        "source=command"
    )
    aws_s3_outage_detail_marker = (
        "ok: S3 provider outage detail profile aws_us_east_1 "
        "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
        "scheme=https region=us-east-1 path_style=false "
        "down=true healed=true fail_closed=true recovered=true "
        "source=command"
    )
    aws_s3_process_crash_detail_marker = (
        "ok: S3 provider process-crash detail profile aws_us_east_1 "
        "bucket=zmq-aws-it topic=zmq-process-crash "
        "group=zmq-process-crash-group killed_broker=true "
        "fresh_data_dir=true first_offset=0 committed_offset=1 "
        "replacement_offset=2 recovered_payloads=2 source=command"
    )
    aws_s3_multipart_fault_detail_marker = (
        "ok: S3 multipart fault profile aws_us_east_1 "
        "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
        "scheme=https region=us-east-1 path_style=false "
        "injected=true recovered=true source=command"
    )

    missing_s3_provider_required_profiles_command = sample_manifest()
    missing_s3_provider_required_profiles_command["commands"][
        provider_matrix_command_index
    ]["command"] = missing_s3_provider_required_profiles_command["commands"][
        provider_matrix_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_PROVIDER_REQUIRED_PROFILES=minio,aws_us_east_1 ",
        "",
    )
    failures = validate_release_evidence(missing_s3_provider_required_profiles_command)
    if not any(
        "S3 provider matrix" in failure
        and "ZMQ_S3_PROVIDER_REQUIRED_PROFILES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing S3 provider matrix required-profile command assignment was accepted"
        )

    mismatched_s3_provider_profiles_command = sample_manifest()
    mismatched_s3_provider_profiles_command["commands"][
        provider_matrix_command_index
    ]["command"] = mismatched_s3_provider_profiles_command["commands"][
        provider_matrix_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_PROVIDER_PROFILES=minio,aws_us_east_1",
        "ZMQ_S3_PROVIDER_PROFILES=minio",
    )
    failures = validate_release_evidence(mismatched_s3_provider_profiles_command)
    if not any(
        "S3 provider matrix" in failure
        and "ZMQ_S3_PROVIDER_PROFILES" in failure
        and "minio" in failure
        and "aws_us_east_1" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider matrix selected-profile command assignment was accepted"
        )

    missing_s3_provider_outage_profiles_command = sample_manifest()
    missing_s3_provider_outage_profiles_command["commands"][
        provider_matrix_command_index
    ]["command"] = missing_s3_provider_outage_profiles_command["commands"][
        provider_matrix_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES=aws_us_east_1 ",
        "",
    )
    failures = validate_release_evidence(missing_s3_provider_outage_profiles_command)
    if not any(
        "S3 provider matrix" in failure
        and "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing S3 provider outage-profile command assignment was accepted"
        )

    mismatched_s3_provider_multipart_fault_profiles_command = sample_manifest()
    mismatched_s3_provider_multipart_fault_profiles_command["commands"][
        provider_matrix_command_index
    ]["command"] = mismatched_s3_provider_multipart_fault_profiles_command["commands"][
        provider_matrix_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES=aws_us_east_1",
        "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES=minio",
    )
    failures = validate_release_evidence(
        mismatched_s3_provider_multipart_fault_profiles_command
    )
    if not any(
        "S3 provider matrix" in failure
        and "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES" in failure
        and "aws_us_east_1" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider multipart-fault command assignment was accepted"
        )

    missing_s3_provider_process_crash_enable_command = sample_manifest()
    missing_s3_provider_process_crash_enable_command["commands"][
        provider_matrix_command_index
    ]["command"] = missing_s3_provider_process_crash_enable_command["commands"][
        provider_matrix_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH=1 ",
        "",
    )
    failures = validate_release_evidence(missing_s3_provider_process_crash_enable_command)
    if not any(
        "S3 provider matrix" in failure
        and "ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing S3 provider process-crash enable command assignment was accepted"
        )

    for env_name, assertion_message in (
        (
            "ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE",
            "missing S3 provider outage enable command assignment was accepted",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_REQUIRE_LIST_PAGINATION",
            "missing S3 provider list-pagination enable command assignment was accepted",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE",
            "missing S3 provider multipart-edge enable command assignment was accepted",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT",
            "missing S3 provider multipart-fault enable command assignment was accepted",
        ),
    ):
        missing_s3_provider_enable_command = sample_manifest()
        missing_s3_provider_enable_command["commands"][provider_matrix_command_index][
            "command"
        ] = missing_s3_provider_enable_command["commands"][
            provider_matrix_command_index
        ][
            "command"
        ].replace(
            f"{env_name}=1 ",
            "",
        )
        failures = validate_release_evidence(missing_s3_provider_enable_command)
        if not any(
            "S3 provider matrix" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    mismatched_s3_provider_multipart_edge_enable_command = sample_manifest()
    mismatched_s3_provider_multipart_edge_enable_command["commands"][
        provider_matrix_command_index
    ]["command"] = mismatched_s3_provider_multipart_edge_enable_command["commands"][
        provider_matrix_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE=1",
        "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE=0",
    )
    failures = validate_release_evidence(
        mismatched_s3_provider_multipart_edge_enable_command
    )
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE" in failure
        and "manifest environment records" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider multipart-edge enable command assignment was accepted"
        )

    global_s3_provider_enable_command = sample_manifest()
    global_s3_provider_enable_command["environment"].pop(
        "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE"
    )
    global_s3_provider_enable_command["environment"][
        "ZMQ_S3_REQUIRE_MULTIPART_EDGE"
    ] = "1"
    global_s3_provider_enable_command["commands"][provider_matrix_command_index][
        "command"
    ] = sample_requirement_command(
        REQUIRED_COMMANDS[provider_matrix_command_index],
        global_s3_provider_enable_command["environment"],
    )
    failures = validate_release_evidence(global_s3_provider_enable_command)
    if failures:
        raise AssertionError(
            "global S3 provider enable command provenance was rejected: "
            + "; ".join(failures)
        )

    bare_s3_provider_profile_marker = sample_manifest()
    bare_s3_provider_profile_marker["commands"][provider_matrix_command_index]["output"] = (
        bare_s3_provider_profile_marker["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            minio_s3_provider_marker,
            "ok: S3 provider profile minio",
        )
    )
    failures = validate_release_evidence(bare_s3_provider_profile_marker)
    if not any("scheme=<scheme>" in failure for failure in failures):
        raise AssertionError("bare S3 provider profile output marker was accepted")

    legacy_s3_provider_profile_marker = sample_manifest()
    legacy_s3_provider_profile_marker["commands"][provider_matrix_command_index]["output"] = (
        legacy_s3_provider_profile_marker["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            minio_s3_provider_marker,
            "ok: S3 provider profile minio endpoint=127.0.0.1:9000 bucket=zmq-minio-it",
        )
    )
    failures = validate_release_evidence(legacy_s3_provider_profile_marker)
    if not any("path_style" in failure for failure in failures):
        raise AssertionError("legacy S3 provider profile output marker was accepted")

    missing_source_s3_provider_profile_marker = sample_manifest()
    missing_source_s3_provider_profile_marker["commands"][
        provider_matrix_command_index
    ]["output"] = missing_source_s3_provider_profile_marker["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        minio_s3_provider_marker,
        minio_s3_provider_marker.replace(" source=command", ""),
    )
    failures = validate_release_evidence(missing_source_s3_provider_profile_marker)
    if not any(
        "S3 provider profile output marker" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing S3 provider profile output marker command source was accepted"
        )

    mismatched_source_s3_provider_profile_marker = sample_manifest()
    mismatched_source_s3_provider_profile_marker["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_source_s3_provider_profile_marker["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        minio_s3_provider_marker,
        minio_s3_provider_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_source_s3_provider_profile_marker)
    if not any(
        "S3 provider profile output marker" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider profile output marker command source was accepted"
        )

    placeholder_s3_provider_profile_marker = sample_manifest()
    placeholder_s3_provider_profile_marker["commands"][provider_matrix_command_index]["output"] = (
        placeholder_s3_provider_profile_marker["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            minio_s3_provider_marker,
            (
                "ok: S3 provider profile minio endpoint=placeholder "
                "bucket=placeholder scheme=placeholder region=placeholder "
                "path_style=true source=command"
            ),
        )
    )
    failures = validate_release_evidence(placeholder_s3_provider_profile_marker)
    if not any("non-placeholder endpoint" in failure for failure in failures):
        raise AssertionError("placeholder S3 provider profile output marker was accepted")

    missing_s3_provider_endpoint_provenance = sample_manifest()
    missing_s3_provider_endpoint_provenance["environment"].pop(
        "ZMQ_S3_AWS_US_EAST_1_ENDPOINT"
    )
    missing_s3_provider_endpoint_provenance["environment"].pop("ZMQ_S3_ENDPOINT")
    failures = validate_release_evidence(missing_s3_provider_endpoint_provenance)
    if not any("profile setting ENDPOINT" in failure for failure in failures):
        raise AssertionError("missing S3 provider endpoint provenance was accepted")

    global_s3_provider_endpoint_provenance = sample_manifest()
    for suffix, value in (
        ("ENDPOINT", "127.0.0.1"),
        ("PORT", "9000"),
        ("BUCKET", "zmq-minio-it"),
    ):
        global_s3_provider_endpoint_provenance["environment"].pop(
            f"ZMQ_S3_MINIO_{suffix}"
        )
        global_s3_provider_endpoint_provenance["environment"][
            f"ZMQ_S3_{suffix}"
        ] = value
    global_s3_provider_endpoint_provenance["commands"][
        live_s3_benchmark_command_index
    ]["command"] = global_s3_provider_endpoint_provenance["commands"][
        live_s3_benchmark_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_ENDPOINT=s3-bench.example.test",
        "ZMQ_S3_ENDPOINT=127.0.0.1",
    ).replace(
        "ZMQ_S3_PORT=9443",
        "ZMQ_S3_PORT=9000",
    ).replace(
        "ZMQ_S3_BUCKET=zmq-live-bench",
        "ZMQ_S3_BUCKET=zmq-minio-it",
    )
    global_s3_provider_endpoint_provenance["commands"][
        live_s3_benchmark_command_index
    ]["output"] = global_s3_provider_endpoint_provenance["commands"][
        live_s3_benchmark_command_index
    ][
        "output"
    ].replace(
        live_s3_benchmark_provider_marker,
        (
            "Live S3 provider endpoint=127.0.0.1:9000 "
            "bucket=zmq-minio-it scheme=http region=us-east-1 "
            "path_style=true"
        ),
    )
    failures = validate_release_evidence(global_s3_provider_endpoint_provenance)
    if failures:
        raise AssertionError(
            "global S3 provider endpoint provenance was rejected: "
            + "; ".join(failures)
        )

    global_s3_provider_settings_provenance = sample_manifest()
    for suffix, value in (
        ("SCHEME", "https"),
        ("REGION", "us-west-2"),
        ("PATH_STYLE", "false"),
    ):
        global_s3_provider_settings_provenance["environment"][
            f"ZMQ_S3_{suffix}"
        ] = value
    global_s3_provider_settings_provenance["commands"][
        provider_matrix_command_index
    ]["output"] = global_s3_provider_settings_provenance["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        minio_s3_provider_marker,
        (
            "ok: S3 provider profile minio endpoint=127.0.0.1:9000 "
            "bucket=zmq-minio-it scheme=https region=us-west-2 "
            "path_style=false source=command"
        ),
    )
    global_s3_provider_settings_provenance["commands"][
        live_s3_benchmark_command_index
    ]["command"] = global_s3_provider_settings_provenance["commands"][
        live_s3_benchmark_command_index
    ][
        "command"
    ].replace(
        "ZMQ_S3_SCHEME=http ZMQ_S3_REGION=us-east-1 ZMQ_S3_PATH_STYLE=true",
        "ZMQ_S3_SCHEME=https ZMQ_S3_REGION=us-west-2 ZMQ_S3_PATH_STYLE=false",
    )
    global_s3_provider_settings_provenance["commands"][
        live_s3_benchmark_command_index
    ]["output"] = global_s3_provider_settings_provenance["commands"][
        live_s3_benchmark_command_index
    ][
        "output"
    ].replace(
        live_s3_benchmark_provider_marker,
        (
            "Live S3 provider endpoint=s3-bench.example.test:9443 "
            "bucket=zmq-live-bench scheme=https region=us-west-2 "
            "path_style=false"
        ),
    )
    failures = validate_release_evidence(global_s3_provider_settings_provenance)
    if failures:
        raise AssertionError(
            "global S3 provider settings provenance was rejected: "
            + "; ".join(failures)
        )

    invalid_s3_provider_scheme_provenance = sample_manifest()
    invalid_s3_provider_scheme_provenance["environment"][
        "ZMQ_S3_AWS_US_EAST_1_SCHEME"
    ] = "ftp"
    failures = validate_release_evidence(invalid_s3_provider_scheme_provenance)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_SCHEME" in failure and "http or https" in failure
        for failure in failures
    ):
        raise AssertionError("invalid S3 provider scheme provenance was accepted")

    invalid_s3_provider_path_style_provenance = sample_manifest()
    invalid_s3_provider_path_style_provenance["environment"][
        "ZMQ_S3_AWS_US_EAST_1_PATH_STYLE"
    ] = "sometimes"
    failures = validate_release_evidence(invalid_s3_provider_path_style_provenance)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_PATH_STYLE" in failure and "true or false" in failure
        for failure in failures
    ):
        raise AssertionError("invalid S3 provider path-style provenance was accepted")

    missing_non_minio_s3_provider_setting = sample_manifest()
    missing_non_minio_s3_provider_setting["environment"].pop(
        "ZMQ_S3_AWS_US_EAST_1_SCHEME"
    )
    missing_non_minio_s3_provider_setting["environment"].pop("ZMQ_S3_SCHEME")
    failures = validate_release_evidence(missing_non_minio_s3_provider_setting)
    if not any(
        "non-minio S3 provider matrix aws_us_east_1" in failure
        and "SCHEME" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing explicit non-minio S3 provider setting was accepted"
        )

    blank_s3_provider_endpoint_with_global = sample_manifest()
    blank_s3_provider_endpoint_with_global["environment"]["ZMQ_S3_ENDPOINT"] = (
        "global-s3.example.test"
    )
    blank_s3_provider_endpoint_with_global["environment"][
        "ZMQ_S3_AWS_US_EAST_1_ENDPOINT"
    ] = ""
    failures = validate_release_evidence(blank_s3_provider_endpoint_with_global)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_ENDPOINT" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank S3 provider endpoint used global release-evidence fallback"
        )

    blank_s3_provider_scheme_with_global = sample_manifest()
    blank_s3_provider_scheme_with_global["environment"]["ZMQ_S3_SCHEME"] = "https"
    blank_s3_provider_scheme_with_global["environment"][
        "ZMQ_S3_AWS_US_EAST_1_SCHEME"
    ] = ""
    failures = validate_release_evidence(blank_s3_provider_scheme_with_global)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_SCHEME" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank S3 provider scheme used global release-evidence fallback"
        )

    blank_s3_provider_enable_with_global = sample_manifest()
    blank_s3_provider_enable_with_global["environment"][
        "ZMQ_S3_RUN_LIVE_OUTAGE"
    ] = "1"
    blank_s3_provider_enable_with_global["environment"][
        "ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE"
    ] = ""
    failures = validate_release_evidence(blank_s3_provider_enable_with_global)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank S3 provider enable used global release-evidence fallback"
        )

    mismatched_s3_provider_global_fallback_output = sample_manifest()
    for suffix, value in (
        ("ENDPOINT", "127.0.0.1"),
        ("PORT", "9443"),
        ("BUCKET", "zmq-minio-it"),
    ):
        mismatched_s3_provider_global_fallback_output["environment"].pop(
            f"ZMQ_S3_MINIO_{suffix}"
        )
        mismatched_s3_provider_global_fallback_output["environment"][
            f"ZMQ_S3_{suffix}"
        ] = value
    failures = validate_release_evidence(mismatched_s3_provider_global_fallback_output)
    if not any("must match selected endpoint" in failure for failure in failures):
        raise AssertionError(
            "mismatched S3 provider global fallback output was accepted"
        )

    mismatched_s3_provider_settings_output = sample_manifest()
    mismatched_s3_provider_settings_output["commands"][provider_matrix_command_index][
        "output"
    ] = mismatched_s3_provider_settings_output["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        aws_s3_provider_marker,
        (
            "ok: S3 provider profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=http region=us-east-1 path_style=false source=command"
        ),
        1,
    )
    failures = validate_release_evidence(mismatched_s3_provider_settings_output)
    if not any("scheme=https" in failure for failure in failures):
        raise AssertionError("mismatched S3 provider settings output was accepted")

    mismatched_s3_provider_endpoint_output = sample_manifest()
    mismatched_s3_provider_endpoint_output["commands"][provider_matrix_command_index][
        "output"
    ] = mismatched_s3_provider_endpoint_output["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        aws_s3_provider_marker,
        (
            "ok: S3 provider profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:9000 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false source=command"
        ),
        1,
    )
    failures = validate_release_evidence(mismatched_s3_provider_endpoint_output)
    if not any("must match selected endpoint" in failure for failure in failures):
        raise AssertionError("mismatched S3 provider endpoint output was accepted")

    duplicate_s3_provider_profile_output = sample_manifest()
    duplicate_s3_provider_profile_output["commands"][provider_matrix_command_index][
        "output"
    ] = duplicate_s3_provider_profile_output["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        aws_s3_provider_marker,
        (
            aws_s3_provider_marker
            + "\n"
            + aws_s3_provider_marker.replace(
                "endpoint=s3.amazonaws.com:443",
                "endpoint=s3.amazonaws.com:9000",
            )
        ),
        1,
    )
    failures = validate_release_evidence(duplicate_s3_provider_profile_output)
    if not any("S3 provider matrix aws_us_east_1 must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate S3 provider profile output marker was accepted")

    bare_s3_provider_outage_marker = sample_manifest()
    bare_s3_provider_outage_marker["commands"][provider_matrix_command_index][
        "output"
    ] = bare_s3_provider_outage_marker["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider outage profile aws_us_east_1 down=true healed=true fail_closed=true recovered=true source=command",
        "ok: S3 provider outage profile aws_us_east_1",
    )
    failures = validate_release_evidence(bare_s3_provider_outage_marker)
    if not any("fail_closed=true recovered=true" in failure for failure in failures):
        raise AssertionError("bare S3 provider outage marker was accepted")

    unverified_s3_provider_outage_marker = sample_manifest()
    unverified_s3_provider_outage_marker["commands"][provider_matrix_command_index][
        "output"
    ] = unverified_s3_provider_outage_marker["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider outage profile aws_us_east_1 down=true healed=true fail_closed=true recovered=true source=command",
        "ok: S3 provider outage profile aws_us_east_1 down=true healed=false fail_closed=true recovered=false",
    )
    failures = validate_release_evidence(unverified_s3_provider_outage_marker)
    if not any("fail_closed=true recovered=true" in failure for failure in failures):
        raise AssertionError("unverified S3 provider outage marker was accepted")

    missing_s3_provider_outage_detail = sample_manifest()
    missing_s3_provider_outage_detail["commands"][provider_matrix_command_index][
        "output"
    ] = missing_s3_provider_outage_detail["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        (
            "ok: S3 provider outage detail profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false "
            "down=true healed=true fail_closed=true recovered=true "
            "source=command\n"
        ),
        "",
    )
    failures = validate_release_evidence(missing_s3_provider_outage_detail)
    if not any("outage detail marker" in failure for failure in failures):
        raise AssertionError("missing S3 provider outage detail marker was accepted")

    mismatched_s3_provider_outage_detail_endpoint = sample_manifest()
    mismatched_s3_provider_outage_detail_endpoint["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_s3_provider_outage_detail_endpoint["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it",
        "endpoint=s3.amazonaws.com:9000 bucket=zmq-aws-it",
    )
    failures = validate_release_evidence(mismatched_s3_provider_outage_detail_endpoint)
    if not any("endpoint=s3.amazonaws.com:443" in failure for failure in failures):
        raise AssertionError("mismatched S3 provider outage detail endpoint was accepted")

    unverified_s3_provider_outage_detail = sample_manifest()
    unverified_s3_provider_outage_detail["commands"][provider_matrix_command_index][
        "output"
    ] = unverified_s3_provider_outage_detail["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "down=true healed=true fail_closed=true recovered=true",
        "down=true healed=false fail_closed=true recovered=false",
        1,
    )
    failures = validate_release_evidence(unverified_s3_provider_outage_detail)
    if not any("fail_closed=true recovered=true" in failure for failure in failures):
        raise AssertionError("unverified S3 provider outage detail marker was accepted")

    missing_source_s3_provider_outage_detail = sample_manifest()
    missing_source_s3_provider_outage_detail["commands"][
        provider_matrix_command_index
    ]["output"] = missing_source_s3_provider_outage_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_outage_detail_marker,
        aws_s3_outage_detail_marker.replace(" source=command", ""),
    )
    failures = validate_release_evidence(missing_source_s3_provider_outage_detail)
    if not any(
        "source=command" in failure and "outage detail" in failure
        for failure in failures
    ):
        raise AssertionError("missing S3 provider outage detail command source was accepted")

    mismatched_source_s3_provider_outage_detail = sample_manifest()
    mismatched_source_s3_provider_outage_detail["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_source_s3_provider_outage_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_outage_detail_marker,
        aws_s3_outage_detail_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_source_s3_provider_outage_detail)
    if not any(
        "source=command" in failure and "outage detail" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider outage detail command source was accepted"
        )

    duplicate_s3_provider_outage_detail = sample_manifest()
    duplicate_s3_provider_outage_detail["commands"][provider_matrix_command_index][
        "output"
    ] = duplicate_s3_provider_outage_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_outage_detail_marker,
        (
            aws_s3_outage_detail_marker.replace(
                "bucket=zmq-aws-it",
                "bucket=wrong-bucket",
            )
            + "\n"
            + aws_s3_outage_detail_marker
        ),
    )
    failures = validate_release_evidence(duplicate_s3_provider_outage_detail)
    if not any("outage detail marker must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate S3 provider outage detail marker was accepted")

    bare_s3_provider_process_crash_marker = sample_manifest()
    bare_s3_provider_process_crash_marker["commands"][provider_matrix_command_index][
        "output"
    ] = bare_s3_provider_process_crash_marker["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider process-crash profile aws_us_east_1 killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
        "ok: S3 provider process-crash profile aws_us_east_1",
    )
    failures = validate_release_evidence(bare_s3_provider_process_crash_marker)
    if not any("recovered_payloads=2" in failure for failure in failures):
        raise AssertionError("bare S3 provider process-crash marker was accepted")

    unverified_s3_provider_process_crash_marker = sample_manifest()
    unverified_s3_provider_process_crash_marker["commands"][provider_matrix_command_index][
        "output"
    ] = unverified_s3_provider_process_crash_marker["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "ok: S3 provider process-crash profile aws_us_east_1 killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
        "ok: S3 provider process-crash profile aws_us_east_1 killed_broker=true fresh_data_dir=false recovered_payloads=1",
    )
    failures = validate_release_evidence(unverified_s3_provider_process_crash_marker)
    if not any("recovered_payloads=2" in failure for failure in failures):
        raise AssertionError("unverified S3 provider process-crash marker was accepted")

    missing_s3_provider_process_crash_detail = sample_manifest()
    missing_s3_provider_process_crash_detail["commands"][provider_matrix_command_index][
        "output"
    ] = missing_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        (
            "ok: S3 provider process-crash detail profile aws_us_east_1 "
            "bucket=zmq-aws-it topic=zmq-process-crash "
            "group=zmq-process-crash-group killed_broker=true "
            "fresh_data_dir=true first_offset=0 committed_offset=1 "
            "replacement_offset=2 recovered_payloads=2 source=command\n"
        ),
        "",
    )
    failures = validate_release_evidence(missing_s3_provider_process_crash_detail)
    if not any("process-crash detail marker" in failure for failure in failures):
        raise AssertionError("missing S3 provider process-crash detail marker was accepted")

    mismatched_s3_provider_process_crash_detail_bucket = sample_manifest()
    mismatched_s3_provider_process_crash_detail_bucket["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_s3_provider_process_crash_detail_bucket["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "bucket=zmq-aws-it topic=zmq-process-crash",
        "bucket=wrong-bucket topic=zmq-process-crash",
    )
    failures = validate_release_evidence(mismatched_s3_provider_process_crash_detail_bucket)
    if not any("bucket=zmq-aws-it" in failure for failure in failures):
        raise AssertionError("mismatched S3 provider process-crash detail bucket was accepted")

    stale_s3_provider_process_crash_detail = sample_manifest()
    stale_s3_provider_process_crash_detail["commands"][provider_matrix_command_index][
        "output"
    ] = stale_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "replacement_offset=2 recovered_payloads=2",
        "replacement_offset=0 recovered_payloads=2",
    )
    failures = validate_release_evidence(stale_s3_provider_process_crash_detail)
    if not any("replacement_offset=<offset>" in failure for failure in failures):
        raise AssertionError("stale S3 provider process-crash detail offset was accepted")

    missing_source_s3_provider_process_crash_detail = sample_manifest()
    missing_source_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"] = missing_source_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_process_crash_detail_marker,
        aws_s3_process_crash_detail_marker.replace(" source=command", ""),
    )
    failures = validate_release_evidence(missing_source_s3_provider_process_crash_detail)
    if not any(
        "source=command" in failure and "process-crash detail" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing S3 provider process-crash detail command source was accepted"
        )

    mismatched_source_s3_provider_process_crash_detail = sample_manifest()
    mismatched_source_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_source_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_process_crash_detail_marker,
        aws_s3_process_crash_detail_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_source_s3_provider_process_crash_detail)
    if not any(
        "source=command" in failure and "process-crash detail" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider process-crash detail command source was accepted"
        )

    duplicate_s3_provider_process_crash_detail = sample_manifest()
    duplicate_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"] = duplicate_s3_provider_process_crash_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_process_crash_detail_marker,
        (
            aws_s3_process_crash_detail_marker.replace(
                "replacement_offset=2",
                "replacement_offset=0",
            )
            + "\n"
            + aws_s3_process_crash_detail_marker
        ),
    )
    failures = validate_release_evidence(duplicate_s3_provider_process_crash_detail)
    if not any("process-crash detail marker must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate S3 provider process-crash detail marker was accepted")

    bare_s3_provider_live_suite_marker = sample_manifest()
    bare_s3_provider_live_suite_marker["commands"][provider_matrix_command_index][
        "output"
    ] = bare_s3_provider_live_suite_marker["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider live-suite profile minio command_started=true completed=true source=command",
        "ok: S3 provider live-suite profile minio",
    )
    failures = validate_release_evidence(bare_s3_provider_live_suite_marker)
    if not any("command_started=true completed=true" in failure for failure in failures):
        raise AssertionError("bare S3 provider live-suite marker was accepted")

    minio_s3_live_suite_marker = (
        "ok: S3 provider live-suite profile minio "
        "command_started=true completed=true source=command"
    )
    missing_source_s3_provider_live_suite_marker = sample_manifest()
    missing_source_s3_provider_live_suite_marker["commands"][
        provider_matrix_command_index
    ]["output"] = missing_source_s3_provider_live_suite_marker["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        minio_s3_live_suite_marker,
        minio_s3_live_suite_marker.replace(" source=command", ""),
    )
    failures = validate_release_evidence(missing_source_s3_provider_live_suite_marker)
    if not any(
        "live-suite profile minio" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("missing S3 provider live-suite command source was accepted")

    mismatched_source_s3_provider_live_suite_marker = sample_manifest()
    mismatched_source_s3_provider_live_suite_marker["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_source_s3_provider_live_suite_marker["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        minio_s3_live_suite_marker,
        minio_s3_live_suite_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(
        mismatched_source_s3_provider_live_suite_marker
    )
    if not any(
        "live-suite profile minio" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider live-suite command source was accepted"
        )

    detached_s3_provider_live_suite_marker = sample_manifest()
    detached_s3_provider_live_suite_marker["commands"][provider_matrix_command_index]["output"] = (
        detached_s3_provider_live_suite_marker["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            (
                "ok: S3 provider live-suite profile minio "
                "command_started=true completed=true source=command\n"
                + minio_s3_provider_marker
            ),
            (
                minio_s3_provider_marker
                + "\n"
                "ok: S3 provider live-suite profile minio "
                "command_started=true completed=true source=command"
            ),
        )
    )
    failures = validate_release_evidence(detached_s3_provider_live_suite_marker)
    if not any("same-block S3 provider live-suite" in failure for failure in failures):
        raise AssertionError("detached S3 provider live-suite marker was accepted")

    detached_s3_provider_subprofile_marker = sample_manifest()
    detached_s3_provider_subprofile_marker["commands"][provider_matrix_command_index]["output"] = (
        detached_s3_provider_subprofile_marker["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            (
                "ok: S3 provider multipart-fault profile aws_us_east_1 "
                "command_started=true completed=true injected=true recovered=true "
                "source=command\n"
                + aws_s3_provider_marker
            ),
            (
                aws_s3_provider_marker
                + "\n"
                "ok: S3 provider multipart-fault profile aws_us_east_1 "
                "command_started=true completed=true injected=true recovered=true "
                "source=command"
            ),
        )
    )
    failures = validate_release_evidence(detached_s3_provider_subprofile_marker)
    if not any("same-block S3 provider multipart-fault" in failure for failure in failures):
        raise AssertionError("detached S3 provider subprofile marker was accepted")

    bare_s3_provider_list_pagination_marker = sample_manifest()
    bare_s3_provider_list_pagination_marker["commands"][provider_matrix_command_index][
        "output"
    ] = bare_s3_provider_list_pagination_marker["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "ok: S3 provider list-pagination profile aws_us_east_1 required=true completed=true source=command",
        "ok: S3 provider list-pagination profile aws_us_east_1",
    )
    failures = validate_release_evidence(bare_s3_provider_list_pagination_marker)
    if not any("required=true completed=true" in failure for failure in failures):
        raise AssertionError("bare S3 provider list-pagination marker was accepted")

    unverified_s3_provider_multipart_edge_marker = sample_manifest()
    unverified_s3_provider_multipart_edge_marker["commands"][provider_matrix_command_index][
        "output"
    ] = unverified_s3_provider_multipart_edge_marker["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "ok: S3 provider multipart-edge profile aws_us_east_1 required=true completed=true source=command",
        "ok: S3 provider multipart-edge profile aws_us_east_1 required=true completed=false",
    )
    failures = validate_release_evidence(unverified_s3_provider_multipart_edge_marker)
    if not any("required=true completed=true" in failure for failure in failures):
        raise AssertionError("unverified S3 provider multipart-edge marker was accepted")

    bare_s3_provider_multipart_fault_marker = sample_manifest()
    bare_s3_provider_multipart_fault_marker["commands"][provider_matrix_command_index][
        "output"
    ] = bare_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "ok: S3 provider multipart-fault profile aws_us_east_1 command_started=true completed=true injected=true recovered=true source=command",
        "ok: S3 provider multipart-fault profile aws_us_east_1",
    )
    failures = validate_release_evidence(bare_s3_provider_multipart_fault_marker)
    if not any("command_started=true completed=true" in failure for failure in failures):
        raise AssertionError("bare S3 provider multipart-fault marker was accepted")

    missing_source_s3_provider_multipart_fault_marker = sample_manifest()
    missing_source_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ]["output"] = missing_source_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        aws_s3_multipart_fault_marker,
        aws_s3_multipart_fault_marker.replace(" source=command", ""),
    )
    failures = validate_release_evidence(
        missing_source_s3_provider_multipart_fault_marker
    )
    if not any(
        "multipart-fault profile aws_us_east_1" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing S3 provider multipart-fault marker command source was accepted"
        )

    mismatched_source_s3_provider_multipart_fault_marker = sample_manifest()
    mismatched_source_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_source_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ][
        "output"
    ].replace(
        aws_s3_multipart_fault_marker,
        aws_s3_multipart_fault_marker.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(
        mismatched_source_s3_provider_multipart_fault_marker
    )
    if not any(
        "multipart-fault profile aws_us_east_1" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched S3 provider multipart-fault marker command source was accepted"
        )

    duplicate_s3_provider_multipart_fault_marker = sample_manifest()
    duplicate_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ]["output"] = duplicate_s3_provider_multipart_fault_marker["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_multipart_fault_marker,
        aws_s3_multipart_fault_marker + "\n" + aws_s3_multipart_fault_marker,
    )
    failures = validate_release_evidence(duplicate_s3_provider_multipart_fault_marker)
    if not any("multipart-fault marker must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate S3 provider multipart-fault marker was accepted")

    missing_s3_provider_multipart_fault_detail = sample_manifest()
    missing_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"] = missing_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        (
            "ok: S3 multipart fault profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false "
            "injected=true recovered=true source=command\n"
        ),
        "",
    )
    failures = validate_release_evidence(missing_s3_provider_multipart_fault_detail)
    if not any("multipart-fault detail marker" in failure for failure in failures):
        raise AssertionError("missing S3 provider multipart-fault detail marker was accepted")

    detached_s3_provider_multipart_fault_detail = sample_manifest()
    detached_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"] = detached_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_multipart_fault_detail_marker + "\n",
        "",
        1,
    ).replace(
        s3_provider_matrix_summary,
        (
            s3_provider_matrix_summary
            + "\n"
            + aws_s3_multipart_fault_detail_marker
            + "\n"
            + aws_s3_multipart_fault_marker
            + "\n"
            + aws_s3_provider_marker
        ),
        1,
    )
    failures = validate_release_evidence(detached_s3_provider_multipart_fault_detail)
    if not any("multipart-fault detail marker" in failure for failure in failures):
        raise AssertionError("detached S3 provider multipart-fault detail marker was accepted")

    mismatched_s3_provider_multipart_fault_detail_bucket = sample_manifest()
    mismatched_s3_provider_multipart_fault_detail_bucket["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_s3_provider_multipart_fault_detail_bucket["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        (
            "ok: S3 multipart fault profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false "
            "injected=true recovered=true source=command"
        ),
        (
            "ok: S3 multipart fault profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=wrong-bucket "
            "scheme=https region=us-east-1 path_style=false "
            "injected=true recovered=true source=command"
        ),
    )
    failures = validate_release_evidence(
        mismatched_s3_provider_multipart_fault_detail_bucket
    )
    if not any("bucket=zmq-aws-it" in failure for failure in failures):
        raise AssertionError(
            "mismatched S3 provider multipart-fault detail bucket was accepted"
        )

    unverified_s3_provider_multipart_fault_detail = sample_manifest()
    unverified_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"] = unverified_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "injected=true recovered=true",
        "injected=false recovered=false",
        1,
    )
    failures = validate_release_evidence(unverified_s3_provider_multipart_fault_detail)
    if not any("injected=true recovered=true" in failure for failure in failures):
        raise AssertionError("unverified S3 provider multipart-fault detail was accepted")

    missing_source_s3_provider_multipart_fault_detail = sample_manifest()
    missing_source_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"] = missing_source_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        " source=command",
        "",
        1,
    )
    failures = validate_release_evidence(
        missing_source_s3_provider_multipart_fault_detail
    )
    if not any("source=command" in failure for failure in failures):
        raise AssertionError(
            "missing S3 provider multipart-fault command source was accepted"
        )

    mismatched_source_s3_provider_multipart_fault_detail = sample_manifest()
    mismatched_source_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"] = mismatched_source_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        "source=command",
        "source=wrapper",
        1,
    )
    failures = validate_release_evidence(
        mismatched_source_s3_provider_multipart_fault_detail
    )
    if not any("source=command" in failure for failure in failures):
        raise AssertionError(
            "mismatched S3 provider multipart-fault command source was accepted"
        )

    duplicate_s3_provider_multipart_fault_detail = sample_manifest()
    duplicate_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"] = duplicate_s3_provider_multipart_fault_detail["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_multipart_fault_detail_marker,
        (
            aws_s3_multipart_fault_detail_marker.replace(
                "bucket=zmq-aws-it",
                "bucket=wrong-bucket",
            )
            + "\n"
            + aws_s3_multipart_fault_detail_marker
        ),
    )
    failures = validate_release_evidence(duplicate_s3_provider_multipart_fault_detail)
    if not any("multipart-fault detail marker must not repeat" in failure for failure in failures):
        raise AssertionError("duplicate S3 provider multipart-fault detail marker was accepted")

    misattributed_s3_provider_profile_marker = sample_manifest()
    misattributed_s3_provider_profile_marker["commands"][provider_matrix_command_index][
        "output"
    ] = misattributed_s3_provider_profile_marker["commands"][
        provider_matrix_command_index
    ]["output"].replace(
        aws_s3_provider_marker,
        (
            "ok: S3 provider profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:9000 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false source=command\n"
            "ok: S3 provider profile aws_us_east_1 "
            "endpoint=s3.amazonaws.com:443 bucket=zmq-aws-it "
            "scheme=https region=us-east-1 path_style=false source=command"
        ),
        1,
    )
    failures = validate_release_evidence(misattributed_s3_provider_profile_marker)
    if not any(
        "same-block S3 provider" in failure
        and "matching provider-settings" in failure
        for failure in failures
    ):
        raise AssertionError("misattributed S3 provider profile marker block was accepted")

    bare_s3_provider_matrix_summary = sample_manifest()
    bare_s3_provider_matrix_summary["commands"][provider_matrix_command_index]["output"] = (
        bare_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
            "ok: S3 provider matrix passed",
        )
    )
    failures = validate_release_evidence(bare_s3_provider_matrix_summary)
    if not any("S3 provider matrix summary" in failure for failure in failures):
        raise AssertionError("bare S3 provider matrix summary marker was accepted")

    missing_s3_provider_matrix_summary_source = sample_manifest()
    missing_s3_provider_matrix_summary_source["commands"][provider_matrix_command_index]["output"] = (
        missing_s3_provider_matrix_summary_source["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
            "ok: S3 provider matrix passed for minio, aws_us_east_1",
        )
    )
    failures = validate_release_evidence(missing_s3_provider_matrix_summary_source)
    if not any(
        "S3 provider matrix summary" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("S3 provider matrix summary without source=command was accepted")

    suffixed_s3_provider_matrix_summary = sample_manifest()
    suffixed_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
        "output"
    ] = suffixed_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
        "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command wrapper=1",
    )
    failures = validate_release_evidence(suffixed_s3_provider_matrix_summary)
    if not any(
        "S3 provider matrix summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed S3 provider matrix summary marker was accepted")

    mismatched_s3_provider_matrix_summary = sample_manifest()
    mismatched_s3_provider_matrix_summary["commands"][provider_matrix_command_index]["output"] = (
        mismatched_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
            "output"
        ].replace(
            "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
            "ok: S3 provider matrix passed for minio source=command",
        )
    )
    failures = validate_release_evidence(mismatched_s3_provider_matrix_summary)
    if not any(
        "ZMQ_S3_PROVIDER_PROFILES" in failure and "aws_us_east_1" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched S3 provider matrix summary profiles were accepted")

    blank_s3_provider_matrix_summary = sample_manifest()
    blank_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
        "output"
    ] = blank_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
        "ok: S3 provider matrix passed for minio,, aws_us_east_1 source=command",
    )
    failures = validate_release_evidence(blank_s3_provider_matrix_summary)
    if not any("S3 provider matrix summary profiles" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank S3 provider matrix summary profile was accepted")

    duplicate_s3_provider_matrix_summary = sample_manifest()
    duplicate_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
        "output"
    ] = duplicate_s3_provider_matrix_summary["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        "ok: S3 provider matrix passed for minio, aws_us_east_1 source=command",
        (
            "ok: S3 provider matrix passed for minio, aws_us_east_1 "
            "source=command\n"
            "ok: S3 provider matrix passed for minio, aws_us_east_1 "
            "source=command"
        ),
    )
    failures = validate_release_evidence(duplicate_s3_provider_matrix_summary)
    if not any(
        "S3 provider matrix summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate S3 provider matrix summary marker was accepted")

    detached_s3_provider_profile_block = sample_manifest()
    aws_s3_provider_block = aws_s3_multipart_fault_marker + "\n" + aws_s3_provider_marker
    detached_s3_provider_profile_block["commands"][provider_matrix_command_index][
        "output"
    ] = detached_s3_provider_profile_block["commands"][provider_matrix_command_index][
        "output"
    ].replace(
        aws_s3_provider_block,
        "",
        1,
    ).replace(
        s3_provider_matrix_summary,
        s3_provider_matrix_summary + "\n" + aws_s3_provider_block,
        1,
    )
    failures = validate_release_evidence(detached_s3_provider_profile_block)
    if not any(
        "S3 provider profile output marker" in failure
        and "aws_us_east_1" in failure
        for failure in failures
    ):
        raise AssertionError("detached S3 provider profile block was accepted")

    missing_provider_profile_marker = sample_manifest()
    missing_provider_profile_marker["commands"][provider_matrix_command_index]["output"] = (
        missing_provider_profile_marker["commands"][provider_matrix_command_index]["output"].replace(
            "ok: S3 provider multipart-fault profile aws_us_east_1 command_started=true completed=true injected=true recovered=true source=command",
            "",
        )
    )
    failures = validate_release_evidence(missing_provider_profile_marker)
    if not any("multipart-fault profile aws_us_east_1" in failure for failure in failures):
        raise AssertionError("missing S3 provider profile coverage marker was not reported")

    missing_provider_live_suite_marker = sample_manifest()
    missing_provider_live_suite_marker["commands"][provider_matrix_command_index]["output"] = (
        missing_provider_live_suite_marker["commands"][provider_matrix_command_index]["output"].replace(
            "ok: S3 provider live-suite profile minio command_started=true completed=true source=command",
            "",
        )
    )
    failures = validate_release_evidence(missing_provider_live_suite_marker)
    if not any(
        "ok: S3 provider live-suite profile minio command_started=true completed=true source=command"
        in failure
        for failure in failures
    ):
        raise AssertionError("missing S3 provider live-suite output marker was not reported")

    missing_e2e_phase_marker = sample_manifest()
    e2e_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "Docker E2E gate"
    )

    missing_e2e_required_chaos_command = sample_manifest()
    missing_e2e_required_chaos_command["commands"][e2e_command_index]["command"] = (
        missing_e2e_required_chaos_command["commands"][e2e_command_index]["command"].replace(
            "ZMQ_E2E_REQUIRED_CHAOS_PHASES=cross-broker ",
            "",
        )
    )
    failures = validate_release_evidence(missing_e2e_required_chaos_command)
    if not any(
        "Docker E2E gate" in failure
        and "ZMQ_E2E_REQUIRED_CHAOS_PHASES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing Docker E2E required-chaos command assignment was accepted"
        )

    for env_name, assertion_message in (
        (
            "ZMQ_E2E_CHAOS_MATRIX",
            "missing Docker E2E chaos-matrix command assignment was accepted",
        ),
        (
            "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
            "missing Docker E2E required-load-scale command assignment was accepted",
        ),
        (
            "ZMQ_E2E_LOAD_SCALE_MATRIX",
            "missing Docker E2E load-scale-matrix command assignment was accepted",
        ),
    ):
        missing_e2e_command_assignment = sample_manifest()
        e2e_command_value = missing_e2e_command_assignment["environment"][env_name]
        missing_e2e_command_assignment["commands"][e2e_command_index][
            "command"
        ] = missing_e2e_command_assignment["commands"][e2e_command_index][
            "command"
        ].replace(
            f"{env_name}={e2e_command_value} ",
            "",
        )
        failures = validate_release_evidence(missing_e2e_command_assignment)
        if not any(
            "Docker E2E gate" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    mismatched_e2e_chaos_matrix_command = sample_manifest()
    mismatched_e2e_chaos_matrix_command["commands"][e2e_command_index]["command"] = (
        mismatched_e2e_chaos_matrix_command["commands"][e2e_command_index]["command"].replace(
            "ZMQ_E2E_CHAOS_MATRIX=cross-broker",
            "ZMQ_E2E_CHAOS_MATRIX=rack-a",
        )
    )
    failures = validate_release_evidence(mismatched_e2e_chaos_matrix_command)
    if not any(
        "Docker E2E gate" in failure
        and "ZMQ_E2E_CHAOS_MATRIX" in failure
        and "cross-broker" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched Docker E2E chaos matrix command assignment was accepted"
        )

    mismatched_e2e_load_scale_command = sample_manifest()
    mismatched_e2e_load_scale_command["commands"][e2e_command_index]["command"] = (
        mismatched_e2e_load_scale_command["commands"][e2e_command_index]["command"].replace(
            "ZMQ_E2E_LOAD_SCALE_MATRIX=load,scale-in,scale-out",
            "ZMQ_E2E_LOAD_SCALE_MATRIX=load",
        )
    )
    failures = validate_release_evidence(mismatched_e2e_load_scale_command)
    if not any(
        "Docker E2E gate" in failure
        and "ZMQ_E2E_LOAD_SCALE_MATRIX" in failure
        and "scale-out" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched Docker E2E load/scale command assignment was accepted"
        )

    e2e_load_scale_scale_out_marker = (
        "ok: E2E load/scale phase scale-out applied=true restored=true "
        "marker_payloads=hook-owned apply_source=hook restore_source=hook "
        "source=command"
    )
    missing_e2e_phase_marker["commands"][e2e_command_index]["output"] = (
        missing_e2e_phase_marker["commands"][e2e_command_index]["output"].replace(
            e2e_load_scale_scale_out_marker,
            "",
        )
    )
    failures = validate_release_evidence(missing_e2e_phase_marker)
    if not any("ok: E2E load/scale phase scale-out" in failure for failure in failures):
        raise AssertionError("missing E2E required phase output marker was not reported")

    bare_e2e_load_scale_phase_detail = sample_manifest()
    bare_e2e_load_scale_phase_detail["commands"][e2e_command_index]["output"] = (
        bare_e2e_load_scale_phase_detail["commands"][e2e_command_index]["output"].replace(
            e2e_load_scale_scale_out_marker,
            "ok: E2E load/scale phase scale-out",
        )
    )
    failures = validate_release_evidence(bare_e2e_load_scale_phase_detail)
    if not any("applied=true restored=true" in failure for failure in failures):
        raise AssertionError("bare E2E load/scale phase detail marker was accepted")

    unverified_e2e_load_scale_phase_detail = sample_manifest()
    unverified_e2e_load_scale_phase_detail["commands"][e2e_command_index]["output"] = (
        unverified_e2e_load_scale_phase_detail["commands"][e2e_command_index]["output"].replace(
            e2e_load_scale_scale_out_marker,
            (
                "ok: E2E load/scale phase scale-out applied=true restored=false "
                "marker_payloads=hook-owned apply_source=hook restore_source=hook "
                "source=command"
            ),
        )
    )
    failures = validate_release_evidence(unverified_e2e_load_scale_phase_detail)
    if not any("applied=true restored=true" in failure for failure in failures):
        raise AssertionError("unverified E2E load/scale phase detail marker was accepted")

    mismatched_e2e_load_scale_phase_source = sample_manifest()
    mismatched_e2e_load_scale_phase_source["commands"][e2e_command_index]["output"] = (
        mismatched_e2e_load_scale_phase_source["commands"][e2e_command_index]["output"].replace(
            e2e_load_scale_scale_out_marker,
            (
                "ok: E2E load/scale phase scale-out applied=true restored=true "
                "marker_payloads=hook-owned apply_source=fixture restore_source=hook "
                "source=command action=scale-out"
            ),
        )
    )
    failures = validate_release_evidence(mismatched_e2e_load_scale_phase_source)
    if not any("apply_source=hook restore_source=hook" in failure for failure in failures):
        raise AssertionError("mismatched E2E load/scale phase source was accepted")

    missing_e2e_load_scale_command_source = sample_manifest()
    missing_e2e_load_scale_command_source["commands"][e2e_command_index]["output"] = (
        missing_e2e_load_scale_command_source["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_load_scale_scale_out_marker,
            e2e_load_scale_scale_out_marker.replace(" source=command", ""),
        )
    )
    failures = validate_release_evidence(missing_e2e_load_scale_command_source)
    if not any(
        "source=command" in failure and "scale-out" in failure
        for failure in failures
    ):
        raise AssertionError("missing E2E load/scale phase command source was accepted")

    mismatched_e2e_load_scale_command_source = sample_manifest()
    mismatched_e2e_load_scale_command_source["commands"][e2e_command_index]["output"] = (
        mismatched_e2e_load_scale_command_source["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_load_scale_scale_out_marker,
            e2e_load_scale_scale_out_marker.replace("source=command", "source=wrapper"),
        )
    )
    failures = validate_release_evidence(mismatched_e2e_load_scale_command_source)
    if not any(
        "source=command" in failure and "scale-out" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched E2E load/scale phase command source was accepted"
        )

    hook_owned_e2e_load_scale_with_action = sample_manifest()
    hook_owned_e2e_load_scale_with_action["commands"][e2e_command_index][
        "output"
    ] = hook_owned_e2e_load_scale_with_action["commands"][e2e_command_index][
        "output"
    ].replace(
        e2e_load_scale_scale_out_marker,
        (
            "ok: E2E load/scale phase scale-out applied=true restored=true "
            "marker_payloads=hook-owned apply_source=hook restore_source=hook "
            "source=command action=scale-out"
        ),
    )
    failures = validate_release_evidence(hook_owned_e2e_load_scale_with_action)
    if not any("must not report fixture action" in failure for failure in failures):
        raise AssertionError(
            "hook-owned E2E load/scale marker with fixture action was accepted"
        )

    duplicate_e2e_load_scale_phase_detail = sample_manifest()
    duplicate_e2e_load_scale_phase_detail["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_load_scale_phase_detail["commands"][e2e_command_index]["output"].replace(
            e2e_load_scale_scale_out_marker,
            (
                "ok: E2E load/scale phase scale-out applied=true restored=true "
                "marker_payloads=hook-owned apply_source=fixture restore_source=hook "
                "source=command action=scale-out\n"
                + e2e_load_scale_scale_out_marker
            ),
        )
    )
    failures = validate_release_evidence(duplicate_e2e_load_scale_phase_detail)
    if not any("must not repeat phase scale-out" in failure for failure in failures):
        raise AssertionError("duplicate E2E load/scale phase detail marker was accepted")

    detached_e2e_load_scale_phase_detail = sample_manifest()
    detached_e2e_output = detached_e2e_load_scale_phase_detail["commands"][
        e2e_command_index
    ]["output"].replace(e2e_load_scale_scale_out_marker + "\n", "")
    e2e_load_scale_summary = (
        "ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command"
    )
    detached_e2e_output = detached_e2e_output.replace(
        e2e_load_scale_summary,
        e2e_load_scale_summary + "\n" + e2e_load_scale_scale_out_marker,
    )
    detached_e2e_load_scale_phase_detail["commands"][e2e_command_index][
        "output"
    ] = detached_e2e_output
    failures = validate_release_evidence(detached_e2e_load_scale_phase_detail)
    if not any(
        "before the E2E load/scale summary line" in failure
        and "scale-out" in failure
        for failure in failures
    ):
        raise AssertionError(
            "detached E2E load/scale phase detail marker was accepted"
        )

    e2e_chaos_phase_marker = (
        "ok: E2E chaos phase cross-broker down=true "
        "observed=failed healed=true recovered=true expect=fail source=command"
    )
    stale_e2e_chaos_phase_marker = (
        "ok: E2E chaos phase cross-broker down=true healed=true expect=fail"
    )

    bare_e2e_chaos_phase_detail = sample_manifest()
    bare_e2e_chaos_phase_detail["commands"][e2e_command_index]["output"] = (
        bare_e2e_chaos_phase_detail["commands"][e2e_command_index]["output"].replace(
            e2e_chaos_phase_marker,
            "ok: E2E chaos phase cross-broker",
        )
    )
    failures = validate_release_evidence(bare_e2e_chaos_phase_detail)
    if not any(
        "down=true observed=failed" in failure and "recovered=true" in failure
        for failure in failures
    ):
        raise AssertionError("bare E2E chaos phase detail marker was accepted")

    stale_e2e_chaos_phase_detail = sample_manifest()
    stale_e2e_chaos_phase_detail["commands"][e2e_command_index]["output"] = (
        stale_e2e_chaos_phase_detail["commands"][e2e_command_index]["output"].replace(
            e2e_chaos_phase_marker,
            stale_e2e_chaos_phase_marker,
        )
    )
    failures = validate_release_evidence(stale_e2e_chaos_phase_detail)
    if not any(
        "down=true observed=failed" in failure and "recovered=true" in failure
        for failure in failures
    ):
        raise AssertionError("stale E2E chaos phase detail marker was accepted")

    mismatched_e2e_chaos_phase_expect = sample_manifest()
    mismatched_e2e_chaos_phase_expect["commands"][e2e_command_index]["output"] = (
        mismatched_e2e_chaos_phase_expect["commands"][e2e_command_index]["output"].replace(
            e2e_chaos_phase_marker,
            (
                "ok: E2E chaos phase cross-broker down=true "
                "observed=failed healed=true recovered=true expect=survive "
                "source=command"
            ),
        )
    )
    failures = validate_release_evidence(mismatched_e2e_chaos_phase_expect)
    if not any("expect=fail" in failure for failure in failures):
        raise AssertionError("mismatched E2E chaos phase expectation was accepted")

    mismatched_e2e_chaos_phase_observed = sample_manifest()
    mismatched_e2e_chaos_phase_observed["commands"][e2e_command_index][
        "output"
    ] = mismatched_e2e_chaos_phase_observed["commands"][e2e_command_index][
        "output"
    ].replace(
        e2e_chaos_phase_marker,
        (
            "ok: E2E chaos phase cross-broker down=true "
            "observed=survived healed=true recovered=true expect=fail "
            "source=command"
        ),
    )
    failures = validate_release_evidence(mismatched_e2e_chaos_phase_observed)
    if not any("observed=failed" in failure for failure in failures):
        raise AssertionError("mismatched E2E chaos phase observed result was accepted")

    missing_e2e_chaos_phase_source = sample_manifest()
    missing_e2e_chaos_phase_source["commands"][e2e_command_index]["output"] = (
        missing_e2e_chaos_phase_source["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_chaos_phase_marker,
            e2e_chaos_phase_marker.replace(" source=command", ""),
        )
    )
    failures = validate_release_evidence(missing_e2e_chaos_phase_source)
    if not any(
        "source=command" in failure and "cross-broker" in failure
        for failure in failures
    ):
        raise AssertionError("missing E2E chaos phase source was accepted")

    mismatched_e2e_chaos_phase_source = sample_manifest()
    mismatched_e2e_chaos_phase_source["commands"][e2e_command_index]["output"] = (
        mismatched_e2e_chaos_phase_source["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_chaos_phase_marker,
            e2e_chaos_phase_marker.replace("source=command", "source=wrapper"),
        )
    )
    failures = validate_release_evidence(mismatched_e2e_chaos_phase_source)
    if not any(
        "source=command" in failure and "cross-broker" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched E2E chaos phase source was accepted")

    duplicate_e2e_chaos_phase_detail = sample_manifest()
    duplicate_e2e_chaos_phase_detail["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_chaos_phase_detail["commands"][e2e_command_index]["output"].replace(
            e2e_chaos_phase_marker,
            (
                "ok: E2E chaos phase cross-broker down=true "
                "observed=survived healed=true recovered=true expect=survive "
                "source=command\n"
                "ok: E2E chaos phase cross-broker down=true "
                "observed=failed healed=true recovered=true expect=fail "
                "source=command"
            ),
        )
    )
    failures = validate_release_evidence(duplicate_e2e_chaos_phase_detail)
    if not any("must not repeat phase cross-broker" in failure for failure in failures):
        raise AssertionError("duplicate E2E chaos phase detail marker was accepted")

    detached_e2e_chaos_phase_detail = sample_manifest()
    detached_e2e_chaos_output = detached_e2e_chaos_phase_detail["commands"][
        e2e_command_index
    ]["output"].replace(e2e_chaos_phase_marker + "\n", "")
    e2e_chaos_summary = "ok: E2E chaos passed for cross-broker phase(s) source=command"
    detached_e2e_chaos_output = detached_e2e_chaos_output.replace(
        e2e_chaos_summary,
        e2e_chaos_summary + "\n" + e2e_chaos_phase_marker,
    )
    detached_e2e_chaos_phase_detail["commands"][e2e_command_index][
        "output"
    ] = detached_e2e_chaos_output
    failures = validate_release_evidence(detached_e2e_chaos_phase_detail)
    if not any(
        "before the E2E chaos summary line" in failure
        and "cross-broker" in failure
        for failure in failures
    ):
        raise AssertionError("detached E2E chaos phase detail marker was accepted")

    bare_e2e_chaos_summary = sample_manifest()
    bare_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        bare_e2e_chaos_summary["commands"][e2e_command_index]["output"].replace(
            "ok: E2E chaos passed for cross-broker phase(s) source=command",
            "ok: E2E chaos passed",
        )
    )
    failures = validate_release_evidence(bare_e2e_chaos_summary)
    if not any("Docker E2E chaos summary" in failure for failure in failures):
        raise AssertionError("bare Docker E2E chaos summary marker was accepted")

    missing_source_e2e_chaos_summary = sample_manifest()
    missing_source_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        missing_source_e2e_chaos_summary["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_chaos_summary,
            e2e_chaos_summary.replace(" source=command", ""),
        )
    )
    failures = validate_release_evidence(missing_source_e2e_chaos_summary)
    if not any(
        "Docker E2E chaos summary" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "Docker E2E chaos summary without source=command was accepted"
        )

    mismatched_source_e2e_chaos_summary = sample_manifest()
    mismatched_source_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        mismatched_source_e2e_chaos_summary["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_chaos_summary,
            e2e_chaos_summary.replace("source=command", "source=wrapper"),
        )
    )
    failures = validate_release_evidence(mismatched_source_e2e_chaos_summary)
    if not any(
        "Docker E2E chaos summary" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "Docker E2E chaos summary with wrapper source was accepted"
        )

    suffixed_e2e_chaos_summary = sample_manifest()
    suffixed_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        suffixed_e2e_chaos_summary["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_chaos_summary,
            e2e_chaos_summary + " wrapper=1",
        )
    )
    failures = validate_release_evidence(suffixed_e2e_chaos_summary)
    if not any(
        "Docker E2E chaos summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed Docker E2E chaos summary marker was accepted")

    mismatched_e2e_chaos_summary = sample_manifest()
    mismatched_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        mismatched_e2e_chaos_summary["commands"][e2e_command_index]["output"].replace(
            "ok: E2E chaos passed for cross-broker phase(s) source=command",
            "ok: E2E chaos passed for rack-a phase(s) source=command",
        )
    )
    failures = validate_release_evidence(mismatched_e2e_chaos_summary)
    if not any(
        "ZMQ_E2E_CHAOS_MATRIX" in failure and "cross-broker" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched Docker E2E chaos summary phases were accepted")

    blank_e2e_chaos_summary = sample_manifest()
    blank_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        blank_e2e_chaos_summary["commands"][e2e_command_index]["output"].replace(
            "ok: E2E chaos passed for cross-broker phase(s) source=command",
            "ok: E2E chaos passed for cross-broker,, phase(s) source=command",
        )
    )
    failures = validate_release_evidence(blank_e2e_chaos_summary)
    if not any("Docker E2E chaos summary phases" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank Docker E2E chaos summary phase was accepted")

    duplicate_e2e_chaos_summary = sample_manifest()
    duplicate_e2e_chaos_summary["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_chaos_summary["commands"][e2e_command_index]["output"].replace(
            e2e_chaos_summary,
            e2e_chaos_summary + "\n" + e2e_chaos_summary,
        )
    )
    failures = validate_release_evidence(duplicate_e2e_chaos_summary)
    if not any(
        "Docker E2E chaos summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate Docker E2E chaos summary marker was accepted")

    bare_e2e_load_scale_summary = sample_manifest()
    bare_e2e_load_scale_summary["commands"][e2e_command_index]["output"] = (
        bare_e2e_load_scale_summary["commands"][e2e_command_index]["output"].replace(
            "ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command",
            "ok: E2E load/scale passed",
        )
    )
    failures = validate_release_evidence(bare_e2e_load_scale_summary)
    if not any("Docker E2E load/scale summary" in failure for failure in failures):
        raise AssertionError("bare Docker E2E load/scale summary marker was accepted")

    missing_source_e2e_load_scale_summary = sample_manifest()
    missing_source_e2e_load_scale_summary["commands"][e2e_command_index][
        "output"
    ] = missing_source_e2e_load_scale_summary["commands"][e2e_command_index][
        "output"
    ].replace(
        e2e_load_scale_summary,
        e2e_load_scale_summary.replace(" source=command", ""),
    )
    failures = validate_release_evidence(missing_source_e2e_load_scale_summary)
    if not any(
        "Docker E2E load/scale summary" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "Docker E2E load/scale summary without source=command was accepted"
        )

    mismatched_source_e2e_load_scale_summary = sample_manifest()
    mismatched_source_e2e_load_scale_summary["commands"][e2e_command_index][
        "output"
    ] = mismatched_source_e2e_load_scale_summary["commands"][e2e_command_index][
        "output"
    ].replace(
        e2e_load_scale_summary,
        e2e_load_scale_summary.replace("source=command", "source=wrapper"),
    )
    failures = validate_release_evidence(mismatched_source_e2e_load_scale_summary)
    if not any(
        "Docker E2E load/scale summary" in failure
        and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError(
            "Docker E2E load/scale summary with wrapper source was accepted"
        )

    suffixed_e2e_load_scale_summary = sample_manifest()
    suffixed_e2e_load_scale_summary["commands"][e2e_command_index]["output"] = (
        suffixed_e2e_load_scale_summary["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_load_scale_summary,
            e2e_load_scale_summary + " wrapper=1",
        )
    )
    failures = validate_release_evidence(suffixed_e2e_load_scale_summary)
    if not any(
        "Docker E2E load/scale summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed Docker E2E load/scale summary marker was accepted")

    mismatched_e2e_load_scale_summary = sample_manifest()
    mismatched_e2e_load_scale_summary["commands"][e2e_command_index]["output"] = (
        mismatched_e2e_load_scale_summary["commands"][e2e_command_index]["output"].replace(
            "ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command",
            "ok: E2E load/scale passed for load phase(s) source=command",
        )
    )
    failures = validate_release_evidence(mismatched_e2e_load_scale_summary)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_MATRIX" in failure and "scale-out" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched Docker E2E load/scale summary phases were accepted")

    blank_e2e_load_scale_summary = sample_manifest()
    blank_e2e_load_scale_summary["commands"][e2e_command_index]["output"] = (
        blank_e2e_load_scale_summary["commands"][e2e_command_index]["output"].replace(
            "ok: E2E load/scale passed for load, scale-in, scale-out phase(s) source=command",
            "ok: E2E load/scale passed for load, scale-in,, scale-out phase(s) source=command",
        )
    )
    failures = validate_release_evidence(blank_e2e_load_scale_summary)
    if not any("Docker E2E load/scale summary phases" in failure and "blank" in failure for failure in failures):
        raise AssertionError("embedded blank Docker E2E load/scale summary phase was accepted")

    duplicate_e2e_load_scale_summary = sample_manifest()
    duplicate_e2e_load_scale_summary["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_load_scale_summary["commands"][e2e_command_index][
            "output"
        ].replace(
            e2e_load_scale_summary,
            e2e_load_scale_summary + "\n" + e2e_load_scale_summary,
        )
    )
    failures = validate_release_evidence(duplicate_e2e_load_scale_summary)
    if not any(
        "Docker E2E load/scale summary output marker must appear exactly once"
        in failure
        for failure in failures
    ):
        raise AssertionError("duplicate Docker E2E load/scale summary marker was accepted")

    bare_e2e_results_line = sample_manifest()
    bare_e2e_results_line["commands"][e2e_command_index]["output"] = (
        bare_e2e_results_line["commands"][e2e_command_index]["output"].replace(
            "Results: 53/53 passed, 0 failed",
            "Results:",
        )
    )
    failures = validate_release_evidence(bare_e2e_results_line)
    if not any("Docker E2E final results line" in failure for failure in failures):
        raise AssertionError("bare Docker E2E results line was accepted")

    failed_e2e_results_line = sample_manifest()
    failed_e2e_results_line["commands"][e2e_command_index]["output"] = (
        failed_e2e_results_line["commands"][e2e_command_index]["output"].replace(
            "Results: 53/53 passed, 0 failed",
            "Results: 52/53 passed, 1 failed",
        )
    )
    failures = validate_release_evidence(failed_e2e_results_line)
    if not any("0 failed" in failure and "52/53" in failure for failure in failures):
        raise AssertionError("failed Docker E2E results line was accepted")

    duplicate_e2e_results_line = sample_manifest()
    duplicate_e2e_results_line["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_results_line["commands"][e2e_command_index]["output"].replace(
            "Results: 53/53 passed, 0 failed",
            "Results: 53/53 passed, 0 failed\nResults: 53/53 passed, 0 failed",
        )
    )
    failures = validate_release_evidence(duplicate_e2e_results_line)
    if not any(
        "Docker E2E final results line must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate Docker E2E results line was accepted")

    duplicate_detached_e2e_results_marker = sample_manifest()
    duplicate_detached_e2e_results_marker["commands"][e2e_command_index]["output"] = (
        duplicate_detached_e2e_results_marker["commands"][e2e_command_index]["output"].replace(
            "ok: E2E chaos phase cross-broker",
            "Results:\nok: E2E chaos phase cross-broker",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_detached_e2e_results_marker)
    if not any(
        "Docker E2E output line marker must appear exactly once: Results:" in failure
        for failure in failures
    ):
        raise AssertionError("detached duplicate Docker E2E results marker was accepted")

    detached_e2e_results_line = sample_manifest()
    detached_e2e_output = detached_e2e_results_line["commands"][e2e_command_index][
        "output"
    ].replace(
        "Results: 53/53 passed, 0 failed",
        "",
        1,
    )
    detached_e2e_output = detached_e2e_output.replace(
        "ok: E2E chaos passed for cross-broker phase(s) source=command",
        "Results: 53/53 passed, 0 failed\n"
        "ok: E2E chaos passed for cross-broker phase(s) source=command",
        1,
    )
    detached_e2e_results_line["commands"][e2e_command_index][
        "output"
    ] = detached_e2e_output
    failures = validate_release_evidence(detached_e2e_results_line)
    if not any("after required E2E phase summaries" in failure for failure in failures):
        raise AssertionError("detached Docker E2E results line was accepted")

    boxed_e2e_title_output = sample_manifest()
    boxed_e2e_title_output["commands"][e2e_command_index]["output"] = (
        boxed_e2e_title_output["commands"][e2e_command_index]["output"].replace(
            "3-Node E2E Test Suite",
            "\u2551  ZMQ \u2014 3-Node E2E Test Suite (combined mode + MinIO S3)   \u2551",
            1,
        )
    )
    failures = validate_release_evidence(boxed_e2e_title_output)
    if failures:
        raise AssertionError(f"boxed Docker E2E title marker was rejected: {failures}")

    embedded_e2e_title_output = sample_manifest()
    embedded_e2e_title_output["commands"][e2e_command_index]["output"] = (
        embedded_e2e_title_output["commands"][e2e_command_index]["output"].replace(
            "3-Node E2E Test Suite",
            "previous 3-Node E2E Test Suite",
            1,
        )
    )
    failures = validate_release_evidence(embedded_e2e_title_output)
    if not any("3-Node E2E Test Suite" in failure for failure in failures):
        raise AssertionError("embedded Docker E2E title marker was accepted")

    duplicate_e2e_title_output = sample_manifest()
    duplicate_e2e_title_output["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_title_output["commands"][e2e_command_index]["output"].replace(
            "3-Node E2E Test Suite",
            "3-Node E2E Test Suite\n3-Node E2E Test Suite",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_e2e_title_output)
    if not any(
        "Docker E2E output line marker must appear exactly once: "
        "3-Node E2E Test Suite" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate Docker E2E title marker was accepted")

    duplicate_e2e_chaos_section_output = sample_manifest()
    duplicate_e2e_chaos_section_output["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_chaos_section_output["commands"][e2e_command_index]["output"].replace(
            "[Test m] Cross-broker chaos phases",
            "[Test m] Cross-broker chaos phases\n"
            "[Test m] Cross-broker chaos phases",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_e2e_chaos_section_output)
    if not any(
        "Docker E2E output line marker must appear exactly once: "
        "[Test m] Cross-broker chaos phases" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate Docker E2E chaos section marker was accepted")

    duplicate_e2e_load_scale_section_output = sample_manifest()
    duplicate_e2e_load_scale_section_output["commands"][e2e_command_index]["output"] = (
        duplicate_e2e_load_scale_section_output["commands"][e2e_command_index]["output"].replace(
            "[Test n] Live load/scale phases",
            "[Test n] Live load/scale phases\n"
            "[Test n] Live load/scale phases",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_e2e_load_scale_section_output)
    if not any(
        "Docker E2E output line marker must appear exactly once: "
        "[Test n] Live load/scale phases" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate Docker E2E load/scale section marker was accepted")

    embedded_e2e_results_output = sample_manifest()
    embedded_e2e_results_output["commands"][e2e_command_index]["output"] = (
        embedded_e2e_results_output["commands"][e2e_command_index]["output"].replace(
            "Results:",
            "Previous Results:",
        )
    )
    failures = validate_release_evidence(embedded_e2e_results_output)
    if not any("Results:" in failure for failure in failures):
        raise AssertionError("embedded Docker E2E results marker was accepted")

    chaos_skip_line_output = sample_manifest()
    chaos_skip_line_output["commands"][chaos_command_index]["output"] += (
        "\nskip: set ZMQ_RUN_CHAOS_TESTS=1 to run broker chaos harness"
    )
    failures = validate_release_evidence(chaos_skip_line_output)
    if not any("captured skip output for broker chaos harness" in failure for failure in failures):
        raise AssertionError("line-aware broker chaos skip marker was not reported")

    embedded_chaos_skip_output = sample_manifest()
    embedded_chaos_skip_output["commands"][chaos_command_index]["output"] += (
        "\nnot skip: set ZMQ_RUN_CHAOS_TESTS=1 to run broker chaos harness"
    )
    failures = validate_release_evidence(embedded_chaos_skip_output)
    if any("captured skip output for broker chaos harness" in failure for failure in failures):
        raise AssertionError("embedded broker chaos skip marker was treated as a skip")

    skipped_minio_output = sample_manifest()
    minio_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "MinIO/S3 integration gate"
    )
    skipped_minio_output["commands"][minio_command_index]["output"] = (
        "Build Summary: 3/3 steps succeeded; 2/8 tests passed (6 skipped)\n"
        "test-minio success\n"
        "+- run test 2 pass, 6 skip (8 total)"
    )
    failures = validate_release_evidence(skipped_minio_output)
    if not any(
        "captured skip output for MinIO/S3 integration gate" in failure
        or "successful Zig Build Summary" in failure
        for failure in failures
    ):
        raise AssertionError("skipped MinIO Zig test output was not rejected")

    embedded_minio_count_output = sample_manifest()
    embedded_minio_count_output["commands"][minio_command_index]["output"] = (
        embedded_minio_count_output["commands"][minio_command_index]["output"].replace(
            "8/8 tests passed",
            "previous 8/8 tests passed",
            1,
        )
    )
    failures = validate_release_evidence(embedded_minio_count_output)
    if not any("8/8 tests passed" in failure for failure in failures):
        raise AssertionError("embedded MinIO test-count marker was accepted")

    duplicate_minio_count_output = sample_manifest()
    duplicate_minio_count_output["commands"][minio_command_index]["output"] = (
        duplicate_minio_count_output["commands"][minio_command_index]["output"].replace(
            "8/8 tests passed",
            "8/8 tests passed\n8/8 tests passed",
            1,
        )
    )
    failures = validate_release_evidence(duplicate_minio_count_output)
    if not any(
        "MinIO 8/8 tests passed output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate MinIO test-count marker was accepted")

    missing_minio_edge_command = sample_manifest()
    missing_minio_edge_command["commands"][minio_command_index]["command"] = (
        missing_minio_edge_command["commands"][minio_command_index]["command"].replace(
            "ZMQ_S3_REQUIRE_MULTIPART_EDGE=1 ",
            "",
        )
    )
    failures = validate_release_evidence(missing_minio_edge_command)
    if not any("MinIO/S3 integration gate" in failure for failure in failures):
        raise AssertionError("missing MinIO multipart-edge command gate was accepted")

    missing_minio_pagination_command = sample_manifest()
    missing_minio_pagination_command["commands"][minio_command_index]["command"] = (
        missing_minio_pagination_command["commands"][minio_command_index][
            "command"
        ].replace(
            "ZMQ_S3_REQUIRE_LIST_PAGINATION=1 ",
            "",
        )
    )
    failures = validate_release_evidence(missing_minio_pagination_command)
    if not any("MinIO/S3 integration gate" in failure for failure in failures):
        raise AssertionError("missing MinIO pagination command gate was accepted")

    process_crash_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "S3 process-crash replacement gate"
    )

    bare_process_crash_summary = sample_manifest()
    bare_process_crash_summary["commands"][process_crash_command_index]["output"] = (
        bare_process_crash_summary["commands"][process_crash_command_index][
            "output"
        ].replace(
            (
                "ok: S3 process crash/replacement harness passed "
                "(bucket=zmq-crash-release, topic=s3-crash-release, "
                "group=s3-crash-group-release, killed_broker=true, "
                "fresh_data_dir=true, first_offset=0, committed_offset=1, "
                "replacement_offset=1, recovered_payloads=2) source=command"
            ),
            "ok: S3 process crash/replacement harness passed",
        )
    )
    failures = validate_release_evidence(bare_process_crash_summary)
    if not any("S3 process-crash summary" in failure for failure in failures):
        raise AssertionError("bare S3 process-crash summary marker was accepted")

    missing_source_process_crash_summary = sample_manifest()
    missing_source_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ] = missing_source_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ].replace(
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command"
        ),
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2)"
        ),
    )
    failures = validate_release_evidence(missing_source_process_crash_summary)
    if not any(
        "S3 process-crash summary" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("S3 process-crash summary without source=command was accepted")

    mismatched_source_process_crash_summary = sample_manifest()
    mismatched_source_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ] = mismatched_source_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ].replace(
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command"
        ),
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=wrapper"
        ),
    )
    failures = validate_release_evidence(mismatched_source_process_crash_summary)
    if not any(
        "S3 process-crash summary" in failure and "source=command" in failure
        for failure in failures
    ):
        raise AssertionError("S3 process-crash summary with wrapper source was accepted")

    suffixed_process_crash_summary = sample_manifest()
    suffixed_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ] = suffixed_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ].replace(
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command"
        ),
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command wrapper=1"
        ),
    )
    failures = validate_release_evidence(suffixed_process_crash_summary)
    if not any(
        "S3 process-crash summary output marker must appear exactly once "
        "with source=command as its own stripped line" in failure
        for failure in failures
    ):
        raise AssertionError("suffixed S3 process-crash summary marker was accepted")

    duplicate_process_crash_summary_marker = sample_manifest()
    duplicate_process_crash_summary_marker["commands"][process_crash_command_index][
        "output"
    ] = duplicate_process_crash_summary_marker["commands"][process_crash_command_index][
        "output"
    ].replace(
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command"
        ),
        (
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command\n"
            "ok: S3 process crash/replacement harness passed "
            "(bucket=zmq-crash-release, topic=s3-crash-release, "
            "group=s3-crash-group-release, killed_broker=true, "
            "fresh_data_dir=true, first_offset=0, committed_offset=1, "
            "replacement_offset=1, recovered_payloads=2) source=command"
        ),
    )
    failures = validate_release_evidence(duplicate_process_crash_summary_marker)
    if not any(
        "S3 process-crash summary output marker must appear exactly once" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate S3 process-crash summary marker was accepted")

    placeholder_process_crash_summary = sample_manifest()
    placeholder_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ] = placeholder_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ].replace(
        "bucket=zmq-crash-release",
        "bucket=/path/to/bucket",
    )
    failures = validate_release_evidence(placeholder_process_crash_summary)
    if not any("non-placeholder bucket" in failure for failure in failures):
        raise AssertionError("placeholder S3 process-crash bucket was accepted")

    stale_process_crash_offset = sample_manifest()
    stale_process_crash_offset["commands"][process_crash_command_index]["output"] = (
        stale_process_crash_offset["commands"][process_crash_command_index][
            "output"
        ].replace(
            "replacement_offset=1",
            "replacement_offset=0",
        )
    )
    failures = validate_release_evidence(stale_process_crash_offset)
    if not any("replacement_offset greater than first_offset" in failure for failure in failures):
        raise AssertionError("stale S3 process-crash replacement offset was accepted")

    duplicate_process_crash_summary = sample_manifest()
    duplicate_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ] = duplicate_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ].replace(
        "bucket=zmq-crash-release",
        "bucket=/path/to/bucket, bucket=zmq-crash-release",
    )
    failures = validate_release_evidence(duplicate_process_crash_summary)
    if not any("S3 process-crash summary must not repeat fields" in failure for failure in failures):
        raise AssertionError("duplicate S3 process-crash summary field was accepted")

    unknown_process_crash_summary = sample_manifest()
    unknown_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ] = unknown_process_crash_summary["commands"][process_crash_command_index][
        "output"
    ].replace(
        "recovered_payloads=2) source=command",
        "recovered_payloads=2, unchecked=true) source=command",
    )
    failures = validate_release_evidence(unknown_process_crash_summary)
    if not any("S3 process-crash summary must not include unknown fields" in failure for failure in failures):
        raise AssertionError("unknown S3 process-crash summary field was accepted")

    bad_zig_summary_output = sample_manifest()
    bad_zig_summary_output["commands"][0]["output"] = "test success"
    failures = validate_release_evidence(bad_zig_summary_output)
    if not any("Build Summary" in failure for failure in failures):
        raise AssertionError("missing Zig Build Summary output was not reported")

    failed_zig_summary_output = sample_manifest()
    failed_zig_summary_output["commands"][0]["output"] = (
        "Build Summary: 0/1 steps succeeded\n"
        "test success"
    )
    failures = validate_release_evidence(failed_zig_summary_output)
    if not any("successful Zig Build Summary" in failure for failure in failures):
        raise AssertionError("failed Zig Build Summary output was accepted")

    failed_zig_test_count_summary = sample_manifest()
    failed_zig_test_count_summary["commands"][0]["output"] = (
        "Build Summary: 1/1 steps succeeded; 0/1 tests passed (1 failed)\n"
        "test success"
    )
    failures = validate_release_evidence(failed_zig_test_count_summary)
    if not any("successful Zig Build Summary" in failure for failure in failures):
        raise AssertionError("failed Zig test-count Build Summary output was accepted")

    mixed_zig_summary_output = sample_manifest()
    mixed_zig_summary_output["commands"][0]["output"] = (
        "Build Summary: 0/1 steps succeeded\n"
        "Build Summary: 1/1 steps succeeded\n"
        "test success"
    )
    failures = validate_release_evidence(mixed_zig_summary_output)
    if not any("unsuccessful Zig Build Summary" in failure for failure in failures):
        raise AssertionError("mixed failed/successful Zig Build Summary output was accepted")

    duplicate_zig_summary_output = sample_manifest()
    duplicate_zig_summary_output["commands"][0]["output"] = (
        "Build Summary: 1/1 steps succeeded\n"
        "Build Summary: 1/1 steps succeeded\n"
        "test success"
    )
    failures = validate_release_evidence(duplicate_zig_summary_output)
    if not any("multiple successful Zig Build Summary" in failure for failure in failures):
        raise AssertionError("duplicate successful Zig Build Summary output was accepted")

    bad_zig_success_output = sample_manifest()
    bad_zig_success_output["commands"][0]["output"] = (
        "Build Summary: 1/1 steps succeeded\n"
        "not success"
    )
    failures = validate_release_evidence(bad_zig_success_output)
    if not any("Zig success line" in failure for failure in failures):
        raise AssertionError("negated Zig success output was accepted")

    wrong_zig_success_step_output = sample_manifest()
    wrong_zig_success_step_output["commands"][0]["output"] = (
        "Build Summary: 1/1 steps succeeded\n"
        "bench success"
    )
    failures = validate_release_evidence(wrong_zig_success_step_output)
    if not any("invoked build step" in failure for failure in failures):
        raise AssertionError("wrong Zig build-step success output was accepted")

    unpinned_zig = sample_manifest()
    unpinned_zig["commands"][0]["command"] = unpinned_zig["commands"][0]["command"].replace(
        RELEASE_ZIG,
        "zig",
    )
    failures = validate_release_evidence(unpinned_zig)
    if not any("default Zig test suite" in failure for failure in failures):
        raise AssertionError("unpinned Zig release command was not rejected")

    echoed_zig_command = sample_manifest()
    echoed_zig_command["commands"][0]["command"] = (
        f"echo {RELEASE_ZIG} build test --summary all"
    )
    failures = validate_release_evidence(echoed_zig_command)
    if not any("default Zig test suite" in failure for failure in failures):
        raise AssertionError("echoed Zig command text satisfied release command matching")

    embedded_zig_output_marker_command = sample_manifest()
    embedded_zig_output_marker_command["commands"][0]["command"] += (
        " && echo 'Build Summary: 1/1 steps succeeded'"
    )
    failures = validate_release_evidence(embedded_zig_output_marker_command)
    if not any("embeds output marker text" in failure for failure in failures):
        raise AssertionError("embedded Zig output marker command was accepted")

    embedded_live_output_marker_command = sample_manifest()
    chaos_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "broker chaos harness"
    )
    embedded_live_output_marker_command["commands"][chaos_command_index]["command"] += (
        " && echo 'ok: chaos harness passed for sigkill-restart source=command'"
    )
    failures = validate_release_evidence(embedded_live_output_marker_command)
    if not any("embeds output marker text" in failure for failure in failures):
        raise AssertionError("embedded live harness output marker command was accepted")

    masked_zig_command = sample_manifest()
    masked_zig_command["commands"][0]["command"] += " || true"
    failures = validate_release_evidence(masked_zig_command)
    if not any("non-success shell separator" in failure for failure in failures):
        raise AssertionError("failure-masked Zig command was accepted")

    piped_zig_command = sample_manifest()
    piped_zig_command["commands"][0]["command"] += " | tee /tmp/zmq-release.log"
    failures = validate_release_evidence(piped_zig_command)
    if not any("disallowed shell operator '|'" in failure for failure in failures):
        raise AssertionError("piped Zig command was accepted")

    redirected_zig_command = sample_manifest()
    redirected_zig_command["commands"][0]["command"] += " > /tmp/zmq-release.log"
    failures = validate_release_evidence(redirected_zig_command)
    if not any("disallowed shell operator '>'" in failure for failure in failures):
        raise AssertionError("redirected Zig command was accepted")

    combined_redirected_zig_command = sample_manifest()
    combined_redirected_zig_command["commands"][0]["command"] += (
        " &>/tmp/zmq-release.log"
    )
    failures = validate_release_evidence(combined_redirected_zig_command)
    if not any("disallowed shell operator '&>'" in failure for failure in failures):
        raise AssertionError("combined-redirected Zig command was accepted")

    backgrounded_zig_command = sample_manifest()
    backgrounded_zig_command["commands"][0]["command"] += " &"
    failures = validate_release_evidence(backgrounded_zig_command)
    if not any("disallowed shell operator '&'" in failure for failure in failures):
        raise AssertionError("backgrounded Zig command was accepted")

    subshell_zig_command = sample_manifest()
    subshell_zig_command["commands"][0]["command"] = (
        f"({subshell_zig_command['commands'][0]['command']})"
    )
    failures = validate_release_evidence(subshell_zig_command)
    if not any("disallowed shell operator '('" in failure for failure in failures):
        raise AssertionError("subshell-wrapped Zig command was accepted")

    substitution_zig_command = sample_manifest()
    substitution_zig_command["commands"][0]["command"] = (
        f"echo $({RELEASE_ZIG} build test --summary all)"
    )
    failures = validate_release_evidence(substitution_zig_command)
    if not any("command substitution" in failure for failure in failures):
        raise AssertionError("command-substitution Zig command was accepted")

    prefixed_zig_segment_command = sample_manifest()
    prefixed_zig_segment_command["commands"][0]["command"] = (
        "true && " + prefixed_zig_segment_command["commands"][0]["command"]
    )
    failures = validate_release_evidence(prefixed_zig_segment_command)
    if not any("unexpected extra shell command segments" in failure for failure in failures):
        raise AssertionError("prefixed Zig command segment was accepted")

    suffixed_zig_segment_command = sample_manifest()
    suffixed_zig_segment_command["commands"][0]["command"] += " && true"
    failures = validate_release_evidence(suffixed_zig_segment_command)
    if not any("unexpected extra shell command segments" in failure for failure in failures):
        raise AssertionError("suffixed Zig command segment was accepted")

    extra_compose_segment = sample_manifest()
    extra_compose_segment["commands"][root_compose_command_index]["command"] += (
        " && true"
    )
    failures = validate_release_evidence(extra_compose_segment)
    if not any("unexpected extra shell command segments" in failure for failure in failures):
        raise AssertionError("extra compose command segment was accepted")

    detached_gate_env = sample_manifest()
    detached_gate_env["commands"][chaos_command_index]["command"] = (
        f"ZMQ_RUN_CHAOS_TESTS=1 true && {RELEASE_ZIG} build test-chaos --summary all"
    )
    failures = validate_release_evidence(detached_gate_env)
    if not any("broker chaos harness" in failure for failure in failures):
        raise AssertionError("detached gate environment satisfied release command matching")

    overwritten_gate_env = sample_manifest()
    overwritten_gate_env["commands"][chaos_command_index]["command"] = (
        f"ZMQ_RUN_CHAOS_TESTS=1 ZMQ_RUN_CHAOS_TESTS=0 "
        f"{RELEASE_ZIG} build test-chaos --summary all"
    )
    failures = validate_release_evidence(overwritten_gate_env)
    if not any("broker chaos harness" in failure for failure in failures):
        raise AssertionError("overwritten gate environment satisfied release command matching")

    duplicate_gate_env = sample_manifest()
    duplicate_gate_env["commands"][chaos_command_index]["command"] = (
        f"ZMQ_RUN_CHAOS_TESTS=0 ZMQ_RUN_CHAOS_TESTS=1 "
        f"{RELEASE_ZIG} build test-chaos --summary all"
    )
    failures = validate_release_evidence(duplicate_gate_env)
    if not any(
        "repeats environment assignment" in failure
        and "ZMQ_RUN_CHAOS_TESTS" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate gate environment assignment was accepted")

    missing_required_gate_manifest = sample_manifest()
    missing_required_gate_manifest["environment"].pop("ZMQ_RUN_CHAOS_TESTS")
    failures = validate_release_evidence(missing_required_gate_manifest)
    if not any(
        "broker chaos harness" in failure
        and "ZMQ_RUN_CHAOS_TESTS" in failure
        and "must record non-empty" in failure
        for failure in failures
    ):
        raise AssertionError("missing required gate manifest assignment was accepted")

    matching_required_gate_manifest = sample_manifest()
    matching_required_gate_manifest["environment"]["ZMQ_RUN_CHAOS_TESTS"] = "1"
    failures = validate_release_evidence(matching_required_gate_manifest)
    if failures:
        raise AssertionError(
            f"matching required gate manifest assignment was rejected: {failures}"
        )

    mismatched_required_gate_manifest = sample_manifest()
    mismatched_required_gate_manifest["environment"]["ZMQ_RUN_CHAOS_TESTS"] = "0"
    failures = validate_release_evidence(mismatched_required_gate_manifest)
    if not any(
        "broker chaos harness" in failure
        and "ZMQ_RUN_CHAOS_TESTS" in failure
        and "manifest environment records" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched required gate manifest assignment was accepted"
        )

    newline_detached_gate_env = sample_manifest()
    newline_detached_gate_env["commands"][chaos_command_index]["command"] = (
        f"ZMQ_RUN_CHAOS_TESTS=1\n{RELEASE_ZIG} build test-chaos --summary all"
    )
    failures = validate_release_evidence(newline_detached_gate_env)
    if not any("line break" in failure for failure in failures):
        raise AssertionError("newline-detached gate environment was accepted")

    quoted_gate_env = sample_manifest()
    quoted_gate_env["commands"][chaos_command_index]["command"] = (
        f"'ZMQ_RUN_CHAOS_TESTS=1' {RELEASE_ZIG} build test-chaos --summary all"
    )
    failures = validate_release_evidence(quoted_gate_env)
    if not any("shell quote character" in failure for failure in failures):
        raise AssertionError("quoted gate environment assignment was accepted")

    escaped_gate_env = sample_manifest()
    escaped_gate_env["commands"][chaos_command_index]["command"] = (
        f"ZMQ_RUN_CHAOS_TESTS\\=1 {RELEASE_ZIG} build test-chaos --summary all"
    )
    failures = validate_release_evidence(escaped_gate_env)
    if not any("shell escape character" in failure for failure in failures):
        raise AssertionError("escaped gate environment assignment was accepted")

    reversed_compose_echo = sample_manifest()
    reversed_compose_echo["commands"][root_compose_command_index]["command"] = (
        "echo ok: root compose config && "
        "docker compose -f docker-compose.yml config --quiet"
    )
    failures = validate_release_evidence(reversed_compose_echo)
    if not any("root compose config validation" in failure for failure in failures):
        raise AssertionError("reversed compose marker satisfied release command matching")

    semicolon_compose_echo = sample_manifest()
    semicolon_compose_echo["commands"][root_compose_command_index]["command"] = (
        "docker compose -f docker-compose.yml config --quiet ; "
        "echo ok: root compose config"
    )
    failures = validate_release_evidence(semicolon_compose_echo)
    if not any("non-success shell separator" in failure for failure in failures):
        raise AssertionError("semicolon compose marker satisfied release command matching")

    missing_env = sample_manifest()
    missing_env["environment"].pop("ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS")
    failures = validate_release_evidence(missing_env)
    if not any("ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS" in failure for failure in failures):
        raise AssertionError("missing required environment variable was not reported")

    missing_chaos_required_scenario = sample_manifest()
    missing_chaos_required_scenario["environment"]["ZMQ_CHAOS_REQUIRED_SCENARIOS"] = (
        "sigkill-restart,slow-partial-client,clock-skewed-records,s3-outage"
    )
    failures = validate_release_evidence(missing_chaos_required_scenario)
    if not any("network-partition" in failure for failure in failures):
        raise AssertionError("missing chaos required scenario was not reported")

    placeholder_env = sample_manifest()
    placeholder_env["environment"]["ZMQ_E2E_REQUIRED_CHAOS_PHASES"] = "required"
    failures = validate_release_evidence(placeholder_env)
    if not any("ZMQ_E2E_REQUIRED_CHAOS_PHASES" in failure for failure in failures):
        raise AssertionError("placeholder environment variable was not reported")

    placeholder_port_env = sample_manifest()
    placeholder_port_env["environment"]["ZMQ_S3_PORT"] = "placeholder"
    failures = validate_release_evidence(placeholder_port_env)
    if not any("ZMQ_S3_PORT" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("placeholder integer environment variable was not reported")

    malformed_port_env = sample_manifest()
    malformed_port_env["environment"]["ZMQ_S3_PORT"] = "not-a-port"
    failures = validate_release_evidence(malformed_port_env)
    if not any("ZMQ_S3_PORT" in failure and "integer" in failure for failure in failures):
        raise AssertionError("malformed integer environment variable was not reported")

    json_port_env = sample_manifest()
    json_port_env["environment"]["ZMQ_S3_PORT"] = 9443
    failures = validate_release_evidence(json_port_env)
    if not any("ZMQ_S3_PORT" in failure and "integer string" in failure for failure in failures):
        raise AssertionError("JSON integer environment variable was accepted")

    placeholder_s3_string_env = sample_manifest()
    placeholder_s3_string_env["environment"]["ZMQ_S3_MINIO_SECRET_KEY"] = "todo"
    failures = validate_release_evidence(placeholder_s3_string_env)
    if not any("ZMQ_S3_MINIO_SECRET_KEY" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("placeholder S3 string provenance was not reported")

    angle_placeholder_s3_string_env = sample_manifest()
    angle_placeholder_s3_string_env["environment"]["ZMQ_S3_ENDPOINT"] = "<host>:9443"
    failures = validate_release_evidence(angle_placeholder_s3_string_env)
    if not any("ZMQ_S3_ENDPOINT" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("angle-bracket placeholder S3 string provenance was not reported")

    json_s3_string_env = sample_manifest()
    json_s3_string_env["environment"]["ZMQ_S3_MINIO_BUCKET"] = True
    failures = validate_release_evidence(json_s3_string_env)
    if not any("ZMQ_S3_MINIO_BUCKET" in failure and "must be a string" in failure for failure in failures):
        raise AssertionError("JSON S3 string provenance was accepted")

    invalid_s3_scheme_env = sample_manifest()
    invalid_s3_scheme_env["environment"]["ZMQ_S3_SCHEME"] = "ftp"
    failures = validate_release_evidence(invalid_s3_scheme_env)
    if not any("ZMQ_S3_SCHEME" in failure and "http or https" in failure for failure in failures):
        raise AssertionError("invalid S3 scheme provenance was accepted")

    negative_phase_index_env = sample_manifest()
    negative_phase_index_env["environment"]["ZMQ_E2E_LOAD_SCALE_PHASE_INDEX"] = "-1"
    failures = validate_release_evidence(negative_phase_index_env)
    if not any("ZMQ_E2E_LOAD_SCALE_PHASE_INDEX" in failure and "non-negative" in failure for failure in failures):
        raise AssertionError("negative phase-index environment variable was accepted")

    placeholder_load_records_env = sample_manifest()
    placeholder_load_records_env["environment"][
        "ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS"
    ] = "placeholder"
    failures = validate_release_evidence(placeholder_load_records_env)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS" in failure
        and "placeholder" in failure
        for failure in failures
    ):
        raise AssertionError("placeholder E2E fixture load records was accepted")

    negative_load_records_env = sample_manifest()
    negative_load_records_env["environment"][
        "ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS"
    ] = "-1"
    failures = validate_release_evidence(negative_load_records_env)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_FIXTURE_LOAD_RECORDS" in failure
        and "positive integer" in failure
        for failure in failures
    ):
        raise AssertionError("negative E2E fixture load records was accepted")

    invalid_fixture_action_env = sample_manifest()
    invalid_fixture_action_env["environment"]["ZMQ_E2E_LOAD_SCALE_FIXTURE_ACTION"] = "resize"
    failures = validate_release_evidence(invalid_fixture_action_env)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_FIXTURE_ACTION" in failure
        and "scale-in" in failure
        for failure in failures
    ):
        raise AssertionError("invalid E2E fixture action was accepted")

    blank_phase_fixture_action_with_global = sample_manifest()
    blank_phase_fixture_action_with_global["environment"][
        "ZMQ_E2E_LOAD_SCALE_FIXTURE_ACTION"
    ] = "load"
    blank_phase_fixture_action_with_global["environment"][
        "ZMQ_E2E_LOAD_SCALE_SCALE_IN_FIXTURE_ACTION"
    ] = ""
    failures = validate_release_evidence(blank_phase_fixture_action_with_global)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_SCALE_IN_FIXTURE_ACTION" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank E2E fixture action used global release-evidence fallback"
        )

    empty_csv_env = sample_manifest()
    empty_csv_env["environment"]["ZMQ_CHAOS_REQUIRED_NETWORK_PHASES"] = ",,,"
    failures = validate_release_evidence(empty_csv_env)
    if not any("at least one comma-separated value" in failure for failure in failures):
        raise AssertionError("empty comma-separated environment variable was not reported")

    duplicate_csv_env = sample_manifest()
    duplicate_csv_env["environment"]["ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"] = (
        "load,scale-in,scale-out,load"
    )
    failures = validate_release_evidence(duplicate_csv_env)
    if not any(
        "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES" in failure
        and "duplicate comma-separated" in failure
        and "load" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comma-separated environment variable was not reported")

    embedded_blank_required_target = sample_manifest()
    embedded_blank_required_target["environment"][
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"
    ] = "zmq,,kafka,automq"
    failures = validate_release_evidence(embedded_blank_required_target)
    if not any(
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS" in failure
        and "blank comma-separated" in failure
        for failure in failures
    ):
        raise AssertionError("embedded blank required target value was not reported")

    missing_s3_selector = sample_manifest()
    missing_s3_selector["environment"].pop("ZMQ_S3_PROVIDER_PROFILES")
    failures = validate_release_evidence(missing_s3_selector)
    if not any("coverage selector ZMQ_S3_PROVIDER_PROFILES" in failure for failure in failures):
        raise AssertionError("missing S3 provider selector provenance was not reported")

    blank_s3_selector = sample_manifest()
    blank_s3_selector["environment"]["ZMQ_S3_PROVIDER_PROFILES"] = "   "
    failures = validate_release_evidence(blank_s3_selector)
    if not any(
        "ZMQ_S3_PROVIDER_PROFILES" in failure and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError("blank S3 provider selector value was not reported")

    embedded_blank_s3_selector = sample_manifest()
    embedded_blank_s3_selector["environment"][
        "ZMQ_S3_PROVIDER_PROFILES"
    ] = "minio,,aws_us_east_1"
    failures = validate_release_evidence(embedded_blank_s3_selector)
    if not any(
        "ZMQ_S3_PROVIDER_PROFILES" in failure
        and "blank comma-separated" in failure
        for failure in failures
    ):
        raise AssertionError("embedded blank S3 provider selector value was not reported")

    blank_fixture_load_scale_selector = sample_manifest()
    blank_fixture_load_scale_selector["environment"][
        "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"
    ] = "1"
    blank_fixture_load_scale_selector["environment"][
        "ZMQ_E2E_LOAD_SCALE_MATRIX"
    ] = "   "
    failures = validate_release_evidence(blank_fixture_load_scale_selector)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_MATRIX" in failure and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError("blank fixture-backed E2E load/scale selector was accepted")

    missing_client_selector_value = sample_manifest()
    missing_client_selector_value["environment"]["ZMQ_CLIENT_MATRIX_PROFILES"] = (
        "kcat_sec,kafka_cli_sec"
    )
    failures = validate_release_evidence(missing_client_selector_value)
    if not any(
        "ZMQ_CLIENT_MATRIX_PROFILES" in failure and "go_1_21" in failure
        for failure in failures
    ):
        raise AssertionError("client profile selector subset mismatch was not reported")

    blank_client_profile_bootstrap_with_global = sample_manifest()
    blank_client_profile_bootstrap_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_BOOTSTRAP"
    ] = ""
    failures = validate_release_evidence(blank_client_profile_bootstrap_with_global)
    if not any(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_BOOTSTRAP" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank client profile bootstrap used global release-evidence fallback"
        )

    missing_client_profile_tools = sample_manifest()
    missing_client_profile_tools["environment"].pop(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS"
    )
    failures = validate_release_evidence(missing_client_profile_tools)
    if not any("client profile setting TOOLS" in failure for failure in failures):
        raise AssertionError("missing client profile tool provenance was not reported")

    blank_client_profile_tools_with_global = sample_manifest()
    blank_client_profile_tools_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_TOOLS"
    ] = "kcat"
    blank_client_profile_tools_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS"
    ] = ""
    failures = validate_release_evidence(blank_client_profile_tools_with_global)
    if not any(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank client profile tools used global release-evidence fallback"
        )

    auto_client_profile_tools = sample_manifest()
    auto_client_profile_tools["environment"]["ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS"] = (
        "auto"
    )
    failures = validate_release_evidence(auto_client_profile_tools)
    if not any("explicitly list selected tools" in failure for failure in failures):
        raise AssertionError("auto client profile tools were accepted")

    duplicate_client_profile_tool_provenance = sample_manifest()
    duplicate_client_profile_tool_provenance["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS"
    ] = "kcat,kcat"
    failures = validate_release_evidence(duplicate_client_profile_tool_provenance)
    if not any(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_TOOLS" in failure
        and "duplicate comma-separated" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate client profile tool provenance was accepted")

    duplicate_client_profile_semantic_provenance = sample_manifest()
    duplicate_client_profile_semantic_provenance["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SEMANTICS"
    ] = "basic,security,security,security-negative"
    failures = validate_release_evidence(duplicate_client_profile_semantic_provenance)
    if not any(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SEMANTICS" in failure
        and "duplicate comma-separated" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate client profile semantic provenance was accepted")

    unsupported_client_profile_semantic = sample_manifest()
    unsupported_client_profile_semantic["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SEMANTICS"
    ] = "basic,rebalance,security,security-negative"
    failures = validate_release_evidence(unsupported_client_profile_semantic)
    if not any("no rebalance probe" in failure for failure in failures):
        raise AssertionError("unsupported client profile semantic was accepted")

    missing_client_profile_semantic_coverage = sample_manifest()
    missing_client_profile_semantic_coverage["environment"][
        "ZMQ_CLIENT_MATRIX_CONFLUENT_2_3_SEMANTICS"
    ] = "basic,admin,groups,security,security-negative"
    missing_client_profile_semantic_coverage["environment"][
        "ZMQ_CLIENT_MATRIX_JAVA_3_7_SEMANTICS"
    ] = "basic,security,security-negative"
    failures = validate_release_evidence(missing_client_profile_semantic_coverage)
    if not any("rebalance" in failure and "transactions" in failure for failure in failures):
        raise AssertionError("missing client profile semantic provenance was not reported")

    missing_client_profile_version = sample_manifest()
    missing_client_profile_version["environment"].pop(
        "ZMQ_CLIENT_MATRIX_GO_1_21_VERSION"
    )
    failures = validate_release_evidence(missing_client_profile_version)
    if not any("client profile setting VERSION" in failure for failure in failures):
        raise AssertionError("missing client profile version provenance was not reported")

    floating_go_module = sample_manifest()
    floating_go_module["environment"]["ZMQ_CLIENT_MATRIX_GO_1_21_GO_MODULE"] = (
        "github.com/segmentio/kafka-go@latest"
    )
    failures = validate_release_evidence(floating_go_module)
    if not any("Go module version" in failure for failure in failures):
        raise AssertionError("floating go-kafka module provenance was not reported")

    implicit_latest_go_module = sample_manifest()
    implicit_latest_go_module["environment"]["ZMQ_CLIENT_MATRIX_GO_1_21_GO_MODULE"] = (
        "github.com/segmentio/kafka-go"
    )
    failures = validate_release_evidence(implicit_latest_go_module)
    if not any("Go module version" in failure for failure in failures):
        raise AssertionError("implicit-latest go-kafka module provenance was not reported")

    missing_client_python = sample_manifest()
    missing_client_python["environment"].pop("ZMQ_CLIENT_MATRIX_PYTHON")
    failures = validate_release_evidence(missing_client_python)
    if not any("PYTHON" in failure for failure in failures):
        raise AssertionError("missing client Python executable provenance was not reported")

    blank_client_python_with_global = sample_manifest()
    blank_client_python_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_PYTHON"
    ] = ""
    failures = validate_release_evidence(blank_client_python_with_global)
    if not any(
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_PYTHON" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank client profile Python used global release-evidence fallback"
        )

    missing_client_security_protocol = sample_manifest()
    missing_client_security_protocol["environment"].pop(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SECURITY_PROTOCOL"
    )
    failures = validate_release_evidence(missing_client_security_protocol)
    if not any("SECURITY_PROTOCOL" in failure for failure in failures):
        raise AssertionError("missing client security protocol provenance was not reported")

    blank_client_security_protocol_with_global = sample_manifest()
    blank_client_security_protocol_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_SECURITY_PROTOCOL"
    ] = "SASL_PLAINTEXT"
    blank_client_security_protocol_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SECURITY_PROTOCOL"
    ] = ""
    failures = validate_release_evidence(blank_client_security_protocol_with_global)
    if not any(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SECURITY_PROTOCOL" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank client security protocol used global release-evidence fallback"
        )

    invalid_client_security_protocol = sample_manifest()
    invalid_client_security_protocol["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SECURITY_PROTOCOL"
    ] = "BROKEN"
    failures = validate_release_evidence(invalid_client_security_protocol)
    if not any("unknown security protocol" in failure for failure in failures):
        raise AssertionError("invalid client security protocol provenance was accepted")

    invalid_client_sasl_mechanism = sample_manifest()
    invalid_client_sasl_mechanism["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SASL_MECHANISM"
    ] = "GSSAPI"
    failures = validate_release_evidence(invalid_client_sasl_mechanism)
    if not any("unknown SASL mechanism" in failure for failure in failures):
        raise AssertionError("invalid client SASL mechanism provenance was accepted")

    oauth_profile_without_security_semantic = sample_manifest()
    oauth_profile_without_security_semantic["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_SEMANTICS"
    ] = "basic,security-negative"
    failures = validate_release_evidence(oauth_profile_without_security_semantic)
    if not any(
        "OAuth profile kcat_sec" in failure and "security semantics" in failure
        for failure in failures
    ):
        raise AssertionError("OAuth profile without security semantic was accepted")

    missing_client_oauth_fixture = sample_manifest()
    missing_client_oauth_fixture["environment"].pop(
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_OAUTH_TOKEN"
    )
    failures = validate_release_evidence(missing_client_oauth_fixture)
    if not any("OAUTH_TOKEN" in failure for failure in failures):
        raise AssertionError("missing client OAuth fixture provenance was not reported")

    blank_client_oauth_fixture_with_global = sample_manifest()
    blank_client_oauth_fixture_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_OAUTH_TOKEN"
    ] = (
        "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6OTk5OTk5OTk5OX0."
    )
    blank_client_oauth_fixture_with_global["environment"][
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_OAUTH_TOKEN"
    ] = ""
    failures = validate_release_evidence(blank_client_oauth_fixture_with_global)
    if not any(
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_OAUTH_TOKEN" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank client OAuth fixture used global release-evidence fallback"
        )

    malformed_client_oauth_fixture = sample_manifest()
    malformed_client_oauth_fixture["environment"][
        "ZMQ_CLIENT_MATRIX_KAFKA_PYTHON_SEC_OAUTH_TOKEN"
    ] = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJub2V4cCJ9."
    failures = validate_release_evidence(malformed_client_oauth_fixture)
    if not any("positive OAuth fixture" in failure for failure in failures):
        raise AssertionError("malformed client OAuth fixture was accepted")

    missing_client_oauth_negative_fixture = sample_manifest()
    missing_client_oauth_negative_fixture["environment"].pop(
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_BAD_OAUTHBEARER_CONFIG"
    )
    failures = validate_release_evidence(missing_client_oauth_negative_fixture)
    if not any("BAD_OAUTHBEARER_CONFIG" in failure for failure in failures):
        raise AssertionError(
            "missing client OAuth-negative fixture provenance was not reported"
        )

    valid_client_oauth_negative_fixture = sample_manifest()
    valid_client_oauth_negative_fixture["environment"][
        "ZMQ_CLIENT_MATRIX_KCAT_SEC_BAD_OAUTHBEARER_CONFIG"
    ] = "principal=matrix-user lifeSeconds=3600"
    failures = validate_release_evidence(valid_client_oauth_negative_fixture)
    if not any("negative OAuth fixture" in failure for failure in failures):
        raise AssertionError("future-valid client OAuth-negative fixture was accepted")

    colliding_kraft_selector = sample_manifest()
    colliding_kraft_selector["environment"]["ZMQ_KRAFT_NETWORK_MATRIX"] = (
        "leader-isolation,leader_isolation,broker-link"
    )
    failures = validate_release_evidence(colliding_kraft_selector)
    if not any("same environment-variable token" in failure for failure in failures):
        raise AssertionError("colliding KRaft selector phase tokens were not reported")

    blank_chaos_expectation_with_global = sample_manifest()
    blank_chaos_expectation_with_global["environment"][
        "ZMQ_CHAOS_NETWORK_EXPECT"
    ] = "fail"
    blank_chaos_expectation_with_global["environment"][
        "ZMQ_CHAOS_NETWORK_BROKER_LINK_EXPECT"
    ] = ""
    failures = validate_release_evidence(blank_chaos_expectation_with_global)
    if not any(
        "ZMQ_CHAOS_NETWORK_BROKER_LINK_EXPECT" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank chaos network expectation used global release-evidence fallback"
        )

    blank_kraft_expectation_with_global = sample_manifest()
    blank_kraft_expectation_with_global["environment"][
        "ZMQ_KRAFT_NETWORK_EXPECT"
    ] = "fail"
    blank_kraft_expectation_with_global["environment"][
        "ZMQ_KRAFT_NETWORK_LEADER_ISOLATION_EXPECT"
    ] = ""
    failures = validate_release_evidence(blank_kraft_expectation_with_global)
    if not any(
        "ZMQ_KRAFT_NETWORK_LEADER_ISOLATION_EXPECT" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank KRaft network expectation used global release-evidence fallback"
        )

    blank_e2e_expectation_with_global = sample_manifest()
    blank_e2e_expectation_with_global["environment"][
        "ZMQ_E2E_CHAOS_EXPECT"
    ] = "fail"
    blank_e2e_expectation_with_global["environment"][
        "ZMQ_E2E_CHAOS_CROSS_BROKER_EXPECT"
    ] = ""
    failures = validate_release_evidence(blank_e2e_expectation_with_global)
    if not any(
        "ZMQ_E2E_CHAOS_CROSS_BROKER_EXPECT" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank E2E chaos expectation used global release-evidence fallback"
        )

    missing_chaos_hook = sample_manifest()
    missing_chaos_hook["environment"].pop("ZMQ_CHAOS_NETWORK_BROKER_LINK_DOWN")
    failures = validate_release_evidence(missing_chaos_hook)
    if not any("missing hook command" in failure and "broker-link" in failure for failure in failures):
        raise AssertionError("missing chaos network hook provenance was not reported")

    blank_chaos_hook_with_global = sample_manifest()
    blank_chaos_hook_with_global["environment"]["ZMQ_CHAOS_NETWORK_DOWN"] = "true"
    blank_chaos_hook_with_global["environment"][
        "ZMQ_CHAOS_NETWORK_BROKER_LINK_DOWN"
    ] = ""
    failures = validate_release_evidence(blank_chaos_hook_with_global)
    if not any(
        "ZMQ_CHAOS_NETWORK_BROKER_LINK_DOWN" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank chaos network release-evidence hook used global fallback"
        )

    malformed_kraft_hook = sample_manifest()
    malformed_kraft_hook["environment"]["ZMQ_KRAFT_NETWORK_DOWN"] = "'unterminated"
    failures = validate_release_evidence(malformed_kraft_hook)
    if not any("hook command ZMQ_KRAFT_NETWORK_DOWN" in failure and "malformed" in failure for failure in failures):
        raise AssertionError("malformed KRaft hook command was not reported")

    blank_kraft_hook_with_global = sample_manifest()
    blank_kraft_hook_with_global["environment"][
        "ZMQ_KRAFT_NETWORK_LEADER_ISOLATION_DOWN"
    ] = ""
    failures = validate_release_evidence(blank_kraft_hook_with_global)
    if not any(
        "ZMQ_KRAFT_NETWORK_LEADER_ISOLATION_DOWN" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank KRaft network release-evidence hook used global fallback"
        )

    blank_e2e_chaos_hook_with_global = sample_manifest()
    blank_e2e_chaos_hook_with_global["environment"]["ZMQ_E2E_CHAOS_DOWN"] = "true"
    blank_e2e_chaos_hook_with_global["environment"][
        "ZMQ_E2E_CHAOS_CROSS_BROKER_DOWN"
    ] = ""
    failures = validate_release_evidence(blank_e2e_chaos_hook_with_global)
    if not any(
        "ZMQ_E2E_CHAOS_CROSS_BROKER_DOWN" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank E2E chaos release-evidence hook used global fallback"
        )

    blank_e2e_load_scale_hook_with_global = sample_manifest()
    blank_e2e_load_scale_hook_with_global["environment"][
        "ZMQ_E2E_LOAD_SCALE_SCALE_IN_APPLY"
    ] = ""
    failures = validate_release_evidence(blank_e2e_load_scale_hook_with_global)
    if not any(
        "ZMQ_E2E_LOAD_SCALE_SCALE_IN_APPLY" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank E2E load-scale release-evidence hook used global fallback"
        )

    placeholder_s3_fault_hook = sample_manifest()
    placeholder_s3_fault_hook["environment"]["ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"] = (
        "/path/to/fault-script"
    )
    failures = validate_release_evidence(placeholder_s3_fault_hook)
    if not any("MULTIPART_FAULT_CMD" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("placeholder S3 multipart-fault hook was not reported")

    missing_s3_multipart_fault_hook = sample_manifest()
    missing_s3_multipart_fault_hook["environment"].pop("ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD")
    missing_s3_multipart_fault_hook["environment"].pop("ZMQ_S3_MULTIPART_FAULT_CMD", None)
    failures = validate_release_evidence(missing_s3_multipart_fault_hook)
    if not any(
        "missing hook command" in failure
        and "S3 multipart-fault profile aws_us_east_1" in failure
        and "MULTIPART_FAULT_CMD" in failure
        for failure in failures
    ):
        raise AssertionError("missing S3 multipart-fault hook provenance was not reported")

    blank_s3_multipart_fault_hook_with_global = sample_manifest()
    blank_s3_multipart_fault_hook_with_global["environment"][
        "ZMQ_S3_MULTIPART_FAULT_CMD"
    ] = "ci-s3-multipart-fault fallback"
    blank_s3_multipart_fault_hook_with_global["environment"][
        "ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD"
    ] = ""
    failures = validate_release_evidence(blank_s3_multipart_fault_hook_with_global)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_MULTIPART_FAULT_CMD" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError(
            "blank S3 multipart-fault release-evidence hook used global fallback"
        )

    missing_s3_outage_hook = sample_manifest()
    missing_s3_outage_hook["environment"].pop("ZMQ_S3_AWS_US_EAST_1_OUTAGE_DOWN")
    missing_s3_outage_hook["environment"].pop("ZMQ_S3_OUTAGE_DOWN", None)
    failures = validate_release_evidence(missing_s3_outage_hook)
    if not any(
        "missing hook command" in failure
        and "S3 outage profile aws_us_east_1" in failure
        and "OUTAGE_DOWN" in failure
        for failure in failures
    ):
        raise AssertionError("missing S3 outage hook provenance was not reported")

    placeholder_s3_outage_hook = sample_manifest()
    placeholder_s3_outage_hook["environment"]["ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"] = (
        "/path/to/outage-heal"
    )
    failures = validate_release_evidence(placeholder_s3_outage_hook)
    if not any("OUTAGE_UP" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("placeholder S3 outage hook was not reported")

    blank_s3_outage_hook_with_global = sample_manifest()
    blank_s3_outage_hook_with_global["environment"]["ZMQ_S3_OUTAGE_UP"] = "true"
    blank_s3_outage_hook_with_global["environment"][
        "ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP"
    ] = ""
    failures = validate_release_evidence(blank_s3_outage_hook_with_global)
    if not any(
        "ZMQ_S3_AWS_US_EAST_1_OUTAGE_UP" in failure
        and "must not be blank" in failure
        for failure in failures
    ):
        raise AssertionError("blank S3 outage release-evidence hook used global fallback")

    for env_name, global_env_name, expected_fragment, assertion_message in (
        (
            "ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE",
            "ZMQ_S3_RUN_LIVE_OUTAGE",
            "RUN_LIVE_OUTAGE=1",
            "missing S3 outage enable provenance was not reported",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_RUN_PROCESS_CRASH",
            "ZMQ_S3_RUN_PROCESS_CRASH",
            "RUN_PROCESS_CRASH=1",
            "missing S3 process-crash enable provenance was not reported",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_REQUIRE_LIST_PAGINATION",
            "ZMQ_S3_REQUIRE_LIST_PAGINATION",
            "REQUIRE_LIST_PAGINATION=1",
            "missing S3 list-pagination enable provenance was not reported",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_REQUIRE_MULTIPART_EDGE",
            "ZMQ_S3_REQUIRE_MULTIPART_EDGE",
            "REQUIRE_MULTIPART_EDGE=1",
            "missing S3 multipart-edge enable provenance was not reported",
        ),
        (
            "ZMQ_S3_AWS_US_EAST_1_RUN_MULTIPART_FAULT",
            "ZMQ_S3_RUN_MULTIPART_FAULT",
            "RUN_MULTIPART_FAULT=1",
            "missing S3 multipart-fault enable provenance was not reported",
        ),
    ):
        missing_s3_profile_toggle = sample_manifest()
        missing_s3_profile_toggle["environment"].pop(env_name)
        missing_s3_profile_toggle["environment"].pop(global_env_name, None)
        failures = validate_release_evidence(missing_s3_profile_toggle)
        if not any(expected_fragment in failure for failure in failures):
            raise AssertionError(assertion_message)

    disabled_s3_outage_toggle = sample_manifest()
    disabled_s3_outage_toggle["environment"]["ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE"] = "0"
    failures = validate_release_evidence(disabled_s3_outage_toggle)
    if not any("RUN_LIVE_OUTAGE" in failure and "must be truthy" in failure for failure in failures):
        raise AssertionError("disabled S3 outage enable provenance was not reported")

    invalid_s3_outage_toggle = sample_manifest()
    invalid_s3_outage_toggle["environment"]["ZMQ_S3_AWS_US_EAST_1_RUN_LIVE_OUTAGE"] = "maybe"
    failures = validate_release_evidence(invalid_s3_outage_toggle)
    if not any("RUN_LIVE_OUTAGE" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid S3 outage enable provenance was not reported")

    fixture_backed_load_scale = sample_manifest()
    fixture_backed_load_scale["environment"].pop("ZMQ_E2E_LOAD_SCALE_MATRIX")
    fixture_backed_load_scale["environment"].pop("ZMQ_E2E_LOAD_SCALE_APPLY")
    fixture_backed_load_scale["environment"].pop("ZMQ_E2E_LOAD_SCALE_RESTORE")
    fixture_backed_load_scale["environment"]["ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"] = "1"
    fixture_backed_load_scale["commands"][e2e_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[e2e_command_index],
            fixture_backed_load_scale["environment"],
        )
    )
    fixture_backed_load_scale["commands"][e2e_command_index]["output"] = (
        fixture_backed_load_scale["commands"][e2e_command_index]["output"]
        .replace(
            "ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
            "ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=fixture restore_source=fixture source=command action=load load_records=30",
        )
        .replace(
            "ok: E2E load/scale phase scale-in applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
            "ok: E2E load/scale phase scale-in applied=true restored=true marker_payloads=hook-owned apply_source=fixture restore_source=fixture source=command action=scale-in",
        )
        .replace(
            "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
            "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=fixture restore_source=fixture source=command action=scale-out",
        )
    )
    failures = validate_release_evidence(fixture_backed_load_scale)
    if failures:
        raise AssertionError(f"fixture-backed E2E load/scale selector was rejected: {failures}")

    fixture_missing_command_flag = sample_manifest()
    fixture_missing_command_flag["environment"].pop("ZMQ_E2E_LOAD_SCALE_MATRIX")
    fixture_missing_command_flag["environment"].pop("ZMQ_E2E_LOAD_SCALE_APPLY")
    fixture_missing_command_flag["environment"].pop("ZMQ_E2E_LOAD_SCALE_RESTORE")
    fixture_missing_command_flag["environment"]["ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"] = "1"
    fixture_missing_command_flag["commands"][e2e_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[e2e_command_index],
            fixture_missing_command_flag["environment"],
        ).replace("ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1 ", "")
    )
    failures = validate_release_evidence(fixture_missing_command_flag)
    if not any(
        "Docker E2E gate" in failure
        and "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE" in failure
        for failure in failures
    ):
        raise AssertionError("fixture-backed E2E command without fixture flag was accepted")

    fixture_missing_action_marker = sample_manifest()
    fixture_missing_action_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_MATRIX")
    fixture_missing_action_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_APPLY")
    fixture_missing_action_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_RESTORE")
    fixture_missing_action_marker["environment"]["ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"] = "1"
    fixture_missing_action_marker["commands"][e2e_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[e2e_command_index],
            fixture_missing_action_marker["environment"],
        )
    )
    fixture_missing_action_marker["commands"][e2e_command_index]["output"] = (
        fixture_missing_action_marker["commands"][e2e_command_index]["output"].replace(
            "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
            "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=fixture restore_source=fixture source=command",
        )
    )
    failures = validate_release_evidence(fixture_missing_action_marker)
    if not any("fixture action=scale-out" in failure for failure in failures):
        raise AssertionError("fixture-backed E2E load/scale marker without action was accepted")

    fixture_missing_load_records_marker = sample_manifest()
    fixture_missing_load_records_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_MATRIX")
    fixture_missing_load_records_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_APPLY")
    fixture_missing_load_records_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_RESTORE")
    fixture_missing_load_records_marker["environment"][
        "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"
    ] = "1"
    fixture_missing_load_records_marker["commands"][e2e_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[e2e_command_index],
            fixture_missing_load_records_marker["environment"],
        )
    )
    fixture_missing_load_records_marker["commands"][e2e_command_index]["output"] = (
        fixture_missing_load_records_marker["commands"][e2e_command_index][
            "output"
        ].replace(
            "ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
            "ok: E2E load/scale phase load applied=true restored=true marker_payloads=hook-owned apply_source=fixture restore_source=fixture source=command action=load",
        )
    )
    failures = validate_release_evidence(fixture_missing_load_records_marker)
    if not any("load_records=30" in failure for failure in failures):
        raise AssertionError(
            "fixture-backed E2E load marker without load_records was accepted"
        )

    fixture_unexpected_load_records_marker = sample_manifest()
    fixture_unexpected_load_records_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_MATRIX")
    fixture_unexpected_load_records_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_APPLY")
    fixture_unexpected_load_records_marker["environment"].pop("ZMQ_E2E_LOAD_SCALE_RESTORE")
    fixture_unexpected_load_records_marker["environment"][
        "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"
    ] = "1"
    fixture_unexpected_load_records_marker["commands"][e2e_command_index][
        "command"
    ] = sample_requirement_command(
        REQUIRED_COMMANDS[e2e_command_index],
        fixture_unexpected_load_records_marker["environment"],
    )
    fixture_unexpected_load_records_marker["commands"][e2e_command_index][
        "output"
    ] = fixture_unexpected_load_records_marker["commands"][e2e_command_index][
        "output"
    ].replace(
        "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=hook restore_source=hook source=command",
        "ok: E2E load/scale phase scale-out applied=true restored=true marker_payloads=hook-owned apply_source=fixture restore_source=fixture source=command action=scale-out load_records=30",
    )
    failures = validate_release_evidence(fixture_unexpected_load_records_marker)
    if not any("only report load_records" in failure for failure in failures):
        raise AssertionError(
            "fixture-backed E2E non-load marker with load_records was accepted"
        )

    invalid_fixture_enable = sample_manifest()
    invalid_fixture_enable["environment"]["ZMQ_E2E_LOAD_SCALE_USE_FIXTURE"] = "maybe"
    failures = validate_release_evidence(invalid_fixture_enable)
    if not any("ZMQ_E2E_LOAD_SCALE_USE_FIXTURE" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid E2E fixture enable provenance was not reported")

    placeholder_run_gate = sample_manifest()
    placeholder_run_gate["environment"]["ZMQ_RUN_CLIENT_MATRIX"] = "placeholder"
    failures = validate_release_evidence(placeholder_run_gate)
    if not any("ZMQ_RUN_CLIENT_MATRIX" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("placeholder top-level run gate provenance was not reported")

    blank_run_gate = sample_manifest()
    blank_run_gate["environment"]["ZMQ_RUN_CHAOS_TESTS"] = "   "
    failures = validate_release_evidence(blank_run_gate)
    if not any("ZMQ_RUN_CHAOS_TESTS" in failure and "must not be blank" in failure for failure in failures):
        raise AssertionError("blank top-level run gate provenance was not reported")

    invalid_bench_compare_run_gate = sample_manifest()
    invalid_bench_compare_run_gate["environment"]["ZMQ_RUN_BENCH_COMPARE"] = "maybe"
    failures = validate_release_evidence(invalid_bench_compare_run_gate)
    if not any("ZMQ_RUN_BENCH_COMPARE" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid comparative benchmark run gate provenance was not reported")

    blank_bench_enforce_gate = sample_manifest()
    blank_bench_enforce_gate["environment"]["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "   "
    failures = validate_release_evidence(blank_bench_enforce_gate)
    if not any("ZMQ_BENCH_COMPARE_ENFORCE_GATES" in failure and "must not be blank" in failure for failure in failures):
        raise AssertionError("blank benchmark enforce-gates provenance was not reported")

    invalid_bench_enforce_gate = sample_manifest()
    invalid_bench_enforce_gate["environment"]["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "maybe"
    failures = validate_release_evidence(invalid_bench_enforce_gate)
    if not any("ZMQ_BENCH_COMPARE_ENFORCE_GATES" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid benchmark enforce-gates provenance was not reported")

    enforced_compare_gate = sample_manifest()
    enforced_compare_gate["environment"]["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "1"
    enforced_compare_gate["commands"][comparative_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[comparative_command_index],
            enforced_compare_gate["environment"],
        )
    )
    failures = validate_release_evidence(enforced_compare_gate)
    if failures:
        raise AssertionError(
            "comparative benchmark enforce-gates command provenance was rejected: "
            + "; ".join(failures)
        )

    missing_enforce_gate_command = sample_manifest()
    missing_enforce_gate_command["environment"]["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "1"
    failures = validate_release_evidence(missing_enforce_gate_command)
    if not any(
        "comparative benchmark gate" in failure
        and "ZMQ_BENCH_COMPARE_ENFORCE_GATES" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing comparative benchmark enforce-gates command assignment was accepted"
        )

    mismatched_enforce_gate_command = sample_manifest()
    mismatched_enforce_gate_command["environment"]["ZMQ_BENCH_COMPARE_ENFORCE_GATES"] = "1"
    mismatched_enforce_gate_command["commands"][comparative_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[comparative_command_index],
            mismatched_enforce_gate_command["environment"],
        ).replace(
            "ZMQ_BENCH_COMPARE_ENFORCE_GATES=1",
            "ZMQ_BENCH_COMPARE_ENFORCE_GATES=0",
        )
    )
    failures = validate_release_evidence(mismatched_enforce_gate_command)
    if not any(
        "ZMQ_BENCH_COMPARE_ENFORCE_GATES" in failure
        and "manifest environment records" in failure
        for failure in failures
    ):
        raise AssertionError(
            "mismatched comparative benchmark enforce-gates command assignment was accepted"
        )

    json_bool_run_gate = sample_manifest()
    json_bool_run_gate["environment"]["ZMQ_RUN_E2E_TESTS"] = True
    failures = validate_release_evidence(json_bool_run_gate)
    if not any("ZMQ_RUN_E2E_TESTS" in failure and "boolean string" in failure for failure in failures):
        raise AssertionError("JSON boolean environment run gate was accepted")

    placeholder_client_enable_go = sample_manifest()
    placeholder_client_enable_go["environment"]["ZMQ_CLIENT_MATRIX_ENABLE_GO"] = "placeholder"
    failures = validate_release_evidence(placeholder_client_enable_go)
    if not any("ZMQ_CLIENT_MATRIX_ENABLE_GO" in failure and "placeholder" in failure for failure in failures):
        raise AssertionError("placeholder client enable-go provenance was not reported")

    invalid_profile_enable_go = sample_manifest()
    invalid_profile_enable_go["environment"]["ZMQ_CLIENT_MATRIX_GO_1_21_ENABLE_GO"] = "maybe"
    failures = validate_release_evidence(invalid_profile_enable_go)
    if not any("ZMQ_CLIENT_MATRIX_GO_1_21_ENABLE_GO" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid profile enable-go provenance was not reported")

    placeholder_command = sample_manifest()
    placeholder_command["commands"][-1]["command"] += " /path/to/prior-results.json"
    failures = validate_release_evidence(placeholder_command)
    if not any("placeholder value" in failure for failure in failures):
        raise AssertionError("placeholder command path was not reported")

    angle_placeholder_command = sample_manifest()
    angle_placeholder_command["commands"][-1]["command"] = (
        "ZMQ_EXTRA=<value> " + angle_placeholder_command["commands"][-1]["command"]
    )
    failures = validate_release_evidence(angle_placeholder_command)
    if not any("placeholder value" in failure for failure in failures):
        raise AssertionError("angle-bracket placeholder command value was not reported")

    malformed_integer_command = sample_manifest()
    malformed_integer_command["commands"][live_s3_benchmark_command_index][
        "command"
    ] = malformed_integer_command["commands"][live_s3_benchmark_command_index][
        "command"
    ].replace("ZMQ_S3_PORT=9443", "ZMQ_S3_PORT=not-a-port")
    failures = validate_release_evidence(malformed_integer_command)
    if not any("ZMQ_S3_PORT" in failure and "integer" in failure for failure in failures):
        raise AssertionError("malformed integer command assignment was accepted")

    missing_compare_targets_assignment = sample_manifest()
    missing_compare_targets_assignment["commands"][-1]["command"] = (
        missing_compare_targets_assignment["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS=zmq,kafka,automq ",
            "",
        )
    )
    failures = validate_release_evidence(missing_compare_targets_assignment)
    if not any("non-empty ZMQ_BENCH_COMPARE_REQUIRED_TARGETS=" in failure for failure in failures):
        raise AssertionError("missing comparative target command assignment was not reported")

    mismatched_compare_targets_assignment = sample_manifest()
    mismatched_compare_targets_assignment["commands"][-1]["command"] = (
        mismatched_compare_targets_assignment["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS=zmq,kafka,automq",
            "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS=zmq,kafka",
        )
    )
    failures = validate_release_evidence(mismatched_compare_targets_assignment)
    if not any(
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS" in failure
        and "manifest environment records" in failure
        for failure in failures
    ):
        raise AssertionError("mismatched comparative target command assignment was not reported")

    missing_baseline_assignment = sample_manifest()
    missing_baseline_assignment["commands"][-1]["command"] = (
        missing_baseline_assignment["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=",
        )
    )
    failures = validate_release_evidence(missing_baseline_assignment)
    if not any("non-empty ZMQ_BENCH_COMPARE_TREND_BASELINE=" in failure for failure in failures):
        raise AssertionError("missing trend baseline command assignment was not reported")

    detached_baseline_assignment = sample_manifest()
    original_compare_command = detached_baseline_assignment["commands"][-1]["command"]
    detached_baseline_assignment["commands"][-1]["command"] = (
        "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json true && "
        + original_compare_command.replace(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json ",
            "",
        )
    )
    failures = validate_release_evidence(detached_baseline_assignment)
    if not any(
        "unexpected extra shell command segments" in failure
        or "comparative benchmark gate" in failure
        for failure in failures
    ):
        raise AssertionError("detached trend baseline command assignment was not reported")

    mismatched_baseline_assignment = sample_manifest()
    mismatched_baseline_assignment["commands"][-1]["command"] = (
        mismatched_baseline_assignment["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/other-results.json",
        )
    )
    failures = validate_release_evidence(mismatched_baseline_assignment)
    if not any("manifest environment records" in failure for failure in failures):
        raise AssertionError("mismatched trend baseline command assignment was not reported")

    overwritten_baseline_assignment = sample_manifest()
    overwritten_baseline_assignment["commands"][-1]["command"] = (
        overwritten_baseline_assignment["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json",
            (
                "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json "
                "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/other-results.json"
            ),
        )
    )
    failures = validate_release_evidence(overwritten_baseline_assignment)
    if not any(
        "repeats environment assignment" in failure
        and "ZMQ_BENCH_COMPARE_TREND_BASELINE" in failure
        for failure in failures
    ):
        raise AssertionError("overwritten trend baseline command assignment was not reported")

    current_results_baseline = sample_manifest()
    current_results_baseline["environment"]["ZMQ_BENCH_COMPARE_TREND_BASELINE"] = (
        "benchmarks/results.json"
    )
    current_results_baseline["commands"][-1]["command"] = (
        current_results_baseline["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results.json",
        )
    )
    failures = validate_release_evidence(current_results_baseline)
    if not any("prior benchmark artifact" in failure for failure in failures):
        raise AssertionError("current results artifact was accepted as trend baseline")

    current_results_baseline_command = sample_manifest()
    current_results_baseline_command["commands"][-1]["command"] = (
        current_results_baseline_command["commands"][-1]["command"].replace(
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=benchmarks/results-previous.json",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE=./benchmarks/results.json",
        )
    )
    failures = validate_release_evidence(current_results_baseline_command)
    if not any("prior benchmark artifact" in failure for failure in failures):
        raise AssertionError("current results artifact command assignment was accepted")

    custom_threshold_evidence = sample_manifest()
    custom_threshold_evidence["environment"][
        "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"
    ] = "0.10"
    custom_threshold_evidence["commands"][comparative_command_index]["command"] = (
        sample_requirement_command(
            REQUIRED_COMMANDS[comparative_command_index],
            custom_threshold_evidence["environment"],
        )
    )
    default_threshold_line = (
        "thresholds: throughput_ratio>=0.05x, p50_ratio<=20.00x, "
        "p99_ratio<=20.00x, error_rate<=0.00%"
    )
    custom_threshold_line = (
        "thresholds: throughput_ratio>=0.10x, p50_ratio<=20.00x, "
        "p99_ratio<=20.00x, error_rate<=0.00%"
    )
    custom_threshold_evidence["commands"][comparative_command_index]["output"] = (
        custom_threshold_evidence["commands"][comparative_command_index]["output"].replace(
            default_threshold_line,
            custom_threshold_line,
        )
    )
    failures = validate_release_evidence(custom_threshold_evidence)
    if failures:
        raise AssertionError(
            f"custom comparative benchmark threshold evidence was rejected: {failures}"
        )

    missing_threshold_command = sample_manifest()
    missing_threshold_command["environment"][
        "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"
    ] = "0.10"
    missing_threshold_command["commands"][comparative_command_index]["output"] = (
        missing_threshold_command["commands"][comparative_command_index]["output"].replace(
            default_threshold_line,
            custom_threshold_line,
        )
    )
    failures = validate_release_evidence(missing_threshold_command)
    if not any(
        "comparative benchmark gate" in failure
        and "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO" in failure
        for failure in failures
    ):
        raise AssertionError(
            "missing comparative threshold command assignment was accepted"
        )

    for env_name, env_value, assertion_message in (
        (
            "ZMQ_BENCH_COMPARE_MAX_P50_LATENCY_RATIO",
            "4.0",
            "missing comparative max-p50 threshold command assignment was accepted",
        ),
        (
            "ZMQ_BENCH_COMPARE_MAX_P99_LATENCY_RATIO",
            "8.0",
            "missing comparative max-p99 threshold command assignment was accepted",
        ),
        (
            "ZMQ_BENCH_COMPARE_MAX_ERROR_RATE",
            "0.01",
            "missing comparative max-error-rate threshold command assignment was accepted",
        ),
        (
            "ZMQ_BENCH_COMPARE_MIN_TREND_THROUGHPUT_RATIO",
            "0.80",
            "missing comparative trend-throughput threshold command assignment was accepted",
        ),
        (
            "ZMQ_BENCH_COMPARE_MAX_TREND_P50_LATENCY_RATIO",
            "1.50",
            "missing comparative trend-p50 threshold command assignment was accepted",
        ),
        (
            "ZMQ_BENCH_COMPARE_MAX_TREND_P99_LATENCY_RATIO",
            "1.75",
            "missing comparative trend-p99 threshold command assignment was accepted",
        ),
    ):
        missing_compare_threshold_command = sample_manifest()
        missing_compare_threshold_command["environment"][env_name] = env_value
        failures = validate_release_evidence(missing_compare_threshold_command)
        if not any(
            "comparative benchmark gate" in failure and env_name in failure
            for failure in failures
        ):
            raise AssertionError(assertion_message)

    nonfinite_threshold_env = sample_manifest()
    nonfinite_threshold_env["environment"]["ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO"] = "nan"
    failures = validate_release_evidence(nonfinite_threshold_env)
    if not any(
        "ZMQ_BENCH_COMPARE_MIN_THROUGHPUT_RATIO" in failure
        and "finite non-negative float" in failure
        for failure in failures
    ):
        raise AssertionError("non-finite benchmark threshold environment was not reported")

    negative_threshold_command = sample_manifest()
    negative_threshold_command["commands"][-1]["command"] = (
        "ZMQ_BENCH_COMPARE_MAX_ERROR_RATE=-1 "
        + negative_threshold_command["commands"][-1]["command"]
    )
    failures = validate_release_evidence(negative_threshold_command)
    if not any(
        "ZMQ_BENCH_COMPARE_MAX_ERROR_RATE" in failure
        and "finite non-negative float" in failure
        for failure in failures
    ):
        raise AssertionError("negative benchmark threshold command assignment was accepted")

    disabled_trend = sample_manifest()
    disabled_trend["environment"]["ZMQ_BENCH_COMPARE_REQUIRE_TREND"] = "0"
    failures = validate_release_evidence(disabled_trend)
    if not any("ZMQ_BENCH_COMPARE_REQUIRE_TREND=1" in failure for failure in failures):
        raise AssertionError("disabled trend requirement was not reported")

    invalid_trend = sample_manifest()
    invalid_trend["environment"]["ZMQ_BENCH_COMPARE_REQUIRE_TREND"] = "maybe"
    failures = validate_release_evidence(invalid_trend)
    if not any("ZMQ_BENCH_COMPARE_REQUIRE_TREND" in failure and "true or false" in failure for failure in failures):
        raise AssertionError("invalid trend requirement was not reported")

    missing_zmq_target = sample_manifest()
    missing_zmq_target["environment"]["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "kafka"
    failures = validate_release_evidence(missing_zmq_target)
    if not any("targets must include zmq" in failure for failure in failures):
        raise AssertionError("missing ZMQ comparative target was not reported")

    missing_baseline_target = sample_manifest()
    missing_baseline_target["environment"]["ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"] = "zmq"
    failures = validate_release_evidence(missing_baseline_target)
    if not any("targets must include kafka or automq" in failure for failure in failures):
        raise AssertionError("missing comparative baseline target was not reported")

    uppercase_comparative_target = sample_manifest()
    uppercase_comparative_target["environment"][
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"
    ] = "ZMQ,kafka"
    failures = validate_release_evidence(uppercase_comparative_target)
    if not any(
        "unknown comparative benchmark required targets: ZMQ" in failure
        for failure in failures
    ):
        raise AssertionError("uppercase comparative target was accepted")

    duplicate_comparative_required_target = sample_manifest()
    duplicate_comparative_required_target["environment"][
        "ZMQ_BENCH_COMPARE_REQUIRED_TARGETS"
    ] = "zmq,kafka,kafka"
    failures = validate_release_evidence(duplicate_comparative_required_target)
    if not any(
        "comparative benchmark required targets" in failure
        and "duplicate values: kafka" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate comparative benchmark required target was accepted")

    missing_provider = sample_manifest()
    missing_provider["environment"]["ZMQ_S3_PROVIDER_REQUIRED_PROFILES"] = "minio"
    failures = validate_release_evidence(missing_provider)
    if not any(
        "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES" in failure
        for failure in failures
    ):
        raise AssertionError("S3 profile coverage mismatch was not reported")

    missing_client_profile = sample_manifest()
    missing_client_profile["environment"]["ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES"] = (
        "kcat_sec"
    )
    failures = validate_release_evidence(missing_client_profile)
    if not any(
        "ZMQ_CLIENT_MATRIX_REQUIRED_VERSIONED_PROFILES" in failure
        for failure in failures
    ):
        raise AssertionError("client profile coverage mismatch was not reported")

    missing_client_tool = sample_manifest()
    missing_client_tool["environment"]["ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS"] = (
        "kcat,kafka-cli,kafka-python,confluent-kafka,java-kafka"
    )
    failures = validate_release_evidence(missing_client_tool)
    if not any("go-kafka" in failure for failure in failures):
        raise AssertionError("missing required client tool was not reported")

    unknown_client_tool = sample_manifest()
    unknown_client_tool["environment"]["ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS"] += ",custom-client"
    failures = validate_release_evidence(unknown_client_tool)
    if not any("unknown client matrix required tools" in failure for failure in failures):
        raise AssertionError("unknown required client tool was not reported")

    missing_client_semantic = sample_manifest()
    missing_client_semantic["environment"]["ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS"] = (
        "basic,admin,groups,rebalance,transactions,security"
    )
    failures = validate_release_evidence(missing_client_semantic)
    if not any("security-negative" in failure for failure in failures):
        raise AssertionError("missing required client semantic was not reported")

    unknown_client_semantic = sample_manifest()
    unknown_client_semantic["environment"]["ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS"] += (
        ",custom-semantic"
    )
    failures = validate_release_evidence(unknown_client_semantic)
    if not any("unknown client matrix required semantics" in failure for failure in failures):
        raise AssertionError("unknown required client semantic was not reported")

    missing_load_scale = sample_manifest()
    missing_load_scale["environment"]["ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES"] = "load"
    failures = validate_release_evidence(missing_load_scale)
    if not any("scale-in" in failure and "scale-out" in failure for failure in failures):
        raise AssertionError("missing E2E load/scale phases were not reported")

    missing_cross_broker = sample_manifest()
    missing_cross_broker["environment"]["ZMQ_E2E_REQUIRED_CHAOS_PHASES"] = "rack"
    failures = validate_release_evidence(missing_cross_broker)
    if not any("cross-broker coverage" in failure for failure in failures):
        raise AssertionError("missing E2E cross-broker chaos coverage was not reported")

    no_output = sample_manifest()
    no_output["commands"][0]["output"] = ""
    failures = validate_release_evidence(no_output)
    if not any("missing captured output" in failure for failure in failures):
        raise AssertionError("missing command output was not reported")

    skipped_output = sample_manifest()
    chaos_command_index = next(
        index
        for index, requirement in enumerate(REQUIRED_COMMANDS)
        if requirement["label"] == "broker chaos harness"
    )
    skipped_output["commands"][chaos_command_index]["output"] = (
        "Build Summary: 6/6 steps succeeded\n"
        "test-chaos success\n"
        "skip: set ZMQ_RUN_CHAOS_TESTS=1 to run broker chaos harness"
    )
    failures = validate_release_evidence(skipped_output)
    if not any(
        "captured skip output for broker chaos harness" in failure
        for failure in failures
    ):
        raise AssertionError("captured skip output was not reported")

    missing_unsupported_surfaces = sample_manifest()
    missing_unsupported_surfaces.pop("unsupported_or_partial_surfaces")
    failures = validate_release_evidence(missing_unsupported_surfaces)
    if not any("must include unsupported_or_partial_surfaces list" in failure for failure in failures):
        raise AssertionError("missing unsupported surfaces list was accepted")

    non_list_unsupported_surfaces = sample_manifest()
    non_list_unsupported_surfaces["unsupported_or_partial_surfaces"] = {}
    failures = validate_release_evidence(non_list_unsupported_surfaces)
    if not any("must include unsupported_or_partial_surfaces list" in failure for failure in failures):
        raise AssertionError("non-list unsupported surfaces accounting was accepted")

    for required_surface in REQUIRED_UNSUPPORTED_SURFACES:
        missing_surface = sample_manifest()
        missing_surface["unsupported_or_partial_surfaces"] = [
            entry
            for entry in missing_surface["unsupported_or_partial_surfaces"]
            if not unsupported_surface_entry_matches(
                unsupported_surface_text(entry),
                required_surface,
            )
        ]
        failures = validate_release_evidence(missing_surface)
        if not any(required_surface["label"] in failure for failure in failures):
            raise AssertionError("missing unsupported surface accounting was not reported")

    catch_all_surface = sample_manifest()
    catch_all_surface["unsupported_or_partial_surfaces"] = [
        {
            "surface": (
                "ZooKeeper-era inter-broker API keys 4-7; "
                "broker-only stateless replacement; CI execution release blockers"
            ),
            "status": "partial and release-CI-required",
            "evidence": (
                "broker and controller ApiVersions neither port generated-only "
                "direct broker/controller probes fail closed local cache/state "
                "assumptions S3/quorum replay paths cross-broker chaos "
                "E2E load/scale KRaft failover live "
                "provider outage comparative Kafka/AutoMQ performance "
                "profile/trend gates"
            ),
        }
    ]
    failures = validate_release_evidence(catch_all_surface)
    if not any("broker-only stateless replacement" in failure for failure in failures):
        raise AssertionError("catch-all unsupported surface accounting was accepted")

    duplicate_surface = sample_manifest()
    duplicate_surface["unsupported_or_partial_surfaces"].append(
        dict(duplicate_surface["unsupported_or_partial_surfaces"][1])
    )
    failures = validate_release_evidence(duplicate_surface)
    if not any(
        "duplicate unsupported/partial surface accounting" in failure
        and "broker-only stateless replacement" in failure
        for failure in failures
    ):
        raise AssertionError("duplicate unsupported surface accounting was accepted")

    extra_surface = sample_manifest()
    extra_surface["unsupported_or_partial_surfaces"].append({
        "surface": "undocumented release blocker",
        "status": "blocked",
        "evidence": "extra release blocker not tracked by the verifier catalog",
    })
    failures = validate_release_evidence(extra_surface)
    if not any("outside the verifier catalog" in failure for failure in failures):
        raise AssertionError("extra unsupported surface accounting was accepted")

    malformed_surface = sample_manifest()
    malformed_surface["unsupported_or_partial_surfaces"][0] = 42
    failures = validate_release_evidence(malformed_surface)
    if not any("entry 0 must be an object" in failure for failure in failures):
        raise AssertionError("malformed unsupported surface entry was not reported")

    string_surface = sample_manifest()
    string_surface["unsupported_or_partial_surfaces"][0] = (
        "ZooKeeper-era inter-broker API keys 4-7 broker and controller ApiVersions "
        "neither port generated-only fail closed"
    )
    failures = validate_release_evidence(string_surface)
    if not any("entry 0 must be an object" in failure for failure in failures):
        raise AssertionError("string unsupported surface entry was not rejected")

    missing_surface_evidence = sample_manifest()
    missing_surface_evidence["unsupported_or_partial_surfaces"][0].pop("evidence")
    failures = validate_release_evidence(missing_surface_evidence)
    if not any("missing required field evidence" in failure for failure in failures):
        raise AssertionError("missing unsupported surface evidence was not reported")

    unexpected_surface_field = sample_manifest()
    unexpected_surface_field["unsupported_or_partial_surfaces"][0]["complete"] = False
    failures = validate_release_evidence(unexpected_surface_field)
    if not any(
        "unsupported_or_partial_surfaces entry 0 contains unexpected field 'complete'"
        in failure
        for failure in failures
    ):
        raise AssertionError("unknown unsupported surface field was accepted")

    vague_surface_name = sample_manifest()
    vague_surface_name["unsupported_or_partial_surfaces"][1]["surface"] = (
        "stateless replacement notes"
    )
    vague_surface_name["unsupported_or_partial_surfaces"][1]["evidence"] = (
        "broker-only stateless replacement local cache/state assumptions "
        "remain outside the covered S3/quorum replay paths"
    )
    failures = validate_release_evidence(vague_surface_name)
    if not any(
        "surface field for broker-only stateless replacement" in failure
        and "must name the known surface" in failure
        for failure in failures
    ):
        raise AssertionError(
            "unsupported surface evidence was allowed to hide a vague surface name"
        )

    placeholder_surface_notes = sample_manifest()
    placeholder_surface_notes["unsupported_or_partial_surfaces"][0]["notes"] = "TODO"
    failures = validate_release_evidence(placeholder_surface_notes)
    if not any("optional field notes uses placeholder value" in failure for failure in failures):
        raise AssertionError("placeholder unsupported surface notes were accepted")

    blank_surface_mitigation = sample_manifest()
    blank_surface_mitigation["unsupported_or_partial_surfaces"][0]["mitigation"] = "   "
    failures = validate_release_evidence(blank_surface_mitigation)
    if not any("optional field mitigation must not be blank" in failure for failure in failures):
        raise AssertionError("blank unsupported surface mitigation was accepted")

    empty_surface_notes = sample_manifest()
    empty_surface_notes["unsupported_or_partial_surfaces"][0]["notes"] = []
    failures = validate_release_evidence(empty_surface_notes)
    if not any("optional field notes must not be an empty list" in failure for failure in failures):
        raise AssertionError("empty unsupported surface notes list was accepted")

    placeholder_surface_note_item = sample_manifest()
    placeholder_surface_note_item["unsupported_or_partial_surfaces"][0]["notes"] = [
        "covered by <release-ci-job>",
    ]
    failures = validate_release_evidence(placeholder_surface_note_item)
    if not any("optional field notes item 0 uses placeholder value" in failure for failure in failures):
        raise AssertionError("placeholder unsupported surface note item was accepted")

    non_string_surface_mitigation = sample_manifest()
    non_string_surface_mitigation["unsupported_or_partial_surfaces"][0][
        "mitigation"
    ] = 42
    failures = validate_release_evidence(non_string_surface_mitigation)
    if not any("optional field mitigation must be a string or list of strings" in failure for failure in failures):
        raise AssertionError("non-string unsupported surface mitigation was accepted")

    non_string_surface_note = sample_manifest()
    non_string_surface_note["unsupported_or_partial_surfaces"][0]["notes"] = [None]
    failures = validate_release_evidence(non_string_surface_note)
    if not any("optional field notes item 0 must be a non-empty string" in failure for failure in failures):
        raise AssertionError("non-string unsupported surface note was accepted")

    placeholder_surface_status = sample_manifest()
    placeholder_surface_status["unsupported_or_partial_surfaces"][0]["status"] = "TBD"
    failures = validate_release_evidence(placeholder_surface_status)
    if not any("placeholder status" in failure for failure in failures):
        raise AssertionError("placeholder unsupported surface status was not reported")

    vague_surface_status = sample_manifest()
    vague_surface_status["unsupported_or_partial_surfaces"][0]["status"] = "covered"
    failures = validate_release_evidence(vague_surface_status)
    if not any("status must explicitly mark" in failure for failure in failures):
        raise AssertionError("vague unsupported surface status was accepted")

    release_ci_surface_status = sample_manifest()
    release_ci_surface_status["unsupported_or_partial_surfaces"][2]["status"] = (
        "release-CI-required coverage"
    )
    failures = validate_release_evidence(release_ci_surface_status)
    if failures:
        raise AssertionError(
            f"documented release-CI-required surface status was rejected: {failures}"
        )

    misclassified_stateless_surface_status = sample_manifest()
    misclassified_stateless_surface_status["unsupported_or_partial_surfaces"][1][
        "status"
    ] = "not advertised"
    failures = validate_release_evidence(misclassified_stateless_surface_status)
    if not any(
        "broker-only stateless replacement" in failure
        and "partial or blocked" in failure
        for failure in failures
    ):
        raise AssertionError(
            "misclassified broker-only stateless surface status was accepted"
        )

    misclassified_ci_surface_status = sample_manifest()
    misclassified_ci_surface_status["unsupported_or_partial_surfaces"][2][
        "status"
    ] = "fail-closed/not-advertised"
    failures = validate_release_evidence(misclassified_ci_surface_status)
    if not any(
        "external client/security/OAuth live matrix" in failure
        and "release-CI-required or blocked" in failure
        for failure in failures
    ):
        raise AssertionError("misclassified live CI surface status was accepted")

    bad_release = sample_manifest()
    bad_release["automq_complete"] = True
    failures = validate_release_evidence(bad_release)
    if not any("unsupported or partial surfaces" in failure for failure in failures):
        raise AssertionError("unsupported AutoMQ-complete evidence was not rejected")
    if not any("verifier catalog" in failure for failure in failures):
        raise AssertionError("catalog-blocked AutoMQ-complete evidence was not rejected")

    elided_complete_surfaces = sample_manifest()
    elided_complete_surfaces["automq_complete"] = True
    elided_complete_surfaces["unsupported_or_partial_surfaces"] = []
    failures = validate_release_evidence(elided_complete_surfaces)
    if not any("verifier catalog" in failure for failure in failures):
        raise AssertionError(
            "AutoMQ-complete evidence with elided unsupported surfaces was accepted"
        )

    invalid_complete_flag = sample_manifest()
    invalid_complete_flag["automq_complete"] = "true"
    failures = validate_release_evidence(invalid_complete_flag)
    if not any("automq_complete" in failure and "JSON boolean" in failure for failure in failures):
        raise AssertionError("string AutoMQ-complete flag was accepted")

    missing_complete_flag = sample_manifest()
    missing_complete_flag.pop("automq_complete")
    failures = validate_release_evidence(missing_complete_flag)
    if not any("missing automq_complete=false" in failure for failure in failures):
        raise AssertionError("missing AutoMQ-complete flag was accepted")

    missing_blocking_flag = sample_manifest()
    missing_blocking_flag.pop("advertised_stub_api")
    failures = validate_release_evidence(missing_blocking_flag)
    if not any("missing blocking flag advertised_stub_api" in failure for failure in failures):
        raise AssertionError("missing blocking release flag was accepted")

    string_blocking_flag = sample_manifest()
    string_blocking_flag["known_data_loss_bug"] = "false"
    failures = validate_release_evidence(string_blocking_flag)
    if not any("known_data_loss_bug" in failure and "JSON boolean" in failure for failure in failures):
        raise AssertionError("string blocking release flag was accepted")

    bad_release = sample_manifest()
    bad_release["known_data_loss_bug"] = True
    failures = validate_release_evidence(bad_release)
    if not any("known_data_loss_bug" in failure for failure in failures):
        raise AssertionError("blocking data-loss flag was not rejected")

    bad_release = sample_manifest()
    bad_release["advertised_stub_api"] = True
    failures = validate_release_evidence(bad_release)
    if not any("advertised_stub_api" in failure for failure in failures):
        raise AssertionError("blocking advertised stub API flag was not rejected")

    bad_release = sample_manifest()
    bad_release["untriaged_durability_failure"] = True
    failures = validate_release_evidence(bad_release)
    if not any("untriaged_durability_failure" in failure for failure in failures):
        raise AssertionError("blocking untriaged durability flag was not rejected")

    try:
        load_release_evidence_manifest("/path/to/release-evidence.json")
        raise AssertionError("placeholder release evidence manifest path was accepted")
    except ValueError as exc:
        if "placeholder path" not in str(exc):
            raise

    print("ok: release evidence self-test")
    return 0


def main():
    if "--self-test" in sys.argv:
        return self_test()

    manifest_path = os.environ.get("ZMQ_RELEASE_EVIDENCE", "").strip()
    if not manifest_path:
        print("skip: set ZMQ_RELEASE_EVIDENCE to validate release evidence manifest")
        return 0

    try:
        manifest = load_release_evidence_manifest(manifest_path)
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    failures = validate_release_evidence_for_checkout(
        manifest,
        current_commit=current_git_commit(),
        tracked_worktree_dirty=tracked_worktree_dirty(),
    )
    if failures:
        print("FAIL: release evidence is incomplete", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(f"ok: release evidence validated {manifest_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Static build wiring audit.

This verifies that Python-backed release gates keep their deterministic
``--self-test`` commands wired into both the named build step and the default
``zig build test`` graph. It catches build.zig drift without needing Zig 0.16
test compilation.

Run:
    python3 tests/build_static_audit.py --self-test
"""

import ast
import math
import os
import re
import sys


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_ZIG = os.path.join(PROJECT_DIR, "build.zig")
MAKEFILE = os.path.join(PROJECT_DIR, "Makefile")
DOCKERFILE = os.path.join(PROJECT_DIR, "Dockerfile")
ROOT_COMPOSE = os.path.join(PROJECT_DIR, "docker-compose.yml")
KAFKA_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "kafka-compose.yml")
AUTOMQ_COMPOSE = os.path.join(PROJECT_DIR, "benchmarks", "automq-compose.yml")
README = os.path.join(PROJECT_DIR, "README.md")
RELEASE_CRITERIA = os.path.join(PROJECT_DIR, "docs", "RELEASE_CRITERIA.md")
AUTOMQ_PARITY = os.path.join(PROJECT_DIR, "docs", "AUTOMQ_PARITY.md")
BENCHMARK_MAIN = os.path.join(PROJECT_DIR, "benchmarks", "main.zig")
BENCHMARK_COMPARE = os.path.join(PROJECT_DIR, "benchmarks", "benchmark_compare.py")
PROTOCOL_STATIC_AUDIT = os.path.join(PROJECT_DIR, "tests", "protocol_static_audit.py")
OBSERVABILITY_STATIC_AUDIT = os.path.join(
    PROJECT_DIR, "tests", "observability_static_audit.py"
)
PRODUCTION_READINESS_TEST = os.path.join(
    PROJECT_DIR, "tests", "production_readiness_test.zig"
)
S3_PROCESS_CRASH_TEST = os.path.join(
    PROJECT_DIR, "tests", "s3_process_crash_test.py"
)
S3_PROVIDER_MATRIX_TEST = os.path.join(
    PROJECT_DIR, "tests", "s3_provider_matrix_test.py"
)
CLIENT_MATRIX_TEST = os.path.join(PROJECT_DIR, "tests", "client_matrix_test.py")
E2E_TEST = os.path.join(PROJECT_DIR, "tests", "e2e_test.py")
CHAOS_TEST = os.path.join(PROJECT_DIR, "tests", "chaos_test.py")
KRAFT_FAILOVER_TEST = os.path.join(PROJECT_DIR, "tests", "kraft_failover_test.py")
MINIO_S3_TEST = os.path.join(PROJECT_DIR, "tests", "minio_s3_test.zig")
CONFIG_ZIG = os.path.join(PROJECT_DIR, "src", "config.zig")
MAIN_ZIG = os.path.join(PROJECT_DIR, "src", "main.zig")
BROKER_HANDLER_ZIG = os.path.join(PROJECT_DIR, "src", "broker", "handler.zig")
REQUIRED_ZIG_VERSION = "0.16.0"
REQUIRED_ZIG_PATH = "/tmp/zig-aarch64-linux-0.16.0/zig"
REQUIRED_KAFKA_IMAGE = "apache/kafka:4.0.2"
REQUIRED_AUTOMQ_IMAGE = "automqinc/automq:1.6.5"
REQUIRED_MINIO_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
REQUIRED_MINIO_MC_IMAGE = "minio/mc:RELEASE.2025-08-13T08-35-41Z"
STALE_README_COMPOSE_FRAGMENTS = (
    "docker-compose up",
    "docker-compose ps",
    "docker-compose logs",
    "docker-compose down",
    "zmq-broker-1",
    "localhost:9092 -t test-topic",
    "localhost:9090/health",
)

PYTHON_SELF_TEST_GATES = (
    ("test-release-evidence", "tests/release_evidence_test.py"),
    ("test-protocol-static-audit", "tests/protocol_static_audit.py"),
    ("test-observability-static-audit", "tests/observability_static_audit.py"),
    ("test-build-static-audit", "tests/build_static_audit.py"),
    ("test-s3-process-crash", "tests/s3_process_crash_test.py"),
    ("test-s3-provider-matrix", "tests/s3_provider_matrix_test.py"),
    ("test-client-matrix", "tests/client_matrix_test.py"),
    ("test-kraft-failover", "tests/kraft_failover_test.py"),
    ("test-chaos", "tests/chaos_test.py"),
    ("test-e2e", "tests/e2e_test.py"),
    ("bench-compare", "benchmarks/benchmark_compare.py"),
)

PYTHON_RUNTIME_GATES = (
    ("test-release-evidence", "tests/release_evidence_test.py"),
    ("test-s3-process-crash", "tests/s3_process_crash_test.py"),
    ("test-s3-provider-matrix", "tests/s3_provider_matrix_test.py"),
    ("test-client-matrix", "tests/client_matrix_test.py"),
    ("test-kraft-failover", "tests/kraft_failover_test.py"),
    ("test-chaos", "tests/chaos_test.py"),
    ("test-e2e", "tests/e2e_test.py"),
    ("bench-compare", "benchmarks/benchmark_compare.py"),
)

INSTALL_DEPENDENT_RUNTIME_GATES = {
    "test-s3-process-crash",
    "test-s3-provider-matrix",
    "test-client-matrix",
    "test-kraft-failover",
    "test-chaos",
}

MAKE_STATIC_AUDIT_STEPS = (
    "test-protocol-static-audit",
    "test-observability-static-audit",
    "test-build-static-audit",
    "test-release-evidence",
)

MONOTONIC_PYTHON_HARNESSES = tuple(path for _, path in PYTHON_RUNTIME_GATES) + (
    "tests/cluster_validation_test.py",
    "benchmarks/run_bench.py",
)

LIVE_HOOK_PREFLIGHT_CONTRACTS = (
    (
        "tests/chaos_test.py",
        (
            "def validate_phase_tokens_unique(",
            "map to the same environment token",
            "def placeholder_env_value(",
            "def require_non_placeholder_setting(",
            "def setting_with_fallback(",
            "if name in os.environ:",
            "if fallback_name in os.environ:",
            "def live_s3_required_setting(",
            "def hook_command_words(",
            "def preflight_selected_live_hooks(",
            "words = shlex.split(raw)",
            "hook command must contain at least one word",
            "except OSError as exc:",
            "hook command could not start",
            "colliding chaos network phase names were accepted",
            "placeholder chaos network phase was accepted",
            "placeholder chaos network expectation was accepted",
            "invalid chaos network expectation was accepted",
            "blank chaos hook command was accepted",
            "blank phase-specific chaos network hook preflight was accepted",
            "malformed chaos hook command was accepted",
            "placeholder chaos hook command was accepted",
            "unstartable chaos hook command was accepted",
            "malformed chaos network hook preflight was accepted",
            "placeholder chaos live S3 hook preflight was accepted",
            "angle-bracket placeholder live S3 endpoint was accepted",
            "blank chaos scenario selector was accepted",
            "embedded blank chaos scenario selector was accepted",
            "blank global chaos network hook did not select all scenario",
            "blank global chaos network hook preflight was skipped",
            "blank global chaos live-S3 hook did not select all scenario",
            "blank global chaos live-S3 hook preflight was skipped",
            "blank chaos network phase list was accepted",
            "embedded blank chaos network phase list was accepted",
            "duplicate chaos network phase was accepted",
            "blank required chaos scenario list was accepted",
            "embedded blank required chaos scenario list was accepted",
            "blank required chaos network phase list was accepted",
            "embedded blank required chaos network phase list was accepted",
            "placeholder required chaos scenario was accepted",
            "placeholder required chaos network phase was accepted",
            "placeholder chaos scenario selector was accepted",
            "placeholder configured chaos broker port was accepted",
            "malformed configured chaos broker port was accepted",
            "non-positive configured chaos broker port was accepted",
            "blank chaos live S3 endpoint override was accepted",
            "blank chaos live S3 TLS CA override was accepted",
            "placeholder live S3 endpoint was accepted",
            "missing live S3 port was accepted",
            "non-positive live S3 port was accepted",
            "missing live S3 bucket was accepted",
            "missing live S3 scheme was accepted",
            "invalid live S3 path-style was accepted",
            "truthy live S3 path-style flag did not parse",
            "false live S3 path-style flag did not parse",
            "live S3 provider summary did not match selected config",
            "source=command",
            "ok: chaos live-s3-outage provider endpoint=",
            "def network_partition_phase_marker(",
            "ok: chaos network-partition phase {phase} down=true ",
            "observed={observed} healed=true recovered=true expect={expect}",
            "chaos network failed phase marker formatting failed",
            "chaos network survived phase marker formatting failed",
            "ok: chaos network-partition source=command",
            "ok: chaos harness passed for {', '.join(scenarios)} source=command",
        ),
    ),
    (
        "tests/s3_provider_matrix_test.py",
        (
            "def run_command_string(",
            "words = shlex.split(command or \"\")",
            "command must contain at least one word",
            "except OSError as exc:",
            "command could not start",
            "def validate_profile_tokens_unique(",
            "map to the same environment token",
            "def parse_configured_names(",
            "def reject_placeholder_list_values(",
            "EXPLICIT_PROVIDER_SETTING_SUFFIXES",
            "def require_explicit_provider_settings(",
            "colliding S3 provider profile names were accepted",
            "blank S3 provider profile list was accepted",
            "embedded blank S3 provider profile was accepted",
            "duplicate S3 provider profile was accepted",
            "placeholder S3 provider profile was accepted",
            "angle-bracket placeholder S3 provider profile was accepted",
            "blank {env_name} list was accepted",
            "embedded blank {env_name} list was accepted",
            "duplicate {env_name} list was accepted",
            "placeholder required S3 provider profile was accepted",
            "missing explicit non-minio S3 provider settings was accepted",
            "global-only S3 provider endpoint fallback failed",
            "blank multipart-fault command was accepted",
            "malformed multipart-fault command was accepted",
            "unstartable multipart-fault command was accepted",
            "def validate_provider_env(",
            "def require_non_placeholder_setting(",
            "placeholder S3 provider endpoint was accepted",
            "angle-bracket placeholder S3 provider endpoint was accepted",
            "non-positive S3 provider port was accepted",
            "placeholder S3 provider region was accepted",
            "blank S3 provider TLS CA was accepted",
            "placeholder S3 provider outage hook was accepted",
            "placeholder required outage hooks did not fail validation",
            "missing required outage hooks did not fail validation",
            "placeholder multipart-fault command was accepted",
            "missing required multipart-fault command did not fail validation",
            "def strict_bool_text(",
            "def profile_bool_setting(",
            "invalid S3 provider skip-ensure-bucket flag was accepted",
            "invalid S3 provider skip-minio-health flag was accepted",
            "placeholder S3 provider pagination gate flag was accepted",
            "invalid S3 provider multipart-edge gate flag was accepted",
            "invalid S3 provider live-outage gate flag was accepted",
            "invalid S3 provider multipart-fault gate flag was accepted",
            "invalid S3 provider process-crash gate flag was accepted",
            "def provider_summary_settings(",
            "endpoint={env['ZMQ_S3_ENDPOINT']}:{env['ZMQ_S3_PORT']} ",
            "bucket={env['ZMQ_S3_BUCKET']} scheme={scheme} region={region} ",
            "path_style={path_style} source=command",
            "def process_crash_summary_fields(",
            "PROCESS_CRASH_SUMMARY_FIELDS",
            "def process_crash_summary_int(",
            "def require_process_crash_evidence(",
            "def process_crash_detail_marker(",
            "recovered_payloads={fields['recovered_payloads']} source=command",
            "profile {profile} process-crash output missing detailed evidence",
            "process-crash output bucket must match ",
            "selected provider bucket",
            "committed_offset=1",
            "bare process-crash output evidence was accepted",
            "process-crash output without source=command was accepted",
            "process-crash output with wrapper source was accepted",
            "duplicate process-crash output field was accepted",
            "unknown process-crash output field was accepted",
            "mismatched process-crash bucket was accepted",
            "stale process-crash replacement offset was accepted",
            "wrong process-crash committed offset was accepted",
            "wrong process-crash recovered payload count was accepted",
            "process-crash detail marker did not match validated fields",
            "def require_outage_evidence(",
            "def outage_provider_evidence_marker(",
            "def outage_detail_marker(",
            "path_style={path_style} source=command",
            "fail_closed=true recovered=true source=command",
            "selected provider summary line",
            "profile {profile} live outage output missing evidence",
            "profile {profile} live outage output missing provider evidence",
            "outage output without provider evidence was accepted",
            "mismatched outage provider evidence was accepted",
            "bare outage output evidence was accepted",
            "outage detail marker did not match selected provider",
            "ok: S3 provider live-suite profile {profile} command_started=true completed=true source=command",
            "ok: S3 provider outage detail profile {profile}",
            "ok: S3 provider process-crash detail profile {profile}",
            "ok: S3 provider process-crash profile {profile} killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
            "ok: S3 provider outage profile {profile} down=true healed=true fail_closed=true recovered=true source=command",
            "ok: S3 provider list-pagination profile {profile} required=true completed=true source=command",
            "ok: S3 provider multipart-edge profile {profile} required=true completed=true source=command",
            "ok: S3 multipart fault profile {profile}",
            "source=command",
            "return marker",
            "fault_marker = require_multipart_fault_evidence(",
            "ok: S3 provider multipart-fault profile {profile} command_started=true completed=true injected=true recovered=true source=command",
            "ok: S3 provider matrix passed for {', '.join(profiles)} source=command",
            "global S3 provider endpoint fallback failed",
            "global S3 provider region fallback failed",
            "global S3 provider scheme fallback failed",
            "global S3 provider path-style fallback failed",
            "blank S3 provider endpoint used global fallback",
            "blank S3 provider scheme used global fallback",
            "blank S3 provider outage enable used global fallback",
            "blank S3 provider outage hook used global fallback",
            "blank S3 provider multipart-fault enable used global fallback",
            "blank S3 provider multipart-fault command used global fallback",
            "profile endpoint override failed",
            "profile region override failed",
            "profile scheme override failed",
            "profile path-style override failed",
            "invalid S3 provider scheme was accepted",
            "invalid S3 provider path-style was accepted",
        ),
    ),
    (
        "tests/s3_process_crash_test.py",
        (
            "def validate_s3_config(",
            "def validate_process_ports(",
            "def require_non_placeholder_setting(",
            "def require_bool_setting(",
            "def require_bool_text_setting(",
            "ZMQ_S3_SCHEME must be http or https",
            "S3_PATH_STYLE = require_bool_text_setting(\"ZMQ_S3_PATH_STYLE\", S3_PATH_STYLE)",
            "placeholder S3 process-crash endpoint was accepted",
            "angle-bracket placeholder S3 process-crash endpoint was accepted",
            "blank S3 process-crash endpoint was accepted",
            "blank S3 process-crash port was accepted",
            "non-positive S3 process-crash port was accepted",
            "placeholder S3 process-crash broker port was accepted",
            "malformed S3 process-crash broker port was accepted",
            "non-positive S3 process-crash broker port was accepted",
            "placeholder S3 process-crash bucket was accepted",
            "blank S3 process-crash access key was accepted",
            "blank S3 process-crash secret key was accepted",
            "blank S3 process-crash region was accepted",
            "blank S3 process-crash TLS CA file was accepted",
            "invalid S3 process-crash scheme was accepted",
            "invalid S3 process-crash path-style was accepted",
            "truthy S3 process-crash path-style flag did not parse",
            "false S3 process-crash path-style flag did not parse",
            "placeholder S3 process-crash skip-health flag was accepted",
            "invalid S3 process-crash skip-health flag was accepted",
            "ok: S3 process crash/replacement harness passed ",
            "source=command",
        ),
    ),
    (
        "tests/client_matrix_test.py",
        (
            "def validate_profile_tokens_unique(",
            "map to the same environment token",
            "def parse_configured_names(",
            "def reject_placeholder_list_values(",
            "def profile_setting_source(",
            "def profile_bool_setting(",
            "ZMQ_CLIENT_MATRIX_GO_1_21_BOOTSTRAP",
            "go profile bootstrap override failed",
            "def validate_client_security_settings(",
            "def validate_client_profile_runtime_settings(",
            "def validate_required_profile_provenance(",
            "def validate_required_security_profile_context(",
            "def validate_required_profile_subset(",
            "def client_security_detail_line(",
            "def client_profile_pass_marker(",
            "for tool in tools:",
            "ok: client security detail profile {ACTIVE_PROFILE}",
            "source=command",
            "probes ({semantics_csv()}) source=command",
            "ok: client matrix profile {ACTIVE_PROFILE} passed for",
            "f\"{', '.join(tools)} against {BOOTSTRAP}{version_suffix} \"",
            "ok: client matrix passed for {', '.join(profiles)} profile(s) source=command",
            "colliding client matrix profile names were accepted",
            "blank client matrix profile list was accepted",
            "embedded blank client matrix profile was accepted",
            "duplicate client matrix profile was accepted",
            "placeholder client matrix profile was accepted",
            "angle-bracket placeholder client matrix profile was accepted",
            "blank {env_name} client matrix list was accepted",
            "embedded blank {env_name} client matrix list was accepted",
            "blank required client profile bootstrap used global fallback",
            "blank required client profile tools used global fallback",
            "blank required client security protocol used global fallback",
            "blank profile client Python used global fallback",
            "blank profile client OAuth token used global fallback",
            "placeholder required client profile was accepted",
            "def reject_nonstandard_json_constant(",
            "def reject_duplicate_json_object_keys(",
            "parse_constant=reject_nonstandard_json_constant",
            "object_pairs_hook=reject_duplicate_json_object_keys",
            "placeholder client matrix enable-go flag was accepted",
            "invalid client matrix enable-go flag was accepted",
            "placeholder client matrix profile enable-go flag was accepted",
            "invalid client matrix profile enable-go flag was accepted",
            "auto required client tools were accepted",
            "duplicate required client tool list was accepted",
            "embedded blank required client profile tools were accepted",
            "duplicate required client profile tools were accepted",
            "duplicate required client profile semantics were accepted",
            "missing required client bootstrap provenance was accepted",
            "missing required client semantics provenance was accepted",
            "embedded blank client semantic was accepted",
            "duplicate client semantic was accepted",
            "embedded blank client tool was accepted",
            "duplicate client tool list was accepted",
            "placeholder required client version was accepted",
            "plaintext required secured-client profile was accepted",
            "required client sub-profile outside required profile set was accepted",
            "required {label} profiles must set SECURITY_PROTOCOL provenance",
            "invalid client security protocol was accepted",
            "invalid client SASL mechanism was accepted",
            "client security detail marker self-test failed",
            "placeholder client bootstrap was accepted",
            "angle-bracket placeholder client bootstrap was accepted",
            "non-numeric client bootstrap port was accepted",
            "zero client bootstrap port was accepted",
            "Java OAuth positive fixture self-test failed",
            "Java OAuth negative fixture self-test failed",
            "Java OAuth environment self-test failed",
            "future-valid Java bad OAuth JAAS fixture was accepted as negative",
            "missing-exp Java OAuth JAAS fixture was accepted as positive",
            "Kafka CLI OAuth security config self-test failed",
            "kcat OAuth positive config self-test failed",
            "kcat OAuth config with unsupported principal claim was accepted",
            "kcat unsupported-principal-claim config was not accepted as negative",
            "future-valid kcat bad OAuth config was accepted as negative",
            "OAuth profile missing a selected-tool fixture was accepted",
            "OAuth profile without security semantic was accepted",
            "OAuth-negative profile missing a kcat negative fixture was accepted",
            "future-valid kcat bad OAuth config was accepted",
            "non-OAuth required client profile was accepted",
            "floating go-kafka module was accepted",
            "placeholder client Python executable was accepted",
            "non-standard JSON OAuth token was accepted",
            "duplicate-key OAuth token was accepted",
            "kafka-python OAuth token provider self-test failed",
            "kafka-python OAuth negative vector self-test failed",
            "kafka-python missing-exp OAuth negative vector self-test failed",
            "future-valid bad OAuth token was accepted as a negative vector",
            "missing-exp OAuth token was accepted as a positive fixture",
        ),
    ),
    (
        "tests/kraft_failover_test.py",
        (
            "def validate_network_phase_tokens_unique(",
            "map to the same environment token",
            "def placeholder_env_value(",
            "def validate_port_config(",
            "LEGACY_INTER_BROKER_API_VERSIONS = {",
            "controller unsupported cases missing",
            "def api_case_summary(",
            "def controller_unsupported_summary(",
            "def broker_non_broker_api_rejection_cases(",
            "def broker_non_broker_api_rejection_summary(",
            "controller_unsupported_cases={controller_unsupported_summary(controller_unsupported_cases())}, ",
            "broker_non_broker_api_rejection_cases={broker_non_broker_api_rejection_summary()}, ",
            "def hook_command_words(",
            "words = shlex.split(stripped)",
            "command must contain at least one word",
            "except OSError as exc:",
            "command could not start",
            "def network_partition_summary(",
            "network_partition={network_partition_summary(network_partition_result)}, ",
            "reassignment_topic={automq_result['reassignment_topic']}, ",
            "reassignment_old_owner_rejected=true, ",
            "reassignment_target_fetch_verified=true, ",
            "def network_partition_phase_marker(",
            "ok: KRaft network partition phase {result['phase']} down=true ",
            "observed={observed} healed={healed} ",
            "healed_leader={result['leader_id']} healed_fetch={healed_fetch} ",
            "expect={result['expect']} source=command",
            "colliding KRaft network phase names were accepted",
            "blank KRaft network phase list was accepted",
            "embedded blank KRaft network phase list was accepted",
            "duplicate KRaft network phase was accepted",
            "placeholder KRaft network phase was accepted",
            "placeholder KRaft network expectation was accepted",
            "blank required KRaft network phase list was accepted",
            "embedded blank required KRaft network phase list was accepted",
            "placeholder required KRaft network phase was accepted",
            "placeholder KRaft controller port base was accepted",
            "angle-bracket placeholder KRaft controller port base was accepted",
            "malformed KRaft controller port base was accepted",
            "non-positive KRaft controller port base was accepted",
            "blank KRaft network hook command was accepted",
            "blank global KRaft network hook was accepted",
            "blank phase-specific KRaft network hook was accepted",
            "malformed KRaft network hook command was accepted",
            "placeholder KRaft network hook command was accepted",
            "unstartable KRaft network hook command was accepted",
        ),
    ),
    (
        "tests/e2e_test.py",
        (
            "def validate_phase_tokens_unique(",
            "map to the same environment token",
            "def placeholder_env_value(",
            "def strict_bool_text(",
            "def non_negative_int_setting(",
            "def e2e_bool_setting(",
            "def e2e_load_scale_fixture_env(",
            "def e2e_load_scale_fixture_bool(",
            "def hook_command_words(",
            "words = shlex.split(stripped)",
            "command must contain at least one word",
            "except OSError as exc:",
            "command could not start",
            "colliding E2E chaos phase names were accepted",
            "blank E2E chaos phase list was accepted",
            "embedded blank E2E chaos phase list was accepted",
            "duplicate E2E chaos phase was accepted",
            "placeholder E2E chaos phase was accepted",
            "angle-bracket placeholder E2E chaos phase was accepted",
            "placeholder E2E chaos expectation was accepted",
            "blank required E2E chaos phase list was accepted",
            "embedded blank required E2E chaos phase list was accepted",
            "placeholder required E2E chaos phase was accepted",
            "blank global E2E chaos hook preflight was accepted",
            "placeholder E2E chaos hook preflight was accepted",
            "blank phase-specific E2E chaos hook preflight was accepted",
            "malformed E2E chaos hook preflight was accepted",
            "colliding E2E load/scale phase names were accepted",
            "blank E2E load/scale phase list was accepted",
            "embedded blank E2E load/scale phase list was accepted",
            "duplicate E2E load/scale phase was accepted",
            "placeholder E2E load/scale phase was accepted",
            "blank required E2E load/scale phase list was accepted",
            "embedded blank required E2E load/scale phase list was accepted",
            "placeholder required E2E load/scale phase was accepted",
            "blank global E2E load/scale hook preflight was accepted",
            "placeholder E2E load/scale hook preflight was accepted",
            "blank phase-specific E2E load/scale hook preflight was accepted",
            "malformed E2E load/scale hook preflight was accepted",
            "placeholder E2E load/scale fixture enable flag was accepted",
            "invalid E2E load/scale fixture enable flag was accepted",
            "placeholder E2E load/scale fixture action was accepted",
            "invalid E2E load/scale fixture action was accepted",
            "placeholder E2E load/scale fixture load records were accepted",
            "negative E2E load/scale fixture load records were accepted",
            "blank E2E load/scale fixture node override was accepted",
            "placeholder E2E load/scale fixture producer node was accepted",
            "duplicate E2E named hook context was accepted",
            "must not repeat name",
            "blank E2E hook command was accepted",
            "malformed E2E hook command was accepted",
            "placeholder E2E hook command was accepted",
            "unstartable E2E chaos hook command was accepted",
            "placeholder E2E load/scale hook command was accepted",
            "unstartable E2E load/scale hook command was accepted",
            "placeholder E2E load/scale fixture dry-run flag was accepted",
            "placeholder E2E load/scale fixture phase index was accepted",
            "negative E2E load/scale fixture phase index was accepted",
            "invalid E2E load/scale fixture prestop flag was accepted",
            "ZMQ_E2E_LOAD_SCALE_APPLY_MARKER",
            "ZMQ_E2E_LOAD_SCALE_RESTORE_MARKER",
            "ok: load/scale fixture {kind} phase={phase_name} ",
            "action={action} dry_run={dry_run} source=command",
            "def e2e_chaos_phase_marker(",
            "ok: E2E chaos phase {phase['name']} down=true ",
            "observed={observed} healed={healed_text} recovered={recovered_text} ",
            "expect={phase['expect']} source=command",
            "E2E chaos failed phase marker formatting failed",
            "E2E chaos survived phase marker formatting failed",
            "marker_payloads=hook-owned",
            "apply_source={phase['apply_source']}",
            "restore_source={phase['restore_source']} source=command",
            "fixture_load_records",
            "wait_for_fixture_load_payloads(",
            "load_records={phase['fixture_load_records']}",
            "wait_for_existing_cross_node_payload(",
            "ok: E2E chaos passed for {', '.join(phase['name'] for phase in phases)} phase(s) source=command",
            "ok: E2E load/scale passed for {', '.join(phase['name'] for phase in phases)} phase(s) source=command",
        ),
    ),
)

RUN_GATE_BOOL_PREFLIGHT_CONTRACTS = (
    (
        "tests/chaos_test.py",
        (
            "def run_gate_enabled(",
            "if not run_gate_enabled(\"ZMQ_RUN_CHAOS_TESTS\")",
            "placeholder chaos run gate was accepted",
            "blank chaos run gate was accepted",
            "invalid chaos run gate was accepted",
        ),
    ),
    (
        "tests/client_matrix_test.py",
        (
            "def run_gate_enabled(",
            "if not run_gate_enabled(\"ZMQ_RUN_CLIENT_MATRIX\")",
            "placeholder client matrix run gate was accepted",
            "blank client matrix run gate was accepted",
            "invalid client matrix run gate was accepted",
        ),
    ),
    (
        "tests/e2e_test.py",
        (
            "def run_gate_enabled(",
            "if not run_gate_enabled(\"ZMQ_RUN_E2E_TESTS\")",
            "placeholder E2E run gate was accepted",
            "blank E2E run gate was accepted",
            "invalid E2E run gate was accepted",
        ),
    ),
    (
        "tests/kraft_failover_test.py",
        (
            "def run_gate_enabled(",
            "if not run_gate_enabled(\"ZMQ_RUN_KRAFT_FAILOVER_TESTS\")",
            "placeholder KRaft run gate was accepted",
            "blank KRaft run gate was accepted",
            "invalid KRaft run gate was accepted",
        ),
    ),
    (
        "tests/s3_process_crash_test.py",
        (
            "def run_gate_enabled(",
            "if not run_gate_enabled(\"ZMQ_RUN_PROCESS_CRASH_TESTS\")",
            "placeholder S3 process-crash run gate was accepted",
            "blank S3 process-crash run gate was accepted",
            "invalid S3 process-crash run gate was accepted",
        ),
    ),
    (
        "tests/s3_provider_matrix_test.py",
        (
            "def run_gate_enabled(",
            "if not run_gate_enabled(\"ZMQ_RUN_S3_PROVIDER_MATRIX\")",
            "placeholder S3 provider run gate was accepted",
            "blank S3 provider run gate was accepted",
            "invalid S3 provider run gate was accepted",
        ),
    ),
    (
        "benchmarks/benchmark_compare.py",
        (
            "env_bool(\"ZMQ_RUN_BENCH_COMPARE\", False)",
            "ZMQ_BENCH_COMPARE_ENFORCE_GATES",
            "placeholder comparative benchmark run gate was accepted",
            "blank comparative benchmark run gate was accepted",
            "invalid benchmark enforce-gates flag was accepted",
            "blank benchmark enforce-gates flag was accepted",
        ),
    ),
    (
        "tests/release_evidence_test.py",
        (
            "\"ZMQ_RUN_CHAOS_TESTS\"",
            "\"ZMQ_RUN_CLIENT_MATRIX\"",
            "\"ZMQ_RUN_BENCH_COMPARE\"",
            "\"ZMQ_BENCH_COMPARE_ENFORCE_GATES\"",
            "placeholder top-level run gate provenance was not reported",
            "blank top-level run gate provenance was not reported",
            "blank benchmark enforce-gates provenance was not reported",
            "invalid comparative benchmark run gate provenance was not reported",
        ),
    ),
)

RUN_GATE_BOOL_PREFLIGHT_DOC_FRAGMENTS = (
    "Top-level `ZMQ_RUN_*` opt-in gates",
    "`ZMQ_BENCH_COMPARE_ENFORCE_GATES`",
    "must parse as real booleans",
    "cannot silently skip",
)

PROTOCOL_STATIC_AUDIT_CONTRACT = (
    "CODEGEN = os.path.join(",
    "CODEGEN_V2 = os.path.join(",
    "def assert_codegen_strict_json_self_test(",
    "def assert_codegen_failure_exit_self_test(",
    "def audit(",
    "parse_schema_json('{\"name\": NaN}')",
    "accepted a non-standard JSON constant",
    "accepted a duplicate JSON object key",
    "exited successfully after a schema parse error",
    "did not report the schema parse error",
    "generated modules missing non-default golden fixtures",
    "non-default golden fixtures reference unknown generated modules",
    "broker handleRequest switch/table drift",
    "controller handleRequest switch/table drift",
    "fail-closed handler key is advertised",
    "non-broker generated key is broker dispatched",
    "legacy_inter_broker_request_api_keys",
    "legacy inter-broker API key catalogue drifted",
    "legacy inter-broker key is broker advertised/dispatched",
    "controller advertised telemetry keys 71/72 as KRaft APIs",
)

OBSERVABILITY_STATIC_AUDIT_CONTRACT = (
    "CRITICAL_ALERTS = {",
    "REQUIRED_ALERT_GROUPS = (",
    "def parse_strict_json(",
    "def collect_yaml_promql_expressions(",
    "def assert_dashboard_grid_position_well_formed(",
    "def assert_dashboard_target_well_formed(",
    "def assert_alert_blocks_well_formed(",
    "def assert_alert_name_contract(",
    "def assert_alert_group_contract(",
    "non-standard JSON constant was accepted",
    "duplicate JSON object key was accepted",
    "invalid dashboard grid position was accepted",
    "unexpected dashboard target field was accepted",
    "missing dashboard target legend was accepted",
    "unpinned alert name was accepted",
    "critical alert severity downgrade was accepted",
    "critical alert rules were downgraded",
    "missing critical alert block was accepted",
    "duplicate alert group name was accepted",
    "missing required alert group was accepted",
    "unpinned alert group name was accepted",
    "duplicate alert name was accepted",
    "missing alert name was accepted",
    "unpinned dashboard metric reference was accepted",
    "missing pinned alert metric reference was accepted",
    "registered unprefixed metric references were not collected",
    "quoted label values were collected as metric references",
)

S3_PROCESS_CRASH_SELFTEST_ASSERTIONS = (
    "message set fixture is too short",
    "message set fixture layout drifted",
    "placeholder S3 process-crash run gate was accepted",
    "blank S3 process-crash run gate was accepted",
    "invalid S3 process-crash run gate was accepted",
    "truthy S3 process-crash run gate was not accepted",
    "placeholder S3 process-crash broker port was accepted",
    "malformed S3 process-crash broker port was accepted",
    "non-positive S3 process-crash broker port was accepted",
    "S3 process-crash broker port did not parse",
    "placeholder S3 process-crash endpoint was accepted",
    "angle-bracket placeholder S3 process-crash endpoint was accepted",
    "blank S3 process-crash endpoint was accepted",
    "blank S3 process-crash port was accepted",
    "non-positive S3 process-crash port was accepted",
    "placeholder S3 process-crash bucket was accepted",
    "blank S3 process-crash access key was accepted",
    "blank S3 process-crash secret key was accepted",
    "invalid S3 process-crash scheme was accepted",
    "blank S3 process-crash region was accepted",
    "invalid S3 process-crash path-style was accepted",
    "truthy S3 process-crash path-style flag did not parse",
    "false S3 process-crash path-style flag did not parse",
    "placeholder S3 process-crash skip-health flag was accepted",
    "invalid S3 process-crash skip-health flag was accepted",
    "truthy S3 process-crash skip-health flag did not parse",
    "false S3 process-crash skip-health flag did not parse",
    "blank S3 process-crash TLS CA file was accepted",
)

BENCHMARK_COMPARE_SELFTEST_ASSERTIONS = (
    "all target parsing failed",
    "subset target parsing failed",
    "ambiguous target alias was accepted",
    "invalid target parsing did not fail",
    "empty target parsing did not fail",
    "throughput ratio formatting failed",
    "latency ratio formatting failed",
    "Produce v3 timestamp fixture transaction id drifted",
    "Produce v3 timestamp fixture topic count drifted",
    "Produce v3 timestamp fixture partition count drifted",
    "Produce v3 timestamp fixture timestamp mismatch",
    "Produce v3 record timestamp must use wall-clock epoch milliseconds",
    "passing comparison gate failed",
    "missing required benchmark target was not reported",
    "missing selected benchmark target was not reported",
    "malformed benchmark target result was not reported",
    "comparative table header target labels drifted",
    "comparative table ratio labels drifted",
    "comparative profile marker formatting drifted",
    "malformed current benchmark metric was not reported",
    "non-finite current benchmark metric was not reported",
    "zero current benchmark latency was not reported",
    "malformed benchmark error count was not reported",
    "failing comparison gate passed",
    "throughput regression was not reported",
    "error-rate regression was not reported",
    "missing baseline was not reported",
    "trend throughput regression was not reported",
    "trend latency regression was not reported",
    "missing ZMQ trend target was not reported",
    "missing ZMQ trend baseline was not reported",
    "malformed trend baseline throughput was not reported",
    "non-finite trend baseline throughput was not reported",
    "negative trend baseline latency was not reported",
    "placeholder comparative benchmark run gate was accepted",
    "invalid comparative benchmark run gate was accepted",
    "blank comparative benchmark run gate was accepted",
    "truthy comparative benchmark run gate was not accepted",
    "placeholder benchmark enforce-gates flag was accepted",
    "invalid benchmark enforce-gates flag was accepted",
    "blank benchmark enforce-gates flag was accepted",
    "truthy benchmark enforce-gates flag was not accepted",
    "environment threshold parsing failed",
    "environment trend threshold parsing failed",
    "non-finite threshold parsing did not fail",
    "placeholder threshold parsing did not fail",
    "blank threshold parsing did not fail",
    "negative threshold parsing did not fail",
    "environment required target parsing failed",
    "blank required target list was accepted",
    "embedded blank required target was accepted",
    "placeholder required target list was accepted",
    "angle-bracket placeholder required target list was accepted",
    "duplicate required target was accepted",
    "required target list without ZMQ was accepted",
    "required target list without baseline was accepted",
    "required target alias was accepted",
    "uppercase required target was accepted",
    "release required target parsing failed",
    "required target selection validation failed",
    "missing release required target list was accepted",
    "trend requirement parsing failed",
    "placeholder trend requirement flag was accepted",
    "invalid trend requirement flag was accepted",
    "trend baseline loading failed",
    "relative trend baseline loading was not project-rooted",
    "benchmark result artifact metadata schema missing",
    "benchmark result artifact target metadata missing",
    "benchmark result artifact trend baseline metadata missing",
    "benchmark result artifact threshold metadata missing",
    "benchmark result artifact target-label metadata missing",
    "benchmark result artifact iteration metadata missing",
    "benchmark result artifact warmup metadata missing",
    "trend baseline artifact metadata missing was accepted",
    "trend baseline artifact schema drift was accepted",
    "trend baseline artifact metadata without ZMQ was accepted",
    "mismatched trend baseline artifact target metadata was accepted",
    "trend baseline artifact result target outside selected targets was accepted",
    "trend baseline artifact required target outside selected targets was accepted",
    "trend baseline artifact unknown top-level key was accepted",
    "trend baseline artifact missing target label was accepted",
    "trend baseline artifact mismatched target label was accepted",
    "trend baseline artifact mismatched iterations were accepted",
    "trend baseline artifact missing threshold was accepted",
    "trend baseline artifact non-finite threshold was accepted",
    "trend baseline artifact non-boolean gate flag was accepted",
    "trend baseline artifact missing required trend-baseline path was accepted",
    "string benchmark result artifact selected targets were accepted",
    "duplicate benchmark result artifact selected targets were accepted",
    "unknown benchmark result artifact required target was accepted",
    "benchmark result artifact selected targets missing result target was accepted",
    "benchmark result artifact required target outside selected targets was accepted",
    "non-boolean benchmark result artifact gate flag was accepted",
    "trend-required benchmark result artifact without trend baseline was accepted",
    "non-string benchmark result artifact trend baseline was accepted",
    "non-object benchmark result artifact map was accepted",
    "unknown benchmark result artifact result target was accepted",
    "non-object benchmark result artifact target result was accepted",
    "benchmark result artifact missing benchmark row was accepted",
    "unknown benchmark result artifact benchmark key was accepted",
    "malformed benchmark result artifact metric was accepted",
    "zero benchmark result artifact metric was accepted",
    "malformed benchmark result artifact count was accepted",
    "non-standard JSON trend baseline was accepted",
    "duplicate-key JSON trend baseline was accepted",
    "non-standard JSON benchmark result was written",
    "non-standard JSON benchmark result clobbered existing artifact",
    "malformed benchmark result artifact was written",
    "malformed benchmark result artifact clobbered existing artifact",
    "non-enforced failing benchmark result artifact was not writable",
    "passing enforced benchmark result artifact was not writable",
    "failing enforced benchmark result artifact was writable",
    "failing enforced benchmark result artifact was written",
    "failing enforced benchmark result clobbered existing artifact",
    "benchmark results artifact display label drifted",
    "placeholder trend baseline path was accepted",
    "current results artifact was accepted as trend baseline",
    "missing required trend baseline was accepted",
)

E2E_SELFTEST_ASSERTIONS = (
    "E2E harness must define exactly three nodes",
    "MinIO port must not collide with broker/controller/metrics ports",
    "E2E Produce helper did not preserve payload as MessageSet value",
    "broker offset rewrite would corrupt E2E MessageSet payload",
    "placeholder E2E run gate was accepted",
    "blank E2E run gate was accepted",
    "invalid E2E run gate was accepted",
    "truthy E2E run gate was not accepted",
    "E2E chaos phases unexpectedly configured",
    "E2E load/scale phases unexpectedly configured",
    "blank global E2E chaos hook preflight was accepted",
    "blank global E2E load/scale hook preflight was accepted",
    "placeholder E2E load/scale fixture enable flag was accepted",
    "invalid E2E load/scale fixture enable flag was accepted",
    "false E2E load/scale fixture enable flag configured phases",
    "blank E2E chaos phase list was accepted",
    "embedded blank E2E chaos phase list was accepted",
    "duplicate E2E chaos phase was accepted",
    "blank phase-specific E2E chaos hook preflight was accepted",
    "colliding E2E chaos phase names were accepted",
    "placeholder E2E chaos phase was accepted",
    "angle-bracket placeholder E2E chaos phase was accepted",
    "placeholder E2E chaos hook preflight was accepted",
    "malformed E2E chaos hook preflight was accepted",
    "placeholder E2E chaos expectation was accepted",
    "empty required E2E chaos phase list was accepted",
    "blank required E2E chaos phase list was accepted",
    "embedded blank required E2E chaos phase list was accepted",
    "placeholder required E2E chaos phase was accepted",
    "missing required E2E chaos phase was not rejected",
    "E2E chaos phase context failed",
    "E2E chaos topic context failed",
    "E2E chaos broker port context failed",
    "E2E chaos metrics port context failed",
    "E2E chaos container context failed",
    "E2E chaos MinIO context failed",
    "blank E2E hook command was accepted",
    "malformed E2E hook command was accepted",
    "placeholder E2E hook command was accepted",
    "unstartable E2E chaos hook command was accepted",
    "blank E2E load/scale phase list was accepted",
    "embedded blank E2E load/scale phase list was accepted",
    "duplicate E2E load/scale phase was accepted",
    "blank phase-specific E2E load/scale hook preflight was accepted",
    "colliding E2E load/scale phase names were accepted",
    "placeholder E2E load/scale phase was accepted",
    "placeholder E2E load/scale hook preflight was accepted",
    "malformed E2E load/scale hook preflight was accepted",
    "empty required E2E load/scale phase list was accepted",
    "blank required E2E load/scale phase list was accepted",
    "embedded blank required E2E load/scale phase list was accepted",
    "placeholder required E2E load/scale phase was accepted",
    "missing required E2E load/scale phase was not rejected",
    "E2E load/scale phase context failed",
    "E2E load/scale apply marker context failed",
    "E2E load/scale restore marker context failed",
    "E2E load/scale topic context failed",
    "E2E load/scale controller port context failed",
    "E2E load/scale metrics port context failed",
    "E2E load/scale MinIO context failed",
    "duplicate E2E named hook context was accepted",
    "placeholder E2E load/scale hook command was accepted",
    "unstartable E2E load/scale hook command was accepted",
    "E2E load/scale apply fixture payload drifted",
    "E2E load/scale restore fixture payload drifted",
    "placeholder E2E load/scale fixture dry-run flag was accepted",
    "placeholder E2E load/scale fixture phase index was accepted",
    "negative E2E load/scale fixture phase index was accepted",
    "invalid E2E load/scale fixture prestop flag was accepted",
    "E2E fixture apply command drifted",
    "E2E fixture restore command drifted",
    "E2E fixture load-record metadata drifted",
    "E2E fixture load-record override drifted",
    "placeholder E2E load/scale fixture load records were accepted",
    "negative E2E load/scale fixture load records were accepted",
    "blank E2E load/scale fixture node override was accepted",
    "placeholder E2E load/scale fixture producer node was accepted",
    "placeholder E2E load/scale fixture action was accepted",
    "invalid E2E load/scale fixture action was accepted",
)

CHAOS_SELFTEST_ERRORS = (
    "placeholder chaos run gate was accepted",
    "blank chaos run gate was accepted",
    "invalid chaos run gate was accepted",
    "truthy chaos run gate was not accepted",
    "default scenario selection failed",
    "scenario alias selection failed",
    "live S3/network scenario alias selection failed",
    "blank chaos scenario selector was accepted",
    "embedded blank chaos scenario selector was accepted",
    "blank global chaos network hook did not select all scenario",
    "blank global chaos network hook preflight was skipped",
    "blank global chaos live-S3 hook did not select all scenario",
    "blank global chaos live-S3 hook preflight was skipped",
    "all scenario selection did not include hooked chaos scenarios",
    "network partition phase parsing failed",
    "colliding chaos network phase names were accepted",
    "placeholder chaos network phase was accepted",
    "blank chaos network phase list was accepted",
    "embedded blank chaos network phase list was accepted",
    "duplicate chaos network phase was accepted",
    "network phase hook selection failed",
    "network phase expect selection failed",
    "placeholder chaos network expectation was accepted",
    "invalid chaos network expectation was accepted",
    "blank phase-specific chaos network hook preflight was accepted",
    "blank chaos hook command was accepted",
    "malformed chaos hook command was accepted",
    "placeholder chaos hook command was accepted",
    "malformed chaos network hook preflight was accepted",
    "placeholder chaos live S3 hook preflight was accepted",
    "unstartable chaos hook command was accepted",
    "empty required chaos scenario list was accepted",
    "blank required chaos scenario list was accepted",
    "embedded blank required chaos scenario list was accepted",
    "placeholder required chaos scenario was accepted",
    "placeholder required chaos network phase was accepted",
    "blank required chaos network phase list was accepted",
    "embedded blank required chaos network phase list was accepted",
    "placeholder chaos scenario selector was accepted",
    "missing required network phase was not rejected",
    "placeholder configured chaos broker port was accepted",
    "malformed configured chaos broker port was accepted",
    "non-positive configured chaos broker port was accepted",
    "configured chaos broker port did not parse",
    "blank chaos live S3 endpoint override was accepted",
    "blank chaos live S3 TLS CA override was accepted",
    "placeholder live S3 endpoint was accepted",
    "angle-bracket placeholder live S3 endpoint was accepted",
    "missing live S3 port was accepted",
    "non-positive live S3 port was accepted",
    "missing live S3 bucket was accepted",
    "missing live S3 scheme was accepted",
    "invalid live S3 path-style was accepted",
    "truthy live S3 path-style flag did not parse",
    "false live S3 path-style flag did not parse",
    "live S3 config parsing failed",
    "live S3 provider summary did not match selected config",
    "string encoding self-test failed",
    "record batch header self-test failed",
)

KRAFT_FAILOVER_SELFTEST_ERRORS = (
    "placeholder KRaft run gate was accepted",
    "blank KRaft run gate was accepted",
    "invalid KRaft run gate was accepted",
    "truthy KRaft run gate was not accepted",
    "placeholder KRaft controller port base was accepted",
    "angle-bracket placeholder KRaft controller port base was accepted",
    "malformed KRaft controller port base was accepted",
    "non-positive KRaft controller port base was accepted",
    "configured KRaft ports did not parse",
    "non-standard JSON ExportClusterManifest was accepted",
    "duplicate-key ExportClusterManifest was accepted",
    "strict ExportClusterManifest JSON parsing failed",
    "wyhash test vector failed",
    "network hooks unexpectedly configured",
    "blank global KRaft network hook was accepted",
    "network hooks were not detected",
    "blank KRaft network phase list was accepted",
    "embedded blank KRaft network phase list was accepted",
    "duplicate KRaft network phase was accepted",
    "blank phase-specific KRaft network hook was accepted",
    "colliding KRaft network phase names were accepted",
    "placeholder KRaft network phase was accepted",
    "placeholder KRaft network expectation was accepted",
    "empty network partition summary formatting failed",
    "network partition fail marker formatting failed",
    "network partition survive marker formatting failed",
    "empty required KRaft network phase list was accepted",
    "blank required KRaft network phase list was accepted",
    "embedded blank required KRaft network phase list was accepted",
    "placeholder required KRaft network phase was accepted",
    "missing required KRaft network phase was not rejected",
    "hook leader context failed",
    "hook controller port context failed",
    "hook broker pid context failed",
    "blank KRaft network hook command was accepted",
    "malformed KRaft network hook command was accepted",
    "placeholder KRaft network hook command was accepted",
    "unstartable KRaft network hook command was accepted",
    "OffsetCommit fixture parser failed",
    "OffsetCommit v9 fixture parser failed",
    "OffsetDelete fixture parser failed",
    "DeleteGroups fixture parser failed",
    "SyncGroup fixture parser failed",
    "record batch fixture too short",
    "record batch fixture length mismatch",
    "record batch fixture magic mismatch",
    "record batch fixture producer id mismatch",
    "record batch fixture producer epoch mismatch",
    "record batch fixture base sequence mismatch",
    "broker non-broker cases missing ",
)

CLIENT_MATRIX_SELFTEST_ERRORS = (
    "placeholder client matrix run gate was accepted",
    "blank client matrix run gate was accepted",
    "invalid client matrix run gate was accepted",
    "truthy client matrix run gate was not accepted",
    "blank client matrix profile list was accepted",
    "embedded blank client matrix profile was accepted",
    "duplicate client matrix profile was accepted",
    "blank required client profile bootstrap used global fallback",
    "blank required client profile tools used global fallback",
    "blank required client security protocol used global fallback",
    "blank profile client Python used global fallback",
    "blank profile client OAuth token used global fallback",
    "placeholder client matrix enable-go flag was accepted",
    "invalid client matrix enable-go flag was accepted",
    "truthy client matrix enable-go flag was not accepted",
    "empty client matrix profile list was accepted",
    "colliding client matrix profile names were accepted",
    "placeholder client matrix profile was accepted",
    "angle-bracket placeholder client matrix profile was accepted",
    "placeholder required client profile was accepted",
    "empty required client tool list was accepted",
    "duplicate required client tool list was accepted",
    "required client sub-profile outside required profile set was accepted",
    "unpinned required versioned client profile was accepted",
    "missing required client tool was accepted",
    "missing required client semantic was accepted",
    "missing required client bootstrap provenance was accepted",
    "malformed required client bootstrap provenance was accepted",
    "auto required client tools were accepted",
    "embedded blank required client profile tools were accepted",
    "duplicate required client profile tools were accepted",
    "duplicate required client profile semantics were accepted",
    "missing required client semantics provenance was accepted",
    "placeholder required client version was accepted",
    "plaintext required secured-client profile was accepted",
    "java profile override failed",
    "java profile bootstrap fallback failed",
    "client profile version marker self-test failed",
    "empty client semantic list was accepted",
    "embedded blank client semantic was accepted",
    "duplicate client semantic was accepted",
    "empty client tool list was accepted",
    "embedded blank client tool was accepted",
    "duplicate client tool list was accepted",
    "security semantic did not enable security config",
    "security-negative vector profile override failed",
    "ACL negative topic profile override failed",
    "active Java security environment self-test failed",
    "Kafka CLI security config self-test failed",
    "invalid client security protocol was accepted",
    "invalid client SASL mechanism was accepted",
    "placeholder client bootstrap was accepted",
    "angle-bracket placeholder client bootstrap was accepted",
    "non-numeric client bootstrap port was accepted",
    "zero client bootstrap port was accepted",
    "Java OAuth positive fixture self-test failed",
    "Java OAuth negative fixture self-test failed",
    "Java OAuth environment self-test failed",
    "client security detail marker self-test failed",
    "future-valid Java bad OAuth JAAS fixture was accepted as negative",
    "missing-exp Java OAuth JAAS fixture was accepted as positive",
    "Kafka CLI OAuth security config self-test failed",
    "kcat OAuth positive config self-test failed",
    "kcat OAuth config with unsupported principal claim was accepted",
    "kcat unsupported-principal-claim config was not accepted as negative",
    "future-valid kcat bad OAuth config was accepted as negative",
    "OAuth profile missing a selected-tool fixture was accepted",
    "OAuth profile without security semantic was accepted",
    "OAuth-negative profile missing a kcat negative fixture was accepted",
    "future-valid kcat bad OAuth config was accepted",
    "non-OAuth required client profile was accepted",
    "go profile override failed",
    "go profile bootstrap override failed",
    "placeholder client matrix profile enable-go flag was accepted",
    "invalid client matrix profile enable-go flag was accepted",
    "truthy client matrix profile enable-go flag was not accepted",
    "floating go-kafka module was accepted",
    "placeholder client Python executable was accepted",
    "semantic all parsing failed",
    "unsupported rebalance tool was accepted",
    "unsupported security tool was accepted",
    "required secured profile with unsupported tool was accepted",
    "unsupported security-negative tool was accepted",
    "security-negative without vectors was accepted",
    "non-standard JSON OAuth token was accepted",
    "duplicate-key OAuth token was accepted",
    "kafka-python OAuth token provider self-test failed",
    "kafka-python OAuth negative vector self-test failed",
    "kafka-python missing-exp OAuth negative vector self-test failed",
    "future-valid bad OAuth token was accepted as a negative vector",
    "missing-exp OAuth token was accepted as a positive fixture",
    "unsecured required client profile was accepted",
    "negative-security profile without vectors was accepted",
    "unknown semantic probe was accepted",
)

S3_PROVIDER_MATRIX_SELFTEST_ERRORS = (
    "placeholder S3 provider run gate was accepted",
    "blank S3 provider run gate was accepted",
    "invalid S3 provider run gate was accepted",
    "truthy S3 provider run gate was not accepted",
    "blank S3 provider profile list was accepted",
    "embedded blank S3 provider profile was accepted",
    "duplicate S3 provider profile was accepted",
    "empty S3 provider profile list was accepted",
    "colliding S3 provider profile names were accepted",
    "placeholder S3 provider profile was accepted",
    "angle-bracket placeholder S3 provider profile was accepted",
    "empty required S3 provider list was accepted",
    "duplicate {env_name} list was accepted",
    "placeholder required S3 provider profile was accepted",
    "global S3 provider endpoint fallback failed",
    "global S3 provider port fallback failed",
    "global S3 provider bucket fallback failed",
    "global S3 provider access-key fallback failed",
    "global S3 provider secret-key fallback failed",
    "global S3 provider region fallback failed",
    "global S3 provider scheme fallback failed",
    "global S3 provider path-style fallback failed",
    "global S3 provider TLS CA fallback failed",
    "global-only S3 provider endpoint fallback failed",
    "global-only S3 provider credential fallback failed",
    "global-only S3 provider scheme fallback failed",
    "global-only S3 provider path-style fallback failed",
    "blank S3 provider endpoint used global fallback",
    "blank S3 provider scheme used global fallback",
    "blank S3 provider outage enable used global fallback",
    "blank S3 provider outage hook used global fallback",
    "blank S3 provider multipart-fault enable used global fallback",
    "blank S3 provider multipart-fault command used global fallback",
    "missing explicit non-minio S3 provider settings was accepted",
    "profile endpoint override failed",
    "profile port override failed",
    "profile bucket override failed",
    "profile access-key override failed",
    "profile secret-key override failed",
    "profile region override failed",
    "profile scheme override failed",
    "profile path-style override failed",
    "profile TLS CA override failed",
    "profile skip-ensure-bucket override failed",
    "profile skip-minio-health override failed",
    "profile pagination gate override failed",
    "profile multipart-edge gate override failed",
    "profile multipart-fault gate override failed",
    "profile process-crash gate override failed",
    "profile live-outage gate override failed",
    "profile live-outage scenario override failed",
    "profile live-outage endpoint pass-through failed",
    "profile live-outage down hook override failed",
    "profile multipart-fault context failed",
    "profile multipart-fault bucket pass-through failed",
    "profile multipart-fault access-key pass-through failed",
    "profile multipart-fault secret-key pass-through failed",
    "profile multipart-fault scheme pass-through failed",
    "profile multipart-fault region pass-through failed",
    "profile multipart-fault path-style pass-through failed",
    "profile multipart-fault TLS CA pass-through failed",
    "placeholder S3 provider endpoint was accepted",
    "angle-bracket placeholder S3 provider endpoint was accepted",
    "non-positive S3 provider port was accepted",
    "placeholder S3 provider region was accepted",
    "blank S3 provider TLS CA was accepted",
    "placeholder S3 provider outage hook was accepted",
    "placeholder required outage hooks did not fail validation",
    "placeholder multipart-fault command was accepted",
    "invalid S3 provider scheme was accepted",
    "invalid S3 provider path-style was accepted",
    "invalid S3 provider skip-ensure-bucket flag was accepted",
    "placeholder S3 provider pagination gate flag was accepted",
    "invalid S3 provider process-crash gate flag was accepted",
    "multipart-fault detail marker did not match selected provider",
    "bare multipart-fault output evidence was accepted",
    "mismatched multipart-fault output evidence was accepted",
    "blank multipart-fault command was accepted",
    "malformed multipart-fault command was accepted",
    "unstartable multipart-fault command was accepted",
    "process-crash detail marker did not match validated fields",
    "bare process-crash output evidence was accepted",
    "process-crash output without source=command was accepted",
    "process-crash output with wrapper source was accepted",
    "mismatched process-crash bucket was accepted",
    "duplicate process-crash output field was accepted",
    "unknown process-crash output field was accepted",
    "stale process-crash replacement offset was accepted",
    "wrong process-crash committed offset was accepted",
    "wrong process-crash recovered payload count was accepted",
    "outage detail marker did not match selected provider",
    "outage output without provider evidence was accepted",
    "mismatched outage provider evidence was accepted",
    "bare outage output evidence was accepted",
    "required sub-profile outside provider set was accepted",
    "missing required outage hooks did not fail validation",
    "missing required multipart-fault command did not fail validation",
    "missing required outage profile did not fail validation",
    "missing required process-crash profile did not fail validation",
    "missing required list-pagination profile did not fail validation",
    "missing required multipart-edge profile did not fail validation",
    "missing required multipart-fault profile did not fail validation",
)

PYTHON_SELFTEST_FORMATTED_ERROR_FRAGMENTS = (
    (
        "tests/protocol_static_audit.py",
        "assert_codegen_strict_json_self_test",
        ("AssertionError",),
        (
            " did not parse a valid commented schema",
            " strict JSON failure was not explanatory: ",
            " accepted a non-standard JSON constant",
            " duplicate-key failure was not explanatory: ",
            " accepted a duplicate JSON object key",
            "protocol schema ",
            " must be strict JSON: ",
        ),
    ),
    (
        "tests/protocol_static_audit.py",
        "assert_codegen_failure_exit_self_test",
        ("AssertionError",),
        (
            " exited successfully after a schema parse error",
            " did not report the schema parse error: ",
        ),
    ),
    (
        "tests/release_evidence_test.py",
        ("AssertionError",),
        (
            "passing release evidence failed: ",
            "passing current-checkout release evidence failed: ",
            "passing strict-checkout release evidence failed: ",
            "boxed Docker E2E title marker was rejected: ",
            "matching required gate manifest assignment was rejected: ",
            "fixture-backed E2E load/scale selector was rejected: ",
            "custom comparative benchmark threshold evidence was rejected: ",
            "documented release-CI-required surface status was rejected: ",
        ),
    ),
    (
        "tests/s3_process_crash_test.py",
        ("AssertionError", "TestError", "MatrixError"),
        (
            "message set base offset drifted: ",
            "message length mismatch: ",
            "message CRC mismatch: ",
            "default MinIO health URL drifted: ",
            "scheme-qualified MinIO health URL drifted: ",
            "process crash summary missing ",
        ),
    ),
    (
        "tests/s3_provider_matrix_test.py",
        ("AssertionError", "TestError", "MatrixError"),
        (
            "profile parsing failed: ",
        ),
    ),
    (
        "tests/client_matrix_test.py",
        ("AssertionError", "TestError", "MatrixError"),
        (
            "profile parsing failed: ",
            "java semantics override failed: ",
            "security property override failed: ",
            "go semantics override failed: ",
        ),
    ),
    (
        "tests/kraft_failover_test.py",
        ("AssertionError", "TestError", "MatrixError"),
        (
            "replica directory id derivation failed: ",
            "replica directory variant derivation failed: ",
            "controller unsupported cases missing ",
            "broker non-broker cases missing ",
            "default network phase selection failed: ",
            "network matrix phase parsing failed: ",
            "network matrix expectation parsing failed: ",
            "network partition summary formatting failed: ",
            "network partition fail marker formatting failed: ",
            "network partition survive marker formatting failed: ",
            "OffsetFetch fixture parser failed: ",
            "OffsetFetch error fixture parser failed: ",
            "OffsetFetch v8 grouped fixture parser failed: ",
            "OffsetFetch v9 member fixture parser failed: ",
            "CreateTopics fixture parser failed: ",
            "AllocateProducerIds fixture parser failed: ",
            "InitProducerId fixture parser failed: ",
            "ListOffsets fixture parser failed: ",
            "OffsetForLeaderEpoch fixture parser failed: ",
            "DeleteRecords fixture parser failed: ",
            "CreatePartitions fixture parser failed: ",
            "BrokerHeartbeat fixture parser failed: ",
            "DescribeConfigs fixture parser failed: ",
            "DescribeLogDirs fixture parser failed: ",
            "AlterReplicaLogDirs fixture parser failed: ",
            "AssignReplicasToDirs fixture parser failed: ",
            "ElectLeaders fixture parser failed: ",
            "DescribeTopicPartitions fixture parser failed: ",
            "DescribeQuorum v2 fixture parser failed: ",
            "FetchSnapshot v1 fixture parser failed: ",
            "DescribeCluster fixture parser failed: ",
            "DescribeProducers fixture parser failed: ",
            "ListTransactions fixture parser failed: ",
            "DescribeTransactions fixture parser failed: ",
            "JoinGroup fixture parser failed: ",
            "DescribeGroups fixture parser failed: ",
            "ConsumerGroupDescribe fixture parser failed: ",
            "ListGroups fixture parser failed: ",
            "FindCoordinator fixture parser failed: ",
            "ConsumerGroupHeartbeat fixture parser failed: ",
            "ShareGroupHeartbeat fixture parser failed: ",
            "ShareGroupDescribe fixture parser failed: ",
            "ShareFetch fixture parser failed: ",
            "ShareAcknowledge fixture parser failed: ",
            "Share state result fixture parser failed: ",
            "ReadShareGroupState fixture parser failed: ",
            "ReadShareGroupStateSummary fixture parser failed: ",
            "Produce v9 fixture parser failed: ",
            "AutomqGetNodes tag fixture parser failed: ",
            "DescribeStreams tag fixture parser failed: ",
        ),
    ),
    (
        "tests/chaos_test.py",
        ("AssertionError", "TestError", "MatrixError"),
        (
            "record count self-test failed: ",
            "live S3 broker args missing ",
        ),
    ),
    (
        "tests/e2e_test.py",
        ("AssertionError", "TestError", "MatrixError"),
        (
            "E2E JoinGroup parser drifted: ",
            "E2E SyncGroup parser drifted: ",
            "E2E Heartbeat parser drifted: ",
            "E2E node ",
            " missing keys: ",
            "default E2E chaos phase selection failed: ",
            "E2E chaos matrix parsing failed: ",
            "E2E chaos expectation parsing failed: ",
            "default E2E load/scale phase selection failed: ",
            "default E2E load/scale hook source failed: ",
            "E2E load/scale matrix parsing failed: ",
            "E2E fixture default phase selection failed: ",
            "E2E fixture source metadata drifted: ",
            "E2E fixture load-record metadata drifted: ",
            "E2E fixture load-record override drifted: ",
            "E2E fixture required-phase inference failed: ",
            "E2E fixture required-phase source inference failed: ",
            "E2E fixture matrix parsing failed: ",
            " has invalid ",
            "E2E port ",
            " is reused",
        ),
    ),
)

PYTHON_SELFTEST_DYNAMIC_ERROR_FRAGMENTS = (
    "live-S3 benchmark settings provenance was rejected: ",
    "live-S3 benchmark iteration/payload-size command provenance was rejected: ",
    "live-S3 chaos detail marker fixture was rejected: ",
    "live-S3 chaos fallback command provenance was rejected: ",
    "global S3 provider enable command provenance was rejected: ",
    "global S3 provider endpoint provenance was rejected: ",
    "global S3 provider settings provenance was rejected: ",
    "comparative benchmark enforce-gates command provenance was rejected: ",
    "missing S3 multipart-fault enable provenance was not reported",
)

PYTHON_SELFTEST_RAISE_SHAPE_SPECS = (
    (
        "tests/protocol_static_audit.py",
        "assert_codegen_strict_json_self_test",
        ("AssertionError",),
        ("JoinedStr",),
    ),
    (
        "tests/protocol_static_audit.py",
        "assert_codegen_failure_exit_self_test",
        ("AssertionError",),
        ("JoinedStr",),
    ),
    (
        "tests/observability_static_audit.py",
        "self_test",
        ("AssertionError",),
        ("Constant",),
    ),
    (
        "benchmarks/benchmark_compare.py",
        "self_test",
        ("AssertionError",),
        ("Constant",),
    ),
    (
        "tests/release_evidence_test.py",
        "self_test",
        ("AssertionError",),
        ("BinOp", "Constant", "JoinedStr", "Name"),
    ),
    (
        "tests/s3_process_crash_test.py",
        "self_test",
        ("AssertionError",),
        ("Constant", "JoinedStr"),
    ),
    (
        "tests/s3_provider_matrix_test.py",
        "self_test",
        ("MatrixError",),
        ("Constant", "JoinedStr", "Name"),
    ),
    (
        "tests/client_matrix_test.py",
        "self_test",
        ("MatrixError",),
        ("Constant", "JoinedStr"),
    ),
    (
        "tests/kraft_failover_test.py",
        "self_test",
        ("TestError",),
        ("Constant", "JoinedStr"),
    ),
    (
        "tests/chaos_test.py",
        "self_test",
        ("TestError",),
        ("Constant", "JoinedStr"),
    ),
    (
        "tests/e2e_test.py",
        "self_test",
        ("AssertionError",),
        ("Constant", "JoinedStr"),
    ),
)

PYTHON_SELFTEST_RAISE_SHAPE_NO_RAISE_PATHS = (
    "tests/build_static_audit.py",
)

PYTHON_SELFTEST_RAISE_SHAPE_DOC_FRAGMENTS = (
    "Python self-test raise-shape catalogue",
    "checked Python self-test gate list",
    "literal strings, f-strings, concatenated strings, and loop-selected messages",
    "new self-test raise message form",
    "build static audit scanner",
)

RELEASE_EVIDENCE_COMMAND_PROVENANCE_DISPATCH = (
    (
        "broker chaos harness",
        "validate_chaos_live_s3_command_provenance",
        (
            'for suffix in ("ENDPOINT", "PORT", "BUCKET", "SCHEME", "REGION", "PATH_STYLE")',
            "chaos_live_s3_setting_value(environment, suffix)",
            "non-empty {env_name}= assignment for live-S3 outage",
        ),
    ),
    (
        "S3 provider matrix",
        "validate_s3_provider_matrix_command_provenance",
        (
            "ZMQ_S3_PROVIDER_REQUIRED_PROFILES",
            "ZMQ_S3_PROVIDER_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_OUTAGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_PROCESS_CRASH_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_LIST_PAGINATION_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_EDGE_PROFILES",
            "ZMQ_S3_PROVIDER_REQUIRED_MULTIPART_FAULT_PROFILES",
            "s3_profile_enable_command_env_names(environment)",
        ),
    ),
    (
        "Docker E2E gate",
        "validate_e2e_command_provenance",
        (
            "ZMQ_E2E_REQUIRED_CHAOS_PHASES",
            "ZMQ_E2E_CHAOS_MATRIX",
            "ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES",
            "ZMQ_E2E_LOAD_SCALE_MATRIX",
            "ZMQ_E2E_LOAD_SCALE_USE_FIXTURE",
        ),
    ),
    (
        "live-S3 benchmark gate",
        "validate_live_s3_benchmark_command_provenance",
        (
            "ZMQ_BENCH_LIVE_S3_ITERATIONS",
            "ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES",
            "POSITIVE_INTEGER_ENV_VARS",
        ),
    ),
    (
        "comparative benchmark gate",
        "validate_comparative_benchmark_command_provenance",
        (
            "ZMQ_BENCH_COMPARE_ENFORCE_GATES",
            "BENCHMARK_THRESHOLD_ENV_VARS",
        ),
    ),
)

RELEASE_EVIDENCE_OUTPUT_MARKER_DISPATCH = (
    (
        "broker chaos harness",
        (
            "validate_chaos_harness_summary_output",
            "validate_chaos_scenario_detail_output",
            "validate_chaos_network_phase_detail_output",
        ),
    ),
    (
        "external client matrix",
        (
            "validate_client_profile_output_markers",
            "validate_client_profile_scoped_probe_markers",
            "validate_client_matrix_summary_output",
        ),
    ),
    (
        "S3 process-crash replacement gate",
        ("validate_s3_process_crash_summary_output",),
    ),
    (
        "MinIO/S3 integration gate",
        ("validate_minio_test_count_output",),
    ),
    (
        "S3 provider matrix",
        (
            "validate_s3_provider_profile_output_markers",
            "validate_s3_provider_profile_scoped_markers",
            "validate_s3_provider_matrix_summary_output",
        ),
    ),
    (
        "KRaft failover gate",
        (
            "validate_kraft_network_summary_output",
            "validate_kraft_network_phase_detail_output",
            "validate_kraft_reassignment_summary_output",
        ),
    ),
    (
        "Docker E2E gate",
        (
            "validate_e2e_output_line_markers",
            "validate_e2e_phase_summary_output",
            "validate_e2e_chaos_phase_detail_output",
            "validate_e2e_load_scale_phase_detail_output",
            "validate_e2e_final_results_output",
        ),
    ),
    (
        "local benchmark gate",
        ("validate_local_benchmark_summary_output",),
    ),
    (
        "live-S3 benchmark gate",
        (
            "validate_live_s3_benchmark_summary_output",
            "validate_live_s3_benchmark_provider_output",
            "validate_live_s3_benchmark_operation_summary_output",
            "validate_live_s3_benchmark_request_volume_output",
        ),
    ),
    (
        "comparative benchmark gate",
        ("validate_comparative_benchmark_summary_output",),
    ),
)

RELEASE_EVIDENCE_OUTPUT_MARKER_DISPATCH_DOC_FRAGMENTS = (
    "release-evidence output-marker dispatch catalogue",
    "requirement-specific output validators",
    "broker chaos, client matrix, S3, KRaft, Docker E2E, and benchmark markers",
    "new release-evidence output validator",
    "build static audit dispatch catalogue",
)

RELEASE_EVIDENCE_UNSUPPORTED_SURFACE_CATALOG_DOC_FRAGMENTS = (
    "unsupported-surface catalogue",
    "release-evidence verifier, release criteria, parity notes, and production-readiness pins",
    "each known surface label",
    "new unsupported or partial surface",
    "build static audit unsupported-surface catalogue",
)

RELEASE_EVIDENCE_UNSUPPORTED_STATUS_CATALOG_DOC_FRAGMENTS = (
    "release-evidence unsupported surface status-marker catalogue",
    "UNSUPPORTED_SURFACE_STATUS_MARKERS",
    "explicit unsupported/partial status markers",
    "build static audit unsupported-status catalogue",
)

RELEASE_EVIDENCE_UNSUPPORTED_SURFACE_TEXT_FIELD_CATALOG_DOC_FRAGMENTS = (
    "release-evidence unsupported surface text-field catalogue",
    "UNSUPPORTED_SURFACE_TEXT_FIELDS",
    "unsupported-surface text aggregation",
    "id, surface, status, evidence, mitigation, and notes",
    "build static audit unsupported-surface-text-field catalogue",
)

RELEASE_EVIDENCE_REQUIRED_COMMAND_BLOCK_DOC_FRAGMENTS = (
    "required command catalogue mirror",
    "release-evidence REQUIRED_COMMANDS",
    "fenced release criteria command block",
    "same order",
    "build static audit command-block catalogue",
)

RELEASE_EVIDENCE_REQUIRED_ENV_CATALOG_DOC_FRAGMENTS = (
    "required environment-variable catalogue",
    "release-evidence REQUIRED_ENV_VARS",
    "release criteria, parity notes, and production-readiness pins",
    "every required coverage variable",
    "build static audit environment catalogue",
)

RELEASE_EVIDENCE_COMMAND_ENV_ASSIGNMENT_CATALOG_DOC_FRAGMENTS = (
    "command environment-assignment catalogue",
    "per-gate command_env_assignments",
    "release criteria, parity notes, and production-readiness pins",
    "same-gate command provenance variable",
    "build static audit command-env catalogue",
)

RELEASE_EVIDENCE_COMMAND_SHAPE_CATALOG_DOC_FRAGMENTS = (
    "release-evidence command-shape catalogue",
    "ENV_ASSIGNMENT_RE",
    "ENV_NAME_RE",
    "SHELL_COMMAND_SEPARATORS",
    "SUCCESS_SHELL_COMMAND_SEPARATOR",
    "DISALLOWED_SHELL_OPERATOR_TOKENS",
    "DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS",
    "DISALLOWED_COMMAND_LINE_BREAKS",
    "DISALLOWED_COMMAND_QUOTE_CHARS",
    "DISALLOWED_COMMAND_ESCAPE_CHARS",
    "ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS",
    "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS",
    "FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS",
    "single-line direct invocations",
    "build static audit command-shape catalogue",
)

RELEASE_EVIDENCE_SKIP_MARKER_CATALOG_DOC_FRAGMENTS = (
    "release-evidence skip-marker catalogue",
    "per-gate skip_markers",
    "release criteria, parity notes, and production-readiness pins",
    "skipped live gate",
    "build static audit skip-marker catalogue",
)

RELEASE_EVIDENCE_OUTPUT_MARKER_CATALOG_DOC_FRAGMENTS = (
    "release-evidence output-marker catalogue",
    "per-gate output_markers",
    "release criteria, parity notes, and production-readiness pins",
    "required success marker",
    "static audit output marker must appear exactly once",
    "compose config output marker must appear exactly once",
    "build static audit output-marker catalogue",
)

RELEASE_EVIDENCE_FORBIDDEN_FRAGMENT_CATALOG_DOC_FRAGMENTS = (
    "forbidden command-fragment catalogue",
    "per-gate forbidden fragments",
    "release criteria, parity notes, and production-readiness pins",
    "local benchmark gate",
    "build static audit forbidden-fragment catalogue",
)

RELEASE_EVIDENCE_SCHEMA_FIELD_CATALOG_DOC_FRAGMENTS = (
    "release-evidence schema field catalogue",
    "RELEASE_EVIDENCE_FIELDS, COMMAND_ENTRY_FIELDS, and UNSUPPORTED_SURFACE_FIELDS",
    "release criteria, parity notes, and production-readiness pins",
    "closed schema field",
    "build static audit schema-field catalogue",
)

RELEASE_EVIDENCE_BLOCKING_FLAG_CATALOG_DOC_FRAGMENTS = (
    "release-evidence blocking-flag catalogue",
    "BLOCKING_FLAGS",
    "release criteria, parity notes, and production-readiness pins",
    "blocking flag",
    "build static audit blocking-flag catalogue",
)

RELEASE_EVIDENCE_NUMERIC_ENV_CATALOG_DOC_FRAGMENTS = (
    "release-evidence numeric environment catalogue",
    "BENCHMARK_THRESHOLD_ENV_VARS and POSITIVE_INTEGER_ENV_VARS",
    "finite non-negative floats",
    "positive integers",
    "build static audit numeric-env catalogue",
)

RELEASE_EVIDENCE_COVERAGE_SELECTOR_CATALOG_DOC_FRAGMENTS = (
    "release-evidence coverage selector catalogue",
    "COVERAGE_SELECTOR_REQUIREMENTS",
    "selector, required, label, token_style, and fixture",
    "coverage selector assignments",
    "build static audit coverage-selector catalogue",
)

RELEASE_EVIDENCE_COMMA_ENV_CATALOG_DOC_FRAGMENTS = (
    "release-evidence comma-separated environment catalogue",
    "COMMA_SEPARATED_ENV_VARS",
    "REQUIRED_ENV_VARS except",
    "ZMQ_BENCH_COMPARE_REQUIRE_TREND",
    "ZMQ_BENCH_COMPARE_TREND_BASELINE",
    "blank comma-separated entries",
    "duplicate comma-separated entries",
    "build static audit comma-env catalogue",
)

RELEASE_EVIDENCE_BOOLEAN_ENV_CATALOG_DOC_FRAGMENTS = (
    "release-evidence boolean environment catalogue",
    "BOOLEAN_ENV_VARS",
    "CLIENT_PROFILE_BOOL_SUFFIXES",
    "E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES",
    "S3_BOOL_SUFFIXES",
    "real booleans",
    "build static audit boolean-env catalogue",
)

RELEASE_EVIDENCE_TOKEN_VOCABULARY_CATALOG_DOC_FRAGMENTS = (
    "release-evidence token vocabulary catalogue",
    "PLACEHOLDER_ENV_VALUES",
    "BOOL_TRUE_VALUES",
    "BOOL_FALSE_VALUES",
    "placeholder and boolean token values",
    "build static audit token-vocabulary catalogue",
)

RELEASE_EVIDENCE_S3_STRING_ENV_CATALOG_DOC_FRAGMENTS = (
    "release-evidence S3 string environment catalogue",
    "S3_STRING_SUFFIXES",
    "nonblank S3 string settings",
    "TLS_CA_FILE",
    "build static audit S3-string catalogue",
)

RELEASE_EVIDENCE_S3_SCOPED_MARKER_CATALOG_DOC_FRAGMENTS = (
    "release-evidence S3 provider scoped marker catalogue",
    "S3_PROVIDER_SCOPED_MARKER_TEMPLATES",
    "profile-scoped provider markers",
    "live-suite, outage, process-crash, list-pagination, multipart-edge, and multipart-fault",
    "build static audit S3-scoped-marker catalogue",
)

RELEASE_EVIDENCE_SAMPLE_ENV_OUTPUT_CATALOG_DOC_FRAGMENTS = (
    "release-evidence sample environment output-marker catalogue",
    "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS",
    "sample release evidence manifests",
    "broker chaos harness, external client matrix, S3 provider matrix, KRaft failover gate, Docker E2E gate, and comparative benchmark gate",
    "build static audit sample-env-output catalogue",
)

RELEASE_EVIDENCE_BUILD_SUMMARY_CATALOG_DOC_FRAGMENTS = (
    "release-evidence build summary and benchmark artifact catalogue",
    "BENCHMARK_RESULTS_ARTIFACT",
    "ZIG_BUILD_SUMMARY_RE",
    "benchmarks/results.json",
    "Results saved to benchmarks/results.json",
    "Build Summary:",
    "steps succeeded",
    "tests passed",
    "exactly one successful",
    "build static audit build-summary catalogue",
)

RELEASE_EVIDENCE_HOOK_PROVENANCE_CATALOG_DOC_FRAGMENTS = (
    "release-evidence hook-provenance catalogue",
    "PHASE_HOOK_PROVENANCE_REQUIREMENTS",
    "PROFILE_HOOK_PROVENANCE_REQUIREMENTS",
    "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS",
    "phase hook, profile hook, and S3 enable provenance",
    "build static audit hook-provenance catalogue",
)

RELEASE_EVIDENCE_CLIENT_CAPABILITY_CATALOG_DOC_FRAGMENTS = (
    "release-evidence client capability catalogue",
    "REQUIRED_CLIENT_TOOLS",
    "REQUIRED_CLIENT_SEMANTICS",
    "CLIENT_SECURITY_PROTOCOLS",
    "CLIENT_SASL_MECHANISMS",
    "CLIENT_SECURITY_TOOLS",
    "CLIENT_REBALANCE_TOOLS",
    "CLIENT_TRANSACTION_TOOLS",
    "build static audit client-capability catalogue",
)

RELEASE_EVIDENCE_CLIENT_TOOL_MARKER_CATALOG_DOC_FRAGMENTS = (
    "release-evidence client tool marker catalogue",
    "CLIENT_TOOL_OUTPUT_MARKERS",
    "REQUIRED_CLIENT_TOOLS",
    "per-tool probe markers",
    "build static audit client-tool-marker catalogue",
)

RELEASE_EVIDENCE_CLIENT_VERSION_CATALOG_DOC_FRAGMENTS = (
    "release-evidence client version/provenance catalogue",
    "CLIENT_PYTHON_TOOLS",
    "CLIENT_UNPINNED_VERSION_LABELS",
    "Python client matrix profile",
    "client/library version",
    "build static audit client-version catalogue",
)

RELEASE_EVIDENCE_CHAOS_SCENARIO_CATALOG_DOC_FRAGMENTS = (
    "release-evidence chaos scenario catalogue",
    "CHAOS_SCENARIO_ALIASES",
    "REQUIRED_CHAOS_SCENARIOS",
    "CHAOS_SCENARIO_MARKERS",
    "canonical broker chaos scenarios",
    "build static audit chaos-scenario catalogue",
)

RELEASE_EVIDENCE_DETAIL_OUTPUT_MARKER_CATALOG_DOC_FRAGMENTS = (
    "release-evidence detail output marker catalogue",
    "COMPARATIVE_TABLE_ROW_MARKERS",
    "BENCHMARK_OUTPUT_LINE_MARKERS",
    "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS",
    "KRAFT_DETAIL_OUTPUT_MARKERS",
    "E2E_OUTPUT_LINE_MARKERS",
    "KRaft, Docker E2E, benchmark, and comparative benchmark detail markers",
    "build static audit detail-output-marker catalogue",
)

RELEASE_EVIDENCE_COMPARATIVE_BENCHMARK_CATALOG_DOC_FRAGMENTS = (
    "release-evidence comparative benchmark catalogue",
    "COMPARATIVE_TARGET_LABELS",
    "COMPARATIVE_TABLE_TARGET_HEADERS",
    "COMPARATIVE_TABLE_METRICS",
    "COMPARATIVE_MEASUREMENT_RE",
    "COMPARATIVE_RATIO_RE",
    "COMPARATIVE_PROFILE_MARKER_KEYS",
    "COMPARATIVE_RATIO_RE entries must keep the comparative target labels",
    "table metric keys, ratio parser, and comparative benchmark profile marker aligned",
    "build static audit comparative-benchmark catalogue",
)

RELEASE_EVIDENCE_COMPARATIVE_THRESHOLD_DEFAULT_CATALOG_DOC_FRAGMENTS = (
    "release-evidence comparative threshold default catalogue",
    "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS",
    "COMPARATIVE_BENCHMARK_THRESHOLD_ENV",
    "default comparative threshold keys and values",
    "build static audit comparative-threshold-default catalogue",
)

RELEASE_EVIDENCE_E2E_FIXTURE_ACTION_CATALOG_DOC_FRAGMENTS = (
    "release-evidence E2E load/scale fixture action catalogue",
    "E2E_LOAD_SCALE_FIXTURE_ACTIONS",
    "built-in Docker E2E load/scale fixture actions",
    "build static audit E2E-fixture-action catalogue",
)

RELEASE_EVIDENCE_REQUIRED_COMMAND_KEYS = (
    "label",
    "required",
    "forbidden",
    "command_env_assignments",
    "skip_markers",
    "output_markers",
)

RELEASE_EVIDENCE_VERIFIER_CONTRACT = (
    "def validate_client_profile_provenance(",
    "BENCHMARK_MAIN_PATH",
    "def csv_value_failures(",
    "def output_template_marker_present(",
    "def zig_build_summary_success_count(",
    "EXACT_ONCE_OUTPUT_MARKERS_BY_LABEL",
    "def exact_once_output_marker_lines(",
    "def exact_once_requirement_output_marker_failures(",
    "E2E_EXACT_ONCE_OUTPUT_LINE_MARKERS",
    "def e2e_title_output_marker_line_matches(",
    "def e2e_output_marker_lines(",
    "def validate_e2e_output_line_markers(",
    "healed_leader=<id>",
    "def validate_client_profile_tools(",
    "def validate_client_profile_semantics(",
    "def validate_client_profile_tool_semantic_compatibility(",
    "def bool_environment_value(",
    "def validate_boolean_environment(",
    "def validate_s3_string_environment(",
    "def validate_environment_names_and_values(",
    "def output_csv_values(",
    "def integer_environment_rule(",
    "def validate_integer_environment(",
    "def validate_e2e_load_scale_fixture_environment(",
    "def validate_duplicate_command_env_assignments(",
    "def validate_integer_command_assignments(",
    "def validate_required_command_env_assignments(",
    "def required_command_lines(",
    "def validate_required_command_block_line(",
    "def assert_required_command_block_matches_validator(",
    "release criteria required command block must list exactly",
    "release criteria required command block line",
    "violates release command syntax",
    "def manifest_bool_value(",
    "def reject_nonstandard_json_constant(",
    "def reject_duplicate_json_object_keys(",
    "def validate_release_evidence_for_checkout(",
    "RELEASE_EVIDENCE_FIELDS",
    "COMMAND_ENTRY_FIELDS",
    "UNSUPPORTED_SURFACE_FIELDS",
    "def validate_object_fields(",
    "def current_git_commit(",
    "def tracked_worktree_dirty(",
    "ZMQ_RELEASE_EVIDENCE must not use a placeholder path",
    "def markdown_section(",
    "def known_unsupported_surface_bullets(",
    "def assert_known_unsupported_surfaces_match_validator(",
    "release criteria Known Unsupported Or Partial Surfaces must list",
    "duplicate bullets for",
    "must mark the surface as",
    "def validate_client_profile_version(",
    "def client_profile_expected_version(",
    "def client_profile_setting_environment_value(",
    "client_profile_setting_names(profile, suffix),",
    "for {label} {profile} must not be blank",
    "def validate_client_security_context(",
    "def validate_client_oauth_fixture_value(",
    "def raw_oauth_token_positive_configured(",
    "def oauth_jaas_positive_configured(",
    "def librdkafka_oauthbearer_positive_configured(",
    "CLIENT_PYTHON_TOOLS",
    "def client_probe_semantics_by_tool(",
    "def client_matrix_lines_before_summary(",
    "def client_profile_semantic_output_present(",
    "def client_profile_expected_bootstrap(",
    "def client_profile_output_blocks(",
    "def validate_client_profile_output_markers(",
    "def validate_client_profile_scoped_probe_markers(",
    "def parse_client_security_detail_line(",
    "def client_security_detail_expected(",
    "def client_security_detail_valid(",
    "def validate_client_matrix_summary_output(",
    "def split_summary_field_tokens(",
    "def parse_summary_key_value_fields(",
    "def parenthesized_summary_payload(",
    "def append_phase_detail(",
    "def minio_test_count_lines(",
    "def validate_minio_test_count_output(",
    "def s3_process_crash_summary_details(",
    "def validate_s3_process_crash_summary_output(",
    "def s3_provider_profile_output_details(",
    "def s3_provider_profile_output_blocks(",
    "def s3_provider_matrix_lines_before_summary(",
    "def parse_s3_provider_outage_detail_line(",
    "def s3_provider_outage_detail_valid(",
    "def parse_s3_provider_multipart_fault_detail_line(",
    "def s3_provider_multipart_fault_detail_valid(",
    "def parse_s3_provider_process_crash_detail_line(",
    "def s3_provider_process_crash_detail_valid(",
    "def s3_provider_bool_text(",
    "def s3_provider_profile_expected_settings(",
    "def validate_s3_provider_profile_output_markers(",
    "def s3_provider_block_marker_count(",
    "def validate_s3_provider_profile_scoped_markers(",
    "def s3_provider_matrix_summary_profiles(",
    "def validate_s3_provider_matrix_summary_output(",
    "def s3_profile_enable_command_env_names(",
    "def validate_s3_provider_matrix_command_provenance(",
    "def validate_e2e_command_provenance(",
    "def validate_comparative_benchmark_command_provenance(",
    "def validate_chaos_live_s3_command_provenance(",
    "def first_present_environment_value(",
    "first_present_environment_value(environment, names)",
    "release evidence hook command {env_name} for {label} must not be blank",
    "def e2e_phase_summary_output_phases(",
    "def validate_e2e_phase_summary_output(",
    "def kraft_network_phase_expected_result(",
    "def kraft_network_phase_required_marker(",
    "def validate_kraft_network_phase_detail_output(",
            "def e2e_chaos_phase_expected_result(",
            "def e2e_chaos_phase_expected_observed(",
            "def e2e_chaos_phase_required_marker(",
            "def validate_e2e_chaos_phase_detail_output(",
            "def e2e_load_scale_expected_fixture_load_records(",
            "def e2e_load_scale_phase_required_marker(",
            "def validate_e2e_load_scale_phase_detail_output(",
            "def e2e_lines_after_phase_summaries(",
            "def e2e_final_result_lines(",
            "def e2e_final_results(",
            "def validate_e2e_final_results_output(",
            "def benchmark_lines_before_completion(",
            "def local_benchmark_s3_request_volumes(",
            "def local_benchmark_s3_request_volume(",
            "def local_benchmark_memory_summaries(",
            "def validate_local_benchmark_summary_output(",
    "def live_s3_benchmark_put_summaries(",
    "def live_s3_benchmark_put_summary(",
    "def live_s3_benchmark_get_summaries(",
    "def live_s3_benchmark_get_summary(",
    "def live_s3_benchmark_provider_details(",
    "def live_s3_benchmark_scheme_text(",
    "def live_s3_benchmark_expected_settings(",
    "def validate_live_s3_benchmark_command_provenance(",
    "def validate_live_s3_benchmark_provider_output(",
    "def validate_live_s3_benchmark_operation_summary_output(",
    "def live_s3_benchmark_request_volumes(",
    "def live_s3_benchmark_request_volume(",
    "def validate_live_s3_benchmark_request_volume_output(",
    "def comparative_benchmark_gate_index(",
    "def comparative_benchmark_comparison_index(",
    "def comparative_benchmark_table_section(",
    "def comparative_benchmark_gate_section(",
    "def comparative_benchmark_results_artifact_indexes(",
    "def validate_comparative_benchmark_results_artifact_output(",
    "def comparative_benchmark_metric_rows(",
    "def comparative_benchmark_metric_row_payloads(",
    "def comparative_benchmark_metric_measurements(",
    "def comparative_benchmark_table_target_columns(",
    "def comparative_benchmark_table_ratio_columns(",
    "def comparative_required_table_target_columns(",
    "def comparative_required_target_labels(",
    "def comparative_required_table_ratio_columns(",
    "def comparative_metric_row_cells(",
    "def validate_comparative_benchmark_metric_row_cells(",
    "COMPARATIVE_RATIO_RE",
    "COMPARATIVE_RATIO_MARKERS",
    "def positive_measurement_count(",
    "def positive_finite_metric_cell(",
    "def positive_finite_ratio_cell(",
    "def validate_comparative_benchmark_summary_output(",
    "def kraft_network_summary_phases(",
    "def kraft_failover_summary_line(",
    "def kraft_failover_summary_field(",
    "def validate_kraft_network_summary_output(",
    "def validate_kraft_reassignment_summary_output(",
    "def chaos_live_s3_provider_details(",
    "def chaos_live_s3_setting_value(",
    "return first_present_environment_value(",
    "def chaos_live_s3_expected_settings(",
    "def validate_chaos_live_s3_provider_output(",
    "def unsupported_surface_status_matches(",
    "def chaos_lines_before_harness_summary(",
    "def chaos_harness_summary_scenarios(",
    "def validate_chaos_harness_summary_output(",
    "def chaos_network_phase_expected_result(",
    "def chaos_network_phase_expected_observed(",
    "def chaos_network_phase_required_marker(",
    "def validate_chaos_network_phase_detail_output(",
    "CLIENT_REBALANCE_TOOLS",
    "CLIENT_TRANSACTION_TOOLS",
    "CLIENT_SECURITY_TOOLS",
    "client profile setting TOOLS",
    "must explicitly list selected tools, not auto",
    "tools from ZMQ_CLIENT_MATRIX_REQUIRED_TOOLS",
    "semantics from ZMQ_CLIENT_MATRIX_REQUIRED_SEMANTICS",
    "client/library version",
    "Go module version",
    "Python client matrix profile",
    "unknown security protocol",
    "unknown SASL mechanism",
    "SASL_MECHANISM=OAUTHBEARER",
    "BAD_OAUTHBEARER_CONFIG",
    "positive OAuth fixture",
    "negative OAuth fixture",
    "passed client profile output marker",
    "profile-selected tool marker",
    "selected bootstrap",
    "unsupported client profile semantic was accepted",
    "misattributed client semantic marker was accepted",
    "bare client profile output marker was accepted",
    "client profile output marker without source=command was accepted",
    "mismatched client profile output tools were accepted",
    "missing client bootstrap provenance was accepted",
    "mismatched client profile bootstrap output was accepted",
    "missing client profile version marker was accepted",
    "mismatched client profile version marker was accepted",
    "duplicate client profile output marker was accepted",
    "missing client Python executable provenance was not reported",
    "blank client profile bootstrap used global release-evidence fallback",
    "blank client profile tools used global release-evidence fallback",
    "blank client profile Python used global release-evidence fallback",
    "blank client security protocol used global release-evidence fallback",
    "blank client OAuth fixture used global release-evidence fallback",
    "embedded blank client profile tool was accepted",
    "duplicate client profile tool was accepted",
    "duplicate client profile tool provenance was accepted",
    "duplicate client profile semantic provenance was accepted",
    "unscoped client profile tool marker was accepted",
    "split client profile probe/pass block was accepted",
    "detached client profile block was accepted",
    "client tool probe marker without source=command was accepted",
    "embedded blank client tool probe semantic was accepted",
    "duplicate client tool probe semantic was accepted",
    "duplicate client tool probe marker was accepted",
    "missing client security detail marker was accepted",
    "bare client security detail marker was accepted",
    "missing client security detail command source was accepted",
    "mismatched client security detail marker was accepted",
    "mismatched client security detail command source was accepted",
    "duplicate client security detail marker was accepted",
    "detached client security detail marker was accepted",
    "missing client matrix required-profile command assignment was accepted",
    "missing client matrix selected-profile command assignment was accepted",
    "missing client matrix required-tool command assignment was accepted",
    "missing client matrix required-semantic command assignment was accepted",
    "missing client matrix required-versioned-profile command assignment was accepted",
    "missing client matrix required-security-profile command assignment was accepted",
    "missing client matrix required-security-negative-profile command assignment was accepted",
    "missing client matrix required-oauth-profile command assignment was accepted",
    "missing client matrix required-oauth-negative-profile command assignment was accepted",
    "mismatched client matrix tool command assignment was accepted",
    "mismatched client matrix semantic command assignment was accepted",
    "missing chaos required-network-phases command assignment was accepted",
    "missing chaos network-matrix command assignment was accepted",
    "bare client matrix summary marker was accepted",
    "client matrix summary without source=command was accepted",
    "suffixed client matrix summary marker was accepted",
    "mismatched client matrix summary profiles were accepted",
    "embedded blank client matrix summary profile was accepted",
    "duplicate client matrix summary marker was accepted",
    "bare S3 process-crash summary marker was accepted",
    "S3 process-crash summary without source=command was accepted",
    "S3 process-crash summary with wrapper source was accepted",
    "suffixed S3 process-crash summary marker was accepted",
    "duplicate S3 process-crash summary marker was accepted",
    "placeholder S3 process-crash bucket was accepted",
    "stale S3 process-crash replacement offset was accepted",
    "duplicate S3 process-crash summary field was accepted",
    "unknown S3 process-crash summary field was accepted",
    "missing MinIO multipart-edge command gate was accepted",
    "missing MinIO pagination command gate was accepted",
    "bare S3 provider profile output marker was accepted",
    "legacy S3 provider profile output marker was accepted",
    "missing S3 provider profile output marker command source was accepted",
    "mismatched S3 provider profile output marker command source was accepted",
    "placeholder S3 provider profile output marker was accepted",
    "missing S3 provider endpoint provenance was accepted",
    "global S3 provider endpoint provenance was rejected",
    "global S3 provider settings provenance was rejected",
    "invalid S3 provider scheme provenance was accepted",
    "invalid S3 provider path-style provenance was accepted",
    "missing explicit non-minio S3 provider setting was accepted",
    "blank S3 provider endpoint used global release-evidence fallback",
    "blank S3 provider scheme used global release-evidence fallback",
    "blank S3 provider enable used global release-evidence fallback",
    "mismatched S3 provider global fallback output was accepted",
    "mismatched S3 provider settings output was accepted",
    "mismatched S3 provider endpoint output was accepted",
    "duplicate S3 provider profile output marker was accepted",
    "bare S3 provider outage marker was accepted",
    "unverified S3 provider outage marker was accepted",
    "missing S3 provider outage detail marker was accepted",
    "mismatched S3 provider outage detail endpoint was accepted",
    "unverified S3 provider outage detail marker was accepted",
    "missing S3 provider outage detail command source was accepted",
    "mismatched S3 provider outage detail command source was accepted",
    "duplicate S3 provider outage detail marker was accepted",
    "bare S3 provider process-crash marker was accepted",
            "unverified S3 provider process-crash marker was accepted",
            "missing S3 provider process-crash detail marker was accepted",
            "mismatched S3 provider process-crash detail bucket was accepted",
            "stale S3 provider process-crash detail offset was accepted",
            "missing S3 provider process-crash detail command source was accepted",
            "mismatched S3 provider process-crash detail command source was accepted",
            "duplicate S3 provider process-crash detail marker was accepted",
            "invalid S3 outage enable provenance was not reported",
    "bare S3 provider live-suite marker was accepted",
    "missing S3 provider live-suite command source was accepted",
    "mismatched S3 provider live-suite command source was accepted",
    "bare S3 provider list-pagination marker was accepted",
    "missing S3 provider matrix required-profile command assignment was accepted",
    "mismatched S3 provider matrix selected-profile command assignment was accepted",
    "missing S3 provider process-crash enable command assignment was accepted",
    "mismatched S3 provider multipart-edge enable command assignment was accepted",
    "global S3 provider enable command provenance was rejected",
    "unverified S3 provider multipart-edge marker was accepted",
    "bare S3 provider multipart-fault marker was accepted",
    "missing S3 provider multipart-fault marker command source was accepted",
    "mismatched S3 provider multipart-fault marker command source was accepted",
    "duplicate S3 provider multipart-fault marker was accepted",
    "missing S3 provider multipart-fault detail marker was accepted",
    "mismatched S3 provider multipart-fault detail bucket was accepted",
    "unverified S3 provider multipart-fault detail was accepted",
    "missing S3 provider multipart-fault command source was accepted",
    "mismatched S3 provider multipart-fault command source was accepted",
    "duplicate S3 provider multipart-fault detail marker was accepted",
    "detached S3 provider live-suite marker was accepted",
    "detached S3 provider subprofile marker was accepted",
    "detached S3 provider profile block was accepted",
    "detached S3 provider multipart-fault detail marker was accepted",
    "misattributed S3 provider profile marker block was accepted",
    "bare S3 provider matrix summary marker was accepted",
    "S3 provider matrix summary without source=command was accepted",
    "suffixed S3 provider matrix summary marker was accepted",
    "mismatched S3 provider matrix summary profiles were accepted",
    "embedded blank S3 provider matrix summary profile was accepted",
    "duplicate S3 provider matrix summary marker was accepted",
    "bare Docker E2E chaos summary marker was accepted",
    "Docker E2E chaos summary without source=command was accepted",
    "Docker E2E chaos summary with wrapper source was accepted",
    "suffixed Docker E2E chaos summary marker was accepted",
    "mismatched Docker E2E chaos summary phases were accepted",
    "embedded blank Docker E2E chaos summary phase was accepted",
    "duplicate Docker E2E chaos summary marker was accepted",
    "bare Docker E2E load/scale summary marker was accepted",
    "Docker E2E load/scale summary without source=command was accepted",
    "Docker E2E load/scale summary with wrapper source was accepted",
    "suffixed Docker E2E load/scale summary marker was accepted",
    "mismatched Docker E2E load/scale summary phases were accepted",
    "embedded blank Docker E2E load/scale summary phase was accepted",
    "duplicate Docker E2E load/scale summary marker was accepted",
    "duplicate Docker E2E results line was accepted",
    "detached duplicate Docker E2E results marker was accepted",
    "duplicate Docker E2E title marker was accepted",
    "duplicate Docker E2E chaos section marker was accepted",
    "duplicate Docker E2E load/scale section marker was accepted",
    "invalid E2E fixture enable provenance was not reported",
    "string AutoMQ-complete flag was accepted",
    "missing AutoMQ-complete flag was accepted",
    "catalog-blocked AutoMQ-complete evidence was not rejected",
    "AutoMQ-complete evidence with elided unsupported surfaces was accepted",
    "string blocking release flag was accepted",
    "placeholder release evidence manifest path was accepted",
    "non-standard JSON release evidence manifest was accepted",
    "duplicate-key release evidence manifest was accepted",
    "unknown current git commit was not reported",
    "unknown tracked worktree status was not reported",
    "commit mismatch was not reported",
    "dirty tracked worktree was not reported",
    "non-object release evidence manifest was accepted",
    "malformed release evidence commit was accepted",
    "unknown release evidence manifest field was accepted",
    "missing release evidence environment object was accepted",
    "non-list release evidence commands were accepted",
    "unknown release evidence command field was accepted",
    "failed release evidence command exit code was accepted",
    "JSON boolean environment run gate was accepted",
    "placeholder integer environment variable was not reported",
    "malformed integer environment variable was not reported",
    "JSON integer environment variable was accepted",
    "placeholder S3 string provenance was not reported",
    "angle-bracket placeholder S3 string provenance was not reported",
    "JSON S3 string provenance was accepted",
    "invalid S3 scheme provenance was accepted",
    "negative phase-index environment variable was accepted",
    "placeholder E2E fixture load records was accepted",
    "negative E2E fixture load records was accepted",
    "invalid E2E fixture action was accepted",
    "duplicate gate environment assignment was accepted",
    "missing required gate manifest assignment was accepted",
    "matching required gate manifest assignment was rejected",
    "mismatched required gate manifest assignment was accepted",
    "embedded blank required target value was not reported",
    "blank S3 provider selector value was not reported",
    "embedded blank S3 provider selector value was not reported",
    "blank fixture-backed E2E load/scale selector was accepted",
    "malformed integer command assignment was accepted",
    "uses placeholder value in command string",
    "placeholder command path was not reported",
    "angle-bracket placeholder command value was not reported",
    "blank E2E fixture action used global release-evidence fallback",
    "placeholder client enable-go provenance was not reported",
    "invalid profile enable-go provenance was not reported",
    "placeholder S3 multipart-fault hook was not reported",
    "missing S3 multipart-fault hook provenance was not reported",
    "blank S3 multipart-fault release-evidence hook used global fallback",
    "missing S3 outage hook provenance was not reported",
    "placeholder S3 outage hook was not reported",
    "blank S3 outage release-evidence hook used global fallback",
    "missing S3 provider outage enable command assignment was accepted",
    "missing S3 provider list-pagination enable command assignment was accepted",
    "missing S3 provider multipart-edge enable command assignment was accepted",
    "missing S3 provider multipart-fault enable command assignment was accepted",
    "missing S3 outage enable provenance was not reported",
    "missing S3 process-crash enable provenance was not reported",
    "missing S3 list-pagination enable provenance was not reported",
    "missing S3 multipart-edge enable provenance was not reported",
    "bare E2E chaos phase detail marker was accepted",
    "stale E2E chaos phase detail marker was accepted",
    "mismatched E2E chaos phase expectation was accepted",
    "mismatched E2E chaos phase observed result was accepted",
    "missing E2E chaos phase source was accepted",
    "mismatched E2E chaos phase source was accepted",
    "duplicate E2E chaos phase detail marker was accepted",
    "detached E2E chaos phase detail marker was accepted",
    "bare E2E load/scale phase detail marker was accepted",
    "unverified E2E load/scale phase detail marker was accepted",
    "mismatched E2E load/scale phase source was accepted",
    "missing E2E load/scale phase command source was accepted",
    "mismatched E2E load/scale phase command source was accepted",
    "hook-owned E2E load/scale marker with fixture action was accepted",
    "duplicate E2E load/scale phase detail marker was accepted",
    "detached E2E load/scale phase detail marker was accepted",
    "fixture-backed E2E load/scale marker without action was accepted",
    "fixture-backed E2E load marker without load_records was accepted",
    "fixture-backed E2E non-load marker with load_records was accepted",
    "bare Docker E2E results line was accepted",
    "failed Docker E2E results line was accepted",
    "detached Docker E2E results line was accepted",
    "embedded Docker E2E title marker was accepted",
    "embedded Docker E2E results marker was accepted",
    "line-aware broker chaos skip marker was not reported",
    "embedded broker chaos skip marker was treated as a skip",
    "skipped MinIO Zig test output was not rejected",
    "embedded MinIO test-count marker was accepted",
    "duplicate MinIO test-count marker was accepted",
    "missing Zig Build Summary output was not reported",
    "failed Zig Build Summary output was accepted",
    "failed Zig test-count Build Summary output was accepted",
    "mixed failed/successful Zig Build Summary output was accepted",
    "duplicate successful Zig Build Summary output was accepted",
    "negated Zig success output was accepted",
    "wrong Zig build-step success output was accepted",
    "unpinned Zig release command was not rejected",
    "echoed Zig command text satisfied release command matching",
    "embedded Zig output marker command was accepted",
    "embedded live harness output marker command was accepted",
    "failure-masked Zig command was accepted",
    "piped Zig command was accepted",
    "redirected Zig command was accepted",
    "combined-redirected Zig command was accepted",
    "backgrounded Zig command was accepted",
    "subshell-wrapped Zig command was accepted",
    "command-substitution Zig command was accepted",
    "prefixed Zig command segment was accepted",
    "suffixed Zig command segment was accepted",
    "extra compose command segment was accepted",
    "detached gate environment satisfied release command matching",
    "overwritten gate environment satisfied release command matching",
    "newline-detached gate environment was accepted",
    "quoted gate environment assignment was accepted",
    "escaped gate environment assignment was accepted",
    "reversed compose marker satisfied release command matching",
    "semicolon compose marker satisfied release command matching",
    "missing required environment variable was not reported",
    "missing chaos required scenario was not reported",
    "release criteria required command block must not contain placeholder paths",
    "release criteria required command block must not contain angle-bracket placeholders",
    "release criteria required command block mismatch was accepted",
    "release criteria required command block duplicate assignment was accepted",
    "missing required command was not reported",
    "duplicate required command entry was accepted",
    "missing observability static audit was not reported",
    "missing protocol static audit was not reported",
    "missing build static audit was not reported",
    "missing static audit output marker was not reported",
    "prefixed static audit output marker was accepted",
    "suffixed static audit output marker was accepted",
    "duplicate static audit output marker was accepted",
    "missing compose config echo command was not reported",
    "missing compose config output marker was not reported",
    "suffixed compose config output marker was accepted",
    "duplicate compose config output marker was accepted",
    "missing local benchmark output marker was not reported",
    "embedded local benchmark output marker was accepted",
    "missing live-S3 manifest endpoint provenance was accepted",
    "missing live-S3 scheme command assignment was accepted",
    "embedded live-S3 benchmark output marker was accepted",
    "missing comparative benchmark target output was not reported",
    "missing comparative benchmark row was not reported",
    "embedded comparative benchmark table header was accepted",
    "embedded comparative benchmark row label was accepted",
    "missing comparative benchmark table target column was accepted",
    "duplicate comparative benchmark table target column was accepted",
    "unknown comparative benchmark table target column was accepted",
    "reordered comparative benchmark table target columns were accepted",
    "missing comparative benchmark table ratio column was accepted",
    "duplicate comparative benchmark table ratio column was accepted",
    "unknown comparative benchmark table ratio column was accepted",
    "reordered comparative benchmark table ratio columns were accepted",
    "interleaved comparative benchmark row cells were accepted",
    "missing comparative benchmark ratio cell was accepted",
    "malformed comparative benchmark ratio cell was accepted",
    "zero comparative benchmark ratio cell was accepted",
    "embedded comparative target label was accepted",
    "reordered comparative target labels were accepted",
    "embedded comparative pass marker was accepted",
    "missing comparative benchmark trend baseline was accepted",
    "mismatched comparative benchmark trend baseline was accepted",
    "missing live harness output marker was not reported",
    "missing required client profile output marker was not reported",
    "malformed client bootstrap provenance was accepted",
    "malformed client profile bootstrap output was accepted",
    "missing required client tool output marker was not reported",
    "missing required client semantic output marker was not reported",
    "missing exact client semantic token was not reported",
    "unrecognized client probe semantic marker was accepted",
    "missing chaos required scenario output marker was not reported",
    "missing chaos required phase output marker was not reported",
    "network phase marker satisfied chaos scenario summary",
    "blank chaos live-S3 endpoint fallback was accepted",
    "blank chaos live-S3 TLS CA fallback was accepted",
    "missing KRaft network partition matrix output was not reported",
    "detached KRaft network partition marker was accepted",
    "missing S3 provider outage-profile command assignment was accepted",
    "mismatched S3 provider multipart-fault command assignment was accepted",
    "missing S3 provider profile coverage marker was not reported",
    "missing S3 provider live-suite output marker was not reported",
    "missing Docker E2E required-chaos command assignment was accepted",
    "mismatched Docker E2E chaos matrix command assignment was accepted",
    "mismatched Docker E2E load/scale command assignment was accepted",
    "missing E2E required phase output marker was not reported",
    "placeholder environment variable was not reported",
    "empty comma-separated environment variable was not reported",
    "duplicate comma-separated environment variable was not reported",
    "missing S3 provider selector provenance was not reported",
    "client profile selector subset mismatch was not reported",
    "colliding KRaft selector phase tokens were not reported",
    "blank chaos network expectation used global release-evidence fallback",
    "blank KRaft network expectation used global release-evidence fallback",
    "blank E2E chaos expectation used global release-evidence fallback",
    "missing chaos network hook provenance was not reported",
    "blank chaos network release-evidence hook used global fallback",
    "malformed KRaft hook command was not reported",
    "blank KRaft network release-evidence hook used global fallback",
    "blank E2E chaos release-evidence hook used global fallback",
    "blank E2E load-scale release-evidence hook used global fallback",
    "disabled S3 outage enable provenance was not reported",
    "fixture-backed E2E command without fixture flag was accepted",
    "invalid benchmark enforce-gates provenance was not reported",
    "missing comparative target command assignment was not reported",
    "mismatched comparative target command assignment was not reported",
    "missing trend baseline command assignment was not reported",
    "detached trend baseline command assignment was not reported",
    "mismatched trend baseline command assignment was not reported",
    "overwritten trend baseline command assignment was not reported",
    "current results artifact was accepted as trend baseline",
    "current results artifact command assignment was accepted",
    "missing comparative threshold command assignment was accepted",
    "non-finite benchmark threshold environment was not reported",
    "negative benchmark threshold command assignment was accepted",
    "disabled trend requirement was not reported",
    "missing ZMQ comparative target was not reported",
    "missing comparative baseline target was not reported",
    "S3 profile coverage mismatch was not reported",
    "client profile coverage mismatch was not reported",
    "missing required client tool was not reported",
    "unknown required client tool was not reported",
    "missing required client semantic was not reported",
    "unknown required client semantic was not reported",
    "missing E2E load/scale phases were not reported",
    "missing E2E cross-broker chaos coverage was not reported",
    "missing command output was not reported",
    "captured skip output was not reported",
    "extra known unsupported surface bullet was accepted",
    "known unsupported surface bullet removal fixture did not apply",
    "missing known unsupported surface bullet was accepted",
    "known unsupported surface duplicate fixture did not apply",
    "duplicate known unsupported surface bullet was accepted",
    "known unsupported surface status fixture did not apply",
    "misclassified known unsupported surface bullet status was accepted",
    "missing unsupported surfaces list was accepted",
    "non-list unsupported surfaces accounting was accepted",
    "non-object release evidence environment was accepted",
    "invalid release evidence environment variable name was accepted",
    "JSON boolean release evidence environment value was accepted",
    "blank release evidence environment value was accepted",
    "missing unsupported surface accounting was not reported",
    "catch-all unsupported surface accounting was accepted",
    "extra unsupported surface accounting was accepted",
    "malformed unsupported surface entry was not reported",
    "string unsupported surface entry was not rejected",
    "missing unsupported surface evidence was not reported",
    "unknown unsupported surface field was accepted",
    "unsupported surface evidence was allowed to hide a vague surface name",
    "placeholder unsupported surface notes were accepted",
    "blank unsupported surface mitigation was accepted",
    "empty unsupported surface notes list was accepted",
    "placeholder unsupported surface note item was accepted",
    "non-string unsupported surface mitigation was accepted",
    "non-string unsupported surface note was accepted",
    "placeholder unsupported surface status was not reported",
    "vague unsupported surface status was accepted",
    "unsupported AutoMQ-complete evidence was not rejected",
    "missing AutoMQ-complete flag was accepted",
    "blocking data-loss flag was not rejected",
    "local benchmark summary without source=command was accepted",
    "local benchmark summary with wrapper source was accepted",
    "suffixed local benchmark summary marker was accepted",
    "duplicate local benchmark summary marker was accepted",
    "suffixed local benchmark completion marker was accepted",
    "bare local benchmark request-volume marker was accepted",
    "bare local benchmark memory marker was accepted",
    "detached local benchmark detail markers were accepted",
    "duplicate local benchmark request-volume marker was accepted",
    "duplicate local benchmark memory marker was accepted",
    "bare live-S3 benchmark provider marker was accepted",
    "live-S3 benchmark summary without source=command was accepted",
    "live-S3 benchmark summary with wrapper source was accepted",
    "suffixed live-S3 benchmark summary marker was accepted",
    "duplicate live-S3 benchmark summary marker was accepted",
    "suffixed live-S3 benchmark completion marker was accepted",
    "legacy live-S3 benchmark provider marker was accepted",
    "missing live-S3 benchmark endpoint provenance was accepted",
    "missing live-S3 benchmark endpoint command assignment was accepted",
    "missing live-S3 benchmark port command assignment was accepted",
    "missing live-S3 benchmark bucket command assignment was accepted",
    "missing live-S3 benchmark scheme command assignment was accepted",
    "missing live-S3 benchmark region command assignment was accepted",
    "missing live-S3 benchmark path-style command assignment was accepted",
    "mismatched live-S3 benchmark provider output was accepted",
    "live-S3 benchmark settings provenance was rejected",
    "invalid live-S3 benchmark scheme provenance was accepted",
    "invalid live-S3 benchmark path-style provenance was accepted",
    "mismatched live-S3 benchmark settings output was accepted",
    "missing live-S3 benchmark iteration command assignment was accepted",
    "missing live-S3 benchmark payload-size command assignment was accepted",
    "mismatched live-S3 benchmark iteration command assignment was accepted",
    "mismatched live-S3 benchmark payload-size command assignment was accepted",
    "bare live-S3 benchmark request-volume marker was accepted",
    "bare live-S3 benchmark put marker was accepted",
    "bare live-S3 benchmark get marker was accepted",
    "detached live-S3 benchmark detail markers were accepted",
    "duplicate live-S3 benchmark provider marker was accepted",
    "duplicate live-S3 benchmark put marker was accepted",
    "duplicate live-S3 benchmark get marker was accepted",
    "duplicate live-S3 benchmark request-volume marker was accepted",
    "bare comparative benchmark thresholds were accepted",
    "detached comparative benchmark table header was accepted",
    "detached comparative target label was accepted",
    "suffixed comparative benchmark comparison line was accepted",
    "detached comparative benchmark thresholds were accepted",
    "mismatched comparative benchmark trend thresholds were accepted",
    "detached comparative benchmark trend thresholds were accepted",
    "detached comparative benchmark trend baseline was accepted",
    "suffixed comparative benchmark gate marker was accepted",
    "invalid trend requirement was not reported",
    "comparative benchmark enforce-gates command provenance was rejected",
    "missing comparative benchmark enforce-gates command assignment was accepted",
    "mismatched comparative benchmark enforce-gates command assignment was accepted",
    "missing comparative max-p50 threshold command assignment was accepted",
    "missing comparative max-p99 threshold command assignment was accepted",
    "missing comparative max-error-rate threshold command assignment was accepted",
    "missing comparative trend-throughput threshold command assignment was accepted",
    "missing comparative trend-p50 threshold command assignment was accepted",
    "missing comparative trend-p99 threshold command assignment was accepted",
    "detached comparative benchmark pass marker was accepted",
    "missing comparative benchmark latency row was accepted",
    "detached comparative benchmark metric row was accepted",
    "malformed comparative benchmark measurement was accepted",
    "zero comparative benchmark measurement was accepted",
    "interleaved comparative benchmark row cells were accepted",
    "missing comparative benchmark ratio cell was accepted",
    "malformed comparative benchmark ratio cell was accepted",
    "zero comparative benchmark ratio cell was accepted",
    "duplicate comparative benchmark comparison line was accepted",
    "duplicate comparative target label was accepted",
    "duplicate comparative benchmark required target was accepted",
    "duplicate comparative benchmark table header was accepted",
    "duplicate comparative benchmark metric row was accepted",
    "duplicate comparative benchmark thresholds line was accepted",
    "duplicate comparative benchmark trend thresholds line was accepted",
    "duplicate comparative benchmark trend baseline line was accepted",
    "duplicate comparative benchmark gate result line was accepted",
    "missing comparative benchmark results artifact line was accepted",
    "mismatched comparative benchmark results artifact path was accepted",
    "detached comparative benchmark results artifact line was accepted",
    "duplicate comparative benchmark results artifact line was accepted",
    "missing comparative benchmark profile marker was accepted",
    "bare comparative benchmark profile marker was accepted",
    "unknown comparative benchmark profile marker field was accepted",
    "duplicate comparative benchmark profile marker field was accepted",
    "blank comparative benchmark profile marker field was accepted",
    "wrapper comparative benchmark profile marker was accepted",
    "mismatched comparative benchmark profile required targets were accepted",
    "mismatched comparative benchmark profile selected targets were accepted",
    "mismatched comparative benchmark profile result targets were accepted",
    "mismatched comparative benchmark profile iterations were accepted",
    "detached comparative benchmark profile marker was accepted",
    "uppercase comparative target was accepted",
    "missing chaos required-scenarios command assignment was accepted",
    "bare chaos sigkill-restart marker was accepted",
    "unverified chaos sigkill-restart marker was accepted",
    "bare chaos slow-partial-client marker was accepted",
    "missing chaos detail command source was accepted",
    "mismatched chaos detail command source was accepted",
    "bare chaos clock-skewed-records marker was accepted",
    "bare chaos s3-outage marker was accepted",
    "unverified chaos s3-outage marker was accepted",
    "detached chaos scenario detail marker was accepted",
    "duplicate chaos scenario detail marker was accepted",
    "bare chaos live-s3-outage marker was accepted",
    "missing chaos live-S3 provider marker was accepted",
    "mismatched chaos live-S3 provider marker was accepted",
    "mismatched chaos live-S3 provider source was accepted",
    "duplicate chaos live-S3 provider marker was accepted",
    "missing chaos live-S3 hook provenance was accepted",
    "missing chaos live-S3 command provenance was accepted",
    "invalid chaos live-S3 path-style provenance was accepted",
    "detached chaos live-S3 detail marker was accepted",
    "bare chaos network-partition phase detail marker was accepted",
    "stale chaos network-partition phase detail marker was accepted",
    "mismatched chaos network phase source was accepted",
    "mismatched chaos network-partition expectation was accepted",
    "mismatched chaos network-partition observed result was accepted",
    "duplicate chaos network-partition phase detail marker was accepted",
    "detached chaos network-partition phase detail marker was accepted",
    "chaos network summary without source=command was accepted",
    "chaos network summary with wrapper source was accepted",
    "suffixed chaos network summary marker was accepted",
    "duplicate chaos network summary marker was accepted",
    "missing KRaft required-network-phases command assignment was accepted",
    "missing KRaft network-matrix command assignment was accepted",
    "mismatched KRaft network command assignment was accepted",
    "mismatched KRaft network partition summary phases were accepted",
    "empty KRaft network partition summary phases were accepted",
    "embedded blank KRaft network partition summary phase was accepted",
    "bare KRaft network partition phase detail marker was accepted",
    "stale KRaft network partition phase marker was accepted",
    "mismatched KRaft network phase source was accepted",
    "mismatched KRaft network partition expectation was accepted",
    "mismatched KRaft network partition observed result was accepted",
    "duplicate KRaft network partition phase detail marker was accepted",
    "detached KRaft network partition phase detail marker was accepted",
    "KRaft failover summary without source=command was accepted",
    "KRaft failover summary with wrapper source was accepted",
    "suffixed KRaft failover summary marker was accepted",
    "duplicate KRaft failover summary marker was accepted",
    "missing KRaft reassignment summary was accepted",
    "detached KRaft reassignment summary was accepted",
    "unverified KRaft reassignment summary was accepted",
    "missing KRaft coordinator summary was accepted",
    "unverified KRaft coordinator summary was accepted",
    "unverified KRaft follower rejection summary was accepted",
    "unverified KRaft admin summary was accepted",
    "unverified KRaft group summary was accepted",
    "missing KRaft controller unsupported cases summary was accepted",
    "incomplete KRaft controller unsupported cases summary was accepted",
    "missing KRaft broker non-broker cases summary was accepted",
    "incomplete KRaft broker non-broker cases summary was accepted",
    "placeholder KRaft AutoMQ summary was accepted",
    "negative KRaft AutoMQ summary id was accepted",
    "mismatched KRaft transaction count was accepted",
    "duplicate KRaft summary field was accepted",
    "unknown KRaft summary field was accepted",
    "missing Docker E2E chaos-matrix command assignment was accepted",
    "missing Docker E2E required-load-scale command assignment was accepted",
    "missing Docker E2E load-scale-matrix command assignment was accepted",
    "Live S3 provider endpoint={s}:{d} bucket={s} scheme={s} region={s} path_style={s}",
    "Live S3 request volume   puts={d} gets={d} requests/MiB={d:.2}",
    "bare chaos harness summary marker was accepted",
    "chaos harness summary without source=command was accepted",
    "chaos harness summary with wrapper source was accepted",
    "suffixed chaos harness summary marker was accepted",
    "mismatched chaos harness summary scenarios were accepted",
    "unrequired chaos harness summary scenario was accepted",
    "embedded blank chaos harness summary scenario was accepted",
    "duplicate chaos harness summary marker was accepted",
    "malformed client OAuth fixture was accepted",
    "future-valid client OAuth-negative fixture was accepted",
    "Duplicate objects for the same known surface are rejected",
    "outside the verifier catalog",
    "status class must match the surface",
    "duplicate unsupported surface accounting was accepted",
    "misclassified broker-only stateless surface status was accepted",
    "misclassified live CI surface status was accepted",
    "missing blocking release flag was accepted",
    "blocking advertised stub API flag was not rejected",
    "blocking untriaged durability flag was not rejected",
    "missing client profile tool provenance was not reported",
    "auto client profile tools were accepted",
    "duplicate client profile tool provenance was accepted",
    "duplicate client profile semantic provenance was accepted",
    "missing client profile semantic provenance was not reported",
    "missing client profile version provenance was not reported",
    "floating go-kafka module provenance was not reported",
    "implicit-latest go-kafka module provenance was not reported",
    "missing client Python executable provenance was not reported",
    "missing client security protocol provenance was not reported",
    "invalid client security protocol provenance was accepted",
    "invalid client SASL mechanism provenance was accepted",
    "missing client OAuth fixture provenance was not reported",
    "missing client OAuth-negative fixture provenance was not reported",
)

BENCHMARK_MAIN_LIVE_S3_PREFLIGHT_CONTRACT = (
    "fn requireLiveS3Setting(",
    "fn hasAngleBracketPlaceholder(",
    "hasAngleBracketPlaceholder(trimmed);",
    "live-S3 benchmark settings reject blank and placeholder values",
    " <host>:9443 ",
    "fn liveS3Port(",
    "fn liveS3Scheme(",
    "fn liveS3Bool(",
    "fn benchmarkSetting(",
    "fn benchmarkUsize(",
    "fn envF64(",
    "live-S3 benchmark setting {s} must not be blank or placeholder",
    "live-S3 benchmark setting {s} must be http or https",
    "live-S3 benchmark setting {s} must be true or false",
    "benchmark setting {s} must not be blank or placeholder",
    "benchmark setting {s} must be a positive integer",
    "benchmark setting {s} must be a finite non-negative float",
    "try liveS3Bool(\"ZMQ_RUN_BENCH_LIVE_S3\", false)",
    "ok: local benchmark gate source=command",
    "ok: live-S3 benchmark gate source=command",
    "try liveS3Bool(\"ZMQ_S3_SKIP_ENSURE_BUCKET\", false)",
    "try envF64(\"ZMQ_BENCH_S3_WAL_MAX_REQUESTS_PER_MIB\", 1024.0)",
    "try envF64(\"ZMQ_BENCH_S3_WAL_MAX_REBUILD_MS\", 10_000.0)",
    "try benchmarkUsize(\"ZMQ_BENCH_LIVE_S3_ITERATIONS\", 20)",
    "try benchmarkUsize(\"ZMQ_BENCH_LIVE_S3_PAYLOAD_BYTES\", 64 * 1024)",
    ".host = try requireLiveS3Setting(\"ZMQ_S3_ENDPOINT\"",
    ".port = try liveS3Port(\"ZMQ_S3_PORT\", 9000)",
    ".bucket = try requireLiveS3Setting(\"ZMQ_S3_BUCKET\"",
    ".region = try requireLiveS3Setting(\"ZMQ_S3_REGION\"",
    ".tls_ca_file = try optionalLiveS3Setting(\"ZMQ_S3_TLS_CA_FILE\")",
)

MINIO_S3_LIVE_BOOL_PREFLIGHT_CONTRACT = (
    "fn requireMinioSetting(",
    "fn hasAngleBracketPlaceholder(",
    "hasAngleBracketPlaceholder(trimmed);",
    "fn optionalMinioSetting(",
    "fn parseMinioPort(",
    "fn parseEnvBool(",
    "fn envBool(",
    "error.InvalidMinioSetting",
    "error.InvalidMinioBoolean",
    "ZMQ_RUN_MINIO_TESTS",
    "ZMQ_S3_ENDPOINT",
    "ZMQ_S3_BUCKET",
    "ZMQ_S3_ACCESS_KEY",
    "ZMQ_S3_SECRET_KEY",
    "ZMQ_S3_REGION",
    "ZMQ_S3_TLS_CA_FILE",
    "ZMQ_S3_PATH_STYLE",
    "ZMQ_S3_SKIP_ENSURE_BUCKET",
    "ZMQ_S3_REQUIRE_MULTIPART_EDGE",
    "ZMQ_S3_REQUIRE_LIST_PAGINATION",
    "<host>:9000",
    "MinIO live settings reject blank and placeholder values",
    "MinIO live boolean and port settings fail closed",
)

STARTUP_CONFIG_FAIL_CLOSED_SOURCE_CONTRACTS = (
    (
        "src/config.zig",
        (
            "pub fn firstCommaSeparatedValueStrict(",
            "if (trimmed.len == 0) return error.InvalidConfigString;",
            "if (try cfg.getNonBlankStringStrict(\"log.dirs\")) |d| config.data_dir = try firstCommaSeparatedValueStrict(d);",
            "if (try cfg.getNonBlankStringStrict(\"cluster.id\")) |id| config.cluster_id = id;",
            "pub fn parseSecurityProtocolStrict(",
            "pub fn parseTlsClientAuthStrict(",
            "pub fn validateSaslUsersStrict(",
            "pub fn validateSuperUsersStrict(",
            "pub fn validateSaslMechanismsStrict(",
            "pub fn parseListenerEndpointStrict(",
            "pub fn firstListenerEndpointStrict(",
            "pub fn validateListenerNamesStrict(",
            "fn validateListenerEndpointNameUniqueBeforeStrict(",
            "pub fn firstListenerEndpointMatchingNamesStrict(",
            "pub fn firstListenerEndpointExcludingNamesStrict(",
            "pub fn listenerEndpointForNameStrict(",
            "fn validateListenerSecurityMapNameUniqueBeforeStrict(",
            "pub fn validateListenerSecurityProtocolMapStrict(",
            "pub fn listenerSecurityProtocolTextForNameStrict(",
            "pub fn listenerSecurityProtocolForNameStrict(",
            "pub fn validateListenerSecurityProtocolMapForListenersStrict(",
            "pub fn validateAdvertisedListenersMatchListenersStrict(",
            "pub fn controllerVoterSetContainsNodeIdStrict(",
            "pub fn controllerVoterSetLocalPortMatchesStrict(",
            "_ = try parseSecurityProtocolStrict(p);",
            "_ = try parseTlsClientAuthStrict(a);",
            "try validateSaslUsersStrict(u);",
            "try validateSuperUsersStrict(u);",
            "try validateSaslMechanismsStrict(m);",
            "const listener_endpoints = try cfg.getNonBlankStringStrict(\"listeners\");",
            "if (listener_endpoints) |l| _ = try firstListenerEndpointStrict(l, true);",
            "if (try cfg.getNonBlankStringStrict(\"controller.listener.names\")) |n| try validateListenerNamesStrict(n);",
            "const inter_broker_listener_name = try cfg.getNonBlankStringStrict(\"inter.broker.listener.name\");",
            "const security_inter_broker_protocol = try cfg.getNonBlankStringStrict(\"security.inter.broker.protocol\");",
            "return error.InvalidConfigInterBrokerProtocolConflict;",
            "if (inter_broker_listener_name) |n| {",
            "try validateListenerNameStrict(n);",
            "if (listener_endpoints) |l| _ = try listenerEndpointForNameStrict(l, n, true);",
            "if (security_inter_broker_protocol) |p| {",
            "config.security_protocol = p;",
            "const listener_security_protocol_map = try cfg.getNonBlankStringStrict(\"listener.security.protocol.map\");",
            "if (listener_endpoints) |l| try validateListenerSecurityProtocolMapForListenersStrict(m, l);",
            "if (listener_security_protocol_map) |m| config.security_protocol = try listenerSecurityProtocolTextForNameStrict(m, n);",
            "if (try cfg.getNonBlankStringStrict(\"advertised.listeners\")) |a| {",
            "if (listener_endpoints) |l| try validateAdvertisedListenersMatchListenersStrict(a, l);",
            "ConfigFile comma-separated values reject blank entries",
            "ConfigFile parses strict TLS startup enums",
            "ConfigFile validates SASL startup strings strictly",
            "parseListenerEndpoint parses strict Kafka listeners",
            "parseListenerEndpoint rejects malformed Kafka listeners",
            "try validateListenerSecurityProtocolMapStrict(\"PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL\");",
            "try testing.expectEqualStrings(\"SASL_SSL\", try listenerSecurityProtocolTextForNameStrict(\"PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL\", \"SASL_SSL\"));",
            "try validateListenerSecurityProtocolMapForListenersStrict(\"CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT\", listeners);",
            "try validateAdvertisedListenersMatchListenersStrict(\"PLAINTEXT://broker.example:19092\", listeners);",
            "validateAdvertisedListenersMatchListenersStrict(\"SSL://broker.example:9093\", listeners)",
            "\"PLAINTEXT://localhost:9092,plaintext://localhost:9093\"",
            "\"CONTROLLER,controller\"",
            "\"PLAINTEXT:PLAINTEXT,plaintext:SSL\"",
            "try testing.expect(try controllerVoterSetContainsNodeIdStrict(\"1@controller-1:9093,2@controller-2:9093\", 2));",
            "try testing.expect(try controllerVoterSetLocalPortMatchesStrict(\"1@controller-1:9093,2@controller-2:19093\", 2, 19093));",
            "ConfigFile rejects malformed lines",
            "try testing.expectError(error.InvalidConfigLine, cfg.parse(\"valid=yes\\nno-equals-sign\\n\"));",
            "try testing.expectError(error.InvalidConfigKey, empty_key.parse(\"=no-key\\n\"));",
            "ConfigFile applies strict SASL security settings",
            "ConfigFile applyConfig rejects malformed SASL security settings",
            "ConfigFile applies strict Kafka listener settings",
            "ConfigFile applyConfig rejects malformed Kafka listener settings",
            "ConfigFile applyConfig rejects invalid S3 storage settings",
            "\"security.inter.broker.protocol=SASL_PLAINTEXT\\n\"",
            "\\\\listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT",
            "try testing.expectEqualStrings(\"SSL\", broker_config_listener_security_protocol.security_protocol);",
            "\"sasl.users=alice\\n\"",
            "\"sasl.enabled.mechanisms=GSSAPI\\n\"",
            "\"listeners=PLAINTEXT://localhost\\n\"",
            "\"advertised.listeners=PLAINTEXT://:9092\\n\"",
            "\"listeners=PLAINTEXT://localhost:9092\\ninter.broker.listener.name=SSL\\n\"",
            "\"listeners=PLAINTEXT://localhost:9092,plaintext://localhost:9093\\n\"",
            "\"listeners=PLAINTEXT://localhost:9092\\nadvertised.listeners=SSL://broker.example:9093\\n\"",
            "\"controller.listener.names=CONTROLLER,controller\\n\"",
            "\"controller.listener.names=CONTROLLER,\\n\"",
            "\"listener.security.protocol.map=PLAINTEXT:tls\\n\"",
            "\"listener.security.protocol.map=PLAINTEXT:PLAINTEXT,plaintext:SSL\\n\"",
            "\"listeners=PLAINTEXT://localhost:9092\\nlistener.security.protocol.map=CONTROLLER:PLAINTEXT\\n\"",
            "\"log.dirs=/tmp/zmq-a,,/tmp/zmq-b\\n\"",
            "\"security.protocol=\\n\"",
            "\"security.inter.broker.protocol=\\n\"",
            "\"security.inter.broker.protocol=tls\\n\"",
            "try testing.expectError(error.InvalidConfigInterBrokerProtocolConflict, applyConfig(&broker_config, &cfg_inter_broker_protocol_conflict));",
            "try testing.expectError(error.InvalidConfigSecurityProtocol, applyConfig(&broker_config, &cfg_security_protocol));",
            "try testing.expectError(error.InvalidConfigTlsClientAuth, applyConfig(&broker_config, &cfg_client_auth));",
        ),
    ),
    (
        "src/main.zig",
        (
            "fn parseSecurityProtocol(",
            "fn parseTlsClientAuth(",
            "fn nextRequiredNonBlankArg(",
            "fn configNodeIdStrict(",
            "fn applyConfigNodeIdAliasesStrict(",
            "error.LocalControllerVoterMissing",
            "error.LocalControllerVoterPortMismatch",
            "fn applyConfigBoolStrict(",
            "fn configSaslUsersOrStrict(",
            "fn configSuperUsersOrStrict(",
            "fn configSaslMechanismsOrStrict(",
            "fn configListenerEndpointStrict(",
            "fn configListenerNamesStrict(",
            "fn configListenerNameStrict(",
            "fn configSecurityProtocolStrict(",
            "fn configListenerSecurityProtocolMapStrict(",
            "fn configListenerSecurityProtocolMapCoversListenersStrict(",
            "fn configListenerSecurityProtocolForNameStrict(",
            "fn configAdvertisedListenersMatchListenersStrict(",
            "var cli_advertised_host_set = false;",
            "s3_host = try nextRequiredNonBlankArg(&stdout, &args, \"--s3-endpoint\");",
            "cluster_id = try nextRequiredNonBlankArg(&stdout, &args, \"--cluster-id\");",
            "security_protocol = try nextRequiredNonBlankArg(&stdout, &args, \"--security-protocol\");",
            "cli_advertised_host_set = true;",
            "fn firstLogDirStrict(",
            "config_mod.firstCommaSeparatedValueStrict(raw) catch",
            "sasl_users = try configSaslUsersOrStrict(&stdout, &cfg, \"sasl.users\", sasl_users);",
            "sasl_enabled_mechanisms = try configSaslMechanismsOrStrict(&stdout, &cfg, \"sasl.enabled.mechanisms\", sasl_enabled_mechanisms);",
            "const config_controller_listener_names = try configListenerNamesStrict(&stdout, &cfg, \"controller.listener.names\");",
            "const config_advertised_listeners = try configStringStrict(&stdout, &cfg, \"advertised.listeners\");",
            "const config_inter_broker_listener_name = try configListenerNameStrict(&stdout, &cfg, \"inter.broker.listener.name\");",
            "const config_security_protocol = try configSecurityProtocolStrict(&stdout, &cfg, \"security.protocol\");",
            "const config_inter_broker_security_protocol = try configSecurityProtocolStrict(&stdout, &cfg, \"security.inter.broker.protocol\");",
            "config 'inter.broker.listener.name' and 'security.inter.broker.protocol' cannot both be set",
            "const config_listener_security_protocol_map = try configListenerSecurityProtocolMapStrict(&stdout, &cfg, \"listener.security.protocol.map\");",
            "var config_broker_listener: ?config_mod.ListenerEndpoint = null;",
            "try configListenerSecurityProtocolMapCoversListenersStrict(&stdout, map, listeners);",
            "try configAdvertisedListenersMatchListenersStrict(&stdout, advertised, listeners);",
            "config_broker_listener = if (config_inter_broker_listener_name) |listener_name|",
            "try configNonControllerListenerEndpointTextStrict(&stdout, listeners, listener_names, true)",
            "const listener = try configControllerListenerEndpointTextStrict(&stdout, listeners, listener_names, true);",
            "if (try configStringStrict(&stdout, &cfg, \"advertised.host.name\")) |h| advertised_host = h;",
            "const listener = try parseConfigListenerEndpointTextStrict(&stdout, \"advertised.listeners\", advertised, false);",
            "try applyConfigNodeIdAliasesStrict(&stdout, &cfg, &node_id);",
            "config 'broker.id' and 'node.id' must match when both are set",
            "parseAndRegisterVoters(&ctrl.raft_state, voters_str.?, controller_port, &raft_pool.?)",
            "Failed to persist Raft epoch/vote metadata",
            "return error.ControllerElectionPersistenceFailed;",
            "Failed to start election loop",
            "return error.ElectionLoopStartFailed;",
            "Failed to start metadata client",
            "return error.MetadataClientStartFailed;",
            "Failed to start controller server",
            "return error.ControllerServerStartFailed;",
            "if (!(try config_mod.controllerVoterSetContainsNodeIdStrict(voters, raft.node_id))) return error.LocalControllerVoterMissing;",
            "if (!(try config_mod.controllerVoterSetLocalPortMatchesStrict(voters, raft.node_id, local_controller_port))) return error.LocalControllerVoterPortMismatch;",
            "log.dirs/--data-dir must contain comma-separated nonblank directories",
            "Broker.deriveReplicaDirectoryIds(data_dir) catch",
            "const tls_protocol = parseSecurityProtocol(security_protocol) catch",
            "security_protocol = p;",
            "security_protocol = try configListenerSecurityProtocolForNameStrict(&stdout, map, listener.name);",
            "Failed to initialize TLS: {s}",
            "return error.TlsInitializationFailed;",
            "const client_auth = parseTlsClientAuth(tls_client_auth_str) catch",
            "if (tls_protocol == .sasl_plaintext or tls_protocol == .sasl_ssl) sasl_enabled = true;",
            ".sasl_enabled = sasl_enabled,",
            ".sasl_users = sasl_users,",
            ".sasl_enabled_mechanisms = sasl_enabled_mechanisms,",
            "security.protocol/--security-protocol must be plaintext, ssl, sasl_plaintext, or sasl_ssl",
            "ssl.client.auth/--tls-client-auth must be none, requested, or required",
        ),
    ),
    (
        "src/broker/handler.zig",
        (
            "pub fn deriveReplicaDirectoryIds(log_dirs: ?[]const u8) !ReplicaDirectoryIdSet",
            "if (trimmed.len == 0) return error.InvalidLogDirs;",
            "if (result.len >= max_local_replica_directories) return error.InvalidLogDirs;",
            "if (result.len == 0) return error.InvalidLogDirs;",
            "Broker deriveReplicaDirectoryIds rejects embedded blank log.dirs entries",
        ),
    ),
)

STARTUP_CONFIG_FAIL_CLOSED_DOC_FRAGMENTS = (
    "Startup configuration must fail closed",
    "properties lines",
    "empty property keys",
    "embedded-blank",
    "`log.dirs`/`--data-dir` entries",
    "blank CLI string",
    "blank S3 string settings",
    "malformed SASL security settings",
    "invalid Kafka listener endpoints",
    "duplicate listener names across listener lists/maps",
    "selected listener-map security protocols",
    "derives the executable broker security protocol from",
    "advertised listener names must match configured listeners",
    "`advertised.listeners` names that do not match configured `listeners`",
    "local voter endpoint does not match",
    "local voter at a different controller listener port",
    "self-election persistence failures",
    "critical startup thread failures",
    "invalid `security.protocol`",
    "invalid `security.inter.broker.protocol`",
    "mutually exclusive",
    "TLS context initialization failures",
    "invalid `ssl.client.auth`",
)

FORBIDDEN_WALL_CLOCK_DEADLINE_PATTERNS = (
    ("deadline assignment", re.compile(r"\bdeadline\s*=\s*time\.time\s*\(")),
    (
        "wall-clock deadline loop",
        re.compile(r"\bwhile\s+time\.time\s*\(\s*\)\s*<\s*deadline\b"),
    ),
    ("remaining deadline", re.compile(r"\bdeadline\s*-\s*time\.time\s*\(\s*\)")),
    ("start assignment", re.compile(r"\bstart\s*=\s*time\.time\s*\(")),
    ("total start assignment", re.compile(r"\btotal_start\s*=\s*time\.time\s*\(")),
    ("elapsed start", re.compile(r"\btime\.time\s*\(\s*\)\s*-\s*start\b")),
    ("elapsed total start", re.compile(r"\btime\.time\s*\(\s*\)\s*-\s*total_start\b")),
)


def read(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def find_step_var(build_zig, step_name):
    pattern = re.compile(
        rf'\bconst\s+(?P<var>[A-Za-z0-9_]+)\s*=\s*b\.step\("{re.escape(step_name)}"',
    )
    match = pattern.search(build_zig)
    if not match:
        raise AssertionError(f"missing build step {step_name}")
    return match.group("var")


def find_python_command_var(build_zig, path, self_test):
    pattern = re.compile(
        rf'\bconst\s+(?P<var>[A-Za-z0-9_]+)\s*=\s*b\.addSystemCommand'
        rf'\(&\.\{{(?P<args>.*?)\}}\);',
        re.DOTALL,
    )
    for match in pattern.finditer(build_zig):
        args = match.group("args")
        if f'"python3"' not in args or f'"{path}"' not in args:
            continue
        has_self_test = '"--self-test"' in args
        if has_self_test == self_test:
            return match.group("var")

    suffix = " --self-test" if self_test else ""
    raise AssertionError(f"missing python command: python3 {path}{suffix}")


def assert_depends(build_zig, owner_var, dependency_var, label):
    needle = f"{owner_var}.dependOn(&{dependency_var}.step);"
    if needle not in build_zig:
        raise AssertionError(f"{label} missing dependency {needle}")


def assert_install_dependency(build_zig, command_var, label):
    needle = f"{command_var}.step.dependOn(b.getInstallStep());"
    if needle not in build_zig:
        raise AssertionError(f"{label} missing install dependency {needle}")


def make_target_body(makefile, target):
    marker = f"{target}:"
    start = makefile.find(marker)
    if start < 0:
        raise AssertionError(f"missing Makefile target {target}")
    next_target = re.search(r"(?m)^[A-Za-z0-9_-]+:", makefile[start + len(marker) :])
    if next_target:
        return makefile[start : start + len(marker) + next_target.start()]
    return makefile[start:]


def section_between(text, start_marker, end_marker):
    start = text.find(start_marker)
    if start < 0:
        raise AssertionError(f"missing section marker {start_marker!r}")
    end = text.find(end_marker, start + len(start_marker))
    if end < 0:
        raise AssertionError(f"missing section end marker {end_marker!r}")
    return text[start:end]


def assert_fragments(text, label, fragments):
    missing = [fragment for fragment in fragments if fragment not in text]
    if missing:
        raise AssertionError(
            f"{label} missing required fragments: " + ", ".join(missing)
        )


def assert_release_criteria_pins_compose_images(release_criteria):
    for image in (
        REQUIRED_KAFKA_IMAGE,
        REQUIRED_AUTOMQ_IMAGE,
        REQUIRED_MINIO_IMAGE,
        REQUIRED_MINIO_MC_IMAGE,
    ):
        if image not in release_criteria:
            raise AssertionError(f"release criteria must document pinned image {image}")
    if "must not use `:latest`" not in release_criteria:
        raise AssertionError("release criteria must reject floating compose image tags")


def assert_automq_parity_release_evidence_contract(parity=None):
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        parity,
        "AutoMQ parity release evidence docs",
        (
            "per-required coverage markers",
            "comma-separated coverage",
            "parse to at least one value",
            "blank comma-separated entries",
            "explicit blank selector rejection",
            "angle-bracket placeholders",
            "`<host>`",
            "`<port>`",
            "`<bucket>`",
            "strict JSON manifest parsing",
            "rejects non-standard JSON constants",
            "duplicate JSON object keys",
            "before schema validation",
            "strict JSON parsing in both checked-in schema generators",
            "before generated Zig protocol schemas are written",
            "codegen exits nonzero on schema parse errors",
            "worktree cleanliness cannot be determined",
            "placeholder `ZMQ_RELEASE_EVIDENCE`",
            "token-aware command validation",
            "same shell command segment",
            "command strings must be single-line and unquoted",
            "CR/LF line breaks",
            "newline command separators",
            "shell quote characters",
            "quoted assignment words",
            "masquerade as active gate environment",
            "backslash escapes are rejected",
            "escaped assignment words",
            "cannot satisfy required gate environment",
            "Required command environment assignments",
            "manifest environment",
            "untracked shell provenance",
            "Repeated environment assignments are rejected",
            "cannot contain contradictory provenance",
            "Duplicate successful command entries",
            "same required gate",
            "comparative benchmark command must include",
            "`ZMQ_BENCH_COMPARE_REQUIRED_TARGETS`",
            "`ZMQ_BENCH_COMPARE_ENFORCE_GATES`",
            "same gate-control assignment",
            "custom comparative benchmark thresholds",
            "`ZMQ_BENCH_COMPARE_{MIN_THROUGHPUT_RATIO,MAX_P50_LATENCY_RATIO,MAX_P99_LATENCY_RATIO,MAX_ERROR_RATE,MIN_TREND_THROUGHPUT_RATIO,MAX_TREND_P50_LATENCY_RATIO,MAX_TREND_P99_LATENCY_RATIO}`",
            "matching the manifest environment",
            "live-S3 benchmark command must include",
            "`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
            "manifest environment must record the same values",
            "`ZMQ_BENCH_LIVE_S3_{ITERATIONS,PAYLOAD_BYTES}`",
            "matching live-S3 benchmark command assignments",
            "blank, duplicate, or placeholder entries",
            "same phase is repeated",
            "contradictory phase output",
            "Client profile `passed for <tools>` lists",
            "strict output CSV rules",
            "success-dependent `&&` separators only",
            "only documented compose config",
            "may use multi-segment release gate chains",
            "quoted/echoed command text cannot satisfy",
            "release gate commands",
            "must be direct invocations",
            "pipes, backgrounding, redirection, subshell",
            "`&>`/`&>>` combined redirects",
            "command substitution are rejected",
            "cannot embed release output marker text",
            "those markers must come from captured command output",
            "structured objects with non-empty",
            "`surface`, `status`, and",
            "bare strings or placeholders cannot satisfy release accounting",
            "objects are closed",
            "schemas; unknown fields are rejected",
            "Optional unsupported-surface accounting fields",
            "non-empty strings or lists of non-empty strings",
            "Optional accounting lists must be non-empty",
            "placeholder optional accounting fields",
            "Each `surface` field must name the known surface",
            "evidence, mitigation, and notes cannot be the only matching fields",
            "broker and controller ApiVersions omit them",
            "neither port has a dispatch/no-op path",
            "Each required surface must be covered by a distinct object",
            "catch-all entries",
            "multiple known surfaces",
            "Duplicate objects for the same known surface are rejected",
            "entries outside the verifier catalog are rejected",
            "status-class checked against the verifier catalog",
            "Each surface status must explicitly mark",
            "fail-closed/not-advertised",
            "vague completion-style statuses are rejected",
            "status class must match the surface",
            "broker-only stateless replacement must remain partial/blocked",
            "live CI matrix/performance accounting",
            "including each external-client, chaos, load/scale, failover, provider, and performance surface",
            "must remain release-CI-required or blocked",
            "external-client, secured-client, and OAuth profile execution",
            "scheduled cross-broker chaos and broader multi-broker chaos",
            "Docker E2E load/scale live orchestration",
            "KRaft failover network matrices",
            "live provider outage and multipart-fault profile execution",
            "comparative Kafka/AutoMQ performance profile/trend gates",
            "explicit false",
            "`known_data_loss_bug`",
            "`advertised_stub_api`",
            "`untriaged_durability_failure`",
            "`automq_complete=false`",
            "checked against the verifier catalog",
            "must be JSON booleans",
            "sub-profiles must also be listed within",
            "`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`",
            "within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`",
            "selector/provenance variables",
            "S3 provider matrix command must include",
            "`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`",
            "`ZMQ_S3_PROVIDER_PROFILES`",
            "`ZMQ_S3_PROVIDER_REQUIRED_{OUTAGE,PROCESS_CRASH,LIST_PAGINATION,MULTIPART_EDGE,MULTIPART_FAULT}_PROFILES`",
            "`ZMQ_S3_<PROFILE>_{RUN_LIVE_OUTAGE,RUN_PROCESS_CRASH,REQUIRE_LIST_PAGINATION,REQUIRE_MULTIPART_EDGE,RUN_MULTIPART_FAULT}`",
            "documented global fallback enable assignments",
            "`ZMQ_CHAOS_NETWORK_MATRIX`",
            "`ZMQ_KRAFT_NETWORK_MATRIX`",
            "`ZMQ_E2E_CHAOS_MATRIX`",
            "`ZMQ_E2E_LOAD_SCALE_MATRIX`",
            "`ZMQ_S3_PROVIDER_PROFILES`",
            "`ZMQ_CLIENT_MATRIX_PROFILES`",
            "fixture-backed inference",
            "environment-token collisions",
            "hook command",
            "parseable hook command",
            "documented global",
            "E2E load/scale fixture exception",
            "Docker E2E load/scale fixture now applies",
            "E2E fixture target and producer node selectors",
            "Docker E2E command must include",
            "`ZMQ_E2E_REQUIRED_CHAOS_PHASES`, `ZMQ_E2E_CHAOS_MATRIX`, and",
            "`ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` matching the manifest environment",
            "`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE` whenever fixture",
            "fixture enable, dry-run, and",
            "truthy release-evidence provenance",
            "`RUN_LIVE_OUTAGE`",
            "`RUN_PROCESS_CRASH`",
            "`REQUIRE_LIST_PAGINATION`",
            "`REQUIRE_MULTIPART_EDGE`",
            "`RUN_MULTIPART_FAULT`",
            "selected enable assignments",
            "strictly parses those profile/global enable",
            "`SKIP_ENSURE_BUCKET`",
            "`SKIP_MINIO_HEALTH`",
            "same boolean provenance",
            "Captured environment variables must remain strings",
            "valid shell variable names",
            "blank or placeholder values",
            "benchmark trend requirements",
            "client-profile markers",
            "profile marker line shape",
            "`passed for <tools> against <bootstrap> version=<version> source=command`",
            "profile-selected tools",
            "selected bootstrap",
            "profile-scoped tool probe",
            "before the corresponding profile pass marker",
            "client security detail marker",
            "ok: client security detail profile <profile>",
            "source=command",
            "same-block client security detail marker",
            "same profile block",
            "matching passed-for tools/bootstrap/version/source line",
            "client tool probe markers now require `source=command`",
            "markers plus required client security detail",
            "stale or contradictory semantic or",
            "client profile pass marker is now unique",
            "contradictory bootstrap/tool evidence",
            "before the final client matrix summary",
            "post-summary profile blocks cannot satisfy",
            "ok: client matrix passed for <profiles> profile(s) source=command",
            "exactly matching `ZMQ_CLIENT_MATRIX_PROFILES`",
            "ok: S3 provider matrix passed for <profiles> source=command",
            "exactly matching `ZMQ_S3_PROVIDER_PROFILES`",
            "`ZMQ_S3_<PROFILE>_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
            "`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
            "S3 provider matrix command must include",
            "`ZMQ_S3_PROVIDER_REQUIRED_PROFILES` and `ZMQ_S3_PROVIDER_PROFILES`",
            "S3 provider matrix self-test error catalogue",
            "provider profile fallback validation",
            "outage, process-crash, and multipart-fault evidence validation",
            "non-`minio` provider profiles must set explicit profile/global S3 settings",
            "profile/global endpoint",
            "effective scheme/region/path-style settings",
            "process-crash output omits or misattributes",
            "bucket differs from the selected provider bucket",
            "must match to the selected provider bucket",
            "`SCHEME` must parse as `http` or `https`",
            "`PATH_STYLE` must parse as",
            "endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>",
            "same profile block",
            "matching provider-settings profile",
            "unique within each provider block",
            "stale or contradictory endpoint, bucket, outage, process-crash, or",
            "provider-settings profile markers are also unique",
            "contradictory endpoint/bucket evidence",
            "before the final S3 provider matrix summary",
            "post-summary provider blocks cannot satisfy",
            "profile settings",
            "selected the tools and semantic suites",
            "exact version labels",
            "pinned Go module versions",
            "Python executables",
            "secured-client protocol/SASL/TLS settings",
            "positive/negative OAuth fixtures",
            "`auto` tool selection",
            "floating `@latest`",
            "missing security protocol provenance",
            "OAUTHBEARER positive or negative fixture variables",
            "profile semantic/tool mismatches",
            "rebalance, transactional, or",
            "live matrix does not",
            "OAuth fixture validation now mirrors",
            "raw JWTs, Java/Kafka CLI JAAS configs",
            "future-valid negative fixtures",
            "`kcat`, `kafka-cli`, `kafka-python`,",
            "`basic`, `admin`, `groups`, `rebalance`,",
            "probe markers using `ok: <client> probes (<semantics>) source=command`",
            "ok: kafka-python probes",
            "ok: confluent-kafka probes",
            "client matrix self-test error catalogue",
            "required client profile/tool/semantic coverage",
            "security and OAuth fixture validation",
            "exact semantic tokens inside client probe marker",
            "for every semantic named by",
            "recognized profile-selected",
            "required client-tool probe markers",
            "tools whose profile did not enable",
            "Go auto-discovery enable flags",
            "strictly parses global",
            "network_partition=[<phases>]",
            "exactly matching `ZMQ_KRAFT_NETWORK_MATRIX`",
            "KRaft failover command must include",
            "`ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` and",
            "`ZMQ_KRAFT_NETWORK_MATRIX`",
            "KRaft failover self-test error catalogue",
            "protocol fixture parsers",
            "record-batch fixture invariants",
            "old_leader_rejoined=true",
            "old_leader_fresh_rejoin=true",
            "automq_old_leader_fresh_rejoin=true",
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
            "must parse as non-placeholder non-negative integers",
            "allocate_producer_ids_checked=true",
            "allocate_producer_ids_follower_rejection_checked=true",
            "describe_quorum_v2_checked=true",
            "fetch_snapshot_v1_checked=true",
            "all_controller_fetch_snapshot_v1_checked=true",
            "controller_api_versions_checked=true",
            "all_controller_api_versions_checked=true",
            "controller_unsupported_checked=true",
            "all_controller_unsupported_checked=true",
            "controller_unsupported_cases=[<api_key>:<version>,...]",
            "dynamic_raft_voter_negative_checked=true",
            "dynamic_raft_voter_follower_rejection_checked=true",
            "all_controller_describe_quorum_v2_checked=true",
            "broker_lifecycle_negative_checked=true",
            "broker_lifecycle_follower_rejection_checked=true",
            "controller_registration_negative_checked=true",
            "controller_registration_follower_rejection_checked=true",
            "broker_registration_follower_rejection_checked=true",
            "broker_non_broker_api_rejection_checked=true",
            "broker_non_broker_api_rejection_cases=[<api_key>:<version>,...]",
            "committed_offset=",
            "transactions_checked=5",
            "must parse as exactly `5`",
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
            "reassignment_topic=<topic>",
            "reassignment_old_owner_rejected=true",
            "reassignment_target_fetch_verified=true",
            "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2",
            "second_offset=<positive> source=command",
            "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command",
            "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command",
            "ok: chaos s3-outage",
            "base_offset_negative=true serving=true source=command",
            "ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command",
            "ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command",
            "broker chaos self-test error catalogue",
            "record-batch fixtures",
            "before the broker chaos harness summary line",
            "scenario detail markers must be unique per required scenario",
            "`ZMQ_CHAOS_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
            "`ZMQ_CHAOS_S3_TLS_CA_FILE`",
            "fail closed instead of falling back to `ZMQ_S3_*`",
            "broker chaos command must include",
            "non-sensitive live-S3 outage provider assignments",
            "coverage selector assignments for `ZMQ_CHAOS_REQUIRED_SCENARIOS`",
            "`ZMQ_CHAOS_REQUIRED_NETWORK_PHASES`, and `ZMQ_CHAOS_NETWORK_MATRIX`",
            "ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command",
            "ok: chaos network-partition source=command",
            "before the chaos network-partition scenario marker",
            "ok: chaos harness passed for <scenarios> source=command",
            "`ZMQ_CHAOS_REQUIRED_SCENARIOS` entry",
            "scenario summary must appear as its own stripped line",
            "chaos network-partition scenario summary must appear exactly once",
            "markers cannot satisfy it",
            "ok: client matrix profile",
            "before the final client matrix summary",
            "post-summary profile blocks cannot satisfy",
            "external client matrix command must include",
            "required profile, selected profile, required tool, required semantic",
            "ok: S3 process crash/replacement harness passed (bucket=<bucket>",
            "S3 process-crash summary marker must appear exactly once",
            "killed_broker=true",
            "fresh_data_dir=true",
            "replacement_offset=<offset>",
            "recovered_payloads=2",
            "source=command",
            "replacement offset greater than the",
            "ok: S3 provider live-suite profile ... command_started=true completed=true source=command",
            "ok: S3 provider outage detail profile ...",
            "fail_closed=true recovered=true source=command",
            "before the final S3 provider matrix summary",
            "post-summary provider blocks cannot satisfy",
            "ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command",
            "ok: S3 provider process-crash detail profile ...",
            "recovered_payloads=2 source=command",
            "ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command",
            "ok: S3 provider list-pagination profile ... required=true completed=true source=command",
            "ok: S3 provider multipart-edge profile ... required=true completed=true source=command",
            "ok: S3 provider multipart-fault profile ... command_started=true completed=true injected=true recovered=true source=command",
            "command-owned marker",
            "source=command",
            "ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command",
            "before the KRaft failover summary line",
            "ok: E2E chaos phase",
            "down=true observed=<failed|survived> healed=true recovered=true",
            "expect=<fail|survive> source=command",
            "before the E2E chaos summary line",
            "ok: E2E load/scale phase",
            "applied=true restored=true",
            "marker_payloads=hook-owned",
            "apply_source=<hook|fixture>",
            "restore_source=<hook|fixture> source=command",
            "load_records=<count>",
            "must not report a fixture action",
            "before the E2E load/scale summary line",
            "ok: E2E chaos passed for <phases> phase(s) source=command",
            "ok: E2E load/scale passed for <phases> phase(s) source=command",
            "MinIO `8/8 tests passed` evidence",
            "MinIO `8/8 tests passed` marker must appear exactly once",
            "`ZMQ_S3_REQUIRE_MULTIPART_EDGE=1`",
            "`ZMQ_S3_REQUIRE_LIST_PAGINATION=1`",
            "provider-edge subtests skipped",
            "must not contain any unsuccessful `Build Summary:` line",
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
            "old_leader=",
            "new_leader=",
            "restarted_controller=",
            "epoch=",
            "automq_old_leader=",
            "automq_new_leader=",
            "must parse as non-placeholder non-negative integers",
            "allocate_producer_ids_checked=true",
            "allocate_producer_ids_follower_rejection_checked=true",
            "describe_quorum_v2_checked=true",
            "fetch_snapshot_v1_checked=true",
            "all_controller_fetch_snapshot_v1_checked=true",
            "controller_api_versions_checked=true",
            "all_controller_api_versions_checked=true",
            "controller_unsupported_checked=true",
            "all_controller_unsupported_checked=true",
            "controller_unsupported_cases=[<api_key>:<version>,...]",
            "dynamic_raft_voter_negative_checked=true",
            "dynamic_raft_voter_follower_rejection_checked=true",
            "all_controller_describe_quorum_v2_checked=true",
            "broker_lifecycle_negative_checked=true",
            "broker_lifecycle_follower_rejection_checked=true",
            "controller_registration_negative_checked=true",
            "controller_registration_follower_rejection_checked=true",
            "broker_registration_follower_rejection_checked=true",
            "broker_non_broker_api_rejection_checked=true",
            "broker_non_broker_api_rejection_cases=[<api_key>:<version>,...]",
            "committed_offset=",
            "transactions_checked=5",
            "must parse as exactly `5`",
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
            "reassignment_topic=<topic>",
            "reassignment_old_owner_rejected=true",
            "reassignment_target_fetch_verified=true",
            "comparative benchmark `COMPARISON:`",
            "`Benchmark` rows for `ApiVersions`",
            "`Produce (fresh)`",
            "`thresholds:` line",
            "line-aware output marker matching",
            "not ok:",
            "previous result: pass",
            "Captured skip markers are also line-aware",
            "Zig `Build Summary:` skip count",
            "Docker E2E section markers are line-aware",
            "`3-Node E2E Test Suite`",
            "`Results:`",
            "Docker E2E output line markers must appear exactly once",
            "Docker E2E phase summaries must",
            "Results: <passed>/<total> passed, 0 failed",
            "Docker E2E final results line must appear exactly once",
            "after the required E2E phase summaries",
            "earlier detached results output",
            "`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE=1`",
            "Docker E2E self-test assertion catalogue",
            "fixture override rejection",
            "Local and live-S3 benchmark markers are also line-aware",
            "ok: local benchmark gate source=command",
            "ok: live-S3 benchmark gate source=command",
            "`S3 WAL request volume`",
            "`Live S3 provider`",
            "`Live S3 request volume`",
            "local benchmark summary must appear exactly once",
            "live-S3 benchmark summary must appear exactly once",
            "exact stripped `=== Benchmarks complete ===` line",
            "before the `=== Benchmarks complete ===` marker",
            "S3 WAL request volume puts=<puts> lists=<lists>",
            "PartitionStore memory <rate>/s retained=<retained>",
            "Live S3 provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>",
            "command/env-selected `ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`",
            "`SCHEME` parsing as `http` or `https`",
            "`PATH_STYLE` parsing as `true` or `false`",
            "Live S3 put <MiB/s> MiB/s p99=<ms> ms objects=<objects>",
            "Live S3 get <MiB/s> MiB/s p99=<ms> ms requests/MiB=<value>",
            "Live S3 request volume puts=<puts> gets=<gets>",
            "concrete measurements",
            "before the benchmark completion marker",
            "Comparative benchmark table markers are also line-aware",
            "section-scoped",
            "target labels must",
            "appear on the `COMPARISON:` line",
            "profile marker is a closed key=value schema",
            "every required field must appear exactly once",
            "fields must not be blank",
            "unknown fields are rejected",
            "exact selected-target `COMPARISON:` line",
            "before the gate",
            "exactly once",
            "`Benchmark` marker must be a table",
            "throughput (`tput`) row",
            "detached post-gate line",
            "concrete `tput`, `p50`, and `p99`",
            "before the gate",
            "positive finite target",
            "must not repeat",
            "Table target columns are limited to the known target headers",
            "ratio columns are limited to known ZMQ-to-baseline pairs",
            "bounded `COMPARATIVE BENCHMARK GATE`",
            "exact stripped `COMPARATIVE BENCHMARK GATE` line",
            "inside the bounded `COMPARATIVE BENCHMARK GATE`",
            "gate section result",
            "ZMQ (Zig)",
            "Apache Kafka",
            "AutoMQ (Java)",
            "`trend thresholds:`",
            "Trend baseline metrics must be strict structured numeric finite benchmark",
            "non-standard JSON constants such as `NaN`, `Infinity`, or `-Infinity`",
            "rejected while parsing archived baselines",
            "while writing current",
            "`benchmarks/results.json`",
            "serialized before replacing the existing artifact",
            "only replace",
            "`benchmarks/results.json` after the gate passes",
            "cannot clobber the prior artifact",
            "selected/required target metadata must list",
            "concrete known unique targets",
            "`targets_with_results` must match result",
            "target-label, iteration/warmup, threshold, gate,",
            "trend-baseline metadata",
            "required target metadata must be a subset of selected target metadata",
            "Result artifact maps must be objects with",
            "only known target keys and per-target object results",
            "no unknown",
            "benchmark result keys",
            "Archived trend baselines must include schema-version 1",
            "artifact metadata whose targets_with_results includes zmq",
            "trend baseline must not resolve to the current `benchmarks/results.json`",
            "rejects blank",
            "blank or duplicate required-target entries",
            "placeholder, negative, and non-finite threshold values",
            "malformed trend-baseline",
            "Current comparative",
            "result rows now apply the same",
            "fail-closed checks",
            "non-numeric or",
            "non-finite throughput/latency metrics",
            "non-integral",
            "malformed comparison-result rejection",
            "comparative benchmark self-test assertion catalogue",
            "artifact-metadata failure cases",
        ),
    )


def assert_toolchain_contract():
    dockerfile = read(DOCKERFILE)
    makefile = read(MAKEFILE)
    readme = read(README)
    release_criteria = read(RELEASE_CRITERIA)

    if f"ZIG_VERSION={REQUIRED_ZIG_VERSION}" not in dockerfile:
        raise AssertionError(f"Dockerfile must pin Zig {REQUIRED_ZIG_VERSION}")
    if f"ZIG_0_16 ?= {REQUIRED_ZIG_PATH}" not in makefile:
        raise AssertionError(f"Makefile must default to {REQUIRED_ZIG_PATH} when present")
    if "ZIG ?= $(if $(wildcard $(ZIG_0_16)),$(ZIG_0_16),zig)" not in makefile:
        raise AssertionError("Makefile must let ZIG override the pinned Zig fallback")
    if "ZIG_GLOBAL_CACHE_DIR ?= /tmp/zig-cache-zmq" not in makefile:
        raise AssertionError("Makefile must use the shared local Zig 0.16 cache")
    if f"Zig {REQUIRED_ZIG_VERSION}" not in readme:
        raise AssertionError(f"README must document Zig {REQUIRED_ZIG_VERSION}")
    if REQUIRED_ZIG_PATH not in release_criteria:
        raise AssertionError(f"release criteria must pin {REQUIRED_ZIG_PATH}")
    assert_release_criteria_pins_compose_images(release_criteria)


def assert_no_stale_readme_compose_fragments(local_cluster_docs):
    stale = [
        fragment
        for fragment in STALE_README_COMPOSE_FRAGMENTS
        if fragment in local_cluster_docs
    ]
    if stale:
        raise AssertionError(
            "README local compose docs contain stale fragments: " + ", ".join(stale)
        )


def assert_no_floating_compose_images(compose_entries):
    for label, compose in compose_entries:
        if ":latest" in compose:
            raise AssertionError(f"{label} must not use floating :latest images")


def assert_compose_contract():
    root_compose = read(ROOT_COMPOSE)
    kafka_compose = read(KAFKA_COMPOSE)
    automq_compose = read(AUTOMQ_COMPOSE)
    readme = read(README)

    assert_fragments(
        root_compose,
        "docker-compose.yml",
        (
            "minio:",
            "minio-init:",
            "node0:",
            "node1:",
            "node2:",
            f"image: {REQUIRED_MINIO_IMAGE}",
            f"image: {REQUIRED_MINIO_MC_IMAGE}",
            "container_name: zmq-node-0",
            "container_name: zmq-node-1",
            "container_name: zmq-node-2",
            '"19092:9092"',
            '"19093:9093"',
            '"19090:9090"',
            '"19094:9092"',
            '"19095:9093"',
            '"19091:9090"',
            '"19096:9092"',
            '"19097:9093"',
            '"19098:9090"',
            "--process-roles controller,broker",
            "--s3-endpoint minio",
            "--s3-bucket automq",
            "--metrics-port 9090",
            "--voters 0@node0:9093,1@node1:9093,2@node2:9093",
            "condition: service_completed_successfully",
            "curl -f http://localhost:9090/health",
        ),
    )

    assert_fragments(
        kafka_compose,
        "benchmarks/kafka-compose.yml",
        (
            "broker1:",
            "broker2:",
            "broker3:",
            f"image: {REQUIRED_KAFKA_IMAGE}",
            '"19092:9092"',
            "KAFKA_PROCESS_ROLES: broker,controller",
            "KAFKA_CONTROLLER_QUORUM_VOTERS: 0@broker1:9093,1@broker2:9093,2@broker3:9093",
            "KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT",
            "KAFKA_HEAP_OPTS: -Xms1g -Xmx4g",
        ),
    )

    assert_fragments(
        automq_compose,
        "benchmarks/automq-compose.yml",
        (
            "minio:",
            "mc:",
            "server1:",
            "server2:",
            "server3:",
            f"image: {REQUIRED_MINIO_IMAGE}",
            f"image: {REQUIRED_MINIO_MC_IMAGE}",
            f"image: {REQUIRED_AUTOMQ_IMAGE}",
            '"19092:9092"',
            "controller.quorum.voters=0@server1:9093,1@server2:9093,2@server3:9093",
            "s3.data.buckets='0@s3://automq-data?region=us-east-1&endpoint=http://minio:9000&pathStyle=true'",
            "s3.ops.buckets='1@s3://automq-ops?region=us-east-1&endpoint=http://minio:9000&pathStyle=true'",
            "s3.wal.path='0@s3://automq-data?region=us-east-1&endpoint=http://minio:9000&pathStyle=true'",
            "condition: service_completed_successfully",
        ),
    )

    local_cluster_docs = section_between(
        readme,
        "## Deploying a Multi-Broker Cluster Locally",
        "### Running Without Docker",
    )
    assert_fragments(
        local_cluster_docs,
        "README local compose docs",
        (
            "docker compose up -d --build",
            "docker compose -f docker-compose.yml config --quiet",
            "docker compose logs -f node0 node1 node2",
            "`zmq-node-0`",
            "`zmq-node-1`",
            "`zmq-node-2`",
            "`19092` (Kafka)",
            "`19093` (controller)",
            "`19090` (metrics)",
            "localhost:19092",
            "localhost:19090/health",
        ),
    )
    assert_no_stale_readme_compose_fragments(local_cluster_docs)

    assert_no_floating_compose_images(
        (
            ("docker-compose.yml", root_compose),
            ("benchmarks/kafka-compose.yml", kafka_compose),
            ("benchmarks/automq-compose.yml", automq_compose),
        )
    )


def assert_python_harness_deadlines_monotonic(harness_texts=None):
    if harness_texts is None:
        harness_texts = [
            (path, read(os.path.join(PROJECT_DIR, path)))
            for path in MONOTONIC_PYTHON_HARNESSES
        ]
    for path, text in harness_texts:
        forbidden = [
            label
            for label, pattern in FORBIDDEN_WALL_CLOCK_DEADLINE_PATTERNS
            if pattern.search(text)
        ]
        if forbidden:
            raise AssertionError(
                f"{path} uses wall-clock time for runtime gates: "
                + ", ".join(forbidden)
            )


def assert_python_monotonic_harness_scope_complete(runtime_gates=None, harnesses=None):
    if runtime_gates is None:
        runtime_gates = PYTHON_RUNTIME_GATES
    if harnesses is None:
        harnesses = MONOTONIC_PYTHON_HARNESSES

    harness_set = set(harnesses)
    missing = sorted({path for _, path in runtime_gates} - harness_set)
    if missing:
        raise AssertionError(
            "Python runtime gates missing monotonic deadline audit coverage: "
            + ", ".join(missing)
        )


def assert_python_kafka_visible_timestamps_wall_clock(benchmark_compare=None):
    if benchmark_compare is None:
        benchmark_compare = read(BENCHMARK_COMPARE)

    forbidden = (
        "now_ms = int(time.monotonic() * 1000)",
        "timestamp_ms = int(time.monotonic() * 1000)",
    )
    found = [fragment for fragment in forbidden if fragment in benchmark_compare]
    if found:
        raise AssertionError(
            "benchmark_compare.py uses monotonic time for Kafka-visible "
            "record timestamps: " + ", ".join(found)
        )
    assert_fragments(
        benchmark_compare,
        "benchmark_compare.py Kafka-visible timestamp contract",
        (
            "def current_time_ms():",
            "return int(time.time() * 1000)",
            "now_ms = current_time_ms()",
            "Produce v3 record timestamp must use wall-clock epoch milliseconds",
        ),
    )
    assert_fragments(
        benchmark_compare,
        "benchmark_compare.py trend baseline provenance contract",
        (
            "def placeholder_env_value(",
            "def env_bool(",
            "def trend_baseline_path_from_env(",
            "def trend_baseline_uses_placeholder(",
            "resolved_path = project_path(path)",
            "ZMQ_BENCH_COMPARE_REQUIRE_TREND",
            "must not use a placeholder value",
            "ZMQ_BENCH_COMPARE_TREND_BASELINE must not use a placeholder path",
            "trend baseline:",
            "placeholder threshold parsing did not fail",
            "blank threshold parsing did not fail",
            "negative threshold parsing did not fail",
            "blank required target list was accepted",
            "embedded blank required target was accepted",
            "placeholder required target list was accepted",
            "angle-bracket placeholder required target list was accepted",
            "duplicate required target was accepted",
            "placeholder trend requirement flag was accepted",
            "invalid trend requirement flag was accepted",
            "relative trend baseline loading was not project-rooted",
            "def validate_trend_baseline_artifact_metadata(",
            "def artifact_payload_result_map(",
            "def validate_artifact_metadata(",
            "def artifact_target_list(",
            "def validate_artifact_target_labels(",
            "def validate_artifact_profile_int_map(",
            "def validate_artifact_thresholds(",
            "def validate_artifact_bool_metadata(",
            "def validate_artifact_trend_baseline_metadata(",
            "def validate_artifact_results_map(",
            "def benchmark_result_object_failures(",
            "def should_write_results_artifact(",
            "def write_results_file_if_allowed(",
            "def comparative_profile_marker(",
            'RESULTS_ARTIFACT = os.path.join("benchmarks", "results.json")',
            "RESULTS_FILE = os.path.join(PROJECT_DIR, RESULTS_ARTIFACT)",
            "Results saved to {RESULTS_ARTIFACT}",
            "selected=zmq,kafka,automq",
            "results_targets=zmq,kafka,automq",
            "source=command",
            "benchmark result artifact metadata",
            "comparative profile marker formatting drifted",
            "trend baseline artifact metadata missing was accepted",
            "trend baseline artifact schema drift was accepted",
            "trend baseline artifact metadata without ZMQ was accepted",
            "mismatched trend baseline artifact target metadata was accepted",
            "trend baseline artifact missing target label was accepted",
            "trend baseline artifact non-finite threshold was accepted",
            "trend baseline artifact non-boolean gate flag was accepted",
            "benchmark result artifact selected targets missing result target was accepted",
            "trend-required benchmark result artifact without trend baseline was accepted",
            "string benchmark result artifact selected targets were accepted",
            "duplicate benchmark result artifact selected targets were accepted",
            "unknown benchmark result artifact required target was accepted",
            "non-object benchmark result artifact map was accepted",
            "unknown benchmark result artifact result target was accepted",
            "non-object benchmark result artifact target result was accepted",
            "benchmark result artifact missing benchmark row was accepted",
            "unknown benchmark result artifact benchmark key was accepted",
            "malformed benchmark result artifact metric was accepted",
            "zero benchmark result artifact metric was accepted",
            "malformed benchmark result artifact count was accepted",
            "non-standard JSON benchmark result clobbered existing artifact",
            "malformed benchmark result artifact clobbered existing artifact",
            "failing enforced benchmark result artifact was writable",
            "failing enforced benchmark result clobbered existing artifact",
            "placeholder trend baseline path was accepted",
        ),
    )


def assert_live_hook_preflight_contract(harness_texts=None, parity=None):
    if harness_texts is None:
        harness_texts = {
            path: read(os.path.join(PROJECT_DIR, path))
            for path, _ in LIVE_HOOK_PREFLIGHT_CONTRACTS
        }
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    for path, fragments in LIVE_HOOK_PREFLIGHT_CONTRACTS:
        assert_fragments(
            harness_texts.get(path, ""),
            path,
            fragments,
        )

    assert_fragments(
        parity,
        "AutoMQ parity live-hook preflight docs",
        (
            "Latest live-hook preflight tranche",
            "operator-provided hook commands",
            "chaos network partitions",
            "Broker live-S3 chaos hooks are parsed",
            "S3 provider multipart faults",
            "process-crash/replacement output",
            "selected provider bucket",
            "KRaft network",
            "Docker E2E chaos/load-scale phases",
            "blank, malformed, or cannot",
            "before a live gate can report coverage from an invalid hook",
            "normalize to the same environment-variable token",
        ),
    )


def assert_run_gate_bool_preflight(harness_texts=None, release_criteria=None, parity=None):
    if harness_texts is None:
        harness_texts = {
            path: read(os.path.join(PROJECT_DIR, path))
            for path, _ in RUN_GATE_BOOL_PREFLIGHT_CONTRACTS
        }
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    for path, fragments in RUN_GATE_BOOL_PREFLIGHT_CONTRACTS:
        assert_fragments(
            harness_texts.get(path, ""),
            path,
            fragments,
        )

    assert_fragments(
        release_criteria,
        "release criteria run-gate boolean preflight docs",
        RUN_GATE_BOOL_PREFLIGHT_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity run-gate boolean preflight docs",
        RUN_GATE_BOOL_PREFLIGHT_DOC_FRAGMENTS,
    )


def assert_release_evidence_verifier_contract(release_evidence=None):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))

    assert_fragments(
        release_evidence,
        "tests/release_evidence_test.py client profile provenance verifier",
        RELEASE_EVIDENCE_VERIFIER_CONTRACT,
    )


def requirement_dispatch_block(validate_release_evidence_body, label):
    marker = f'if requirement["label"] == "{label}":'
    start = validate_release_evidence_body.find(marker)
    if start < 0:
        raise AssertionError(f"missing release-evidence dispatch for {label}")
    next_label = re.search(
        r'\n\s+if requirement\["label"\] == "',
        validate_release_evidence_body[start + len(marker) :],
    )
    if next_label:
        block = validate_release_evidence_body[
            start : start + len(marker) + next_label.start()
        ]
    else:
        block = validate_release_evidence_body[start:]

    common_marker = (
        "\n            failures.extend(\n"
        "                validate_required_command_env_assignments("
    )
    common_start = block.find(common_marker)
    if common_start >= 0:
        block = block[:common_start]
    return block


def command_provenance_validator_names(release_evidence):
    parsed = ast.parse(release_evidence)
    names = []
    for node in parsed.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if not re.match(r"^validate_.*command_provenance$", node.name):
            continue
        arg_names = [arg.arg for arg in node.args.args[:3]]
        if arg_names == ["command", "environment", "required"]:
            names.append(node.name)
    return names


def output_marker_dispatch_calls(block):
    return sorted(
        set(
            re.findall(
                r"\b(validate_[A-Za-z0-9_]*(?:output|markers))\s*\(",
                block,
            )
        )
    )


def output_marker_dispatch_calls_by_label(validate_release_evidence_body):
    calls = {}
    labels = re.findall(
        r'if requirement\["label"\] == "([^"]+)":',
        validate_release_evidence_body,
    )
    for label in labels:
        block = requirement_dispatch_block(validate_release_evidence_body, label)
        label_calls = output_marker_dispatch_calls(block)
        if label_calls:
            calls[label] = label_calls
    return calls


def assert_release_evidence_command_provenance_dispatch_pinned(
    release_evidence=None,
    specs=None,
    check_scope=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if specs is None:
        specs = RELEASE_EVIDENCE_COMMAND_PROVENANCE_DISPATCH

    validate_body = section_between(
        release_evidence,
        "def validate_release_evidence(",
        "\ndef current_git_commit(",
    )
    missing = []
    if check_scope:
        expected_validators = {validator_name for _, validator_name, _ in specs}
        discovered_validators = set(command_provenance_validator_names(release_evidence))
        missing_specs = sorted(discovered_validators - expected_validators)
        stale_specs = sorted(expected_validators - discovered_validators)
        if missing_specs:
            missing.append(
                "missing command-provenance dispatch catalogue entries for "
                + ", ".join(missing_specs)
            )
        if stale_specs:
            missing.append(
                "command-provenance dispatch catalogue references missing "
                "validators "
                + ", ".join(stale_specs)
            )
    for label, validator_name, fragments in specs:
        block = requirement_dispatch_block(validate_body, label)
        if f"{validator_name}(" not in block:
            missing.append(
                f"{label} dispatch does not call {validator_name}"
            )
        for fragment in fragments:
            if fragment not in release_evidence:
                missing.append(
                    f"{label} provenance contract missing {fragment}"
                )
    if missing:
        raise AssertionError(
            "tests/release_evidence_test.py command provenance dispatch drift: "
            + "; ".join(missing)
        )


def assert_release_evidence_output_marker_dispatch_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria output-marker dispatch docs",
        RELEASE_EVIDENCE_OUTPUT_MARKER_DISPATCH_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity output-marker dispatch docs",
        RELEASE_EVIDENCE_OUTPUT_MARKER_DISPATCH_DOC_FRAGMENTS,
    )


def assert_release_evidence_output_marker_dispatch_pinned(
    release_evidence=None,
    specs=None,
    check_scope=True,
    check_docs=True,
    release_criteria=None,
    parity=None,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if specs is None:
        specs = RELEASE_EVIDENCE_OUTPUT_MARKER_DISPATCH

    if check_docs:
        assert_release_evidence_output_marker_dispatch_docs(release_criteria, parity)

    validate_body = section_between(
        release_evidence,
        "def validate_release_evidence(",
        "\ndef current_git_commit(",
    )
    expected_pairs = {
        (label, validator_name)
        for label, validator_names in specs
        for validator_name in validator_names
    }
    discovered_by_label = output_marker_dispatch_calls_by_label(validate_body)
    discovered_pairs = {
        (label, validator_name)
        for label, validator_names in discovered_by_label.items()
        for validator_name in validator_names
    }

    missing = []
    if check_scope:
        missing_specs = sorted(discovered_pairs - expected_pairs)
        stale_specs = sorted(expected_pairs - discovered_pairs)
        if missing_specs:
            missing.append(
                "missing output-marker dispatch catalogue entries for "
                + ", ".join(
                    f"{label}: {validator_name}"
                    for label, validator_name in missing_specs
                )
            )
        if stale_specs:
            missing.append(
                "output-marker dispatch catalogue references missing "
                "validator calls "
                + ", ".join(
                    f"{label}: {validator_name}"
                    for label, validator_name in stale_specs
                )
            )

    for label, validator_names in specs:
        block = requirement_dispatch_block(validate_body, label)
        for validator_name in validator_names:
            if f"{validator_name}(" not in block:
                missing.append(
                    f"{label} dispatch does not call {validator_name}"
                )

    if missing:
        raise AssertionError(
            "tests/release_evidence_test.py output marker dispatch drift: "
            + "; ".join(missing)
        )


def literal_module_assignment(source, name):
    parsed = ast.parse(source)
    for node in parsed.body:
        value = None
        targets = []
        if isinstance(node, ast.Assign):
            value = node.value
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            value = node.value
            targets = [node.target]
        if value is None:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id == name:
                return ast.literal_eval(value)
    raise AssertionError(f"missing module assignment {name}")


def release_evidence_required_unsupported_surfaces(release_evidence):
    surfaces = literal_module_assignment(
        release_evidence,
        "REQUIRED_UNSUPPORTED_SURFACES",
    )
    if not isinstance(surfaces, list) or not surfaces:
        raise AssertionError("REQUIRED_UNSUPPORTED_SURFACES must be a non-empty list")

    labels = []
    failures = []
    for index, surface in enumerate(surfaces):
        if not isinstance(surface, dict):
            failures.append(f"entry {index} must be an object")
            continue
        for field in (
            "label",
            "surface_fragments",
            "fragments",
            "status_label",
            "status_markers",
        ):
            if field not in surface:
                failures.append(f"entry {index} missing {field}")
        label = surface.get("label")
        if isinstance(label, str) and label.strip():
            labels.append(label)
        else:
            failures.append(f"entry {index} label must be non-empty")
        for field in ("surface_fragments", "fragments", "status_markers"):
            value = surface.get(field)
            if (
                not isinstance(value, list)
                or not value
                or any(not isinstance(item, str) or not item for item in value)
            ):
                failures.append(f"entry {index} {field} must be non-empty strings")
        status_label = surface.get("status_label")
        if not isinstance(status_label, str) or not status_label.strip():
            failures.append(f"entry {index} status_label must be non-empty")

    duplicates = sorted({label for label in labels if labels.count(label) > 1})
    if duplicates:
        failures.append("duplicate unsupported surface labels: " + ", ".join(duplicates))
    if failures:
        raise AssertionError(
            "release evidence unsupported-surface catalog drift: "
            + "; ".join(failures)
        )
    return surfaces


def known_unsupported_surface_bullets_from_criteria(criteria):
    section = section_between(
        criteria,
        "## Known Unsupported Or Partial Surfaces",
        "\n## Release Decision",
    )
    bullets = []
    current = []
    for line in section.splitlines():
        if line.startswith("- "):
            if current:
                bullets.append(" ".join(current))
            current = [line[2:].strip()]
        elif current:
            stripped = line.strip()
            if stripped:
                current.append(stripped)
    if current:
        bullets.append(" ".join(current))
    return bullets


def unsupported_surface_catalog_entry_matches(text, surface):
    return all(fragment in text for fragment in surface["fragments"])


def unsupported_surface_status_marker_present(text, surface):
    lowered = text.lower()
    return any(marker in lowered for marker in surface["status_markers"])


def assert_unsupported_surface_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria unsupported-surface catalogue docs",
        RELEASE_EVIDENCE_UNSUPPORTED_SURFACE_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity unsupported-surface catalogue docs",
        RELEASE_EVIDENCE_UNSUPPORTED_SURFACE_CATALOG_DOC_FRAGMENTS,
    )


def assert_unsupported_surface_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_unsupported_surface_catalog_docs(release_criteria, parity)

    surfaces = release_evidence_required_unsupported_surfaces(release_evidence)
    bullets = known_unsupported_surface_bullets_from_criteria(release_criteria)
    failures = []

    if len(bullets) != len(surfaces):
        failures.append(
            "release criteria Known Unsupported Or Partial Surfaces must list "
            f"exactly {len(surfaces)} top-level surfaces, found {len(bullets)}"
        )

    matched_bullets = set()
    for surface in surfaces:
        matches = [
            index
            for index, bullet in enumerate(bullets)
            if unsupported_surface_catalog_entry_matches(bullet, surface)
        ]
        if not matches:
            failures.append(
                "release criteria Known Unsupported Or Partial Surfaces missing "
                f"top-level bullet for {surface['label']}"
            )
        elif len(matches) > 1:
            failures.append(
                "release criteria Known Unsupported Or Partial Surfaces has "
                f"duplicate bullets for {surface['label']}: "
                + ", ".join(str(index) for index in matches)
            )
        elif matches[0] in matched_bullets:
            failures.append(
                "release criteria Known Unsupported Or Partial Surfaces bullet "
                f"{matches[0]} matches multiple verifier surfaces"
            )
        else:
            bullet = bullets[matches[0]]
            if not unsupported_surface_status_marker_present(bullet, surface):
                failures.append(
                    "release criteria Known Unsupported Or Partial Surfaces bullet "
                    f"for {surface['label']} must mark the surface as "
                    f"{surface['status_label']}"
                )
            matched_bullets.add(matches[0])

        if surface["label"] not in production_readiness:
            failures.append(
                "production readiness missing unsupported-surface catalogue pin "
                f"for {surface['label']}"
            )

        parity_fragments = [surface["label"]] + surface["surface_fragments"]
        if not any(fragment in parity for fragment in parity_fragments):
            failures.append(
                "AutoMQ parity docs missing unsupported-surface catalogue pin "
                f"for {surface['label']}"
            )

    unmatched = sorted(set(range(len(bullets))) - matched_bullets)
    if unmatched:
        failures.append(
            "release criteria Known Unsupported Or Partial Surfaces has "
            "unmatched top-level bullets: "
            + ", ".join(str(index) for index in unmatched)
        )
    if failures:
        raise AssertionError(
            "unsupported-surface catalogue drift: " + "; ".join(failures)
        )


def release_evidence_unsupported_status_catalog(release_evidence):
    markers = literal_module_assignment(
        release_evidence,
        "UNSUPPORTED_SURFACE_STATUS_MARKERS",
    )
    failures = []
    if (
        not isinstance(markers, (list, tuple))
        or not markers
        or any(not isinstance(marker, str) or not marker for marker in markers)
    ):
        failures.append(
            "UNSUPPORTED_SURFACE_STATUS_MARKERS must be a non-empty string sequence"
        )
        markers = ()
    duplicates = sorted({marker for marker in markers if markers.count(marker) > 1})
    if duplicates:
        failures.append(
            "UNSUPPORTED_SURFACE_STATUS_MARKERS repeats markers: "
            + ", ".join(duplicates)
        )
    non_lower = sorted(
        marker for marker in markers if isinstance(marker, str) and marker != marker.lower()
    )
    if non_lower:
        failures.append(
            "UNSUPPORTED_SURFACE_STATUS_MARKERS entries must be lowercase: "
            + ", ".join(non_lower)
        )
    if failures:
        raise AssertionError(
            "release-evidence unsupported status catalogue drift: "
            + "; ".join(failures)
        )
    return tuple(markers)


def assert_unsupported_status_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria unsupported status catalogue docs",
        RELEASE_EVIDENCE_UNSUPPORTED_STATUS_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity unsupported status catalogue docs",
        RELEASE_EVIDENCE_UNSUPPORTED_STATUS_CATALOG_DOC_FRAGMENTS,
    )


def assert_unsupported_status_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_unsupported_status_catalog_docs(release_criteria, parity)

    markers = release_evidence_unsupported_status_catalog(release_evidence)
    failures = []
    for marker in markers:
        for target_label, text in (
            ("release criteria", release_criteria),
            ("AutoMQ parity", parity),
            ("production readiness", production_readiness),
        ):
            if marker not in text:
                failures.append(
                    f"{target_label} missing unsupported status marker {marker}"
                )
    if failures:
        raise AssertionError(
            "release-evidence unsupported status catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_unsupported_text_field_catalog(release_evidence):
    wanted = {
        "REQUIRED_UNSUPPORTED_SURFACE_FIELDS",
        "OPTIONAL_UNSUPPORTED_SURFACE_FIELDS",
        "UNSUPPORTED_SURFACE_FIELDS",
        "UNSUPPORTED_SURFACE_TEXT_FIELDS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence unsupported text-field catalogue missing "
            "assignments: "
            + ", ".join(missing)
        )

    failures = []
    for name in sorted(wanted):
        fields = constants[name]
        if (
            not isinstance(fields, (list, tuple))
            or not fields
            or any(not isinstance(field, str) or not field for field in fields)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({field for field in fields if fields.count(field) > 1})
        if duplicates:
            failures.append(f"{name} repeats unsupported-surface fields: " + ", ".join(duplicates))

    expected_unsupported = tuple(constants["REQUIRED_UNSUPPORTED_SURFACE_FIELDS"]) + tuple(
        constants["OPTIONAL_UNSUPPORTED_SURFACE_FIELDS"]
    )
    if tuple(constants["UNSUPPORTED_SURFACE_FIELDS"]) != expected_unsupported:
        failures.append(
            "UNSUPPORTED_SURFACE_FIELDS must equal required plus optional "
            "unsupported-surface fields"
        )

    text_fields = tuple(constants["UNSUPPORTED_SURFACE_TEXT_FIELDS"])
    if set(text_fields) != set(constants["UNSUPPORTED_SURFACE_FIELDS"]):
        failures.append(
            "UNSUPPORTED_SURFACE_TEXT_FIELDS must equal unsupported-surface fields"
        )

    if failures:
        raise AssertionError(
            "release-evidence unsupported surface text-field catalogue drift: "
            + "; ".join(failures)
        )

    return (
        ("unsupported surface text-field constant", ("UNSUPPORTED_SURFACE_TEXT_FIELDS",)),
        ("unsupported surface text field", text_fields),
    )


def assert_unsupported_text_field_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria unsupported text-field catalogue docs",
        RELEASE_EVIDENCE_UNSUPPORTED_SURFACE_TEXT_FIELD_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity unsupported text-field catalogue docs",
        RELEASE_EVIDENCE_UNSUPPORTED_SURFACE_TEXT_FIELD_CATALOG_DOC_FRAGMENTS,
    )


def assert_unsupported_text_field_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_unsupported_text_field_catalog_docs(release_criteria, parity)

    entries = release_evidence_unsupported_text_field_catalog(release_evidence)
    failures = []
    for label, fields in entries:
        for field in fields:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if field not in text:
                    failures.append(f"{target_label} missing {label} {field}")
    if failures:
        raise AssertionError(
            "release-evidence unsupported surface text-field catalogue drift: "
            + "; ".join(failures)
        )


def static_eval_release_command_node(node, constants):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.List):
        return [static_eval_release_command_node(item, constants) for item in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(static_eval_release_command_node(item, constants) for item in node.elts)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for value in node.values:
            evaluated = static_eval_release_command_node(value, constants)
            if not isinstance(evaluated, str):
                raise AssertionError("release command f-string part is not a string")
            parts.append(evaluated)
        return "".join(parts)
    if isinstance(node, ast.FormattedValue):
        return static_eval_release_command_node(node.value, constants)
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = static_eval_release_command_node(node.left, constants)
        right = static_eval_release_command_node(node.right, constants)
        if not isinstance(left, (list, tuple)) or not isinstance(right, (list, tuple)):
            raise AssertionError("release command catalogue can only concatenate lists")
        return list(left) + list(right)
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "list"
        and len(node.args) == 1
        and not node.keywords
    ):
        value = static_eval_release_command_node(node.args[0], constants)
        if not isinstance(value, (list, tuple)):
            raise AssertionError("release command catalogue list() argument is not a sequence")
        return list(value)
    raise AssertionError(
        "unsupported release command catalogue expression "
        + type(node).__name__
    )


def release_evidence_required_command_catalog(release_evidence):
    parsed = ast.parse(release_evidence)
    constants = {}
    commands_node = None
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "REQUIRED_COMMANDS":
                commands_node = node.value
            elif target.id in ("RELEASE_ZIG", "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS"):
                constants[target.id] = static_eval_release_command_node(
                    node.value,
                    constants,
                )

    if commands_node is None:
        raise AssertionError("missing REQUIRED_COMMANDS catalogue")
    if not isinstance(commands_node, ast.List):
        raise AssertionError("REQUIRED_COMMANDS must be a list")

    catalog = []
    for index, entry_node in enumerate(commands_node.elts):
        if not isinstance(entry_node, ast.Dict):
            raise AssertionError(f"REQUIRED_COMMANDS entry {index} must be a dict")
        entry = {}
        for key_node, value_node in zip(entry_node.keys, entry_node.values):
            key = static_eval_release_command_node(key_node, constants)
            if key not in RELEASE_EVIDENCE_REQUIRED_COMMAND_KEYS:
                raise AssertionError(
                    f"REQUIRED_COMMANDS entry {index} has untracked key {key!r}"
                )
            entry[key] = static_eval_release_command_node(value_node, constants)
        if not isinstance(entry.get("label"), str) or not entry["label"]:
            raise AssertionError(f"REQUIRED_COMMANDS entry {index} missing label")
        required = entry.get("required")
        if (
            not isinstance(required, list)
            or not required
            or any(not isinstance(fragment, str) or not fragment for fragment in required)
        ):
            raise AssertionError(
                f"REQUIRED_COMMANDS entry {index} must list required fragments"
            )
        forbidden = entry.get("forbidden", [])
        if not isinstance(forbidden, list) or any(
            not isinstance(fragment, str) or not fragment for fragment in forbidden
        ):
            raise AssertionError(
                f"REQUIRED_COMMANDS entry {index} forbidden fragments must be strings"
            )
        command_env_assignments = entry.get("command_env_assignments", [])
        if not isinstance(command_env_assignments, list) or any(
            not isinstance(name, str) or not name for name in command_env_assignments
        ):
            raise AssertionError(
                f"REQUIRED_COMMANDS entry {index} command_env_assignments "
                "must be strings"
            )
        skip_markers = entry.get("skip_markers", [])
        if not isinstance(skip_markers, list) or any(
            not isinstance(marker, str) or not marker for marker in skip_markers
        ):
            raise AssertionError(
                f"REQUIRED_COMMANDS entry {index} skip_markers must be strings"
            )
        output_markers = entry.get("output_markers", [])
        if not isinstance(output_markers, list) or any(
            not isinstance(marker, str) or not marker for marker in output_markers
        ):
            raise AssertionError(
                f"REQUIRED_COMMANDS entry {index} output_markers must be strings"
            )
        catalog.append(entry)
    return catalog


def release_criteria_required_command_lines(criteria):
    section = section_between(
        criteria,
        "## Required Commands",
        "\nRelease CI must",
    )
    start_marker = "```sh\n"
    start = section.find(start_marker)
    if start < 0:
        raise AssertionError("release criteria required command block missing ```sh fence")
    start += len(start_marker)
    end = section.find("\n```", start)
    if end < 0:
        raise AssertionError("release criteria required command block missing closing fence")
    block = section[start:end]
    return [line.strip() for line in block.splitlines() if line.strip()]


def required_command_line_matches(line, requirement):
    return all(fragment in line for fragment in requirement["required"]) and not any(
        fragment in line for fragment in requirement.get("forbidden", [])
    )


def assert_required_command_block_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria required command block catalogue docs",
        RELEASE_EVIDENCE_REQUIRED_COMMAND_BLOCK_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity required command block catalogue docs",
        RELEASE_EVIDENCE_REQUIRED_COMMAND_BLOCK_DOC_FRAGMENTS,
    )


def assert_required_command_block_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    if check_docs:
        assert_required_command_block_catalog_docs(release_criteria, parity)

    catalog = release_evidence_required_command_catalog(release_evidence)
    lines = release_criteria_required_command_lines(release_criteria)
    failures = []
    if len(lines) != len(catalog):
        failures.append(
            "release criteria required command block must list exactly "
            f"{len(catalog)} command lines, found {len(lines)}"
        )

    for index, requirement in enumerate(catalog):
        if index >= len(lines):
            failures.append(
                "release criteria required command block missing line "
                f"{index + 1} for {requirement['label']}"
            )
            continue
        if not required_command_line_matches(lines[index], requirement):
            failures.append(
                "release criteria required command block line "
                f"{index + 1} must match {requirement['label']}"
            )

    if failures:
        raise AssertionError(
            "required command block catalogue drift: " + "; ".join(failures)
        )


def release_evidence_required_env_catalog(release_evidence):
    names = literal_module_assignment(release_evidence, "REQUIRED_ENV_VARS")
    if (
        not isinstance(names, list)
        or not names
        or any(not isinstance(name, str) or not name for name in names)
    ):
        raise AssertionError("REQUIRED_ENV_VARS must be a non-empty string list")
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise AssertionError(
            "REQUIRED_ENV_VARS contains duplicate entries: " + ", ".join(duplicates)
        )
    return names


def assert_required_env_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria required environment catalogue docs",
        RELEASE_EVIDENCE_REQUIRED_ENV_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity required environment catalogue docs",
        RELEASE_EVIDENCE_REQUIRED_ENV_CATALOG_DOC_FRAGMENTS,
    )


def assert_required_env_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_required_env_catalog_docs(release_criteria, parity)

    names = release_evidence_required_env_catalog(release_evidence)
    failures = []
    for name in names:
        for label, text in (
            ("release criteria", release_criteria),
            ("AutoMQ parity", parity),
            ("production readiness", production_readiness),
        ):
            if name not in text:
                failures.append(f"{label} missing required environment variable {name}")
    if failures:
        raise AssertionError(
            "required environment-variable catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_command_env_assignment_catalog(release_evidence):
    catalog = release_evidence_required_command_catalog(release_evidence)
    entries = []
    failures = []
    for requirement in catalog:
        names = requirement.get("command_env_assignments", [])
        if not names:
            continue
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            failures.append(
                f"{requirement['label']} repeats command_env_assignments: "
                + ", ".join(duplicates)
            )
        entries.append((requirement["label"], names))
    if failures:
        raise AssertionError(
            "release evidence command-env assignment catalog drift: "
            + "; ".join(failures)
        )
    return entries


def assert_command_env_assignment_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria command-env assignment catalogue docs",
        RELEASE_EVIDENCE_COMMAND_ENV_ASSIGNMENT_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity command-env assignment catalogue docs",
        RELEASE_EVIDENCE_COMMAND_ENV_ASSIGNMENT_CATALOG_DOC_FRAGMENTS,
    )


def assert_command_env_assignment_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_command_env_assignment_catalog_docs(release_criteria, parity)

    entries = release_evidence_command_env_assignment_catalog(release_evidence)
    failures = []
    for label, names in entries:
        if label not in release_criteria:
            failures.append(f"release criteria missing command-env gate label {label}")
        if label not in parity:
            failures.append(f"AutoMQ parity missing command-env gate label {label}")
        for name in names:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if name not in text:
                    failures.append(
                        f"{target_label} missing command-env assignment "
                        f"{name} for {label}"
                    )
    if failures:
        raise AssertionError(
            "command environment-assignment catalogue drift: "
            + "; ".join(failures)
        )


RELEASE_EVIDENCE_COMMAND_SHAPE_CONSTANTS = (
    "ENV_ASSIGNMENT_RE",
    "ENV_NAME_RE",
    "SHELL_COMMAND_SEPARATORS",
    "SUCCESS_SHELL_COMMAND_SEPARATOR",
    "DISALLOWED_SHELL_OPERATOR_TOKENS",
    "DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS",
    "DISALLOWED_COMMAND_LINE_BREAKS",
    "DISALLOWED_COMMAND_QUOTE_CHARS",
    "DISALLOWED_COMMAND_ESCAPE_CHARS",
    "ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS",
    "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS",
    "FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS",
)


def static_eval_release_command_shape_node(node, constants):
    if isinstance(node, ast.Call):
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "compile"
            and isinstance(func.value, ast.Name)
            and func.value.id == "re"
            and len(node.args) == 1
            and not node.keywords
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            return node.args[0].value
    return static_eval_release_constant_node(node, constants)


def release_evidence_command_shape_sequence(name, values):
    if (
        not isinstance(values, (list, tuple, set))
        or not values
        or any(not isinstance(value, str) or not value for value in values)
    ):
        raise AssertionError(f"{name} must be a non-empty string sequence")
    sequence = tuple(values) if not isinstance(values, set) else tuple(sorted(values))
    duplicates = sorted({value for value in sequence if sequence.count(value) > 1})
    if duplicates:
        raise AssertionError(f"{name} repeats command-shape entries: " + ", ".join(duplicates))
    return sequence


def release_evidence_command_shape_pin(value):
    special = {
        "\n": r"\n",
        "\r": r"\r",
        "'": "single quote",
        '"': "double quote",
        "\\": "backslash",
        "`": "backtick",
    }
    if value in special:
        return special[value]
    stripped = value.strip()
    return stripped if stripped else value


def release_evidence_command_shape_catalog(release_evidence):
    wanted = set(RELEASE_EVIDENCE_COMMAND_SHAPE_CONSTANTS)
    dependencies = {"KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS"}
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (
                not isinstance(target, ast.Name)
                or target.id not in wanted | dependencies
            ):
                continue
            constants[target.id] = static_eval_release_command_shape_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence command-shape catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    regex_entries = []
    for name in ("ENV_ASSIGNMENT_RE", "ENV_NAME_RE"):
        value = constants[name]
        if not isinstance(value, str) or not value:
            failures.append(f"{name} must be a non-empty regex pattern string")
            continue
        try:
            regex_entries.append((name, re.compile(value)))
        except re.error as exc:
            failures.append(f"{name} is not a valid regex pattern: {exc}")

    regex_by_name = {name: regex for name, regex in regex_entries}
    if "ENV_ASSIGNMENT_RE" in regex_by_name:
        assignment_re = regex_by_name["ENV_ASSIGNMENT_RE"]
        if assignment_re.fullmatch("ZMQ_GATE=1") is None:
            failures.append("ENV_ASSIGNMENT_RE must match shell assignment tokens")
        if assignment_re.fullmatch("1BAD=1") is not None:
            failures.append("ENV_ASSIGNMENT_RE must reject invalid variable names")
    if "ENV_NAME_RE" in regex_by_name:
        name_re = regex_by_name["ENV_NAME_RE"]
        if name_re.fullmatch("ZMQ_GATE") is None:
            failures.append("ENV_NAME_RE must match shell variable names")
        if name_re.fullmatch("ZMQ_GATE=1") is not None:
            failures.append("ENV_NAME_RE must reject assignment tokens")

    sequence_names = (
        "SHELL_COMMAND_SEPARATORS",
        "DISALLOWED_SHELL_OPERATOR_TOKENS",
        "DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS",
        "DISALLOWED_COMMAND_LINE_BREAKS",
        "DISALLOWED_COMMAND_QUOTE_CHARS",
        "DISALLOWED_COMMAND_ESCAPE_CHARS",
        "ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS",
        "FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS",
    )
    sequences = {}
    for name in sequence_names:
        try:
            sequences[name] = release_evidence_command_shape_sequence(
                name,
                constants[name],
            )
        except AssertionError as exc:
            failures.append(str(exc))
            sequences[name] = ()

    success_separator = constants["SUCCESS_SHELL_COMMAND_SEPARATOR"]
    if not isinstance(success_separator, str) or not success_separator:
        failures.append("SUCCESS_SHELL_COMMAND_SEPARATOR must be a non-empty string")
    elif success_separator != "&&":
        failures.append("SUCCESS_SHELL_COMMAND_SEPARATOR must be &&")
    elif success_separator not in set(sequences["SHELL_COMMAND_SEPARATORS"]):
        failures.append("SUCCESS_SHELL_COMMAND_SEPARATOR must be a shell separator")

    separator_overlap = (
        set(sequences["SHELL_COMMAND_SEPARATORS"])
        & set(sequences["DISALLOWED_SHELL_OPERATOR_TOKENS"])
    )
    if separator_overlap:
        failures.append(
            "SHELL_COMMAND_SEPARATORS and DISALLOWED_SHELL_OPERATOR_TOKENS "
            "must be disjoint: "
            + ", ".join(sorted(separator_overlap))
        )

    chains = constants["ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS"]
    normalized_chains = []
    if not isinstance(chains, (list, tuple)) or not chains:
        failures.append(
            "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS must be a non-empty sequence"
        )
    else:
        for index, chain in enumerate(chains):
            if (
                not isinstance(chain, (list, tuple))
                or len(chain) != 2
                or any(not isinstance(fragment, str) or not fragment for fragment in chain)
            ):
                failures.append(
                    "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS entry "
                    f"{index} must contain two command fragments"
                )
                continue
            normalized_chains.append(tuple(chain))
    duplicate_chains = sorted(
        {
            " && ".join(chain)
            for chain in normalized_chains
            if normalized_chains.count(chain) > 1
        }
    )
    if duplicate_chains:
        failures.append(
            "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS repeats chains: "
            + ", ".join(duplicate_chains)
        )

    allowed_markers = set(sequences["ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS"])
    chain_markers = {chain[-1] for chain in normalized_chains}
    missing_chain_markers = sorted(allowed_markers - chain_markers)
    if missing_chain_markers:
        failures.append(
            "ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS missing allowed chains: "
            + ", ".join(missing_chain_markers)
        )

    exact_marker_overlap = allowed_markers & set(
        sequences["FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS"]
    )
    if exact_marker_overlap:
        failures.append(
            "allowed and forbidden command output markers overlap: "
            + ", ".join(sorted(exact_marker_overlap))
        )

    kraft_markers = constants.get("KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS", ())
    if isinstance(kraft_markers, (list, tuple, set)):
        missing_kraft_forbidden = sorted(
            set(kraft_markers)
            - set(sequences["FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS"])
        )
        if missing_kraft_forbidden:
            failures.append(
                "FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS missing KRaft "
                "detail markers: "
                + ", ".join(missing_kraft_forbidden)
            )

    if failures:
        raise AssertionError(
            "release-evidence command-shape catalogue drift: "
            + "; ".join(failures)
        )

    return (
        ("command-shape constant", RELEASE_EVIDENCE_COMMAND_SHAPE_CONSTANTS),
        (
            "command-shape regex",
            (constants["ENV_ASSIGNMENT_RE"], constants["ENV_NAME_RE"]),
        ),
        ("shell command separator", tuple(sorted(sequences["SHELL_COMMAND_SEPARATORS"]))),
        ("success shell command separator", (success_separator,)),
        (
            "disallowed shell operator token",
            tuple(
                release_evidence_command_shape_pin(value)
                for value in sorted(sequences["DISALLOWED_SHELL_OPERATOR_TOKENS"])
            ),
        ),
        (
            "disallowed command-substitution fragment",
            tuple(
                release_evidence_command_shape_pin(value)
                for value in sequences["DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS"]
            ),
        ),
        (
            "disallowed line-break token",
            tuple(
                release_evidence_command_shape_pin(value)
                for value in sequences["DISALLOWED_COMMAND_LINE_BREAKS"]
            ),
        ),
        (
            "disallowed quote token",
            tuple(
                release_evidence_command_shape_pin(value)
                for value in sequences["DISALLOWED_COMMAND_QUOTE_CHARS"]
            ),
        ),
        (
            "disallowed escape token",
            tuple(
                release_evidence_command_shape_pin(value)
                for value in sequences["DISALLOWED_COMMAND_ESCAPE_CHARS"]
            ),
        ),
        (
            "allowed command output marker fragment",
            sequences["ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS"],
        ),
        (
            "allowed multi-segment command chain",
            tuple(" && ".join(chain) for chain in normalized_chains),
        ),
        (
            "forbidden command output marker fragment",
            tuple(
                release_evidence_command_shape_pin(value)
                for value in sequences["FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS"]
            ),
        ),
    )


def assert_command_shape_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria command-shape catalogue docs",
        RELEASE_EVIDENCE_COMMAND_SHAPE_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity command-shape catalogue docs",
        RELEASE_EVIDENCE_COMMAND_SHAPE_CATALOG_DOC_FRAGMENTS,
    )


def assert_command_shape_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_command_shape_catalog_docs(release_criteria, parity)

    entries = release_evidence_command_shape_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence command-shape catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_skip_marker_catalog(release_evidence):
    catalog = release_evidence_required_command_catalog(release_evidence)
    entries = []
    failures = []
    for requirement in catalog:
        markers = requirement.get("skip_markers", [])
        if not markers:
            continue
        duplicates = sorted({marker for marker in markers if markers.count(marker) > 1})
        if duplicates:
            failures.append(
                f"{requirement['label']} repeats skip_markers: "
                + ", ".join(duplicates)
            )
        entries.append((requirement["label"], markers))
    if failures:
        raise AssertionError(
            "release evidence skip-marker catalog drift: "
            + "; ".join(failures)
        )
    return entries


def assert_skip_marker_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria skip-marker catalogue docs",
        RELEASE_EVIDENCE_SKIP_MARKER_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity skip-marker catalogue docs",
        RELEASE_EVIDENCE_SKIP_MARKER_CATALOG_DOC_FRAGMENTS,
    )


def assert_skip_marker_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_skip_marker_catalog_docs(release_criteria, parity)

    entries = release_evidence_skip_marker_catalog(release_evidence)
    failures = []
    for label, markers in entries:
        if label not in release_criteria:
            failures.append(f"release criteria missing skip-marker gate label {label}")
        if label not in parity:
            failures.append(f"AutoMQ parity missing skip-marker gate label {label}")
        for marker in markers:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if marker not in text:
                    failures.append(
                        f"{target_label} missing skip marker {marker!r} for {label}"
                    )
    if failures:
        raise AssertionError(
            "release-evidence skip-marker catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_output_marker_catalog(release_evidence):
    catalog = release_evidence_required_command_catalog(release_evidence)
    entries = []
    failures = []
    for requirement in catalog:
        markers = requirement.get("output_markers", [])
        if not markers:
            continue
        duplicates = sorted({marker for marker in markers if markers.count(marker) > 1})
        if duplicates:
            failures.append(
                f"{requirement['label']} repeats output_markers: "
                + ", ".join(duplicates)
            )
        entries.append((requirement["label"], markers))
    if failures:
        raise AssertionError(
            "release evidence output-marker catalog drift: "
            + "; ".join(failures)
        )
    return entries


def assert_output_marker_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria output-marker catalogue docs",
        RELEASE_EVIDENCE_OUTPUT_MARKER_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity output-marker catalogue docs",
        RELEASE_EVIDENCE_OUTPUT_MARKER_CATALOG_DOC_FRAGMENTS,
    )


def assert_output_marker_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_output_marker_catalog_docs(release_criteria, parity)

    entries = release_evidence_output_marker_catalog(release_evidence)
    failures = []
    for label, markers in entries:
        if label not in release_criteria:
            failures.append(f"release criteria missing output-marker gate label {label}")
        if label not in parity:
            failures.append(f"AutoMQ parity missing output-marker gate label {label}")
        for marker in markers:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if marker not in text:
                    failures.append(
                        f"{target_label} missing output marker {marker!r} for {label}"
                    )
    if failures:
        raise AssertionError(
            "release-evidence output-marker catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_forbidden_fragment_catalog(release_evidence):
    catalog = release_evidence_required_command_catalog(release_evidence)
    entries = []
    failures = []
    for requirement in catalog:
        fragments = requirement.get("forbidden", [])
        if not fragments:
            continue
        duplicates = sorted(
            {fragment for fragment in fragments if fragments.count(fragment) > 1}
        )
        if duplicates:
            failures.append(
                f"{requirement['label']} repeats forbidden fragments: "
                + ", ".join(duplicates)
            )
        entries.append((requirement["label"], fragments))
    if failures:
        raise AssertionError(
            "release evidence forbidden-fragment catalog drift: "
            + "; ".join(failures)
        )
    return entries


def assert_forbidden_fragment_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria forbidden-fragment catalogue docs",
        RELEASE_EVIDENCE_FORBIDDEN_FRAGMENT_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity forbidden-fragment catalogue docs",
        RELEASE_EVIDENCE_FORBIDDEN_FRAGMENT_CATALOG_DOC_FRAGMENTS,
    )


def assert_forbidden_fragment_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_forbidden_fragment_catalog_docs(release_criteria, parity)

    entries = release_evidence_forbidden_fragment_catalog(release_evidence)
    failures = []
    for label, fragments in entries:
        if label not in release_criteria:
            failures.append(f"release criteria missing forbidden-fragment gate label {label}")
        if label not in parity:
            failures.append(f"AutoMQ parity missing forbidden-fragment gate label {label}")
        for fragment in fragments:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if fragment not in text:
                    failures.append(
                        f"{target_label} missing forbidden command fragment "
                        f"{fragment!r} for {label}"
                    )
    if failures:
        raise AssertionError(
            "forbidden command-fragment catalogue drift: "
            + "; ".join(failures)
        )


def static_eval_schema_field_node(node, constants):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    if isinstance(node, (ast.List, ast.Tuple)):
        values = []
        for item in node.elts:
            if isinstance(item, ast.Starred):
                starred = static_eval_schema_field_node(item.value, constants)
                if not isinstance(starred, (list, tuple)):
                    raise AssertionError(
                        "release evidence schema field star expression is not a sequence"
                    )
                values.extend(starred)
            else:
                values.append(static_eval_schema_field_node(item, constants))
        return tuple(values) if isinstance(node, ast.Tuple) else values
    raise AssertionError(
        "unsupported release evidence schema field expression "
        + type(node).__name__
    )


def release_evidence_schema_field_catalog(release_evidence):
    wanted = {
        "RELEASE_EVIDENCE_FIELDS",
        "COMMAND_ENTRY_FIELDS",
        "REQUIRED_UNSUPPORTED_SURFACE_FIELDS",
        "OPTIONAL_UNSUPPORTED_SURFACE_FIELDS",
        "UNSUPPORTED_SURFACE_FIELDS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_schema_field_node(node.value, constants)

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence schema field catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    for name in sorted(wanted):
        fields = constants[name]
        if (
            not isinstance(fields, (list, tuple))
            or not fields
            or any(not isinstance(field, str) or not field for field in fields)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({field for field in fields if fields.count(field) > 1})
        if duplicates:
            failures.append(f"{name} repeats schema fields: " + ", ".join(duplicates))

    expected_unsupported = tuple(constants["REQUIRED_UNSUPPORTED_SURFACE_FIELDS"]) + tuple(
        constants["OPTIONAL_UNSUPPORTED_SURFACE_FIELDS"]
    )
    if tuple(constants["UNSUPPORTED_SURFACE_FIELDS"]) != expected_unsupported:
        failures.append(
            "UNSUPPORTED_SURFACE_FIELDS must equal required plus optional "
            "unsupported-surface fields"
        )
    if failures:
        raise AssertionError(
            "release-evidence schema field catalogue drift: "
            + "; ".join(failures)
        )

    return (
        ("release manifest", tuple(constants["RELEASE_EVIDENCE_FIELDS"])),
        ("command entry", tuple(constants["COMMAND_ENTRY_FIELDS"])),
        ("unsupported surface", tuple(constants["UNSUPPORTED_SURFACE_FIELDS"])),
    )


def assert_schema_field_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria schema-field catalogue docs",
        RELEASE_EVIDENCE_SCHEMA_FIELD_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity schema-field catalogue docs",
        RELEASE_EVIDENCE_SCHEMA_FIELD_CATALOG_DOC_FRAGMENTS,
    )


def assert_schema_field_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_schema_field_catalog_docs(release_criteria, parity)

    entries = release_evidence_schema_field_catalog(release_evidence)
    failures = []
    for label, fields in entries:
        if label not in release_criteria:
            failures.append(f"release criteria missing schema-field label {label}")
        if label not in parity:
            failures.append(f"AutoMQ parity missing schema-field label {label}")
        for field in fields:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if field not in text:
                    failures.append(
                        f"{target_label} missing {label} closed schema field {field}"
                    )
    if failures:
        raise AssertionError(
            "release-evidence schema field catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_blocking_flag_catalog(release_evidence):
    wanted = {"BLOCKING_FLAGS", "RELEASE_EVIDENCE_FIELDS"}
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_schema_field_node(node.value, constants)

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence blocking-flag catalogue missing assignments: "
            + ", ".join(missing)
        )

    flags = constants["BLOCKING_FLAGS"]
    manifest_fields = constants["RELEASE_EVIDENCE_FIELDS"]
    failures = []
    if (
        not isinstance(flags, (list, tuple))
        or not flags
        or any(not isinstance(flag, str) or not flag for flag in flags)
    ):
        failures.append("BLOCKING_FLAGS must be a non-empty string sequence")
    else:
        duplicates = sorted({flag for flag in flags if flags.count(flag) > 1})
        if duplicates:
            failures.append(
                "BLOCKING_FLAGS repeats blocking flags: " + ", ".join(duplicates)
            )

    if not isinstance(manifest_fields, (list, tuple)):
        failures.append("RELEASE_EVIDENCE_FIELDS must be a sequence for BLOCKING_FLAGS")
    elif isinstance(flags, (list, tuple)):
        unknown = sorted(
            {
                flag
                for flag in flags
                if isinstance(flag, str) and flag not in manifest_fields
            }
        )
        if unknown:
            failures.append(
                "BLOCKING_FLAGS entries must be release manifest fields: "
                + ", ".join(unknown)
            )

    if failures:
        raise AssertionError(
            "release-evidence blocking-flag catalogue drift: "
            + "; ".join(failures)
        )
    return tuple(flags)


def assert_blocking_flag_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria blocking-flag catalogue docs",
        RELEASE_EVIDENCE_BLOCKING_FLAG_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity blocking-flag catalogue docs",
        RELEASE_EVIDENCE_BLOCKING_FLAG_CATALOG_DOC_FRAGMENTS,
    )


def assert_blocking_flag_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_blocking_flag_catalog_docs(release_criteria, parity)

    flags = release_evidence_blocking_flag_catalog(release_evidence)
    failures = []
    for flag in flags:
        false_pin = f"{flag}=false"
        for target_label, text in (
            ("release criteria", release_criteria),
            ("AutoMQ parity", parity),
            ("production readiness", production_readiness),
        ):
            if false_pin not in text:
                failures.append(
                    f"{target_label} missing blocking flag false pin {false_pin}"
                )
    if failures:
        raise AssertionError(
            "release-evidence blocking-flag catalogue drift: "
            + "; ".join(failures)
        )


def static_eval_release_constant_node(node, constants):
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name) and node.id in constants:
        return constants[node.id]
    if isinstance(node, ast.List):
        values = []
        for item in node.elts:
            if isinstance(item, ast.Starred):
                expanded = static_eval_release_constant_node(item.value, constants)
                if not isinstance(expanded, (list, tuple, set)):
                    raise AssertionError(
                        "release evidence starred expression must expand "
                        "to a sequence"
                    )
                values.extend(expanded)
                continue
            values.append(static_eval_release_constant_node(item, constants))
        return values
    if isinstance(node, ast.Tuple):
        values = []
        for item in node.elts:
            if isinstance(item, ast.Starred):
                expanded = static_eval_release_constant_node(item.value, constants)
                if not isinstance(expanded, (list, tuple, set)):
                    raise AssertionError(
                        "release evidence starred expression must expand "
                        "to a sequence"
                    )
                values.extend(expanded)
                continue
            values.append(static_eval_release_constant_node(item, constants))
        return tuple(values)
    if isinstance(node, ast.Set):
        values = []
        for item in node.elts:
            if isinstance(item, ast.Starred):
                expanded = static_eval_release_constant_node(item.value, constants)
                if not isinstance(expanded, (list, tuple, set)):
                    raise AssertionError(
                        "release evidence starred expression must expand "
                        "to a sequence"
                    )
                values.extend(expanded)
                continue
            values.append(static_eval_release_constant_node(item, constants))
        return values
    if isinstance(node, ast.Dict):
        return {
            static_eval_release_constant_node(key, constants): static_eval_release_constant_node(
                value,
                constants,
            )
            for key, value in zip(node.keys, node.values)
        }
    raise AssertionError(
        "unsupported release evidence constant expression " + type(node).__name__
    )


def release_evidence_numeric_env_catalog(release_evidence):
    wanted = {
        "BENCHMARK_THRESHOLD_ENV_VARS",
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV",
        "POSITIVE_INTEGER_ENV_VARS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence numeric environment catalogue missing assignments: "
            + ", ".join(missing)
        )

    threshold_vars = constants["BENCHMARK_THRESHOLD_ENV_VARS"]
    positive_integer_vars = constants["POSITIVE_INTEGER_ENV_VARS"]
    threshold_map = constants["COMPARATIVE_BENCHMARK_THRESHOLD_ENV"]
    failures = []

    for name, values in (
        ("BENCHMARK_THRESHOLD_ENV_VARS", threshold_vars),
        ("POSITIVE_INTEGER_ENV_VARS", positive_integer_vars),
    ):
        if (
            not isinstance(values, (list, tuple))
            or not values
            or any(not isinstance(value, str) or not value for value in values)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(f"{name} repeats environment variables: " + ", ".join(duplicates))

    if not isinstance(threshold_map, (list, tuple)) or not threshold_map:
        failures.append("COMPARATIVE_BENCHMARK_THRESHOLD_ENV must be a non-empty sequence")
        threshold_map_names = ()
    else:
        threshold_map_names = []
        threshold_keys = []
        for index, entry in enumerate(threshold_map):
            if (
                not isinstance(entry, (list, tuple))
                or len(entry) != 2
                or not isinstance(entry[0], str)
                or not entry[0]
                or not isinstance(entry[1], str)
                or not entry[1]
            ):
                failures.append(
                    f"COMPARATIVE_BENCHMARK_THRESHOLD_ENV entry {index} "
                    "must be a pair of non-empty strings"
                )
                continue
            threshold_map_names.append(entry[0])
            threshold_keys.append(entry[1])
        for label, values in (
            ("environment variables", threshold_map_names),
            ("threshold keys", threshold_keys),
        ):
            duplicates = sorted({value for value in values if values.count(value) > 1})
            if duplicates:
                failures.append(
                    f"COMPARATIVE_BENCHMARK_THRESHOLD_ENV repeats {label}: "
                    + ", ".join(duplicates)
                )
        if tuple(threshold_map_names) != tuple(threshold_vars):
            failures.append(
                "COMPARATIVE_BENCHMARK_THRESHOLD_ENV variables must match "
                "BENCHMARK_THRESHOLD_ENV_VARS in order"
            )

    if failures:
        raise AssertionError(
            "release-evidence numeric environment catalogue drift: "
            + "; ".join(failures)
        )
    return (
        ("benchmark threshold", tuple(threshold_vars)),
        ("positive integer", tuple(sorted(positive_integer_vars))),
    )


def assert_numeric_env_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria numeric environment catalogue docs",
        RELEASE_EVIDENCE_NUMERIC_ENV_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity numeric environment catalogue docs",
        RELEASE_EVIDENCE_NUMERIC_ENV_CATALOG_DOC_FRAGMENTS,
    )


def assert_numeric_env_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_numeric_env_catalog_docs(release_criteria, parity)

    entries = release_evidence_numeric_env_catalog(release_evidence)
    failures = []
    for label, names in entries:
        for name in names:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if name not in text:
                    failures.append(
                        f"{target_label} missing {label} environment variable {name}"
                    )
    if failures:
        raise AssertionError(
            "release-evidence numeric environment catalogue drift: "
            + "; ".join(failures)
        )


RELEASE_EVIDENCE_COVERAGE_SELECTOR_REQUIRED_KEYS = {
    "selector",
    "required",
    "label",
    "token_style",
}
RELEASE_EVIDENCE_COVERAGE_SELECTOR_KEYS = (
    "selector",
    "required",
    "label",
    "token_style",
    "fixture",
)
RELEASE_EVIDENCE_COVERAGE_SELECTOR_TOKEN_STYLES = {"collapsed", "literal"}


def release_evidence_coverage_selector_catalog(release_evidence):
    wanted = {"COVERAGE_SELECTOR_REQUIREMENTS", "REQUIRED_ENV_VARS"}
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence coverage selector catalogue missing assignments: "
            + ", ".join(missing)
        )

    requirements = constants["COVERAGE_SELECTOR_REQUIREMENTS"]
    required_env_vars = constants["REQUIRED_ENV_VARS"]
    failures = []
    if (
        not isinstance(required_env_vars, (list, tuple))
        or not required_env_vars
        or any(not isinstance(name, str) or not name for name in required_env_vars)
    ):
        failures.append("REQUIRED_ENV_VARS must be a non-empty string sequence")
    if not isinstance(requirements, list) or not requirements:
        failures.append("COVERAGE_SELECTOR_REQUIREMENTS must be a non-empty list")
        requirements = []

    entries = []
    selectors = []
    required_names = []
    for index, entry in enumerate(requirements):
        if not isinstance(entry, dict):
            failures.append(f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} must be a dict")
            continue
        unknown_keys = sorted(
            key for key in entry.keys() if key not in RELEASE_EVIDENCE_COVERAGE_SELECTOR_KEYS
        )
        if unknown_keys:
            failures.append(
                f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} has untracked keys: "
                + ", ".join(unknown_keys)
            )
        missing_keys = sorted(
            RELEASE_EVIDENCE_COVERAGE_SELECTOR_REQUIRED_KEYS - set(entry.keys())
        )
        if missing_keys:
            failures.append(
                f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} missing keys: "
                + ", ".join(missing_keys)
            )
            continue
        bad_value = False
        for key in RELEASE_EVIDENCE_COVERAGE_SELECTOR_REQUIRED_KEYS:
            if not isinstance(entry.get(key), str) or not entry[key]:
                failures.append(
                    f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} {key} "
                    "must be a non-empty string"
                )
                bad_value = True
        if "fixture" in entry and (
            not isinstance(entry["fixture"], str) or not entry["fixture"]
        ):
            failures.append(
                f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} fixture "
                "must be a non-empty string"
            )
            bad_value = True
        if bad_value:
            continue
        if entry["token_style"] not in RELEASE_EVIDENCE_COVERAGE_SELECTOR_TOKEN_STYLES:
            failures.append(
                f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} has unknown "
                f"token_style {entry['token_style']}"
            )
        if (
            isinstance(required_env_vars, (list, tuple))
            and entry["required"] not in required_env_vars
        ):
            failures.append(
                f"COVERAGE_SELECTOR_REQUIREMENTS entry {index} required variable "
                f"{entry['required']} is not in REQUIRED_ENV_VARS"
            )
        selectors.append(entry["selector"])
        required_names.append(entry["required"])
        entries.append(entry)

    for label, values in (("selectors", selectors), ("required variables", required_names)):
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                "COVERAGE_SELECTOR_REQUIREMENTS repeats "
                + label
                + ": "
                + ", ".join(duplicates)
            )

    if failures:
        raise AssertionError(
            "release-evidence coverage selector catalogue drift: "
            + "; ".join(failures)
        )
    return entries


def assert_coverage_selector_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria coverage selector catalogue docs",
        RELEASE_EVIDENCE_COVERAGE_SELECTOR_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity coverage selector catalogue docs",
        RELEASE_EVIDENCE_COVERAGE_SELECTOR_CATALOG_DOC_FRAGMENTS,
    )


def assert_coverage_selector_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_coverage_selector_catalog_docs(release_criteria, parity)

    entries = release_evidence_coverage_selector_catalog(release_evidence)
    failures = []
    for entry in entries:
        fragments = [entry["selector"], entry["required"], entry["label"]]
        if "fixture" in entry:
            fragments.append(entry["fixture"])
        for fragment in fragments:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if fragment not in text:
                    failures.append(
                        f"{target_label} missing coverage selector fragment "
                        f"{fragment!r}"
                    )
    if failures:
        raise AssertionError(
            "release-evidence coverage selector catalogue drift: "
            + "; ".join(failures)
        )


def static_eval_comma_env_catalog_node(node, constants):
    if not isinstance(node, ast.ListComp):
        raise AssertionError("COMMA_SEPARATED_ENV_VARS must be a list comprehension")
    if len(node.generators) != 1:
        raise AssertionError(
            "COMMA_SEPARATED_ENV_VARS must have one list-comprehension generator"
        )
    generator = node.generators[0]
    if not isinstance(generator.target, ast.Name):
        raise AssertionError("COMMA_SEPARATED_ENV_VARS target must be a name")
    target_name = generator.target.id
    if not isinstance(node.elt, ast.Name) or node.elt.id != target_name:
        raise AssertionError(
            "COMMA_SEPARATED_ENV_VARS must yield the generator target"
        )
    if not isinstance(generator.iter, ast.Name) or generator.iter.id != "REQUIRED_ENV_VARS":
        raise AssertionError(
            "COMMA_SEPARATED_ENV_VARS must iterate REQUIRED_ENV_VARS"
        )
    if len(generator.ifs) != 1:
        raise AssertionError(
            "COMMA_SEPARATED_ENV_VARS must have one exclusion predicate"
        )

    predicate = generator.ifs[0]
    if (
        not isinstance(predicate, ast.Compare)
        or len(predicate.ops) != 1
        or not isinstance(predicate.ops[0], ast.NotIn)
        or len(predicate.comparators) != 1
        or not isinstance(predicate.left, ast.Name)
        or predicate.left.id != target_name
    ):
        raise AssertionError(
            "COMMA_SEPARATED_ENV_VARS must exclude names with a not-in predicate"
        )
    exclusions = static_eval_release_constant_node(predicate.comparators[0], constants)
    if (
        not isinstance(exclusions, (list, tuple))
        or not exclusions
        or any(not isinstance(name, str) or not name for name in exclusions)
    ):
        raise AssertionError(
            "COMMA_SEPARATED_ENV_VARS exclusions must be a non-empty string sequence"
        )
    required = constants["REQUIRED_ENV_VARS"]
    return [name for name in required if name not in exclusions], tuple(exclusions)


def release_evidence_comma_env_catalog(release_evidence):
    parsed = ast.parse(release_evidence)
    constants = {}
    comma_env_node = None
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "REQUIRED_ENV_VARS":
                constants[target.id] = static_eval_release_constant_node(
                    node.value,
                    constants,
                )
            elif target.id == "COMMA_SEPARATED_ENV_VARS":
                comma_env_node = node.value

    missing = []
    if "REQUIRED_ENV_VARS" not in constants:
        missing.append("REQUIRED_ENV_VARS")
    if comma_env_node is None:
        missing.append("COMMA_SEPARATED_ENV_VARS")
    if missing:
        raise AssertionError(
            "release evidence comma-separated environment catalogue missing assignments: "
            + ", ".join(missing)
        )

    required = constants["REQUIRED_ENV_VARS"]
    failures = []
    if (
        not isinstance(required, (list, tuple))
        or not required
        or any(not isinstance(name, str) or not name for name in required)
    ):
        failures.append("REQUIRED_ENV_VARS must be a non-empty string sequence")
    else:
        duplicates = sorted({name for name in required if required.count(name) > 1})
        if duplicates:
            failures.append(
                "REQUIRED_ENV_VARS repeats environment variables: "
                + ", ".join(duplicates)
            )

    if failures:
        raise AssertionError(
            "release-evidence comma-separated environment catalogue drift: "
            + "; ".join(failures)
        )

    names, exclusions = static_eval_comma_env_catalog_node(comma_env_node, constants)
    failures = []
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        failures.append(
            "COMMA_SEPARATED_ENV_VARS repeats environment variables: "
            + ", ".join(duplicates)
        )
    duplicate_exclusions = sorted(
        {name for name in exclusions if exclusions.count(name) > 1}
    )
    if duplicate_exclusions:
        failures.append(
            "COMMA_SEPARATED_ENV_VARS repeats exclusions: "
            + ", ".join(duplicate_exclusions)
        )
    unknown_exclusions = sorted(name for name in exclusions if name not in required)
    if unknown_exclusions:
        failures.append(
            "COMMA_SEPARATED_ENV_VARS exclusions are not in REQUIRED_ENV_VARS: "
            + ", ".join(unknown_exclusions)
        )
    if failures:
        raise AssertionError(
            "release-evidence comma-separated environment catalogue drift: "
            + "; ".join(failures)
        )
    return tuple(names), tuple(exclusions)


def assert_comma_env_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria comma-separated environment catalogue docs",
        RELEASE_EVIDENCE_COMMA_ENV_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity comma-separated environment catalogue docs",
        RELEASE_EVIDENCE_COMMA_ENV_CATALOG_DOC_FRAGMENTS,
    )


def assert_comma_env_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_comma_env_catalog_docs(release_criteria, parity)

    names, exclusions = release_evidence_comma_env_catalog(release_evidence)
    failures = []
    for label, values in (
        ("comma-separated environment variable", names),
        ("non-comma-separated environment variable", exclusions),
    ):
        for name in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if name not in text:
                    failures.append(f"{target_label} missing {label} {name}")
    if failures:
        raise AssertionError(
            "release-evidence comma-separated environment catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_boolean_env_catalog(release_evidence):
    wanted = {
        "BOOLEAN_ENV_VARS",
        "CLIENT_PROFILE_BOOL_SUFFIXES",
        "E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES",
        "S3_BOOL_SUFFIXES",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence boolean environment catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    for name in sorted(wanted):
        values = constants[name]
        if (
            not isinstance(values, (list, tuple))
            or not values
            or any(not isinstance(value, str) or not value for value in values)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                f"{name} repeats boolean entries: " + ", ".join(duplicates)
            )

    if failures:
        raise AssertionError(
            "release-evidence boolean environment catalogue drift: "
            + "; ".join(failures)
        )
    return (
        ("boolean environment variable", tuple(sorted(constants["BOOLEAN_ENV_VARS"]))),
        (
            "client profile boolean suffix",
            tuple(constants["CLIENT_PROFILE_BOOL_SUFFIXES"]),
        ),
        (
            "E2E load/scale fixture boolean suffix",
            tuple(constants["E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES"]),
        ),
        ("S3 boolean suffix", tuple(constants["S3_BOOL_SUFFIXES"])),
    )


def assert_boolean_env_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria boolean environment catalogue docs",
        RELEASE_EVIDENCE_BOOLEAN_ENV_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity boolean environment catalogue docs",
        RELEASE_EVIDENCE_BOOLEAN_ENV_CATALOG_DOC_FRAGMENTS,
    )


def assert_boolean_env_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_boolean_env_catalog_docs(release_criteria, parity)

    entries = release_evidence_boolean_env_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence boolean environment catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_token_vocabulary_catalog(release_evidence):
    wanted = {
        "PLACEHOLDER_ENV_VALUES",
        "BOOL_TRUE_VALUES",
        "BOOL_FALSE_VALUES",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence token vocabulary catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    normalized = {}
    for name in sorted(wanted):
        values = constants[name]
        if (
            not isinstance(values, (list, tuple, set))
            or not values
            or any(not isinstance(value, str) or not value for value in values)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            normalized[name] = ()
            continue
        sequence = tuple(values) if not isinstance(values, set) else tuple(sorted(values))
        duplicates = sorted({value for value in sequence if sequence.count(value) > 1})
        if duplicates:
            failures.append(f"{name} repeats token values: " + ", ".join(duplicates))
        non_lower = sorted(value for value in sequence if value != value.lower())
        if non_lower:
            failures.append(f"{name} entries must be lowercase: " + ", ".join(non_lower))
        normalized[name] = tuple(sorted(sequence))

    true_values = set(normalized.get("BOOL_TRUE_VALUES", ()))
    false_values = set(normalized.get("BOOL_FALSE_VALUES", ()))
    placeholder_values = set(normalized.get("PLACEHOLDER_ENV_VALUES", ()))
    if true_values & false_values:
        failures.append(
            "BOOL_TRUE_VALUES and BOOL_FALSE_VALUES must be disjoint: "
            + ", ".join(sorted(true_values & false_values))
        )
    if placeholder_values & (true_values | false_values):
        failures.append(
            "PLACEHOLDER_ENV_VALUES must not overlap boolean tokens: "
            + ", ".join(sorted(placeholder_values & (true_values | false_values)))
        )

    if failures:
        raise AssertionError(
            "release-evidence token vocabulary catalogue drift: "
            + "; ".join(failures)
        )

    return (
        ("placeholder token", normalized["PLACEHOLDER_ENV_VALUES"]),
        ("boolean true token", normalized["BOOL_TRUE_VALUES"]),
        ("boolean false token", normalized["BOOL_FALSE_VALUES"]),
    )


def assert_token_vocabulary_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria token vocabulary catalogue docs",
        RELEASE_EVIDENCE_TOKEN_VOCABULARY_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity token vocabulary catalogue docs",
        RELEASE_EVIDENCE_TOKEN_VOCABULARY_CATALOG_DOC_FRAGMENTS,
    )


def assert_token_vocabulary_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_token_vocabulary_catalog_docs(release_criteria, parity)

    entries = release_evidence_token_vocabulary_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence token vocabulary catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_s3_string_env_catalog(release_evidence):
    parsed = ast.parse(release_evidence)
    values = None
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id != "S3_STRING_SUFFIXES":
                continue
            values = static_eval_release_constant_node(node.value, {})

    if values is None:
        raise AssertionError(
            "release evidence S3 string environment catalogue missing assignment: "
            "S3_STRING_SUFFIXES"
        )

    failures = []
    if (
        not isinstance(values, (list, tuple))
        or not values
        or any(not isinstance(value, str) or not value for value in values)
    ):
        failures.append("S3_STRING_SUFFIXES must be a non-empty string sequence")
    else:
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                "S3_STRING_SUFFIXES repeats string suffixes: " + ", ".join(duplicates)
            )

    if failures:
        raise AssertionError(
            "release-evidence S3 string environment catalogue drift: "
            + "; ".join(failures)
        )
    return tuple(values)


def assert_s3_string_env_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria S3 string environment catalogue docs",
        RELEASE_EVIDENCE_S3_STRING_ENV_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity S3 string environment catalogue docs",
        RELEASE_EVIDENCE_S3_STRING_ENV_CATALOG_DOC_FRAGMENTS,
    )


def assert_s3_string_env_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_s3_string_env_catalog_docs(release_criteria, parity)

    suffixes = release_evidence_s3_string_env_catalog(release_evidence)
    failures = []
    for suffix in suffixes:
        for target_label, text in (
            ("release criteria", release_criteria),
            ("AutoMQ parity", parity),
            ("production readiness", production_readiness),
        ):
            if suffix not in text:
                failures.append(f"{target_label} missing S3 string suffix {suffix}")
    if failures:
        raise AssertionError(
            "release-evidence S3 string environment catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_s3_scoped_marker_catalog(release_evidence):
    parsed = ast.parse(release_evidence)
    values = None
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (
                not isinstance(target, ast.Name)
                or target.id != "S3_PROVIDER_SCOPED_MARKER_TEMPLATES"
            ):
                continue
            values = static_eval_release_constant_node(node.value, {})

    if values is None:
        raise AssertionError(
            "release evidence S3 scoped marker catalogue missing assignment: "
            "S3_PROVIDER_SCOPED_MARKER_TEMPLATES"
        )

    failures = []
    entries = []
    if not isinstance(values, (list, tuple)) or not values:
        failures.append(
            "S3_PROVIDER_SCOPED_MARKER_TEMPLATES must be a non-empty sequence"
        )
    else:
        for index, entry in enumerate(values):
            if (
                not isinstance(entry, (list, tuple))
                or len(entry) != 2
                or any(not isinstance(value, str) or not value for value in entry)
            ):
                failures.append(
                    "S3_PROVIDER_SCOPED_MARKER_TEMPLATES entry "
                    f"{index} must contain marker and template strings"
                )
                continue
            marker, template = entry
            if template.count("{profile}") != 1:
                failures.append(
                    f"S3 scoped marker {marker} template must contain "
                    "one {profile} placeholder"
                )
            rendered = template.replace("{profile}", "example")
            if not rendered.startswith(f"ok: S3 provider {marker} profile example "):
                failures.append(
                    f"S3 scoped marker {marker} template does not match "
                    "its marker key"
                )
            entries.append((marker, template))

    markers = [marker for marker, _template in entries]
    duplicate_markers = sorted(
        {marker for marker in markers if markers.count(marker) > 1}
    )
    if duplicate_markers:
        failures.append(
            "S3_PROVIDER_SCOPED_MARKER_TEMPLATES repeats markers: "
            + ", ".join(duplicate_markers)
        )
    rendered_templates = [
        template.replace("{profile}", "<profile>")
        for _marker, template in entries
    ]
    duplicate_templates = sorted(
        {
            template
            for template in rendered_templates
            if rendered_templates.count(template) > 1
        }
    )
    if duplicate_templates:
        failures.append(
            "S3_PROVIDER_SCOPED_MARKER_TEMPLATES repeats templates: "
            + ", ".join(duplicate_templates)
        )

    if failures:
        raise AssertionError(
            "release-evidence S3 scoped marker catalogue drift: "
            + "; ".join(failures)
        )

    return (
        (
            "S3 scoped marker constant",
            ("S3_PROVIDER_SCOPED_MARKER_TEMPLATES",),
        ),
        ("S3 scoped marker key", tuple(markers)),
        ("S3 scoped marker template", tuple(rendered_templates)),
    )


def assert_s3_scoped_marker_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria S3 scoped marker catalogue docs",
        RELEASE_EVIDENCE_S3_SCOPED_MARKER_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity S3 scoped marker catalogue docs",
        RELEASE_EVIDENCE_S3_SCOPED_MARKER_CATALOG_DOC_FRAGMENTS,
    )


def assert_s3_scoped_marker_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_s3_scoped_marker_catalog_docs(release_criteria, parity)

    entries = release_evidence_s3_scoped_marker_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence S3 scoped marker catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_sample_env_output_catalog(release_evidence):
    parsed = ast.parse(release_evidence)
    values = None
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (
                not isinstance(target, ast.Name)
                or target.id != "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS"
            ):
                continue
            values = static_eval_release_constant_node(node.value, {})

    if values is None:
        raise AssertionError(
            "release evidence sample environment output catalogue missing "
            "assignment: SAMPLE_ENVIRONMENT_OUTPUT_MARKERS"
        )

    failures = []
    if not isinstance(values, dict) or not values:
        failures.append("SAMPLE_ENVIRONMENT_OUTPUT_MARKERS must be a non-empty dict")
        values = {}

    labels = []
    markers = []
    for label, label_markers in values.items():
        if not isinstance(label, str) or not label:
            failures.append("SAMPLE_ENVIRONMENT_OUTPUT_MARKERS labels must be strings")
            continue
        labels.append(label)
        if (
            not isinstance(label_markers, (list, tuple))
            or not label_markers
            or any(
                not isinstance(marker, str) or not marker
                for marker in label_markers
            )
        ):
            failures.append(f"{label} sample output markers must be strings")
            continue
        duplicate_markers = sorted(
            {
                marker
                for marker in label_markers
                if label_markers.count(marker) > 1
            }
        )
        if duplicate_markers:
            failures.append(
                f"{label} repeats sample output markers: "
                + ", ".join(duplicate_markers)
            )
        markers.extend(label_markers)

    duplicate_labels = sorted({label for label in labels if labels.count(label) > 1})
    if duplicate_labels:
        failures.append(
            "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS repeats labels: "
            + ", ".join(duplicate_labels)
        )
    duplicate_global_markers = sorted(
        {marker for marker in markers if markers.count(marker) > 1}
    )
    if duplicate_global_markers:
        failures.append(
            "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS repeats markers across labels: "
            + ", ".join(duplicate_global_markers)
        )

    if failures:
        raise AssertionError(
            "release-evidence sample environment output-marker catalogue drift: "
            + "; ".join(failures)
        )

    return (
        (
            "sample environment output constant",
            ("SAMPLE_ENVIRONMENT_OUTPUT_MARKERS",),
        ),
        ("sample environment output label", tuple(labels)),
        ("sample environment output marker", tuple(markers)),
    )


def assert_sample_env_output_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria sample environment output catalogue docs",
        RELEASE_EVIDENCE_SAMPLE_ENV_OUTPUT_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity sample environment output catalogue docs",
        RELEASE_EVIDENCE_SAMPLE_ENV_OUTPUT_CATALOG_DOC_FRAGMENTS,
    )


def assert_sample_env_output_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_sample_env_output_catalog_docs(release_criteria, parity)

    entries = release_evidence_sample_env_output_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence sample environment output-marker catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_build_summary_catalog(release_evidence):
    wanted = {"BENCHMARK_RESULTS_ARTIFACT", "ZIG_BUILD_SUMMARY_RE"}
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_command_shape_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence build summary catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    artifact = constants["BENCHMARK_RESULTS_ARTIFACT"]
    if artifact != "benchmarks/results.json":
        failures.append(
            "BENCHMARK_RESULTS_ARTIFACT must be benchmarks/results.json"
        )
    pattern = constants["ZIG_BUILD_SUMMARY_RE"]
    if not isinstance(pattern, str) or not pattern:
        failures.append("ZIG_BUILD_SUMMARY_RE must be a non-empty regex string")
    else:
        try:
            regex = re.compile(pattern)
        except re.error as exc:
            failures.append(f"ZIG_BUILD_SUMMARY_RE is not valid regex: {exc}")
        else:
            success = regex.fullmatch(
                "Build Summary: 50/50 steps succeeded; 1997/1997 tests passed",
            )
            failed_steps = regex.fullmatch(
                "Build Summary: 49/50 steps succeeded; 1997/1997 tests passed",
            )
            failed_tests = regex.fullmatch(
                "Build Summary: 50/50 steps succeeded; 1996/1997 tests passed",
            )
            if success is None:
                failures.append("ZIG_BUILD_SUMMARY_RE must match Zig success summaries")
            elif success.group(1) != "50" or success.group(3) != "1997":
                failures.append(
                    "ZIG_BUILD_SUMMARY_RE must capture step and test counts"
                )
            if failed_steps is None or failed_tests is None:
                failures.append(
                    "ZIG_BUILD_SUMMARY_RE must match unsuccessful summaries "
                    "for failure detection"
                )

    if failures:
        raise AssertionError(
            "release-evidence build summary catalogue drift: "
            + "; ".join(failures)
        )

    return (
        (
            "build summary verifier constant",
            ("BENCHMARK_RESULTS_ARTIFACT", "ZIG_BUILD_SUMMARY_RE"),
        ),
        ("benchmark results artifact", (artifact,)),
        (
            "Zig build summary marker fragment",
            ("Build Summary:", "steps succeeded", "tests passed"),
        ),
        (
            "benchmark results artifact output marker",
            (f"Results saved to {artifact}",),
        ),
    )


def assert_build_summary_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria build summary catalogue docs",
        RELEASE_EVIDENCE_BUILD_SUMMARY_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity build summary catalogue docs",
        RELEASE_EVIDENCE_BUILD_SUMMARY_CATALOG_DOC_FRAGMENTS,
    )


def assert_build_summary_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_build_summary_catalog_docs(release_criteria, parity)

    entries = release_evidence_build_summary_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence build summary catalogue drift: "
            + "; ".join(failures)
        )


RELEASE_EVIDENCE_HOOK_PROVENANCE_REQUIRED_KEYS = {
    "required",
    "prefix",
    "label",
    "suffixes",
    "token_style",
}
RELEASE_EVIDENCE_HOOK_PROVENANCE_KEYS = (
    "required",
    "prefix",
    "label",
    "suffixes",
    "token_style",
    "fixture",
)
RELEASE_EVIDENCE_HOOK_PROVENANCE_TOKEN_STYLES = {"collapsed", "literal"}


def validate_hook_provenance_entries(
    catalog_name,
    entries,
    required_env_vars,
    boolean_env_vars,
):
    failures = []
    if not isinstance(entries, list) or not entries:
        raise AssertionError(f"{catalog_name} must be a non-empty list")

    valid_entries = []
    required_names = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            failures.append(f"{catalog_name} entry {index} must be a dict")
            continue
        unknown_keys = sorted(
            key for key in entry.keys() if key not in RELEASE_EVIDENCE_HOOK_PROVENANCE_KEYS
        )
        if unknown_keys:
            failures.append(
                f"{catalog_name} entry {index} has untracked keys: "
                + ", ".join(unknown_keys)
            )
        missing_keys = sorted(
            RELEASE_EVIDENCE_HOOK_PROVENANCE_REQUIRED_KEYS - set(entry.keys())
        )
        if missing_keys:
            failures.append(
                f"{catalog_name} entry {index} missing keys: "
                + ", ".join(missing_keys)
            )
            continue

        bad_value = False
        for key in ("required", "prefix", "label", "token_style"):
            if not isinstance(entry.get(key), str) or not entry[key]:
                failures.append(
                    f"{catalog_name} entry {index} {key} "
                    "must be a non-empty string"
                )
                bad_value = True

        suffixes = entry.get("suffixes")
        if (
            not isinstance(suffixes, (list, tuple))
            or not suffixes
            or any(not isinstance(suffix, str) or not suffix for suffix in suffixes)
        ):
            failures.append(
                f"{catalog_name} entry {index} suffixes "
                "must be a non-empty string sequence"
            )
            bad_value = True
        else:
            duplicate_suffixes = sorted(
                {suffix for suffix in suffixes if suffixes.count(suffix) > 1}
            )
            if duplicate_suffixes:
                failures.append(
                    f"{catalog_name} entry {index} repeats suffixes: "
                    + ", ".join(duplicate_suffixes)
                )

        fixture = entry.get("fixture")
        if fixture is not None:
            if not isinstance(fixture, str) or not fixture:
                failures.append(
                    f"{catalog_name} entry {index} fixture "
                    "must be a non-empty string"
                )
                bad_value = True
            elif fixture not in boolean_env_vars:
                failures.append(
                    f"{catalog_name} entry {index} fixture {fixture} "
                    "is not in BOOLEAN_ENV_VARS"
                )

        if bad_value:
            continue
        if entry["token_style"] not in RELEASE_EVIDENCE_HOOK_PROVENANCE_TOKEN_STYLES:
            failures.append(
                f"{catalog_name} entry {index} has unknown token_style "
                f"{entry['token_style']}"
            )
        if entry["required"] not in required_env_vars:
            failures.append(
                f"{catalog_name} entry {index} required variable "
                f"{entry['required']} is not in REQUIRED_ENV_VARS"
            )
        required_names.append(entry["required"])
        valid_entries.append(entry)

    duplicates = sorted(
        {name for name in required_names if required_names.count(name) > 1}
    )
    if duplicates:
        failures.append(
            f"{catalog_name} repeats required variables: " + ", ".join(duplicates)
        )
    if failures:
        raise AssertionError(
            "release-evidence hook-provenance catalogue drift: "
            + "; ".join(failures)
        )
    return valid_entries


def validate_s3_profile_enable_provenance_entries(
    entries,
    required_env_vars,
    s3_bool_suffixes,
):
    failures = []
    if not isinstance(entries, (list, tuple)) or not entries:
        raise AssertionError(
            "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS must be a non-empty sequence"
        )

    valid_entries = []
    required_names = []
    suffixes = []
    for index, entry in enumerate(entries):
        if (
            not isinstance(entry, (list, tuple))
            or len(entry) != 3
            or any(not isinstance(value, str) or not value for value in entry)
        ):
            failures.append(
                "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS entry "
                f"{index} must be a triple of non-empty strings"
            )
            continue
        required_name, suffix, label = entry
        if required_name not in required_env_vars:
            failures.append(
                "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS entry "
                f"{index} required variable {required_name} is not in REQUIRED_ENV_VARS"
            )
        if suffix not in s3_bool_suffixes:
            failures.append(
                "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS entry "
                f"{index} suffix {suffix} is not in S3_BOOL_SUFFIXES"
            )
        required_names.append(required_name)
        suffixes.append(suffix)
        valid_entries.append(
            {
                "required": required_name,
                "suffix": suffix,
                "label": label,
            }
        )

    for label, values in (
        ("required variables", required_names),
        ("suffixes", suffixes),
    ):
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS repeats "
                + label
                + ": "
                + ", ".join(duplicates)
            )
    if failures:
        raise AssertionError(
            "release-evidence hook-provenance catalogue drift: "
            + "; ".join(failures)
        )
    return valid_entries


def release_evidence_hook_provenance_catalog(release_evidence):
    wanted = {
        "PHASE_HOOK_PROVENANCE_REQUIREMENTS",
        "PROFILE_HOOK_PROVENANCE_REQUIREMENTS",
        "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS",
        "REQUIRED_ENV_VARS",
        "BOOLEAN_ENV_VARS",
        "S3_BOOL_SUFFIXES",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence hook-provenance catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    for name in ("REQUIRED_ENV_VARS", "BOOLEAN_ENV_VARS", "S3_BOOL_SUFFIXES"):
        values = constants[name]
        if (
            not isinstance(values, (list, tuple))
            or not values
            or any(not isinstance(value, str) or not value for value in values)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                f"{name} repeats entries: " + ", ".join(duplicates)
            )
    if failures:
        raise AssertionError(
            "release-evidence hook-provenance catalogue drift: "
            + "; ".join(failures)
        )

    required_env_vars = constants["REQUIRED_ENV_VARS"]
    boolean_env_vars = constants["BOOLEAN_ENV_VARS"]
    s3_bool_suffixes = constants["S3_BOOL_SUFFIXES"]
    return (
        (
            "phase hook",
            validate_hook_provenance_entries(
                "PHASE_HOOK_PROVENANCE_REQUIREMENTS",
                constants["PHASE_HOOK_PROVENANCE_REQUIREMENTS"],
                required_env_vars,
                boolean_env_vars,
            ),
        ),
        (
            "profile hook",
            validate_hook_provenance_entries(
                "PROFILE_HOOK_PROVENANCE_REQUIREMENTS",
                constants["PROFILE_HOOK_PROVENANCE_REQUIREMENTS"],
                required_env_vars,
                boolean_env_vars,
            ),
        ),
        (
            "S3 profile enable",
            validate_s3_profile_enable_provenance_entries(
                constants["S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS"],
                required_env_vars,
                s3_bool_suffixes,
            ),
        ),
    )


def assert_hook_provenance_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria hook-provenance catalogue docs",
        RELEASE_EVIDENCE_HOOK_PROVENANCE_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity hook-provenance catalogue docs",
        RELEASE_EVIDENCE_HOOK_PROVENANCE_CATALOG_DOC_FRAGMENTS,
    )


def assert_hook_provenance_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_hook_provenance_catalog_docs(release_criteria, parity)

    catalogs = release_evidence_hook_provenance_catalog(release_evidence)
    failures = []
    for label, entries in catalogs:
        for entry in entries:
            if label == "S3 profile enable":
                fragments = [entry["required"], entry["suffix"], entry["label"]]
            else:
                fragments = [
                    entry["required"],
                    entry["prefix"],
                    entry["label"],
                    entry["token_style"],
                    *entry["suffixes"],
                ]
                if "fixture" in entry:
                    fragments.append(entry["fixture"])
            for fragment in fragments:
                for target_label, text in (
                    ("release criteria", release_criteria),
                    ("AutoMQ parity", parity),
                    ("production readiness", production_readiness),
                ):
                    if fragment not in text:
                        failures.append(
                            f"{target_label} missing {label} fragment {fragment!r}"
                        )
    if failures:
        raise AssertionError(
            "release-evidence hook-provenance catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_client_capability_catalog(release_evidence):
    wanted = {
        "REQUIRED_CLIENT_TOOLS",
        "REQUIRED_CLIENT_SEMANTICS",
        "CLIENT_SECURITY_PROTOCOLS",
        "CLIENT_SASL_MECHANISMS",
        "CLIENT_SECURITY_TOOLS",
        "CLIENT_REBALANCE_TOOLS",
        "CLIENT_TRANSACTION_TOOLS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence client capability catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    for name in sorted(wanted):
        values = constants[name]
        if (
            not isinstance(values, (list, tuple))
            or not values
            or any(not isinstance(value, str) or not value for value in values)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                f"{name} repeats client capability entries: "
                + ", ".join(duplicates)
            )

    required_tools = set(constants["REQUIRED_CLIENT_TOOLS"])
    required_semantics = set(constants["REQUIRED_CLIENT_SEMANTICS"])
    for name in (
        "CLIENT_SECURITY_TOOLS",
        "CLIENT_REBALANCE_TOOLS",
        "CLIENT_TRANSACTION_TOOLS",
    ):
        unknown = sorted(value for value in constants[name] if value not in required_tools)
        if unknown:
            failures.append(
                f"{name} entries must be REQUIRED_CLIENT_TOOLS: "
                + ", ".join(unknown)
            )

    for name, semantic in (
        ("CLIENT_SECURITY_TOOLS", "security"),
        ("CLIENT_REBALANCE_TOOLS", "rebalance"),
        ("CLIENT_TRANSACTION_TOOLS", "transactions"),
    ):
        if constants[name] and semantic not in required_semantics:
            failures.append(
                f"{name} requires REQUIRED_CLIENT_SEMANTICS entry {semantic}"
            )
    if constants["CLIENT_SECURITY_TOOLS"] and "security-negative" not in required_semantics:
        failures.append(
            "CLIENT_SECURITY_TOOLS requires REQUIRED_CLIENT_SEMANTICS entry "
            "security-negative"
        )

    if failures:
        raise AssertionError(
            "release-evidence client capability catalogue drift: "
            + "; ".join(failures)
        )
    return (
        ("required client tool", tuple(constants["REQUIRED_CLIENT_TOOLS"])),
        ("required client semantic", tuple(constants["REQUIRED_CLIENT_SEMANTICS"])),
        (
            "client security protocol",
            tuple(sorted(constants["CLIENT_SECURITY_PROTOCOLS"])),
        ),
        (
            "client SASL mechanism",
            tuple(sorted(constants["CLIENT_SASL_MECHANISMS"])),
        ),
        (
            "client security-compatible tool",
            tuple(sorted(constants["CLIENT_SECURITY_TOOLS"])),
        ),
        (
            "client rebalance-compatible tool",
            tuple(sorted(constants["CLIENT_REBALANCE_TOOLS"])),
        ),
        (
            "client transaction-compatible tool",
            tuple(sorted(constants["CLIENT_TRANSACTION_TOOLS"])),
        ),
    )


def assert_client_capability_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria client capability catalogue docs",
        RELEASE_EVIDENCE_CLIENT_CAPABILITY_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity client capability catalogue docs",
        RELEASE_EVIDENCE_CLIENT_CAPABILITY_CATALOG_DOC_FRAGMENTS,
    )


def assert_client_capability_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_client_capability_catalog_docs(release_criteria, parity)

    entries = release_evidence_client_capability_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence client capability catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_client_tool_marker_catalog(release_evidence):
    wanted = {"REQUIRED_CLIENT_TOOLS", "CLIENT_TOOL_OUTPUT_MARKERS"}
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence client tool marker catalogue missing assignments: "
            + ", ".join(missing)
        )

    required_tools = constants["REQUIRED_CLIENT_TOOLS"]
    markers = constants["CLIENT_TOOL_OUTPUT_MARKERS"]
    failures = []
    if (
        not isinstance(required_tools, (list, tuple))
        or not required_tools
        or any(not isinstance(tool, str) or not tool for tool in required_tools)
    ):
        failures.append("REQUIRED_CLIENT_TOOLS must be a non-empty string sequence")
    else:
        duplicates = sorted(
            {tool for tool in required_tools if required_tools.count(tool) > 1}
        )
        if duplicates:
            failures.append(
                "REQUIRED_CLIENT_TOOLS repeats client tools: " + ", ".join(duplicates)
            )

    if not isinstance(markers, dict) or not markers:
        failures.append("CLIENT_TOOL_OUTPUT_MARKERS must be a non-empty dict")
        marker_keys = set()
    else:
        marker_keys = set()
        marker_values = []
        for tool, marker in markers.items():
            if not isinstance(tool, str) or not tool:
                failures.append("CLIENT_TOOL_OUTPUT_MARKERS keys must be non-empty strings")
                continue
            marker_keys.add(tool)
            if not isinstance(marker, str) or not marker:
                failures.append(
                    f"CLIENT_TOOL_OUTPUT_MARKERS marker for {tool} "
                    "must be a non-empty string"
                )
                continue
            marker_values.append(marker)
        duplicate_markers = sorted(
            {marker for marker in marker_values if marker_values.count(marker) > 1}
        )
        if duplicate_markers:
            failures.append(
                "CLIENT_TOOL_OUTPUT_MARKERS repeats output markers: "
                + ", ".join(duplicate_markers)
            )

    if isinstance(required_tools, (list, tuple)):
        required_tool_set = set(required_tools)
        missing_markers = sorted(required_tool_set - marker_keys)
        extra_markers = sorted(marker_keys - required_tool_set)
        if missing_markers:
            failures.append(
                "CLIENT_TOOL_OUTPUT_MARKERS missing required tools: "
                + ", ".join(missing_markers)
            )
        if extra_markers:
            failures.append(
                "CLIENT_TOOL_OUTPUT_MARKERS contains non-required tools: "
                + ", ".join(extra_markers)
            )

    if failures:
        raise AssertionError(
            "release-evidence client tool marker catalogue drift: "
            + "; ".join(failures)
        )
    return tuple((tool, markers[tool]) for tool in required_tools)


def assert_client_tool_marker_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria client tool marker catalogue docs",
        RELEASE_EVIDENCE_CLIENT_TOOL_MARKER_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity client tool marker catalogue docs",
        RELEASE_EVIDENCE_CLIENT_TOOL_MARKER_CATALOG_DOC_FRAGMENTS,
    )


def assert_client_tool_marker_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_client_tool_marker_catalog_docs(release_criteria, parity)

    entries = release_evidence_client_tool_marker_catalog(release_evidence)
    failures = []
    for tool, marker in entries:
        for label, fragment in (("client tool", tool), ("client tool marker", marker)):
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if fragment not in text:
                    failures.append(f"{target_label} missing {label} {fragment}")
    if failures:
        raise AssertionError(
            "release-evidence client tool marker catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_client_version_catalog(release_evidence):
    wanted = {
        "REQUIRED_CLIENT_TOOLS",
        "CLIENT_PYTHON_TOOLS",
        "CLIENT_UNPINNED_VERSION_LABELS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence client version/provenance catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    for name in sorted(wanted):
        values = constants[name]
        if (
            not isinstance(values, (list, tuple))
            or not values
            or any(not isinstance(value, str) or not value for value in values)
        ):
            failures.append(f"{name} must be a non-empty string sequence")
            continue
        duplicates = sorted({value for value in values if values.count(value) > 1})
        if duplicates:
            failures.append(
                f"{name} repeats client version entries: " + ", ".join(duplicates)
            )

    required_tools = set(constants["REQUIRED_CLIENT_TOOLS"])
    unknown_python_tools = sorted(
        tool for tool in constants["CLIENT_PYTHON_TOOLS"] if tool not in required_tools
    )
    if unknown_python_tools:
        failures.append(
            "CLIENT_PYTHON_TOOLS entries must be REQUIRED_CLIENT_TOOLS: "
            + ", ".join(unknown_python_tools)
        )

    non_lowercase_labels = sorted(
        label
        for label in constants["CLIENT_UNPINNED_VERSION_LABELS"]
        if label != label.lower()
    )
    if non_lowercase_labels:
        failures.append(
            "CLIENT_UNPINNED_VERSION_LABELS entries must be lowercase: "
            + ", ".join(non_lowercase_labels)
        )

    if failures:
        raise AssertionError(
            "release-evidence client version/provenance catalogue drift: "
            + "; ".join(failures)
        )
    return (
        ("client Python tool", tuple(sorted(constants["CLIENT_PYTHON_TOOLS"]))),
        (
            "client unpinned version label",
            tuple(sorted(constants["CLIENT_UNPINNED_VERSION_LABELS"])),
        ),
    )


def assert_client_version_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria client version/provenance catalogue docs",
        RELEASE_EVIDENCE_CLIENT_VERSION_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity client version/provenance catalogue docs",
        RELEASE_EVIDENCE_CLIENT_VERSION_CATALOG_DOC_FRAGMENTS,
    )


def assert_client_version_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_client_version_catalog_docs(release_criteria, parity)

    entries = release_evidence_client_version_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence client version/provenance catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_chaos_scenario_catalog(release_evidence):
    wanted = {
        "CHAOS_SCENARIO_ALIASES",
        "REQUIRED_CHAOS_SCENARIOS",
        "CHAOS_SCENARIO_MARKERS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence chaos scenario catalogue missing assignments: "
            + ", ".join(missing)
        )

    aliases = constants["CHAOS_SCENARIO_ALIASES"]
    required = constants["REQUIRED_CHAOS_SCENARIOS"]
    markers = constants["CHAOS_SCENARIO_MARKERS"]
    failures = []
    if (
        not isinstance(required, (list, tuple))
        or not required
        or any(not isinstance(scenario, str) or not scenario for scenario in required)
    ):
        failures.append("REQUIRED_CHAOS_SCENARIOS must be a non-empty string sequence")
    else:
        duplicates = sorted(
            {scenario for scenario in required if required.count(scenario) > 1}
        )
        if duplicates:
            failures.append(
                "REQUIRED_CHAOS_SCENARIOS repeats scenarios: "
                + ", ".join(duplicates)
            )

    for name, catalog in (
        ("CHAOS_SCENARIO_ALIASES", aliases),
        ("CHAOS_SCENARIO_MARKERS", markers),
    ):
        if not isinstance(catalog, dict) or not catalog:
            failures.append(f"{name} must be a non-empty dict")
            continue
        values = []
        for key, value in catalog.items():
            if not isinstance(key, str) or not key:
                failures.append(f"{name} keys must be non-empty strings")
                continue
            if not isinstance(value, str) or not value:
                failures.append(f"{name} value for {key} must be a non-empty string")
                continue
            values.append(value)
        if name == "CHAOS_SCENARIO_MARKERS":
            duplicates = sorted({value for value in values if values.count(value) > 1})
            if duplicates:
                failures.append(
                    "CHAOS_SCENARIO_MARKERS repeats output markers: "
                    + ", ".join(duplicates)
                )

    marker_keys = set(markers) if isinstance(markers, dict) else set()
    if isinstance(required, (list, tuple)):
        missing_markers = sorted(scenario for scenario in required if scenario not in marker_keys)
        if missing_markers:
            failures.append(
                "CHAOS_SCENARIO_MARKERS missing required scenarios: "
                + ", ".join(missing_markers)
            )
    if isinstance(aliases, dict):
        bad_alias_targets = sorted(
            target for target in aliases.values() if target not in marker_keys
        )
        if bad_alias_targets:
            failures.append(
                "CHAOS_SCENARIO_ALIASES targets must be CHAOS_SCENARIO_MARKERS keys: "
                + ", ".join(bad_alias_targets)
            )

    if failures:
        raise AssertionError(
            "release-evidence chaos scenario catalogue drift: "
            + "; ".join(failures)
        )
    return (
        ("chaos scenario alias", tuple(sorted(aliases.keys()))),
        ("chaos scenario", tuple(required)),
        (
            "chaos scenario marker",
            tuple(markers[scenario] for scenario in sorted(markers)),
        ),
    )


def assert_chaos_scenario_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria chaos scenario catalogue docs",
        RELEASE_EVIDENCE_CHAOS_SCENARIO_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity chaos scenario catalogue docs",
        RELEASE_EVIDENCE_CHAOS_SCENARIO_CATALOG_DOC_FRAGMENTS,
    )


def assert_chaos_scenario_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_chaos_scenario_catalog_docs(release_criteria, parity)

    entries = release_evidence_chaos_scenario_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence chaos scenario catalogue drift: "
            + "; ".join(failures)
        )


def static_eval_detail_output_marker_node(name, node, constants):
    if (
        name == "KRAFT_DETAIL_OUTPUT_MARKERS"
        and isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "set"
        and len(node.args) == 1
        and not node.keywords
    ):
        return set(static_eval_release_constant_node(node.args[0], constants))
    return static_eval_release_constant_node(node, constants)


def release_evidence_detail_output_marker_catalog(release_evidence):
    wanted = {
        "COMPARATIVE_TABLE_ROW_MARKERS",
        "BENCHMARK_OUTPUT_LINE_MARKERS",
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS",
        "KRAFT_DETAIL_OUTPUT_MARKERS",
        "E2E_OUTPUT_LINE_MARKERS",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_detail_output_marker_node(
                target.id,
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence detail output marker catalogue missing assignments: "
            + ", ".join(missing)
        )

    failures = []
    values_by_name = {}
    for name in sorted(wanted):
        value = constants[name]
        if not isinstance(value, (list, tuple, set)) or not value:
            failures.append(f"{name} must be a non-empty string sequence")
            values_by_name[name] = ()
            continue
        values = tuple(value) if not isinstance(value, set) else tuple(sorted(value))
        bad_values = [marker for marker in values if not isinstance(marker, str) or not marker]
        if bad_values:
            failures.append(f"{name} entries must be non-empty strings")
        string_values = [marker for marker in values if isinstance(marker, str)]
        duplicates = sorted(
            {marker for marker in string_values if string_values.count(marker) > 1}
        )
        if duplicates:
            failures.append(f"{name} repeats markers: " + ", ".join(duplicates))
        values_by_name[name] = values

    if set(values_by_name["KRAFT_DETAIL_OUTPUT_MARKERS"]) != set(
        values_by_name["KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS"]
    ):
        failures.append(
            "KRAFT_DETAIL_OUTPUT_MARKERS must mirror "
            "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS"
        )

    if failures:
        raise AssertionError(
            "release-evidence detail output marker catalogue drift: "
            + "; ".join(failures)
        )

    return (
        (
            "comparative table row marker",
            values_by_name["COMPARATIVE_TABLE_ROW_MARKERS"],
        ),
        (
            "benchmark output line marker",
            values_by_name["BENCHMARK_OUTPUT_LINE_MARKERS"],
        ),
        (
            "KRaft detail output marker",
            values_by_name["KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS"],
        ),
        (
            "Docker E2E output line marker",
            values_by_name["E2E_OUTPUT_LINE_MARKERS"],
        ),
    )


def assert_detail_output_marker_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria detail output marker catalogue docs",
        RELEASE_EVIDENCE_DETAIL_OUTPUT_MARKER_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity detail output marker catalogue docs",
        RELEASE_EVIDENCE_DETAIL_OUTPUT_MARKER_CATALOG_DOC_FRAGMENTS,
    )


def assert_detail_output_marker_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_detail_output_marker_catalog_docs(release_criteria, parity)

    entries = release_evidence_detail_output_marker_catalog(release_evidence)
    failures = []
    for label, markers in entries:
        for marker in markers:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if marker not in text:
                    failures.append(f"{target_label} missing {label} {marker}")
    if failures:
        raise AssertionError(
            "release-evidence detail output marker catalogue drift: "
            + "; ".join(failures)
        )


def static_eval_re_compile_pattern_node(node):
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "re"
        and node.func.attr == "compile"
        and len(node.args) == 1
        and not node.keywords
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.args[0].value
    raise AssertionError("COMPARATIVE_MEASUREMENT_RE values must be re.compile strings")


def static_eval_comparative_measurement_re_node(node, constants):
    if not isinstance(node, ast.Dict):
        raise AssertionError("COMPARATIVE_MEASUREMENT_RE must be a dict")
    patterns = {}
    for key_node, value_node in zip(node.keys, node.values):
        key = static_eval_release_constant_node(key_node, constants)
        patterns[key] = static_eval_re_compile_pattern_node(value_node)
    return patterns


def static_module_constant(module_text, name):
    parsed = ast.parse(module_text)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == name:
                return static_eval_release_constant_node(node.value, constants)
    raise AssertionError(f"missing module constant {name}")


def release_evidence_comparative_benchmark_catalog(release_evidence):
    wanted = {
        "COMPARATIVE_TARGET_LABELS",
        "COMPARATIVE_TABLE_TARGET_HEADERS",
        "COMPARATIVE_TABLE_METRICS",
        "COMPARATIVE_MEASUREMENT_RE",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    failures = []
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            try:
                if target.id == "COMPARATIVE_MEASUREMENT_RE":
                    constants[target.id] = static_eval_comparative_measurement_re_node(
                        node.value,
                        constants,
                    )
                else:
                    constants[target.id] = static_eval_release_constant_node(
                        node.value,
                        constants,
                    )
            except AssertionError as exc:
                failures.append(str(exc))

    missing = sorted(wanted - set(constants))
    if missing:
        failures.append(
            "missing comparative benchmark catalogue assignments: "
            + ", ".join(missing)
        )

    target_labels = constants.get("COMPARATIVE_TARGET_LABELS", {})
    if not isinstance(target_labels, dict) or not target_labels:
        failures.append("COMPARATIVE_TARGET_LABELS must be a non-empty dict")
        target_labels = {}
    else:
        for key, label in target_labels.items():
            if not isinstance(key, str) or not key:
                failures.append("COMPARATIVE_TARGET_LABELS keys must be non-empty strings")
            elif key != key.lower():
                failures.append(
                    "COMPARATIVE_TARGET_LABELS keys must be lowercase: " + key
                )
            if not isinstance(label, str) or not label:
                failures.append(
                    f"COMPARATIVE_TARGET_LABELS value for {key} must be non-empty"
                )
        labels = list(target_labels.values())
        duplicates = sorted({label for label in labels if labels.count(label) > 1})
        if duplicates:
            failures.append(
                "COMPARATIVE_TARGET_LABELS repeats labels: " + ", ".join(duplicates)
            )

    target_headers = constants.get("COMPARATIVE_TABLE_TARGET_HEADERS", {})
    if not isinstance(target_headers, dict) or not target_headers:
        failures.append("COMPARATIVE_TABLE_TARGET_HEADERS must be a non-empty dict")
        target_headers = {}
    elif target_labels:
        missing_headers = sorted(set(target_labels) - set(target_headers))
        extra_headers = sorted(set(target_headers) - set(target_labels))
        if missing_headers or extra_headers:
            details = []
            if missing_headers:
                details.append("missing " + ", ".join(missing_headers))
            if extra_headers:
                details.append("extra " + ", ".join(extra_headers))
            failures.append(
                "COMPARATIVE_TABLE_TARGET_HEADERS keys must match "
                "COMPARATIVE_TARGET_LABELS: "
                + "; ".join(details)
            )
        for key, header in target_headers.items():
            if not isinstance(key, str) or not key:
                failures.append(
                    "COMPARATIVE_TABLE_TARGET_HEADERS keys must be non-empty strings"
                )
            elif key != key.lower():
                failures.append(
                    "COMPARATIVE_TABLE_TARGET_HEADERS keys must be lowercase: " + key
                )
            if not isinstance(header, str) or not header:
                failures.append(
                    f"COMPARATIVE_TABLE_TARGET_HEADERS value for {key} must be non-empty"
                )
            elif any(char.isspace() for char in header) or "/" in header:
                failures.append(
                    "COMPARATIVE_TABLE_TARGET_HEADERS values must be single table "
                    f"column tokens: {header}"
                )
        headers = [
            header
            for header in target_headers.values()
            if isinstance(header, str)
        ]
        duplicates = sorted({header for header in headers if headers.count(header) > 1})
        if duplicates:
            failures.append(
                "COMPARATIVE_TABLE_TARGET_HEADERS repeats headers: "
                + ", ".join(duplicates)
            )

    metrics = constants.get("COMPARATIVE_TABLE_METRICS", ())
    if (
        not isinstance(metrics, (list, tuple))
        or not metrics
        or any(not isinstance(metric, str) or not metric for metric in metrics)
    ):
        failures.append("COMPARATIVE_TABLE_METRICS must be a non-empty string sequence")
        metrics = ()
    else:
        duplicates = sorted({metric for metric in metrics if metrics.count(metric) > 1})
        if duplicates:
            failures.append(
                "COMPARATIVE_TABLE_METRICS repeats metrics: " + ", ".join(duplicates)
            )

    measurement_re = constants.get("COMPARATIVE_MEASUREMENT_RE", {})
    if not isinstance(measurement_re, dict) or not measurement_re:
        failures.append("COMPARATIVE_MEASUREMENT_RE must be a non-empty dict")
        measurement_re = {}
    elif metrics:
        missing_re = sorted(set(metrics) - set(measurement_re))
        extra_re = sorted(set(measurement_re) - set(metrics))
        if missing_re or extra_re:
            details = []
            if missing_re:
                details.append("missing " + ", ".join(missing_re))
            if extra_re:
                details.append("extra " + ", ".join(extra_re))
            failures.append(
                "COMPARATIVE_MEASUREMENT_RE keys must match "
                "COMPARATIVE_TABLE_METRICS: "
                + "; ".join(details)
            )
        for metric, pattern in measurement_re.items():
            if not isinstance(metric, str) or not metric:
                failures.append("COMPARATIVE_MEASUREMENT_RE keys must be non-empty strings")
            if not isinstance(pattern, str) or not pattern:
                failures.append(
                    f"COMPARATIVE_MEASUREMENT_RE pattern for {metric} must be non-empty"
                )
                continue
            try:
                re.compile(pattern)
            except re.error as exc:
                failures.append(
                    f"COMPARATIVE_MEASUREMENT_RE pattern for {metric} is invalid: {exc}"
                )

    if failures:
        raise AssertionError(
            "release-evidence comparative benchmark catalogue drift: "
            + "; ".join(failures)
        )

    sorted_targets = tuple(sorted(target_labels))
    return (
        ("comparative target key", sorted_targets),
        (
            "comparative target label",
            tuple(target_labels[target] for target in sorted_targets),
        ),
        (
            "comparative table target header",
            tuple(target_headers[target] for target in sorted_targets),
        ),
        ("comparative table metric", tuple(metrics)),
    )


def assert_comparative_benchmark_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria comparative benchmark catalogue docs",
        RELEASE_EVIDENCE_COMPARATIVE_BENCHMARK_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity comparative benchmark catalogue docs",
        RELEASE_EVIDENCE_COMPARATIVE_BENCHMARK_CATALOG_DOC_FRAGMENTS,
    )


def assert_comparative_benchmark_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_comparative_benchmark_catalog_docs(release_criteria, parity)

    entries = release_evidence_comparative_benchmark_catalog(release_evidence)
    failures = []
    for label, values in entries:
        for value in values:
            for target_label, text in (
                ("release criteria", release_criteria),
                ("AutoMQ parity", parity),
                ("production readiness", production_readiness),
            ):
                if value not in text:
                    failures.append(f"{target_label} missing {label} {value}")
    if failures:
        raise AssertionError(
            "release-evidence comparative benchmark catalogue drift: "
            + "; ".join(failures)
        )


def assert_benchmark_compare_table_headers_match_release_evidence(
    benchmark_compare=None,
    release_evidence=None,
):
    if benchmark_compare is None:
        benchmark_compare = read(BENCHMARK_COMPARE)
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))

    producer_headers = static_module_constant(benchmark_compare, "TARGET_SHORT_LABELS")
    verifier_headers = static_module_constant(
        release_evidence,
        "COMPARATIVE_TABLE_TARGET_HEADERS",
    )
    if producer_headers != verifier_headers:
        raise AssertionError(
            "benchmark_compare.py TARGET_SHORT_LABELS must match "
            "release-evidence COMPARATIVE_TABLE_TARGET_HEADERS"
        )


def assert_benchmark_compare_target_labels_match_release_evidence(
    benchmark_compare=None,
    release_evidence=None,
):
    if benchmark_compare is None:
        benchmark_compare = read(BENCHMARK_COMPARE)
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))

    producer_targets = static_module_constant(benchmark_compare, "ALL_TARGETS")
    producer_labels = static_module_constant(benchmark_compare, "TARGET_LABELS")
    verifier_labels = static_module_constant(
        release_evidence,
        "COMPARATIVE_TARGET_LABELS",
    )
    if not isinstance(producer_targets, (list, tuple)):
        raise AssertionError("benchmark_compare.py ALL_TARGETS must be a sequence")
    if list(producer_targets) != list(verifier_labels):
        raise AssertionError(
            "benchmark_compare.py ALL_TARGETS must match release-evidence "
            "COMPARATIVE_TARGET_LABELS order"
        )
    if producer_labels != verifier_labels:
        raise AssertionError(
            "benchmark_compare.py TARGET_LABELS must match release-evidence "
            "COMPARATIVE_TARGET_LABELS"
        )


def release_evidence_comparative_threshold_default_catalog(release_evidence):
    wanted = {
        "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS",
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV",
    }
    parsed = ast.parse(release_evidence)
    constants = {}
    for node in parsed.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name) or target.id not in wanted:
                continue
            constants[target.id] = static_eval_release_constant_node(
                node.value,
                constants,
            )

    missing = sorted(wanted - set(constants))
    if missing:
        raise AssertionError(
            "release evidence comparative threshold default catalogue "
            "missing assignments: "
            + ", ".join(missing)
        )

    defaults = constants["DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS"]
    threshold_env = constants["COMPARATIVE_BENCHMARK_THRESHOLD_ENV"]
    failures = []
    if not isinstance(defaults, dict) or not defaults:
        failures.append(
            "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS must be a non-empty dict"
        )
        defaults = {}
    else:
        for key, value in defaults.items():
            if not isinstance(key, str) or not key:
                failures.append(
                    "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS keys must be "
                    "non-empty strings"
                )
            if (
                not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0
            ):
                failures.append(
                    "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS values must be "
                    f"finite non-negative numbers: {key}"
                )

    threshold_keys = []
    if not isinstance(threshold_env, (list, tuple)) or not threshold_env:
        failures.append("COMPARATIVE_BENCHMARK_THRESHOLD_ENV must be a non-empty sequence")
    else:
        for index, entry in enumerate(threshold_env):
            if (
                not isinstance(entry, (list, tuple))
                or len(entry) != 2
                or not isinstance(entry[1], str)
                or not entry[1]
            ):
                failures.append(
                    f"COMPARATIVE_BENCHMARK_THRESHOLD_ENV entry {index} "
                    "must include a non-empty threshold key"
                )
                continue
            threshold_keys.append(entry[1])
        duplicates = sorted(
            {key for key in threshold_keys if threshold_keys.count(key) > 1}
        )
        if duplicates:
            failures.append(
                "COMPARATIVE_BENCHMARK_THRESHOLD_ENV repeats threshold keys: "
                + ", ".join(duplicates)
            )
        if set(threshold_keys) != set(defaults):
            missing_defaults = sorted(set(threshold_keys) - set(defaults))
            stale_defaults = sorted(set(defaults) - set(threshold_keys))
            details = []
            if missing_defaults:
                details.append("missing defaults " + ", ".join(missing_defaults))
            if stale_defaults:
                details.append("stale defaults " + ", ".join(stale_defaults))
            failures.append(
                "COMPARATIVE_BENCHMARK_THRESHOLD_ENV keys must match "
                "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS: "
                + "; ".join(details)
            )

    if failures:
        raise AssertionError(
            "release-evidence comparative threshold default catalogue drift: "
            + "; ".join(failures)
        )

    return tuple(
        f"{key}={defaults[key]!r}"
        for key in sorted(defaults)
    )


def assert_comparative_threshold_default_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria comparative threshold default catalogue docs",
        RELEASE_EVIDENCE_COMPARATIVE_THRESHOLD_DEFAULT_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity comparative threshold default catalogue docs",
        RELEASE_EVIDENCE_COMPARATIVE_THRESHOLD_DEFAULT_CATALOG_DOC_FRAGMENTS,
    )


def assert_comparative_threshold_default_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_comparative_threshold_default_catalog_docs(release_criteria, parity)

    entries = release_evidence_comparative_threshold_default_catalog(release_evidence)
    failures = []
    for entry in entries:
        for target_label, text in (
            ("release criteria", release_criteria),
            ("AutoMQ parity", parity),
            ("production readiness", production_readiness),
        ):
            if entry not in text:
                failures.append(
                    f"{target_label} missing comparative threshold default {entry}"
                )
    if failures:
        raise AssertionError(
            "release-evidence comparative threshold default catalogue drift: "
            + "; ".join(failures)
        )


def release_evidence_e2e_fixture_action_catalog(release_evidence):
    actions = literal_module_assignment(
        release_evidence,
        "E2E_LOAD_SCALE_FIXTURE_ACTIONS",
    )
    failures = []
    if (
        not isinstance(actions, (list, tuple, set))
        or not actions
        or any(not isinstance(action, str) or not action for action in actions)
    ):
        failures.append("E2E_LOAD_SCALE_FIXTURE_ACTIONS must be a non-empty string set")
        actions = ()
    values = tuple(actions) if not isinstance(actions, set) else tuple(sorted(actions))
    duplicates = sorted({action for action in values if values.count(action) > 1})
    if duplicates:
        failures.append(
            "E2E_LOAD_SCALE_FIXTURE_ACTIONS repeats actions: "
            + ", ".join(duplicates)
        )
    non_lower = sorted(
        action for action in values if isinstance(action, str) and action != action.lower()
    )
    if non_lower:
        failures.append(
            "E2E_LOAD_SCALE_FIXTURE_ACTIONS entries must be lowercase: "
            + ", ".join(non_lower)
        )
    if failures:
        raise AssertionError(
            "release-evidence E2E fixture action catalogue drift: "
            + "; ".join(failures)
        )
    return tuple(sorted(values))


def assert_e2e_fixture_action_catalog_docs(
    release_criteria=None,
    parity=None,
):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria E2E fixture action catalogue docs",
        RELEASE_EVIDENCE_E2E_FIXTURE_ACTION_CATALOG_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity E2E fixture action catalogue docs",
        RELEASE_EVIDENCE_E2E_FIXTURE_ACTION_CATALOG_DOC_FRAGMENTS,
    )


def assert_e2e_fixture_action_catalog_pinned(
    release_evidence=None,
    release_criteria=None,
    parity=None,
    production_readiness=None,
    check_docs=True,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)
    if production_readiness is None:
        production_readiness = read(PRODUCTION_READINESS_TEST)

    if check_docs:
        assert_e2e_fixture_action_catalog_docs(release_criteria, parity)

    actions = release_evidence_e2e_fixture_action_catalog(release_evidence)
    failures = []
    for action in actions:
        for target_label, text in (
            ("release criteria", release_criteria),
            ("AutoMQ parity", parity),
            ("production readiness", production_readiness),
        ):
            if action not in text:
                failures.append(f"{target_label} missing E2E fixture action {action}")
    if failures:
        raise AssertionError(
            "release-evidence E2E fixture action catalogue drift: "
            + "; ".join(failures)
        )


def assert_protocol_static_audit_contract(protocol_static_audit=None):
    if protocol_static_audit is None:
        protocol_static_audit = read(PROTOCOL_STATIC_AUDIT)

    assert_fragments(
        protocol_static_audit,
        "tests/protocol_static_audit.py strict-codegen contract",
        PROTOCOL_STATIC_AUDIT_CONTRACT,
    )


def assert_observability_static_audit_contract(observability_static_audit=None):
    if observability_static_audit is None:
        observability_static_audit = read(OBSERVABILITY_STATIC_AUDIT)

    assert_fragments(
        observability_static_audit,
        "tests/observability_static_audit.py self-test contract",
        OBSERVABILITY_STATIC_AUDIT_CONTRACT,
    )


def literal_raise_messages(source, exception_names, function_name=None):
    messages = []
    parsed = ast.parse(source)
    roots = [parsed]
    if function_name is not None:
        roots = [
            node
            for node in parsed.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        ]
        if not roots:
            raise AssertionError(f"missing Python function {function_name}")

    for root in roots:
        for node in ast.walk(root):
            if not isinstance(node, ast.Raise):
                continue
            exc = node.exc
            if not isinstance(exc, ast.Call):
                continue
            func_name = getattr(exc.func, "id", "") or getattr(exc.func, "attr", "")
            if func_name not in exception_names:
                continue
            if not exc.args:
                continue
            first_arg = exc.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                messages.append((node.lineno, first_arg.value))
    return messages


def literal_assertion_error_messages(source, function_name=None):
    return literal_raise_messages(source, ("AssertionError",), function_name)


def meaningful_formatted_message_fragment(fragment):
    stripped = fragment.strip()
    return len(stripped) >= 8 and any(char.isalpha() for char in stripped)


def formatted_raise_message_fragments(source, exception_names, function_name=None):
    fragments = []
    parsed = ast.parse(source)
    roots = [parsed]
    if function_name is not None:
        roots = [
            node
            for node in parsed.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        ]
        if not roots:
            raise AssertionError(f"missing Python function {function_name}")

    for root in roots:
        for node in ast.walk(root):
            if not isinstance(node, ast.Raise):
                continue
            exc = node.exc
            if not isinstance(exc, ast.Call):
                continue
            func_name = getattr(exc.func, "id", "") or getattr(exc.func, "attr", "")
            if func_name not in exception_names:
                continue
            if not exc.args or not isinstance(exc.args[0], ast.JoinedStr):
                continue
            for value in exc.args[0].values:
                if (
                    isinstance(value, ast.Constant)
                    and isinstance(value.value, str)
                    and meaningful_formatted_message_fragment(value.value)
                ):
                    fragments.append((node.lineno, value.value))
    return fragments


def string_message_fragments_from_expr(expr):
    fragments = []
    for node in ast.walk(expr):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and meaningful_formatted_message_fragment(node.value)
        ):
            fragments.append(node.value)
    return fragments


def tuple_target_names(target):
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Tuple):
        return [
            element.id if isinstance(element, ast.Name) else None
            for element in target.elts
        ]
    return []


def for_loop_message_values(for_node, target_name):
    target_names = tuple_target_names(for_node.target)
    if target_name not in target_names:
        return []
    target_index = target_names.index(target_name)
    values = []
    for tuple_node in ast.walk(for_node.iter):
        if not isinstance(tuple_node, ast.Tuple):
            continue
        if len(tuple_node.elts) <= target_index:
            continue
        element = tuple_node.elts[target_index]
        if (
            isinstance(element, ast.Constant)
            and isinstance(element.value, str)
            and meaningful_formatted_message_fragment(element.value)
        ):
            values.append(element.value)
    return values


def dynamic_raise_message_fragments(source, exception_names, function_name=None):
    fragments = []
    parsed = ast.parse(source)
    roots = [parsed]
    if function_name is not None:
        roots = [
            node
            for node in parsed.body
            if isinstance(node, ast.FunctionDef) and node.name == function_name
        ]
        if not roots:
            raise AssertionError(f"missing Python function {function_name}")

    for root in roots:
        parents = {}
        for parent in ast.walk(root):
            for child in ast.iter_child_nodes(parent):
                parents[child] = parent

        for node in ast.walk(root):
            if not isinstance(node, ast.Raise):
                continue
            exc = node.exc
            if not isinstance(exc, ast.Call):
                continue
            func_name = getattr(exc.func, "id", "") or getattr(exc.func, "attr", "")
            if func_name not in exception_names:
                continue
            if not exc.args:
                continue
            first_arg = exc.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                continue
            if isinstance(first_arg, ast.JoinedStr):
                continue
            if isinstance(first_arg, ast.Name):
                current = node
                while current in parents:
                    current = parents[current]
                    if isinstance(current, ast.For):
                        for value in for_loop_message_values(current, first_arg.id):
                            fragments.append((node.lineno, value))
                        break
                    if isinstance(current, ast.FunctionDef):
                        break
                continue
            for fragment in string_message_fragments_from_expr(first_arg):
                fragments.append((node.lineno, fragment))
    return fragments


def formatted_error_spec_parts(spec):
    if len(spec) == 3:
        relative_path, exception_names, fragments = spec
        return relative_path, "self_test", exception_names, fragments
    if len(spec) == 4:
        return spec
    raise AssertionError(f"malformed formatted self-test error spec: {spec!r}")


def function_roots(parsed, function_name):
    roots = [
        node
        for node in parsed.body
        if isinstance(node, ast.FunctionDef) and node.name == function_name
    ]
    if not roots:
        raise AssertionError(f"missing Python function {function_name}")
    return roots


def raise_call_name(exc):
    if not isinstance(exc, ast.Call):
        return ""
    return getattr(exc.func, "id", "") or getattr(exc.func, "attr", "")


def selftest_raise_message_shapes(source, function_name):
    shapes = []
    parsed = ast.parse(source)
    for root in function_roots(parsed, function_name):
        for node in ast.walk(root):
            if not isinstance(node, ast.Raise):
                continue
            exc = node.exc
            if exc is None:
                continue
            if not isinstance(exc, ast.Call):
                shapes.append((node.lineno, "<non-call>", type(exc).__name__))
                continue
            func_name = raise_call_name(exc)
            first_arg = exc.args[0] if exc.args else None
            shape = type(first_arg).__name__ if first_arg is not None else "noarg"
            shapes.append((node.lineno, func_name, shape))
    return shapes


def assert_release_evidence_selftest_assertions_pinned(
    release_evidence=None,
    build_static_audit=None,
):
    if release_evidence is None:
        release_evidence = read(os.path.join(PROJECT_DIR, "tests/release_evidence_test.py"))
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_assertion_error_messages(release_evidence)
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing release evidence "
            "self-test assertion pins: " + "; ".join(missing)
        )


def assert_observability_selftest_assertions_pinned(
    observability_static_audit=None,
    build_static_audit=None,
):
    if observability_static_audit is None:
        observability_static_audit = read(OBSERVABILITY_STATIC_AUDIT)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_assertion_error_messages(
            observability_static_audit,
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing observability "
            "self-test assertion pins: " + "; ".join(missing)
        )


def assert_s3_process_crash_selftest_assertions_pinned(
    s3_process_crash=None,
    build_static_audit=None,
):
    if s3_process_crash is None:
        s3_process_crash = read(S3_PROCESS_CRASH_TEST)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py S3 process-crash self-test assertion pins",
        S3_PROCESS_CRASH_SELFTEST_ASSERTIONS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_assertion_error_messages(
            s3_process_crash,
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing S3 process-crash "
            "self-test assertion pins: " + "; ".join(missing)
        )


def assert_benchmark_compare_selftest_assertions_pinned(
    benchmark_compare=None,
    build_static_audit=None,
):
    if benchmark_compare is None:
        benchmark_compare = read(BENCHMARK_COMPARE)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py comparative benchmark self-test assertion pins",
        BENCHMARK_COMPARE_SELFTEST_ASSERTIONS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_assertion_error_messages(
            benchmark_compare,
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing comparative benchmark "
            "self-test assertion pins: " + "; ".join(missing)
        )


def assert_e2e_selftest_assertions_pinned(
    e2e_test=None,
    build_static_audit=None,
):
    if e2e_test is None:
        e2e_test = read(E2E_TEST)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py E2E self-test assertion pins",
        E2E_SELFTEST_ASSERTIONS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_assertion_error_messages(
            e2e_test,
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing E2E self-test "
            "assertion pins: " + "; ".join(missing)
        )


def assert_chaos_selftest_errors_pinned(
    chaos_test=None,
    build_static_audit=None,
):
    if chaos_test is None:
        chaos_test = read(CHAOS_TEST)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py chaos self-test error pins",
        CHAOS_SELFTEST_ERRORS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_raise_messages(
            chaos_test,
            ("TestError", "AssertionError"),
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing chaos self-test "
            "error pins: " + "; ".join(missing)
        )


def assert_kraft_failover_selftest_errors_pinned(
    kraft_failover_test=None,
    build_static_audit=None,
):
    if kraft_failover_test is None:
        kraft_failover_test = read(KRAFT_FAILOVER_TEST)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py KRaft failover self-test error pins",
        KRAFT_FAILOVER_SELFTEST_ERRORS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_raise_messages(
            kraft_failover_test,
            ("TestError", "AssertionError"),
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing KRaft failover "
            "self-test error pins: " + "; ".join(missing)
        )


def assert_client_matrix_selftest_errors_pinned(
    client_matrix_test=None,
    build_static_audit=None,
):
    if client_matrix_test is None:
        client_matrix_test = read(CLIENT_MATRIX_TEST)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py client matrix self-test error pins",
        CLIENT_MATRIX_SELFTEST_ERRORS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_raise_messages(
            client_matrix_test,
            ("MatrixError", "AssertionError"),
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing client matrix "
            "self-test error pins: " + "; ".join(missing)
        )


def assert_s3_provider_matrix_selftest_errors_pinned(
    s3_provider_matrix_test=None,
    build_static_audit=None,
):
    if s3_provider_matrix_test is None:
        s3_provider_matrix_test = read(S3_PROVIDER_MATRIX_TEST)
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py S3 provider matrix self-test error pins",
        S3_PROVIDER_MATRIX_SELFTEST_ERRORS,
    )
    missing = [
        f"{lineno}: {message}"
        for lineno, message in literal_raise_messages(
            s3_provider_matrix_test,
            ("MatrixError", "AssertionError"),
            "self_test",
        )
        if message not in build_static_audit
    ]
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing S3 provider matrix "
            "self-test error pins: " + "; ".join(missing)
        )


def assert_python_selftest_formatted_errors_pinned(
    source_texts=None,
    build_static_audit=None,
    specs=None,
):
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))
    if specs is None:
        specs = PYTHON_SELFTEST_FORMATTED_ERROR_FRAGMENTS
    if source_texts is None:
        source_texts = {}

    expected_fragments = tuple(
        fragment
        for spec in specs
        for fragment in formatted_error_spec_parts(spec)[3]
    )
    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py formatted self-test error pins",
        expected_fragments,
    )

    missing = []
    for spec in specs:
        relative_path, function_name, exception_names, _ = formatted_error_spec_parts(
            spec
        )
        source = source_texts.get(relative_path)
        if source is None:
            source = read(os.path.join(PROJECT_DIR, relative_path))
        for lineno, fragment in formatted_raise_message_fragments(
            source,
            exception_names,
            function_name,
        ):
            if fragment not in build_static_audit:
                missing.append(f"{relative_path}:{lineno}: {fragment}")
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing formatted self-test "
            "error pins: " + "; ".join(missing)
        )


def assert_python_selftest_dynamic_errors_pinned(
    source_texts=None,
    build_static_audit=None,
):
    if build_static_audit is None:
        build_static_audit = read(os.path.abspath(__file__))
    if source_texts is None:
        source_texts = {}

    assert_fragments(
        build_static_audit,
        "tests/build_static_audit.py dynamic self-test error pins",
        PYTHON_SELFTEST_DYNAMIC_ERROR_FRAGMENTS,
    )

    specs = (
        ("tests/release_evidence_test.py", ("AssertionError",)),
        ("tests/s3_provider_matrix_test.py", ("AssertionError", "TestError", "MatrixError")),
    )
    missing = []
    for relative_path, exception_names in specs:
        source = source_texts.get(relative_path)
        if source is None:
            source = read(os.path.join(PROJECT_DIR, relative_path))
        for lineno, fragment in dynamic_raise_message_fragments(
            source,
            exception_names,
            "self_test",
        ):
            if fragment not in build_static_audit:
                missing.append(f"{relative_path}:{lineno}: {fragment}")
    if missing:
        raise AssertionError(
            "tests/build_static_audit.py missing dynamic self-test "
            "error pins: " + "; ".join(missing)
        )


def assert_python_selftest_raise_shape_docs(release_criteria=None, parity=None):
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    assert_fragments(
        release_criteria,
        "release criteria Python self-test raise-shape docs",
        PYTHON_SELFTEST_RAISE_SHAPE_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity Python self-test raise-shape docs",
        PYTHON_SELFTEST_RAISE_SHAPE_DOC_FRAGMENTS,
    )


def assert_python_selftest_raise_shape_scope_complete(
    source_texts=None,
    specs=None,
    self_test_gates=None,
    no_raise_paths=None,
):
    if source_texts is None:
        source_texts = {}
    if specs is None:
        specs = PYTHON_SELFTEST_RAISE_SHAPE_SPECS
    if self_test_gates is None:
        self_test_gates = PYTHON_SELF_TEST_GATES
    if no_raise_paths is None:
        no_raise_paths = PYTHON_SELFTEST_RAISE_SHAPE_NO_RAISE_PATHS

    gate_paths = {path for _, path in self_test_gates}
    covered_paths = {relative_path for relative_path, *_ in specs}
    no_raise_path_set = set(no_raise_paths)
    missing = sorted(gate_paths - covered_paths - no_raise_path_set)
    extra = sorted((covered_paths | no_raise_path_set) - gate_paths)
    overlapping = sorted(covered_paths & no_raise_path_set)

    failures = []
    if missing:
        failures.append("missing raise-shape coverage for " + ", ".join(missing))
    if extra:
        failures.append("raise-shape coverage for non-gates " + ", ".join(extra))
    if overlapping:
        failures.append(
            "paths are both covered and marked no-direct-raise: "
            + ", ".join(overlapping)
        )

    for relative_path in sorted(no_raise_path_set):
        source = source_texts.get(relative_path)
        if source is None:
            source = read(os.path.join(PROJECT_DIR, relative_path))
        raises = selftest_raise_message_shapes(source, "self_test")
        if raises:
            formatted = ", ".join(
                f"{lineno}:{exception_name}:{shape}"
                for lineno, exception_name, shape in raises
            )
            failures.append(
                f"{relative_path} is marked no-direct-raise but has {formatted}"
            )

    if failures:
        raise AssertionError(
            "Python self-test raise-shape scope drift: " + "; ".join(failures)
        )


def assert_python_selftest_raise_shapes_pinned(
    source_texts=None,
    specs=None,
    self_test_gates=None,
    no_raise_paths=None,
    release_criteria=None,
    parity=None,
):
    if source_texts is None:
        source_texts = {}
    if specs is None:
        specs = PYTHON_SELFTEST_RAISE_SHAPE_SPECS

    assert_python_selftest_raise_shape_docs(release_criteria, parity)
    assert_python_selftest_raise_shape_scope_complete(
        source_texts,
        specs,
        self_test_gates,
        no_raise_paths,
    )

    unexpected = []
    for relative_path, function_name, exception_names, allowed_shapes in specs:
        source = source_texts.get(relative_path)
        if source is None:
            source = read(os.path.join(PROJECT_DIR, relative_path))
        raises = selftest_raise_message_shapes(source, function_name)
        if not raises:
            unexpected.append(f"{relative_path}:{function_name}: no pinned raises")
            continue
        for lineno, exception_name, shape in raises:
            if exception_name not in exception_names:
                unexpected.append(
                    f"{relative_path}:{function_name}:{lineno}: "
                    f"unexpected self-test exception {exception_name}"
                )
                continue
            if shape not in allowed_shapes:
                unexpected.append(
                    f"{relative_path}:{function_name}:{lineno}: "
                    f"unexpected self-test raise message shape {shape}"
                )
    if unexpected:
        raise AssertionError(
            "Python self-test raise-shape catalogue drift: "
            + "; ".join(unexpected)
        )


def assert_benchmark_main_live_s3_preflight(benchmark_main=None):
    if benchmark_main is None:
        benchmark_main = read(BENCHMARK_MAIN)

    assert_fragments(
        benchmark_main,
        "benchmarks/main.zig live-S3 preflight contract",
        BENCHMARK_MAIN_LIVE_S3_PREFLIGHT_CONTRACT,
    )


def assert_benchmark_main_unit_test_wired(build_zig=None):
    if build_zig is None:
        build_zig = read(BUILD_ZIG)

    assert_fragments(
        build_zig,
        "build.zig benchmark main unit-test wiring",
        (
            "const bench_unit_test = b.addTest(",
            ".root_module = bench_mod",
            "const run_bench_unit_test = b.addRunArtifact(bench_unit_test)",
            "test_step.dependOn(&run_bench_unit_test.step)",
        ),
    )


def assert_minio_s3_live_bool_preflight(minio_s3_test=None):
    if minio_s3_test is None:
        minio_s3_test = read(MINIO_S3_TEST)

    assert_fragments(
        minio_s3_test,
        "tests/minio_s3_test.zig live MinIO boolean preflight contract",
        MINIO_S3_LIVE_BOOL_PREFLIGHT_CONTRACT,
    )


def assert_startup_config_fail_closed_contract(
    source_texts=None, release_criteria=None, parity=None
):
    if source_texts is None:
        source_texts = {
            "src/config.zig": read(CONFIG_ZIG),
            "src/main.zig": read(MAIN_ZIG),
            "src/broker/handler.zig": read(BROKER_HANDLER_ZIG),
        }
    if release_criteria is None:
        release_criteria = read(RELEASE_CRITERIA)
    if parity is None:
        parity = read(AUTOMQ_PARITY)

    for path, fragments in STARTUP_CONFIG_FAIL_CLOSED_SOURCE_CONTRACTS:
        assert_fragments(source_texts.get(path, ""), path, fragments)

    assert_fragments(
        release_criteria,
        "release criteria startup config fail-closed docs",
        STARTUP_CONFIG_FAIL_CLOSED_DOC_FRAGMENTS,
    )
    assert_fragments(
        parity,
        "AutoMQ parity startup config fail-closed docs",
        STARTUP_CONFIG_FAIL_CLOSED_DOC_FRAGMENTS,
    )


def audit():
    build_zig = read(BUILD_ZIG)
    makefile = read(MAKEFILE)
    assert_toolchain_contract()
    assert_compose_contract()
    assert_automq_parity_release_evidence_contract()
    assert_python_monotonic_harness_scope_complete()
    assert_python_harness_deadlines_monotonic()
    assert_python_kafka_visible_timestamps_wall_clock()
    assert_live_hook_preflight_contract()
    assert_run_gate_bool_preflight()
    assert_release_evidence_verifier_contract()
    assert_release_evidence_command_provenance_dispatch_pinned()
    assert_release_evidence_output_marker_dispatch_pinned()
    assert_unsupported_surface_catalog_pinned()
    assert_unsupported_status_catalog_pinned()
    assert_unsupported_text_field_catalog_pinned()
    assert_required_command_block_catalog_pinned()
    assert_required_env_catalog_pinned()
    assert_command_env_assignment_catalog_pinned()
    assert_command_shape_catalog_pinned()
    assert_skip_marker_catalog_pinned()
    assert_output_marker_catalog_pinned()
    assert_forbidden_fragment_catalog_pinned()
    assert_schema_field_catalog_pinned()
    assert_blocking_flag_catalog_pinned()
    assert_numeric_env_catalog_pinned()
    assert_coverage_selector_catalog_pinned()
    assert_comma_env_catalog_pinned()
    assert_boolean_env_catalog_pinned()
    assert_token_vocabulary_catalog_pinned()
    assert_s3_string_env_catalog_pinned()
    assert_s3_scoped_marker_catalog_pinned()
    assert_sample_env_output_catalog_pinned()
    assert_build_summary_catalog_pinned()
    assert_hook_provenance_catalog_pinned()
    assert_client_capability_catalog_pinned()
    assert_client_tool_marker_catalog_pinned()
    assert_client_version_catalog_pinned()
    assert_chaos_scenario_catalog_pinned()
    assert_detail_output_marker_catalog_pinned()
    assert_comparative_benchmark_catalog_pinned()
    assert_benchmark_compare_table_headers_match_release_evidence()
    assert_benchmark_compare_target_labels_match_release_evidence()
    assert_comparative_threshold_default_catalog_pinned()
    assert_e2e_fixture_action_catalog_pinned()
    assert_release_evidence_selftest_assertions_pinned()
    assert_protocol_static_audit_contract()
    assert_observability_static_audit_contract()
    assert_observability_selftest_assertions_pinned()
    assert_s3_process_crash_selftest_assertions_pinned()
    assert_benchmark_compare_selftest_assertions_pinned()
    assert_e2e_selftest_assertions_pinned()
    assert_chaos_selftest_errors_pinned()
    assert_kraft_failover_selftest_errors_pinned()
    assert_client_matrix_selftest_errors_pinned()
    assert_s3_provider_matrix_selftest_errors_pinned()
    assert_python_selftest_formatted_errors_pinned()
    assert_python_selftest_dynamic_errors_pinned()
    assert_python_selftest_raise_shapes_pinned()
    assert_benchmark_main_live_s3_preflight()
    assert_benchmark_main_unit_test_wired(build_zig)
    assert_minio_s3_live_bool_preflight()
    assert_startup_config_fail_closed_contract()

    for step_name, path in PYTHON_SELF_TEST_GATES:
        step_var = find_step_var(build_zig, step_name)
        command_var = find_python_command_var(build_zig, path, self_test=True)
        assert_depends(build_zig, step_var, command_var, step_name)
        assert_depends(build_zig, "test_step", command_var, "default test step")

    for step_name, path in PYTHON_RUNTIME_GATES:
        step_var = find_step_var(build_zig, step_name)
        command_var = find_python_command_var(build_zig, path, self_test=False)
        assert_depends(build_zig, step_var, command_var, step_name)
        if step_name in INSTALL_DEPENDENT_RUNTIME_GATES:
            assert_install_dependency(build_zig, command_var, step_name)

    static_audit_target = make_target_body(makefile, "static-audit")
    for step_name in MAKE_STATIC_AUDIT_STEPS:
        if f"build {step_name} --summary all" not in static_audit_target:
            raise AssertionError(f"Makefile static-audit target missing {step_name}")


def expect_failure(label, func, fragment):
    try:
        func()
    except AssertionError as exc:
        if fragment not in str(exc):
            raise AssertionError(
                f"{label} failed with wrong message: {exc}"
            ) from exc
        return
    raise AssertionError(f"{label} unexpectedly passed")


def self_test():
    audit()
    expect_failure(
        "floating compose image negative",
        lambda: assert_no_floating_compose_images(
            (("benchmarks/kafka-compose.yml", "image: apache/kafka:latest"),)
        ),
        "floating :latest images",
    )
    expect_failure(
        "stale README compose command negative",
        lambda: assert_no_stale_readme_compose_fragments(
            "docker-compose up\nlocalhost:9092 -t test-topic"
        ),
        "stale fragments",
    )
    expect_failure(
        "missing pinned compose image documentation negative",
        lambda: assert_release_criteria_pins_compose_images(
            "\n".join(
                (
                    REQUIRED_KAFKA_IMAGE,
                    REQUIRED_MINIO_IMAGE,
                    REQUIRED_MINIO_MC_IMAGE,
                    "must not use `:latest`",
                )
            )
        ),
        REQUIRED_AUTOMQ_IMAGE,
    )
    expect_failure(
        "wall-clock Python deadline negative",
        lambda: assert_python_harness_deadlines_monotonic(
            (("tests/example.py", "deadline = time.time() + 30\n"),)
        ),
        "wall-clock time for runtime gates",
    )
    expect_failure(
        "compact wall-clock Python deadline negative",
        lambda: assert_python_harness_deadlines_monotonic(
            (
                (
                    "tests/example.py",
                    "deadline=time.time()+30\nwhile time.time()<deadline:\n    pass\n",
                ),
            )
        ),
        "wall-clock time for runtime gates",
    )
    expect_failure(
        "monotonic Python harness scope negative",
        lambda: assert_python_monotonic_harness_scope_complete(
            runtime_gates=(("test-example", "tests/example.py"),),
            harnesses=(),
        ),
        "missing monotonic deadline audit coverage",
    )
    expect_failure(
        "Kafka-visible monotonic timestamp negative",
        lambda: assert_python_kafka_visible_timestamps_wall_clock(
            "def current_time_ms():\n"
            "    return int(time.time() * 1000)\n"
            "now_ms = int(time.monotonic() * 1000)\n"
        ),
        "monotonic time for Kafka-visible record timestamps",
    )
    expect_failure(
        "MinIO live boolean preflight negative",
        lambda: assert_minio_s3_live_bool_preflight(
            "fn envBool(name: [:0]const u8, default: bool) bool {\n"
            "    return default;\n"
            "}\n"
        ),
        "tests/minio_s3_test.zig live MinIO boolean preflight contract",
    )
    expect_failure(
        "live-hook preflight static audit negative",
        lambda: assert_live_hook_preflight_contract(
            harness_texts={"tests/chaos_test.py": "def validate_phase_tokens_unique(\n"},
            parity="Latest live-hook preflight tranche\n",
        ),
        "tests/chaos_test.py missing required fragments",
    )
    expect_failure(
        "run-gate boolean preflight static audit negative",
        lambda: assert_run_gate_bool_preflight(
            harness_texts={"tests/chaos_test.py": "def run_gate_enabled(\n"},
            release_criteria="\n".join(RUN_GATE_BOOL_PREFLIGHT_DOC_FRAGMENTS),
            parity="\n".join(RUN_GATE_BOOL_PREFLIGHT_DOC_FRAGMENTS),
        ),
        "tests/chaos_test.py missing required fragments",
    )
    expect_failure(
        "startup config fail-closed static audit negative",
        lambda: assert_startup_config_fail_closed_contract(
            source_texts={"src/config.zig": "pub fn firstCommaSeparatedValueStrict(\n"},
            release_criteria="\n".join(STARTUP_CONFIG_FAIL_CLOSED_DOC_FRAGMENTS),
            parity="\n".join(STARTUP_CONFIG_FAIL_CLOSED_DOC_FRAGMENTS),
        ),
        "src/config.zig missing required fragments",
    )
    expect_failure(
        "release-evidence client profile verifier negative",
        lambda: assert_release_evidence_verifier_contract(
            "def validate_client_profile_provenance(\n"
            "def validate_client_profile_tools(\n"
        ),
        "tests/release_evidence_test.py client profile provenance verifier",
    )
    expect_failure(
        "release-evidence command provenance dispatch negative",
        lambda: assert_release_evidence_command_provenance_dispatch_pinned(
            'def validate_release_evidence(\n'
            '    if requirement["label"] == "Docker E2E gate":\n'
            '        pass\n'
            '\n'
            'def current_git_commit(\n',
            specs=(
                (
                    "Docker E2E gate",
                    "validate_e2e_command_provenance",
                    ("ZMQ_E2E_REQUIRED_CHAOS_PHASES",),
                ),
            ),
            check_scope=False,
        ),
        "Docker E2E gate dispatch does not call validate_e2e_command_provenance",
    )
    expect_failure(
        "release-evidence command provenance dispatch scope negative",
        lambda: assert_release_evidence_command_provenance_dispatch_pinned(
            'def validate_new_command_provenance(command, environment, required):\n'
            '    return []\n'
            '\n'
            'def validate_release_evidence():\n'
            '    return []\n'
            '\n'
            'def current_git_commit():\n'
            '    return "0" * 40\n',
            specs=(),
        ),
        "missing command-provenance dispatch catalogue entries for "
        "validate_new_command_provenance",
    )
    expect_failure(
        "release-evidence output marker dispatch negative",
        lambda: assert_release_evidence_output_marker_dispatch_pinned(
            'def validate_release_evidence():\n'
            '    if requirement["label"] == "Docker E2E gate":\n'
            '        pass\n'
            '\n'
            'def current_git_commit():\n',
            specs=(
                (
                    "Docker E2E gate",
                    ("validate_e2e_final_results_output",),
                ),
            ),
            check_scope=False,
            check_docs=False,
        ),
        "Docker E2E gate dispatch does not call validate_e2e_final_results_output",
    )
    expect_failure(
        "release-evidence output marker dispatch scope negative",
        lambda: assert_release_evidence_output_marker_dispatch_pinned(
            'def validate_release_evidence():\n'
            '    if requirement["label"] == "new gate":\n'
            '        failures.extend(validate_new_summary_output(match_output))\n'
            '\n'
            'def current_git_commit():\n',
            specs=(),
            check_docs=False,
        ),
        "missing output-marker dispatch catalogue entries for "
        "new gate: validate_new_summary_output",
    )
    unsupported_surface_fixture = (
        "REQUIRED_UNSUPPORTED_SURFACES = [\n"
        "    {\n"
        "        'label': 'example live matrix',\n"
        "        'surface_fragments': ['example live matrix'],\n"
        "        'fragments': ['example live matrix', 'release-CI-required'],\n"
        "        'status_label': 'release-CI-required or blocked',\n"
        "        'status_markers': ['release-ci-required', 'blocked'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "unsupported-surface catalogue criteria negative",
        lambda: assert_unsupported_surface_catalog_pinned(
            release_evidence=unsupported_surface_fixture,
            release_criteria=(
                "## Known Unsupported Or Partial Surfaces\n\n"
                "- unrelated release-CI-required surface\n\n"
                "## Release Decision\n"
            ),
            parity="example live matrix\n",
            production_readiness="example live matrix\n",
            check_docs=False,
        ),
        "missing top-level bullet for example live matrix",
    )
    expect_failure(
        "unsupported-surface catalogue readiness negative",
        lambda: assert_unsupported_surface_catalog_pinned(
            release_evidence=unsupported_surface_fixture,
            release_criteria=(
                "## Known Unsupported Or Partial Surfaces\n\n"
                "- example live matrix remains release-CI-required.\n\n"
                "## Release Decision\n"
            ),
            parity="example live matrix\n",
            production_readiness="",
            check_docs=False,
        ),
        "production readiness missing unsupported-surface catalogue pin "
        "for example live matrix",
    )
    unsupported_status_fixture = (
        "UNSUPPORTED_SURFACE_STATUS_MARKERS = (\n"
        "    'unsupported',\n"
        "    'release-ci-required',\n"
        ")\n"
    )
    unsupported_status_pins = "unsupported\nrelease-ci-required\n"
    expect_failure(
        "unsupported status catalogue readiness negative",
        lambda: assert_unsupported_status_catalog_pinned(
            release_evidence=unsupported_status_fixture,
            release_criteria=unsupported_status_pins,
            parity=unsupported_status_pins,
            production_readiness=unsupported_status_pins.replace(
                "release-ci-required\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing unsupported status marker release-ci-required",
    )
    duplicate_unsupported_status_fixture = (
        "UNSUPPORTED_SURFACE_STATUS_MARKERS = ('unsupported', 'unsupported')\n"
    )
    expect_failure(
        "unsupported status catalogue duplicate negative",
        lambda: assert_unsupported_status_catalog_pinned(
            release_evidence=duplicate_unsupported_status_fixture,
            release_criteria="unsupported\n",
            parity="unsupported\n",
            production_readiness="unsupported\n",
            check_docs=False,
        ),
        "UNSUPPORTED_SURFACE_STATUS_MARKERS repeats markers: unsupported",
    )
    unsupported_text_field_fixture = (
        "REQUIRED_UNSUPPORTED_SURFACE_FIELDS = ('surface', 'status', 'evidence')\n"
        "OPTIONAL_UNSUPPORTED_SURFACE_FIELDS = ('id', 'mitigation', 'notes')\n"
        "UNSUPPORTED_SURFACE_FIELDS = (\n"
        "    *REQUIRED_UNSUPPORTED_SURFACE_FIELDS,\n"
        "    *OPTIONAL_UNSUPPORTED_SURFACE_FIELDS,\n"
        ")\n"
        "UNSUPPORTED_SURFACE_TEXT_FIELDS = (\n"
        "    'id', 'surface', 'status', 'evidence', 'mitigation', 'notes'\n"
        ")\n"
    )
    unsupported_text_field_pins = (
        "UNSUPPORTED_SURFACE_TEXT_FIELDS\n"
        "id\nsurface\nstatus\nevidence\nmitigation\nnotes\n"
    )
    expect_failure(
        "unsupported text-field catalogue readiness negative",
        lambda: assert_unsupported_text_field_catalog_pinned(
            release_evidence=unsupported_text_field_fixture,
            release_criteria=unsupported_text_field_pins,
            parity=unsupported_text_field_pins,
            production_readiness=unsupported_text_field_pins.replace("notes\n", ""),
            check_docs=False,
        ),
        "production readiness missing unsupported surface text field notes",
    )
    unknown_unsupported_text_field_fixture = unsupported_text_field_fixture.replace(
        "'id', 'surface', 'status', 'evidence', 'mitigation', 'notes'",
        "'id', 'surface', 'status', 'evidence', 'mitigation', 'owner'",
    )
    expect_failure(
        "unsupported text-field catalogue unknown-field negative",
        lambda: release_evidence_unsupported_text_field_catalog(
            unknown_unsupported_text_field_fixture,
        ),
        "UNSUPPORTED_SURFACE_TEXT_FIELDS must equal unsupported-surface fields",
    )
    required_command_fixture = (
        'RELEASE_ZIG = "/tmp/zig-aarch64-linux-0.16.0/zig"\n'
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example gate',\n"
        "        'required': [f'{RELEASE_ZIG} build test-example --summary all'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "required command block catalogue mismatch negative",
        lambda: assert_required_command_block_catalog_pinned(
            release_evidence=required_command_fixture,
            release_criteria=(
                "## Required Commands\n\n"
                "```sh\n"
                "/tmp/zig-aarch64-linux-0.16.0/zig build test-other --summary all\n"
                "```\n"
                "Release CI must\n"
            ),
            parity="",
            check_docs=False,
        ),
        "line 1 must match example gate",
    )
    expect_failure(
        "required command block catalogue count negative",
        lambda: assert_required_command_block_catalog_pinned(
            release_evidence=required_command_fixture,
            release_criteria=(
                "## Required Commands\n\n"
                "```sh\n"
                "/tmp/zig-aarch64-linux-0.16.0/zig build test-example --summary all\n"
                "/tmp/zig-aarch64-linux-0.16.0/zig build test-other --summary all\n"
                "```\n"
                "Release CI must\n"
            ),
            parity="",
            check_docs=False,
        ),
        "must list exactly 1 command lines, found 2",
    )
    untracked_command_key_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example gate',\n"
        "        'required': ['zig build test-example'],\n"
        "        'new_metadata': ['untracked'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "required command untracked key negative",
        lambda: release_evidence_required_command_catalog(
            untracked_command_key_fixture,
        ),
        "REQUIRED_COMMANDS entry 0 has untracked key 'new_metadata'",
    )
    required_env_fixture = (
        "REQUIRED_ENV_VARS = [\n"
        "    'ZMQ_EXAMPLE_REQUIRED_COVERAGE',\n"
        "]\n"
    )
    expect_failure(
        "required environment catalogue criteria negative",
        lambda: assert_required_env_catalog_pinned(
            release_evidence=required_env_fixture,
            release_criteria="",
            parity="ZMQ_EXAMPLE_REQUIRED_COVERAGE\n",
            production_readiness="ZMQ_EXAMPLE_REQUIRED_COVERAGE\n",
            check_docs=False,
        ),
        "release criteria missing required environment variable "
        "ZMQ_EXAMPLE_REQUIRED_COVERAGE",
    )
    expect_failure(
        "required environment catalogue readiness negative",
        lambda: assert_required_env_catalog_pinned(
            release_evidence=required_env_fixture,
            release_criteria="ZMQ_EXAMPLE_REQUIRED_COVERAGE\n",
            parity="ZMQ_EXAMPLE_REQUIRED_COVERAGE\n",
            production_readiness="",
            check_docs=False,
        ),
        "production readiness missing required environment variable "
        "ZMQ_EXAMPLE_REQUIRED_COVERAGE",
    )
    command_env_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example live gate',\n"
        "        'required': ['ZMQ_RUN_EXAMPLE=1', 'zig build test-example'],\n"
        "        'command_env_assignments': ['ZMQ_EXAMPLE_PROFILE'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "command-env assignment catalogue readiness negative",
        lambda: assert_command_env_assignment_catalog_pinned(
            release_evidence=command_env_fixture,
            release_criteria="example live gate\nZMQ_EXAMPLE_PROFILE\n",
            parity="example live gate\nZMQ_EXAMPLE_PROFILE\n",
            production_readiness="",
            check_docs=False,
        ),
        "production readiness missing command-env assignment "
        "ZMQ_EXAMPLE_PROFILE for example live gate",
    )
    duplicate_command_env_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example live gate',\n"
        "        'required': ['ZMQ_RUN_EXAMPLE=1', 'zig build test-example'],\n"
        "        'command_env_assignments': [\n"
        "            'ZMQ_EXAMPLE_PROFILE',\n"
        "            'ZMQ_EXAMPLE_PROFILE',\n"
        "        ],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "command-env assignment catalogue duplicate negative",
        lambda: assert_command_env_assignment_catalog_pinned(
            release_evidence=duplicate_command_env_fixture,
            release_criteria="example live gate\nZMQ_EXAMPLE_PROFILE\n",
            parity="example live gate\nZMQ_EXAMPLE_PROFILE\n",
            production_readiness="ZMQ_EXAMPLE_PROFILE\n",
            check_docs=False,
        ),
        "example live gate repeats command_env_assignments: ZMQ_EXAMPLE_PROFILE",
    )
    command_shape_fixture = (
        "import re\n"
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS = ('network_partition=[',)\n"
        "ENV_ASSIGNMENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*=.*$')\n"
        "ENV_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')\n"
        "SHELL_COMMAND_SEPARATORS = {'&&', '||', ';'}\n"
        'SUCCESS_SHELL_COMMAND_SEPARATOR = "&&"\n'
        "DISALLOWED_SHELL_OPERATOR_TOKENS = {'|', '>'}\n"
        "DISALLOWED_COMMAND_SUBSTITUTION_FRAGMENTS = ('$(', '`')\n"
        "DISALLOWED_COMMAND_LINE_BREAKS = ('\\n', '\\r')\n"
        "DISALLOWED_COMMAND_QUOTE_CHARS = (\"'\", '\"')\n"
        "DISALLOWED_COMMAND_ESCAPE_CHARS = ('\\\\',)\n"
        "ALLOWED_COMMAND_OUTPUT_MARKER_FRAGMENTS = (\n"
        "    'echo ok: root compose config',\n"
        ")\n"
        "ALLOWED_MULTI_SEGMENT_COMMAND_CHAINS = (\n"
        "    (\n"
        "        'docker compose -f docker-compose.yml config --quiet',\n"
        "        'echo ok: root compose config',\n"
        "    ),\n"
        ")\n"
        "FORBIDDEN_COMMAND_OUTPUT_MARKER_FRAGMENTS = (\n"
        "    'Build Summary:',\n"
        "    'ok:',\n"
        "    *KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS,\n"
        ")\n"
    )
    command_shape_pins = (
        "\n".join(
            value
            for _label, values in release_evidence_command_shape_catalog(
                command_shape_fixture,
            )
            for value in values
        )
        + "\n"
    )
    command_shape_chain_pin = (
        "docker compose -f docker-compose.yml config --quiet && "
        "echo ok: root compose config\n"
    )
    expect_failure(
        "command-shape catalogue readiness negative",
        lambda: assert_command_shape_catalog_pinned(
            release_evidence=command_shape_fixture,
            release_criteria=command_shape_pins,
            parity=command_shape_pins,
            production_readiness=command_shape_pins.replace(
                command_shape_chain_pin,
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing allowed multi-segment command chain "
        "docker compose -f docker-compose.yml config --quiet && "
        "echo ok: root compose config",
    )
    bad_success_command_shape_fixture = command_shape_fixture.replace(
        'SUCCESS_SHELL_COMMAND_SEPARATOR = "&&"\n',
        'SUCCESS_SHELL_COMMAND_SEPARATOR = "||"\n',
    )
    expect_failure(
        "command-shape catalogue success-separator negative",
        lambda: release_evidence_command_shape_catalog(
            bad_success_command_shape_fixture,
        ),
        "SUCCESS_SHELL_COMMAND_SEPARATOR must be &&",
    )
    skip_marker_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example live gate',\n"
        "        'required': ['ZMQ_RUN_EXAMPLE=1', 'zig build test-example'],\n"
        "        'skip_markers': ['skip: set ZMQ_RUN_EXAMPLE=1'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "skip-marker catalogue readiness negative",
        lambda: assert_skip_marker_catalog_pinned(
            release_evidence=skip_marker_fixture,
            release_criteria="example live gate\nskip: set ZMQ_RUN_EXAMPLE=1\n",
            parity="example live gate\nskip: set ZMQ_RUN_EXAMPLE=1\n",
            production_readiness="",
            check_docs=False,
        ),
        "production readiness missing skip marker 'skip: set ZMQ_RUN_EXAMPLE=1' "
        "for example live gate",
    )
    duplicate_skip_marker_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example live gate',\n"
        "        'required': ['ZMQ_RUN_EXAMPLE=1', 'zig build test-example'],\n"
        "        'skip_markers': [\n"
        "            'skip: set ZMQ_RUN_EXAMPLE=1',\n"
        "            'skip: set ZMQ_RUN_EXAMPLE=1',\n"
        "        ],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "skip-marker catalogue duplicate negative",
        lambda: assert_skip_marker_catalog_pinned(
            release_evidence=duplicate_skip_marker_fixture,
            release_criteria="example live gate\nskip: set ZMQ_RUN_EXAMPLE=1\n",
            parity="example live gate\nskip: set ZMQ_RUN_EXAMPLE=1\n",
            production_readiness="skip: set ZMQ_RUN_EXAMPLE=1\n",
            check_docs=False,
        ),
        "example live gate repeats skip_markers: skip: set ZMQ_RUN_EXAMPLE=1",
    )
    output_marker_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example live gate',\n"
        "        'required': ['ZMQ_RUN_EXAMPLE=1', 'zig build test-example'],\n"
        "        'output_markers': ['ok: example live gate'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "output-marker catalogue readiness negative",
        lambda: assert_output_marker_catalog_pinned(
            release_evidence=output_marker_fixture,
            release_criteria="example live gate\nok: example live gate\n",
            parity="example live gate\nok: example live gate\n",
            production_readiness="",
            check_docs=False,
        ),
        "production readiness missing output marker 'ok: example live gate' "
        "for example live gate",
    )
    duplicate_output_marker_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example live gate',\n"
        "        'required': ['ZMQ_RUN_EXAMPLE=1', 'zig build test-example'],\n"
        "        'output_markers': [\n"
        "            'ok: example live gate',\n"
        "            'ok: example live gate',\n"
        "        ],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "output-marker catalogue duplicate negative",
        lambda: assert_output_marker_catalog_pinned(
            release_evidence=duplicate_output_marker_fixture,
            release_criteria="example live gate\nok: example live gate\n",
            parity="example live gate\nok: example live gate\n",
            production_readiness="ok: example live gate\n",
            check_docs=False,
        ),
        "example live gate repeats output_markers: ok: example live gate",
    )
    forbidden_fragment_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example local gate',\n"
        "        'required': ['zig build test-example'],\n"
        "        'forbidden': ['ZMQ_RUN_EXAMPLE_LIVE=1'],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "forbidden-fragment catalogue readiness negative",
        lambda: assert_forbidden_fragment_catalog_pinned(
            release_evidence=forbidden_fragment_fixture,
            release_criteria="example local gate\nZMQ_RUN_EXAMPLE_LIVE=1\n",
            parity="example local gate\nZMQ_RUN_EXAMPLE_LIVE=1\n",
            production_readiness="",
            check_docs=False,
        ),
        "production readiness missing forbidden command fragment "
        "'ZMQ_RUN_EXAMPLE_LIVE=1' for example local gate",
    )
    duplicate_forbidden_fragment_fixture = (
        "REQUIRED_COMMANDS = [\n"
        "    {\n"
        "        'label': 'example local gate',\n"
        "        'required': ['zig build test-example'],\n"
        "        'forbidden': [\n"
        "            'ZMQ_RUN_EXAMPLE_LIVE=1',\n"
        "            'ZMQ_RUN_EXAMPLE_LIVE=1',\n"
        "        ],\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "forbidden-fragment catalogue duplicate negative",
        lambda: assert_forbidden_fragment_catalog_pinned(
            release_evidence=duplicate_forbidden_fragment_fixture,
            release_criteria="example local gate\nZMQ_RUN_EXAMPLE_LIVE=1\n",
            parity="example local gate\nZMQ_RUN_EXAMPLE_LIVE=1\n",
            production_readiness="ZMQ_RUN_EXAMPLE_LIVE=1\n",
            check_docs=False,
        ),
        "example local gate repeats forbidden fragments: ZMQ_RUN_EXAMPLE_LIVE=1",
    )
    schema_field_fixture = (
        "RELEASE_EVIDENCE_FIELDS = ('commit', 'environment')\n"
        "COMMAND_ENTRY_FIELDS = ('command', 'exit_code', 'output')\n"
        "REQUIRED_UNSUPPORTED_SURFACE_FIELDS = ('surface', 'status', 'evidence')\n"
        "OPTIONAL_UNSUPPORTED_SURFACE_FIELDS = ('notes',)\n"
        "UNSUPPORTED_SURFACE_FIELDS = (\n"
        "    *REQUIRED_UNSUPPORTED_SURFACE_FIELDS,\n"
        "    *OPTIONAL_UNSUPPORTED_SURFACE_FIELDS,\n"
        ")\n"
    )
    expect_failure(
        "schema-field catalogue readiness negative",
        lambda: assert_schema_field_catalog_pinned(
            release_evidence=schema_field_fixture,
            release_criteria=(
                "release manifest command entry unsupported surface\n"
                "commit environment command exit_code output surface status evidence notes\n"
            ),
            parity=(
                "release manifest command entry unsupported surface\n"
                "commit environment command exit_code output surface status evidence notes\n"
            ),
            production_readiness=(
                "commit environment command exit_code output surface status evidence\n"
            ),
            check_docs=False,
        ),
        "production readiness missing unsupported surface closed schema field notes",
    )
    duplicate_schema_field_fixture = (
        "RELEASE_EVIDENCE_FIELDS = ('commit', 'commit')\n"
        "COMMAND_ENTRY_FIELDS = ('command', 'exit_code', 'output')\n"
        "REQUIRED_UNSUPPORTED_SURFACE_FIELDS = ('surface', 'status', 'evidence')\n"
        "OPTIONAL_UNSUPPORTED_SURFACE_FIELDS = ('notes',)\n"
        "UNSUPPORTED_SURFACE_FIELDS = (\n"
        "    *REQUIRED_UNSUPPORTED_SURFACE_FIELDS,\n"
        "    *OPTIONAL_UNSUPPORTED_SURFACE_FIELDS,\n"
        ")\n"
    )
    expect_failure(
        "schema-field catalogue duplicate negative",
        lambda: assert_schema_field_catalog_pinned(
            release_evidence=duplicate_schema_field_fixture,
            release_criteria=(
                "release manifest command entry unsupported surface\n"
                "commit environment command exit_code output surface status evidence notes\n"
            ),
            parity=(
                "release manifest command entry unsupported surface\n"
                "commit environment command exit_code output surface status evidence notes\n"
            ),
            production_readiness=(
                "commit environment command exit_code output surface status evidence notes\n"
            ),
            check_docs=False,
        ),
        "RELEASE_EVIDENCE_FIELDS repeats schema fields: commit",
    )
    blocking_flag_fixture = (
        "BLOCKING_FLAGS = ['known_data_loss_bug', 'advertised_stub_api']\n"
        "RELEASE_EVIDENCE_FIELDS = (\n"
        "    'known_data_loss_bug',\n"
        "    'advertised_stub_api',\n"
        ")\n"
    )
    expect_failure(
        "blocking-flag catalogue readiness negative",
        lambda: assert_blocking_flag_catalog_pinned(
            release_evidence=blocking_flag_fixture,
            release_criteria=(
                "known_data_loss_bug=false\n"
                "advertised_stub_api=false\n"
            ),
            parity=(
                "known_data_loss_bug=false\n"
                "advertised_stub_api=false\n"
            ),
            production_readiness="known_data_loss_bug=false\n",
            check_docs=False,
        ),
        "production readiness missing blocking flag false pin advertised_stub_api=false",
    )
    duplicate_blocking_flag_fixture = (
        "BLOCKING_FLAGS = ['known_data_loss_bug', 'known_data_loss_bug']\n"
        "RELEASE_EVIDENCE_FIELDS = ('known_data_loss_bug',)\n"
    )
    expect_failure(
        "blocking-flag catalogue duplicate negative",
        lambda: assert_blocking_flag_catalog_pinned(
            release_evidence=duplicate_blocking_flag_fixture,
            release_criteria="known_data_loss_bug=false\n",
            parity="known_data_loss_bug=false\n",
            production_readiness="known_data_loss_bug=false\n",
            check_docs=False,
        ),
        "BLOCKING_FLAGS repeats blocking flags: known_data_loss_bug",
    )
    unknown_blocking_flag_fixture = (
        "BLOCKING_FLAGS = ['unknown_blocking_flag']\n"
        "RELEASE_EVIDENCE_FIELDS = ('known_data_loss_bug',)\n"
    )
    expect_failure(
        "blocking-flag catalogue manifest-field negative",
        lambda: assert_blocking_flag_catalog_pinned(
            release_evidence=unknown_blocking_flag_fixture,
            release_criteria="unknown_blocking_flag=false\n",
            parity="unknown_blocking_flag=false\n",
            production_readiness="unknown_blocking_flag=false\n",
            check_docs=False,
        ),
        "BLOCKING_FLAGS entries must be release manifest fields: unknown_blocking_flag",
    )
    numeric_env_fixture = (
        "BENCHMARK_THRESHOLD_ENV_VARS = ['ZMQ_THRESHOLD_ONE', 'ZMQ_THRESHOLD_TWO']\n"
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV = (\n"
        "    ('ZMQ_THRESHOLD_ONE', 'threshold_one'),\n"
        "    ('ZMQ_THRESHOLD_TWO', 'threshold_two'),\n"
        ")\n"
        "POSITIVE_INTEGER_ENV_VARS = {'ZMQ_POSITIVE_COUNT'}\n"
    )
    expect_failure(
        "numeric environment catalogue readiness negative",
        lambda: assert_numeric_env_catalog_pinned(
            release_evidence=numeric_env_fixture,
            release_criteria=(
                "ZMQ_THRESHOLD_ONE\nZMQ_THRESHOLD_TWO\nZMQ_POSITIVE_COUNT\n"
            ),
            parity=(
                "ZMQ_THRESHOLD_ONE\nZMQ_THRESHOLD_TWO\nZMQ_POSITIVE_COUNT\n"
            ),
            production_readiness="ZMQ_THRESHOLD_ONE\nZMQ_THRESHOLD_TWO\n",
            check_docs=False,
        ),
        "production readiness missing positive integer environment variable "
        "ZMQ_POSITIVE_COUNT",
    )
    duplicate_numeric_env_fixture = (
        "BENCHMARK_THRESHOLD_ENV_VARS = ['ZMQ_THRESHOLD_ONE', 'ZMQ_THRESHOLD_ONE']\n"
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV = (\n"
        "    ('ZMQ_THRESHOLD_ONE', 'threshold_one'),\n"
        "    ('ZMQ_THRESHOLD_ONE', 'threshold_one_again'),\n"
        ")\n"
        "POSITIVE_INTEGER_ENV_VARS = {'ZMQ_POSITIVE_COUNT'}\n"
    )
    expect_failure(
        "numeric environment catalogue duplicate negative",
        lambda: assert_numeric_env_catalog_pinned(
            release_evidence=duplicate_numeric_env_fixture,
            release_criteria="ZMQ_THRESHOLD_ONE\nZMQ_POSITIVE_COUNT\n",
            parity="ZMQ_THRESHOLD_ONE\nZMQ_POSITIVE_COUNT\n",
            production_readiness="ZMQ_THRESHOLD_ONE\nZMQ_POSITIVE_COUNT\n",
            check_docs=False,
        ),
        "BENCHMARK_THRESHOLD_ENV_VARS repeats environment variables: ZMQ_THRESHOLD_ONE",
    )
    mismatched_numeric_env_fixture = (
        "BENCHMARK_THRESHOLD_ENV_VARS = ['ZMQ_THRESHOLD_ONE', 'ZMQ_THRESHOLD_TWO']\n"
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV = (\n"
        "    ('ZMQ_THRESHOLD_TWO', 'threshold_two'),\n"
        "    ('ZMQ_THRESHOLD_ONE', 'threshold_one'),\n"
        ")\n"
        "POSITIVE_INTEGER_ENV_VARS = {'ZMQ_POSITIVE_COUNT'}\n"
    )
    expect_failure(
        "numeric environment catalogue threshold-map negative",
        lambda: assert_numeric_env_catalog_pinned(
            release_evidence=mismatched_numeric_env_fixture,
            release_criteria=(
                "ZMQ_THRESHOLD_ONE\nZMQ_THRESHOLD_TWO\nZMQ_POSITIVE_COUNT\n"
            ),
            parity=(
                "ZMQ_THRESHOLD_ONE\nZMQ_THRESHOLD_TWO\nZMQ_POSITIVE_COUNT\n"
            ),
            production_readiness=(
                "ZMQ_THRESHOLD_ONE\nZMQ_THRESHOLD_TWO\nZMQ_POSITIVE_COUNT\n"
            ),
            check_docs=False,
        ),
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV variables must match "
        "BENCHMARK_THRESHOLD_ENV_VARS in order",
    )
    coverage_selector_fixture = (
        "REQUIRED_ENV_VARS = ['ZMQ_REQUIRED_PHASES']\n"
        "COVERAGE_SELECTOR_REQUIREMENTS = [\n"
        "    {\n"
        "        'selector': 'ZMQ_SELECTOR_MATRIX',\n"
        "        'required': 'ZMQ_REQUIRED_PHASES',\n"
        "        'label': 'example phases',\n"
        "        'token_style': 'collapsed',\n"
        "        'fixture': 'ZMQ_FIXTURE_ENABLE',\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "coverage selector catalogue readiness negative",
        lambda: assert_coverage_selector_catalog_pinned(
            release_evidence=coverage_selector_fixture,
            release_criteria=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\n"
                "example phases\nZMQ_FIXTURE_ENABLE\n"
            ),
            parity=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\n"
                "example phases\nZMQ_FIXTURE_ENABLE\n"
            ),
            production_readiness=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nZMQ_FIXTURE_ENABLE\n"
            ),
            check_docs=False,
        ),
        "production readiness missing coverage selector fragment 'example phases'",
    )
    untracked_coverage_selector_fixture = (
        "REQUIRED_ENV_VARS = ['ZMQ_REQUIRED_PHASES']\n"
        "COVERAGE_SELECTOR_REQUIREMENTS = [\n"
        "    {\n"
        "        'selector': 'ZMQ_SELECTOR_MATRIX',\n"
        "        'required': 'ZMQ_REQUIRED_PHASES',\n"
        "        'label': 'example phases',\n"
        "        'token_style': 'collapsed',\n"
        "        'extra': 'untracked',\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "coverage selector catalogue untracked-key negative",
        lambda: assert_coverage_selector_catalog_pinned(
            release_evidence=untracked_coverage_selector_fixture,
            release_criteria=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nexample phases\n"
            ),
            parity="ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nexample phases\n",
            production_readiness=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nexample phases\n"
            ),
            check_docs=False,
        ),
        "COVERAGE_SELECTOR_REQUIREMENTS entry 0 has untracked keys: extra",
    )
    unknown_coverage_required_fixture = (
        "REQUIRED_ENV_VARS = ['ZMQ_OTHER_REQUIRED_PHASES']\n"
        "COVERAGE_SELECTOR_REQUIREMENTS = [\n"
        "    {\n"
        "        'selector': 'ZMQ_SELECTOR_MATRIX',\n"
        "        'required': 'ZMQ_REQUIRED_PHASES',\n"
        "        'label': 'example phases',\n"
        "        'token_style': 'collapsed',\n"
        "    },\n"
        "]\n"
    )
    expect_failure(
        "coverage selector catalogue required-env negative",
        lambda: assert_coverage_selector_catalog_pinned(
            release_evidence=unknown_coverage_required_fixture,
            release_criteria=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nexample phases\n"
            ),
            parity="ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nexample phases\n",
            production_readiness=(
                "ZMQ_SELECTOR_MATRIX\nZMQ_REQUIRED_PHASES\nexample phases\n"
            ),
            check_docs=False,
        ),
        "COVERAGE_SELECTOR_REQUIREMENTS entry 0 required variable "
        "ZMQ_REQUIRED_PHASES is not in REQUIRED_ENV_VARS",
    )
    comma_env_fixture = (
        "REQUIRED_ENV_VARS = ['ZMQ_REQUIRED_PHASES', 'ZMQ_REQUIRED_TREND']\n"
        "COMMA_SEPARATED_ENV_VARS = [\n"
        "    name\n"
        "    for name in REQUIRED_ENV_VARS\n"
        "    if name not in ('ZMQ_REQUIRED_TREND',)\n"
        "]\n"
    )
    expect_failure(
        "comma-separated environment catalogue readiness negative",
        lambda: assert_comma_env_catalog_pinned(
            release_evidence=comma_env_fixture,
            release_criteria="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            parity="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            production_readiness="ZMQ_REQUIRED_TREND\n",
            check_docs=False,
        ),
        "production readiness missing comma-separated environment variable "
        "ZMQ_REQUIRED_PHASES",
    )
    duplicate_comma_env_fixture = (
        "REQUIRED_ENV_VARS = [\n"
        "    'ZMQ_REQUIRED_PHASES',\n"
        "    'ZMQ_REQUIRED_PHASES',\n"
        "    'ZMQ_REQUIRED_TREND',\n"
        "]\n"
        "COMMA_SEPARATED_ENV_VARS = [\n"
        "    name\n"
        "    for name in REQUIRED_ENV_VARS\n"
        "    if name not in ('ZMQ_REQUIRED_TREND',)\n"
        "]\n"
    )
    expect_failure(
        "comma-separated environment catalogue duplicate negative",
        lambda: assert_comma_env_catalog_pinned(
            release_evidence=duplicate_comma_env_fixture,
            release_criteria="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            parity="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            production_readiness="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            check_docs=False,
        ),
        "REQUIRED_ENV_VARS repeats environment variables: ZMQ_REQUIRED_PHASES",
    )
    explicit_comma_env_fixture = (
        "REQUIRED_ENV_VARS = ['ZMQ_REQUIRED_PHASES', 'ZMQ_REQUIRED_TREND']\n"
        "COMMA_SEPARATED_ENV_VARS = ['ZMQ_REQUIRED_PHASES']\n"
    )
    expect_failure(
        "comma-separated environment catalogue derived-list negative",
        lambda: assert_comma_env_catalog_pinned(
            release_evidence=explicit_comma_env_fixture,
            release_criteria="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            parity="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            production_readiness="ZMQ_REQUIRED_PHASES\nZMQ_REQUIRED_TREND\n",
            check_docs=False,
        ),
        "COMMA_SEPARATED_ENV_VARS must be a list comprehension",
    )
    boolean_env_fixture = (
        "BOOLEAN_ENV_VARS = {'ZMQ_BOOL_GATE'}\n"
        "CLIENT_PROFILE_BOOL_SUFFIXES = ('ENABLE_EXAMPLE',)\n"
        "E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES = ('FIXTURE_DRY_RUN',)\n"
        "S3_BOOL_SUFFIXES = ('PATH_STYLE',)\n"
    )
    expect_failure(
        "boolean environment catalogue readiness negative",
        lambda: assert_boolean_env_catalog_pinned(
            release_evidence=boolean_env_fixture,
            release_criteria=(
                "ZMQ_BOOL_GATE\nENABLE_EXAMPLE\nFIXTURE_DRY_RUN\nPATH_STYLE\n"
            ),
            parity=(
                "ZMQ_BOOL_GATE\nENABLE_EXAMPLE\nFIXTURE_DRY_RUN\nPATH_STYLE\n"
            ),
            production_readiness="ENABLE_EXAMPLE\nFIXTURE_DRY_RUN\nPATH_STYLE\n",
            check_docs=False,
        ),
        "production readiness missing boolean environment variable ZMQ_BOOL_GATE",
    )
    duplicate_boolean_env_fixture = (
        "BOOLEAN_ENV_VARS = {'ZMQ_BOOL_GATE'}\n"
        "CLIENT_PROFILE_BOOL_SUFFIXES = ('ENABLE_EXAMPLE', 'ENABLE_EXAMPLE')\n"
        "E2E_LOAD_SCALE_FIXTURE_BOOL_SUFFIXES = ('FIXTURE_DRY_RUN',)\n"
        "S3_BOOL_SUFFIXES = ('PATH_STYLE',)\n"
    )
    expect_failure(
        "boolean environment catalogue duplicate negative",
        lambda: assert_boolean_env_catalog_pinned(
            release_evidence=duplicate_boolean_env_fixture,
            release_criteria=(
                "ZMQ_BOOL_GATE\nENABLE_EXAMPLE\nFIXTURE_DRY_RUN\nPATH_STYLE\n"
            ),
            parity=(
                "ZMQ_BOOL_GATE\nENABLE_EXAMPLE\nFIXTURE_DRY_RUN\nPATH_STYLE\n"
            ),
            production_readiness=(
                "ZMQ_BOOL_GATE\nENABLE_EXAMPLE\nFIXTURE_DRY_RUN\nPATH_STYLE\n"
            ),
            check_docs=False,
        ),
        "CLIENT_PROFILE_BOOL_SUFFIXES repeats boolean entries: ENABLE_EXAMPLE",
    )
    token_vocabulary_fixture = (
        "PLACEHOLDER_ENV_VALUES = {'placeholder', 'todo'}\n"
        "BOOL_TRUE_VALUES = {'1', 'true'}\n"
        "BOOL_FALSE_VALUES = {'0', 'false'}\n"
    )
    token_vocabulary_pins = "placeholder\ntodo\n1\ntrue\n0\nfalse\n"
    expect_failure(
        "token vocabulary catalogue readiness negative",
        lambda: assert_token_vocabulary_catalog_pinned(
            release_evidence=token_vocabulary_fixture,
            release_criteria=token_vocabulary_pins,
            parity=token_vocabulary_pins,
            production_readiness=token_vocabulary_pins.replace("todo\n", ""),
            check_docs=False,
        ),
        "production readiness missing placeholder token todo",
    )
    overlapping_token_vocabulary_fixture = (
        "PLACEHOLDER_ENV_VALUES = {'placeholder'}\n"
        "BOOL_TRUE_VALUES = {'1', 'true'}\n"
        "BOOL_FALSE_VALUES = {'0', 'true'}\n"
    )
    expect_failure(
        "token vocabulary catalogue boolean-overlap negative",
        lambda: assert_token_vocabulary_catalog_pinned(
            release_evidence=overlapping_token_vocabulary_fixture,
            release_criteria="placeholder\n1\ntrue\n0\n",
            parity="placeholder\n1\ntrue\n0\n",
            production_readiness="placeholder\n1\ntrue\n0\n",
            check_docs=False,
        ),
        "BOOL_TRUE_VALUES and BOOL_FALSE_VALUES must be disjoint: true",
    )
    s3_string_env_fixture = (
        "S3_STRING_SUFFIXES = ('ENDPOINT', 'BUCKET', 'TLS_CA_FILE')\n"
    )
    expect_failure(
        "S3 string environment catalogue readiness negative",
        lambda: assert_s3_string_env_catalog_pinned(
            release_evidence=s3_string_env_fixture,
            release_criteria="ENDPOINT\nBUCKET\nTLS_CA_FILE\n",
            parity="ENDPOINT\nBUCKET\nTLS_CA_FILE\n",
            production_readiness="ENDPOINT\nBUCKET\n",
            check_docs=False,
        ),
        "production readiness missing S3 string suffix TLS_CA_FILE",
    )
    duplicate_s3_string_env_fixture = (
        "S3_STRING_SUFFIXES = ('ENDPOINT', 'ENDPOINT')\n"
    )
    expect_failure(
        "S3 string environment catalogue duplicate negative",
        lambda: assert_s3_string_env_catalog_pinned(
            release_evidence=duplicate_s3_string_env_fixture,
            release_criteria="ENDPOINT\n",
            parity="ENDPOINT\n",
            production_readiness="ENDPOINT\n",
            check_docs=False,
        ),
        "S3_STRING_SUFFIXES repeats string suffixes: ENDPOINT",
    )
    s3_scoped_marker_fixture = (
        "S3_PROVIDER_SCOPED_MARKER_TEMPLATES = (\n"
        "    (\n"
        "        'live-suite',\n"
        "        'ok: S3 provider live-suite profile {profile} '\n"
        "        'command_started=true completed=true source=command',\n"
        "    ),\n"
        ")\n"
    )
    s3_scoped_marker_pins = (
        "S3_PROVIDER_SCOPED_MARKER_TEMPLATES\n"
        "live-suite\n"
        "ok: S3 provider live-suite profile <profile> "
        "command_started=true completed=true source=command\n"
    )
    expect_failure(
        "S3 scoped marker catalogue readiness negative",
        lambda: assert_s3_scoped_marker_catalog_pinned(
            release_evidence=s3_scoped_marker_fixture,
            release_criteria=s3_scoped_marker_pins,
            parity=s3_scoped_marker_pins,
            production_readiness=s3_scoped_marker_pins.replace(
                "ok: S3 provider live-suite profile <profile> "
                "command_started=true completed=true source=command\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing S3 scoped marker template "
        "ok: S3 provider live-suite profile <profile> "
        "command_started=true completed=true source=command",
    )
    missing_profile_s3_scoped_marker_fixture = s3_scoped_marker_fixture.replace(
        "{profile}",
        "minio",
    )
    expect_failure(
        "S3 scoped marker catalogue placeholder negative",
        lambda: release_evidence_s3_scoped_marker_catalog(
            missing_profile_s3_scoped_marker_fixture,
        ),
        "S3 scoped marker live-suite template must contain one {profile} placeholder",
    )
    sample_env_output_fixture = (
        "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS = {\n"
        "    'example gate': ['ok: example marker'],\n"
        "}\n"
    )
    sample_env_output_pins = (
        "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS\n"
        "example gate\n"
        "ok: example marker\n"
    )
    expect_failure(
        "sample environment output catalogue readiness negative",
        lambda: assert_sample_env_output_catalog_pinned(
            release_evidence=sample_env_output_fixture,
            release_criteria=sample_env_output_pins,
            parity=sample_env_output_pins,
            production_readiness=sample_env_output_pins.replace(
                "ok: example marker\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing sample environment output marker "
        "ok: example marker",
    )
    duplicate_sample_env_output_fixture = (
        "SAMPLE_ENVIRONMENT_OUTPUT_MARKERS = {\n"
        "    'example gate': ['ok: example marker', 'ok: example marker'],\n"
        "}\n"
    )
    expect_failure(
        "sample environment output catalogue duplicate negative",
        lambda: release_evidence_sample_env_output_catalog(
            duplicate_sample_env_output_fixture,
        ),
        "example gate repeats sample output markers: ok: example marker",
    )
    build_summary_fixture = (
        "import re\n"
        "BENCHMARK_RESULTS_ARTIFACT = 'benchmarks/results.json'\n"
        "ZIG_BUILD_SUMMARY_RE = re.compile(\n"
        "    r'Build Summary:\\s+([1-9][0-9]*)/([1-9][0-9]*) steps succeeded'\n"
        "    r'(?:;\\s+([0-9][0-9]*)/([0-9][0-9]*) tests passed(?:.*)?)?'\n"
        ")\n"
    )
    build_summary_pins = (
        "BENCHMARK_RESULTS_ARTIFACT\n"
        "ZIG_BUILD_SUMMARY_RE\n"
        "benchmarks/results.json\n"
        "Build Summary:\n"
        "steps succeeded\n"
        "tests passed\n"
    )
    expect_failure(
        "build summary catalogue readiness negative",
        lambda: assert_build_summary_catalog_pinned(
            release_evidence=build_summary_fixture,
            release_criteria=build_summary_pins,
            parity=build_summary_pins,
            production_readiness=build_summary_pins.replace(
                "benchmarks/results.json\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing benchmark results artifact "
        "benchmarks/results.json",
    )
    malformed_build_summary_fixture = build_summary_fixture.replace(
        "([1-9][0-9]*)/([1-9][0-9]*) steps succeeded",
        "steps succeeded",
    )
    expect_failure(
        "build summary catalogue capture negative",
        lambda: release_evidence_build_summary_catalog(
            malformed_build_summary_fixture,
        ),
        "ZIG_BUILD_SUMMARY_RE must match Zig success summaries",
    )
    hook_provenance_fixture = (
        "REQUIRED_ENV_VARS = [\n"
        "    'ZMQ_REQUIRED_PHASES',\n"
        "    'ZMQ_REQUIRED_PROFILE',\n"
        "    'ZMQ_REQUIRED_ENABLE',\n"
        "]\n"
        "BOOLEAN_ENV_VARS = {'ZMQ_FIXTURE_ENABLE'}\n"
        "S3_BOOL_SUFFIXES = ('RUN_EXAMPLE',)\n"
        "PHASE_HOOK_PROVENANCE_REQUIREMENTS = [\n"
        "    {\n"
        "        'required': 'ZMQ_REQUIRED_PHASES',\n"
        "        'prefix': 'ZMQ_EXAMPLE_PHASE',\n"
        "        'label': 'example phase',\n"
        "        'suffixes': ('DOWN', 'UP'),\n"
        "        'token_style': 'collapsed',\n"
        "        'fixture': 'ZMQ_FIXTURE_ENABLE',\n"
        "    },\n"
        "]\n"
        "PROFILE_HOOK_PROVENANCE_REQUIREMENTS = [\n"
        "    {\n"
        "        'required': 'ZMQ_REQUIRED_PROFILE',\n"
        "        'prefix': 'ZMQ_EXAMPLE_PROFILE',\n"
        "        'label': 'example profile',\n"
        "        'suffixes': ('HOOK_CMD',),\n"
        "        'token_style': 'literal',\n"
        "    },\n"
        "]\n"
        "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS = [\n"
        "    ('ZMQ_REQUIRED_ENABLE', 'RUN_EXAMPLE', 'example enable'),\n"
        "]\n"
    )
    hook_provenance_pins = (
        "ZMQ_REQUIRED_PHASES\nZMQ_EXAMPLE_PHASE\nexample phase\n"
        "DOWN\nUP\ncollapsed\nZMQ_FIXTURE_ENABLE\n"
        "ZMQ_REQUIRED_PROFILE\nZMQ_EXAMPLE_PROFILE\nexample profile\n"
        "HOOK_CMD\nliteral\n"
        "ZMQ_REQUIRED_ENABLE\nRUN_EXAMPLE\nexample enable\n"
    )
    expect_failure(
        "hook-provenance catalogue readiness negative",
        lambda: assert_hook_provenance_catalog_pinned(
            release_evidence=hook_provenance_fixture,
            release_criteria=hook_provenance_pins,
            parity=hook_provenance_pins,
            production_readiness=hook_provenance_pins.replace("example enable\n", ""),
            check_docs=False,
        ),
        "production readiness missing S3 profile enable fragment 'example enable'",
    )
    duplicate_hook_provenance_fixture = (
        "REQUIRED_ENV_VARS = [\n"
        "    'ZMQ_REQUIRED_PHASES',\n"
        "    'ZMQ_REQUIRED_PROFILE',\n"
        "    'ZMQ_REQUIRED_ENABLE',\n"
        "]\n"
        "BOOLEAN_ENV_VARS = {'ZMQ_FIXTURE_ENABLE'}\n"
        "S3_BOOL_SUFFIXES = ('RUN_EXAMPLE',)\n"
        "PHASE_HOOK_PROVENANCE_REQUIREMENTS = [\n"
        "    {\n"
        "        'required': 'ZMQ_REQUIRED_PHASES',\n"
        "        'prefix': 'ZMQ_EXAMPLE_PHASE',\n"
        "        'label': 'example phase',\n"
        "        'suffixes': ('DOWN', 'UP'),\n"
        "        'token_style': 'collapsed',\n"
        "    },\n"
        "    {\n"
        "        'required': 'ZMQ_REQUIRED_PHASES',\n"
        "        'prefix': 'ZMQ_EXAMPLE_PHASE_2',\n"
        "        'label': 'example phase duplicate',\n"
        "        'suffixes': ('DOWN', 'UP'),\n"
        "        'token_style': 'collapsed',\n"
        "    },\n"
        "]\n"
        "PROFILE_HOOK_PROVENANCE_REQUIREMENTS = [\n"
        "    {\n"
        "        'required': 'ZMQ_REQUIRED_PROFILE',\n"
        "        'prefix': 'ZMQ_EXAMPLE_PROFILE',\n"
        "        'label': 'example profile',\n"
        "        'suffixes': ('HOOK_CMD',),\n"
        "        'token_style': 'literal',\n"
        "    },\n"
        "]\n"
        "S3_PROFILE_ENABLE_PROVENANCE_REQUIREMENTS = [\n"
        "    ('ZMQ_REQUIRED_ENABLE', 'RUN_EXAMPLE', 'example enable'),\n"
        "]\n"
    )
    expect_failure(
        "hook-provenance catalogue duplicate negative",
        lambda: assert_hook_provenance_catalog_pinned(
            release_evidence=duplicate_hook_provenance_fixture,
            release_criteria=hook_provenance_pins,
            parity=hook_provenance_pins,
            production_readiness=hook_provenance_pins,
            check_docs=False,
        ),
        "PHASE_HOOK_PROVENANCE_REQUIREMENTS repeats required variables: "
        "ZMQ_REQUIRED_PHASES",
    )
    client_capability_fixture = (
        "REQUIRED_CLIENT_TOOLS = ['tool-a', 'tool-b']\n"
        "REQUIRED_CLIENT_SEMANTICS = [\n"
        "    'basic',\n"
        "    'security',\n"
        "    'security-negative',\n"
        "    'rebalance',\n"
        "    'transactions',\n"
        "]\n"
        "CLIENT_SECURITY_PROTOCOLS = {'PLAINTEXT', 'SASL_SSL'}\n"
        "CLIENT_SASL_MECHANISMS = {'PLAIN'}\n"
        "CLIENT_SECURITY_TOOLS = {'tool-a'}\n"
        "CLIENT_REBALANCE_TOOLS = {'tool-b'}\n"
        "CLIENT_TRANSACTION_TOOLS = {'tool-b'}\n"
    )
    client_capability_pins = (
        "tool-a\ntool-b\nbasic\nsecurity\nsecurity-negative\n"
        "rebalance\ntransactions\nPLAINTEXT\nSASL_SSL\nPLAIN\n"
    )
    expect_failure(
        "client capability catalogue readiness negative",
        lambda: assert_client_capability_catalog_pinned(
            release_evidence=client_capability_fixture,
            release_criteria=client_capability_pins,
            parity=client_capability_pins,
            production_readiness=client_capability_pins.replace("SASL_SSL\n", ""),
            check_docs=False,
        ),
        "production readiness missing client security protocol SASL_SSL",
    )
    unknown_client_tool_fixture = (
        "REQUIRED_CLIENT_TOOLS = ['tool-a']\n"
        "REQUIRED_CLIENT_SEMANTICS = ['basic', 'transactions']\n"
        "CLIENT_SECURITY_PROTOCOLS = {'PLAINTEXT'}\n"
        "CLIENT_SASL_MECHANISMS = {'PLAIN'}\n"
        "CLIENT_SECURITY_TOOLS = {'tool-a'}\n"
        "CLIENT_REBALANCE_TOOLS = {'tool-a'}\n"
        "CLIENT_TRANSACTION_TOOLS = {'tool-b'}\n"
    )
    expect_failure(
        "client capability catalogue subset negative",
        lambda: assert_client_capability_catalog_pinned(
            release_evidence=unknown_client_tool_fixture,
            release_criteria="tool-a\ntool-b\nbasic\ntransactions\nPLAINTEXT\nPLAIN\n",
            parity="tool-a\ntool-b\nbasic\ntransactions\nPLAINTEXT\nPLAIN\n",
            production_readiness=(
                "tool-a\ntool-b\nbasic\ntransactions\nPLAINTEXT\nPLAIN\n"
            ),
            check_docs=False,
        ),
        "CLIENT_TRANSACTION_TOOLS entries must be REQUIRED_CLIENT_TOOLS: tool-b",
    )
    client_tool_marker_fixture = (
        "REQUIRED_CLIENT_TOOLS = ['tool-a', 'tool-b']\n"
        "CLIENT_TOOL_OUTPUT_MARKERS = {\n"
        "    'tool-a': 'ok: tool-a probes',\n"
        "    'tool-b': 'ok: tool-b probes',\n"
        "}\n"
    )
    client_tool_marker_pins = (
        "tool-a\ntool-b\nok: tool-a probes\nok: tool-b probes\n"
    )
    expect_failure(
        "client tool marker catalogue readiness negative",
        lambda: assert_client_tool_marker_catalog_pinned(
            release_evidence=client_tool_marker_fixture,
            release_criteria=client_tool_marker_pins,
            parity=client_tool_marker_pins,
            production_readiness=client_tool_marker_pins.replace(
                "ok: tool-b probes\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing client tool marker ok: tool-b probes",
    )
    duplicate_client_tool_marker_fixture = (
        "REQUIRED_CLIENT_TOOLS = ['tool-a', 'tool-b']\n"
        "CLIENT_TOOL_OUTPUT_MARKERS = {\n"
        "    'tool-a': 'ok: tool probes',\n"
        "    'tool-b': 'ok: tool probes',\n"
        "}\n"
    )
    expect_failure(
        "client tool marker catalogue duplicate negative",
        lambda: assert_client_tool_marker_catalog_pinned(
            release_evidence=duplicate_client_tool_marker_fixture,
            release_criteria="tool-a\ntool-b\nok: tool probes\n",
            parity="tool-a\ntool-b\nok: tool probes\n",
            production_readiness="tool-a\ntool-b\nok: tool probes\n",
            check_docs=False,
        ),
        "CLIENT_TOOL_OUTPUT_MARKERS repeats output markers: ok: tool probes",
    )
    client_version_fixture = (
        "REQUIRED_CLIENT_TOOLS = ['python-tool', 'other-tool']\n"
        "CLIENT_PYTHON_TOOLS = {'python-tool'}\n"
        "CLIENT_UNPINNED_VERSION_LABELS = {'auto', 'default', 'latest'}\n"
    )
    client_version_pins = "python-tool\nauto\ndefault\nlatest\n"
    expect_failure(
        "client version/provenance catalogue readiness negative",
        lambda: assert_client_version_catalog_pinned(
            release_evidence=client_version_fixture,
            release_criteria=client_version_pins,
            parity=client_version_pins,
            production_readiness=client_version_pins.replace("latest\n", ""),
            check_docs=False,
        ),
        "production readiness missing client unpinned version label latest",
    )
    unknown_client_python_tool_fixture = (
        "REQUIRED_CLIENT_TOOLS = ['tool-a']\n"
        "CLIENT_PYTHON_TOOLS = {'tool-b'}\n"
        "CLIENT_UNPINNED_VERSION_LABELS = {'auto'}\n"
    )
    expect_failure(
        "client version/provenance catalogue subset negative",
        lambda: assert_client_version_catalog_pinned(
            release_evidence=unknown_client_python_tool_fixture,
            release_criteria="tool-a\ntool-b\nauto\n",
            parity="tool-a\ntool-b\nauto\n",
            production_readiness="tool-a\ntool-b\nauto\n",
            check_docs=False,
        ),
        "CLIENT_PYTHON_TOOLS entries must be REQUIRED_CLIENT_TOOLS: tool-b",
    )
    chaos_scenario_fixture = (
        "CHAOS_SCENARIO_ALIASES = {\n"
        "    'sigkill': 'sigkill-restart',\n"
        "    'live-s3': 'live-s3-outage',\n"
        "}\n"
        "REQUIRED_CHAOS_SCENARIOS = ['sigkill-restart']\n"
        "CHAOS_SCENARIO_MARKERS = {\n"
        "    'sigkill-restart': 'ok: chaos sigkill-restart',\n"
        "    'live-s3-outage': 'ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command',\n"
        "}\n"
    )
    chaos_scenario_pins = (
        "sigkill\nlive-s3\nsigkill-restart\nlive-s3-outage\n"
        "ok: chaos sigkill-restart\n"
        "ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command\n"
    )
    expect_failure(
        "chaos scenario catalogue readiness negative",
        lambda: assert_chaos_scenario_catalog_pinned(
            release_evidence=chaos_scenario_fixture,
            release_criteria=chaos_scenario_pins,
            parity=chaos_scenario_pins,
            production_readiness=chaos_scenario_pins.replace(
                "ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing chaos scenario marker "
        "ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command",
    )
    bad_chaos_alias_fixture = (
        "CHAOS_SCENARIO_ALIASES = {'bad': 'missing'}\n"
        "REQUIRED_CHAOS_SCENARIOS = ['sigkill-restart']\n"
        "CHAOS_SCENARIO_MARKERS = {'sigkill-restart': 'ok: chaos sigkill-restart'}\n"
    )
    expect_failure(
        "chaos scenario catalogue alias-target negative",
        lambda: assert_chaos_scenario_catalog_pinned(
            release_evidence=bad_chaos_alias_fixture,
            release_criteria="bad\nmissing\nsigkill-restart\nok: chaos sigkill-restart\n",
            parity="bad\nmissing\nsigkill-restart\nok: chaos sigkill-restart\n",
            production_readiness=(
                "bad\nmissing\nsigkill-restart\nok: chaos sigkill-restart\n"
            ),
            check_docs=False,
        ),
        "CHAOS_SCENARIO_ALIASES targets must be CHAOS_SCENARIO_MARKERS keys: "
        "missing",
    )
    duplicate_chaos_marker_fixture = (
        "CHAOS_SCENARIO_ALIASES = {'sigkill': 'sigkill-restart'}\n"
        "REQUIRED_CHAOS_SCENARIOS = ['sigkill-restart', 's3-outage']\n"
        "CHAOS_SCENARIO_MARKERS = {\n"
        "    'sigkill-restart': 'ok: duplicate chaos',\n"
        "    's3-outage': 'ok: duplicate chaos',\n"
        "}\n"
    )
    expect_failure(
        "chaos scenario catalogue duplicate-marker negative",
        lambda: assert_chaos_scenario_catalog_pinned(
            release_evidence=duplicate_chaos_marker_fixture,
            release_criteria="sigkill\nsigkill-restart\ns3-outage\nok: duplicate chaos\n",
            parity="sigkill\nsigkill-restart\ns3-outage\nok: duplicate chaos\n",
            production_readiness=(
                "sigkill\nsigkill-restart\ns3-outage\nok: duplicate chaos\n"
            ),
            check_docs=False,
        ),
        "CHAOS_SCENARIO_MARKERS repeats output markers: ok: duplicate chaos",
    )
    detail_output_marker_fixture = (
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS = (\n"
        "    'network_partition=[',\n"
        "    'controller_api_versions_checked=true',\n"
        ")\n"
        "COMPARATIVE_TABLE_ROW_MARKERS = ('ApiVersions', 'Fetch')\n"
        "BENCHMARK_OUTPUT_LINE_MARKERS = {\n"
        "    '=== Benchmarks complete ===',\n"
        "    'S3 WAL request volume',\n"
        "}\n"
        "KRAFT_DETAIL_OUTPUT_MARKERS = set(KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS)\n"
        "E2E_OUTPUT_LINE_MARKERS = {'3-Node E2E Test Suite', 'Results:'}\n"
    )
    detail_output_marker_pins = (
        "network_partition=[\n"
        "controller_api_versions_checked=true\n"
        "ApiVersions\n"
        "Fetch\n"
        "=== Benchmarks complete ===\n"
        "S3 WAL request volume\n"
        "3-Node E2E Test Suite\n"
        "Results:\n"
    )
    expect_failure(
        "detail output marker catalogue readiness negative",
        lambda: assert_detail_output_marker_catalog_pinned(
            release_evidence=detail_output_marker_fixture,
            release_criteria=detail_output_marker_pins,
            parity=detail_output_marker_pins,
            production_readiness=detail_output_marker_pins.replace("Fetch\n", ""),
            check_docs=False,
        ),
        "production readiness missing comparative table row marker Fetch",
    )
    mismatched_kraft_detail_marker_fixture = (
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS = ('network_partition=[',)\n"
        "COMPARATIVE_TABLE_ROW_MARKERS = ('ApiVersions',)\n"
        "BENCHMARK_OUTPUT_LINE_MARKERS = {'=== Benchmarks complete ==='}\n"
        "KRAFT_DETAIL_OUTPUT_MARKERS = {'controller_api_versions_checked=true'}\n"
        "E2E_OUTPUT_LINE_MARKERS = {'Results:'}\n"
    )
    expect_failure(
        "detail output marker catalogue mirror negative",
        lambda: assert_detail_output_marker_catalog_pinned(
            release_evidence=mismatched_kraft_detail_marker_fixture,
            release_criteria=(
                "network_partition=[\nApiVersions\n=== Benchmarks complete ===\n"
                "controller_api_versions_checked=true\nResults:\n"
            ),
            parity=(
                "network_partition=[\nApiVersions\n=== Benchmarks complete ===\n"
                "controller_api_versions_checked=true\nResults:\n"
            ),
            production_readiness=(
                "network_partition=[\nApiVersions\n=== Benchmarks complete ===\n"
                "controller_api_versions_checked=true\nResults:\n"
            ),
            check_docs=False,
        ),
        "KRAFT_DETAIL_OUTPUT_MARKERS must mirror "
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS",
    )
    duplicate_detail_output_marker_fixture = (
        "KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS = ('network_partition=[',)\n"
        "COMPARATIVE_TABLE_ROW_MARKERS = ('ApiVersions', 'ApiVersions')\n"
        "BENCHMARK_OUTPUT_LINE_MARKERS = {'=== Benchmarks complete ==='}\n"
        "KRAFT_DETAIL_OUTPUT_MARKERS = set(KRAFT_FAILOVER_DETAIL_OUTPUT_MARKERS)\n"
        "E2E_OUTPUT_LINE_MARKERS = {'Results:'}\n"
    )
    expect_failure(
        "detail output marker catalogue duplicate negative",
        lambda: assert_detail_output_marker_catalog_pinned(
            release_evidence=duplicate_detail_output_marker_fixture,
            release_criteria=(
                "network_partition=[\nApiVersions\n=== Benchmarks complete ===\n"
                "Results:\n"
            ),
            parity=(
                "network_partition=[\nApiVersions\n=== Benchmarks complete ===\n"
                "Results:\n"
            ),
            production_readiness=(
                "network_partition=[\nApiVersions\n=== Benchmarks complete ===\n"
                "Results:\n"
            ),
            check_docs=False,
        ),
        "COMPARATIVE_TABLE_ROW_MARKERS repeats markers: ApiVersions",
    )
    comparative_benchmark_fixture = (
        "COMPARATIVE_TARGET_LABELS = {\n"
        "    'zmq': 'ZMQ (Zig)',\n"
        "    'kafka': 'Apache Kafka',\n"
        "    'automq': 'AutoMQ (Java)',\n"
        "}\n"
        "COMPARATIVE_TABLE_TARGET_HEADERS = {\n"
        "    'zmq': 'ZMQ',\n"
        "    'kafka': 'Kafka',\n"
        "    'automq': 'AutoMQ',\n"
        "}\n"
        "COMPARATIVE_TABLE_METRICS = ('tput', 'p50')\n"
        "COMPARATIVE_MEASUREMENT_RE = {\n"
        "    'tput': re.compile(r'(?<![\\w.,+-])([0-9]+)/s\\b'),\n"
        "    'p50': re.compile(r'(?<![\\w.,+-])([0-9]+)ms\\b'),\n"
        "}\n"
    )
    comparative_benchmark_pins = (
        "zmq\nkafka\nautomq\nZMQ (Zig)\nApache Kafka\nAutoMQ (Java)\n"
        "ZMQ\nKafka\nAutoMQ\ntput\np50\n"
    )
    expect_failure(
        "comparative benchmark catalogue readiness negative",
        lambda: assert_comparative_benchmark_catalog_pinned(
            release_evidence=comparative_benchmark_fixture,
            release_criteria=comparative_benchmark_pins,
            parity=comparative_benchmark_pins,
            production_readiness=comparative_benchmark_pins.replace(
                "AutoMQ (Java)\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing comparative target label AutoMQ (Java)",
    )
    missing_comparative_regex_fixture = (
        "COMPARATIVE_TARGET_LABELS = {'zmq': 'ZMQ (Zig)'}\n"
        "COMPARATIVE_TABLE_TARGET_HEADERS = {'zmq': 'ZMQ'}\n"
        "COMPARATIVE_TABLE_METRICS = ('tput', 'p99')\n"
        "COMPARATIVE_MEASUREMENT_RE = {\n"
        "    'tput': re.compile(r'(?<![\\w.,+-])([0-9]+)/s\\b'),\n"
        "}\n"
    )
    expect_failure(
        "comparative benchmark catalogue measurement-key negative",
        lambda: assert_comparative_benchmark_catalog_pinned(
            release_evidence=missing_comparative_regex_fixture,
            release_criteria="zmq\nZMQ (Zig)\ntput\np99\n",
            parity="zmq\nZMQ (Zig)\ntput\np99\n",
            production_readiness="zmq\nZMQ (Zig)\ntput\np99\n",
            check_docs=False,
        ),
        "COMPARATIVE_MEASUREMENT_RE keys must match COMPARATIVE_TABLE_METRICS: "
        "missing p99",
    )
    duplicate_comparative_label_fixture = (
        "COMPARATIVE_TARGET_LABELS = {'zmq': 'ZMQ', 'kafka': 'ZMQ'}\n"
        "COMPARATIVE_TABLE_TARGET_HEADERS = {'zmq': 'ZMQ', 'kafka': 'Kafka'}\n"
        "COMPARATIVE_TABLE_METRICS = ('tput',)\n"
        "COMPARATIVE_MEASUREMENT_RE = {\n"
        "    'tput': re.compile(r'(?<![\\w.,+-])([0-9]+)/s\\b'),\n"
        "}\n"
    )
    expect_failure(
        "comparative benchmark catalogue duplicate-label negative",
        lambda: assert_comparative_benchmark_catalog_pinned(
            release_evidence=duplicate_comparative_label_fixture,
            release_criteria="zmq\nkafka\nZMQ\ntput\n",
            parity="zmq\nkafka\nZMQ\ntput\n",
            production_readiness="zmq\nkafka\nZMQ\ntput\n",
            check_docs=False,
        ),
        "COMPARATIVE_TARGET_LABELS repeats labels: ZMQ",
    )
    mismatched_comparative_header_fixture = (
        "COMPARATIVE_TARGET_LABELS = {'zmq': 'ZMQ (Zig)', 'kafka': 'Apache Kafka'}\n"
        "COMPARATIVE_TABLE_TARGET_HEADERS = {'zmq': 'ZMQ'}\n"
        "COMPARATIVE_TABLE_METRICS = ('tput',)\n"
        "COMPARATIVE_MEASUREMENT_RE = {\n"
        "    'tput': re.compile(r'(?<![\\w.,+-])([0-9]+)/s\\b'),\n"
        "}\n"
    )
    expect_failure(
        "comparative benchmark catalogue target-header negative",
        lambda: assert_comparative_benchmark_catalog_pinned(
            release_evidence=mismatched_comparative_header_fixture,
            release_criteria="zmq\nkafka\nZMQ (Zig)\nApache Kafka\nZMQ\ntput\n",
            parity="zmq\nkafka\nZMQ (Zig)\nApache Kafka\nZMQ\ntput\n",
            production_readiness="zmq\nkafka\nZMQ (Zig)\nApache Kafka\nZMQ\ntput\n",
            check_docs=False,
        ),
        "COMPARATIVE_TABLE_TARGET_HEADERS keys must match COMPARATIVE_TARGET_LABELS: "
        "missing kafka",
    )
    expect_failure(
        "benchmark compare table-header mirror negative",
        lambda: assert_benchmark_compare_table_headers_match_release_evidence(
            benchmark_compare="TARGET_SHORT_LABELS = {'zmq': 'Zig'}\n",
            release_evidence="COMPARATIVE_TABLE_TARGET_HEADERS = {'zmq': 'ZMQ'}\n",
        ),
        "TARGET_SHORT_LABELS must match",
    )
    expect_failure(
        "benchmark compare target-label mirror negative",
        lambda: assert_benchmark_compare_target_labels_match_release_evidence(
            benchmark_compare=(
                "ALL_TARGETS = ['zmq', 'kafka']\n"
                "TARGET_LABELS = {'zmq': 'ZMQ (Zig)', 'kafka': 'Kafka'}\n"
            ),
            release_evidence=(
                "COMPARATIVE_TARGET_LABELS = {\n"
                "    'zmq': 'ZMQ (Zig)',\n"
                "    'kafka': 'Apache Kafka',\n"
                "}\n"
            ),
        ),
        "TARGET_LABELS must match",
    )
    comparative_threshold_default_fixture = (
        "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS = {\n"
        "    'min_throughput_ratio': 0.05,\n"
        "    'max_error_rate': 0.0,\n"
        "}\n"
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV = (\n"
        "    ('ZMQ_MIN_TPUT', 'min_throughput_ratio'),\n"
        "    ('ZMQ_MAX_ERROR', 'max_error_rate'),\n"
        ")\n"
    )
    comparative_threshold_default_pins = (
        "min_throughput_ratio=0.05\nmax_error_rate=0.0\n"
    )
    expect_failure(
        "comparative threshold default catalogue readiness negative",
        lambda: assert_comparative_threshold_default_catalog_pinned(
            release_evidence=comparative_threshold_default_fixture,
            release_criteria=comparative_threshold_default_pins,
            parity=comparative_threshold_default_pins,
            production_readiness=comparative_threshold_default_pins.replace(
                "max_error_rate=0.0\n",
                "",
            ),
            check_docs=False,
        ),
        "production readiness missing comparative threshold default "
        "max_error_rate=0.0",
    )
    missing_comparative_threshold_default_fixture = (
        "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS = {\n"
        "    'min_throughput_ratio': 0.05,\n"
        "}\n"
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV = (\n"
        "    ('ZMQ_MIN_TPUT', 'min_throughput_ratio'),\n"
        "    ('ZMQ_MAX_ERROR', 'max_error_rate'),\n"
        ")\n"
    )
    expect_failure(
        "comparative threshold default catalogue key negative",
        lambda: assert_comparative_threshold_default_catalog_pinned(
            release_evidence=missing_comparative_threshold_default_fixture,
            release_criteria="min_throughput_ratio=0.05\nmax_error_rate=0.0\n",
            parity="min_throughput_ratio=0.05\nmax_error_rate=0.0\n",
            production_readiness="min_throughput_ratio=0.05\nmax_error_rate=0.0\n",
            check_docs=False,
        ),
        "COMPARATIVE_BENCHMARK_THRESHOLD_ENV keys must match "
        "DEFAULT_COMPARATIVE_BENCHMARK_THRESHOLDS: missing defaults max_error_rate",
    )
    e2e_fixture_action_fixture = (
        "E2E_LOAD_SCALE_FIXTURE_ACTIONS = {'scale-in', 'noop'}\n"
    )
    e2e_fixture_action_pins = "scale-in\nnoop\n"
    expect_failure(
        "E2E fixture action catalogue readiness negative",
        lambda: assert_e2e_fixture_action_catalog_pinned(
            release_evidence=e2e_fixture_action_fixture,
            release_criteria=e2e_fixture_action_pins,
            parity=e2e_fixture_action_pins,
            production_readiness=e2e_fixture_action_pins.replace("noop\n", ""),
            check_docs=False,
        ),
        "production readiness missing E2E fixture action noop",
    )
    duplicate_e2e_fixture_action_fixture = (
        "E2E_LOAD_SCALE_FIXTURE_ACTIONS = ['scale-in', 'scale-in']\n"
    )
    expect_failure(
        "E2E fixture action catalogue duplicate negative",
        lambda: assert_e2e_fixture_action_catalog_pinned(
            release_evidence=duplicate_e2e_fixture_action_fixture,
            release_criteria="scale-in\n",
            parity="scale-in\n",
            production_readiness="scale-in\n",
            check_docs=False,
        ),
        "E2E_LOAD_SCALE_FIXTURE_ACTIONS repeats actions: scale-in",
    )
    expect_failure(
        "release-evidence self-test assertion pin negative",
        lambda: assert_release_evidence_selftest_assertions_pinned(
            'raise AssertionError("untracked release evidence assertion")\n',
            "",
        ),
        "untracked release evidence assertion",
    )
    expect_failure(
        "protocol static audit contract negative",
        lambda: assert_protocol_static_audit_contract(
            "def audit():\n"
            "    pass\n"
        ),
        "tests/protocol_static_audit.py strict-codegen contract",
    )
    expect_failure(
        "observability self-test assertion pin negative",
        lambda: assert_observability_selftest_assertions_pinned(
            'def self_test():\n'
            '    raise AssertionError("untracked observability assertion")\n',
            "",
        ),
        "untracked observability assertion",
    )
    expect_failure(
        "S3 process-crash self-test assertion pin negative",
        lambda: assert_s3_process_crash_selftest_assertions_pinned(
            'def self_test():\n'
            '    raise AssertionError("untracked S3 process-crash assertion")\n',
            "\n".join(S3_PROCESS_CRASH_SELFTEST_ASSERTIONS),
        ),
        "untracked S3 process-crash assertion",
    )
    expect_failure(
        "comparative benchmark self-test assertion pin negative",
        lambda: assert_benchmark_compare_selftest_assertions_pinned(
            'def self_test():\n'
            '    raise AssertionError("untracked comparative benchmark assertion")\n',
            "\n".join(BENCHMARK_COMPARE_SELFTEST_ASSERTIONS),
        ),
        "untracked comparative benchmark assertion",
    )
    expect_failure(
        "E2E self-test assertion pin negative",
        lambda: assert_e2e_selftest_assertions_pinned(
            'def self_test():\n'
            '    raise AssertionError("untracked E2E assertion")\n',
            "\n".join(E2E_SELFTEST_ASSERTIONS),
        ),
        "untracked E2E assertion",
    )
    expect_failure(
        "chaos self-test error pin negative",
        lambda: assert_chaos_selftest_errors_pinned(
            'def self_test():\n'
            '    raise TestError("untracked chaos error")\n',
            "\n".join(CHAOS_SELFTEST_ERRORS),
        ),
        "untracked chaos error",
    )
    expect_failure(
        "KRaft failover self-test error pin negative",
        lambda: assert_kraft_failover_selftest_errors_pinned(
            'def self_test():\n'
            '    raise TestError("untracked KRaft failover error")\n',
            "\n".join(KRAFT_FAILOVER_SELFTEST_ERRORS),
        ),
        "untracked KRaft failover error",
    )
    expect_failure(
        "client matrix self-test error pin negative",
        lambda: assert_client_matrix_selftest_errors_pinned(
            'def self_test():\n'
            '    raise MatrixError("untracked client matrix error")\n',
            "\n".join(CLIENT_MATRIX_SELFTEST_ERRORS),
        ),
        "untracked client matrix error",
    )
    expect_failure(
        "S3 provider matrix self-test error pin negative",
        lambda: assert_s3_provider_matrix_selftest_errors_pinned(
            'def self_test():\n'
            '    raise MatrixError("untracked S3 provider matrix error")\n',
            "\n".join(S3_PROVIDER_MATRIX_SELFTEST_ERRORS),
        ),
        "untracked S3 provider matrix error",
    )
    expect_failure(
        "formatted self-test error pin negative",
        lambda: assert_python_selftest_formatted_errors_pinned(
            source_texts={
                "tests/example.py": (
                    "def self_test():\n"
                    "    raise AssertionError(f\"untracked formatted {value} error\")\n"
                ),
            },
            build_static_audit="",
            specs=(("tests/example.py", ("AssertionError",), ()),),
        ),
        "untracked formatted ",
    )
    expect_failure(
        "dynamic self-test error pin negative",
        lambda: assert_python_selftest_dynamic_errors_pinned(
            source_texts={
                "tests/release_evidence_test.py": (
                    "def self_test():\n"
                    "    failures = ['boom']\n"
                    "    raise AssertionError('untracked dynamic error: ' + '; '.join(failures))\n"
                ),
                "tests/s3_provider_matrix_test.py": "def self_test():\n    pass\n",
            },
            build_static_audit="\n".join(PYTHON_SELFTEST_DYNAMIC_ERROR_FRAGMENTS),
        ),
        "untracked dynamic error: ",
    )
    expect_failure(
        "self-test raise shape pin negative",
        lambda: assert_python_selftest_raise_shapes_pinned(
            source_texts={
                "tests/example.py": (
                    "def self_test():\n"
                    "    raise AssertionError(make_message())\n"
                ),
            },
            specs=(
                (
                    "tests/example.py",
                    "self_test",
                    ("AssertionError",),
                    ("Constant",),
                ),
            ),
            self_test_gates=(("test-example", "tests/example.py"),),
            no_raise_paths=(),
        ),
        "unexpected self-test raise message shape Call",
    )
    expect_failure(
        "self-test raise shape scope negative",
        lambda: assert_python_selftest_raise_shapes_pinned(
            source_texts={
                "tests/example.py": "def self_test():\n    pass\n",
            },
            specs=(),
            self_test_gates=(("test-example", "tests/example.py"),),
            no_raise_paths=(),
        ),
        "missing raise-shape coverage for tests/example.py",
    )
    expect_failure(
        "missing AutoMQ parity evidence marker documentation negative",
        lambda: assert_automq_parity_release_evidence_contract(
            "per-required coverage markers\n"
            "comma-separated coverage\n"
            "parse to at least one value\n"
            "worktree cleanliness cannot be determined\n"
            "token-aware command validation\n"
            "same shell command segment\n"
            "only documented compose config\n"
            "may use multi-segment release gate chains\n"
            "quoted/echoed command text cannot satisfy\n"
            "release gate commands\n"
            "must be direct invocations\n"
            "pipes, backgrounding, redirection, subshell\n"
            "command substitution are rejected\n"
            "structured objects with non-empty\n"
            "`surface`, `status`, and\n"
            "bare strings or placeholders cannot satisfy release accounting\n"
            "Optional unsupported-surface accounting fields\n"
            "non-empty strings or lists of non-empty strings\n"
            "placeholder optional accounting fields\n"
            "Each required surface must be covered by a distinct object\n"
            "catch-all entries\n"
            "multiple known surfaces\n"
            "Each surface status must explicitly mark\n"
            "vague completion-style statuses are rejected\n"
            "sub-profiles must also be listed within\n"
            "`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`\n"
            "within `ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES`\n"
            "selector/provenance variables\n"
            "S3 provider matrix command must include\n"
            "`ZMQ_S3_PROVIDER_REQUIRED_PROFILES`\n"
            "`ZMQ_S3_PROVIDER_PROFILES`\n"
            "`ZMQ_S3_PROVIDER_REQUIRED_{OUTAGE,PROCESS_CRASH,LIST_PAGINATION,MULTIPART_EDGE,MULTIPART_FAULT}_PROFILES`\n"
            "`ZMQ_CHAOS_NETWORK_MATRIX`\n"
            "`ZMQ_KRAFT_NETWORK_MATRIX`\n"
            "`ZMQ_E2E_CHAOS_MATRIX`\n"
            "`ZMQ_E2E_LOAD_SCALE_MATRIX`\n"
            "`ZMQ_S3_PROVIDER_PROFILES`\n"
            "`ZMQ_CLIENT_MATRIX_PROFILES`\n"
            "fixture-backed inference\n"
            "environment-token collisions\n"
            "hook command\n"
            "parseable hook command\n"
            "documented global\n"
            "coverage selector assignments for `ZMQ_CHAOS_REQUIRED_SCENARIOS`\n"
            "`ZMQ_CHAOS_REQUIRED_NETWORK_PHASES`, and `ZMQ_CHAOS_NETWORK_MATRIX`\n"
            "KRaft failover command must include\n"
            "`ZMQ_KRAFT_REQUIRED_NETWORK_PHASES` and\n"
            "`ZMQ_KRAFT_NETWORK_MATRIX`\n"
            "KRaft failover self-test error catalogue\n"
            "protocol fixture parsers\n"
            "record-batch fixture invariants\n"
            "E2E load/scale fixture exception\n"
            "Docker E2E command must include\n"
            "`ZMQ_E2E_REQUIRED_CHAOS_PHASES`, `ZMQ_E2E_CHAOS_MATRIX`, and\n"
            "`ZMQ_E2E_REQUIRED_LOAD_SCALE_PHASES` matching the manifest environment\n"
            "`ZMQ_E2E_LOAD_SCALE_USE_FIXTURE` whenever fixture\n"
            "truthy release-evidence provenance\n"
            "`RUN_LIVE_OUTAGE`\n"
            "`RUN_PROCESS_CRASH`\n"
            "`REQUIRE_LIST_PAGINATION`\n"
            "`REQUIRE_MULTIPART_EDGE`\n"
            "`RUN_MULTIPART_FAULT`\n"
            "strictly parses those profile/global enable\n"
            "`SKIP_ENSURE_BUCKET`\n"
            "`SKIP_MINIO_HEALTH`\n"
            "client-profile markers\n"
            "profile marker line shape\n"
            "`passed for <tools> against <bootstrap> version=<version> source=command`\n"
            "profile-selected tools\n"
            "profile-scoped tool probe\n"
            "before the corresponding profile pass marker\n"
            "same profile block\n"
            "matching passed-for tools/bootstrap/version/source line\n"
            "before the final client matrix summary\n"
            "post-summary profile blocks cannot satisfy\n"
            "ok: client matrix passed for <profiles> profile(s) source=command\n"
            "exactly matching `ZMQ_CLIENT_MATRIX_PROFILES`\n"
            "external client matrix command must include\n"
            "required profile, selected profile, required tool, required semantic\n"
            "profile settings\n"
            "selected the tools and semantic suites\n"
            "exact version labels\n"
            "pinned Go module versions\n"
            "secured-client protocol/SASL/TLS settings\n"
            "positive/negative OAuth fixtures\n"
            "`auto` tool selection\n"
            "floating `@latest`\n"
            "missing security protocol provenance\n"
            "OAUTHBEARER positive or negative fixture variables\n"
            "profile semantic/tool mismatches\n"
            "rebalance, transactional, or\n"
            "live matrix does not\n"
            "OAuth fixture validation now mirrors\n"
            "raw JWTs, Java/Kafka CLI JAAS configs\n"
            "future-valid negative fixtures\n"
            "`kcat`, `kafka-cli`, `kafka-python`,\n"
            "`basic`, `admin`, `groups`, `rebalance`,\n"
            "probe markers using `ok: <client> probes (<semantics>) source=command`\n"
            "ok: kafka-python probes\n"
            "ok: confluent-kafka probes\n"
            "client matrix self-test error catalogue\n"
            "required client profile/tool/semantic coverage\n"
            "security and OAuth fixture validation\n"
            "exact semantic tokens inside client probe marker\n"
            "for every semantic named by\n"
            "recognized profile-selected\n"
            "required client-tool probe markers\n"
            "tools whose profile did not enable\n"
            "ok: chaos sigkill-restart killed=true restarted=true recovered_payloads=2\n"
            "second_offset=<positive> source=command\n"
            "ok: chaos slow-partial-client partial_frame=true truncated_frame=true survived=true source=command\n"
            "ok: chaos clock-skewed-records future_timestamp=true fetched=true serving=true source=command\n"
            "ok: chaos s3-outage\n"
            "base_offset_negative=true serving=true source=command\n"
            "ok: chaos live-s3-outage provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command\n"
            "ok: chaos live-s3-outage down=true healed=true fail_closed=true recovered=true source=command\n"
            "before the broker chaos harness summary line\n"
            "scenario detail markers must be unique per required scenario\n"
            "`ZMQ_CHAOS_S3_*`\n"
            "broker chaos command must include\n"
            "non-sensitive live-S3 outage provider assignments\n"
            "ok: chaos network-partition phase ... down=true observed=<failed|survived> healed=true recovered=true expect=<fail|survive> source=command\n"
            "ok: chaos network-partition source=command\n"
            "chaos network-partition scenario summary must appear exactly once\n"
            "ok: chaos harness passed for <scenarios> source=command\n"
            "`ZMQ_CHAOS_REQUIRED_SCENARIOS` entry\n"
            "ok: client matrix profile\n"
            "ok: S3 provider live-suite profile ... command_started=true completed=true source=command\n"
            "ok: S3 provider profile <profile> endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false> source=command\n"
            "before the final S3 provider matrix summary\n"
            "post-summary provider blocks cannot satisfy\n"
            "`ZMQ_S3_<PROFILE>_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`\n"
            "`ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`\n"
            "S3 provider matrix command must include\n"
            "`ZMQ_S3_PROVIDER_REQUIRED_PROFILES` and `ZMQ_S3_PROVIDER_PROFILES`\n"
            "S3 provider matrix self-test error catalogue\n"
            "provider profile fallback validation\n"
            "outage, process-crash, and multipart-fault evidence validation\n"
            "profile/global endpoint\n"
            "effective scheme/region/path-style settings\n"
            "`SCHEME` must parse as `http` or `https`\n"
            "`PATH_STYLE` must parse as\n"
            "ok: S3 provider outage detail profile ... endpoint=<endpoint>:<port>\n"
            "fail_closed=true recovered=true source=command\n"
            "ok: S3 provider outage profile ... down=true healed=true fail_closed=true recovered=true source=command\n"
            "underlying process-crash output\n"
            "bucket=<bucket>` matching the selected provider bucket\n"
            "ok: S3 provider process-crash detail profile ... bucket=<bucket>\n"
            "recovered_payloads=2 source=command\n"
            "ok: S3 provider process-crash profile ... killed_broker=true fresh_data_dir=true recovered_payloads=2 source=command\n"
            "ok: S3 provider list-pagination profile ... required=true completed=true source=command\n"
            "ok: S3 provider multipart-edge profile ... required=true completed=true source=command\n"
            "ok: S3 provider multipart-fault profile ... command_started=true completed=true injected=true recovered=true source=command\n"
            "command-owned marker\n"
            "source=command\n"
            "ok: KRaft network partition phase ... down=true observed=<failed|survived> healed=true healed_leader=<id> healed_fetch=true expect=<fail|survive> source=command\n"
            "ok: E2E chaos phase\n"
            "down=true observed=<failed|survived> healed=true recovered=true\n"
            "expect=<fail|survive> source=command\n"
            "restore_source=<hook|fixture> source=command\n"
            "load_records=<count>\n"
            "ok: E2E chaos passed for <phases> phase(s) source=command\n"
            "ok: E2E load/scale passed for <phases> phase(s) source=command\n"
            "Results: <passed>/<total> passed, 0 failed\n"
            "Docker E2E final results line must appear exactly once\n"
            "Docker E2E output line markers must appear exactly once\n"
            "after the required E2E phase summaries\n"
            "earlier detached results output\n"
            "MinIO `8/8 tests passed` evidence\n"
            "MinIO `8/8 tests passed` marker must appear exactly once\n"
            "`ok: KRaft controller failover harness passed ... source=command` line\n"
            "KRaft failover summary must appear exactly once\n"
            "old_leader_rejoined=true\n"
            "old_leader_fresh_rejoin=true\n"
            "automq_old_leader_fresh_rejoin=true\n"
            "automq_stream_id=\n"
            "automq_deleted_stream_id=\n"
            "automq_stream_set_object_id=\n"
            "automq_node_id=\n"
            "automq_zone_router_epoch=\n"
            "old_leader=\n"
            "new_leader=\n"
            "restarted_controller=\n"
            "epoch=\n"
            "automq_old_leader=\n"
            "automq_new_leader=\n"
            "must parse as non-placeholder non-negative integers\n"
            "allocate_producer_ids_checked=true\n"
            "allocate_producer_ids_follower_rejection_checked=true\n"
            "describe_quorum_v2_checked=true\n"
            "fetch_snapshot_v1_checked=true\n"
            "all_controller_fetch_snapshot_v1_checked=true\n"
            "controller_api_versions_checked=true\n"
            "all_controller_api_versions_checked=true\n"
            "controller_unsupported_checked=true\n"
            "all_controller_unsupported_checked=true\n"
            "controller_unsupported_cases=[<api_key>:<version>,...]\n"
            "dynamic_raft_voter_negative_checked=true\n"
            "dynamic_raft_voter_follower_rejection_checked=true\n"
            "all_controller_describe_quorum_v2_checked=true\n"
            "broker_lifecycle_negative_checked=true\n"
            "broker_lifecycle_follower_rejection_checked=true\n"
            "controller_registration_negative_checked=true\n"
            "controller_registration_follower_rejection_checked=true\n"
            "broker_registration_follower_rejection_checked=true\n"
            "broker_non_broker_api_rejection_checked=true\n"
            "broker_non_broker_api_rejection_cases=[<api_key>:<version>,...]\n"
            "committed_offset=\n"
            "transactions_checked=5\n"
            "must parse as exactly `5`\n"
            "transaction_introspection_checked=true\n"
            "transaction_abort_checked=true\n"
            "txn_offset_commit_checked=true\n"
            "offset_fetch_v8_grouped_checked=true\n"
            "log_position_apis_checked=true\n"
            "delete_records_checked=true\n"
            "delete_topics_checked=true\n"
            "create_topics_checked=true\n"
            "create_partitions_checked=true\n"
            "client_quotas_checked=true\n"
            "scram_credentials_checked=true\n"
            "client_telemetry_checked=true\n"
            "delegation_tokens_checked=true\n"
            "finalized_features_checked=true\n"
            "acl_admin_checked=true\n"
            "config_admin_checked=true\n"
            "describe_topic_partitions_checked=true\n"
            "describe_configs_checked=true\n"
            "describe_log_dirs_checked=true\n"
            "alter_replica_log_dirs_checked=true\n"
            "assign_replicas_to_dirs_checked=true\n"
            "elect_leaders_checked=true\n"
            "describe_cluster_checked=true\n"
            "idempotent_producer_fencing=true\n"
            "describe_producers_checked=true\n"
            "delete_groups_checked=true\n"
            "classic_group_heartbeats=true\n"
            "group_describe_checked=true\n"
            "consumer_group_describe_checked=true\n"
            "list_groups_checked=true\n"
            "find_coordinator_checked=true\n"
            "share_group_heartbeat_checked=true\n"
            "share_group_describe_checked=true\n"
            "consumer_group_heartbeat_checked=true\n"
            "share_fetch_session_checked=true\n"
            "share_acknowledge_checked=true\n"
            "share_state_apis_checked=true\n"
            "kip848_describe_checked=true\n"
            "kip848_rejoin_checked=true\n"
            "kip848_rack_checked=true\n"
            "kip848_owned_assignment_checked=true\n"
            "kip848_subscription_update_checked=true\n"
            "kip848_negative_join_checked=true\n"
            "kip848_static_rejoin_checked=true\n"
            "offset_commit_v9_member_checked=true\n"
            "offset_fetch_v9_member_checked=true\n"
            "reassignment_topic=<topic>\n"
            "reassignment_old_owner_rejected=true\n"
            "reassignment_target_fetch_verified=true\n"
            "ZMQ (Zig)\n"
            "Apache Kafka\n"
            "AutoMQ (Java)\n"
            "Local and live-S3 benchmark markers are also line-aware\n"
            "ok: local benchmark gate source=command\n"
            "ok: live-S3 benchmark gate source=command\n"
            "`S3 WAL request volume`\n"
            "`Live S3 provider`\n"
            "local benchmark summary must appear exactly once\n"
            "live-S3 benchmark summary must appear exactly once\n"
            "before the `=== Benchmarks complete ===` marker\n"
            "Live S3 provider endpoint=<endpoint>:<port> bucket=<bucket> scheme=<scheme> region=<region> path_style=<true|false>\n"
            "command/env-selected `ZMQ_S3_{ENDPOINT,PORT,BUCKET,SCHEME,REGION,PATH_STYLE}`\n"
            "`SCHEME` parsing as `http` or `https`\n"
            "`PATH_STYLE` parsing as `true` or `false`\n"
            "Live S3 put <MiB/s> MiB/s p99=<ms> ms objects=<objects>\n"
            "Live S3 get <MiB/s> MiB/s p99=<ms> ms requests/MiB=<value>\n"
            "before the benchmark completion marker\n"
            "Comparative benchmark table markers are also line-aware\n"
            "section-scoped\n"
            "appear on the `COMPARISON:` line\n"
            "before the gate\n"
            "throughput (`tput`) row\n"
            "detached post-gate line\n"
            "concrete `tput`, `p50`, and `p99`\n"
            "before the gate\n"
            "positive finite target\n"
            "bounded `COMPARATIVE BENCHMARK GATE`\n"
            "inside the bounded `COMPARATIVE BENCHMARK GATE`\n"
            "gate section result\n"
            "`trend thresholds:`\n"
        ),
        "marker_payloads=hook-owned",
    )


def main():
    if len(sys.argv) > 1 and sys.argv[1] != "--self-test":
        raise SystemExit(f"unknown argument: {sys.argv[1]}")
    self_test()
    print("ok: build static audit")
    return 0


if __name__ == "__main__":
    sys.exit(main())

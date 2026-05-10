#!/usr/bin/env python3
"""
Static protocol catalog audit.

The Zig default suite already audits ApiVersions catalog drift, generated schema
coverage, and broker/controller handler switch coverage. This Python audit
mirrors the source-level parts that can run without a Zig 0.16 toolchain.

Run:
    python3 tests/protocol_static_audit.py --self-test
"""

import importlib.util
import os
import re
import subprocess
import sys
import tempfile


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_SUPPORT = os.path.join(PROJECT_DIR, "src/protocol/api_support.zig")
GENERATED_INDEX = os.path.join(PROJECT_DIR, "src/protocol/generated_index.zig")
GENERATED_ROUNDTRIP = os.path.join(PROJECT_DIR, "src/protocol/generated_roundtrip.zig")
BROKER_HANDLER = os.path.join(PROJECT_DIR, "src/broker/handler.zig")
CONTROLLER = os.path.join(PROJECT_DIR, "src/controller/controller.zig")
SCHEMA_DIR = os.path.join(PROJECT_DIR, "src/protocol/schemas")
CODEGEN = os.path.join(PROJECT_DIR, "src/protocol/codegen/codegen.py")
CODEGEN_V2 = os.path.join(PROJECT_DIR, "src/protocol/codegen/codegen_v2.py")


ENTRY_RE = re.compile(
    r"""\.\{\s*
        \.key\s*=\s*(?P<key>-?\d+),\s*
        \.name\s*=\s*"(?P<name>[^"]+)",\s*
        (?:(?:\.metric\s*=\s*"(?P<metric>[^"]+)",\s*)?)
        \.min\s*=\s*(?P<min>-?\d+),\s*
        \.max\s*=\s*(?P<max>-?\d+)\s*
    \}""",
    re.VERBOSE,
)


def read(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def load_python_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load Python module {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_codegen_strict_json_self_test():
    codegen_v2 = None
    for label, path in (
        ("protocol_codegen_v1", CODEGEN),
        ("protocol_codegen_v2", CODEGEN_V2),
    ):
        module = load_python_module(path, label)
        if label == "protocol_codegen_v2":
            codegen_v2 = module
        parsed = module.parse_schema_json('{"name": "Ok", "fields": []} // comment\n')
        if parsed.get("name") != "Ok":
            raise AssertionError(f"{label} did not parse a valid commented schema")
        try:
            module.parse_schema_json('{"name": NaN}')
        except ValueError as exc:
            message = str(exc)
            if "strict JSON" not in message or "non-standard JSON constant" not in message:
                raise AssertionError(f"{label} strict JSON failure was not explanatory: {exc}") from exc
        else:
            raise AssertionError(f"{label} accepted a non-standard JSON constant")
        try:
            module.parse_schema_json('{"name": "Ok", "name": "Shadowed"}')
        except ValueError as exc:
            message = str(exc)
            if "strict JSON" not in message or "duplicate JSON object key" not in message:
                raise AssertionError(f"{label} duplicate-key failure was not explanatory: {exc}") from exc
        else:
            raise AssertionError(f"{label} accepted a duplicate JSON object key")

    for filename in sorted(os.listdir(SCHEMA_DIR)):
        if not filename.endswith(".json"):
            continue
        path = os.path.join(SCHEMA_DIR, filename)
        try:
            codegen_v2.parse_schema_json(read(path))
        except ValueError as exc:
            raise AssertionError(f"protocol schema {filename} must be strict JSON: {exc}") from exc


def assert_codegen_failure_exit_self_test():
    for label, script in (
        ("protocol_codegen_v1", CODEGEN),
        ("protocol_codegen_v2", CODEGEN_V2),
    ):
        with tempfile.TemporaryDirectory() as schema_dir, tempfile.TemporaryDirectory() as output_dir:
            bad_schema = os.path.join(schema_dir, "BadRequest.json")
            with open(bad_schema, "w", encoding="utf-8") as f:
                f.write('{"name": "BadRequest", "name": "ShadowedRequest"}')
            result = subprocess.run(
                [sys.executable, script, schema_dir, output_dir],
                cwd=PROJECT_DIR,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        if result.returncode == 0:
            raise AssertionError(f"{label} exited successfully after a schema parse error")
        if "duplicate JSON object key" not in result.stderr:
            raise AssertionError(f"{label} did not report the schema parse error: {result.stderr!r}")


def array_block(source, name):
    pattern = re.compile(
        rf"pub const {re.escape(name)} = \[_\][^{{]*\{{(?P<body>.*?)\}};",
        re.DOTALL,
    )
    match = pattern.search(source)
    if not match:
        raise AssertionError(f"missing array {name}")
    return match.group("body")


def parse_api_entries(source, name):
    body = array_block(source, name)
    entries = []
    for match in ENTRY_RE.finditer(body):
        entries.append(
            {
                "key": int(match.group("key")),
                "name": match.group("name"),
                "metric": match.group("metric"),
                "min": int(match.group("min")),
                "max": int(match.group("max")),
            }
        )
    if not entries:
        raise AssertionError(f"array {name} had no parsed API entries")
    return entries


def parse_i16_array(source, name):
    body = array_block(source, name)
    return [int(value) for value in re.findall(r"(?m)^\s*(-?\d+),", body)]


def assert_sorted_unique(keys, label):
    if keys != sorted(keys):
        raise AssertionError(f"{label} is not sorted")
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    if duplicates:
        raise AssertionError(f"{label} contains duplicate keys: {duplicates}")


def assert_entry_table(entries, label, require_metric=False):
    keys = [entry["key"] for entry in entries]
    assert_sorted_unique(keys, label)
    for entry in entries:
        if entry["min"] > entry["max"]:
            raise AssertionError(f"{label} has invalid version range: {entry}")
        if require_metric and not entry["metric"]:
            raise AssertionError(f"{label} missing metric for key {entry['key']}")


def braced_body_after(source, marker):
    start = source.find(marker)
    if start < 0:
        raise AssertionError(f"missing body marker {marker!r}")
    open_brace = source.find("{", start)
    if open_brace < 0:
        raise AssertionError(f"missing body for {marker!r}")

    depth = 0
    for index in range(open_brace, len(source)):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[open_brace + 1 : index]
    raise AssertionError(f"unterminated body for {marker!r}")


def switch_body(source, marker):
    return braced_body_after(source, marker)


def switch_case_keys(body):
    return [int(value) for value in re.findall(r"(?m)^\s*(-?\d+)\s*=>", body)]


def count_generated_exports(generated_index, suffix):
    return len(re.findall(rf"{re.escape(suffix)} = @import\(\"generated/", generated_index))


def generated_request_response_modules(generated_index):
    return set(
        re.findall(
            r"pub const ([a-z0-9_]+_(?:request|response)) = @import",
            generated_index,
        )
    )


def generated_golden_modules(generated_roundtrip):
    body = braced_body_after(
        generated_roundtrip,
        'test "generated non-default golden fixtures cover legacy and flexible wire encodings"',
    )
    return set(re.findall(r"\bgenerated\.([a-z0-9_]+_(?:request|response))\b", body))


def audit():
    api_support = read(API_SUPPORT)
    generated_index = read(GENERATED_INDEX)
    generated_roundtrip = read(GENERATED_ROUNDTRIP)
    broker_handler = read(BROKER_HANDLER)
    controller = read(CONTROLLER)

    broker_supported = parse_api_entries(api_support, "broker_supported_apis")
    controller_supported = parse_api_entries(api_support, "controller_supported_apis")
    generated_requests = parse_api_entries(api_support, "generated_request_apis")

    broker_handler_keys = parse_i16_array(api_support, "broker_handler_api_keys")
    controller_handler_keys = parse_i16_array(api_support, "controller_handler_api_keys")
    non_advertised_handler_keys = parse_i16_array(api_support, "non_advertised_handler_api_keys")
    fail_closed_handler_keys = parse_i16_array(api_support, "fail_closed_generated_handler_api_keys")
    generated_non_broker_keys = parse_i16_array(api_support, "generated_non_broker_request_api_keys")
    legacy_inter_broker_keys = parse_i16_array(api_support, "legacy_inter_broker_request_api_keys")

    assert_entry_table(broker_supported, "broker_supported_apis", require_metric=True)
    assert_entry_table(controller_supported, "controller_supported_apis")
    assert_entry_table(generated_requests, "generated_request_apis")
    assert_sorted_unique(broker_handler_keys, "broker_handler_api_keys")
    assert_sorted_unique(controller_handler_keys, "controller_handler_api_keys")
    assert_sorted_unique(non_advertised_handler_keys, "non_advertised_handler_api_keys")
    assert_sorted_unique(fail_closed_handler_keys, "fail_closed_generated_handler_api_keys")
    assert_sorted_unique(generated_non_broker_keys, "generated_non_broker_request_api_keys")
    assert_sorted_unique(legacy_inter_broker_keys, "legacy_inter_broker_request_api_keys")
    if legacy_inter_broker_keys != [4, 5, 6, 7]:
        raise AssertionError(
            "legacy inter-broker API key catalogue drifted: "
            + ", ".join(str(key) for key in legacy_inter_broker_keys)
        )

    generated_by_key = {entry["key"]: entry for entry in generated_requests}
    broker_supported_keys = {entry["key"] for entry in broker_supported}
    controller_supported_keys = {entry["key"] for entry in controller_supported}
    broker_handler_key_set = set(broker_handler_keys)
    controller_handler_key_set = set(controller_handler_keys)

    request_exports = count_generated_exports(generated_index, "_request")
    response_exports = count_generated_exports(generated_index, "_response")
    if request_exports != len(generated_requests):
        raise AssertionError(
            f"generated request exports={request_exports} catalog={len(generated_requests)}"
        )
    if response_exports != len(generated_requests):
        raise AssertionError(
            f"generated response exports={response_exports} catalog={len(generated_requests)}"
        )

    generated_modules = generated_request_response_modules(generated_index)
    golden_modules = generated_golden_modules(generated_roundtrip)
    missing_golden_modules = sorted(generated_modules - golden_modules)
    extra_golden_modules = sorted(golden_modules - generated_modules)
    if missing_golden_modules:
        raise AssertionError(
            "generated modules missing non-default golden fixtures: "
            + ", ".join(missing_golden_modules)
        )
    if extra_golden_modules:
        raise AssertionError(
            "non-default golden fixtures reference unknown generated modules: "
            + ", ".join(extra_golden_modules)
        )

    for entry in broker_supported:
        schema = generated_by_key.get(entry["key"])
        if schema is None:
            raise AssertionError(f"broker API missing generated schema: {entry}")
        if entry["min"] < schema["min"] or entry["max"] > schema["max"]:
            raise AssertionError(f"broker API advertises beyond generated schema: {entry}")
        if entry["key"] not in broker_handler_key_set:
            raise AssertionError(f"broker API missing handler table coverage: {entry}")

    for entry in controller_supported:
        schema = generated_by_key.get(entry["key"])
        if schema is None:
            raise AssertionError(f"controller API missing generated schema: {entry}")
        if entry["min"] < schema["min"] or entry["max"] > schema["max"]:
            raise AssertionError(f"controller API advertises beyond generated schema: {entry}")
        if entry["key"] not in controller_handler_key_set:
            raise AssertionError(f"controller API missing handler table coverage: {entry}")

    broker_switch = switch_body(broker_handler, "const result = switch (api_key)")
    broker_switch_keys = switch_case_keys(broker_switch)
    if set(broker_switch_keys) != broker_handler_key_set:
        raise AssertionError(
            "broker handleRequest switch/table drift: "
            f"switch={sorted(broker_switch_keys)} table={broker_handler_keys}"
        )

    controller_switch = switch_body(controller, "return switch (api_key)")
    controller_switch_keys = switch_case_keys(controller_switch)
    if set(controller_switch_keys) != controller_handler_key_set:
        raise AssertionError(
            "controller handleRequest switch/table drift: "
            f"switch={sorted(controller_switch_keys)} table={controller_handler_keys}"
        )

    for key in fail_closed_handler_keys:
        if key not in broker_handler_key_set:
            raise AssertionError(f"fail-closed handler key lacks broker switch case: {key}")
        if key in broker_supported_keys:
            raise AssertionError(f"fail-closed handler key is advertised: {key}")
        if key not in generated_by_key:
            raise AssertionError(f"fail-closed handler key lacks generated schema: {key}")

    for key in generated_non_broker_keys:
        if key in broker_supported_keys:
            raise AssertionError(f"non-broker generated key is broker advertised: {key}")
        if key in broker_handler_key_set:
            raise AssertionError(f"non-broker generated key is broker dispatched: {key}")
        if key not in generated_by_key:
            raise AssertionError(f"non-broker generated key lacks generated schema: {key}")

    for key in legacy_inter_broker_keys:
        if key not in generated_by_key:
            raise AssertionError(f"legacy inter-broker key missing generated schema: {key}")
        if key in broker_supported_keys or key in broker_handler_key_set:
            raise AssertionError(f"legacy inter-broker key is broker advertised/dispatched: {key}")
        if key in controller_supported_keys or key in controller_handler_key_set:
            raise AssertionError(f"legacy inter-broker key is controller advertised/dispatched: {key}")

    if 71 in controller_supported_keys or 72 in controller_supported_keys:
        raise AssertionError("controller advertised telemetry keys 71/72 as KRaft APIs")
    for key in (70, 80, 81, 82):
        if key not in controller_supported_keys:
            raise AssertionError(f"controller missing generated dynamic-voter key: {key}")


def main():
    self_test = len(sys.argv) > 1 and sys.argv[1] == "--self-test"
    if len(sys.argv) > 1 and not self_test:
        raise SystemExit(f"unknown argument: {sys.argv[1]}")
    if self_test:
        assert_codegen_strict_json_self_test()
        assert_codegen_failure_exit_self_test()
    audit()
    print("ok: protocol static audit")
    return 0


if __name__ == "__main__":
    sys.exit(main())

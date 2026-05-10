#!/usr/bin/env python3
"""
Static observability artifact audit.

The Zig production-readiness suite validates checked-in Grafana and Prometheus
alert artifacts against the registered broker metric corpus. This Python audit
mirrors the source-level checks that can run without a Zig 0.16 toolchain.

Run:
    python3 tests/observability_static_audit.py --self-test
"""

import json
import os
import re
import sys


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
METRICS_ZIG = os.path.join(PROJECT_DIR, "src/broker/metrics.zig")
API_SUPPORT_ZIG = os.path.join(PROJECT_DIR, "src/protocol/api_support.zig")
READINESS_ZIG = os.path.join(PROJECT_DIR, "tests/production_readiness_test.zig")
DASHBOARD_JSON = os.path.join(
    PROJECT_DIR, "docs/observability/zmq-grafana-dashboard.json"
)
ALERTS_YAML = os.path.join(
    PROJECT_DIR, "docs/observability/zmq-prometheus-alerts.yaml"
)
SRC_DIR = os.path.join(PROJECT_DIR, "src")

METRIC_REGISTER_FUNCTIONS = (
    "registerBrokerMetrics",
    "registerS3Metrics",
    "registerCompactionMetrics",
    "registerCacheMetrics",
    "registerRaftMetrics",
)

CRITICAL_ALERTS = {
    "ZMQNoActiveRaftLeader",
    "ZMQNoActiveController",
    "ZMQNoActiveBroker",
    "ZMQBrokerNotRunning",
    "ZMQOfflinePartitions",
    "ZMQUnderMinIsrPartitions",
    "ZMQOfflineLogDirectories",
    "ZMQControllerOfflinePartitions",
    "ZMQUncleanLeaderElections",
}

REQUIRED_ALERT_GROUPS = (
    "zmq-broker",
    "zmq-controller",
    "zmq-storage",
    "zmq-client-telemetry",
)

DASHBOARD_GRID_COLUMNS = 24
DASHBOARD_TARGET_FIELDS = frozenset(("expr", "legendFormat"))

REGISTER_RE = re.compile(
    r"registry\.register(?P<labeled>Labeled)?(?P<kind>Counter|Gauge|Histogram)"
    r"\(\s*\"(?P<name>[^\"]+)\"",
    re.DOTALL,
)


def read(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def reject_nonstandard_json_constant(value):
    raise ValueError(f"non-standard JSON constant {value!r} is not allowed in strict JSON")


def reject_duplicate_json_object_keys(pairs):
    parsed = {}
    for key, value in pairs:
        if key in parsed:
            raise ValueError(f"duplicate JSON object key {key!r} is not allowed in strict JSON")
        parsed[key] = value
    return parsed


def parse_strict_json(text, label):
    try:
        return json.loads(
            text,
            parse_constant=reject_nonstandard_json_constant,
            object_pairs_hook=reject_duplicate_json_object_keys,
        )
    except ValueError as exc:
        raise AssertionError(f"{label} must be strict JSON: {exc}") from exc


def function_body(source, name):
    match = re.search(rf"\bpub\s+fn\s+{re.escape(name)}\b", source)
    if not match:
        raise AssertionError(f"missing function {name}")

    open_brace = source.find("{", match.end())
    if open_brace < 0:
        raise AssertionError(f"missing function body for {name}")

    depth = 0
    for index in range(open_brace, len(source)):
        char = source[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[open_brace + 1 : index]
    raise AssertionError(f"unterminated function body for {name}")


def zig_string_array(source, name):
    pattern = re.compile(
        rf"\bconst\s+{re.escape(name)}\s*=\s*\[_\]\[\]const u8\s*\{{"
        r"(?P<body>.*?)\n\s*\};",
        re.DOTALL,
    )
    match = pattern.search(source)
    if not match:
        raise AssertionError(f"missing string array {name}")
    return re.findall(r"\"((?:\\.|[^\"\\])*)\"", match.group("body"))


def api_metric_names(api_support):
    pattern = re.compile(
        r"pub const broker_supported_apis = \[_\]BrokerApiSupport\{"
        r"(?P<body>.*?)\n\};",
        re.DOTALL,
    )
    match = pattern.search(api_support)
    if not match:
        raise AssertionError("missing broker_supported_apis")
    metrics = re.findall(r"\.metric\s*=\s*\"([^\"]+)\"", match.group("body"))
    if not metrics:
        raise AssertionError("broker_supported_apis had no metric names")
    return metrics


def registered_metrics():
    metrics_source = read(METRICS_ZIG)
    api_support = read(API_SUPPORT_ZIG)

    registered = set()
    histograms = set()
    for function in METRIC_REGISTER_FUNCTIONS:
        body = function_body(metrics_source, function)
        for match in REGISTER_RE.finditer(body):
            name = match.group("name")
            registered.add(name)
            if match.group("kind") == "Histogram":
                histograms.add(name)

    for name in api_metric_names(api_support):
        registered.add(name)

    if len(registered) < 60:
        raise AssertionError(
            f"registered metric corpus is unexpectedly small: {len(registered)}"
        )
    return registered, histograms


def literal_registered_metrics():
    metrics_source = read(METRICS_ZIG)
    registered = set()
    for function in METRIC_REGISTER_FUNCTIONS:
        body = function_body(metrics_source, function)
        for match in REGISTER_RE.finditer(body):
            registered.add(match.group("name"))
    return registered


def non_test_source_text():
    chunks = []
    for root, _, files in os.walk(SRC_DIR):
        for filename in files:
            if not filename.endswith(".zig"):
                continue
            path = os.path.join(root, filename)
            if os.path.abspath(path) == os.path.abspath(METRICS_ZIG):
                continue
            text = read(path)
            test_index = text.find('\ntest "')
            if test_index >= 0:
                text = text[:test_index]
            chunks.append(text)
    return "\n".join(chunks)


def collect_json_promql_expressions(value):
    expressions = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "expr" and isinstance(child, str):
                expressions.append(child)
            expressions.extend(collect_json_promql_expressions(child))
    elif isinstance(value, list):
        for child in value:
            expressions.extend(collect_json_promql_expressions(child))
    return expressions


def assert_dashboard_grid_position_well_formed(title, grid_pos):
    if not isinstance(grid_pos, dict):
        raise AssertionError(f"dashboard panel {title} missing gridPos")
    for field in ("h", "w", "x", "y"):
        if type(grid_pos.get(field)) is not int:
            raise AssertionError(f"dashboard panel {title} missing gridPos.{field}")

    if grid_pos["h"] <= 0 or grid_pos["w"] <= 0:
        raise AssertionError(f"dashboard panel {title} gridPos dimensions must be positive")
    if grid_pos["x"] < 0 or grid_pos["y"] < 0:
        raise AssertionError(f"dashboard panel {title} gridPos origin must be non-negative")
    if grid_pos["x"] + grid_pos["w"] > DASHBOARD_GRID_COLUMNS:
        raise AssertionError(
            f"dashboard panel {title} exceeds {DASHBOARD_GRID_COLUMNS}-column grid"
        )


def assert_dashboard_target_well_formed(title, target_index, target):
    if not isinstance(target, dict):
        raise AssertionError(
            f"dashboard panel {title} target {target_index} must be an object"
        )
    unexpected = sorted(set(target) - DASHBOARD_TARGET_FIELDS)
    if unexpected:
        raise AssertionError(
            f"dashboard panel {title} target {target_index} contains unexpected "
            f"fields: {', '.join(unexpected)}"
        )
    for field in ("expr", "legendFormat"):
        value = target.get(field)
        if not isinstance(value, str) or not value.strip():
            raise AssertionError(
                f"dashboard panel {title} target {target_index} missing {field}"
            )


def assert_dashboard_panels_well_formed(dashboard):
    panels = dashboard.get("panels")
    if not isinstance(panels, list) or len(panels) < 9:
        raise AssertionError("dashboard must define at least 9 panels")

    seen_ids = set()
    seen_titles = set()
    for index, panel in enumerate(panels):
        if not isinstance(panel, dict):
            raise AssertionError(f"dashboard panel {index} must be an object")

        panel_id = panel.get("id")
        if type(panel_id) is not int or panel_id <= 0:
            raise AssertionError(f"dashboard panel {index} missing integer id")
        if panel_id in seen_ids:
            raise AssertionError(f"dashboard has duplicate panel id {panel_id}")
        seen_ids.add(panel_id)

        if panel.get("type") != "timeseries":
            raise AssertionError(f"dashboard panel {panel_id} must use timeseries type")

        title = panel.get("title")
        if not isinstance(title, str) or not title.strip():
            raise AssertionError(f"dashboard panel {panel_id} missing title")
        if title in seen_titles:
            raise AssertionError(f"dashboard has duplicate panel title {title}")
        seen_titles.add(title)

        assert_dashboard_grid_position_well_formed(title, panel.get("gridPos"))

        targets = panel.get("targets")
        if not isinstance(targets, list) or not targets:
            raise AssertionError(f"dashboard panel {title} missing targets")
        for target_index, target in enumerate(targets):
            assert_dashboard_target_well_formed(title, target_index, target)


def unquote_yaml_scalar(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def collect_yaml_promql_expressions(yaml):
    expressions = []
    lines = yaml.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        trimmed = line.strip()
        if not trimmed.startswith("expr:"):
            index += 1
            continue

        indent = len(line) - len(line.lstrip(" \t"))
        expr = trimmed[len("expr:") :].strip()
        if expr in ("|", ">", "|-", ">-", "|+", ">+"):
            block = []
            index += 1
            while index < len(lines):
                child = lines[index]
                if child.strip():
                    child_indent = len(child) - len(child.lstrip(" \t"))
                    if child_indent <= indent:
                        break
                    block.append(child.strip())
                index += 1
            expressions.append(" ".join(block))
            continue

        expressions.append(unquote_yaml_scalar(expr))
        index += 1
    return expressions


def promql_tokens(expr):
    tokens = []
    index = 0
    while index < len(expr):
        char = expr[index]
        if is_promql_string_start(char):
            index = skip_promql_string(expr, index)
            continue
        if not is_promql_ident_start(char):
            index += 1
            continue

        start = index
        index += 1
        while index < len(expr) and is_promql_ident_char(expr[index]):
            index += 1
        tokens.append(expr[start:index])
    return tokens


def is_promql_string_start(char):
    return char in ("'", '"', "`")


def skip_promql_string(expr, start):
    quote = expr[start]
    index = start + 1
    while index < len(expr):
        char = expr[index]
        if quote != "`" and char == "\\":
            index += 2
            continue
        index += 1
        if char == quote:
            break
    return index


def is_promql_ident_start(char):
    return (
        ("A" <= char <= "Z")
        or ("a" <= char <= "z")
        or char == "_"
        or char == ":"
    )


def is_promql_ident_char(char):
    return is_promql_ident_start(char) or ("0" <= char <= "9")


def is_promql_metric_identifier(
    identifier, skipped, prefixes, registered=None, histograms=None
):
    if identifier in skipped:
        return False
    if registered is not None and histograms is not None:
        if is_registered_prometheus_metric(identifier, registered, histograms):
            return True
    return any(identifier.startswith(prefix) for prefix in prefixes)


def strip_histogram_suffix(name):
    for suffix in ("_bucket", "_sum", "_count"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return None


def is_registered_prometheus_metric(name, registered, histograms):
    if name in registered:
        return True
    base = strip_histogram_suffix(name)
    return base is not None and base in histograms


def assert_required_strings(label, text, required):
    missing = [item for item in required if item not in text]
    if missing:
        raise AssertionError(f"{label} missing required strings: {', '.join(missing)}")


def collect_metric_references(
    source, expressions, skipped, prefixes, registered=None, histograms=None
):
    refs = []
    for expr in expressions:
        if not expr.strip():
            raise AssertionError(f"{source} has empty PromQL expression")
        for token in promql_tokens(expr):
            if not is_promql_metric_identifier(
                token, skipped, prefixes, registered, histograms
            ):
                continue
            refs.append((token, expr))
    return refs


def assert_collected_metric_references_registered(source, refs, registered, histograms):
    for token, expr in refs:
        if not is_registered_prometheus_metric(token, registered, histograms):
            raise AssertionError(
                f"unregistered {source} metric reference: {token} "
                f"in expression: {expr}"
            )


def metric_contract_key(name, histograms):
    base = strip_histogram_suffix(name)
    if base is not None and base in histograms:
        return base
    return name


def assert_metric_reference_contract(source, refs, required_metrics, histograms):
    ref_keys = {metric_contract_key(name, histograms) for name, _ in refs}
    required_keys = {
        metric_contract_key(name, histograms) for name in required_metrics
    }
    unpinned_refs = sorted(
        {
            name
            for name, _ in refs
            if metric_contract_key(name, histograms) not in required_keys
        }
    )
    if unpinned_refs:
        raise AssertionError(
            f"{source} has unpinned metric references: " + ", ".join(unpinned_refs)
        )

    missing_refs = sorted(
        {
            name
            for name in required_metrics
            if metric_contract_key(name, histograms) not in ref_keys
        }
    )
    if missing_refs:
        raise AssertionError(
            f"{source} missing pinned metric references: " + ", ".join(missing_refs)
        )


def alert_names(alerts):
    return re.findall(r"(?m)^\s*-\s+alert:\s*([A-Za-z0-9_]+)\s*$", alerts)


def alert_group_names(alerts):
    return re.findall(r"(?m)^\s*-\s+name:\s*([A-Za-z0-9_-]+)\s*$", alerts)


def alert_blocks(alerts):
    blocks = {}
    current_name = None
    current_lines = []
    for line in alerts.splitlines():
        match = re.match(r"\s*-\s+alert:\s*([A-Za-z0-9_]+)\s*$", line)
        if match:
            if current_name is not None:
                blocks[current_name] = "\n".join(current_lines)
            current_name = match.group(1)
            current_lines = [line]
            continue
        if current_name is not None:
            current_lines.append(line)

    if current_name is not None:
        blocks[current_name] = "\n".join(current_lines)
    return blocks


def assert_alert_blocks_well_formed(blocks):
    for name, block in blocks.items():
        if not name.startswith("ZMQ"):
            raise AssertionError(f"alert {name} must use ZMQ prefix")
        for required in ("expr:", "for:", "labels:", "severity:", "annotations:"):
            if required not in block:
                raise AssertionError(f"alert {name} missing {required}")
        if re.search(r"(?m)^\s+summary:\s+\S", block) is None:
            raise AssertionError(f"alert {name} missing summary annotation")
        if re.search(r"(?m)^\s+description:\s+\S", block) is None:
            raise AssertionError(f"alert {name} missing description annotation")
        if re.search(r"(?m)^\s+severity:\s+(warning|critical)\s*$", block) is None:
            raise AssertionError(f"alert {name} has invalid severity")

    missing_critical = sorted(CRITICAL_ALERTS - set(blocks))
    if missing_critical:
        raise AssertionError(
            "alerts file missing critical alert rules: " + ", ".join(missing_critical)
        )

    downgraded = sorted(
        name
        for name in CRITICAL_ALERTS
        if re.search(r"(?m)^\s+severity:\s+critical\s*$", blocks[name]) is None
    )
    if downgraded:
        raise AssertionError(
            "critical alert rules were downgraded: " + ", ".join(downgraded)
        )


def assert_alert_name_contract(names, required_names):
    if len(names) != len(set(names)):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise AssertionError("duplicate alert names: " + ", ".join(duplicates))
    missing_named_rules = sorted(set(required_names) - set(names))
    if missing_named_rules:
        raise AssertionError(
            "alerts file missing named rules: " + ", ".join(missing_named_rules)
        )
    unpinned_named_rules = sorted(set(names) - set(required_names))
    if unpinned_named_rules:
        raise AssertionError(
            "alerts file has unpinned named rules: " + ", ".join(unpinned_named_rules)
        )


def assert_alert_group_contract(groups):
    if len(groups) != len(set(groups)):
        duplicates = sorted({name for name in groups if groups.count(name) > 1})
        raise AssertionError("duplicate alert group names: " + ", ".join(duplicates))
    required = set(REQUIRED_ALERT_GROUPS)
    for required_group in REQUIRED_ALERT_GROUPS:
        if required_group not in groups:
            raise AssertionError(f"alerts file missing group {required_group}")
    unpinned_groups = sorted(set(groups) - required)
    if unpinned_groups:
        raise AssertionError(
            "alerts file has unpinned groups: " + ", ".join(unpinned_groups)
        )


def audit():
    readiness = read(READINESS_ZIG)
    dashboard_required_metrics = zig_string_array(readiness, "dashboard_metrics")
    alert_required_metrics = zig_string_array(readiness, "alert_metrics")
    alert_required_names = zig_string_array(readiness, "alert_names")
    skipped = set(zig_string_array(readiness, "skipped"))
    prefixes = tuple(zig_string_array(readiness, "metric_prefixes"))

    if len(dashboard_required_metrics) < 55:
        raise AssertionError("dashboard metric contract is unexpectedly small")
    if len(alert_required_metrics) < 30:
        raise AssertionError("alert metric contract is unexpectedly small")
    if len(alert_required_names) < 20:
        raise AssertionError("alert-name contract is unexpectedly small")

    registered, histograms = registered_metrics()
    source_text = non_test_source_text()
    missing_emission_refs = sorted(
        metric for metric in literal_registered_metrics() if metric not in source_text
    )
    if missing_emission_refs:
        raise AssertionError(
            "literal registered metrics missing non-test source references: "
            + ", ".join(missing_emission_refs)
        )

    dashboard_text = read(DASHBOARD_JSON)
    dashboard = parse_strict_json(dashboard_text, "Grafana dashboard")
    assert_dashboard_panels_well_formed(dashboard)
    dashboard_expressions = collect_json_promql_expressions(dashboard)
    if len(dashboard_expressions) < 9:
        raise AssertionError("dashboard has too few PromQL expressions")

    assert_required_strings("dashboard", dashboard_text, dashboard_required_metrics)
    if "ZMQ AutoMQ Parity Overview" not in dashboard_text:
        raise AssertionError("dashboard missing ZMQ AutoMQ Parity Overview title")
    dashboard_ref_pairs = collect_metric_references(
        "dashboard", dashboard_expressions, skipped, prefixes, registered, histograms
    )
    assert_collected_metric_references_registered(
        "dashboard", dashboard_ref_pairs, registered, histograms
    )
    assert_metric_reference_contract(
        "dashboard", dashboard_ref_pairs, dashboard_required_metrics, histograms
    )
    dashboard_refs = len(dashboard_ref_pairs)
    if dashboard_refs < len(dashboard_required_metrics):
        raise AssertionError(
            f"dashboard metric references={dashboard_refs} "
            f"required={len(dashboard_required_metrics)}"
        )

    alerts_text = read(ALERTS_YAML)
    alert_expressions = collect_yaml_promql_expressions(alerts_text)
    if len(alert_expressions) < 9:
        raise AssertionError("alerts file has too few PromQL expressions")

    groups = alert_group_names(alerts_text)
    assert_alert_group_contract(groups)

    assert_required_strings("alerts", alerts_text, alert_required_metrics)
    assert_required_strings("alerts", alerts_text, alert_required_names)
    if "severity: critical" not in alerts_text:
        raise AssertionError("alerts file missing critical severity")

    names = alert_names(alerts_text)
    assert_alert_name_contract(names, alert_required_names)
    blocks = alert_blocks(alerts_text)
    assert_alert_blocks_well_formed(blocks)

    alert_ref_pairs = collect_metric_references(
        "alerts", alert_expressions, skipped, prefixes, registered, histograms
    )
    assert_collected_metric_references_registered(
        "alerts", alert_ref_pairs, registered, histograms
    )
    assert_metric_reference_contract(
        "alerts", alert_ref_pairs, alert_required_metrics, histograms
    )
    alert_refs = len(alert_ref_pairs)
    if alert_refs < len(alert_required_metrics):
        raise AssertionError(
            f"alert metric references={alert_refs} "
            f"required={len(alert_required_metrics)}"
        )

    for metric in dashboard_required_metrics + alert_required_metrics:
        if not is_registered_prometheus_metric(metric, registered, histograms):
            raise AssertionError(f"required metric is not registered: {metric}")


def self_test():
    try:
        parse_strict_json('{"value": NaN}', "observability self-test")
        raise AssertionError("non-standard JSON constant was accepted")
    except AssertionError as exc:
        message = str(exc)
        if "strict JSON" not in message or "non-standard JSON constant" not in message:
            raise
    try:
        parse_strict_json('{"value": 1, "value": 2}', "observability self-test")
        raise AssertionError("duplicate JSON object key was accepted")
    except AssertionError as exc:
        message = str(exc)
        if "strict JSON" not in message or "duplicate JSON object key" not in message:
            raise
    try:
        assert_dashboard_grid_position_well_formed(
            "synthetic", {"h": 8, "w": 25, "x": 0, "y": 0}
        )
        raise AssertionError("invalid dashboard grid position was accepted")
    except AssertionError as exc:
        if "24-column grid" not in str(exc):
            raise
    try:
        assert_dashboard_target_well_formed(
            "synthetic", 0, {"expr": "up", "legendFormat": "Up", "refId": "A"}
        )
        raise AssertionError("unexpected dashboard target field was accepted")
    except AssertionError as exc:
        if "unexpected fields" not in str(exc):
            raise
    try:
        assert_dashboard_target_well_formed("synthetic", 0, {"expr": "up"})
        raise AssertionError("missing dashboard target legend was accepted")
    except AssertionError as exc:
        if "missing legendFormat" not in str(exc):
            raise
    try:
        assert_alert_name_contract(["ZMQPinned", "ZMQUnpinned"], ["ZMQPinned"])
        raise AssertionError("unpinned alert name was accepted")
    except AssertionError as exc:
        if "unpinned named rules" not in str(exc):
            raise
    try:
        assert_alert_name_contract(["ZMQPinned", "ZMQPinned"], ["ZMQPinned"])
        raise AssertionError("duplicate alert name was accepted")
    except AssertionError as exc:
        if "duplicate alert names" not in str(exc):
            raise
    try:
        assert_alert_name_contract([], ["ZMQPinned"])
        raise AssertionError("missing alert name was accepted")
    except AssertionError as exc:
        if "missing named rules" not in str(exc):
            raise
    try:
        assert_alert_group_contract(["zmq-broker", "zmq-broker"])
        raise AssertionError("duplicate alert group name was accepted")
    except AssertionError as exc:
        if "duplicate alert group names" not in str(exc):
            raise
    try:
        assert_alert_group_contract(["zmq-broker", "zmq-controller"])
        raise AssertionError("missing required alert group was accepted")
    except AssertionError as exc:
        if "missing group" not in str(exc):
            raise
    try:
        assert_alert_group_contract([*REQUIRED_ALERT_GROUPS, "zmq-extra"])
        raise AssertionError("unpinned alert group name was accepted")
    except AssertionError as exc:
        if "unpinned groups" not in str(exc):
            raise
    critical_blocks = {
        name: (
            f"      - alert: {name}\n"
            "        expr: vector(1)\n"
            "        for: 1m\n"
            "        labels:\n"
            "          severity: critical\n"
            "        annotations:\n"
            "          summary: synthetic critical alert\n"
            "          description: synthetic critical alert"
        )
        for name in CRITICAL_ALERTS
    }
    downgraded_blocks = dict(critical_blocks)
    downgraded_blocks["ZMQNoActiveRaftLeader"] = downgraded_blocks[
        "ZMQNoActiveRaftLeader"
    ].replace("severity: critical", "severity: warning")
    try:
        assert_alert_blocks_well_formed(downgraded_blocks)
        raise AssertionError("critical alert severity downgrade was accepted")
    except AssertionError as exc:
        if "critical alert rules were downgraded" not in str(exc):
            raise
    missing_critical_blocks = dict(critical_blocks)
    missing_critical_blocks.pop("ZMQNoActiveRaftLeader")
    try:
        assert_alert_blocks_well_formed(missing_critical_blocks)
        raise AssertionError("missing critical alert block was accepted")
    except AssertionError as exc:
        if "missing critical alert rules" not in str(exc):
            raise
    try:
        assert_metric_reference_contract(
            "dashboard",
            [
                ("kafka_server_requests_total", "rate(kafka_server_requests_total[5m])"),
                ("kafka_server_unpinned_total", "kafka_server_unpinned_total"),
            ],
            ["kafka_server_requests_total"],
            set(),
        )
        raise AssertionError("unpinned dashboard metric reference was accepted")
    except AssertionError as exc:
        if "unpinned metric references" not in str(exc):
            raise
    try:
        assert_metric_reference_contract(
            "alerts",
            [],
            ["kafka_server_requests_total"],
            set(),
        )
        raise AssertionError("missing pinned alert metric reference was accepted")
    except AssertionError as exc:
        if "missing pinned metric references" not in str(exc):
            raise
    refs = collect_metric_references(
        "dashboard",
        ["sum(rate(api_versions[5m])) + sum(rate(custom_latency_seconds_count[5m]))"],
        {"rate", "sum"},
        ("kafka_",),
        {"api_versions"},
        {"custom_latency_seconds"},
    )
    if [name for name, _ in refs] != ["api_versions", "custom_latency_seconds_count"]:
        raise AssertionError("registered unprefixed metric references were not collected")
    refs = collect_metric_references(
        "dashboard",
        ['sum(rate(Kafka_request_count_total{type="api_versions"}[5m]))'],
        {"rate", "sum"},
        ("Kafka_",),
        {"api_versions", "Kafka_request_count_total"},
        set(),
    )
    if [name for name, _ in refs] != ["Kafka_request_count_total"]:
        raise AssertionError("quoted label values were collected as metric references")
    assert_metric_reference_contract(
        "dashboard",
        [
            (
                "kafka_server_request_latency_seconds_count",
                "kafka_server_request_latency_seconds_count",
            )
        ],
        ["kafka_server_request_latency_seconds_bucket"],
        {"kafka_server_request_latency_seconds"},
    )
    audit()


def main():
    if len(sys.argv) > 1 and sys.argv[1] != "--self-test":
        raise SystemExit(f"unknown argument: {sys.argv[1]}")
    if len(sys.argv) > 1 and sys.argv[1] == "--self-test":
        self_test()
    else:
        audit()
    print("ok: observability static audit")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Gated real-client compatibility matrix.

This harness intentionally depends on external Kafka clients. It is skipped by
default so normal local/CI Zig tests remain deterministic.

Run against a running ZMQ broker:

    ZMQ_RUN_CLIENT_MATRIX=1 zig build test-client-matrix

Optional environment:
    ZMQ_CLIENT_MATRIX_BOOTSTRAP   localhost:9092
    ZMQ_CLIENT_MATRIX_TOOLS       auto | kcat,kafka-cli,kafka-python,confluent-kafka,java-kafka,go-kafka
    ZMQ_CLIENT_MATRIX_SEMANTICS   basic | admin | groups | rebalance | transactions | security | security-negative | all
    ZMQ_CLIENT_MATRIX_SECURITY_PROTOCOL
                                  PLAINTEXT | SASL_PLAINTEXT | SSL | SASL_SSL
    ZMQ_CLIENT_MATRIX_SASL_MECHANISM
                                  PLAIN | SCRAM-SHA-256 | OAUTHBEARER
    ZMQ_CLIENT_MATRIX_SASL_USERNAME
    ZMQ_CLIENT_MATRIX_SASL_PASSWORD
    ZMQ_CLIENT_MATRIX_OAUTH_TOKEN
                                  Valid JWT token for kafka-python/confluent-kafka OAUTHBEARER probes
    ZMQ_CLIENT_MATRIX_BAD_OAUTH_TOKEN
                                  Invalid/expired JWT token expected to fail OAUTHBEARER authentication
    ZMQ_CLIENT_MATRIX_OAUTH_JAAS_CONFIG
                                  Java/Kafka CLI OAUTHBEARER JAAS config
    ZMQ_CLIENT_MATRIX_BAD_OAUTH_JAAS_CONFIG
                                  Java/Kafka CLI OAUTHBEARER JAAS config expected to fail
    ZMQ_CLIENT_MATRIX_OAUTHBEARER_CONFIG
                                  kcat/librdkafka OAUTHBEARER config string
    ZMQ_CLIENT_MATRIX_BAD_OAUTHBEARER_CONFIG
                                  kcat/librdkafka OAUTHBEARER config string expected to fail
    ZMQ_CLIENT_MATRIX_SSL_CA_LOCATION
    ZMQ_CLIENT_MATRIX_BAD_SSL_CA_LOCATION
    ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC
    ZMQ_CLIENT_MATRIX_PROFILES    Comma-separated profile names for explicit client/library version sets
    ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES
                                  Comma-separated profile names that must be present
    ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES
                                  Profiles that must run secured-client probes
    ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES
                                  Profiles that must run configured negative security probes
    ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES
                                  Profiles that must run OAUTHBEARER secured-client probes
    ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES
                                  Profiles that must run OAuth-specific negative probes
    ZMQ_CLIENT_MATRIX_JAVA_CLASSPATH
                                  Classpath containing kafka-clients jars for java-kafka
    ZMQ_CLIENT_MATRIX_ENABLE_GO   1 to include go-kafka in auto mode
    ZMQ_CLIENT_MATRIX_GO_MODULE   Go module for go-kafka (default github.com/segmentio/kafka-go@latest)
    ZMQ_CLIENT_MATRIX_PYTHON      Python executable for kafka-python/confluent-kafka probes

Per-profile overrides:
    For profile "apache_3_7", set ZMQ_CLIENT_MATRIX_APACHE_3_7_TOOLS,
    ZMQ_CLIENT_MATRIX_APACHE_3_7_JAVA_CLASSPATH, etc. A profile inherits the
    corresponding global setting when its override is not present.

Supported probes:
    kcat             metadata plus produce/fetch round trip
    kafka-cli        API/topic admin probes plus console produce/fetch round trip
    kafka-python     AdminClient metadata plus producer/consumer and offset commit round trip
    confluent-kafka  AdminClient metadata plus producer/consumer and offset commit round trip
    java-kafka       Apache kafka-clients AdminClient plus producer/consumer and offset commit round trip
    go-kafka         segmentio kafka-go metadata plus writer/reader and offset commit round trip

Semantic probes:
    basic            metadata, produce/fetch, and offset commit where supported
    admin            explicit topic/config admin through real clients
    groups           consumer-group join/commit/list/describe where supported
    rebalance        multi-consumer assignment convergence where supported
    transactions     transactional produce through clients that expose Kafka transactions
    security         run the selected probes with configured TLS/SASL client properties
    security-negative
                      verify bad credentials, bad TLS trust, or ACL-denied produce fail closed where configured
"""

import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time


RUN_ENABLED = os.environ.get("ZMQ_RUN_CLIENT_MATRIX") == "1"
BOOTSTRAP = os.environ.get("ZMQ_CLIENT_MATRIX_BOOTSTRAP", "localhost:9092")
TOOLS = os.environ.get("ZMQ_CLIENT_MATRIX_TOOLS", "auto")
SEMANTIC_ORDER = [
    "basic",
    "admin",
    "groups",
    "rebalance",
    "transactions",
    "security",
    "security-negative",
]
REBALANCE_TOOLS = {"kafka-python", "confluent-kafka", "java-kafka"}
TRANSACTION_TOOLS = {"confluent-kafka", "java-kafka"}
SECURITY_TOOLS = {"kcat", "kafka-cli", "kafka-python", "confluent-kafka", "java-kafka"}
JAVA_CLASSPATH = os.environ.get("ZMQ_CLIENT_MATRIX_JAVA_CLASSPATH")
ENABLE_GO_AUTO = os.environ.get("ZMQ_CLIENT_MATRIX_ENABLE_GO") == "1"
GO_MODULE = os.environ.get("ZMQ_CLIENT_MATRIX_GO_MODULE", "github.com/segmentio/kafka-go@latest")
PYTHON = os.environ.get("ZMQ_CLIENT_MATRIX_PYTHON", sys.executable)
ACTIVE_PROFILE = "default"
SEMANTICS = None
SECURITY_PROTOCOL = "PLAINTEXT"
SASL_MECHANISM = None
SASL_USERNAME = None
SASL_PASSWORD = None
OAUTH_TOKEN = None
BAD_OAUTH_TOKEN = None
OAUTH_JAAS_CONFIG = None
BAD_OAUTH_JAAS_CONFIG = None
OAUTHBEARER_CONFIG = None
BAD_OAUTHBEARER_CONFIG = None
SSL_CA_LOCATION = None
BAD_SSL_CA_LOCATION = None
ACL_DENIED_TOPIC = None


class MatrixError(Exception):
    pass


class StaticOAuthTokenProvider:
    def __init__(self, token):
        self._token = token

    def token(self):
        return self._token


def parse_semantics(raw):
    raw = (raw or "basic").strip()
    if raw in ("", "default", "basic"):
        return {"basic"}
    selected = set()
    for item in raw.split(","):
        name = item.strip().lower()
        if not name:
            continue
        if name == "all":
            selected.update(SEMANTIC_ORDER)
            continue
        if name not in SEMANTIC_ORDER:
            raise MatrixError(f"unknown client matrix semantic probe: {name}")
        selected.add(name)
    selected.add("basic")
    return selected


def semantics_csv():
    return ",".join(name for name in SEMANTIC_ORDER if name in SEMANTICS)


def semantic_enabled(name):
    return name in SEMANTICS


def security_enabled():
    return (
        semantic_enabled("security")
        or semantic_enabled("security-negative")
        or SECURITY_PROTOCOL.upper() != "PLAINTEXT"
    )


def oauth_enabled():
    return (SASL_MECHANISM or "").upper() == "OAUTHBEARER"


def oauth_token_expiry():
    raw = os.environ.get("ZMQ_CLIENT_MATRIX_OAUTH_TOKEN_EXPIRY", "9999999999")
    try:
        return int(raw)
    except ValueError:
        raise MatrixError(f"invalid ZMQ_CLIENT_MATRIX_OAUTH_TOKEN_EXPIRY: {raw!r}")


def security_properties(password_override=None, ssl_ca_override=None, jaas_config_override=None):
    if not security_enabled():
        return {}
    props = {"security.protocol": SECURITY_PROTOCOL}
    if SASL_MECHANISM:
        props["sasl.mechanism"] = SASL_MECHANISM
        props["sasl.mechanisms"] = SASL_MECHANISM
    if not oauth_enabled() and SASL_USERNAME:
        props["sasl.username"] = SASL_USERNAME
    password = SASL_PASSWORD if password_override is None else password_override
    if not oauth_enabled() and password:
        props["sasl.password"] = password
    jaas_config = OAUTH_JAAS_CONFIG if jaas_config_override is None else jaas_config_override
    if oauth_enabled() and jaas_config:
        props["sasl.jaas.config"] = jaas_config
    ssl_ca = SSL_CA_LOCATION if ssl_ca_override is None else ssl_ca_override
    if ssl_ca:
        props["ssl.ca.location"] = ssl_ca
    return props


def kcat_security_args(password_override=None, ssl_ca_override=None, oauthbearer_config_override=None):
    args = []
    props = security_properties(password_override=password_override, ssl_ca_override=ssl_ca_override)
    if not props:
        return args
    for key in ("security.protocol", "sasl.mechanisms", "sasl.username", "sasl.password", "ssl.ca.location"):
        value = props.get(key)
        if value:
            args += ["-X", f"{key}={value}"]
    oauth_config = OAUTHBEARER_CONFIG if oauthbearer_config_override is None else oauthbearer_config_override
    if oauth_enabled() and oauth_config:
        args += ["-X", "enable.sasl.oauthbearer.unsecure.jwt=true"]
        args += ["-X", f"sasl.oauthbearer.config={oauth_config}"]
    return args


def kafka_cli_security_config_path(password_override=None, ssl_ca_override=None, jaas_config_override=None):
    props = security_properties(
        password_override=password_override,
        ssl_ca_override=ssl_ca_override,
        jaas_config_override=jaas_config_override,
    )
    if not props:
        return None
    fd, path = tempfile.mkstemp(prefix="zmq-client-matrix-kafka-cli-", suffix=".properties")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for key in (
            "security.protocol",
            "sasl.mechanism",
            "sasl.username",
            "sasl.password",
            "sasl.jaas.config",
            "ssl.ca.location",
        ):
            value = props.get(key)
            if value:
                f.write(f"{key}={value}\n")
    return path


def kafka_cli_command_config(args, config_path):
    if config_path is None:
        return args
    return args + ["--command-config", config_path]


def kafka_python_security_config(password_override=None, ssl_ca_override=None, oauth_token_override=None):
    if not security_enabled():
        return {}
    config = {"security_protocol": SECURITY_PROTOCOL}
    if SASL_MECHANISM:
        config["sasl_mechanism"] = SASL_MECHANISM
    if oauth_enabled():
        token = OAUTH_TOKEN if oauth_token_override is None else oauth_token_override
        if token:
            config["sasl_oauth_token_provider"] = StaticOAuthTokenProvider(token)
        return config
    if SASL_USERNAME:
        config["sasl_plain_username"] = SASL_USERNAME
    password = SASL_PASSWORD if password_override is None else password_override
    if password:
        config["sasl_plain_password"] = password
    ssl_ca = SSL_CA_LOCATION if ssl_ca_override is None else ssl_ca_override
    if ssl_ca:
        config["ssl_cafile"] = ssl_ca
    return config


def confluent_security_config(password_override=None, ssl_ca_override=None, oauth_token_override=None):
    props = security_properties(password_override=password_override, ssl_ca_override=ssl_ca_override)
    if not props:
        return {}
    config = {"security.protocol": props["security.protocol"]}
    if SASL_MECHANISM:
        config["sasl.mechanisms"] = SASL_MECHANISM
    if oauth_enabled():
        token = OAUTH_TOKEN if oauth_token_override is None else oauth_token_override
        if token:
            config["oauth_cb"] = lambda *_args: (token, oauth_token_expiry())
        return config
    if SASL_USERNAME:
        config["sasl.username"] = SASL_USERNAME
    password = SASL_PASSWORD if password_override is None else password_override
    if password:
        config["sasl.password"] = password
    if SSL_CA_LOCATION:
        config["ssl.ca.location"] = SSL_CA_LOCATION
    return config


def bad_sasl_password():
    if not SASL_PASSWORD:
        raise MatrixError("security-negative semantic requires ZMQ_CLIENT_MATRIX_SASL_PASSWORD")
    return SASL_PASSWORD + "-invalid"


def sasl_negative_enabled():
    return bool(SASL_PASSWORD) and not oauth_enabled()


def oauth_token_negative_enabled():
    return oauth_enabled() and bool(BAD_OAUTH_TOKEN)


def oauth_jaas_negative_enabled():
    return oauth_enabled() and bool(BAD_OAUTH_JAAS_CONFIG)


def oauthbearer_config_negative_enabled():
    return oauth_enabled() and bool(BAD_OAUTHBEARER_CONFIG)


def oauth_negative_configured():
    return (
        oauth_token_negative_enabled()
        or oauth_jaas_negative_enabled()
        or oauthbearer_config_negative_enabled()
    )


def tls_negative_enabled():
    return bool(BAD_SSL_CA_LOCATION) and SECURITY_PROTOCOL.upper() in ("SSL", "SASL_SSL")


def acl_negative_enabled():
    return bool(ACL_DENIED_TOPIC)


def security_negative_configured():
    return (
        sasl_negative_enabled()
        or oauth_token_negative_enabled()
        or oauth_jaas_negative_enabled()
        or oauthbearer_config_negative_enabled()
        or tls_negative_enabled()
        or acl_negative_enabled()
    )


def oauth_positive_configured_for_tool(tool):
    if not oauth_enabled():
        return True
    if tool in ("kafka-python", "confluent-kafka"):
        return bool(OAUTH_TOKEN)
    if tool in ("kafka-cli", "java-kafka"):
        return bool(OAUTH_JAAS_CONFIG)
    if tool == "kcat":
        return bool(OAUTHBEARER_CONFIG)
    return False


def security_negative_configured_for_tool(tool):
    if sasl_negative_enabled() or tls_negative_enabled() or acl_negative_enabled():
        return True
    return oauth_negative_configured_for_tool(tool)


def oauth_negative_configured_for_tool(tool):
    if tool in ("kafka-python", "confluent-kafka") and oauth_token_negative_enabled():
        return True
    if tool in ("kafka-cli", "java-kafka") and oauth_jaas_negative_enabled():
        return True
    if tool == "kcat" and oauthbearer_config_negative_enabled():
        return True
    return False


def run(cmd, timeout=30, input_text=None, cwd=None, env=None):
    proc = subprocess.run(
        cmd,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        cwd=cwd,
        env=env,
    )
    if proc.returncode != 0:
        raise MatrixError(f"{cmd[0]} failed with exit code {proc.returncode}\n{proc.stdout}")
    return proc.stdout


def run_expect_failure(cmd, timeout=30, input_text=None, cwd=None, env=None):
    proc = subprocess.run(
        cmd,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        cwd=cwd,
        env=env,
    )
    if proc.returncode == 0:
        raise MatrixError(f"{cmd[0]} unexpectedly succeeded\n{proc.stdout}")
    return proc.stdout


def have(exe):
    return shutil.which(exe) is not None


def have_module(module):
    return importlib.util.find_spec(module) is not None


def have_python_module(module):
    if python_is_current():
        return have_module(module)
    try:
        run(
            [
                PYTHON,
                "-c",
                f"import importlib.util; raise SystemExit(0 if importlib.util.find_spec({module!r}) else 1)",
            ],
            timeout=15,
        )
        return True
    except (MatrixError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def python_is_current():
    try:
        return os.path.realpath(PYTHON) == os.path.realpath(sys.executable)
    except OSError:
        return PYTHON == sys.executable


def profile_env_name(profile, suffix):
    sanitized = "".join(ch.upper() if ch.isalnum() else "_" for ch in profile)
    return f"ZMQ_CLIENT_MATRIX_{sanitized}_{suffix}"


def profile_names():
    raw = os.environ.get("ZMQ_CLIENT_MATRIX_PROFILES", "")
    names = [name.strip() for name in raw.split(",") if name.strip()]
    return names if names else ["default"]


def configured_names(env_name):
    raw = os.environ.get(env_name, "")
    return [name.strip() for name in raw.split(",") if name.strip()]


def profile_setting(profile, suffix, fallback):
    if profile == "default":
        return os.environ.get(f"ZMQ_CLIENT_MATRIX_{suffix}", fallback)
    return os.environ.get(profile_env_name(profile, suffix), os.environ.get(f"ZMQ_CLIENT_MATRIX_{suffix}", fallback))


def apply_profile(profile):
    global ACTIVE_PROFILE, BOOTSTRAP, TOOLS, SEMANTICS, JAVA_CLASSPATH, ENABLE_GO_AUTO, GO_MODULE, PYTHON
    global SECURITY_PROTOCOL, SASL_MECHANISM, SASL_USERNAME, SASL_PASSWORD, SSL_CA_LOCATION
    global OAUTH_TOKEN, BAD_OAUTH_TOKEN, OAUTH_JAAS_CONFIG, BAD_OAUTH_JAAS_CONFIG
    global OAUTHBEARER_CONFIG, BAD_OAUTHBEARER_CONFIG, BAD_SSL_CA_LOCATION, ACL_DENIED_TOPIC

    ACTIVE_PROFILE = profile
    BOOTSTRAP = profile_setting(profile, "BOOTSTRAP", "localhost:9092")
    TOOLS = profile_setting(profile, "TOOLS", "auto")
    SEMANTICS = parse_semantics(profile_setting(profile, "SEMANTICS", "basic"))
    SECURITY_PROTOCOL = profile_setting(profile, "SECURITY_PROTOCOL", "PLAINTEXT")
    SASL_MECHANISM = profile_setting(profile, "SASL_MECHANISM", None)
    SASL_USERNAME = profile_setting(profile, "SASL_USERNAME", None)
    SASL_PASSWORD = profile_setting(profile, "SASL_PASSWORD", None)
    OAUTH_TOKEN = profile_setting(profile, "OAUTH_TOKEN", None)
    BAD_OAUTH_TOKEN = profile_setting(profile, "BAD_OAUTH_TOKEN", None)
    OAUTH_JAAS_CONFIG = profile_setting(profile, "OAUTH_JAAS_CONFIG", None)
    BAD_OAUTH_JAAS_CONFIG = profile_setting(profile, "BAD_OAUTH_JAAS_CONFIG", None)
    OAUTHBEARER_CONFIG = profile_setting(profile, "OAUTHBEARER_CONFIG", None)
    BAD_OAUTHBEARER_CONFIG = profile_setting(profile, "BAD_OAUTHBEARER_CONFIG", None)
    SSL_CA_LOCATION = profile_setting(profile, "SSL_CA_LOCATION", None)
    BAD_SSL_CA_LOCATION = profile_setting(profile, "BAD_SSL_CA_LOCATION", None)
    ACL_DENIED_TOPIC = profile_setting(profile, "ACL_DENIED_TOPIC", None)
    JAVA_CLASSPATH = profile_setting(profile, "JAVA_CLASSPATH", None)
    ENABLE_GO_AUTO = profile_setting(profile, "ENABLE_GO", "0") == "1"
    GO_MODULE = profile_setting(profile, "GO_MODULE", "github.com/segmentio/kafka-go@latest")
    PYTHON = profile_setting(profile, "PYTHON", sys.executable)


def validate_required_profiles(profiles):
    profile_set = set(profiles)
    required = configured_names("ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES")
    missing = [profile for profile in required if profile not in profile_set]
    if missing:
        raise MatrixError(
            "required client matrix profiles missing from ZMQ_CLIENT_MATRIX_PROFILES: "
            + ", ".join(missing)
        )

    required_security = configured_names("ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES")
    missing_security = [profile for profile in required_security if profile not in profile_set]
    if missing_security:
        raise MatrixError(
            "required secured-client profiles missing from ZMQ_CLIENT_MATRIX_PROFILES: "
            + ", ".join(missing_security)
        )
    unsecured = []
    for profile in required_security:
        apply_profile(profile)
        if not security_enabled():
            unsecured.append(profile)
    if unsecured:
        raise MatrixError(
            "required secured-client profiles must enable security semantics or a secured protocol: "
            + ", ".join(unsecured)
        )

    required_negative = configured_names(
        "ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES"
    )
    missing_negative = [profile for profile in required_negative if profile not in profile_set]
    if missing_negative:
        raise MatrixError(
            "required negative-security profiles missing from ZMQ_CLIENT_MATRIX_PROFILES: "
            + ", ".join(missing_negative)
        )
    negative_without_vectors = []
    for profile in required_negative:
        apply_profile(profile)
        if not semantic_enabled("security-negative") or not security_negative_configured():
            negative_without_vectors.append(profile)
    if negative_without_vectors:
        raise MatrixError(
            "required negative-security profiles must enable security-negative "
            "and configure at least one negative vector: "
            + ", ".join(negative_without_vectors)
        )

    required_oauth = configured_names("ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES")
    missing_oauth = [profile for profile in required_oauth if profile not in profile_set]
    if missing_oauth:
        raise MatrixError(
            "required OAuth profiles missing from ZMQ_CLIENT_MATRIX_PROFILES: "
            + ", ".join(missing_oauth)
        )
    oauth_without_vectors = []
    for profile in required_oauth:
        apply_profile(profile)
        tools = selected_tools()
        if not security_enabled() or not oauth_enabled() or not any(
            oauth_positive_configured_for_tool(tool) for tool in tools
        ):
            oauth_without_vectors.append(profile)
    if oauth_without_vectors:
        raise MatrixError(
            "required OAuth profiles must enable OAUTHBEARER security and "
            "configure a positive fixture for at least one selected tool: "
            + ", ".join(oauth_without_vectors)
        )

    required_oauth_negative = configured_names(
        "ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES"
    )
    missing_oauth_negative = [
        profile for profile in required_oauth_negative if profile not in profile_set
    ]
    if missing_oauth_negative:
        raise MatrixError(
            "required OAuth-negative profiles missing from ZMQ_CLIENT_MATRIX_PROFILES: "
            + ", ".join(missing_oauth_negative)
        )
    oauth_negative_without_vectors = []
    for profile in required_oauth_negative:
        apply_profile(profile)
        tools = selected_tools()
        if (
            not semantic_enabled("security-negative")
            or not oauth_negative_configured()
            or not any(oauth_negative_configured_for_tool(tool) for tool in tools)
        ):
            oauth_negative_without_vectors.append(profile)
    if oauth_negative_without_vectors:
        raise MatrixError(
            "required OAuth-negative profiles must enable security-negative "
            "and configure an OAuth negative vector for at least one selected tool: "
            + ", ".join(oauth_negative_without_vectors)
        )


def matrix_topic(tool):
    profile_prefix = "" if ACTIVE_PROFILE == "default" else f"{ACTIVE_PROFILE}_"
    sanitized = "".join(ch if ch.isalnum() else "_" for ch in profile_prefix + tool)
    return f"zmq_client_matrix_{sanitized}_{os.getpid()}"


def matrix_payload(tool):
    return f"zmq-client-matrix-{ACTIVE_PROFILE}-{tool}-{os.getpid()}"


def run_python_subtool(tool):
    if python_is_current():
        return False

    env = os.environ.copy()
    env["ZMQ_RUN_CLIENT_MATRIX"] = "1"
    env["ZMQ_CLIENT_MATRIX_BOOTSTRAP"] = BOOTSTRAP
    env["ZMQ_CLIENT_MATRIX_TOOLS"] = tool
    env["ZMQ_CLIENT_MATRIX_SEMANTICS"] = semantics_csv()
    env["ZMQ_CLIENT_MATRIX_SECURITY_PROTOCOL"] = SECURITY_PROTOCOL
    if SASL_MECHANISM:
        env["ZMQ_CLIENT_MATRIX_SASL_MECHANISM"] = SASL_MECHANISM
    if SASL_USERNAME:
        env["ZMQ_CLIENT_MATRIX_SASL_USERNAME"] = SASL_USERNAME
    if SASL_PASSWORD:
        env["ZMQ_CLIENT_MATRIX_SASL_PASSWORD"] = SASL_PASSWORD
    if OAUTH_TOKEN:
        env["ZMQ_CLIENT_MATRIX_OAUTH_TOKEN"] = OAUTH_TOKEN
    if BAD_OAUTH_TOKEN:
        env["ZMQ_CLIENT_MATRIX_BAD_OAUTH_TOKEN"] = BAD_OAUTH_TOKEN
    if OAUTH_JAAS_CONFIG:
        env["ZMQ_CLIENT_MATRIX_OAUTH_JAAS_CONFIG"] = OAUTH_JAAS_CONFIG
    if BAD_OAUTH_JAAS_CONFIG:
        env["ZMQ_CLIENT_MATRIX_BAD_OAUTH_JAAS_CONFIG"] = BAD_OAUTH_JAAS_CONFIG
    if OAUTHBEARER_CONFIG:
        env["ZMQ_CLIENT_MATRIX_OAUTHBEARER_CONFIG"] = OAUTHBEARER_CONFIG
    if BAD_OAUTHBEARER_CONFIG:
        env["ZMQ_CLIENT_MATRIX_BAD_OAUTHBEARER_CONFIG"] = BAD_OAUTHBEARER_CONFIG
    if SSL_CA_LOCATION:
        env["ZMQ_CLIENT_MATRIX_SSL_CA_LOCATION"] = SSL_CA_LOCATION
    if BAD_SSL_CA_LOCATION:
        env["ZMQ_CLIENT_MATRIX_BAD_SSL_CA_LOCATION"] = BAD_SSL_CA_LOCATION
    if ACL_DENIED_TOPIC:
        env["ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC"] = ACL_DENIED_TOPIC
    env["ZMQ_CLIENT_MATRIX_ENABLE_GO"] = "0"
    env["ZMQ_CLIENT_MATRIX_PYTHON"] = PYTHON
    env.pop("ZMQ_CLIENT_MATRIX_PROFILES", None)

    run([PYTHON, __file__], timeout=120, env=env)
    return True


def active_security_env():
    env = os.environ.copy()
    env["ZMQ_CLIENT_MATRIX_SECURITY_PROTOCOL"] = SECURITY_PROTOCOL
    if SASL_MECHANISM:
        env["ZMQ_CLIENT_MATRIX_SASL_MECHANISM"] = SASL_MECHANISM
    else:
        env.pop("ZMQ_CLIENT_MATRIX_SASL_MECHANISM", None)
    if SASL_USERNAME:
        env["ZMQ_CLIENT_MATRIX_SASL_USERNAME"] = SASL_USERNAME
    else:
        env.pop("ZMQ_CLIENT_MATRIX_SASL_USERNAME", None)
    if SASL_PASSWORD:
        env["ZMQ_CLIENT_MATRIX_SASL_PASSWORD"] = SASL_PASSWORD
    else:
        env.pop("ZMQ_CLIENT_MATRIX_SASL_PASSWORD", None)
    if OAUTH_TOKEN:
        env["ZMQ_CLIENT_MATRIX_OAUTH_TOKEN"] = OAUTH_TOKEN
    else:
        env.pop("ZMQ_CLIENT_MATRIX_OAUTH_TOKEN", None)
    if BAD_OAUTH_TOKEN:
        env["ZMQ_CLIENT_MATRIX_BAD_OAUTH_TOKEN"] = BAD_OAUTH_TOKEN
    else:
        env.pop("ZMQ_CLIENT_MATRIX_BAD_OAUTH_TOKEN", None)
    if OAUTH_JAAS_CONFIG:
        env["ZMQ_CLIENT_MATRIX_OAUTH_JAAS_CONFIG"] = OAUTH_JAAS_CONFIG
    else:
        env.pop("ZMQ_CLIENT_MATRIX_OAUTH_JAAS_CONFIG", None)
    if BAD_OAUTH_JAAS_CONFIG:
        env["ZMQ_CLIENT_MATRIX_BAD_OAUTH_JAAS_CONFIG"] = BAD_OAUTH_JAAS_CONFIG
    else:
        env.pop("ZMQ_CLIENT_MATRIX_BAD_OAUTH_JAAS_CONFIG", None)
    if OAUTHBEARER_CONFIG:
        env["ZMQ_CLIENT_MATRIX_OAUTHBEARER_CONFIG"] = OAUTHBEARER_CONFIG
    else:
        env.pop("ZMQ_CLIENT_MATRIX_OAUTHBEARER_CONFIG", None)
    if BAD_OAUTHBEARER_CONFIG:
        env["ZMQ_CLIENT_MATRIX_BAD_OAUTHBEARER_CONFIG"] = BAD_OAUTHBEARER_CONFIG
    else:
        env.pop("ZMQ_CLIENT_MATRIX_BAD_OAUTHBEARER_CONFIG", None)
    if SSL_CA_LOCATION:
        env["ZMQ_CLIENT_MATRIX_SSL_CA_LOCATION"] = SSL_CA_LOCATION
    else:
        env.pop("ZMQ_CLIENT_MATRIX_SSL_CA_LOCATION", None)
    if BAD_SSL_CA_LOCATION:
        env["ZMQ_CLIENT_MATRIX_BAD_SSL_CA_LOCATION"] = BAD_SSL_CA_LOCATION
    else:
        env.pop("ZMQ_CLIENT_MATRIX_BAD_SSL_CA_LOCATION", None)
    if ACL_DENIED_TOPIC:
        env["ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC"] = ACL_DENIED_TOPIC
    else:
        env.pop("ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC", None)
    return env


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
        if have_python_module("kafka"):
            tools.append("kafka-python")
        if have_python_module("confluent_kafka"):
            tools.append("confluent-kafka")
        if have("javac") and have("java") and JAVA_CLASSPATH:
            tools.append("java-kafka")
        if ENABLE_GO_AUTO and have("go"):
            tools.append("go-kafka")
        return tools
    return [tool.strip() for tool in TOOLS.split(",") if tool.strip()]


def ensure_tool_supports_semantics(tool):
    if semantic_enabled("rebalance") and tool not in REBALANCE_TOOLS:
        raise MatrixError(
            f"{tool} selected with rebalance semantic, but only "
            f"{', '.join(sorted(REBALANCE_TOOLS))} have rebalance probes"
        )
    if semantic_enabled("transactions") and tool not in TRANSACTION_TOOLS:
        raise MatrixError(
            f"{tool} selected with transactions semantic, but only "
            f"{', '.join(sorted(TRANSACTION_TOOLS))} have transaction probes"
        )
    if security_enabled() and tool not in SECURITY_TOOLS:
        raise MatrixError(
            f"{tool} selected with security configuration, but only "
            f"{', '.join(sorted(SECURITY_TOOLS))} have security interop probes"
        )
    if security_enabled() and oauth_enabled() and not oauth_positive_configured_for_tool(tool):
        raise MatrixError(
            f"{tool} selected with OAUTHBEARER security, but no compatible positive OAuth fixture is configured"
        )
    if semantic_enabled("security-negative") and not security_negative_configured():
        raise MatrixError(
            "security-negative semantic requires at least one configured negative vector: "
            "ZMQ_CLIENT_MATRIX_SASL_PASSWORD, ZMQ_CLIENT_MATRIX_BAD_OAUTH_TOKEN, "
            "ZMQ_CLIENT_MATRIX_BAD_OAUTH_JAAS_CONFIG, ZMQ_CLIENT_MATRIX_BAD_OAUTHBEARER_CONFIG, "
            "ZMQ_CLIENT_MATRIX_BAD_SSL_CA_LOCATION with SSL/SASL_SSL, or ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC"
        )
    if semantic_enabled("security-negative") and not security_negative_configured_for_tool(tool):
        raise MatrixError(
            f"{tool} selected with security-negative semantic, but no compatible negative vector is configured"
        )


def test_kcat():
    if not have("kcat"):
        raise MatrixError("kcat selected but not found in PATH")
    kcat_args = ["kcat"] + kcat_security_args()
    out = run(kcat_args + ["-L", "-b", BOOTSTRAP])
    if "Metadata for" not in out and "broker" not in out.lower():
        raise MatrixError(f"kcat metadata output did not look valid\n{out}")

    topic = matrix_topic("kcat")
    payload = matrix_payload("kcat")
    run(kcat_args + ["-P", "-b", BOOTSTRAP, "-t", topic], input_text=payload + "\n", timeout=45)
    fetched = run(kcat_args + ["-C", "-b", BOOTSTRAP, "-t", topic, "-o", "beginning", "-c", "1", "-e"], timeout=45)
    require_output_contains(fetched, payload, "kcat")

    if semantic_enabled("groups"):
        group = f"zmq-client-matrix-kcat-{os.getpid()}"
        group_fetched = run(
            kcat_args + [
                "-C",
                "-b",
                BOOTSTRAP,
                "-G",
                group,
                topic,
                "-X",
                "auto.offset.reset=earliest",
                "-c",
                "1",
                "-e",
            ],
            timeout=45,
        )
        require_output_contains(group_fetched, payload, "kcat group")

    if semantic_enabled("security-negative") and sasl_negative_enabled():
        run_expect_failure(["kcat"] + kcat_security_args(password_override=bad_sasl_password()) + ["-L", "-b", BOOTSTRAP])
    if semantic_enabled("security-negative") and tls_negative_enabled():
        run_expect_failure(
            ["kcat"] + kcat_security_args(ssl_ca_override=BAD_SSL_CA_LOCATION) + ["-L", "-b", BOOTSTRAP],
            timeout=45,
        )
    if semantic_enabled("security-negative") and oauthbearer_config_negative_enabled():
        run_expect_failure(
            ["kcat"] + kcat_security_args(oauthbearer_config_override=BAD_OAUTHBEARER_CONFIG) + ["-L", "-b", BOOTSTRAP],
            timeout=45,
        )
    if semantic_enabled("security-negative") and acl_negative_enabled():
        run_expect_failure(
            kcat_args + ["-P", "-b", BOOTSTRAP, "-t", ACL_DENIED_TOPIC],
            input_text=payload + "-denied\n",
            timeout=45,
        )

    print(f"ok: kcat probes ({semantics_csv()})")


def test_kafka_cli():
    if not have("kafka-broker-api-versions.sh"):
        raise MatrixError("kafka-broker-api-versions.sh selected but not found in PATH")
    if not have("kafka-topics.sh"):
        raise MatrixError("kafka-topics.sh selected but not found in PATH")
    if not have("kafka-console-producer.sh"):
        raise MatrixError("kafka-console-producer.sh selected but not found in PATH")
    if not have("kafka-console-consumer.sh"):
        raise MatrixError("kafka-console-consumer.sh selected but not found in PATH")
    if semantic_enabled("admin") and not have("kafka-configs.sh"):
        raise MatrixError("kafka-cli admin semantics selected but kafka-configs.sh is not in PATH")
    if semantic_enabled("groups") and not have("kafka-consumer-groups.sh"):
        raise MatrixError("kafka-cli group semantics selected but kafka-consumer-groups.sh is not in PATH")

    config_path = kafka_cli_security_config_path()
    try:
        api_out = run(
            kafka_cli_command_config(
                ["kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP],
                config_path,
            ),
            timeout=45,
        )
        if "Produce" not in api_out and "ApiVersions" not in api_out:
            raise MatrixError(f"kafka-broker-api-versions output did not include expected APIs\n{api_out}")

        run(
            kafka_cli_command_config(
                ["kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--list"],
                config_path,
            ),
            timeout=45,
        )

        topic = matrix_topic("kafka-cli")
        payload = matrix_payload("kafka-cli")
        run(
            kafka_cli_command_config(
                [
                    "kafka-topics.sh",
                    "--bootstrap-server",
                    BOOTSTRAP,
                    "--create",
                    "--if-not-exists",
                    "--topic",
                    topic,
                    "--partitions",
                    "1",
                    "--replication-factor",
                    "1",
                ],
                config_path,
            ),
            timeout=45,
        )
        describe_out = run(
            kafka_cli_command_config(
                ["kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--describe", "--topic", topic],
                config_path,
            ),
            timeout=45,
        )
        if topic not in describe_out:
            raise MatrixError(f"kafka-topics describe output did not include {topic!r}\n{describe_out}")

        if semantic_enabled("admin"):
            run(
                kafka_cli_command_config(
                    [
                        "kafka-configs.sh",
                        "--bootstrap-server",
                        BOOTSTRAP,
                        "--entity-type",
                        "topics",
                        "--entity-name",
                        topic,
                        "--alter",
                        "--add-config",
                        "max.message.bytes=1048576",
                    ],
                    config_path,
                ),
                timeout=45,
            )
            config_out = run(
                kafka_cli_command_config(
                    [
                        "kafka-configs.sh",
                        "--bootstrap-server",
                        BOOTSTRAP,
                        "--entity-type",
                        "topics",
                        "--entity-name",
                        topic,
                        "--describe",
                    ],
                    config_path,
                ),
                timeout=45,
            )
            if "max.message.bytes" not in config_out:
                raise MatrixError(f"kafka-configs describe did not include altered topic config\n{config_out}")

        producer_cmd = ["kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP, "--topic", topic]
        if config_path is not None:
            producer_cmd += ["--producer.config", config_path]
        run(producer_cmd, input_text=payload + "\n", timeout=45)

        group = f"zmq-client-matrix-kafka-cli-{os.getpid()}"
        consumer_cmd = [
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
        ]
        if config_path is not None:
            consumer_cmd += ["--consumer.config", config_path]
        if semantic_enabled("groups"):
            consumer_cmd += ["--group", group]
        fetched = run(consumer_cmd, timeout=45)
        require_output_contains(fetched, payload, "kafka-cli")
        if semantic_enabled("groups"):
            groups_out = run(
                kafka_cli_command_config(
                    ["kafka-consumer-groups.sh", "--bootstrap-server", BOOTSTRAP, "--list"],
                    config_path,
                ),
                timeout=45,
            )
            if group not in groups_out:
                raise MatrixError(f"kafka-consumer-groups --list did not include {group!r}\n{groups_out}")
            run(
                kafka_cli_command_config(
                    ["kafka-consumer-groups.sh", "--bootstrap-server", BOOTSTRAP, "--describe", "--group", group],
                    config_path,
                ),
                timeout=45,
            )
        if semantic_enabled("security-negative") and sasl_negative_enabled():
            bad_config_path = kafka_cli_security_config_path(password_override=bad_sasl_password())
            try:
                run_expect_failure(
                    kafka_cli_command_config(
                        ["kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP],
                        bad_config_path,
                    ),
                    timeout=45,
                )
            finally:
                if bad_config_path is not None:
                    try:
                        os.unlink(bad_config_path)
                    except FileNotFoundError:
                        pass
        if semantic_enabled("security-negative") and tls_negative_enabled():
            bad_config_path = kafka_cli_security_config_path(ssl_ca_override=BAD_SSL_CA_LOCATION)
            try:
                run_expect_failure(
                    kafka_cli_command_config(
                        ["kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP],
                        bad_config_path,
                    ),
                    timeout=45,
                )
            finally:
                if bad_config_path is not None:
                    try:
                        os.unlink(bad_config_path)
                    except FileNotFoundError:
                        pass
        if semantic_enabled("security-negative") and oauth_jaas_negative_enabled():
            bad_config_path = kafka_cli_security_config_path(jaas_config_override=BAD_OAUTH_JAAS_CONFIG)
            try:
                run_expect_failure(
                    kafka_cli_command_config(
                        ["kafka-broker-api-versions.sh", "--bootstrap-server", BOOTSTRAP],
                        bad_config_path,
                    ),
                    timeout=45,
                )
            finally:
                if bad_config_path is not None:
                    try:
                        os.unlink(bad_config_path)
                    except FileNotFoundError:
                        pass
        if semantic_enabled("security-negative") and acl_negative_enabled():
            denied_cmd = ["kafka-console-producer.sh", "--bootstrap-server", BOOTSTRAP, "--topic", ACL_DENIED_TOPIC]
            if config_path is not None:
                denied_cmd += ["--producer.config", config_path]
            run_expect_failure(denied_cmd, input_text=payload + "-denied\n", timeout=45)
        print(f"ok: kafka CLI probes ({semantics_csv()})")
    finally:
        if config_path is not None:
            try:
                os.unlink(config_path)
            except FileNotFoundError:
                pass


def test_kafka_python():
    if run_python_subtool("kafka-python"):
        print(f"ok: kafka-python delegated to {PYTHON}")
        return
    if not have_module("kafka"):
        raise MatrixError("kafka-python selected but import 'kafka' is not available")

    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    from kafka.structs import OffsetAndMetadata

    topic = matrix_topic("kafka-python")
    payload = matrix_payload("kafka-python").encode()
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP,
        client_id="zmq-client-matrix-kafka-python",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
        **kafka_python_security_config(),
    )
    try:
        topics = admin.list_topics()
        if topics is None:
            raise MatrixError("kafka-python list_topics returned None")
        if semantic_enabled("admin") or semantic_enabled("rebalance"):
            try:
                admin.create_topics(
                    [NewTopic(topic, num_partitions=2, replication_factor=1)],
                    timeout_ms=10000,
                )
            except TopicAlreadyExistsError:
                pass
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc) and "TopicAlreadyExists" not in str(exc):
                    raise
    finally:
        admin.close()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        client_id="zmq-client-matrix-kafka-python-producer",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
        **kafka_python_security_config(),
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
        **kafka_python_security_config(),
    )
    try:
        for message in consumer:
            if message.value == payload:
                tp = TopicPartition(message.topic, message.partition)
                expected_offset = message.offset + 1
                consumer.commit({tp: OffsetAndMetadata(expected_offset, "zmq-client-matrix")})
                committed = consumer.committed(tp)
                if committed != expected_offset:
                    raise MatrixError(
                        f"kafka-python committed offset mismatch: expected={expected_offset} got={committed}"
                    )
                if semantic_enabled("rebalance"):
                    test_kafka_python_rebalance(topic)
                if semantic_enabled("security-negative"):
                    test_kafka_python_security_negative()
                    test_kafka_python_tls_negative()
                    test_kafka_python_oauth_negative()
                    test_kafka_python_acl_negative(payload)
                print(f"ok: kafka-python probes ({semantics_csv()})")
                return
        raise MatrixError("kafka-python consumer did not fetch produced payload")
    finally:
        consumer.close()


def test_kafka_python_rebalance(topic):
    from kafka import KafkaConsumer

    group = f"zmq-client-matrix-kafka-python-rebalance-{os.getpid()}"
    consumers = [
        KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            client_id=f"zmq-client-matrix-kafka-python-rebalance-{idx}",
            group_id=group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000,
            **kafka_python_security_config(),
        )
        for idx in range(2)
    ]
    try:
        for consumer in consumers:
            consumer.subscribe([topic])
        deadline = time.time() + 20
        while time.time() < deadline:
            assignments = []
            for consumer in consumers:
                consumer.poll(timeout_ms=500)
                assignments.append(consumer.assignment())
            partitions = {
                tp.partition
                for assignment in assignments
                for tp in assignment
                if tp.topic == topic
            }
            if all(assignments) and partitions.issuperset({0, 1}):
                return
        raise MatrixError(f"kafka-python rebalance did not assign both partitions: {assignments}")
    finally:
        for consumer in consumers:
            consumer.close()


def test_kafka_python_security_negative():
    if not sasl_negative_enabled():
        return
    from kafka import KafkaAdminClient

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP,
            client_id="zmq-client-matrix-kafka-python-negative",
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000,
            **kafka_python_security_config(password_override=bad_sasl_password()),
        )
    except Exception:
        return
    try:
        try:
            admin.list_topics()
        except Exception:
            return
        raise MatrixError("kafka-python bad credentials unexpectedly succeeded")
    finally:
        admin.close()


def test_kafka_python_tls_negative():
    if not tls_negative_enabled():
        return
    from kafka import KafkaAdminClient

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP,
            client_id="zmq-client-matrix-kafka-python-bad-tls",
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000,
            **kafka_python_security_config(ssl_ca_override=BAD_SSL_CA_LOCATION),
        )
    except Exception:
        return
    try:
        try:
            admin.list_topics()
        except Exception:
            return
        raise MatrixError("kafka-python bad TLS trust unexpectedly succeeded")
    finally:
        admin.close()


def test_kafka_python_oauth_negative():
    if not oauth_token_negative_enabled():
        return
    from kafka import KafkaAdminClient

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP,
            client_id="zmq-client-matrix-kafka-python-bad-oauth",
            request_timeout_ms=10000,
            api_version_auto_timeout_ms=5000,
            **kafka_python_security_config(oauth_token_override=BAD_OAUTH_TOKEN),
        )
    except Exception:
        return
    try:
        try:
            admin.list_topics()
        except Exception:
            return
        raise MatrixError("kafka-python bad OAuth token unexpectedly succeeded")
    finally:
        admin.close()


def test_kafka_python_acl_negative(payload):
    if not acl_negative_enabled():
        return
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        client_id="zmq-client-matrix-kafka-python-acl-denied",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=5000,
        **kafka_python_security_config(),
    )
    try:
        try:
            producer.send(ACL_DENIED_TOPIC, payload + b"-denied").get(timeout=10)
            producer.flush(timeout=10)
        except Exception:
            return
        raise MatrixError("kafka-python ACL-denied produce unexpectedly succeeded")
    finally:
        producer.close()


def test_confluent_kafka():
    if run_python_subtool("confluent-kafka"):
        print(f"ok: confluent-kafka delegated to {PYTHON}")
        return
    if not have_module("confluent_kafka"):
        raise MatrixError("confluent-kafka selected but import 'confluent_kafka' is not available")

    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaException

    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-kafka",
        "socket.timeout.ms": 10000,
        **confluent_security_config(),
    })
    topic = matrix_topic("confluent-kafka")
    payload = matrix_payload("confluent-kafka").encode()
    metadata = admin.list_topics(timeout=10)
    if not metadata.brokers:
        raise MatrixError("confluent-kafka metadata response did not include brokers")
    if semantic_enabled("admin") or semantic_enabled("rebalance"):
        futures = admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
        for future in futures.values():
            try:
                future.result(timeout=10)
            except KafkaException as exc:
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-producer",
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
        **confluent_security_config(),
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
        **confluent_security_config(),
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
                expected_offset = message.offset() + 1
                consumer.commit(message=message, asynchronous=False)
                committed = consumer.committed([TopicPartition(topic, message.partition())], timeout=10)
                if not committed or committed[0].offset < expected_offset:
                    raise MatrixError(
                        f"confluent-kafka committed offset mismatch: expected>={expected_offset} got={committed}"
                    )
                if semantic_enabled("transactions"):
                    test_confluent_transaction(topic)
                if semantic_enabled("rebalance"):
                    test_confluent_rebalance(topic)
                if semantic_enabled("security-negative"):
                    test_confluent_security_negative()
                    test_confluent_tls_negative()
                    test_confluent_oauth_negative()
                    test_confluent_acl_negative(payload)
                print(f"ok: confluent-kafka probes ({semantics_csv()})")
                return
        raise MatrixError("confluent-kafka consumer did not fetch produced payload")
    finally:
        consumer.close()


def test_confluent_rebalance(topic):
    from confluent_kafka import Consumer

    group = f"zmq-client-matrix-confluent-rebalance-{os.getpid()}"
    consumers = [
        Consumer({
            "bootstrap.servers": BOOTSTRAP,
            "client.id": f"zmq-client-matrix-confluent-rebalance-{idx}",
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "socket.timeout.ms": 10000,
            **confluent_security_config(),
        })
        for idx in range(2)
    ]
    try:
        for consumer in consumers:
            consumer.subscribe([topic])
        deadline = time.time() + 20
        while time.time() < deadline:
            for consumer in consumers:
                consumer.poll(0.5)
            assignments = [consumer.assignment() for consumer in consumers]
            partitions = {
                tp.partition
                for assignment in assignments
                for tp in assignment
                if tp.topic == topic
            }
            if all(assignments) and partitions.issuperset({0, 1}):
                return
        raise MatrixError(f"confluent-kafka rebalance did not assign both partitions: {assignments}")
    finally:
        for consumer in consumers:
            consumer.close()


def test_confluent_security_negative():
    if not sasl_negative_enabled():
        return
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-negative",
        "socket.timeout.ms": 10000,
        **confluent_security_config(password_override=bad_sasl_password()),
    })
    try:
        admin.list_topics(timeout=10)
    except Exception:
        return
    raise MatrixError("confluent-kafka bad credentials unexpectedly succeeded")


def test_confluent_tls_negative():
    if not tls_negative_enabled():
        return
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-bad-tls",
        "socket.timeout.ms": 10000,
        **confluent_security_config(ssl_ca_override=BAD_SSL_CA_LOCATION),
    })
    try:
        admin.list_topics(timeout=10)
    except Exception:
        return
    raise MatrixError("confluent-kafka bad TLS trust unexpectedly succeeded")


def test_confluent_oauth_negative():
    if not oauth_token_negative_enabled():
        return
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-bad-oauth",
        "socket.timeout.ms": 10000,
        **confluent_security_config(oauth_token_override=BAD_OAUTH_TOKEN),
    })
    try:
        admin.list_topics(timeout=10)
    except Exception:
        return
    raise MatrixError("confluent-kafka bad OAuth token unexpectedly succeeded")


def test_confluent_acl_negative(payload):
    if not acl_negative_enabled():
        return
    from confluent_kafka import Producer

    failures = []

    def delivery_report(err, _msg):
        if err is not None:
            failures.append(str(err))

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-acl-denied",
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
        **confluent_security_config(),
    })
    producer.produce(ACL_DENIED_TOPIC, payload + b"-denied", callback=delivery_report)
    producer.flush(10)
    if failures:
        return
    raise MatrixError("confluent-kafka ACL-denied produce unexpectedly succeeded")


def test_confluent_transaction(topic):
    from confluent_kafka import Producer

    payload = f"{matrix_payload('confluent-kafka-txn')}-txn".encode()
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "zmq-client-matrix-confluent-transactional-producer",
        "transactional.id": f"zmq-client-matrix-confluent-txn-{os.getpid()}",
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
        **confluent_security_config(),
    })
    producer.init_transactions(15)
    producer.begin_transaction()
    producer.produce(topic, payload)
    if producer.flush(10) != 0:
        producer.abort_transaction(10)
        raise MatrixError("confluent-kafka transactional producer did not flush")
    producer.commit_transaction(15)


JAVA_MATRIX_SOURCE = r"""
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ZmqKafkaClientMatrix {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = args[1];
        String payload = args[2];
        String group = args[3];
        String semantics = args[4];

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        adminProps.put("client.id", "zmq-client-matrix-java-admin");
        adminProps.put("request.timeout.ms", "10000");
        applySecurity(adminProps);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (topics == null) {
                throw new RuntimeException("AdminClient listTopics returned null");
            }
            if (semantics.contains("admin") || semantics.contains("rebalance")) {
                try {
                    admin.createTopics(Collections.singleton(new NewTopic(topic, 2, (short) 1))).all().get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (!e.toString().contains("TopicExistsException") && !e.toString().contains("TOPIC_ALREADY_EXISTS")) {
                        throw e;
                    }
                }
            }
            if (semantics.contains("security-negative")) {
                runSecurityNegative(bootstrap);
                runTlsNegative(bootstrap);
                runAclNegative(bootstrap, payload + "-denied");
            }
        }

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        producerProps.put("client.id", "zmq-client-matrix-java-producer");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("acks", "1");
        producerProps.put("request.timeout.ms", "10000");
        producerProps.put("delivery.timeout.ms", "15000");
        applySecurity(producerProps);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, payload)).get(10, TimeUnit.SECONDS);
            producer.flush();
        }

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrap);
        consumerProps.put("client.id", "zmq-client-matrix-java-consumer");
        consumerProps.put("group.id", group);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("request.timeout.ms", "10000");
        consumerProps.put("session.timeout.ms", "6000");
        consumerProps.put("heartbeat.interval.ms", "2000");
        applySecurity(consumerProps);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 10000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (payload.equals(record.value())) {
                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        long expectedOffset = record.offset() + 1;
                        consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(expectedOffset)));
                        OffsetAndMetadata committed = consumer.committed(tp);
                        if (committed == null || committed.offset() < expectedOffset) {
                            throw new RuntimeException("Java committed offset mismatch");
                        }
                        if (semantics.contains("transactions")) {
                            runTransaction(bootstrap, topic, payload + "-txn");
                        }
                        if (semantics.contains("rebalance")) {
                            runRebalance(bootstrap, topic);
                        }
                        return;
                    }
                }
            }
            throw new RuntimeException("Java consumer did not fetch produced payload");
        }
    }

    private static void runTransaction(String bootstrap, String topic, String payload) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        producerProps.put("client.id", "zmq-client-matrix-java-transactional-producer");
        producerProps.put("transactional.id", "zmq-client-matrix-java-txn-" + System.currentTimeMillis());
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("request.timeout.ms", "10000");
        producerProps.put("delivery.timeout.ms", "15000");
        applySecurity(producerProps);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, payload)).get(10, TimeUnit.SECONDS);
            producer.commitTransaction();
        }
    }

    private static void runRebalance(String bootstrap, String topic) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrap);
        consumerProps.put("group.id", "zmq-client-matrix-java-rebalance-" + System.currentTimeMillis());
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("request.timeout.ms", "10000");
        consumerProps.put("session.timeout.ms", "6000");
        consumerProps.put("heartbeat.interval.ms", "2000");
        applySecurity(consumerProps);

        Properties firstProps = new Properties();
        firstProps.putAll(consumerProps);
        firstProps.put("client.id", "zmq-client-matrix-java-rebalance-0");
        Properties secondProps = new Properties();
        secondProps.putAll(consumerProps);
        secondProps.put("client.id", "zmq-client-matrix-java-rebalance-1");

        try (
            KafkaConsumer<String, String> first = new KafkaConsumer<>(firstProps);
            KafkaConsumer<String, String> second = new KafkaConsumer<>(secondProps)
        ) {
            first.subscribe(Collections.singletonList(topic));
            second.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < deadline) {
                first.poll(Duration.ofMillis(500));
                second.poll(Duration.ofMillis(500));
                Set<Integer> partitions = new HashSet<>();
                first.assignment().forEach(tp -> {
                    if (topic.equals(tp.topic())) partitions.add(tp.partition());
                });
                second.assignment().forEach(tp -> {
                    if (topic.equals(tp.topic())) partitions.add(tp.partition());
                });
                if (!first.assignment().isEmpty() && !second.assignment().isEmpty()
                    && partitions.contains(0) && partitions.contains(1)) {
                    return;
                }
            }
            throw new RuntimeException("Java rebalance did not assign both partitions");
        }
    }

    private static void runSecurityNegative(String bootstrap) throws Exception {
        String mechanism = System.getenv("ZMQ_CLIENT_MATRIX_SASL_MECHANISM");
        if ("OAUTHBEARER".equals(mechanism)) {
            runOAuthNegative(bootstrap);
            return;
        }
        String password = System.getenv("ZMQ_CLIENT_MATRIX_SASL_PASSWORD");
        if (password == null || password.isEmpty()) {
            return;
        }
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        adminProps.put("client.id", "zmq-client-matrix-java-negative");
        adminProps.put("request.timeout.ms", "10000");
        applySecurity(adminProps);
        adminProps.put("sasl.password", password + "-invalid");
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.listTopics().names().get(10, TimeUnit.SECONDS);
            throw new RuntimeException("Java bad credentials unexpectedly succeeded");
        } catch (Exception expected) {
            if (expected.toString().contains("unexpectedly succeeded")) {
                throw expected;
            }
        }
    }

    private static void runOAuthNegative(String bootstrap) throws Exception {
        String badJaas = System.getenv("ZMQ_CLIENT_MATRIX_BAD_OAUTH_JAAS_CONFIG");
        if (badJaas == null || badJaas.isEmpty()) {
            return;
        }
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        adminProps.put("client.id", "zmq-client-matrix-java-bad-oauth");
        adminProps.put("request.timeout.ms", "10000");
        applySecurity(adminProps);
        adminProps.put("sasl.jaas.config", badJaas);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.listTopics().names().get(10, TimeUnit.SECONDS);
            throw new RuntimeException("Java bad OAuth token unexpectedly succeeded");
        } catch (Exception expected) {
            if (expected.toString().contains("unexpectedly succeeded")) {
                throw expected;
            }
        }
    }

    private static void runTlsNegative(String bootstrap) throws Exception {
        String badCa = System.getenv("ZMQ_CLIENT_MATRIX_BAD_SSL_CA_LOCATION");
        String protocol = System.getenv("ZMQ_CLIENT_MATRIX_SECURITY_PROTOCOL");
        if (badCa == null || badCa.isEmpty() || protocol == null || !protocol.contains("SSL")) {
            return;
        }
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        adminProps.put("client.id", "zmq-client-matrix-java-bad-tls");
        adminProps.put("request.timeout.ms", "10000");
        applySecurity(adminProps);
        adminProps.put("ssl.truststore.location", badCa);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.listTopics().names().get(10, TimeUnit.SECONDS);
            throw new RuntimeException("Java bad TLS trust unexpectedly succeeded");
        } catch (Exception expected) {
            if (expected.toString().contains("unexpectedly succeeded")) {
                throw expected;
            }
        }
    }

    private static void runAclNegative(String bootstrap, String payload) throws Exception {
        String deniedTopic = System.getenv("ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC");
        if (deniedTopic == null || deniedTopic.isEmpty()) {
            return;
        }
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        producerProps.put("client.id", "zmq-client-matrix-java-acl-denied");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("acks", "1");
        producerProps.put("request.timeout.ms", "10000");
        producerProps.put("delivery.timeout.ms", "15000");
        applySecurity(producerProps);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(deniedTopic, payload)).get(10, TimeUnit.SECONDS);
            throw new RuntimeException("Java ACL-denied produce unexpectedly succeeded");
        } catch (Exception expected) {
            if (expected.toString().contains("unexpectedly succeeded")) {
                throw expected;
            }
        }
    }

    private static void applySecurity(Properties props) {
        putEnv(props, "security.protocol", "ZMQ_CLIENT_MATRIX_SECURITY_PROTOCOL");
        putEnv(props, "sasl.mechanism", "ZMQ_CLIENT_MATRIX_SASL_MECHANISM");
        putEnv(props, "sasl.username", "ZMQ_CLIENT_MATRIX_SASL_USERNAME");
        putEnv(props, "sasl.password", "ZMQ_CLIENT_MATRIX_SASL_PASSWORD");
        putEnv(props, "sasl.jaas.config", "ZMQ_CLIENT_MATRIX_OAUTH_JAAS_CONFIG");
        putEnv(props, "ssl.truststore.location", "ZMQ_CLIENT_MATRIX_SSL_CA_LOCATION");
    }

    private static void putEnv(Properties props, String key, String env) {
        String value = System.getenv(env);
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
"""


def test_java_kafka():
    if not have("javac") or not have("java"):
        raise MatrixError("java-kafka selected but javac/java are not available in PATH")
    if not JAVA_CLASSPATH:
        raise MatrixError("java-kafka selected but ZMQ_CLIENT_MATRIX_JAVA_CLASSPATH is not set")

    topic = matrix_topic("java-kafka")
    payload = matrix_payload("java-kafka")
    group = f"zmq-client-matrix-java-{os.getpid()}"

    with tempfile.TemporaryDirectory(prefix="zmq-client-matrix-java-") as tmp:
        source_path = os.path.join(tmp, "ZmqKafkaClientMatrix.java")
        with open(source_path, "w", encoding="utf-8") as f:
            f.write(JAVA_MATRIX_SOURCE)

        run(["javac", "-cp", JAVA_CLASSPATH, source_path], timeout=60)
        run(
            [
                "java",
                "-cp",
                f"{tmp}:{JAVA_CLASSPATH}",
                "ZmqKafkaClientMatrix",
                BOOTSTRAP,
                topic,
                payload,
                group,
                semantics_csv(),
            ],
            timeout=90,
            env=active_security_env(),
        )
    print(f"ok: java-kafka probes ({semantics_csv()})")


GO_MATRIX_SOURCE = r"""
package main

import (
    "context"
    "fmt"
    "os"
    "strings"
    "time"

    "github.com/segmentio/kafka-go"
)

func main() {
    if len(os.Args) != 6 {
        panic("usage: go-matrix <bootstrap> <topic> <payload> <group> <semantics>")
    }
    bootstrap := os.Args[1]
    topic := os.Args[2]
    payload := os.Args[3]
    group := os.Args[4]
    semantics := os.Args[5]

    conn, err := kafka.Dial("tcp", bootstrap)
    if err != nil {
        panic(err)
    }
    partitions, err := conn.ReadPartitions()
    if err != nil {
        _ = conn.Close()
        panic(err)
    }
    _ = partitions
    if strings.Contains(semantics, "admin") {
        if err := conn.CreateTopics(kafka.TopicConfig{
            Topic:             topic,
            NumPartitions:     2,
            ReplicationFactor: 1,
        }); err != nil && !strings.Contains(strings.ToLower(err.Error()), "exist") {
            _ = conn.Close()
            panic(err)
        }
    }
    _ = conn.Close()

    writer := &kafka.Writer{
        Addr:         kafka.TCP(bootstrap),
        Topic:        topic,
        RequiredAcks: kafka.RequireOne,
        BatchTimeout: 10 * time.Millisecond,
    }
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()
    if err := writer.WriteMessages(ctx, kafka.Message{Value: []byte(payload)}); err != nil {
        _ = writer.Close()
        panic(err)
    }
    if err := writer.Close(); err != nil {
        panic(err)
    }

    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{bootstrap},
        Topic:   topic,
        GroupID: group,
        MinBytes: 1,
        MaxBytes: 10000000,
        MaxWait:  500 * time.Millisecond,
    })
    defer reader.Close()

    deadline, done := context.WithTimeout(context.Background(), 15*time.Second)
    defer done()
    for {
        message, err := reader.FetchMessage(deadline)
        if err != nil {
            panic(err)
        }
        if string(message.Value) == payload {
            commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
            if err := reader.CommitMessages(commitCtx, message); err != nil {
                commitCancel()
                panic(err)
            }
            commitCancel()
            fmt.Println("ok")
            return
        }
    }
}
"""


def test_go_kafka():
    if not have("go"):
        raise MatrixError("go-kafka selected but go is not available in PATH")

    topic = matrix_topic("go-kafka")
    payload = matrix_payload("go-kafka")
    group = f"zmq-client-matrix-go-{os.getpid()}"

    with tempfile.TemporaryDirectory(prefix="zmq-client-matrix-go-") as tmp:
        source_path = os.path.join(tmp, "main.go")
        with open(source_path, "w", encoding="utf-8") as f:
            f.write(textwrap.dedent(GO_MATRIX_SOURCE).lstrip())

        run(["go", "mod", "init", "zmq-client-matrix-go"], timeout=30, cwd=tmp)
        run(["go", "get", GO_MODULE], timeout=120, cwd=tmp)
        run(["go", "run", ".", BOOTSTRAP, topic, payload, group, semantics_csv()], timeout=120, cwd=tmp)
    print(f"ok: go-kafka probes ({semantics_csv()})")


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_CLIENT_MATRIX=1 to run real-client matrix")
        return 0

    profiles = profile_names()
    validate_required_profiles(profiles)
    for profile in profiles:
        apply_profile(profile)
        run_profile()

    print(f"ok: client matrix passed for {', '.join(profiles)} profile(s)")
    return 0


def run_profile():
    tools = selected_tools()
    if not tools:
        raise MatrixError(
            "no supported Kafka client tools found; install kcat or Kafka CLI, "
            "or set ZMQ_CLIENT_MATRIX_TOOLS explicitly"
        )

    for tool in tools:
        ensure_tool_supports_semantics(tool)
        if tool == "kcat":
            test_kcat()
        elif tool == "kafka-cli":
            test_kafka_cli()
        elif tool == "kafka-python":
            test_kafka_python()
        elif tool == "confluent-kafka":
            test_confluent_kafka()
        elif tool == "java-kafka":
            test_java_kafka()
        elif tool == "go-kafka":
            test_go_kafka()
        else:
            raise MatrixError(f"unknown client matrix tool: {tool}")

    print(f"ok: client matrix profile {ACTIVE_PROFILE} passed for {', '.join(tools)} against {BOOTSTRAP}")


def self_test():
    old_env = os.environ.copy()
    try:
        os.environ["ZMQ_CLIENT_MATRIX_PROFILES"] = "apache_3_7, go_1_21"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_TOOLS"] = "java-kafka"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_JAVA_CLASSPATH"] = "/opt/kafka-3.7/libs/*"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SEMANTICS"] = (
            "admin,rebalance,transactions,security,security-negative"
        )
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SASL_MECHANISM"] = "PLAIN"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SASL_USERNAME"] = "matrix-user"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SASL_PASSWORD"] = "matrix-pass"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_BAD_SSL_CA_LOCATION"] = "/tmp/matrix-bad-ca.pem"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_ACL_DENIED_TOPIC"] = "matrix-denied-topic"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_TOOLS"] = "go-kafka"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_GO_MODULE"] = "github.com/segmentio/kafka-go@v0.4.47"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "admin,groups"
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_PROFILES"] = "apache_3_7,go_1_21"
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES"] = "apache_3_7"
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES"] = "apache_3_7"

        names = profile_names()
        if names != ["apache_3_7", "go_1_21"]:
            raise MatrixError(f"profile parsing failed: {names}")
        validate_required_profiles(names)

        apply_profile("apache_3_7")
        if TOOLS != "java-kafka" or JAVA_CLASSPATH != "/opt/kafka-3.7/libs/*":
            raise MatrixError("java profile override failed")
        if semantics_csv() != "basic,admin,rebalance,transactions,security,security-negative":
            raise MatrixError(f"java semantics override failed: {semantics_csv()}")
        if not security_enabled():
            raise MatrixError("security semantic did not enable security config")
        props = security_properties()
        if props.get("security.protocol") != "SASL_PLAINTEXT" or props.get("sasl.mechanism") != "PLAIN":
            raise MatrixError(f"security property override failed: {props}")
        if not security_negative_configured() or BAD_SSL_CA_LOCATION != "/tmp/matrix-bad-ca.pem":
            raise MatrixError("security-negative vector profile override failed")
        if ACL_DENIED_TOPIC != "matrix-denied-topic":
            raise MatrixError("ACL negative topic profile override failed")
        java_env = active_security_env()
        if java_env.get("ZMQ_CLIENT_MATRIX_ACL_DENIED_TOPIC") != "matrix-denied-topic":
            raise MatrixError("active Java security environment self-test failed")
        kafka_props_path = kafka_cli_security_config_path()
        try:
            with open(kafka_props_path, "r", encoding="utf-8") as f:
                kafka_props = f.read()
            if "security.protocol=SASL_PLAINTEXT" not in kafka_props:
                raise MatrixError("Kafka CLI security config self-test failed")
        finally:
            os.unlink(kafka_props_path)

        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SASL_MECHANISM"] = "OAUTHBEARER"
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_OAUTH_JAAS_CONFIG"] = (
            'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required '
            'unsecuredLoginStringClaim_sub="matrix-user";'
        )
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_BAD_OAUTH_JAAS_CONFIG"] = (
            'org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required '
            'unsecuredLoginStringClaim_sub="matrix-user" unsecuredLoginNumberClaim_exp="1000";'
        )
        apply_profile("apache_3_7")
        if not oauth_enabled() or not oauth_positive_configured_for_tool("java-kafka"):
            raise MatrixError("Java OAuth positive fixture self-test failed")
        if not security_negative_configured_for_tool("java-kafka"):
            raise MatrixError("Java OAuth negative fixture self-test failed")
        java_env = active_security_env()
        if "unsecuredLoginStringClaim_sub" not in java_env.get("ZMQ_CLIENT_MATRIX_OAUTH_JAAS_CONFIG", ""):
            raise MatrixError("Java OAuth environment self-test failed")
        oauth_props_path = kafka_cli_security_config_path()
        try:
            with open(oauth_props_path, "r", encoding="utf-8") as f:
                oauth_props = f.read()
            if "sasl.mechanism=OAUTHBEARER" not in oauth_props or "sasl.jaas.config=" not in oauth_props:
                raise MatrixError("Kafka CLI OAuth security config self-test failed")
        finally:
            os.unlink(oauth_props_path)
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES"] = "apache_3_7"
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES"] = "apache_3_7"
        validate_required_profiles(names)
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES"] = "go_1_21"
        try:
            validate_required_profiles(names)
            raise MatrixError("non-OAuth required client profile was accepted")
        except MatrixError as exc:
            if "required OAuth profiles" not in str(exc):
                raise
        os.environ.pop("ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_PROFILES", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_REQUIRED_OAUTH_NEGATIVE_PROFILES", None)
        os.environ["ZMQ_CLIENT_MATRIX_APACHE_3_7_SASL_MECHANISM"] = "PLAIN"

        apply_profile("go_1_21")
        if TOOLS != "go-kafka" or GO_MODULE != "github.com/segmentio/kafka-go@v0.4.47":
            raise MatrixError("go profile override failed")
        if semantics_csv() != "basic,admin,groups":
            raise MatrixError(f"go semantics override failed: {semantics_csv()}")

        if parse_semantics("all") != set(SEMANTIC_ORDER):
            raise MatrixError("semantic all parsing failed")
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "rebalance"
        apply_profile("go_1_21")
        try:
            ensure_tool_supports_semantics("go-kafka")
            raise MatrixError("unsupported rebalance tool was accepted")
        except MatrixError as exc:
            if "rebalance probes" not in str(exc):
                raise
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "security"
        apply_profile("go_1_21")
        try:
            ensure_tool_supports_semantics("go-kafka")
            raise MatrixError("unsupported security tool was accepted")
        except MatrixError as exc:
            if "security interop probes" not in str(exc):
                raise
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "security-negative"
        apply_profile("go_1_21")
        try:
            ensure_tool_supports_semantics("go-kafka")
            raise MatrixError("unsupported security-negative tool was accepted")
        except MatrixError as exc:
            if "security interop probes" not in str(exc):
                raise
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "admin,groups"
        apply_profile("go_1_21")
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_TOOLS"] = "kcat"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "security-negative"
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_BAD_SSL_CA_LOCATION", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_ACL_DENIED_TOPIC", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_BAD_OAUTH_TOKEN", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_BAD_OAUTH_JAAS_CONFIG", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_BAD_OAUTHBEARER_CONFIG", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_SASL_PASSWORD", None)
        apply_profile("go_1_21")
        try:
            ensure_tool_supports_semantics("kcat")
            raise MatrixError("security-negative without vectors was accepted")
        except MatrixError as exc:
            if "at least one configured negative vector" not in str(exc):
                raise
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_TOOLS"] = "kafka-python"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "security,security-negative"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SECURITY_PROTOCOL"] = "SASL_PLAINTEXT"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SASL_MECHANISM"] = "OAUTHBEARER"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_OAUTH_TOKEN"] = (
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6OTk5OTk5OTk5OX0."
        )
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_BAD_OAUTH_TOKEN"] = (
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJtYXRyaXgtdXNlciIsImV4cCI6MTAwMH0."
        )
        apply_profile("go_1_21")
        ensure_tool_supports_semantics("kafka-python")
        oauth_config = kafka_python_security_config()
        provider = oauth_config.get("sasl_oauth_token_provider")
        if provider is None or provider.token() != OAUTH_TOKEN:
            raise MatrixError("kafka-python OAuth token provider self-test failed")
        if not security_negative_configured_for_tool("kafka-python"):
            raise MatrixError("kafka-python OAuth negative vector self-test failed")
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_TOOLS"] = "go-kafka"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_SEMANTICS"] = "admin,groups"
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_SECURITY_PROTOCOL", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_SASL_MECHANISM", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_OAUTH_TOKEN", None)
        os.environ.pop("ZMQ_CLIENT_MATRIX_GO_1_21_BAD_OAUTH_TOKEN", None)
        apply_profile("go_1_21")
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES"] = "apache_3_7,go_1_21"
        try:
            validate_required_profiles(names)
            raise MatrixError("unsecured required client profile was accepted")
        except MatrixError as exc:
            if "secured-client profiles" not in str(exc):
                raise
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_PROFILES"] = "apache_3_7"
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES"] = "go_1_21"
        try:
            validate_required_profiles(names)
            raise MatrixError("negative-security profile without vectors was accepted")
        except MatrixError as exc:
            if "negative-security profiles" not in str(exc):
                raise
        os.environ["ZMQ_CLIENT_MATRIX_REQUIRED_SECURITY_NEGATIVE_PROFILES"] = "apache_3_7"
        try:
            parse_semantics("basic,unknown")
            raise MatrixError("unknown semantic probe was accepted")
        except MatrixError as exc:
            if "unknown client matrix semantic probe" not in str(exc):
                raise

        print("ok: client matrix profile self-test")
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

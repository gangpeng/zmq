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
    ZMQ_CLIENT_MATRIX_PROFILES    Comma-separated profile names for explicit client/library version sets
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
"""

import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap


RUN_ENABLED = os.environ.get("ZMQ_RUN_CLIENT_MATRIX") == "1"
BOOTSTRAP = os.environ.get("ZMQ_CLIENT_MATRIX_BOOTSTRAP", "localhost:9092")
TOOLS = os.environ.get("ZMQ_CLIENT_MATRIX_TOOLS", "auto")
JAVA_CLASSPATH = os.environ.get("ZMQ_CLIENT_MATRIX_JAVA_CLASSPATH")
ENABLE_GO_AUTO = os.environ.get("ZMQ_CLIENT_MATRIX_ENABLE_GO") == "1"
GO_MODULE = os.environ.get("ZMQ_CLIENT_MATRIX_GO_MODULE", "github.com/segmentio/kafka-go@latest")
PYTHON = os.environ.get("ZMQ_CLIENT_MATRIX_PYTHON", sys.executable)
ACTIVE_PROFILE = "default"


class MatrixError(Exception):
    pass


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


def profile_setting(profile, suffix, fallback):
    if profile == "default":
        return os.environ.get(f"ZMQ_CLIENT_MATRIX_{suffix}", fallback)
    return os.environ.get(profile_env_name(profile, suffix), os.environ.get(f"ZMQ_CLIENT_MATRIX_{suffix}", fallback))


def apply_profile(profile):
    global ACTIVE_PROFILE, BOOTSTRAP, TOOLS, JAVA_CLASSPATH, ENABLE_GO_AUTO, GO_MODULE, PYTHON

    ACTIVE_PROFILE = profile
    BOOTSTRAP = profile_setting(profile, "BOOTSTRAP", "localhost:9092")
    TOOLS = profile_setting(profile, "TOOLS", "auto")
    JAVA_CLASSPATH = profile_setting(profile, "JAVA_CLASSPATH", None)
    ENABLE_GO_AUTO = profile_setting(profile, "ENABLE_GO", "0") == "1"
    GO_MODULE = profile_setting(profile, "GO_MODULE", "github.com/segmentio/kafka-go@latest")
    PYTHON = profile_setting(profile, "PYTHON", sys.executable)


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
    env["ZMQ_CLIENT_MATRIX_ENABLE_GO"] = "0"
    env["ZMQ_CLIENT_MATRIX_PYTHON"] = PYTHON
    env.pop("ZMQ_CLIENT_MATRIX_PROFILES", None)

    run([PYTHON, __file__], timeout=120, env=env)
    return True


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
        timeout=45,
    )
    describe_out = run(["kafka-topics.sh", "--bootstrap-server", BOOTSTRAP, "--describe", "--topic", topic], timeout=45)
    if topic not in describe_out:
        raise MatrixError(f"kafka-topics describe output did not include {topic!r}\n{describe_out}")

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
    print("ok: kafka CLI API/topic admin and produce/fetch probes")


def test_kafka_python():
    if run_python_subtool("kafka-python"):
        print(f"ok: kafka-python delegated to {PYTHON}")
        return
    if not have_module("kafka"):
        raise MatrixError("kafka-python selected but import 'kafka' is not available")

    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.structs import OffsetAndMetadata

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
                tp = TopicPartition(message.topic, message.partition)
                expected_offset = message.offset + 1
                consumer.commit({tp: OffsetAndMetadata(expected_offset, "zmq-client-matrix")})
                committed = consumer.committed(tp)
                if committed != expected_offset:
                    raise MatrixError(
                        f"kafka-python committed offset mismatch: expected={expected_offset} got={committed}"
                    )
                print("ok: kafka-python metadata, produce/fetch, and offset commit probes")
                return
        raise MatrixError("kafka-python consumer did not fetch produced payload")
    finally:
        consumer.close()


def test_confluent_kafka():
    if run_python_subtool("confluent-kafka"):
        print(f"ok: confluent-kafka delegated to {PYTHON}")
        return
    if not have_module("confluent_kafka"):
        raise MatrixError("confluent-kafka selected but import 'confluent_kafka' is not available")

    from confluent_kafka import Consumer, Producer, TopicPartition
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
                expected_offset = message.offset() + 1
                consumer.commit(message=message, asynchronous=False)
                committed = consumer.committed([TopicPartition(topic, message.partition())], timeout=10)
                if not committed or committed[0].offset < expected_offset:
                    raise MatrixError(
                        f"confluent-kafka committed offset mismatch: expected>={expected_offset} got={committed}"
                    )
                print("ok: confluent-kafka metadata, produce/fetch, and offset commit probes")
                return
        raise MatrixError("confluent-kafka consumer did not fetch produced payload")
    finally:
        consumer.close()


JAVA_MATRIX_SOURCE = r"""
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
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

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        adminProps.put("client.id", "zmq-client-matrix-java-admin");
        adminProps.put("request.timeout.ms", "10000");
        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> topics = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            if (topics == null) {
                throw new RuntimeException("AdminClient listTopics returned null");
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
                        return;
                    }
                }
            }
            throw new RuntimeException("Java consumer did not fetch produced payload");
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
            ["java", "-cp", f"{tmp}:{JAVA_CLASSPATH}", "ZmqKafkaClientMatrix", BOOTSTRAP, topic, payload, group],
            timeout=90,
        )
    print("ok: java-kafka metadata, produce/fetch, and offset commit probes")


GO_MATRIX_SOURCE = r"""
package main

import (
    "context"
    "fmt"
    "os"
    "time"

    "github.com/segmentio/kafka-go"
)

func main() {
    if len(os.Args) != 5 {
        panic("usage: go-matrix <bootstrap> <topic> <payload> <group>")
    }
    bootstrap := os.Args[1]
    topic := os.Args[2]
    payload := os.Args[3]
    group := os.Args[4]

    conn, err := kafka.Dial("tcp", bootstrap)
    if err != nil {
        panic(err)
    }
    partitions, err := conn.ReadPartitions()
    _ = conn.Close()
    if err != nil {
        panic(err)
    }
    _ = partitions

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
        run(["go", "run", ".", BOOTSTRAP, topic, payload, group], timeout=120, cwd=tmp)
    print("ok: go-kafka metadata, produce/fetch, and offset commit probes")


def main():
    if not RUN_ENABLED:
        print("skip: set ZMQ_RUN_CLIENT_MATRIX=1 to run real-client matrix")
        return 0

    profiles = profile_names()
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
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_TOOLS"] = "go-kafka"
        os.environ["ZMQ_CLIENT_MATRIX_GO_1_21_GO_MODULE"] = "github.com/segmentio/kafka-go@v0.4.47"

        names = profile_names()
        if names != ["apache_3_7", "go_1_21"]:
            raise MatrixError(f"profile parsing failed: {names}")

        apply_profile("apache_3_7")
        if TOOLS != "java-kafka" or JAVA_CLASSPATH != "/opt/kafka-3.7/libs/*":
            raise MatrixError("java profile override failed")

        apply_profile("go_1_21")
        if TOOLS != "go-kafka" or GO_MODULE != "github.com/segmentio/kafka-go@v0.4.47":
            raise MatrixError("go profile override failed")

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

#!/usr/bin/env python3
"""
ZMQ — Minimal Kafka produce/consume demo using raw wire protocol.

Usage:
  python3 demo.py produce <topic> <message> [--broker host:port]
  python3 demo.py consume <topic>           [--broker host:port]
  python3 demo.py create  <topic>           [--broker host:port] [--partitions N]
  python3 demo.py topics                    [--broker host:port]

Examples:
  python3 demo.py create  my-topic
  python3 demo.py produce my-topic "Hello from ZMQ!"
  python3 demo.py consume my-topic
  python3 demo.py topics
"""

import socket
import struct
import sys
import time

# ---------------------------------------------------------------------------
# Low-level Kafka wire protocol helpers
# ---------------------------------------------------------------------------

def kafka_request(sock, api_key, api_version, correlation_id, body=b''):
    """Send a Kafka request with v1 header (non-flexible)."""
    client_id = b'zmq-demo'
    header = struct.pack('>hhih', api_key, api_version, correlation_id, len(client_id))
    header += client_id
    frame_body = header + body
    frame = struct.pack('>I', len(frame_body)) + frame_body
    sock.send(frame)
    time.sleep(0.15)
    resp = b''
    while True:
        try:
            chunk = sock.recv(65536)
            if not chunk:
                break
            resp += chunk
            if len(resp) >= 4:
                expected = struct.unpack('>I', resp[:4])[0] + 4
                if len(resp) >= expected:
                    break
        except socket.timeout:
            break
    if len(resp) < 4:
        return None
    size = struct.unpack('>I', resp[:4])[0]
    return resp[4:4+size]


def connect(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    s.connect((host, port))
    return s


def build_record_batch(value):
    """Build a minimal Kafka v2 RecordBatch containing a single record."""
    if isinstance(value, str):
        value = value.encode()

    # Build the inner record (v2 format)
    # Record: length(varint) + attributes(1) + timestampDelta(varint) +
    #         offsetDelta(varint) + keyLength(varint) + key + valueLength(varint) + value +
    #         headersCount(varint)
    record = b''
    record += b'\x00'             # attributes
    record += b'\x00'             # timestamp delta (varint 0)
    record += b'\x00'             # offset delta (varint 0)
    record += b'\x01'             # key length = -1 (null) as varint
    # value length as varint
    record += encode_varint(len(value))
    record += value
    record += b'\x00'             # headers count (varint 0)

    # Record length prefix (varint)
    record_with_len = encode_varint(len(record)) + record

    records_count = 1
    timestamp = int(time.time() * 1000)

    # RecordBatch header (bytes 0..60 before records):
    # baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1) +
    # crc(4) + attributes(2) + lastOffsetDelta(4) + firstTimestamp(8) +
    # maxTimestamp(8) + producerId(8) + producerEpoch(2) + baseSequence(4) +
    # recordsCount(4)
    # Total header = 61 bytes, then records follow

    # We'll compute batchLength after building the rest
    # Everything after batchLength field is included in batchLength
    after_batch_len = b''
    after_batch_len += struct.pack('>i', -1)                   # partitionLeaderEpoch
    after_batch_len += struct.pack('>b', 2)                    # magic (v2)
    crc_placeholder_pos = len(after_batch_len)
    after_batch_len += struct.pack('>I', 0)                    # crc (placeholder)
    crc_start = len(after_batch_len)
    after_batch_len += struct.pack('>h', 0)                    # attributes
    after_batch_len += struct.pack('>i', 0)                    # lastOffsetDelta
    after_batch_len += struct.pack('>q', timestamp)            # firstTimestamp
    after_batch_len += struct.pack('>q', timestamp)            # maxTimestamp
    after_batch_len += struct.pack('>q', -1)                   # producerId
    after_batch_len += struct.pack('>h', -1)                   # producerEpoch
    after_batch_len += struct.pack('>i', -1)                   # baseSequence
    after_batch_len += struct.pack('>i', records_count)        # records count
    after_batch_len += record_with_len

    batch_length = len(after_batch_len)

    batch = b''
    batch += struct.pack('>q', 0)             # baseOffset
    batch += struct.pack('>i', batch_length)   # batchLength
    batch += after_batch_len

    return batch


def encode_varint(value):
    """Encode a signed int as a zigzag varint."""
    # Kafka uses zigzag encoding for signed varints
    value = (value << 1) ^ (value >> 31)
    result = b''
    while value > 0x7F:
        result += bytes([(value & 0x7F) | 0x80])
        value >>= 7
    result += bytes([value & 0x7F])
    return result


def decode_varint(data, pos):
    """Decode a zigzag varint, return (value, new_pos)."""
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    # Zigzag decode
    result = (result >> 1) ^ -(result & 1)
    return result, pos


# ---------------------------------------------------------------------------
# Kafka API implementations
# ---------------------------------------------------------------------------

def create_topic(host, port, topic, num_partitions=3):
    """CreateTopics (API key 19, version 0)."""
    sock = connect(host, port)
    name_b = topic.encode()
    body = struct.pack('>i', 1) + struct.pack('>h', len(name_b)) + name_b
    body += struct.pack('>ih', num_partitions, 1) + struct.pack('>iii', 0, 0, 30000)
    body += struct.pack('>?', False)

    data = kafka_request(sock, 19, 0, 1, body)
    sock.close()

    if data is None or len(data) < 12:
        print("✗ No response from broker")
        return

    # Parse: skip correlation_id(4) + parse topics
    pos = 4
    num_topics = struct.unpack_from('>i', data, pos)[0]; pos += 4
    for _ in range(num_topics):
        name_len = struct.unpack_from('>h', data, pos)[0]; pos += 2
        name = data[pos:pos+name_len].decode(); pos += name_len
        error_code = struct.unpack_from('>h', data, pos)[0]; pos += 2
        if error_code == 0:
            print(f"✓ Topic '{name}' created successfully ({num_partitions} partitions)")
        elif error_code == 36:
            print(f"✓ Topic '{name}' already exists")
        else:
            print(f"✗ Topic '{name}' error: code {error_code}")


def produce_message(host, port, topic, message, partition=0):
    """Produce (API key 0, version 0) — single message using v2 RecordBatch."""
    sock = connect(host, port)
    topic_b = topic.encode()

    record_batch = build_record_batch(message)

    body = struct.pack('>hi', -1, 30000) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>ii', 1, partition)
    body += struct.pack('>i', len(record_batch)) + record_batch

    data = kafka_request(sock, 0, 0, 1, body)
    sock.close()

    if data is None or len(data) < 20:
        print("✗ No response from broker")
        return

    pos = 4 + 4  # skip corr + num_topics
    name_len = struct.unpack_from('>h', data, pos)[0]; pos += 2
    tname = data[pos:pos+name_len].decode(); pos += name_len
    pos += 4  # num_partitions
    part_id = struct.unpack_from('>i', data, pos)[0]; pos += 4
    error_code = struct.unpack_from('>h', data, pos)[0]; pos += 2
    base_offset = struct.unpack_from('>q', data, pos)[0]; pos += 8

    if error_code == 0:
        print(f"✓ Produced to {tname}[{part_id}] at offset {base_offset}: \"{message}\"")
    else:
        print(f"✗ Produce error on {tname}[{part_id}]: code {error_code}")


def fetch_messages(host, port, topic, partition=0, offset=0, max_bytes=1048576):
    """Fetch (API key 1, version 0) — read messages from a partition."""
    try:
        sock = connect(host, port)
    except Exception:
        return 0
    topic_b = topic.encode()

    body = struct.pack('>iii', -1, 5000, 1) + struct.pack('>i', 1)
    body += struct.pack('>h', len(topic_b)) + topic_b
    body += struct.pack('>i', 1) + struct.pack('>iqi', partition, offset, max_bytes)

    try:
        data = kafka_request(sock, 1, 0, 1, body)
    except (ConnectionError, OSError):
        data = None
    sock.close()

    if data is None or len(data) < 20:
        return 0

    pos = 4 + 4  # skip corr + num_topics
    name_len = struct.unpack_from('>h', data, pos)[0]; pos += 2
    tname = data[pos:pos+name_len].decode(); pos += name_len
    pos += 4  # num_partitions
    part_id = struct.unpack_from('>i', data, pos)[0]; pos += 4
    error_code = struct.unpack_from('>h', data, pos)[0]; pos += 2
    hw = struct.unpack_from('>q', data, pos)[0]; pos += 8
    rec_len = struct.unpack_from('>i', data, pos)[0]; pos += 4

    if error_code != 0:
        print(f"  {tname}[{part_id}]: error code {error_code}")
        return 0

    if rec_len <= 0:
        return 0

    records = data[pos:pos+rec_len]
    count = 0

    # Parse record batches (v2 format)
    rpos = 0
    while rpos + 61 <= len(records):
        base_offset_val = struct.unpack_from('>q', records, rpos)[0]; rpos += 8
        batch_length = struct.unpack_from('>i', records, rpos)[0]; rpos += 4

        if batch_length <= 0 or rpos + batch_length > len(records):
            break

        batch_end = rpos + batch_length

        # Skip: partitionLeaderEpoch(4) + magic(1) + crc(4) + attributes(2) +
        #        lastOffsetDelta(4) + firstTimestamp(8) + maxTimestamp(8) +
        #        producerId(8) + producerEpoch(2) + baseSequence(4)
        rpos += 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4
        records_count = struct.unpack_from('>i', records, rpos)[0]; rpos += 4

        for i in range(records_count):
            if rpos >= batch_end:
                break
            # Record: length(varint) + attributes(1) + timestampDelta(varint) +
            #         offsetDelta(varint) + keyLen(varint) + key + valueLen(varint) + value + headersCount(varint)
            rec_length, rpos = decode_varint(records, rpos)
            rec_end = rpos + rec_length

            rpos += 1  # attributes
            _, rpos = decode_varint(records, rpos)  # timestampDelta
            offset_delta, rpos = decode_varint(records, rpos)  # offsetDelta

            key_len, rpos = decode_varint(records, rpos)
            if key_len > 0:
                rpos += key_len

            value_len, rpos = decode_varint(records, rpos)
            if value_len > 0:
                value = records[rpos:rpos+value_len]
                rpos += value_len
                msg_offset = base_offset_val + offset_delta
                print(f"  offset={msg_offset}  \"{value.decode(errors='replace')}\"")
                count += 1
            else:
                rpos = max(rpos, rpos + value_len)

            # Skip to record end
            rpos = rec_end

        rpos = batch_end

    return count


def list_topics(host, port):
    """Metadata (API key 3, version 0) — list all topics."""
    sock = connect(host, port)
    body = struct.pack('>i', 0)
    data = kafka_request(sock, 3, 0, 1, body)
    sock.close()

    if data is None or len(data) < 8:
        print("✗ No response from broker")
        return

    pos = 4  # skip correlation_id
    # Brokers
    num_brokers = struct.unpack_from('>i', data, pos)[0]; pos += 4
    print(f"Brokers ({num_brokers}):")
    for _ in range(num_brokers):
        node_id = struct.unpack_from('>i', data, pos)[0]; pos += 4
        host_len = struct.unpack_from('>h', data, pos)[0]; pos += 2
        broker_host = data[pos:pos+host_len].decode(); pos += host_len
        broker_port = struct.unpack_from('>i', data, pos)[0]; pos += 4
        print(f"  node {node_id}: {broker_host}:{broker_port}")

    # Topics
    num_topics = struct.unpack_from('>i', data, pos)[0]; pos += 4
    if num_topics == 0:
        print("\nTopics: (none)")
    else:
        print(f"\nTopics ({num_topics}):")
        for _ in range(num_topics):
            if pos + 4 > len(data):
                break
            error = struct.unpack_from('>h', data, pos)[0]; pos += 2
            name_len = struct.unpack_from('>h', data, pos)[0]; pos += 2
            name = data[pos:pos+name_len].decode(); pos += name_len
            num_parts = struct.unpack_from('>i', data, pos)[0]; pos += 4
            print(f"  {name} ({num_parts} partitions)")
            for _ in range(num_parts):
                if pos + 10 > len(data):
                    break
                pos += 2  # error
                pos += 4  # partition_index
                pos += 4  # leader
                num_replicas = struct.unpack_from('>i', data, pos)[0]; pos += 4
                pos += max(num_replicas, 0) * 4
                num_isr = struct.unpack_from('>i', data, pos)[0]; pos += 4
                pos += max(num_isr, 0) * 4


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    broker = "localhost:9092"

    # Parse flags
    args = sys.argv[2:]
    filtered = []
    i = 0
    partitions = 3
    while i < len(args):
        if args[i] == "--broker" and i + 1 < len(args):
            broker = args[i + 1]; i += 2
        elif args[i] == "--partitions" and i + 1 < len(args):
            partitions = int(args[i + 1]); i += 2
        else:
            filtered.append(args[i]); i += 1

    host, port_str = broker.split(":")
    port = int(port_str)

    if cmd == "create":
        if not filtered:
            print("Usage: demo.py create <topic>"); sys.exit(1)
        create_topic(host, port, filtered[0], partitions)

    elif cmd == "produce":
        if len(filtered) < 2:
            print("Usage: demo.py produce <topic> <message>"); sys.exit(1)
        produce_message(host, port, filtered[0], filtered[1])

    elif cmd == "consume":
        if not filtered:
            print("Usage: demo.py consume <topic>"); sys.exit(1)
        topic = filtered[0]
        print(f"Fetching from {topic}:")
        total = 0
        for p in range(partitions):
            count = fetch_messages(host, port, topic, partition=p)
            total += count
        if total == 0:
            print("  (no messages found)")
        else:
            print(f"\n✓ Total: {total} message(s)")

    elif cmd == "topics":
        list_topics(host, port)

    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()

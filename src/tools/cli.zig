const std = @import("std");
const Allocator = std.mem.Allocator;
const ser = @import("../protocol/serialization.zig");
const KafkaClient = @import("client.zig").KafkaClient;
const TopicAdmin = @import("client.zig").TopicAdmin;

/// Kafka CLI — command-line interface for managing the broker.
///
/// Commands:
///   topics     — list, create, delete, describe topics
///   produce    — produce messages to a topic (console-producer)
///   consume    — consume messages from a topic (console-consumer)
///   groups     — list, describe consumer groups
///   configs    — describe, alter broker/topic configs
///   acls       — list, create, delete ACLs
///
/// Usage:
///   zmq-cli topics --list --bootstrap-server localhost:9092
///   zmq-cli produce --topic test --bootstrap-server localhost:9092
///   zmq-cli consume --topic test --group g1 --bootstrap-server localhost:9092

/// Parse "host:port" string.
fn parseBootstrapServer(server: []const u8) struct { host: []const u8, port: u16 } {
    if (std.mem.indexOf(u8, server, ":")) |colon| {
        const port_str = server[colon + 1 ..];
        const port = std.fmt.parseInt(u16, port_str, 10) catch 9092;
        return .{ .host = server[0..colon], .port = port };
    }
    return .{ .host = server, .port = 9092 };
}

/// List all topics from the broker.
pub fn listTopics(alloc: Allocator, host: []const u8, port: u16) !void {
    const stdout = std.io.getStdOut().writer();
    var client = try KafkaClient.init(alloc, host, port);
    defer client.close();
    try client.connect();

    // Metadata request v0: list all topics (0 topics = all)
    var body: [4]u8 = undefined;
    var pos: usize = 0;
    ser.writeI32(&body, &pos, 0); // 0 topics = request all

    const resp = try client.sendRequest(3, 0, body[0..pos]);
    defer alloc.free(resp);

    // Parse Metadata response: skip correlation_id(4) + brokers array
    var rpos: usize = 4;
    const num_brokers = ser.readI32(resp, &rpos);
    // Skip broker entries
    for (0..@as(usize, @intCast(num_brokers))) |_| {
        _ = ser.readI32(resp, &rpos); // node_id
        _ = try ser.readString(resp, &rpos); // host
        _ = ser.readI32(resp, &rpos); // port
    }

    // Topic metadata
    const num_topics = ser.readI32(resp, &rpos);
    try stdout.print("Topics ({d}):\n", .{num_topics});
    for (0..@as(usize, @intCast(num_topics))) |_| {
        const error_code = ser.readI16(resp, &rpos);
        const topic_name = (try ser.readString(resp, &rpos)) orelse "<null>";
        const num_partitions = ser.readI32(resp, &rpos);

        // Skip partition details
        for (0..@as(usize, @intCast(num_partitions))) |_| {
            _ = ser.readI16(resp, &rpos); // error_code
            _ = ser.readI32(resp, &rpos); // partition_index
            _ = ser.readI32(resp, &rpos); // leader_id
            const num_replicas = ser.readI32(resp, &rpos);
            rpos += @as(usize, @intCast(num_replicas)) * 4; // skip replica ids
            const num_isr = ser.readI32(resp, &rpos);
            rpos += @as(usize, @intCast(num_isr)) * 4; // skip isr ids
        }

        const status = if (error_code == 0) "" else " [ERROR]";
        try stdout.print("  {s} ({d} partitions){s}\n", .{ topic_name, num_partitions, status });
    }
}

/// Create a topic.
pub fn createTopic(alloc: Allocator, host: []const u8, port: u16, name: []const u8, partitions: i32, replication_factor: i16) !void {
    const stdout = std.io.getStdOut().writer();
    var client = try KafkaClient.init(alloc, host, port);
    defer client.close();
    try client.connect();

    var admin = TopicAdmin.init(&client);
    const result = try admin.createTopic(name, partitions, replication_factor);

    if (result.error_code == 0) {
        try stdout.print("Created topic '{s}' with {d} partitions\n", .{ name, partitions });
    } else if (result.error_code == 36) {
        try stdout.print("Topic '{s}' already exists\n", .{name});
    } else {
        try stdout.print("Error creating topic '{s}': error_code={d}\n", .{ name, result.error_code });
    }
}

/// Delete a topic.
pub fn deleteTopic(alloc: Allocator, host: []const u8, port: u16, name: []const u8) !void {
    const stdout = std.io.getStdOut().writer();
    var client = try KafkaClient.init(alloc, host, port);
    defer client.close();
    try client.connect();

    var body: [256]u8 = undefined;
    var pos: usize = 0;
    ser.writeArrayLen(&body, &pos, 1); // 1 topic
    ser.writeString(&body, &pos, name);
    ser.writeI32(&body, &pos, 30000); // timeout

    const resp = try client.sendRequest(20, 0, body[0..pos]);
    defer alloc.free(resp);

    try stdout.print("Deleted topic '{s}'\n", .{name});
}

/// Produce a single message to a topic.
pub fn produceMessage(alloc: Allocator, host: []const u8, port: u16, topic: []const u8, key: ?[]const u8, value: []const u8) !void {
    const stdout = std.io.getStdOut().writer();
    var client = try KafkaClient.init(alloc, host, port);
    defer client.close();
    try client.connect();

    // Build record batch for a single record
    var record_buf: [1024]u8 = undefined;
    var rpos: usize = 0;

    // Record batch header (simplified V2)
    std.mem.writeInt(i64, record_buf[rpos..][0..8], 0, .big); // baseOffset
    rpos += 8;
    const batch_len_pos = rpos;
    rpos += 4; // placeholder for batchLength
    std.mem.writeInt(i32, record_buf[rpos..][0..4], 0, .big); // partitionLeaderEpoch
    rpos += 4;
    record_buf[rpos] = 2; // magic
    rpos += 1;
    const crc_pos = rpos;
    rpos += 4; // placeholder for CRC
    std.mem.writeInt(i16, record_buf[rpos..][0..2], 0, .big); // attributes
    rpos += 2;
    std.mem.writeInt(i32, record_buf[rpos..][0..4], 0, .big); // lastOffsetDelta
    rpos += 4;
    std.mem.writeInt(i64, record_buf[rpos..][0..8], @import("time_compat").milliTimestamp(), .big); // baseTimestamp
    rpos += 8;
    std.mem.writeInt(i64, record_buf[rpos..][0..8], @import("time_compat").milliTimestamp(), .big); // maxTimestamp
    rpos += 8;
    std.mem.writeInt(i64, record_buf[rpos..][0..8], -1, .big); // producerId
    rpos += 8;
    std.mem.writeInt(i16, record_buf[rpos..][0..2], -1, .big); // producerEpoch
    rpos += 2;
    std.mem.writeInt(i32, record_buf[rpos..][0..4], -1, .big); // baseSequence
    rpos += 4;
    std.mem.writeInt(i32, record_buf[rpos..][0..4], 1, .big); // numRecords = 1
    rpos += 4;

    // Single record (varint encoded)
    const record_start = rpos;
    // Record: length(varint) + attributes(varint=0) + timestampDelta(varint=0) + offsetDelta(varint=0)
    //         + keyLength(varint) + key + valueLength(varint) + value + headersCount(varint=0)
    const key_len: i32 = if (key) |k| @intCast(k.len) else -1;

    // Calculate record body size first
    const record_body_size: usize = 1 + 1 + 1 + // attributes + timestampDelta + offsetDelta
        (if (key_len < 0) @as(usize, 1) else @as(usize, 1) + @as(usize, @intCast(key_len))) +
        1 + value.len + 1; // valueLen + value + headersCount

    // Write record length as varint
    ser.writeUnsignedVarint(&record_buf, &rpos, record_body_size);
    record_buf[rpos] = 0; rpos += 1; // attributes
    record_buf[rpos] = 0; rpos += 1; // timestampDelta
    record_buf[rpos] = 0; rpos += 1; // offsetDelta

    // Key
    if (key) |k| {
        ser.writeUnsignedVarint(&record_buf, &rpos, k.len);
        @memcpy(record_buf[rpos .. rpos + k.len], k);
        rpos += k.len;
    } else {
        ser.writeUnsignedVarint(&record_buf, &rpos, 0);
        rpos -= 1; // Undo the varint, write -1 instead
        record_buf[rpos] = 1; rpos += 1; // varint(-1) = 1 in zigzag
    }

    // Value
    ser.writeUnsignedVarint(&record_buf, &rpos, value.len);
    @memcpy(record_buf[rpos .. rpos + value.len], value);
    rpos += value.len;

    record_buf[rpos] = 0; rpos += 1; // 0 headers
    _ = record_start;

    // Fill in batchLength
    std.mem.writeInt(i32, record_buf[batch_len_pos..][0..4], @intCast(rpos - batch_len_pos - 4), .big);

    // Skip CRC for now (broker should handle records without strict CRC in produce)
    std.mem.writeInt(u32, record_buf[crc_pos..][0..4], 0, .big);

    // Build Produce request body
    var body: [2048]u8 = undefined;
    var bpos: usize = 0;
    ser.writeI16(&body, &bpos, 1); // acks = 1
    ser.writeI32(&body, &bpos, 30000); // timeout
    ser.writeArrayLen(&body, &bpos, 1); // 1 topic
    ser.writeString(&body, &bpos, topic);
    ser.writeArrayLen(&body, &bpos, 1); // 1 partition
    ser.writeI32(&body, &bpos, 0); // partition 0
    ser.writeI32(&body, &bpos, @intCast(rpos)); // record batch size
    @memcpy(body[bpos .. bpos + rpos], record_buf[0..rpos]);
    bpos += rpos;

    const resp = try client.sendRequest(0, 3, body[0..bpos]);
    defer alloc.free(resp);

    try stdout.print("Produced message to '{s}' partition 0\n", .{topic});
}

/// Describe consumer groups.
pub fn describeGroups(alloc: Allocator, host: []const u8, port: u16) !void {
    const stdout = std.io.getStdOut().writer();
    var client = try KafkaClient.init(alloc, host, port);
    defer client.close();
    try client.connect();

    // ListGroups request (API 16, version 0)
    const resp = try client.sendRequest(16, 0, "");
    defer alloc.free(resp);

    var rpos: usize = 4; // skip correlation_id
    const error_code = ser.readI16(resp, &rpos);
    if (error_code != 0) {
        try stdout.print("Error listing groups: {d}\n", .{error_code});
        return;
    }

    const num_groups = ser.readI32(resp, &rpos);
    try stdout.print("Consumer Groups ({d}):\n", .{num_groups});
    for (0..@as(usize, @intCast(num_groups))) |_| {
        const group_id = (try ser.readString(resp, &rpos)) orelse "<null>";
        const protocol_type = (try ser.readString(resp, &rpos)) orelse "";
        try stdout.print("  {s} ({s})\n", .{ group_id, protocol_type });
    }
}

/// Get broker API versions.
pub fn apiVersions(alloc: Allocator, host: []const u8, port: u16) !void {
    const stdout = std.io.getStdOut().writer();
    var client = try KafkaClient.init(alloc, host, port);
    defer client.close();
    try client.connect();

    const resp = try client.sendRequest(18, 0, "");
    defer alloc.free(resp);

    var rpos: usize = 4; // skip correlation_id
    const error_code = ser.readI16(resp, &rpos);
    if (error_code != 0) {
        try stdout.print("Error: {d}\n", .{error_code});
        return;
    }

    const num_apis = ser.readI32(resp, &rpos);
    try stdout.print("Supported APIs ({d}):\n", .{num_apis});
    for (0..@as(usize, @intCast(num_apis))) |_| {
        const api_key = ser.readI16(resp, &rpos);
        const min_version = ser.readI16(resp, &rpos);
        const max_version = ser.readI16(resp, &rpos);
        try stdout.print("  API {d:>3}: versions {d}-{d}\n", .{ api_key, min_version, max_version });
    }
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "parseBootstrapServer with port" {
    const result = parseBootstrapServer("localhost:9093");
    try testing.expectEqualStrings("localhost", result.host);
    try testing.expectEqual(@as(u16, 9093), result.port);
}

test "parseBootstrapServer without port" {
    const result = parseBootstrapServer("localhost");
    try testing.expectEqualStrings("localhost", result.host);
    try testing.expectEqual(@as(u16, 9092), result.port);
}

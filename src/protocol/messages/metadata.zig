const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ser = @import("../serialization.zig");

/// MetadataRequest (ApiKey 3)
///
/// Versions 0-12. Flexible from version 9+.
/// Used by clients to discover brokers, topics, and partition leaders.
pub const MetadataRequest = struct {
    topics: ?[]MetadataRequestTopic = null, // null = all topics (v1+)
    allow_auto_topic_creation: bool = true, // versions 4+
    include_cluster_authorized_operations: bool = false, // versions 8-10
    include_topic_authorized_operations: bool = false, // versions 8+

    pub const MetadataRequestTopic = struct {
        topic_id: [16]u8 = [_]u8{0} ** 16, // versions 10+
        name: ?[]const u8 = null,
    };

    pub fn deserialize(alloc: Allocator, buf: []const u8, pos: *usize, version: i16) !MetadataRequest {
        var result = MetadataRequest{};
        const flexible = version >= 9;

        // Topics array
        const topics_len = if (flexible)
            try ser.readCompactArrayLen(buf, pos)
        else
            try ser.readArrayLen(buf, pos);

        if (topics_len) |len| {
            result.topics = try alloc.alloc(MetadataRequestTopic, len);
            for (0..len) |i| {
                var topic = MetadataRequestTopic{};
                if (version >= 10) {
                    topic.topic_id = try ser.readUuid(buf, pos);
                }
                if (flexible) {
                    topic.name = try ser.readCompactString(buf, pos);
                } else {
                    topic.name = try ser.readString(buf, pos);
                }
                if (flexible) try ser.skipTaggedFields(buf, pos);
                result.topics.?[i] = topic;
            }
        }

        if (version >= 4) {
            result.allow_auto_topic_creation = try ser.readBool(buf, pos);
        }

        if (version >= 8 and version <= 10) {
            result.include_cluster_authorized_operations = try ser.readBool(buf, pos);
        }

        if (version >= 8) {
            result.include_topic_authorized_operations = try ser.readBool(buf, pos);
        }

        if (flexible) try ser.skipTaggedFields(buf, pos);

        return result;
    }

    pub fn serialize(self: *const MetadataRequest, buf: []u8, pos: *usize, version: i16) void {
        const flexible = version >= 9;

        // Topics array
        if (self.topics) |topics| {
            if (flexible) {
                ser.writeCompactArrayLen(buf, pos, topics.len);
            } else {
                ser.writeArrayLen(buf, pos, topics.len);
            }
            for (topics) |topic| {
                if (version >= 10) {
                    ser.writeUuid(buf, pos, topic.topic_id);
                }
                if (flexible) {
                    ser.writeCompactString(buf, pos, topic.name);
                } else {
                    ser.writeString(buf, pos, topic.name);
                }
                if (flexible) ser.writeEmptyTaggedFields(buf, pos);
            }
        } else {
            if (flexible) {
                ser.writeCompactArrayLen(buf, pos, null);
            } else {
                ser.writeArrayLen(buf, pos, null);
            }
        }

        if (version >= 4) {
            ser.writeBool(buf, pos, self.allow_auto_topic_creation);
        }
        if (version >= 8 and version <= 10) {
            ser.writeBool(buf, pos, self.include_cluster_authorized_operations);
        }
        if (version >= 8) {
            ser.writeBool(buf, pos, self.include_topic_authorized_operations);
        }
        if (flexible) ser.writeEmptyTaggedFields(buf, pos);
    }

    pub fn deinit(self: *MetadataRequest, alloc: Allocator) void {
        if (self.topics) |topics| {
            alloc.free(topics);
        }
        self.* = undefined;
    }
};

/// MetadataResponse (ApiKey 3)
///
/// Returns broker list, controller info, and topic/partition metadata.
pub const MetadataResponse = struct {
    throttle_time_ms: i32 = 0, // versions 3+
    brokers: []MetadataResponseBroker = &.{},
    cluster_id: ?[]const u8 = null, // versions 2+
    controller_id: i32 = -1, // versions 1+
    topics: []MetadataResponseTopic = &.{},

    pub const MetadataResponseBroker = struct {
        node_id: i32,
        host: []const u8,
        port: i32,
        rack: ?[]const u8 = null, // versions 1+
    };

    pub const MetadataResponseTopic = struct {
        error_code: i16 = 0,
        name: ?[]const u8 = null,
        topic_id: [16]u8 = [_]u8{0} ** 16, // versions 10+
        is_internal: bool = false, // versions 1+
        partitions: []MetadataResponsePartition = &.{},
        topic_authorized_operations: i32 = -0x80000000, // versions 8+
    };

    pub const MetadataResponsePartition = struct {
        error_code: i16 = 0,
        partition_index: i32,
        leader_id: i32,
        leader_epoch: i32 = -1, // versions 7+
        replica_nodes: []i32 = &.{},
        isr_nodes: []i32 = &.{},
        offline_replicas: []i32 = &.{}, // versions 5+
    };

    pub fn serialize(self: *const MetadataResponse, buf: []u8, pos: *usize, version: i16) void {
        const flexible = version >= 9;

        if (version >= 3) {
            ser.writeI32(buf, pos, self.throttle_time_ms);
        }

        // Brokers array
        if (flexible) {
            ser.writeCompactArrayLen(buf, pos, self.brokers.len);
        } else {
            ser.writeArrayLen(buf, pos, self.brokers.len);
        }
        for (self.brokers) |broker| {
            ser.writeI32(buf, pos, broker.node_id);
            if (flexible) {
                ser.writeCompactString(buf, pos, broker.host);
            } else {
                ser.writeString(buf, pos, broker.host);
            }
            ser.writeI32(buf, pos, broker.port);
            if (version >= 1) {
                if (flexible) {
                    ser.writeCompactString(buf, pos, broker.rack);
                } else {
                    ser.writeString(buf, pos, broker.rack);
                }
            }
            if (flexible) ser.writeEmptyTaggedFields(buf, pos);
        }

        // Cluster ID
        if (version >= 2) {
            if (flexible) {
                ser.writeCompactString(buf, pos, self.cluster_id);
            } else {
                ser.writeString(buf, pos, self.cluster_id);
            }
        }

        // Controller ID
        if (version >= 1) {
            ser.writeI32(buf, pos, self.controller_id);
        }

        // Topics array
        if (flexible) {
            ser.writeCompactArrayLen(buf, pos, self.topics.len);
        } else {
            ser.writeArrayLen(buf, pos, self.topics.len);
        }
        for (self.topics) |topic| {
            ser.writeI16(buf, pos, topic.error_code);
            if (flexible) {
                ser.writeCompactString(buf, pos, topic.name);
            } else {
                ser.writeString(buf, pos, topic.name);
            }
            if (version >= 10) {
                ser.writeUuid(buf, pos, topic.topic_id);
            }
            if (version >= 1) {
                ser.writeBool(buf, pos, topic.is_internal);
            }

            // Partitions
            if (flexible) {
                ser.writeCompactArrayLen(buf, pos, topic.partitions.len);
            } else {
                ser.writeArrayLen(buf, pos, topic.partitions.len);
            }
            for (topic.partitions) |part| {
                ser.writeI16(buf, pos, part.error_code);
                ser.writeI32(buf, pos, part.partition_index);
                ser.writeI32(buf, pos, part.leader_id);
                if (version >= 7) {
                    ser.writeI32(buf, pos, part.leader_epoch);
                }
                // Replica nodes
                writeI32Array(buf, pos, part.replica_nodes, flexible);
                // ISR nodes
                writeI32Array(buf, pos, part.isr_nodes, flexible);
                // Offline replicas
                if (version >= 5) {
                    writeI32Array(buf, pos, part.offline_replicas, flexible);
                }
                if (flexible) ser.writeEmptyTaggedFields(buf, pos);
            }

            if (version >= 8) {
                ser.writeI32(buf, pos, topic.topic_authorized_operations);
            }
            if (flexible) ser.writeEmptyTaggedFields(buf, pos);
        }

        if (flexible) ser.writeEmptyTaggedFields(buf, pos);
    }

    pub fn deinit(self: *MetadataResponse, alloc: Allocator) void {
        for (self.topics) |topic| {
            for (topic.partitions) |part| {
                if (part.replica_nodes.len > 0) alloc.free(part.replica_nodes);
                if (part.isr_nodes.len > 0) alloc.free(part.isr_nodes);
                if (part.offline_replicas.len > 0) alloc.free(part.offline_replicas);
            }
            if (topic.partitions.len > 0) alloc.free(topic.partitions);
        }
        if (self.topics.len > 0) alloc.free(self.topics);
        if (self.brokers.len > 0) alloc.free(self.brokers);
        self.* = undefined;
    }
};

fn writeI32Array(buf: []u8, pos: *usize, arr: []const i32, flexible: bool) void {
    if (flexible) {
        ser.writeCompactArrayLen(buf, pos, arr.len);
    } else {
        ser.writeArrayLen(buf, pos, arr.len);
    }
    for (arr) |val| {
        ser.writeI32(buf, pos, val);
    }
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "MetadataRequest v0 round-trip" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    var topics = [_]MetadataRequest.MetadataRequestTopic{
        .{ .name = "test-topic" },
    };
    const req = MetadataRequest{ .topics = &topics };
    req.serialize(&buf, &wpos, 0);

    var rpos: usize = 0;
    var read_req = try MetadataRequest.deserialize(testing.allocator, &buf, &rpos, 0);
    defer read_req.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), read_req.topics.?.len);
    try testing.expectEqualStrings("test-topic", read_req.topics.?[0].name.?);
    try testing.expectEqual(wpos, rpos);
}

test "MetadataRequest v9 flexible round-trip" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    var topics = [_]MetadataRequest.MetadataRequestTopic{
        .{ .name = "topic-a" },
        .{ .name = "topic-b" },
    };
    const req = MetadataRequest{
        .topics = &topics,
        .allow_auto_topic_creation = false,
        .include_topic_authorized_operations = true,
    };
    req.serialize(&buf, &wpos, 9);

    var rpos: usize = 0;
    var read_req = try MetadataRequest.deserialize(testing.allocator, &buf, &rpos, 9);
    defer read_req.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 2), read_req.topics.?.len);
    try testing.expectEqualStrings("topic-a", read_req.topics.?[0].name.?);
    try testing.expectEqual(false, read_req.allow_auto_topic_creation);
    try testing.expectEqual(true, read_req.include_topic_authorized_operations);
    try testing.expectEqual(wpos, rpos);
}

test "MetadataRequest null topics (all topics)" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;

    const req = MetadataRequest{ .topics = null };
    req.serialize(&buf, &wpos, 1);

    var rpos: usize = 0;
    var read_req = try MetadataRequest.deserialize(testing.allocator, &buf, &rpos, 1);
    defer read_req.deinit(testing.allocator);

    try testing.expect(read_req.topics == null);
}

test "MetadataResponse serialize single broker single topic" {
    var buf: [512]u8 = undefined;
    var wpos: usize = 0;

    var replicas = [_]i32{ 0, 1, 2 };
    var isr = [_]i32{ 0, 1 };
    var partitions = [_]MetadataResponse.MetadataResponsePartition{
        .{
            .partition_index = 0,
            .leader_id = 0,
            .replica_nodes = &replicas,
            .isr_nodes = &isr,
        },
    };
    var topics = [_]MetadataResponse.MetadataResponseTopic{
        .{
            .name = "test-topic",
            .is_internal = false,
            .partitions = &partitions,
        },
    };
    var brokers = [_]MetadataResponse.MetadataResponseBroker{
        .{ .node_id = 0, .host = "localhost", .port = 9092, .rack = null },
    };

    const resp = MetadataResponse{
        .brokers = &brokers,
        .cluster_id = "test-cluster",
        .controller_id = 0,
        .topics = &topics,
    };

    // Serialize as v1 (non-flexible)
    resp.serialize(&buf, &wpos, 1);
    try testing.expect(wpos > 0);
}

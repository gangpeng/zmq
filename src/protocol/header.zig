const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ser = @import("serialization.zig");

/// Kafka request/response headers.
///
/// Request Header v0 (non-flexible):
///   api_key: int16, api_version: int16, correlation_id: int32, client_id: nullable string
///
/// Request Header v1 (flexible):
///   api_key: int16, api_version: int16, correlation_id: int32, client_id: nullable string, tagged_fields
///
/// Response Header v0: correlation_id: int32
/// Response Header v1 (flexible): correlation_id: int32, tagged_fields
///
/// The header version depends on the API key and its version — flexible versions
/// of an API use header v1/v1, non-flexible use header v0/v0.

pub const RequestHeader = struct {
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: ?[]const u8,
    tagged_fields: []const ser.TaggedField = &.{},

    /// Deserialize a request header from the buffer.
    /// `header_version`:
    ///   0 = api_key + api_version + correlation_id (no client_id, no tagged fields)
    ///   1 = + client_id
    ///   2 = + client_id + tagged_fields (flexible)
    pub fn deserialize(alloc: Allocator, buf: []const u8, pos: *usize, header_version: i16) !RequestHeader {
        const api_key = ser.readI16(buf, pos);
        const api_version = ser.readI16(buf, pos);
        const correlation_id = ser.readI32(buf, pos);

        // Client ID is present in header versions >= 1
        // In v1: non-compact (int16 length prefix)
        // In v2: compact (unsigned varint length prefix)
        var client_id: ?[]const u8 = null;
        if (header_version >= 2) {
            client_id = try ser.readCompactString(buf, pos);
        } else if (header_version >= 1) {
            client_id = try ser.readString(buf, pos);
        }

        // Tagged fields are present in header version >= 2 (flexible)
        var tagged_fields: []const ser.TaggedField = &.{};
        if (header_version >= 2) {
            tagged_fields = try ser.readTaggedFields(alloc, buf, pos);
        }

        return .{
            .api_key = api_key,
            .api_version = api_version,
            .correlation_id = correlation_id,
            .client_id = client_id,
            .tagged_fields = tagged_fields,
        };
    }

    /// Serialize a request header to the buffer.
    pub fn serialize(self: *const RequestHeader, buf: []u8, pos: *usize, header_version: i16) void {
        ser.writeI16(buf, pos, self.api_key);
        ser.writeI16(buf, pos, self.api_version);
        ser.writeI32(buf, pos, self.correlation_id);
        if (header_version >= 2) {
            ser.writeCompactString(buf, pos, self.client_id);
        } else if (header_version >= 1) {
            ser.writeString(buf, pos, self.client_id);
        }
        if (header_version >= 2) {
            ser.writeTaggedFields(buf, pos, self.tagged_fields);
        }
    }

    /// Calculate the serialized size of this header.
    pub fn calcSize(self: *const RequestHeader, header_version: i16) usize {
        var size: usize = 0;
        size += 2; // api_key
        size += 2; // api_version
        size += 4; // correlation_id
        if (header_version >= 2) {
            size += ser.compactStringSize(self.client_id);
        } else if (header_version >= 1) {
            size += ser.stringSize(self.client_id);
        }
        if (header_version >= 2) {
            size += taggedFieldsSize(self.tagged_fields);
        }
        return size;
    }

    pub fn deinit(self: *RequestHeader, alloc: Allocator) void {
        if (self.tagged_fields.len > 0) {
            alloc.free(self.tagged_fields);
        }
        self.* = undefined;
    }
};

pub const ResponseHeader = struct {
    correlation_id: i32,
    tagged_fields: []const ser.TaggedField = &.{},

    /// Deserialize a response header from the buffer.
    /// `header_version` is 0 for non-flexible, 1 for flexible.
    pub fn deserialize(alloc: Allocator, buf: []const u8, pos: *usize, header_version: i16) !ResponseHeader {
        const correlation_id = ser.readI32(buf, pos);

        var tagged_fields: []const ser.TaggedField = &.{};
        if (header_version >= 1) {
            tagged_fields = try ser.readTaggedFields(alloc, buf, pos);
        }

        return .{
            .correlation_id = correlation_id,
            .tagged_fields = tagged_fields,
        };
    }

    /// Serialize a response header to the buffer.
    pub fn serialize(self: *const ResponseHeader, buf: []u8, pos: *usize, header_version: i16) void {
        ser.writeI32(buf, pos, self.correlation_id);

        if (header_version >= 1) {
            ser.writeTaggedFields(buf, pos, self.tagged_fields);
        }
    }

    /// Calculate the serialized size.
    pub fn calcSize(self: *const ResponseHeader, header_version: i16) usize {
        var size: usize = 4; // correlation_id
        if (header_version >= 1) {
            size += taggedFieldsSize(self.tagged_fields);
        }
        return size;
    }

    pub fn deinit(self: *ResponseHeader, alloc: Allocator) void {
        if (self.tagged_fields.len > 0) {
            alloc.free(self.tagged_fields);
        }
        self.* = undefined;
    }
};

/// Determine the request header version for a given API key and API version.
/// Flexible versions use header v2 (which has tagged fields).
/// Non-flexible use header v1 (legacy, no tagged fields in header).
///
/// NOTE: ApiVersions is special — it always uses header v0 for the request
/// even in flexible versions, to maintain bootstrap compatibility.
pub fn requestHeaderVersion(api_key: i16, api_version: i16) i16 {
    // ApiVersions (key 18) is special — always header v0 for request (never v2)
    // to allow version negotiation to work even with old brokers
    if (api_key == 18) return if (api_version >= 3) 2 else 1;

    // For all other APIs, check if the version is a flexible version.
    // The exact mapping is derived from the JSON schemas' "flexibleVersions" fields.
    // See isFlexibleVersion() for the complete per-API mapping.
    return if (isFlexibleVersion(api_key, api_version)) 2 else 1;
}

/// Determine the response header version.
/// ApiVersions response always uses header v0 even in flexible versions.
pub fn responseHeaderVersion(api_key: i16, api_version: i16) i16 {
    if (api_key == 18) return 0;
    return if (isFlexibleVersion(api_key, api_version)) 1 else 0;
}

/// Check if a given API version is a flexible version.
/// Derived from the "flexibleVersions" field in each API's JSON schema.
/// This mapping covers all 37 supported Kafka API keys.
fn isFlexibleVersion(api_key: i16, api_version: i16) bool {
    return switch (api_key) {
        0 => api_version >= 9, // Produce: 9+
        1 => api_version >= 12, // Fetch: 12+
        2 => api_version >= 6, // ListOffsets: 6+
        3 => api_version >= 9, // Metadata: 9+
        4 => api_version >= 4, // LeaderAndIsr: 4+
        5 => api_version >= 2, // StopReplica: 2+
        6 => api_version >= 6, // UpdateMetadata: 6+
        7 => api_version >= 3, // ControlledShutdown: 3+
        8 => api_version >= 8, // OffsetCommit: 8+
        9 => api_version >= 6, // OffsetFetch: 6+
        10 => api_version >= 3, // FindCoordinator: 3+
        11 => api_version >= 6, // JoinGroup: 6+
        12 => api_version >= 4, // Heartbeat: 4+
        13 => api_version >= 4, // LeaveGroup: 4+
        14 => api_version >= 4, // SyncGroup: 4+
        15 => api_version >= 5, // DescribeGroups: 5+
        16 => api_version >= 3, // ListGroups: 3+
        17 => false, // SaslHandshake: never flexible
        18 => api_version >= 3, // ApiVersions: 3+
        19 => api_version >= 5, // CreateTopics: 5+
        20 => api_version >= 4, // DeleteTopics: 4+
        21 => api_version >= 2, // DeleteRecords: 2+
        22 => api_version >= 2, // InitProducerId: 2+
        23 => api_version >= 4, // OffsetForLeaderEpoch: 4+
        24 => api_version >= 3, // AddPartitionsToTxn: 3+
        26 => api_version >= 3, // EndTxn: 3+
        27 => api_version >= 1, // WriteTxnMarkers: 1+
        28 => api_version >= 3, // TxnOffsetCommit: 3+
        29 => api_version >= 2, // DescribeAcls: 2+
        30 => api_version >= 2, // CreateAcls: 2+
        31 => api_version >= 2, // DeleteAcls: 2+
        32 => api_version >= 4, // DescribeConfigs: 4+
        33 => api_version >= 2, // AlterConfigs: 2+
        35 => api_version >= 2, // DescribeLogDirs: 2+
        36 => api_version >= 2, // SaslAuthenticate: 2+
        37 => api_version >= 2, // CreatePartitions: 2+
        43 => api_version >= 2, // ElectLeaders: 2+
        44 => true, // AlterPartitionReassignments: 0+ (always flexible)
        45 => true, // ListPartitionReassignments: 0+ (always flexible)
        52 => true, // Vote: 0+ (always flexible)
        53 => true, // BeginQuorumEpoch: 0+ (always flexible)
        54 => true, // EndQuorumEpoch: 0+ (always flexible)
        55 => true, // DescribeQuorum: 0+ (always flexible)
        60 => true, // DescribeCluster: 0+ (always flexible)
        61 => true, // DescribeProducers: 0+ (always flexible)
        else => false, // Unknown API: conservative default
    };
}

fn taggedFieldsSize(fields: []const ser.TaggedField) usize {
    var size = ser.unsignedVarintSize(fields.len);
    for (fields) |field| {
        size += ser.unsignedVarintSize(field.tag);
        size += ser.unsignedVarintSize(field.data.len);
        size += field.data.len;
    }
    return size;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "RequestHeader v1 round-trip (with client_id, no tagged fields)" {
    var buf: [128]u8 = undefined;
    var wpos: usize = 0;

    const header_val = RequestHeader{
        .api_key = 18,
        .api_version = 2,
        .correlation_id = 42,
        .client_id = "my-client",
    };
    header_val.serialize(&buf, &wpos, 1);

    var rpos: usize = 0;
    var read_header = try RequestHeader.deserialize(testing.allocator, &buf, &rpos, 1);
    defer read_header.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 18), read_header.api_key);
    try testing.expectEqual(@as(i16, 2), read_header.api_version);
    try testing.expectEqual(@as(i32, 42), read_header.correlation_id);
    try testing.expectEqualStrings("my-client", read_header.client_id.?);
    try testing.expectEqual(wpos, rpos);
}

test "RequestHeader v2 with tagged fields" {
    var buf: [128]u8 = undefined;
    var wpos: usize = 0;

    const header_val = RequestHeader{
        .api_key = 3,
        .api_version = 12,
        .correlation_id = 100,
        .client_id = "test",
    };
    header_val.serialize(&buf, &wpos, 2);

    var rpos: usize = 0;
    var read_header = try RequestHeader.deserialize(testing.allocator, &buf, &rpos, 2);
    defer read_header.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 3), read_header.api_key);
    try testing.expectEqual(@as(i32, 100), read_header.correlation_id);
    try testing.expectEqual(wpos, rpos);
}

test "ResponseHeader v0 round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;

    const header = ResponseHeader{ .correlation_id = 77 };
    header.serialize(&buf, &wpos, 0);

    var rpos: usize = 0;
    var read_header = try ResponseHeader.deserialize(testing.allocator, &buf, &rpos, 0);
    defer read_header.deinit(testing.allocator);

    try testing.expectEqual(@as(i32, 77), read_header.correlation_id);
    try testing.expectEqual(@as(usize, 4), rpos);
}

test "ResponseHeader v1 round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;

    const header = ResponseHeader{ .correlation_id = 99 };
    header.serialize(&buf, &wpos, 1);

    var rpos: usize = 0;
    var read_header = try ResponseHeader.deserialize(testing.allocator, &buf, &rpos, 1);
    defer read_header.deinit(testing.allocator);

    try testing.expectEqual(@as(i32, 99), read_header.correlation_id);
    try testing.expectEqual(@as(usize, 5), rpos); // 4 + varint(0)
}

test "RequestHeader null client_id" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;

    const header_val = RequestHeader{
        .api_key = 0,
        .api_version = 0,
        .correlation_id = 1,
        .client_id = null,
    };
    header_val.serialize(&buf, &wpos, 1);

    var rpos: usize = 0;
    var read_header = try RequestHeader.deserialize(testing.allocator, &buf, &rpos, 1);
    defer read_header.deinit(testing.allocator);

    try testing.expect(read_header.client_id == null);
}

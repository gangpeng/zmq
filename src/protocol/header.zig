const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ser = @import("serialization.zig");
const api_support = @import("api_support.zig");

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
/// The header version depends on the API key and its version: flexible request
/// versions use request header v2 and response header v1; non-flexible versions
/// use request header v1 and response header v0.
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
/// NOTE: ApiVersions is special only for responses: request v0-v2 use header v1
/// and request v3+ uses header v2, but the response header is always v0.
pub fn requestHeaderVersion(api_key: i16, api_version: i16) i16 {
    // ApiVersions (key 18) follows the request header schema rule directly.
    if (api_key == 18) return if (api_version >= 3) 2 else 1;

    // For all other APIs, check if the version is a flexible version.
    // The mapping is derived from the JSON schemas' "flexibleVersions" fields.
    return if (isFlexibleVersion(api_key, api_version)) 2 else 1;
}

/// Determine the response header version.
/// ApiVersions response always uses header v0 even in flexible versions.
pub fn responseHeaderVersion(api_key: i16, api_version: i16) i16 {
    if (api_key == 18) return 0;
    return if (isFlexibleVersion(api_key, api_version)) 1 else 0;
}

pub const FlexibleVersionRule = struct {
    min_key: i16,
    max_key: i16,
    first_flexible_version: ?i16,

    fn appliesTo(self: FlexibleVersionRule, api_key: i16) bool {
        return api_key >= self.min_key and api_key <= self.max_key;
    }
};

/// Request/response flexible-version rules from the generated JSON schemas.
/// A null first_flexible_version means the API is explicitly never flexible.
pub const flexible_version_rules = [_]FlexibleVersionRule{
    .{ .min_key = 0, .max_key = 0, .first_flexible_version = 9 }, // Produce
    .{ .min_key = 1, .max_key = 1, .first_flexible_version = 12 }, // Fetch
    .{ .min_key = 2, .max_key = 2, .first_flexible_version = 6 }, // ListOffsets
    .{ .min_key = 3, .max_key = 3, .first_flexible_version = 9 }, // Metadata
    .{ .min_key = 4, .max_key = 4, .first_flexible_version = 4 }, // LeaderAndIsr
    .{ .min_key = 5, .max_key = 5, .first_flexible_version = 2 }, // StopReplica
    .{ .min_key = 6, .max_key = 6, .first_flexible_version = 6 }, // UpdateMetadata
    .{ .min_key = 7, .max_key = 7, .first_flexible_version = 3 }, // ControlledShutdown
    .{ .min_key = 8, .max_key = 8, .first_flexible_version = 8 }, // OffsetCommit
    .{ .min_key = 9, .max_key = 9, .first_flexible_version = 6 }, // OffsetFetch
    .{ .min_key = 10, .max_key = 10, .first_flexible_version = 3 }, // FindCoordinator
    .{ .min_key = 11, .max_key = 11, .first_flexible_version = 6 }, // JoinGroup
    .{ .min_key = 12, .max_key = 12, .first_flexible_version = 4 }, // Heartbeat
    .{ .min_key = 13, .max_key = 13, .first_flexible_version = 4 }, // LeaveGroup
    .{ .min_key = 14, .max_key = 14, .first_flexible_version = 4 }, // SyncGroup
    .{ .min_key = 15, .max_key = 15, .first_flexible_version = 5 }, // DescribeGroups
    .{ .min_key = 16, .max_key = 16, .first_flexible_version = 3 }, // ListGroups
    .{ .min_key = 17, .max_key = 17, .first_flexible_version = null }, // SaslHandshake
    .{ .min_key = 18, .max_key = 18, .first_flexible_version = 3 }, // ApiVersions
    .{ .min_key = 19, .max_key = 19, .first_flexible_version = 5 }, // CreateTopics
    .{ .min_key = 20, .max_key = 20, .first_flexible_version = 4 }, // DeleteTopics
    .{ .min_key = 21, .max_key = 21, .first_flexible_version = 2 }, // DeleteRecords
    .{ .min_key = 22, .max_key = 22, .first_flexible_version = 2 }, // InitProducerId
    .{ .min_key = 23, .max_key = 23, .first_flexible_version = 4 }, // OffsetForLeaderEpoch
    .{ .min_key = 24, .max_key = 24, .first_flexible_version = 3 }, // AddPartitionsToTxn
    .{ .min_key = 25, .max_key = 25, .first_flexible_version = 3 }, // AddOffsetsToTxn
    .{ .min_key = 26, .max_key = 26, .first_flexible_version = 3 }, // EndTxn
    .{ .min_key = 27, .max_key = 27, .first_flexible_version = 1 }, // WriteTxnMarkers
    .{ .min_key = 28, .max_key = 28, .first_flexible_version = 3 }, // TxnOffsetCommit
    .{ .min_key = 29, .max_key = 29, .first_flexible_version = 2 }, // DescribeAcls
    .{ .min_key = 30, .max_key = 30, .first_flexible_version = 2 }, // CreateAcls
    .{ .min_key = 31, .max_key = 31, .first_flexible_version = 2 }, // DeleteAcls
    .{ .min_key = 32, .max_key = 32, .first_flexible_version = 4 }, // DescribeConfigs
    .{ .min_key = 33, .max_key = 33, .first_flexible_version = 2 }, // AlterConfigs
    .{ .min_key = 34, .max_key = 34, .first_flexible_version = 2 }, // AlterReplicaLogDirs
    .{ .min_key = 35, .max_key = 35, .first_flexible_version = 2 }, // DescribeLogDirs
    .{ .min_key = 36, .max_key = 36, .first_flexible_version = 2 }, // SaslAuthenticate
    .{ .min_key = 37, .max_key = 37, .first_flexible_version = 2 }, // CreatePartitions
    .{ .min_key = 38, .max_key = 41, .first_flexible_version = 2 }, // DelegationToken APIs
    .{ .min_key = 42, .max_key = 42, .first_flexible_version = 2 }, // DeleteGroups
    .{ .min_key = 43, .max_key = 43, .first_flexible_version = 2 }, // ElectLeaders
    .{ .min_key = 44, .max_key = 44, .first_flexible_version = 1 }, // IncrementalAlterConfigs
    .{ .min_key = 45, .max_key = 45, .first_flexible_version = 0 }, // AlterPartitionReassignments
    .{ .min_key = 46, .max_key = 46, .first_flexible_version = 0 }, // ListPartitionReassignments
    .{ .min_key = 47, .max_key = 47, .first_flexible_version = null }, // OffsetDelete
    .{ .min_key = 48, .max_key = 49, .first_flexible_version = 1 }, // ClientQuotas
    .{ .min_key = 50, .max_key = 52, .first_flexible_version = 0 }, // SCRAM and Vote
    .{ .min_key = 53, .max_key = 54, .first_flexible_version = 1 }, // Begin/EndQuorumEpoch
    .{ .min_key = 55, .max_key = 87, .first_flexible_version = 0 }, // KRaft/controller and newer Kafka APIs
    .{ .min_key = 501, .max_key = 519, .first_flexible_version = 0 }, // AutoMQ extensions
    .{ .min_key = 600, .max_key = 602, .first_flexible_version = 0 }, // AutoMQ extensions
};

pub fn findFlexibleVersionRule(api_key: i16) ?FlexibleVersionRule {
    for (flexible_version_rules) |rule| {
        if (rule.appliesTo(api_key)) return rule;
    }
    return null;
}

/// Check if a given API version is a flexible version.
pub fn isFlexibleVersion(api_key: i16, api_version: i16) bool {
    const rule = findFlexibleVersionRule(api_key) orelse return false;
    const first = rule.first_flexible_version orelse return false;
    return api_version >= first;
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

test "header version mapping matches shifted Kafka API keys" {
    try testing.expectEqual(@as(i16, 1), requestHeaderVersion(42, 1)); // DeleteGroups v1 is non-flexible.
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(42, 2));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(42, 2));

    try testing.expectEqual(@as(i16, 1), requestHeaderVersion(44, 0)); // IncrementalAlterConfigs v0 is non-flexible.
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(44, 1));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(45, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(46, 0));

    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(25, 3)); // AddOffsetsToTxn flexible versions.
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(25, 3));
}

test "flexible version catalog is sorted and non-overlapping" {
    var previous_max_key: ?i16 = null;
    for (flexible_version_rules) |rule| {
        try testing.expect(rule.min_key <= rule.max_key);
        if (previous_max_key) |prev| {
            try testing.expect(rule.min_key > prev);
        }
        if (rule.first_flexible_version) |first| {
            try testing.expect(first >= 0);
        }
        previous_max_key = rule.max_key;
    }
}

test "advertised APIs have explicit header flexible-version rules" {
    for (api_support.broker_supported_apis) |api| {
        const rule = findFlexibleVersionRule(api.key) orelse return error.MissingHeaderFlexibleVersionRule;

        var version = api.min;
        while (version <= api.max) : (version += 1) {
            const flexible = if (rule.first_flexible_version) |first| version >= first else false;
            const expected_request_header: i16 = if (flexible) 2 else 1;
            const expected_response_header: i16 = if (api.key == 18) 0 else if (flexible) 1 else 0;

            try testing.expectEqual(expected_request_header, requestHeaderVersion(api.key, version));
            try testing.expectEqual(expected_response_header, responseHeaderVersion(api.key, version));
        }
    }
}

test "controller advertised APIs have explicit header flexible-version rules" {
    for (api_support.controller_supported_apis) |api| {
        const rule = findFlexibleVersionRule(api.key) orelse return error.MissingHeaderFlexibleVersionRule;

        var version = api.min;
        while (version <= api.max) : (version += 1) {
            const flexible = if (rule.first_flexible_version) |first| version >= first else false;
            const expected_request_header: i16 = if (flexible) 2 else 1;
            const expected_response_header: i16 = if (api.key == 18) 0 else if (flexible) 1 else 0;

            try testing.expectEqual(expected_request_header, requestHeaderVersion(api.key, version));
            try testing.expectEqual(expected_response_header, responseHeaderVersion(api.key, version));
        }
    }
}

test "KRaft quorum APIs use non-flexible v0 and flexible v1 headers" {
    try testing.expectEqual(@as(i16, 1), requestHeaderVersion(53, 0));
    try testing.expectEqual(@as(i16, 0), responseHeaderVersion(53, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(53, 1));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(53, 1));

    try testing.expectEqual(@as(i16, 1), requestHeaderVersion(54, 0));
    try testing.expectEqual(@as(i16, 0), responseHeaderVersion(54, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(54, 1));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(54, 1));
}

test "controller lifecycle APIs use generated flexible v0 headers" {
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(62, 0));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(62, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(63, 0));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(63, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(67, 0));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(67, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(80, 0));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(80, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(81, 0));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(81, 0));
    try testing.expectEqual(@as(i16, 2), requestHeaderVersion(82, 0));
    try testing.expectEqual(@as(i16, 1), responseHeaderVersion(82, 0));
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

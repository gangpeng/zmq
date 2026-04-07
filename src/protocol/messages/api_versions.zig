const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const ser = @import("../serialization.zig");

/// ApiVersionsRequest (ApiKey 18)
///
/// Versions 0-2: empty request body.
/// Versions 3+: flexible, adds ClientSoftwareName and ClientSoftwareVersion.
///
/// This is always the first request a client sends to negotiate which
/// API versions the broker supports.
pub const ApiVersionsRequest = struct {
    client_software_name: ?[]const u8 = null, // versions 3+
    client_software_version: ?[]const u8 = null, // versions 3+
    tagged_fields: []const ser.TaggedField = &.{},

    pub fn deserialize(alloc: Allocator, buf: []const u8, pos: *usize, version: i16) !ApiVersionsRequest {
        var result = ApiVersionsRequest{};

        if (version >= 3) {
            result.client_software_name = try ser.readCompactString(buf, pos);
            result.client_software_version = try ser.readCompactString(buf, pos);
            result.tagged_fields = try ser.readTaggedFields(alloc, buf, pos);
        }

        return result;
    }

    pub fn serialize(self: *const ApiVersionsRequest, buf: []u8, pos: *usize, version: i16) void {
        if (version >= 3) {
            ser.writeCompactString(buf, pos, self.client_software_name);
            ser.writeCompactString(buf, pos, self.client_software_version);
            ser.writeTaggedFields(buf, pos, self.tagged_fields);
        }
    }

    pub fn calcSize(self: *const ApiVersionsRequest, version: i16) usize {
        var size: usize = 0;
        if (version >= 3) {
            size += ser.compactStringSize(self.client_software_name);
            size += ser.compactStringSize(self.client_software_version);
            size += taggedFieldsSize(self.tagged_fields);
        }
        return size;
    }

    pub fn deinit(self: *ApiVersionsRequest, alloc: Allocator) void {
        if (self.tagged_fields.len > 0) {
            alloc.free(self.tagged_fields);
        }
        self.* = undefined;
    }
};

/// ApiVersionsResponse (ApiKey 18)
///
/// Returns the list of API keys supported by the broker with min/max versions.
pub const ApiVersionsResponse = struct {
    error_code: i16 = 0,
    api_keys: []ApiVersion = &.{},
    throttle_time_ms: i32 = 0, // versions 1+
    // Tagged fields for versions 3+:
    supported_features: []SupportedFeatureKey = &.{}, // tag 0
    finalized_features_epoch: i64 = -1, // tag 1
    finalized_features: []FinalizedFeatureKey = &.{}, // tag 2
    zk_migration_ready: bool = false, // tag 3
    tagged_fields: []const ser.TaggedField = &.{},

    pub const ApiVersion = struct {
        api_key: i16,
        min_version: i16,
        max_version: i16,
    };

    pub const SupportedFeatureKey = struct {
        name: []const u8,
        min_version: i16,
        max_version: i16,
    };

    pub const FinalizedFeatureKey = struct {
        name: []const u8,
        max_version_level: i16,
        min_version_level: i16,
    };

    pub fn serialize(self: *const ApiVersionsResponse, buf: []u8, pos: *usize, version: i16) void {
        ser.writeI16(buf, pos, self.error_code);

        // ApiKeys array
        if (version >= 3) {
            ser.writeCompactArrayLen(buf, pos, self.api_keys.len);
        } else {
            ser.writeArrayLen(buf, pos, self.api_keys.len);
        }
        for (self.api_keys) |key| {
            ser.writeI16(buf, pos, key.api_key);
            ser.writeI16(buf, pos, key.min_version);
            ser.writeI16(buf, pos, key.max_version);
            if (version >= 3) {
                ser.writeEmptyTaggedFields(buf, pos); // per-element tagged fields
            }
        }

        if (version >= 1) {
            ser.writeI32(buf, pos, self.throttle_time_ms);
        }

        if (version >= 3) {
            // Tagged fields in the response body.
            // Empty tagged fields (count=0) is valid — indicates no optional features
            // (supported_features, finalized_features, zk_migration_ready).
            // Real Kafka brokers include these for KRaft feature negotiation,
            // but they are optional and clients handle their absence correctly.
            ser.writeEmptyTaggedFields(buf, pos);
        }
    }

    pub fn calcSize(self: *const ApiVersionsResponse, version: i16) usize {
        var size: usize = 0;
        size += 2; // error_code

        // api_keys array
        if (version >= 3) {
            size += ser.unsignedVarintSize(self.api_keys.len + 1);
        } else {
            size += 4; // int32 array length
        }
        for (self.api_keys) |_| {
            size += 6; // api_key(2) + min_version(2) + max_version(2)
            if (version >= 3) {
                size += 1; // empty tagged fields per element
            }
        }

        if (version >= 1) {
            size += 4; // throttle_time_ms
        }

        if (version >= 3) {
            size += 1; // empty tagged fields
        }

        return size;
    }

    pub fn deserialize(alloc: Allocator, buf: []const u8, pos: *usize, version: i16) !ApiVersionsResponse {
        var result = ApiVersionsResponse{};

        result.error_code = ser.readI16(buf, pos);

        // Read api_keys array
        const array_len = if (version >= 3)
            try ser.readCompactArrayLen(buf, pos)
        else
            try ser.readArrayLen(buf, pos);

        if (array_len) |len| {
            result.api_keys = try alloc.alloc(ApiVersion, len);
            for (0..len) |i| {
                result.api_keys[i] = .{
                    .api_key = ser.readI16(buf, pos),
                    .min_version = ser.readI16(buf, pos),
                    .max_version = ser.readI16(buf, pos),
                };
                if (version >= 3) {
                    try ser.skipTaggedFields(buf, pos);
                }
            }
        }

        if (version >= 1) {
            result.throttle_time_ms = ser.readI32(buf, pos);
        }

        if (version >= 3) {
            try ser.skipTaggedFields(buf, pos);
        }

        return result;
    }

    pub fn deinit(self: *ApiVersionsResponse, alloc: Allocator) void {
        if (self.api_keys.len > 0) {
            alloc.free(self.api_keys);
        }
        self.* = undefined;
    }
};

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

test "ApiVersionsRequest v0 empty" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;

    const req = ApiVersionsRequest{};
    req.serialize(&buf, &wpos, 0);
    try testing.expectEqual(@as(usize, 0), wpos); // v0 is empty

    var rpos: usize = 0;
    var read_req = try ApiVersionsRequest.deserialize(testing.allocator, &buf, &rpos, 0);
    defer read_req.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 0), rpos);
}

test "ApiVersionsRequest v3 with client info" {
    var buf: [128]u8 = undefined;
    var wpos: usize = 0;

    const req = ApiVersionsRequest{
        .client_software_name = "zmq",
        .client_software_version = "0.1.0",
    };
    req.serialize(&buf, &wpos, 3);

    var rpos: usize = 0;
    var read_req = try ApiVersionsRequest.deserialize(testing.allocator, &buf, &rpos, 3);
    defer read_req.deinit(testing.allocator);

    try testing.expectEqualStrings("zmq", read_req.client_software_name.?);
    try testing.expectEqualStrings("0.1.0", read_req.client_software_version.?);
    try testing.expectEqual(wpos, rpos);
}

test "ApiVersionsResponse v0 round-trip" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    var api_keys = [_]ApiVersionsResponse.ApiVersion{
        .{ .api_key = 0, .min_version = 0, .max_version = 11 }, // Produce
        .{ .api_key = 1, .min_version = 0, .max_version = 17 }, // Fetch
        .{ .api_key = 18, .min_version = 0, .max_version = 4 }, // ApiVersions
    };

    const resp = ApiVersionsResponse{
        .error_code = 0,
        .api_keys = &api_keys,
    };
    resp.serialize(&buf, &wpos, 0);

    var rpos: usize = 0;
    var read_resp = try ApiVersionsResponse.deserialize(testing.allocator, &buf, &rpos, 0);
    defer read_resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), read_resp.error_code);
    try testing.expectEqual(@as(usize, 3), read_resp.api_keys.len);
    try testing.expectEqual(@as(i16, 0), read_resp.api_keys[0].api_key);
    try testing.expectEqual(@as(i16, 11), read_resp.api_keys[0].max_version);
    try testing.expectEqual(@as(i16, 18), read_resp.api_keys[2].api_key);
    try testing.expectEqual(wpos, rpos);
}

test "ApiVersionsResponse v3 round-trip" {
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;

    var api_keys = [_]ApiVersionsResponse.ApiVersion{
        .{ .api_key = 18, .min_version = 0, .max_version = 4 },
    };

    const resp = ApiVersionsResponse{
        .error_code = 0,
        .api_keys = &api_keys,
        .throttle_time_ms = 100,
    };
    resp.serialize(&buf, &wpos, 3);

    var rpos: usize = 0;
    var read_resp = try ApiVersionsResponse.deserialize(testing.allocator, &buf, &rpos, 3);
    defer read_resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), read_resp.error_code);
    try testing.expectEqual(@as(usize, 1), read_resp.api_keys.len);
    try testing.expectEqual(@as(i32, 100), read_resp.throttle_time_ms);
    try testing.expectEqual(wpos, rpos);
}

test "ApiVersionsResponse calcSize matches actual" {
    var api_keys = [_]ApiVersionsResponse.ApiVersion{
        .{ .api_key = 0, .min_version = 0, .max_version = 11 },
        .{ .api_key = 1, .min_version = 0, .max_version = 17 },
    };

    const resp = ApiVersionsResponse{
        .error_code = 0,
        .api_keys = &api_keys,
        .throttle_time_ms = 0,
    };

    // Test v0
    const size_v0 = resp.calcSize(0);
    var buf: [256]u8 = undefined;
    var wpos: usize = 0;
    resp.serialize(&buf, &wpos, 0);
    try testing.expectEqual(size_v0, wpos);

    // Test v3
    const size_v3 = resp.calcSize(3);
    wpos = 0;
    resp.serialize(&buf, &wpos, 3);
    try testing.expectEqual(size_v3, wpos);
}

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
            self.writeTopLevelTaggedFields(buf, pos);
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
            size += self.topLevelTaggedFieldsSize();
        }

        return size;
    }

    pub fn deserialize(alloc: Allocator, buf: []const u8, pos: *usize, version: i16) !ApiVersionsResponse {
        var result = ApiVersionsResponse{};
        errdefer result.deinit(alloc);

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
            const tagged_fields = try ser.readTaggedFields(alloc, buf, pos);
            result.tagged_fields = tagged_fields;

            var seen_supported_features = false;
            var seen_finalized_features_epoch = false;
            var seen_finalized_features = false;
            var seen_zk_migration_ready = false;

            for (tagged_fields) |field| {
                switch (field.tag) {
                    0 => {
                        if (seen_supported_features) return error.DuplicateTaggedField;
                        seen_supported_features = true;
                        result.supported_features = try parseSupportedFeaturesTag(alloc, field.data);
                    },
                    1 => {
                        if (seen_finalized_features_epoch) return error.DuplicateTaggedField;
                        seen_finalized_features_epoch = true;
                        result.finalized_features_epoch = try parseFinalizedFeaturesEpochTag(field.data);
                    },
                    2 => {
                        if (seen_finalized_features) return error.DuplicateTaggedField;
                        seen_finalized_features = true;
                        result.finalized_features = try parseFinalizedFeaturesTag(alloc, field.data);
                    },
                    3 => {
                        if (seen_zk_migration_ready) return error.DuplicateTaggedField;
                        seen_zk_migration_ready = true;
                        result.zk_migration_ready = try parseZkMigrationReadyTag(field.data);
                    },
                    else => {},
                }
            }
        }

        return result;
    }

    pub fn deinit(self: *ApiVersionsResponse, alloc: Allocator) void {
        if (self.api_keys.len > 0) {
            alloc.free(self.api_keys);
        }
        if (self.supported_features.len > 0) {
            alloc.free(self.supported_features);
        }
        if (self.finalized_features.len > 0) {
            alloc.free(self.finalized_features);
        }
        if (self.tagged_fields.len > 0) {
            alloc.free(self.tagged_fields);
        }
        self.* = undefined;
    }

    fn writeTopLevelTaggedFields(self: *const ApiVersionsResponse, buf: []u8, pos: *usize) void {
        ser.writeUnsignedVarint(buf, pos, self.topLevelTaggedFieldCount());

        if (self.supported_features.len > 0) {
            writeTaggedFieldHeader(buf, pos, 0, supportedFeaturesTagSize(self.supported_features));
            ser.writeCompactArrayLen(buf, pos, self.supported_features.len);
            for (self.supported_features) |feature| {
                ser.writeCompactString(buf, pos, feature.name);
                ser.writeI16(buf, pos, feature.min_version);
                ser.writeI16(buf, pos, feature.max_version);
                ser.writeEmptyTaggedFields(buf, pos);
            }
        }

        if (self.finalized_features_epoch != -1) {
            writeTaggedFieldHeader(buf, pos, 1, 8);
            ser.writeI64(buf, pos, self.finalized_features_epoch);
        }

        if (self.finalized_features.len > 0) {
            writeTaggedFieldHeader(buf, pos, 2, finalizedFeaturesTagSize(self.finalized_features));
            ser.writeCompactArrayLen(buf, pos, self.finalized_features.len);
            for (self.finalized_features) |feature| {
                ser.writeCompactString(buf, pos, feature.name);
                ser.writeI16(buf, pos, feature.max_version_level);
                ser.writeI16(buf, pos, feature.min_version_level);
                ser.writeEmptyTaggedFields(buf, pos);
            }
        }

        if (self.zk_migration_ready) {
            writeTaggedFieldHeader(buf, pos, 3, 1);
            ser.writeBool(buf, pos, true);
        }

        for (self.tagged_fields) |field| {
            if (isKnownApiVersionsResponseTag(field.tag)) continue;
            writeTaggedFieldHeader(buf, pos, field.tag, field.data.len);
            @memcpy(buf[pos.* .. pos.* + field.data.len], field.data);
            pos.* += field.data.len;
        }
    }

    fn topLevelTaggedFieldsSize(self: *const ApiVersionsResponse) usize {
        var size = ser.unsignedVarintSize(self.topLevelTaggedFieldCount());

        if (self.supported_features.len > 0) {
            const field_size = supportedFeaturesTagSize(self.supported_features);
            size += taggedFieldHeaderSize(0, field_size) + field_size;
        }
        if (self.finalized_features_epoch != -1) {
            size += taggedFieldHeaderSize(1, 8) + 8;
        }
        if (self.finalized_features.len > 0) {
            const field_size = finalizedFeaturesTagSize(self.finalized_features);
            size += taggedFieldHeaderSize(2, field_size) + field_size;
        }
        if (self.zk_migration_ready) {
            size += taggedFieldHeaderSize(3, 1) + 1;
        }
        for (self.tagged_fields) |field| {
            if (isKnownApiVersionsResponseTag(field.tag)) continue;
            size += taggedFieldHeaderSize(field.tag, field.data.len) + field.data.len;
        }

        return size;
    }

    fn topLevelTaggedFieldCount(self: *const ApiVersionsResponse) usize {
        var count: usize = 0;
        if (self.supported_features.len > 0) count += 1;
        if (self.finalized_features_epoch != -1) count += 1;
        if (self.finalized_features.len > 0) count += 1;
        if (self.zk_migration_ready) count += 1;
        for (self.tagged_fields) |field| {
            if (!isKnownApiVersionsResponseTag(field.tag)) count += 1;
        }
        return count;
    }
};

fn parseSupportedFeaturesTag(alloc: Allocator, data: []const u8) ![]ApiVersionsResponse.SupportedFeatureKey {
    var pos: usize = 0;
    const len = (try ser.readCompactArrayLen(data, &pos)) orelse return error.InvalidTaggedField;
    if (len == 0) {
        if (pos != data.len) return error.InvalidTaggedField;
        return &.{};
    }

    const features = try alloc.alloc(ApiVersionsResponse.SupportedFeatureKey, len);
    errdefer alloc.free(features);
    for (features) |*feature| {
        feature.* = .{
            .name = (try ser.readCompactString(data, &pos)) orelse return error.InvalidTaggedField,
            .min_version = try readI16Checked(data, &pos),
            .max_version = try readI16Checked(data, &pos),
        };
        try ser.skipTaggedFields(data, &pos);
    }
    if (pos != data.len) return error.InvalidTaggedField;
    return features;
}

fn parseFinalizedFeaturesEpochTag(data: []const u8) !i64 {
    var pos: usize = 0;
    const epoch = try readI64Checked(data, &pos);
    if (pos != data.len) return error.InvalidTaggedField;
    return epoch;
}

fn parseFinalizedFeaturesTag(alloc: Allocator, data: []const u8) ![]ApiVersionsResponse.FinalizedFeatureKey {
    var pos: usize = 0;
    const len = (try ser.readCompactArrayLen(data, &pos)) orelse return error.InvalidTaggedField;
    if (len == 0) {
        if (pos != data.len) return error.InvalidTaggedField;
        return &.{};
    }

    const features = try alloc.alloc(ApiVersionsResponse.FinalizedFeatureKey, len);
    errdefer alloc.free(features);
    for (features) |*feature| {
        feature.* = .{
            .name = (try ser.readCompactString(data, &pos)) orelse return error.InvalidTaggedField,
            .max_version_level = try readI16Checked(data, &pos),
            .min_version_level = try readI16Checked(data, &pos),
        };
        try ser.skipTaggedFields(data, &pos);
    }
    if (pos != data.len) return error.InvalidTaggedField;
    return features;
}

fn parseZkMigrationReadyTag(data: []const u8) !bool {
    var pos: usize = 0;
    const value = try readBoolChecked(data, &pos);
    if (pos != data.len) return error.InvalidTaggedField;
    return value;
}

fn supportedFeaturesTagSize(features: []const ApiVersionsResponse.SupportedFeatureKey) usize {
    var size = ser.unsignedVarintSize(features.len + 1);
    for (features) |feature| {
        size += ser.compactStringSize(feature.name);
        size += 2; // min_version
        size += 2; // max_version
        size += 1; // element tagged fields
    }
    return size;
}

fn finalizedFeaturesTagSize(features: []const ApiVersionsResponse.FinalizedFeatureKey) usize {
    var size = ser.unsignedVarintSize(features.len + 1);
    for (features) |feature| {
        size += ser.compactStringSize(feature.name);
        size += 2; // max_version_level
        size += 2; // min_version_level
        size += 1; // element tagged fields
    }
    return size;
}

fn writeTaggedFieldHeader(buf: []u8, pos: *usize, tag: usize, size: usize) void {
    ser.writeUnsignedVarint(buf, pos, tag);
    ser.writeUnsignedVarint(buf, pos, size);
}

fn taggedFieldHeaderSize(tag: usize, size: usize) usize {
    return ser.unsignedVarintSize(tag) + ser.unsignedVarintSize(size);
}

fn isKnownApiVersionsResponseTag(tag: usize) bool {
    return tag <= 3;
}

fn readI16Checked(buf: []const u8, pos: *usize) !i16 {
    if (pos.* + 2 > buf.len) return error.BufferUnderflow;
    return ser.readI16(buf, pos);
}

fn readI64Checked(buf: []const u8, pos: *usize) !i64 {
    if (pos.* + 8 > buf.len) return error.BufferUnderflow;
    return ser.readI64(buf, pos);
}

fn readBoolChecked(buf: []const u8, pos: *usize) !bool {
    if (pos.* + 1 > buf.len) return error.BufferUnderflow;
    return try ser.readBool(buf, pos);
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

test "ApiVersionsResponse v4 feature tagged fields round-trip" {
    var buf: [512]u8 = undefined;
    var wpos: usize = 0;

    var api_keys = [_]ApiVersionsResponse.ApiVersion{
        .{ .api_key = 18, .min_version = 0, .max_version = 4 },
        .{ .api_key = 57, .min_version = 0, .max_version = 1 },
    };
    var supported_features = [_]ApiVersionsResponse.SupportedFeatureKey{
        .{ .name = "metadata.version", .min_version = 0, .max_version = 3 },
        .{ .name = "kraft.version", .min_version = 0, .max_version = 1 },
    };
    var finalized_features = [_]ApiVersionsResponse.FinalizedFeatureKey{
        .{ .name = "metadata.version", .max_version_level = 3, .min_version_level = 1 },
    };
    const unknown_tag_data = [_]u8{ 0xaa, 0xbb, 0xcc };
    const unknown_tags = [_]ser.TaggedField{.{
        .tag = 8,
        .data = &unknown_tag_data,
    }};

    const resp = ApiVersionsResponse{
        .error_code = 0,
        .api_keys = &api_keys,
        .throttle_time_ms = 25,
        .supported_features = &supported_features,
        .finalized_features_epoch = 42,
        .finalized_features = &finalized_features,
        .zk_migration_ready = true,
        .tagged_fields = &unknown_tags,
    };
    resp.serialize(&buf, &wpos, 4);
    try testing.expectEqual(resp.calcSize(4), wpos);

    var rpos: usize = 0;
    var read_resp = try ApiVersionsResponse.deserialize(testing.allocator, &buf, &rpos, 4);
    defer read_resp.deinit(testing.allocator);

    try testing.expectEqual(@as(i16, 0), read_resp.error_code);
    try testing.expectEqual(@as(usize, 2), read_resp.api_keys.len);
    try testing.expectEqual(@as(i32, 25), read_resp.throttle_time_ms);
    try testing.expectEqual(@as(usize, 2), read_resp.supported_features.len);
    try testing.expectEqualStrings("metadata.version", read_resp.supported_features[0].name);
    try testing.expectEqual(@as(i16, 0), read_resp.supported_features[0].min_version);
    try testing.expectEqual(@as(i16, 3), read_resp.supported_features[0].max_version);
    try testing.expectEqualStrings("kraft.version", read_resp.supported_features[1].name);
    try testing.expectEqual(@as(i64, 42), read_resp.finalized_features_epoch);
    try testing.expectEqual(@as(usize, 1), read_resp.finalized_features.len);
    try testing.expectEqualStrings("metadata.version", read_resp.finalized_features[0].name);
    try testing.expectEqual(@as(i16, 3), read_resp.finalized_features[0].max_version_level);
    try testing.expectEqual(@as(i16, 1), read_resp.finalized_features[0].min_version_level);
    try testing.expect(read_resp.zk_migration_ready);
    try testing.expectEqual(@as(usize, 5), read_resp.tagged_fields.len);
    try testing.expectEqual(@as(usize, 8), read_resp.tagged_fields[4].tag);
    try testing.expectEqualSlices(u8, &unknown_tag_data, read_resp.tagged_fields[4].data);
    try testing.expectEqual(wpos, rpos);

    var roundtrip_buf: [512]u8 = undefined;
    var roundtrip_wpos: usize = 0;
    read_resp.serialize(&roundtrip_buf, &roundtrip_wpos, 4);
    try testing.expectEqual(wpos, roundtrip_wpos);
    try testing.expectEqualSlices(u8, buf[0..wpos], roundtrip_buf[0..roundtrip_wpos]);
}

test "ApiVersionsResponse v3 rejects duplicate feature tags" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;

    ser.writeI16(&buf, &wpos, 0); // error_code
    ser.writeCompactArrayLen(&buf, &wpos, 0); // api_keys
    ser.writeI32(&buf, &wpos, 0); // throttle_time_ms
    ser.writeUnsignedVarint(&buf, &wpos, 2); // tagged fields
    ser.writeUnsignedVarint(&buf, &wpos, 3); // zk_migration_ready
    ser.writeUnsignedVarint(&buf, &wpos, 1);
    ser.writeBool(&buf, &wpos, true);
    ser.writeUnsignedVarint(&buf, &wpos, 3); // duplicate zk_migration_ready
    ser.writeUnsignedVarint(&buf, &wpos, 1);
    ser.writeBool(&buf, &wpos, false);

    var rpos: usize = 0;
    try testing.expectError(error.DuplicateTaggedField, ApiVersionsResponse.deserialize(testing.allocator, buf[0..wpos], &rpos, 3));
}

test "ApiVersionsResponse calcSize matches actual" {
    var api_keys = [_]ApiVersionsResponse.ApiVersion{
        .{ .api_key = 0, .min_version = 0, .max_version = 11 },
        .{ .api_key = 1, .min_version = 0, .max_version = 17 },
    };
    var supported_features = [_]ApiVersionsResponse.SupportedFeatureKey{.{
        .name = "metadata.version",
        .min_version = 0,
        .max_version = 1,
    }};
    var finalized_features = [_]ApiVersionsResponse.FinalizedFeatureKey{.{
        .name = "metadata.version",
        .max_version_level = 1,
        .min_version_level = 1,
    }};

    const resp = ApiVersionsResponse{
        .error_code = 0,
        .api_keys = &api_keys,
        .throttle_time_ms = 0,
        .supported_features = &supported_features,
        .finalized_features_epoch = 9,
        .finalized_features = &finalized_features,
        .zk_migration_ready = true,
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

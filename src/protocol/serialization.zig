const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Low-level Kafka protocol serialization primitives.
///
/// Kafka uses two encoding styles:
/// 1. Non-flexible (legacy): lengths are int16/int32, no tagged fields
/// 2. Flexible (modern): lengths are unsigned varints, tagged fields appended
///
/// This module provides read/write functions for both styles.

// ---------------------------------------------------------------
// Reading primitives
// ---------------------------------------------------------------

/// Read a non-compact string (int16 length prefix).
/// Returns null for length == -1 (nullable).
pub fn readString(buf: []const u8, pos: *usize) !?[]const u8 {
    const len = readI16(buf, pos);
    if (len < 0) return null;
    const length: usize = @intCast(len);
    if (pos.* + length > buf.len) return error.BufferUnderflow;
    const result = buf[pos.* .. pos.* + length];
    pos.* += length;
    return result;
}

/// Read a compact string (unsigned varint length prefix, length = actual + 1, 0 = null).
pub fn readCompactString(buf: []const u8, pos: *usize) !?[]const u8 {
    const raw_len = try readUnsignedVarint(buf, pos);
    if (raw_len == 0) return null;
    const length: usize = raw_len - 1;
    if (pos.* + length > buf.len) return error.BufferUnderflow;
    const result = buf[pos.* .. pos.* + length];
    pos.* += length;
    return result;
}

/// Read a non-compact byte array (int32 length prefix).
pub fn readBytes(buf: []const u8, pos: *usize) !?[]const u8 {
    const len = readI32(buf, pos);
    if (len < 0) return null;
    const length: usize = @intCast(len);
    if (pos.* + length > buf.len) return error.BufferUnderflow;
    const result = buf[pos.* .. pos.* + length];
    pos.* += length;
    return result;
}

/// Read compact bytes (unsigned varint length prefix, length = actual + 1, 0 = null).
pub fn readCompactBytes(buf: []const u8, pos: *usize) !?[]const u8 {
    const raw_len = try readUnsignedVarint(buf, pos);
    if (raw_len == 0) return null;
    const length: usize = raw_len - 1;
    if (pos.* + length > buf.len) return error.BufferUnderflow;
    const result = buf[pos.* .. pos.* + length];
    pos.* += length;
    return result;
}

/// Read a non-compact array length (int32). Returns null for -1.
pub fn readArrayLen(buf: []const u8, pos: *usize) !?usize {
    const len = readI32(buf, pos);
    if (len < 0) return null;
    return @intCast(len);
}

/// Read a compact array length (unsigned varint, length = actual + 1, 0 = null).
pub fn readCompactArrayLen(buf: []const u8, pos: *usize) !?usize {
    const raw_len = try readUnsignedVarint(buf, pos);
    if (raw_len == 0) return null;
    return raw_len - 1;
}

/// Read a UUID (16 bytes).
pub fn readUuid(buf: []const u8, pos: *usize) ![16]u8 {
    if (pos.* + 16 > buf.len) return error.BufferUnderflow;
    var result: [16]u8 = undefined;
    @memcpy(&result, buf[pos.* .. pos.* + 16]);
    pos.* += 16;
    return result;
}

/// Read a boolean (1 byte, 0 = false, 1 = true).
pub fn readBool(buf: []const u8, pos: *usize) !bool {
    return readU8(buf, pos) != 0;
}

pub fn readI8(buf: []const u8, pos: *usize) i8 {
    return @bitCast(readU8(buf, pos));
}

pub fn readU8(buf: []const u8, pos: *usize) u8 {
    const val = buf[pos.*];
    pos.* += 1;
    return val;
}

pub fn readU16(buf: []const u8, pos: *usize) u16 {
    const val = std.mem.readInt(u16, buf[pos.*..][0..2], .big);
    pos.* += 2;
    return val;
}

pub fn readI16(buf: []const u8, pos: *usize) i16 {
    const val = std.mem.readInt(i16, buf[pos.*..][0..2], .big);
    pos.* += 2;
    return val;
}

pub fn readI32(buf: []const u8, pos: *usize) i32 {
    const val = std.mem.readInt(i32, buf[pos.*..][0..4], .big);
    pos.* += 4;
    return val;
}

pub fn readI64(buf: []const u8, pos: *usize) i64 {
    const val = std.mem.readInt(i64, buf[pos.*..][0..8], .big);
    pos.* += 8;
    return val;
}

pub fn readU64(buf: []const u8, pos: *usize) u64 {
    const val = std.mem.readInt(u64, buf[pos.*..][0..8], .big);
    pos.* += 8;
    return val;
}

pub fn readU32(buf: []const u8, pos: *usize) u32 {
    const val = std.mem.readInt(u32, buf[pos.*..][0..4], .big);
    pos.* += 4;
    return val;
}

pub fn readF64(buf: []const u8, pos: *usize) f64 {
    const bits = std.mem.readInt(u64, buf[pos.*..][0..8], .big);
    pos.* += 8;
    return @bitCast(bits);
}

/// Read an unsigned varint (up to 5 bytes for u32-range values).
pub fn readUnsignedVarint(buf: []const u8, pos: *usize) !usize {
    var result: usize = 0;
    var shift: u6 = 0;
    for (0..5) |_| {
        if (pos.* >= buf.len) return error.BufferUnderflow;
        const byte = buf[pos.*];
        pos.* += 1;
        result |= @as(usize, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) return result;
        shift += 7;
    }
    return error.VarintTooLong;
}

/// Read tagged fields. Returns a list of raw tagged field entries.
/// Each entry is (tag, data_slice).
pub fn readTaggedFields(alloc: Allocator, buf: []const u8, pos: *usize) ![]TaggedField {
    const num_fields = try readUnsignedVarint(buf, pos);
    if (num_fields == 0) return &[_]TaggedField{};

    var fields = try alloc.alloc(TaggedField, num_fields);
    errdefer alloc.free(fields);

    for (0..num_fields) |i| {
        const tag = try readUnsignedVarint(buf, pos);
        const size = try readUnsignedVarint(buf, pos);
        if (pos.* + size > buf.len) return error.BufferUnderflow;
        fields[i] = .{
            .tag = tag,
            .data = buf[pos.* .. pos.* + size],
        };
        pos.* += size;
    }

    return fields;
}

/// Skip tagged fields without allocating.
pub fn skipTaggedFields(buf: []const u8, pos: *usize) !void {
    const num_fields = try readUnsignedVarint(buf, pos);
    for (0..num_fields) |_| {
        _ = try readUnsignedVarint(buf, pos); // tag
        const size = try readUnsignedVarint(buf, pos); // size
        if (pos.* + size > buf.len) return error.BufferUnderflow;
        pos.* += size;
    }
}

// ---------------------------------------------------------------
// Writing primitives
// ---------------------------------------------------------------

/// Write a non-compact string (int16 length prefix).
pub fn writeString(buf: []u8, pos: *usize, str: ?[]const u8) void {
    if (str) |s| {
        writeI16(buf, pos, @intCast(s.len));
        @memcpy(buf[pos.* .. pos.* + s.len], s);
        pos.* += s.len;
    } else {
        writeI16(buf, pos, -1);
    }
}

/// Write a compact string (unsigned varint length prefix).
pub fn writeCompactString(buf: []u8, pos: *usize, str: ?[]const u8) void {
    if (str) |s| {
        writeUnsignedVarint(buf, pos, s.len + 1);
        @memcpy(buf[pos.* .. pos.* + s.len], s);
        pos.* += s.len;
    } else {
        writeUnsignedVarint(buf, pos, 0);
    }
}

/// Write non-compact bytes (int32 length prefix).
pub fn writeBytesBuf(buf: []u8, pos: *usize, data: ?[]const u8) void {
    if (data) |d| {
        writeI32(buf, pos, @intCast(d.len));
        @memcpy(buf[pos.* .. pos.* + d.len], d);
        pos.* += d.len;
    } else {
        writeI32(buf, pos, -1);
    }
}

/// Write compact bytes (unsigned varint length prefix).
pub fn writeCompactBytes(buf: []u8, pos: *usize, data: ?[]const u8) void {
    if (data) |d| {
        writeUnsignedVarint(buf, pos, d.len + 1);
        @memcpy(buf[pos.* .. pos.* + d.len], d);
        pos.* += d.len;
    } else {
        writeUnsignedVarint(buf, pos, 0);
    }
}

/// Write non-compact array length (int32). Writes -1 for null.
pub fn writeArrayLen(buf: []u8, pos: *usize, len: ?usize) void {
    if (len) |l| {
        writeI32(buf, pos, @intCast(l));
    } else {
        writeI32(buf, pos, -1);
    }
}

/// Write compact array length (unsigned varint, actual + 1, 0 = null).
pub fn writeCompactArrayLen(buf: []u8, pos: *usize, len: ?usize) void {
    if (len) |l| {
        writeUnsignedVarint(buf, pos, l + 1);
    } else {
        writeUnsignedVarint(buf, pos, 0);
    }
}

/// Write a UUID (16 bytes).
pub fn writeUuid(buf: []u8, pos: *usize, uuid: [16]u8) void {
    @memcpy(buf[pos.* .. pos.* + 16], &uuid);
    pos.* += 16;
}

/// Write a boolean.
pub fn writeBool(buf: []u8, pos: *usize, val: bool) void {
    writeU8(buf, pos, if (val) 1 else 0);
}

pub fn writeU8(buf: []u8, pos: *usize, val: u8) void {
    buf[pos.*] = val;
    pos.* += 1;
}

pub fn writeU16(buf: []u8, pos: *usize, val: u16) void {
    std.mem.writeInt(u16, buf[pos.*..][0..2], val, .big);
    pos.* += 2;
}

pub fn writeI8(buf: []u8, pos: *usize, val: i8) void {
    writeU8(buf, pos, @bitCast(val));
}

pub fn writeI16(buf: []u8, pos: *usize, val: i16) void {
    std.mem.writeInt(i16, buf[pos.*..][0..2], val, .big);
    pos.* += 2;
}

pub fn writeI32(buf: []u8, pos: *usize, val: i32) void {
    std.mem.writeInt(i32, buf[pos.*..][0..4], val, .big);
    pos.* += 4;
}

pub fn writeI64(buf: []u8, pos: *usize, val: i64) void {
    std.mem.writeInt(i64, buf[pos.*..][0..8], val, .big);
    pos.* += 8;
}

pub fn writeU64(buf: []u8, pos: *usize, val: u64) void {
    std.mem.writeInt(u64, buf[pos.*..][0..8], val, .big);
    pos.* += 8;
}

pub fn writeU32(buf: []u8, pos: *usize, val: u32) void {
    std.mem.writeInt(u32, buf[pos.*..][0..4], val, .big);
    pos.* += 4;
}

pub fn writeF64(buf: []u8, pos: *usize, val: f64) void {
    std.mem.writeInt(u64, buf[pos.*..][0..8], @bitCast(val), .big);
    pos.* += 8;
}

/// Write an unsigned varint.
pub fn writeUnsignedVarint(buf: []u8, pos: *usize, value: usize) void {
    var v = value;
    while (v > 0x7F) {
        buf[pos.*] = @as(u8, @truncate(v & 0x7F)) | 0x80;
        pos.* += 1;
        v >>= 7;
    }
    buf[pos.*] = @as(u8, @truncate(v));
    pos.* += 1;
}

/// Write empty tagged fields (just the count = 0).
pub fn writeEmptyTaggedFields(buf: []u8, pos: *usize) void {
    writeUnsignedVarint(buf, pos, 0);
}

/// Write tagged fields from a list.
pub fn writeTaggedFields(buf: []u8, pos: *usize, fields: []const TaggedField) void {
    writeUnsignedVarint(buf, pos, fields.len);
    for (fields) |field| {
        writeUnsignedVarint(buf, pos, field.tag);
        writeUnsignedVarint(buf, pos, field.data.len);
        @memcpy(buf[pos.* .. pos.* + field.data.len], field.data);
        pos.* += field.data.len;
    }
}

// ---------------------------------------------------------------
// Size calculation helpers
// ---------------------------------------------------------------

/// Size of an unsigned varint encoding.
pub fn unsignedVarintSize(value: usize) usize {
    var v = value;
    var size: usize = 1;
    while (v > 0x7F) : (size += 1) {
        v >>= 7;
    }
    return size;
}

/// Size of a non-compact string.
pub fn stringSize(str: ?[]const u8) usize {
    if (str) |s| return 2 + s.len; // int16 length + data
    return 2; // just -1
}

/// Size of a compact string.
pub fn compactStringSize(str: ?[]const u8) usize {
    if (str) |s| return unsignedVarintSize(s.len + 1) + s.len;
    return 1; // varint 0
}

/// Size of non-compact bytes.
pub fn bytesSize(data: ?[]const u8) usize {
    if (data) |d| return 4 + d.len;
    return 4;
}

/// Size of compact bytes.
pub fn compactBytesSize(data: ?[]const u8) usize {
    if (data) |d| return unsignedVarintSize(d.len + 1) + d.len;
    return 1;
}

// ---------------------------------------------------------------
// Types
// ---------------------------------------------------------------

/// A raw tagged field preserved during deserialization.
pub const TaggedField = struct {
    tag: usize,
    data: []const u8,
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "readI16/writeI16 round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeI16(&buf, &wpos, 12345);
    writeI16(&buf, &wpos, -1);

    var rpos: usize = 0;
    try testing.expectEqual(@as(i16, 12345), readI16(&buf, &rpos));
    try testing.expectEqual(@as(i16, -1), readI16(&buf, &rpos));
}

test "readI32/writeI32 round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeI32(&buf, &wpos, 0x12345678);

    var rpos: usize = 0;
    try testing.expectEqual(@as(i32, 0x12345678), readI32(&buf, &rpos));
}

test "string round-trip (non-compact)" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;
    writeString(&buf, &wpos, "hello");
    writeString(&buf, &wpos, null);

    var rpos: usize = 0;
    const s1 = try readString(&buf, &rpos);
    try testing.expectEqualStrings("hello", s1.?);
    const s2 = try readString(&buf, &rpos);
    try testing.expect(s2 == null);
}

test "compact string round-trip" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;
    writeCompactString(&buf, &wpos, "world");
    writeCompactString(&buf, &wpos, null);

    var rpos: usize = 0;
    const s1 = try readCompactString(&buf, &rpos);
    try testing.expectEqualStrings("world", s1.?);
    const s2 = try readCompactString(&buf, &rpos);
    try testing.expect(s2 == null);
}

test "compact array len round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeCompactArrayLen(&buf, &wpos, 5);
    writeCompactArrayLen(&buf, &wpos, null);
    writeCompactArrayLen(&buf, &wpos, 0);

    var rpos: usize = 0;
    try testing.expectEqual(@as(?usize, 5), try readCompactArrayLen(&buf, &rpos));
    try testing.expectEqual(@as(?usize, null), try readCompactArrayLen(&buf, &rpos));
    try testing.expectEqual(@as(?usize, 0), try readCompactArrayLen(&buf, &rpos));
}

test "tagged fields round-trip" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;

    const fields = [_]TaggedField{
        .{ .tag = 0, .data = "tag0data" },
        .{ .tag = 5, .data = "t5" },
    };
    writeTaggedFields(&buf, &wpos, &fields);

    var rpos: usize = 0;
    const read_fields = try readTaggedFields(testing.allocator, &buf, &rpos);
    defer testing.allocator.free(read_fields);

    try testing.expectEqual(@as(usize, 2), read_fields.len);
    try testing.expectEqual(@as(usize, 0), read_fields[0].tag);
    try testing.expectEqualStrings("tag0data", read_fields[0].data);
    try testing.expectEqual(@as(usize, 5), read_fields[1].tag);
    try testing.expectEqualStrings("t5", read_fields[1].data);
}

test "empty tagged fields" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeEmptyTaggedFields(&buf, &wpos);

    var rpos: usize = 0;
    try skipTaggedFields(&buf, &rpos);
    try testing.expectEqual(@as(usize, 1), rpos); // just the varint 0
}

test "uuid round-trip" {
    var buf: [32]u8 = undefined;
    const uuid = [16]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    var wpos: usize = 0;
    writeUuid(&buf, &wpos, uuid);

    var rpos: usize = 0;
    const read_uuid = try readUuid(&buf, &rpos);
    try testing.expectEqualSlices(u8, &uuid, &read_uuid);
}

test "unsigned varint round-trip" {
    var buf: [16]u8 = undefined;

    const test_values = [_]usize{ 0, 1, 127, 128, 16383, 16384, 2097151, 268435455 };
    for (test_values) |val| {
        var wpos: usize = 0;
        writeUnsignedVarint(&buf, &wpos, val);

        var rpos: usize = 0;
        const read_val = try readUnsignedVarint(&buf, &rpos);
        try testing.expectEqual(val, read_val);
        try testing.expectEqual(wpos, rpos);
    }
}

test "size calculations" {
    try testing.expectEqual(@as(usize, 7), stringSize("hello"));
    try testing.expectEqual(@as(usize, 2), stringSize(null));
    try testing.expectEqual(@as(usize, 6), compactStringSize("hello")); // varint(6) + 5
    try testing.expectEqual(@as(usize, 1), compactStringSize(null)); // varint(0)
}

test "readString buffer underflow returns error" {
    // String with claimed length 10 but buffer only has 2 bytes total (just the length prefix)
    var buf: [2]u8 = undefined;
    std.mem.writeInt(i16, &buf, 10, .big); // claim 10 bytes of string data

    var rpos: usize = 0;
    const result = readString(&buf, &rpos);
    try testing.expectError(error.BufferUnderflow, result);
}

test "readCompactString buffer underflow returns error" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeUnsignedVarint(&buf, &wpos, 100); // claim 99 bytes (100-1)

    var rpos: usize = 0;
    const result = readCompactString(&buf, &rpos);
    try testing.expectError(error.BufferUnderflow, result);
}

test "readBytes buffer underflow returns error" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeI32(&buf, &wpos, 1000); // claim 1000 bytes

    var rpos: usize = 0;
    const result = readBytes(&buf, &rpos);
    try testing.expectError(error.BufferUnderflow, result);
}

test "readCompactBytes buffer underflow returns error" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeUnsignedVarint(&buf, &wpos, 200); // claim 199 bytes

    var rpos: usize = 0;
    const result = readCompactBytes(&buf, &rpos);
    try testing.expectError(error.BufferUnderflow, result);
}

test "readUnsignedVarint buffer underflow on empty" {
    const buf = [_]u8{};
    var pos: usize = 0;
    const result = readUnsignedVarint(&buf, &pos);
    try testing.expectError(error.BufferUnderflow, result);
}

test "readUnsignedVarint too long returns error" {
    // 6 continuation bytes — exceeds the 5-byte limit
    const buf = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 };
    var pos: usize = 0;
    const result = readUnsignedVarint(&buf, &pos);
    try testing.expectError(error.VarintTooLong, result);
}

test "readUuid buffer underflow" {
    var buf: [8]u8 = undefined;
    var pos: usize = 0;
    const result = readUuid(&buf, &pos);
    try testing.expectError(error.BufferUnderflow, result);
}

test "readBool round-trip" {
    var buf: [4]u8 = undefined;
    var wpos: usize = 0;
    writeBool(&buf, &wpos, true);
    writeBool(&buf, &wpos, false);

    var rpos: usize = 0;
    try testing.expect(try readBool(&buf, &rpos));
    try testing.expect(!try readBool(&buf, &rpos));
}

test "readI64/writeI64 round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeI64(&buf, &wpos, 0x0102030405060708);
    writeI64(&buf, &wpos, -1);

    var rpos: usize = 0;
    try testing.expectEqual(@as(i64, 0x0102030405060708), readI64(&buf, &rpos));
    try testing.expectEqual(@as(i64, -1), readI64(&buf, &rpos));
}

test "readF64/writeF64 round-trip" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeF64(&buf, &wpos, 3.14159);

    var rpos: usize = 0;
    try testing.expectApproxEqAbs(@as(f64, 3.14159), readF64(&buf, &rpos), 1e-10);
}

test "bytes round-trip (non-compact)" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;
    writeBytesBuf(&buf, &wpos, "binary data");
    writeBytesBuf(&buf, &wpos, null);

    var rpos: usize = 0;
    const b1 = try readBytes(&buf, &rpos);
    try testing.expectEqualStrings("binary data", b1.?);
    const b2 = try readBytes(&buf, &rpos);
    try testing.expect(b2 == null);
}

test "compact bytes round-trip" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;
    writeCompactBytes(&buf, &wpos, "compact data");
    writeCompactBytes(&buf, &wpos, null);

    var rpos: usize = 0;
    const b1 = try readCompactBytes(&buf, &rpos);
    try testing.expectEqualStrings("compact data", b1.?);
    const b2 = try readCompactBytes(&buf, &rpos);
    try testing.expect(b2 == null);
}

test "array len round-trip (non-compact)" {
    var buf: [16]u8 = undefined;
    var wpos: usize = 0;
    writeArrayLen(&buf, &wpos, 42);
    writeArrayLen(&buf, &wpos, null);
    writeArrayLen(&buf, &wpos, 0);

    var rpos: usize = 0;
    try testing.expectEqual(@as(?usize, 42), try readArrayLen(&buf, &rpos));
    try testing.expectEqual(@as(?usize, null), try readArrayLen(&buf, &rpos));
    try testing.expectEqual(@as(?usize, 0), try readArrayLen(&buf, &rpos));
}

test "skipTaggedFields with data" {
    var buf: [64]u8 = undefined;
    var wpos: usize = 0;

    const fields = [_]TaggedField{
        .{ .tag = 0, .data = "tag0data" },
        .{ .tag = 5, .data = "t5" },
    };
    writeTaggedFields(&buf, &wpos, &fields);

    var rpos: usize = 0;
    try skipTaggedFields(&buf, &rpos);
    try testing.expectEqual(wpos, rpos); // should consume same number of bytes
}

test "unsignedVarintSize edge values" {
    try testing.expectEqual(@as(usize, 1), unsignedVarintSize(0));
    try testing.expectEqual(@as(usize, 1), unsignedVarintSize(127));
    try testing.expectEqual(@as(usize, 2), unsignedVarintSize(128));
    try testing.expectEqual(@as(usize, 2), unsignedVarintSize(16383));
    try testing.expectEqual(@as(usize, 3), unsignedVarintSize(16384));
}

test "bytesSize and compactBytesSize" {
    try testing.expectEqual(@as(usize, 4 + 5), bytesSize("hello"));
    try testing.expectEqual(@as(usize, 4), bytesSize(null));
    try testing.expectEqual(@as(usize, 1 + 5), compactBytesSize("hello")); // varint(6) + 5
    try testing.expectEqual(@as(usize, 1), compactBytesSize(null)); // varint(0)
}

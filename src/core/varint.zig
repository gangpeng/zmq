const std = @import("std");
const testing = std.testing;

/// Kafka-style varint encoding/decoding.
///
/// Kafka uses two varint formats:
/// 1. **Unsigned varint** (for lengths, counts, tag IDs in flexible versions):
///    - Each byte uses 7 data bits + 1 continuation bit (MSB).
///    - Little-endian byte order within the varint.
///    - Maximum 5 bytes for u32, 10 bytes for u64.
///
/// 2. **Signed zigzag varint** (for deltas in record batches):
///    - Maps signed integers to unsigned using zigzag encoding:
///      0 → 0, -1 → 1, 1 → 2, -2 → 3, 2 → 4, ...
///    - Then encoded as unsigned varint.

/// Encode an unsigned 32-bit varint into the buffer.
/// Returns the number of bytes written (1–5).
pub fn encodeUnsignedVarint32(buf: []u8, value: u32) usize {
    var v = value;
    var i: usize = 0;
    while (v > 0x7F) : (i += 1) {
        buf[i] = @as(u8, @truncate(v & 0x7F)) | 0x80;
        v >>= 7;
    }
    buf[i] = @as(u8, @truncate(v));
    return i + 1;
}

/// Encode an unsigned 64-bit varint into the buffer.
/// Returns the number of bytes written (1–10).
pub fn encodeUnsignedVarint64(buf: []u8, value: u64) usize {
    var v = value;
    var i: usize = 0;
    while (v > 0x7F) : (i += 1) {
        buf[i] = @as(u8, @truncate(v & 0x7F)) | 0x80;
        v >>= 7;
    }
    buf[i] = @as(u8, @truncate(v));
    return i + 1;
}

/// Decode an unsigned 32-bit varint from the buffer.
/// Returns the decoded value and number of bytes consumed.
pub fn decodeUnsignedVarint32(buf: []const u8) !struct { value: u32, bytes_read: usize } {
    var result: u32 = 0;
    var shift: u5 = 0;
    for (buf, 0..) |byte, i| {
        if (i >= 5) return error.VarintTooLong;
        result |= @as(u32, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) {
            return .{ .value = result, .bytes_read = i + 1 };
        }
        shift +%= 7;
    }
    return error.VarintTruncated;
}

/// Decode an unsigned 64-bit varint from the buffer.
/// Returns the decoded value and number of bytes consumed.
pub fn decodeUnsignedVarint64(buf: []const u8) !struct { value: u64, bytes_read: usize } {
    var result: u64 = 0;
    var shift: u6 = 0;
    for (buf, 0..) |byte, i| {
        if (i >= 10) return error.VarintTooLong;
        result |= @as(u64, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) {
            return .{ .value = result, .bytes_read = i + 1 };
        }
        shift +%= 7;
    }
    return error.VarintTruncated;
}

/// Zigzag encode a signed 32-bit integer to unsigned.
/// Maps: 0→0, -1→1, 1→2, -2→3, 2→4, ...
pub fn zigzagEncode32(value: i32) u32 {
    return @bitCast((value << 1) ^ (value >> 31));
}

/// Zigzag decode an unsigned 32-bit integer back to signed.
pub fn zigzagDecode32(value: u32) i32 {
    return @as(i32, @bitCast((value >> 1))) ^ -@as(i32, @bitCast(value & 1));
}

/// Zigzag encode a signed 64-bit integer to unsigned.
pub fn zigzagEncode64(value: i64) u64 {
    return @bitCast((value << 1) ^ (value >> 63));
}

/// Zigzag decode an unsigned 64-bit integer back to signed.
pub fn zigzagDecode64(value: u64) i64 {
    return @as(i64, @bitCast((value >> 1))) ^ -@as(i64, @bitCast(value & 1));
}

/// Encode a signed 32-bit value as a zigzag varint.
/// Returns number of bytes written.
pub fn encodeSignedVarint32(buf: []u8, value: i32) usize {
    return encodeUnsignedVarint32(buf, zigzagEncode32(value));
}

/// Decode a signed 32-bit zigzag varint.
pub fn decodeSignedVarint32(buf: []const u8) !struct { value: i32, bytes_read: usize } {
    const result = try decodeUnsignedVarint32(buf);
    return .{ .value = zigzagDecode32(result.value), .bytes_read = result.bytes_read };
}

/// Encode a signed 64-bit value as a zigzag varint (varlong).
/// Returns number of bytes written.
pub fn encodeSignedVarint64(buf: []u8, value: i64) usize {
    return encodeUnsignedVarint64(buf, zigzagEncode64(value));
}

/// Decode a signed 64-bit zigzag varint (varlong).
pub fn decodeSignedVarint64(buf: []const u8) !struct { value: i64, bytes_read: usize } {
    const result = try decodeUnsignedVarint64(buf);
    return .{ .value = zigzagDecode64(result.value), .bytes_read = result.bytes_read };
}

/// Compute the encoded size of an unsigned 32-bit varint without actually encoding.
pub fn unsignedVarint32Size(value: u32) usize {
    if (value < (1 << 7)) return 1;
    if (value < (1 << 14)) return 2;
    if (value < (1 << 21)) return 3;
    if (value < (1 << 28)) return 4;
    return 5;
}

/// Compute the encoded size of an unsigned 64-bit varint without actually encoding.
pub fn unsignedVarint64Size(value: u64) usize {
    var v = value;
    var size: usize = 1;
    while (v > 0x7F) : (size += 1) {
        v >>= 7;
    }
    return size;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "unsigned varint32 round-trip" {
    var buf: [5]u8 = undefined;

    const test_values = [_]u32{ 0, 1, 127, 128, 255, 256, 16383, 16384, 0x7FFFFFFF, 0xFFFFFFFF };
    for (test_values) |val| {
        const written = encodeUnsignedVarint32(&buf, val);
        const result = try decodeUnsignedVarint32(buf[0..written]);
        try testing.expectEqual(val, result.value);
        try testing.expectEqual(written, result.bytes_read);
    }
}

test "unsigned varint64 round-trip" {
    var buf: [10]u8 = undefined;

    const test_values = [_]u64{ 0, 1, 127, 128, 0xFFFFFFFF, 0x100000000, 0xFFFFFFFFFFFFFFFF };
    for (test_values) |val| {
        const written = encodeUnsignedVarint64(&buf, val);
        const result = try decodeUnsignedVarint64(buf[0..written]);
        try testing.expectEqual(val, result.value);
        try testing.expectEqual(written, result.bytes_read);
    }
}

test "zigzag encoding" {
    try testing.expectEqual(@as(u32, 0), zigzagEncode32(0));
    try testing.expectEqual(@as(u32, 1), zigzagEncode32(-1));
    try testing.expectEqual(@as(u32, 2), zigzagEncode32(1));
    try testing.expectEqual(@as(u32, 3), zigzagEncode32(-2));
    try testing.expectEqual(@as(u32, 4), zigzagEncode32(2));

    // Round-trip
    const test_values = [_]i32{ 0, 1, -1, 2, -2, 127, -128, 32767, -32768, 0x7FFFFFFF, -0x7FFFFFFF - 1 };
    for (test_values) |val| {
        try testing.expectEqual(val, zigzagDecode32(zigzagEncode32(val)));
    }
}

test "zigzag64 encoding" {
    const test_values = [_]i64{ 0, 1, -1, 2, -2, 0x7FFFFFFFFFFFFFFF, -0x7FFFFFFFFFFFFFFF - 1 };
    for (test_values) |val| {
        try testing.expectEqual(val, zigzagDecode64(zigzagEncode64(val)));
    }
}

test "signed varint32 round-trip" {
    var buf: [5]u8 = undefined;

    const test_values = [_]i32{ 0, 1, -1, 127, -128, 32767, -32768, 0x7FFFFFFF, -0x7FFFFFFF - 1 };
    for (test_values) |val| {
        const written = encodeSignedVarint32(&buf, val);
        const result = try decodeSignedVarint32(buf[0..written]);
        try testing.expectEqual(val, result.value);
        try testing.expectEqual(written, result.bytes_read);
    }
}

test "varint size computation" {
    try testing.expectEqual(@as(usize, 1), unsignedVarint32Size(0));
    try testing.expectEqual(@as(usize, 1), unsignedVarint32Size(127));
    try testing.expectEqual(@as(usize, 2), unsignedVarint32Size(128));
    try testing.expectEqual(@as(usize, 2), unsignedVarint32Size(16383));
    try testing.expectEqual(@as(usize, 3), unsignedVarint32Size(16384));
    try testing.expectEqual(@as(usize, 5), unsignedVarint32Size(0xFFFFFFFF));
}

test "varint truncated input" {
    // Continuation bit set but no more bytes
    const buf = [_]u8{0x80};
    try testing.expectError(error.VarintTruncated, decodeUnsignedVarint32(&buf));
}

test "varint too long" {
    // 6 bytes with continuation bits — too long for u32
    const buf = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 };
    try testing.expectError(error.VarintTooLong, decodeUnsignedVarint32(&buf));
}

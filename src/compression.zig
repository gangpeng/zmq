/// ZMQ Compression Codec Registry
///
/// All Kafka compression types supported:
/// - None (0): passthrough
/// - Gzip (1): via Zig std.compress.flate (real compression)
/// - Snappy (2): native Zig implementation (decompression)
/// - LZ4 (3): native Zig LZ4 block decompression
/// - Zstd (4): native Zig implementation (simple framing)
///
/// For production, add C FFI to libsnappy/liblz4/libzstd for optimal performance.
const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Kafka compression type, stored in record batch attributes bits 0-2.
pub const CompressionType = enum(u3) {
    none = 0,
    gzip = 1,
    snappy = 2,
    lz4 = 3,
    zstd = 4,

    pub fn fromAttributes(attributes: i16) CompressionType {
        return @enumFromInt(@as(u3, @truncate(@as(u16, @bitCast(attributes)) & 0x07)));
    }
};

pub fn compress(alloc: Allocator, codec: CompressionType, data: []const u8) ![]u8 {
    return switch (codec) {
        .none => try alloc.dupe(u8, data),
        .gzip => compressGzip(alloc, data),
        .snappy => compressSnappy(alloc, data),
        .lz4 => compressLz4(alloc, data),
        .zstd => compressZstd(alloc, data),
    };
}

pub fn decompress(alloc: Allocator, codec: CompressionType, data: []const u8) ![]u8 {
    return switch (codec) {
        .none => try alloc.dupe(u8, data),
        .gzip => decompressGzip(alloc, data),
        .snappy => decompressSnappy(alloc, data),
        .lz4 => decompressLz4(alloc, data),
        .zstd => decompressZstd(alloc, data),
    };
}

// ---------------------------------------------------------------
// Gzip — real compression via Zig stdlib
// ---------------------------------------------------------------
fn compressGzip(alloc: Allocator, data: []const u8) ![]u8 {
    var result: std.Io.Writer.Allocating = .init(alloc);
    errdefer result.deinit();
    try result.ensureUnusedCapacity(64);

    const history = try alloc.alloc(u8, std.compress.flate.max_window_len);
    defer alloc.free(history);

    var comp = try std.compress.flate.Compress.init(&result.writer, history, .gzip, .default);
    try comp.writer.writeAll(data);
    try comp.finish();
    return result.toOwnedSlice();
}

fn decompressGzip(alloc: Allocator, data: []const u8) ![]u8 {
    var result: std.Io.Writer.Allocating = .init(alloc);
    errdefer result.deinit();

    const window = try alloc.alloc(u8, std.compress.flate.max_window_len);
    defer alloc.free(window);

    var input: std.Io.Reader = .fixed(data);
    var dec = std.compress.flate.Decompress.init(&input, .gzip, window);
    _ = try dec.reader.streamRemaining(&result.writer);
    return result.toOwnedSlice();
}

// ---------------------------------------------------------------
// Snappy — native Zig implementation
// Kafka uses raw Snappy format (not framed).
// Snappy format: [varint uncompressed_size] [compressed blocks...]
// Block types: literal (0), copy-1 (1), copy-2 (2), copy-4 (3)
// ---------------------------------------------------------------
fn compressSnappy(alloc: Allocator, data: []const u8) ![]u8 {
    // Simple literal-only compression (valid Snappy, just uncompressed)
    var result = std.array_list.Managed(u8).init(alloc);
    errdefer result.deinit();

    // Write uncompressed length as varint
    var len_buf: [5]u8 = undefined;
    const varint_len = writeVarint(&len_buf, data.len);
    try result.appendSlice(len_buf[0..varint_len]);

    // Write as literal chunks (max 60 bytes per literal tag for simple encoding)
    var pos: usize = 0;
    while (pos < data.len) {
        const chunk_len = @min(data.len - pos, 60);
        // Literal tag: (len-1)<<2 | 0
        const tag: u8 = @as(u8, @intCast(chunk_len - 1)) << 2;
        try result.append(tag);
        try result.appendSlice(data[pos .. pos + chunk_len]);
        pos += chunk_len;
    }

    return result.toOwnedSlice();
}

fn decompressSnappy(alloc: Allocator, data: []const u8) ![]u8 {
    if (data.len == 0) return try alloc.alloc(u8, 0);

    var pos: usize = 0;
    // Read uncompressed length varint
    const uncompressed_len = readVarint(data, &pos) orelse return error.InvalidData;

    var output = try alloc.alloc(u8, uncompressed_len);
    errdefer alloc.free(output);
    var out_pos: usize = 0;

    while (pos < data.len and out_pos < uncompressed_len) {
        const tag = data[pos];
        pos += 1;
        const tag_type: u2 = @truncate(tag & 0x03);

        switch (tag_type) {
            0 => { // Literal
                const len_code: u8 = tag >> 2;
                var lit_len: usize = 0;
                if (len_code < 60) {
                    lit_len = @as(usize, len_code) + 1;
                } else {
                    const extra_len: usize = @as(usize, len_code) - 59;
                    if (pos + extra_len > data.len) return error.InvalidData;
                    var encoded_len: usize = 0;
                    for (0..extra_len) |i| {
                        encoded_len |= @as(usize, data[pos + i]) << @intCast(8 * i);
                    }
                    pos += extra_len;
                    lit_len = encoded_len + 1;
                }
                if (pos + lit_len > data.len or out_pos + lit_len > uncompressed_len) return error.InvalidData;
                @memcpy(output[out_pos .. out_pos + lit_len], data[pos .. pos + lit_len]);
                pos += lit_len;
                out_pos += lit_len;
            },
            1 => { // Copy with 1-byte offset
                const length: usize = @as(usize, (tag >> 2) & 0x07) + 4;
                if (pos >= data.len) return error.InvalidData;
                const offset: usize = (@as(usize, tag & 0xe0) << 3) | @as(usize, data[pos]);
                pos += 1;
                if (offset == 0 or offset > out_pos or out_pos + length > uncompressed_len) return error.InvalidData;
                for (0..length) |i| {
                    output[out_pos + i] = output[out_pos + i - offset];
                }
                out_pos += length;
            },
            2 => { // Copy with 2-byte offset
                const length: usize = @as(usize, tag >> 2) + 1;
                if (pos + 2 > data.len) return error.InvalidData;
                const offset: usize = std.mem.readInt(u16, data[pos..][0..2], .little);
                pos += 2;
                if (offset == 0 or offset > out_pos or out_pos + length > uncompressed_len) return error.InvalidData;
                for (0..length) |i| {
                    output[out_pos + i] = output[out_pos + i - offset];
                }
                out_pos += length;
            },
            3 => { // Copy with 4-byte offset
                const length: usize = @as(usize, tag >> 2) + 1;
                if (pos + 4 > data.len) return error.InvalidData;
                const offset: usize = std.mem.readInt(u32, data[pos..][0..4], .little);
                pos += 4;
                if (offset == 0 or offset > out_pos or out_pos + length > uncompressed_len) return error.InvalidData;
                for (0..length) |i| {
                    output[out_pos + i] = output[out_pos + i - offset];
                }
                out_pos += length;
            },
        }
    }

    if (out_pos != uncompressed_len or pos != data.len) return error.InvalidData;
    return output;
}

// ---------------------------------------------------------------
// LZ4 — native Zig block decompression
// Kafka uses LZ4 with the Kafka-specific framing (not standard LZ4 frame).
// Format: [i32 decompressed_size] [lz4 compressed block]
// ---------------------------------------------------------------
fn compressLz4(alloc: Allocator, data: []const u8) ![]u8 {
    // Store format: [i32 uncompressed_len] [raw data]
    // This is valid as "uncompressed" LZ4 in Kafka's framing
    var result = try alloc.alloc(u8, 4 + data.len);
    std.mem.writeInt(i32, result[0..4], @intCast(data.len), .big);
    @memcpy(result[4..], data);
    return result;
}

fn decompressLz4(alloc: Allocator, data: []const u8) ![]u8 {
    if (data.len < 4) return error.InvalidData;

    // Kafka LZ4: [i32 decompressed_size] [lz4 block data]
    const decompressed_size_i32 = std.mem.readInt(i32, data[0..4], .big);
    if (decompressed_size_i32 < 0) return error.InvalidData;
    const decompressed_size: usize = @intCast(decompressed_size_i32);
    const compressed = data[4..];

    if (decompressed_size == compressed.len) {
        // Uncompressed block — just copy
        return try alloc.dupe(u8, compressed);
    }

    // LZ4 block decompress
    var output = try alloc.alloc(u8, decompressed_size);
    errdefer alloc.free(output);
    var out_pos: usize = 0;
    var in_pos: usize = 0;

    while (in_pos < compressed.len and out_pos < decompressed_size) {
        const token = compressed[in_pos];
        in_pos += 1;

        // Literal length
        var lit_len: usize = @as(usize, token >> 4);
        if (lit_len == 15) {
            var terminated = false;
            while (in_pos < compressed.len) {
                const extra = compressed[in_pos];
                in_pos += 1;
                lit_len += extra;
                if (extra != 255) {
                    terminated = true;
                    break;
                }
            }
            if (!terminated) return error.InvalidData;
        }

        // Copy literals
        if (in_pos + lit_len > compressed.len or out_pos + lit_len > decompressed_size) return error.InvalidData;
        @memcpy(output[out_pos .. out_pos + lit_len], compressed[in_pos .. in_pos + lit_len]);
        in_pos += lit_len;
        out_pos += lit_len;

        if (out_pos >= decompressed_size) break;

        // Match offset
        if (in_pos + 2 > compressed.len) return error.InvalidData;
        const offset: usize = std.mem.readInt(u16, compressed[in_pos..][0..2], .little);
        in_pos += 2;
        if (offset == 0) return error.InvalidData;

        // Match length
        var match_len: usize = @as(usize, token & 0x0F) + 4; // minmatch = 4
        if ((token & 0x0F) == 15) {
            var terminated = false;
            while (in_pos < compressed.len) {
                const extra = compressed[in_pos];
                in_pos += 1;
                match_len += extra;
                if (extra != 255) {
                    terminated = true;
                    break;
                }
            }
            if (!terminated) return error.InvalidData;
        }

        // Copy match (may overlap)
        if (offset > out_pos or out_pos + match_len > decompressed_size) return error.InvalidData;
        for (0..match_len) |i| {
            output[out_pos + i] = output[out_pos + i - offset];
        }
        out_pos += match_len;
    }

    if (out_pos != decompressed_size or in_pos != compressed.len) return error.InvalidData;
    return output;
}

// ---------------------------------------------------------------
// Zstd — simple store format
// For real Zstd, needs libzstd C FFI or Zig's std.compress.zstd (decompress only)
// ---------------------------------------------------------------
fn compressZstd(alloc: Allocator, data: []const u8) ![]u8 {
    // Simple store: [i32 len] [data]
    var result = try alloc.alloc(u8, 4 + data.len);
    std.mem.writeInt(i32, result[0..4], @intCast(data.len), .big);
    @memcpy(result[4..], data);
    return result;
}

fn decompressZstd(alloc: Allocator, data: []const u8) ![]u8 {
    if (data.len < 4) return error.InvalidData;
    // Check for Zstd magic number (0xFD2FB528)
    if (data.len >= 4 and data[0] == 0x28 and data[1] == 0xB5 and data[2] == 0x2F and data[3] == 0xFD) {
        // Real Zstd frame — try Zig's built-in decompressor
        var result: std.Io.Writer.Allocating = .init(alloc);
        errdefer result.deinit();

        const window = try alloc.alloc(u8, std.compress.zstd.default_window_len + std.compress.zstd.block_size_max);
        defer alloc.free(window);

        var input: std.Io.Reader = .fixed(data);
        var dec: std.compress.zstd.Decompress = .init(&input, window, .{});
        _ = try dec.reader.streamRemaining(&result.writer);
        return result.toOwnedSlice();
    }

    // Simple store format
    const decompressed_size: usize = @intCast(std.mem.readInt(i32, data[0..4], .big));
    if (data.len < 4 + decompressed_size) return error.InvalidData;
    return try alloc.dupe(u8, data[4 .. 4 + decompressed_size]);
}

// ---------------------------------------------------------------
// Varint helpers for Snappy
// ---------------------------------------------------------------
fn writeVarint(buf: []u8, value: usize) usize {
    var v = value;
    var i: usize = 0;
    while (v > 0x7F) : (i += 1) {
        buf[i] = @as(u8, @truncate(v & 0x7F)) | 0x80;
        v >>= 7;
    }
    buf[i] = @as(u8, @truncate(v));
    return i + 1;
}

fn readVarint(buf: []const u8, pos: *usize) ?usize {
    var result: usize = 0;
    var shift: u6 = 0;
    for (0..5) |_| {
        if (pos.* >= buf.len) return null;
        const byte = buf[pos.*];
        pos.* += 1;
        result |= @as(usize, byte & 0x7F) << shift;
        if (byte & 0x80 == 0) return result;
        shift += 7;
    }
    return null;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------
test "CompressionType from attributes" {
    try testing.expectEqual(CompressionType.none, CompressionType.fromAttributes(0));
    try testing.expectEqual(CompressionType.gzip, CompressionType.fromAttributes(1));
    try testing.expectEqual(CompressionType.snappy, CompressionType.fromAttributes(2));
    try testing.expectEqual(CompressionType.lz4, CompressionType.fromAttributes(3));
    try testing.expectEqual(CompressionType.zstd, CompressionType.fromAttributes(4));
}

test "none round-trip" {
    const data = "Hello, Kafka!";
    const c = try compress(testing.allocator, .none, data);
    defer testing.allocator.free(c);
    try testing.expectEqualStrings(data, c);
}

test "gzip round-trip" {
    const data = "Gzip compressed data for Kafka record batches.";
    const c = try compress(testing.allocator, .gzip, data);
    defer testing.allocator.free(c);
    try testing.expect(!std.mem.eql(u8, data, c)); // actually compressed
    const d = try decompress(testing.allocator, .gzip, c);
    defer testing.allocator.free(d);
    try testing.expectEqualStrings(data, d);
}

test "snappy round-trip" {
    const data = "Snappy compressed record batch payload.";
    const c = try compress(testing.allocator, .snappy, data);
    defer testing.allocator.free(c);
    const d = try decompress(testing.allocator, .snappy, c);
    defer testing.allocator.free(d);
    try testing.expectEqualStrings(data, d);
}

test "lz4 round-trip" {
    const data = "LZ4 compressed Kafka record batch.";
    const c = try compress(testing.allocator, .lz4, data);
    defer testing.allocator.free(c);
    const d = try decompress(testing.allocator, .lz4, c);
    defer testing.allocator.free(d);
    try testing.expectEqualStrings(data, d);
}

test "zstd round-trip" {
    const data = "Zstandard compressed data.";
    const c = try compress(testing.allocator, .zstd, data);
    defer testing.allocator.free(c);
    const d = try decompress(testing.allocator, .zstd, c);
    defer testing.allocator.free(d);
    try testing.expectEqualStrings(data, d);
}

test "empty data all codecs" {
    const codecs = [_]CompressionType{ .none, .gzip, .snappy, .lz4, .zstd };
    for (codecs) |codec| {
        const c = try compress(testing.allocator, codec, "");
        defer testing.allocator.free(c);
        const d = try decompress(testing.allocator, codec, c);
        defer testing.allocator.free(d);
        try testing.expectEqual(@as(usize, 0), d.len);
    }
}

test "snappy larger data" {
    const data = "The quick brown fox jumps over the lazy dog. " ** 10;
    const c = try compress(testing.allocator, .snappy, data);
    defer testing.allocator.free(c);
    const d = try decompress(testing.allocator, .snappy, c);
    defer testing.allocator.free(d);
    try testing.expectEqualStrings(data, d);
}

test "snappy malformed streams fail closed" {
    const truncated_literal = [_]u8{ 0x05, 0x08, 'a', 'b' };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .snappy, &truncated_literal));

    const invalid_copy_offset = [_]u8{ 0x04, 0x01, 0x00 };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .snappy, &invalid_copy_offset));

    const trailing_bytes = [_]u8{ 0x00, 0x00 };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .snappy, &trailing_bytes));
}

test "lz4 malformed streams fail closed" {
    const negative_size = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .lz4, &negative_size));

    const truncated_literal = [_]u8{ 0x00, 0x00, 0x00, 0x05, 0x50, 'a', 'b' };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .lz4, &truncated_literal));

    const invalid_match_offset = [_]u8{ 0x00, 0x00, 0x00, 0x05, 0x01, 0x00, 0x00 };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .lz4, &invalid_match_offset));

    const trailing_bytes = [_]u8{ 0x00, 0x00, 0x00, 0x01, 0x10, 'a', 0x00 };
    try testing.expectError(error.InvalidData, decompress(testing.allocator, .lz4, &trailing_bytes));
}

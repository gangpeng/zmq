const std = @import("std");
const mem = std.mem;
const math = std.math;
const testing = std.testing;

/// A byte buffer with read/write cursors supporting network byte order (big-endian).
///
/// This is the primary abstraction for serializing and deserializing Kafka wire
/// protocol messages. All multi-byte integers are stored in big-endian (network)
/// byte order, matching the Kafka protocol specification.
pub const ByteBuffer = struct {
    data: []u8,
    pos: usize,
    limit: usize,
    allocator: ?std.mem.Allocator,
    owned: bool,

    /// Wrap an existing byte slice (non-owning).
    pub fn wrap(data: []u8) ByteBuffer {
        return .{
            .data = data,
            .pos = 0,
            .limit = data.len,
            .allocator = null,
            .owned = false,
        };
    }

    /// Wrap a const byte slice for reading only.
    pub fn wrapConst(data: []const u8) ByteBuffer {
        return .{
            // Safe cast: we track ownership and won't write to non-owned buffers
            .data = @constCast(data),
            .pos = 0,
            .limit = data.len,
            .allocator = null,
            .owned = false,
        };
    }

    /// Allocate a new buffer of the given size.
    pub fn allocate(allocator: std.mem.Allocator, size: usize) !ByteBuffer {
        const data = try allocator.alloc(u8, size);
        return .{
            .data = data,
            .pos = 0,
            .limit = size,
            .allocator = allocator,
            .owned = true,
        };
    }

    /// Free the underlying memory if this buffer owns it.
    pub fn deinit(self: *ByteBuffer) void {
        if (self.owned) {
            if (self.allocator) |alloc| {
                alloc.free(self.data);
            }
        }
        self.* = undefined;
    }

    /// Returns the number of bytes remaining between pos and limit.
    pub fn remaining(self: *const ByteBuffer) usize {
        return self.limit - self.pos;
    }

    /// Returns a slice of the remaining readable bytes.
    pub fn remainingSlice(self: *const ByteBuffer) []const u8 {
        return self.data[self.pos..self.limit];
    }

    /// Reset position to 0 for re-reading.
    pub fn flip(self: *ByteBuffer) void {
        self.limit = self.pos;
        self.pos = 0;
    }

    /// Reset position and limit to full capacity.
    pub fn clear(self: *ByteBuffer) void {
        self.pos = 0;
        self.limit = self.data.len;
    }

    // ---------------------------------------------------------------
    // Big-endian readers (network byte order)
    // ---------------------------------------------------------------

    pub fn readByte(self: *ByteBuffer) !u8 {
        if (self.pos >= self.limit) return error.BufferUnderflow;
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    pub fn readI8(self: *ByteBuffer) !i8 {
        return @bitCast(try self.readByte());
    }

    pub fn readI16(self: *ByteBuffer) !i16 {
        if (self.pos + 2 > self.limit) return error.BufferUnderflow;
        const val = mem.readInt(i16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    pub fn readU16(self: *ByteBuffer) !u16 {
        if (self.pos + 2 > self.limit) return error.BufferUnderflow;
        const val = mem.readInt(u16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    pub fn readI32(self: *ByteBuffer) !i32 {
        if (self.pos + 4 > self.limit) return error.BufferUnderflow;
        const val = mem.readInt(i32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    pub fn readU32(self: *ByteBuffer) !u32 {
        if (self.pos + 4 > self.limit) return error.BufferUnderflow;
        const val = mem.readInt(u32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    pub fn readI64(self: *ByteBuffer) !i64 {
        if (self.pos + 8 > self.limit) return error.BufferUnderflow;
        const val = mem.readInt(i64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    pub fn readU64(self: *ByteBuffer) !u64 {
        if (self.pos + 8 > self.limit) return error.BufferUnderflow;
        const val = mem.readInt(u64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    pub fn readF64(self: *ByteBuffer) !f64 {
        const bits = try self.readU64();
        return @bitCast(bits);
    }

    /// Read `len` bytes as a slice (zero-copy view into the buffer).
    pub fn readBytes(self: *ByteBuffer, len: usize) ![]const u8 {
        if (self.pos + len > self.limit) return error.BufferUnderflow;
        const slice = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return slice;
    }

    /// Read a UUID (16 bytes).
    pub fn readUuid(self: *ByteBuffer) ![16]u8 {
        if (self.pos + 16 > self.limit) return error.BufferUnderflow;
        var result: [16]u8 = undefined;
        @memcpy(&result, self.data[self.pos..][0..16]);
        self.pos += 16;
        return result;
    }

    // ---------------------------------------------------------------
    // Big-endian writers (network byte order)
    // ---------------------------------------------------------------

    pub fn writeByte(self: *ByteBuffer, val: u8) !void {
        if (self.pos >= self.limit) return error.BufferOverflow;
        self.data[self.pos] = val;
        self.pos += 1;
    }

    pub fn writeI8(self: *ByteBuffer, val: i8) !void {
        try self.writeByte(@bitCast(val));
    }

    pub fn writeI16(self: *ByteBuffer, val: i16) !void {
        if (self.pos + 2 > self.limit) return error.BufferOverflow;
        mem.writeInt(i16, self.data[self.pos..][0..2], val, .big);
        self.pos += 2;
    }

    pub fn writeU16(self: *ByteBuffer, val: u16) !void {
        if (self.pos + 2 > self.limit) return error.BufferOverflow;
        mem.writeInt(u16, self.data[self.pos..][0..2], val, .big);
        self.pos += 2;
    }

    pub fn writeI32(self: *ByteBuffer, val: i32) !void {
        if (self.pos + 4 > self.limit) return error.BufferOverflow;
        mem.writeInt(i32, self.data[self.pos..][0..4], val, .big);
        self.pos += 4;
    }

    pub fn writeU32(self: *ByteBuffer, val: u32) !void {
        if (self.pos + 4 > self.limit) return error.BufferOverflow;
        mem.writeInt(u32, self.data[self.pos..][0..4], val, .big);
        self.pos += 4;
    }

    pub fn writeI64(self: *ByteBuffer, val: i64) !void {
        if (self.pos + 8 > self.limit) return error.BufferOverflow;
        mem.writeInt(i64, self.data[self.pos..][0..8], val, .big);
        self.pos += 8;
    }

    pub fn writeU64(self: *ByteBuffer, val: u64) !void {
        if (self.pos + 8 > self.limit) return error.BufferOverflow;
        mem.writeInt(u64, self.data[self.pos..][0..8], val, .big);
        self.pos += 8;
    }

    pub fn writeF64(self: *ByteBuffer, val: f64) !void {
        try self.writeU64(@bitCast(val));
    }

    /// Write a byte slice.
    pub fn writeSlice(self: *ByteBuffer, data: []const u8) !void {
        if (self.pos + data.len > self.limit) return error.BufferOverflow;
        @memcpy(self.data[self.pos .. self.pos + data.len], data);
        self.pos += data.len;
    }

    /// Write a UUID (16 bytes).
    pub fn writeUuid(self: *ByteBuffer, val: [16]u8) !void {
        try self.writeSlice(&val);
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "ByteBuffer round-trip integers" {
    var buf_data: [64]u8 = undefined;
    var buf = ByteBuffer.wrap(&buf_data);

    // Write various integers
    try buf.writeByte(0xAB);
    try buf.writeI16(-1234);
    try buf.writeI32(0x12345678);
    try buf.writeI64(0x0102030405060708);

    // Flip to read mode
    buf.flip();

    // Read back and verify
    try testing.expectEqual(@as(u8, 0xAB), try buf.readByte());
    try testing.expectEqual(@as(i16, -1234), try buf.readI16());
    try testing.expectEqual(@as(i32, 0x12345678), try buf.readI32());
    try testing.expectEqual(@as(i64, 0x0102030405060708), try buf.readI64());

    // Should have no remaining bytes
    try testing.expectEqual(@as(usize, 0), buf.remaining());
}

test "ByteBuffer underflow" {
    var buf_data: [2]u8 = undefined;
    var buf = ByteBuffer.wrap(&buf_data);
    buf.limit = 0; // empty

    try testing.expectError(error.BufferUnderflow, buf.readByte());
    try testing.expectError(error.BufferUnderflow, buf.readI32());
}

test "ByteBuffer overflow" {
    var buf_data: [2]u8 = undefined;
    var buf = ByteBuffer.wrap(&buf_data);

    try buf.writeByte(1);
    try buf.writeByte(2);
    try testing.expectError(error.BufferOverflow, buf.writeByte(3));
}

test "ByteBuffer allocate and deinit" {
    var buf = try ByteBuffer.allocate(testing.allocator, 128);
    defer buf.deinit();

    try buf.writeI32(42);
    buf.flip();
    try testing.expectEqual(@as(i32, 42), try buf.readI32());
}

test "ByteBuffer readBytes zero-copy" {
    const data = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05 };
    var buf = ByteBuffer.wrapConst(&data);

    const slice = try buf.readBytes(3);
    try testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x02, 0x03 }, slice);
    try testing.expectEqual(@as(usize, 2), buf.remaining());
}

test "ByteBuffer UUID round-trip" {
    var buf_data: [32]u8 = undefined;
    var buf = ByteBuffer.wrap(&buf_data);

    const uuid_val = [16]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10 };
    try buf.writeUuid(uuid_val);
    buf.flip();

    const read_uuid = try buf.readUuid();
    try testing.expectEqualSlices(u8, &uuid_val, &read_uuid);
}

test "ByteBuffer f64 round-trip" {
    var buf_data: [16]u8 = undefined;
    var buf = ByteBuffer.wrap(&buf_data);

    try buf.writeF64(3.14159265358979);
    buf.flip();

    const val = try buf.readF64();
    try testing.expectApproxEqAbs(@as(f64, 3.14159265358979), val, 1e-15);
}

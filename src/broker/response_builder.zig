const std = @import("std");
const Allocator = std.mem.Allocator;
const protocol = @import("protocol");
const ser = protocol.serialization;
const header_mod = protocol.header;
const ResponseHeader = header_mod.ResponseHeader;

/// Response builder utility for constructing Kafka response frames.
///
/// Provides a fluent API for building response buffers with proper
/// frame sizing, header serialization, and tagged fields.
///
/// Usage:
///   var builder = ResponseBuilder.init(alloc, correlation_id, header_version);
///   builder.writeI16(0); // error_code
///   builder.writeString("topic-name");
///   return builder.finish();
pub const ResponseBuilder = struct {
    buf: []u8,
    pos: usize,
    allocator: Allocator,
    resp_header_version: i16,

    pub fn init(alloc: Allocator, correlation_id: i32, resp_header_version: i16) ?ResponseBuilder {
        const buf = alloc.alloc(u8, 8192) catch return null;
        var pos: usize = 0;

        // Write response header
        const rh = ResponseHeader{ .correlation_id = correlation_id };
        rh.serialize(buf, &pos, resp_header_version);

        return .{
            .buf = buf,
            .pos = pos,
            .allocator = alloc,
            .resp_header_version = resp_header_version,
        };
    }

    pub fn writeI8(self: *ResponseBuilder, val: i8) void {
        ser.writeI8(self.buf, &self.pos, val);
    }

    pub fn writeI16(self: *ResponseBuilder, val: i16) void {
        ser.writeI16(self.buf, &self.pos, val);
    }

    pub fn writeI32(self: *ResponseBuilder, val: i32) void {
        ser.writeI32(self.buf, &self.pos, val);
    }

    pub fn writeI64(self: *ResponseBuilder, val: i64) void {
        ser.writeI64(self.buf, &self.pos, val);
    }

    pub fn writeBool(self: *ResponseBuilder, val: bool) void {
        ser.writeBool(self.buf, &self.pos, val);
    }

    pub fn writeString(self: *ResponseBuilder, val: ?[]const u8) void {
        ser.writeString(self.buf, &self.pos, val);
    }

    pub fn writeCompactString(self: *ResponseBuilder, val: ?[]const u8) void {
        ser.writeCompactString(self.buf, &self.pos, val);
    }

    pub fn writeBytes(self: *ResponseBuilder, val: ?[]const u8) void {
        ser.writeBytesBuf(self.buf, &self.pos, val);
    }

    pub fn writeArrayLen(self: *ResponseBuilder, len: usize) void {
        ser.writeArrayLen(self.buf, &self.pos, len);
    }

    pub fn writeCompactArrayLen(self: *ResponseBuilder, len: usize) void {
        ser.writeCompactArrayLen(self.buf, &self.pos, len);
    }

    pub fn writeEmptyTaggedFields(self: *ResponseBuilder) void {
        ser.writeEmptyTaggedFields(self.buf, &self.pos);
    }

    /// Write a generated struct's serialized form.
    pub fn writeStruct(self: *ResponseBuilder, comptime T: type, val: *const T, version: i16) void {
        val.serialize(self.buf, &self.pos, version);
    }

    /// Finalize the response buffer, trimming to actual size.
    pub fn finish(self: *ResponseBuilder) ?[]u8 {
        if (self.resp_header_version >= 1) {
            ser.writeEmptyTaggedFields(self.buf, &self.pos);
        }
        return (self.allocator.realloc(self.buf, self.pos) catch self.buf)[0..self.pos];
    }

    /// Finalize without adding tagged fields at the end.
    pub fn finishRaw(self: *ResponseBuilder) ?[]u8 {
        return (self.allocator.realloc(self.buf, self.pos) catch self.buf)[0..self.pos];
    }

    pub fn deinit(self: *ResponseBuilder) void {
        self.allocator.free(self.buf);
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "ResponseBuilder basic usage" {
    var builder = ResponseBuilder.init(testing.allocator, 42, 0) orelse return;
    builder.writeI16(0); // error_code
    builder.writeI32(100); // throttle_time_ms
    const result = builder.finishRaw();
    try testing.expect(result != null);
    testing.allocator.free(result.?);
}

test "ResponseBuilder with tagged fields" {
    var builder = ResponseBuilder.init(testing.allocator, 1, 1) orelse return;
    builder.writeI16(0);
    const result = builder.finish();
    try testing.expect(result != null);
    testing.allocator.free(result.?);
}

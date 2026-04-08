const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// In-memory S3 mock for testing.
///
/// Implements PutObject, GetObject, DeleteObject, and range reads.
/// Stores objects in a HashMap keyed by object path.
pub const MockS3 = struct {
    objects: std.StringHashMap(Object),
    allocator: Allocator,
    put_count: u64 = 0,
    get_count: u64 = 0,

    const Object = struct {
        key: []u8,
        data: []u8,
        size: u64,
    };

    pub fn init(alloc: Allocator) MockS3 {
        return .{
            .objects = std.StringHashMap(Object).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *MockS3) void {
        var it = self.objects.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.key);
            self.allocator.free(entry.value_ptr.data);
        }
        self.objects.deinit();
    }

    pub fn putObject(self: *MockS3, key: []const u8, data: []const u8) !void {
        // Remove existing if present
        if (self.objects.fetchRemove(key)) |existing| {
            self.allocator.free(existing.value.key);
            self.allocator.free(existing.value.data);
        }

        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);
        const data_copy = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(data_copy);

        try self.objects.put(key_copy, .{
            .key = key_copy,
            .data = data_copy,
            .size = data.len,
        });
        self.put_count += 1;
    }

    pub fn getObject(self: *MockS3, key: []const u8) ?[]const u8 {
        self.get_count += 1;
        if (self.objects.get(key)) |obj| {
            return obj.data;
        }
        return null;
    }

    /// Range read: get bytes [start, end) from an object.
    pub fn getObjectRange(self: *MockS3, key: []const u8, start: usize, end: usize) ?[]const u8 {
        self.get_count += 1;
        if (self.objects.get(key)) |obj| {
            if (start >= obj.data.len) return null;
            const actual_end = @min(end, obj.data.len);
            return obj.data[start..actual_end];
        }
        return null;
    }

    pub fn deleteObject(self: *MockS3, key: []const u8) bool {
        if (self.objects.fetchRemove(key)) |existing| {
            self.allocator.free(existing.value.key);
            self.allocator.free(existing.value.data);
            return true;
        }
        return false;
    }

    pub fn objectCount(self: *const MockS3) usize {
        return self.objects.count();
    }

    pub fn objectSize(self: *const MockS3, key: []const u8) ?u64 {
        if (self.objects.get(key)) |obj| return obj.size;
        return null;
    }
};

// ---------------------------------------------------------------
// S3 Object Format
// ---------------------------------------------------------------

/// ZMQ S3 object format writer.
///
/// Format:
///   [DataBlock 0] [DataBlock 1] ... [DataBlock N]
///   [IndexBlock: DataBlockIndex entries (36 bytes each)]
///   [Footer: index_position(8) + index_size(4) + magic(4)]
///
/// DataBlockIndex entry (36 bytes):
///   stream_id: u64
///   start_offset: u64
///   end_offset_delta: u32
///   record_count: u32
///   block_position: u64
///   block_size: u32
pub const ObjectWriter = struct {
    const FOOTER_MAGIC: u32 = 0x4155544F; // "AUTO"
    const FOOTER_SIZE: usize = 16; // index_position(8) + index_size(4) + magic(4)
    const INDEX_ENTRY_SIZE: usize = 36;

    data: std.ArrayList(u8),
    index_entries: std.ArrayList(DataBlockIndex),
    allocator: Allocator,

    pub const DataBlockIndex = struct {
        stream_id: u64,
        start_offset: u64,
        end_offset_delta: u32,
        record_count: u32,
        block_position: u64,
        block_size: u32,
    };

    pub fn init(alloc: Allocator) ObjectWriter {
        return .{
            .data = std.ArrayList(u8).init(alloc),
            .index_entries = std.ArrayList(DataBlockIndex).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ObjectWriter) void {
        self.data.deinit();
        self.index_entries.deinit();
    }

    /// Add a data block for a stream.
    pub fn addDataBlock(self: *ObjectWriter, stream_id: u64, start_offset: u64, end_offset_delta: u32, record_count: u32, block_data: []const u8) !void {
        const position = self.data.items.len;

        try self.data.appendSlice(block_data);

        try self.index_entries.append(.{
            .stream_id = stream_id,
            .start_offset = start_offset,
            .end_offset_delta = end_offset_delta,
            .record_count = record_count,
            .block_position = @intCast(position),
            .block_size = @intCast(block_data.len),
        });
    }

    /// Finalize and return the complete S3 object bytes.
    pub fn build(self: *ObjectWriter) ![]u8 {
        const index_position = self.data.items.len;

        // Write index entries
        for (self.index_entries.items) |entry| {
            var idx_buf: [INDEX_ENTRY_SIZE]u8 = undefined;
            std.mem.writeInt(u64, idx_buf[0..8], entry.stream_id, .big);
            std.mem.writeInt(u64, idx_buf[8..16], entry.start_offset, .big);
            std.mem.writeInt(u32, idx_buf[16..20], entry.end_offset_delta, .big);
            std.mem.writeInt(u32, idx_buf[20..24], entry.record_count, .big);
            std.mem.writeInt(u64, idx_buf[24..32], entry.block_position, .big);
            std.mem.writeInt(u32, idx_buf[32..36], entry.block_size, .big);
            try self.data.appendSlice(&idx_buf);
        }

        const index_size: u32 = @intCast(self.index_entries.items.len * INDEX_ENTRY_SIZE);

        // Write footer
        var footer: [FOOTER_SIZE]u8 = undefined;
        std.mem.writeInt(u64, footer[0..8], @intCast(index_position), .big);
        std.mem.writeInt(u32, footer[8..12], index_size, .big);
        std.mem.writeInt(u32, footer[12..16], FOOTER_MAGIC, .big);
        try self.data.appendSlice(&footer);

        return try self.data.toOwnedSlice();
    }
};

/// S3 Object reader — parse the ZMQ/AutoMQ object format.
pub const ObjectReader = struct {
    const FOOTER_MAGIC: u32 = 0x4155544F;
    const FOOTER_SIZE: usize = 16;
    const INDEX_ENTRY_SIZE: usize = 36;

    data: []const u8,
    index_position: u64,
    index_size: u32,
    index_entries: []ObjectWriter.DataBlockIndex,
    allocator: Allocator,

    pub fn parse(alloc: Allocator, data: []const u8) !ObjectReader {
        if (data.len < FOOTER_SIZE) return error.ObjectTooSmall;

        // Read footer
        const footer_start = data.len - FOOTER_SIZE;
        const magic = std.mem.readInt(u32, data[footer_start + 12 ..][0..4], .big);
        if (magic != FOOTER_MAGIC) return error.InvalidMagic;

        const index_position = std.mem.readInt(u64, data[footer_start ..][0..8], .big);
        const index_size = std.mem.readInt(u32, data[footer_start + 8 ..][0..4], .big);

        // Parse index entries
        const entry_count = index_size / INDEX_ENTRY_SIZE;
        var entries = try alloc.alloc(ObjectWriter.DataBlockIndex, entry_count);

        var pos = @as(usize, @intCast(index_position));
        for (0..entry_count) |i| {
            entries[i] = .{
                .stream_id = std.mem.readInt(u64, data[pos..][0..8], .big),
                .start_offset = std.mem.readInt(u64, data[pos + 8 ..][0..16][0..8], .big),
                .end_offset_delta = std.mem.readInt(u32, data[pos + 16 ..][0..4], .big),
                .record_count = std.mem.readInt(u32, data[pos + 20 ..][0..4], .big),
                .block_position = std.mem.readInt(u64, data[pos + 24 ..][0..8], .big),
                .block_size = std.mem.readInt(u32, data[pos + 32 ..][0..4], .big),
            };
            pos += INDEX_ENTRY_SIZE;
        }

        return .{
            .data = data,
            .index_position = index_position,
            .index_size = index_size,
            .index_entries = entries,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ObjectReader) void {
        self.allocator.free(self.index_entries);
    }

    /// Get the data block for a given index entry.
    pub fn readBlock(self: *const ObjectReader, entry_index: usize) ?[]const u8 {
        if (entry_index >= self.index_entries.len) return null;
        const entry = self.index_entries[entry_index];
        const start = @as(usize, @intCast(entry.block_position));
        const end = start + entry.block_size;
        if (end > self.data.len) return null;
        return self.data[start..end];
    }

    /// Find index entries for a given stream and offset range.
    pub fn findEntries(self: *const ObjectReader, stream_id: u64, start_offset: u64, end_offset: u64) []const ObjectWriter.DataBlockIndex {
        // Simple linear scan — can be optimized with binary search
        var start_idx: usize = 0;
        var end_idx: usize = 0;
        var found_start = false;

        for (self.index_entries, 0..) |entry, i| {
            if (entry.stream_id != stream_id) continue;
            const entry_end = entry.start_offset + entry.end_offset_delta;
            if (!found_start and entry_end > start_offset) {
                start_idx = i;
                found_start = true;
            }
            if (found_start) {
                end_idx = i + 1;
                if (entry.start_offset >= end_offset) break;
            }
        }

        if (!found_start) return &.{};
        return self.index_entries[start_idx..end_idx];
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "MockS3 put and get" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("bucket/key1", "hello world");
    try testing.expectEqualStrings("hello world", s3.getObject("bucket/key1").?);
    try testing.expect(s3.getObject("nonexistent") == null);
    try testing.expectEqual(@as(usize, 1), s3.objectCount());
}

test "MockS3 range read" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("key", "0123456789");
    try testing.expectEqualStrings("345", s3.getObjectRange("key", 3, 6).?);
}

test "MockS3 delete" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("key", "data");
    try testing.expect(s3.deleteObject("key"));
    try testing.expect(s3.getObject("key") == null);
    try testing.expectEqual(@as(usize, 0), s3.objectCount());
}

test "ObjectWriter and ObjectReader round-trip" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "block1-data-stream1");
    try writer.addDataBlock(1, 10, 5, 5, "block2-data");
    try writer.addDataBlock(2, 0, 3, 3, "block3-stream2");

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    try testing.expectEqual(@as(usize, 3), reader.index_entries.len);

    // Read block 0
    const block0 = reader.readBlock(0).?;
    try testing.expectEqualStrings("block1-data-stream1", block0);

    // Read block 2
    const block2 = reader.readBlock(2).?;
    try testing.expectEqualStrings("block3-stream2", block2);

    // Find entries for stream 1 in offset range [0, 20)
    const entries = reader.findEntries(1, 0, 20);
    try testing.expectEqual(@as(usize, 2), entries.len);
    try testing.expectEqual(@as(u64, 0), entries[0].start_offset);
    try testing.expectEqual(@as(u64, 10), entries[1].start_offset);

    // Find entries for stream 2
    const s2_entries = reader.findEntries(2, 0, 10);
    try testing.expectEqual(@as(usize, 1), s2_entries.len);
}

test "ObjectReader.parse too-small buffer" {
    var short_buf: [8]u8 = undefined;
    const result = ObjectReader.parse(testing.allocator, &short_buf);
    try testing.expectError(error.ObjectTooSmall, result);
}

test "ObjectReader.parse invalid magic" {
    // Create a buffer with valid footer size but wrong magic
    var buf: [ObjectWriter.FOOTER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], 0, .big); // index_position
    std.mem.writeInt(u32, buf[8..12], 0, .big); // index_size
    std.mem.writeInt(u32, buf[12..16], 0xDEADBEEF, .big); // wrong magic

    const result = ObjectReader.parse(testing.allocator, &buf);
    try testing.expectError(error.InvalidMagic, result);
}

test "ObjectReader.readBlock out of bounds" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 1, 1, "data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    // Valid index
    try testing.expect(reader.readBlock(0) != null);
    // Invalid index
    try testing.expect(reader.readBlock(99) == null);
}

test "ObjectReader.findEntries no match" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 1, 1, "data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    // Search for non-existent stream
    const entries = reader.findEntries(999, 0, 100);
    try testing.expectEqual(@as(usize, 0), entries.len);
}

test "ObjectWriter empty build" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    // Build with no data blocks — just footer
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    try testing.expectEqual(@as(usize, 0), reader.index_entries.len);
}

test "MockS3 overwrite" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("key", "value1");
    try s3.putObject("key", "value2"); // overwrite

    try testing.expectEqualStrings("value2", s3.getObject("key").?);
    try testing.expectEqual(@as(usize, 1), s3.objectCount());
}

test "MockS3 range read edge cases" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("key", "0123456789");

    // Start beyond end
    try testing.expect(s3.getObjectRange("key", 100, 200) == null);
    // End beyond data length — should clamp
    const data = s3.getObjectRange("key", 5, 100);
    try testing.expect(data != null);
    try testing.expectEqualStrings("56789", data.?);
    // Full range
    try testing.expectEqualStrings("0123456789", s3.getObjectRange("key", 0, 10).?);
}

test "MockS3 delete nonexistent" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try testing.expect(!s3.deleteObject("nonexistent"));
}

test "MockS3 objectSize" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("key", "12345");
    try testing.expectEqual(@as(u64, 5), s3.objectSize("key").?);
    try testing.expect(s3.objectSize("missing") == null);
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const crc32c = @import("core").crc32c;

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

    /// Return owned object keys matching prefix, sorted for deterministic recovery.
    pub fn listObjectKeys(self: *const MockS3, alloc: Allocator, prefix: []const u8) ![][]u8 {
        var keys = std.array_list.Managed([]u8).init(alloc);
        errdefer {
            for (keys.items) |key| alloc.free(key);
            keys.deinit();
        }

        var it = self.objects.keyIterator();
        while (it.next()) |key_ptr| {
            if (std.mem.startsWith(u8, key_ptr.*, prefix)) {
                const key_copy = try alloc.dupe(u8, key_ptr.*);
                errdefer alloc.free(key_copy);
                try keys.append(key_copy);
            }
        }

        std.mem.sort([]u8, keys.items, {}, struct {
            fn lessThan(_: void, a: []u8, b: []u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lessThan);

        return try keys.toOwnedSlice();
    }
};

// ---------------------------------------------------------------
// S3 Object Format
// ---------------------------------------------------------------

/// ZMQ S3 object format writer.
///
/// Format v2 (current):
///   [DataBlock 0] [DataBlock 1] ... [DataBlock N]
///   [IndexBlock: DataBlockIndex entries (36 bytes each)]
///   [Footer: index_position(8) + index_size(4) + magic_v2(4) + crc32c(4)]
///
/// The CRC32C covers all bytes before it (data blocks + index + footer fields + magic).
/// This detects silent bit-rot corruption that would otherwise go unnoticed.
///
/// Format v1 (legacy, read-only):
///   [DataBlock 0] [DataBlock 1] ... [DataBlock N]
///   [IndexBlock: DataBlockIndex entries (36 bytes each)]
///   [Footer: index_position(8) + index_size(4) + magic_v1(4)]
///
/// DataBlockIndex entry (36 bytes):
///   stream_id: u64
///   start_offset: u64
///   end_offset_delta: u32
///   record_count: u32
///   block_position: u64
///   block_size: u32
pub const ObjectWriter = struct {
    const FOOTER_MAGIC_V1: u32 = 0x4155544F; // "AUTO" — legacy format without CRC
    const FOOTER_MAGIC_V2: u32 = 0x41555432; // "AUT2" — current format with CRC32C
    const FOOTER_SIZE_V1: usize = 16; // index_position(8) + index_size(4) + magic(4)
    const FOOTER_SIZE_V2: usize = 20; // index_position(8) + index_size(4) + magic(4) + crc32c(4)
    const INDEX_ENTRY_SIZE: usize = 36;

    data: std.array_list.Managed(u8),
    index_entries: std.array_list.Managed(DataBlockIndex),
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
            .data = std.array_list.Managed(u8).init(alloc),
            .index_entries = std.array_list.Managed(DataBlockIndex).init(alloc),
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
    /// Writes v2 format: data blocks + index + footer (with magic_v2) + CRC32C.
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

        // Write footer (first 12 bytes: index_position + index_size)
        var footer_prefix: [12]u8 = undefined;
        std.mem.writeInt(u64, footer_prefix[0..8], @intCast(index_position), .big);
        std.mem.writeInt(u32, footer_prefix[8..12], index_size, .big);
        try self.data.appendSlice(&footer_prefix);

        // Write v2 magic
        var magic_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &magic_buf, FOOTER_MAGIC_V2, .big);
        try self.data.appendSlice(&magic_buf);

        // Compute CRC32C over all bytes written so far (data + index + footer prefix + magic)
        const checksum = crc32c.compute(self.data.items);

        // Append CRC32C as final 4 bytes
        var crc_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &crc_buf, checksum, .big);
        try self.data.appendSlice(&crc_buf);

        return try self.data.toOwnedSlice();
    }
};

/// S3 Object reader — parse the ZMQ/AutoMQ object format.
///
/// Supports both v1 (legacy, no CRC) and v2 (with CRC32C checksum).
/// Detection: tries v2 first (magic_v2 at len-8), falls back to v1 (magic_v1 at len-4).
/// For v2 objects, verifies the CRC32C checksum and returns error.ChecksumMismatch
/// if the data has been corrupted (bit-rot, truncation, etc.).
pub const ObjectReader = struct {
    const FOOTER_MAGIC_V1: u32 = 0x4155544F; // "AUTO"
    const FOOTER_MAGIC_V2: u32 = 0x41555432; // "AUT2"
    const FOOTER_SIZE_V1: usize = 16;
    const FOOTER_SIZE_V2: usize = 20;
    const INDEX_ENTRY_SIZE: usize = 36;

    data: []const u8,
    index_position: u64,
    index_size: u32,
    index_entries: []ObjectWriter.DataBlockIndex,
    allocator: Allocator,
    has_checksum: bool,

    pub fn parse(alloc: Allocator, data: []const u8) !ObjectReader {
        // Try v2 format first (20-byte footer: index_pos(8) + index_size(4) + magic_v2(4) + crc(4))
        if (data.len >= FOOTER_SIZE_V2) {
            const magic_offset = data.len - 8; // magic is 8 bytes from end (before 4-byte CRC)
            const magic = std.mem.readInt(u32, data[magic_offset..][0..4], .big);
            if (magic == FOOTER_MAGIC_V2) {
                return parseV2(alloc, data);
            }
        }

        // Fall back to v1 format (16-byte footer: index_pos(8) + index_size(4) + magic_v1(4))
        if (data.len >= FOOTER_SIZE_V1) {
            const magic_offset = data.len - 4; // magic is last 4 bytes
            const magic = std.mem.readInt(u32, data[magic_offset..][0..4], .big);
            if (magic == FOOTER_MAGIC_V1) {
                return parseV1(alloc, data);
            }
        }

        if (data.len < FOOTER_SIZE_V1) return error.ObjectTooSmall;
        return error.InvalidMagic;
    }

    /// Parse v2 format: validates CRC32C checksum before returning.
    fn parseV2(alloc: Allocator, data: []const u8) !ObjectReader {
        // CRC is the last 4 bytes, computed over everything before it
        const crc_offset = data.len - 4;
        const stored_crc = std.mem.readInt(u32, data[crc_offset..][0..4], .big);
        const computed_crc = crc32c.compute(data[0..crc_offset]);

        if (stored_crc != computed_crc) return error.ChecksumMismatch;

        // Footer fields are at: [len-20..len-12] = index_pos, [len-12..len-8] = index_size
        const footer_start = data.len - FOOTER_SIZE_V2;
        const index_position = std.mem.readInt(u64, data[footer_start..][0..8], .big);
        const index_size = std.mem.readInt(u32, data[footer_start + 8 ..][0..4], .big);

        const entries = try parseIndexEntries(alloc, data, index_position, index_size, footer_start);

        return .{
            .data = data,
            .index_position = index_position,
            .index_size = index_size,
            .index_entries = entries,
            .allocator = alloc,
            .has_checksum = true,
        };
    }

    /// Parse v1 format (legacy): no checksum verification.
    fn parseV1(alloc: Allocator, data: []const u8) !ObjectReader {
        const footer_start = data.len - FOOTER_SIZE_V1;
        const index_position = std.mem.readInt(u64, data[footer_start..][0..8], .big);
        const index_size = std.mem.readInt(u32, data[footer_start + 8 ..][0..4], .big);

        const entries = try parseIndexEntries(alloc, data, index_position, index_size, footer_start);

        return .{
            .data = data,
            .index_position = index_position,
            .index_size = index_size,
            .index_entries = entries,
            .allocator = alloc,
            .has_checksum = false,
        };
    }

    /// Shared index parsing logic for both v1 and v2.
    fn parseIndexEntries(alloc: Allocator, data: []const u8, index_position: u64, index_size: u32, index_limit: usize) ![]ObjectWriter.DataBlockIndex {
        if (index_size % INDEX_ENTRY_SIZE != 0) return error.InvalidIndexSize;
        const index_start = std.math.cast(usize, index_position) orelse return error.InvalidIndexPosition;
        if (index_start > index_limit) return error.InvalidIndexPosition;
        const index_end = std.math.add(usize, index_start, index_size) catch return error.InvalidIndexBounds;
        if (index_end > index_limit) return error.InvalidIndexBounds;

        const entry_count = index_size / INDEX_ENTRY_SIZE;
        var entries = try alloc.alloc(ObjectWriter.DataBlockIndex, entry_count);
        errdefer alloc.free(entries);

        var pos = index_start;
        for (0..entry_count) |i| {
            entries[i] = .{
                .stream_id = std.mem.readInt(u64, data[pos..][0..8], .big),
                .start_offset = std.mem.readInt(u64, data[pos + 8 ..][0..16][0..8], .big),
                .end_offset_delta = std.mem.readInt(u32, data[pos + 16 ..][0..4], .big),
                .record_count = std.mem.readInt(u32, data[pos + 20 ..][0..4], .big),
                .block_position = std.mem.readInt(u64, data[pos + 24 ..][0..8], .big),
                .block_size = std.mem.readInt(u32, data[pos + 32 ..][0..4], .big),
            };
            const block_start = std.math.cast(usize, entries[i].block_position) orelse return error.InvalidBlockPosition;
            const block_end = std.math.add(usize, block_start, entries[i].block_size) catch return error.InvalidBlockBounds;
            if (block_end > index_start) return error.InvalidBlockBounds;
            pos += INDEX_ENTRY_SIZE;
        }

        return entries;
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

    /// Find global index positions for blocks from one stream that overlap the
    /// requested offset range. Returning positions instead of a slice is
    /// required because StreamSetObjects can interleave entries from many
    /// streams in a single object.
    pub fn findEntryIndexes(self: *const ObjectReader, alloc: Allocator, stream_id: u64, start_offset: u64, end_offset: u64) ![]usize {
        var indexes = std.array_list.Managed(usize).init(alloc);
        errdefer indexes.deinit();

        for (self.index_entries, 0..) |entry, i| {
            if (entry.stream_id != stream_id) continue;
            const entry_end = std.math.add(u64, entry.start_offset, entry.end_offset_delta) catch std.math.maxInt(u64);
            if (entry_end <= start_offset) continue;
            if (entry.start_offset >= end_offset) continue;
            try indexes.append(i);
        }

        return try indexes.toOwnedSlice();
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
    const entry_indexes = try reader.findEntryIndexes(testing.allocator, 1, 0, 20);
    defer testing.allocator.free(entry_indexes);
    try testing.expectEqual(@as(usize, 2), entry_indexes.len);
    try testing.expectEqual(@as(u64, 0), reader.index_entries[entry_indexes[0]].start_offset);
    try testing.expectEqual(@as(u64, 10), reader.index_entries[entry_indexes[1]].start_offset);

    // Find entries for stream 2
    const s2_entry_indexes = try reader.findEntryIndexes(testing.allocator, 2, 0, 10);
    defer testing.allocator.free(s2_entry_indexes);
    try testing.expectEqual(@as(usize, 1), s2_entry_indexes.len);
}

test "ObjectReader.findEntryIndexes filters interleaved streams" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 1, 1, "s1-0");
    try writer.addDataBlock(2, 0, 1, 1, "s2-0");
    try writer.addDataBlock(1, 1, 1, 1, "s1-1");

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    const s1_indexes = try reader.findEntryIndexes(testing.allocator, 1, 0, 2);
    defer testing.allocator.free(s1_indexes);
    try testing.expectEqual(@as(usize, 2), s1_indexes.len);
    try testing.expectEqual(@as(usize, 0), s1_indexes[0]);
    try testing.expectEqual(@as(usize, 2), s1_indexes[1]);
    try testing.expectEqualStrings("s1-0", reader.readBlock(s1_indexes[0]).?);
    try testing.expectEqualStrings("s1-1", reader.readBlock(s1_indexes[1]).?);

    const s2_indexes = try reader.findEntryIndexes(testing.allocator, 2, 0, 1);
    defer testing.allocator.free(s2_indexes);
    try testing.expectEqual(@as(usize, 1), s2_indexes.len);
    try testing.expectEqual(@as(usize, 1), s2_indexes[0]);
    try testing.expectEqualStrings("s2-0", reader.readBlock(s2_indexes[0]).?);
}

test "ObjectReader.parse too-small buffer" {
    var short_buf: [8]u8 = undefined;
    const result = ObjectReader.parse(testing.allocator, &short_buf);
    try testing.expectError(error.ObjectTooSmall, result);
}

test "ObjectReader.parse invalid magic" {
    // Create a buffer with valid v1 footer size but wrong magic — neither v1 nor v2
    var buf: [ObjectWriter.FOOTER_SIZE_V1]u8 = undefined;
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

test "ObjectReader.findEntryIndexes no match" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 1, 1, "data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    // Search for non-existent stream
    const entry_indexes = try reader.findEntryIndexes(testing.allocator, 999, 0, 100);
    defer testing.allocator.free(entry_indexes);
    try testing.expectEqual(@as(usize, 0), entry_indexes.len);
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

test "MockS3 listObjectKeys filters and sorts" {
    var s3 = MockS3.init(testing.allocator);
    defer s3.deinit();

    try s3.putObject("wal/epoch-0/bulk/0000000002", "b");
    try s3.putObject("data/topic/0/obj", "ignored");
    try s3.putObject("wal/epoch-0/bulk/0000000001", "a");

    const keys = try s3.listObjectKeys(testing.allocator, "wal/");
    defer {
        for (keys) |key| testing.allocator.free(key);
        testing.allocator.free(keys);
    }

    try testing.expectEqual(@as(usize, 2), keys.len);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000001", keys[0]);
    try testing.expectEqualStrings("wal/epoch-0/bulk/0000000002", keys[1]);
}

test "ObjectWriter v2 writes CRC32C checksum" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 5, 5, "hello");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Verify v2 magic is at offset len-8
    const magic = std.mem.readInt(u32, obj_data[obj_data.len - 8 ..][0..4], .big);
    try testing.expectEqual(ObjectWriter.FOOTER_MAGIC_V2, magic);

    // Verify CRC32C is at last 4 bytes and matches
    const stored_crc = std.mem.readInt(u32, obj_data[obj_data.len - 4 ..][0..4], .big);
    const computed_crc = crc32c.compute(obj_data[0 .. obj_data.len - 4]);
    try testing.expectEqual(computed_crc, stored_crc);
}

test "ObjectReader v2 round-trip with checksum verification" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "block1-data-stream1");
    try writer.addDataBlock(2, 0, 3, 3, "block3-stream2");

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    try testing.expect(reader.has_checksum);
    try testing.expectEqual(@as(usize, 2), reader.index_entries.len);
    try testing.expectEqualStrings("block1-data-stream1", reader.readBlock(0).?);
    try testing.expectEqualStrings("block3-stream2", reader.readBlock(1).?);
}

test "ObjectReader v2 detects corrupted data via CRC mismatch" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 5, 5, "original-data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Corrupt a byte in the data block region (simulate bit-rot)
    obj_data[0] ^= 0xFF;

    const result = ObjectReader.parse(testing.allocator, obj_data);
    try testing.expectError(error.ChecksumMismatch, result);
}

test "ObjectReader v2 detects corrupted index via CRC mismatch" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 5, 5, "some-data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Corrupt a byte in the index region (after data, before footer)
    // Index starts after "some-data" (9 bytes), so byte 10 is in the index
    const data_len = "some-data".len;
    if (obj_data.len > data_len + 5) {
        obj_data[data_len + 2] ^= 0xFF;
    }

    const result = ObjectReader.parse(testing.allocator, obj_data);
    try testing.expectError(error.ChecksumMismatch, result);
}

test "ObjectReader v2 detects corrupted footer via CRC mismatch" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 5, 5, "footer-test");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Corrupt the index_position field in the footer (byte at len-20)
    const footer_pos = obj_data.len - ObjectWriter.FOOTER_SIZE_V2;
    obj_data[footer_pos] ^= 0xFF;

    const result = ObjectReader.parse(testing.allocator, obj_data);
    try testing.expectError(error.ChecksumMismatch, result);
}

test "ObjectReader backward compatibility with v1 format" {
    // Manually construct a v1-format object (no CRC, old magic)
    const block_data = "legacy-block-data";
    var data_buf = std.array_list.Managed(u8).init(testing.allocator);
    defer data_buf.deinit();

    // Data block
    try data_buf.appendSlice(block_data);
    const index_position = data_buf.items.len;

    // Index entry (36 bytes) for stream_id=1, start_offset=0, end_offset_delta=5, record_count=5
    var idx_buf: [36]u8 = undefined;
    std.mem.writeInt(u64, idx_buf[0..8], 1, .big); // stream_id
    std.mem.writeInt(u64, idx_buf[8..16], 0, .big); // start_offset
    std.mem.writeInt(u32, idx_buf[16..20], 5, .big); // end_offset_delta
    std.mem.writeInt(u32, idx_buf[20..24], 5, .big); // record_count
    std.mem.writeInt(u64, idx_buf[24..32], 0, .big); // block_position
    std.mem.writeInt(u32, idx_buf[32..36], @intCast(block_data.len), .big); // block_size
    try data_buf.appendSlice(&idx_buf);

    // v1 footer: index_position(8) + index_size(4) + magic_v1(4) = 16 bytes
    var footer: [16]u8 = undefined;
    std.mem.writeInt(u64, footer[0..8], @intCast(index_position), .big);
    std.mem.writeInt(u32, footer[8..12], 36, .big); // one entry = 36 bytes
    std.mem.writeInt(u32, footer[12..16], 0x4155544F, .big); // "AUTO" v1 magic
    try data_buf.appendSlice(&footer);

    const obj_data = try data_buf.toOwnedSlice();
    defer testing.allocator.free(obj_data);

    // Parse should succeed as v1 (no checksum)
    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    try testing.expect(!reader.has_checksum);
    try testing.expectEqual(@as(usize, 1), reader.index_entries.len);
    try testing.expectEqualStrings(block_data, reader.readBlock(0).?);
}

test "ObjectReader rejects malformed v1 index bounds" {
    var bad_index_size: [ObjectWriter.FOOTER_SIZE_V1]u8 = undefined;
    std.mem.writeInt(u64, bad_index_size[0..8], 0, .big);
    std.mem.writeInt(u32, bad_index_size[8..12], 1, .big);
    std.mem.writeInt(u32, bad_index_size[12..16], ObjectWriter.FOOTER_MAGIC_V1, .big);
    try testing.expectError(error.InvalidIndexSize, ObjectReader.parse(testing.allocator, &bad_index_size));

    var bad_index_position: [ObjectWriter.FOOTER_SIZE_V1]u8 = undefined;
    std.mem.writeInt(u64, bad_index_position[0..8], 1, .big);
    std.mem.writeInt(u32, bad_index_position[8..12], 0, .big);
    std.mem.writeInt(u32, bad_index_position[12..16], ObjectWriter.FOOTER_MAGIC_V1, .big);
    try testing.expectError(error.InvalidIndexPosition, ObjectReader.parse(testing.allocator, &bad_index_position));
}

test "ObjectReader rejects v1 block ranges outside data section" {
    var data_buf = std.array_list.Managed(u8).init(testing.allocator);
    defer data_buf.deinit();

    try data_buf.appendSlice("data");
    const index_position = data_buf.items.len;

    var idx_buf: [ObjectWriter.INDEX_ENTRY_SIZE]u8 = undefined;
    std.mem.writeInt(u64, idx_buf[0..8], 1, .big);
    std.mem.writeInt(u64, idx_buf[8..16], 0, .big);
    std.mem.writeInt(u32, idx_buf[16..20], 1, .big);
    std.mem.writeInt(u32, idx_buf[20..24], 1, .big);
    std.mem.writeInt(u64, idx_buf[24..32], 0, .big);
    std.mem.writeInt(u32, idx_buf[32..36], 5, .big);
    try data_buf.appendSlice(&idx_buf);

    var footer: [ObjectWriter.FOOTER_SIZE_V1]u8 = undefined;
    std.mem.writeInt(u64, footer[0..8], @intCast(index_position), .big);
    std.mem.writeInt(u32, footer[8..12], ObjectWriter.INDEX_ENTRY_SIZE, .big);
    std.mem.writeInt(u32, footer[12..16], ObjectWriter.FOOTER_MAGIC_V1, .big);
    try data_buf.appendSlice(&footer);

    const obj_data = try data_buf.toOwnedSlice();
    defer testing.allocator.free(obj_data);

    try testing.expectError(error.InvalidBlockBounds, ObjectReader.parse(testing.allocator, obj_data));
}

test "ObjectWriter empty build with CRC" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Empty object should be exactly FOOTER_SIZE_V2 = 20 bytes
    try testing.expectEqual(@as(usize, ObjectWriter.FOOTER_SIZE_V2), obj_data.len);

    var reader = try ObjectReader.parse(testing.allocator, obj_data);
    defer reader.deinit();

    try testing.expect(reader.has_checksum);
    try testing.expectEqual(@as(usize, 0), reader.index_entries.len);
}

test "ObjectReader v2 corrupted CRC bytes themselves" {
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 3, 3, "crc-test");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Corrupt the CRC field itself (last 4 bytes) — flips a bit in stored checksum
    obj_data[obj_data.len - 1] ^= 0x01;

    const result = ObjectReader.parse(testing.allocator, obj_data);
    try testing.expectError(error.ChecksumMismatch, result);
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = std.fs;

/// Persists broker metadata (topics, offsets) to a JSON file in the data directory.
/// Loaded on startup, saved on changes.
pub const MetadataPersistence = struct {
    data_dir: ?[]const u8,
    allocator: Allocator,

    pub fn init(alloc: Allocator, data_dir: ?[]const u8) MetadataPersistence {
        return .{ .data_dir = data_dir, .allocator = alloc };
    }

    /// Save topic metadata to disk.
    pub fn saveTopics(self: *MetadataPersistence, topics: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/topics.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        var it = topics.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr;
            try writer.print("{s}\t{d}\t{d}\n", .{ info.name, info.num_partitions, info.replication_factor });
        }
    }

    /// Load topic metadata from disk. Returns list of (name, partitions, rf) tuples.
    pub fn loadTopics(self: *MetadataPersistence) ![]TopicEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/topics.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return &.{};
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch return &.{};
        defer self.allocator.free(content);

        var entries = std.ArrayList(TopicEntry).init(self.allocator);

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            var fields = std.mem.splitSequence(u8, line, "\t");
            const name = fields.next() orelse continue;
            const parts_str = fields.next() orelse continue;
            const rf_str = fields.next() orelse continue;

            const num_parts = std.fmt.parseInt(i32, parts_str, 10) catch continue;
            const rf = std.fmt.parseInt(i16, rf_str, 10) catch continue;

            try entries.append(.{
                .name = try self.allocator.dupe(u8, name),
                .num_partitions = num_parts,
                .replication_factor = rf,
            });
        }

        return entries.toOwnedSlice();
    }

    /// Save committed offsets to disk.
    pub fn saveOffsets(self: *MetadataPersistence, offsets: anytype) !void {
        const dir = self.data_dir orelse return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/offsets.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        const writer = file.writer();
        var it = offsets.iterator();
        while (it.next()) |entry| {
            try writer.print("{s}\t{d}\n", .{ entry.key_ptr.*, entry.value_ptr.* });
        }
    }

    /// Load committed offsets from disk.
    pub fn loadOffsets(self: *MetadataPersistence) ![]OffsetEntry {
        const dir = self.data_dir orelse return &.{};

        const path = try std.fmt.allocPrint(self.allocator, "{s}/offsets.meta", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return &.{};
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch return &.{};
        defer self.allocator.free(content);

        var entries = std.ArrayList(OffsetEntry).init(self.allocator);

        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            var fields = std.mem.splitSequence(u8, line, "\t");
            const key = fields.next() orelse continue;
            const val_str = fields.next() orelse continue;
            const offset = std.fmt.parseInt(i64, val_str, 10) catch continue;
            try entries.append(.{
                .key = try self.allocator.dupe(u8, key),
                .offset = offset,
            });
        }

        return entries.toOwnedSlice();
    }

    pub const TopicEntry = struct {
        name: []u8,
        num_partitions: i32,
        replication_factor: i16,
    };

    pub const OffsetEntry = struct {
        key: []u8,
        offset: i64,
    };
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "MetadataPersistence save and load topics" {
    const tmp_dir = "/tmp/automq-meta-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    fs.makeDirAbsolute(tmp_dir) catch {};

    var persistence = MetadataPersistence.init(testing.allocator, tmp_dir);

    // Create a mock topics map
    var topics = std.StringHashMap(struct { name: []const u8, num_partitions: i32, replication_factor: i16 }).init(testing.allocator);
    defer topics.deinit();

    try topics.put("topic-a", .{ .name = "topic-a", .num_partitions = 3, .replication_factor = 1 });
    try topics.put("topic-b", .{ .name = "topic-b", .num_partitions = 1, .replication_factor = 2 });

    try persistence.saveTopics(&topics);

    const loaded = try persistence.loadTopics();
    defer {
        for (loaded) |e| testing.allocator.free(e.name);
        testing.allocator.free(loaded);
    }

    try testing.expectEqual(@as(usize, 2), loaded.len);

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "MetadataPersistence no data dir" {
    var persistence = MetadataPersistence.init(testing.allocator, null);
    const loaded = try persistence.loadTopics();
    try testing.expectEqual(@as(usize, 0), loaded.len);
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.compaction);

const stream_mod = @import("stream.zig");
const ObjectManager = stream_mod.ObjectManager;
const StreamOffsetRange = stream_mod.StreamOffsetRange;
const ObjectWriter = @import("s3.zig").ObjectWriter;
const ObjectReader = @import("s3.zig").ObjectReader;
const S3Storage = @import("s3_client.zig").S3Storage;
const MockS3 = @import("s3.zig").MockS3;

/// CompactionManager periodically splits multi-stream StreamSetObjects into
/// per-stream StreamObjects, and merges small StreamObjects into larger ones.
///
/// This matches Java AutoMQ's CompactionManager:
/// - Force split: SSOs with multiple streams → per-stream SOs
/// - Merge: many small SOs for same stream → fewer larger SOs
/// - Cleanup: old SSOs deleted after data is fully split
pub const CompactionManager = struct {
    allocator: Allocator,
    object_manager: *ObjectManager,

    /// S3 backend. We use MockS3 for putObject/getObject/deleteObject.
    /// In production, S3Storage wraps either MockS3 or real S3Client.
    mock_s3: ?*MockS3 = null,
    s3_storage: ?*S3Storage = null,

    /// Interval between compaction runs (default 5 minutes).
    compaction_interval_ms: i64 = 5 * 60 * 1000,

    /// Minimum number of StreamObjects per stream to trigger merge.
    merge_min_object_count: usize = 10,

    /// Maximum size of a merged StreamObject (default 256MB).
    merge_max_size: u64 = 256 * 1024 * 1024,

    /// Timestamp of last compaction run.
    last_compaction_ms: i64 = 0,

    /// Stats
    total_splits: u64 = 0,
    total_merges: u64 = 0,
    total_cleanups: u64 = 0,

    pub fn init(
        allocator: Allocator,
        object_manager: *ObjectManager,
    ) CompactionManager {
        return .{
            .allocator = allocator,
            .object_manager = object_manager,
        };
    }

    /// Set MockS3 backend (for testing).
    pub fn setMockS3(self: *CompactionManager, mock: *MockS3) void {
        self.mock_s3 = mock;
    }

    /// Set S3Storage backend (for production).
    pub fn setS3Storage(self: *CompactionManager, storage: *S3Storage) void {
        self.s3_storage = storage;
    }

    /// Called from Broker.tick(). Checks if enough time has elapsed and runs compaction.
    pub fn maybeCompact(self: *CompactionManager) void {
        const now = std.time.milliTimestamp();
        if (now - self.last_compaction_ms < self.compaction_interval_ms) return;
        self.last_compaction_ms = now;

        self.runCompaction() catch |err| {
            log.warn("Compaction failed: {}", .{err});
        };
    }

    /// Full compaction cycle: force-split → merge → cleanup.
    pub fn runCompaction(self: *CompactionManager) !void {
        log.info("Starting compaction cycle (SSOs={d}, SOs={d})", .{
            self.object_manager.getStreamSetObjectCount(),
            self.object_manager.getStreamObjectCount(),
        });

        const splits = try self.forceSplitAll();
        const merges = try self.mergeAll();

        self.total_splits += splits;
        self.total_merges += merges;

        log.info("Compaction complete: {d} splits, {d} merges", .{ splits, merges });
    }

    // ---- Phase 1: Force Split ----

    /// Split all multi-stream SSOs into per-stream StreamObjects.
    fn forceSplitAll(self: *CompactionManager) !u64 {
        var split_count: u64 = 0;

        // Collect SSO IDs to split (can't modify map while iterating)
        var to_split = std.ArrayList(u64).init(self.allocator);
        defer to_split.deinit();

        var it = self.object_manager.stream_set_objects.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.isSingleStream()) {
                try to_split.append(entry.value_ptr.object_id);
            }
        }

        for (to_split.items) |sso_id| {
            self.splitStreamSetObject(sso_id) catch |err| {
                log.warn("Failed to split SSO {d}: {}", .{ sso_id, err });
                continue;
            };
            split_count += 1;
        }

        return split_count;
    }

    /// Split a single SSO: read from S3, extract per-stream data,
    /// write individual StreamObjects, remove old SSO.
    fn splitStreamSetObject(self: *CompactionManager, sso_id: u64) !void {
        const sso = self.object_manager.stream_set_objects.get(sso_id) orelse return;

        // 1. Read the SSO data from S3
        const obj_data = try self.getS3Object(sso.s3_key);
        defer self.allocator.free(obj_data);

        // 2. Parse with ObjectReader
        var reader = try ObjectReader.parse(self.allocator, obj_data);
        defer reader.deinit();

        // 3. Get distinct stream IDs
        // We need to borrow the SSO again after potential modifications
        const sso_ref = self.object_manager.stream_set_objects.get(sso_id) orelse return;
        const stream_ids = try sso_ref.distinctStreamIds(self.allocator);
        defer self.allocator.free(stream_ids);

        // 4. For each stream, collect its blocks and write a StreamObject
        for (stream_ids) |sid| {
            var writer = ObjectWriter.init(self.allocator);
            defer writer.deinit();

            var so_start_offset: u64 = std.math.maxInt(u64);
            var so_end_offset: u64 = 0;
            var block_count: usize = 0;

            for (reader.index_entries, 0..) |entry, i| {
                if (entry.stream_id != sid) continue;

                const block_data = reader.readBlock(i) orelse continue;
                try writer.addDataBlock(
                    entry.stream_id,
                    entry.start_offset,
                    entry.end_offset_delta,
                    entry.record_count,
                    block_data,
                );

                so_start_offset = @min(so_start_offset, entry.start_offset);
                const entry_end = entry.start_offset + entry.end_offset_delta;
                so_end_offset = @max(so_end_offset, entry_end);
                block_count += 1;
            }

            if (block_count == 0) continue;

            const so_data = try writer.build();
            defer self.allocator.free(so_data);

            // 5. Upload new StreamObject to S3
            const new_obj_id = self.object_manager.allocateObjectId();
            const so_key = try std.fmt.allocPrint(
                self.allocator,
                "data/so/{d}/{d}-{d}",
                .{ sid, so_start_offset, new_obj_id },
            );
            defer self.allocator.free(so_key);

            try self.putS3Object(so_key, so_data);

            // 6. Register with ObjectManager
            try self.object_manager.commitStreamObject(
                new_obj_id,
                sid,
                so_start_offset,
                so_end_offset,
                so_key,
                so_data.len,
            );

            log.info("Split SSO {d}: created SO {d} for stream {d} [{d}..{d})", .{
                sso_id, new_obj_id, sid, so_start_offset, so_end_offset,
            });
        }

        // 7. Delete old SSO from S3 and remove from ObjectManager
        // Re-read the key before deleting (the SSO entry might still be valid)
        if (self.object_manager.stream_set_objects.get(sso_id)) |sso_final| {
            self.deleteS3Object(sso_final.s3_key);
        }
        self.object_manager.removeStreamSetObject(sso_id);
    }

    // ---- Phase 2: Merge ----

    /// For each stream with many small StreamObjects, merge them.
    fn mergeAll(self: *CompactionManager) !u64 {
        var merge_count: u64 = 0;

        // Collect stream IDs to consider (can't modify while iterating)
        var stream_ids = std.ArrayList(u64).init(self.allocator);
        defer stream_ids.deinit();

        var it = self.object_manager.stream_object_index.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.items.len >= self.merge_min_object_count) {
                try stream_ids.append(entry.key_ptr.*);
            }
        }

        for (stream_ids.items) |sid| {
            // Re-check after potential modifications
            const so_ids_list = self.object_manager.stream_object_index.get(sid) orelse continue;
            if (so_ids_list.items.len < 2) continue;

            // Copy object IDs to avoid issues with iterator invalidation
            const so_ids = try self.allocator.dupe(u64, so_ids_list.items);
            defer self.allocator.free(so_ids);

            // Group contiguous objects for merging
            var group = std.ArrayList(u64).init(self.allocator);
            defer group.deinit();
            var group_size: u64 = 0;

            for (so_ids) |obj_id| {
                const so = self.object_manager.stream_objects.get(obj_id) orelse continue;

                if (group_size + so.object_size > self.merge_max_size and group.items.len > 1) {
                    // Flush current merge group
                    self.mergeStreamObjects(sid, group.items) catch |err| {
                        log.warn("Merge failed for stream {d}: {}", .{ sid, err });
                    };
                    merge_count += 1;
                    group.clearRetainingCapacity();
                    group_size = 0;
                }

                try group.append(obj_id);
                group_size += so.object_size;
            }

            // Merge remaining group if ≥ 2 objects
            if (group.items.len >= 2) {
                try self.mergeStreamObjects(sid, group.items);
                merge_count += 1;
            }
        }

        return merge_count;
    }

    /// Merge a list of StreamObject IDs for a single stream into one.
    fn mergeStreamObjects(self: *CompactionManager, stream_id: u64, object_ids: []const u64) !void {
        if (object_ids.len < 2) return;

        var writer = ObjectWriter.init(self.allocator);
        defer writer.deinit();

        var merged_start: u64 = std.math.maxInt(u64);
        var merged_end: u64 = 0;

        // Read each SO, extract blocks, append to new writer
        for (object_ids) |obj_id| {
            const so = self.object_manager.stream_objects.get(obj_id) orelse continue;

            const obj_data = self.getS3Object(so.s3_key) catch |err| {
                log.warn("Failed to read SO {d} for merge: {}", .{ obj_id, err });
                continue;
            };
            defer self.allocator.free(obj_data);

            var reader = ObjectReader.parse(self.allocator, obj_data) catch continue;
            defer reader.deinit();

            for (reader.index_entries, 0..) |entry, i| {
                const block_data = reader.readBlock(i) orelse continue;
                try writer.addDataBlock(
                    entry.stream_id,
                    entry.start_offset,
                    entry.end_offset_delta,
                    entry.record_count,
                    block_data,
                );
                merged_start = @min(merged_start, entry.start_offset);
                const entry_end = entry.start_offset + entry.end_offset_delta;
                merged_end = @max(merged_end, entry_end);
            }
        }

        if (merged_start == std.math.maxInt(u64)) return; // No data to merge

        // Write merged object
        const merged_data = try writer.build();
        defer self.allocator.free(merged_data);

        const new_obj_id = self.object_manager.allocateObjectId();
        const merged_key = try std.fmt.allocPrint(
            self.allocator,
            "data/so/{d}/{d}-{d}",
            .{ stream_id, merged_start, new_obj_id },
        );
        defer self.allocator.free(merged_key);

        try self.putS3Object(merged_key, merged_data);
        try self.object_manager.commitStreamObject(
            new_obj_id,
            stream_id,
            merged_start,
            merged_end,
            merged_key,
            merged_data.len,
        );

        // Remove old SOs
        for (object_ids) |obj_id| {
            if (self.object_manager.stream_objects.get(obj_id)) |so| {
                self.deleteS3Object(so.s3_key);
            }
            self.object_manager.removeStreamObject(obj_id);
        }

        log.info("Merged {d} SOs into SO {d} for stream {d} [{d}..{d})", .{
            object_ids.len, new_obj_id, stream_id, merged_start, merged_end,
        });
    }

    // ---- S3 helpers (abstract over MockS3 vs S3Storage) ----

    fn getS3Object(self: *CompactionManager, key: []const u8) ![]u8 {
        if (self.mock_s3) |s3| {
            const data = s3.getObject(key) orelse return error.S3ObjectNotFound;
            return try self.allocator.dupe(u8, data);
        }
        if (self.s3_storage) |s3| {
            const data = (try s3.getObject(key)) orelse return error.S3ObjectNotFound;
            return data;
        }
        return error.NoS3Backend;
    }

    fn putS3Object(self: *CompactionManager, key: []const u8, data: []const u8) !void {
        if (self.mock_s3) |s3| {
            try s3.putObject(key, data);
            return;
        }
        if (self.s3_storage) |s3| {
            try s3.putObject(key, data);
            return;
        }
        return error.NoS3Backend;
    }

    fn deleteS3Object(self: *CompactionManager, key: []const u8) void {
        if (self.mock_s3) |s3| {
            _ = s3.deleteObject(key);
            return;
        }
        if (self.s3_storage) |s3| {
            s3.deleteObject(key) catch {};
            return;
        }
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "CompactionManager forceSplit splits multi-stream SSO" {
    // Setup: ObjectManager + MockS3
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create a multi-stream S3 object using ObjectWriter
    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "stream1-block-data-a");
    try writer.addDataBlock(2, 0, 5, 5, "stream2-block-data");
    try writer.addDataBlock(1, 10, 5, 5, "stream1-block-data-b");
    try writer.addDataBlock(3, 0, 3, 3, "stream3-block-data");

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    // Upload to mock S3
    try mock_s3.putObject("sso/0/1", obj_data);

    // Register as SSO in ObjectManager
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 15 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 5 },
        .{ .stream_id = 3, .start_offset = 0, .end_offset = 3 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());

    // Run compaction
    var cm = CompactionManager.init(testing.allocator, &om);
    cm.setMockS3(&mock_s3);

    try cm.runCompaction();

    // Verify: SSO should be gone, 3 SOs should exist
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());

    // Verify we can query each stream
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);
    try testing.expectEqual(stream_mod.S3ObjectType.stream, r1[0].object_type);
    try testing.expectEqual(@as(u64, 0), r1[0].start_offset);
    try testing.expectEqual(@as(u64, 15), r1[0].end_offset);

    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);

    const r3 = try om.getObjects(3, 0, 100, 10);
    defer testing.allocator.free(r3);
    try testing.expectEqual(@as(usize, 1), r3.len);

    // Verify the split SOs are readable from S3
    var so_it = om.stream_objects.iterator();
    const so1 = so_it.next().?.value_ptr;
    const so1_data = mock_s3.getObject(so1.s3_key);
    try testing.expect(so1_data != null);

    try testing.expectEqual(@as(u64, 1), cm.total_splits);
}

test "CompactionManager does not split single-stream SSO" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();

    try writer.addDataBlock(1, 0, 10, 10, "data");
    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    try mock_s3.putObject("sso/0/1", obj_data);
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 10 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    var cm = CompactionManager.init(testing.allocator, &om);
    cm.setMockS3(&mock_s3);

    try cm.runCompaction();

    // Single-stream SSO should NOT be split
    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 0), cm.total_splits);
}

test "CompactionManager merge combines small StreamObjects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create 12 small StreamObjects for stream 1
    var i: u64 = 0;
    while (i < 12) : (i += 1) {
        var writer = ObjectWriter.init(testing.allocator);
        defer writer.deinit();

        const block_data = "record-data-block";
        try writer.addDataBlock(1, i * 10, 10, 10, block_data);

        const obj_data = try writer.build();
        defer testing.allocator.free(obj_data);

        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);

        try mock_s3.putObject(key, obj_data);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, obj_data.len);
    }

    try testing.expectEqual(@as(usize, 12), om.getStreamObjectCount());

    var cm = CompactionManager.init(testing.allocator, &om);
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;

    try cm.runCompaction();

    // Should have merged into 1 StreamObject
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    try testing.expect(cm.total_merges > 0);

    // Verify merged SO covers full range
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(@as(u64, 120), results[0].end_offset);
}

test "CompactionManager maybeCompact respects interval" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var cm = CompactionManager.init(testing.allocator, &om);
    cm.compaction_interval_ms = 60_000; // 60 seconds

    // First call should set the timestamp but not run (no S3 backend)
    cm.maybeCompact();
    const first_ts = cm.last_compaction_ms;
    try testing.expect(first_ts > 0);

    // Second call immediately should NOT run (< 60s)
    cm.maybeCompact();
    // last_compaction_ms should not have changed significantly
    try testing.expect(cm.last_compaction_ms == first_ts);
}

test "CompactionManager merge skips streams with too few objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create only 3 StreamObjects (below merge_min_object_count=10)
    var i: u64 = 0;
    while (i < 3) : (i += 1) {
        var writer = ObjectWriter.init(testing.allocator);
        defer writer.deinit();
        try writer.addDataBlock(1, i * 10, 10, 10, "data");
        const obj_data = try writer.build();
        defer testing.allocator.free(obj_data);

        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);

        try mock_s3.putObject(key, obj_data);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, obj_data.len);
    }

    var cm = CompactionManager.init(testing.allocator, &om);
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;

    try cm.runCompaction();

    // Should NOT have merged (only 3 objects, threshold is 10)
    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());
    try testing.expectEqual(@as(u64, 0), cm.total_merges);
}

test "CompactionManager forceSplit preserves data integrity" {
    // Verify that after splitting, the data is byte-for-byte identical
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    const s1_data = "STREAM-1-RECORD-DATA-BLOCK";
    const s2_data = "STREAM-2-DIFFERENT-DATA";

    var writer = ObjectWriter.init(testing.allocator);
    defer writer.deinit();
    try writer.addDataBlock(1, 0, 5, 5, s1_data);
    try writer.addDataBlock(2, 0, 3, 3, s2_data);

    const obj_data = try writer.build();
    defer testing.allocator.free(obj_data);

    try mock_s3.putObject("sso/0/1", obj_data);

    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 5 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 3 },
    };
    try om.commitStreamSetObject(1, 0, 1, &ranges, "sso/0/1", obj_data.len);

    var cm = CompactionManager.init(testing.allocator, &om);
    cm.setMockS3(&mock_s3);
    try cm.runCompaction();

    // Verify data integrity: read each StreamObject and check block data
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // Find and verify stream 1's data
    const r1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 1), r1.len);
    const so1_data = mock_s3.getObject(r1[0].s3_key).?;
    var reader1 = try ObjectReader.parse(testing.allocator, so1_data);
    defer reader1.deinit();
    try testing.expectEqualStrings(s1_data, reader1.readBlock(0).?);

    // Find and verify stream 2's data
    const r2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 1), r2.len);
    const so2_data = mock_s3.getObject(r2[0].s3_key).?;
    var reader2 = try ObjectReader.parse(testing.allocator, so2_data);
    defer reader2.deinit();
    try testing.expectEqualStrings(s2_data, reader2.readBlock(0).?);
}

test "CompactionManager merge preserves data integrity" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    var mock_s3 = MockS3.init(testing.allocator);
    defer mock_s3.deinit();

    // Create 12 SOs with known data
    const expected_blocks: [12][]const u8 = .{
        "block-00", "block-01", "block-02", "block-03",
        "block-04", "block-05", "block-06", "block-07",
        "block-08", "block-09", "block-10", "block-11",
    };

    var i: u64 = 0;
    while (i < 12) : (i += 1) {
        var w = ObjectWriter.init(testing.allocator);
        defer w.deinit();
        try w.addDataBlock(1, i * 10, 10, 10, expected_blocks[i]);
        const d = try w.build();
        defer testing.allocator.free(d);

        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);
        try mock_s3.putObject(key, d);
        try om.commitStreamObject(i + 1, 1, i * 10, (i + 1) * 10, key, d.len);
    }

    var cm = CompactionManager.init(testing.allocator, &om);
    cm.setMockS3(&mock_s3);
    cm.merge_min_object_count = 10;
    try cm.runCompaction();

    // Verify merged object contains all 12 blocks in order
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);

    const merged_data = mock_s3.getObject(results[0].s3_key).?;
    var reader = try ObjectReader.parse(testing.allocator, merged_data);
    defer reader.deinit();

    try testing.expectEqual(@as(usize, 12), reader.index_entries.len);
    i = 0;
    while (i < 12) : (i += 1) {
        const block = reader.readBlock(i).?;
        try testing.expectEqualStrings(expected_blocks[i], block);
    }
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.stream);

// ---------------------------------------------------------------
// Stream — represents a partition's append-only data lifecycle
// ---------------------------------------------------------------

pub const StreamState = enum(u8) {
    opened,
    closed,
};

pub const StreamRange = struct {
    epoch: u64,
    start_offset: u64,
    end_offset: u64,
    node_id: i32,
};

/// A Stream is the metadata abstraction for a partition's data in S3.
/// Each Kafka partition maps to one Stream (via hashPartitionKey).
///
/// The stream tracks:
/// - epoch: monotonically increasing, bumped on ownership transfer
/// - start_offset / end_offset: the logical offset range
/// - ranges: ownership history (which broker owned which offsets)
pub const Stream = struct {
    stream_id: u64,
    epoch: u64 = 1,
    start_offset: u64 = 0,
    end_offset: u64 = 0,
    state: StreamState = .opened,
    node_id: i32,
    ranges: std.ArrayList(StreamRange),
    allocator: Allocator,

    pub fn init(allocator: Allocator, stream_id: u64, node_id: i32) Stream {
        return .{
            .stream_id = stream_id,
            .node_id = node_id,
            .ranges = std.ArrayList(StreamRange).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Stream) void {
        self.ranges.deinit();
    }

    /// Called when ownership transfers to a new broker.
    /// Closes the current range, bumps epoch, starts a new range.
    pub fn transferOwnership(self: *Stream, new_node_id: i32) !void {
        // Close current range
        if (self.ranges.items.len > 0) {
            self.ranges.items[self.ranges.items.len - 1].end_offset = self.end_offset;
        }
        self.epoch += 1;
        self.node_id = new_node_id;
        // Open new range starting at current end
        try self.ranges.append(.{
            .epoch = self.epoch,
            .start_offset = self.end_offset,
            .end_offset = self.end_offset,
            .node_id = new_node_id,
        });
    }

    /// Called after produce appends records to this stream.
    pub fn advanceEndOffset(self: *Stream, new_end_offset: u64) void {
        if (new_end_offset > self.end_offset) {
            self.end_offset = new_end_offset;
            // Update the current range's end_offset
            if (self.ranges.items.len > 0) {
                self.ranges.items[self.ranges.items.len - 1].end_offset = new_end_offset;
            }
        }
    }

    /// Trim the stream's start offset (log retention / truncation).
    pub fn trim(self: *Stream, new_start_offset: u64) void {
        if (new_start_offset > self.start_offset) {
            self.start_offset = new_start_offset;
        }
    }

    /// Open the stream with a new epoch. Creates an initial range.
    pub fn open(self: *Stream, epoch: u64) !void {
        self.epoch = epoch;
        self.state = .opened;
        try self.ranges.append(.{
            .epoch = epoch,
            .start_offset = self.end_offset,
            .end_offset = self.end_offset,
            .node_id = self.node_id,
        });
    }

    /// Close the stream. Finalizes the current range.
    pub fn close(self: *Stream) void {
        self.state = .closed;
        if (self.ranges.items.len > 0) {
            self.ranges.items[self.ranges.items.len - 1].end_offset = self.end_offset;
        }
    }
};

// ---------------------------------------------------------------
// StreamOffsetRange — portion of a stream within an S3 object
// ---------------------------------------------------------------

pub const StreamOffsetRange = struct {
    stream_id: u64,
    start_offset: u64,
    end_offset: u64,
};

// ---------------------------------------------------------------
// StreamSetObject — multi-stream S3 object (WAL flush output)
// ---------------------------------------------------------------

/// A StreamSetObject contains data from MULTIPLE streams interleaved in
/// a single S3 object. Created by S3WalBatcher when it flushes buffered
/// records from all partitions on a broker.
pub const StreamSetObject = struct {
    object_id: u64,
    node_id: i32,
    order_id: u64,
    data_time_ms: i64,
    object_size: u64,
    s3_key: []const u8, // owned, heap-allocated
    stream_ranges: std.ArrayList(StreamOffsetRange),

    pub fn deinit(self: *StreamSetObject, allocator: Allocator) void {
        allocator.free(self.s3_key);
        self.stream_ranges.deinit();
    }

    /// Check if this SSO contains data for exactly one stream.
    pub fn isSingleStream(self: *const StreamSetObject) bool {
        if (self.stream_ranges.items.len == 0) return true;
        const first_id = self.stream_ranges.items[0].stream_id;
        for (self.stream_ranges.items[1..]) |r| {
            if (r.stream_id != first_id) return false;
        }
        return true;
    }

    /// Get the set of distinct stream IDs in this SSO.
    /// Caller owns the returned slice.
    pub fn distinctStreamIds(self: *const StreamSetObject, allocator: Allocator) ![]u64 {
        var set = std.AutoHashMap(u64, void).init(allocator);
        defer set.deinit();
        for (self.stream_ranges.items) |r| {
            try set.put(r.stream_id, {});
        }
        var result = try allocator.alloc(u64, set.count());
        var i: usize = 0;
        var it = set.keyIterator();
        while (it.next()) |key| {
            result[i] = key.*;
            i += 1;
        }
        return result;
    }
};

// ---------------------------------------------------------------
// StreamObject — single-stream S3 object (compaction output)
// ---------------------------------------------------------------

/// A StreamObject contains data from exactly ONE stream. Created by the
/// CompactionManager when it splits a multi-stream StreamSetObject into
/// per-stream objects.
pub const StreamObject = struct {
    object_id: u64,
    stream_id: u64,
    start_offset: u64,
    end_offset: u64,
    object_size: u64,
    s3_key: []const u8, // owned, heap-allocated

    pub fn deinit(self: *StreamObject, allocator: Allocator) void {
        allocator.free(self.s3_key);
    }
};

// ---------------------------------------------------------------
// S3ObjectMetadata — unified query result
// ---------------------------------------------------------------

pub const S3ObjectType = enum {
    stream_set,
    stream,
};

/// Unified metadata returned by ObjectManager.getObjects().
/// Describes one S3 object (or a relevant portion of it) for a queried stream.
pub const S3ObjectMetadata = struct {
    object_id: u64,
    object_type: S3ObjectType,
    s3_key: []const u8,
    start_offset: u64,
    end_offset: u64,
    object_size: u64,
};

// ---------------------------------------------------------------
// ObjectManager — metadata registry + object resolution
// ---------------------------------------------------------------

/// ObjectManager maintains an in-memory registry of all S3 objects and
/// the stream-offset ranges they contain. It resolves fetch queries
/// ("for stream X, offsets [A,B), which S3 objects have the data?").
///
/// This matches AutoMQ's ObjectManager / S3ObjectControlManager.
pub const ObjectManager = struct {
    allocator: Allocator,
    node_id: i32,
    next_object_id: u64 = 1,
    next_order_id: u64 = 1,

    // Primary stores
    streams: std.AutoHashMap(u64, Stream),
    stream_set_objects: std.AutoHashMap(u64, StreamSetObject),
    stream_objects: std.AutoHashMap(u64, StreamObject),

    // Secondary index: streamId → sorted list of StreamObject IDs (by start_offset)
    stream_object_index: std.AutoHashMap(u64, std.ArrayList(u64)),

    // Secondary index: streamId → list of StreamSetObject IDs containing this stream
    stream_sso_index: std.AutoHashMap(u64, std.ArrayList(u64)),

    pub fn init(allocator: Allocator, node_id: i32) ObjectManager {
        return .{
            .allocator = allocator,
            .node_id = node_id,
            .streams = std.AutoHashMap(u64, Stream).init(allocator),
            .stream_set_objects = std.AutoHashMap(u64, StreamSetObject).init(allocator),
            .stream_objects = std.AutoHashMap(u64, StreamObject).init(allocator),
            .stream_object_index = std.AutoHashMap(u64, std.ArrayList(u64)).init(allocator),
            .stream_sso_index = std.AutoHashMap(u64, std.ArrayList(u64)).init(allocator),
        };
    }

    pub fn deinit(self: *ObjectManager) void {
        // Deinit streams
        var stream_it = self.streams.iterator();
        while (stream_it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.streams.deinit();

        // Deinit stream_set_objects
        var sso_it = self.stream_set_objects.iterator();
        while (sso_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.stream_set_objects.deinit();

        // Deinit stream_objects
        var so_it = self.stream_objects.iterator();
        while (so_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.stream_objects.deinit();

        // Deinit secondary indexes
        var soi_it = self.stream_object_index.iterator();
        while (soi_it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.stream_object_index.deinit();

        var ssoi_it = self.stream_sso_index.iterator();
        while (ssoi_it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.stream_sso_index.deinit();
    }

    // ---- Stream lifecycle ----

    /// Create a new stream with an auto-allocated stream_id.
    pub fn createStream(self: *ObjectManager, node_id: i32) !*Stream {
        const stream_id = self.next_object_id;
        self.next_object_id += 1;
        return self.createStreamWithId(stream_id, node_id);
    }

    /// Create a new stream with a specific stream_id.
    pub fn createStreamWithId(self: *ObjectManager, stream_id: u64, node_id: i32) !*Stream {
        var stream = Stream.init(self.allocator, stream_id, node_id);
        // Create the initial range
        try stream.ranges.append(.{
            .epoch = stream.epoch,
            .start_offset = 0,
            .end_offset = 0,
            .node_id = node_id,
        });
        try self.streams.put(stream_id, stream);
        return self.streams.getPtr(stream_id).?;
    }

    /// Get a mutable pointer to a stream.
    pub fn getStream(self: *ObjectManager, stream_id: u64) ?*Stream {
        return self.streams.getPtr(stream_id);
    }

    /// Open a stream with a new epoch (used during failover).
    pub fn openStream(self: *ObjectManager, stream_id: u64, epoch: u64) !void {
        const stream = self.streams.getPtr(stream_id) orelse return error.StreamNotFound;
        try stream.open(epoch);
    }

    /// Close a stream.
    pub fn closeStream(self: *ObjectManager, stream_id: u64) !void {
        const stream = self.streams.getPtr(stream_id) orelse return error.StreamNotFound;
        stream.close();
    }

    /// Trim a stream's start offset.
    pub fn trimStream(self: *ObjectManager, stream_id: u64, new_start_offset: u64) !void {
        const stream = self.streams.getPtr(stream_id) orelse return error.StreamNotFound;
        stream.trim(new_start_offset);
    }

    // ---- Object ID allocation ----

    pub fn allocateObjectId(self: *ObjectManager) u64 {
        const id = self.next_object_id;
        self.next_object_id += 1;
        return id;
    }

    // ---- Object registration ----

    /// Register a StreamSetObject (multi-stream WAL flush output).
    pub fn commitStreamSetObject(
        self: *ObjectManager,
        object_id: u64,
        node_id: i32,
        order_id: u64,
        ranges: []const StreamOffsetRange,
        s3_key: []const u8,
        object_size: u64,
    ) !void {
        // Ensure next_object_id stays ahead of any committed ID
        if (object_id >= self.next_object_id) {
            self.next_object_id = object_id + 1;
        }
        var range_list = std.ArrayList(StreamOffsetRange).init(self.allocator);
        for (ranges) |r| {
            try range_list.append(r);
        }

        const key_copy = try self.allocator.dupe(u8, s3_key);

        try self.stream_set_objects.put(object_id, .{
            .object_id = object_id,
            .node_id = node_id,
            .order_id = order_id,
            .data_time_ms = std.time.milliTimestamp(),
            .object_size = object_size,
            .s3_key = key_copy,
            .stream_ranges = range_list,
        });

        // Update secondary SSO index for each stream
        for (ranges) |r| {
            var gop = try self.stream_sso_index.getOrPut(r.stream_id);
            if (!gop.found_existing) {
                gop.value_ptr.* = std.ArrayList(u64).init(self.allocator);
            }
            try gop.value_ptr.append(object_id);
        }
    }

    /// Register a StreamObject (single-stream, compaction output).
    pub fn commitStreamObject(
        self: *ObjectManager,
        object_id: u64,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        s3_key: []const u8,
        object_size: u64,
    ) !void {
        // Ensure next_object_id stays ahead of any committed ID
        if (object_id >= self.next_object_id) {
            self.next_object_id = object_id + 1;
        }

        const key_copy = try self.allocator.dupe(u8, s3_key);

        try self.stream_objects.put(object_id, .{
            .object_id = object_id,
            .stream_id = stream_id,
            .start_offset = start_offset,
            .end_offset = end_offset,
            .object_size = object_size,
            .s3_key = key_copy,
        });

        // Update stream_object_index — insert maintaining sorted order by start_offset
        var gop = try self.stream_object_index.getOrPut(stream_id);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList(u64).init(self.allocator);
        }

        // Find insertion point to maintain sorted order
        var insert_pos: usize = gop.value_ptr.items.len;
        for (gop.value_ptr.items, 0..) |existing_id, i| {
            const existing = self.stream_objects.get(existing_id) orelse continue;
            if (existing.start_offset > start_offset) {
                insert_pos = i;
                break;
            }
        }
        try gop.value_ptr.insert(insert_pos, object_id);
    }

    /// Remove a StreamSetObject from the registry.
    pub fn removeStreamSetObject(self: *ObjectManager, object_id: u64) void {
        if (self.stream_set_objects.fetchRemove(object_id)) |kv| {
            var sso = kv.value;
            // Remove from secondary indexes
            for (sso.stream_ranges.items) |r| {
                if (self.stream_sso_index.getPtr(r.stream_id)) |list| {
                    // Remove this object_id from the list
                    var i: usize = 0;
                    while (i < list.items.len) {
                        if (list.items[i] == object_id) {
                            _ = list.orderedRemove(i);
                        } else {
                            i += 1;
                        }
                    }
                }
            }
            sso.deinit(self.allocator);
        }
    }

    /// Remove a StreamObject from the registry.
    pub fn removeStreamObject(self: *ObjectManager, object_id: u64) void {
        if (self.stream_objects.fetchRemove(object_id)) |kv| {
            var so = kv.value;
            // Remove from secondary index
            if (self.stream_object_index.getPtr(so.stream_id)) |list| {
                var i: usize = 0;
                while (i < list.items.len) {
                    if (list.items[i] == object_id) {
                        _ = list.orderedRemove(i);
                    } else {
                        i += 1;
                    }
                }
            }
            so.deinit(self.allocator);
        }
    }

    // ---- Object resolution (fetch path) ----

    /// For stream X, offsets [start_offset, end_offset), return all S3 objects
    /// containing relevant data. Results are sorted by start_offset.
    /// Caller owns the returned slice.
    pub fn getObjects(
        self: *ObjectManager,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: u32,
    ) ![]S3ObjectMetadata {
        var results = std.ArrayList(S3ObjectMetadata).init(self.allocator);
        errdefer results.deinit();

        // 1. Collect StreamObjects that overlap [start_offset, end_offset)
        if (self.stream_object_index.get(stream_id)) |so_ids| {
            for (so_ids.items) |obj_id| {
                if (results.items.len >= limit) break;
                const so = self.stream_objects.get(obj_id) orelse continue;
                // Check overlap
                if (so.end_offset <= start_offset) continue;
                if (so.start_offset >= end_offset) break; // sorted, no more matches
                try results.append(.{
                    .object_id = so.object_id,
                    .object_type = .stream,
                    .s3_key = so.s3_key,
                    .start_offset = so.start_offset,
                    .end_offset = so.end_offset,
                    .object_size = so.object_size,
                });
            }
        }

        // 2. Collect StreamSetObjects with ranges for this stream overlapping
        if (self.stream_sso_index.get(stream_id)) |sso_ids| {
            for (sso_ids.items) |obj_id| {
                if (results.items.len >= limit) break;
                const sso = self.stream_set_objects.get(obj_id) orelse continue;
                for (sso.stream_ranges.items) |range| {
                    if (range.stream_id != stream_id) continue;
                    if (range.end_offset <= start_offset) continue;
                    if (range.start_offset >= end_offset) continue;
                    try results.append(.{
                        .object_id = sso.object_id,
                        .object_type = .stream_set,
                        .s3_key = sso.s3_key,
                        .start_offset = range.start_offset,
                        .end_offset = range.end_offset,
                        .object_size = sso.object_size,
                    });
                    break; // Only add this SSO once per stream
                }
            }
        }

        // 3. Sort by start_offset
        std.mem.sort(S3ObjectMetadata, results.items, {}, struct {
            fn lessThan(_: void, a: S3ObjectMetadata, b: S3ObjectMetadata) bool {
                if (a.start_offset != b.start_offset) return a.start_offset < b.start_offset;
                // Prefer StreamObjects over StreamSetObjects at same offset
                return @intFromEnum(a.object_type) > @intFromEnum(b.object_type);
            }
        }.lessThan);

        // 4. Truncate to limit
        if (results.items.len > limit) {
            results.shrinkRetainingCapacity(limit);
        }

        return try results.toOwnedSlice();
    }

    // ---- Query helpers ----

    pub fn getStreamSetObjectCount(self: *const ObjectManager) usize {
        return self.stream_set_objects.count();
    }

    pub fn getStreamObjectCount(self: *const ObjectManager) usize {
        return self.stream_objects.count();
    }

    pub fn streamCount(self: *const ObjectManager) usize {
        return self.streams.count();
    }

    /// Check if a StreamObject already exists that covers the exact offset range.
    /// Used for idempotent compaction — skip duplicate SOs.
    pub fn hasStreamObjectCovering(self: *const ObjectManager, stream_id: u64, start_offset: u64, end_offset: u64) bool {
        const so_ids = self.stream_object_index.get(stream_id) orelse return false;
        for (so_ids.items) |obj_id| {
            const so = self.stream_objects.get(obj_id) orelse continue;
            if (so.start_offset == start_offset and so.end_offset == end_offset) return true;
        }
        return false;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "Stream init and lifecycle" {
    var stream = Stream.init(testing.allocator, 100, 0);
    defer stream.deinit();

    try testing.expectEqual(@as(u64, 100), stream.stream_id);
    try testing.expectEqual(@as(u64, 1), stream.epoch);
    try testing.expectEqual(@as(u64, 0), stream.end_offset);
    try testing.expectEqual(StreamState.opened, stream.state);

    stream.advanceEndOffset(50);
    try testing.expectEqual(@as(u64, 50), stream.end_offset);

    stream.trim(10);
    try testing.expectEqual(@as(u64, 10), stream.start_offset);

    // trim should not go backwards
    stream.trim(5);
    try testing.expectEqual(@as(u64, 10), stream.start_offset);
}

test "Stream transferOwnership" {
    var stream = Stream.init(testing.allocator, 100, 0);
    defer stream.deinit();

    try stream.open(1);
    stream.advanceEndOffset(50);

    try stream.transferOwnership(1);
    try testing.expectEqual(@as(u64, 2), stream.epoch);
    try testing.expectEqual(@as(i32, 1), stream.node_id);
    try testing.expectEqual(@as(usize, 2), stream.ranges.items.len);
    // New range starts at old end_offset
    try testing.expectEqual(@as(u64, 50), stream.ranges.items[1].start_offset);
}

test "Stream close" {
    var stream = Stream.init(testing.allocator, 100, 0);
    defer stream.deinit();

    try stream.open(1);
    stream.advanceEndOffset(100);
    stream.close();

    try testing.expectEqual(StreamState.closed, stream.state);
    try testing.expectEqual(@as(u64, 100), stream.ranges.items[0].end_offset);
}

test "StreamSetObject isSingleStream" {
    var ranges = std.ArrayList(StreamOffsetRange).init(testing.allocator);
    defer ranges.deinit();

    try ranges.append(.{ .stream_id = 1, .start_offset = 0, .end_offset = 10 });
    try ranges.append(.{ .stream_id = 1, .start_offset = 10, .end_offset = 20 });

    const sso = StreamSetObject{
        .object_id = 1,
        .node_id = 0,
        .order_id = 1,
        .data_time_ms = 0,
        .object_size = 100,
        .s3_key = "test-key",
        .stream_ranges = ranges,
    };

    try testing.expect(sso.isSingleStream());
}

test "StreamSetObject multi-stream" {
    var ranges = std.ArrayList(StreamOffsetRange).init(testing.allocator);
    defer ranges.deinit();

    try ranges.append(.{ .stream_id = 1, .start_offset = 0, .end_offset = 10 });
    try ranges.append(.{ .stream_id = 2, .start_offset = 0, .end_offset = 5 });

    const sso = StreamSetObject{
        .object_id = 1,
        .node_id = 0,
        .order_id = 1,
        .data_time_ms = 0,
        .object_size = 100,
        .s3_key = "test-key",
        .stream_ranges = ranges,
    };

    try testing.expect(!sso.isSingleStream());

    const ids = try sso.distinctStreamIds(testing.allocator);
    defer testing.allocator.free(ids);
    try testing.expectEqual(@as(usize, 2), ids.len);
}

test "ObjectManager init and deinit" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try testing.expectEqual(@as(usize, 0), om.streamCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());
}

test "ObjectManager createStream" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const s = try om.createStreamWithId(100, 0);
    try testing.expectEqual(@as(u64, 100), s.stream_id);
    try testing.expectEqual(@as(i32, 0), s.node_id);
    try testing.expectEqual(@as(usize, 1), om.streamCount());

    const s2 = om.getStream(100);
    try testing.expect(s2 != null);
    try testing.expectEqual(@as(u64, 100), s2.?.stream_id);

    try testing.expect(om.getStream(999) == null);
}

test "ObjectManager commitStreamSetObject and getObjects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 50 },
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 30 },
    };

    try om.commitStreamSetObject(10, 0, 1, &ranges, "sso/10", 1024);

    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());

    // Query stream 1
    const results1 = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(results1);
    try testing.expectEqual(@as(usize, 1), results1.len);
    try testing.expectEqual(@as(u64, 0), results1[0].start_offset);
    try testing.expectEqual(@as(u64, 50), results1[0].end_offset);
    try testing.expectEqual(S3ObjectType.stream_set, results1[0].object_type);

    // Query stream 2
    const results2 = try om.getObjects(2, 0, 100, 10);
    defer testing.allocator.free(results2);
    try testing.expectEqual(@as(usize, 1), results2.len);
    try testing.expectEqual(@as(u64, 30), results2[0].end_offset);

    // Query non-existent stream
    const results3 = try om.getObjects(999, 0, 100, 10);
    defer testing.allocator.free(results3);
    try testing.expectEqual(@as(usize, 0), results3.len);
}

test "ObjectManager commitStreamObject and getObjects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(20, 1, 0, 100, "so/1/0-20", 2048);
    try om.commitStreamObject(21, 1, 100, 200, "so/1/100-21", 2048);

    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // Query covering both objects
    const results = try om.getObjects(1, 0, 300, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 2), results.len);
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(@as(u64, 100), results[0].end_offset);
    try testing.expectEqual(@as(u64, 100), results[1].start_offset);
    try testing.expectEqual(@as(u64, 200), results[1].end_offset);
    try testing.expectEqual(S3ObjectType.stream, results[0].object_type);

    // Query partial range
    const results2 = try om.getObjects(1, 50, 150, 10);
    defer testing.allocator.free(results2);
    try testing.expectEqual(@as(usize, 2), results2.len);

    // Query only second object
    const results3 = try om.getObjects(1, 100, 300, 10);
    defer testing.allocator.free(results3);
    try testing.expectEqual(@as(usize, 1), results3.len);
    try testing.expectEqual(@as(u64, 100), results3[0].start_offset);
}

test "ObjectManager getObjects merges SSO and SO sorted by offset" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // SSO with stream 1 offsets [0, 50)
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 50 },
    };
    try om.commitStreamSetObject(10, 0, 1, &ranges, "sso/10", 512);

    // SO with stream 1 offsets [50, 100)
    try om.commitStreamObject(20, 1, 50, 100, "so/1/50-20", 1024);

    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);

    try testing.expectEqual(@as(usize, 2), results.len);
    // First should be offset 0 (SSO)
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(S3ObjectType.stream_set, results[0].object_type);
    // Second should be offset 50 (SO)
    try testing.expectEqual(@as(u64, 50), results[1].start_offset);
    try testing.expectEqual(S3ObjectType.stream, results[1].object_type);
}

test "ObjectManager getObjects respects limit" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create 5 StreamObjects
    var i: u64 = 0;
    while (i < 5) : (i += 1) {
        const key = try std.fmt.allocPrint(testing.allocator, "so/1/{d}", .{i});
        defer testing.allocator.free(key);
        try om.commitStreamObject(i + 1, 1, i * 100, (i + 1) * 100, key, 100);
    }

    const results = try om.getObjects(1, 0, 1000, 3);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 3), results.len);
}

test "ObjectManager removeStreamSetObject" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 50 },
    };
    try om.commitStreamSetObject(10, 0, 1, &ranges, "sso/10", 512);

    try testing.expectEqual(@as(usize, 1), om.getStreamSetObjectCount());

    om.removeStreamSetObject(10);
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());

    // Should no longer appear in queries
    const results = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 0), results.len);
}

test "ObjectManager removeStreamObject" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(20, 1, 0, 100, "so/1/0-20", 1024);
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());

    om.removeStreamObject(20);
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());

    const results = try om.getObjects(1, 0, 100, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 0), results.len);
}

test "ObjectManager allocateObjectId" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const id1 = om.allocateObjectId();
    const id2 = om.allocateObjectId();
    const id3 = om.allocateObjectId();

    try testing.expect(id1 < id2);
    try testing.expect(id2 < id3);
}

test "ObjectManager getObjects no overlap" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(1, 1, 100, 200, "so/1/100", 1024);

    // Query before the object
    const r1 = try om.getObjects(1, 0, 50, 10);
    defer testing.allocator.free(r1);
    try testing.expectEqual(@as(usize, 0), r1.len);

    // Query after the object
    const r2 = try om.getObjects(1, 200, 300, 10);
    defer testing.allocator.free(r2);
    try testing.expectEqual(@as(usize, 0), r2.len);

    // Query touching start boundary exactly
    const r3 = try om.getObjects(1, 100, 150, 10);
    defer testing.allocator.free(r3);
    try testing.expectEqual(@as(usize, 1), r3.len);
}

test "ObjectManager openStream and closeStream" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    _ = try om.createStreamWithId(100, 0);

    // Open with epoch 2
    try om.openStream(100, 2);
    const s = om.getStream(100).?;
    try testing.expectEqual(@as(u64, 2), s.epoch);
    try testing.expectEqual(StreamState.opened, s.state);
    try testing.expectEqual(@as(usize, 2), s.ranges.items.len);

    // Close
    try om.closeStream(100);
    try testing.expectEqual(StreamState.closed, om.getStream(100).?.state);

    // Non-existent stream
    try testing.expectError(error.StreamNotFound, om.openStream(999, 1));
    try testing.expectError(error.StreamNotFound, om.closeStream(999));
}

test "ObjectManager trimStream" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const s = try om.createStreamWithId(100, 0);
    s.advanceEndOffset(200);

    try om.trimStream(100, 50);
    try testing.expectEqual(@as(u64, 50), om.getStream(100).?.start_offset);

    // Trim again with lower value (no-op)
    try om.trimStream(100, 30);
    try testing.expectEqual(@as(u64, 50), om.getStream(100).?.start_offset);

    try testing.expectError(error.StreamNotFound, om.trimStream(999, 10));
}

test "ObjectManager getObjects with overlapping SSO and SO at same offset" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // SSO covering [0, 100) for stream 1
    const sso_ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 100 },
    };
    try om.commitStreamSetObject(10, 0, 1, &sso_ranges, "sso/10", 1024);

    // SO covering [0, 50) for stream 1 (from compaction - overlaps with SSO)
    try om.commitStreamObject(20, 1, 0, 50, "so/1/0-20", 512);

    // Both should be returned, SO before SSO at same offset
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 2), results.len);
    // StreamObject (.stream) sorts after StreamSetObject at same offset due to enum ordering
}

test "ObjectManager multiple SSOs for same stream" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const r1 = [_]StreamOffsetRange{.{ .stream_id = 1, .start_offset = 0, .end_offset = 50 }};
    try om.commitStreamSetObject(10, 0, 1, &r1, "sso/10", 512);

    const r2 = [_]StreamOffsetRange{.{ .stream_id = 1, .start_offset = 50, .end_offset = 100 }};
    try om.commitStreamSetObject(11, 0, 2, &r2, "sso/11", 512);

    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 2), results.len);
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(@as(u64, 50), results[1].start_offset);
}

test "ObjectManager commitStreamObject maintains sorted index" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Insert out of order
    try om.commitStreamObject(3, 1, 200, 300, "so/200", 100);
    try om.commitStreamObject(1, 1, 0, 100, "so/0", 100);
    try om.commitStreamObject(2, 1, 100, 200, "so/100", 100);

    const results = try om.getObjects(1, 0, 500, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 3), results.len);
    // Should be sorted by start_offset
    try testing.expectEqual(@as(u64, 0), results[0].start_offset);
    try testing.expectEqual(@as(u64, 100), results[1].start_offset);
    try testing.expectEqual(@as(u64, 200), results[2].start_offset);
}

test "ObjectManager hasStreamObjectCovering" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // No SOs yet — should return false
    try testing.expect(!om.hasStreamObjectCovering(1, 0, 100));

    // Add a SO for stream 1 covering [0, 100)
    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);

    // Exact match — should return true
    try testing.expect(om.hasStreamObjectCovering(1, 0, 100));

    // Different start — should return false
    try testing.expect(!om.hasStreamObjectCovering(1, 10, 100));

    // Different end — should return false
    try testing.expect(!om.hasStreamObjectCovering(1, 0, 50));

    // Different stream — should return false
    try testing.expect(!om.hasStreamObjectCovering(2, 0, 100));

    // Add another SO for stream 1 covering [100, 200)
    try om.commitStreamObject(2, 1, 100, 200, "so/1/100-2", 1024);
    try testing.expect(om.hasStreamObjectCovering(1, 100, 200));
    // Original still matches
    try testing.expect(om.hasStreamObjectCovering(1, 0, 100));
}

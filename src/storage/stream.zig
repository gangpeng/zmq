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
        const old_node = self.node_id;
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
        log.info("Stream {d} ownership transferred: node {d} -> {d}, epoch={d}", .{ self.stream_id, old_node, new_node_id, self.epoch });
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
        log.debug("Stream created: id={d}, node={d}", .{ stream_id, node_id });
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

    // ---- Snapshot persistence ----

    /// Snapshot format version. Bumped when the binary layout changes.
    /// Forward compatibility: loadSnapshot rejects unknown versions.
    const SNAPSHOT_VERSION: u8 = 1;

    /// Serialize all ObjectManager state (streams, SOs, SSOs, orphaned keys) to
    /// a binary buffer suitable for writing to disk. The caller owns the returned
    /// slice and must free it with the same allocator.
    ///
    /// Binary layout (all integers little-endian):
    ///   [1 byte]  version
    ///   [8 bytes] next_object_id (u64)
    ///   [8 bytes] next_order_id (u64)
    ///   --- Streams ---
    ///   [4 bytes] stream_count (u32)
    ///   For each stream:
    ///     [8 bytes] stream_id (u64)
    ///     [8 bytes] epoch (u64)
    ///     [8 bytes] start_offset (u64)
    ///     [8 bytes] end_offset (u64)
    ///     [1 byte]  state (u8: 0=opened, 1=closed)
    ///     [4 bytes] node_id (i32)
    ///     [4 bytes] range_count (u32)
    ///     For each range:
    ///       [8 bytes] epoch (u64)
    ///       [8 bytes] start_offset (u64)
    ///       [8 bytes] end_offset (u64)
    ///       [4 bytes] node_id (i32)
    ///   --- StreamObjects ---
    ///   [4 bytes] so_count (u32)
    ///   For each SO:
    ///     [8 bytes] object_id (u64)
    ///     [8 bytes] stream_id (u64)
    ///     [8 bytes] start_offset (u64)
    ///     [8 bytes] end_offset (u64)
    ///     [8 bytes] object_size (u64)
    ///     [2 bytes] key_len (u16)
    ///     [key_len] s3_key bytes
    ///   --- StreamSetObjects ---
    ///   [4 bytes] sso_count (u32)
    ///   For each SSO:
    ///     [8 bytes] object_id (u64)
    ///     [4 bytes] node_id (i32)
    ///     [8 bytes] order_id (u64)
    ///     [8 bytes] data_time_ms (i64)
    ///     [8 bytes] object_size (u64)
    ///     [2 bytes] key_len (u16)
    ///     [key_len] s3_key bytes
    ///     [4 bytes] range_count (u32)
    ///     For each range:
    ///       [8 bytes] stream_id (u64)
    ///       [8 bytes] start_offset (u64)
    ///       [8 bytes] end_offset (u64)
    ///   --- Orphaned Keys ---
    ///   [4 bytes] orphan_count (u32)
    ///   For each key:
    ///     [2 bytes] key_len (u16)
    ///     [key_len] key bytes
    pub fn takeSnapshot(self: *const ObjectManager, orphaned_keys: []const []const u8) ![]u8 {
        // Pre-compute buffer size to avoid repeated reallocations
        var size: usize = 0;
        size += 1; // version
        size += 8; // next_object_id
        size += 8; // next_order_id

        // Streams
        size += 4; // stream_count
        var stream_it = self.streams.iterator();
        while (stream_it.next()) |entry| {
            const s = entry.value_ptr;
            size += 8 + 8 + 8 + 8 + 1 + 4; // fixed fields
            size += 4; // range_count
            size += s.ranges.items.len * (8 + 8 + 8 + 4); // ranges
        }

        // StreamObjects
        size += 4; // so_count
        var so_it = self.stream_objects.iterator();
        while (so_it.next()) |entry| {
            const so = entry.value_ptr;
            size += 8 + 8 + 8 + 8 + 8; // fixed fields
            size += 2 + so.s3_key.len; // key_len + key
        }

        // StreamSetObjects
        size += 4; // sso_count
        var sso_it = self.stream_set_objects.iterator();
        while (sso_it.next()) |entry| {
            const sso = entry.value_ptr;
            size += 8 + 4 + 8 + 8 + 8; // fixed fields
            size += 2 + sso.s3_key.len; // key_len + key
            size += 4; // range_count
            size += sso.stream_ranges.items.len * (8 + 8 + 8); // ranges
        }

        // Orphaned keys
        size += 4; // orphan_count
        for (orphaned_keys) |key| {
            size += 2 + key.len; // key_len + key
        }

        var buf = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(buf);
        var pos: usize = 0;

        // Version
        buf[pos] = SNAPSHOT_VERSION;
        pos += 1;

        // next_object_id, next_order_id
        writeU64(buf, &pos, self.next_object_id);
        writeU64(buf, &pos, self.next_order_id);

        // --- Streams ---
        writeU32(buf, &pos, @intCast(self.streams.count()));
        var stream_it2 = self.streams.iterator();
        while (stream_it2.next()) |entry| {
            const s = entry.value_ptr;
            writeU64(buf, &pos, s.stream_id);
            writeU64(buf, &pos, s.epoch);
            writeU64(buf, &pos, s.start_offset);
            writeU64(buf, &pos, s.end_offset);
            buf[pos] = @intFromEnum(s.state);
            pos += 1;
            writeI32(buf, &pos, s.node_id);
            writeU32(buf, &pos, @intCast(s.ranges.items.len));
            for (s.ranges.items) |r| {
                writeU64(buf, &pos, r.epoch);
                writeU64(buf, &pos, r.start_offset);
                writeU64(buf, &pos, r.end_offset);
                writeI32(buf, &pos, r.node_id);
            }
        }

        // --- StreamObjects ---
        writeU32(buf, &pos, @intCast(self.stream_objects.count()));
        var so_it2 = self.stream_objects.iterator();
        while (so_it2.next()) |entry| {
            const so = entry.value_ptr;
            writeU64(buf, &pos, so.object_id);
            writeU64(buf, &pos, so.stream_id);
            writeU64(buf, &pos, so.start_offset);
            writeU64(buf, &pos, so.end_offset);
            writeU64(buf, &pos, so.object_size);
            writeU16(buf, &pos, @intCast(so.s3_key.len));
            @memcpy(buf[pos .. pos + so.s3_key.len], so.s3_key);
            pos += so.s3_key.len;
        }

        // --- StreamSetObjects ---
        writeU32(buf, &pos, @intCast(self.stream_set_objects.count()));
        var sso_it2 = self.stream_set_objects.iterator();
        while (sso_it2.next()) |entry| {
            const sso = entry.value_ptr;
            writeU64(buf, &pos, sso.object_id);
            writeI32(buf, &pos, sso.node_id);
            writeU64(buf, &pos, sso.order_id);
            writeI64(buf, &pos, sso.data_time_ms);
            writeU64(buf, &pos, sso.object_size);
            writeU16(buf, &pos, @intCast(sso.s3_key.len));
            @memcpy(buf[pos .. pos + sso.s3_key.len], sso.s3_key);
            pos += sso.s3_key.len;
            writeU32(buf, &pos, @intCast(sso.stream_ranges.items.len));
            for (sso.stream_ranges.items) |r| {
                writeU64(buf, &pos, r.stream_id);
                writeU64(buf, &pos, r.start_offset);
                writeU64(buf, &pos, r.end_offset);
            }
        }

        // --- Orphaned Keys ---
        writeU32(buf, &pos, @intCast(orphaned_keys.len));
        for (orphaned_keys) |key| {
            writeU16(buf, &pos, @intCast(key.len));
            @memcpy(buf[pos .. pos + key.len], key);
            pos += key.len;
        }

        std.debug.assert(pos == size);
        return buf;
    }

    /// Restore ObjectManager state from a snapshot buffer produced by takeSnapshot().
    /// Also returns any persisted orphaned keys. The caller owns the returned slice
    /// and each key within it (allocated with self.allocator).
    ///
    /// On corrupt/truncated data, returns error.CorruptSnapshot.
    /// On unknown version, returns error.UnsupportedSnapshotVersion.
    pub fn loadSnapshot(self: *ObjectManager, data: []const u8) ![][]u8 {
        if (data.len < 1) return error.CorruptSnapshot;

        var pos: usize = 0;

        // Version check
        const version = data[pos];
        pos += 1;
        if (version != SNAPSHOT_VERSION) return error.UnsupportedSnapshotVersion;

        // next_object_id, next_order_id
        self.next_object_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
        self.next_order_id = readU64(data, &pos) orelse return error.CorruptSnapshot;

        // --- Streams ---
        const stream_count = readU32(data, &pos) orelse return error.CorruptSnapshot;
        var si: u32 = 0;
        while (si < stream_count) : (si += 1) {
            const stream_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const epoch = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const start_offset = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const end_offset = readU64(data, &pos) orelse return error.CorruptSnapshot;
            if (pos >= data.len) return error.CorruptSnapshot;
            const state_byte = data[pos];
            pos += 1;
            if (state_byte > 1) return error.CorruptSnapshot;
            const state: StreamState = @enumFromInt(state_byte);
            const node_id = readI32(data, &pos) orelse return error.CorruptSnapshot;
            const range_count = readU32(data, &pos) orelse return error.CorruptSnapshot;

            var ranges = std.ArrayList(StreamRange).init(self.allocator);
            errdefer ranges.deinit();
            var ri: u32 = 0;
            while (ri < range_count) : (ri += 1) {
                const r_epoch = readU64(data, &pos) orelse return error.CorruptSnapshot;
                const r_start = readU64(data, &pos) orelse return error.CorruptSnapshot;
                const r_end = readU64(data, &pos) orelse return error.CorruptSnapshot;
                const r_node = readI32(data, &pos) orelse return error.CorruptSnapshot;
                try ranges.append(.{
                    .epoch = r_epoch,
                    .start_offset = r_start,
                    .end_offset = r_end,
                    .node_id = r_node,
                });
            }

            const stream = Stream{
                .stream_id = stream_id,
                .epoch = epoch,
                .start_offset = start_offset,
                .end_offset = end_offset,
                .state = state,
                .node_id = node_id,
                .ranges = ranges,
                .allocator = self.allocator,
            };
            // If the stream ID already exists, clean up the old entry first
            if (self.streams.fetchRemove(stream_id)) |old| {
                var old_stream = old.value;
                old_stream.deinit();
            }
            try self.streams.put(stream_id, stream);
        }

        // --- StreamObjects ---
        const so_count = readU32(data, &pos) orelse return error.CorruptSnapshot;
        var soi: u32 = 0;
        while (soi < so_count) : (soi += 1) {
            const object_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const stream_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const start_offset = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const end_offset = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const object_size = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const key_len = readU16(data, &pos) orelse return error.CorruptSnapshot;
            if (pos + key_len > data.len) return error.CorruptSnapshot;
            const s3_key = data[pos .. pos + key_len];
            pos += key_len;

            // Use commitStreamObject to rebuild both primary store and secondary index
            try self.commitStreamObject(object_id, stream_id, start_offset, end_offset, s3_key, object_size);
        }

        // --- StreamSetObjects ---
        const sso_count = readU32(data, &pos) orelse return error.CorruptSnapshot;
        var ssoi: u32 = 0;
        while (ssoi < sso_count) : (ssoi += 1) {
            const object_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const node_id = readI32(data, &pos) orelse return error.CorruptSnapshot;
            const order_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const data_time_ms = readI64(data, &pos) orelse return error.CorruptSnapshot;
            const object_size = readU64(data, &pos) orelse return error.CorruptSnapshot;
            const key_len = readU16(data, &pos) orelse return error.CorruptSnapshot;
            if (pos + key_len > data.len) return error.CorruptSnapshot;
            const s3_key = data[pos .. pos + key_len];
            pos += key_len;
            const range_count = readU32(data, &pos) orelse return error.CorruptSnapshot;

            var ranges_buf = try self.allocator.alloc(StreamOffsetRange, range_count);
            defer self.allocator.free(ranges_buf);
            var ri: u32 = 0;
            while (ri < range_count) : (ri += 1) {
                const r_stream_id = readU64(data, &pos) orelse return error.CorruptSnapshot;
                const r_start = readU64(data, &pos) orelse return error.CorruptSnapshot;
                const r_end = readU64(data, &pos) orelse return error.CorruptSnapshot;
                ranges_buf[ri] = .{
                    .stream_id = r_stream_id,
                    .start_offset = r_start,
                    .end_offset = r_end,
                };
            }

            // commitStreamSetObject duplicates the key and ranges, and sets data_time_ms
            // to now. We need to preserve the original data_time_ms, so we register
            // directly instead of calling commitStreamSetObject.
            if (object_id >= self.next_object_id) {
                self.next_object_id = object_id + 1;
            }
            var range_list = std.ArrayList(StreamOffsetRange).init(self.allocator);
            for (ranges_buf[0..range_count]) |r| {
                try range_list.append(r);
            }

            const key_copy = try self.allocator.dupe(u8, s3_key);

            try self.stream_set_objects.put(object_id, .{
                .object_id = object_id,
                .node_id = node_id,
                .order_id = order_id,
                .data_time_ms = data_time_ms,
                .object_size = object_size,
                .s3_key = key_copy,
                .stream_ranges = range_list,
            });

            // Rebuild secondary SSO index
            for (ranges_buf[0..range_count]) |r| {
                var gop = try self.stream_sso_index.getOrPut(r.stream_id);
                if (!gop.found_existing) {
                    gop.value_ptr.* = std.ArrayList(u64).init(self.allocator);
                }
                try gop.value_ptr.append(object_id);
            }
        }

        // --- Orphaned Keys ---
        const orphan_count = readU32(data, &pos) orelse return error.CorruptSnapshot;
        var orphan_list = try self.allocator.alloc([]u8, orphan_count);
        var loaded: u32 = 0;
        errdefer {
            // On error, free any already-allocated orphan keys
            var k: u32 = 0;
            while (k < loaded) : (k += 1) {
                self.allocator.free(orphan_list[k]);
            }
            self.allocator.free(orphan_list);
        }
        while (loaded < orphan_count) : (loaded += 1) {
            const key_len = readU16(data, &pos) orelse return error.CorruptSnapshot;
            if (pos + key_len > data.len) return error.CorruptSnapshot;
            orphan_list[loaded] = try self.allocator.dupe(u8, data[pos .. pos + key_len]);
            pos += key_len;
        }

        // Verify we consumed exactly all bytes (detect trailing garbage)
        if (pos != data.len) return error.CorruptSnapshot;

        log.info("Snapshot loaded: {d} streams, {d} SOs, {d} SSOs, {d} orphaned keys", .{
            stream_count, so_count, sso_count, orphan_count,
        });

        return orphan_list;
    }

    // ---- Binary encoding helpers (little-endian) ----

    fn writeU64(buf: []u8, pos: *usize, value: u64) void {
        @as(*align(1) u64, @ptrCast(buf.ptr + pos.*)).* = std.mem.nativeToLittle(u64, value);
        pos.* += 8;
    }

    fn writeI64(buf: []u8, pos: *usize, value: i64) void {
        @as(*align(1) i64, @ptrCast(buf.ptr + pos.*)).* = std.mem.nativeToLittle(i64, value);
        pos.* += 8;
    }

    fn writeU32(buf: []u8, pos: *usize, value: u32) void {
        @as(*align(1) u32, @ptrCast(buf.ptr + pos.*)).* = std.mem.nativeToLittle(u32, value);
        pos.* += 4;
    }

    fn writeI32(buf: []u8, pos: *usize, value: i32) void {
        @as(*align(1) i32, @ptrCast(buf.ptr + pos.*)).* = std.mem.nativeToLittle(i32, value);
        pos.* += 4;
    }

    fn writeU16(buf: []u8, pos: *usize, value: u16) void {
        @as(*align(1) u16, @ptrCast(buf.ptr + pos.*)).* = std.mem.nativeToLittle(u16, value);
        pos.* += 2;
    }

    fn readU64(data: []const u8, pos: *usize) ?u64 {
        if (pos.* + 8 > data.len) return null;
        const value = std.mem.littleToNative(u64, @as(*align(1) const u64, @ptrCast(data.ptr + pos.*)).*);
        pos.* += 8;
        return value;
    }

    fn readI64(data: []const u8, pos: *usize) ?i64 {
        if (pos.* + 8 > data.len) return null;
        const value = std.mem.littleToNative(i64, @as(*align(1) const i64, @ptrCast(data.ptr + pos.*)).*);
        pos.* += 8;
        return value;
    }

    fn readU32(data: []const u8, pos: *usize) ?u32 {
        if (pos.* + 4 > data.len) return null;
        const value = std.mem.littleToNative(u32, @as(*align(1) const u32, @ptrCast(data.ptr + pos.*)).*);
        pos.* += 4;
        return value;
    }

    fn readI32(data: []const u8, pos: *usize) ?i32 {
        if (pos.* + 4 > data.len) return null;
        const value = std.mem.littleToNative(i32, @as(*align(1) const i32, @ptrCast(data.ptr + pos.*)).*);
        pos.* += 4;
        return value;
    }

    fn readU16(data: []const u8, pos: *usize) ?u16 {
        if (pos.* + 2 > data.len) return null;
        const value = std.mem.littleToNative(u16, @as(*align(1) const u16, @ptrCast(data.ptr + pos.*)).*);
        pos.* += 2;
        return value;
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

// ---------------------------------------------------------------
// Snapshot tests
// ---------------------------------------------------------------

test "ObjectManager snapshot roundtrip — empty state" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Take a snapshot with no data and no orphaned keys
    const empty_orphans = [_][]const u8{};
    const snap = try om.takeSnapshot(&empty_orphans);
    defer testing.allocator.free(snap);

    // Verify version byte is present
    try testing.expect(snap.len >= 1);
    try testing.expectEqual(@as(u8, 1), snap[0]);

    // Load into a fresh ObjectManager
    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();

    const orphans = try om2.loadSnapshot(snap);
    defer testing.allocator.free(orphans);

    try testing.expectEqual(@as(usize, 0), om2.streamCount());
    try testing.expectEqual(@as(usize, 0), om2.getStreamObjectCount());
    try testing.expectEqual(@as(usize, 0), om2.getStreamSetObjectCount());
    try testing.expectEqual(@as(usize, 0), orphans.len);
}

test "ObjectManager snapshot roundtrip — streams with ranges" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create streams with various states
    const s1 = try om.createStreamWithId(10, 1);
    s1.advanceEndOffset(500);
    s1.trim(50);
    try s1.transferOwnership(2); // bumps epoch to 2, new range

    const s2 = try om.createStreamWithId(20, 3);
    s2.advanceEndOffset(200);
    s2.close(); // state = closed

    // Take snapshot
    const empty_orphans = [_][]const u8{};
    const snap = try om.takeSnapshot(&empty_orphans);
    defer testing.allocator.free(snap);

    // Load into fresh ObjectManager
    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();

    const orphans = try om2.loadSnapshot(snap);
    defer testing.allocator.free(orphans);

    try testing.expectEqual(@as(usize, 2), om2.streamCount());

    // Verify stream 10
    const rs1 = om2.getStream(10).?;
    try testing.expectEqual(@as(u64, 10), rs1.stream_id);
    try testing.expectEqual(@as(u64, 2), rs1.epoch);
    try testing.expectEqual(@as(u64, 50), rs1.start_offset);
    try testing.expectEqual(@as(u64, 500), rs1.end_offset);
    try testing.expectEqual(StreamState.opened, rs1.state);
    try testing.expectEqual(@as(i32, 2), rs1.node_id);
    // Should have 2 ranges: initial + ownership transfer
    try testing.expectEqual(@as(usize, 2), rs1.ranges.items.len);
    try testing.expectEqual(@as(u64, 1), rs1.ranges.items[0].epoch);
    try testing.expectEqual(@as(u64, 2), rs1.ranges.items[1].epoch);
    try testing.expectEqual(@as(i32, 2), rs1.ranges.items[1].node_id);
    try testing.expectEqual(@as(u64, 500), rs1.ranges.items[1].start_offset);

    // Verify stream 20
    const rs2 = om2.getStream(20).?;
    try testing.expectEqual(@as(u64, 20), rs2.stream_id);
    try testing.expectEqual(StreamState.closed, rs2.state);
    try testing.expectEqual(@as(i32, 3), rs2.node_id);
    try testing.expectEqual(@as(u64, 200), rs2.end_offset);
}

test "ObjectManager snapshot roundtrip — stream objects and stream set objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Register StreamObjects
    try om.commitStreamObject(100, 1, 0, 500, "so/1/0-100", 4096);
    try om.commitStreamObject(101, 1, 500, 1000, "so/1/500-101", 8192);
    try om.commitStreamObject(102, 2, 0, 300, "so/2/0-102", 2048);

    // Register StreamSetObjects
    const sso_ranges1 = [_]StreamOffsetRange{
        .{ .stream_id = 3, .start_offset = 0, .end_offset = 100 },
        .{ .stream_id = 4, .start_offset = 0, .end_offset = 50 },
    };
    try om.commitStreamSetObject(200, 0, 1, &sso_ranges1, "sso/200", 16384);

    const sso_ranges2 = [_]StreamOffsetRange{
        .{ .stream_id = 3, .start_offset = 100, .end_offset = 200 },
    };
    try om.commitStreamSetObject(201, 0, 2, &sso_ranges2, "sso/201", 4096);

    // Record next_object_id before snapshot
    const orig_next_id = om.next_object_id;
    const orig_next_order = om.next_order_id;

    // Take snapshot
    const empty_orphans = [_][]const u8{};
    const snap = try om.takeSnapshot(&empty_orphans);
    defer testing.allocator.free(snap);

    // Load into fresh ObjectManager
    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();

    const orphans = try om2.loadSnapshot(snap);
    defer testing.allocator.free(orphans);

    // Verify counts
    try testing.expectEqual(@as(usize, 3), om2.getStreamObjectCount());
    try testing.expectEqual(@as(usize, 2), om2.getStreamSetObjectCount());

    // Verify next_object_id and next_order_id preserved
    try testing.expectEqual(orig_next_id, om2.next_object_id);
    try testing.expectEqual(orig_next_order, om2.next_order_id);

    // Verify StreamObject data via query
    const results1 = try om2.getObjects(1, 0, 2000, 10);
    defer testing.allocator.free(results1);
    try testing.expectEqual(@as(usize, 2), results1.len);
    try testing.expectEqual(@as(u64, 0), results1[0].start_offset);
    try testing.expectEqual(@as(u64, 500), results1[0].end_offset);
    try testing.expectEqual(@as(u64, 4096), results1[0].object_size);
    try testing.expectEqual(S3ObjectType.stream, results1[0].object_type);
    try testing.expectEqual(@as(u64, 500), results1[1].start_offset);
    try testing.expectEqual(@as(u64, 1000), results1[1].end_offset);

    // Verify secondary index rebuilt correctly (sorted order)
    const results2 = try om2.getObjects(2, 0, 500, 10);
    defer testing.allocator.free(results2);
    try testing.expectEqual(@as(usize, 1), results2.len);
    try testing.expectEqual(@as(u64, 0), results2[0].start_offset);
    try testing.expectEqual(@as(u64, 300), results2[0].end_offset);

    // Verify SSO data via query (stream 3 should have data from both SSOs)
    const results3 = try om2.getObjects(3, 0, 500, 10);
    defer testing.allocator.free(results3);
    try testing.expectEqual(@as(usize, 2), results3.len);
    try testing.expectEqual(S3ObjectType.stream_set, results3[0].object_type);
    try testing.expectEqual(@as(u64, 0), results3[0].start_offset);
    try testing.expectEqual(@as(u64, 100), results3[0].end_offset);

    // Verify SSO stream 4 data
    const results4 = try om2.getObjects(4, 0, 500, 10);
    defer testing.allocator.free(results4);
    try testing.expectEqual(@as(usize, 1), results4.len);
    try testing.expectEqual(@as(u64, 0), results4[0].start_offset);
    try testing.expectEqual(@as(u64, 50), results4[0].end_offset);

    // Verify s3_key strings are preserved
    const so_100 = om2.stream_objects.get(100).?;
    try testing.expectEqualStrings("so/1/0-100", so_100.s3_key);
    const sso_200 = om2.stream_set_objects.get(200).?;
    try testing.expectEqualStrings("sso/200", sso_200.s3_key);
}

test "ObjectManager snapshot roundtrip — orphaned keys persisted" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create some state so the snapshot isn't empty
    _ = try om.createStreamWithId(1, 0);

    // Simulate orphaned keys from failed S3 deletes
    const orphan_keys = [_][]const u8{
        "orphan/sso/42",
        "orphan/so/7/0-100",
        "orphan/sso/99",
    };
    const snap = try om.takeSnapshot(&orphan_keys);
    defer testing.allocator.free(snap);

    // Load into fresh ObjectManager
    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();

    const loaded_orphans = try om2.loadSnapshot(snap);
    defer {
        for (loaded_orphans) |key| {
            testing.allocator.free(key);
        }
        testing.allocator.free(loaded_orphans);
    }

    // Verify all orphaned keys survived the roundtrip
    try testing.expectEqual(@as(usize, 3), loaded_orphans.len);
    try testing.expectEqualStrings("orphan/sso/42", loaded_orphans[0]);
    try testing.expectEqualStrings("orphan/so/7/0-100", loaded_orphans[1]);
    try testing.expectEqualStrings("orphan/sso/99", loaded_orphans[2]);
}

test "ObjectManager snapshot — corrupt data detected" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Empty buffer
    try testing.expectError(error.CorruptSnapshot, om.loadSnapshot(""));

    // Just a version byte, missing everything else
    try testing.expectError(error.CorruptSnapshot, om.loadSnapshot(&[_]u8{1}));

    // Wrong version
    try testing.expectError(error.UnsupportedSnapshotVersion, om.loadSnapshot(&[_]u8{99}));

    // Version 0 is also unsupported
    try testing.expectError(error.UnsupportedSnapshotVersion, om.loadSnapshot(&[_]u8{0}));

    // Valid header but truncated after next_object_id (missing next_order_id + rest)
    var truncated: [9]u8 = undefined;
    truncated[0] = 1; // version
    @as(*align(1) u64, @ptrCast(truncated[1..9].ptr)).* = std.mem.nativeToLittle(u64, 42);
    try testing.expectError(error.CorruptSnapshot, om.loadSnapshot(&truncated));
}

test "ObjectManager snapshot — trailing garbage detected" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create a valid empty snapshot
    const empty_orphans = [_][]const u8{};
    const snap = try om.takeSnapshot(&empty_orphans);
    defer testing.allocator.free(snap);

    // Append garbage bytes
    var corrupted = try testing.allocator.alloc(u8, snap.len + 3);
    defer testing.allocator.free(corrupted);
    @memcpy(corrupted[0..snap.len], snap);
    corrupted[snap.len] = 0xFF;
    corrupted[snap.len + 1] = 0xAB;
    corrupted[snap.len + 2] = 0xCD;

    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();
    try testing.expectError(error.CorruptSnapshot, om2.loadSnapshot(corrupted));
}

test "ObjectManager snapshot roundtrip — full state with streams, objects, and orphans" {
    // Integration test: populate ObjectManager with realistic state, snapshot, restore,
    // verify the restored ObjectManager answers queries identically.
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create streams
    const s1 = try om.createStreamWithId(1, 0);
    s1.advanceEndOffset(1000);
    s1.trim(100);

    const s2 = try om.createStreamWithId(2, 1);
    s2.advanceEndOffset(500);
    try s2.transferOwnership(2);
    s2.advanceEndOffset(750);

    // Add SOs
    try om.commitStreamObject(10, 1, 100, 500, "so/1/100-10", 4096);
    try om.commitStreamObject(11, 1, 500, 1000, "so/1/500-11", 8192);
    try om.commitStreamObject(12, 2, 0, 300, "so/2/0-12", 2048);

    // Add SSOs
    const sso_ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 100 },
        .{ .stream_id = 2, .start_offset = 300, .end_offset = 500 },
    };
    try om.commitStreamSetObject(50, 0, 1, &sso_ranges, "sso/50", 32768);

    // Orphaned keys
    const orphan_keys = [_][]const u8{ "dead/obj/1", "dead/obj/2" };

    // Take snapshot
    const snap = try om.takeSnapshot(&orphan_keys);
    defer testing.allocator.free(snap);

    // Query the original ObjectManager for reference values
    const orig_s1_objs = try om.getObjects(1, 0, 2000, 10);
    defer testing.allocator.free(orig_s1_objs);
    const orig_s2_objs = try om.getObjects(2, 0, 2000, 10);
    defer testing.allocator.free(orig_s2_objs);

    // Load into fresh ObjectManager
    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();

    const loaded_orphans = try om2.loadSnapshot(snap);
    defer {
        for (loaded_orphans) |key| testing.allocator.free(key);
        testing.allocator.free(loaded_orphans);
    }

    // Verify stream state
    try testing.expectEqual(@as(usize, 2), om2.streamCount());
    const rs1 = om2.getStream(1).?;
    try testing.expectEqual(@as(u64, 100), rs1.start_offset);
    try testing.expectEqual(@as(u64, 1000), rs1.end_offset);

    const rs2 = om2.getStream(2).?;
    try testing.expectEqual(@as(u64, 2), rs2.epoch);
    try testing.expectEqual(@as(i32, 2), rs2.node_id);
    try testing.expectEqual(@as(u64, 750), rs2.end_offset);

    // Verify object queries match original
    const new_s1_objs = try om2.getObjects(1, 0, 2000, 10);
    defer testing.allocator.free(new_s1_objs);
    try testing.expectEqual(orig_s1_objs.len, new_s1_objs.len);
    for (orig_s1_objs, new_s1_objs) |orig, new| {
        try testing.expectEqual(orig.object_id, new.object_id);
        try testing.expectEqual(orig.start_offset, new.start_offset);
        try testing.expectEqual(orig.end_offset, new.end_offset);
        try testing.expectEqual(orig.object_type, new.object_type);
        try testing.expectEqual(orig.object_size, new.object_size);
    }

    const new_s2_objs = try om2.getObjects(2, 0, 2000, 10);
    defer testing.allocator.free(new_s2_objs);
    try testing.expectEqual(orig_s2_objs.len, new_s2_objs.len);

    // Verify orphaned keys
    try testing.expectEqual(@as(usize, 2), loaded_orphans.len);
    try testing.expectEqualStrings("dead/obj/1", loaded_orphans[0]);
    try testing.expectEqualStrings("dead/obj/2", loaded_orphans[1]);

    // Verify next_object_id/next_order_id preserved — new allocations shouldn't conflict
    const new_id = om2.allocateObjectId();
    try testing.expect(new_id >= 51); // Should be beyond any ID we committed
}

test "ObjectManager snapshot — version header present" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const empty_orphans = [_][]const u8{};
    const snap = try om.takeSnapshot(&empty_orphans);
    defer testing.allocator.free(snap);

    // First byte must be the version
    try testing.expectEqual(@as(u8, 1), snap[0]);

    // Minimum size: version(1) + next_object_id(8) + next_order_id(8) +
    // stream_count(4) + so_count(4) + sso_count(4) + orphan_count(4) = 33
    try testing.expectEqual(@as(usize, 33), snap.len);
}

test "ObjectManager snapshot — SSO data_time_ms preserved" {
    // Verify that data_time_ms (a field not set by commitStreamSetObject's
    // caller) survives the roundtrip. commitStreamSetObject uses
    // std.time.milliTimestamp() so we register directly to control the value.
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const sso_ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 100 },
    };
    try om.commitStreamSetObject(5, 0, 1, &sso_ranges, "sso/5", 1024);

    // Record the data_time_ms that was set
    const orig_time = om.stream_set_objects.get(5).?.data_time_ms;

    const empty_orphans = [_][]const u8{};
    const snap = try om.takeSnapshot(&empty_orphans);
    defer testing.allocator.free(snap);

    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();

    const orphans = try om2.loadSnapshot(snap);
    defer testing.allocator.free(orphans);

    const restored_sso = om2.stream_set_objects.get(5).?;
    try testing.expectEqual(orig_time, restored_sso.data_time_ms);
    try testing.expectEqual(@as(u64, 1024), restored_sso.object_size);
    try testing.expectEqual(@as(i32, 0), restored_sso.node_id);
    try testing.expectEqual(@as(u64, 1), restored_sso.order_id);
}

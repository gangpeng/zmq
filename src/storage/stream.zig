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

/// S3 object lifecycle states matching AutoMQ's S3ObjectControlManager.
///
/// NOTE: AutoMQ uses a full S3ObjectControlManager with HashedWheelTimer
/// for TTL tracking. ZMQ integrates state tracking directly into the
/// ObjectManager for simplicity in the single-threaded event loop model.
/// The DESTROYED state is not represented here because destroyed objects
/// are removed from metadata entirely.
pub const S3ObjectState = enum(u8) {
    /// Allocated but not yet written to S3. Auto-expires after prepared_ttl_ms.
    prepared = 0,
    /// Written to S3 and metadata committed. Normal live state.
    committed = 1,
    /// Scheduled for deletion. Retains for mark_destroyed_retention_ms
    /// so consumers can finish reading before physical delete.
    mark_destroyed = 2,
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
    /// Lifecycle state: prepared → committed → mark_destroyed → (removed).
    state: S3ObjectState = .committed,
    /// Timestamp (ms) when state last changed. Used for TTL/retention checks.
    state_changed_ms: i64 = 0,

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
    /// Maximum record timestamp (milliseconds since epoch) in this object.
    /// Used by time-based retention to find the trim point without parsing
    /// raw RecordBatch data. Populated from RecordBatch.max_timestamp during
    /// force-split or from StreamSetObject.data_time_ms.
    ///
    /// NOTE: AutoMQ tracks this in the Stream layer (ElasticLog) and uses it
    /// to drive ListOffsets-by-timestamp and time-based retention. ZMQ stores
    /// it per-object for simpler retention scanning.
    max_timestamp_ms: i64 = 0,
    /// Lifecycle state: prepared → committed → mark_destroyed → (removed).
    state: S3ObjectState = .committed,
    /// Timestamp (ms) when state last changed. Used for TTL/retention checks.
    state_changed_ms: i64 = 0,

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

    /// Compute total bytes of all StreamObjects for a given stream.
    /// Used by size-based retention to check if a partition exceeds retention_bytes.
    pub fn getStreamTotalBytes(self: *const ObjectManager, stream_id: u64) u64 {
        const obj_ids = self.stream_object_index.get(stream_id) orelse return 0;
        var total: u64 = 0;
        for (obj_ids.items) |obj_id| {
            if (self.stream_objects.get(obj_id)) |so| {
                total += so.object_size;
            }
        }
        return total;
    }

    /// Find the trim offset for time-based retention.
    /// Returns the end_offset of the last StreamObject whose max_timestamp_ms
    /// is entirely before `cutoff_ms`, or null if no objects qualify.
    ///
    /// After trimming to this offset, all objects with timestamps before the
    /// retention cutoff will be eligible for cleanup by cleanupExpired().
    pub fn findTrimOffsetByTimestamp(self: *const ObjectManager, stream_id: u64, cutoff_ms: i64) ?u64 {
        const obj_ids = self.stream_object_index.get(stream_id) orelse return null;
        var trim_offset: ?u64 = null;

        // Objects in stream_object_index are sorted by start_offset (ascending).
        // Walk from oldest to newest, collecting objects that are entirely expired.
        for (obj_ids.items) |obj_id| {
            const so = self.stream_objects.get(obj_id) orelse continue;
            if (so.max_timestamp_ms > 0 and so.max_timestamp_ms < cutoff_ms) {
                trim_offset = so.end_offset;
            } else {
                // Once we hit an object that is NOT expired, stop — all
                // subsequent objects are newer (sorted by start_offset).
                break;
            }
        }

        return trim_offset;
    }

    /// Find the trim offset for size-based retention.
    /// Returns the end_offset to trim to so that the total remaining bytes
    /// are at most `max_bytes`, or null if already within the limit.
    ///
    /// Walks from the oldest objects and accumulates bytes to drop until the
    /// remaining total fits within the budget.
    pub fn findTrimOffsetBySize(self: *const ObjectManager, stream_id: u64, max_bytes: u64) ?u64 {
        const obj_ids = self.stream_object_index.get(stream_id) orelse return null;
        const total = self.getStreamTotalBytes(stream_id);
        if (total <= max_bytes) return null;

        var excess = total - max_bytes;
        var trim_offset: ?u64 = null;

        for (obj_ids.items) |obj_id| {
            const so = self.stream_objects.get(obj_id) orelse continue;
            trim_offset = so.end_offset;
            if (so.object_size >= excess) break;
            excess -= so.object_size;
        }

        return trim_offset;
    }

    // ---- Object ID allocation ----

    pub fn allocateObjectId(self: *ObjectManager) u64 {
        const id = self.next_object_id;
        self.next_object_id += 1;
        return id;
    }

    // ---- S3 Object Lifecycle ----

    /// Allocate a prepared S3 object ID. The object must be committed within
    /// prepared_ttl_ms (default 60 min) or it will be auto-expired.
    /// The caller should later call commitStreamObject/commitStreamSetObject
    /// to transition the object to committed state.
    pub fn prepareObject(self: *ObjectManager) u64 {
        return self.allocateObjectId();
    }

    /// Mark a committed object for destruction. It will be physically deleted
    /// after mark_destroyed_retention_ms (default 10 min) to give consumers
    /// time to finish reading in-flight data.
    ///
    /// NOTE: AutoMQ's S3ObjectControlManager uses a HashedWheelTimer to track
    /// destruction delay. ZMQ checks timestamps during compaction cycles instead,
    /// trading sub-second precision for implementation simplicity (compaction
    /// runs every 5 minutes, so the effective delay is retention_ms ± 5 min).
    pub fn markDestroyed(self: *ObjectManager, object_id: u64) void {
        self.markDestroyedAt(object_id, std.time.milliTimestamp());
    }

    /// Mark a committed object for destruction with an explicit timestamp.
    /// Separated from markDestroyed() for testability — tests can inject
    /// controlled timestamps without depending on wall-clock time.
    pub fn markDestroyedAt(self: *ObjectManager, object_id: u64, now_ms: i64) void {
        if (self.stream_objects.getPtr(object_id)) |so| {
            if (so.state != .committed) {
                log.warn("markDestroyed on SO {d} in state {}, expected committed", .{ object_id, @intFromEnum(so.state) });
                return;
            }
            so.state = .mark_destroyed;
            so.state_changed_ms = now_ms;
            log.debug("SO {d} marked for destruction (stream={d}, offsets=[{d}..{d}))", .{
                object_id, so.stream_id, so.start_offset, so.end_offset,
            });
            return;
        }
        if (self.stream_set_objects.getPtr(object_id)) |sso| {
            if (sso.state != .committed) {
                log.warn("markDestroyed on SSO {d} in state {}, expected committed", .{ object_id, @intFromEnum(sso.state) });
                return;
            }
            sso.state = .mark_destroyed;
            sso.state_changed_ms = now_ms;
            log.debug("SSO {d} marked for destruction", .{object_id});
            return;
        }
        log.warn("markDestroyed: object {d} not found", .{object_id});
    }

    /// Register a prepared StreamObject that has not yet been committed.
    /// The object starts in .prepared state with a timestamp for TTL tracking.
    /// The caller must later commit the object by calling commitPreparedStreamObject().
    pub fn registerPreparedStreamObject(
        self: *ObjectManager,
        object_id: u64,
        stream_id: u64,
        s3_key: []const u8,
    ) !void {
        return self.registerPreparedStreamObjectAt(object_id, stream_id, s3_key, std.time.milliTimestamp());
    }

    /// Register a prepared StreamObject with an explicit timestamp for testability.
    pub fn registerPreparedStreamObjectAt(
        self: *ObjectManager,
        object_id: u64,
        stream_id: u64,
        s3_key: []const u8,
        now_ms: i64,
    ) !void {
        if (object_id >= self.next_object_id) {
            self.next_object_id = object_id + 1;
        }
        const key_copy = try self.allocator.dupe(u8, s3_key);

        try self.stream_objects.put(object_id, .{
            .object_id = object_id,
            .stream_id = stream_id,
            .start_offset = 0,
            .end_offset = 0,
            .object_size = 0,
            .s3_key = key_copy,
            .state = .prepared,
            .state_changed_ms = now_ms,
        });
        // Prepared objects are not added to stream_object_index because they
        // have no valid offset range yet and should not appear in fetch queries.
    }

    /// Expire prepared objects that have exceeded their TTL.
    /// Returns the count of expired objects.
    ///
    /// Prepared objects are allocated with prepareObject()/registerPreparedStreamObject()
    /// but never committed (e.g., the producer crashed between allocation and S3 upload).
    /// This prevents leaked object IDs from accumulating indefinitely.
    pub fn expirePreparedObjects(self: *ObjectManager, prepared_ttl_ms: i64) u64 {
        return self.expirePreparedObjectsAt(prepared_ttl_ms, std.time.milliTimestamp());
    }

    /// Expire prepared objects with an explicit "now" timestamp for testability.
    pub fn expirePreparedObjectsAt(self: *ObjectManager, prepared_ttl_ms: i64, now_ms: i64) u64 {
        var expired_count: u64 = 0;

        // Collect expired SO IDs (can't modify map while iterating)
        var expired_so_ids = std.ArrayList(u64).init(self.allocator);
        defer expired_so_ids.deinit();
        {
            var it = self.stream_objects.iterator();
            while (it.next()) |entry| {
                const so = entry.value_ptr;
                if (so.state == .prepared and so.state_changed_ms > 0 and
                    now_ms - so.state_changed_ms >= prepared_ttl_ms)
                {
                    expired_so_ids.append(so.object_id) catch continue;
                }
            }
        }
        for (expired_so_ids.items) |obj_id| {
            log.info("Expiring prepared SO {d} (exceeded TTL of {d}ms)", .{ obj_id, prepared_ttl_ms });
            self.removeStreamObject(obj_id);
            expired_count += 1;
        }

        // Collect expired SSO IDs
        var expired_sso_ids = std.ArrayList(u64).init(self.allocator);
        defer expired_sso_ids.deinit();
        {
            var it = self.stream_set_objects.iterator();
            while (it.next()) |entry| {
                const sso = entry.value_ptr;
                if (sso.state == .prepared and sso.state_changed_ms > 0 and
                    now_ms - sso.state_changed_ms >= prepared_ttl_ms)
                {
                    expired_sso_ids.append(sso.object_id) catch continue;
                }
            }
        }
        for (expired_sso_ids.items) |obj_id| {
            log.info("Expiring prepared SSO {d} (exceeded TTL of {d}ms)", .{ obj_id, prepared_ttl_ms });
            self.removeStreamSetObject(obj_id);
            expired_count += 1;
        }

        return expired_count;
    }

    /// Find mark_destroyed objects ready for physical deletion (retention period elapsed).
    /// Returns list of s3_keys to delete and removes those objects from metadata.
    /// Caller owns the returned slice and each key within it.
    ///
    /// Write-before-delete: objects are removed from metadata (this function) BEFORE
    /// the caller deletes them from S3. If the process crashes after metadata removal
    /// but before S3 delete, the S3 objects become orphans (cleaned up next cycle).
    pub fn collectDestroyedObjects(self: *ObjectManager, retention_ms: i64, allocator: Allocator) ![][]u8 {
        return self.collectDestroyedObjectsAt(retention_ms, allocator, std.time.milliTimestamp());
    }

    /// Collect destroyed objects with an explicit "now" timestamp for testability.
    pub fn collectDestroyedObjectsAt(self: *ObjectManager, retention_ms: i64, allocator: Allocator, now_ms: i64) ![][]u8 {
        var keys = std.ArrayList([]u8).init(allocator);
        errdefer {
            for (keys.items) |k| allocator.free(k);
            keys.deinit();
        }

        // Collect SO IDs ready for destruction
        var ready_so_ids = std.ArrayList(u64).init(self.allocator);
        defer ready_so_ids.deinit();
        {
            var it = self.stream_objects.iterator();
            while (it.next()) |entry| {
                const so = entry.value_ptr;
                if (so.state == .mark_destroyed and so.state_changed_ms > 0 and
                    now_ms - so.state_changed_ms >= retention_ms)
                {
                    ready_so_ids.append(so.object_id) catch continue;
                }
            }
        }
        for (ready_so_ids.items) |obj_id| {
            if (self.stream_objects.get(obj_id)) |so| {
                const key_copy = try allocator.dupe(u8, so.s3_key);
                try keys.append(key_copy);
                log.info("Collecting destroyed SO {d} for physical deletion (key='{s}')", .{ obj_id, so.s3_key });
            }
            self.removeStreamObject(obj_id);
        }

        // Collect SSO IDs ready for destruction
        var ready_sso_ids = std.ArrayList(u64).init(self.allocator);
        defer ready_sso_ids.deinit();
        {
            var it = self.stream_set_objects.iterator();
            while (it.next()) |entry| {
                const sso = entry.value_ptr;
                if (sso.state == .mark_destroyed and sso.state_changed_ms > 0 and
                    now_ms - sso.state_changed_ms >= retention_ms)
                {
                    ready_sso_ids.append(sso.object_id) catch continue;
                }
            }
        }
        for (ready_sso_ids.items) |obj_id| {
            if (self.stream_set_objects.get(obj_id)) |sso| {
                const key_copy = try allocator.dupe(u8, sso.s3_key);
                try keys.append(key_copy);
                log.info("Collecting destroyed SSO {d} for physical deletion (key='{s}')", .{ obj_id, sso.s3_key });
            }
            self.removeStreamSetObject(obj_id);
        }

        return try keys.toOwnedSlice();
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
        return self.commitStreamObjectWithTimestamp(object_id, stream_id, start_offset, end_offset, s3_key, object_size, 0);
    }

    /// Register a StreamObject with an explicit max timestamp.
    /// The timestamp is used by time-based retention to find objects older than
    /// the retention period without parsing raw RecordBatch data.
    pub fn commitStreamObjectWithTimestamp(
        self: *ObjectManager,
        object_id: u64,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        s3_key: []const u8,
        object_size: u64,
        max_timestamp_ms: i64,
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
            .max_timestamp_ms = max_timestamp_ms,
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
    /// Only returns objects in .committed state — prepared and mark_destroyed
    /// objects are excluded from fetch results.
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

        // 1. Collect committed StreamObjects that overlap [start_offset, end_offset)
        if (self.stream_object_index.get(stream_id)) |so_ids| {
            for (so_ids.items) |obj_id| {
                if (results.items.len >= limit) break;
                const so = self.stream_objects.get(obj_id) orelse continue;
                if (so.state != .committed) continue;
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

        // 2. Collect committed StreamSetObjects with ranges for this stream overlapping
        if (self.stream_sso_index.get(stream_id)) |sso_ids| {
            for (sso_ids.items) |obj_id| {
                if (results.items.len >= limit) break;
                const sso = self.stream_set_objects.get(obj_id) orelse continue;
                if (sso.state != .committed) continue;
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

    /// Check if a committed StreamObject already exists that covers the exact offset range.
    /// Used for idempotent compaction — skip duplicate SOs.
    /// Only checks committed objects: if a previous SO was mark_destroyed, we should
    /// re-create it from the SSO data.
    pub fn hasStreamObjectCovering(self: *const ObjectManager, stream_id: u64, start_offset: u64, end_offset: u64) bool {
        const so_ids = self.stream_object_index.get(stream_id) orelse return false;
        for (so_ids.items) |obj_id| {
            const so = self.stream_objects.get(obj_id) orelse continue;
            if (so.state != .committed) continue;
            if (so.start_offset == start_offset and so.end_offset == end_offset) return true;
        }
        return false;
    }

    // ---- Snapshot persistence ----

    /// Snapshot format version. Bumped when the binary layout changes.
    /// Forward compatibility: loadSnapshot rejects unknown versions.
    /// v1: initial format
    /// v2: added S3ObjectState (u8) and state_changed_ms (i64) to SO and SSO
    const SNAPSHOT_VERSION: u8 = 2;

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
    ///     [8 bytes] max_timestamp_ms (i64)
    ///     [1 byte]  s3_object_state (u8: 0=prepared, 1=committed, 2=mark_destroyed) [v2+]
    ///     [8 bytes] state_changed_ms (i64) [v2+]
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
    ///     [1 byte]  s3_object_state (u8: 0=prepared, 1=committed, 2=mark_destroyed) [v2+]
    ///     [8 bytes] state_changed_ms (i64) [v2+]
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
            size += 8 + 8 + 8 + 8 + 8 + 8; // fixed fields (incl. max_timestamp_ms)
            size += 1 + 8; // state (u8) + state_changed_ms (i64)
            size += 2 + so.s3_key.len; // key_len + key
        }

        // StreamSetObjects
        size += 4; // sso_count
        var sso_it = self.stream_set_objects.iterator();
        while (sso_it.next()) |entry| {
            const sso = entry.value_ptr;
            size += 8 + 4 + 8 + 8 + 8; // fixed fields
            size += 1 + 8; // state (u8) + state_changed_ms (i64)
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
            writeI64(buf, &pos, so.max_timestamp_ms);
            buf[pos] = @intFromEnum(so.state);
            pos += 1;
            writeI64(buf, &pos, so.state_changed_ms);
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
            buf[pos] = @intFromEnum(sso.state);
            pos += 1;
            writeI64(buf, &pos, sso.state_changed_ms);
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

        // Version check — accept v1 (legacy) and v2 (with lifecycle state)
        const version = data[pos];
        pos += 1;
        if (version != 1 and version != 2) return error.UnsupportedSnapshotVersion;

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
            const max_timestamp_ms = readI64(data, &pos) orelse return error.CorruptSnapshot;

            // v2 added lifecycle state fields
            var so_state: S3ObjectState = .committed;
            var so_state_changed_ms: i64 = 0;
            if (version >= 2) {
                if (pos >= data.len) return error.CorruptSnapshot;
                const state_byte = data[pos];
                pos += 1;
                if (state_byte > 2) return error.CorruptSnapshot;
                so_state = @enumFromInt(state_byte);
                so_state_changed_ms = readI64(data, &pos) orelse return error.CorruptSnapshot;
            }

            const key_len = readU16(data, &pos) orelse return error.CorruptSnapshot;
            if (pos + key_len > data.len) return error.CorruptSnapshot;
            const s3_key = data[pos .. pos + key_len];
            pos += key_len;

            // Use commitStreamObjectWithTimestamp to rebuild both primary store and secondary index
            try self.commitStreamObjectWithTimestamp(object_id, stream_id, start_offset, end_offset, s3_key, object_size, max_timestamp_ms);

            // Restore lifecycle state after commit (commit sets state to .committed by default)
            if (self.stream_objects.getPtr(object_id)) |so_ptr| {
                so_ptr.state = so_state;
                so_ptr.state_changed_ms = so_state_changed_ms;
            }
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

            // v2 added lifecycle state fields
            var sso_state: S3ObjectState = .committed;
            var sso_state_changed_ms: i64 = 0;
            if (version >= 2) {
                if (pos >= data.len) return error.CorruptSnapshot;
                const state_byte = data[pos];
                pos += 1;
                if (state_byte > 2) return error.CorruptSnapshot;
                sso_state = @enumFromInt(state_byte);
                sso_state_changed_ms = readI64(data, &pos) orelse return error.CorruptSnapshot;
            }

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
            // to now. We need to preserve the original data_time_ms and lifecycle state,
            // so we register directly instead of calling commitStreamSetObject.
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
                .state = sso_state,
                .state_changed_ms = sso_state_changed_ms,
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
    try testing.expectEqual(@as(u8, 2), snap[0]);

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

    // Just a version byte (v1 or v2), missing everything else
    try testing.expectError(error.CorruptSnapshot, om.loadSnapshot(&[_]u8{1}));
    try testing.expectError(error.CorruptSnapshot, om.loadSnapshot(&[_]u8{2}));

    // Wrong version
    try testing.expectError(error.UnsupportedSnapshotVersion, om.loadSnapshot(&[_]u8{99}));

    // Version 0 is unsupported
    try testing.expectError(error.UnsupportedSnapshotVersion, om.loadSnapshot(&[_]u8{0}));

    // Version 3 is unsupported
    try testing.expectError(error.UnsupportedSnapshotVersion, om.loadSnapshot(&[_]u8{3}));

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
    try testing.expectEqual(@as(u8, 2), snap[0]);

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

// ---------------------------------------------------------------
// Retention query helper tests
// ---------------------------------------------------------------

test "ObjectManager getStreamTotalBytes sums all StreamObject sizes" {
    var om = ObjectManager.init(testing.allocator, 1);
    defer om.deinit();
    _ = try om.createStream(1);

    try om.commitStreamObject(1, 1, 0, 100, "so/1", 1024);
    try om.commitStreamObject(2, 1, 100, 200, "so/2", 2048);
    try om.commitStreamObject(3, 1, 200, 300, "so/3", 512);

    try testing.expectEqual(@as(u64, 3584), om.getStreamTotalBytes(1));
    // Unknown stream returns 0
    try testing.expectEqual(@as(u64, 0), om.getStreamTotalBytes(999));
}

test "ObjectManager findTrimOffsetByTimestamp finds correct cutoff" {
    var om = ObjectManager.init(testing.allocator, 1);
    defer om.deinit();
    _ = try om.createStream(1);

    // Three objects with ascending timestamps
    try om.commitStreamObjectWithTimestamp(1, 1, 0, 100, "so/1", 1024, 1000); // ts=1000
    try om.commitStreamObjectWithTimestamp(2, 1, 100, 200, "so/2", 1024, 2000); // ts=2000
    try om.commitStreamObjectWithTimestamp(3, 1, 200, 300, "so/3", 1024, 3000); // ts=3000

    // Cutoff at 1500: only object 1 (ts=1000) is expired → trim to 100
    try testing.expectEqual(@as(u64, 100), om.findTrimOffsetByTimestamp(1, 1500).?);

    // Cutoff at 2500: objects 1 and 2 expired → trim to 200
    try testing.expectEqual(@as(u64, 200), om.findTrimOffsetByTimestamp(1, 2500).?);

    // Cutoff at 3500: all expired → trim to 300
    try testing.expectEqual(@as(u64, 300), om.findTrimOffsetByTimestamp(1, 3500).?);

    // Cutoff at 500: nothing expired → null
    try testing.expect(om.findTrimOffsetByTimestamp(1, 500) == null);

    // Unknown stream → null
    try testing.expect(om.findTrimOffsetByTimestamp(999, 5000) == null);
}

test "ObjectManager findTrimOffsetByTimestamp skips objects with zero timestamp" {
    var om = ObjectManager.init(testing.allocator, 1);
    defer om.deinit();
    _ = try om.createStream(1);

    // Object with no timestamp (legacy/unknown) should NOT be trimmed by time
    try om.commitStreamObjectWithTimestamp(1, 1, 0, 100, "so/1", 1024, 0);
    try om.commitStreamObjectWithTimestamp(2, 1, 100, 200, "so/2", 1024, 2000);

    // Cutoff at 5000: object 1 has ts=0, treated as "unknown" → skip → stop
    // Object 1 blocks trimming even though object 2 would qualify
    try testing.expect(om.findTrimOffsetByTimestamp(1, 5000) == null);
}

test "ObjectManager findTrimOffsetBySize trims oldest objects first" {
    var om = ObjectManager.init(testing.allocator, 1);
    defer om.deinit();
    _ = try om.createStream(1);

    try om.commitStreamObject(1, 1, 0, 100, "so/1", 1000);
    try om.commitStreamObject(2, 1, 100, 200, "so/2", 1000);
    try om.commitStreamObject(3, 1, 200, 300, "so/3", 1000);

    // Total = 3000. Max budget = 2000 → need to drop 1000 → trim first object
    try testing.expectEqual(@as(u64, 100), om.findTrimOffsetBySize(1, 2000).?);

    // Max budget = 1000 → need to drop 2000 → trim first two objects
    try testing.expectEqual(@as(u64, 200), om.findTrimOffsetBySize(1, 1000).?);

    // Max budget = 500 → need to drop 2500 → trim all three
    try testing.expectEqual(@as(u64, 300), om.findTrimOffsetBySize(1, 500).?);

    // Max budget = 3000 → nothing to drop
    try testing.expect(om.findTrimOffsetBySize(1, 3000) == null);

    // Max budget = 5000 → nothing to drop (under budget)
    try testing.expect(om.findTrimOffsetBySize(1, 5000) == null);
}

test "ObjectManager StreamObject max_timestamp_ms persists through snapshot" {
    var om = ObjectManager.init(testing.allocator, 1);
    defer om.deinit();
    _ = try om.createStream(1);

    try om.commitStreamObjectWithTimestamp(1, 1, 0, 100, "so/1", 1024, 42000);

    const snap = try om.takeSnapshot(&.{});
    defer testing.allocator.free(snap);

    var om2 = ObjectManager.init(testing.allocator, 1);
    defer om2.deinit();
    const orphans = try om2.loadSnapshot(snap);
    defer testing.allocator.free(orphans);

    const so = om2.stream_objects.get(1).?;
    try testing.expectEqual(@as(i64, 42000), so.max_timestamp_ms);
}

// ---------------------------------------------------------------
// S3 Object Lifecycle tests
// ---------------------------------------------------------------

test "S3ObjectState enum values" {
    try testing.expectEqual(@as(u8, 0), @intFromEnum(S3ObjectState.prepared));
    try testing.expectEqual(@as(u8, 1), @intFromEnum(S3ObjectState.committed));
    try testing.expectEqual(@as(u8, 2), @intFromEnum(S3ObjectState.mark_destroyed));
}

test "ObjectManager markDestroyed transitions SO state" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);

    // Initially committed
    const so = om.stream_objects.get(1).?;
    try testing.expectEqual(S3ObjectState.committed, so.state);
    try testing.expectEqual(@as(i64, 0), so.state_changed_ms);

    // Mark destroyed
    om.markDestroyedAt(1, 5000);

    const so2 = om.stream_objects.get(1).?;
    try testing.expectEqual(S3ObjectState.mark_destroyed, so2.state);
    try testing.expectEqual(@as(i64, 5000), so2.state_changed_ms);
}

test "ObjectManager markDestroyed transitions SSO state" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 1, .start_offset = 0, .end_offset = 50 },
    };
    try om.commitStreamSetObject(10, 0, 1, &ranges, "sso/10", 512);

    // Initially committed
    const sso = om.stream_set_objects.get(10).?;
    try testing.expectEqual(S3ObjectState.committed, sso.state);

    // Mark destroyed
    om.markDestroyedAt(10, 7000);

    const sso2 = om.stream_set_objects.get(10).?;
    try testing.expectEqual(S3ObjectState.mark_destroyed, sso2.state);
    try testing.expectEqual(@as(i64, 7000), sso2.state_changed_ms);
}

test "ObjectManager markDestroyed ignores non-committed objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Register a prepared object
    try om.registerPreparedStreamObjectAt(1, 1, "so/prepared/1", 1000);

    // Attempt to mark it destroyed — should be a no-op (warning logged)
    om.markDestroyedAt(1, 5000);

    const so = om.stream_objects.get(1).?;
    try testing.expectEqual(S3ObjectState.prepared, so.state);
    try testing.expectEqual(@as(i64, 1000), so.state_changed_ms);
}

test "ObjectManager markDestroyed on nonexistent object is no-op" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Should not crash — just logs a warning
    om.markDestroyedAt(999, 5000);
}

test "ObjectManager getObjects excludes mark_destroyed objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);
    try om.commitStreamObject(2, 1, 100, 200, "so/1/100-2", 1024);

    // Mark first object for destruction
    om.markDestroyedAt(1, 5000);

    // Fetch should only return the committed object
    const results = try om.getObjects(1, 0, 300, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 100), results[0].start_offset);
    try testing.expectEqual(@as(u64, 200), results[0].end_offset);
}

test "ObjectManager getObjects excludes prepared objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Register a prepared SO (not yet committed, no valid offsets)
    try om.registerPreparedStreamObjectAt(1, 1, "so/prepared/1", 1000);

    // Commit a real SO
    try om.commitStreamObject(2, 1, 0, 100, "so/1/0-2", 1024);

    // Fetch should only return the committed object
    const results = try om.getObjects(1, 0, 200, 10);
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 2), results[0].object_id);
}

test "ObjectManager collectDestroyedObjects returns keys after retention" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);
    try om.commitStreamObject(2, 1, 100, 200, "so/1/100-2", 2048);

    // Mark both destroyed at time 1000
    om.markDestroyedAt(1, 1000);
    om.markDestroyedAt(2, 1000);

    // At time 5000 with 10000ms retention — not yet ready
    const keys_early = try om.collectDestroyedObjectsAt(10000, testing.allocator, 5000);
    defer testing.allocator.free(keys_early);
    try testing.expectEqual(@as(usize, 0), keys_early.len);

    // Both objects should still exist
    try testing.expectEqual(@as(usize, 2), om.getStreamObjectCount());

    // At time 12000 with 10000ms retention — ready (1000 + 10000 = 11000 < 12000)
    const keys = try om.collectDestroyedObjectsAt(10000, testing.allocator, 12000);
    defer {
        for (keys) |k| testing.allocator.free(k);
        testing.allocator.free(keys);
    }
    try testing.expectEqual(@as(usize, 2), keys.len);

    // Objects should be removed from metadata
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());
}

test "ObjectManager collectDestroyedObjects does not affect committed objects" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);
    try om.commitStreamObject(2, 1, 100, 200, "so/1/100-2", 2048);

    // Only mark the first one destroyed
    om.markDestroyedAt(1, 1000);

    // Collect at a time well past retention
    const keys = try om.collectDestroyedObjectsAt(100, testing.allocator, 50000);
    defer {
        for (keys) |k| testing.allocator.free(k);
        testing.allocator.free(keys);
    }

    // Only 1 key returned (the mark_destroyed one)
    try testing.expectEqual(@as(usize, 1), keys.len);
    try testing.expectEqualStrings("so/1/0-1", keys[0]);

    // The committed one is still there
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    const so = om.stream_objects.get(2).?;
    try testing.expectEqual(S3ObjectState.committed, so.state);
}

test "ObjectManager expirePreparedObjects cleans stale prepared SOs" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Register prepared objects at time 1000
    try om.registerPreparedStreamObjectAt(1, 1, "so/prep/1", 1000);
    try om.registerPreparedStreamObjectAt(2, 2, "so/prep/2", 1000);

    // Also commit a normal object — should not be affected
    try om.commitStreamObject(3, 1, 0, 100, "so/1/0-3", 1024);

    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());

    // At time 2000 with 3600000ms TTL (1 hour) — not yet expired
    const expired_early = om.expirePreparedObjectsAt(3600000, 2000);
    try testing.expectEqual(@as(u64, 0), expired_early);
    try testing.expectEqual(@as(usize, 3), om.getStreamObjectCount());

    // At time 3601001 with 3600000ms TTL — both prepared objects have expired
    const expired = om.expirePreparedObjectsAt(3600000, 3601001);
    try testing.expectEqual(@as(u64, 2), expired);

    // Only the committed object remains
    try testing.expectEqual(@as(usize, 1), om.getStreamObjectCount());
    const so = om.stream_objects.get(3).?;
    try testing.expectEqual(S3ObjectState.committed, so.state);
}

test "ObjectManager prepareObject allocates unique IDs" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    const id1 = om.prepareObject();
    const id2 = om.prepareObject();
    const id3 = om.prepareObject();

    try testing.expect(id1 < id2);
    try testing.expect(id2 < id3);
}

test "ObjectManager lifecycle snapshot roundtrip preserves state" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create a committed SO
    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);

    // Create a mark_destroyed SO
    try om.commitStreamObject(2, 1, 100, 200, "so/1/100-2", 2048);
    om.markDestroyedAt(2, 5000);

    // Create a prepared SO
    try om.registerPreparedStreamObjectAt(3, 2, "so/prep/3", 3000);

    // Create a mark_destroyed SSO
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 3, .start_offset = 0, .end_offset = 50 },
    };
    try om.commitStreamSetObject(10, 0, 1, &ranges, "sso/10", 512);
    om.markDestroyedAt(10, 8000);

    // Snapshot
    const snap = try om.takeSnapshot(&.{});
    defer testing.allocator.free(snap);

    // Load into fresh ObjectManager
    var om2 = ObjectManager.init(testing.allocator, 0);
    defer om2.deinit();
    const orphans = try om2.loadSnapshot(snap);
    defer testing.allocator.free(orphans);

    // Verify SO states preserved
    const so1 = om2.stream_objects.get(1).?;
    try testing.expectEqual(S3ObjectState.committed, so1.state);
    try testing.expectEqual(@as(i64, 0), so1.state_changed_ms);

    const so2 = om2.stream_objects.get(2).?;
    try testing.expectEqual(S3ObjectState.mark_destroyed, so2.state);
    try testing.expectEqual(@as(i64, 5000), so2.state_changed_ms);

    const so3 = om2.stream_objects.get(3).?;
    try testing.expectEqual(S3ObjectState.prepared, so3.state);
    try testing.expectEqual(@as(i64, 3000), so3.state_changed_ms);

    // Verify SSO state preserved
    const sso = om2.stream_set_objects.get(10).?;
    try testing.expectEqual(S3ObjectState.mark_destroyed, sso.state);
    try testing.expectEqual(@as(i64, 8000), sso.state_changed_ms);
}

test "ObjectManager collectDestroyedObjects handles mixed SO and SSO" {
    var om = ObjectManager.init(testing.allocator, 0);
    defer om.deinit();

    // Create a committed SO and mark it destroyed
    try om.commitStreamObject(1, 1, 0, 100, "so/1/0-1", 1024);
    om.markDestroyedAt(1, 1000);

    // Create a committed SSO and mark it destroyed
    const ranges = [_]StreamOffsetRange{
        .{ .stream_id = 2, .start_offset = 0, .end_offset = 50 },
    };
    try om.commitStreamSetObject(10, 0, 1, &ranges, "sso/10", 512);
    om.markDestroyedAt(10, 1000);

    // Collect after retention
    const keys = try om.collectDestroyedObjectsAt(5000, testing.allocator, 10000);
    defer {
        for (keys) |k| testing.allocator.free(k);
        testing.allocator.free(keys);
    }

    try testing.expectEqual(@as(usize, 2), keys.len);
    try testing.expectEqual(@as(usize, 0), om.getStreamObjectCount());
    try testing.expectEqual(@as(usize, 0), om.getStreamSetObjectCount());
}

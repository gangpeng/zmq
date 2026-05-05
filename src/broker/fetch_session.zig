const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.fetch_session);

fn monotonicMs() i64 {
    return @intCast(@import("time_compat").monotonicMilliTimestamp());
}

/// KIP-227 Incremental Fetch Sessions.
///
/// Tracks per-connection fetch state so that subsequent fetches only
/// return partitions whose data has changed since the last fetch.
/// This significantly reduces network bandwidth for idle consumers.
///
/// Session lifecycle:
/// 1. Client sends fetch with session_id=0, epoch=0 → creates new session
/// 2. Server responds with session_id, epoch=1
/// 3. Client sends next fetch with session_id, epoch=1 → incremental
/// 4. Server only returns partitions with new data
/// 5. Client can close session by sending epoch=FINAL_EPOCH (-1)
pub const FetchSessionManager = struct {
    sessions: std.AutoHashMap(i32, FetchSession),
    next_session_id: i32 = 1,
    allocator: Allocator,

    pub fn init(alloc: Allocator) FetchSessionManager {
        return .{
            .sessions = std.AutoHashMap(i32, FetchSession).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *FetchSessionManager) void {
        var it = self.sessions.valueIterator();
        while (it.next()) |session| {
            session.deinit();
        }
        self.sessions.deinit();
    }

    /// Create or retrieve a fetch session.
    /// session_id=0 means create new session.
    /// epoch=FINAL_EPOCH means close session.
    pub fn getOrCreate(self: *FetchSessionManager, session_id: i32, epoch: i32) !FetchSessionResult {
        const FINAL_EPOCH: i32 = -1;

        if (epoch == FINAL_EPOCH) {
            // Close session
            if (self.sessions.fetchRemove(session_id)) |removed| {
                var session = removed.value;
                session.deinit();
                log.debug("Fetch session closed: id={d}", .{session_id});
            }
            return .{ .session_id = 0, .epoch = 0, .is_new = false, .is_closed = true };
        }

        if (session_id == 0) {
            // Create new session
            const new_id = self.next_session_id;
            self.next_session_id += 1;

            var new_session = FetchSession.init(self.allocator);
            new_session.epoch = 1;
            try self.sessions.put(new_id, new_session);
            log.debug("Fetch session created: id={d}", .{new_id});
            return .{ .session_id = new_id, .epoch = 1, .is_new = true, .is_closed = false };
        }

        // Existing session — increment epoch
        if (self.sessions.getPtr(session_id)) |session| {
            session.epoch += 1;
            return .{ .session_id = session_id, .epoch = session.epoch, .is_new = false, .is_closed = false };
        }

        // Session not found — create new one
        const new_id = self.next_session_id;
        self.next_session_id += 1;
        try self.sessions.put(new_id, FetchSession.init(self.allocator));
        return .{ .session_id = new_id, .epoch = 1, .is_new = true, .is_closed = false };
    }

    /// Update a session's cached high watermarks for a partition.
    /// Returns true if the partition has new data (high watermark changed).
    pub fn updatePartition(self: *FetchSessionManager, session_id: i32, topic: []const u8, partition: i32, high_watermark: i64) !bool {
        const session = self.sessions.getPtr(session_id) orelse return true;

        const key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ topic, partition });

        if (session.partition_hwm.getPtr(key)) |ptr| {
            const cached_hwm = ptr.*;
            self.allocator.free(key);
            if (cached_hwm == high_watermark) {
                return false; // No new data
            }
            // Update cached value — need to look up again since key was freed
            const rekey = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ topic, partition });
            defer self.allocator.free(rekey);
            if (session.partition_hwm.getPtr(rekey)) |p| {
                p.* = high_watermark;
            }
            return true;
        }

        // First time seeing this partition
        try session.partition_hwm.put(key, high_watermark);
        return true;
    }

    /// Number of active sessions.
    pub fn sessionCount(self: *const FetchSessionManager) usize {
        return self.sessions.count();
    }
};

pub const FetchSession = struct {
    epoch: i32 = 0,
    partition_hwm: std.StringHashMap(i64),
    allocator: Allocator,
    created_ms: i64,

    pub fn init(alloc: Allocator) FetchSession {
        return .{
            .partition_hwm = std.StringHashMap(i64).init(alloc),
            .allocator = alloc,
            .created_ms = monotonicMs(),
        };
    }

    pub fn deinit(self: *FetchSession) void {
        var it = self.partition_hwm.keyIterator();
        while (it.next()) |key| {
            self.allocator.free(key.*);
        }
        self.partition_hwm.deinit();
    }
};

pub const FetchSessionResult = struct {
    session_id: i32,
    epoch: i32,
    is_new: bool,
    is_closed: bool,
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "FetchSessionManager create and retrieve" {
    var mgr = FetchSessionManager.init(testing.allocator);
    defer mgr.deinit();

    // Create new session
    const result1 = try mgr.getOrCreate(0, 0);
    try testing.expect(result1.is_new);
    try testing.expectEqual(@as(i32, 1), result1.session_id);
    try testing.expectEqual(@as(i32, 1), result1.epoch);

    // Next fetch on same session
    const result2 = try mgr.getOrCreate(1, 1);
    try testing.expect(!result2.is_new);
    try testing.expectEqual(@as(i32, 1), result2.session_id);
    try testing.expectEqual(@as(i32, 2), result2.epoch);

    try testing.expectEqual(@as(usize, 1), mgr.sessionCount());
}

test "FetchSessionManager close session" {
    var mgr = FetchSessionManager.init(testing.allocator);
    defer mgr.deinit();

    const result1 = try mgr.getOrCreate(0, 0);
    try testing.expectEqual(@as(usize, 1), mgr.sessionCount());

    // Close session
    const result2 = try mgr.getOrCreate(result1.session_id, -1);
    try testing.expect(result2.is_closed);
    try testing.expectEqual(@as(usize, 0), mgr.sessionCount());
}

test "FetchSessionManager partition tracking" {
    var mgr = FetchSessionManager.init(testing.allocator);
    defer mgr.deinit();

    const result = try mgr.getOrCreate(0, 0);
    const sid = result.session_id;

    // First time: always has new data
    const has_new1 = try mgr.updatePartition(sid, "topic-a", 0, 10);
    try testing.expect(has_new1);

    // Same watermark: no new data
    const has_new2 = try mgr.updatePartition(sid, "topic-a", 0, 10);
    try testing.expect(!has_new2);

    // Different watermark: new data
    const has_new3 = try mgr.updatePartition(sid, "topic-a", 0, 15);
    try testing.expect(has_new3);
}

test "FetchSessionManager multiple sessions" {
    var mgr = FetchSessionManager.init(testing.allocator);
    defer mgr.deinit();

    const r1 = try mgr.getOrCreate(0, 0);
    const r2 = try mgr.getOrCreate(0, 0);
    try testing.expect(r1.session_id != r2.session_id);
    try testing.expectEqual(@as(usize, 2), mgr.sessionCount());
}

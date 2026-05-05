const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.connections);

fn monotonicMs() i64 {
    return @intCast(@import("time_compat").monotonicMilliTimestamp());
}

/// Connection manager tracks all active client connections.
///
/// Provides connection IDs, per-connection state, idle timeout
/// tracking, and connection limits.
pub const ConnectionManager = struct {
    connections: std.AutoHashMap(u64, ConnectionInfo),
    next_id: u64 = 1,
    max_connections: usize,
    allocator: Allocator,

    pub const ConnectionInfo = struct {
        id: u64,
        fd: std.posix.socket_t,
        created_at_ms: i64,
        last_active_ms: i64,
        bytes_received: u64 = 0,
        bytes_sent: u64 = 0,
        requests_processed: u64 = 0,
        client_id: ?[]const u8 = null,
        authenticated: bool = false,
    };

    pub fn init(alloc: Allocator, max_connections: usize) ConnectionManager {
        return .{
            .connections = std.AutoHashMap(u64, ConnectionInfo).init(alloc),
            .max_connections = max_connections,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ConnectionManager) void {
        self.connections.deinit();
    }

    /// Register a new connection. Returns the connection ID or error if at limit.
    pub fn addConnection(self: *ConnectionManager, fd: std.posix.socket_t) !u64 {
        if (self.connections.count() >= self.max_connections) {
            log.warn("Connection rejected: limit reached ({d}/{d})", .{ self.connections.count(), self.max_connections });
            return error.TooManyConnections;
        }

        const id = self.next_id;
        self.next_id += 1;

        const now = monotonicMs();
        try self.connections.put(id, .{
            .id = id,
            .fd = fd,
            .created_at_ms = now,
            .last_active_ms = now,
        });

        return id;
    }

    /// Remove a connection.
    pub fn removeConnection(self: *ConnectionManager, id: u64) void {
        _ = self.connections.fetchRemove(id);
    }

    /// Update last active timestamp and request count.
    pub fn recordActivity(self: *ConnectionManager, id: u64, bytes_in: u64, bytes_out: u64) void {
        if (self.connections.getPtr(id)) |info| {
            info.last_active_ms = monotonicMs();
            info.bytes_received += bytes_in;
            info.bytes_sent += bytes_out;
            info.requests_processed += 1;
        }
    }

    /// Set client ID after parsing first request header.
    pub fn setClientId(self: *ConnectionManager, id: u64, client_id: []const u8) void {
        if (self.connections.getPtr(id)) |info| {
            info.client_id = client_id;
        }
    }

    /// Get number of active connections.
    pub fn count(self: *const ConnectionManager) usize {
        return self.connections.count();
    }

    /// Find connections idle longer than `timeout_ms` and return their IDs.
    pub fn findIdleConnections(self: *const ConnectionManager, timeout_ms: i64, result_buf: []u64) usize {
        const now = monotonicMs();
        var found: usize = 0;
        var it = self.connections.iterator();
        while (it.next()) |entry| {
            if (found >= result_buf.len) break;
            if (now - entry.value_ptr.last_active_ms >= timeout_ms) {
                result_buf[found] = entry.key_ptr.*;
                found += 1;
            }
        }
        return found;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "ConnectionManager basic add/remove" {
    var mgr = ConnectionManager.init(testing.allocator, 100);
    defer mgr.deinit();

    const id1 = try mgr.addConnection(10);
    const id2 = try mgr.addConnection(11);

    try testing.expectEqual(@as(usize, 2), mgr.count());
    try testing.expect(id1 != id2);

    mgr.removeConnection(id1);
    try testing.expectEqual(@as(usize, 1), mgr.count());

    mgr.removeConnection(id2);
    try testing.expectEqual(@as(usize, 0), mgr.count());
}

test "ConnectionManager connection limit" {
    var mgr = ConnectionManager.init(testing.allocator, 2);
    defer mgr.deinit();

    _ = try mgr.addConnection(10);
    _ = try mgr.addConnection(11);
    try testing.expectError(error.TooManyConnections, mgr.addConnection(12));
}

test "ConnectionManager activity tracking" {
    var mgr = ConnectionManager.init(testing.allocator, 10);
    defer mgr.deinit();

    const id = try mgr.addConnection(10);
    mgr.recordActivity(id, 100, 200);
    mgr.recordActivity(id, 50, 75);

    const info = mgr.connections.get(id).?;
    try testing.expectEqual(@as(u64, 150), info.bytes_received);
    try testing.expectEqual(@as(u64, 275), info.bytes_sent);
    try testing.expectEqual(@as(u64, 2), info.requests_processed);
}

test "ConnectionManager removeConnection no-op for unknown ID" {
    var mgr = ConnectionManager.init(testing.allocator, 100);
    defer mgr.deinit();

    // Remove non-existent connection — should not crash
    mgr.removeConnection(999);
    try testing.expectEqual(@as(usize, 0), mgr.count());
}

test "ConnectionManager setClientId sets and retrieves ID" {
    var mgr = ConnectionManager.init(testing.allocator, 100);
    defer mgr.deinit();

    const id = try mgr.addConnection(10);
    mgr.setClientId(id, "my-kafka-client");

    const info = mgr.connections.get(id).?;
    try testing.expect(info.client_id != null);
    try testing.expectEqualStrings("my-kafka-client", info.client_id.?);
}

test "ConnectionManager setClientId no-op for unknown connection" {
    var mgr = ConnectionManager.init(testing.allocator, 100);
    defer mgr.deinit();

    // Should not crash
    mgr.setClientId(999, "unknown");
    try testing.expectEqual(@as(usize, 0), mgr.count());
}

test "ConnectionManager findIdleConnections returns idle ones" {
    var mgr = ConnectionManager.init(testing.allocator, 100);
    defer mgr.deinit();

    _ = try mgr.addConnection(10);
    _ = try mgr.addConnection(11);
    _ = try mgr.addConnection(12);

    // With timeout_ms = 0, all connections are idle (since time has advanced)
    var result_buf: [10]u64 = undefined;
    const found = mgr.findIdleConnections(0, &result_buf);
    try testing.expectEqual(@as(usize, 3), found);
}

test "ConnectionManager limit then remove allows new connection" {
    var mgr = ConnectionManager.init(testing.allocator, 2);
    defer mgr.deinit();

    const id1 = try mgr.addConnection(10);
    _ = try mgr.addConnection(11);
    try testing.expectError(error.TooManyConnections, mgr.addConnection(12));

    // Remove one, should allow new
    mgr.removeConnection(id1);
    const id3 = try mgr.addConnection(12);
    try testing.expectEqual(@as(usize, 2), mgr.count());
    try testing.expect(id3 > id1); // IDs are monotonically increasing
}

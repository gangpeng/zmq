const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.broker_registry);

/// Tracks live brokers registered with the controller quorum.
///
/// Broker-only nodes send BrokerRegistration on startup and periodic
/// BrokerHeartbeat to stay alive. The controller uses this registry to:
/// - Include all active brokers in Metadata responses
/// - Detect dead brokers (no heartbeat within timeout) and fence them
/// - Assign broker epochs for WAL fencing on failover
pub const BrokerRegistry = struct {
    /// Map: broker_id → BrokerInfo
    brokers: std.AutoHashMap(i32, BrokerInfo),
    allocator: Allocator,
    /// Monotonically increasing epoch counter for broker registrations.
    next_broker_epoch: i64 = 1,

    pub const BrokerInfo = struct {
        broker_id: i32,
        host: []u8,
        port: u16,
        rack: ?[]u8 = null,
        /// Epoch assigned by controller on registration (used for WAL fencing).
        broker_epoch: i64 = 0,
        /// Last heartbeat timestamp (ms since epoch).
        last_heartbeat_ms: i64 = 0,
        /// A fenced broker is not accepting client traffic.
        /// Unfenced after first successful heartbeat.
        fenced: bool = true,
    };

    pub fn init(alloc: Allocator) BrokerRegistry {
        return .{
            .brokers = std.AutoHashMap(i32, BrokerInfo).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *BrokerRegistry) void {
        var it = self.brokers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.host);
            if (entry.value_ptr.rack) |r| self.allocator.free(r);
        }
        self.brokers.deinit();
    }

    /// Register or re-register a broker. Returns the assigned broker_epoch.
    /// Called when a broker sends BrokerRegistration (API 62).
    pub fn register(self: *BrokerRegistry, broker_id: i32, host: []const u8, port: u16) !i64 {
        const epoch = self.next_broker_epoch;
        try self.registerWithEpoch(broker_id, host, port, epoch, true);
        return epoch;
    }

    /// Install a registration that already has a controller-assigned epoch.
    /// Used by Raft metadata replay on follower promotion/restart.
    pub fn registerWithEpoch(self: *BrokerRegistry, broker_id: i32, host: []const u8, port: u16, broker_epoch: i64, fenced: bool) !void {
        if (broker_epoch <= 0) return error.InvalidBrokerEpoch;
        const now = @import("time_compat").milliTimestamp();

        // If re-registering, free old host string
        if (self.brokers.getPtr(broker_id)) |existing| {
            self.allocator.free(existing.host);
            if (existing.rack) |r| self.allocator.free(r);
        }

        const host_copy = try self.allocator.dupe(u8, host);

        try self.brokers.put(broker_id, .{
            .broker_id = broker_id,
            .host = host_copy,
            .port = port,
            .broker_epoch = broker_epoch,
            .last_heartbeat_ms = now,
            .fenced = fenced,
        });

        if (broker_epoch >= self.next_broker_epoch) self.next_broker_epoch = broker_epoch + 1;
        log.info("Broker {d} registered: {s}:{d} epoch={d}", .{ broker_id, host, port, broker_epoch });
    }

    /// Process a heartbeat from a broker. Returns true if the broker is active.
    /// Called when a broker sends BrokerHeartbeat (API 63).
    pub fn heartbeat(self: *BrokerRegistry, broker_id: i32, broker_epoch: i64) !bool {
        const entry = self.brokers.getPtr(broker_id) orelse return error.BrokerNotRegistered;

        // Reject heartbeats from stale epochs (broker was replaced)
        if (broker_epoch != entry.broker_epoch) {
            log.warn("Broker {d} heartbeat rejected: stale epoch {d} (current {d})", .{
                broker_id, broker_epoch, entry.broker_epoch,
            });
            return false;
        }

        entry.last_heartbeat_ms = @import("time_compat").milliTimestamp();
        // Unfence after first successful heartbeat
        if (entry.fenced) {
            entry.fenced = false;
            log.info("Broker {d} unfenced", .{broker_id});
        }
        return true;
    }

    /// Evict brokers that haven't sent a heartbeat within timeout_ms.
    /// Returns the number of evicted brokers.
    pub fn evictExpired(self: *BrokerRegistry, timeout_ms: i64) usize {
        const now = @import("time_compat").milliTimestamp();
        var to_evict: [64]i32 = undefined;
        var evict_count: usize = 0;

        var it = self.brokers.iterator();
        while (it.next()) |entry| {
            const elapsed = now - entry.value_ptr.last_heartbeat_ms;
            if (elapsed > timeout_ms) {
                if (evict_count < 64) {
                    to_evict[evict_count] = entry.key_ptr.*;
                    evict_count += 1;
                }
            }
        }

        for (to_evict[0..evict_count]) |bid| {
            if (self.brokers.fetchRemove(bid)) |removed| {
                self.allocator.free(removed.value.host);
                if (removed.value.rack) |r| self.allocator.free(r);
                log.info("Broker {d} evicted (heartbeat timeout)", .{bid});
            }
        }

        return evict_count;
    }

    /// Get the number of registered brokers.
    pub fn count(self: *const BrokerRegistry) usize {
        return self.brokers.count();
    }

    /// Get the number of active (non-fenced) brokers.
    pub fn activeCount(self: *const BrokerRegistry) usize {
        var active: usize = 0;
        var it = self.brokers.iterator();
        while (it.next()) |entry| {
            if (!entry.value_ptr.fenced) active += 1;
        }
        return active;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "BrokerRegistry register and heartbeat" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    // Register broker 100
    const epoch = try registry.register(100, "broker1.example.com", 9092);
    try testing.expect(epoch > 0);
    try testing.expectEqual(@as(usize, 1), registry.count());

    // Initially fenced
    try testing.expectEqual(@as(usize, 0), registry.activeCount());

    // Heartbeat unfences
    const active = try registry.heartbeat(100, epoch);
    try testing.expect(active);
    try testing.expectEqual(@as(usize, 1), registry.activeCount());
}

test "BrokerRegistry reject stale epoch" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const epoch1 = try registry.register(100, "broker1", 9092);
    // Re-register (simulating restart) — gets new epoch
    const epoch2 = try registry.register(100, "broker1", 9092);
    try testing.expect(epoch2 > epoch1);

    // Old epoch heartbeat should fail
    const active = try registry.heartbeat(100, epoch1);
    try testing.expect(!active);
}

test "BrokerRegistry evict expired" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    _ = try registry.register(100, "broker1", 9092);
    _ = try registry.register(101, "broker2", 9092);
    try testing.expectEqual(@as(usize, 2), registry.count());

    // Force expiration by setting last_heartbeat_ms to the past
    if (registry.brokers.getPtr(100)) |info| {
        info.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 60_000;
    }

    const evicted = registry.evictExpired(30_000);
    try testing.expectEqual(@as(usize, 1), evicted);
    try testing.expectEqual(@as(usize, 1), registry.count());
}

test "BrokerRegistry re-register replaces old entry" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const epoch1 = try registry.register(100, "host1", 9092);
    // Re-register with new host — simulates broker restart on different host
    const epoch2 = try registry.register(100, "host2", 9093);

    try testing.expect(epoch2 > epoch1);
    try testing.expectEqual(@as(usize, 1), registry.count());

    // Verify updated host/port
    if (registry.brokers.get(100)) |info| {
        try testing.expectEqualStrings("host2", info.host);
        try testing.expectEqual(@as(u16, 9093), info.port);
    }
}

test "BrokerRegistry registerWithEpoch replays durable registration" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerWithEpoch(100, "host1", 9092, 7, true);
    try testing.expectEqual(@as(usize, 1), registry.count());
    try testing.expectEqual(@as(i64, 8), registry.next_broker_epoch);

    if (registry.brokers.get(100)) |info| {
        try testing.expectEqual(@as(i64, 7), info.broker_epoch);
        try testing.expectEqualStrings("host1", info.host);
        try testing.expect(info.fenced);
    } else {
        return error.TestUnexpectedResult;
    }
}

test "BrokerRegistry heartbeat on unknown broker returns error" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const result = registry.heartbeat(999, 1);
    try testing.expectError(error.BrokerNotRegistered, result);
}

test "BrokerRegistry activeCount excludes fenced brokers" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const epoch1 = try registry.register(100, "host1", 9092);
    const epoch2 = try registry.register(101, "host2", 9092);

    // Both initially fenced
    try testing.expectEqual(@as(usize, 0), registry.activeCount());

    // Unfence one via heartbeat
    _ = try registry.heartbeat(100, epoch1);
    try testing.expectEqual(@as(usize, 1), registry.activeCount());

    // Unfence other
    _ = try registry.heartbeat(101, epoch2);
    try testing.expectEqual(@as(usize, 2), registry.activeCount());
}

test "BrokerRegistry eviction does not affect healthy brokers" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    _ = try registry.register(100, "healthy", 9092);
    _ = try registry.register(101, "dying", 9092);

    // Force only broker 101 to expire
    if (registry.brokers.getPtr(101)) |info| {
        info.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 60_000;
    }

    const evicted = registry.evictExpired(30_000);
    try testing.expectEqual(@as(usize, 1), evicted);
    try testing.expectEqual(@as(usize, 1), registry.count());

    // Verify healthy broker survived
    try testing.expect(registry.brokers.contains(100));
    try testing.expect(!registry.brokers.contains(101));
}

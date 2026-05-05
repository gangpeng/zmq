const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.broker_registry);

fn monotonicMs() i64 {
    return @intCast(@import("time_compat").monotonicMilliTimestamp());
}

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
        /// Stable replica directory UUIDs advertised by BrokerRegistration v2+.
        log_dirs: [][16]u8 = &.{},
        /// Log directories reported offline by the latest BrokerHeartbeat v1.
        offline_log_dirs: [][16]u8 = &.{},
        /// Epoch assigned by controller on registration (used for WAL fencing).
        broker_epoch: i64 = 0,
        /// Last heartbeat timestamp from a monotonic clock.
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
            if (entry.value_ptr.log_dirs.len > 0) self.allocator.free(entry.value_ptr.log_dirs);
            if (entry.value_ptr.offline_log_dirs.len > 0) self.allocator.free(entry.value_ptr.offline_log_dirs);
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
        try self.registerWithEpochAndRack(broker_id, host, port, null, broker_epoch, fenced);
    }

    /// Install a registration with optional topology metadata.
    pub fn registerWithEpochAndRack(self: *BrokerRegistry, broker_id: i32, host: []const u8, port: u16, rack: ?[]const u8, broker_epoch: i64, fenced: bool) !void {
        try self.registerWithEpochRackAndLogDirs(broker_id, host, port, rack, &.{}, broker_epoch, fenced);
    }

    /// Install a registration with optional topology and local JBOD directory metadata.
    pub fn registerWithEpochRackAndLogDirs(self: *BrokerRegistry, broker_id: i32, host: []const u8, port: u16, rack: ?[]const u8, log_dirs: []const [16]u8, broker_epoch: i64, fenced: bool) !void {
        if (broker_epoch <= 0) return error.InvalidBrokerEpoch;
        const now = monotonicMs();

        const host_copy = try self.allocator.dupe(u8, host);
        errdefer self.allocator.free(host_copy);
        const rack_copy = if (rack) |r| try self.allocator.dupe(u8, r) else null;
        errdefer if (rack_copy) |r| self.allocator.free(r);
        const log_dirs_copy = try self.allocator.dupe([16]u8, log_dirs);
        errdefer if (log_dirs_copy.len > 0) self.allocator.free(log_dirs_copy);

        const old = try self.brokers.fetchPut(broker_id, .{
            .broker_id = broker_id,
            .host = host_copy,
            .port = port,
            .rack = rack_copy,
            .log_dirs = log_dirs_copy,
            .offline_log_dirs = &.{},
            .broker_epoch = broker_epoch,
            .last_heartbeat_ms = now,
            .fenced = fenced,
        });
        if (old) |entry| {
            self.allocator.free(entry.value.host);
            if (entry.value.rack) |r| self.allocator.free(r);
            if (entry.value.log_dirs.len > 0) self.allocator.free(entry.value.log_dirs);
            if (entry.value.offline_log_dirs.len > 0) self.allocator.free(entry.value.offline_log_dirs);
        }

        if (broker_epoch >= self.next_broker_epoch) self.next_broker_epoch = broker_epoch + 1;
        log.info("Broker {d} registered: {s}:{d} epoch={d}", .{ broker_id, host, port, broker_epoch });
    }

    /// Process a heartbeat from a broker. Returns false for stale epochs.
    /// Called when a broker sends BrokerHeartbeat (API 63).
    pub fn heartbeat(self: *BrokerRegistry, broker_id: i32, broker_epoch: i64) !bool {
        return self.heartbeatWithOfflineLogDirs(broker_id, broker_epoch, false, &.{});
    }

    pub fn heartbeatWithOfflineLogDirs(self: *BrokerRegistry, broker_id: i32, broker_epoch: i64, want_fence: bool, offline_log_dirs: []const [16]u8) !bool {
        const entry = self.brokers.getPtr(broker_id) orelse return error.BrokerNotRegistered;

        // Reject heartbeats from stale epochs (broker was replaced)
        if (broker_epoch != entry.broker_epoch) {
            log.warn("Broker {d} heartbeat rejected: stale epoch {d} (current {d})", .{
                broker_id, broker_epoch, entry.broker_epoch,
            });
            return false;
        }

        const should_fence = want_fence or allRegisteredLogDirsOffline(entry.log_dirs, offline_log_dirs);
        try self.installLogDirStatus(broker_id, broker_epoch, should_fence, offline_log_dirs);
        if (!should_fence) log.info("Broker {d} unfenced", .{broker_id});
        return true;
    }

    pub fn validateHeartbeatOfflineLogDirs(self: *const BrokerRegistry, broker_id: i32, broker_epoch: i64, offline_log_dirs: []const [16]u8) !void {
        const entry = self.brokers.get(broker_id) orelse return error.BrokerNotRegistered;
        if (broker_epoch != entry.broker_epoch) return error.StaleBrokerEpoch;
        try self.validateOfflineLogDirs(entry, offline_log_dirs);
    }

    pub fn desiredFencedForOfflineLogDirs(self: *const BrokerRegistry, broker_id: i32, want_fence: bool, offline_log_dirs: []const [16]u8) !bool {
        const entry = self.brokers.get(broker_id) orelse return error.BrokerNotRegistered;
        return want_fence or allRegisteredLogDirsOffline(entry.log_dirs, offline_log_dirs);
    }

    pub fn offlineLogDirsChanged(self: *const BrokerRegistry, broker_id: i32, offline_log_dirs: []const [16]u8) bool {
        const entry = self.brokers.get(broker_id) orelse return false;
        return !uuidSliceEquals(entry.offline_log_dirs, offline_log_dirs);
    }

    pub fn installLogDirStatus(self: *BrokerRegistry, broker_id: i32, broker_epoch: i64, fenced: bool, offline_log_dirs: []const [16]u8) !void {
        const entry = self.brokers.getPtr(broker_id) orelse return error.BrokerNotRegistered;
        if (broker_epoch != entry.broker_epoch) return error.StaleBrokerEpoch;
        try self.validateOfflineLogDirs(entry.*, offline_log_dirs);

        const offline_copy = try self.allocator.dupe([16]u8, offline_log_dirs);
        errdefer if (offline_copy.len > 0) self.allocator.free(offline_copy);

        entry.last_heartbeat_ms = monotonicMs();
        if (entry.offline_log_dirs.len > 0) self.allocator.free(entry.offline_log_dirs);
        entry.offline_log_dirs = offline_copy;
        entry.fenced = fenced;
    }

    /// Remove a broker registration. Returns false when the broker is unknown.
    pub fn unregister(self: *BrokerRegistry, broker_id: i32) bool {
        const removed = self.brokers.fetchRemove(broker_id) orelse return false;
        self.allocator.free(removed.value.host);
        if (removed.value.rack) |r| self.allocator.free(r);
        if (removed.value.log_dirs.len > 0) self.allocator.free(removed.value.log_dirs);
        if (removed.value.offline_log_dirs.len > 0) self.allocator.free(removed.value.offline_log_dirs);
        log.info("Broker {d} unregistered", .{broker_id});
        return true;
    }

    pub fn hasLogDir(self: *const BrokerRegistry, broker_id: i32, directory_id: [16]u8) bool {
        const info = self.brokers.get(broker_id) orelse return false;
        for (info.log_dirs) |registered| {
            if (std.mem.eql(u8, registered[0..], directory_id[0..])) return true;
        }
        return false;
    }

    fn validateOfflineLogDirs(self: *const BrokerRegistry, info: BrokerInfo, offline_log_dirs: []const [16]u8) !void {
        _ = self;
        for (offline_log_dirs, 0..) |dir, index| {
            if (isZeroUuid(dir)) return error.InvalidOfflineLogDir;
            if (!containsLogDir(info.log_dirs, dir)) return error.InvalidOfflineLogDir;
            for (offline_log_dirs[0..index]) |previous| {
                if (uuidEquals(previous, dir)) return error.InvalidOfflineLogDir;
            }
        }
    }

    fn allRegisteredLogDirsOffline(log_dirs: []const [16]u8, offline_log_dirs: []const [16]u8) bool {
        if (log_dirs.len == 0) return false;
        for (log_dirs) |dir| {
            if (!containsLogDir(offline_log_dirs, dir)) return false;
        }
        return true;
    }

    fn containsLogDir(log_dirs: []const [16]u8, directory_id: [16]u8) bool {
        for (log_dirs) |registered| {
            if (uuidEquals(registered, directory_id)) return true;
        }
        return false;
    }

    fn isZeroUuid(uuid: [16]u8) bool {
        return std.mem.allEqual(u8, uuid[0..], 0);
    }

    fn uuidEquals(a: [16]u8, b: [16]u8) bool {
        return std.mem.eql(u8, a[0..], b[0..]);
    }

    fn uuidSliceEquals(a: []const [16]u8, b: []const [16]u8) bool {
        if (a.len != b.len) return false;
        for (a, b) |left, right| {
            if (!uuidEquals(left, right)) return false;
        }
        return true;
    }

    /// Evict brokers that haven't sent a heartbeat within timeout_ms.
    /// Returns the number of evicted brokers.
    pub fn evictExpired(self: *BrokerRegistry, timeout_ms: i64) usize {
        const now = monotonicMs();
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
            if (self.unregister(bid)) log.info("Broker {d} evicted (heartbeat timeout)", .{bid});
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
        info.last_heartbeat_ms = monotonicMs() - 60_000;
    }

    const evicted = registry.evictExpired(30_000);
    try testing.expectEqual(@as(usize, 1), evicted);
    try testing.expectEqual(@as(usize, 1), registry.count());
}

test "BrokerRegistry heartbeat freshness uses monotonic timestamps" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const before = monotonicMs();
    const epoch = try registry.register(100, "broker1", 9092);
    var info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    const registered_at = info.last_heartbeat_ms;
    const after_register = monotonicMs();
    try testing.expect(registered_at >= before);
    try testing.expect(registered_at <= after_register);

    _ = try registry.heartbeat(100, epoch);
    info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    const heartbeat_at = info.last_heartbeat_ms;
    const after_heartbeat = monotonicMs();
    try testing.expect(heartbeat_at >= registered_at);
    try testing.expect(heartbeat_at <= after_heartbeat);
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

test "BrokerRegistry unregister removes broker" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    _ = try registry.register(100, "host1", 9092);
    try testing.expect(registry.unregister(100));
    try testing.expectEqual(@as(usize, 0), registry.count());
    try testing.expect(!registry.unregister(100));
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

test "BrokerRegistry registerWithEpochAndRack preserves rack metadata" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerWithEpochAndRack(100, "host1", 9092, "rack-a", 7, true);
    try testing.expectEqual(@as(usize, 1), registry.count());

    const info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("host1", info.host);
    try testing.expectEqualStrings("rack-a", info.rack orelse return error.TestUnexpectedResult);

    try registry.registerWithEpochAndRack(100, "host2", 9093, "rack-b", 8, false);
    const updated = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("host2", updated.host);
    try testing.expectEqualStrings("rack-b", updated.rack orelse return error.TestUnexpectedResult);
    try testing.expect(!updated.fenced);
}

test "BrokerRegistry registerWithEpochRackAndLogDirs preserves local directories" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const dir_a = [_]u8{0xa1} ** 16;
    const dir_b = [_]u8{0xb2} ** 16;
    const dirs = [_][16]u8{ dir_a, dir_b };
    try registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, "rack-a", &dirs, 7, true);

    const info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 2), info.log_dirs.len);
    try testing.expectEqualSlices(u8, dir_a[0..], info.log_dirs[0][0..]);
    try testing.expectEqualSlices(u8, dir_b[0..], info.log_dirs[1][0..]);
    try testing.expect(registry.hasLogDir(100, dir_a));
    try testing.expect(!registry.hasLogDir(100, [_]u8{0xc3} ** 16));

    const dir_c = [_]u8{0xc4} ** 16;
    const replacement = [_][16]u8{dir_c};
    try registry.registerWithEpochRackAndLogDirs(100, "host2", 9093, null, &replacement, 8, false);

    const updated = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 1), updated.log_dirs.len);
    try testing.expectEqualSlices(u8, dir_c[0..], updated.log_dirs[0][0..]);
    try testing.expect(registry.hasLogDir(100, dir_c));
    try testing.expect(!registry.hasLogDir(100, dir_a));
}

test "BrokerRegistry heartbeatWithOfflineLogDirs fences fully offline broker" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const dir_a = [_]u8{0xa5} ** 16;
    const dir_b = [_]u8{0xb6} ** 16;
    const dirs = [_][16]u8{ dir_a, dir_b };
    try registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, null, &dirs, 7, true);

    const one_offline = [_][16]u8{dir_a};
    try testing.expect(try registry.heartbeatWithOfflineLogDirs(100, 7, false, &one_offline));
    var info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expect(!info.fenced);
    try testing.expectEqual(@as(usize, 1), info.offline_log_dirs.len);
    try testing.expectEqualSlices(u8, dir_a[0..], info.offline_log_dirs[0][0..]);

    try testing.expect(try registry.heartbeatWithOfflineLogDirs(100, 7, true, &.{}));
    info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expect(info.fenced);
    try testing.expectEqual(@as(usize, 0), info.offline_log_dirs.len);

    try testing.expect(try registry.heartbeatWithOfflineLogDirs(100, 7, false, &dirs));
    info = registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expect(info.fenced);
    try testing.expectEqual(@as(usize, 2), info.offline_log_dirs.len);
}

test "BrokerRegistry heartbeatWithOfflineLogDirs rejects unknown or duplicate directories" {
    var registry = BrokerRegistry.init(testing.allocator);
    defer registry.deinit();

    const dir_a = [_]u8{0xa7} ** 16;
    const dir_b = [_]u8{0xb8} ** 16;
    const dirs = [_][16]u8{dir_a};
    try registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, null, &dirs, 7, true);

    const unknown = [_][16]u8{dir_b};
    try testing.expectError(error.InvalidOfflineLogDir, registry.heartbeatWithOfflineLogDirs(100, 7, false, &unknown));

    const duplicate = [_][16]u8{ dir_a, dir_a };
    try testing.expectError(error.InvalidOfflineLogDir, registry.heartbeatWithOfflineLogDirs(100, 7, false, &duplicate));
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
        info.last_heartbeat_ms = monotonicMs() - 60_000;
    }

    const evicted = registry.evictExpired(30_000);
    try testing.expectEqual(@as(usize, 1), evicted);
    try testing.expectEqual(@as(usize, 1), registry.count());

    // Verify healthy broker survived
    try testing.expect(registry.brokers.contains(100));
    try testing.expect(!registry.brokers.contains(101));
}

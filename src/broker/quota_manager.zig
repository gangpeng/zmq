const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.quota);

/// Client quota manager.
///
/// Tracks per-client produce/fetch byte rates and request rates.
/// When a client exceeds its quota, responses include a throttle_time_ms
/// field to slow the client down.
pub const QuotaManager = struct {
    client_quotas: std.StringHashMap(ClientQuota),
    default_produce_rate: f64, // bytes/sec, 0 = unlimited
    default_fetch_rate: f64,
    default_request_rate: f64, // requests/sec
    allocator: Allocator,

    pub const ClientQuota = struct {
        client_id: []u8,
        produce_rate_limit: f64, // bytes/sec
        fetch_rate_limit: f64,
        request_rate_limit: f64,
        produce_window: RateWindow = .{},
        fetch_window: RateWindow = .{},
        request_window: RateWindow = .{},
    };

    pub const RateWindow = struct {
        bytes_in_window: u64 = 0,
        window_start_ms: i64 = 0,
        window_size_ms: i64 = 1000, // 1 second window
    };

    pub fn init(alloc: Allocator) QuotaManager {
        return .{
            .client_quotas = std.StringHashMap(ClientQuota).init(alloc),
            .default_produce_rate = 0, // unlimited
            .default_fetch_rate = 0,
            .default_request_rate = 0,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *QuotaManager) void {
        var it = self.client_quotas.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.client_id);
            self.allocator.free(entry.key_ptr.*);
        }
        self.client_quotas.deinit();
    }

    /// Set quota for a specific client.
    pub fn setClientQuota(self: *QuotaManager, client_id: []const u8, produce_rate: f64, fetch_rate: f64, request_rate: f64) !void {
        if (self.client_quotas.fetchRemove(client_id)) |old| {
            self.allocator.free(old.value.client_id);
            self.allocator.free(old.key);
        }

        const key = try self.allocator.dupe(u8, client_id);
        errdefer self.allocator.free(key);
        const cid = try self.allocator.dupe(u8, client_id);

        try self.client_quotas.put(key, .{
            .client_id = cid,
            .produce_rate_limit = produce_rate,
            .fetch_rate_limit = fetch_rate,
            .request_rate_limit = request_rate,
        });
        log.info("Quota set for client {s}: produce={d:.0}/s, fetch={d:.0}/s, request={d:.0}/s", .{ client_id, produce_rate, fetch_rate, request_rate });
    }

    /// Record produce bytes and return throttle time in ms (0 = no throttle).
    pub fn recordProduce(self: *QuotaManager, client_id: []const u8, bytes: u64) i32 {
        return self.recordAndThrottle(client_id, bytes, .produce);
    }

    /// Record fetch bytes and return throttle time in ms.
    pub fn recordFetch(self: *QuotaManager, client_id: []const u8, bytes: u64) i32 {
        return self.recordAndThrottle(client_id, bytes, .fetch);
    }

    const QuotaType = enum { produce, fetch, request };

    fn recordAndThrottle(self: *QuotaManager, client_id: []const u8, bytes: u64, qtype: QuotaType) i32 {
        const quota = self.client_quotas.getPtr(client_id) orelse return 0;
        const now = std.time.milliTimestamp();

        const window = switch (qtype) {
            .produce => &quota.produce_window,
            .fetch => &quota.fetch_window,
            .request => &quota.request_window,
        };

        const limit = switch (qtype) {
            .produce => quota.produce_rate_limit,
            .fetch => quota.fetch_rate_limit,
            .request => quota.request_rate_limit,
        };

        if (limit <= 0) return 0; // unlimited

        // Reset window if expired
        if (now - window.window_start_ms >= window.window_size_ms) {
            window.bytes_in_window = 0;
            window.window_start_ms = now;
        }

        window.bytes_in_window += bytes;

        // Check if over quota
        const rate = @as(f64, @floatFromInt(window.bytes_in_window)) /
            (@as(f64, @floatFromInt(window.window_size_ms)) / 1000.0);

        if (rate > limit) {
            // Calculate throttle time
            const excess = rate - limit;
            const throttle_ms: i32 = @intFromFloat(@min(excess / limit * 1000.0, 30000.0));
            log.info("Client {s} throttled for {d}ms: rate {d:.0}/s exceeds limit {d:.0}/s", .{ client_id, @max(throttle_ms, 1), rate, limit });
            return @max(throttle_ms, 1);
        }

        return 0;
    }

    /// Set default quotas for all clients.
    pub fn setDefaults(self: *QuotaManager, produce_rate: f64, fetch_rate: f64, request_rate: f64) void {
        self.default_produce_rate = produce_rate;
        self.default_fetch_rate = fetch_rate;
        self.default_request_rate = request_rate;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "QuotaManager no quotas = no throttle" {
    var qm = QuotaManager.init(testing.allocator);
    defer qm.deinit();

    const throttle = qm.recordProduce("client-1", 1000000);
    try testing.expectEqual(@as(i32, 0), throttle);
}

test "QuotaManager set and check quota" {
    var qm = QuotaManager.init(testing.allocator);
    defer qm.deinit();

    try qm.setClientQuota("client-1", 1000, 2000, 100); // 1KB/s produce

    // First produce within quota
    const t1 = qm.recordProduce("client-1", 500);
    try testing.expectEqual(@as(i32, 0), t1);

    // Exceed quota in same window
    const t2 = qm.recordProduce("client-1", 2000);
    try testing.expect(t2 > 0); // Should be throttled
}

test "QuotaManager different clients independent" {
    var qm = QuotaManager.init(testing.allocator);
    defer qm.deinit();

    try qm.setClientQuota("client-a", 1000, 0, 0);
    try qm.setClientQuota("client-b", 1000, 0, 0);

    _ = qm.recordProduce("client-a", 5000); // over quota
    const t = qm.recordProduce("client-b", 100); // should be fine
    try testing.expectEqual(@as(i32, 0), t);
}

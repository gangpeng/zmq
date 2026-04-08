const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Metric registry for broker observability.
///
/// Supports counters, gauges, and histograms.
/// Exports metrics in Prometheus exposition format.
pub const MetricRegistry = struct {
    counters: std.StringHashMap(Counter),
    gauges: std.StringHashMap(Gauge),
    histograms: std.StringHashMap(Histogram),
    allocator: Allocator,

    pub const Counter = struct {
        name: []u8,
        help: []u8,
        value: u64 = 0,
    };

    pub const Gauge = struct {
        name: []u8,
        help: []u8,
        value: f64 = 0,
    };

    pub const Histogram = struct {
        name: []u8,
        help: []u8,
        count: u64 = 0,
        sum: f64 = 0,
        buckets: [BUCKET_COUNT]u64 = [_]u64{0} ** BUCKET_COUNT,

        const BUCKET_COUNT = 11;
        const bucket_bounds = [BUCKET_COUNT]f64{ 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0 };

        pub fn observe(self: *Histogram, value: f64) void {
            self.count += 1;
            self.sum += value;
            for (bucket_bounds, 0..) |bound, i| {
                if (value <= bound) {
                    self.buckets[i] += 1;
                }
            }
        }
    };

    pub fn init(alloc: Allocator) MetricRegistry {
        return .{
            .counters = std.StringHashMap(Counter).init(alloc),
            .gauges = std.StringHashMap(Gauge).init(alloc),
            .histograms = std.StringHashMap(Histogram).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *MetricRegistry) void {
        var cit = self.counters.iterator();
        while (cit.next()) |e| {
            self.allocator.free(e.value_ptr.name);
            self.allocator.free(e.value_ptr.help);
            self.allocator.free(e.key_ptr.*);
        }
        self.counters.deinit();

        var git = self.gauges.iterator();
        while (git.next()) |e| {
            self.allocator.free(e.value_ptr.name);
            self.allocator.free(e.value_ptr.help);
            self.allocator.free(e.key_ptr.*);
        }
        self.gauges.deinit();

        var hit = self.histograms.iterator();
        while (hit.next()) |e| {
            self.allocator.free(e.value_ptr.name);
            self.allocator.free(e.value_ptr.help);
            self.allocator.free(e.key_ptr.*);
        }
        self.histograms.deinit();
    }

    /// Register a counter metric.
    pub fn registerCounter(self: *MetricRegistry, name: []const u8, help: []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.counters.put(key, .{
            .name = try self.allocator.dupe(u8, name),
            .help = try self.allocator.dupe(u8, help),
        });
    }

    /// Register a gauge metric.
    pub fn registerGauge(self: *MetricRegistry, name: []const u8, help: []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.gauges.put(key, .{
            .name = try self.allocator.dupe(u8, name),
            .help = try self.allocator.dupe(u8, help),
        });
    }

    /// Register a histogram metric.
    pub fn registerHistogram(self: *MetricRegistry, name: []const u8, help: []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.histograms.put(key, .{
            .name = try self.allocator.dupe(u8, name),
            .help = try self.allocator.dupe(u8, help),
        });
    }

    /// Increment a counter.
    pub fn incrementCounter(self: *MetricRegistry, name: []const u8) void {
        if (self.counters.getPtr(name)) |c| c.value += 1;
    }

    /// Add to a counter.
    pub fn addCounter(self: *MetricRegistry, name: []const u8, delta: u64) void {
        if (self.counters.getPtr(name)) |c| c.value += delta;
    }

    /// Set a gauge value.
    pub fn setGauge(self: *MetricRegistry, name: []const u8, value: f64) void {
        if (self.gauges.getPtr(name)) |g| g.value = value;
    }

    /// Observe a histogram value.
    pub fn observeHistogram(self: *MetricRegistry, name: []const u8, value: f64) void {
        if (self.histograms.getPtr(name)) |h| h.observe(value);
    }

    /// Export all metrics in Prometheus exposition format.
    pub fn exportPrometheus(self: *const MetricRegistry, alloc: Allocator) ![]u8 {
        var buf = std.ArrayList(u8).init(alloc);
        const writer = buf.writer();

        // Counters
        var cit = self.counters.iterator();
        while (cit.next()) |entry| {
            const c = entry.value_ptr;
            try writer.print("# HELP {s} {s}\n", .{ c.name, c.help });
            try writer.print("# TYPE {s} counter\n", .{c.name});
            try writer.print("{s} {d}\n\n", .{ c.name, c.value });
        }

        // Gauges
        var git = self.gauges.iterator();
        while (git.next()) |entry| {
            const g = entry.value_ptr;
            try writer.print("# HELP {s} {s}\n", .{ g.name, g.help });
            try writer.print("# TYPE {s} gauge\n", .{g.name});
            try writer.print("{s} {d:.6}\n\n", .{ g.name, g.value });
        }

        // Histograms
        var hit = self.histograms.iterator();
        while (hit.next()) |entry| {
            const h = entry.value_ptr;
            try writer.print("# HELP {s} {s}\n", .{ h.name, h.help });
            try writer.print("# TYPE {s} histogram\n", .{h.name});
            var cumulative: u64 = 0;
            for (Histogram.bucket_bounds, 0..) |bound, i| {
                cumulative += h.buckets[i];
                try writer.print("{s}_bucket{{le=\"{d:.3}\"}} {d}\n", .{ h.name, bound, cumulative });
            }
            try writer.print("{s}_bucket{{le=\"+Inf\"}} {d}\n", .{ h.name, h.count });
            try writer.print("{s}_sum {d:.6}\n", .{ h.name, h.sum });
            try writer.print("{s}_count {d}\n\n", .{ h.name, h.count });
        }

        return buf.toOwnedSlice();
    }
};

/// Register standard broker metrics.
pub fn registerBrokerMetrics(registry: *MetricRegistry) !void {
    try registry.registerCounter("kafka_server_requests_total", "Total number of requests processed");
    try registry.registerCounter("kafka_server_bytes_in_total", "Total bytes received");
    try registry.registerCounter("kafka_server_bytes_out_total", "Total bytes sent");
    try registry.registerGauge("kafka_server_active_connections", "Number of active connections");
    try registry.registerGauge("kafka_server_partition_count", "Number of partitions");
    try registry.registerHistogram("kafka_server_request_latency_seconds", "Request processing latency");
    try registry.registerCounter("kafka_server_produce_requests_total", "Total produce requests");
    try registry.registerCounter("kafka_server_fetch_requests_total", "Total fetch requests");
    // Per-API latency histograms
    try registry.registerHistogram("kafka_server_produce_latency_seconds", "Produce request latency");
    try registry.registerHistogram("kafka_server_fetch_latency_seconds", "Fetch request latency");
    // Group/topic gauges (used by tick())
    try registry.registerGauge("kafka_server_group_count", "Number of consumer groups");
    try registry.registerGauge("kafka_server_topic_count", "Number of topics");
    try registry.registerGauge("kafka_server_member_count", "Number of consumer group members");
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "MetricRegistry counter" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerCounter("requests_total", "Total requests");
    registry.incrementCounter("requests_total");
    registry.incrementCounter("requests_total");
    registry.addCounter("requests_total", 5);

    const counter = registry.counters.get("requests_total").?;
    try testing.expectEqual(@as(u64, 7), counter.value);
}

test "MetricRegistry gauge" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerGauge("connections", "Active connections");
    registry.setGauge("connections", 42.0);

    const gauge = registry.gauges.get("connections").?;
    try testing.expectEqual(@as(f64, 42.0), gauge.value);
}

test "MetricRegistry histogram" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerHistogram("latency", "Request latency");
    registry.observeHistogram("latency", 0.003);
    registry.observeHistogram("latency", 0.05);
    registry.observeHistogram("latency", 1.5);

    const h = registry.histograms.get("latency").?;
    try testing.expectEqual(@as(u64, 3), h.count);
    try testing.expect(h.sum > 1.5);
}

test "MetricRegistry prometheus export" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerCounter("test_counter", "A test counter");
    registry.addCounter("test_counter", 42);

    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.indexOf(u8, output, "test_counter 42") != null);
    try testing.expect(std.mem.indexOf(u8, output, "# TYPE test_counter counter") != null);
}

test "registerBrokerMetrics" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registerBrokerMetrics(&registry);
    try testing.expect(registry.counters.contains("kafka_server_requests_total"));
    try testing.expect(registry.gauges.contains("kafka_server_active_connections"));
    try testing.expect(registry.histograms.contains("kafka_server_request_latency_seconds"));
}

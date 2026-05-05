const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Metric registry for broker observability.
///
/// Supports counters, gauges, histograms, and labeled variants.
/// Exports metrics in Prometheus exposition format.
/// Thread-safe: a mutex protects all read/write access so the MetricsServer
/// thread can safely export while the handler thread updates values.
pub const MetricRegistry = struct {
    counters: std.StringHashMap(Counter),
    gauges: std.StringHashMap(Gauge),
    histograms: std.StringHashMap(Histogram),
    /// Labeled counters keyed by canonical Prometheus labels:
    /// name{label1="val1",label2="val2"}
    labeled_counters: std.StringHashMap(LabeledCounterEntry),
    /// Metadata for labeled counter families (keyed by base name)
    labeled_counter_meta: std.StringHashMap(LabeledMeta),
    /// Labeled histograms keyed by canonical Prometheus labels.
    labeled_histograms: std.StringHashMap(LabeledHistogramEntry),
    /// Metadata for labeled histogram families (keyed by base name)
    labeled_histogram_meta: std.StringHashMap(LabeledMeta),
    /// Labeled gauges keyed by canonical Prometheus labels.
    labeled_gauges: std.StringHashMap(LabeledGaugeEntry),
    /// Metadata for labeled gauge families (keyed by base name)
    labeled_gauge_meta: std.StringHashMap(LabeledMeta),
    allocator: Allocator,
    mutex: @import("mutex_compat").Mutex = .{},

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

    /// Metadata for a labeled metric family (shared HELP/TYPE across label combos).
    pub const LabeledMeta = struct {
        name: []u8,
        help: []u8,
        label_names: [][]u8,
    };

    /// A single labeled counter instance (one specific label combination).
    pub const LabeledCounterEntry = struct {
        value: u64 = 0,
    };

    /// A single labeled gauge instance (one specific label combination).
    pub const LabeledGaugeEntry = struct {
        value: f64 = 0,
    };

    /// A single labeled histogram instance (one specific label combination).
    pub const LabeledHistogramEntry = struct {
        count: u64 = 0,
        sum: f64 = 0,
        buckets: [Histogram.BUCKET_COUNT]u64 = [_]u64{0} ** Histogram.BUCKET_COUNT,

        pub fn observe(self: *LabeledHistogramEntry, value: f64) void {
            self.count += 1;
            self.sum += value;
            for (Histogram.bucket_bounds, 0..) |bound, i| {
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
            .labeled_counters = std.StringHashMap(LabeledCounterEntry).init(alloc),
            .labeled_counter_meta = std.StringHashMap(LabeledMeta).init(alloc),
            .labeled_histograms = std.StringHashMap(LabeledHistogramEntry).init(alloc),
            .labeled_histogram_meta = std.StringHashMap(LabeledMeta).init(alloc),
            .labeled_gauges = std.StringHashMap(LabeledGaugeEntry).init(alloc),
            .labeled_gauge_meta = std.StringHashMap(LabeledMeta).init(alloc),
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

        // Free labeled counters
        var lcit = self.labeled_counters.iterator();
        while (lcit.next()) |e| {
            self.allocator.free(e.key_ptr.*);
        }
        self.labeled_counters.deinit();

        var lcmit = self.labeled_counter_meta.iterator();
        while (lcmit.next()) |e| {
            self.allocator.free(e.value_ptr.name);
            self.allocator.free(e.value_ptr.help);
            for (e.value_ptr.label_names) |ln| self.allocator.free(ln);
            self.allocator.free(e.value_ptr.label_names);
            self.allocator.free(e.key_ptr.*);
        }
        self.labeled_counter_meta.deinit();

        // Free labeled histograms
        var lhit = self.labeled_histograms.iterator();
        while (lhit.next()) |e| {
            self.allocator.free(e.key_ptr.*);
        }
        self.labeled_histograms.deinit();

        var lhmit = self.labeled_histogram_meta.iterator();
        while (lhmit.next()) |e| {
            self.allocator.free(e.value_ptr.name);
            self.allocator.free(e.value_ptr.help);
            for (e.value_ptr.label_names) |ln| self.allocator.free(ln);
            self.allocator.free(e.value_ptr.label_names);
            self.allocator.free(e.key_ptr.*);
        }
        self.labeled_histogram_meta.deinit();

        // Free labeled gauges
        var lgit = self.labeled_gauges.iterator();
        while (lgit.next()) |e| {
            self.allocator.free(e.key_ptr.*);
        }
        self.labeled_gauges.deinit();

        var lgmit = self.labeled_gauge_meta.iterator();
        while (lgmit.next()) |e| {
            self.allocator.free(e.value_ptr.name);
            self.allocator.free(e.value_ptr.help);
            for (e.value_ptr.label_names) |ln| self.allocator.free(ln);
            self.allocator.free(e.value_ptr.label_names);
            self.allocator.free(e.key_ptr.*);
        }
        self.labeled_gauge_meta.deinit();
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

    /// Register a labeled counter family.
    /// label_names defines the label keys (e.g., &.{"operation", "status"}).
    /// Individual label value combinations are created on first increment.
    pub fn registerLabeledCounter(self: *MetricRegistry, name: []const u8, help: []const u8, label_names: []const []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const duped_names = try self.allocator.alloc([]u8, label_names.len);
        errdefer self.allocator.free(duped_names);
        for (label_names, 0..) |ln, i| {
            duped_names[i] = try self.allocator.dupe(u8, ln);
        }
        try self.labeled_counter_meta.put(key, .{
            .name = try self.allocator.dupe(u8, name),
            .help = try self.allocator.dupe(u8, help),
            .label_names = duped_names,
        });
    }

    /// Register a labeled histogram family.
    pub fn registerLabeledHistogram(self: *MetricRegistry, name: []const u8, help: []const u8, label_names: []const []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const duped_names = try self.allocator.alloc([]u8, label_names.len);
        errdefer self.allocator.free(duped_names);
        for (label_names, 0..) |ln, i| {
            duped_names[i] = try self.allocator.dupe(u8, ln);
        }
        try self.labeled_histogram_meta.put(key, .{
            .name = try self.allocator.dupe(u8, name),
            .help = try self.allocator.dupe(u8, help),
            .label_names = duped_names,
        });
    }

    /// Register a labeled gauge family.
    /// label_names defines the label keys (e.g., &.{"group", "topic", "partition"}).
    /// Individual label value combinations are created on first set.
    pub fn registerLabeledGauge(self: *MetricRegistry, name: []const u8, help: []const u8, label_names: []const []const u8) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const duped_names = try self.allocator.alloc([]u8, label_names.len);
        errdefer self.allocator.free(duped_names);
        for (label_names, 0..) |ln, i| {
            duped_names[i] = try self.allocator.dupe(u8, ln);
        }
        try self.labeled_gauge_meta.put(key, .{
            .name = try self.allocator.dupe(u8, name),
            .help = try self.allocator.dupe(u8, help),
            .label_names = duped_names,
        });
    }

    /// Increment a counter.
    pub fn incrementCounter(self: *MetricRegistry, name: []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.counters.getPtr(name)) |c| c.value += 1;
    }

    /// Add to a counter.
    pub fn addCounter(self: *MetricRegistry, name: []const u8, delta: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.counters.getPtr(name)) |c| c.value += delta;
    }

    /// Set a gauge value.
    pub fn setGauge(self: *MetricRegistry, name: []const u8, value: f64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.gauges.getPtr(name)) |g| g.value = value;
    }

    /// Observe a histogram value.
    pub fn observeHistogram(self: *MetricRegistry, name: []const u8, value: f64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.histograms.getPtr(name)) |h| h.observe(value);
    }

    /// Increment a labeled counter. Creates the label combination on first use.
    /// label_values must match the label_names order from registration.
    pub fn incrementLabeledCounter(self: *MetricRegistry, name: []const u8, label_values: []const []const u8) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const meta = self.labeled_counter_meta.get(name) orelse return;
        const key = buildLabelKey(self.allocator, name, meta.label_names, label_values) catch return;
        if (self.labeled_counters.getPtr(key)) |entry| {
            self.allocator.free(key);
            entry.value += 1;
        } else {
            self.labeled_counters.put(key, .{ .value = 1 }) catch {
                self.allocator.free(key);
            };
        }
    }

    /// Add to a labeled counter. Creates the label combination on first use.
    pub fn addLabeledCounter(self: *MetricRegistry, name: []const u8, label_values: []const []const u8, delta: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const meta = self.labeled_counter_meta.get(name) orelse return;
        const key = buildLabelKey(self.allocator, name, meta.label_names, label_values) catch return;
        if (self.labeled_counters.getPtr(key)) |entry| {
            self.allocator.free(key);
            entry.value += delta;
        } else {
            self.labeled_counters.put(key, .{ .value = delta }) catch {
                self.allocator.free(key);
            };
        }
    }

    /// Observe a labeled histogram value. Creates the label combination on first use.
    pub fn observeLabeledHistogram(self: *MetricRegistry, name: []const u8, label_values: []const []const u8, value: f64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const meta = self.labeled_histogram_meta.get(name) orelse return;
        const key = buildLabelKey(self.allocator, name, meta.label_names, label_values) catch return;
        if (self.labeled_histograms.getPtr(key)) |entry| {
            self.allocator.free(key);
            entry.observe(value);
        } else {
            var new_entry = LabeledHistogramEntry{};
            new_entry.observe(value);
            self.labeled_histograms.put(key, new_entry) catch {
                self.allocator.free(key);
            };
        }
    }

    /// Set a labeled gauge value. Creates the label combination on first use.
    /// label_values must match the label_names order from registration.
    pub fn setLabeledGauge(self: *MetricRegistry, name: []const u8, label_values: []const []const u8, value: f64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const meta = self.labeled_gauge_meta.get(name) orelse return;
        const key = buildLabelKey(self.allocator, name, meta.label_names, label_values) catch return;
        if (self.labeled_gauges.getPtr(key)) |entry| {
            self.allocator.free(key);
            entry.value = value;
        } else {
            self.labeled_gauges.put(key, .{ .value = value }) catch {
                self.allocator.free(key);
            };
        }
    }

    /// Build a canonical Prometheus label key like: name{label1="val1",label2="val2"}.
    fn buildLabelKey(alloc: Allocator, name: []const u8, label_names: []const []const u8, label_values: []const []const u8) ![]u8 {
        if (label_names.len != label_values.len) return error.InvalidLabelCount;

        var buf = std.array_list.Managed(u8).init(alloc);
        errdefer buf.deinit();
        const writer = @import("list_compat").writer(&buf);
        try writer.writeAll(name);
        try writer.writeByte('{');
        for (label_values, 0..) |val, i| {
            if (i > 0) try writer.writeByte(',');
            try writer.print("{s}=\"", .{label_names[i]});
            try writeEscapedPrometheusString(writer, val);
            try writer.writeByte('"');
        }
        try writer.writeByte('}');
        return buf.toOwnedSlice();
    }

    fn writeEscapedPrometheusString(writer: anytype, value: []const u8) !void {
        for (value) |byte| {
            switch (byte) {
                '\\' => try writer.writeAll("\\\\"),
                '"' => try writer.writeAll("\\\""),
                '\n' => try writer.writeAll("\\n"),
                else => try writer.writeByte(byte),
            }
        }
    }

    fn writeHelpLine(writer: anytype, name: []const u8, help: []const u8) !void {
        try writer.print("# HELP {s} ", .{name});
        try writeEscapedPrometheusString(writer, help);
        try writer.writeByte('\n');
    }

    fn labelSetFromFullKey(name: []const u8, full_key: []const u8) ?[]const u8 {
        if (!std.mem.startsWith(u8, full_key, name)) return null;
        if (full_key.len <= name.len + 1) return null;
        if (full_key[name.len] != '{') return null;
        if (full_key[full_key.len - 1] != '}') return null;
        return full_key[name.len + 1 .. full_key.len - 1];
    }

    /// Export all metrics in Prometheus exposition format.
    pub fn exportPrometheus(self: *MetricRegistry, alloc: Allocator) ![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var buf = std.array_list.Managed(u8).init(alloc);
        errdefer buf.deinit();
        const writer = @import("list_compat").writer(&buf);

        // Counters
        var cit = self.counters.iterator();
        while (cit.next()) |entry| {
            const c = entry.value_ptr;
            try writeHelpLine(writer, c.name, c.help);
            try writer.print("# TYPE {s} counter\n", .{c.name});
            try writer.print("{s} {d}\n\n", .{ c.name, c.value });
        }

        // Gauges
        var git = self.gauges.iterator();
        while (git.next()) |entry| {
            const g = entry.value_ptr;
            try writeHelpLine(writer, g.name, g.help);
            try writer.print("# TYPE {s} gauge\n", .{g.name});
            try writer.print("{s} {d:.6}\n\n", .{ g.name, g.value });
        }

        // Histograms
        var hit = self.histograms.iterator();
        while (hit.next()) |entry| {
            const h = entry.value_ptr;
            try writeHelpLine(writer, h.name, h.help);
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

        // Labeled counters — group by base metric name
        try self.exportLabeledCounters(writer);

        // Labeled histograms — group by base metric name
        try self.exportLabeledHistograms(writer);

        // Labeled gauges — group by base metric name
        try self.exportLabeledGauges(writer);

        return buf.toOwnedSlice();
    }

    /// Export labeled counters grouped by base metric name.
    fn exportLabeledCounters(self: *const MetricRegistry, writer: anytype) !void {
        var meta_it = self.labeled_counter_meta.iterator();
        while (meta_it.next()) |meta_entry| {
            const meta = meta_entry.value_ptr;
            try writeHelpLine(writer, meta.name, meta.help);
            try writer.print("# TYPE {s} counter\n", .{meta.name});

            // Find all labeled counter entries that start with this base name
            var lc_it = self.labeled_counters.iterator();
            while (lc_it.next()) |lc_entry| {
                const full_key = lc_entry.key_ptr.*;
                _ = labelSetFromFullKey(meta.name, full_key) orelse continue;
                try writer.print("{s} {d}\n", .{ full_key, lc_entry.value_ptr.value });
            }
            try writer.writeByte('\n');
        }
    }

    /// Export labeled histograms grouped by base metric name.
    fn exportLabeledHistograms(self: *const MetricRegistry, writer: anytype) !void {
        var meta_it = self.labeled_histogram_meta.iterator();
        while (meta_it.next()) |meta_entry| {
            const meta = meta_entry.value_ptr;
            try writeHelpLine(writer, meta.name, meta.help);
            try writer.print("# TYPE {s} histogram\n", .{meta.name});

            var lh_it = self.labeled_histograms.iterator();
            while (lh_it.next()) |lh_entry| {
                const full_key = lh_entry.key_ptr.*;
                const labels_str = labelSetFromFullKey(meta.name, full_key) orelse continue;
                const h = lh_entry.value_ptr;

                var cumulative: u64 = 0;
                for (Histogram.bucket_bounds, 0..) |bound, i| {
                    cumulative += h.buckets[i];
                    try writer.print("{s}_bucket{{{s},le=\"{d:.3}\"}} {d}\n", .{ meta.name, labels_str, bound, cumulative });
                }
                try writer.print("{s}_bucket{{{s},le=\"+Inf\"}} {d}\n", .{ meta.name, labels_str, h.count });
                try writer.print("{s}_sum{{{s}}} {d:.6}\n", .{ meta.name, labels_str, h.sum });
                try writer.print("{s}_count{{{s}}} {d}\n", .{ meta.name, labels_str, h.count });
            }
            try writer.writeByte('\n');
        }
    }

    /// Export labeled gauges grouped by base metric name.
    fn exportLabeledGauges(self: *const MetricRegistry, writer: anytype) !void {
        var meta_it = self.labeled_gauge_meta.iterator();
        while (meta_it.next()) |meta_entry| {
            const meta = meta_entry.value_ptr;
            try writeHelpLine(writer, meta.name, meta.help);
            try writer.print("# TYPE {s} gauge\n", .{meta.name});

            var lg_it = self.labeled_gauges.iterator();
            while (lg_it.next()) |lg_entry| {
                const full_key = lg_entry.key_ptr.*;
                _ = labelSetFromFullKey(meta.name, full_key) orelse continue;
                try writer.print("{s} {d:.6}\n", .{ full_key, lg_entry.value_ptr.value });
            }
            try writer.writeByte('\n');
        }
    }
};

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

test "MetricRegistry labeled counter" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledCounter("s3_requests_total", "Total S3 requests", &.{"operation"});

    registry.incrementLabeledCounter("s3_requests_total", &.{"put"});
    registry.incrementLabeledCounter("s3_requests_total", &.{"put"});
    registry.incrementLabeledCounter("s3_requests_total", &.{"get"});

    // Verify internal state
    const put_key = "s3_requests_total{operation=\"put\"}";
    const get_key = "s3_requests_total{operation=\"get\"}";
    try testing.expectEqual(@as(u64, 2), registry.labeled_counters.get(put_key).?.value);
    try testing.expectEqual(@as(u64, 1), registry.labeled_counters.get(get_key).?.value);

    // Verify Prometheus export
    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.indexOf(u8, output, "# TYPE s3_requests_total counter") != null);
    try testing.expect(std.mem.indexOf(u8, output, "s3_requests_total{operation=\"put\"} 2") != null);
    try testing.expect(std.mem.indexOf(u8, output, "s3_requests_total{operation=\"get\"} 1") != null);
}

test "MetricRegistry labeled counter with multiple labels" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledCounter("cache_ops", "Cache operations", &.{ "cache", "result" });

    registry.incrementLabeledCounter("cache_ops", &.{ "log", "hit" });
    registry.incrementLabeledCounter("cache_ops", &.{ "log", "hit" });
    registry.incrementLabeledCounter("cache_ops", &.{ "log", "miss" });
    registry.incrementLabeledCounter("cache_ops", &.{ "s3_block", "hit" });

    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.indexOf(u8, output, "cache=\"log\",result=\"hit\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cache=\"log\",result=\"miss\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "cache=\"s3_block\",result=\"hit\"") != null);
}

test "MetricRegistry labeled histogram" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledHistogram("s3_request_duration_seconds", "S3 latency", &.{"operation"});

    registry.observeLabeledHistogram("s3_request_duration_seconds", &.{"put"}, 0.05);
    registry.observeLabeledHistogram("s3_request_duration_seconds", &.{"put"}, 0.15);
    registry.observeLabeledHistogram("s3_request_duration_seconds", &.{"get"}, 0.003);

    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.indexOf(u8, output, "# TYPE s3_request_duration_seconds histogram") != null);
    try testing.expect(std.mem.indexOf(u8, output, "s3_request_duration_seconds_bucket{operation=\"put\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "s3_request_duration_seconds_bucket{operation=\"get\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "s3_request_duration_seconds_count{operation=\"put\"} 2") != null);
    try testing.expect(std.mem.indexOf(u8, output, "s3_request_duration_seconds_count{operation=\"get\"} 1") != null);
}

test "MetricRegistry addLabeledCounter" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledCounter("s3_bytes_total", "S3 bytes", &.{"direction"});

    registry.addLabeledCounter("s3_bytes_total", &.{"upload"}, 1024);
    registry.addLabeledCounter("s3_bytes_total", &.{"upload"}, 2048);
    registry.addLabeledCounter("s3_bytes_total", &.{"download"}, 4096);

    const upload_key = "s3_bytes_total{direction=\"upload\"}";
    const download_key = "s3_bytes_total{direction=\"download\"}";
    try testing.expectEqual(@as(u64, 3072), registry.labeled_counters.get(upload_key).?.value);
    try testing.expectEqual(@as(u64, 4096), registry.labeled_counters.get(download_key).?.value);
}

test "MetricRegistry labeled gauge" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledGauge("kafka_consumer_lag", "Consumer lag", &.{ "group", "topic", "partition" });

    registry.setLabeledGauge("kafka_consumer_lag", &.{ "my-group", "my-topic", "0" }, 42.0);
    registry.setLabeledGauge("kafka_consumer_lag", &.{ "my-group", "my-topic", "1" }, 17.0);

    // Verify internal state
    const key0 = "kafka_consumer_lag{group=\"my-group\",topic=\"my-topic\",partition=\"0\"}";
    const key1 = "kafka_consumer_lag{group=\"my-group\",topic=\"my-topic\",partition=\"1\"}";
    try testing.expectEqual(@as(f64, 42.0), registry.labeled_gauges.get(key0).?.value);
    try testing.expectEqual(@as(f64, 17.0), registry.labeled_gauges.get(key1).?.value);

    // Update existing value
    registry.setLabeledGauge("kafka_consumer_lag", &.{ "my-group", "my-topic", "0" }, 5.0);
    try testing.expectEqual(@as(f64, 5.0), registry.labeled_gauges.get(key0).?.value);

    // Verify Prometheus export
    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.indexOf(u8, output, "# TYPE kafka_consumer_lag gauge") != null);
    try testing.expect(std.mem.indexOf(u8, output, "group=\"my-group\",topic=\"my-topic\",partition=\"0\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "group=\"my-group\",topic=\"my-topic\",partition=\"1\"") != null);
}

test "MetricRegistry escapes Prometheus help and label values" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerCounter("escaped_help_total", "Backslash \\ quote \" newline\ntext");
    registry.incrementCounter("escaped_help_total");

    try registry.registerLabeledCounter("escaped_labels_total", "Escaped labels", &.{"operation"});
    registry.incrementLabeledCounter("escaped_labels_total", &.{"put,get"});
    registry.incrementLabeledCounter("escaped_labels_total", &.{"quote\"slash\\line\nbreak"});

    const output = try registry.exportPrometheus(testing.allocator);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.indexOf(u8, output, "# HELP escaped_help_total Backslash \\\\ quote \\\" newline\\ntext") != null);
    try testing.expect(std.mem.indexOf(u8, output, "escaped_labels_total{operation=\"put,get\"} 1") != null);
    try testing.expect(std.mem.indexOf(u8, output, "escaped_labels_total{operation=\"quote\\\"slash\\\\line\\nbreak\"} 1") != null);
}

test "MetricRegistry ignores labeled updates with invalid arity" {
    var registry = MetricRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.registerLabeledCounter("arity_total", "Arity checked labels", &.{ "operation", "status" });

    registry.incrementLabeledCounter("arity_total", &.{"put"});
    registry.incrementLabeledCounter("arity_total", &.{ "put", "ok", "extra" });
    try testing.expectEqual(@as(u32, 0), registry.labeled_counters.count());

    registry.incrementLabeledCounter("arity_total", &.{ "put", "ok" });
    try testing.expectEqual(@as(u32, 1), registry.labeled_counters.count());
    try testing.expect(registry.labeled_counters.contains("arity_total{operation=\"put\",status=\"ok\"}"));
}

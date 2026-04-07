const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Kafka Streams — stream processing topology.
///
/// Core abstractions:
/// - Topology: DAG of processing nodes (sources, processors, sinks)
/// - KStream: unbounded record stream
/// - StateStore: local key-value state for aggregations
/// - Serde: serializer/deserializer interface

/// Processing record flowing through the topology.
pub const Record = struct {
    key: ?[]const u8,
    value: ?[]const u8,
    timestamp: i64,
    topic: ?[]const u8 = null,
    partition: i32 = 0,
    offset: i64 = 0,
};

/// Topology — directed acyclic graph of processing nodes.
pub const Topology = struct {
    nodes: std.ArrayList(Node),
    edges: std.ArrayList(Edge),
    allocator: Allocator,

    pub const NodeType = enum { source, processor, sink };

    pub const Node = struct {
        name: []u8,
        node_type: NodeType,
        processor: ?*const fn (record: *const Record, ctx: *ProcessorContext) void = null,
    };

    pub const Edge = struct {
        from: usize, // node index
        to: usize,
    };

    pub fn init(alloc: Allocator) Topology {
        return .{
            .nodes = std.ArrayList(Node).init(alloc),
            .edges = std.ArrayList(Edge).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *Topology) void {
        for (self.nodes.items) |node| {
            self.allocator.free(node.name);
        }
        self.nodes.deinit();
        self.edges.deinit();
    }

    /// Add a source node that reads from a topic.
    pub fn addSource(self: *Topology, name: []const u8) !usize {
        const name_copy = try self.allocator.dupe(u8, name);
        try self.nodes.append(.{ .name = name_copy, .node_type = .source });
        return self.nodes.items.len - 1;
    }

    /// Add a processor node.
    pub fn addProcessor(self: *Topology, name: []const u8, processor: *const fn (*const Record, *ProcessorContext) void, parent: usize) !usize {
        const name_copy = try self.allocator.dupe(u8, name);
        try self.nodes.append(.{ .name = name_copy, .node_type = .processor, .processor = processor });
        const idx = self.nodes.items.len - 1;
        try self.edges.append(.{ .from = parent, .to = idx });
        return idx;
    }

    /// Add a sink node that writes to a topic.
    pub fn addSink(self: *Topology, name: []const u8, parent: usize) !usize {
        const name_copy = try self.allocator.dupe(u8, name);
        try self.nodes.append(.{ .name = name_copy, .node_type = .sink });
        const idx = self.nodes.items.len - 1;
        try self.edges.append(.{ .from = parent, .to = idx });
        return idx;
    }

    pub fn nodeCount(self: *const Topology) usize {
        return self.nodes.items.len;
    }

    /// Get downstream nodes for a given node.
    pub fn getDownstream(self: *const Topology, node_idx: usize, buf: []usize) usize {
        var count: usize = 0;
        for (self.edges.items) |edge| {
            if (edge.from == node_idx and count < buf.len) {
                buf[count] = edge.to;
                count += 1;
            }
        }
        return count;
    }
};

/// Processor context — passed to processor functions.
pub const ProcessorContext = struct {
    output_records: std.ArrayList(Record),
    state_store: *KeyValueStore,
    allocator: Allocator,

    pub fn init(alloc: Allocator, store: *KeyValueStore) ProcessorContext {
        return .{
            .output_records = std.ArrayList(Record).init(alloc),
            .state_store = store,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ProcessorContext) void {
        self.output_records.deinit();
    }

    /// Forward a record downstream.
    pub fn forward(self: *ProcessorContext, record: Record) !void {
        try self.output_records.append(record);
    }
};

/// In-memory key-value state store.
pub const KeyValueStore = struct {
    data: std.StringHashMap([]u8),
    allocator: Allocator,
    name: []const u8,

    pub fn init(alloc: Allocator, name: []const u8) KeyValueStore {
        return .{
            .data = std.StringHashMap([]u8).init(alloc),
            .allocator = alloc,
            .name = name,
        };
    }

    pub fn deinit(self: *KeyValueStore) void {
        var it = self.data.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.data.deinit();
    }

    pub fn put(self: *KeyValueStore, key: []const u8, value: []const u8) !void {
        if (self.data.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        const k = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(k);
        const v = try self.allocator.dupe(u8, value);
        try self.data.put(k, v);
    }

    pub fn get(self: *const KeyValueStore, key: []const u8) ?[]const u8 {
        return self.data.get(key);
    }

    pub fn delete(self: *KeyValueStore, key: []const u8) bool {
        if (self.data.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
            return true;
        }
        return false;
    }

    pub fn count(self: *const KeyValueStore) usize {
        return self.data.count();
    }
};

/// KStream — high-level stream DSL.
///
/// Wraps a series of records with transformation operations.
pub const KStream = struct {
    records: std.ArrayList(Record),
    allocator: Allocator,

    pub fn init(alloc: Allocator) KStream {
        return .{
            .records = std.ArrayList(Record).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *KStream) void {
        self.records.deinit();
    }

    pub fn addRecord(self: *KStream, record: Record) !void {
        try self.records.append(record);
    }

    /// Filter records based on a predicate.
    pub fn filter(self: *const KStream, alloc: Allocator, predicate: *const fn (*const Record) bool) !KStream {
        var result = KStream.init(alloc);
        for (self.records.items) |*rec| {
            if (predicate(rec)) {
                try result.addRecord(rec.*);
            }
        }
        return result;
    }

    /// Map values using a transform function.
    pub fn mapValues(self: *const KStream, alloc: Allocator, mapper: *const fn (?[]const u8) ?[]const u8) !KStream {
        var result = KStream.init(alloc);
        for (self.records.items) |rec| {
            var mapped = rec;
            mapped.value = mapper(rec.value);
            try result.addRecord(mapped);
        }
        return result;
    }

    pub fn count(self: *const KStream) usize {
        return self.records.items.len;
    }

    /// FlatMap: transform each record into zero or more records.
    pub fn flatMapValues(self: *const KStream, alloc: Allocator, mapper: *const fn (?[]const u8) []const ?[]const u8) !KStream {
        var result = KStream.init(alloc);
        for (self.records.items) |rec| {
            const values = mapper(rec.value);
            for (values) |v| {
                var new_rec = rec;
                new_rec.value = v;
                try result.addRecord(new_rec);
            }
        }
        return result;
    }

    /// Peek: execute side-effect for each record without modifying the stream.
    pub fn peek(self: *const KStream, action: *const fn (*const Record) void) void {
        for (self.records.items) |*rec| {
            action(rec);
        }
    }

    /// GroupByKey: group records by their key. Returns a map of key → records.
    pub fn groupByKey(self: *const KStream, alloc: Allocator) !std.StringHashMap(std.ArrayList(Record)) {
        var groups = std.StringHashMap(std.ArrayList(Record)).init(alloc);
        for (self.records.items) |rec| {
            const key = rec.key orelse continue;
            const entry = try groups.getOrPut(key);
            if (!entry.found_existing) {
                entry.value_ptr.* = std.ArrayList(Record).init(alloc);
            }
            try entry.value_ptr.append(rec);
        }
        return groups;
    }

    /// ForEach: terminal operation that processes each record.
    pub fn forEach(self: *const KStream, action: *const fn (*const Record) void) void {
        for (self.records.items) |*rec| {
            action(rec);
        }
    }

    /// Convert to list of records.
    pub fn toList(self: *const KStream) []const Record {
        return self.records.items;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "Topology build source → processor → sink" {
    var topo = Topology.init(testing.allocator);
    defer topo.deinit();

    const src = try topo.addSource("source-input");
    const proc = try topo.addProcessor("filter-proc", &struct {
        fn process(_: *const Record, _: *ProcessorContext) void {}
    }.process, src);
    const sink = try topo.addSink("sink-output", proc);

    try testing.expectEqual(@as(usize, 3), topo.nodeCount());

    var downstream: [4]usize = undefined;
    const n = topo.getDownstream(src, &downstream);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectEqual(proc, downstream[0]);

    const n2 = topo.getDownstream(proc, &downstream);
    try testing.expectEqual(@as(usize, 1), n2);
    try testing.expectEqual(sink, downstream[0]);
}

test "KeyValueStore put/get/delete" {
    var store = KeyValueStore.init(testing.allocator, "test-store");
    defer store.deinit();

    try store.put("key1", "value1");
    try store.put("key2", "value2");

    try testing.expectEqualStrings("value1", store.get("key1").?);
    try testing.expectEqual(@as(usize, 2), store.count());

    try testing.expect(store.delete("key1"));
    try testing.expect(store.get("key1") == null);
    try testing.expectEqual(@as(usize, 1), store.count());
}

test "KeyValueStore update" {
    var store = KeyValueStore.init(testing.allocator, "test-store");
    defer store.deinit();

    try store.put("key", "v1");
    try store.put("key", "v2");
    try testing.expectEqualStrings("v2", store.get("key").?);
    try testing.expectEqual(@as(usize, 1), store.count());
}

fn testFilter(rec: *const Record) bool {
    if (rec.value) |v| return v.len > 3;
    return false;
}

/// Window store for windowed aggregations.
///
/// Supports tumbling and hopping time windows.
/// Windows are identified by (key, window_start_ms).
pub const WindowStore = struct {
    windows: std.StringHashMap(WindowBucket),
    window_size_ms: i64,
    allocator: Allocator,
    name: []const u8,

    pub const WindowBucket = struct {
        window_start: i64,
        window_end: i64,
        values: std.ArrayList([]u8),
    };

    pub fn init(alloc: Allocator, name: []const u8, window_size_ms: i64) WindowStore {
        return .{
            .windows = std.StringHashMap(WindowBucket).init(alloc),
            .window_size_ms = window_size_ms,
            .allocator = alloc,
            .name = name,
        };
    }

    pub fn deinit(self: *WindowStore) void {
        var it = self.windows.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.values.items) |v| self.allocator.free(v);
            entry.value_ptr.values.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.windows.deinit();
    }

    /// Add a value to the appropriate window bucket.
    pub fn put(self: *WindowStore, key: []const u8, value: []const u8, timestamp: i64) !void {
        const window_start = @divFloor(timestamp, self.window_size_ms) * self.window_size_ms;
        const window_key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ key, window_start });

        const entry = try self.windows.getOrPut(window_key);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{
                .window_start = window_start,
                .window_end = window_start + self.window_size_ms,
                .values = std.ArrayList([]u8).init(self.allocator),
            };
        } else {
            self.allocator.free(window_key);
        }

        try entry.value_ptr.values.append(try self.allocator.dupe(u8, value));
    }

    /// Get all values in a window for a key.
    pub fn fetch(self: *const WindowStore, key: []const u8, window_start: i64) ?[]const []u8 {
        var key_buf: [256]u8 = undefined;
        const window_key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ key, window_start }) catch return null;
        if (self.windows.get(window_key)) |bucket| {
            return bucket.values.items;
        }
        return null;
    }

    /// Get the count of values in a window.
    pub fn fetchCount(self: *const WindowStore, key: []const u8, window_start: i64) usize {
        if (self.fetch(key, window_start)) |vals| return vals.len;
        return 0;
    }

    pub fn windowCount(self: *const WindowStore) usize {
        return self.windows.count();
    }
};

/// Session store for session windows.
///
/// Groups events into sessions based on inactivity gaps.
pub const SessionStore = struct {
    sessions: std.StringHashMap(Session),
    inactivity_gap_ms: i64,
    allocator: Allocator,

    pub const Session = struct {
        start_ms: i64,
        end_ms: i64,
        record_count: u64 = 0,
        value: ?[]u8 = null,
    };

    pub fn init(alloc: Allocator, inactivity_gap_ms: i64) SessionStore {
        return .{
            .sessions = std.StringHashMap(Session).init(alloc),
            .inactivity_gap_ms = inactivity_gap_ms,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *SessionStore) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.value) |v| self.allocator.free(v);
            self.allocator.free(entry.key_ptr.*);
        }
        self.sessions.deinit();
    }

    /// Add an event to a session. Creates new session or extends existing one.
    pub fn put(self: *SessionStore, key: []const u8, value: []const u8, timestamp: i64) !void {
        const duped_key = try self.allocator.dupe(u8, key);
        const entry = try self.sessions.getOrPut(duped_key);
        if (entry.found_existing) {
            // The duped key is not needed — the map already has an equivalent key
            self.allocator.free(duped_key);
            // Check if event falls within inactivity gap
            if (timestamp - entry.value_ptr.end_ms <= self.inactivity_gap_ms) {
                // Extend session
                entry.value_ptr.end_ms = timestamp;
                entry.value_ptr.record_count += 1;
                if (entry.value_ptr.value) |v| self.allocator.free(v);
                entry.value_ptr.value = try self.allocator.dupe(u8, value);
            } else {
                // Start new session (replace old)
                if (entry.value_ptr.value) |v| self.allocator.free(v);
                entry.value_ptr.* = .{
                    .start_ms = timestamp,
                    .end_ms = timestamp,
                    .record_count = 1,
                    .value = try self.allocator.dupe(u8, value),
                };
            }
        } else {
            entry.value_ptr.* = .{
                .start_ms = timestamp,
                .end_ms = timestamp,
                .record_count = 1,
                .value = try self.allocator.dupe(u8, value),
            };
        }
    }

    pub fn getSession(self: *const SessionStore, key: []const u8) ?Session {
        return self.sessions.get(key);
    }

    pub fn sessionCount(self: *const SessionStore) usize {
        return self.sessions.count();
    }
};

/// Serde interface for key/value serialization.
pub const Serde = struct {
    pub const StringSerde = struct {
        pub fn serialize(data: []const u8) []const u8 {
            return data;
        }

        pub fn deserialize(data: []const u8) []const u8 {
            return data;
        }
    };

    pub const I64Serde = struct {
        pub fn serialize(buf: *[8]u8, value: i64) []const u8 {
            std.mem.writeInt(i64, buf, value, .big);
            return buf;
        }

        pub fn deserialize(data: []const u8) i64 {
            if (data.len < 8) return 0;
            return std.mem.readInt(i64, data[0..8], .big);
        }
    };
};

/// Streams configuration.
pub const StreamsConfig = struct {
    application_id: []const u8,
    bootstrap_servers: []const u8 = "localhost:9092",
    num_stream_threads: u32 = 1,
    commit_interval_ms: i64 = 30000,
    cache_max_bytes: u64 = 10 * 1024 * 1024,
    default_key_serde: []const u8 = "string",
    default_value_serde: []const u8 = "string",
    processing_guarantee: ProcessingGuarantee = .at_least_once,

    pub const ProcessingGuarantee = enum {
        at_least_once,
        exactly_once,
        exactly_once_v2,
    };
};

test "KStream filter" {
    var stream = KStream.init(testing.allocator);
    defer stream.deinit();

    try stream.addRecord(.{ .key = "a", .value = "hi", .timestamp = 1 });
    try stream.addRecord(.{ .key = "b", .value = "hello", .timestamp = 2 });
    try stream.addRecord(.{ .key = "c", .value = "yo", .timestamp = 3 });

    var filtered = try stream.filter(testing.allocator, &testFilter);
    defer filtered.deinit();

    try testing.expectEqual(@as(usize, 1), filtered.count());
    try testing.expectEqualStrings("hello", filtered.toList()[0].value.?);
}

test "ProcessorContext forward" {
    var store = KeyValueStore.init(testing.allocator, "ctx-store");
    defer store.deinit();

    var ctx = ProcessorContext.init(testing.allocator, &store);
    defer ctx.deinit();

    try ctx.forward(.{ .key = "k", .value = "v", .timestamp = 100 });
    try testing.expectEqual(@as(usize, 1), ctx.output_records.items.len);
}

test "WindowStore basic windowing" {
    var store = WindowStore.init(testing.allocator, "click-counts", 60000); // 1 minute windows
    defer store.deinit();

    // Add events to same window
    try store.put("user-1", "click-a", 1000);
    try store.put("user-1", "click-b", 30000);
    try store.put("user-1", "click-c", 90000); // different window

    // First window (0-60000) should have 2 events
    try testing.expectEqual(@as(usize, 2), store.fetchCount("user-1", 0));
    // Second window (60000-120000) should have 1 event
    try testing.expectEqual(@as(usize, 1), store.fetchCount("user-1", 60000));
}

test "SessionStore session detection" {
    var store = SessionStore.init(testing.allocator, 5000); // 5 second gap
    defer store.deinit();

    // Events within gap → same session
    try store.put("user-1", "event-1", 1000);
    try store.put("user-1", "event-2", 3000);
    try store.put("user-1", "event-3", 6000);

    const session = store.getSession("user-1").?;
    try testing.expectEqual(@as(i64, 1000), session.start_ms);
    try testing.expectEqual(@as(i64, 6000), session.end_ms);
    try testing.expectEqual(@as(u64, 3), session.record_count);
}

test "StreamsConfig defaults" {
    const config = StreamsConfig{ .application_id = "word-count" };
    try testing.expectEqualStrings("word-count", config.application_id);
    try testing.expectEqual(@as(u32, 1), config.num_stream_threads);
    try testing.expectEqual(StreamsConfig.ProcessingGuarantee.at_least_once, config.processing_guarantee);
}

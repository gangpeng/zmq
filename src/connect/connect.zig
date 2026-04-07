const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Kafka Connect — connector framework.
///
/// Core abstractions:
/// - Connector: configuration and task management
/// - Task: actual data movement (SourceTask reads, SinkTask writes)
/// - ConnectRecord: data record flowing through connectors

/// A record flowing through the Connect pipeline.
pub const ConnectRecord = struct {
    topic: []const u8,
    partition: ?i32 = null,
    key: ?[]const u8 = null,
    value: ?[]const u8 = null,
    timestamp: i64 = 0,
    headers: ?[]const Header = null,

    pub const Header = struct {
        key: []const u8,
        value: ?[]const u8,
    };
};

/// Source connector interface — reads from external system.
pub const SourceConnector = struct {
    name: []const u8,
    config: std.StringHashMap([]const u8),
    poll_fn: *const fn (alloc: Allocator) ?[]ConnectRecord,
    allocator: Allocator,

    pub fn init(alloc: Allocator, name: []const u8, poll_fn: *const fn (Allocator) ?[]ConnectRecord) SourceConnector {
        return .{
            .name = name,
            .config = std.StringHashMap([]const u8).init(alloc),
            .poll_fn = poll_fn,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *SourceConnector) void {
        self.config.deinit();
    }

    /// Poll for new records from the source.
    pub fn poll(self: *const SourceConnector) ?[]ConnectRecord {
        return self.poll_fn(self.allocator);
    }
};

/// Sink connector interface — writes to external system.
pub const SinkConnector = struct {
    name: []const u8,
    config: std.StringHashMap([]const u8),
    put_fn: *const fn (records: []const ConnectRecord) void,
    allocator: Allocator,

    pub fn init(alloc: Allocator, name: []const u8, put_fn: *const fn ([]const ConnectRecord) void) SinkConnector {
        return .{
            .name = name,
            .config = std.StringHashMap([]const u8).init(alloc),
            .put_fn = put_fn,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *SinkConnector) void {
        self.config.deinit();
    }

    /// Put records to the sink.
    pub fn put(self: *const SinkConnector, records: []const ConnectRecord) void {
        self.put_fn(records);
    }
};

/// Standalone Connect worker — runs connectors in a single process.
pub const StandaloneWorker = struct {
    source_connectors: std.ArrayList(SourceConnector),
    sink_connectors: std.ArrayList(SinkConnector),
    records_produced: u64 = 0,
    records_consumed: u64 = 0,
    allocator: Allocator,

    pub fn init(alloc: Allocator) StandaloneWorker {
        return .{
            .source_connectors = std.ArrayList(SourceConnector).init(alloc),
            .sink_connectors = std.ArrayList(SinkConnector).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *StandaloneWorker) void {
        for (self.source_connectors.items) |*c| c.deinit();
        for (self.sink_connectors.items) |*c| c.deinit();
        self.source_connectors.deinit();
        self.sink_connectors.deinit();
    }

    pub fn addSourceConnector(self: *StandaloneWorker, connector: SourceConnector) !void {
        try self.source_connectors.append(connector);
    }

    pub fn addSinkConnector(self: *StandaloneWorker, connector: SinkConnector) !void {
        try self.sink_connectors.append(connector);
    }

    /// Run one iteration: poll sources, route to sinks.
    pub fn tick(self: *StandaloneWorker) void {
        for (self.source_connectors.items) |*source| {
            if (source.poll()) |records| {
                self.records_produced += records.len;
                // Route to all sinks
                for (self.sink_connectors.items) |*sink| {
                    sink.put(records);
                    self.records_consumed += records.len;
                }
                self.allocator.free(records);
            }
        }
    }

    pub fn connectorCount(self: *const StandaloneWorker) usize {
        return self.source_connectors.items.len + self.sink_connectors.items.len;
    }
};

// ---------------------------------------------------------------
// Built-in connectors
// ---------------------------------------------------------------

/// File source connector — reads lines from a file and produces records.
pub const FileSourceConnector = struct {
    file_path: []const u8,
    topic: []const u8,
    lines_read: u64 = 0,
    allocator: Allocator,

    pub fn init(alloc: Allocator, file_path: []const u8, topic: []const u8) FileSourceConnector {
        return .{
            .file_path = file_path,
            .topic = topic,
            .allocator = alloc,
        };
    }

    /// Read all lines from the file and return as ConnectRecords.
    pub fn readAll(self: *FileSourceConnector) ![]ConnectRecord {
        const file = try std.fs.openFileAbsolute(self.file_path, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, 100 * 1024 * 1024); // max 100MB
        defer self.allocator.free(content);

        // Count lines
        var line_count: usize = 0;
        for (content) |c| {
            if (c == '\n') line_count += 1;
        }
        if (content.len > 0 and content[content.len - 1] != '\n') line_count += 1;

        var records = try self.allocator.alloc(ConnectRecord, line_count);
        var idx: usize = 0;
        var start: usize = 0;

        for (content, 0..) |c, i| {
            if (c == '\n') {
                const line = content[start..i];
                if (line.len > 0 and idx < line_count) {
                    records[idx] = .{
                        .topic = self.topic,
                        .value = try self.allocator.dupe(u8, line),
                        .timestamp = std.time.milliTimestamp(),
                    };
                    idx += 1;
                }
                start = i + 1;
            }
        }

        // Last line without newline
        if (start < content.len and idx < line_count) {
            records[idx] = .{
                .topic = self.topic,
                .value = try self.allocator.dupe(u8, content[start..]),
                .timestamp = std.time.milliTimestamp(),
            };
            idx += 1;
        }

        self.lines_read += idx;
        return records[0..idx];
    }
};

/// File sink connector — writes records to a file (one value per line).
pub const FileSinkConnector = struct {
    file_path: []const u8,
    records_written: u64 = 0,
    allocator: Allocator,

    pub fn init(alloc: Allocator, file_path: []const u8) FileSinkConnector {
        return .{
            .file_path = file_path,
            .allocator = alloc,
        };
    }

    /// Write records to the output file.
    pub fn writeRecords(self: *FileSinkConnector, records: []const ConnectRecord) !void {
        const file = try std.fs.createFileAbsolute(self.file_path, .{
            .truncate = false,
        });
        defer file.close();

        // Seek to end for append
        file.seekFromEnd(0) catch {};

        for (records) |rec| {
            if (rec.value) |val| {
                try file.writeAll(val);
                try file.writeAll("\n");
                self.records_written += 1;
            }
        }
    }
};

/// Connector manager — manages lifecycle of all connectors.
pub const ConnectorManager = struct {
    workers: std.ArrayList(StandaloneWorker),
    file_sources: std.ArrayList(FileSourceConnector),
    file_sinks: std.ArrayList(FileSinkConnector),
    allocator: Allocator,

    pub fn init(alloc: Allocator) ConnectorManager {
        return .{
            .workers = std.ArrayList(StandaloneWorker).init(alloc),
            .file_sources = std.ArrayList(FileSourceConnector).init(alloc),
            .file_sinks = std.ArrayList(FileSinkConnector).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ConnectorManager) void {
        for (self.workers.items) |*w| w.deinit();
        self.workers.deinit();
        self.file_sources.deinit();
        self.file_sinks.deinit();
    }

    pub fn addFileSource(self: *ConnectorManager, file_path: []const u8, topic: []const u8) !void {
        try self.file_sources.append(FileSourceConnector.init(self.allocator, file_path, topic));
    }

    pub fn addFileSink(self: *ConnectorManager, file_path: []const u8) !void {
        try self.file_sinks.append(FileSinkConnector.init(self.allocator, file_path));
    }

    pub fn connectorCount(self: *const ConnectorManager) usize {
        return self.file_sources.items.len + self.file_sinks.items.len + self.workers.items.len;
    }
};

/// Console sink — prints records to stdout.
var console_sink_count: usize = 0;

pub fn consoleSinkPut(records: []const ConnectRecord) void {
    console_sink_count += records.len;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

var test_poll_calls: usize = 0;

fn testSourcePoll(alloc: Allocator) ?[]ConnectRecord {
    test_poll_calls += 1;
    if (test_poll_calls <= 1) {
        var records = alloc.alloc(ConnectRecord, 2) catch return null;
        records[0] = .{ .topic = "test", .value = "record-1", .timestamp = 1 };
        records[1] = .{ .topic = "test", .value = "record-2", .timestamp = 2 };
        return records;
    }
    return null;
}

var test_sink_records: usize = 0;

fn testSinkPut(records: []const ConnectRecord) void {
    test_sink_records += records.len;
}

test "StandaloneWorker source→sink pipeline" {
    test_poll_calls = 0;
    test_sink_records = 0;

    var worker = StandaloneWorker.init(testing.allocator);
    defer worker.deinit();

    try worker.addSourceConnector(SourceConnector.init(testing.allocator, "test-source", &testSourcePoll));
    try worker.addSinkConnector(SinkConnector.init(testing.allocator, "test-sink", &testSinkPut));

    try testing.expectEqual(@as(usize, 2), worker.connectorCount());

    worker.tick(); // should produce 2 records and route to sink
    try testing.expectEqual(@as(u64, 2), worker.records_produced);
    try testing.expectEqual(@as(usize, 2), test_sink_records);

    worker.tick(); // no more records
    try testing.expectEqual(@as(u64, 2), worker.records_produced);
}

test "ConnectRecord creation" {
    const rec = ConnectRecord{
        .topic = "my-topic",
        .key = "key1",
        .value = "value1",
        .timestamp = 1704067200000,
    };
    try testing.expectEqualStrings("my-topic", rec.topic);
    try testing.expectEqualStrings("key1", rec.key.?);
}

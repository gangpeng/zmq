const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

/// Structured JSON logger for production observability.
///
/// Produces one JSON object per line (newline-delimited JSON):
///   {"ts":"2026-04-10T12:34:56.789Z","level":"info","msg":"request handled","correlation_id":42}
///
/// NOTE: AutoMQ uses SLF4J + Logback with JSON layout for structured logging
/// in production. ZMQ implements a lightweight equivalent without external
/// dependencies, outputting to stderr (which can be captured by container
/// runtimes and log aggregators like Fluentd or Vector).
pub const JsonLogger = struct {
    allocator: Allocator,
    /// Output buffer for building JSON log lines. Reused across calls to avoid allocation.
    output: std.ArrayList(u8),
    /// Optional writer for testing — when null, writes to stderr.
    test_writer: ?*std.ArrayList(u8) = null,

    pub const Level = enum {
        debug,
        info,
        warn,
        err,

        pub fn string(self: Level) []const u8 {
            return switch (self) {
                .debug => "debug",
                .info => "info",
                .warn => "warn",
                .err => "error",
            };
        }
    };

    pub fn init(alloc: Allocator) JsonLogger {
        return .{
            .allocator = alloc,
            .output = std.ArrayList(u8).init(alloc),
        };
    }

    pub fn initWithWriter(alloc: Allocator, writer: *std.ArrayList(u8)) JsonLogger {
        return .{
            .allocator = alloc,
            .output = std.ArrayList(u8).init(alloc),
            .test_writer = writer,
        };
    }

    pub fn deinit(self: *JsonLogger) void {
        self.output.deinit();
    }

    /// Log a message with level and optional correlation_id.
    pub fn log(self: *JsonLogger, level: Level, msg: []const u8, correlation_id: ?i32) void {
        self.logWithFields(level, msg, correlation_id, &.{});
    }

    /// Log a message with level, optional correlation_id, and extra key-value fields.
    /// Fields are passed as pairs: &.{ "api_key", "18", "client_id", "test-client" }
    pub fn logWithFields(self: *JsonLogger, level: Level, msg: []const u8, correlation_id: ?i32, fields: []const []const u8) void {
        self.output.clearRetainingCapacity();
        const writer = self.output.writer();

        writer.writeAll("{\"ts\":\"") catch return;
        self.writeTimestamp(writer) catch return;
        writer.writeAll("\",\"level\":\"") catch return;
        writer.writeAll(level.string()) catch return;
        writer.writeAll("\",\"msg\":\"") catch return;
        writeJsonEscaped(writer, msg) catch return;
        writer.writeByte('"') catch return;

        if (correlation_id) |cid| {
            writer.print(",\"correlation_id\":{d}", .{cid}) catch return;
        }

        // Write extra fields as key-value pairs
        var i: usize = 0;
        while (i + 1 < fields.len) : (i += 2) {
            writer.writeAll(",\"") catch return;
            writeJsonEscaped(writer, fields[i]) catch return;
            writer.writeAll("\":\"") catch return;
            writeJsonEscaped(writer, fields[i + 1]) catch return;
            writer.writeByte('"') catch return;
        }

        writer.writeAll("}\n") catch return;

        // Write to test buffer or stderr
        if (self.test_writer) |tw| {
            tw.appendSlice(self.output.items) catch {};
        } else {
            std.io.getStdErr().writeAll(self.output.items) catch {};
        }
    }

    /// Write ISO 8601 timestamp in UTC.
    fn writeTimestamp(self: *JsonLogger, writer: anytype) !void {
        _ = self;
        const ts_ms = std.time.milliTimestamp();
        // Convert to epoch seconds and milliseconds
        const ts_s = @divTrunc(ts_ms, 1000);
        const ms_frac: u64 = @intCast(@mod(ts_ms, 1000));

        // Use Zig's epoch seconds to get date/time components
        const epoch_seconds = std.time.epoch.EpochSeconds{ .secs = @intCast(ts_s) };
        const day_seconds = epoch_seconds.getDaySeconds();
        const year_day = epoch_seconds.getEpochDay().calculateYearDay();

        const month_day = year_day.calculateMonthDay();

        try writer.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z", .{
            year_day.year,
            month_day.month.numeric(),
            month_day.day_index + 1,
            day_seconds.getHoursIntoDay(),
            day_seconds.getMinutesIntoHour(),
            day_seconds.getSecondsIntoMinute(),
            ms_frac,
        });
    }

    /// Write a string with JSON escaping (quotes, backslashes, control chars).
    fn writeJsonEscaped(writer: anytype, s: []const u8) !void {
        for (s) |c| {
            switch (c) {
                '"' => try writer.writeAll("\\\""),
                '\\' => try writer.writeAll("\\\\"),
                '\n' => try writer.writeAll("\\n"),
                '\r' => try writer.writeAll("\\r"),
                '\t' => try writer.writeAll("\\t"),
                else => {
                    if (c < 0x20) {
                        try writer.print("\\u{d:0>4}", .{c});
                    } else {
                        try writer.writeByte(c);
                    }
                },
            }
        }
    }

    /// Format a JSON log line into an allocator-owned buffer (for testing).
    pub fn formatLine(self: *JsonLogger, alloc: Allocator, level: Level, msg: []const u8, correlation_id: ?i32) ![]u8 {
        var buf = std.ArrayList(u8).init(alloc);
        const writer = buf.writer();

        try writer.writeAll("{\"ts\":\"");
        try self.writeTimestamp(writer);
        try writer.writeAll("\",\"level\":\"");
        try writer.writeAll(level.string());
        try writer.writeAll("\",\"msg\":\"");
        try writeJsonEscaped(writer, msg);
        try writer.writeByte('"');

        if (correlation_id) |cid| {
            try writer.print(",\"correlation_id\":{d}", .{cid});
        }

        try writer.writeAll("}\n");
        return buf.toOwnedSlice();
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "JsonLogger produces valid JSON output" {
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(testing.allocator, &output_buf);
    defer logger.deinit();

    logger.log(.info, "test message", 42);

    const output = output_buf.items;
    // Must contain all required JSON fields
    try testing.expect(std.mem.indexOf(u8, output, "\"ts\":\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"level\":\"info\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"msg\":\"test message\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"correlation_id\":42") != null);
    // Must end with newline (NDJSON)
    try testing.expect(output[output.len - 1] == '\n');
    // Must start with { and end with }\n
    try testing.expect(output[0] == '{');
}

test "JsonLogger handles null correlation_id" {
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(testing.allocator, &output_buf);
    defer logger.deinit();

    logger.log(.warn, "no correlation", null);

    const output = output_buf.items;
    try testing.expect(std.mem.indexOf(u8, output, "\"level\":\"warn\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"msg\":\"no correlation\"") != null);
    // correlation_id should NOT appear
    try testing.expect(std.mem.indexOf(u8, output, "correlation_id") == null);
}

test "JsonLogger escapes special characters" {
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(testing.allocator, &output_buf);
    defer logger.deinit();

    logger.log(.err, "line1\nline2\ttab\"quote\\back", null);

    const output = output_buf.items;
    try testing.expect(std.mem.indexOf(u8, output, "line1\\nline2\\ttab\\\"quote\\\\back") != null);
}

test "JsonLogger logWithFields includes extra fields" {
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(testing.allocator, &output_buf);
    defer logger.deinit();

    logger.logWithFields(.info, "API request", 7, &.{ "api_key", "18", "client_id", "test-client" });

    const output = output_buf.items;
    try testing.expect(std.mem.indexOf(u8, output, "\"api_key\":\"18\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"client_id\":\"test-client\"") != null);
    try testing.expect(std.mem.indexOf(u8, output, "\"correlation_id\":7") != null);
}

test "JsonLogger all log levels" {
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(testing.allocator, &output_buf);
    defer logger.deinit();

    logger.log(.debug, "d", null);
    try testing.expect(std.mem.indexOf(u8, output_buf.items, "\"level\":\"debug\"") != null);

    output_buf.clearRetainingCapacity();
    logger.log(.info, "i", null);
    try testing.expect(std.mem.indexOf(u8, output_buf.items, "\"level\":\"info\"") != null);

    output_buf.clearRetainingCapacity();
    logger.log(.warn, "w", null);
    try testing.expect(std.mem.indexOf(u8, output_buf.items, "\"level\":\"warn\"") != null);

    output_buf.clearRetainingCapacity();
    logger.log(.err, "e", null);
    try testing.expect(std.mem.indexOf(u8, output_buf.items, "\"level\":\"error\"") != null);
}

test "JsonLogger formatLine returns allocator-owned buffer" {
    var logger = JsonLogger.init(testing.allocator);
    defer logger.deinit();

    const line = try logger.formatLine(testing.allocator, .info, "hello", 99);
    defer testing.allocator.free(line);

    try testing.expect(std.mem.indexOf(u8, line, "\"msg\":\"hello\"") != null);
    try testing.expect(std.mem.indexOf(u8, line, "\"correlation_id\":99") != null);
    try testing.expect(line[line.len - 1] == '\n');
}

test "JsonLogger timestamp has ISO 8601 format" {
    var output_buf = std.ArrayList(u8).init(testing.allocator);
    defer output_buf.deinit();

    var logger = JsonLogger.initWithWriter(testing.allocator, &output_buf);
    defer logger.deinit();

    logger.log(.info, "ts test", null);

    const output = output_buf.items;
    // Timestamp should contain 'T' separator and 'Z' suffix
    try testing.expect(std.mem.indexOf(u8, output, "T") != null);
    try testing.expect(std.mem.indexOf(u8, output, "Z\"") != null);
}

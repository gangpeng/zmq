const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = @import("fs_compat");

/// Kafka-style properties file parser (server.properties format).
///
/// Supports:
/// - key=value pairs
/// - # comments
/// - Whitespace trimming
/// - Multi-line values are NOT supported (same as Java Properties)
pub const ConfigFile = struct {
    props: std.StringHashMap([]const u8),
    allocator: Allocator,

    pub fn init(alloc: Allocator) ConfigFile {
        return .{
            .props = std.StringHashMap([]const u8).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ConfigFile) void {
        var it = self.props.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.props.deinit();
    }

    /// Load a properties file.
    pub fn load(self: *ConfigFile, path: []const u8) !void {
        const file = try fs.cwd().openFile(path, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, 1024 * 1024);
        defer self.allocator.free(content);

        try self.parse(content);
    }

    /// Load from an absolute path.
    pub fn loadAbsolute(self: *ConfigFile, path: []const u8) !void {
        const file = try fs.openFileAbsolute(path, .{});
        defer file.close();

        const content = try file.readToEndAlloc(self.allocator, 1024 * 1024);
        defer self.allocator.free(content);

        try self.parse(content);
    }

    /// Parse properties content string.
    pub fn parse(self: *ConfigFile, content: []const u8) !void {
        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r");
            if (line.len == 0) continue;
            if (line[0] == '#' or line[0] == '!') continue;

            // Find the = delimiter
            const eq_pos = std.mem.indexOf(u8, line, "=") orelse continue;
            const key = std.mem.trim(u8, line[0..eq_pos], " \t");
            const value = std.mem.trim(u8, line[eq_pos + 1 ..], " \t");

            if (key.len == 0) continue;

            // Remove old entry if exists
            if (self.props.fetchRemove(key)) |old| {
                self.allocator.free(old.key);
                self.allocator.free(old.value);
            }

            const key_copy = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(key_copy);
            const value_copy = try self.allocator.dupe(u8, value);

            try self.props.put(key_copy, value_copy);
        }
    }

    /// Get a string property.
    pub fn getString(self: *const ConfigFile, key: []const u8) ?[]const u8 {
        return self.props.get(key);
    }

    /// Get a string property with default.
    pub fn getStringOr(self: *const ConfigFile, key: []const u8, default: []const u8) []const u8 {
        return self.props.get(key) orelse default;
    }

    /// Get an integer property.
    pub fn getInt(self: *const ConfigFile, comptime T: type, key: []const u8, default: T) T {
        const str = self.props.get(key) orelse return default;
        return std.fmt.parseInt(T, str, 10) catch default;
    }

    /// Get an integer property, failing when the property exists but is invalid.
    pub fn getIntStrict(self: *const ConfigFile, comptime T: type, key: []const u8) !?T {
        const str = self.props.get(key) orelse return null;
        return std.fmt.parseInt(T, str, 10) catch error.InvalidConfigInteger;
    }

    /// Get a boolean property.
    pub fn getBool(self: *const ConfigFile, key: []const u8, default: bool) bool {
        const str = self.props.get(key) orelse return default;
        if (std.mem.eql(u8, str, "true")) return true;
        if (std.mem.eql(u8, str, "false")) return false;
        return default;
    }

    /// Number of properties loaded.
    pub fn count(self: *const ConfigFile) usize {
        return self.props.count();
    }
};

pub const ControllerVoter = struct {
    node_id: i32,
    host: []const u8,
    port: u16,
};

pub fn parseControllerVoter(raw: []const u8) !ControllerVoter {
    const entry = std.mem.trim(u8, raw, " \t\r\n");
    if (entry.len == 0) return error.InvalidControllerVoter;

    const at_pos = std.mem.indexOfScalar(u8, entry, '@') orelse return error.InvalidControllerVoter;
    if (at_pos == 0 or at_pos + 1 >= entry.len) return error.InvalidControllerVoter;

    const node_id = std.fmt.parseInt(i32, entry[0..at_pos], 10) catch return error.InvalidControllerVoter;
    if (node_id < 0) return error.InvalidControllerVoter;
    const addr = entry[at_pos + 1 ..];
    const colon = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidControllerVoter;
    if (colon == 0 or colon + 1 >= addr.len) return error.InvalidControllerVoter;

    const host = std.mem.trim(u8, addr[0..colon], " \t\r\n");
    const port_text = std.mem.trim(u8, addr[colon + 1 ..], " \t\r\n");
    if (host.len == 0 or port_text.len == 0) return error.InvalidControllerVoter;

    const port = std.fmt.parseInt(u16, port_text, 10) catch return error.InvalidControllerVoter;
    if (port == 0) return error.InvalidControllerVoter;

    return .{
        .node_id = node_id,
        .host = host,
        .port = port,
    };
}

/// Apply config file properties to BrokerConfig.
/// Supports the following Kafka-standard properties:
/// - s3.endpoint.host, s3.endpoint.port, s3.bucket, s3.access.key, s3.secret.key, s3.scheme, s3.region, s3.path.style, s3.tls.ca.file
/// - log.dirs (data directory)
/// - num.partitions (default partition count for auto-created topics)
/// - default.replication.factor
/// - auto.create.topics.enable
/// - advertised.listeners (host extraction)
pub fn applyConfig(config: *@import("broker").Broker.BrokerConfig, cfg: *const ConfigFile) void {
    if (cfg.getString("s3.endpoint.host")) |h| config.s3_endpoint_host = h;
    config.s3_endpoint_port = cfg.getInt(u16, "s3.endpoint.port", config.s3_endpoint_port);
    if (cfg.getString("s3.bucket")) |b| config.s3_bucket = b;
    if (cfg.getString("s3.access.key")) |k| config.s3_access_key = k;
    if (cfg.getString("s3.secret.key")) |k| config.s3_secret_key = k;
    if (cfg.getString("s3.scheme")) |s| {
        if (std.ascii.eqlIgnoreCase(s, "https")) {
            config.s3_scheme = .https;
        } else if (std.ascii.eqlIgnoreCase(s, "http")) {
            config.s3_scheme = .http;
        }
    }
    if (cfg.getString("s3.region")) |r| config.s3_region = r;
    config.s3_path_style = cfg.getBool("s3.path.style", config.s3_path_style);
    if (cfg.getString("s3.tls.ca.file")) |f| config.s3_tls_ca_file = f;
    if (cfg.getString("log.dirs")) |d| config.data_dir = d;

    // Additional Kafka-standard config properties
    config.default_num_partitions = cfg.getInt(i32, "num.partitions", config.default_num_partitions);
    config.default_replication_factor = cfg.getInt(i16, "default.replication.factor", config.default_replication_factor);
    config.auto_create_topics = cfg.getBool("auto.create.topics.enable", config.auto_create_topics);
    if (cfg.getString("advertised.host.name")) |h| config.advertised_host = h;

    // Apply S3 WAL and cache configuration from properties file
    config.s3_wal_batch_size = @intCast(cfg.getInt(u64, "s3.wal.batch.size", @intCast(config.s3_wal_batch_size)));
    config.s3_wal_flush_interval_ms = cfg.getInt(i64, "s3.wal.flush.interval.ms", config.s3_wal_flush_interval_ms);
    if (cfg.getString("s3.wal.flush.mode")) |m| {
        const std_mem = @import("std").mem;
        if (std_mem.eql(u8, m, "async")) {
            config.s3_wal_flush_mode = .async_flush;
        } else if (std_mem.eql(u8, m, "group_commit")) {
            config.s3_wal_flush_mode = .group_commit;
        } else {
            config.s3_wal_flush_mode = .sync;
        }
    }
    config.s3_block_cache_size = @intCast(cfg.getInt(u64, "s3.block.cache.size", @intCast(config.s3_block_cache_size)));
    config.cache_max_size = @intCast(cfg.getInt(u64, "log.cache.max.size", @intCast(config.cache_max_size)));
    config.compaction_interval_ms = cfg.getInt(i64, "s3.compaction.interval.ms", config.compaction_interval_ms);

    // Security configuration
    config.sasl_enabled = cfg.getBool("sasl.enabled", config.sasl_enabled);
    if (cfg.getString("sasl.users")) |u| config.sasl_users = u;
    if (cfg.getString("super.users")) |u| config.super_users = u;
    config.allow_everyone_if_no_acl = cfg.getBool("allow.everyone.if.no.acl.found", config.allow_everyone_if_no_acl);
    if (cfg.getString("sasl.enabled.mechanisms")) |m| config.sasl_enabled_mechanisms = m;
    if (cfg.getString("sasl.oauthbearer.expected.issuer")) |i| config.oauth_issuer = i;
    if (cfg.getString("sasl.oauthbearer.expected.audience")) |a| config.oauth_audience = a;

    // TLS configuration from config file
    // NOTE: AutoMQ uses Java's ssl.* properties (JKS keystore format). ZMQ uses
    // PEM-based cert/key files because Zig/OpenSSL has no JKS support. The property
    // names (ssl.certfile, ssl.keyfile, etc.) follow the PEM convention.
    if (cfg.getString("security.protocol")) |p| config.security_protocol = p;
    if (cfg.getString("ssl.certfile")) |f| config.tls_cert_file = f;
    if (cfg.getString("ssl.keyfile")) |f| config.tls_key_file = f;
    if (cfg.getString("ssl.cafile")) |f| config.tls_ca_file = f;
    if (cfg.getString("ssl.client.auth")) |a| config.tls_client_auth = a;

    // Client telemetry export configuration. Accepted PushTelemetry payloads are
    // written as opaque base64 JSONL records to this append-only sink.
    if (cfg.getString("client.telemetry.export.file")) |f| config.client_telemetry_export_path = f;
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "ConfigFile parse basic properties" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse(
        \\# This is a comment
        \\broker.id=0
        \\listeners=PLAINTEXT://0.0.0.0:9092
        \\log.dirs=/data/kafka
        \\num.partitions=3
        \\auto.create.topics.enable=true
        \\s3.endpoint.port=9000
        \\
        \\# Another comment
        \\  key.with.spaces = value with spaces
    );

    try testing.expectEqualStrings("0", cfg.getString("broker.id").?);
    try testing.expectEqualStrings("PLAINTEXT://0.0.0.0:9092", cfg.getString("listeners").?);
    try testing.expectEqualStrings("/data/kafka", cfg.getString("log.dirs").?);
    try testing.expectEqual(@as(i32, 3), cfg.getInt(i32, "num.partitions", 1));
    try testing.expect(cfg.getBool("auto.create.topics.enable", false));
    try testing.expectEqual(@as(u16, 9000), cfg.getInt(u16, "s3.endpoint.port", 9000));
    try testing.expectEqualStrings("value with spaces", cfg.getString("key.with.spaces").?);
}

test "ConfigFile getters with defaults" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try testing.expectEqual(@as(i32, 42), cfg.getInt(i32, "nonexistent", 42));
    try testing.expect(!cfg.getBool("nonexistent", false));
    try testing.expectEqualStrings("default", cfg.getStringOr("nonexistent", "default"));
    try testing.expect(cfg.getString("nonexistent") == null);
}

test "ConfigFile getIntStrict rejects malformed integers" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse(
        \\broker.id=7
        \\controller.listener.port=bad
    );

    try testing.expectEqual(@as(i32, 7), (try cfg.getIntStrict(i32, "broker.id")).?);
    try testing.expect((try cfg.getIntStrict(u16, "missing")) == null);
    try testing.expectError(error.InvalidConfigInteger, cfg.getIntStrict(u16, "controller.listener.port"));
}

test "ConfigFile override duplicate keys" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("key=first\nkey=second\n");
    try testing.expectEqualStrings("second", cfg.getString("key").?);
    try testing.expectEqual(@as(usize, 1), cfg.count());
}

test "ConfigFile empty value" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("empty.key=\nnormal.key=value\n");
    try testing.expectEqualStrings("", cfg.getString("empty.key").?);
    try testing.expectEqualStrings("value", cfg.getString("normal.key").?);
    try testing.expectEqual(@as(usize, 2), cfg.count());
}

test "ConfigFile skip malformed lines" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("valid=yes\nno-equals-sign\n!bang comment\n=no-key\nalso.valid=1\n");
    try testing.expectEqual(@as(usize, 2), cfg.count());
    try testing.expectEqualStrings("yes", cfg.getString("valid").?);
    try testing.expectEqualStrings("1", cfg.getString("also.valid").?);
}

test "ConfigFile values with equals sign" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("url=http://host:9000/path?a=1&b=2\n");
    try testing.expectEqualStrings("http://host:9000/path?a=1&b=2", cfg.getString("url").?);
}

test "parseControllerVoter parses strict controller quorum entries" {
    const voter = try parseControllerVoter(" 1@controller-1:9093 ");
    try testing.expectEqual(@as(i32, 1), voter.node_id);
    try testing.expectEqualStrings("controller-1", voter.host);
    try testing.expectEqual(@as(u16, 9093), voter.port);

    const ipv6 = try parseControllerVoter("2@::1:19093");
    try testing.expectEqual(@as(i32, 2), ipv6.node_id);
    try testing.expectEqualStrings("::1", ipv6.host);
    try testing.expectEqual(@as(u16, 19093), ipv6.port);
}

test "parseControllerVoter rejects malformed controller quorum entries" {
    const invalid = [_][]const u8{
        "",
        "localhost:9093",
        "x@localhost:9093",
        "-1@localhost:9093",
        "1@:9093",
        "1@localhost:",
        "1@localhost:notaport",
        "1@localhost:0",
        "1@localhost:70000",
    };
    for (invalid) |entry| {
        try testing.expectError(error.InvalidControllerVoter, parseControllerVoter(entry));
    }
}

test "ConfigFile applies client telemetry export sink" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("client.telemetry.export.file=/var/lib/zmq/client-telemetry.jsonl\n");

    var broker_config = @import("broker").Broker.BrokerConfig{};
    applyConfig(&broker_config, &cfg);
    try testing.expectEqualStrings("/var/lib/zmq/client-telemetry.jsonl", broker_config.client_telemetry_export_path.?);
}

test "ConfigFile applies S3 provider region and addressing" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse(
        \\s3.scheme=https
        \\s3.region=us-west-2
        \\s3.path.style=false
    );

    var broker_config = @import("broker").Broker.BrokerConfig{};
    applyConfig(&broker_config, &cfg);
    try testing.expectEqual(@import("storage").S3Client.Scheme.https, broker_config.s3_scheme);
    try testing.expectEqualStrings("us-west-2", broker_config.s3_region);
    try testing.expect(!broker_config.s3_path_style);
}

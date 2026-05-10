const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = @import("fs_compat");
const Broker = @import("broker").Broker;
const TlsConfig = @import("security").tls.TlsConfig;
const OpenSslLib = @import("security").openssl.OpenSslLib;
const S3Scheme = @import("storage").S3Client.Scheme;
const WalFlushMode = Broker.WalFlushMode;

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

            // Find the = delimiter.
            const eq_pos = std.mem.indexOf(u8, line, "=") orelse return error.InvalidConfigLine;
            const key = std.mem.trim(u8, line[0..eq_pos], " \t");
            const value = std.mem.trim(u8, line[eq_pos + 1 ..], " \t");

            if (key.len == 0) return error.InvalidConfigKey;

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

    /// Get a string property, failing when the property exists but is blank.
    pub fn getNonBlankStringStrict(self: *const ConfigFile, key: []const u8) !?[]const u8 {
        const str = self.props.get(key) orelse return null;
        if (std.mem.trim(u8, str, " \t\r\n").len == 0) return error.InvalidConfigString;
        return str;
    }

    /// Get a string property with default.
    pub fn getStringOr(self: *const ConfigFile, key: []const u8, default: []const u8) []const u8 {
        return self.props.get(key) orelse default;
    }

    /// Get an integer property with a default, failing when the property exists but is invalid.
    pub fn getInt(self: *const ConfigFile, comptime T: type, key: []const u8, default: T) !T {
        const value = try self.getIntStrict(T, key);
        return value orelse default;
    }

    /// Get an integer property, failing when the property exists but is invalid.
    pub fn getIntStrict(self: *const ConfigFile, comptime T: type, key: []const u8) !?T {
        const str = self.props.get(key) orelse return null;
        return std.fmt.parseInt(T, str, 10) catch error.InvalidConfigInteger;
    }

    /// Get a boolean property with a default, failing when the property exists but is invalid.
    pub fn getBool(self: *const ConfigFile, key: []const u8, default: bool) !bool {
        const value = try self.getBoolStrict(key);
        return value orelse default;
    }

    /// Get a boolean property, failing when the property exists but is invalid.
    pub fn getBoolStrict(self: *const ConfigFile, key: []const u8) !?bool {
        const str = self.props.get(key) orelse return null;
        return @as(?bool, try parseBoolStrict(str));
    }

    /// Number of properties loaded.
    pub fn count(self: *const ConfigFile) usize {
        return self.props.count();
    }
};

pub fn firstCommaSeparatedValueStrict(raw: []const u8) ![]const u8 {
    var first: ?[]const u8 = null;
    var parts = std.mem.splitScalar(u8, raw, ',');
    while (parts.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        if (trimmed.len == 0) return error.InvalidConfigString;
        if (first == null) first = trimmed;
    }
    return first orelse error.InvalidConfigString;
}

pub fn parseBoolStrict(text: []const u8) !bool {
    if (std.mem.eql(u8, text, "1") or
        std.ascii.eqlIgnoreCase(text, "true") or
        std.ascii.eqlIgnoreCase(text, "yes") or
        std.ascii.eqlIgnoreCase(text, "on"))
    {
        return true;
    }
    if (std.mem.eql(u8, text, "0") or
        std.ascii.eqlIgnoreCase(text, "false") or
        std.ascii.eqlIgnoreCase(text, "no") or
        std.ascii.eqlIgnoreCase(text, "off"))
    {
        return false;
    }
    return error.InvalidConfigBool;
}

pub fn parseS3SchemeStrict(text: []const u8) !S3Scheme {
    if (std.ascii.eqlIgnoreCase(text, "https")) return .https;
    if (std.ascii.eqlIgnoreCase(text, "http")) return .http;
    return error.InvalidConfigS3Scheme;
}

pub fn parseWalFlushModeStrict(text: []const u8) !WalFlushMode {
    if (std.mem.eql(u8, text, "sync")) return .sync;
    if (std.mem.eql(u8, text, "async")) return .async_flush;
    if (std.mem.eql(u8, text, "group_commit")) return .group_commit;
    return error.InvalidConfigWalFlushMode;
}

pub fn parseSecurityProtocolStrict(text: []const u8) !TlsConfig.SecurityProtocol {
    if (std.ascii.eqlIgnoreCase(text, "plaintext")) return .plaintext;
    if (std.ascii.eqlIgnoreCase(text, "ssl")) return .ssl;
    if (std.ascii.eqlIgnoreCase(text, "sasl_plaintext")) return .sasl_plaintext;
    if (std.ascii.eqlIgnoreCase(text, "sasl_ssl")) return .sasl_ssl;
    return error.InvalidConfigSecurityProtocol;
}

pub fn parseTlsClientAuthStrict(text: []const u8) !TlsConfig.ClientAuth {
    if (std.ascii.eqlIgnoreCase(text, "none")) return .none;
    if (std.ascii.eqlIgnoreCase(text, "requested")) return .requested;
    if (std.ascii.eqlIgnoreCase(text, "required")) return .required;
    return error.InvalidConfigTlsClientAuth;
}

pub fn validateSaslUsersStrict(text: []const u8) !void {
    var entries = std.mem.splitScalar(u8, text, ',');
    while (entries.next()) |entry| {
        const trimmed = std.mem.trim(u8, entry, " \t\r\n");
        if (trimmed.len == 0 or trimmed.len != entry.len) return error.InvalidConfigString;

        const colon = std.mem.indexOfScalar(u8, entry, ':') orelse return error.InvalidConfigString;
        if (colon == 0 or colon + 1 >= entry.len) return error.InvalidConfigString;

        const username = entry[0..colon];
        const password = entry[colon + 1 ..];
        if (std.mem.trim(u8, username, " \t\r\n").len != username.len) return error.InvalidConfigString;
        if (std.mem.trim(u8, password, " \t\r\n").len != password.len) return error.InvalidConfigString;
    }
}

pub fn validateSuperUsersStrict(text: []const u8) !void {
    var entries = std.mem.splitScalar(u8, text, ';');
    while (entries.next()) |entry| {
        const trimmed = std.mem.trim(u8, entry, " \t\r\n");
        if (trimmed.len == 0 or trimmed.len != entry.len) return error.InvalidConfigString;
    }
}

pub fn validateSaslMechanismsStrict(text: []const u8) !void {
    var entries = std.mem.splitScalar(u8, text, ',');
    while (entries.next()) |entry| {
        const trimmed = std.mem.trim(u8, entry, " \t\r\n");
        if (trimmed.len == 0 or trimmed.len != entry.len) return error.InvalidConfigString;
        if (std.mem.eql(u8, entry, "PLAIN")) continue;
        if (std.mem.eql(u8, entry, "SCRAM-SHA-256")) continue;
        if (std.mem.eql(u8, entry, "OAUTHBEARER")) continue;
        return error.InvalidConfigString;
    }
}

pub const ControllerVoter = struct {
    node_id: i32,
    host: []const u8,
    port: u16,
};

pub const ListenerEndpoint = struct {
    name: []const u8,
    host: []const u8,
    port: u16,
};

pub fn validateListenerNameStrict(text: []const u8) !void {
    if (text.len == 0) return error.InvalidConfigListenerName;
    if (std.mem.trim(u8, text, " \t\r\n").len != text.len) return error.InvalidConfigListenerName;

    for (text) |ch| {
        switch (ch) {
            'A'...'Z', 'a'...'z', '0'...'9', '_', '-', '.' => {},
            else => return error.InvalidConfigListenerName,
        }
    }
}

pub fn validateListenerNamesStrict(raw: []const u8) !void {
    var count: usize = 0;
    var entries = std.mem.splitScalar(u8, raw, ',');
    while (entries.next()) |entry| {
        try validateListenerNameStrict(entry);
        var prior_count: usize = 0;
        var prior_entries = std.mem.splitScalar(u8, raw, ',');
        while (prior_entries.next()) |prior_entry| {
            if (prior_count >= count) break;
            if (std.ascii.eqlIgnoreCase(prior_entry, entry)) return error.InvalidConfigListenerName;
            prior_count += 1;
        }
        count += 1;
    }
    if (count == 0) return error.InvalidConfigListenerName;
}

fn validateListenerEndpointNameUniqueBeforeStrict(raw_listeners: []const u8, listener: ListenerEndpoint, current_index: usize, allow_blank_host: bool) !void {
    var prior_index: usize = 0;
    var entries = std.mem.splitScalar(u8, raw_listeners, ',');
    while (entries.next()) |entry| {
        if (prior_index >= current_index) break;
        const prior = try parseListenerEndpointStrict(entry, allow_blank_host);
        if (std.ascii.eqlIgnoreCase(prior.name, listener.name)) return error.InvalidConfigListener;
        prior_index += 1;
    }
}

fn listenerNameInListStrict(raw_names: []const u8, name: []const u8) !bool {
    try validateListenerNamesStrict(raw_names);

    var entries = std.mem.splitScalar(u8, raw_names, ',');
    while (entries.next()) |entry| {
        if (std.ascii.eqlIgnoreCase(entry, name)) return true;
    }
    return false;
}

pub fn parseListenerEndpointStrict(raw: []const u8, allow_blank_host: bool) !ListenerEndpoint {
    const entry = std.mem.trim(u8, raw, " \t\r\n");
    if (entry.len == 0) return error.InvalidConfigListener;

    const scheme_sep = std.mem.indexOf(u8, entry, "://") orelse return error.InvalidConfigListener;
    if (scheme_sep == 0 or scheme_sep + 3 >= entry.len) return error.InvalidConfigListener;

    const raw_name = entry[0..scheme_sep];
    const name = std.mem.trim(u8, raw_name, " \t\r\n");
    if (name.len != raw_name.len) return error.InvalidConfigListener;
    validateListenerNameStrict(name) catch return error.InvalidConfigListener;

    const addr = entry[scheme_sep + 3 ..];
    const colon = std.mem.lastIndexOfScalar(u8, addr, ':') orelse return error.InvalidConfigListener;
    if (colon + 1 >= addr.len) return error.InvalidConfigListener;

    const raw_host = std.mem.trim(u8, addr[0..colon], " \t\r\n");
    const host = if (raw_host.len >= 2 and raw_host[0] == '[' and raw_host[raw_host.len - 1] == ']')
        raw_host[1 .. raw_host.len - 1]
    else
        raw_host;
    if (!allow_blank_host and host.len == 0) return error.InvalidConfigListener;

    const port_text = std.mem.trim(u8, addr[colon + 1 ..], " \t\r\n");
    if (port_text.len == 0) return error.InvalidConfigListener;
    const port = std.fmt.parseInt(u16, port_text, 10) catch return error.InvalidConfigListener;
    if (port == 0) return error.InvalidConfigListener;

    return .{
        .name = name,
        .host = host,
        .port = port,
    };
}

pub fn firstListenerEndpointStrict(raw: []const u8, allow_blank_host: bool) !ListenerEndpoint {
    var first: ?ListenerEndpoint = null;
    var index: usize = 0;
    var entries = std.mem.splitScalar(u8, raw, ',');
    while (entries.next()) |entry| {
        const listener = try parseListenerEndpointStrict(entry, allow_blank_host);
        try validateListenerEndpointNameUniqueBeforeStrict(raw, listener, index, allow_blank_host);
        if (first == null) first = listener;
        index += 1;
    }
    return first orelse error.InvalidConfigListener;
}

pub fn listenerEndpointForNameStrict(raw_listeners: []const u8, raw_name: []const u8, allow_blank_host: bool) !ListenerEndpoint {
    try validateListenerNameStrict(raw_name);

    var match: ?ListenerEndpoint = null;
    var index: usize = 0;
    var entries = std.mem.splitScalar(u8, raw_listeners, ',');
    while (entries.next()) |entry| {
        const listener = try parseListenerEndpointStrict(entry, allow_blank_host);
        try validateListenerEndpointNameUniqueBeforeStrict(raw_listeners, listener, index, allow_blank_host);
        if (match == null and std.ascii.eqlIgnoreCase(listener.name, raw_name)) match = listener;
        index += 1;
    }
    return match orelse error.InvalidConfigListener;
}

pub fn firstListenerEndpointMatchingNamesStrict(raw_listeners: []const u8, raw_names: []const u8, allow_blank_host: bool) !ListenerEndpoint {
    try validateListenerNamesStrict(raw_names);

    var match: ?ListenerEndpoint = null;
    var index: usize = 0;
    var entries = std.mem.splitScalar(u8, raw_listeners, ',');
    while (entries.next()) |entry| {
        const listener = try parseListenerEndpointStrict(entry, allow_blank_host);
        try validateListenerEndpointNameUniqueBeforeStrict(raw_listeners, listener, index, allow_blank_host);
        if (match == null and try listenerNameInListStrict(raw_names, listener.name)) match = listener;
        index += 1;
    }
    return match orelse error.InvalidConfigListener;
}

pub fn firstListenerEndpointExcludingNamesStrict(raw_listeners: []const u8, raw_names: []const u8, allow_blank_host: bool) !ListenerEndpoint {
    try validateListenerNamesStrict(raw_names);

    var first: ?ListenerEndpoint = null;
    var index: usize = 0;
    var entries = std.mem.splitScalar(u8, raw_listeners, ',');
    while (entries.next()) |entry| {
        const listener = try parseListenerEndpointStrict(entry, allow_blank_host);
        try validateListenerEndpointNameUniqueBeforeStrict(raw_listeners, listener, index, allow_blank_host);
        if (first == null and !(try listenerNameInListStrict(raw_names, listener.name))) first = listener;
        index += 1;
    }
    return first orelse error.InvalidConfigListener;
}

fn validateListenerSecurityMapNameUniqueBeforeStrict(raw_map: []const u8, name: []const u8, current_index: usize) !void {
    var prior_index: usize = 0;
    var entries = std.mem.splitScalar(u8, raw_map, ',');
    while (entries.next()) |entry| {
        if (prior_index >= current_index) break;
        const colon = std.mem.indexOfScalar(u8, entry, ':') orelse return error.InvalidConfigListenerSecurityMap;
        const prior_name = entry[0..colon];
        validateListenerNameStrict(prior_name) catch return error.InvalidConfigListenerSecurityMap;
        if (std.ascii.eqlIgnoreCase(prior_name, name)) return error.InvalidConfigListenerSecurityMap;
        prior_index += 1;
    }
}

pub fn validateListenerSecurityProtocolMapStrict(raw: []const u8) !void {
    var count: usize = 0;
    var entries = std.mem.splitScalar(u8, raw, ',');
    while (entries.next()) |entry| {
        if (entry.len == 0 or std.mem.trim(u8, entry, " \t\r\n").len != entry.len) return error.InvalidConfigListenerSecurityMap;
        const colon = std.mem.indexOfScalar(u8, entry, ':') orelse return error.InvalidConfigListenerSecurityMap;
        if (colon == 0 or colon + 1 >= entry.len) return error.InvalidConfigListenerSecurityMap;
        if (std.mem.indexOfScalar(u8, entry[colon + 1 ..], ':') != null) return error.InvalidConfigListenerSecurityMap;

        const name = entry[0..colon];
        validateListenerNameStrict(name) catch return error.InvalidConfigListenerSecurityMap;
        try validateListenerSecurityMapNameUniqueBeforeStrict(raw, name, count);
        _ = parseSecurityProtocolStrict(entry[colon + 1 ..]) catch return error.InvalidConfigListenerSecurityMap;
        count += 1;
    }
    if (count == 0) return error.InvalidConfigListenerSecurityMap;
}

pub fn listenerSecurityProtocolTextForNameStrict(raw_map: []const u8, listener_name: []const u8) ![]const u8 {
    try validateListenerNameStrict(listener_name);
    try validateListenerSecurityProtocolMapStrict(raw_map);

    var entries = std.mem.splitScalar(u8, raw_map, ',');
    while (entries.next()) |entry| {
        const colon = std.mem.indexOfScalar(u8, entry, ':') orelse return error.InvalidConfigListenerSecurityMap;
        if (std.ascii.eqlIgnoreCase(entry[0..colon], listener_name)) return entry[colon + 1 ..];
    }

    return error.InvalidConfigListenerSecurityMap;
}

pub fn listenerSecurityProtocolForNameStrict(raw_map: []const u8, listener_name: []const u8) !TlsConfig.SecurityProtocol {
    const protocol_text = try listenerSecurityProtocolTextForNameStrict(raw_map, listener_name);
    return parseSecurityProtocolStrict(protocol_text) catch error.InvalidConfigListenerSecurityMap;
}

pub fn validateListenerSecurityProtocolMapForListenersStrict(raw_map: []const u8, raw_listeners: []const u8) !void {
    try validateListenerSecurityProtocolMapStrict(raw_map);

    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, raw_listeners, ',');
    while (entries.next()) |entry| {
        const listener = try parseListenerEndpointStrict(entry, true);
        parsed_any = true;
        _ = try listenerSecurityProtocolForNameStrict(raw_map, listener.name);
    }
    if (!parsed_any) return error.InvalidConfigListener;
}

pub fn validateAdvertisedListenersMatchListenersStrict(raw_advertised: []const u8, raw_listeners: []const u8) !void {
    _ = try firstListenerEndpointStrict(raw_advertised, false);
    _ = try firstListenerEndpointStrict(raw_listeners, true);

    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, raw_advertised, ',');
    while (entries.next()) |entry| {
        const advertised = try parseListenerEndpointStrict(entry, false);
        parsed_any = true;
        _ = try listenerEndpointForNameStrict(raw_listeners, advertised.name, true);
    }
    if (!parsed_any) return error.InvalidConfigListener;
}

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

pub fn validateControllerVoterSet(alloc: Allocator, voters: []const u8) !void {
    var parsed = std.array_list.Managed(ControllerVoter).init(alloc);
    defer parsed.deinit();

    var entries = std.mem.splitScalar(u8, voters, ',');
    while (entries.next()) |raw_entry| {
        const voter = try parseControllerVoter(raw_entry);
        for (parsed.items) |previous| {
            if (previous.node_id == voter.node_id) return error.DuplicateControllerVoter;
            if (previous.port == voter.port and std.mem.eql(u8, previous.host, voter.host)) {
                return error.DuplicateControllerVoterEndpoint;
            }
        }
        try parsed.append(voter);
    }

    if (parsed.items.len == 0) return error.InvalidControllerVoter;
}

pub fn controllerVoterSetContainsNodeIdStrict(voters: []const u8, node_id: i32) !bool {
    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, voters, ',');
    while (entries.next()) |raw_entry| {
        const voter = try parseControllerVoter(raw_entry);
        parsed_any = true;
        if (voter.node_id == node_id) return true;
    }
    if (!parsed_any) return error.InvalidControllerVoter;
    return false;
}

pub fn controllerVoterSetLocalPortMatchesStrict(voters: []const u8, node_id: i32, local_port: u16) !bool {
    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, voters, ',');
    while (entries.next()) |raw_entry| {
        const voter = try parseControllerVoter(raw_entry);
        parsed_any = true;
        if (voter.node_id == node_id) return voter.port == local_port;
    }
    if (!parsed_any) return error.InvalidControllerVoter;
    return false;
}

/// Apply config file properties to BrokerConfig.
/// Supports the following Kafka-standard properties:
/// - s3.endpoint.host, s3.endpoint.port, s3.bucket, s3.access.key, s3.secret.key, s3.scheme, s3.region, s3.path.style, s3.tls.ca.file
/// - log.dirs (data directory)
/// - num.partitions (default partition count for auto-created topics)
/// - default.replication.factor
/// - auto.create.topics.enable
/// - cluster.id
/// - advertised.listeners (host extraction)
pub fn applyConfig(config: *Broker.BrokerConfig, cfg: *const ConfigFile) !void {
    if (try cfg.getNonBlankStringStrict("s3.endpoint.host")) |h| config.s3_endpoint_host = h;
    config.s3_endpoint_port = try cfg.getInt(u16, "s3.endpoint.port", config.s3_endpoint_port);
    if (try cfg.getNonBlankStringStrict("s3.bucket")) |b| config.s3_bucket = b;
    if (try cfg.getNonBlankStringStrict("s3.access.key")) |k| config.s3_access_key = k;
    if (try cfg.getNonBlankStringStrict("s3.secret.key")) |k| config.s3_secret_key = k;
    if (cfg.getString("s3.scheme")) |s| config.s3_scheme = try parseS3SchemeStrict(s);
    if (try cfg.getNonBlankStringStrict("s3.region")) |r| config.s3_region = r;
    config.s3_path_style = try cfg.getBool("s3.path.style", config.s3_path_style);
    if (try cfg.getNonBlankStringStrict("s3.tls.ca.file")) |f| config.s3_tls_ca_file = f;
    if (try cfg.getNonBlankStringStrict("log.dirs")) |d| config.data_dir = try firstCommaSeparatedValueStrict(d);

    // Additional Kafka-standard config properties
    config.default_num_partitions = try cfg.getInt(i32, "num.partitions", config.default_num_partitions);
    config.default_replication_factor = try cfg.getInt(i16, "default.replication.factor", config.default_replication_factor);
    config.auto_create_topics = try cfg.getBool("auto.create.topics.enable", config.auto_create_topics);
    if (try cfg.getNonBlankStringStrict("cluster.id")) |id| config.cluster_id = id;
    if (try cfg.getNonBlankStringStrict("advertised.host.name")) |h| config.advertised_host = h;
    const listener_endpoints = try cfg.getNonBlankStringStrict("listeners");
    if (listener_endpoints) |l| _ = try firstListenerEndpointStrict(l, true);
    if (try cfg.getNonBlankStringStrict("controller.listener.names")) |n| try validateListenerNamesStrict(n);
    const inter_broker_listener_name = try cfg.getNonBlankStringStrict("inter.broker.listener.name");
    const security_inter_broker_protocol = try cfg.getNonBlankStringStrict("security.inter.broker.protocol");
    if (inter_broker_listener_name != null and security_inter_broker_protocol != null) {
        return error.InvalidConfigInterBrokerProtocolConflict;
    }
    if (inter_broker_listener_name) |n| {
        try validateListenerNameStrict(n);
        if (listener_endpoints) |l| _ = try listenerEndpointForNameStrict(l, n, true);
    }
    if (security_inter_broker_protocol) |p| {
        _ = try parseSecurityProtocolStrict(p);
        config.security_protocol = p;
    }
    const listener_security_protocol_map = try cfg.getNonBlankStringStrict("listener.security.protocol.map");
    if (listener_security_protocol_map) |m| {
        try validateListenerSecurityProtocolMapStrict(m);
        if (listener_endpoints) |l| try validateListenerSecurityProtocolMapForListenersStrict(m, l);
    }
    if (inter_broker_listener_name) |n| {
        if (listener_security_protocol_map) |m| config.security_protocol = try listenerSecurityProtocolTextForNameStrict(m, n);
    }
    if (try cfg.getNonBlankStringStrict("advertised.listeners")) |a| {
        if (listener_endpoints) |l| try validateAdvertisedListenersMatchListenersStrict(a, l);
        const endpoint = try firstListenerEndpointStrict(a, false);
        config.advertised_host = endpoint.host;
    }

    // Apply S3 WAL and cache configuration from properties file
    config.s3_wal_batch_size = @intCast(try cfg.getInt(u64, "s3.wal.batch.size", @intCast(config.s3_wal_batch_size)));
    config.s3_wal_flush_interval_ms = try cfg.getInt(i64, "s3.wal.flush.interval.ms", config.s3_wal_flush_interval_ms);
    if (try cfg.getNonBlankStringStrict("s3.wal.flush.mode")) |m| config.s3_wal_flush_mode = try parseWalFlushModeStrict(m);
    config.s3_block_cache_size = @intCast(try cfg.getInt(u64, "s3.block.cache.size", @intCast(config.s3_block_cache_size)));
    config.cache_max_size = @intCast(try cfg.getInt(u64, "log.cache.max.size", @intCast(config.cache_max_size)));
    config.compaction_interval_ms = try cfg.getInt(i64, "s3.compaction.interval.ms", config.compaction_interval_ms);

    // Security configuration
    config.sasl_enabled = try cfg.getBool("sasl.enabled", config.sasl_enabled);
    if (try cfg.getNonBlankStringStrict("sasl.users")) |u| {
        try validateSaslUsersStrict(u);
        config.sasl_users = u;
    }
    if (try cfg.getNonBlankStringStrict("super.users")) |u| {
        try validateSuperUsersStrict(u);
        config.super_users = u;
    }
    config.allow_everyone_if_no_acl = try cfg.getBool("allow.everyone.if.no.acl.found", config.allow_everyone_if_no_acl);
    if (try cfg.getNonBlankStringStrict("sasl.enabled.mechanisms")) |m| {
        try validateSaslMechanismsStrict(m);
        config.sasl_enabled_mechanisms = m;
    }
    if (try cfg.getNonBlankStringStrict("sasl.oauthbearer.expected.issuer")) |i| config.oauth_issuer = i;
    if (try cfg.getNonBlankStringStrict("sasl.oauthbearer.expected.audience")) |a| config.oauth_audience = a;

    // TLS configuration from config file
    // NOTE: AutoMQ uses Java's ssl.* properties (JKS keystore format). ZMQ uses
    // PEM-based cert/key files because Zig/OpenSSL has no JKS support. The property
    // names (ssl.certfile, ssl.keyfile, etc.) follow the PEM convention.
    if (try cfg.getNonBlankStringStrict("security.protocol")) |p| {
        _ = try parseSecurityProtocolStrict(p);
        config.security_protocol = p;
    }
    if (try cfg.getNonBlankStringStrict("ssl.certfile")) |f| config.tls_cert_file = f;
    if (try cfg.getNonBlankStringStrict("ssl.keyfile")) |f| config.tls_key_file = f;
    if (try cfg.getNonBlankStringStrict("ssl.cafile")) |f| config.tls_ca_file = f;
    if (try cfg.getNonBlankStringStrict("ssl.client.auth")) |a| {
        _ = try parseTlsClientAuthStrict(a);
        config.tls_client_auth = a;
    }
    if (try cfg.getNonBlankStringStrict("ssl.principal.mapping.rules")) |r| {
        try OpenSslLib.validatePrincipalMappingRules(r);
        config.tls_principal_mapping_rules = r;
    }

    // Client telemetry export configuration. Accepted PushTelemetry payloads are
    // written as opaque base64 JSONL records to this append-only sink.
    if (try cfg.getNonBlankStringStrict("client.telemetry.export.file")) |f| config.client_telemetry_export_path = f;
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
    try testing.expectEqual(@as(i32, 3), try cfg.getInt(i32, "num.partitions", 1));
    try testing.expect(try cfg.getBool("auto.create.topics.enable", false));
    try testing.expectEqual(@as(u16, 9000), try cfg.getInt(u16, "s3.endpoint.port", 9000));
    try testing.expectEqualStrings("value with spaces", cfg.getString("key.with.spaces").?);
}

test "ConfigFile getters with defaults" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try testing.expectEqual(@as(i32, 42), try cfg.getInt(i32, "nonexistent", 42));
    try testing.expect(!(try cfg.getBool("nonexistent", false)));
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

test "ConfigFile strict getters reject malformed typed values" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse(
        \\s3.path.style=sometimes
        \\sasl.enabled=placeholder
        \\num.partitions=not-a-number
    );

    try testing.expectError(error.InvalidConfigBool, cfg.getBoolStrict("s3.path.style"));
    try testing.expectError(error.InvalidConfigBool, cfg.getBool("sasl.enabled", false));
    try testing.expectError(error.InvalidConfigInteger, cfg.getInt(i32, "num.partitions", 1));
}

test "ConfigFile parses strict TLS startup enums" {
    try testing.expectEqual(TlsConfig.SecurityProtocol.sasl_ssl, try parseSecurityProtocolStrict("SASL_SSL"));
    try testing.expectEqual(TlsConfig.SecurityProtocol.sasl_plaintext, try parseSecurityProtocolStrict("sasl_plaintext"));
    try testing.expectEqual(TlsConfig.ClientAuth.required, try parseTlsClientAuthStrict("REQUIRED"));
    try testing.expectEqual(TlsConfig.ClientAuth.requested, try parseTlsClientAuthStrict("requested"));

    try testing.expectError(error.InvalidConfigSecurityProtocol, parseSecurityProtocolStrict("tls"));
    try testing.expectError(error.InvalidConfigTlsClientAuth, parseTlsClientAuthStrict("maybe"));
}

test "ConfigFile validates SASL startup strings strictly" {
    try validateSaslUsersStrict("alice:secret,bob:p:a:s:s");
    try validateSuperUsersStrict("User:admin;User:broker");
    try validateSaslMechanismsStrict("PLAIN,SCRAM-SHA-256,OAUTHBEARER");

    const invalid_users = [_][]const u8{
        "",
        "alice",
        ":secret",
        "alice:",
        "alice:secret,",
        "alice:secret,,bob:secret",
        "alice:secret, bob:secret",
    };
    for (invalid_users) |value| {
        try testing.expectError(error.InvalidConfigString, validateSaslUsersStrict(value));
    }

    const invalid_super_users = [_][]const u8{
        "",
        "User:admin;",
        "User:admin;;User:broker",
        "User:admin; User:broker",
    };
    for (invalid_super_users) |value| {
        try testing.expectError(error.InvalidConfigString, validateSuperUsersStrict(value));
    }

    const invalid_mechanisms = [_][]const u8{
        "",
        "PLAIN,",
        "PLAIN,,SCRAM-SHA-256",
        "PLAIN, SCRAM-SHA-256",
        "plain",
        "GSSAPI",
        "SCRAM-SHA-512",
    };
    for (invalid_mechanisms) |value| {
        try testing.expectError(error.InvalidConfigString, validateSaslMechanismsStrict(value));
    }
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
    try testing.expectError(error.InvalidConfigString, cfg.getNonBlankStringStrict("empty.key"));
    try testing.expectEqualStrings("value", (try cfg.getNonBlankStringStrict("normal.key")).?);
    try testing.expectEqual(@as(usize, 2), cfg.count());
}

test "ConfigFile comma-separated values reject blank entries" {
    try testing.expectEqualStrings("/tmp/zmq-a", try firstCommaSeparatedValueStrict(" /tmp/zmq-a , /tmp/zmq-b "));

    const invalid = [_][]const u8{
        "",
        "   ",
        ",/tmp/zmq-b",
        "/tmp/zmq-a,",
        "/tmp/zmq-a,,/tmp/zmq-b",
        "/tmp/zmq-a,   ,/tmp/zmq-b",
    };
    for (invalid) |value| {
        try testing.expectError(error.InvalidConfigString, firstCommaSeparatedValueStrict(value));
    }
}

test "ConfigFile rejects malformed lines" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try testing.expectError(error.InvalidConfigLine, cfg.parse("valid=yes\nno-equals-sign\n"));

    var empty_key = ConfigFile.init(testing.allocator);
    defer empty_key.deinit();
    try testing.expectError(error.InvalidConfigKey, empty_key.parse("=no-key\n"));

    var comments = ConfigFile.init(testing.allocator);
    defer comments.deinit();
    try comments.parse("valid=yes\n!bang comment\n#hash comment\nalso.valid=1\n");
    try testing.expectEqual(@as(usize, 2), comments.count());
    try testing.expectEqualStrings("yes", comments.getString("valid").?);
    try testing.expectEqualStrings("1", comments.getString("also.valid").?);
}

test "ConfigFile values with equals sign" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("url=http://host:9000/path?a=1&b=2\n");
    try testing.expectEqualStrings("http://host:9000/path?a=1&b=2", cfg.getString("url").?);
}

test "parseListenerEndpoint parses strict Kafka listeners" {
    const listener = try parseListenerEndpointStrict("PLAINTEXT://0.0.0.0:9092", true);
    try testing.expectEqualStrings("PLAINTEXT", listener.name);
    try testing.expectEqualStrings("0.0.0.0", listener.host);
    try testing.expectEqual(@as(u16, 9092), listener.port);

    const wildcard = try parseListenerEndpointStrict("PLAINTEXT://:9092", true);
    try testing.expectEqualStrings("", wildcard.host);
    try testing.expectEqual(@as(u16, 9092), wildcard.port);

    const ipv6 = try parseListenerEndpointStrict("CONTROLLER://[::1]:19093", false);
    try testing.expectEqualStrings("CONTROLLER", ipv6.name);
    try testing.expectEqualStrings("::1", ipv6.host);
    try testing.expectEqual(@as(u16, 19093), ipv6.port);

    const first = try firstListenerEndpointStrict("PLAINTEXT://broker-a:9092,SSL://broker-b:9093", false);
    try testing.expectEqualStrings("PLAINTEXT", first.name);
    try testing.expectEqualStrings("broker-a", first.host);
    try testing.expectEqual(@as(u16, 9092), first.port);

    try validateListenerNamesStrict("CONTROLLER,CONTROLLER_SSL");
    const listeners = "CONTROLLER://0.0.0.0:19093,PLAINTEXT://0.0.0.0:9092";
    const controller = try firstListenerEndpointMatchingNamesStrict(listeners, "CONTROLLER", true);
    try testing.expectEqualStrings("CONTROLLER", controller.name);
    try testing.expectEqual(@as(u16, 19093), controller.port);
    const broker = try firstListenerEndpointExcludingNamesStrict(listeners, "CONTROLLER", true);
    try testing.expectEqualStrings("PLAINTEXT", broker.name);
    try testing.expectEqual(@as(u16, 9092), broker.port);
    const inter_broker = try listenerEndpointForNameStrict(listeners, "PLAINTEXT", true);
    try testing.expectEqualStrings("PLAINTEXT", inter_broker.name);
    try validateListenerSecurityProtocolMapStrict("PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL");
    try testing.expectEqualStrings("SASL_SSL", try listenerSecurityProtocolTextForNameStrict("PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL", "SASL_SSL"));
    try testing.expectEqual(TlsConfig.SecurityProtocol.plaintext, try listenerSecurityProtocolForNameStrict("PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT", "CONTROLLER"));
    try validateListenerSecurityProtocolMapForListenersStrict("CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT", listeners);
    try validateAdvertisedListenersMatchListenersStrict("PLAINTEXT://broker.example:19092", listeners);
}

test "parseListenerEndpoint rejects malformed Kafka listeners" {
    const invalid = [_][]const u8{
        "",
        "PLAINTEXT://",
        "://localhost:9092",
        "PLAINTEXT://localhost",
        "PLAINTEXT://localhost:",
        "PLAINTEXT://localhost:0",
        "PLAINTEXT://localhost:70000",
        "PLAINTEXT://localhost:9092,",
        "PLAINTEXT://localhost:9092,,SSL://localhost:9093",
        "PLAINTEXT://localhost:9092,plaintext://localhost:9093",
    };
    for (invalid) |entry| {
        try testing.expectError(error.InvalidConfigListener, firstListenerEndpointStrict(entry, false));
    }

    try testing.expectError(error.InvalidConfigListener, parseListenerEndpointStrict("PLAINTEXT://:9092", false));
    try testing.expectError(error.InvalidConfigListener, parseListenerEndpointStrict("PLAINTEXT ://localhost:9092", false));

    const invalid_names = [_][]const u8{
        "",
        "CONTROLLER,",
        "CONTROLLER,,BROKER",
        "CONTROLLER, BROKER",
        "CONTROLLER,controller",
        "CONTROLLER LISTENER",
        "CONTROLLER:PLAINTEXT",
    };
    for (invalid_names) |entry| {
        try testing.expectError(error.InvalidConfigListenerName, validateListenerNamesStrict(entry));
    }

    const listeners = "PLAINTEXT://localhost:9092,CONTROLLER://localhost:19093";
    try testing.expectError(error.InvalidConfigListener, listenerEndpointForNameStrict(listeners, "SSL", false));
    try testing.expectError(error.InvalidConfigListener, firstListenerEndpointMatchingNamesStrict(listeners, "SSL", false));
    try testing.expectError(error.InvalidConfigListener, firstListenerEndpointExcludingNamesStrict("CONTROLLER://localhost:19093", "CONTROLLER", false));

    const invalid_maps = [_][]const u8{
        "",
        "PLAINTEXT",
        "PLAINTEXT:",
        ":PLAINTEXT",
        "PLAINTEXT:tls",
        "PLAINTEXT :PLAINTEXT",
        "PLAINTEXT:PLAINTEXT:",
        "PLAINTEXT:PLAINTEXT,",
        "PLAINTEXT:PLAINTEXT,plaintext:SSL",
    };
    for (invalid_maps) |entry| {
        try testing.expectError(error.InvalidConfigListenerSecurityMap, validateListenerSecurityProtocolMapStrict(entry));
    }
    try testing.expectError(
        error.InvalidConfigListenerSecurityMap,
        validateListenerSecurityProtocolMapForListenersStrict("CONTROLLER:PLAINTEXT", listeners),
    );
    try testing.expectError(
        error.InvalidConfigListener,
        validateAdvertisedListenersMatchListenersStrict("SSL://broker.example:9093", listeners),
    );
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

test "validateControllerVoterSet rejects duplicate controller quorum voters" {
    try validateControllerVoterSet(testing.allocator, "1@controller-1:9093,2@controller-2:9093");
    try testing.expect(try controllerVoterSetContainsNodeIdStrict("1@controller-1:9093,2@controller-2:9093", 2));
    try testing.expect(!(try controllerVoterSetContainsNodeIdStrict("1@controller-1:9093,2@controller-2:9093", 3)));
    try testing.expect(try controllerVoterSetLocalPortMatchesStrict("1@controller-1:9093,2@controller-2:19093", 2, 19093));
    try testing.expect(!(try controllerVoterSetLocalPortMatchesStrict("1@controller-1:9093,2@controller-2:19093", 2, 9093)));

    try testing.expectError(
        error.DuplicateControllerVoter,
        validateControllerVoterSet(testing.allocator, "1@controller-1:9093,1@controller-1-alt:19093"),
    );
    try testing.expectError(
        error.DuplicateControllerVoterEndpoint,
        validateControllerVoterSet(testing.allocator, "1@controller-1:9093,2@controller-1:9093"),
    );
    try testing.expectError(
        error.InvalidControllerVoter,
        validateControllerVoterSet(testing.allocator, "1@controller-1:9093,,2@controller-2:9093"),
    );
}

test "ConfigFile applies client telemetry export sink" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("client.telemetry.export.file=/var/lib/zmq/client-telemetry.jsonl\n");

    var broker_config = @import("broker").Broker.BrokerConfig{};
    try applyConfig(&broker_config, &cfg);
    try testing.expectEqualStrings("/var/lib/zmq/client-telemetry.jsonl", broker_config.client_telemetry_export_path.?);
}

test "ConfigFile applies TLS principal mapping rules" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("ssl.principal.mapping.rules=RULE:.*CN=([^,]+).*/$1/L,DEFAULT\n");

    var broker_config = Broker.BrokerConfig{};
    try applyConfig(&broker_config, &cfg);
    try testing.expectEqualStrings("RULE:.*CN=([^,]+).*/$1/L,DEFAULT", broker_config.tls_principal_mapping_rules.?);
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
    try applyConfig(&broker_config, &cfg);
    try testing.expectEqual(@import("storage").S3Client.Scheme.https, broker_config.s3_scheme);
    try testing.expectEqualStrings("us-west-2", broker_config.s3_region);
    try testing.expect(!broker_config.s3_path_style);
}

test "ConfigFile applies strict SASL security settings" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse(
        \\sasl.enabled=true
        \\sasl.users=alice:secret,bob:p:a:s:s
        \\super.users=User:admin;User:broker
        \\allow.everyone.if.no.acl.found=false
        \\sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,OAUTHBEARER
        \\sasl.oauthbearer.expected.issuer=https://issuer.example
        \\sasl.oauthbearer.expected.audience=zmq-clients
    );

    var broker_config = Broker.BrokerConfig{};
    try applyConfig(&broker_config, &cfg);
    try testing.expect(broker_config.sasl_enabled);
    try testing.expectEqualStrings("alice:secret,bob:p:a:s:s", broker_config.sasl_users);
    try testing.expectEqualStrings("User:admin;User:broker", broker_config.super_users);
    try testing.expect(!broker_config.allow_everyone_if_no_acl);
    try testing.expectEqualStrings("PLAIN,SCRAM-SHA-256,OAUTHBEARER", broker_config.sasl_enabled_mechanisms);
    try testing.expectEqualStrings("https://issuer.example", broker_config.oauth_issuer);
    try testing.expectEqualStrings("zmq-clients", broker_config.oauth_audience);
}

test "ConfigFile applies strict Kafka listener settings" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse(
        \\advertised.host.name=legacy.example
        \\listeners=PLAINTEXT://0.0.0.0:9092
        \\advertised.listeners=PLAINTEXT://broker.example:19092
        \\controller.listener.names=CONTROLLER
        \\inter.broker.listener.name=PLAINTEXT
        \\listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
    );

    var broker_config = Broker.BrokerConfig{};
    try applyConfig(&broker_config, &cfg);
    try testing.expectEqualStrings("broker.example", broker_config.advertised_host);

    var cfg_inter_broker_protocol = ConfigFile.init(testing.allocator);
    defer cfg_inter_broker_protocol.deinit();
    try cfg_inter_broker_protocol.parse("security.inter.broker.protocol=SASL_PLAINTEXT\n");

    var broker_config_inter_broker_protocol = Broker.BrokerConfig{};
    try applyConfig(&broker_config_inter_broker_protocol, &cfg_inter_broker_protocol);
    try testing.expectEqualStrings("SASL_PLAINTEXT", broker_config_inter_broker_protocol.security_protocol);

    var cfg_listener_security_protocol = ConfigFile.init(testing.allocator);
    defer cfg_listener_security_protocol.deinit();
    try cfg_listener_security_protocol.parse(
        \\listeners=SSL://0.0.0.0:9094,CONTROLLER://0.0.0.0:19093
        \\inter.broker.listener.name=SSL
        \\listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
    );

    var broker_config_listener_security_protocol = Broker.BrokerConfig{};
    try applyConfig(&broker_config_listener_security_protocol, &cfg_listener_security_protocol);
    try testing.expectEqualStrings("SSL", broker_config_listener_security_protocol.security_protocol);
}

test "ConfigFile applyConfig rejects invalid S3 storage settings" {
    var cfg = ConfigFile.init(testing.allocator);
    defer cfg.deinit();

    try cfg.parse("s3.scheme=ftp\n");
    var broker_config = Broker.BrokerConfig{};
    try testing.expectError(error.InvalidConfigS3Scheme, applyConfig(&broker_config, &cfg));

    var cfg_bool = ConfigFile.init(testing.allocator);
    defer cfg_bool.deinit();
    try cfg_bool.parse("s3.path.style=maybe\n");
    try testing.expectError(error.InvalidConfigBool, applyConfig(&broker_config, &cfg_bool));

    var cfg_mode = ConfigFile.init(testing.allocator);
    defer cfg_mode.deinit();
    try cfg_mode.parse("s3.wal.flush.mode=eventually\n");
    try testing.expectError(error.InvalidConfigWalFlushMode, applyConfig(&broker_config, &cfg_mode));

    var cfg_security_protocol = ConfigFile.init(testing.allocator);
    defer cfg_security_protocol.deinit();
    try cfg_security_protocol.parse("security.protocol=tls\n");
    try testing.expectError(error.InvalidConfigSecurityProtocol, applyConfig(&broker_config, &cfg_security_protocol));

    var cfg_inter_broker_security_protocol = ConfigFile.init(testing.allocator);
    defer cfg_inter_broker_security_protocol.deinit();
    try cfg_inter_broker_security_protocol.parse("security.inter.broker.protocol=tls\n");
    try testing.expectError(error.InvalidConfigSecurityProtocol, applyConfig(&broker_config, &cfg_inter_broker_security_protocol));

    var cfg_client_auth = ConfigFile.init(testing.allocator);
    defer cfg_client_auth.deinit();
    try cfg_client_auth.parse("ssl.client.auth=maybe\n");
    try testing.expectError(error.InvalidConfigTlsClientAuth, applyConfig(&broker_config, &cfg_client_auth));

    const blank_s3_settings = [_][]const u8{
        "s3.endpoint.host=\n",
        "s3.bucket=\n",
        "s3.access.key=\n",
        "s3.secret.key=\n",
        "s3.region=\n",
        "s3.tls.ca.file=\n",
    };
    for (blank_s3_settings) |content| {
        var cfg_blank = ConfigFile.init(testing.allocator);
        defer cfg_blank.deinit();
        try cfg_blank.parse(content);
        try testing.expectError(error.InvalidConfigString, applyConfig(&broker_config, &cfg_blank));
    }

    const blank_startup_settings = [_][]const u8{
        "cluster.id=\n",
        "advertised.host.name=\n",
        "s3.wal.flush.mode=\n",
        "security.protocol=\n",
        "security.inter.broker.protocol=\n",
        "ssl.certfile=\n",
        "ssl.keyfile=\n",
        "ssl.cafile=\n",
        "ssl.client.auth=\n",
        "ssl.principal.mapping.rules=\n",
        "controller.listener.names=\n",
        "inter.broker.listener.name=\n",
        "listener.security.protocol.map=\n",
        "client.telemetry.export.file=\n",
    };
    for (blank_startup_settings) |content| {
        var cfg_blank = ConfigFile.init(testing.allocator);
        defer cfg_blank.deinit();
        try cfg_blank.parse(content);
        try testing.expectError(error.InvalidConfigString, applyConfig(&broker_config, &cfg_blank));
    }

    const invalid_log_dirs = [_][]const u8{
        "log.dirs=\n",
        "log.dirs=/tmp/zmq-a,\n",
        "log.dirs=/tmp/zmq-a,,/tmp/zmq-b\n",
    };
    for (invalid_log_dirs) |content| {
        var cfg_log_dirs = ConfigFile.init(testing.allocator);
        defer cfg_log_dirs.deinit();
        try cfg_log_dirs.parse(content);
        try testing.expectError(error.InvalidConfigString, applyConfig(&broker_config, &cfg_log_dirs));
    }

    var cfg_multi_log_dirs = ConfigFile.init(testing.allocator);
    defer cfg_multi_log_dirs.deinit();
    try cfg_multi_log_dirs.parse("log.dirs= /tmp/zmq-a , /tmp/zmq-b\n");
    try applyConfig(&broker_config, &cfg_multi_log_dirs);
    try testing.expectEqualStrings("/tmp/zmq-a", broker_config.data_dir.?);
}

test "ConfigFile applyConfig rejects malformed Kafka listener settings" {
    const blank_listener_settings = [_][]const u8{
        "listeners=\n",
        "advertised.listeners=\n",
    };
    for (blank_listener_settings) |content| {
        var cfg = ConfigFile.init(testing.allocator);
        defer cfg.deinit();
        try cfg.parse(content);

        var broker_config = Broker.BrokerConfig{};
        try testing.expectError(error.InvalidConfigString, applyConfig(&broker_config, &cfg));
    }

    const invalid_listener_settings = [_][]const u8{
        "listeners=PLAINTEXT://localhost\n",
        "listeners=PLAINTEXT://localhost:0\n",
        "listeners=PLAINTEXT://localhost:9092,\n",
        "advertised.listeners=PLAINTEXT://:9092\n",
        "advertised.listeners=PLAINTEXT://localhost:notaport\n",
        "listeners=PLAINTEXT://localhost:9092\ninter.broker.listener.name=SSL\n",
        "listeners=PLAINTEXT://localhost:9092,plaintext://localhost:9093\n",
        "listeners=PLAINTEXT://localhost:9092\nadvertised.listeners=SSL://broker.example:9093\n",
    };

    for (invalid_listener_settings) |content| {
        var cfg = ConfigFile.init(testing.allocator);
        defer cfg.deinit();
        try cfg.parse(content);

        var broker_config = Broker.BrokerConfig{};
        try testing.expectError(error.InvalidConfigListener, applyConfig(&broker_config, &cfg));
    }

    const invalid_listener_name_settings = [_][]const u8{
        "controller.listener.names=CONTROLLER,\n",
        "controller.listener.names=CONTROLLER,controller\n",
        "inter.broker.listener.name=PLAINTEXT BROKER\n",
    };
    for (invalid_listener_name_settings) |content| {
        var cfg = ConfigFile.init(testing.allocator);
        defer cfg.deinit();
        try cfg.parse(content);

        var broker_config = Broker.BrokerConfig{};
        try testing.expectError(error.InvalidConfigListenerName, applyConfig(&broker_config, &cfg));
    }

    const invalid_listener_map_settings = [_][]const u8{
        "listener.security.protocol.map=PLAINTEXT:tls\n",
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,\n",
        "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,plaintext:SSL\n",
        "listeners=PLAINTEXT://localhost:9092\nlistener.security.protocol.map=CONTROLLER:PLAINTEXT\n",
    };
    for (invalid_listener_map_settings) |content| {
        var cfg = ConfigFile.init(testing.allocator);
        defer cfg.deinit();
        try cfg.parse(content);

        var broker_config = Broker.BrokerConfig{};
        try testing.expectError(error.InvalidConfigListenerSecurityMap, applyConfig(&broker_config, &cfg));
    }

    var cfg_inter_broker_protocol_conflict = ConfigFile.init(testing.allocator);
    defer cfg_inter_broker_protocol_conflict.deinit();
    try cfg_inter_broker_protocol_conflict.parse(
        \\listeners=PLAINTEXT://localhost:9092
        \\inter.broker.listener.name=PLAINTEXT
        \\security.inter.broker.protocol=PLAINTEXT
    );
    var broker_config = Broker.BrokerConfig{};
    try testing.expectError(error.InvalidConfigInterBrokerProtocolConflict, applyConfig(&broker_config, &cfg_inter_broker_protocol_conflict));
}

test "ConfigFile applyConfig rejects malformed SASL security settings" {
    const invalid_sasl_settings = [_][]const u8{
        "sasl.users=\n",
        "sasl.users=alice\n",
        "sasl.users=:secret\n",
        "sasl.users=alice:\n",
        "sasl.users=alice:secret,,bob:secret\n",
        "super.users=\n",
        "super.users=User:admin;\n",
        "sasl.enabled.mechanisms=\n",
        "sasl.enabled.mechanisms=PLAIN,\n",
        "sasl.enabled.mechanisms=GSSAPI\n",
        "sasl.oauthbearer.expected.issuer=\n",
        "sasl.oauthbearer.expected.audience=\n",
    };

    for (invalid_sasl_settings) |content| {
        var cfg = ConfigFile.init(testing.allocator);
        defer cfg.deinit();
        try cfg.parse(content);

        var broker_config = Broker.BrokerConfig{};
        try testing.expectError(error.InvalidConfigString, applyConfig(&broker_config, &cfg));
    }
}

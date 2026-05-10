const std = @import("std");
const posix = std.posix;
const log = std.log.scoped(.main);

const network = @import("network");
const Server = network.Server;
const MetricsServer = network.MetricsServer;
const RaftClientPool = network.RaftClientPool;
const storage = @import("storage");
const broker_mod = @import("broker");
const handler = broker_mod.handler;
const Broker = broker_mod.Broker;
const config_mod = @import("config.zig");
const ConfigFile = config_mod.ConfigFile;
const raft_mod = @import("raft");
const ElectionLoop = raft_mod.ElectionLoop;
const RaftState = raft_mod.RaftState;
const ProcessRoles = @import("roles.zig").ProcessRoles;
const Controller = @import("controller/controller.zig").Controller;
const MetadataClient = @import("controller/metadata_client.zig").MetadataClient;
const handler_routing = @import("network/handler_routing.zig");
const security = @import("security");
const TlsConfig = security.tls.TlsConfig;
const TlsContext = security.tls.TlsContext;

pub const std_options: std.Options = .{
    .log_level = .info,
};

const Stdout = struct {
    fn print(_: *Stdout, comptime fmt: []const u8, args: anytype) !void {
        var stack_buf: [8192]u8 = undefined;
        const text = std.fmt.bufPrint(&stack_buf, fmt, args) catch {
            const heap_text = try std.fmt.allocPrint(std.heap.c_allocator, fmt, args);
            defer std.heap.c_allocator.free(heap_text);
            try writeAll(posix.STDOUT_FILENO, heap_text);
            return;
        };
        try writeAll(posix.STDOUT_FILENO, text);
    }
};

fn writeAll(fd: posix.fd_t, bytes: []const u8) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.os.linux.write(fd, bytes[written..].ptr, bytes.len - written);
        switch (std.os.linux.errno(rc)) {
            .SUCCESS => written += @intCast(rc),
            .INTR => {},
            else => return error.WriteFailed,
        }
    }
}

fn parseBoolFlag(text: []const u8) !bool {
    if (std.mem.eql(u8, text, "1") or std.ascii.eqlIgnoreCase(text, "true") or std.ascii.eqlIgnoreCase(text, "yes")) {
        return true;
    }
    if (std.mem.eql(u8, text, "0") or std.ascii.eqlIgnoreCase(text, "false") or std.ascii.eqlIgnoreCase(text, "no")) {
        return false;
    }
    return error.InvalidBoolFlag;
}

fn parseS3Scheme(text: []const u8) !storage.S3Client.Scheme {
    if (std.ascii.eqlIgnoreCase(text, "https")) return .https;
    if (std.ascii.eqlIgnoreCase(text, "http")) return .http;
    return error.InvalidS3Scheme;
}

fn parseWalFlushMode(text: []const u8) !handler.Broker.WalFlushMode {
    if (std.mem.eql(u8, text, "sync")) return .sync;
    if (std.mem.eql(u8, text, "async")) return .async_flush;
    if (std.mem.eql(u8, text, "group_commit")) return .group_commit;
    return error.InvalidWalFlushMode;
}

fn parseSecurityProtocol(text: []const u8) !TlsConfig.SecurityProtocol {
    if (std.ascii.eqlIgnoreCase(text, "plaintext")) return .plaintext;
    if (std.ascii.eqlIgnoreCase(text, "ssl")) return .ssl;
    if (std.ascii.eqlIgnoreCase(text, "sasl_plaintext")) return .sasl_plaintext;
    if (std.ascii.eqlIgnoreCase(text, "sasl_ssl")) return .sasl_ssl;
    return error.InvalidSecurityProtocol;
}

fn parseTlsClientAuth(text: []const u8) !TlsConfig.ClientAuth {
    if (std.ascii.eqlIgnoreCase(text, "none")) return .none;
    if (std.ascii.eqlIgnoreCase(text, "requested")) return .requested;
    if (std.ascii.eqlIgnoreCase(text, "required")) return .required;
    return error.InvalidTlsClientAuth;
}

fn nextRequiredArg(stdout: *Stdout, args: *std.process.Args.Iterator, flag: []const u8) ![]const u8 {
    return args.next() orelse {
        try stdout.print("  ERROR: missing value for {s}\n", .{flag});
        return error.InvalidConfiguration;
    };
}

fn nextRequiredNonBlankArg(stdout: *Stdout, args: *std.process.Args.Iterator, flag: []const u8) ![]const u8 {
    const text = try nextRequiredArg(stdout, args, flag);
    if (std.mem.trim(u8, text, " \t\r\n").len == 0) {
        try stdout.print("  ERROR: {s} must not be blank\n", .{flag});
        return error.InvalidConfiguration;
    }
    return text;
}

fn parseRequiredIntArg(comptime T: type, stdout: *Stdout, args: *std.process.Args.Iterator, flag: []const u8) !T {
    const text = try nextRequiredArg(stdout, args, flag);
    return std.fmt.parseInt(T, text, 10) catch {
        try stdout.print("  ERROR: invalid integer for {s}: '{s}'\n", .{ flag, text });
        return error.InvalidConfiguration;
    };
}

fn parseRequiredPortArg(stdout: *Stdout, args: *std.process.Args.Iterator, flag: []const u8) !u16 {
    const value = try parseRequiredIntArg(u16, stdout, args, flag);
    if (value == 0) {
        try stdout.print("  ERROR: {s} must be in the range 1-65535\n", .{flag});
        return error.InvalidConfiguration;
    }
    return value;
}

fn parseRequiredNodeIdArg(stdout: *Stdout, args: *std.process.Args.Iterator, flag: []const u8) !i32 {
    const value = try parseRequiredIntArg(i32, stdout, args, flag);
    if (value < 0) {
        try stdout.print("  ERROR: {s} must be non-negative\n", .{flag});
        return error.InvalidConfiguration;
    }
    return value;
}

fn applyConfigIntStrict(comptime T: type, stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, target: *T) !void {
    const value = cfg.getIntStrict(T, key) catch {
        try stdout.print("  ERROR: invalid integer for config '{s}'\n", .{key});
        return error.InvalidConfiguration;
    };
    if (value) |v| target.* = v;
}

fn applyConfigPortStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, target: *u16) !void {
    const value = cfg.getIntStrict(u16, key) catch {
        try stdout.print("  ERROR: invalid port for config '{s}'\n", .{key});
        return error.InvalidConfiguration;
    };
    if (value) |v| {
        if (v == 0) {
            try stdout.print("  ERROR: config '{s}' must be in the range 1-65535\n", .{key});
            return error.InvalidConfiguration;
        }
        target.* = v;
    }
}

fn configNodeIdStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8) !?i32 {
    const value = cfg.getIntStrict(i32, key) catch {
        try stdout.print("  ERROR: invalid node id for config '{s}'\n", .{key});
        return error.InvalidConfiguration;
    };
    if (value) |v| {
        if (v < 0) {
            try stdout.print("  ERROR: config '{s}' must be non-negative\n", .{key});
            return error.InvalidConfiguration;
        }
        return v;
    }
    return null;
}

fn applyConfigNodeIdStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, target: *i32) !void {
    if (try configNodeIdStrict(stdout, cfg, key)) |v| target.* = v;
}

fn applyConfigNodeIdAliasesStrict(stdout: *Stdout, cfg: *const ConfigFile, target: *i32) !void {
    const broker_id = try configNodeIdStrict(stdout, cfg, "broker.id");
    const kraft_node_id = try configNodeIdStrict(stdout, cfg, "node.id");

    if (broker_id) |b| {
        if (kraft_node_id) |n| {
            if (b != n) {
                try stdout.print("  ERROR: config 'broker.id' and 'node.id' must match when both are set ({d} != {d})\n", .{ b, n });
                return error.InvalidConfiguration;
            }
        }
    }

    if (kraft_node_id) |n| {
        target.* = n;
    } else if (broker_id) |b| {
        target.* = b;
    }
}

fn applyConfigBoolStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, target: *bool) !void {
    const value = cfg.getBoolStrict(key) catch {
        try stdout.print("  ERROR: config '{s}' must be true or false\n", .{key});
        return error.InvalidConfiguration;
    };
    if (value) |v| target.* = v;
}

fn configStringStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8) !?[]const u8 {
    return cfg.getNonBlankStringStrict(key) catch {
        try stdout.print("  ERROR: config '{s}' must not be blank\n", .{key});
        return error.InvalidConfiguration;
    };
}

fn configStringOrStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, default: []const u8) ![]const u8 {
    return (try configStringStrict(stdout, cfg, key)) orelse default;
}

fn configSaslUsersOrStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, default: []const u8) ![]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return default;
    config_mod.validateSaslUsersStrict(value) catch {
        try stdout.print("  ERROR: config '{s}' must contain comma-separated user:password pairs with nonblank fields\n", .{key});
        return error.InvalidConfiguration;
    };
    return value;
}

fn configSuperUsersOrStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, default: []const u8) ![]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return default;
    config_mod.validateSuperUsersStrict(value) catch {
        try stdout.print("  ERROR: config '{s}' must contain semicolon-separated nonblank principals\n", .{key});
        return error.InvalidConfiguration;
    };
    return value;
}

fn configSaslMechanismsOrStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, default: []const u8) ![]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return default;
    config_mod.validateSaslMechanismsStrict(value) catch {
        try stdout.print("  ERROR: config '{s}' must list supported SASL mechanisms without blank entries\n", .{key});
        return error.InvalidConfiguration;
    };
    return value;
}

fn configListenerEndpointStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8, allow_blank_host: bool) !?config_mod.ListenerEndpoint {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return null;
    return config_mod.firstListenerEndpointStrict(value, allow_blank_host) catch {
        try stdout.print("  ERROR: config '{s}' must contain comma-separated NAME://host:port listener endpoints\n", .{key});
        return error.InvalidConfiguration;
    };
}

fn configListenerNamesStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8) !?[]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return null;
    config_mod.validateListenerNamesStrict(value) catch {
        try stdout.print("  ERROR: config '{s}' must contain comma-separated listener names without blank entries\n", .{key});
        return error.InvalidConfiguration;
    };
    return value;
}

fn configListenerNameStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8) !?[]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return null;
    config_mod.validateListenerNameStrict(value) catch {
        try stdout.print("  ERROR: config '{s}' must contain one listener name\n", .{key});
        return error.InvalidConfiguration;
    };
    return value;
}

fn configSecurityProtocolStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8) !?[]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return null;
    _ = parseSecurityProtocol(value) catch {
        try stdout.print("  ERROR: config '{s}' must be plaintext, ssl, sasl_plaintext, or sasl_ssl, got '{s}'\n", .{ key, value });
        return error.InvalidConfiguration;
    };
    return value;
}

fn configListenerSecurityProtocolMapStrict(stdout: *Stdout, cfg: *const ConfigFile, key: []const u8) !?[]const u8 {
    const value = (try configStringStrict(stdout, cfg, key)) orelse return null;
    config_mod.validateListenerSecurityProtocolMapStrict(value) catch {
        try stdout.print("  ERROR: config '{s}' must contain comma-separated listener:security-protocol entries\n", .{key});
        return error.InvalidConfiguration;
    };
    return value;
}

fn configListenerSecurityProtocolMapCoversListenersStrict(stdout: *Stdout, map: []const u8, listeners: []const u8) !void {
    config_mod.validateListenerSecurityProtocolMapForListenersStrict(map, listeners) catch {
        try stdout.print("  ERROR: config 'listener.security.protocol.map' must include every listener from config 'listeners'\n", .{});
        return error.InvalidConfiguration;
    };
}

fn configListenerSecurityProtocolForNameStrict(stdout: *Stdout, map: []const u8, listener_name: []const u8) ![]const u8 {
    return config_mod.listenerSecurityProtocolTextForNameStrict(map, listener_name) catch {
        try stdout.print("  ERROR: config 'listener.security.protocol.map' must define listener '{s}' with a valid security protocol\n", .{listener_name});
        return error.InvalidConfiguration;
    };
}

fn configAdvertisedListenersMatchListenersStrict(stdout: *Stdout, advertised: []const u8, listeners: []const u8) !void {
    config_mod.validateAdvertisedListenersMatchListenersStrict(advertised, listeners) catch {
        try stdout.print("  ERROR: config 'advertised.listeners' must use listener names from config 'listeners'\n", .{});
        return error.InvalidConfiguration;
    };
}

fn parseConfigListenerEndpointTextStrict(stdout: *Stdout, key: []const u8, value: []const u8, allow_blank_host: bool) !config_mod.ListenerEndpoint {
    return config_mod.firstListenerEndpointStrict(value, allow_blank_host) catch {
        try stdout.print("  ERROR: config '{s}' must contain comma-separated NAME://host:port listener endpoints\n", .{key});
        return error.InvalidConfiguration;
    };
}

fn configListenerEndpointForNameTextStrict(stdout: *Stdout, listeners: []const u8, listener_name: []const u8, allow_blank_host: bool) !config_mod.ListenerEndpoint {
    return config_mod.listenerEndpointForNameStrict(listeners, listener_name, allow_blank_host) catch {
        try stdout.print("  ERROR: config 'listeners' does not contain listener '{s}'\n", .{listener_name});
        return error.InvalidConfiguration;
    };
}

fn configControllerListenerEndpointTextStrict(stdout: *Stdout, listeners: []const u8, listener_names: []const u8, allow_blank_host: bool) !config_mod.ListenerEndpoint {
    return config_mod.firstListenerEndpointMatchingNamesStrict(listeners, listener_names, allow_blank_host) catch {
        try stdout.print("  ERROR: config 'listeners' does not contain a controller listener from controller.listener.names\n", .{});
        return error.InvalidConfiguration;
    };
}

fn configNonControllerListenerEndpointTextStrict(stdout: *Stdout, listeners: []const u8, listener_names: []const u8, allow_blank_host: bool) !config_mod.ListenerEndpoint {
    return config_mod.firstListenerEndpointExcludingNamesStrict(listeners, listener_names, allow_blank_host) catch {
        try stdout.print("  ERROR: config 'listeners' must include a broker listener outside controller.listener.names\n", .{});
        return error.InvalidConfiguration;
    };
}

fn firstLogDirStrict(stdout: *Stdout, log_dirs: ?[]const u8) !?[]const u8 {
    const raw = log_dirs orelse return null;
    return config_mod.firstCommaSeparatedValueStrict(raw) catch {
        try stdout.print("  ERROR: log.dirs/--data-dir must contain comma-separated nonblank directories\n", .{});
        return error.InvalidConfiguration;
    };
}

/// Global pointers for signal handler access.
var global_server: ?*Server = null;
var global_controller_server: ?*Server = null;
var global_metrics_server: ?*MetricsServer = null;
var global_shutdown: bool = false;
/// Global broker pointer for shutdown WAL flush/snapshot.
var global_broker_ptr: ?*Broker = null;
/// Global controller pointer for Raft snapshot during shutdown.
var global_controller_ptr: ?*Controller = null;

/// Callback for ElectionLoop: serialize the PreparedObjectRegistry before
/// each Raft snapshot so prepared objects survive log truncation.
fn serializePreparedRegistry() ?[]const u8 {
    const brk = global_broker_ptr orelse return null;
    return brk.object_manager.prepared_registry.serialize(brk.allocator) catch |err| {
        log.warn("Failed to serialize prepared registry for snapshot: {}", .{err});
        return null;
    };
}

/// Callback for ElectionLoop: append complete controller and AutoMQ metadata
/// snapshot records before Raft truncates committed metadata records.
fn prepareRaftMetadataSnapshot() bool {
    if (global_controller_ptr) |ctrl| {
        ctrl.prepareControllerMetadataSnapshotForRaftCompaction() catch |err| {
            log.warn("Failed to prepare controller metadata snapshot before Raft compaction: {}", .{err});
            return false;
        };
    }
    if (global_broker_ptr) |brk| {
        brk.prepareAutoMqMetadataSnapshotForRaftCompaction() catch |err| {
            log.warn("Failed to prepare AutoMQ metadata snapshot before Raft compaction: {}", .{err});
            return false;
        };
    }
    return true;
}

fn prepareControllerMetadataSnapshotForShutdown(ctrl: *Controller) bool {
    ctrl.prepareControllerMetadataSnapshotForRaftCompaction() catch |err| {
        log.warn("Graceful shutdown: skipping controller metadata snapshot: {}", .{err});
        return false;
    };
    return true;
}

fn prepareAutoMqMetadataSnapshotForShutdown(brk: *Broker) bool {
    brk.prepareAutoMqMetadataSnapshotForRaftCompaction() catch |err| {
        log.warn("Graceful shutdown: skipping Raft snapshot because AutoMQ metadata snapshot failed: {}", .{err});
        return false;
    };
    return true;
}

fn applyCommittedAutoMqQuorumRecords() void {
    if (global_broker_ptr) |brk| {
        _ = brk.applyCommittedAutoMqQuorumRecords() catch |err| {
            log.warn("Failed to apply committed AutoMQ quorum records: {}", .{err});
        };
    }
}

fn handleSignal(sig: posix.SIG) callconv(.c) void {
    _ = sig;
    if (global_shutdown) {
        // Second signal: force immediate shutdown (no drain)
        if (global_server) |s| s.stop();
        if (global_controller_server) |s| s.stop();
        if (global_metrics_server) |m| m.stop();
        return;
    }
    global_shutdown = true;
    // Tell the broker to reject new requests during drain
    if (global_broker_ptr) |brk| {
        brk.is_shutting_down = true;
    }
    // Initiate graceful drain on all servers (stop accepting, wait for in-flight)
    if (global_server) |s| s.initiateGracefulDrain();
    if (global_controller_server) |s| s.initiateGracefulDrain();
    if (global_metrics_server) |m| m.stop();
}

pub fn main(init: std.process.Init) !void {
    var stdout = Stdout{};
    try stdout.print(
        \\
        \\  ┌─────────────────────────────────────────────┐
        \\  │            ZMQ Broker v0.8.0                 │
        \\  │   Cloud-native Kafka, rewritten in Zig      │
        \\  └─────────────────────────────────────────────┘
        \\
        \\
    , .{});

    // Use libc malloc which is inherently thread-safe and avoids GPA
    // debug-mode overhead that can trigger false protection faults under
    // concurrent access from background threads (ElectionLoop, MetadataClient).
    const alloc = std.heap.c_allocator;

    // Parse CLI
    var port: u16 = 9092;
    var metrics_port: u16 = 9090;
    var data_dir: ?[]const u8 = null;
    var s3_host: ?[]const u8 = null;
    var s3_port: u16 = 9000;
    var s3_bucket: []const u8 = "automq";
    var s3_access_key: []const u8 = "minioadmin";
    var s3_secret_key: []const u8 = "minioadmin";
    var s3_scheme: storage.S3Client.Scheme = .http;
    var s3_region: []const u8 = "us-east-1";
    var s3_path_style: bool = true;
    var s3_tls_ca_file: ?[]const u8 = null;
    var node_id: i32 = 0;
    var config_path: ?[]const u8 = null;
    var advertised_host: []const u8 = "localhost";
    var cluster_id: []const u8 = "automq-cluster";
    var voters_str: ?[]const u8 = null;
    var num_workers: usize = 4;
    // Process roles: controller, broker, or controller,broker (default).
    var process_roles: ProcessRoles = ProcessRoles.combined;
    // Controller listener port (KRaft consensus + broker registration).
    var controller_port: u16 = 9093;
    var cli_port_set = false;
    var cli_metrics_port_set = false;
    var cli_s3_port_set = false;
    var cli_node_id_set = false;
    var cli_process_roles_set = false;
    var cli_controller_port_set = false;
    var cli_s3_wal_batch_size_set = false;
    var cli_s3_wal_flush_interval_set = false;
    var cli_s3_wal_flush_mode_set = false;
    var cli_cache_max_size_set = false;
    var cli_s3_block_cache_size_set = false;
    var cli_compaction_interval_set = false;
    var cli_security_protocol_set = false;
    var cli_tls_client_auth_set = false;
    var cli_advertised_host_set = false;
    // Configurable S3 WAL and performance parameters
    var s3_wal_batch_size: usize = 4 * 1024 * 1024;
    var s3_wal_flush_interval: i64 = 250;
    var s3_wal_flush_mode: []const u8 = "sync";
    var cache_max_size: u64 = 256 * 1024 * 1024;
    var s3_block_cache_size: u64 = 64 * 1024 * 1024;
    var compaction_interval: i64 = 300_000;
    // TLS configuration
    var security_protocol: []const u8 = "plaintext";
    var tls_cert_file: ?[]const u8 = null;
    var tls_key_file: ?[]const u8 = null;
    var tls_ca_file: ?[]const u8 = null;
    var tls_client_auth_str: []const u8 = "none";
    var tls_principal_mapping_rules: ?[]const u8 = null;
    var client_telemetry_export_path: ?[]const u8 = null;
    var sasl_enabled: bool = false;
    var sasl_users: []const u8 = "";
    var super_users: []const u8 = "";
    var allow_everyone_if_no_acl: bool = true;
    var sasl_enabled_mechanisms: []const u8 = "PLAIN";
    var oauth_issuer: []const u8 = "";
    var oauth_audience: []const u8 = "";

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            port = try parseRequiredPortArg(&stdout, &args, "--port");
            cli_port_set = true;
        } else if (std.mem.eql(u8, arg, "--data-dir")) {
            data_dir = try nextRequiredNonBlankArg(&stdout, &args, "--data-dir");
        } else if (std.mem.eql(u8, arg, "--s3-endpoint")) {
            s3_host = try nextRequiredNonBlankArg(&stdout, &args, "--s3-endpoint");
        } else if (std.mem.eql(u8, arg, "--s3-port")) {
            s3_port = try parseRequiredPortArg(&stdout, &args, "--s3-port");
            cli_s3_port_set = true;
        } else if (std.mem.eql(u8, arg, "--s3-bucket")) {
            s3_bucket = try nextRequiredNonBlankArg(&stdout, &args, "--s3-bucket");
        } else if (std.mem.eql(u8, arg, "--s3-access-key")) {
            s3_access_key = try nextRequiredNonBlankArg(&stdout, &args, "--s3-access-key");
        } else if (std.mem.eql(u8, arg, "--s3-secret-key")) {
            s3_secret_key = try nextRequiredNonBlankArg(&stdout, &args, "--s3-secret-key");
        } else if (std.mem.eql(u8, arg, "--s3-scheme")) {
            const value = try nextRequiredNonBlankArg(&stdout, &args, "--s3-scheme");
            s3_scheme = parseS3Scheme(value) catch {
                try stdout.print("  ERROR: --s3-scheme must be http or https, got '{s}'\n", .{value});
                return error.InvalidConfiguration;
            };
        } else if (std.mem.eql(u8, arg, "--s3-region")) {
            s3_region = try nextRequiredNonBlankArg(&stdout, &args, "--s3-region");
        } else if (std.mem.eql(u8, arg, "--s3-path-style")) {
            const value = try nextRequiredNonBlankArg(&stdout, &args, "--s3-path-style");
            s3_path_style = parseBoolFlag(value) catch {
                try stdout.print("  ERROR: --s3-path-style must be true or false, got '{s}'\n", .{value});
                return error.InvalidConfiguration;
            };
        } else if (std.mem.eql(u8, arg, "--s3-ca-file")) {
            s3_tls_ca_file = try nextRequiredNonBlankArg(&stdout, &args, "--s3-ca-file");
        } else if (std.mem.eql(u8, arg, "--metrics-port")) {
            metrics_port = try parseRequiredPortArg(&stdout, &args, "--metrics-port");
            cli_metrics_port_set = true;
        } else if (std.mem.eql(u8, arg, "--node-id")) {
            node_id = try parseRequiredNodeIdArg(&stdout, &args, "--node-id");
            cli_node_id_set = true;
        } else if (std.mem.eql(u8, arg, "--advertised-host")) {
            advertised_host = try nextRequiredNonBlankArg(&stdout, &args, "--advertised-host");
            cli_advertised_host_set = true;
        } else if (std.mem.eql(u8, arg, "--config")) {
            config_path = try nextRequiredNonBlankArg(&stdout, &args, "--config");
        } else if (std.mem.eql(u8, arg, "--cluster-id")) {
            cluster_id = try nextRequiredNonBlankArg(&stdout, &args, "--cluster-id");
        } else if (std.mem.eql(u8, arg, "--voters")) {
            voters_str = try nextRequiredNonBlankArg(&stdout, &args, "--voters");
        } else if (std.mem.eql(u8, arg, "--workers")) {
            num_workers = try parseRequiredIntArg(usize, &stdout, &args, "--workers");
        } else if (std.mem.eql(u8, arg, "--process-roles")) {
            const role_text = try nextRequiredNonBlankArg(&stdout, &args, "--process-roles");
            process_roles = ProcessRoles.parse(role_text) catch {
                try stdout.print("  ERROR: invalid --process-roles '{s}'\n", .{role_text});
                return error.InvalidConfiguration;
            };
            cli_process_roles_set = true;
        } else if (std.mem.eql(u8, arg, "--controller-port")) {
            controller_port = try parseRequiredPortArg(&stdout, &args, "--controller-port");
            cli_controller_port_set = true;
            // S3 WAL and cache configuration CLI flags
        } else if (std.mem.eql(u8, arg, "--s3-wal-batch-size")) {
            s3_wal_batch_size = try parseRequiredIntArg(usize, &stdout, &args, "--s3-wal-batch-size");
            cli_s3_wal_batch_size_set = true;
        } else if (std.mem.eql(u8, arg, "--s3-wal-flush-interval")) {
            s3_wal_flush_interval = try parseRequiredIntArg(i64, &stdout, &args, "--s3-wal-flush-interval");
            cli_s3_wal_flush_interval_set = true;
        } else if (std.mem.eql(u8, arg, "--s3-wal-flush-mode")) {
            s3_wal_flush_mode = try nextRequiredNonBlankArg(&stdout, &args, "--s3-wal-flush-mode");
            cli_s3_wal_flush_mode_set = true;
        } else if (std.mem.eql(u8, arg, "--cache-max-size")) {
            cache_max_size = try parseRequiredIntArg(u64, &stdout, &args, "--cache-max-size");
            cli_cache_max_size_set = true;
        } else if (std.mem.eql(u8, arg, "--s3-block-cache-size")) {
            s3_block_cache_size = try parseRequiredIntArg(u64, &stdout, &args, "--s3-block-cache-size");
            cli_s3_block_cache_size_set = true;
        } else if (std.mem.eql(u8, arg, "--compaction-interval")) {
            compaction_interval = try parseRequiredIntArg(i64, &stdout, &args, "--compaction-interval");
            cli_compaction_interval_set = true;
        } else if (std.mem.eql(u8, arg, "--security-protocol")) {
            security_protocol = try nextRequiredNonBlankArg(&stdout, &args, "--security-protocol");
            cli_security_protocol_set = true;
        } else if (std.mem.eql(u8, arg, "--tls-cert-file")) {
            tls_cert_file = try nextRequiredNonBlankArg(&stdout, &args, "--tls-cert-file");
        } else if (std.mem.eql(u8, arg, "--tls-key-file")) {
            tls_key_file = try nextRequiredNonBlankArg(&stdout, &args, "--tls-key-file");
        } else if (std.mem.eql(u8, arg, "--tls-ca-file")) {
            tls_ca_file = try nextRequiredNonBlankArg(&stdout, &args, "--tls-ca-file");
        } else if (std.mem.eql(u8, arg, "--tls-client-auth")) {
            tls_client_auth_str = try nextRequiredNonBlankArg(&stdout, &args, "--tls-client-auth");
            cli_tls_client_auth_set = true;
        } else if (std.mem.eql(u8, arg, "--tls-principal-mapping-rules")) {
            tls_principal_mapping_rules = try nextRequiredNonBlankArg(&stdout, &args, "--tls-principal-mapping-rules");
        } else if (std.mem.eql(u8, arg, "--client-telemetry-export-file")) {
            client_telemetry_export_path = try nextRequiredNonBlankArg(&stdout, &args, "--client-telemetry-export-file");
        } else {
            port = std.fmt.parseInt(u16, arg, 10) catch {
                try stdout.print("  ERROR: unknown argument '{s}'\n", .{arg});
                return error.InvalidConfiguration;
            };
            if (port == 0) {
                try stdout.print("  ERROR: positional port must be in the range 1-65535\n", .{});
                return error.InvalidConfiguration;
            }
            cli_port_set = true;
        }
    }

    // Load config file if specified (CLI flags take precedence)
    var cfg = ConfigFile.init(alloc);
    defer cfg.deinit();

    if (config_path) |cp| {
        cfg.load(cp) catch |err| {
            try stdout.print("  ERROR: Failed to load config '{s}': {}\n", .{ cp, err });
            return error.InvalidConfiguration;
        };
        if (!cli_process_roles_set) {
            if (try configStringStrict(&stdout, &cfg, "process.roles")) |r| {
                process_roles = ProcessRoles.parse(r) catch {
                    try stdout.print("  ERROR: invalid process.roles '{s}'\n", .{r});
                    return error.InvalidConfiguration;
                };
            }
        }
        const config_listeners = try configStringStrict(&stdout, &cfg, "listeners");
        const config_advertised_listeners = try configStringStrict(&stdout, &cfg, "advertised.listeners");
        const config_controller_listener_names = try configListenerNamesStrict(&stdout, &cfg, "controller.listener.names");
        const config_inter_broker_listener_name = try configListenerNameStrict(&stdout, &cfg, "inter.broker.listener.name");
        const config_security_protocol = try configSecurityProtocolStrict(&stdout, &cfg, "security.protocol");
        const config_inter_broker_security_protocol = try configSecurityProtocolStrict(&stdout, &cfg, "security.inter.broker.protocol");
        if (config_inter_broker_listener_name != null and config_inter_broker_security_protocol != null) {
            try stdout.print("  ERROR: config 'inter.broker.listener.name' and 'security.inter.broker.protocol' cannot both be set\n", .{});
            return error.InvalidConfiguration;
        }
        const config_listener_security_protocol_map = try configListenerSecurityProtocolMapStrict(&stdout, &cfg, "listener.security.protocol.map");
        var config_broker_listener: ?config_mod.ListenerEndpoint = null;
        if (config_listeners) |listeners| {
            _ = try parseConfigListenerEndpointTextStrict(&stdout, "listeners", listeners, true);
            if (config_listener_security_protocol_map) |map| {
                try configListenerSecurityProtocolMapCoversListenersStrict(&stdout, map, listeners);
            }
            if (config_advertised_listeners) |advertised| {
                try configAdvertisedListenersMatchListenersStrict(&stdout, advertised, listeners);
            }
            if (config_inter_broker_listener_name) |listener_name| {
                _ = try configListenerEndpointForNameTextStrict(&stdout, listeners, listener_name, true);
            }
            if (process_roles.is_broker) {
                config_broker_listener = if (config_inter_broker_listener_name) |listener_name|
                    try configListenerEndpointForNameTextStrict(&stdout, listeners, listener_name, true)
                else if (config_controller_listener_names) |listener_names|
                    try configNonControllerListenerEndpointTextStrict(&stdout, listeners, listener_names, true)
                else
                    try parseConfigListenerEndpointTextStrict(&stdout, "listeners", listeners, true);
            }
            if (process_roles.is_controller) {
                if (config_controller_listener_names) |listener_names| {
                    _ = try configControllerListenerEndpointTextStrict(&stdout, listeners, listener_names, true);
                }
            }
        }
        if (data_dir == null) data_dir = try configStringStrict(&stdout, &cfg, "log.dirs");
        if (s3_host == null) s3_host = try configStringStrict(&stdout, &cfg, "s3.endpoint.host");
        if (!cli_s3_port_set) try applyConfigPortStrict(&stdout, &cfg, "s3.endpoint.port", &s3_port);
        s3_bucket = try configStringOrStrict(&stdout, &cfg, "s3.bucket", s3_bucket);
        s3_access_key = try configStringOrStrict(&stdout, &cfg, "s3.access.key", s3_access_key);
        s3_secret_key = try configStringOrStrict(&stdout, &cfg, "s3.secret.key", s3_secret_key);
        if (cfg.getString("s3.scheme")) |s| {
            s3_scheme = parseS3Scheme(s) catch {
                try stdout.print("  ERROR: config 's3.scheme' must be http or https, got '{s}'\n", .{s});
                return error.InvalidConfiguration;
            };
        }
        s3_region = try configStringOrStrict(&stdout, &cfg, "s3.region", s3_region);
        const config_s3_path_style = cfg.getBoolStrict("s3.path.style") catch {
            try stdout.print("  ERROR: config 's3.path.style' must be true or false\n", .{});
            return error.InvalidConfiguration;
        };
        if (config_s3_path_style) |value| s3_path_style = value;
        if (s3_tls_ca_file == null) s3_tls_ca_file = try configStringStrict(&stdout, &cfg, "s3.tls.ca.file");
        if (!cli_port_set and process_roles.is_broker) {
            if (config_broker_listener) |listener| {
                port = listener.port;
            }
            try applyConfigPortStrict(&stdout, &cfg, "listeners.port", &port);
        }
        if (!cli_metrics_port_set) try applyConfigPortStrict(&stdout, &cfg, "metrics.port", &metrics_port);
        if (!cli_node_id_set) {
            try applyConfigNodeIdAliasesStrict(&stdout, &cfg, &node_id);
        }
        cluster_id = try configStringOrStrict(&stdout, &cfg, "cluster.id", cluster_id);
        if (!cli_advertised_host_set) {
            if (try configStringStrict(&stdout, &cfg, "advertised.host.name")) |h| advertised_host = h;
            if (config_advertised_listeners) |advertised| {
                const listener = try parseConfigListenerEndpointTextStrict(&stdout, "advertised.listeners", advertised, false);
                advertised_host = listener.host;
            }
        }
        if (voters_str == null) voters_str = try configStringStrict(&stdout, &cfg, "controller.quorum.voters");
        // Load S3 WAL and cache config from config file
        if (!cli_s3_wal_batch_size_set) {
            var s3_wal_batch_size_cfg: u64 = @intCast(s3_wal_batch_size);
            try applyConfigIntStrict(u64, &stdout, &cfg, "s3.wal.batch.size", &s3_wal_batch_size_cfg);
            s3_wal_batch_size = @intCast(s3_wal_batch_size_cfg);
        }
        if (!cli_s3_wal_flush_interval_set) try applyConfigIntStrict(i64, &stdout, &cfg, "s3.wal.flush.interval.ms", &s3_wal_flush_interval);
        if (!cli_s3_wal_flush_mode_set) {
            if (try configStringStrict(&stdout, &cfg, "s3.wal.flush.mode")) |m| s3_wal_flush_mode = m;
        }
        if (!cli_s3_block_cache_size_set) try applyConfigIntStrict(u64, &stdout, &cfg, "s3.block.cache.size", &s3_block_cache_size);
        if (!cli_cache_max_size_set) try applyConfigIntStrict(u64, &stdout, &cfg, "log.cache.max.size", &cache_max_size);
        if (!cli_compaction_interval_set) try applyConfigIntStrict(i64, &stdout, &cfg, "s3.compaction.interval.ms", &compaction_interval);
        if (!cli_controller_port_set) {
            if (process_roles.is_controller) {
                if (config_listeners) |listeners| {
                    if (config_controller_listener_names) |listener_names| {
                        const listener = try configControllerListenerEndpointTextStrict(&stdout, listeners, listener_names, true);
                        controller_port = listener.port;
                    }
                }
            }
            try applyConfigPortStrict(&stdout, &cfg, "controller.listener.port", &controller_port);
        }
        // TLS configuration from config file (CLI flags take precedence)
        if (!cli_security_protocol_set) {
            if (config_security_protocol) |p| {
                security_protocol = p;
            } else if (config_inter_broker_security_protocol) |p| {
                security_protocol = p;
            } else if (config_listener_security_protocol_map) |map| {
                if (config_broker_listener) |listener| {
                    security_protocol = try configListenerSecurityProtocolForNameStrict(&stdout, map, listener.name);
                }
            }
        }
        if (tls_cert_file == null) tls_cert_file = try configStringStrict(&stdout, &cfg, "ssl.certfile");
        if (tls_key_file == null) tls_key_file = try configStringStrict(&stdout, &cfg, "ssl.keyfile");
        if (tls_ca_file == null) tls_ca_file = try configStringStrict(&stdout, &cfg, "ssl.cafile");
        if (!cli_tls_client_auth_set) tls_client_auth_str = try configStringOrStrict(&stdout, &cfg, "ssl.client.auth", tls_client_auth_str);
        if (tls_principal_mapping_rules == null) tls_principal_mapping_rules = try configStringStrict(&stdout, &cfg, "ssl.principal.mapping.rules");
        if (client_telemetry_export_path == null) client_telemetry_export_path = try configStringStrict(&stdout, &cfg, "client.telemetry.export.file");
        try applyConfigBoolStrict(&stdout, &cfg, "sasl.enabled", &sasl_enabled);
        sasl_users = try configSaslUsersOrStrict(&stdout, &cfg, "sasl.users", sasl_users);
        super_users = try configSuperUsersOrStrict(&stdout, &cfg, "super.users", super_users);
        try applyConfigBoolStrict(&stdout, &cfg, "allow.everyone.if.no.acl.found", &allow_everyone_if_no_acl);
        sasl_enabled_mechanisms = try configSaslMechanismsOrStrict(&stdout, &cfg, "sasl.enabled.mechanisms", sasl_enabled_mechanisms);
        oauth_issuer = try configStringOrStrict(&stdout, &cfg, "sasl.oauthbearer.expected.issuer", oauth_issuer);
        oauth_audience = try configStringOrStrict(&stdout, &cfg, "sasl.oauthbearer.expected.audience", oauth_audience);
    }

    if (num_workers == 0) {
        try stdout.print("  ERROR: --workers must be greater than zero\n", .{});
        return error.InvalidConfiguration;
    }

    // Validate: broker-only mode requires --voters to know the controller quorum
    if (process_roles.is_broker and !process_roles.is_controller and voters_str == null) {
        try stdout.print("  ERROR: --voters is required for broker-only mode (process.roles=broker)\n", .{});
        return error.InvalidConfiguration;
    }
    // Validate: port conflict between broker and controller
    if (process_roles.isCombined() and port == controller_port) {
        try stdout.print("  ERROR: --port and --controller-port must differ in combined mode (both are {d})\n", .{port});
        return error.InvalidConfiguration;
    }

    // Ignore SIGPIPE — writing to a closed TCP socket must not kill the broker.
    // Without this, @import("posix_compat").write() on a disconnected client sends SIGPIPE which
    // terminates the process (the default action). We want write() to return
    // error.BrokenPipe instead so the event loop can close the connection cleanly.
    const sigpipe_sa = posix.Sigaction{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.PIPE, &sigpipe_sa, null);

    // Install signal handlers for graceful shutdown
    const sa = posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &sa, null);
    posix.sigaction(posix.SIG.TERM, &sa, null);

    // Parse WAL flush mode string to enum
    const wal_flush_mode = parseWalFlushMode(s3_wal_flush_mode) catch {
        try stdout.print("  ERROR: s3.wal.flush.mode/--s3-wal-flush-mode must be sync, async, or group_commit, got '{s}'\n", .{s3_wal_flush_mode});
        return error.InvalidConfiguration;
    };
    const tls_protocol = parseSecurityProtocol(security_protocol) catch {
        try stdout.print("  ERROR: security.protocol/--security-protocol must be plaintext, ssl, sasl_plaintext, or sasl_ssl, got '{s}'\n", .{security_protocol});
        return error.InvalidConfiguration;
    };
    const client_auth = parseTlsClientAuth(tls_client_auth_str) catch {
        try stdout.print("  ERROR: ssl.client.auth/--tls-client-auth must be none, requested, or required, got '{s}'\n", .{tls_client_auth_str});
        return error.InvalidConfiguration;
    };
    if (tls_protocol == .sasl_plaintext or tls_protocol == .sasl_ssl) sasl_enabled = true;
    const storage_data_dir = try firstLogDirStrict(&stdout, data_dir);
    const replica_directory_ids = Broker.deriveReplicaDirectoryIds(data_dir) catch {
        try stdout.print("  ERROR: log.dirs/--data-dir must contain comma-separated nonblank directories\n", .{});
        return error.InvalidConfiguration;
    };

    // ═══════════════════════════════════════════════════════════
    // CONTROLLER COMPONENTS (if controller role)
    // ═══════════════════════════════════════════════════════════
    var controller: ?*Controller = null;
    var raft_pool: ?RaftClientPool = null;

    if (process_roles.is_controller) {
        const ctrl = try alloc.create(Controller);
        ctrl.* = if (storage_data_dir) |dir|
            Controller.initWithDataDir(alloc, node_id, cluster_id, dir)
        else
            Controller.init(alloc, node_id, cluster_id);
        controller = ctrl;

        if (voters_str == null) {
            try ctrl.raft_state.addVoter(node_id);
        } else {
            raft_pool = RaftClientPool.init(alloc);
            parseAndRegisterVoters(&ctrl.raft_state, voters_str.?, controller_port, &raft_pool.?) catch |err| {
                try stdout.print("  ERROR: Invalid controller quorum voters '{s}': {}\n", .{ voters_str.?, err });
                return error.InvalidConfiguration;
            };
        }

        if (storage_data_dir != null) {
            const recovered = ctrl.raft_state.loadPersistedLog() catch |err| {
                try stdout.print("  ERROR: Controller Raft log recovery failed: {}\n", .{err});
                return error.ControllerRaftLogRecoveryFailed;
            };
            if (recovered > 0) {
                // This implementation persists only the log image, not a separate
                // commit-index file. Static voters must already be registered so
                // UpdateRaftVoter records can reapply endpoint metadata on restart.
                ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
                ctrl.raft_state.applyCommittedConfigs() catch |err| {
                    try stdout.print("  ERROR: Controller Raft config replay failed: {}\n", .{err});
                    return error.ControllerRaftConfigReplayFailed;
                };
                _ = ctrl.replayCommittedControllerMetadataRecords() catch |err| {
                    log.warn("Controller metadata replay failed: {}", .{err});
                };
            }
        }

        if (ctrl.raft_state.quorumSize() <= 1 and ctrl.raft_state.role == .unattached) {
            _ = ctrl.raft_state.startElection() catch |err| {
                try stdout.print("  ERROR: Failed to persist Raft epoch/vote metadata: {}\n", .{err});
                return error.ControllerElectionPersistenceFailed;
            };
            ctrl.raft_state.becomeLeader();
            log.info("Single-node controller elected before serving requests", .{});
        }

        handler_routing.setGlobalController(ctrl);
        global_controller_ptr = ctrl;
    }
    defer if (controller) |ctrl| {
        ctrl.deinit();
        alloc.destroy(ctrl);
        global_controller_ptr = null;
    };
    defer if (raft_pool) |*p| p.deinit();

    // ═══════════════════════════════════════════════════════════
    // BROKER COMPONENTS (if broker role)
    // ═══════════════════════════════════════════════════════════
    var broker: ?*Broker = null;

    if (process_roles.is_broker) {
        const brk = try alloc.create(Broker);
        brk.* = Broker.initWithConfig(alloc, node_id, port, .{
            .data_dir = storage_data_dir,
            .s3_endpoint_host = s3_host,
            .s3_endpoint_port = s3_port,
            .s3_bucket = s3_bucket,
            .s3_access_key = s3_access_key,
            .s3_secret_key = s3_secret_key,
            .s3_scheme = s3_scheme,
            .s3_region = s3_region,
            .s3_path_style = s3_path_style,
            .s3_tls_ca_file = s3_tls_ca_file,
            .advertised_host = advertised_host,
            .cluster_id = cluster_id,
            .s3_wal_batch_size = s3_wal_batch_size,
            .s3_wal_flush_interval_ms = s3_wal_flush_interval,
            .s3_wal_flush_mode = wal_flush_mode,
            .cache_max_size = cache_max_size,
            .s3_block_cache_size = s3_block_cache_size,
            .compaction_interval_ms = compaction_interval,
            .client_telemetry_export_path = client_telemetry_export_path,
            .replica_directory_ids = replica_directory_ids.slice(),
            .sasl_enabled = sasl_enabled,
            .sasl_users = sasl_users,
            .super_users = super_users,
            .allow_everyone_if_no_acl = allow_everyone_if_no_acl,
            .sasl_enabled_mechanisms = sasl_enabled_mechanisms,
            .oauth_issuer = oauth_issuer,
            .oauth_audience = oauth_audience,
        });
        broker = brk;
        // Re-wire internal pointers that became stale after the struct copy
        // (PartitionStore holds pointers to ObjectManager/S3Client inside Broker,
        // but those moved from initWithConfig's stack frame to the heap)
        brk.store.fixupInternalPointers();
        brk.store.setObjectManager(&brk.object_manager);
        if (brk.compaction_manager) |*cm| {
            cm.object_manager = &brk.object_manager;
        }
        // Re-wire metrics pointers from subsystems (cache, s3_client, groups)
        // to the heap-allocated Broker. These were dangling since initWithConfig
        // built the Broker on the stack before moving it to the heap.
        brk.wireInternalPointers();

        if (controller) |ctrl| {
            brk.setRaftState(&ctrl.raft_state);
            ctrl.raft_commit_hook = &applyCommittedAutoMqQuorumRecords;
        }
        if (voters_str) |vs| {
            parseVotersIntoBrokerPeers(brk, vs, port) catch |err| {
                try stdout.print("  ERROR: Invalid controller quorum voters '{s}': {}\n", .{ vs, err });
                return error.InvalidConfiguration;
            };
        }
        if (!process_roles.is_controller) {
            // Broker-only nodes must prove controller registration/heartbeat
            // before accepting produce requests.
            brk.is_fenced_by_controller = true;
        }

        brk.open() catch |err| {
            try stdout.print("  ERROR: Failed to open storage: {}\n", .{err});
            return error.BrokerOpenFailed;
        };

        // Load persisted PreparedObjectRegistry from the Raft data directory.
        // This restores prepared object tracking that survived across a restart,
        // protecting against data loss when the Raft log is truncated by a snapshot.
        if (controller) |ctrl| {
            if (ctrl.raft_state.loadPreparedRegistry() catch |err| {
                try stdout.print("  ERROR: Failed to load prepared.snapshot: {}\n", .{err});
                return error.PreparedSnapshotRecoveryFailed;
            }) |reg_data| {
                defer alloc.free(reg_data);
                brk.object_manager.prepared_registry.deserialize(reg_data) catch |err| {
                    try stdout.print("  ERROR: Malformed prepared.snapshot: {}\n", .{err});
                    return error.PreparedSnapshotRecoveryFailed;
                };
            }
        }

        handler.setGlobalBroker(brk);
        handler_routing.setGlobalBroker(brk);
        global_broker_ptr = brk;
    }
    defer if (broker) |brk| {
        log.info("Shutting down broker (persisting metadata)...", .{});
        brk.deinit();
        alloc.destroy(brk);
        global_broker_ptr = null;
    };

    // ═══════════════════════════════════════════════════════════
    // METADATA CLIENT (broker-only mode)
    // ═══════════════════════════════════════════════════════════
    var metadata_client: ?*MetadataClient = null;

    if (process_roles.is_broker and !process_roles.is_controller) {
        if (broker) |brk| {
            const mc = try alloc.create(MetadataClient);
            mc.* = MetadataClient.init(
                alloc,
                node_id,
                advertised_host,
                port,
                &brk.cached_leader_epoch,
                &brk.cached_broker_epoch,
                brk.localReplicaDirectoryIds(),
                &brk.is_fenced_by_controller,
                &brk.last_successful_heartbeat_ms,
                &global_shutdown,
            );
            metadata_client = mc;
            if (voters_str) |vs| {
                parseVotersIntoMetadataClient(mc, vs) catch |err| {
                    try stdout.print("  ERROR: Invalid controller quorum voters '{s}': {}\n", .{ vs, err });
                    return error.InvalidConfiguration;
                };
            }
        }
    }
    defer if (metadata_client) |mc| {
        mc.deinit();
        alloc.destroy(mc);
    };

    // ═══════════════════════════════════════════════════════════
    // BACKGROUND THREADS
    // ═══════════════════════════════════════════════════════════

    // Election loop (controller or combined mode only)
    var election_state: ElectionLoop = undefined;
    var election_thread: ?std.Thread = null;
    if (controller) |ctrl| {
        election_state = ElectionLoop{
            .raft_state = &ctrl.raft_state,
            .should_stop = &global_shutdown,
            .raft_client_pool = if (raft_pool != null) &(raft_pool.?) else null,
            .pre_snapshot_fn = if (broker != null) &serializePreparedRegistry else null,
            .prepare_snapshot_fn = &prepareRaftMetadataSnapshot,
            .snapshot_allocator = if (broker != null) alloc else null,
        };
        if (election_state.raft_client_pool != null) {
            log.info("Election loop: RaftClientPool configured with peers", .{});
        } else {
            log.info("Election loop: single-node mode (no RaftClientPool)", .{});
        }
        election_thread = std.Thread.spawn(.{}, ElectionLoop.run, .{&election_state}) catch |err| {
            try stdout.print("  ERROR: Failed to start election loop: {}\n", .{err});
            return error.ElectionLoopStartFailed;
        };
    }
    defer if (election_thread) |t| {
        global_shutdown = true;
        t.join();
    };

    // MetadataClient loop (broker-only mode)
    var mc_thread: ?std.Thread = null;
    if (metadata_client) |mc| {
        mc_thread = std.Thread.spawn(.{}, MetadataClient.run, .{mc}) catch |err| {
            try stdout.print("  ERROR: Failed to start metadata client: {}\n", .{err});
            return error.MetadataClientStartFailed;
        };
    }
    defer if (mc_thread) |t| {
        global_shutdown = true;
        t.join();
    };

    // Metrics server (broker mode only)
    var metrics_thread: ?std.Thread = null;
    if (broker) |brk| {
        var metrics_server = MetricsServer.init(alloc, metrics_port, &brk.metrics);
        global_metrics_server = &metrics_server;
        metrics_thread = std.Thread.spawn(.{}, MetricsServer.serve, .{&metrics_server}) catch null;
    }
    defer if (metrics_thread) |t| {
        if (global_metrics_server) |m| m.stop();
        t.join();
    };

    // ═══════════════════════════════════════════════════════════
    // TCP SERVERS
    // ═══════════════════════════════════════════════════════════

    // Print startup banner
    const storage_mode: []const u8 = if (s3_host != null) "S3" else if (storage_data_dir != null) "persistent" else "in-memory";
    try stdout.print("  Node ID: {d}\n", .{node_id});
    try stdout.print("  Cluster: {s}\n", .{cluster_id});
    try stdout.print("  Roles: {s}\n", .{process_roles.name()});
    if (process_roles.is_broker) {
        try stdout.print("  Broker listening on 0.0.0.0:{d}\n", .{port});
    }
    if (process_roles.is_controller) {
        try stdout.print("  Controller listening on 0.0.0.0:{d}\n", .{controller_port});
    }
    if (process_roles.is_broker) {
        try stdout.print("  Metrics on 0.0.0.0:{d}/metrics\n", .{metrics_port});
        try stdout.print("  Storage: {s}", .{storage_mode});
        if (s3_host) |h| try stdout.print(" (s3://{s}:{d}/{s})", .{ h, s3_port, s3_bucket });
        if (data_dir) |dir| try stdout.print(" (wal: {s})", .{dir});
        try stdout.print("\n", .{});
    }
    try stdout.print("  Graceful shutdown: SIGINT/SIGTERM\n\n", .{});

    // Controller server (on background thread if combined, main thread if controller-only)
    var ctrl_thread: ?std.Thread = null;
    if (process_roles.is_controller) {
        if (process_roles.is_broker) {
            // Combined mode: controller on background thread, broker on main thread
            var ctrl_server = try Server.init(alloc, "0.0.0.0", controller_port, &handler_routing.controllerHandleRequest, num_workers);
            global_controller_server = &ctrl_server;
            ctrl_thread = std.Thread.spawn(.{}, Server.serve, .{&ctrl_server}) catch |err| {
                try stdout.print("  ERROR: Failed to start controller server: {}\n", .{err});
                return error.ControllerServerStartFailed;
            };
        }
    }
    defer if (ctrl_thread) |t| {
        if (global_controller_server) |cs| cs.stop();
        t.join();
    };

    // Main server on the main thread
    if (process_roles.is_broker) {
        // Initialize TLS if configured
        var tls_config = TlsConfig{
            .protocol = tls_protocol,
            .cert_file = tls_cert_file,
            .key_file = tls_key_file,
            .ca_file = tls_ca_file,
            .client_auth = client_auth,
            .principal_mapping_rules = tls_principal_mapping_rules,
        };
        tls_config.enabled = tls_config.needsTls();

        var tls_ctx: ?TlsContext = null;
        if (tls_config.needsTls()) {
            tls_ctx = TlsContext.init(alloc, tls_config) catch |err| {
                try stdout.print("  ERROR: Failed to initialize TLS: {s}\n", .{@errorName(err)});
                try stdout.print("  Make sure cert and key files are valid PEM format.\n", .{});
                return error.TlsInitializationFailed;
            };
            try stdout.print("  TLS enabled: protocol={s}\n", .{security_protocol});
        }
        defer if (tls_ctx) |*ctx| ctx.deinit();

        // Broker server on main thread
        var server = try Server.init(alloc, "0.0.0.0", port, &handler_routing.brokerHandleRequest, num_workers);
        if (tls_ctx) |*ctx| {
            server.tls_context = ctx;
        }
        // Wire metrics registry into server for active connection tracking
        if (broker) |brk| {
            server.metrics = &brk.metrics;
        }
        // Wire up group commit flush callbacks for S3 WAL batching
        server.batch_flush_fn = &handler_routing.brokerFlushPendingWal;
        server.has_pending_flush_fn = &handler_routing.brokerHasPendingFlush;
        // Wire up periodic tick for broker maintenance (retention, compaction, sessions)
        server.tick_fn = &handler_routing.brokerTick;
        global_server = &server;
        defer {
            server.stop();
            global_server = null;
        }
        // Signal readiness after all setup is complete
        if (global_metrics_server) |ms| {
            ms.markStartupComplete();
        }
        server.serve() catch |err| {
            log.info("Broker server stopped: {}", .{err});
        };

        // Graceful shutdown sequence: flush WAL and take snapshots before exit.
        // The server's epoll loop has exited (connections drained or timeout),
        // so we now persist all in-flight data to S3.
        performGracefulShutdown(broker, controller);
    } else {
        // Controller-only: controller server on main thread
        var ctrl_server = try Server.init(alloc, "0.0.0.0", controller_port, &handler_routing.controllerHandleRequest, num_workers);
        global_controller_server = &ctrl_server;
        defer {
            ctrl_server.stop();
            global_controller_server = null;
        }
        ctrl_server.serve() catch |err| {
            log.info("Controller server stopped: {}", .{err});
        };

        // Controller-only shutdown: take Raft snapshot
        performGracefulShutdown(null, controller);
    }

    log.info("Server stopped.", .{});
}

/// Graceful shutdown sequence: flush pending data and take snapshots.
///
/// Called after the server's epoll loop has exited (connections drained or drain
/// timeout reached). This ensures all buffered WAL data reaches S3 and metadata
/// snapshots are persisted before the process exits.
///
/// Shutdown order follows AutoMQ's approach:
/// 1. Flush WAL to S3 (ensures no produce data is lost)
/// 2. Take Raft snapshot (persists committed log entries)
/// 3. Broker deinit (persist topics, offsets) happens in defer
fn performGracefulShutdown(broker_opt: ?*Broker, controller_opt: ?*Controller) void {
    log.info("Graceful shutdown: starting cleanup sequence", .{});

    // Step 1: Flush pending WAL data to S3
    if (broker_opt) |brk| {
        const wal_ok = brk.shutdownFlushWal();
        if (!wal_ok) {
            log.warn("Graceful shutdown: WAL flush failed, some data may not have reached S3", .{});
        }
    }

    // Step 2: Serialize prepared object registry and take Raft snapshot.
    // The prepared registry must be serialized BEFORE the snapshot truncates the
    // Raft log, because prepared entries in the truncated log portion would be lost.
    if (controller_opt) |ctrl| {
        var can_snapshot = true;
        if (!prepareControllerMetadataSnapshotForShutdown(ctrl)) can_snapshot = false;
        if (broker_opt) |brk| {
            if (!prepareAutoMqMetadataSnapshotForShutdown(brk)) can_snapshot = false;
            const reg_data = brk.object_manager.prepared_registry.serialize(ctrl.raft_state.allocator) catch |err| blk: {
                log.warn("Graceful shutdown: skipping Raft snapshot because prepared registry serialization failed: {}", .{err});
                can_snapshot = false;
                break :blk null;
            };
            ctrl.raft_state.prepared_registry_data = reg_data;
        }
        if (can_snapshot) {
            ctrl.raft_state.takeSnapshot() catch |err| {
                log.warn("Graceful shutdown: Raft snapshot failed: {}", .{err});
                can_snapshot = false;
            };
        }
        // Free the serialized data after snapshot persistence
        if (ctrl.raft_state.prepared_registry_data) |d| {
            ctrl.raft_state.allocator.free(d);
            ctrl.raft_state.prepared_registry_data = null;
        }
        if (can_snapshot) {
            log.info("Graceful shutdown: Raft snapshot taken (commit_index={d})", .{ctrl.raft_state.commit_index});
        }
    }

    log.info("Graceful shutdown: cleanup sequence complete", .{});
}

fn parseAndRegisterVoters(raft: *RaftState, voters: []const u8, local_controller_port: u16, pool: *RaftClientPool) !void {
    // Format: "0@localhost:9092,1@host2:9093"
    try config_mod.validateControllerVoterSet(raft.allocator, voters);
    if (!(try config_mod.controllerVoterSetContainsNodeIdStrict(voters, raft.node_id))) return error.LocalControllerVoterMissing;
    if (!(try config_mod.controllerVoterSetLocalPortMatchesStrict(voters, raft.node_id, local_controller_port))) return error.LocalControllerVoterPortMismatch;

    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, voters, ',');
    while (entries.next()) |raw_entry| {
        const voter = try config_mod.parseControllerVoter(raw_entry);
        parsed_any = true;
        try raft.addConfiguredControllerVoter(voter.node_id, voter.host, voter.port);

        // Add peer to RaftClientPool (skip self)
        if (voter.node_id != raft.node_id) {
            try pool.addPeer(voter.node_id, voter.host, voter.port);
        }
    }
    if (!parsed_any) return error.InvalidControllerVoter;
}

fn parseVotersIntoMetadataClient(mc: *MetadataClient, voters: []const u8) !void {
    // Format: "0@localhost:9093,1@host2:9093"
    try config_mod.validateControllerVoterSet(mc.allocator, voters);

    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, voters, ',');
    while (entries.next()) |raw_entry| {
        const voter = try config_mod.parseControllerVoter(raw_entry);
        parsed_any = true;
        try mc.addVoter(voter.node_id, voter.host, voter.port);
    }
    if (!parsed_any) return error.InvalidControllerVoter;
}

fn parseVotersIntoBrokerPeers(brk: *Broker, voters: []const u8, broker_port: u16) !void {
    // Static controller voter strings do not carry broker listener ports.
    // Combined-mode Docker/E2E clusters use the same internal broker port on
    // each node, so use the local broker port with the parsed peer host.
    try config_mod.validateControllerVoterSet(brk.allocator, voters);

    var parsed_any = false;
    var entries = std.mem.splitScalar(u8, voters, ',');
    while (entries.next()) |raw_entry| {
        const voter = try config_mod.parseControllerVoter(raw_entry);
        parsed_any = true;
        try brk.addBrokerPeer(voter.node_id, voter.host, broker_port);
    }
    if (!parsed_any) return error.InvalidControllerVoter;
}

const std = @import("std");
const posix = std.posix;
const log = std.log.scoped(.main);

const Server = @import("network/server.zig").Server;
const MetricsServer = @import("network/metrics_server.zig").MetricsServer;
const handler = @import("broker/handler.zig");
const Broker = handler.Broker;
const ConfigFile = @import("config.zig").ConfigFile;
const ElectionLoop = @import("raft/election_loop.zig").ElectionLoop;
const RaftClientPool = @import("network/raft_client.zig").RaftClientPool;

pub const std_options = .{
    .log_level = .info,
};

/// Global pointers for signal handler access.
var global_server: ?*Server = null;
var global_metrics_server: ?*MetricsServer = null;
var global_shutdown: bool = false;

fn handleSignal(sig: i32) callconv(.C) void {
    _ = sig;
    global_shutdown = true;
    if (global_server) |s| s.stop();
    if (global_metrics_server) |m| m.stop();
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();
    try stdout.print(
        \\
        \\  ┌─────────────────────────────────────────────┐
        \\  │            ZMQ Broker v0.8.0                 │
        \\  │   Cloud-native Kafka, rewritten in Zig      │
        \\  └─────────────────────────────────────────────┘
        \\
        \\
    , .{});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // Parse CLI
    var port: u16 = 9092;
    var metrics_port: u16 = 9090;
    var data_dir: ?[]const u8 = null;
    var s3_host: ?[]const u8 = null;
    var s3_port: u16 = 9000;
    var s3_bucket: []const u8 = "automq";
    var node_id: i32 = 0;
    var config_path: ?[]const u8 = null;
    var advertised_host: []const u8 = "localhost";
    var cluster_id: []const u8 = "automq-cluster";
    var voters_str: ?[]const u8 = null;
    var num_workers: usize = 4;
    // Principle 6 fix: Configurable performance parameters via CLI
    var s3_wal_batch_size: usize = 4 * 1024 * 1024;
    var s3_wal_flush_interval: i64 = 250;
    var s3_wal_flush_mode: []const u8 = "sync";
    var cache_max_size: u64 = 256 * 1024 * 1024;
    var s3_block_cache_size: u64 = 64 * 1024 * 1024;
    var compaction_interval: i64 = 300_000;

    var args = try std.process.argsWithAllocator(alloc);
    defer args.deinit();
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--data-dir")) {
            data_dir = args.next();
        } else if (std.mem.eql(u8, arg, "--s3-endpoint")) {
            s3_host = args.next();
        } else if (std.mem.eql(u8, arg, "--s3-port")) {
            if (args.next()) |p| s3_port = std.fmt.parseInt(u16, p, 10) catch 9000;
        } else if (std.mem.eql(u8, arg, "--s3-bucket")) {
            if (args.next()) |b| s3_bucket = b;
        } else if (std.mem.eql(u8, arg, "--metrics-port")) {
            if (args.next()) |p| metrics_port = std.fmt.parseInt(u16, p, 10) catch 9090;
        } else if (std.mem.eql(u8, arg, "--node-id")) {
            if (args.next()) |n| node_id = std.fmt.parseInt(i32, n, 10) catch 0;
        } else if (std.mem.eql(u8, arg, "--advertised-host")) {
            if (args.next()) |h| advertised_host = h;
        } else if (std.mem.eql(u8, arg, "--config")) {
            config_path = args.next();
        } else if (std.mem.eql(u8, arg, "--cluster-id")) {
            if (args.next()) |c| cluster_id = c;
        } else if (std.mem.eql(u8, arg, "--voters")) {
            voters_str = args.next();
        } else if (std.mem.eql(u8, arg, "--workers")) {
            if (args.next()) |w| num_workers = std.fmt.parseInt(usize, w, 10) catch 4;
        // Principle 6 fix: S3 WAL and cache configuration CLI flags
        } else if (std.mem.eql(u8, arg, "--s3-wal-batch-size")) {
            if (args.next()) |v| s3_wal_batch_size = std.fmt.parseInt(usize, v, 10) catch s3_wal_batch_size;
        } else if (std.mem.eql(u8, arg, "--s3-wal-flush-interval")) {
            if (args.next()) |v| s3_wal_flush_interval = std.fmt.parseInt(i64, v, 10) catch s3_wal_flush_interval;
        } else if (std.mem.eql(u8, arg, "--s3-wal-flush-mode")) {
            if (args.next()) |v| s3_wal_flush_mode = v;
        } else if (std.mem.eql(u8, arg, "--cache-max-size")) {
            if (args.next()) |v| cache_max_size = std.fmt.parseInt(u64, v, 10) catch cache_max_size;
        } else if (std.mem.eql(u8, arg, "--s3-block-cache-size")) {
            if (args.next()) |v| s3_block_cache_size = std.fmt.parseInt(u64, v, 10) catch s3_block_cache_size;
        } else if (std.mem.eql(u8, arg, "--compaction-interval")) {
            if (args.next()) |v| compaction_interval = std.fmt.parseInt(i64, v, 10) catch compaction_interval;
        } else {
            port = std.fmt.parseInt(u16, arg, 10) catch port;
        }
    }

    // Load config file if specified (CLI flags take precedence)
    var cfg = ConfigFile.init(alloc);
    defer cfg.deinit();

    if (config_path) |cp| {
        cfg.load(cp) catch |err| {
            try stdout.print("  WARNING: Failed to load config '{s}': {}\n", .{ cp, err });
        };
        if (data_dir == null) data_dir = cfg.getString("log.dirs");
        if (s3_host == null) s3_host = cfg.getString("s3.endpoint.host");
        s3_port = cfg.getInt(u16, "s3.endpoint.port", s3_port);
        s3_bucket = cfg.getStringOr("s3.bucket", s3_bucket);
        port = cfg.getInt(u16, "listeners.port", port);
        metrics_port = cfg.getInt(u16, "metrics.port", metrics_port);
        node_id = cfg.getInt(i32, "broker.id", node_id);
        cluster_id = cfg.getStringOr("cluster.id", cluster_id);
        // Principle 6 fix: Load S3 WAL and cache config from config file
        s3_wal_batch_size = @intCast(cfg.getInt(u64, "s3.wal.batch.size", @intCast(s3_wal_batch_size)));
        s3_wal_flush_interval = cfg.getInt(i64, "s3.wal.flush.interval.ms", s3_wal_flush_interval);
        if (cfg.getString("s3.wal.flush.mode")) |m| s3_wal_flush_mode = m;
        s3_block_cache_size = @intCast(cfg.getInt(u64, "s3.block.cache.size", @intCast(s3_block_cache_size)));
        cache_max_size = @intCast(cfg.getInt(u64, "log.cache.max.size", @intCast(cache_max_size)));
        compaction_interval = cfg.getInt(i64, "s3.compaction.interval.ms", compaction_interval);
    }

    // Ignore SIGPIPE — writing to a closed TCP socket must not kill the broker.
    // Without this, posix.write() on a disconnected client sends SIGPIPE which
    // terminates the process (the default action). We want write() to return
    // error.BrokenPipe instead so the event loop can close the connection cleanly.
    const sigpipe_sa = posix.Sigaction{
        .handler = .{ .handler = posix.SIG.IGN },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    try posix.sigaction(posix.SIG.PIPE, &sigpipe_sa, null);

    // Install signal handlers for graceful shutdown
    const sa = posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    try posix.sigaction(posix.SIG.INT, &sa, null);
    try posix.sigaction(posix.SIG.TERM, &sa, null);

    // Principle 6 fix: Parse WAL flush mode string to enum
    const wal_flush_mode: handler.Broker.WalFlushMode = if (std.mem.eql(u8, s3_wal_flush_mode, "async"))
        .async_flush
    else
        .sync;

    var broker = Broker.initWithConfig(alloc, node_id, port, .{
        .data_dir = data_dir,
        .s3_endpoint_host = s3_host,
        .s3_endpoint_port = s3_port,
        .s3_bucket = s3_bucket,
        .advertised_host = advertised_host,
        // Principle 6 fix: Pass through all configurable performance parameters
        .s3_wal_batch_size = s3_wal_batch_size,
        .s3_wal_flush_interval_ms = s3_wal_flush_interval,
        .s3_wal_flush_mode = wal_flush_mode,
        .cache_max_size = cache_max_size,
        .s3_block_cache_size = s3_block_cache_size,
        .compaction_interval_ms = compaction_interval,
    });
    defer {
        log.info("Shutting down broker (persisting metadata)...", .{});
        broker.deinit();
    }
    handler.setGlobalBroker(&broker);

    broker.open() catch |err| {
        try stdout.print("  ERROR: Failed to open storage: {}\n", .{err});
        return;
    };

    // Register self as voter if no voters specified (single-node mode)
    var raft_pool: ?RaftClientPool = null;
    if (voters_str == null) {
        broker.raft_state.addVoter(node_id) catch {};
    } else {
        // Parse voters: "0@localhost:9092,1@host2:9092,2@host3:9092"
        raft_pool = RaftClientPool.init(alloc);
        parseAndRegisterVoters(&broker, voters_str.?, &raft_pool.?);
    }
    defer if (raft_pool) |*pool| pool.deinit();

    // Start KRaft election loop on background thread
    var election_state = ElectionLoop{
        .raft_state = &broker.raft_state,
        .should_stop = &global_shutdown,
        .raft_client_pool = if (raft_pool != null) &(raft_pool.?) else null,
    };
    if (election_state.raft_client_pool != null) {
        log.info("Election loop: RaftClientPool configured with peers", .{});
    } else {
        log.info("Election loop: single-node mode (no RaftClientPool)", .{});
    }
    const election_thread = std.Thread.spawn(.{}, ElectionLoop.run, .{&election_state}) catch |err| {
        try stdout.print("  WARNING: Failed to start election loop: {}\n", .{err});
        return startMainServer(alloc, port, s3_host, s3_port, s3_bucket, data_dir, metrics_port, cluster_id, node_id, num_workers, stdout);
    };
    defer {
        global_shutdown = true;
        election_thread.join();
    }

    // Start Prometheus metrics & health server on a separate thread
    var metrics_server = MetricsServer.init(alloc, metrics_port, &broker.metrics);
    global_metrics_server = &metrics_server;
    const metrics_thread = std.Thread.spawn(.{}, MetricsServer.serve, .{&metrics_server}) catch |err| {
        try stdout.print("  WARNING: Failed to start metrics server: {}\n", .{err});
        return startMainServer(alloc, port, s3_host, s3_port, s3_bucket, data_dir, metrics_port, cluster_id, node_id, num_workers, stdout);
    };
    defer {
        metrics_server.stop();
        metrics_thread.join();
    }

    try startMainServer(alloc, port, s3_host, s3_port, s3_bucket, data_dir, metrics_port, cluster_id, node_id, num_workers, stdout);
}

fn parseAndRegisterVoters(broker: *Broker, voters: []const u8, pool: *RaftClientPool) void {
    // Format: "0@localhost:9092,1@host2:9093"
    var start: usize = 0;
    for (voters, 0..) |c, i| {
        if (c == ',' or i == voters.len - 1) {
            const end = if (c == ',') i else i + 1;
            const entry = voters[start..end];
            // Parse "id@host:port"
            if (std.mem.indexOf(u8, entry, "@")) |at_pos| {
                const id_str = entry[0..at_pos];
                const voter_id = std.fmt.parseInt(i32, id_str, 10) catch continue;
                broker.raft_state.addVoter(voter_id) catch continue;

                // Add peer to RaftClientPool (skip self)
                if (voter_id != broker.raft_state.node_id) {
                    const addr_part = entry[at_pos + 1 ..];
                    if (std.mem.indexOf(u8, addr_part, ":")) |colon| {
                        const host = addr_part[0..colon];
                        const port_str = addr_part[colon + 1 ..];
                        const peer_port = std.fmt.parseInt(u16, port_str, 10) catch continue;
                        pool.addPeer(voter_id, host, peer_port) catch continue;
                    }
                }
            }
            start = i + 1;
        }
    }
}

fn startMainServer(alloc: std.mem.Allocator, port: u16, s3_host: ?[]const u8, s3_port: u16, s3_bucket: []const u8, data_dir: ?[]const u8, metrics_port: u16, cluster_id: []const u8, node_id: i32, num_workers: usize, stdout: anytype) !void {
    const storage_mode = if (s3_host != null) "S3" else if (data_dir != null) "persistent" else "in-memory";
    try stdout.print("  Node ID: {d}\n", .{node_id});
    try stdout.print("  Cluster: {s}\n", .{cluster_id});
    try stdout.print("  Listening on 0.0.0.0:{d}\n", .{port});
    try stdout.print("  Metrics on 0.0.0.0:{d}/metrics\n", .{metrics_port});
    try stdout.print("  Endpoints: /health, /ready, /metrics\n", .{});
    try stdout.print("  Storage: {s}", .{storage_mode});
    if (s3_host) |h| try stdout.print(" (s3://{s}:{d}/{s})", .{ h, s3_port, s3_bucket });
    if (data_dir) |dir| try stdout.print(" (wal: {s})", .{dir});
    try stdout.print("\n", .{});
    try stdout.print("  APIs: 41 Kafka APIs + KRaft consensus\n", .{});
    try stdout.print("  Graceful shutdown: SIGINT/SIGTERM\n\n", .{});

    var server = try Server.init(alloc, "0.0.0.0", port, &handler.handleRequest, num_workers);
    global_server = &server;
    defer {
        server.stop();
        global_server = null;
    }

    server.serve() catch |err| {
        log.info("Server stopped: {}", .{err});
    };

    log.info("Server stopped.", .{});
}

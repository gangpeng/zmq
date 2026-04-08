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
const RaftState = @import("raft/state.zig").RaftState;
const ProcessRoles = @import("roles.zig").ProcessRoles;
const Controller = @import("controller/controller.zig").Controller;
const MetadataClient = @import("controller/metadata_client.zig").MetadataClient;
const handler_routing = @import("network/handler_routing.zig");

pub const std_options = .{
    .log_level = .info,
};

/// Global pointers for signal handler access.
var global_server: ?*Server = null;
var global_controller_server: ?*Server = null;
var global_metrics_server: ?*MetricsServer = null;
var global_shutdown: bool = false;

fn handleSignal(sig: i32) callconv(.C) void {
    _ = sig;
    global_shutdown = true;
    if (global_server) |s| s.stop();
    if (global_controller_server) |s| s.stop();
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
    // Process roles: controller, broker, or controller,broker (default).
    var process_roles: ProcessRoles = ProcessRoles.combined;
    // Controller listener port (KRaft consensus + broker registration).
    var controller_port: u16 = 9093;
    // Configurable S3 WAL and performance parameters
    var s3_wal_batch_size: usize = 4 * 1024 * 1024;
    var s3_wal_flush_interval: i64 = 250;
    var s3_wal_flush_mode: []const u8 = "group_commit";
    var cache_max_size: u64 = 256 * 1024 * 1024;
    var s3_block_cache_size: u64 = 64 * 1024 * 1024;
    var compaction_interval: i64 = 300_000;

    var args = try std.process.argsWithAllocator(alloc);
    defer args.deinit();
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port")) {
            if (args.next()) |p| port = std.fmt.parseInt(u16, p, 10) catch port;
        } else if (std.mem.eql(u8, arg, "--data-dir")) {
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
        } else if (std.mem.eql(u8, arg, "--process-roles")) {
            if (args.next()) |r| process_roles = ProcessRoles.parse(r) catch ProcessRoles.combined;
        } else if (std.mem.eql(u8, arg, "--controller-port")) {
            if (args.next()) |p| controller_port = std.fmt.parseInt(u16, p, 10) catch 9093;
        // S3 WAL and cache configuration CLI flags
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
        // Load S3 WAL and cache config from config file
        s3_wal_batch_size = @intCast(cfg.getInt(u64, "s3.wal.batch.size", @intCast(s3_wal_batch_size)));
        s3_wal_flush_interval = cfg.getInt(i64, "s3.wal.flush.interval.ms", s3_wal_flush_interval);
        if (cfg.getString("s3.wal.flush.mode")) |m| s3_wal_flush_mode = m;
        s3_block_cache_size = @intCast(cfg.getInt(u64, "s3.block.cache.size", @intCast(s3_block_cache_size)));
        cache_max_size = @intCast(cfg.getInt(u64, "log.cache.max.size", @intCast(cache_max_size)));
        compaction_interval = cfg.getInt(i64, "s3.compaction.interval.ms", compaction_interval);
        // Process role and controller port from config file (CLI takes precedence)
        if (cfg.getString("process.roles")) |r| {
            process_roles = ProcessRoles.parse(r) catch process_roles;
        }
        controller_port = cfg.getInt(u16, "controller.listener.port", controller_port);
    }

    // Validate: broker-only mode requires --voters to know the controller quorum
    if (process_roles.is_broker and !process_roles.is_controller and voters_str == null) {
        try stdout.print("  ERROR: --voters is required for broker-only mode (process.roles=broker)\n", .{});
        return;
    }
    // Validate: port conflict between broker and controller
    if (process_roles.isCombined() and port == controller_port) {
        try stdout.print("  ERROR: --port and --controller-port must differ in combined mode (both are {d})\n", .{port});
        return;
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

    // Parse WAL flush mode string to enum
    const wal_flush_mode: handler.Broker.WalFlushMode = if (std.mem.eql(u8, s3_wal_flush_mode, "async"))
        .async_flush
    else if (std.mem.eql(u8, s3_wal_flush_mode, "group_commit"))
        .group_commit
    else
        .sync;

    // ═══════════════════════════════════════════════════════════
    // CONTROLLER COMPONENTS (if controller role)
    // ═══════════════════════════════════════════════════════════
    var controller: ?*Controller = null;
    var raft_pool: ?RaftClientPool = null;

    if (process_roles.is_controller) {
        const ctrl = try alloc.create(Controller);
        ctrl.* = Controller.init(alloc, node_id, cluster_id);
        controller = ctrl;

        if (voters_str == null) {
            ctrl.raft_state.addVoter(node_id) catch {};
        } else {
            raft_pool = RaftClientPool.init(alloc);
            parseAndRegisterVoters(&ctrl.raft_state, voters_str.?, &raft_pool.?);
        }

        handler_routing.setGlobalController(ctrl);
    }
    defer if (controller) |ctrl| {
        ctrl.deinit();
        alloc.destroy(ctrl);
    };
    defer if (raft_pool) |*p| p.deinit();

    // ═══════════════════════════════════════════════════════════
    // BROKER COMPONENTS (if broker role)
    // ═══════════════════════════════════════════════════════════
    var broker: ?*Broker = null;

    if (process_roles.is_broker) {
        const brk = try alloc.create(Broker);
        brk.* = Broker.initWithConfig(alloc, node_id, port, .{
            .data_dir = data_dir,
            .s3_endpoint_host = s3_host,
            .s3_endpoint_port = s3_port,
            .s3_bucket = s3_bucket,
            .advertised_host = advertised_host,
            .s3_wal_batch_size = s3_wal_batch_size,
            .s3_wal_flush_interval_ms = s3_wal_flush_interval,
            .s3_wal_flush_mode = wal_flush_mode,
            .cache_max_size = cache_max_size,
            .s3_block_cache_size = s3_block_cache_size,
            .compaction_interval_ms = compaction_interval,
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

        if (controller) |ctrl| {
            brk.setRaftState(&ctrl.raft_state);
        }

        brk.open() catch |err| {
            try stdout.print("  ERROR: Failed to open storage: {}\n", .{err});
            return;
        };

        handler.setGlobalBroker(brk);
        handler_routing.setGlobalBroker(brk);
    }
    defer if (broker) |brk| {
        log.info("Shutting down broker (persisting metadata)...", .{});
        brk.deinit();
        alloc.destroy(brk);
    };

    // ═══════════════════════════════════════════════════════════
    // METADATA CLIENT (broker-only mode)
    // ═══════════════════════════════════════════════════════════
    var metadata_client: ?*MetadataClient = null;

    if (process_roles.is_broker and !process_roles.is_controller) {
        if (broker) |brk| {
            const mc = try alloc.create(MetadataClient);
            mc.* = MetadataClient.init(
                alloc, node_id, advertised_host, port,
                &brk.cached_leader_epoch,
                &brk.is_fenced_by_controller,
                &brk.last_successful_heartbeat_ms,
                &global_shutdown,
            );
            metadata_client = mc;
            if (voters_str) |vs| {
                parseVotersIntoMetadataClient(mc, vs);
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
        };
        if (election_state.raft_client_pool != null) {
            log.info("Election loop: RaftClientPool configured with peers", .{});
        } else {
            log.info("Election loop: single-node mode (no RaftClientPool)", .{});
        }
        election_thread = std.Thread.spawn(.{}, ElectionLoop.run, .{&election_state}) catch |err| {
            try stdout.print("  WARNING: Failed to start election loop: {}\n", .{err});
            return;
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
            try stdout.print("  WARNING: Failed to start metadata client: {}\n", .{err});
            return;
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
    const storage_mode: []const u8 = if (s3_host != null) "S3" else if (data_dir != null) "persistent" else "in-memory";
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
                try stdout.print("  WARNING: Failed to start controller server: {}\n", .{err});
                return;
            };
        }
    }
    defer if (ctrl_thread) |t| {
        if (global_controller_server) |cs| cs.stop();
        t.join();
    };

    // Main server on the main thread
    if (process_roles.is_broker) {
        // Broker server on main thread
        var server = try Server.init(alloc, "0.0.0.0", port, &handler_routing.brokerHandleRequest, num_workers);
        // Wire up group commit flush callbacks for S3 WAL batching
        server.batch_flush_fn = &handler_routing.brokerFlushPendingWal;
        server.has_pending_flush_fn = &handler_routing.brokerHasPendingFlush;
        global_server = &server;
        defer {
            server.stop();
            global_server = null;
        }
        server.serve() catch |err| {
            log.info("Broker server stopped: {}", .{err});
        };
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
    }

    log.info("Server stopped.", .{});
}

fn parseAndRegisterVoters(raft: *RaftState, voters: []const u8, pool: *RaftClientPool) void {
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
                raft.addVoter(voter_id) catch continue;

                // Add peer to RaftClientPool (skip self)
                if (voter_id != raft.node_id) {
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

fn parseVotersIntoMetadataClient(mc: *MetadataClient, voters: []const u8) void {
    // Format: "0@localhost:9093,1@host2:9093"
    var start: usize = 0;
    for (voters, 0..) |c, i| {
        if (c == ',' or i == voters.len - 1) {
            const end = if (c == ',') i else i + 1;
            const entry = voters[start..end];
            if (std.mem.indexOf(u8, entry, "@")) |at_pos| {
                const id_str = entry[0..at_pos];
                const voter_id = std.fmt.parseInt(i32, id_str, 10) catch continue;
                const addr_part = entry[at_pos + 1 ..];
                if (std.mem.indexOf(u8, addr_part, ":")) |colon| {
                    const host = addr_part[0..colon];
                    const port_str = addr_part[colon + 1 ..];
                    const voter_port = std.fmt.parseInt(u16, port_str, 10) catch continue;
                    mc.addVoter(voter_id, host, voter_port) catch continue;
                }
            }
            start = i + 1;
        }
    }
}

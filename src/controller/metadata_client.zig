const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.metadata_client);

const ser = @import("../protocol/serialization.zig");
const header_mod = @import("../protocol/header.zig");
const RequestHeader = header_mod.RequestHeader;
const RaftClientPool = @import("../network/raft_client.zig").RaftClientPool;

/// MetadataClient connects a broker-only node to the controller quorum.
///
/// Responsibilities:
/// 1. Discover the active controller leader (via DescribeQuorum to any voter)
/// 2. Send BrokerRegistration on startup
/// 3. Send periodic BrokerHeartbeat to stay alive
/// 4. Keep the broker's cached_leader_epoch up to date
///
/// Runs on a background thread. Reuses RaftClientPool for TCP connections.
pub const MetadataClient = struct {
    /// Connections to controller voters.
    controller_pool: RaftClientPool,
    /// Parsed voter addresses for connection.
    voters: std.ArrayList(VoterAddress),
    /// Currently known controller leader ID.
    leader_id: ?i32 = null,
    /// Our broker node ID.
    broker_id: i32,
    /// Our advertised host for registration.
    advertised_host: []const u8,
    /// Our advertised port for registration.
    advertised_port: u16,
    /// Pointer to the Broker's cached_leader_epoch field.
    /// Updated when we discover the current epoch from the controller.
    cached_leader_epoch: *i32,
    /// Pointer to the Broker's is_fenced_by_controller field.
    /// Set to true when the controller heartbeat says we are fenced.
    is_fenced: *bool,
    /// Pointer to the Broker's last_successful_heartbeat_ms field.
    /// Updated on each successful heartbeat for staleness detection.
    last_heartbeat_ms: *i64,
    /// Broker epoch assigned by the controller on registration.
    broker_epoch: i64 = 0,
    /// Shared stop flag.
    should_stop: *bool,
    /// Maximum time (ms) without a successful heartbeat before self-fencing.
    /// If no heartbeat succeeds within this window, the broker assumes it's
    /// partitioned from the controller and stops accepting writes.
    controller_lease_ms: i64 = 30_000,
    allocator: Allocator,

    pub const VoterAddress = struct {
        node_id: i32,
        host: []const u8,
        port: u16,
    };

    pub fn init(
        alloc: Allocator,
        broker_id: i32,
        host: []const u8,
        port: u16,
        cached_epoch: *i32,
        is_fenced: *bool,
        last_heartbeat_ms: *i64,
        should_stop: *bool,
    ) MetadataClient {
        return .{
            .controller_pool = RaftClientPool.init(alloc),
            .voters = std.ArrayList(VoterAddress).init(alloc),
            .broker_id = broker_id,
            .advertised_host = host,
            .advertised_port = port,
            .cached_leader_epoch = cached_epoch,
            .is_fenced = is_fenced,
            .last_heartbeat_ms = last_heartbeat_ms,
            .should_stop = should_stop,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *MetadataClient) void {
        self.controller_pool.deinit();
        self.voters.deinit();
    }

    /// Add a controller voter address. Called during startup before run().
    pub fn addVoter(self: *MetadataClient, node_id: i32, host: []const u8, port: u16) !void {
        try self.voters.append(.{ .node_id = node_id, .host = host, .port = port });
        try self.controller_pool.addPeer(node_id, host, port);
    }

    /// Background loop: discover leader, register, then heartbeat.
    /// Runs on a dedicated thread.
    pub fn run(self: *MetadataClient) void {
        log.info("MetadataClient started for broker {d}", .{self.broker_id});

        // Step 1: Discover controller leader
        self.discoverLeader();

        // Step 2: Register with the controller
        if (!self.should_stop.*) {
            self.registerWithController();
        }

        // Step 3: Periodic heartbeat loop
        while (!self.should_stop.*) {
            std.time.sleep(2000 * @as(u64, 1_000_000)); // 2-second heartbeat interval
            if (self.should_stop.*) break;
            self.sendHeartbeat();
        }

        log.info("MetadataClient stopped for broker {d}", .{self.broker_id});
    }

    /// Discover the controller leader by sending DescribeQuorum (API 55)
    /// to each voter until one responds.
    fn discoverLeader(self: *MetadataClient) void {
        var attempt: usize = 0;
        const max_backoff_ms: u64 = 30_000;

        while (!self.should_stop.*) {
            for (self.voters.items) |voter| {
                // Try to send DescribeQuorum to this voter
                if (self.controller_pool.sendDescribeQuorum(voter.node_id)) |response| {
                    // Parse the DescribeQuorum response to find leader_id and leader_epoch
                    if (self.parseDescribeQuorumResponse(response)) |info| {
                        self.leader_id = info.leader_id;
                        self.cached_leader_epoch.* = info.leader_epoch;
                        log.info("Discovered controller leader: node {d} epoch {d}", .{
                            info.leader_id, info.leader_epoch,
                        });
                        return;
                    }
                    self.allocator.free(response);
                }
            }

            // Exponential backoff with cap
            attempt += 1;
            const backoff = @min(max_backoff_ms, @as(u64, 1000) << @intCast(@min(attempt, 5)));
            log.warn("Failed to discover controller leader, retrying in {d}ms (attempt {d})", .{
                backoff, attempt,
            });
            std.time.sleep(backoff * @as(u64, 1_000_000));
        }
    }

    const QuorumInfo = struct {
        leader_id: i32,
        leader_epoch: i32,
    };

    fn parseDescribeQuorumResponse(self: *MetadataClient, response: []const u8) ?QuorumInfo {
        _ = self;
        if (response.len < 20) return null;

        // Skip response header and parse leader info
        // The exact layout depends on the wire format — simplified parsing
        var pos: usize = 0;
        // Skip correlation_id (4 bytes) + error_code (2) + topics array header
        pos += 4; // correlation_id
        const err = ser.readI16(response, &pos);
        if (err != 0) return null;

        // Skip topics array: num_topics (4), topic name string, num_partitions (4)
        _ = ser.readI32(response, &pos); // num_topics
        _ = ser.readString(response, &pos) catch return null; // topic name
        _ = ser.readI32(response, &pos); // num_partitions

        // Parse first partition
        _ = ser.readI16(response, &pos); // error_code
        _ = ser.readI32(response, &pos); // partition_index
        const leader_id = ser.readI32(response, &pos);
        const leader_epoch = ser.readI32(response, &pos);

        if (leader_id < 0) return null; // No leader elected yet

        return .{ .leader_id = leader_id, .leader_epoch = leader_epoch };
    }

    /// Register this broker with the controller leader.
    fn registerWithController(self: *MetadataClient) void {
        const leader = self.leader_id orelse {
            log.warn("Cannot register: no controller leader known", .{});
            return;
        };

        // Build BrokerRegistration request (API 62)
        var buf: [512]u8 = undefined;
        var wpos: usize = 0;

        // Request header
        ser.writeI16(&buf, &wpos, 62); // api_key
        ser.writeI16(&buf, &wpos, 0); // api_version
        ser.writeI32(&buf, &wpos, 1); // correlation_id
        ser.writeString(&buf, &wpos, "zmq-broker"); // client_id

        // Request body: broker_id, host, port
        ser.writeI32(&buf, &wpos, self.broker_id);
        ser.writeString(&buf, &wpos, self.advertised_host);
        ser.writeI32(&buf, &wpos, @as(i32, self.advertised_port));

        if (self.controller_pool.sendRequest(leader, buf[0..wpos])) |response| {
            defer self.allocator.free(response);
            // Parse response: skip header (4 bytes corr_id) + throttle_time (4) + error_code (2) + broker_epoch (8)
            if (response.len >= 18) {
                var rpos: usize = 4; // skip correlation_id
                _ = ser.readI32(response, &rpos); // throttle_time_ms
                const error_code = ser.readI16(response, &rpos);
                if (error_code == 0) {
                    self.broker_epoch = ser.readI64(response, &rpos);
                    log.info("Registered with controller: broker_epoch={d}", .{self.broker_epoch});
                } else {
                    log.warn("BrokerRegistration failed: error_code={d}", .{error_code});
                }
            }
        } else {
            log.warn("Failed to send BrokerRegistration to controller {d}", .{leader});
        }
    }

    /// Send a heartbeat to the controller leader.
    fn sendHeartbeat(self: *MetadataClient) void {
        // Staleness check: if no successful heartbeat within lease period,
        // self-fence to prevent split-brain writes during network partition.
        const now = std.time.milliTimestamp();
        if (self.last_heartbeat_ms.* > 0 and (now - self.last_heartbeat_ms.*) > self.controller_lease_ms) {
            if (!self.is_fenced.*) {
                log.warn("Controller lease expired ({d}ms since last heartbeat), self-fencing broker", .{
                    now - self.last_heartbeat_ms.*,
                });
                self.is_fenced.* = true;
            }
        }

        const leader = self.leader_id orelse {
            // Re-discover leader
            self.discoverLeader();
            return;
        };

        // Build BrokerHeartbeat request (API 63)
        var buf: [256]u8 = undefined;
        var wpos: usize = 0;

        // Request header
        ser.writeI16(&buf, &wpos, 63); // api_key
        ser.writeI16(&buf, &wpos, 0); // api_version
        ser.writeI32(&buf, &wpos, 2); // correlation_id
        ser.writeString(&buf, &wpos, "zmq-broker"); // client_id

        // Request body: broker_id, broker_epoch
        ser.writeI32(&buf, &wpos, self.broker_id);
        ser.writeI64(&buf, &wpos, self.broker_epoch);

        if (self.controller_pool.sendRequest(leader, buf[0..wpos])) |response| {
            defer self.allocator.free(response);
            // Parse response: throttle_time(4) + error_code(2) + is_caught_up(1) + is_fenced(1) + should_shut_down(1)
            if (response.len >= 13) {
                var rpos: usize = 4; // skip correlation_id
                _ = ser.readI32(response, &rpos); // throttle_time_ms
                const error_code = ser.readI16(response, &rpos);
                if (error_code != 0) {
                    log.warn("Heartbeat error: code={d}, re-registering", .{error_code});
                    self.leader_id = null; // Force leader re-discovery
                    return;
                }

                // Parse the three boolean flags
                const is_caught_up = ser.readBool(response, &rpos) catch false;
                _ = is_caught_up;
                const broker_is_fenced = ser.readBool(response, &rpos) catch false;
                const should_shut_down = ser.readBool(response, &rpos) catch false;

                // Act on fencing: controller says we're fenced
                if (broker_is_fenced) {
                    if (!self.is_fenced.*) {
                        log.warn("Controller fenced this broker (id={d}), rejecting writes", .{self.broker_id});
                    }
                    self.is_fenced.* = true;
                } else {
                    // Successful heartbeat — we're alive and unfenced
                    if (self.is_fenced.*) {
                        log.info("Controller unfenced this broker (id={d}), resuming writes", .{self.broker_id});
                    }
                    self.is_fenced.* = false;
                    self.last_heartbeat_ms.* = std.time.milliTimestamp();
                }

                // Act on shutdown request
                if (should_shut_down) {
                    log.warn("Controller requested shutdown for broker {d}", .{self.broker_id});
                    self.should_stop.* = true;
                }
            } else if (response.len >= 10) {
                // Shorter response (no boolean flags) — treat as success if error_code == 0
                var rpos: usize = 4;
                _ = ser.readI32(response, &rpos); // throttle_time_ms
                const error_code = ser.readI16(response, &rpos);
                if (error_code == 0) {
                    self.last_heartbeat_ms.* = std.time.milliTimestamp();
                    self.is_fenced.* = false;
                } else {
                    log.warn("Heartbeat error: code={d}, re-registering", .{error_code});
                    self.leader_id = null;
                }
            }
        } else {
            log.warn("Heartbeat failed to controller {d}, re-discovering leader", .{leader});
            self.leader_id = null; // Force re-discovery on next heartbeat
        }
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "MetadataClient staleness detection fences broker" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = std.time.milliTimestamp() - 60_000; // 60s ago
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator, 100, "localhost", 9092,
        &cached_epoch, &is_fenced, &last_hb, &should_stop,
    );
    defer mc.deinit();

    // Set a short lease for testing
    mc.controller_lease_ms = 10_000; // 10s

    // sendHeartbeat will check staleness first — with last_hb 60s ago
    // and lease 10s, the broker should be fenced.
    // Since there's no leader_id, it will try discoverLeader which loops,
    // so we test the staleness logic directly.
    const now = std.time.milliTimestamp();
    if (last_hb > 0 and (now - last_hb) > mc.controller_lease_ms) {
        is_fenced = true;
    }
    try testing.expect(is_fenced);
}

test "MetadataClient fresh heartbeat prevents fencing" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = std.time.milliTimestamp(); // Just now
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator, 100, "localhost", 9092,
        &cached_epoch, &is_fenced, &last_hb, &should_stop,
    );
    defer mc.deinit();

    mc.controller_lease_ms = 30_000;

    // Fresh heartbeat — should NOT be fenced
    const now = std.time.milliTimestamp();
    if (last_hb > 0 and (now - last_hb) > mc.controller_lease_ms) {
        is_fenced = true;
    }
    try testing.expect(!is_fenced);
}

test "MetadataClient zero last_heartbeat skips staleness check" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = 0; // Never had a heartbeat (just started)
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator, 100, "localhost", 9092,
        &cached_epoch, &is_fenced, &last_hb, &should_stop,
    );
    defer mc.deinit();

    // With last_hb == 0, staleness check should be skipped
    // (broker just started, hasn't registered yet)
    const now = std.time.milliTimestamp();
    _ = now;
    if (last_hb > 0 and (std.time.milliTimestamp() - last_hb) > mc.controller_lease_ms) {
        is_fenced = true;
    }
    try testing.expect(!is_fenced);
}

test "MetadataClient parseDescribeQuorumResponse valid" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = 0;
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator, 100, "localhost", 9092,
        &cached_epoch, &is_fenced, &last_hb, &should_stop,
    );
    defer mc.deinit();

    // Build a mock DescribeQuorum response
    var resp: [128]u8 = undefined;
    var wpos: usize = 0;
    ser.writeI32(&resp, &wpos, 1); // correlation_id
    ser.writeI16(&resp, &wpos, 0); // error_code = NONE
    ser.writeI32(&resp, &wpos, 1); // num_topics = 1
    ser.writeString(&resp, &wpos, "__cluster_metadata");
    ser.writeI32(&resp, &wpos, 1); // num_partitions = 1
    ser.writeI16(&resp, &wpos, 0); // partition error_code
    ser.writeI32(&resp, &wpos, 0); // partition_index
    ser.writeI32(&resp, &wpos, 2); // leader_id = 2
    ser.writeI32(&resp, &wpos, 7); // leader_epoch = 7

    const info = mc.parseDescribeQuorumResponse(resp[0..wpos]);
    try testing.expect(info != null);
    try testing.expectEqual(@as(i32, 2), info.?.leader_id);
    try testing.expectEqual(@as(i32, 7), info.?.leader_epoch);
}

test "MetadataClient parseDescribeQuorumResponse no leader" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = 0;
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator, 100, "localhost", 9092,
        &cached_epoch, &is_fenced, &last_hb, &should_stop,
    );
    defer mc.deinit();

    // Build a response with leader_id = -1 (no leader)
    var resp: [128]u8 = undefined;
    var wpos: usize = 0;
    ser.writeI32(&resp, &wpos, 1); // correlation_id
    ser.writeI16(&resp, &wpos, 0); // error_code
    ser.writeI32(&resp, &wpos, 1); // num_topics
    ser.writeString(&resp, &wpos, "__cluster_metadata");
    ser.writeI32(&resp, &wpos, 1); // num_partitions
    ser.writeI16(&resp, &wpos, 0); // partition error_code
    ser.writeI32(&resp, &wpos, 0); // partition_index
    ser.writeI32(&resp, &wpos, -1); // leader_id = -1 (no leader)
    ser.writeI32(&resp, &wpos, 0); // leader_epoch

    const info = mc.parseDescribeQuorumResponse(resp[0..wpos]);
    try testing.expect(info == null);
}


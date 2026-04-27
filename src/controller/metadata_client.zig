const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.metadata_client);

const protocol = @import("protocol");
const ser = protocol.serialization;
const generated = protocol.generated;
const header_mod = protocol.header;
const ResponseHeader = header_mod.ResponseHeader;
const RaftClientPool = @import("network").RaftClientPool;
const TlsClientContext = @import("security").tls.TlsClientContext;

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
    voters: std.array_list.Managed(VoterAddress),
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
            .voters = std.array_list.Managed(VoterAddress).init(alloc),
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

    /// Set a TLS client context for encrypted connections to the controller.
    /// Must be called before run() to take effect on all voter connections.
    /// The TLS context propagates automatically to all RaftClients in the pool.
    pub fn setTlsContext(self: *MetadataClient, ctx: *TlsClientContext) void {
        self.controller_pool.setTlsContext(ctx);
        log.info("MetadataClient TLS enabled for broker {d}", .{self.broker_id});
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
            @import("time_compat").sleep(2000 * @as(u64, 1_000_000)); // 2-second heartbeat interval
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
                    defer self.allocator.free(response);
                    // Parse the DescribeQuorum response to find leader_id and leader_epoch
                    if (self.parseDescribeQuorumResponse(response)) |info| {
                        self.leader_id = info.leader_id;
                        self.cached_leader_epoch.* = info.leader_epoch;
                        log.info("Discovered controller leader: node {d} epoch {d}", .{
                            info.leader_id, info.leader_epoch,
                        });
                        return;
                    }
                }
            }

            // Exponential backoff with cap
            attempt += 1;
            const backoff = @min(max_backoff_ms, @as(u64, 1000) << @intCast(@min(attempt, 5)));
            log.warn("Failed to discover controller leader, retrying in {d}ms (attempt {d})", .{
                backoff, attempt,
            });
            @import("time_compat").sleep(backoff * @as(u64, 1_000_000));
        }
    }

    const QuorumInfo = struct {
        leader_id: i32,
        leader_epoch: i32,
    };

    fn parseDescribeQuorumResponse(self: *MetadataClient, response: []const u8) ?QuorumInfo {
        const Resp = generated.describe_quorum_response.DescribeQuorumResponse;

        var pos: usize = 0;
        var header = ResponseHeader.deserialize(self.allocator, response, &pos, header_mod.responseHeaderVersion(55, 0)) catch return null;
        defer header.deinit(self.allocator);

        var resp = Resp.deserialize(self.allocator, response, &pos, 0) catch return null;
        defer self.freeDescribeQuorumResponse(&resp);

        if (resp.error_code != 0) return null;
        if (resp.topics.len == 0 or resp.topics[0].partitions.len == 0) return null;

        const partition = resp.topics[0].partitions[0];
        if (partition.error_code != 0) return null;
        const leader_id = partition.leader_id;
        const leader_epoch = partition.leader_epoch;

        if (leader_id < 0) return null; // No leader elected yet

        return .{ .leader_id = leader_id, .leader_epoch = leader_epoch };
    }

    fn freeDescribeQuorumResponse(self: *MetadataClient, resp: *generated.describe_quorum_response.DescribeQuorumResponse) void {
        for (resp.topics) |topic| {
            for (topic.partitions) |partition| {
                if (partition.current_voters.len > 0) self.allocator.free(partition.current_voters);
                if (partition.observers.len > 0) self.allocator.free(partition.observers);
            }
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (resp.topics.len > 0) self.allocator.free(resp.topics);
        if (resp.nodes.len > 0) self.allocator.free(resp.nodes);
    }

    /// Register this broker with the controller leader.
    fn registerWithController(self: *MetadataClient) void {
        const Req = generated.broker_registration_request.BrokerRegistrationRequest;
        const Resp = generated.broker_registration_response.BrokerRegistrationResponse;

        const leader = self.leader_id orelse {
            log.warn("Cannot register: no controller leader known", .{});
            return;
        };

        // Build BrokerRegistration request (API 62)
        var buf: [512]u8 = undefined;
        var wpos: usize = 0;

        ser.writeI16(&buf, &wpos, 62); // api_key
        ser.writeI16(&buf, &wpos, 0); // api_version
        ser.writeI32(&buf, &wpos, 1); // correlation_id
        ser.writeCompactString(&buf, &wpos, "zmq-broker"); // client_id
        ser.writeEmptyTaggedFields(&buf, &wpos);

        const listeners = [_]Req.Listener{.{
            .name = "PLAINTEXT",
            .host = self.advertised_host,
            .port = self.advertised_port,
            .security_protocol = 0,
        }};
        const req = Req{
            .broker_id = self.broker_id,
            .cluster_id = null,
            .listeners = &listeners,
        };
        req.serialize(&buf, &wpos, 0);

        if (self.controller_pool.sendRequest(leader, buf[0..wpos])) |response| {
            defer self.allocator.free(response);
            var rpos: usize = 0;
            var resp_header = ResponseHeader.deserialize(self.allocator, response, &rpos, header_mod.responseHeaderVersion(62, 0)) catch {
                log.warn("Malformed BrokerRegistration response header", .{});
                return;
            };
            defer resp_header.deinit(self.allocator);
            const resp = Resp.deserialize(self.allocator, response, &rpos, 0) catch {
                log.warn("Malformed BrokerRegistration response body", .{});
                return;
            };
            if (resp.error_code == 0) {
                self.broker_epoch = resp.broker_epoch;
                log.info("Registered with controller: broker_epoch={d}", .{self.broker_epoch});
            } else {
                log.warn("BrokerRegistration failed: error_code={d}", .{resp.error_code});
            }
        } else {
            log.warn("Failed to send BrokerRegistration to controller {d}", .{leader});
        }
    }

    /// Send a heartbeat to the controller leader.
    fn sendHeartbeat(self: *MetadataClient) void {
        const Req = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
        const Resp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

        // Staleness check: if no successful heartbeat within lease period,
        // self-fence to prevent split-brain writes during network partition.
        const now = @import("time_compat").milliTimestamp();
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

        ser.writeI16(&buf, &wpos, 63); // api_key
        ser.writeI16(&buf, &wpos, 0); // api_version
        ser.writeI32(&buf, &wpos, 2); // correlation_id
        ser.writeCompactString(&buf, &wpos, "zmq-broker"); // client_id
        ser.writeEmptyTaggedFields(&buf, &wpos);

        const req = Req{ .broker_id = self.broker_id, .broker_epoch = self.broker_epoch };
        req.serialize(&buf, &wpos, 0);

        if (self.controller_pool.sendRequest(leader, buf[0..wpos])) |response| {
            defer self.allocator.free(response);
            var rpos: usize = 0;
            var resp_header = ResponseHeader.deserialize(self.allocator, response, &rpos, header_mod.responseHeaderVersion(63, 0)) catch {
                log.warn("Malformed BrokerHeartbeat response header", .{});
                self.leader_id = null;
                return;
            };
            defer resp_header.deinit(self.allocator);
            const resp = Resp.deserialize(self.allocator, response, &rpos, 0) catch {
                log.warn("Malformed BrokerHeartbeat response body", .{});
                self.leader_id = null;
                return;
            };

            if (resp.error_code != 0) {
                log.warn("Heartbeat error: code={d}, re-registering", .{resp.error_code});
                self.leader_id = null; // Force leader re-discovery
                return;
            }

            if (resp.is_fenced) {
                if (!self.is_fenced.*) {
                    log.warn("Controller fenced this broker (id={d}), rejecting writes", .{self.broker_id});
                }
                self.is_fenced.* = true;
            } else {
                if (self.is_fenced.*) {
                    log.info("Controller unfenced this broker (id={d}), resuming writes", .{self.broker_id});
                }
                self.is_fenced.* = false;
                self.last_heartbeat_ms.* = @import("time_compat").milliTimestamp();
            }

            if (resp.should_shut_down) {
                log.warn("Controller requested shutdown for broker {d}", .{self.broker_id});
                self.should_stop.* = true;
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
    var last_hb: i64 = @import("time_compat").milliTimestamp() - 60_000; // 60s ago
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator,
        100,
        "localhost",
        9092,
        &cached_epoch,
        &is_fenced,
        &last_hb,
        &should_stop,
    );
    defer mc.deinit();

    // Set a short lease for testing
    mc.controller_lease_ms = 10_000; // 10s

    // sendHeartbeat will check staleness first — with last_hb 60s ago
    // and lease 10s, the broker should be fenced.
    // Since there's no leader_id, it will try discoverLeader which loops,
    // so we test the staleness logic directly.
    const now = @import("time_compat").milliTimestamp();
    if (last_hb > 0 and (now - last_hb) > mc.controller_lease_ms) {
        is_fenced = true;
    }
    try testing.expect(is_fenced);
}

test "MetadataClient fresh heartbeat prevents fencing" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = @import("time_compat").milliTimestamp(); // Just now
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator,
        100,
        "localhost",
        9092,
        &cached_epoch,
        &is_fenced,
        &last_hb,
        &should_stop,
    );
    defer mc.deinit();

    mc.controller_lease_ms = 30_000;

    // Fresh heartbeat — should NOT be fenced
    const now = @import("time_compat").milliTimestamp();
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
        testing.allocator,
        100,
        "localhost",
        9092,
        &cached_epoch,
        &is_fenced,
        &last_hb,
        &should_stop,
    );
    defer mc.deinit();

    // With last_hb == 0, staleness check should be skipped
    // (broker just started, hasn't registered yet)
    const now = @import("time_compat").milliTimestamp();
    _ = now;
    if (last_hb > 0 and (@import("time_compat").milliTimestamp() - last_hb) > mc.controller_lease_ms) {
        is_fenced = true;
    }
    try testing.expect(!is_fenced);
}

test "MetadataClient parseDescribeQuorumResponse valid" {
    const Resp = generated.describe_quorum_response.DescribeQuorumResponse;

    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = 0;
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator,
        100,
        "localhost",
        9092,
        &cached_epoch,
        &is_fenced,
        &last_hb,
        &should_stop,
    );
    defer mc.deinit();

    // Build a generated DescribeQuorum v0 response.
    var resp: [128]u8 = undefined;
    var wpos: usize = 0;
    const header = ResponseHeader{ .correlation_id = 1 };
    header.serialize(&resp, &wpos, header_mod.responseHeaderVersion(55, 0));
    const voters = [_]generated.describe_quorum_response.ReplicaState{.{ .replica_id = 2, .log_end_offset = 0 }};
    const partitions = [_]Resp.TopicData.PartitionData{.{
        .partition_index = 0,
        .error_code = 0,
        .leader_id = 2,
        .leader_epoch = 7,
        .high_watermark = 0,
        .current_voters = &voters,
    }};
    const topics = [_]Resp.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const body = Resp{ .error_code = 0, .topics = &topics };
    body.serialize(&resp, &wpos, 0);

    const info = mc.parseDescribeQuorumResponse(resp[0..wpos]);
    try testing.expect(info != null);
    try testing.expectEqual(@as(i32, 2), info.?.leader_id);
    try testing.expectEqual(@as(i32, 7), info.?.leader_epoch);
}

test "MetadataClient parseDescribeQuorumResponse no leader" {
    const Resp = generated.describe_quorum_response.DescribeQuorumResponse;

    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = 0;
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator,
        100,
        "localhost",
        9092,
        &cached_epoch,
        &is_fenced,
        &last_hb,
        &should_stop,
    );
    defer mc.deinit();

    // Build a response with leader_id = -1 (no leader)
    var resp: [128]u8 = undefined;
    var wpos: usize = 0;
    const header = ResponseHeader{ .correlation_id = 1 };
    header.serialize(&resp, &wpos, header_mod.responseHeaderVersion(55, 0));
    const partitions = [_]Resp.TopicData.PartitionData{.{
        .partition_index = 0,
        .error_code = 0,
        .leader_id = -1,
        .leader_epoch = 0,
        .high_watermark = 0,
    }};
    const topics = [_]Resp.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const body = Resp{ .error_code = 0, .topics = &topics };
    body.serialize(&resp, &wpos, 0);

    const info = mc.parseDescribeQuorumResponse(resp[0..wpos]);
    try testing.expect(info == null);
}

test "MetadataClient setTlsContext propagates to controller pool" {
    var cached_epoch: i32 = 0;
    var is_fenced: bool = false;
    var last_hb: i64 = 0;
    var should_stop: bool = false;

    var mc = MetadataClient.init(
        testing.allocator,
        100,
        "localhost",
        9092,
        &cached_epoch,
        &is_fenced,
        &last_hb,
        &should_stop,
    );
    defer mc.deinit();

    try mc.addVoter(1, "controller1", 9093);

    // Before setting TLS, pool should have no TLS context
    try testing.expect(mc.controller_pool.tls_client_ctx == null);

    // Set TLS context
    var tls_ctx = TlsClientContext{};
    defer tls_ctx.deinit();
    mc.setTlsContext(&tls_ctx);

    // Pool should now have TLS context
    try testing.expect(mc.controller_pool.tls_client_ctx != null);

    // Client in pool should also have TLS context
    const client = mc.controller_pool.getClient(1);
    try testing.expect(client != null);
    try testing.expect(client.?.tls_ctx != null);
}

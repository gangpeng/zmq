const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.controller);

const ser = @import("../protocol/serialization.zig");
const header_mod = @import("../protocol/header.zig");
const RequestHeader = header_mod.RequestHeader;
const ResponseHeader = header_mod.ResponseHeader;
const RaftState = @import("../raft/state.zig").RaftState;
const BrokerRegistry = @import("broker_registry.zig").BrokerRegistry;

/// KRaft metadata controller.
///
/// Owns the Raft consensus state machine and manages cluster metadata.
/// In controller-only mode, this is the only active component.
/// In combined mode, the Broker holds a pointer to this Controller's RaftState.
///
/// Handles:
/// - KRaft consensus RPCs (Vote, BeginQuorumEpoch, EndQuorumEpoch, DescribeQuorum)
/// - Broker lifecycle (BrokerRegistration, BrokerHeartbeat)
/// - Controller-scoped ApiVersions
pub const Controller = struct {
    raft_state: RaftState,
    broker_registry: BrokerRegistry,
    allocator: Allocator,
    node_id: i32,
    cluster_id: []const u8,

    /// Next producer ID to allocate. Controller owns the global PID counter.
    /// NOTE: AutoMQ/Kafka uses ProducerIdManager on the controller to allocate
    /// PID blocks to brokers. ZMQ simplifies by having the controller directly
    /// manage a monotonic counter.
    next_producer_id: i64 = 1000,

    pub fn init(alloc: Allocator, node_id: i32, cluster_id: []const u8) Controller {
        return .{
            .raft_state = RaftState.init(alloc, node_id, cluster_id),
            .allocator = alloc,
            .node_id = node_id,
            .cluster_id = cluster_id,
            .broker_registry = BrokerRegistry.init(alloc),
        };
    }

    pub fn deinit(self: *Controller) void {
        self.raft_state.deinit();
        self.broker_registry.deinit();
    }

    /// Periodic maintenance: evict dead brokers.
    /// Called from the ElectionLoop's broker_tick_fn callback.
    pub fn tick(self: *Controller) void {
        const evicted = self.broker_registry.evictExpired(30_000);
        if (evicted > 0) {
            log.info("Controller evicted {d} dead broker(s)", .{evicted});
        }
    }

    /// Handle a Kafka protocol request on the controller port.
    /// Only accepts KRaft APIs + broker registration + ApiVersions.
    pub fn handleRequest(self: *Controller, request_bytes: []const u8) ?[]u8 {
        if (request_bytes.len < 8) {
            log.warn("Controller request too short: {d} bytes", .{request_bytes.len});
            return null;
        }

        // Peek at api_key and api_version to determine header version, then reset pos
        // so RequestHeader.deserialize reads the full header from the start.
        var peek_pos: usize = 0;
        const api_key = ser.readI16(request_bytes, &peek_pos);
        const api_version = ser.readI16(request_bytes, &peek_pos);

        const req_header_version = header_mod.requestHeaderVersion(api_key, api_version);
        const resp_header_version = header_mod.responseHeaderVersion(api_key, api_version);

        var pos: usize = 0; // start from beginning — deserialize reads api_key+api_version too
        var req_header = RequestHeader.deserialize(self.allocator, request_bytes, &pos, req_header_version) catch {
            log.warn("Controller: failed to parse request header (api_key={d} hdr_v={d} len={d})", .{
                api_key, req_header_version, request_bytes.len,
            });
            return null;
        };
        defer req_header.deinit(self.allocator);

        log.debug("Controller api_key={d} corr={d}", .{ api_key, req_header.correlation_id });

        return switch (api_key) {
            18 => self.handleApiVersions(&req_header, resp_header_version),
            52 => self.handleVote(request_bytes, pos, &req_header, resp_header_version),
            53 => self.handleBeginQuorumEpoch(request_bytes, pos, &req_header, resp_header_version),
            54 => self.handleEndQuorumEpoch(request_bytes, pos, &req_header, resp_header_version),
            55 => self.handleDescribeQuorum(&req_header, resp_header_version),
            62 => self.handleBrokerRegistration(request_bytes, pos, &req_header, resp_header_version),
            63 => self.handleBrokerHeartbeat(request_bytes, pos, &req_header, resp_header_version),
            71 => self.handleAddRaftVoter(request_bytes, pos, &req_header, resp_header_version),
            72 => self.handleRemoveRaftVoter(request_bytes, pos, &req_header, resp_header_version),
            67 => self.handleAllocateProducerIds(request_bytes, pos, &req_header, resp_header_version),
            else => self.handleUnsupported(&req_header, api_key, resp_header_version),
        };
    }

    // ---------------------------------------------------------------
    // ApiVersions (key 18) — controller-scoped
    // ---------------------------------------------------------------
    fn handleApiVersions(self: *Controller, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        const supported = [_]struct { key: i16, min: i16, max: i16 }{
            .{ .key = 18, .min = 0, .max = 4 }, // ApiVersions
            .{ .key = 52, .min = 0, .max = 1 }, // Vote
            .{ .key = 53, .min = 0, .max = 1 }, // BeginQuorumEpoch
            .{ .key = 54, .min = 0, .max = 1 }, // EndQuorumEpoch
            .{ .key = 55, .min = 0, .max = 1 }, // DescribeQuorum
            .{ .key = 62, .min = 0, .max = 0 }, // BrokerRegistration
            .{ .key = 63, .min = 0, .max = 0 }, // BrokerHeartbeat
            .{ .key = 71, .min = 0, .max = 0 }, // AddRaftVoter
            .{ .key = 72, .min = 0, .max = 0 }, // RemoveRaftVoter
            .{ .key = 67, .min = 0, .max = 0 }, // AllocateProducerIds
        };

        var buf = self.allocator.alloc(u8, 256) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);

        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeArrayLen(buf, &wpos, supported.len);
        for (supported) |s| {
            ser.writeI16(buf, &wpos, s.key);
            ser.writeI16(buf, &wpos, s.min);
            ser.writeI16(buf, &wpos, s.max);
        }
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // Vote (key 52) — KRaft consensus
    // ---------------------------------------------------------------
    fn handleVote(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        const cluster_id = ser.readCompactString(request_bytes, &pos) catch null;
        _ = cluster_id;

        const candidate_id = ser.readI32(request_bytes, &pos);
        const candidate_epoch = ser.readI32(request_bytes, &pos);
        _ = ser.readI32(request_bytes, &pos); // duplicate candidate_id in wire format
        const last_epoch_end_offset = ser.readI64(request_bytes, &pos);
        const last_epoch = ser.readI32(request_bytes, &pos);

        const vote_result = self.raft_state.handleVoteRequest(
            candidate_id,
            candidate_epoch,
            @intCast(last_epoch_end_offset),
            last_epoch,
        );

        log.info("Vote request from candidate {d} epoch={d}: granted={}", .{
            candidate_id, candidate_epoch, vote_result.vote_granted,
        });

        var buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, if (vote_result.vote_granted) @as(i16, 0) else @as(i16, 87));
        ser.writeI32(buf, &wpos, vote_result.epoch);
        ser.writeBool(buf, &wpos, vote_result.vote_granted);
        ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // BeginQuorumEpoch (key 53) — KRaft leader heartbeat
    // ---------------------------------------------------------------
    fn handleBeginQuorumEpoch(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        const error_code = ser.readI16(request_bytes, &pos);
        _ = error_code;
        const topics_len = ser.readArrayLen(request_bytes, &pos) catch 0;
        _ = topics_len;
        const leader_id = ser.readI32(request_bytes, &pos);
        const leader_epoch = ser.readI32(request_bytes, &pos);

        if (leader_epoch >= self.raft_state.current_epoch) {
            self.raft_state.becomeFollower(leader_epoch, leader_id);
            log.info("Acknowledged leader {d} epoch={d}", .{ leader_id, leader_epoch });
        }

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeI32(buf, &wpos, 0); // topics array len = 0

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // EndQuorumEpoch (key 54) — KRaft leader step-down
    // ---------------------------------------------------------------
    fn handleEndQuorumEpoch(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        _ = request_bytes;
        _ = start_pos;

        if (self.raft_state.role == .follower) {
            log.info("Leader stepped down, will start election", .{});
            self.raft_state.election_timer.reset();
        }

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeI32(buf, &wpos, 0); // topics array len = 0

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // DescribeQuorum (key 55) — KRaft quorum info
    // ---------------------------------------------------------------
    fn handleDescribeQuorum(self: *Controller, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var buf = self.allocator.alloc(u8, 512) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);

        ser.writeI16(buf, &wpos, 0); // error_code
        // Topics array — one topic (__cluster_metadata)
        ser.writeI32(buf, &wpos, 1);
        ser.writeString(buf, &wpos, "__cluster_metadata");
        // Partitions array — one partition
        ser.writeI32(buf, &wpos, 1);
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeI32(buf, &wpos, 0); // partition_index
        ser.writeI32(buf, &wpos, self.raft_state.leader_id orelse -1);
        ser.writeI32(buf, &wpos, self.raft_state.current_epoch);
        ser.writeI64(buf, &wpos, @intCast(self.raft_state.log.lastOffset()));

        // Voters array
        const voter_count = self.raft_state.quorumSize();
        ser.writeI32(buf, &wpos, @intCast(voter_count));

        var vit = self.raft_state.voters.iterator();
        while (vit.next()) |entry| {
            ser.writeI32(buf, &wpos, entry.key_ptr.*); // replica_id
            ser.writeI64(buf, &wpos, @intCast(entry.value_ptr.match_index)); // log_end_offset
        }

        // Observers array (empty — broker-only nodes are not tracked here yet)
        ser.writeI32(buf, &wpos, 0);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // BrokerRegistration (key 62) — broker lifecycle
    // ---------------------------------------------------------------
    fn handleBrokerRegistration(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        // Simplified parsing: broker_id, host (as string), port
        const broker_id = ser.readI32(request_bytes, &pos);
        const host = (ser.readString(request_bytes, &pos) catch null) orelse "unknown";
        const broker_port = ser.readI32(request_bytes, &pos);

        const broker_epoch = self.broker_registry.register(
            broker_id,
            host,
            @intCast(broker_port),
        ) catch {
            log.warn("BrokerRegistration failed for broker {d}", .{broker_id});
            return self.errorResponse(req_header, resp_header_version, 1); // UNKNOWN_SERVER_ERROR
        };

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeI64(buf, &wpos, broker_epoch);
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // BrokerHeartbeat (key 63) — broker liveness
    // ---------------------------------------------------------------
    fn handleBrokerHeartbeat(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        const broker_id = ser.readI32(request_bytes, &pos);
        const broker_epoch = ser.readI64(request_bytes, &pos);

        const is_active = self.broker_registry.heartbeat(broker_id, broker_epoch) catch false;

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code
        ser.writeBool(buf, &wpos, false); // is_caught_up
        ser.writeBool(buf, &wpos, !is_active); // is_fenced
        ser.writeBool(buf, &wpos, false); // should_shut_down
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // AddRaftVoter (key 71) — dynamic voter membership
    // ---------------------------------------------------------------
    fn handleAddRaftVoter(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        // Parse: voter_id
        const voter_id = ser.readI32(request_bytes, &pos);

        const offset = self.raft_state.proposeAddVoter(voter_id) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotLeader => 41, // NOT_CONTROLLER
                error.ConfigChangePending => 89, // CONCURRENT_TRANSACTIONS (reuse for pending config)
                error.VoterAlreadyExists => 73, // DUPLICATE_RESOURCE
                else => 1, // UNKNOWN_SERVER_ERROR
            };
            return self.errorResponse(req_header, resp_header_version, error_code);
        };

        log.info("AddRaftVoter: proposed adding node {d} at offset {d}", .{ voter_id, offset });

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // RemoveRaftVoter (key 72) — dynamic voter membership
    // ---------------------------------------------------------------
    fn handleRemoveRaftVoter(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        // Parse: voter_id
        const voter_id = ser.readI32(request_bytes, &pos);

        const offset = self.raft_state.proposeRemoveVoter(voter_id) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotLeader => 41, // NOT_CONTROLLER
                error.ConfigChangePending => 89, // CONCURRENT_TRANSACTIONS
                error.VoterNotFound => 69, // RESOURCE_NOT_FOUND
                error.CannotRemoveLastVoter => 87, // INVALID_REQUEST
                else => 1,
            };
            return self.errorResponse(req_header, resp_header_version, error_code);
        };

        log.info("RemoveRaftVoter: proposed removing node {d} at offset {d}", .{ voter_id, offset });

        var buf = self.allocator.alloc(u8, 64) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // AllocateProducerIds (key 67) — PID block allocation
    // ---------------------------------------------------------------
    fn handleAllocateProducerIds(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, resp_header_version: i16) ?[]u8 {
        var pos = start_pos;

        const broker_id = ser.readI32(request_bytes, &pos);
        const broker_epoch = ser.readI64(request_bytes, &pos);
        _ = broker_id;
        _ = broker_epoch;

        // Allocate a block of 1000 producer IDs
        const block_start = self.next_producer_id;
        const block_len: i32 = 1000;
        self.next_producer_id += block_len;

        var buf = self.allocator.alloc(u8, 128) catch return null;
        var wpos: usize = 0;
        const rh = ResponseHeader{ .correlation_id = req_header.correlation_id };
        rh.serialize(buf, &wpos, resp_header_version);
        ser.writeI32(buf, &wpos, 0); // throttle_time_ms
        ser.writeI16(buf, &wpos, 0); // error_code: NONE
        ser.writeI64(buf, &wpos, block_start); // producer_id_start
        ser.writeI32(buf, &wpos, block_len); // producer_id_len
        if (resp_header_version >= 1) ser.writeEmptyTaggedFields(buf, &wpos);

        return (self.allocator.realloc(buf, wpos) catch buf)[0..wpos];
    }

    // ---------------------------------------------------------------
    // Unsupported API
    // ---------------------------------------------------------------
    fn handleUnsupported(self: *Controller, req_header: *const RequestHeader, api_key: i16, resp_header_version: i16) ?[]u8 {
        log.warn("Unsupported API on controller port: {d}", .{api_key});
        return self.errorResponse(req_header, resp_header_version, 35); // UNSUPPORTED_VERSION
    }

    /// Build a simple error response.
    fn errorResponse(self: *Controller, req_header: *const RequestHeader, resp_header_version: i16, error_code: i16) ?[]u8 {
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const buf = self.allocator.alloc(u8, header_size + 2) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        ser.writeI16(buf, &wpos, error_code);
        return buf[0..wpos];
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

/// Build a test request with the given API key, version, and correlation ID.
/// Uses the correct header version based on the API key and version.
fn buildTestRequest(buf: []u8, api_key: i16, api_version: i16, correlation_id: i32, header_version: i16) usize {
    var pos: usize = 0;
    ser.writeI16(buf, &pos, api_key);
    ser.writeI16(buf, &pos, api_version);
    ser.writeI32(buf, &pos, correlation_id);
    if (header_version >= 2) {
        ser.writeCompactString(buf, &pos, "test-client");
        ser.writeEmptyTaggedFields(buf, &pos);
    } else if (header_version >= 1) {
        ser.writeString(buf, &pos, "test-client");
    }
    return pos;
}

test "Controller init and deinit" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    try testing.expectEqual(@as(i32, 1), ctrl.node_id);
    try testing.expectEqual(RaftState.Role.unattached, ctrl.raft_state.role);
    try testing.expectEqual(@as(usize, 0), ctrl.broker_registry.count());
}

test "Controller handleRequest rejects too-short request" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Empty request
    try testing.expect(ctrl.handleRequest("") == null);
    // Only 4 bytes — less than minimum header (8 bytes needed)
    try testing.expect(ctrl.handleRequest(&[_]u8{ 0, 0, 0, 0 }) == null);
    // 7 bytes — still less than minimum
    try testing.expect(ctrl.handleRequest(&[_]u8{ 0, 0, 0, 0, 0, 0, 0 }) == null);
}

test "Controller handleRequest ApiVersions returns supported APIs" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // ApiVersions v0 uses request header v1, response header v0
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 18, 0, 42, 1);

    const response = ctrl.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v0: just correlation_id (4 bytes)
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 42), corr_id);

    // error_code
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);

    // Array of supported APIs — controller supports 10 APIs
    const array_len = try ser.readArrayLen(response.?, &rpos);
    try testing.expectEqual(@as(usize, 10), array_len.?);
}

test "Controller handleRequest unsupported API returns error" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Fetch (api_key=1) is not supported on controller port
    // Fetch v0 is not flexible → request header v1, response header v0
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 1, 0, 77, 1);

    const response = ctrl.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v0: correlation_id
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 77), corr_id);

    // error_code: 35 (UNSUPPORTED_VERSION)
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 35), error_code);
}

test "Controller handleRequest Vote grants to valid candidate" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Add voter 2 so the controller knows about candidate 2
    try ctrl.raft_state.addVoter(2);

    // Vote (52, v0) is flexible → request header v2, response header v1
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 52, 0, 10, 2);

    // Vote request body:
    ser.writeCompactString(&buf, &pos, null); // cluster_id (null)
    ser.writeI32(&buf, &pos, 2); // candidate_id
    ser.writeI32(&buf, &pos, 1); // candidate_epoch
    ser.writeI32(&buf, &pos, 2); // candidate_id (duplicate in wire format)
    ser.writeI64(&buf, &pos, 0); // last_epoch_end_offset
    ser.writeI32(&buf, &pos, 0); // last_epoch

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v1: correlation_id (4 bytes) + tagged_fields (1 byte)
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 10), corr_id);
    _ = try ser.readUnsignedVarint(response.?, &rpos); // skip tagged fields

    // Vote response body: error_code (i16) + epoch (i32) + vote_granted (bool)
    _ = ser.readI16(response.?, &rpos); // error_code
    _ = ser.readI32(response.?, &rpos); // epoch
    const vote_granted = try ser.readBool(response.?, &rpos);
    try testing.expect(vote_granted);
}

test "Controller handleRequest Vote rejects stale epoch" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Put controller at epoch 5 as follower of node 2
    try ctrl.raft_state.addVoter(1);
    ctrl.raft_state.becomeFollower(5, 2);

    // Add voter 3 so it can be a candidate
    try ctrl.raft_state.addVoter(3);

    // Vote from candidate 3 at stale epoch 3
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 52, 0, 11, 2);

    ser.writeCompactString(&buf, &pos, null); // cluster_id
    ser.writeI32(&buf, &pos, 3); // candidate_id
    ser.writeI32(&buf, &pos, 3); // candidate_epoch (stale — less than current 5)
    ser.writeI32(&buf, &pos, 3); // candidate_id (duplicate)
    ser.writeI64(&buf, &pos, 0); // last_epoch_end_offset
    ser.writeI32(&buf, &pos, 0); // last_epoch

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Parse response: skip header v1
    var rpos: usize = 0;
    _ = ser.readI32(response.?, &rpos); // correlation_id
    _ = try ser.readUnsignedVarint(response.?, &rpos); // tagged fields

    _ = ser.readI16(response.?, &rpos); // error_code
    _ = ser.readI32(response.?, &rpos); // epoch
    const vote_granted = try ser.readBool(response.?, &rpos);
    try testing.expect(!vote_granted);
}

test "Controller handleRequest BeginQuorumEpoch accepts higher epoch" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Controller starts at epoch 0 (unattached)
    try testing.expectEqual(@as(i32, 0), ctrl.raft_state.current_epoch);

    // BeginQuorumEpoch (53, v0) is flexible → header v2 / response v1
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 53, 0, 20, 2);

    // BeginQuorumEpoch body:
    ser.writeI16(&buf, &pos, 0); // error_code
    ser.writeI32(&buf, &pos, 0); // topics_len (0)
    ser.writeI32(&buf, &pos, 2); // leader_id
    ser.writeI32(&buf, &pos, 5); // leader_epoch

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Verify raft state was updated
    try testing.expectEqual(RaftState.Role.follower, ctrl.raft_state.role);
    try testing.expectEqual(@as(i32, 5), ctrl.raft_state.current_epoch);
    try testing.expectEqual(@as(i32, 2), ctrl.raft_state.leader_id.?);
}

test "Controller handleRequest BeginQuorumEpoch ignores stale epoch" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Put controller at epoch 5
    ctrl.raft_state.becomeFollower(5, 2);
    try testing.expectEqual(@as(i32, 5), ctrl.raft_state.current_epoch);

    // Send BeginQuorumEpoch with stale epoch 3
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 53, 0, 21, 2);

    ser.writeI16(&buf, &pos, 0); // error_code
    ser.writeI32(&buf, &pos, 0); // topics_len
    ser.writeI32(&buf, &pos, 3); // leader_id
    ser.writeI32(&buf, &pos, 3); // leader_epoch (stale)

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Raft state should NOT have changed — epoch still 5
    try testing.expectEqual(@as(i32, 5), ctrl.raft_state.current_epoch);
}

test "Controller handleRequest DescribeQuorum returns quorum info" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Add self as voter and become leader
    try ctrl.raft_state.addVoter(1);
    _ = ctrl.raft_state.startElection();
    ctrl.raft_state.becomeLeader();

    // DescribeQuorum (55, v0) is flexible → header v2 / response v1
    var buf: [256]u8 = undefined;
    const req_len = buildTestRequest(&buf, 55, 0, 30, 2);

    const response = ctrl.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v1: correlation_id + tagged fields
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 30), corr_id);
    _ = try ser.readUnsignedVarint(response.?, &rpos); // tagged fields

    // error_code should be 0
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);
}

test "Controller handleRequest BrokerRegistration registers broker" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // BrokerRegistration (62, v0) is NOT flexible → header v1 / response v0
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 62, 0, 40, 1);

    // BrokerRegistration body:
    ser.writeI32(&buf, &pos, 100); // broker_id
    ser.writeString(&buf, &pos, "host1"); // host
    ser.writeI32(&buf, &pos, 9092); // port

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v0: correlation_id only (4 bytes)
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 40), corr_id);

    // throttle_time_ms
    const throttle = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 0), throttle);

    // error_code
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);

    // broker_epoch (should be > 0)
    const broker_epoch = ser.readI64(response.?, &rpos);
    try testing.expect(broker_epoch > 0);

    // Verify broker is registered
    try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());
}

test "Controller handleRequest BrokerHeartbeat reports active broker" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // First, register a broker to get the epoch
    var reg_buf: [256]u8 = undefined;
    var reg_pos = buildTestRequest(&reg_buf, 62, 0, 40, 1);
    ser.writeI32(&reg_buf, &reg_pos, 100); // broker_id
    ser.writeString(&reg_buf, &reg_pos, "host1"); // host
    ser.writeI32(&reg_buf, &reg_pos, 9092); // port

    const reg_response = ctrl.handleRequest(reg_buf[0..reg_pos]);
    try testing.expect(reg_response != null);
    defer testing.allocator.free(reg_response.?);

    // Parse broker_epoch from registration response
    var reg_rpos: usize = 0;
    _ = ser.readI32(reg_response.?, &reg_rpos); // correlation_id
    _ = ser.readI32(reg_response.?, &reg_rpos); // throttle_time_ms
    _ = ser.readI16(reg_response.?, &reg_rpos); // error_code
    const broker_epoch = ser.readI64(reg_response.?, &reg_rpos);

    // Now send a heartbeat with the correct epoch
    // BrokerHeartbeat (63, v0) is NOT flexible → header v1 / response v0
    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 0, 50, 1);
    ser.writeI32(&hb_buf, &hb_pos, 100); // broker_id
    ser.writeI64(&hb_buf, &hb_pos, broker_epoch); // broker_epoch

    const hb_response = ctrl.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    // Response header v0: correlation_id
    var hb_rpos: usize = 0;
    const corr_id = ser.readI32(hb_response.?, &hb_rpos);
    try testing.expectEqual(@as(i32, 50), corr_id);

    // throttle_time_ms
    _ = ser.readI32(hb_response.?, &hb_rpos);

    // error_code
    const error_code = ser.readI16(hb_response.?, &hb_rpos);
    try testing.expectEqual(@as(i16, 0), error_code);

    // is_caught_up (bool, 1 byte)
    _ = try ser.readBool(hb_response.?, &hb_rpos);

    // is_fenced — should be false (0) since we just heartbeated
    const is_fenced = try ser.readBool(hb_response.?, &hb_rpos);
    try testing.expect(!is_fenced);
}

test "Controller tick evicts dead brokers" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Register a broker
    _ = try ctrl.broker_registry.register(100, "host1", 9092);
    try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());

    // Force the broker's last_heartbeat_ms to be 60 seconds in the past
    if (ctrl.broker_registry.brokers.getPtr(100)) |info| {
        info.last_heartbeat_ms = std.time.milliTimestamp() - 60_000;
    }

    // tick() calls evictExpired(30_000) — broker should be evicted
    ctrl.tick();
    try testing.expectEqual(@as(usize, 0), ctrl.broker_registry.count());
}

test "Controller handleRequest AllocateProducerIds returns block" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // AllocateProducerIds (67, v0) is NOT flexible → header v1 / response v0
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 67, 0, 60, 1);
    ser.writeI32(&buf, &pos, 100); // broker_id
    ser.writeI64(&buf, &pos, 1); // broker_epoch

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Parse: correlation_id, throttle_time_ms, error_code, producer_id_start, producer_id_len
    var rpos: usize = 0;
    _ = ser.readI32(response.?, &rpos); // correlation_id
    _ = ser.readI32(response.?, &rpos); // throttle_time_ms
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(@as(i16, 0), error_code);
    const pid_start = ser.readI64(response.?, &rpos);
    try testing.expect(pid_start >= 1000);
    const pid_len = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 1000), pid_len);
}

test "Controller AllocateProducerIds increments monotonically" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // First allocation
    var buf1: [256]u8 = undefined;
    var pos1 = buildTestRequest(&buf1, 67, 0, 61, 1);
    ser.writeI32(&buf1, &pos1, 100);
    ser.writeI64(&buf1, &pos1, 1);
    const resp1 = ctrl.handleRequest(buf1[0..pos1]);
    try testing.expect(resp1 != null);
    defer testing.allocator.free(resp1.?);

    var rpos1: usize = 0;
    _ = ser.readI32(resp1.?, &rpos1);
    _ = ser.readI32(resp1.?, &rpos1);
    _ = ser.readI16(resp1.?, &rpos1);
    const start1 = ser.readI64(resp1.?, &rpos1);

    // Second allocation
    var buf2: [256]u8 = undefined;
    var pos2 = buildTestRequest(&buf2, 67, 0, 62, 1);
    ser.writeI32(&buf2, &pos2, 100);
    ser.writeI64(&buf2, &pos2, 1);
    const resp2 = ctrl.handleRequest(buf2[0..pos2]);
    try testing.expect(resp2 != null);
    defer testing.allocator.free(resp2.?);

    var rpos2: usize = 0;
    _ = ser.readI32(resp2.?, &rpos2);
    _ = ser.readI32(resp2.?, &rpos2);
    _ = ser.readI16(resp2.?, &rpos2);
    const start2 = ser.readI64(resp2.?, &rpos2);

    // Second block starts after first
    try testing.expect(start2 >= start1 + 1000);
}

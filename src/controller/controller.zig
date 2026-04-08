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

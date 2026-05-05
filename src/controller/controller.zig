const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.controller);
const fs = @import("fs_compat");

const protocol = @import("protocol");
const ser = protocol.serialization;
const generated = protocol.generated;
const api_support = protocol.api_support;
const header_mod = protocol.header;
const RequestHeader = header_mod.RequestHeader;
const ResponseHeader = header_mod.ResponseHeader;
const ErrorCode = protocol.ErrorCode;
const RaftState = @import("raft").RaftState;
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

    /// Last controller metadata log offset applied to broker_registry.
    last_applied_controller_metadata_offset: ?u64 = null,
    /// Optional hook for co-located broker state machines that share this Raft log.
    raft_commit_hook: ?*const fn () void = null,

    pub fn init(alloc: Allocator, node_id: i32, cluster_id: []const u8) Controller {
        return .{
            .raft_state = RaftState.init(alloc, node_id, cluster_id),
            .allocator = alloc,
            .node_id = node_id,
            .cluster_id = cluster_id,
            .broker_registry = BrokerRegistry.init(alloc),
        };
    }

    pub fn initWithDataDir(alloc: Allocator, node_id: i32, cluster_id: []const u8, data_dir: []const u8) Controller {
        return .{
            .raft_state = RaftState.initWithDataDir(alloc, node_id, cluster_id, data_dir),
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

        if (api_key != 18 and api_support.findControllerSupport(api_key) != null and !api_support.isControllerVersionSupported(api_key, api_version)) {
            log.warn("Unsupported controller API version: api_key={d} v={d}", .{ api_key, api_version });
            return self.handleUnsupported(&req_header, api_key, resp_header_version);
        }

        return switch (api_key) {
            18 => self.handleApiVersions(&req_header, api_version, resp_header_version),
            52 => self.handleVote(request_bytes, pos, &req_header, api_version, resp_header_version),
            53 => self.handleBeginQuorumEpoch(request_bytes, pos, &req_header, api_version, resp_header_version),
            54 => self.handleEndQuorumEpoch(request_bytes, pos, &req_header, api_version, resp_header_version),
            55 => self.handleDescribeQuorum(request_bytes, pos, &req_header, api_version, resp_header_version),
            59 => self.handleFetchSnapshot(request_bytes, pos, &req_header, api_version, resp_header_version),
            62 => self.handleBrokerRegistration(request_bytes, pos, &req_header, api_version, resp_header_version),
            63 => self.handleBrokerHeartbeat(request_bytes, pos, &req_header, api_version, resp_header_version),
            64 => self.handleUnregisterBroker(request_bytes, pos, &req_header, api_version, resp_header_version),
            67 => self.handleAllocateProducerIds(request_bytes, pos, &req_header, api_version, resp_header_version),
            70 => self.handleControllerRegistration(request_bytes, pos, &req_header, api_version, resp_header_version),
            80 => self.handleAddRaftVoter(request_bytes, pos, &req_header, api_version, resp_header_version),
            81 => self.handleRemoveRaftVoter(request_bytes, pos, &req_header, api_version, resp_header_version),
            82 => self.handleUpdateRaftVoter(request_bytes, pos, &req_header, api_version, resp_header_version),
            else => self.handleUnsupported(&req_header, api_key, resp_header_version),
        };
    }

    // ---------------------------------------------------------------
    // ApiVersions (key 18) — controller-scoped
    // ---------------------------------------------------------------
    fn handleApiVersions(self: *Controller, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Resp = generated.api_versions_response.ApiVersionsResponse;
        const supported = api_support.controller_supported_apis;

        var api_keys_list: [supported.len]Resp.ApiVersion = undefined;
        for (supported, 0..) |api, i| {
            api_keys_list[i] = .{
                .api_key = api.key,
                .min_version = api.min,
                .max_version = api.max,
            };
        }

        const resp = Resp{
            .error_code = 0,
            .api_keys = api_keys_list[0..],
            .throttle_time_ms = 0,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // Vote (key 52) — KRaft consensus
    // ---------------------------------------------------------------
    fn handleVote(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.vote_request.VoteRequest;
        const Resp = generated.vote_response.VoteResponse;
        const TopicResult = Resp.TopicData;
        const PartitionResult = TopicResult.PartitionData;

        if (!validateVoteRequestFrame(request_bytes, start_pos, api_version)) {
            log.warn("Malformed Vote request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode Vote request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeVoteRequest(&req);

        const topics = self.allocator.alloc(TopicResult, req.topics.len) catch return null;
        for (topics) |*topic| {
            topic.* = .{ .topic_name = null, .partitions = &.{} };
        }
        defer {
            self.freeVoteResponseTopics(topics);
            if (topics.len > 0) self.allocator.free(topics);
        }

        for (req.topics, 0..) |topic, topic_index| {
            const partitions = self.allocator.alloc(PartitionResult, topic.partitions.len) catch return null;
            errdefer self.allocator.free(partitions);

            for (topic.partitions, 0..) |partition, partition_index| {
                const last_offset: u64 = if (partition.last_offset < 0) 0 else @intCast(partition.last_offset);
                const vote_result = self.raft_state.handleVoteRequest(
                    partition.candidate_id,
                    partition.candidate_epoch,
                    last_offset,
                    partition.last_offset_epoch,
                );
                partitions[partition_index] = .{
                    .partition_index = partition.partition_index,
                    .error_code = if (vote_result.vote_granted) ErrorCode.none.toInt() else ErrorCode.invalid_record.toInt(),
                    .leader_id = self.raft_state.leader_id orelse -1,
                    .leader_epoch = vote_result.epoch,
                    .vote_granted = vote_result.vote_granted,
                };

                log.info("Vote request from candidate {d} epoch={d}: granted={}", .{
                    partition.candidate_id,
                    partition.candidate_epoch,
                    vote_result.vote_granted,
                });
            }

            topics[topic_index] = .{
                .topic_name = topic.topic_name,
                .partitions = partitions,
            };
        }

        const resp = Resp{
            .error_code = ErrorCode.none.toInt(),
            .topics = topics,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn freeVoteRequest(self: *Controller, req: *generated.vote_request.VoteRequest) void {
        for (req.topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
    }

    fn freeVoteResponseTopics(self: *Controller, topics: []const generated.vote_response.VoteResponse.TopicData) void {
        for (topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
    }

    // ---------------------------------------------------------------
    // BeginQuorumEpoch (key 53) — KRaft leader heartbeat
    // ---------------------------------------------------------------
    fn handleBeginQuorumEpoch(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.begin_quorum_epoch_request.BeginQuorumEpochRequest;
        const Resp = generated.begin_quorum_epoch_response.BeginQuorumEpochResponse;

        if (!validateBeginQuorumEpochRequestFrame(request_bytes, start_pos, api_version)) {
            log.warn("Malformed BeginQuorumEpoch request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode BeginQuorumEpoch request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeBeginQuorumEpochRequest(&req);

        if (pos < request_bytes.len) {
            const append_response = self.applyInternalRaftAppendEntriesPayload(request_bytes, &pos) catch |err| {
                log.warn("Failed to apply internal AppendEntries payload: {}", .{err});
                const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            const resp = Resp{
                .error_code = if (append_response.success) ErrorCode.none.toInt() else ErrorCode.kafka_storage_error.toInt(),
                .topics = &.{},
            };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const ObservedLeader = struct {
            id: i32,
            epoch: i32,
        };
        var observed_leader: ?ObservedLeader = null;
        for (req.topics) |topic| {
            for (topic.partitions) |partition| {
                if (observed_leader == null or partition.leader_epoch > observed_leader.?.epoch) {
                    observed_leader = .{ .id = partition.leader_id, .epoch = partition.leader_epoch };
                }
            }
        }

        if (observed_leader) |leader| {
            if (leader.epoch >= self.raft_state.current_epoch) {
                self.raft_state.becomeFollower(leader.epoch, leader.id);
                log.info("Acknowledged leader {d} epoch={d}", .{ leader.id, leader.epoch });
            }
        }

        const resp = Resp{ .error_code = ErrorCode.none.toInt(), .topics = &.{} };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // EndQuorumEpoch (key 54) — KRaft leader step-down
    // ---------------------------------------------------------------
    fn handleEndQuorumEpoch(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.end_quorum_epoch_request.EndQuorumEpochRequest;
        const Resp = generated.end_quorum_epoch_response.EndQuorumEpochResponse;

        if (!validateEndQuorumEpochRequestFrame(request_bytes, start_pos, api_version)) {
            log.warn("Malformed EndQuorumEpoch request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode EndQuorumEpoch request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeEndQuorumEpochRequest(&req);

        if (self.raft_state.role == .follower) {
            log.info("Leader stepped down, will start election", .{});
            self.raft_state.election_timer.reset();
        }

        const resp = Resp{ .error_code = ErrorCode.none.toInt(), .topics = &.{} };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn applyInternalRaftAppendEntriesPayload(self: *Controller, request_bytes: []const u8, pos: *usize) !RaftState.AppendEntriesResponse {
        const leader_id = try readRecordI32(request_bytes, pos);
        const leader_epoch = try readRecordI32(request_bytes, pos);

        if (pos.* == request_bytes.len) {
            if (leader_epoch >= self.raft_state.current_epoch) {
                self.raft_state.becomeFollower(leader_epoch, leader_id);
            }
            return .{ .success = true, .epoch = self.raft_state.current_epoch, .match_index = self.raft_state.log.lastOffset() };
        }

        const prev_log_offset_raw = try readRecordI64(request_bytes, pos);
        const prev_log_epoch = try readRecordI32(request_bytes, pos);
        const leader_commit_raw = try readRecordI64(request_bytes, pos);
        const entries_start_index_raw = try readRecordI64(request_bytes, pos);
        const entry_count_raw = try readRecordI32(request_bytes, pos);
        if (prev_log_offset_raw < 0 or leader_commit_raw < 0 or entries_start_index_raw < 0 or entry_count_raw < 0) {
            return error.InvalidAutoMqMetadataRecord;
        }

        const entry_count: usize = @intCast(entry_count_raw);
        const entries = try self.allocator.alloc(RaftState.AppendEntry, entry_count);
        defer self.allocator.free(entries);

        const entries_start_index: u64 = @intCast(entries_start_index_raw);
        const rewrites_applied_metadata = if (self.last_applied_controller_metadata_offset) |last_applied|
            entry_count > 0 and entries_start_index <= last_applied
        else
            false;
        for (entries, 0..) |*entry, i| {
            const entry_len_raw = try readRecordI32(request_bytes, pos);
            if (entry_len_raw < 0) return error.InvalidAutoMqMetadataRecord;
            const entry_len: usize = @intCast(entry_len_raw);
            if (pos.* + entry_len > request_bytes.len) return error.InvalidAutoMqMetadataRecord;
            const offset = std.math.add(u64, entries_start_index, @as(u64, @intCast(i))) catch return error.InvalidAutoMqMetadataRecord;
            entry.* = .{
                .offset = offset,
                .epoch = leader_epoch,
                .data = request_bytes[pos.* .. pos.* + entry_len],
            };
            pos.* += entry_len;
        }
        if (pos.* != request_bytes.len) return error.InvalidAutoMqMetadataRecord;

        const append_response = self.raft_state.handleAppendEntries(
            leader_epoch,
            leader_id,
            @intCast(prev_log_offset_raw),
            prev_log_epoch,
            entries,
            @intCast(leader_commit_raw),
        );
        if (append_response.success) {
            if (rewrites_applied_metadata) {
                _ = try self.replayCommittedControllerMetadataRecords();
            } else {
                _ = try self.applyCommittedControllerMetadataRecords();
            }
            if (self.raft_commit_hook) |hook| hook();
        }
        return append_response;
    }

    fn freeBeginQuorumEpochRequest(self: *Controller, req: *generated.begin_quorum_epoch_request.BeginQuorumEpochRequest) void {
        for (req.topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
        if (req.leader_endpoints.len > 0) self.allocator.free(req.leader_endpoints);
    }

    fn freeEndQuorumEpochRequest(self: *Controller, req: *generated.end_quorum_epoch_request.EndQuorumEpochRequest) void {
        for (req.topics) |topic| {
            for (topic.partitions) |partition| {
                if (partition.preferred_successors.len > 0) self.allocator.free(partition.preferred_successors);
                if (partition.preferred_candidates.len > 0) self.allocator.free(partition.preferred_candidates);
            }
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
        if (req.leader_endpoints.len > 0) self.allocator.free(req.leader_endpoints);
    }

    // ---------------------------------------------------------------
    // DescribeQuorum (key 55) — KRaft quorum info
    // ---------------------------------------------------------------
    fn handleDescribeQuorum(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.describe_quorum_request.DescribeQuorumRequest;
        const Resp = generated.describe_quorum_response.DescribeQuorumResponse;
        const Topic = Resp.TopicData;
        const Partition = Topic.PartitionData;
        const ReplicaState = generated.describe_quorum_response.ReplicaState;
        const Node = Resp.Node;

        var pos = start_pos;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode DescribeQuorum request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeDescribeQuorumRequest(&req);

        if (pos != request_bytes.len) {
            log.warn("DescribeQuorum request has {d} trailing bytes", .{request_bytes.len - pos});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const voter_count = self.raft_state.quorumSize();
        const voters = self.allocator.alloc(ReplicaState, voter_count) catch return null;
        defer self.allocator.free(voters);

        var voter_index: usize = 0;
        var vit = self.raft_state.voters.iterator();
        while (vit.next()) |entry| : (voter_index += 1) {
            voters[voter_index] = .{
                .replica_id = entry.key_ptr.*,
                .replica_directory_id = entry.value_ptr.voter_directory_id,
                .log_end_offset = @intCast(entry.value_ptr.match_index),
                .last_fetch_timestamp = -1,
                .last_caught_up_timestamp = -1,
            };
        }

        const nodes: []const Node = if (api_version >= 2) self.collectDescribeQuorumNodes() catch return null else &.{};
        defer self.freeDescribeQuorumNodes(nodes);

        const requested_topics = if (req.topics.len == 0) 1 else req.topics.len;
        const topics = self.allocator.alloc(Topic, requested_topics) catch return null;
        for (topics) |*topic| {
            topic.* = .{ .topic_name = null, .partitions = &.{} };
        }
        defer {
            for (topics) |topic| {
                if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
            }
            self.allocator.free(topics);
        }

        if (req.topics.len == 0) {
            const partitions = self.allocator.alloc(Partition, 1) catch return null;
            partitions[0] = self.describeQuorumPartition(0, voters, api_version);
            topics[0] = .{
                .topic_name = "__cluster_metadata",
                .partitions = partitions,
            };
        } else {
            for (req.topics, 0..) |topic_req, topic_index| {
                const partition_count = if (topic_req.partitions.len == 0) 1 else topic_req.partitions.len;
                const partitions = self.allocator.alloc(Partition, partition_count) catch return null;
                if (topic_req.partitions.len == 0) {
                    partitions[0] = self.describeQuorumPartition(0, voters, api_version);
                } else {
                    for (topic_req.partitions, 0..) |partition_req, partition_index| {
                        partitions[partition_index] = self.describeQuorumPartition(partition_req.partition_index, voters, api_version);
                    }
                }
                topics[topic_index] = .{
                    .topic_name = topic_req.topic_name,
                    .partitions = partitions,
                };
            }
        }

        const resp = Resp{
            .error_code = 0,
            .topics = topics,
            .nodes = nodes,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn collectDescribeQuorumNodes(self: *Controller) ![]const generated.describe_quorum_response.DescribeQuorumResponse.Node {
        const Node = generated.describe_quorum_response.DescribeQuorumResponse.Node;
        const Listener = Node.Listener;

        const nodes = try self.allocator.alloc(Node, self.raft_state.voters.count());
        var initialized: usize = 0;
        errdefer self.freeDescribeQuorumNodes(nodes[0..initialized]);

        var it = self.raft_state.voters.iterator();
        while (it.next()) |entry| {
            const endpoints = entry.value_ptr.endpoints;
            var listeners: []Listener = &.{};
            if (endpoints.len > 0) listeners = try self.allocator.alloc(Listener, endpoints.len);
            for (endpoints, 0..) |endpoint, listener_index| {
                listeners[listener_index] = .{
                    .name = endpoint.name,
                    .host = endpoint.host,
                    .port = endpoint.port,
                };
            }
            nodes[initialized] = .{
                .node_id = entry.key_ptr.*,
                .listeners = listeners,
            };
            initialized += 1;
        }

        return nodes;
    }

    fn freeDescribeQuorumNodes(self: *Controller, nodes: []const generated.describe_quorum_response.DescribeQuorumResponse.Node) void {
        for (nodes) |node| {
            if (node.listeners.len > 0) self.allocator.free(@constCast(node.listeners));
        }
        if (nodes.len > 0) self.allocator.free(@constCast(nodes));
    }

    fn describeQuorumPartition(self: *Controller, partition_index: i32, voters: []const generated.describe_quorum_response.ReplicaState, api_version: i16) generated.describe_quorum_response.DescribeQuorumResponse.TopicData.PartitionData {
        _ = api_version;
        return .{
            .partition_index = partition_index,
            .error_code = 0,
            .leader_id = self.raft_state.leader_id orelse -1,
            .leader_epoch = self.raft_state.current_epoch,
            .high_watermark = @intCast(self.raft_state.log.lastOffset()),
            .current_voters = voters,
            .observers = &.{},
        };
    }

    fn freeDescribeQuorumRequest(self: *Controller, req: *generated.describe_quorum_request.DescribeQuorumRequest) void {
        for (req.topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
    }

    // ---------------------------------------------------------------
    // FetchSnapshot (key 59) — KRaft snapshot transfer
    // ---------------------------------------------------------------
    fn handleFetchSnapshot(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.fetch_snapshot_request.FetchSnapshotRequest;
        const Resp = generated.fetch_snapshot_response.FetchSnapshotResponse;

        if (!validateFetchSnapshotRequestFrame(request_bytes, start_pos)) {
            log.warn("Malformed FetchSnapshot request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode FetchSnapshot request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .topics = &.{} };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeFetchSnapshotRequest(&req);

        const topics = self.buildFetchSnapshotTopics(&req, api_version) catch return null;
        defer {
            self.freeFetchSnapshotTopics(topics);
            if (topics.len > 0) self.allocator.free(topics);
        }
        const node_endpoints = self.collectFetchSnapshotNodeEndpoints(api_version) catch return null;
        defer if (node_endpoints.len > 0) self.allocator.free(node_endpoints);

        const resp = Resp{
            .throttle_time_ms = 0,
            .error_code = ErrorCode.none.toInt(),
            .topics = topics,
            .node_endpoints = node_endpoints,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn buildFetchSnapshotTopics(self: *Controller, req: *const generated.fetch_snapshot_request.FetchSnapshotRequest, api_version: i16) ![]generated.fetch_snapshot_response.FetchSnapshotResponse.TopicSnapshot {
        const TopicResp = generated.fetch_snapshot_response.FetchSnapshotResponse.TopicSnapshot;
        const PartitionResp = TopicResp.PartitionSnapshot;
        const leader_id = self.raft_state.leader_id orelse -1;
        const current_leader = if (api_version >= 1 and leader_id >= 0)
            PartitionResp.LeaderIdAndEpoch{ .leader_id = leader_id, .leader_epoch = self.raft_state.current_epoch }
        else
            PartitionResp.LeaderIdAndEpoch{};
        var remaining_bytes: usize = if (req.max_bytes <= 0) 0 else @intCast(req.max_bytes);

        if (req.topics.len == 0) return &.{};
        const topics = try self.allocator.alloc(TopicResp, req.topics.len);
        var topics_init: usize = 0;
        errdefer {
            self.freeFetchSnapshotTopics(topics[0..topics_init]);
            self.allocator.free(topics);
        }

        for (req.topics, 0..) |topic_req, topic_index| {
            var partitions: []PartitionResp = &.{};
            if (topic_req.partitions.len > 0) {
                partitions = try self.allocator.alloc(PartitionResp, topic_req.partitions.len);
                for (topic_req.partitions, 0..) |partition_req, partition_index| {
                    partitions[partition_index] = self.buildFetchSnapshotPartition(topic_req.name, partition_req, current_leader, &remaining_bytes);
                }
            }

            topics[topic_index] = .{
                .name = topic_req.name,
                .partitions = partitions,
            };
            topics_init += 1;
        }

        return topics;
    }

    fn buildFetchSnapshotPartition(
        self: *Controller,
        topic_name: ?[]const u8,
        partition_req: generated.fetch_snapshot_request.FetchSnapshotRequest.TopicSnapshot.PartitionSnapshot,
        current_leader: generated.fetch_snapshot_response.FetchSnapshotResponse.TopicSnapshot.PartitionSnapshot.LeaderIdAndEpoch,
        remaining_bytes: *usize,
    ) generated.fetch_snapshot_response.FetchSnapshotResponse.TopicSnapshot.PartitionSnapshot {
        const PartitionResp = generated.fetch_snapshot_response.FetchSnapshotResponse.TopicSnapshot.PartitionSnapshot;
        var resp = PartitionResp{
            .index = partition_req.partition,
            .error_code = ErrorCode.snapshot_not_found.toInt(),
            .snapshot_id = .{
                .end_offset = partition_req.snapshot_id.end_offset,
                .epoch = partition_req.snapshot_id.epoch,
            },
            .size = 0,
            .position = partition_req.position,
            .unaligned_records = null,
            .current_leader = current_leader,
        };

        const snapshot = self.findFetchSnapshotBytes(topic_name, partition_req.partition, partition_req.snapshot_id.end_offset, partition_req.snapshot_id.epoch) orelse return resp;
        resp.size = @intCast(snapshot.len);
        if (partition_req.position < 0 or partition_req.position > resp.size) {
            resp.error_code = ErrorCode.position_out_of_range.toInt();
            return resp;
        }

        const start: usize = @intCast(partition_req.position);
        const available = snapshot.len - start;
        const take = @min(available, remaining_bytes.*);
        resp.error_code = ErrorCode.none.toInt();
        resp.unaligned_records = snapshot[start .. start + take];
        remaining_bytes.* -= take;
        return resp;
    }

    fn findFetchSnapshotBytes(self: *Controller, topic_name: ?[]const u8, partition_id: i32, end_offset: i64, epoch: i32) ?[]const u8 {
        const name = topic_name orelse return null;
        if (!std.mem.eql(u8, name, "__cluster_metadata") or partition_id != 0) return null;
        if (end_offset < 0 or self.raft_state.last_snapshot_offset == 0) return null;
        if (@as(u64, @intCast(end_offset)) != self.raft_state.last_snapshot_offset or epoch != self.raft_state.last_snapshot_epoch) return null;

        const entry = self.raft_state.log.get(self.raft_state.last_snapshot_offset) orelse return null;
        if (!isControllerFullSnapshotRecord(entry.data)) return null;
        return entry.data;
    }

    fn collectFetchSnapshotNodeEndpoints(self: *Controller, api_version: i16) ![]const generated.fetch_snapshot_response.FetchSnapshotResponse.NodeEndpoint {
        const NodeEndpoint = generated.fetch_snapshot_response.FetchSnapshotResponse.NodeEndpoint;
        if (api_version < 1) return &.{};
        const leader_id = self.raft_state.leader_id orelse return &.{};
        const voter = self.raft_state.voters.get(leader_id) orelse return &.{};
        const endpoint = chooseControllerEndpoint(voter.endpoints) orelse return &.{};

        const endpoints = try self.allocator.alloc(NodeEndpoint, 1);
        endpoints[0] = .{
            .node_id = leader_id,
            .host = endpoint.host,
            .port = endpoint.port,
        };
        return endpoints;
    }

    fn chooseControllerEndpoint(endpoints: []const RaftState.VoterEndpoint) ?RaftState.VoterEndpoint {
        if (endpoints.len == 0) return null;
        for (endpoints) |endpoint| {
            if (std.ascii.eqlIgnoreCase(endpoint.name, "CONTROLLER")) return endpoint;
        }
        return endpoints[0];
    }

    fn freeFetchSnapshotRequest(self: *Controller, req: *generated.fetch_snapshot_request.FetchSnapshotRequest) void {
        for (req.topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
        if (req.topics.len > 0) self.allocator.free(req.topics);
    }

    fn freeFetchSnapshotTopics(self: *Controller, topics: []const generated.fetch_snapshot_response.FetchSnapshotResponse.TopicSnapshot) void {
        for (topics) |topic| {
            if (topic.partitions.len > 0) self.allocator.free(topic.partitions);
        }
    }

    const controller_metadata_record_magic = "ZMQCTRL2";

    const ControllerMetadataRecordKind = enum(u8) {
        broker_registration = 1,
        producer_id_allocation = 2,
        full_snapshot = 3,
        broker_unregistration = 4,
        full_snapshot_v2 = 5,
        full_snapshot_v3 = 6,
        broker_log_dir_status = 7,
        full_snapshot_v4 = 8,
    };

    fn controllerBytesFieldSize(bytes: []const u8) !usize {
        if (bytes.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        return 4 + bytes.len;
    }

    fn controllerOptionalBytesFieldSize(bytes: ?[]const u8) !usize {
        return try checkedAddSize(1, if (bytes) |value| try controllerBytesFieldSize(value) else 0);
    }

    fn checkedAddSize(a: usize, b: usize) !usize {
        return std.math.add(usize, a, b) catch error.RecordTooLarge;
    }

    fn isControllerMetadataRecord(data: []const u8) bool {
        return data.len >= controller_metadata_record_magic.len and
            std.mem.eql(u8, data[0..controller_metadata_record_magic.len], controller_metadata_record_magic);
    }

    fn isControllerFullSnapshotRecord(data: []const u8) bool {
        return isControllerMetadataRecord(data) and
            data.len > controller_metadata_record_magic.len and
            (data[controller_metadata_record_magic.len] == @intFromEnum(ControllerMetadataRecordKind.full_snapshot) or
                data[controller_metadata_record_magic.len] == @intFromEnum(ControllerMetadataRecordKind.full_snapshot_v2) or
                data[controller_metadata_record_magic.len] == @intFromEnum(ControllerMetadataRecordKind.full_snapshot_v3) or
                data[controller_metadata_record_magic.len] == @intFromEnum(ControllerMetadataRecordKind.full_snapshot_v4));
    }

    fn controllerRecordKindFromByte(byte: u8) !ControllerMetadataRecordKind {
        return switch (byte) {
            1 => .broker_registration,
            2 => .producer_id_allocation,
            3 => .full_snapshot,
            4 => .broker_unregistration,
            5 => .full_snapshot_v2,
            6 => .full_snapshot_v3,
            7 => .broker_log_dir_status,
            8 => .full_snapshot_v4,
            else => error.InvalidAutoMqMetadataRecord,
        };
    }

    fn buildBrokerRegistrationRecord(self: *Controller, broker_id: i32, host: []const u8, port: u16, broker_epoch: i64) ![]u8 {
        return self.buildBrokerRegistrationRecordWithRack(broker_id, host, port, null, broker_epoch);
    }

    fn buildBrokerRegistrationRecordWithRack(self: *Controller, broker_id: i32, host: []const u8, port: u16, rack: ?[]const u8, broker_epoch: i64) ![]u8 {
        return self.buildBrokerRegistrationRecordWithRackAndLogDirs(broker_id, host, port, rack, &.{}, broker_epoch);
    }

    fn controllerLogDirsFieldSize(log_dirs: []const [16]u8) !usize {
        if (log_dirs.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const bytes = std.math.mul(usize, log_dirs.len, 16) catch return error.RecordTooLarge;
        return try checkedAddSize(4, bytes);
    }

    fn buildBrokerRegistrationRecordWithRackAndLogDirs(self: *Controller, broker_id: i32, host: []const u8, port: u16, rack: ?[]const u8, log_dirs: []const [16]u8, broker_epoch: i64) ![]u8 {
        const host_size = try controllerBytesFieldSize(host);
        const rack_size = try controllerOptionalBytesFieldSize(rack);
        const log_dirs_size = try controllerLogDirsFieldSize(log_dirs);
        var total_len = try checkedAddSize(controller_metadata_record_magic.len, 1);
        total_len = try checkedAddSize(total_len, 4 + 8 + 4 + 1);
        total_len = try checkedAddSize(total_len, host_size);
        total_len = try checkedAddSize(total_len, rack_size);
        total_len = try checkedAddSize(total_len, log_dirs_size);

        const buf = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + controller_metadata_record_magic.len], controller_metadata_record_magic);
        pos += controller_metadata_record_magic.len;
        buf[pos] = @intFromEnum(ControllerMetadataRecordKind.broker_registration);
        pos += 1;
        writeRecordI32(buf, &pos, broker_id);
        writeRecordI64(buf, &pos, broker_epoch);
        writeRecordI32(buf, &pos, @intCast(port));
        writeRecordBool(buf, &pos, true);
        try writeRecordBytes(buf, &pos, host);
        try writeRecordOptionalBytes(buf, &pos, rack);
        try writeRecordLogDirs(buf, &pos, log_dirs);
        std.debug.assert(pos == buf.len);
        return buf;
    }

    fn buildBrokerUnregistrationRecord(self: *Controller, broker_id: i32) ![]u8 {
        var total_len = try checkedAddSize(controller_metadata_record_magic.len, 1);
        total_len = try checkedAddSize(total_len, 4);

        const buf = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + controller_metadata_record_magic.len], controller_metadata_record_magic);
        pos += controller_metadata_record_magic.len;
        buf[pos] = @intFromEnum(ControllerMetadataRecordKind.broker_unregistration);
        pos += 1;
        writeRecordI32(buf, &pos, broker_id);
        std.debug.assert(pos == buf.len);
        return buf;
    }

    fn buildBrokerLogDirStatusRecord(self: *Controller, broker_id: i32, broker_epoch: i64, fenced: bool, offline_log_dirs: []const [16]u8) ![]u8 {
        var total_len = try checkedAddSize(controller_metadata_record_magic.len, 1);
        total_len = try checkedAddSize(total_len, 4 + 8 + 1);
        total_len = try checkedAddSize(total_len, try controllerLogDirsFieldSize(offline_log_dirs));

        const buf = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + controller_metadata_record_magic.len], controller_metadata_record_magic);
        pos += controller_metadata_record_magic.len;
        buf[pos] = @intFromEnum(ControllerMetadataRecordKind.broker_log_dir_status);
        pos += 1;
        writeRecordI32(buf, &pos, broker_id);
        writeRecordI64(buf, &pos, broker_epoch);
        writeRecordBool(buf, &pos, fenced);
        try writeRecordLogDirs(buf, &pos, offline_log_dirs);
        std.debug.assert(pos == buf.len);
        return buf;
    }

    fn buildControllerFullSnapshotRecord(self: *Controller) ![]u8 {
        if (self.broker_registry.count() > std.math.maxInt(u32)) return error.RecordTooLarge;

        var total_len = try checkedAddSize(controller_metadata_record_magic.len, 1);
        total_len = try checkedAddSize(total_len, 8 + 8 + 4);
        var it = self.broker_registry.brokers.iterator();
        while (it.next()) |entry| {
            total_len = try checkedAddSize(total_len, 4 + 8 + 4 + 1);
            total_len = try checkedAddSize(total_len, try controllerBytesFieldSize(entry.value_ptr.host));
            total_len = try checkedAddSize(total_len, try controllerOptionalBytesFieldSize(entry.value_ptr.rack));
            total_len = try checkedAddSize(total_len, try controllerLogDirsFieldSize(entry.value_ptr.log_dirs));
            total_len = try checkedAddSize(total_len, try controllerLogDirsFieldSize(entry.value_ptr.offline_log_dirs));
        }

        const buf = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + controller_metadata_record_magic.len], controller_metadata_record_magic);
        pos += controller_metadata_record_magic.len;
        buf[pos] = @intFromEnum(ControllerMetadataRecordKind.full_snapshot_v4);
        pos += 1;
        writeRecordI64(buf, &pos, self.next_producer_id);
        writeRecordI64(buf, &pos, self.broker_registry.next_broker_epoch);
        writeRecordU32(buf, &pos, @intCast(self.broker_registry.count()));

        it = self.broker_registry.brokers.iterator();
        while (it.next()) |entry| {
            const broker = entry.value_ptr;
            writeRecordI32(buf, &pos, broker.broker_id);
            writeRecordI64(buf, &pos, broker.broker_epoch);
            writeRecordI32(buf, &pos, @intCast(broker.port));
            writeRecordBool(buf, &pos, broker.fenced);
            try writeRecordBytes(buf, &pos, broker.host);
            try writeRecordOptionalBytes(buf, &pos, broker.rack);
            try writeRecordLogDirs(buf, &pos, broker.log_dirs);
            try writeRecordLogDirs(buf, &pos, broker.offline_log_dirs);
        }
        std.debug.assert(pos == buf.len);
        return buf;
    }

    fn buildProducerIdAllocationRecord(self: *Controller, next_producer_id: i64) ![]u8 {
        var total_len = try checkedAddSize(controller_metadata_record_magic.len, 1);
        total_len = try checkedAddSize(total_len, 8);

        const buf = try self.allocator.alloc(u8, total_len);
        var pos: usize = 0;
        @memcpy(buf[pos .. pos + controller_metadata_record_magic.len], controller_metadata_record_magic);
        pos += controller_metadata_record_magic.len;
        buf[pos] = @intFromEnum(ControllerMetadataRecordKind.producer_id_allocation);
        pos += 1;
        writeRecordI64(buf, &pos, next_producer_id);
        std.debug.assert(pos == buf.len);
        return buf;
    }

    fn appendControllerMetadataRecord(self: *Controller, record: []const u8) !u64 {
        if (self.raft_state.role != .leader) return error.NotController;
        const offset = try self.raft_state.appendEntry(record);
        if (self.raft_state.quorumSize() <= 1) {
            self.raft_state.commit_index = offset;
        } else {
            self.raft_state.updateCommitIndex();
        }
        return offset;
    }

    fn applyBrokerRegistrationRecord(self: *Controller, data: []const u8, pos: *usize) !void {
        const broker_id = try readRecordI32(data, pos);
        const broker_epoch = try readRecordI64(data, pos);
        const port_raw = try readRecordI32(data, pos);
        const fenced = try readRecordBool(data, pos);
        const host = try readRecordBytes(data, pos);
        const rack = if (pos.* < data.len) try readRecordOptionalBytes(data, pos) else null;
        const log_dirs = if (pos.* < data.len) try self.readRecordLogDirs(data, pos) else &.{};
        defer self.freeRecordLogDirs(log_dirs);
        if (pos.* != data.len) return error.InvalidAutoMqMetadataRecord;
        if (port_raw < 0 or port_raw > std.math.maxInt(u16)) return error.InvalidAutoMqMetadataRecord;

        try self.broker_registry.registerWithEpochRackAndLogDirs(
            broker_id,
            host,
            @intCast(port_raw),
            rack,
            log_dirs,
            broker_epoch,
            fenced,
        );
    }

    fn applyBrokerUnregistrationRecord(self: *Controller, data: []const u8, pos: *usize) !void {
        const broker_id = try readRecordI32(data, pos);
        if (pos.* != data.len) return error.InvalidAutoMqMetadataRecord;
        _ = self.broker_registry.unregister(broker_id);
    }

    fn applyBrokerLogDirStatusRecord(self: *Controller, data: []const u8, pos: *usize) !void {
        const broker_id = try readRecordI32(data, pos);
        const broker_epoch = try readRecordI64(data, pos);
        const fenced = try readRecordBool(data, pos);
        const offline_log_dirs = try self.readRecordLogDirs(data, pos);
        defer self.freeRecordLogDirs(offline_log_dirs);
        if (pos.* != data.len) return error.InvalidAutoMqMetadataRecord;
        try self.broker_registry.installLogDirStatus(broker_id, broker_epoch, fenced, offline_log_dirs);
    }

    fn applyProducerIdAllocationRecord(self: *Controller, data: []const u8, pos: *usize) !void {
        const next_producer_id = try readRecordI64(data, pos);
        if (pos.* != data.len) return error.InvalidAutoMqMetadataRecord;
        if (next_producer_id <= 0) return error.InvalidAutoMqMetadataRecord;
        self.next_producer_id = @max(self.next_producer_id, next_producer_id);
    }

    fn readRecordLogDirs(self: *Controller, data: []const u8, pos: *usize) ![]const [16]u8 {
        const count = try readRecordU32(data, pos);
        if (count == 0) return &.{};
        const byte_len = std.math.mul(usize, @as(usize, @intCast(count)), 16) catch return error.InvalidAutoMqMetadataRecord;
        if (pos.* > data.len or byte_len > data.len - pos.*) return error.InvalidAutoMqMetadataRecord;
        const log_dirs = try self.allocator.alloc([16]u8, @intCast(count));
        errdefer self.allocator.free(log_dirs);
        for (log_dirs) |*dir| {
            @memcpy(dir[0..], data[pos.* .. pos.* + 16]);
            pos.* += 16;
        }
        return log_dirs;
    }

    fn freeRecordLogDirs(self: *Controller, log_dirs: []const [16]u8) void {
        if (log_dirs.len > 0) self.allocator.free(log_dirs);
    }

    fn applyControllerFullSnapshotRecord(self: *Controller, data: []const u8, pos: *usize, has_rack_metadata: bool, has_log_dirs: bool, has_offline_log_dirs: bool) !void {
        const next_producer_id = try readRecordI64(data, pos);
        const next_broker_epoch = try readRecordI64(data, pos);
        const broker_count = try readRecordU32(data, pos);
        if (next_producer_id <= 0 or next_broker_epoch <= 0) return error.InvalidAutoMqMetadataRecord;

        const brokers_start = pos.*;
        var scan_pos = brokers_start;
        var i: u32 = 0;
        while (i < broker_count) : (i += 1) {
            _ = try readRecordI32(data, &scan_pos);
            _ = try readRecordI64(data, &scan_pos);
            const port_raw = try readRecordI32(data, &scan_pos);
            if (port_raw < 0 or port_raw > std.math.maxInt(u16)) return error.InvalidAutoMqMetadataRecord;
            _ = try readRecordBool(data, &scan_pos);
            _ = try readRecordBytes(data, &scan_pos);
            if (has_rack_metadata) _ = try readRecordOptionalBytes(data, &scan_pos);
            if (has_log_dirs) try skipRecordLogDirs(data, &scan_pos);
            if (has_offline_log_dirs) try skipRecordLogDirs(data, &scan_pos);
        }
        if (scan_pos != data.len) return error.InvalidAutoMqMetadataRecord;

        self.broker_registry.deinit();
        self.broker_registry = BrokerRegistry.init(self.allocator);
        self.next_producer_id = next_producer_id;

        pos.* = brokers_start;
        i = 0;
        while (i < broker_count) : (i += 1) {
            const broker_id = try readRecordI32(data, pos);
            const broker_epoch = try readRecordI64(data, pos);
            const port_raw = try readRecordI32(data, pos);
            const fenced = try readRecordBool(data, pos);
            const host = try readRecordBytes(data, pos);
            const rack = if (has_rack_metadata) try readRecordOptionalBytes(data, pos) else null;
            const log_dirs = if (has_log_dirs) try self.readRecordLogDirs(data, pos) else &.{};
            defer self.freeRecordLogDirs(log_dirs);
            const offline_log_dirs = if (has_offline_log_dirs) try self.readRecordLogDirs(data, pos) else &.{};
            defer self.freeRecordLogDirs(offline_log_dirs);
            try self.broker_registry.registerWithEpochRackAndLogDirs(
                broker_id,
                host,
                @intCast(port_raw),
                rack,
                log_dirs,
                broker_epoch,
                fenced,
            );
            if (has_offline_log_dirs) {
                try self.broker_registry.installLogDirStatus(broker_id, broker_epoch, fenced, offline_log_dirs);
            }
        }
        self.broker_registry.next_broker_epoch = @max(self.broker_registry.next_broker_epoch, next_broker_epoch);
        pos.* = data.len;
    }

    fn applyControllerMetadataRecord(self: *Controller, data: []const u8) !void {
        if (!isControllerMetadataRecord(data)) return;
        var pos = controller_metadata_record_magic.len;
        if (pos + 1 > data.len) return error.InvalidAutoMqMetadataRecord;
        const kind = try controllerRecordKindFromByte(data[pos]);
        pos += 1;

        switch (kind) {
            .broker_registration => try self.applyBrokerRegistrationRecord(data, &pos),
            .producer_id_allocation => try self.applyProducerIdAllocationRecord(data, &pos),
            .full_snapshot => try self.applyControllerFullSnapshotRecord(data, &pos, false, false, false),
            .broker_unregistration => try self.applyBrokerUnregistrationRecord(data, &pos),
            .full_snapshot_v2 => try self.applyControllerFullSnapshotRecord(data, &pos, true, false, false),
            .full_snapshot_v3 => try self.applyControllerFullSnapshotRecord(data, &pos, true, true, false),
            .broker_log_dir_status => try self.applyBrokerLogDirStatusRecord(data, &pos),
            .full_snapshot_v4 => try self.applyControllerFullSnapshotRecord(data, &pos, true, true, true),
        }
    }

    pub fn applyCommittedControllerMetadataRecords(self: *Controller) !usize {
        var applied: usize = 0;
        for (self.raft_state.log.entries.items) |entry| {
            if (entry.offset > self.raft_state.commit_index) break;
            if (self.last_applied_controller_metadata_offset) |last_offset| {
                if (entry.offset <= last_offset) continue;
            }
            if (!isControllerMetadataRecord(entry.data)) continue;
            try self.applyControllerMetadataRecord(entry.data);
            self.last_applied_controller_metadata_offset = entry.offset;
            applied += 1;
        }
        return applied;
    }

    pub fn replayCommittedControllerMetadataRecords(self: *Controller) !usize {
        self.broker_registry.deinit();
        self.broker_registry = BrokerRegistry.init(self.allocator);
        self.next_producer_id = 1000;
        self.last_applied_controller_metadata_offset = null;
        return self.applyCommittedControllerMetadataRecords();
    }

    pub fn prepareControllerMetadataSnapshotForRaftCompaction(self: *Controller) !void {
        if (self.raft_state.role != .leader) return error.NotController;
        if (self.raft_state.quorumSize() > 1) return error.QuorumCommitPending;

        const record = try self.buildControllerFullSnapshotRecord();
        defer self.allocator.free(record);
        const offset = try self.appendControllerMetadataRecord(record);
        self.last_applied_controller_metadata_offset = offset;
    }

    // ---------------------------------------------------------------
    // BrokerRegistration (key 62) — broker lifecycle
    // ---------------------------------------------------------------
    fn handleBrokerRegistration(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.broker_registration_request.BrokerRegistrationRequest;
        const Resp = generated.broker_registration_response.BrokerRegistrationResponse;
        var pos = start_pos;

        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode BrokerRegistration request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .broker_epoch = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeBrokerRegistrationRequest(&req);

        if (pos != request_bytes.len) {
            log.warn("BrokerRegistration request has {d} trailing bytes", .{request_bytes.len - pos});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .broker_epoch = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const listener = if (req.listeners.len > 0) req.listeners[0] else null;
        const host = if (listener) |l| (l.host orelse "unknown") else "unknown";
        const broker_port: u16 = if (listener) |l| l.port else 0;

        if (self.raft_state.role != .leader) {
            const resp = Resp{ .error_code = ErrorCode.not_controller.toInt(), .broker_epoch = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const broker_epoch = self.broker_registry.next_broker_epoch;
        const record = self.buildBrokerRegistrationRecordWithRackAndLogDirs(req.broker_id, host, broker_port, req.rack, req.log_dirs, broker_epoch) catch {
            log.warn("BrokerRegistration record build failed for broker {d}", .{req.broker_id});
            const resp = Resp{ .error_code = ErrorCode.kafka_storage_error.toInt(), .broker_epoch = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.allocator.free(record);

        const offset = self.appendControllerMetadataRecord(record) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotController => ErrorCode.not_controller.toInt(),
                else => ErrorCode.kafka_storage_error.toInt(),
            };
            log.warn("BrokerRegistration metadata append failed for broker {d}: {}", .{ req.broker_id, err });
            const resp = Resp{ .error_code = error_code, .broker_epoch = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        self.broker_registry.registerWithEpochRackAndLogDirs(
            req.broker_id,
            host,
            broker_port,
            req.rack,
            req.log_dirs,
            broker_epoch,
            true,
        ) catch {
            log.warn("BrokerRegistration failed for broker {d}", .{req.broker_id});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .broker_epoch = -1 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        self.last_applied_controller_metadata_offset = offset;

        const resp = Resp{ .throttle_time_ms = 0, .error_code = 0, .broker_epoch = broker_epoch };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn freeBrokerRegistrationRequest(self: *Controller, req: *generated.broker_registration_request.BrokerRegistrationRequest) void {
        if (req.listeners.len > 0) self.allocator.free(req.listeners);
        if (req.features.len > 0) self.allocator.free(req.features);
        if (req.log_dirs.len > 0) self.allocator.free(req.log_dirs);
    }

    // ---------------------------------------------------------------
    // BrokerHeartbeat (key 63) — broker liveness
    // ---------------------------------------------------------------
    fn handleBrokerHeartbeat(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
        const Resp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;
        var pos = start_pos;

        const req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode BrokerHeartbeat request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .is_caught_up = false, .is_fenced = true, .should_shut_down = false };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer if (req.offline_log_dirs.len > 0) self.allocator.free(req.offline_log_dirs);

        if (pos != request_bytes.len) {
            log.warn("BrokerHeartbeat request has {d} trailing bytes", .{request_bytes.len - pos});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .is_caught_up = false, .is_fenced = true, .should_shut_down = false };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (self.raft_state.role != .leader) {
            const resp = Resp{
                .throttle_time_ms = 0,
                .error_code = ErrorCode.not_controller.toInt(),
                .is_caught_up = false,
                .is_fenced = true,
                .should_shut_down = false,
            };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        _ = self.applyCommittedControllerMetadataRecords() catch |err| {
            log.warn("Failed to apply controller metadata before heartbeat: {}", .{err});
            const resp = Resp{
                .throttle_time_ms = 0,
                .error_code = ErrorCode.kafka_storage_error.toInt(),
                .is_caught_up = false,
                .is_fenced = true,
                .should_shut_down = false,
            };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const persist_log_dir_status = req.want_fence or req.offline_log_dirs.len > 0 or self.broker_registry.offlineLogDirsChanged(req.broker_id, req.offline_log_dirs);
        if (persist_log_dir_status) {
            self.broker_registry.validateHeartbeatOfflineLogDirs(req.broker_id, req.broker_epoch, req.offline_log_dirs) catch |err| {
                const error_code = switch (err) {
                    error.BrokerNotRegistered => ErrorCode.broker_id_not_registered.toInt(),
                    error.StaleBrokerEpoch => ErrorCode.stale_broker_epoch.toInt(),
                    error.InvalidOfflineLogDir => ErrorCode.invalid_request.toInt(),
                };
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = error_code,
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };

            const desired_fenced = self.broker_registry.desiredFencedForOfflineLogDirs(req.broker_id, req.want_fence, req.offline_log_dirs) catch {
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = ErrorCode.broker_id_not_registered.toInt(),
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };

            const record = self.buildBrokerLogDirStatusRecord(req.broker_id, req.broker_epoch, desired_fenced, req.offline_log_dirs) catch {
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = ErrorCode.kafka_storage_error.toInt(),
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            defer self.allocator.free(record);

            const offset = self.appendControllerMetadataRecord(record) catch |err| {
                const error_code: i16 = switch (err) {
                    error.NotController => ErrorCode.not_controller.toInt(),
                    else => ErrorCode.kafka_storage_error.toInt(),
                };
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = error_code,
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };

            self.broker_registry.installLogDirStatus(req.broker_id, req.broker_epoch, desired_fenced, req.offline_log_dirs) catch |err| {
                const error_code: i16 = switch (err) {
                    error.BrokerNotRegistered => ErrorCode.broker_id_not_registered.toInt(),
                    error.StaleBrokerEpoch => ErrorCode.stale_broker_epoch.toInt(),
                    error.InvalidOfflineLogDir => ErrorCode.invalid_request.toInt(),
                    else => ErrorCode.kafka_storage_error.toInt(),
                };
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = error_code,
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            self.last_applied_controller_metadata_offset = offset;
        } else {
            const heartbeat_result = self.broker_registry.heartbeatWithOfflineLogDirs(req.broker_id, req.broker_epoch, req.want_fence, req.offline_log_dirs) catch |err| {
                const error_code = switch (err) {
                    error.BrokerNotRegistered => ErrorCode.broker_id_not_registered.toInt(),
                    error.InvalidOfflineLogDir => ErrorCode.invalid_request.toInt(),
                    else => ErrorCode.kafka_storage_error.toInt(),
                };
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = error_code,
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };

            if (!heartbeat_result) {
                const resp = Resp{
                    .throttle_time_ms = 0,
                    .error_code = ErrorCode.stale_broker_epoch.toInt(),
                    .is_caught_up = false,
                    .is_fenced = true,
                    .should_shut_down = false,
                };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            }
        }

        const broker_info = self.broker_registry.brokers.get(req.broker_id) orelse {
            const resp = Resp{
                .throttle_time_ms = 0,
                .error_code = ErrorCode.broker_id_not_registered.toInt(),
                .is_caught_up = false,
                .is_fenced = true,
                .should_shut_down = false,
            };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const resp = Resp{
            .throttle_time_ms = 0,
            .error_code = 0,
            .is_caught_up = !broker_info.fenced,
            .is_fenced = broker_info.fenced,
            .should_shut_down = req.want_shut_down,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // UnregisterBroker (key 64) — broker lifecycle
    // ---------------------------------------------------------------
    fn handleUnregisterBroker(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.unregister_broker_request.UnregisterBrokerRequest;
        const Resp = generated.unregister_broker_response.UnregisterBrokerResponse;

        if (!validateUnregisterBrokerRequestFrame(request_bytes, start_pos)) {
            log.warn("Malformed UnregisterBroker request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed UnregisterBroker request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;
        const req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode UnregisterBroker request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed UnregisterBroker request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        if (self.raft_state.role != .leader) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.not_controller.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        _ = self.applyCommittedControllerMetadataRecords() catch |err| {
            log.warn("Failed to apply controller metadata before unregistering broker {d}: {}", .{ req.broker_id, err });
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.kafka_storage_error.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        if (!self.broker_registry.brokers.contains(req.broker_id)) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.broker_id_not_registered.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const record = self.buildBrokerUnregistrationRecord(req.broker_id) catch {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.kafka_storage_error.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.allocator.free(record);

        const offset = self.appendControllerMetadataRecord(record) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotController => ErrorCode.not_controller.toInt(),
                else => ErrorCode.kafka_storage_error.toInt(),
            };
            log.warn("UnregisterBroker metadata append failed for broker {d}: {}", .{ req.broker_id, err });
            const resp = Resp{ .throttle_time_ms = 0, .error_code = error_code, .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        _ = self.broker_registry.unregister(req.broker_id);
        self.last_applied_controller_metadata_offset = offset;

        const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.none.toInt(), .error_message = null };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // AddRaftVoter (key 80) — dynamic voter membership
    // ---------------------------------------------------------------
    fn handleAddRaftVoter(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.add_raft_voter_request.AddRaftVoterRequest;
        const Resp = generated.add_raft_voter_response.AddRaftVoterResponse;

        if (!validateAddRaftVoterRequestFrame(request_bytes, start_pos)) {
            log.warn("Malformed AddRaftVoter request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed AddRaftVoter request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;

        const req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode AddRaftVoter request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed AddRaftVoter request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer if (req.listeners.len > 0) self.allocator.free(req.listeners);

        if (req.voter_id < 0) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var endpoint_views: []RaftState.VoterEndpointView = &.{};
        if (req.listeners.len > 0) {
            endpoint_views = self.allocator.alloc(RaftState.VoterEndpointView, req.listeners.len) catch return null;
            for (req.listeners, 0..) |listener, index| {
                const name = listener.name orelse {
                    self.allocator.free(endpoint_views);
                    const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt(), .error_message = null };
                    return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
                };
                const host = listener.host orelse {
                    self.allocator.free(endpoint_views);
                    const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt(), .error_message = null };
                    return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
                };
                if (name.len == 0 or host.len == 0 or listener.port == 0) {
                    self.allocator.free(endpoint_views);
                    const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt(), .error_message = null };
                    return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
                }
                endpoint_views[index] = .{
                    .name = name,
                    .host = host,
                    .port = listener.port,
                };
            }
        }
        defer if (endpoint_views.len > 0) self.allocator.free(endpoint_views);

        const offset = (if (endpoint_views.len > 0)
            self.raft_state.proposeAddVoterWithMetadata(req.voter_id, req.voter_directory_id, endpoint_views)
        else
            self.raft_state.proposeAddVoter(req.voter_id)) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotLeader => ErrorCode.not_controller.toInt(),
                error.ConfigChangePending => ErrorCode.concurrent_transactions.toInt(),
                error.VoterAlreadyExists => ErrorCode.duplicate_resource.toInt(),
                error.InvalidEndpoint, error.MessageTooLarge => ErrorCode.invalid_request.toInt(),
                else => ErrorCode.invalid_request.toInt(),
            };
            const resp = Resp{ .error_code = error_code, .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        log.info("AddRaftVoter: proposed adding node {d} at offset {d}", .{ req.voter_id, offset });

        const resp = Resp{ .throttle_time_ms = 0, .error_code = 0, .error_message = null };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // RemoveRaftVoter (key 81) — dynamic voter membership
    // ---------------------------------------------------------------
    fn handleRemoveRaftVoter(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.remove_raft_voter_request.RemoveRaftVoterRequest;
        const Resp = generated.remove_raft_voter_response.RemoveRaftVoterResponse;

        if (!validateRemoveRaftVoterRequestFrame(request_bytes, start_pos)) {
            log.warn("Malformed RemoveRaftVoter request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed RemoveRaftVoter request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;

        const req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode RemoveRaftVoter request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed RemoveRaftVoter request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        const offset = self.raft_state.proposeRemoveVoter(req.voter_id) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotLeader => ErrorCode.not_controller.toInt(),
                error.ConfigChangePending => ErrorCode.concurrent_transactions.toInt(),
                error.VoterNotFound => ErrorCode.resource_not_found.toInt(),
                error.CannotRemoveLastVoter => ErrorCode.invalid_request.toInt(),
                else => ErrorCode.invalid_request.toInt(),
            };
            const resp = Resp{ .error_code = error_code, .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        log.info("RemoveRaftVoter: proposed removing node {d} at offset {d}", .{ req.voter_id, offset });

        const resp = Resp{ .throttle_time_ms = 0, .error_code = 0, .error_message = null };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // UpdateRaftVoter (key 82) — dynamic voter endpoint metadata
    // ---------------------------------------------------------------
    fn handleUpdateRaftVoter(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.update_raft_voter_request.UpdateRaftVoterRequest;
        const Resp = generated.update_raft_voter_response.UpdateRaftVoterResponse;

        if (!validateUpdateRaftVoterRequestFrame(request_bytes, start_pos)) {
            log.warn("Malformed UpdateRaftVoter request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt() };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;

        const req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode UpdateRaftVoter request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt() };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer if (req.listeners.len > 0) self.allocator.free(req.listeners);

        if (req.cluster_id) |cluster_id| {
            if (!std.mem.eql(u8, cluster_id, self.cluster_id)) {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.inconsistent_cluster_id.toInt() };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            }
        }

        if (req.voter_id < 0 or req.listeners.len == 0) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt() };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (self.raft_state.role != .leader) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.not_controller.toInt() };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (!self.raft_state.voters.contains(req.voter_id)) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.resource_not_found.toInt() };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (req.k_raft_version_feature.min_supported_version > req.k_raft_version_feature.max_supported_version) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_update_version.toInt() };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const endpoint_views = self.allocator.alloc(RaftState.VoterEndpointView, req.listeners.len) catch return null;
        defer self.allocator.free(endpoint_views);
        for (req.listeners, 0..) |listener, index| {
            const name = listener.name orelse {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt() };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            const host = listener.host orelse {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt() };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            if (name.len == 0 or host.len == 0 or listener.port == 0) {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_request.toInt() };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            }
            endpoint_views[index] = .{
                .name = name,
                .host = host,
                .port = listener.port,
            };
        }

        const offset = self.raft_state.proposeUpdateVoter(
            req.voter_id,
            req.voter_directory_id,
            endpoint_views,
            req.k_raft_version_feature.min_supported_version,
            req.k_raft_version_feature.max_supported_version,
        ) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotLeader => ErrorCode.not_controller.toInt(),
                error.ConfigChangePending => ErrorCode.concurrent_transactions.toInt(),
                error.VoterNotFound => ErrorCode.resource_not_found.toInt(),
                error.InvalidUpdateVersion => ErrorCode.invalid_update_version.toInt(),
                error.InvalidEndpoint, error.MessageTooLarge => ErrorCode.invalid_request.toInt(),
                else => ErrorCode.invalid_request.toInt(),
            };
            const resp = Resp{ .throttle_time_ms = 0, .error_code = error_code };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        log.info("UpdateRaftVoter: proposed updating node {d} at offset {d}", .{ req.voter_id, offset });

        const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.none.toInt() };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // AllocateProducerIds (key 67) — PID block allocation
    // ---------------------------------------------------------------
    fn handleAllocateProducerIds(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.allocate_producer_ids_request.AllocateProducerIdsRequest;
        const Resp = generated.allocate_producer_ids_response.AllocateProducerIdsResponse;
        var pos = start_pos;

        const req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode AllocateProducerIds request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .producer_id_start = -1, .producer_id_len = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        _ = req.broker_id;
        _ = req.broker_epoch;

        if (pos != request_bytes.len) {
            log.warn("AllocateProducerIds request has {d} trailing bytes", .{request_bytes.len - pos});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .producer_id_start = -1, .producer_id_len = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (self.raft_state.role != .leader) {
            const resp = Resp{ .error_code = ErrorCode.not_controller.toInt(), .producer_id_start = -1, .producer_id_len = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        // Allocate a block of 1000 producer IDs
        const block_start = self.next_producer_id;
        const block_len: i32 = 1000;
        const next_producer_id = block_start + block_len;

        const record = self.buildProducerIdAllocationRecord(next_producer_id) catch {
            const resp = Resp{ .error_code = ErrorCode.kafka_storage_error.toInt(), .producer_id_start = -1, .producer_id_len = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.allocator.free(record);

        const offset = self.appendControllerMetadataRecord(record) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotController => ErrorCode.not_controller.toInt(),
                else => ErrorCode.kafka_storage_error.toInt(),
            };
            const resp = Resp{ .error_code = error_code, .producer_id_start = -1, .producer_id_len = 0 };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        self.next_producer_id = next_producer_id;
        self.last_applied_controller_metadata_offset = offset;

        const resp = Resp{
            .throttle_time_ms = 0,
            .error_code = 0,
            .producer_id_start = block_start,
            .producer_id_len = block_len,
        };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    // ---------------------------------------------------------------
    // ControllerRegistration (key 70) — KRaft controller lifecycle
    // ---------------------------------------------------------------
    fn handleControllerRegistration(self: *Controller, request_bytes: []const u8, start_pos: usize, req_header: *const RequestHeader, api_version: i16, resp_header_version: i16) ?[]u8 {
        const Req = generated.controller_registration_request.ControllerRegistrationRequest;
        const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

        if (!validateControllerRegistrationRequestFrame(request_bytes, start_pos)) {
            log.warn("Malformed ControllerRegistration request", .{});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed ControllerRegistration request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        var pos = start_pos;
        var req = Req.deserialize(self.allocator, request_bytes, &pos, api_version) catch |err| {
            log.warn("Failed to decode ControllerRegistration request: {}", .{err});
            const resp = Resp{ .error_code = ErrorCode.invalid_request.toInt(), .error_message = "malformed ControllerRegistration request" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };
        defer self.freeControllerRegistrationRequest(&req);

        if (req.controller_id < 0 or req.listeners.len == 0) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_registration.toInt(), .error_message = "invalid controller registration" };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (self.raft_state.role != .leader) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.not_controller.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        if (!self.raft_state.voters.contains(req.controller_id)) {
            const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.unknown_controller_id.toInt(), .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        }

        const voter = self.raft_state.voters.get(req.controller_id).?;
        var min_kraft_version = voter.k_raft_min_supported_version;
        var max_kraft_version = voter.k_raft_max_supported_version;

        for (req.features) |feature| {
            if (feature.min_supported_version > feature.max_supported_version) {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_registration.toInt(), .error_message = "invalid controller feature range" };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            }
            const name = feature.name orelse {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_registration.toInt(), .error_message = "invalid controller feature" };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            if (std.mem.eql(u8, name, "kraft.version")) {
                min_kraft_version = feature.min_supported_version;
                max_kraft_version = feature.max_supported_version;
            }
        }

        const endpoint_views = self.allocator.alloc(RaftState.VoterEndpointView, req.listeners.len) catch return null;
        defer self.allocator.free(endpoint_views);
        for (req.listeners, 0..) |listener, index| {
            const name = listener.name orelse {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_registration.toInt(), .error_message = "invalid controller listener" };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            const host = listener.host orelse {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_registration.toInt(), .error_message = "invalid controller listener" };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            };
            if (name.len == 0 or host.len == 0 or listener.port == 0) {
                const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.invalid_registration.toInt(), .error_message = "invalid controller listener" };
                return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
            }
            endpoint_views[index] = .{
                .name = name,
                .host = host,
                .port = listener.port,
            };
        }

        const offset = self.raft_state.proposeUpdateVoter(
            req.controller_id,
            voter.voter_directory_id,
            endpoint_views,
            min_kraft_version,
            max_kraft_version,
        ) catch |err| {
            const error_code: i16 = switch (err) {
                error.NotLeader => ErrorCode.not_controller.toInt(),
                error.ConfigChangePending => ErrorCode.concurrent_transactions.toInt(),
                error.VoterNotFound => ErrorCode.unknown_controller_id.toInt(),
                error.InvalidUpdateVersion, error.InvalidEndpoint, error.MessageTooLarge => ErrorCode.invalid_registration.toInt(),
                else => ErrorCode.invalid_registration.toInt(),
            };
            const resp = Resp{ .throttle_time_ms = 0, .error_code = error_code, .error_message = null };
            return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
        };

        log.info("ControllerRegistration accepted controller {d} with {d} listener(s) at offset {d}", .{ req.controller_id, req.listeners.len, offset });

        const resp = Resp{ .throttle_time_ms = 0, .error_code = ErrorCode.none.toInt(), .error_message = null };
        return self.serializeGeneratedResponse(req_header, resp_header_version, &resp, api_version);
    }

    fn freeControllerRegistrationRequest(self: *Controller, req: *generated.controller_registration_request.ControllerRegistrationRequest) void {
        if (req.listeners.len > 0) self.allocator.free(req.listeners);
        if (req.features.len > 0) self.allocator.free(req.features);
    }

    // ---------------------------------------------------------------
    // Unsupported API
    // ---------------------------------------------------------------
    fn handleUnsupported(self: *Controller, req_header: *const RequestHeader, api_key: i16, resp_header_version: i16) ?[]u8 {
        log.warn("Unsupported API on controller port: {d}", .{api_key});
        return self.errorResponse(req_header, resp_header_version, 35); // UNSUPPORTED_VERSION
    }

    fn serializeGeneratedResponse(self: *Controller, req_header: *const RequestHeader, resp_header_version: i16, resp_body: anytype, body_version: i16) ?[]u8 {
        const resp_header = ResponseHeader{ .correlation_id = req_header.correlation_id };
        const header_size = resp_header.calcSize(resp_header_version);
        const body_size = resp_body.calcSize(body_version);
        const total_size = header_size + body_size;

        const buf = self.allocator.alloc(u8, total_size) catch return null;
        var wpos: usize = 0;
        resp_header.serialize(buf, &wpos, resp_header_version);
        resp_body.serialize(buf, &wpos, body_version);
        return buf[0..wpos];
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

fn validateVoteRequestFrame(buf: []const u8, start_pos: usize, api_version: i16) bool {
    var pos = start_pos;
    if (!skipKafkaString(buf, &pos, true)) return false; // cluster_id
    if (api_version >= 1 and !skipFixedBytes(buf, &pos, 4)) return false; // voter_id
    if (!skipVoteTopics(buf, &pos, api_version)) return false;
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateBeginQuorumEpochRequestFrame(buf: []const u8, start_pos: usize, api_version: i16) bool {
    const flexible = api_version >= 1;
    var pos = start_pos;

    if (!skipKafkaString(buf, &pos, flexible)) return false; // cluster_id
    if (flexible and !skipFixedBytes(buf, &pos, 4)) return false; // voter_id
    if (!skipBeginQuorumTopics(buf, &pos, flexible)) return false;
    if (flexible and !skipQuorumLeaderEndpoints(buf, &pos)) return false;
    if (flexible) ser.skipTaggedFields(buf, &pos) catch return false;
    return pos <= buf.len;
}

fn validateEndQuorumEpochRequestFrame(buf: []const u8, start_pos: usize, api_version: i16) bool {
    const flexible = api_version >= 1;
    var pos = start_pos;

    if (!skipKafkaString(buf, &pos, flexible)) return false; // cluster_id
    if (!skipEndQuorumTopics(buf, &pos, flexible)) return false;
    if (flexible and !skipQuorumLeaderEndpoints(buf, &pos)) return false;
    if (flexible) ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateFetchSnapshotRequestFrame(buf: []const u8, start_pos: usize) bool {
    var pos = start_pos;

    if (!skipFixedBytes(buf, &pos, 8)) return false; // replica_id + max_bytes
    const topic_count = readKafkaArrayCount(buf, &pos, true) orelse return false;
    for (0..topic_count) |_| {
        if (!skipKafkaString(buf, &pos, true)) return false; // topic name
        const partition_count = readKafkaArrayCount(buf, &pos, true) orelse return false;
        for (0..partition_count) |_| {
            if (!skipFixedBytes(buf, &pos, 8)) return false; // partition + current_leader_epoch
            if (!skipFixedBytes(buf, &pos, 12)) return false; // snapshot_id
            ser.skipTaggedFields(buf, &pos) catch return false;
            if (!skipFixedBytes(buf, &pos, 8)) return false; // position
            ser.skipTaggedFields(buf, &pos) catch return false;
        }
        ser.skipTaggedFields(buf, &pos) catch return false;
    }
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateUnregisterBrokerRequestFrame(buf: []const u8, start_pos: usize) bool {
    var pos = start_pos;
    if (!skipFixedBytes(buf, &pos, 4)) return false; // broker_id
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateControllerRegistrationRequestFrame(buf: []const u8, start_pos: usize) bool {
    var pos = start_pos;

    if (!skipFixedBytes(buf, &pos, 21)) return false; // controller_id + incarnation_id + zk_migration_ready
    const listener_count = readKafkaArrayCount(buf, &pos, true) orelse return false;
    for (0..listener_count) |_| {
        if (!skipKafkaString(buf, &pos, true)) return false; // listener name
        if (!skipKafkaString(buf, &pos, true)) return false; // listener host
        if (!skipFixedBytes(buf, &pos, 4)) return false; // listener port + security_protocol
        ser.skipTaggedFields(buf, &pos) catch return false;
    }
    const feature_count = readKafkaArrayCount(buf, &pos, true) orelse return false;
    for (0..feature_count) |_| {
        if (!skipKafkaString(buf, &pos, true)) return false; // feature name
        if (!skipFixedBytes(buf, &pos, 4)) return false; // min/max supported versions
        ser.skipTaggedFields(buf, &pos) catch return false;
    }
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateAddRaftVoterRequestFrame(buf: []const u8, start_pos: usize) bool {
    var pos = start_pos;

    if (!skipKafkaString(buf, &pos, true)) return false; // cluster_id
    if (!skipFixedBytes(buf, &pos, 24)) return false; // timeout_ms + voter_id + voter_directory_id
    const listener_count = readKafkaArrayCount(buf, &pos, true) orelse return false;
    for (0..listener_count) |_| {
        if (!skipKafkaString(buf, &pos, true)) return false; // listener name
        if (!skipKafkaString(buf, &pos, true)) return false; // listener host
        if (!skipFixedBytes(buf, &pos, 2)) return false; // listener port
        ser.skipTaggedFields(buf, &pos) catch return false;
    }
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateRemoveRaftVoterRequestFrame(buf: []const u8, start_pos: usize) bool {
    var pos = start_pos;
    if (!skipKafkaString(buf, &pos, true)) return false; // cluster_id
    if (!skipFixedBytes(buf, &pos, 20)) return false; // voter_id + voter_directory_id
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn validateUpdateRaftVoterRequestFrame(buf: []const u8, start_pos: usize) bool {
    var pos = start_pos;
    if (!skipKafkaString(buf, &pos, true)) return false; // cluster_id
    if (!skipFixedBytes(buf, &pos, 20)) return false; // voter_id + voter_directory_id
    const listener_count = readKafkaArrayCount(buf, &pos, true) orelse return false;
    for (0..listener_count) |_| {
        if (!skipKafkaString(buf, &pos, true)) return false; // listener name
        if (!skipKafkaString(buf, &pos, true)) return false; // listener host
        if (!skipFixedBytes(buf, &pos, 2)) return false; // listener port
        ser.skipTaggedFields(buf, &pos) catch return false;
    }
    if (!skipFixedBytes(buf, &pos, 4)) return false; // KRaftVersionFeature min/max
    ser.skipTaggedFields(buf, &pos) catch return false; // KRaftVersionFeature tags
    ser.skipTaggedFields(buf, &pos) catch return false;
    return pos == buf.len;
}

fn skipVoteTopics(buf: []const u8, pos: *usize, api_version: i16) bool {
    const topic_count = readKafkaArrayCount(buf, pos, true) orelse return false;
    for (0..topic_count) |_| {
        if (!skipKafkaString(buf, pos, true)) return false; // topic_name
        const partition_count = readKafkaArrayCount(buf, pos, true) orelse return false;
        for (0..partition_count) |_| {
            if (!skipFixedBytes(buf, pos, 12)) return false; // partition_index + candidate_epoch + candidate_id
            if (api_version >= 1 and !skipFixedBytes(buf, pos, 32)) return false; // candidate + voter directory IDs
            if (!skipFixedBytes(buf, pos, 12)) return false; // last_offset_epoch + last_offset
            ser.skipTaggedFields(buf, pos) catch return false;
        }
        ser.skipTaggedFields(buf, pos) catch return false;
    }
    return true;
}

fn skipBeginQuorumTopics(buf: []const u8, pos: *usize, flexible: bool) bool {
    const topic_count = readKafkaArrayCount(buf, pos, flexible) orelse return false;
    for (0..topic_count) |_| {
        if (!skipKafkaString(buf, pos, flexible)) return false; // topic_name
        const partition_count = readKafkaArrayCount(buf, pos, flexible) orelse return false;
        for (0..partition_count) |_| {
            if (!skipFixedBytes(buf, pos, 4)) return false; // partition_index
            if (flexible and !skipFixedBytes(buf, pos, 16)) return false; // voter_directory_id
            if (!skipFixedBytes(buf, pos, 8)) return false; // leader_id + leader_epoch
            if (flexible) ser.skipTaggedFields(buf, pos) catch return false;
        }
        if (flexible) ser.skipTaggedFields(buf, pos) catch return false;
    }
    return true;
}

fn skipEndQuorumTopics(buf: []const u8, pos: *usize, flexible: bool) bool {
    const topic_count = readKafkaArrayCount(buf, pos, flexible) orelse return false;
    for (0..topic_count) |_| {
        if (!skipKafkaString(buf, pos, flexible)) return false; // topic_name
        const partition_count = readKafkaArrayCount(buf, pos, flexible) orelse return false;
        for (0..partition_count) |_| {
            if (!skipFixedBytes(buf, pos, 12)) return false; // partition_index + leader_id + leader_epoch
            if (!skipKafkaI32Array(buf, pos, flexible)) return false; // preferred_successors
            if (flexible and !skipEndQuorumPreferredCandidates(buf, pos)) return false;
            if (flexible) ser.skipTaggedFields(buf, pos) catch return false;
        }
        if (flexible) ser.skipTaggedFields(buf, pos) catch return false;
    }
    return true;
}

fn skipEndQuorumPreferredCandidates(buf: []const u8, pos: *usize) bool {
    const candidate_count = readKafkaArrayCount(buf, pos, true) orelse return false;
    for (0..candidate_count) |_| {
        if (!skipFixedBytes(buf, pos, 20)) return false; // candidate_id + candidate_directory_id
        ser.skipTaggedFields(buf, pos) catch return false;
    }
    return true;
}

fn skipQuorumLeaderEndpoints(buf: []const u8, pos: *usize) bool {
    const endpoint_count = readKafkaArrayCount(buf, pos, true) orelse return false;
    for (0..endpoint_count) |_| {
        if (!skipKafkaString(buf, pos, true)) return false; // name
        if (!skipKafkaString(buf, pos, true)) return false; // host
        if (!skipFixedBytes(buf, pos, 2)) return false; // port
        ser.skipTaggedFields(buf, pos) catch return false;
    }
    return true;
}

fn skipKafkaI32Array(buf: []const u8, pos: *usize, flexible: bool) bool {
    const item_count = readKafkaArrayCount(buf, pos, flexible) orelse return false;
    if (item_count > (buf.len - pos.*) / 4) return false;
    pos.* += item_count * 4;
    return true;
}

fn readKafkaArrayCount(buf: []const u8, pos: *usize, flexible: bool) ?usize {
    const item_count = if (flexible)
        (ser.readCompactArrayLen(buf, pos) catch return null) orelse 0
    else
        readLegacyArrayCount(buf, pos) orelse return null;
    if (pos.* > buf.len or item_count > buf.len - pos.* + 1) return null;
    return item_count;
}

fn readLegacyArrayCount(buf: []const u8, pos: *usize) ?usize {
    if (pos.* > buf.len or 4 > buf.len - pos.*) return null;
    const len = std.mem.readInt(i32, buf[pos.*..][0..4], .big);
    pos.* += 4;
    if (len < 0) return 0;
    return @intCast(len);
}

fn skipKafkaString(buf: []const u8, pos: *usize, flexible: bool) bool {
    if (flexible) {
        _ = ser.readCompactString(buf, pos) catch return false;
        return true;
    }

    if (pos.* > buf.len or 2 > buf.len - pos.*) return false;
    const len = std.mem.readInt(i16, buf[pos.*..][0..2], .big);
    pos.* += 2;
    if (len < 0) return true;
    const string_len: usize = @intCast(len);
    if (string_len > buf.len - pos.*) return false;
    pos.* += string_len;
    return true;
}

fn skipFixedBytes(buf: []const u8, pos: *usize, len: usize) bool {
    if (pos.* > buf.len or len > buf.len - pos.*) return false;
    pos.* += len;
    return true;
}

fn readRecordI32(data: []const u8, pos: *usize) !i32 {
    if (pos.* + 4 > data.len) return error.InvalidAutoMqMetadataRecord;
    const value = std.mem.readInt(i32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return value;
}

fn readRecordI64(data: []const u8, pos: *usize) !i64 {
    if (pos.* + 8 > data.len) return error.InvalidAutoMqMetadataRecord;
    const value = std.mem.readInt(i64, data[pos.*..][0..8], .big);
    pos.* += 8;
    return value;
}

fn writeRecordI32(buf: []u8, pos: *usize, value: i32) void {
    std.mem.writeInt(i32, buf[pos.*..][0..4], value, .big);
    pos.* += 4;
}

fn writeRecordI64(buf: []u8, pos: *usize, value: i64) void {
    std.mem.writeInt(i64, buf[pos.*..][0..8], value, .big);
    pos.* += 8;
}

fn writeRecordU32(buf: []u8, pos: *usize, value: u32) void {
    std.mem.writeInt(u32, buf[pos.*..][0..4], value, .big);
    pos.* += 4;
}

fn writeRecordBool(buf: []u8, pos: *usize, value: bool) void {
    buf[pos.*] = if (value) 1 else 0;
    pos.* += 1;
}

fn writeRecordBytes(buf: []u8, pos: *usize, bytes: []const u8) !void {
    if (bytes.len > std.math.maxInt(u32)) return error.RecordTooLarge;
    std.mem.writeInt(u32, buf[pos.*..][0..4], @intCast(bytes.len), .big);
    pos.* += 4;
    @memcpy(buf[pos.* .. pos.* + bytes.len], bytes);
    pos.* += bytes.len;
}

fn writeRecordOptionalBytes(buf: []u8, pos: *usize, bytes: ?[]const u8) !void {
    writeRecordBool(buf, pos, bytes != null);
    if (bytes) |value| try writeRecordBytes(buf, pos, value);
}

fn writeRecordLogDirs(buf: []u8, pos: *usize, log_dirs: []const [16]u8) !void {
    if (log_dirs.len > std.math.maxInt(u32)) return error.RecordTooLarge;
    writeRecordU32(buf, pos, @intCast(log_dirs.len));
    for (log_dirs) |dir| {
        @memcpy(buf[pos.* .. pos.* + 16], dir[0..]);
        pos.* += 16;
    }
}

fn readRecordBool(data: []const u8, pos: *usize) !bool {
    if (pos.* + 1 > data.len) return error.InvalidAutoMqMetadataRecord;
    const byte = data[pos.*];
    pos.* += 1;
    return switch (byte) {
        0 => false,
        1 => true,
        else => error.InvalidAutoMqMetadataRecord,
    };
}

fn readRecordBytes(data: []const u8, pos: *usize) ![]const u8 {
    if (pos.* + 4 > data.len) return error.InvalidAutoMqMetadataRecord;
    const len: usize = @intCast(std.mem.readInt(u32, data[pos.*..][0..4], .big));
    pos.* += 4;
    if (pos.* + len > data.len) return error.InvalidAutoMqMetadataRecord;
    const bytes = data[pos.* .. pos.* + len];
    pos.* += len;
    return bytes;
}

fn readRecordOptionalBytes(data: []const u8, pos: *usize) !?[]const u8 {
    const has_bytes = try readRecordBool(data, pos);
    return if (has_bytes) try readRecordBytes(data, pos) else null;
}

fn readRecordU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidAutoMqMetadataRecord;
    const value = std.mem.readInt(u32, data[pos.*..][0..4], .big);
    pos.* += 4;
    return value;
}

fn skipRecordLogDirs(data: []const u8, pos: *usize) !void {
    const count = try readRecordU32(data, pos);
    const byte_len = std.math.mul(usize, @as(usize, @intCast(count)), 16) catch return error.InvalidAutoMqMetadataRecord;
    if (pos.* > data.len or byte_len > data.len - pos.*) return error.InvalidAutoMqMetadataRecord;
    pos.* += byte_len;
}

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

fn makeTestControllerLeader(ctrl: *Controller) !void {
    try ctrl.raft_state.addVoter(ctrl.node_id);
    _ = ctrl.raft_state.startElection();
    ctrl.raft_state.becomeLeader();
}

fn appendInternalRaftAppendEntriesPayload(
    buf: []u8,
    pos: *usize,
    leader_id: i32,
    leader_epoch: i32,
    prev_log_offset: i64,
    prev_log_epoch: i32,
    leader_commit: i64,
    entries_start_index: i64,
    records: []const []const u8,
) void {
    ser.writeString(buf, pos, "");
    ser.writeI32(buf, pos, 0);
    ser.writeI32(buf, pos, leader_id);
    ser.writeI32(buf, pos, leader_epoch);
    ser.writeI64(buf, pos, prev_log_offset);
    ser.writeI32(buf, pos, prev_log_epoch);
    ser.writeI64(buf, pos, leader_commit);
    ser.writeI64(buf, pos, entries_start_index);
    ser.writeI32(buf, pos, @intCast(records.len));
    for (records) |record| {
        ser.writeI32(buf, pos, @intCast(record.len));
        @memcpy(buf[pos.* .. pos.* + record.len], record);
        pos.* += record.len;
    }
}

fn freeDeserializedVoteResponse(resp: *const generated.vote_response.VoteResponse) void {
    for (resp.topics) |topic| {
        if (topic.partitions.len > 0) testing.allocator.free(topic.partitions);
    }
    if (resp.node_endpoints.len > 0) testing.allocator.free(resp.node_endpoints);
    if (resp.topics.len > 0) testing.allocator.free(resp.topics);
}

fn freeDeserializedFetchSnapshotResponse(resp: *const generated.fetch_snapshot_response.FetchSnapshotResponse) void {
    for (resp.topics) |topic| {
        if (topic.partitions.len > 0) testing.allocator.free(topic.partitions);
    }
    if (resp.node_endpoints.len > 0) testing.allocator.free(resp.node_endpoints);
    if (resp.topics.len > 0) testing.allocator.free(resp.topics);
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

    // Array of supported APIs.
    const array_len = try ser.readArrayLen(response.?, &rpos);
    try testing.expectEqual(api_support.controller_supported_apis.len, array_len.?);
    var saw_update_raft_voter = false;
    for (0..array_len.?) |_| {
        const api_key = ser.readI16(response.?, &rpos);
        _ = ser.readI16(response.?, &rpos);
        _ = ser.readI16(response.?, &rpos);
        if (api_key == 82) saw_update_raft_voter = true;
    }
    try testing.expect(saw_update_raft_voter);
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

test "Controller handleRequest advertised APIs reject versions above catalog max before body decode" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    for (api_support.controller_supported_apis) |api| {
        if (api.key == 18) continue; // ApiVersions remains probeable for version negotiation.

        const unsupported_version = api.max + 1;
        const correlation_id: i32 = 910_000 + @as(i32, @intCast(api.key));
        var buf: [128]u8 = undefined;
        const req_len = buildTestRequest(
            &buf,
            api.key,
            unsupported_version,
            correlation_id,
            header_mod.requestHeaderVersion(api.key, unsupported_version),
        );

        const response = ctrl.handleRequest(buf[0..req_len]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(
            testing.allocator,
            response.?,
            &rpos,
            header_mod.responseHeaderVersion(api.key, unsupported_version),
        );
        defer resp_header.deinit(testing.allocator);

        try testing.expectEqual(correlation_id, resp_header.correlation_id);
        try testing.expectEqual(@as(i16, @intFromEnum(ErrorCode.unsupported_version)), ser.readI16(response.?, &rpos));
        try testing.expectEqual(response.?.len, rpos);
    }
}

test "Controller handleRequest Vote grants to valid candidate" {
    const Req = generated.vote_request.VoteRequest;
    const Resp = generated.vote_response.VoteResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Add voter 2 so the controller knows about candidate 2
    try ctrl.raft_state.addVoter(2);

    // Vote (52, v0) is flexible → request header v2, response header v1
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 52, 0, 10, 2);

    const partitions = [_]Req.TopicData.PartitionData{.{
        .partition_index = 0,
        .candidate_epoch = 1,
        .candidate_id = 2,
        .last_offset_epoch = 0,
        .last_offset = 0,
    }};
    const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .cluster_id = null, .topics = &topics };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(52, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 10), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedVoteResponse(&resp);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.topics.len);
    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions.len);
    try testing.expect(resp.topics[0].partitions[0].vote_granted);
}

test "Controller handleRequest Vote rejects stale epoch" {
    const Req = generated.vote_request.VoteRequest;
    const Resp = generated.vote_response.VoteResponse;

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

    const partitions = [_]Req.TopicData.PartitionData{.{
        .partition_index = 0,
        .candidate_epoch = 3,
        .candidate_id = 3,
        .last_offset_epoch = 0,
        .last_offset = 0,
    }};
    const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .cluster_id = null, .topics = &topics };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(52, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 11), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedVoteResponse(&resp);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.topics.len);
    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions.len);
    try testing.expect(!resp.topics[0].partitions[0].vote_granted);
}

test "Controller handleRequest Vote rejects malformed generated request" {
    const Resp = generated.vote_response.VoteResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [64]u8 = undefined;
    var pos = buildTestRequest(&buf, 52, 0, 12, 2);
    ser.writeCompactString(&buf, &pos, null); // cluster_id
    ser.writeCompactArrayLen(&buf, &pos, 1); // one topic declared, body truncated

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(52, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 12), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedVoteResponse(&resp);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
}

test "Controller handleRequest BeginQuorumEpoch accepts higher epoch" {
    const Req = generated.begin_quorum_epoch_request.BeginQuorumEpochRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Controller starts at epoch 0 (unattached)
    try testing.expectEqual(@as(i32, 0), ctrl.raft_state.current_epoch);

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 53, 0, 20, header_mod.requestHeaderVersion(53, 0));

    const partitions = [_]Req.TopicData.PartitionData{.{
        .partition_index = 0,
        .leader_id = 2,
        .leader_epoch = 5,
    }};
    const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .cluster_id = "", .topics = &topics };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Verify raft state was updated
    try testing.expectEqual(RaftState.Role.follower, ctrl.raft_state.role);
    try testing.expectEqual(@as(i32, 5), ctrl.raft_state.current_epoch);
    try testing.expectEqual(@as(i32, 2), ctrl.raft_state.leader_id.?);
}

test "Controller handleRequest BeginQuorumEpoch ignores stale epoch" {
    const Req = generated.begin_quorum_epoch_request.BeginQuorumEpochRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Put controller at epoch 5
    ctrl.raft_state.becomeFollower(5, 2);
    try testing.expectEqual(@as(i32, 5), ctrl.raft_state.current_epoch);

    // Send BeginQuorumEpoch with stale epoch 3
    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 53, 0, 21, header_mod.requestHeaderVersion(53, 0));

    const partitions = [_]Req.TopicData.PartitionData{.{
        .partition_index = 0,
        .leader_id = 3,
        .leader_epoch = 3,
    }};
    const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .cluster_id = "", .topics = &topics };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Raft state should NOT have changed — epoch still 5
    try testing.expectEqual(@as(i32, 5), ctrl.raft_state.current_epoch);
}

test "Controller handleRequest BeginQuorumEpoch applies internal AppendEntries payload" {
    const Req = generated.begin_quorum_epoch_request.BeginQuorumEpochRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 53, 0, 22, header_mod.requestHeaderVersion(53, 0));
    const req = Req{ .cluster_id = "", .topics = &.{} };
    req.serialize(&buf, &pos, 0);

    const record = "raft-entry";
    ser.writeI32(&buf, &pos, 2); // leader_id
    ser.writeI32(&buf, &pos, 1); // leader_epoch
    ser.writeI64(&buf, &pos, 0); // prev_log_offset
    ser.writeI32(&buf, &pos, 0); // prev_log_epoch
    ser.writeI64(&buf, &pos, 0); // leader_commit
    ser.writeI64(&buf, &pos, 0); // entries_start_index
    ser.writeI32(&buf, &pos, 1); // entry_count
    ser.writeI32(&buf, &pos, @intCast(record.len));
    @memcpy(buf[pos .. pos + record.len], record);
    pos += record.len;

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    try testing.expectEqual(RaftState.Role.follower, ctrl.raft_state.role);
    try testing.expectEqual(@as(i32, 1), ctrl.raft_state.current_epoch);
    try testing.expectEqual(@as(usize, 1), ctrl.raft_state.log.length());
    try testing.expectEqualStrings(record, ctrl.raft_state.log.entries.items[0].data);
}

test "Controller handleRequest DescribeQuorum returns quorum info" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Add self as voter and become leader
    try ctrl.raft_state.addVoter(1);
    _ = ctrl.raft_state.startElection();
    ctrl.raft_state.becomeLeader();

    // DescribeQuorum (55, v0) is flexible → header v2 / response v1
    const Req = generated.describe_quorum_request.DescribeQuorumRequest;
    const Resp = generated.describe_quorum_response.DescribeQuorumResponse;

    var buf: [256]u8 = undefined;
    var req_len = buildTestRequest(&buf, 55, 0, 30, 2);
    const req = Req{};
    req.serialize(&buf, &req_len, 0);

    const response = ctrl.handleRequest(buf[0..req_len]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    // Response header v1: correlation_id + tagged fields
    var rpos: usize = 0;
    const corr_id = ser.readI32(response.?, &rpos);
    try testing.expectEqual(@as(i32, 30), corr_id);
    _ = try ser.readUnsignedVarint(response.?, &rpos); // tagged fields

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer {
        for (resp.topics) |topic| {
            if (topic.partitions.len > 0) {
                for (topic.partitions) |partition| {
                    if (partition.current_voters.len > 0) testing.allocator.free(partition.current_voters);
                    if (partition.observers.len > 0) testing.allocator.free(partition.observers);
                }
                testing.allocator.free(topic.partitions);
            }
        }
        if (resp.topics.len > 0) testing.allocator.free(resp.topics);
    }
    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(i32, 1), resp.topics[0].partitions[0].leader_id);
}

test "Controller handleRequest DescribeQuorum v2 returns voter node endpoints" {
    const DescribeReq = generated.describe_quorum_request.DescribeQuorumRequest;
    const DescribeResp = generated.describe_quorum_response.DescribeQuorumResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const directory_id = [_]u8{7} ** 16;
    const endpoint_views = [_]RaftState.VoterEndpointView{.{
        .name = "CONTROLLER",
        .host = "controller-1.example",
        .port = 19093,
    }};
    _ = try ctrl.raft_state.proposeUpdateVoter(1, directory_id, &endpoint_views, 0, 1);
    ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
    ctrl.raft_state.applyCommittedConfigs();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 55, 2, 5502, header_mod.requestHeaderVersion(55, 2));
    const req = DescribeReq{};
    req.serialize(&buf, &pos, 2);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(55, 2));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 5502), resp_header.correlation_id);

    const resp = try DescribeResp.deserialize(testing.allocator, response.?, &rpos, 2);
    defer {
        for (resp.topics) |topic| {
            for (topic.partitions) |partition| {
                if (partition.current_voters.len > 0) testing.allocator.free(partition.current_voters);
                if (partition.observers.len > 0) testing.allocator.free(partition.observers);
            }
            if (topic.partitions.len > 0) testing.allocator.free(topic.partitions);
        }
        if (resp.topics.len > 0) testing.allocator.free(resp.topics);
        for (resp.nodes) |node| {
            if (node.listeners.len > 0) testing.allocator.free(node.listeners);
        }
        if (resp.nodes.len > 0) testing.allocator.free(resp.nodes);
    }

    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.nodes.len);
    try testing.expectEqual(@as(i32, 1), resp.nodes[0].node_id);
    try testing.expectEqual(@as(usize, 1), resp.nodes[0].listeners.len);
    try testing.expectEqualStrings("CONTROLLER", resp.nodes[0].listeners[0].name.?);
    try testing.expectEqualStrings("controller-1.example", resp.nodes[0].listeners[0].host.?);
    try testing.expectEqual(@as(u16, 19093), resp.nodes[0].listeners[0].port);
    try testing.expectEqualSlices(u8, &directory_id, &resp.topics[0].partitions[0].current_voters[0].replica_directory_id);
}

test "Controller handleRequest FetchSnapshot returns snapshot not found" {
    const Req = generated.fetch_snapshot_request.FetchSnapshotRequest;
    const Resp = generated.fetch_snapshot_response.FetchSnapshotResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    const snapshot_id = Req.TopicSnapshot.PartitionSnapshot.SnapshotId{ .end_offset = 3, .epoch = 1 };
    const partitions = [_]Req.TopicSnapshot.PartitionSnapshot{.{
        .partition = 0,
        .current_leader_epoch = -1,
        .snapshot_id = snapshot_id,
        .position = 0,
    }};
    const topics = [_]Req.TopicSnapshot{.{ .name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .replica_id = 2, .max_bytes = 1024, .topics = &topics };

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 59, 0, 5900, header_mod.requestHeaderVersion(59, 0));
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(59, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 5900), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedFetchSnapshotResponse(&resp);

    try testing.expectEqual(response.?.len, rpos);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.topics.len);
    try testing.expectEqualStrings("__cluster_metadata", resp.topics[0].name.?);
    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions.len);
    try testing.expectEqual(@as(i32, 0), resp.topics[0].partitions[0].index);
    try testing.expectEqual(ErrorCode.snapshot_not_found.toInt(), resp.topics[0].partitions[0].error_code);
    try testing.expectEqual(@as(i64, 3), resp.topics[0].partitions[0].snapshot_id.end_offset);
    try testing.expectEqual(@as(i32, 1), resp.topics[0].partitions[0].snapshot_id.epoch);
}

test "Controller handleRequest FetchSnapshot v1 returns current leader endpoint metadata" {
    const Req = generated.fetch_snapshot_request.FetchSnapshotRequest;
    const Resp = generated.fetch_snapshot_response.FetchSnapshotResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const directory_id = [_]u8{9} ** 16;
    const endpoint_views = [_]RaftState.VoterEndpointView{.{
        .name = "CONTROLLER",
        .host = "controller-1.example",
        .port = 19093,
    }};
    _ = try ctrl.raft_state.proposeUpdateVoter(1, directory_id, &endpoint_views, 0, 1);
    ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
    ctrl.raft_state.applyCommittedConfigs();

    const snapshot_id = Req.TopicSnapshot.PartitionSnapshot.SnapshotId{ .end_offset = 7, .epoch = 2 };
    const partitions = [_]Req.TopicSnapshot.PartitionSnapshot{.{
        .partition = 0,
        .current_leader_epoch = -1,
        .snapshot_id = snapshot_id,
        .position = 12,
        .replica_directory_id = [_]u8{4} ** 16,
    }};
    const topics = [_]Req.TopicSnapshot{.{ .name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .replica_id = 2, .max_bytes = 1024, .topics = &topics };

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 59, 1, 5902, header_mod.requestHeaderVersion(59, 1));
    req.serialize(&buf, &pos, 1);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(59, 1));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 5902), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 1);
    defer freeDeserializedFetchSnapshotResponse(&resp);

    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.topics.len);
    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions.len);
    const partition = resp.topics[0].partitions[0];
    try testing.expectEqual(ErrorCode.snapshot_not_found.toInt(), partition.error_code);
    try testing.expectEqual(@as(i64, 7), partition.snapshot_id.end_offset);
    try testing.expectEqual(@as(i32, 2), partition.snapshot_id.epoch);
    try testing.expectEqual(@as(i64, 12), partition.position);
    try testing.expectEqual(@as(i32, 1), partition.current_leader.leader_id);
    try testing.expectEqual(ctrl.raft_state.current_epoch, partition.current_leader.leader_epoch);
    try testing.expectEqual(@as(usize, 1), resp.node_endpoints.len);
    try testing.expectEqual(@as(i32, 1), resp.node_endpoints[0].node_id);
    try testing.expectEqualStrings("controller-1.example", resp.node_endpoints[0].host.?);
    try testing.expectEqual(@as(u16, 19093), resp.node_endpoints[0].port);
}

test "Controller handleRequest FetchSnapshot returns compacted controller snapshot bytes" {
    const Req = generated.fetch_snapshot_request.FetchSnapshotRequest;
    const Resp = generated.fetch_snapshot_response.FetchSnapshotResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const broker_epoch = ctrl.broker_registry.next_broker_epoch;
    const broker_record = try ctrl.buildBrokerRegistrationRecord(100, "host1", 9092, broker_epoch);
    defer ctrl.allocator.free(broker_record);
    const broker_offset = try ctrl.appendControllerMetadataRecord(broker_record);
    try ctrl.broker_registry.registerWithEpoch(100, "host1", 9092, broker_epoch, false);
    ctrl.last_applied_controller_metadata_offset = broker_offset;

    try ctrl.prepareControllerMetadataSnapshotForRaftCompaction();
    ctrl.raft_state.takeSnapshot();

    const snapshot_offset = ctrl.raft_state.last_snapshot_offset;
    const snapshot_epoch = ctrl.raft_state.last_snapshot_epoch;
    const snapshot_data = ctrl.raft_state.log.get(snapshot_offset).?.data;
    try testing.expect(snapshot_data.len > 10);

    const snapshot_id = Req.TopicSnapshot.PartitionSnapshot.SnapshotId{
        .end_offset = @intCast(snapshot_offset),
        .epoch = snapshot_epoch,
    };
    const partitions = [_]Req.TopicSnapshot.PartitionSnapshot{.{
        .partition = 0,
        .current_leader_epoch = -1,
        .snapshot_id = snapshot_id,
        .position = 2,
    }};
    const topics = [_]Req.TopicSnapshot{.{ .name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .replica_id = 2, .max_bytes = 7, .topics = &topics };

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 59, 0, 5903, header_mod.requestHeaderVersion(59, 0));
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(59, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 5903), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedFetchSnapshotResponse(&resp);

    try testing.expectEqual(response.?.len, rpos);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 1), resp.topics.len);
    try testing.expectEqual(@as(usize, 1), resp.topics[0].partitions.len);
    const partition = resp.topics[0].partitions[0];
    try testing.expectEqual(ErrorCode.none.toInt(), partition.error_code);
    try testing.expectEqual(@as(i64, @intCast(snapshot_offset)), partition.snapshot_id.end_offset);
    try testing.expectEqual(snapshot_epoch, partition.snapshot_id.epoch);
    try testing.expectEqual(@as(i64, @intCast(snapshot_data.len)), partition.size);
    try testing.expectEqual(@as(i64, 2), partition.position);
    try testing.expect(partition.unaligned_records != null);
    try testing.expectEqualSlices(u8, snapshot_data[2..9], partition.unaligned_records.?);
}

test "Controller handleRequest FetchSnapshot rejects positions beyond compacted snapshot" {
    const Req = generated.fetch_snapshot_request.FetchSnapshotRequest;
    const Resp = generated.fetch_snapshot_response.FetchSnapshotResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const broker_epoch = ctrl.broker_registry.next_broker_epoch;
    const broker_record = try ctrl.buildBrokerRegistrationRecord(100, "host1", 9092, broker_epoch);
    defer ctrl.allocator.free(broker_record);
    const broker_offset = try ctrl.appendControllerMetadataRecord(broker_record);
    try ctrl.broker_registry.registerWithEpoch(100, "host1", 9092, broker_epoch, false);
    ctrl.last_applied_controller_metadata_offset = broker_offset;

    try ctrl.prepareControllerMetadataSnapshotForRaftCompaction();
    ctrl.raft_state.takeSnapshot();

    const snapshot_offset = ctrl.raft_state.last_snapshot_offset;
    const snapshot_epoch = ctrl.raft_state.last_snapshot_epoch;
    const snapshot_data = ctrl.raft_state.log.get(snapshot_offset).?.data;
    const snapshot_id = Req.TopicSnapshot.PartitionSnapshot.SnapshotId{
        .end_offset = @intCast(snapshot_offset),
        .epoch = snapshot_epoch,
    };
    const partitions = [_]Req.TopicSnapshot.PartitionSnapshot{.{
        .partition = 0,
        .current_leader_epoch = -1,
        .snapshot_id = snapshot_id,
        .position = @as(i64, @intCast(snapshot_data.len)) + 1,
    }};
    const topics = [_]Req.TopicSnapshot{.{ .name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .replica_id = 2, .max_bytes = 1024, .topics = &topics };

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 59, 0, 5904, header_mod.requestHeaderVersion(59, 0));
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(59, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 5904), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedFetchSnapshotResponse(&resp);

    try testing.expectEqual(response.?.len, rpos);
    const partition = resp.topics[0].partitions[0];
    try testing.expectEqual(ErrorCode.position_out_of_range.toInt(), partition.error_code);
    try testing.expectEqual(@as(i64, @intCast(snapshot_data.len)), partition.size);
    try testing.expect(partition.unaligned_records == null);
}

test "Controller handleRequest FetchSnapshot rejects malformed generated request" {
    const Resp = generated.fetch_snapshot_response.FetchSnapshotResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 59, 0, 5901, header_mod.requestHeaderVersion(59, 0));
    ser.writeI32(&buf, &pos, 2); // replica_id
    ser.writeI32(&buf, &pos, 1024); // max_bytes
    ser.writeCompactArrayLen(&buf, &pos, 1); // one topic declared, body truncated

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(59, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 5901), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    defer freeDeserializedFetchSnapshotResponse(&resp);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 0), resp.topics.len);
}

test "Controller handleRequest BrokerRegistration registers broker" {
    const Req = generated.broker_registration_request.BrokerRegistrationRequest;
    const Resp = generated.broker_registration_response.BrokerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 62, 0, 40, header_mod.requestHeaderVersion(62, 0));
    const listeners = [_]Req.Listener{.{ .name = "PLAINTEXT", .host = "host1", .port = 9092, .security_protocol = 0 }};
    const req = Req{ .broker_id = 100, .cluster_id = "test-cluster", .listeners = &listeners };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(62, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 40), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expect(resp.broker_epoch > 0);

    // Verify broker is registered
    try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());
}

test "Controller handleRequest BrokerRegistration persists advertised log dirs" {
    const Req = generated.broker_registration_request.BrokerRegistrationRequest;
    const Resp = generated.broker_registration_response.BrokerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const dir_a = [_]u8{0x44} ** 16;
    const dir_b = [_]u8{0x55} ** 16;
    const log_dirs = [_][16]u8{ dir_a, dir_b };
    const listeners = [_]Req.Listener{.{ .name = "PLAINTEXT", .host = "host1", .port = 9092, .security_protocol = 0 }};
    const req = Req{ .broker_id = 100, .cluster_id = "test-cluster", .listeners = &listeners, .rack = "rack-a", .log_dirs = &log_dirs };

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 62, 2, 6202, header_mod.requestHeaderVersion(62, 2));
    req.serialize(&buf, &pos, 2);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(62, 2));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6202), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 2);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);

    const info = ctrl.broker_registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 2), info.log_dirs.len);
    try testing.expectEqualSlices(u8, dir_a[0..], info.log_dirs[0][0..]);
    try testing.expectEqualSlices(u8, dir_b[0..], info.log_dirs[1][0..]);
    try testing.expect(ctrl.broker_registry.hasLogDir(100, dir_a));
}

test "Controller handleRequest BrokerHeartbeat reports active broker" {
    const RegReq = generated.broker_registration_request.BrokerRegistrationRequest;
    const RegResp = generated.broker_registration_response.BrokerRegistrationResponse;
    const HbReq = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
    const HbResp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    // First, register a broker to get the epoch
    var reg_buf: [256]u8 = undefined;
    var reg_pos = buildTestRequest(&reg_buf, 62, 0, 40, header_mod.requestHeaderVersion(62, 0));
    const listeners = [_]RegReq.Listener{.{ .name = "PLAINTEXT", .host = "host1", .port = 9092, .security_protocol = 0 }};
    const reg_req = RegReq{ .broker_id = 100, .cluster_id = "test-cluster", .listeners = &listeners };
    reg_req.serialize(&reg_buf, &reg_pos, 0);

    const reg_response = ctrl.handleRequest(reg_buf[0..reg_pos]);
    try testing.expect(reg_response != null);
    defer testing.allocator.free(reg_response.?);

    // Parse broker_epoch from registration response
    var reg_rpos: usize = 0;
    var reg_header = try ResponseHeader.deserialize(testing.allocator, reg_response.?, &reg_rpos, header_mod.responseHeaderVersion(62, 0));
    defer reg_header.deinit(testing.allocator);
    const reg_resp = try RegResp.deserialize(testing.allocator, reg_response.?, &reg_rpos, 0);
    const broker_epoch = reg_resp.broker_epoch;

    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 0, 50, header_mod.requestHeaderVersion(63, 0));
    const hb_req = HbReq{ .broker_id = 100, .broker_epoch = broker_epoch };
    hb_req.serialize(&hb_buf, &hb_pos, 0);

    const hb_response = ctrl.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    var hb_rpos: usize = 0;
    var hb_header = try ResponseHeader.deserialize(testing.allocator, hb_response.?, &hb_rpos, header_mod.responseHeaderVersion(63, 0));
    defer hb_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 50), hb_header.correlation_id);

    const hb_resp = try HbResp.deserialize(testing.allocator, hb_response.?, &hb_rpos, 0);
    try testing.expectEqual(@as(i16, 0), hb_resp.error_code);
    try testing.expect(!hb_resp.is_fenced);
}

test "Controller handleRequest BrokerHeartbeat reports unknown broker for re-registration" {
    const HbReq = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
    const HbResp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 0, 51, header_mod.requestHeaderVersion(63, 0));
    const hb_req = HbReq{ .broker_id = 100, .broker_epoch = 1 };
    hb_req.serialize(&hb_buf, &hb_pos, 0);

    const hb_response = ctrl.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    var hb_rpos: usize = 0;
    var hb_header = try ResponseHeader.deserialize(testing.allocator, hb_response.?, &hb_rpos, header_mod.responseHeaderVersion(63, 0));
    defer hb_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 51), hb_header.correlation_id);

    const hb_resp = try HbResp.deserialize(testing.allocator, hb_response.?, &hb_rpos, 0);
    try testing.expectEqual(ErrorCode.broker_id_not_registered.toInt(), hb_resp.error_code);
    try testing.expect(hb_resp.is_fenced);
}

test "Controller handleRequest BrokerHeartbeat reports stale broker epoch" {
    const HbReq = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
    const HbResp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const epoch = try ctrl.broker_registry.register(100, "host1", 9092);

    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 0, 52, header_mod.requestHeaderVersion(63, 0));
    const hb_req = HbReq{ .broker_id = 100, .broker_epoch = epoch + 1 };
    hb_req.serialize(&hb_buf, &hb_pos, 0);

    const hb_response = ctrl.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    var hb_rpos: usize = 0;
    var hb_header = try ResponseHeader.deserialize(testing.allocator, hb_response.?, &hb_rpos, header_mod.responseHeaderVersion(63, 0));
    defer hb_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 52), hb_header.correlation_id);

    const hb_resp = try HbResp.deserialize(testing.allocator, hb_response.?, &hb_rpos, 0);
    try testing.expectEqual(ErrorCode.stale_broker_epoch.toInt(), hb_resp.error_code);
    try testing.expect(hb_resp.is_fenced);
}

test "Controller handleRequest BrokerHeartbeat v1 records offline log dirs" {
    const HbReq = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
    const HbResp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const dir = [_]u8{0x63} ** 16;
    const dirs = [_][16]u8{dir};
    try ctrl.broker_registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, null, &dirs, 7, false);

    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 1, 6310, header_mod.requestHeaderVersion(63, 1));
    const hb_req = HbReq{ .broker_id = 100, .broker_epoch = 7, .want_shut_down = true, .offline_log_dirs = &dirs };
    hb_req.serialize(&hb_buf, &hb_pos, 1);

    const hb_response = ctrl.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    var hb_rpos: usize = 0;
    var hb_header = try ResponseHeader.deserialize(testing.allocator, hb_response.?, &hb_rpos, header_mod.responseHeaderVersion(63, 1));
    defer hb_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6310), hb_header.correlation_id);

    const hb_resp = try HbResp.deserialize(testing.allocator, hb_response.?, &hb_rpos, 1);
    try testing.expectEqual(ErrorCode.none.toInt(), hb_resp.error_code);
    try testing.expect(hb_resp.is_fenced);
    try testing.expect(!hb_resp.is_caught_up);
    try testing.expect(hb_resp.should_shut_down);

    const info = ctrl.broker_registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 1), info.offline_log_dirs.len);
    try testing.expectEqualSlices(u8, dir[0..], info.offline_log_dirs[0][0..]);
}

test "Controller handleRequest BrokerHeartbeat v1 rejects unknown offline log dir" {
    const HbReq = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
    const HbResp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const dir = [_]u8{0x64} ** 16;
    const unknown_dir = [_]u8{0x65} ** 16;
    const dirs = [_][16]u8{dir};
    const unknown_dirs = [_][16]u8{unknown_dir};
    try ctrl.broker_registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, null, &dirs, 7, false);

    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 1, 6311, header_mod.requestHeaderVersion(63, 1));
    const hb_req = HbReq{ .broker_id = 100, .broker_epoch = 7, .offline_log_dirs = &unknown_dirs };
    hb_req.serialize(&hb_buf, &hb_pos, 1);

    const hb_response = ctrl.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    var hb_rpos: usize = 0;
    var hb_header = try ResponseHeader.deserialize(testing.allocator, hb_response.?, &hb_rpos, header_mod.responseHeaderVersion(63, 1));
    defer hb_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6311), hb_header.correlation_id);

    const hb_resp = try HbResp.deserialize(testing.allocator, hb_response.?, &hb_rpos, 1);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), hb_resp.error_code);
    try testing.expect(hb_resp.is_fenced);

    const info = ctrl.broker_registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 0), info.offline_log_dirs.len);
}

test "Controller replays durable broker offline log-dir status" {
    var ctrl = Controller.init(testing.allocator, 1, "controller-offline-log-dir-replay");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const broker_epoch = ctrl.broker_registry.next_broker_epoch;
    const dir = [_]u8{0x66} ** 16;
    const dirs = [_][16]u8{dir};
    const registration = try ctrl.buildBrokerRegistrationRecordWithRackAndLogDirs(100, "host1", 9092, null, &dirs, broker_epoch);
    defer ctrl.allocator.free(registration);
    var offset = try ctrl.appendControllerMetadataRecord(registration);
    try ctrl.broker_registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, null, &dirs, broker_epoch, false);
    ctrl.last_applied_controller_metadata_offset = offset;

    const status = try ctrl.buildBrokerLogDirStatusRecord(100, broker_epoch, true, &dirs);
    defer ctrl.allocator.free(status);
    offset = try ctrl.appendControllerMetadataRecord(status);
    try ctrl.broker_registry.installLogDirStatus(100, broker_epoch, true, &dirs);
    ctrl.last_applied_controller_metadata_offset = offset;

    try testing.expectEqual(@as(usize, 2), try ctrl.replayCommittedControllerMetadataRecords());
    const info = ctrl.broker_registry.brokers.get(100) orelse return error.TestUnexpectedResult;
    try testing.expect(info.fenced);
    try testing.expectEqual(@as(usize, 1), info.offline_log_dirs.len);
    try testing.expectEqualSlices(u8, dir[0..], info.offline_log_dirs[0][0..]);
}

test "Controller handleRequest UnregisterBroker removes broker" {
    const Req = generated.unregister_broker_request.UnregisterBrokerRequest;
    const Resp = generated.unregister_broker_response.UnregisterBrokerResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);
    try ctrl.broker_registry.registerWithEpoch(100, "host1", 9092, 7, false);

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 64, 0, 6400, header_mod.requestHeaderVersion(64, 0));
    const req = Req{ .broker_id = 100 };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(64, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6400), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(response.?.len, rpos);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 0), ctrl.broker_registry.count());
}

test "Controller handleRequest UnregisterBroker reports missing broker" {
    const Req = generated.unregister_broker_request.UnregisterBrokerRequest;
    const Resp = generated.unregister_broker_response.UnregisterBrokerResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 64, 0, 6401, header_mod.requestHeaderVersion(64, 0));
    const req = Req{ .broker_id = 404 };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(64, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6401), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.broker_id_not_registered.toInt(), resp.error_code);
}

test "Controller handleRequest UnregisterBroker rejects malformed request" {
    const Resp = generated.unregister_broker_response.UnregisterBrokerResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    var buf: [128]u8 = undefined;
    const pos = buildTestRequest(&buf, 64, 0, 6402, header_mod.requestHeaderVersion(64, 0));

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(64, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 6402), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
}

test "Controller applies broker unregistration metadata record" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    try ctrl.broker_registry.registerWithEpoch(100, "host1", 9092, 7, false);
    const record = try ctrl.buildBrokerUnregistrationRecord(100);
    defer ctrl.allocator.free(record);

    try ctrl.applyControllerMetadataRecord(record);
    try testing.expectEqual(@as(usize, 0), ctrl.broker_registry.count());
}

test "Controller replays replicated broker registration after follower promotion" {
    const AppendResp = generated.begin_quorum_epoch_response.BeginQuorumEpochResponse;
    const HbReq = generated.broker_heartbeat_request.BrokerHeartbeatRequest;
    const HbResp = generated.broker_heartbeat_response.BrokerHeartbeatResponse;

    var leader = Controller.init(testing.allocator, 1, "controller-metadata-replication");
    defer leader.deinit();
    try makeTestControllerLeader(&leader);

    const broker_epoch = leader.broker_registry.next_broker_epoch;
    const record = try leader.buildBrokerRegistrationRecord(100, "host1", 9092, broker_epoch);
    defer leader.allocator.free(record);
    const offset = try leader.appendControllerMetadataRecord(record);
    try leader.broker_registry.registerWithEpoch(100, "host1", 9092, broker_epoch, true);
    leader.last_applied_controller_metadata_offset = offset;

    var follower = Controller.init(testing.allocator, 2, "controller-metadata-replication");
    defer follower.deinit();
    try follower.raft_state.addVoter(1);
    try follower.raft_state.addVoter(2);
    follower.raft_state.becomeFollower(leader.raft_state.current_epoch, 1);

    var append_buf: [1024]u8 = undefined;
    var append_pos = buildTestRequest(&append_buf, 53, 0, 5308, header_mod.requestHeaderVersion(53, 0));
    const records = [_][]const u8{record};
    appendInternalRaftAppendEntriesPayload(&append_buf, &append_pos, 1, leader.raft_state.current_epoch, 0, 0, @intCast(offset), 0, &records);

    const append_response = follower.handleRequest(append_buf[0..append_pos]);
    try testing.expect(append_response != null);
    defer testing.allocator.free(append_response.?);

    var append_rpos: usize = 0;
    var append_header = try ResponseHeader.deserialize(testing.allocator, append_response.?, &append_rpos, header_mod.responseHeaderVersion(53, 0));
    defer append_header.deinit(testing.allocator);
    const append_resp = try AppendResp.deserialize(testing.allocator, append_response.?, &append_rpos, 0);
    defer if (append_resp.topics.len > 0) testing.allocator.free(append_resp.topics);
    try testing.expectEqual(ErrorCode.none.toInt(), append_resp.error_code);
    try testing.expectEqual(@as(usize, 1), follower.broker_registry.count());

    _ = follower.raft_state.startElection();
    follower.raft_state.becomeLeader();

    var hb_buf: [256]u8 = undefined;
    var hb_pos = buildTestRequest(&hb_buf, 63, 0, 5309, header_mod.requestHeaderVersion(63, 0));
    const hb_req = HbReq{ .broker_id = 100, .broker_epoch = broker_epoch };
    hb_req.serialize(&hb_buf, &hb_pos, 0);

    const hb_response = follower.handleRequest(hb_buf[0..hb_pos]);
    try testing.expect(hb_response != null);
    defer testing.allocator.free(hb_response.?);

    var hb_rpos: usize = 0;
    var hb_header = try ResponseHeader.deserialize(testing.allocator, hb_response.?, &hb_rpos, header_mod.responseHeaderVersion(63, 0));
    defer hb_header.deinit(testing.allocator);
    const hb_resp = try HbResp.deserialize(testing.allocator, hb_response.?, &hb_rpos, 0);
    try testing.expectEqual(ErrorCode.none.toInt(), hb_resp.error_code);
    try testing.expect(!hb_resp.is_fenced);
}

test "Controller initWithDataDir replays durable broker registrations" {
    const Req = generated.broker_registration_request.BrokerRegistrationRequest;

    const tmp_dir = "/tmp/zmq-controller-metadata-replay-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var ctrl = Controller.initWithDataDir(testing.allocator, 1, "controller-metadata-restart", tmp_dir);
        defer ctrl.deinit();
        try makeTestControllerLeader(&ctrl);

        var buf: [512]u8 = undefined;
        var pos = buildTestRequest(&buf, 62, 2, 5310, header_mod.requestHeaderVersion(62, 2));
        const log_dir = [_]u8{0x62} ** 16;
        const log_dirs = [_][16]u8{log_dir};
        const listeners = [_]Req.Listener{.{ .name = "PLAINTEXT", .host = "host1", .port = 9092, .security_protocol = 0 }};
        const req = Req{ .broker_id = 100, .cluster_id = "controller-metadata-restart", .listeners = &listeners, .rack = "rack-a", .log_dirs = &log_dirs };
        req.serialize(&buf, &pos, 2);

        const response = ctrl.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        testing.allocator.free(response.?);
        try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());
    }

    {
        var ctrl = Controller.initWithDataDir(testing.allocator, 1, "controller-metadata-restart", tmp_dir);
        defer ctrl.deinit();

        const recovered = try ctrl.raft_state.loadPersistedLog();
        try testing.expectEqual(@as(u64, 1), recovered);
        ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
        try testing.expectEqual(@as(usize, 1), try ctrl.replayCommittedControllerMetadataRecords());
        try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());

        const info = ctrl.broker_registry.brokers.get(100).?;
        try testing.expectEqual(@as(i64, 1), info.broker_epoch);
        try testing.expectEqualStrings("host1", info.host);
        try testing.expectEqual(@as(u16, 9092), info.port);
        try testing.expectEqualStrings("rack-a", info.rack orelse return error.TestUnexpectedResult);
        const recovered_log_dir = [_]u8{0x62} ** 16;
        try testing.expectEqual(@as(usize, 1), info.log_dirs.len);
        try testing.expectEqualSlices(u8, recovered_log_dir[0..], info.log_dirs[0][0..]);
    }
}

test "Controller tick evicts dead brokers" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    // Register a broker
    _ = try ctrl.broker_registry.register(100, "host1", 9092);
    try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());

    // Force the broker's last_heartbeat_ms to be 60 seconds in the past
    if (ctrl.broker_registry.brokers.getPtr(100)) |info| {
        info.last_heartbeat_ms = @import("time_compat").milliTimestamp() - 60_000;
    }

    // tick() calls evictExpired(30_000) — broker should be evicted
    ctrl.tick();
    try testing.expectEqual(@as(usize, 0), ctrl.broker_registry.count());
}

test "Controller handleRequest AllocateProducerIds returns block" {
    const Req = generated.allocate_producer_ids_request.AllocateProducerIdsRequest;
    const Resp = generated.allocate_producer_ids_response.AllocateProducerIdsResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 67, 0, 60, header_mod.requestHeaderVersion(67, 0));
    const req = Req{ .broker_id = 100, .broker_epoch = 1 };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(67, 0));
    defer resp_header.deinit(testing.allocator);
    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(@as(i16, 0), resp.error_code);
    try testing.expect(resp.producer_id_start >= 1000);
    try testing.expectEqual(@as(i32, 1000), resp.producer_id_len);
}

test "Controller initWithDataDir replays durable producer id allocations" {
    const Req = generated.allocate_producer_ids_request.AllocateProducerIdsRequest;
    const Resp = generated.allocate_producer_ids_response.AllocateProducerIdsResponse;

    const tmp_dir = "/tmp/zmq-controller-producer-id-replay-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var ctrl = Controller.initWithDataDir(testing.allocator, 1, "controller-producer-id-restart", tmp_dir);
        defer ctrl.deinit();
        try makeTestControllerLeader(&ctrl);

        var buf: [256]u8 = undefined;
        var pos = buildTestRequest(&buf, 67, 0, 6710, header_mod.requestHeaderVersion(67, 0));
        const req = Req{ .broker_id = 100, .broker_epoch = 1 };
        req.serialize(&buf, &pos, 0);

        const response = ctrl.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(67, 0));
        defer resp_header.deinit(testing.allocator);
        const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
        try testing.expectEqual(@as(i64, 1000), resp.producer_id_start);
        try testing.expectEqual(@as(i64, 2000), ctrl.next_producer_id);
    }

    {
        var ctrl = Controller.initWithDataDir(testing.allocator, 1, "controller-producer-id-restart", tmp_dir);
        defer ctrl.deinit();

        const recovered = try ctrl.raft_state.loadPersistedLog();
        try testing.expectEqual(@as(u64, 1), recovered);
        ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
        try testing.expectEqual(@as(usize, 1), try ctrl.replayCommittedControllerMetadataRecords());
        try testing.expectEqual(@as(i64, 2000), ctrl.next_producer_id);

        try makeTestControllerLeader(&ctrl);

        var buf: [256]u8 = undefined;
        var pos = buildTestRequest(&buf, 67, 0, 6711, header_mod.requestHeaderVersion(67, 0));
        const req = Req{ .broker_id = 100, .broker_epoch = 1 };
        req.serialize(&buf, &pos, 0);

        const response = ctrl.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(67, 0));
        defer resp_header.deinit(testing.allocator);
        const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
        try testing.expectEqual(@as(i64, 2000), resp.producer_id_start);
        try testing.expectEqual(@as(i64, 3000), ctrl.next_producer_id);
    }
}

test "Controller full snapshot survives Raft log compaction" {
    var ctrl = Controller.init(testing.allocator, 1, "controller-snapshot");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const broker_epoch = ctrl.broker_registry.next_broker_epoch;
    const log_dir = [_]u8{0x73} ** 16;
    const log_dirs = [_][16]u8{log_dir};
    const broker_record = try ctrl.buildBrokerRegistrationRecordWithRackAndLogDirs(100, "host1", 9092, "rack-a", &log_dirs, broker_epoch);
    defer ctrl.allocator.free(broker_record);
    var offset = try ctrl.appendControllerMetadataRecord(broker_record);
    try ctrl.broker_registry.registerWithEpochRackAndLogDirs(100, "host1", 9092, "rack-a", &log_dirs, broker_epoch, false);
    try ctrl.broker_registry.installLogDirStatus(100, broker_epoch, true, &log_dirs);
    ctrl.last_applied_controller_metadata_offset = offset;

    const pid_record = try ctrl.buildProducerIdAllocationRecord(2000);
    defer ctrl.allocator.free(pid_record);
    offset = try ctrl.appendControllerMetadataRecord(pid_record);
    ctrl.next_producer_id = 2000;
    ctrl.last_applied_controller_metadata_offset = offset;

    try ctrl.prepareControllerMetadataSnapshotForRaftCompaction();
    try testing.expectEqual(@as(usize, 3), ctrl.raft_state.log.length());
    try testing.expectEqual(@as(u64, 2), ctrl.raft_state.commit_index);

    ctrl.raft_state.takeSnapshot();
    try testing.expectEqual(@as(usize, 1), ctrl.raft_state.log.length());
    try testing.expectEqual(@as(u64, 2), ctrl.raft_state.log.entries.items[0].offset);

    try testing.expectEqual(@as(usize, 1), try ctrl.replayCommittedControllerMetadataRecords());
    try testing.expectEqual(@as(usize, 1), ctrl.broker_registry.count());
    try testing.expectEqual(@as(i64, 2000), ctrl.next_producer_id);
    const broker = ctrl.broker_registry.brokers.get(100).?;
    try testing.expectEqual(@as(i64, 1), broker.broker_epoch);
    try testing.expectEqualStrings("host1", broker.host);
    try testing.expectEqualStrings("rack-a", broker.rack orelse return error.TestUnexpectedResult);
    try testing.expectEqual(@as(usize, 1), broker.log_dirs.len);
    try testing.expectEqualSlices(u8, log_dir[0..], broker.log_dirs[0][0..]);
    try testing.expect(broker.fenced);
    try testing.expectEqual(@as(usize, 1), broker.offline_log_dirs.len);
    try testing.expectEqualSlices(u8, log_dir[0..], broker.offline_log_dirs[0][0..]);
}

test "Controller handleRequest ControllerRegistration accepts known voter" {
    const Req = generated.controller_registration_request.ControllerRegistrationRequest;
    const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "127.0.0.1",
        .port = 9093,
        .security_protocol = 0,
    }};
    const features = [_]Req.Feature{
        .{
            .name = "metadata.version",
            .min_supported_version = 1,
            .max_supported_version = 1,
        },
        .{
            .name = "kraft.version",
            .min_supported_version = 0,
            .max_supported_version = 1,
        },
    };

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 7000, header_mod.requestHeaderVersion(70, 0));
    const req = Req{
        .controller_id = 1,
        .listeners = &listeners,
        .features = &features,
    };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(70, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 7000), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);

    try testing.expect(ctrl.raft_state.pending_config_change);
    ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
    ctrl.raft_state.applyCommittedConfigs();

    const voter = ctrl.raft_state.voters.get(1).?;
    try testing.expectEqual(@as(i16, 0), voter.k_raft_min_supported_version);
    try testing.expectEqual(@as(i16, 1), voter.k_raft_max_supported_version);
    try testing.expectEqual(@as(usize, 1), voter.endpoints.len);
    try testing.expectEqualStrings("CONTROLLER", voter.endpoints[0].name);
    try testing.expectEqualStrings("127.0.0.1", voter.endpoints[0].host);
    try testing.expectEqual(@as(u16, 9093), voter.endpoints[0].port);
}

test "Controller handleRequest ControllerRegistration rejects non-leader" {
    const Req = generated.controller_registration_request.ControllerRegistrationRequest;
    const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try ctrl.raft_state.addVoter(1);

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "127.0.0.1",
        .port = 9093,
        .security_protocol = 0,
    }};

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 7001, header_mod.requestHeaderVersion(70, 0));
    const req = Req{ .controller_id = 1, .listeners = &listeners };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(70, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 7001), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.not_controller.toInt(), resp.error_code);
}

test "Controller handleRequest ControllerRegistration rejects unknown controller" {
    const Req = generated.controller_registration_request.ControllerRegistrationRequest;
    const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "127.0.0.1",
        .port = 9093,
        .security_protocol = 0,
    }};

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 7002, header_mod.requestHeaderVersion(70, 0));
    const req = Req{ .controller_id = 2, .listeners = &listeners };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(70, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 7002), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.unknown_controller_id.toInt(), resp.error_code);
}

test "Controller handleRequest ControllerRegistration rejects invalid feature ranges" {
    const Req = generated.controller_registration_request.ControllerRegistrationRequest;
    const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "127.0.0.1",
        .port = 9093,
        .security_protocol = 0,
    }};
    const features = [_]Req.Feature{.{
        .name = "kraft.version",
        .min_supported_version = 2,
        .max_supported_version = 1,
    }};

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 7004, header_mod.requestHeaderVersion(70, 0));
    const req = Req{
        .controller_id = 1,
        .listeners = &listeners,
        .features = &features,
    };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(70, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 7004), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_registration.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 0), ctrl.raft_state.log.length());
    try testing.expect(!ctrl.raft_state.pending_config_change);
}

test "Controller handleRequest ControllerRegistration rejects invalid listeners" {
    const Req = generated.controller_registration_request.ControllerRegistrationRequest;
    const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "",
        .port = 9093,
        .security_protocol = 0,
    }};

    var buf: [512]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 7005, header_mod.requestHeaderVersion(70, 0));
    const req = Req{
        .controller_id = 1,
        .listeners = &listeners,
    };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(70, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 7005), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_registration.toInt(), resp.error_code);
    try testing.expectEqual(@as(usize, 0), ctrl.raft_state.log.length());
    try testing.expect(!ctrl.raft_state.pending_config_change);
}

test "Controller handleRequest ControllerRegistration rejects malformed request" {
    const Resp = generated.controller_registration_response.ControllerRegistrationResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 7003, header_mod.requestHeaderVersion(70, 0));
    ser.writeI32(&buf, &pos, 1); // controller_id, missing incarnation_id and the rest

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(70, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 7003), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
}

test "Controller handleRequest AddRaftVoter uses generated key 80" {
    const Req = generated.add_raft_voter_request.AddRaftVoterRequest;
    const Resp = generated.add_raft_voter_response.AddRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try ctrl.raft_state.addVoter(1);
    _ = ctrl.raft_state.startElection();
    ctrl.raft_state.becomeLeader();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 80, 0, 80, header_mod.requestHeaderVersion(80, 0));
    const req = Req{ .cluster_id = "test-cluster", .timeout_ms = 1000, .voter_id = 2 };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(80, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 80), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expect(ctrl.raft_state.pending_config_change);
}

test "Controller handleRequest AddRaftVoter persists listener metadata after commit" {
    const Req = generated.add_raft_voter_request.AddRaftVoterRequest;
    const Resp = generated.add_raft_voter_response.AddRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const directory_id = [_]u8{8} ** 16;
    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "controller-2.example",
        .port = 29093,
    }};

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 80, 0, 8002, header_mod.requestHeaderVersion(80, 0));
    const req = Req{
        .cluster_id = "test-cluster",
        .timeout_ms = 1000,
        .voter_id = 2,
        .voter_directory_id = directory_id,
        .listeners = &listeners,
    };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(80, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 8002), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);

    ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
    ctrl.raft_state.applyCommittedConfigs();
    const voter = ctrl.raft_state.voters.get(2).?;
    try testing.expectEqualSlices(u8, &directory_id, &voter.voter_directory_id);
    try testing.expectEqual(@as(usize, 1), voter.endpoints.len);
    try testing.expectEqualStrings("CONTROLLER", voter.endpoints[0].name);
    try testing.expectEqualStrings("controller-2.example", voter.endpoints[0].host);
    try testing.expectEqual(@as(u16, 29093), voter.endpoints[0].port);
}

test "Controller handleRequest RemoveRaftVoter uses generated key 81" {
    const Req = generated.remove_raft_voter_request.RemoveRaftVoterRequest;
    const Resp = generated.remove_raft_voter_response.RemoveRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try ctrl.raft_state.addVoter(1);
    try ctrl.raft_state.addVoter(2);
    _ = ctrl.raft_state.startElection();
    ctrl.raft_state.becomeLeader();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 81, 0, 81, header_mod.requestHeaderVersion(81, 0));
    const req = Req{ .cluster_id = "test-cluster", .voter_id = 2 };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(81, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 81), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expect(ctrl.raft_state.pending_config_change);
}

test "Controller handleRequest AddRaftVoter rejects malformed request" {
    const Resp = generated.add_raft_voter_response.AddRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 80, 0, 8001, header_mod.requestHeaderVersion(80, 0));
    ser.writeCompactString(&buf, &pos, "test-cluster");
    ser.writeI32(&buf, &pos, 1000); // timeout_ms, missing voter_id and the rest

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(80, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 8001), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
}

test "Controller handleRequest RemoveRaftVoter rejects malformed request" {
    const Resp = generated.remove_raft_voter_response.RemoveRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 81, 0, 8101, header_mod.requestHeaderVersion(81, 0));
    ser.writeCompactString(&buf, &pos, "test-cluster");
    ser.writeI32(&buf, &pos, 2); // voter_id, missing voter_directory_id and tags

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(81, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 8101), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
}

test "Controller handleRequest UpdateRaftVoter proposes endpoint update" {
    const Req = generated.update_raft_voter_request.UpdateRaftVoterRequest;
    const Resp = generated.update_raft_voter_response.UpdateRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "127.0.0.1",
        .port = 9093,
    }};

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 82, 0, 8200, header_mod.requestHeaderVersion(82, 0));
    const req = Req{
        .cluster_id = "test-cluster",
        .voter_id = 1,
        .listeners = &listeners,
        .k_raft_version_feature = .{ .min_supported_version = 0, .max_supported_version = 0 },
    };
    req.serialize(&buf, &pos, 0);

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(82, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 8200), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.none.toInt(), resp.error_code);
    try testing.expect(ctrl.raft_state.pending_config_change);

    ctrl.raft_state.commit_index = ctrl.raft_state.log.lastOffset();
    ctrl.raft_state.applyCommittedConfigs();
    const voter = ctrl.raft_state.voters.get(1).?;
    try testing.expectEqual(@as(usize, 1), voter.endpoints.len);
    try testing.expectEqualStrings("CONTROLLER", voter.endpoints[0].name);
    try testing.expectEqualStrings("127.0.0.1", voter.endpoints[0].host);
    try testing.expectEqual(@as(u16, 9093), voter.endpoints[0].port);
}

test "Controller handleRequest UpdateRaftVoter rejects non-leader unknown voter and invalid feature" {
    const Req = generated.update_raft_voter_request.UpdateRaftVoterRequest;
    const Resp = generated.update_raft_voter_response.UpdateRaftVoterResponse;

    const listeners = [_]Req.Listener{.{
        .name = "CONTROLLER",
        .host = "127.0.0.1",
        .port = 9093,
    }};

    {
        var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
        defer ctrl.deinit();
        try ctrl.raft_state.addVoter(1);

        var buf: [256]u8 = undefined;
        var pos = buildTestRequest(&buf, 82, 0, 8202, header_mod.requestHeaderVersion(82, 0));
        const req = Req{
            .cluster_id = "test-cluster",
            .voter_id = 1,
            .listeners = &listeners,
            .k_raft_version_feature = .{ .min_supported_version = 0, .max_supported_version = 0 },
        };
        req.serialize(&buf, &pos, 0);

        const response = ctrl.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(82, 0));
        defer resp_header.deinit(testing.allocator);
        try testing.expectEqual(@as(i32, 8202), resp_header.correlation_id);
        const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(ErrorCode.not_controller.toInt(), resp.error_code);
    }

    {
        var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
        defer ctrl.deinit();
        try makeTestControllerLeader(&ctrl);

        var buf: [256]u8 = undefined;
        var pos = buildTestRequest(&buf, 82, 0, 8203, header_mod.requestHeaderVersion(82, 0));
        const req = Req{
            .cluster_id = "test-cluster",
            .voter_id = 2,
            .listeners = &listeners,
            .k_raft_version_feature = .{ .min_supported_version = 0, .max_supported_version = 0 },
        };
        req.serialize(&buf, &pos, 0);

        const response = ctrl.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(82, 0));
        defer resp_header.deinit(testing.allocator);
        try testing.expectEqual(@as(i32, 8203), resp_header.correlation_id);
        const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(ErrorCode.resource_not_found.toInt(), resp.error_code);
    }

    {
        var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
        defer ctrl.deinit();
        try makeTestControllerLeader(&ctrl);

        var buf: [256]u8 = undefined;
        var pos = buildTestRequest(&buf, 82, 0, 8204, header_mod.requestHeaderVersion(82, 0));
        const req = Req{
            .cluster_id = "test-cluster",
            .voter_id = 1,
            .listeners = &listeners,
            .k_raft_version_feature = .{ .min_supported_version = 2, .max_supported_version = 1 },
        };
        req.serialize(&buf, &pos, 0);

        const response = ctrl.handleRequest(buf[0..pos]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(82, 0));
        defer resp_header.deinit(testing.allocator);
        try testing.expectEqual(@as(i32, 8204), resp_header.correlation_id);
        const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
        try testing.expectEqual(ErrorCode.invalid_update_version.toInt(), resp.error_code);
    }
}

test "Controller handleRequest UpdateRaftVoter rejects malformed request" {
    const Resp = generated.update_raft_voter_response.UpdateRaftVoterResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 82, 0, 8201, header_mod.requestHeaderVersion(82, 0));
    ser.writeCompactString(&buf, &pos, "test-cluster");
    ser.writeI32(&buf, &pos, 1); // voter_id, missing voter_directory_id and the rest

    const response = ctrl.handleRequest(buf[0..pos]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(82, 0));
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 8201), resp_header.correlation_id);

    const resp = try Resp.deserialize(testing.allocator, response.?, &rpos, 0);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), resp.error_code);
}

test "Controller rejects telemetry keys 71 and 72 instead of treating them as Raft voter APIs" {
    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    const cases = [_]i16{ 71, 72 };
    for (cases, 0..) |api_key, i| {
        var buf: [128]u8 = undefined;
        const corr: i32 = 7100 + @as(i32, @intCast(i));
        const req_len = buildTestRequest(&buf, api_key, 0, corr, header_mod.requestHeaderVersion(api_key, 0));

        const response = ctrl.handleRequest(buf[0..req_len]);
        try testing.expect(response != null);
        defer testing.allocator.free(response.?);

        var rpos: usize = 0;
        var resp_header = try ResponseHeader.deserialize(testing.allocator, response.?, &rpos, header_mod.responseHeaderVersion(api_key, 0));
        defer resp_header.deinit(testing.allocator);
        try testing.expectEqual(corr, resp_header.correlation_id);
        try testing.expectEqual(@as(i16, 35), ser.readI16(response.?, &rpos));
    }
}

test "Controller AllocateProducerIds increments monotonically" {
    const Req = generated.allocate_producer_ids_request.AllocateProducerIdsRequest;
    const Resp = generated.allocate_producer_ids_response.AllocateProducerIdsResponse;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try makeTestControllerLeader(&ctrl);

    // First allocation
    var buf1: [256]u8 = undefined;
    var pos1 = buildTestRequest(&buf1, 67, 0, 61, header_mod.requestHeaderVersion(67, 0));
    const req1 = Req{ .broker_id = 100, .broker_epoch = 1 };
    req1.serialize(&buf1, &pos1, 0);
    const resp1 = ctrl.handleRequest(buf1[0..pos1]);
    try testing.expect(resp1 != null);
    defer testing.allocator.free(resp1.?);

    var rpos1: usize = 0;
    var header1 = try ResponseHeader.deserialize(testing.allocator, resp1.?, &rpos1, header_mod.responseHeaderVersion(67, 0));
    defer header1.deinit(testing.allocator);
    const body1 = try Resp.deserialize(testing.allocator, resp1.?, &rpos1, 0);
    const start1 = body1.producer_id_start;

    // Second allocation
    var buf2: [256]u8 = undefined;
    var pos2 = buildTestRequest(&buf2, 67, 0, 62, header_mod.requestHeaderVersion(67, 0));
    const req2 = Req{ .broker_id = 100, .broker_epoch = 1 };
    req2.serialize(&buf2, &pos2, 0);
    const resp2 = ctrl.handleRequest(buf2[0..pos2]);
    try testing.expect(resp2 != null);
    defer testing.allocator.free(resp2.?);

    var rpos2: usize = 0;
    var header2 = try ResponseHeader.deserialize(testing.allocator, resp2.?, &rpos2, header_mod.responseHeaderVersion(67, 0));
    defer header2.deinit(testing.allocator);
    const body2 = try Resp.deserialize(testing.allocator, resp2.?, &rpos2, 0);
    const start2 = body2.producer_id_start;

    // Second block starts after first
    try testing.expect(start2 >= start1 + 1000);
}

/// Builds a valid request, appends a trailing byte beyond the schema-encoded
/// payload, and asserts the controller fails closed with the schema-shaped
/// `invalid_request` error code instead of accepting partial reads.
fn expectControllerTrailingByteRejected(
    ctrl: *Controller,
    buf: []u8,
    pos: usize,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    has_throttle_prefix: bool,
) !void {
    try testing.expect(pos < buf.len);
    buf[pos] = 0x7f; // arbitrary non-zero trailing byte
    const response = ctrl.handleRequest(buf[0 .. pos + 1]);
    try testing.expect(response != null);
    defer testing.allocator.free(response.?);

    var rpos: usize = 0;
    var resp_header = try ResponseHeader.deserialize(
        testing.allocator,
        response.?,
        &rpos,
        header_mod.responseHeaderVersion(api_key, api_version),
    );
    defer resp_header.deinit(testing.allocator);
    try testing.expectEqual(correlation_id, resp_header.correlation_id);

    if (has_throttle_prefix) {
        if (rpos + 4 > response.?.len) return error.TestUnexpectedResult;
        rpos += 4; // skip throttle_time_ms
    }
    if (rpos + 2 > response.?.len) return error.TestUnexpectedResult;
    const error_code = ser.readI16(response.?, &rpos);
    try testing.expectEqual(ErrorCode.invalid_request.toInt(), error_code);
}

test "Controller handleRequest Vote rejects trailing bytes" {
    const Req = generated.vote_request.VoteRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();
    try ctrl.raft_state.addVoter(2);

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 52, 0, 9100, 2);
    const partitions = [_]Req.TopicData.PartitionData{.{
        .partition_index = 0,
        .candidate_epoch = 1,
        .candidate_id = 2,
        .last_offset_epoch = 0,
        .last_offset = 0,
    }};
    const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .cluster_id = null, .topics = &topics };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 52, 0, 9100, false);
}

test "Controller handleRequest EndQuorumEpoch rejects trailing bytes" {
    const Req = generated.end_quorum_epoch_request.EndQuorumEpochRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 54, 0, 9101, 1);
    const partitions = [_]Req.TopicData.PartitionData{.{
        .partition_index = 0,
        .leader_id = 2,
        .leader_epoch = 1,
        .preferred_successors = &.{},
    }};
    const topics = [_]Req.TopicData{.{ .topic_name = "__cluster_metadata", .partitions = &partitions }};
    const req = Req{ .cluster_id = null, .topics = &topics };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 54, 0, 9101, false);
}

test "Controller handleRequest UnregisterBroker rejects trailing bytes" {
    const Req = generated.unregister_broker_request.UnregisterBrokerRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 64, 0, 9102, 2);
    const req = Req{ .broker_id = 100 };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 64, 0, 9102, true);
}

test "Controller handleRequest AddRaftVoter rejects trailing bytes" {
    const Req = generated.add_raft_voter_request.AddRaftVoterRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 80, 0, 9103, 2);
    const req = Req{
        .cluster_id = null,
        .timeout_ms = 1000,
        .voter_id = 3,
        .voter_directory_id = [_]u8{0} ** 16,
        .listeners = &.{},
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 80, 0, 9103, true);
}

test "Controller handleRequest RemoveRaftVoter rejects trailing bytes" {
    const Req = generated.remove_raft_voter_request.RemoveRaftVoterRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 81, 0, 9104, 2);
    const req = Req{
        .cluster_id = null,
        .voter_id = 3,
        .voter_directory_id = [_]u8{0} ** 16,
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 81, 0, 9104, true);
}

test "Controller handleRequest BrokerRegistration rejects trailing bytes" {
    const Req = generated.broker_registration_request.BrokerRegistrationRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 62, 0, 9105, 2);
    const req = Req{
        .broker_id = 100,
        .cluster_id = "test-cluster",
        .incarnation_id = [_]u8{0} ** 16,
        .listeners = &.{},
        .features = &.{},
        .rack = null,
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 62, 0, 9105, true);
}

test "Controller handleRequest BrokerHeartbeat rejects trailing bytes" {
    const Req = generated.broker_heartbeat_request.BrokerHeartbeatRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 63, 0, 9106, 2);
    const req = Req{
        .broker_id = 100,
        .broker_epoch = 1,
        .current_metadata_offset = 0,
        .want_fence = false,
        .want_shut_down = false,
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 63, 0, 9106, true);
}

test "Controller handleRequest AllocateProducerIds rejects trailing bytes" {
    const Req = generated.allocate_producer_ids_request.AllocateProducerIdsRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 67, 0, 9107, 2);
    const req = Req{ .broker_id = 1, .broker_epoch = 1 };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 67, 0, 9107, true);
}

test "Controller handleRequest DescribeQuorum rejects trailing bytes" {
    const Req = generated.describe_quorum_request.DescribeQuorumRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 55, 0, 9108, 2);
    const req = Req{ .topics = &.{} };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 55, 0, 9108, false);
}

test "Controller handleRequest ControllerRegistration rejects trailing bytes" {
    const Req = generated.controller_registration_request.ControllerRegistrationRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 70, 0, 9109, 2);
    const req = Req{
        .controller_id = 1,
        .incarnation_id = [_]u8{0} ** 16,
        .zk_migration_ready = false,
        .listeners = &.{},
        .features = &.{},
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 70, 0, 9109, true);
}

test "Controller handleRequest UpdateRaftVoter rejects trailing bytes" {
    const Req = generated.update_raft_voter_request.UpdateRaftVoterRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [256]u8 = undefined;
    var pos = buildTestRequest(&buf, 82, 0, 9110, 2);
    const req = Req{
        .cluster_id = null,
        .voter_id = 3,
        .voter_directory_id = [_]u8{0} ** 16,
        .listeners = &.{},
        .k_raft_version_feature = .{},
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 82, 0, 9110, true);
}

test "Controller handleRequest FetchSnapshot rejects trailing bytes" {
    const Req = generated.fetch_snapshot_request.FetchSnapshotRequest;

    var ctrl = Controller.init(testing.allocator, 1, "test-cluster");
    defer ctrl.deinit();

    var buf: [128]u8 = undefined;
    var pos = buildTestRequest(&buf, 59, 0, 9111, 2);
    const req = Req{
        .cluster_id = null,
        .replica_id = -1,
        .max_bytes = 1024,
        .topics = &.{},
    };
    req.serialize(&buf, &pos, 0);

    try expectControllerTrailingByteRejected(&ctrl, &buf, pos, 59, 0, 9111, true);
}

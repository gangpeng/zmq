const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = @import("fs_compat");
const log = std.log.scoped(.raft);
const MetricRegistry = @import("core").MetricRegistry;

/// KRaft Raft consensus state machine.
///
/// Implements the Raft consensus protocol as used by Kafka's KRaft mode.
/// States: Unattached → Candidate → Leader / Follower
///
/// Key differences from vanilla Raft:
/// - Uses Kafka's own protocol for RPCs (Vote, BeginQuorumEpoch, etc.)
/// - Pre-vote protocol to prevent disruptive elections (KIP-996)
/// - Voters vs Observers distinction
pub const RaftState = struct {
    node_id: i32,
    cluster_id: []const u8,
    role: Role,
    current_epoch: i32 = 0,
    voted_for: ?i32 = null,
    leader_id: ?i32 = null,
    voters: std.AutoHashMap(i32, VoterInfo),
    log: RaftLog,
    election_timer: ElectionTimer,
    allocator: Allocator,

    /// Commit index — entries up to this offset are considered committed.
    /// Only committed entries should be applied to the state machine.
    commit_index: u64 = 0,

    /// Data directory for raft log persistence.
    data_dir: ?[]const u8 = null,

    /// Offset of the last snapshot (entries before this have been compacted).
    last_snapshot_offset: u64 = 0,
    /// Epoch of the last snapshot.
    last_snapshot_epoch: i32 = 0,

    /// Whether there is a pending (uncommitted) config change entry in the log.
    /// Only one config change can be in-flight at a time (Raft single-server rule).
    pending_config_change: bool = false,

    /// Offset of the last applied config change entry (null = none applied yet).
    last_applied_config_offset: ?u64 = null,

    /// Optional Prometheus metric registry for Raft consensus observability.
    metrics: ?*MetricRegistry = null,

    /// Serialized PreparedObjectRegistry data to persist alongside Raft snapshot.
    /// Set by the broker before calling takeSnapshot(), cleared after persistence.
    /// Ownership: caller-owned; RaftState does NOT free this on deinit.
    prepared_registry_data: ?[]const u8 = null,

    pub const Role = enum {
        unattached,
        follower,
        candidate,
        leader,
        resigned,
    };

    pub const VoterInfo = struct {
        node_id: i32,
        voter_directory_id: [16]u8 = zero_uuid,
        endpoints: []const VoterEndpoint = &.{},
        k_raft_min_supported_version: i16 = 0,
        k_raft_max_supported_version: i16 = 0,
        match_index: u64 = 0,
        next_index: u64 = 0,
        last_heartbeat_ms: i64 = 0,
    };

    pub const VoterEndpoint = struct {
        name: []u8,
        host: []u8,
        port: u16,
    };

    pub const VoterEndpointView = struct {
        name: []const u8,
        host: []const u8,
        port: u16,
    };

    const zero_uuid = [_]u8{0} ** 16;

    pub fn init(alloc: Allocator, node_id: i32, cluster_id: []const u8) RaftState {
        var state = RaftState{
            .node_id = node_id,
            .cluster_id = cluster_id,
            .role = .unattached,
            .voters = std.AutoHashMap(i32, VoterInfo).init(alloc),
            .log = RaftLog.init(alloc),
            .election_timer = ElectionTimer.init(1500, 3000), // 1.5-3s
            .allocator = alloc,
        };
        state.election_timer.reseed(node_id);
        return state;
    }

    /// Init with data_dir for raft log persistence (Fix 3).
    pub fn initWithDataDir(alloc: Allocator, node_id: i32, cluster_id: []const u8, data_dir: []const u8) RaftState {
        var state = init(alloc, node_id, cluster_id);
        state.data_dir = data_dir;
        return state;
    }

    pub fn deinit(self: *RaftState) void {
        var it = self.voters.iterator();
        while (it.next()) |entry| {
            self.freeVoterInfo(entry.value_ptr.*);
        }
        self.voters.deinit();
        self.log.deinit();
    }

    /// Register a voter in the cluster.
    pub fn addVoter(self: *RaftState, voter_id: i32) !void {
        if (self.voters.getPtr(voter_id)) |existing| {
            self.freeVoterInfo(existing.*);
            existing.* = .{ .node_id = voter_id };
            return;
        }
        try self.voters.put(voter_id, .{ .node_id = voter_id });
    }

    fn freeVoterEndpoints(self: *RaftState, endpoints: []const VoterEndpoint) void {
        for (endpoints) |endpoint| {
            self.allocator.free(endpoint.name);
            self.allocator.free(endpoint.host);
        }
        if (endpoints.len > 0) self.allocator.free(@constCast(endpoints));
    }

    fn freeVoterInfo(self: *RaftState, info: VoterInfo) void {
        self.freeVoterEndpoints(info.endpoints);
    }

    fn cloneVoterEndpoints(self: *RaftState, endpoints: []const VoterEndpointView) ![]const VoterEndpoint {
        if (endpoints.len == 0) return error.InvalidEndpoint;

        const owned = try self.allocator.alloc(VoterEndpoint, endpoints.len);
        var initialized: usize = 0;
        errdefer {
            for (owned[0..initialized]) |endpoint| {
                self.allocator.free(endpoint.name);
                self.allocator.free(endpoint.host);
            }
            self.allocator.free(owned);
        }

        for (endpoints, 0..) |endpoint, index| {
            if (endpoint.name.len == 0 or endpoint.host.len == 0 or endpoint.port == 0) return error.InvalidEndpoint;
            const name_copy = try self.allocator.dupe(u8, endpoint.name);
            const host_copy = self.allocator.dupe(u8, endpoint.host) catch |err| {
                self.allocator.free(name_copy);
                return err;
            };
            owned[index] = .{
                .name = name_copy,
                .host = host_copy,
                .port = endpoint.port,
            };
            initialized += 1;
        }

        return owned;
    }

    /// Replace endpoint metadata for an existing voter after a committed update
    /// config entry is applied.
    pub fn updateVoterMetadata(
        self: *RaftState,
        voter_id: i32,
        voter_directory_id: [16]u8,
        endpoints: []const VoterEndpointView,
        k_raft_min_supported_version: i16,
        k_raft_max_supported_version: i16,
    ) !void {
        if (k_raft_min_supported_version > k_raft_max_supported_version) return error.InvalidUpdateVersion;

        const voter = self.voters.getPtr(voter_id) orelse return error.VoterNotFound;
        const new_endpoints = try self.cloneVoterEndpoints(endpoints);
        errdefer self.freeVoterEndpoints(new_endpoints);

        self.freeVoterEndpoints(voter.endpoints);
        voter.voter_directory_id = voter_directory_id;
        voter.endpoints = new_endpoints;
        voter.k_raft_min_supported_version = k_raft_min_supported_version;
        voter.k_raft_max_supported_version = k_raft_max_supported_version;
    }

    /// Handle a vote request. Returns whether vote is granted.
    pub fn handleVoteRequest(self: *RaftState, candidate_id: i32, candidate_epoch: i32, last_log_offset: u64, last_log_epoch: i32) VoteResponse {
        // If candidate's epoch is less than ours, reject
        if (candidate_epoch < self.current_epoch) {
            log.debug("Vote rejected from node {d}: stale epoch {d} < {d}", .{ candidate_id, candidate_epoch, self.current_epoch });
            if (self.metrics) |m| m.incrementCounter("raft_votes_rejected_total");
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // If we already voted for someone else this epoch, reject
        if (candidate_epoch == self.current_epoch and self.voted_for != null and self.voted_for.? != candidate_id) {
            log.debug("Vote rejected from node {d}: already voted for {?d} in epoch {d}", .{ candidate_id, self.voted_for, self.current_epoch });
            if (self.metrics) |m| m.incrementCounter("raft_votes_rejected_total");
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // Check if candidate's log is at least as up-to-date as ours
        const our_last_epoch = self.log.lastEpoch();
        const our_last_offset = self.log.lastOffset();

        const log_ok = (last_log_epoch > our_last_epoch) or
            (last_log_epoch == our_last_epoch and last_log_offset >= our_last_offset);

        if (!log_ok) {
            log.debug("Vote rejected from node {d}: log not up-to-date (candidate epoch={d}/offset={d}, ours epoch={d}/offset={d})", .{ candidate_id, last_log_epoch, last_log_offset, our_last_epoch, our_last_offset });
            if (self.metrics) |m| m.incrementCounter("raft_votes_rejected_total");
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // Grant vote
        if (candidate_epoch > self.current_epoch) {
            self.current_epoch = candidate_epoch;
            self.role = .follower;
            self.leader_id = null;
        }
        self.voted_for = candidate_id;
        // Persist to disk BEFORE granting the vote — crash safety requires
        // that voted_for is durable before the vote response is sent.
        self.persistRaftMeta();
        // Reset election timer: granting a vote means the cluster is active,
        // so we shouldn't start our own election immediately.
        self.election_timer.reset();
        log.info("Vote granted to node {d} for epoch {d}", .{ candidate_id, self.current_epoch });

        if (self.metrics) |m| {
            m.incrementCounter("raft_votes_granted_total");
            m.setGauge("raft_current_epoch", @floatFromInt(self.current_epoch));
        }

        return .{ .vote_granted = true, .epoch = self.current_epoch };
    }

    pub const VoteResponse = struct {
        vote_granted: bool,
        epoch: i32,
    };

    /// Start an election. Transitions to candidate and votes for self.
    pub fn startElection(self: *RaftState) ElectionResult {
        self.current_epoch += 1;
        self.role = .candidate;
        self.voted_for = self.node_id;
        self.leader_id = null;
        // Persist new epoch and self-vote to disk before broadcasting
        self.persistRaftMeta();
        self.election_timer.reset();
        log.info("Starting election: node {d} becoming candidate for epoch {d}", .{ self.node_id, self.current_epoch });

        if (self.metrics) |m| {
            m.incrementCounter("raft_elections_started_total");
            m.setGauge("raft_current_epoch", @floatFromInt(self.current_epoch));
            m.setGauge("raft_role", 2.0); // candidate
        }

        return .{
            .epoch = self.current_epoch,
            .last_log_offset = self.log.lastOffset(),
            .last_log_epoch = self.log.lastEpoch(),
        };
    }

    /// Start a pre-vote phase (KIP-996). Does NOT increment epoch.
    /// Returns the tentative next epoch and log info for pre-vote requests.
    pub fn startPreVote(self: *RaftState) ElectionResult {
        // Do NOT increment epoch — this is tentative
        log.info("Starting pre-vote: node {d} for tentative epoch {d}", .{ self.node_id, self.current_epoch + 1 });
        if (self.metrics) |m| {
            m.incrementCounter("raft_pre_votes_started_total");
        }
        return .{
            .epoch = self.current_epoch + 1, // Tentative next epoch
            .last_log_offset = self.log.lastOffset(),
            .last_log_epoch = self.log.lastEpoch(),
        };
    }

    /// Handle a pre-vote request. Grants pre-vote if:
    /// - Candidate's tentative epoch >= our current epoch
    /// - Candidate's log is at least as up-to-date as ours
    /// - We haven't received a recent heartbeat (leader might still be alive)
    /// Does NOT change our epoch or voted_for.
    pub fn handlePreVoteRequest(self: *RaftState, candidate_id: i32, candidate_epoch: i32, last_log_offset: u64, last_log_epoch: i32) VoteResponse {
        _ = candidate_id;

        // If candidate's tentative epoch is less than ours, reject
        if (candidate_epoch < self.current_epoch) {
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // If we recently heard from a leader, reject (leader is still alive)
        if (!self.election_timer.isExpired()) {
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // Check if candidate's log is at least as up-to-date
        const our_last_epoch = self.log.lastEpoch();
        const our_last_offset = self.log.lastOffset();
        const log_ok = (last_log_epoch > our_last_epoch) or
            (last_log_epoch == our_last_epoch and last_log_offset >= our_last_offset);

        if (!log_ok) {
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // Grant pre-vote (does NOT change our state)
        return .{ .vote_granted = true, .epoch = self.current_epoch };
    }

    pub const ElectionResult = struct {
        epoch: i32,
        last_log_offset: u64,
        last_log_epoch: i32,
    };

    /// Called when we receive a majority of votes. Transitions to leader.
    pub fn becomeLeader(self: *RaftState) void {
        self.role = .leader;
        self.leader_id = self.node_id;
        log.info("Node {d} became leader in epoch {d}", .{ self.node_id, self.current_epoch });

        if (self.metrics) |m| {
            m.incrementCounter("raft_leader_elections_won_total");
            m.setGauge("raft_role", 3.0); // leader
        }

        // Initialize next_index for all voters
        var it = self.voters.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.next_index = self.log.nextOffset();
            entry.value_ptr.match_index = 0;
        }
    }

    /// Called when we discover a leader with a higher epoch.
    /// Only accepts epoch >= current_epoch. Lower epochs are ignored
    /// (they represent stale messages from a previous leader).
    pub fn becomeFollower(self: *RaftState, epoch: i32, leader_id: i32) void {
        if (epoch < self.current_epoch) {
            log.debug("Ignoring stale becomeFollower: epoch {d} < current {d}", .{ epoch, self.current_epoch });
            return;
        }
        self.current_epoch = epoch;
        self.role = .follower;
        self.leader_id = leader_id;
        self.voted_for = null;
        // Persist epoch change to disk
        self.persistRaftMeta();
        self.election_timer.reset();
        log.info("Node {d} became follower: epoch={d}, leader={d}", .{ self.node_id, epoch, leader_id });

        if (self.metrics) |m| {
            m.incrementCounter("raft_epoch_changes_total");
            m.setGauge("raft_current_epoch", @floatFromInt(epoch));
            m.setGauge("raft_role", 1.0); // follower
        }
    }

    /// Append an entry to the log (leader only).
    /// Persists entry to raft.log on disk if data_dir is set.
    pub fn appendEntry(self: *RaftState, data: []const u8) !u64 {
        if (self.role != .leader) return error.NotLeader;
        const offset = try self.log.append(self.current_epoch, data);

        // Persist to disk
        if (self.data_dir) |dir| {
            self.persistEntry(dir, self.current_epoch, offset, data) catch |err| {
                log.warn("Failed to persist raft log entry: {}", .{err});
            };
        }

        if (self.metrics) |m| {
            m.incrementCounter("raft_log_entries_appended_total");
        }

        return offset;
    }

    /// Persist a single raft log entry to the raft.log file.
    /// Format: epoch(8 bytes) + offset(8 bytes) + len(4 bytes) + data(len bytes)
    fn persistEntry(self: *RaftState, dir: []const u8, epoch: i32, offset: u64, data: []const u8) !void {
        _ = self;
        const path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/raft.log", .{dir});
        defer std.heap.page_allocator.free(path);

        // Ensure directory exists
        fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try fs.createFileAbsolute(path, .{ .truncate = false });
        defer file.close();

        // Seek to end
        const stat = try file.stat();
        try file.seekTo(stat.size);

        // Write: epoch(i64) + offset(u64) + len(u32) + data
        var header: [20]u8 = undefined;
        std.mem.writeInt(i64, header[0..8], @intCast(epoch), .big);
        std.mem.writeInt(u64, header[8..16], offset, .big);
        std.mem.writeInt(u32, header[16..20], @intCast(data.len), .big);
        try file.writeAll(&header);
        try file.writeAll(data);
        try file.sync();
    }

    /// Persist current_epoch and voted_for to disk.
    /// Called before granting a vote or changing epoch to ensure crash safety.
    /// Without this, a process restart could lead to double-voting in the same epoch.
    fn persistRaftMeta(self: *RaftState) void {
        const dir = self.data_dir orelse return;
        const path = std.fmt.allocPrint(self.allocator, "{s}/raft.meta", .{dir}) catch return;
        defer self.allocator.free(path);

        // Ensure directory exists
        fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return,
        };

        const file = fs.createFileAbsolute(path, .{ .truncate = true }) catch return;
        defer file.close();

        // Write: current_epoch(i32, 4B) + voted_for_present(u8, 1B) + voted_for(i32, 4B)
        var buf: [9]u8 = undefined;
        std.mem.writeInt(i32, buf[0..4], self.current_epoch, .big);
        if (self.voted_for) |vf| {
            buf[4] = 1;
            std.mem.writeInt(i32, buf[5..9], vf, .big);
        } else {
            buf[4] = 0;
            std.mem.writeInt(i32, buf[5..9], 0, .big);
        }
        file.writeAll(&buf) catch |err| {
            log.warn("Failed to persist raft.meta (epoch={d}, voted_for={?d}): {}", .{ self.current_epoch, self.voted_for, err });
        };
    }

    /// Load persisted current_epoch and voted_for from disk on startup.
    /// Returns true if metadata was loaded successfully.
    pub fn loadPersistedMeta(self: *RaftState) bool {
        const dir = self.data_dir orelse return false;
        const path = std.fmt.allocPrint(self.allocator, "{s}/raft.meta", .{dir}) catch return false;
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return false;
        defer file.close();

        var buf: [9]u8 = undefined;
        const n = file.readAll(&buf) catch return false;
        if (n < 9) return false;

        const epoch = std.mem.readInt(i32, buf[0..4], .big);
        const has_voted = buf[4] == 1;
        const voted = std.mem.readInt(i32, buf[5..9], .big);

        if (epoch > self.current_epoch) {
            self.current_epoch = epoch;
        }
        if (has_voted) {
            self.voted_for = voted;
        }
        log.info("Loaded raft.meta: epoch={d}, voted_for={?d}", .{ self.current_epoch, self.voted_for });
        return true;
    }

    /// Load and replay persisted raft log entries on startup.
    pub fn loadPersistedLog(self: *RaftState) !u64 {
        // Load persisted epoch and voted_for first
        _ = self.loadPersistedMeta();

        // Load snapshot metadata (if any) to know which entries were compacted
        _ = self.loadSnapshotMeta();

        const dir = self.data_dir orelse return 0;
        log.info("Starting Raft log recovery from {s}", .{dir});
        const path = try std.fmt.allocPrint(self.allocator, "{s}/raft.log", .{dir});
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return 0;
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 256 * 1024 * 1024) catch return 0;
        defer self.allocator.free(content);

        var recovered: u64 = 0;
        var pos: usize = 0;
        while (pos + 20 <= content.len) {
            const epoch: i32 = @intCast(std.mem.readInt(i64, content[pos..][0..8], .big));
            // skip offset at pos+8
            const data_len = std.mem.readInt(u32, content[pos + 16 ..][0..4], .big);
            pos += 20;
            if (pos + data_len > content.len) break;
            const data = content[pos .. pos + data_len];
            _ = self.log.append(epoch, data) catch |err| {
                log.warn("Raft log recovery truncated at entry {d}: {}", .{ recovered, err });
                break;
            };
            pos += data_len;
            recovered += 1;

            // Update current_epoch to the highest seen
            if (epoch > self.current_epoch) self.current_epoch = epoch;
        }

        log.info("Raft log recovery complete: {d} entries recovered, epoch={d}", .{ recovered, self.current_epoch });
        return recovered;
    }

    /// Build an AppendEntries request for a specific follower (fix #13).
    /// Returns the entries that need to be sent based on the follower's next_index.
    pub fn getAppendEntriesForFollower(self: *RaftState, follower_id: i32) ?AppendEntriesRequest {
        if (self.role != .leader) return null;
        const voter = self.voters.getPtr(follower_id) orelse return null;

        const next_idx = voter.next_index;
        if (next_idx > self.log.lastOffset() + 1) return null;

        // Gather entries from next_index onwards
        var entries_count: usize = 0;
        for (self.log.entries.items) |entry| {
            if (entry.offset >= next_idx) entries_count += 1;
        }

        // Compute prev_log state
        var prev_log_offset: u64 = 0;
        var prev_log_epoch: i32 = 0;
        if (next_idx > 0) {
            if (self.log.get(next_idx - 1)) |prev_entry| {
                prev_log_offset = prev_entry.offset;
                prev_log_epoch = prev_entry.epoch;
            }
        }

        return .{
            .leader_id = self.node_id,
            .epoch = self.current_epoch,
            .prev_log_offset = prev_log_offset,
            .prev_log_epoch = prev_log_epoch,
            .entries_start_index = next_idx,
            .entries_count = entries_count,
            .leader_commit = self.commit_index,
        };
    }

    pub const AppendEntriesRequest = struct {
        leader_id: i32,
        epoch: i32,
        prev_log_offset: u64,
        prev_log_epoch: i32,
        entries_start_index: u64,
        entries_count: usize,
        leader_commit: u64,
    };

    pub const AppendEntriesResponse = struct {
        success: bool,
        epoch: i32,
        match_index: u64,
    };

    pub const AppendEntry = struct {
        offset: u64,
        epoch: i32,
        data: []const u8,
    };

    /// Configuration change entry types for dynamic voter membership.
    pub const ConfigChangeType = enum(u8) {
        add_voter = 1,
        remove_voter = 2,
        update_voter = 3,
    };

    /// A serialized config change entry stored in the Raft log.
    /// Format: type(1B) + voter_id(4B) = 5 bytes
    pub const ConfigChangeEntry = struct {
        change_type: ConfigChangeType,
        voter_id: i32,

        const update_magic = "ZMQCFGU";

        pub fn serialize(self: ConfigChangeEntry) [5]u8 {
            var buf: [5]u8 = undefined;
            buf[0] = @intFromEnum(self.change_type);
            std.mem.writeInt(i32, buf[1..5], self.voter_id, .big);
            return buf;
        }

        pub fn deserialize(data: []const u8) ?ConfigChangeEntry {
            if (data.len < 5) return null;
            // Check for config change magic prefix
            if (data[0] != 1 and data[0] != 2) return null;
            return .{
                .change_type = @enumFromInt(data[0]),
                .voter_id = std.mem.readInt(i32, data[1..5], .big),
            };
        }

        /// Check if a log entry is a config change (vs regular data).
        pub fn isConfigChange(data: []const u8) bool {
            if (isVoterMetadataChange(data)) return true;
            if (data.len < 5) return false;
            return data[0] == 1 or data[0] == 2;
        }

        pub const VoterUpdate = struct {
            change_type: ConfigChangeType = .update_voter,
            voter_id: i32,
            voter_directory_id: [16]u8,
            endpoints: []const VoterEndpointView,
            k_raft_min_supported_version: i16,
            k_raft_max_supported_version: i16,

            pub fn deinit(self: *VoterUpdate, alloc: Allocator) void {
                if (self.endpoints.len > 0) alloc.free(@constCast(self.endpoints));
            }
        };

        pub fn isVoterUpdate(data: []const u8) bool {
            return data.len >= update_magic.len + 1 and
                std.mem.eql(u8, data[0..update_magic.len], update_magic) and
                data[update_magic.len] == @intFromEnum(ConfigChangeType.update_voter);
        }

        pub fn isVoterMetadataChange(data: []const u8) bool {
            if (data.len < update_magic.len + 1) return false;
            if (!std.mem.eql(u8, data[0..update_magic.len], update_magic)) return false;
            return data[update_magic.len] == @intFromEnum(ConfigChangeType.add_voter) or
                data[update_magic.len] == @intFromEnum(ConfigChangeType.update_voter);
        }

        fn checkedAdd(total: *usize, value: usize) !void {
            if (std.math.maxInt(usize) - total.* < value) return error.MessageTooLarge;
            total.* += value;
        }

        fn serializeVoterMetadataChange(
            alloc: Allocator,
            change_type: ConfigChangeType,
            voter_id: i32,
            voter_directory_id: [16]u8,
            endpoints: []const VoterEndpointView,
            k_raft_min_supported_version: i16,
            k_raft_max_supported_version: i16,
        ) ![]u8 {
            if (change_type != .add_voter and change_type != .update_voter) return error.InvalidConfigChangeEntry;
            if (endpoints.len == 0) return error.InvalidEndpoint;
            if (endpoints.len > std.math.maxInt(u32)) return error.MessageTooLarge;
            if (k_raft_min_supported_version > k_raft_max_supported_version) return error.InvalidUpdateVersion;

            var total_len: usize = update_magic.len + 1 + 4 + 16 + 2 + 2 + 4;
            for (endpoints) |endpoint| {
                if (endpoint.name.len == 0 or endpoint.host.len == 0 or endpoint.port == 0) return error.InvalidEndpoint;
                if (endpoint.name.len > std.math.maxInt(u32) or endpoint.host.len > std.math.maxInt(u32)) return error.MessageTooLarge;
                try checkedAdd(&total_len, 4 + endpoint.name.len + 4 + endpoint.host.len + 2);
            }

            const data = try alloc.alloc(u8, total_len);
            var pos: usize = 0;
            @memcpy(data[pos .. pos + update_magic.len], update_magic);
            pos += update_magic.len;
            data[pos] = @intFromEnum(change_type);
            pos += 1;
            std.mem.writeInt(i32, data[pos..][0..4], voter_id, .big);
            pos += 4;
            @memcpy(data[pos .. pos + 16], &voter_directory_id);
            pos += 16;
            std.mem.writeInt(i16, data[pos..][0..2], k_raft_min_supported_version, .big);
            pos += 2;
            std.mem.writeInt(i16, data[pos..][0..2], k_raft_max_supported_version, .big);
            pos += 2;
            std.mem.writeInt(u32, data[pos..][0..4], @intCast(endpoints.len), .big);
            pos += 4;
            for (endpoints) |endpoint| {
                std.mem.writeInt(u32, data[pos..][0..4], @intCast(endpoint.name.len), .big);
                pos += 4;
                @memcpy(data[pos .. pos + endpoint.name.len], endpoint.name);
                pos += endpoint.name.len;
                std.mem.writeInt(u32, data[pos..][0..4], @intCast(endpoint.host.len), .big);
                pos += 4;
                @memcpy(data[pos .. pos + endpoint.host.len], endpoint.host);
                pos += endpoint.host.len;
                std.mem.writeInt(u16, data[pos..][0..2], endpoint.port, .big);
                pos += 2;
            }
            return data;
        }

        pub fn serializeVoterAdd(
            alloc: Allocator,
            voter_id: i32,
            voter_directory_id: [16]u8,
            endpoints: []const VoterEndpointView,
        ) ![]u8 {
            return serializeVoterMetadataChange(alloc, .add_voter, voter_id, voter_directory_id, endpoints, 0, 0);
        }

        pub fn serializeVoterUpdate(
            alloc: Allocator,
            voter_id: i32,
            voter_directory_id: [16]u8,
            endpoints: []const VoterEndpointView,
            k_raft_min_supported_version: i16,
            k_raft_max_supported_version: i16,
        ) ![]u8 {
            return serializeVoterMetadataChange(
                alloc,
                .update_voter,
                voter_id,
                voter_directory_id,
                endpoints,
                k_raft_min_supported_version,
                k_raft_max_supported_version,
            );
        }

        fn readU32(data: []const u8, pos: *usize) !u32 {
            if (pos.* + 4 > data.len) return error.InvalidConfigChangeEntry;
            const value = std.mem.readInt(u32, data[pos.*..][0..4], .big);
            pos.* += 4;
            return value;
        }

        fn readI32(data: []const u8, pos: *usize) !i32 {
            if (pos.* + 4 > data.len) return error.InvalidConfigChangeEntry;
            const value = std.mem.readInt(i32, data[pos.*..][0..4], .big);
            pos.* += 4;
            return value;
        }

        fn readI16(data: []const u8, pos: *usize) !i16 {
            if (pos.* + 2 > data.len) return error.InvalidConfigChangeEntry;
            const value = std.mem.readInt(i16, data[pos.*..][0..2], .big);
            pos.* += 2;
            return value;
        }

        fn readU16(data: []const u8, pos: *usize) !u16 {
            if (pos.* + 2 > data.len) return error.InvalidConfigChangeEntry;
            const value = std.mem.readInt(u16, data[pos.*..][0..2], .big);
            pos.* += 2;
            return value;
        }

        fn readBytes(data: []const u8, pos: *usize, len: usize) ![]const u8 {
            if (pos.* + len > data.len) return error.InvalidConfigChangeEntry;
            const bytes = data[pos.* .. pos.* + len];
            pos.* += len;
            return bytes;
        }

        pub fn deserializeVoterMetadataChange(alloc: Allocator, data: []const u8) !?VoterUpdate {
            if (!isVoterMetadataChange(data)) return null;

            var pos: usize = update_magic.len + 1;
            const change_type: ConfigChangeType = @enumFromInt(data[update_magic.len]);
            const voter_id = try readI32(data, &pos);
            const voter_directory_id = try readBytes(data, &pos, 16);
            const min_supported_version = try readI16(data, &pos);
            const max_supported_version = try readI16(data, &pos);
            const endpoint_count: usize = @intCast(try readU32(data, &pos));
            if (endpoint_count == 0) return error.InvalidConfigChangeEntry;

            const endpoints = try alloc.alloc(VoterEndpointView, endpoint_count);
            errdefer alloc.free(endpoints);

            for (endpoints) |*endpoint| {
                const name_len: usize = @intCast(try readU32(data, &pos));
                const name = try readBytes(data, &pos, name_len);
                const host_len: usize = @intCast(try readU32(data, &pos));
                const host = try readBytes(data, &pos, host_len);
                const port = try readU16(data, &pos);
                if (name.len == 0 or host.len == 0 or port == 0) return error.InvalidConfigChangeEntry;
                endpoint.* = .{
                    .name = name,
                    .host = host,
                    .port = port,
                };
            }
            if (pos != data.len) return error.InvalidConfigChangeEntry;
            if (min_supported_version > max_supported_version) return error.InvalidConfigChangeEntry;

            var directory_id: [16]u8 = undefined;
            @memcpy(&directory_id, voter_directory_id);
            return .{
                .change_type = change_type,
                .voter_id = voter_id,
                .voter_directory_id = directory_id,
                .endpoints = endpoints,
                .k_raft_min_supported_version = min_supported_version,
                .k_raft_max_supported_version = max_supported_version,
            };
        }

        pub fn deserializeVoterUpdate(alloc: Allocator, data: []const u8) !?VoterUpdate {
            const change = (try deserializeVoterMetadataChange(alloc, data)) orelse return null;
            if (change.change_type != .update_voter) {
                var mutable_change = change;
                mutable_change.deinit(alloc);
                return null;
            }
            return change;
        }
    };

    /// Handle an AppendEntries request from the leader (follower side).
    /// Validates prev_log match, truncates conflicting entries, appends new ones.
    /// Returns success=true if entries were accepted.
    pub fn handleAppendEntries(
        self: *RaftState,
        leader_epoch: i32,
        leader_id: i32,
        prev_log_offset: u64,
        prev_log_epoch: i32,
        entries: []const AppendEntry,
        leader_commit: u64,
    ) AppendEntriesResponse {
        // Reject if leader's epoch is stale
        if (leader_epoch < self.current_epoch) {
            log.debug("AppendEntries rejected: stale epoch {d} < {d} from leader {d}", .{ leader_epoch, self.current_epoch, leader_id });
            return .{ .success = false, .epoch = self.current_epoch, .match_index = self.log.lastOffset() };
        }

        // Accept leadership
        if (leader_epoch >= self.current_epoch) {
            self.becomeFollower(leader_epoch, leader_id);
        }

        // Validate prev_log match (skip for first entry)
        if (prev_log_offset > 0) {
            if (self.log.get(prev_log_offset)) |prev_entry| {
                if (prev_entry.epoch != prev_log_epoch) {
                    // Log mismatch — leader should decrement next_index and retry
                    log.debug("AppendEntries log mismatch at offset {d}: expected epoch {d}, got {d}", .{ prev_log_offset, prev_log_epoch, prev_entry.epoch });
                    return .{ .success = false, .epoch = self.current_epoch, .match_index = self.log.lastOffset() };
                }
            } else if (self.log.length() > 0) {
                // We don't have the entry at prev_log_offset
                return .{ .success = false, .epoch = self.current_epoch, .match_index = self.log.lastOffset() };
            }
        }

        // Append new entries (truncate conflicts first)
        for (entries) |entry| {
            if (self.log.hasConflict(entry.offset, entry.epoch)) {
                self.log.truncateFrom(entry.offset);
            }
            // Only append if we don't already have this entry
            if (self.log.get(entry.offset) == null) {
                const appended_offset = self.log.append(entry.epoch, entry.data) catch continue;
                if (self.data_dir) |dir| {
                    self.persistEntry(dir, entry.epoch, appended_offset, entry.data) catch |err| {
                        log.warn("Failed to persist follower raft log entry: {}", .{err});
                    };
                }
            }
        }

        // Update commit index
        if (leader_commit > self.commit_index) {
            self.commit_index = @min(leader_commit, self.log.lastOffset());
        }

        return .{ .success = true, .epoch = self.current_epoch, .match_index = self.log.lastOffset() };
    }

    /// Handle an AppendEntries response from a follower.
    /// Updates match_index and next_index for the follower.
    pub fn handleAppendEntriesResponse(self: *RaftState, follower_id: i32, response: AppendEntriesResponse) void {
        if (self.role != .leader) return;

        // If the follower has a higher epoch, step down.
        // We don't know who the new leader is, so become unattached.
        if (response.epoch > self.current_epoch) {
            self.current_epoch = response.epoch;
            self.role = .unattached;
            self.leader_id = null;
            self.voted_for = null;
            self.election_timer.reset();
            return;
        }

        const voter = self.voters.getPtr(follower_id) orelse return;
        if (response.success) {
            voter.match_index = response.match_index;
            voter.next_index = if (self.log.length() == 0) 0 else response.match_index + 1;
            voter.last_heartbeat_ms = @import("time_compat").milliTimestamp();

            // Recompute commit index after match_index update
            self.updateCommitIndex();
        } else {
            // Decrement next_index to retry with earlier entries
            if (voter.next_index > 0) voter.next_index -= 1;
        }
    }

    /// Compute commit index as median of match_index across quorum.
    /// The leader updates commit_index to the highest offset N such that
    /// a majority of voters have match_index >= N and the entry at N has the current epoch.
    /// The epoch check is critical: without it, a leader could commit entries from a
    /// previous epoch before its own entries are replicated, violating Leader Completeness.
    pub fn updateCommitIndex(self: *RaftState) void {
        if (self.role != .leader) return;
        const n = self.voters.count();
        if (n == 0) return;

        // Collect match indices (leader counts as matched to its own log end)
        var indices_buf: [32]u64 = undefined;
        var count: usize = 0;
        var it = self.voters.iterator();
        while (it.next()) |entry| {
            if (count >= indices_buf.len) break;
            if (entry.key_ptr.* == self.node_id) {
                // Leader is up-to-date with itself
                indices_buf[count] = self.log.lastOffset();
            } else {
                indices_buf[count] = entry.value_ptr.match_index;
            }
            count += 1;
        }

        if (count == 0) return;

        // Sort ascending
        const indices = indices_buf[0..count];
        std.mem.sort(u64, indices, {}, struct {
            fn lessThan(_: void, a: u64, b: u64) bool {
                return a < b;
            }
        }.lessThan);

        // The median (majority position) is at index count/2 for odd, (count-1)/2 for even
        // More precisely, the highest N where majority have match >= N is indices[count - majority]
        const majority = n / 2 + 1;
        if (count >= majority) {
            const median_idx = count - majority;
            const new_commit = indices[median_idx];

            // Raft safety: only commit entries from the current epoch.
            // An entry from a previous epoch can only be committed indirectly
            // when a current-epoch entry after it is committed.
            if (new_commit > self.commit_index) {
                if (self.log.get(new_commit)) |entry| {
                    if (entry.epoch == self.current_epoch) {
                        const old_commit = self.commit_index;
                        self.commit_index = new_commit;
                        // Apply any newly committed config changes
                        self.applyCommittedConfigs();
                        if (self.metrics) |m| {
                            m.addCounter("raft_log_entries_committed_total", new_commit - old_commit);
                            m.setGauge("raft_commit_index", @floatFromInt(new_commit));
                        }
                    }
                } else if (new_commit == 0 and self.log.length() == 0) {
                    // Empty log edge case — commit_index stays at 0
                }
            }
        }
    }

    /// Propose adding a voter to the cluster (leader only).
    /// Returns the log offset of the config change entry, or error if:
    /// - Not the leader
    /// - A config change is already pending
    /// - Voter already exists
    pub fn proposeAddVoter(self: *RaftState, voter_id: i32) !u64 {
        if (self.role != .leader) return error.NotLeader;
        if (self.pending_config_change) return error.ConfigChangePending;
        if (self.voters.contains(voter_id)) return error.VoterAlreadyExists;

        const entry = ConfigChangeEntry{ .change_type = .add_voter, .voter_id = voter_id };
        const data = entry.serialize();
        const offset = try self.appendEntry(&data);
        self.pending_config_change = true;

        log.info("Proposed AddVoter: node {d} at offset {d}", .{ voter_id, offset });
        return offset;
    }

    /// Propose adding a voter with endpoint metadata (leader only).
    pub fn proposeAddVoterWithMetadata(
        self: *RaftState,
        voter_id: i32,
        voter_directory_id: [16]u8,
        endpoints: []const VoterEndpointView,
    ) !u64 {
        if (self.role != .leader) return error.NotLeader;
        if (self.pending_config_change) return error.ConfigChangePending;
        if (self.voters.contains(voter_id)) return error.VoterAlreadyExists;

        const data = try ConfigChangeEntry.serializeVoterAdd(
            self.allocator,
            voter_id,
            voter_directory_id,
            endpoints,
        );
        defer self.allocator.free(data);

        const offset = try self.appendEntry(data);
        self.pending_config_change = true;

        log.info("Proposed AddVoter with endpoint metadata: node {d} at offset {d}", .{ voter_id, offset });
        return offset;
    }

    /// Propose removing a voter from the cluster (leader only).
    /// Returns the log offset of the config change entry, or error if:
    /// - Not the leader
    /// - A config change is already pending
    /// - Voter doesn't exist
    /// - Removing would leave zero voters
    pub fn proposeRemoveVoter(self: *RaftState, voter_id: i32) !u64 {
        if (self.role != .leader) return error.NotLeader;
        if (self.pending_config_change) return error.ConfigChangePending;
        if (!self.voters.contains(voter_id)) return error.VoterNotFound;
        if (self.voters.count() <= 1) return error.CannotRemoveLastVoter;

        const entry = ConfigChangeEntry{ .change_type = .remove_voter, .voter_id = voter_id };
        const data = entry.serialize();
        const offset = try self.appendEntry(&data);
        self.pending_config_change = true;

        log.info("Proposed RemoveVoter: node {d} at offset {d}", .{ voter_id, offset });
        return offset;
    }

    /// Propose updating endpoint metadata for an existing voter (leader only).
    pub fn proposeUpdateVoter(
        self: *RaftState,
        voter_id: i32,
        voter_directory_id: [16]u8,
        endpoints: []const VoterEndpointView,
        k_raft_min_supported_version: i16,
        k_raft_max_supported_version: i16,
    ) !u64 {
        if (self.role != .leader) return error.NotLeader;
        if (self.pending_config_change) return error.ConfigChangePending;
        if (!self.voters.contains(voter_id)) return error.VoterNotFound;

        const data = try ConfigChangeEntry.serializeVoterUpdate(
            self.allocator,
            voter_id,
            voter_directory_id,
            endpoints,
            k_raft_min_supported_version,
            k_raft_max_supported_version,
        );
        defer self.allocator.free(data);

        const offset = try self.appendEntry(data);
        self.pending_config_change = true;

        log.info("Proposed UpdateVoter: node {d} at offset {d}", .{ voter_id, offset });
        return offset;
    }

    /// Remove a voter from the cluster (direct, not through Raft log).
    /// Used internally by applyCommittedConfigs.
    pub fn removeVoter(self: *RaftState, voter_id: i32) void {
        if (self.voters.fetchRemove(voter_id)) |removed| {
            self.freeVoterInfo(removed.value);
        }
    }

    /// Apply any committed config change entries.
    /// Called after commit_index advances. Scans newly committed entries
    /// for config changes and applies them to the voter set.
    pub fn applyCommittedConfigs(self: *RaftState) void {
        for (self.log.entries.items) |entry| {
            if (entry.offset > self.commit_index) break;
            // Only process entries we haven't applied yet
            if (self.last_applied_config_offset) |last_offset| {
                if (entry.offset <= last_offset) continue;
            }

            if (ConfigChangeEntry.deserializeVoterMetadataChange(self.allocator, entry.data) catch null) |change| {
                var mutable_change = change;
                defer mutable_change.deinit(self.allocator);
                switch (change.change_type) {
                    .add_voter => {
                        self.addVoter(change.voter_id) catch |err| {
                            log.warn("Config apply failed: add voter {d}: {}", .{ change.voter_id, err });
                            self.last_applied_config_offset = entry.offset;
                            self.pending_config_change = false;
                            continue;
                        };
                        self.updateVoterMetadata(
                            change.voter_id,
                            change.voter_directory_id,
                            change.endpoints,
                            change.k_raft_min_supported_version,
                            change.k_raft_max_supported_version,
                        ) catch |err| {
                            log.warn("Config apply failed: add voter {d} endpoint metadata: {}", .{ change.voter_id, err });
                        };
                        log.info("Config applied: added voter {d} with endpoint metadata (now {d} voters)", .{
                            change.voter_id, self.voters.count(),
                        });
                    },
                    .update_voter => {
                        self.updateVoterMetadata(
                            change.voter_id,
                            change.voter_directory_id,
                            change.endpoints,
                            change.k_raft_min_supported_version,
                            change.k_raft_max_supported_version,
                        ) catch |err| {
                            log.warn("Config apply failed: update voter {d}: {}", .{ change.voter_id, err });
                            self.last_applied_config_offset = entry.offset;
                            self.pending_config_change = false;
                            continue;
                        };
                        log.info("Config applied: updated voter {d} endpoint metadata", .{change.voter_id});
                    },
                    .remove_voter => {},
                }
                self.last_applied_config_offset = entry.offset;
                self.pending_config_change = false;
            } else if (ConfigChangeEntry.deserialize(entry.data)) |config| {
                switch (config.change_type) {
                    .add_voter => {
                        self.addVoter(config.voter_id) catch {};
                        log.info("Config applied: added voter {d} (now {d} voters)", .{
                            config.voter_id, self.voters.count(),
                        });
                    },
                    .remove_voter => {
                        self.removeVoter(config.voter_id);
                        log.info("Config applied: removed voter {d} (now {d} voters)", .{
                            config.voter_id, self.voters.count(),
                        });
                        // If we removed ourselves, step down
                        if (config.voter_id == self.node_id and self.role == .leader) {
                            log.info("Removed self from voters, stepping down", .{});
                            self.role = .resigned;
                        }
                    },
                    .update_voter => {},
                }
                self.last_applied_config_offset = entry.offset;
                self.pending_config_change = false;
            }
        }
    }

    /// Check if the election timer has expired.
    pub fn isElectionTimedOut(self: *const RaftState) bool {
        return self.election_timer.isExpired();
    }

    /// Get the number of voters in the quorum.
    pub fn quorumSize(self: *const RaftState) usize {
        return self.voters.count();
    }

    /// Get the majority threshold.
    pub fn majorityThreshold(self: *const RaftState) usize {
        return self.quorumSize() / 2 + 1;
    }

    /// Create a snapshot at the current commit_index and truncate the log.
    /// Only entries at or after commit_index are kept.
    pub fn takeSnapshot(self: *RaftState) void {
        if (self.commit_index == 0) return;
        if (self.commit_index <= self.last_snapshot_offset) return;

        // Record snapshot metadata
        if (self.log.get(self.commit_index)) |entry| {
            self.last_snapshot_epoch = entry.epoch;
        }
        self.last_snapshot_offset = self.commit_index;

        // Truncate log entries before commit_index
        self.log.truncateBefore(self.commit_index);

        // Persist snapshot metadata
        if (self.data_dir) |dir| {
            self.persistSnapshotMeta(dir) catch {};

            // Persist prepared object registry alongside the Raft snapshot.
            // This ensures prepared objects survive Raft log truncation.
            if (self.prepared_registry_data) |reg_data| {
                self.persistPreparedRegistry(dir, reg_data) catch |err| {
                    log.warn("Failed to persist prepared.snapshot: {}", .{err});
                };
            }
        }

        if (self.metrics) |m| {
            m.incrementCounter("raft_snapshots_taken_total");
        }
    }

    /// Check if the log is large enough to warrant a snapshot.
    /// Returns true if log has more than max_entries entries.
    pub fn shouldSnapshot(self: *const RaftState, max_entries: usize) bool {
        return self.log.length() > max_entries;
    }

    fn persistSnapshotMeta(self: *RaftState, dir: []const u8) !void {
        const path = try std.fmt.allocPrint(self.allocator, "{s}/snapshot.meta", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        var buf: [16]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], self.last_snapshot_offset, .big);
        std.mem.writeInt(i32, buf[8..12], self.last_snapshot_epoch, .big);
        std.mem.writeInt(i32, buf[12..16], self.current_epoch, .big);
        try file.writeAll(&buf);
    }

    pub fn loadSnapshotMeta(self: *RaftState) bool {
        const dir = self.data_dir orelse return false;
        const path = std.fmt.allocPrint(self.allocator, "{s}/snapshot.meta", .{dir}) catch return false;
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return false;
        defer file.close();

        var buf: [16]u8 = undefined;
        const n = file.readAll(&buf) catch return false;
        if (n < 16) return false;

        self.last_snapshot_offset = std.mem.readInt(u64, buf[0..8], .big);
        self.last_snapshot_epoch = std.mem.readInt(i32, buf[8..12], .big);
        return true;
    }

    /// Persist the PreparedObjectRegistry binary data to {data_dir}/prepared.snapshot.
    fn persistPreparedRegistry(self: *RaftState, dir: []const u8, data: []const u8) !void {
        const path = try std.fmt.allocPrint(self.allocator, "{s}/prepared.snapshot", .{dir});
        defer self.allocator.free(path);

        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        defer file.close();

        try file.writeAll(data);
        log.info("Persisted prepared.snapshot ({d} bytes)", .{data.len});
    }

    /// Load persisted PreparedObjectRegistry data from {data_dir}/prepared.snapshot.
    /// Returns the raw bytes (caller-owned) or null if the file doesn't exist.
    pub fn loadPreparedRegistry(self: *RaftState) ?[]u8 {
        const dir = self.data_dir orelse return null;
        const path = std.fmt.allocPrint(self.allocator, "{s}/prepared.snapshot", .{dir}) catch return null;
        defer self.allocator.free(path);

        const file = fs.openFileAbsolute(path, .{}) catch return null;
        defer file.close();

        const stat = file.stat() catch return null;
        if (stat.size == 0) return null;

        const buf = self.allocator.alloc(u8, stat.size) catch return null;
        const n = file.readAll(buf) catch {
            self.allocator.free(buf);
            return null;
        };
        if (n != stat.size) {
            self.allocator.free(buf);
            return null;
        }

        log.info("Loaded prepared.snapshot ({d} bytes)", .{n});
        return buf;
    }
};

/// Raft log — append-only log of entries indexed by offset.
pub const RaftLog = struct {
    entries: std.array_list.Managed(LogEntry),
    allocator: Allocator,

    pub const LogEntry = struct {
        offset: u64,
        epoch: i32,
        data: []u8,
    };

    pub fn init(alloc: Allocator) RaftLog {
        return .{
            .entries = std.array_list.Managed(LogEntry).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.data);
        }
        self.entries.deinit();
    }

    pub fn append(self: *RaftLog, epoch: i32, data: []const u8) !u64 {
        const offset = self.nextOffset();
        const data_copy = try self.allocator.dupe(u8, data);
        try self.entries.append(.{
            .offset = offset,
            .epoch = epoch,
            .data = data_copy,
        });
        return offset;
    }

    pub fn lastOffset(self: *const RaftLog) u64 {
        if (self.entries.items.len == 0) return 0;
        return self.entries.getLast().offset;
    }

    pub fn lastEpoch(self: *const RaftLog) i32 {
        if (self.entries.items.len == 0) return 0;
        return self.entries.getLast().epoch;
    }

    pub fn nextOffset(self: *const RaftLog) u64 {
        return self.lastOffset() + if (self.entries.items.len > 0) @as(u64, 1) else 0;
    }

    /// Get an entry by offset. Since entries are appended sequentially,
    /// we can compute the index directly for O(1) lookup, with a linear
    /// fallback if offsets are non-contiguous (e.g., after truncation).
    pub fn get(self: *const RaftLog, offset: u64) ?*const LogEntry {
        if (self.entries.items.len == 0) return null;
        // Fast path: direct index (entries start at offset 0, contiguous)
        const first_offset = self.entries.items[0].offset;
        if (offset >= first_offset) {
            const idx = offset - first_offset;
            if (idx < self.entries.items.len) {
                const entry = &self.entries.items[idx];
                if (entry.offset == offset) return entry;
            }
        }
        // Slow path: linear scan (for non-contiguous offsets after truncation)
        for (self.entries.items) |*entry| {
            if (entry.offset == offset) return entry;
        }
        return null;
    }

    pub fn length(self: *const RaftLog) usize {
        return self.entries.items.len;
    }

    /// Truncate the log from a given offset (inclusive).
    /// Removes all entries with offset >= truncate_from.
    /// Used when a follower discovers conflicting entries from a new leader.
    pub fn truncateFrom(self: *RaftLog, truncate_from: u64) void {
        var i: usize = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            if (self.entries.items[i].offset >= truncate_from) {
                self.allocator.free(self.entries.items[i].data);
                _ = self.entries.orderedRemove(i);
            }
        }
    }

    /// Check if there's a conflicting entry at the given offset and epoch.
    /// Returns true if our entry at that offset has a different epoch.
    pub fn hasConflict(self: *const RaftLog, offset: u64, epoch: i32) bool {
        if (self.get(offset)) |entry| {
            return entry.epoch != epoch;
        }
        return false; // No entry at that offset — no conflict
    }

    /// Truncate entries before a given offset (exclusive).
    /// Keeps entries with offset >= keep_from.
    /// Used by snapshotting to reclaim memory from committed entries.
    pub fn truncateBefore(self: *RaftLog, keep_from: u64) void {
        while (self.entries.items.len > 0) {
            if (self.entries.items[0].offset < keep_from) {
                self.allocator.free(self.entries.items[0].data);
                _ = self.entries.orderedRemove(0);
            } else {
                break;
            }
        }
    }
};

/// Metadata image — in-memory materialized view of cluster metadata.
pub const MetadataImage = struct {
    topics: std.StringHashMap(TopicMetadata),
    brokers: std.AutoHashMap(i32, BrokerMetadata),
    epoch: i64 = 0,
    allocator: Allocator,

    pub const TopicMetadata = struct {
        name: []u8,
        topic_id: [16]u8,
        partitions: std.AutoHashMap(i32, PartitionMetadata),
    };

    pub const PartitionMetadata = struct {
        partition_id: i32,
        leader_id: i32,
        leader_epoch: i32,
        replicas: []i32,
        isr: []i32,
    };

    pub const BrokerMetadata = struct {
        node_id: i32,
        host: []u8,
        port: i32,
        rack: ?[]u8 = null,
    };

    pub fn init(alloc: Allocator) MetadataImage {
        return .{
            .topics = std.StringHashMap(TopicMetadata).init(alloc),
            .brokers = std.AutoHashMap(i32, BrokerMetadata).init(alloc),
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *MetadataImage) void {
        var topic_it = self.topics.iterator();
        while (topic_it.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
            entry.value_ptr.partitions.deinit();
        }
        self.topics.deinit();

        var broker_it = self.brokers.iterator();
        while (broker_it.next()) |entry| {
            self.allocator.free(entry.value_ptr.host);
            if (entry.value_ptr.rack) |rack| self.allocator.free(rack);
        }
        self.brokers.deinit();
    }

    pub fn addBroker(self: *MetadataImage, node_id: i32, host: []const u8, port: i32) !void {
        const host_copy = try self.allocator.dupe(u8, host);
        try self.brokers.put(node_id, .{
            .node_id = node_id,
            .host = host_copy,
            .port = port,
        });
    }

    pub fn topicCount(self: *const MetadataImage) usize {
        return self.topics.count();
    }

    pub fn brokerCount(self: *const MetadataImage) usize {
        return self.brokers.count();
    }
};

/// Election timer with randomized timeout.
pub const ElectionTimer = struct {
    min_timeout_ms: i64,
    max_timeout_ms: i64,
    deadline_ms: i64,
    prng: std.Random.DefaultPrng,

    pub fn init(min_ms: i64, max_ms: i64) ElectionTimer {
        var timer = ElectionTimer{
            .min_timeout_ms = min_ms,
            .max_timeout_ms = max_ms,
            .deadline_ms = 0,
            .prng = std.Random.DefaultPrng.init(@intCast(@import("time_compat").milliTimestamp())),
        };
        timer.reset();
        return timer;
    }

    pub fn reset(self: *ElectionTimer) void {
        const range: u64 = @intCast(self.max_timeout_ms - self.min_timeout_ms);
        const jitter = self.prng.random().intRangeAtMost(u64, 0, range);
        self.deadline_ms = @import("time_compat").milliTimestamp() + self.min_timeout_ms + @as(i64, @intCast(jitter));
    }

    pub fn isExpired(self: *const ElectionTimer) bool {
        return @import("time_compat").milliTimestamp() >= self.deadline_ms;
    }

    /// Re-seed the PRNG with better entropy (node_id + timestamp).
    /// Should be called after init to avoid correlated timeouts across nodes.
    pub fn reseed(self: *ElectionTimer, node_id: i32) void {
        const ts: u64 = @intCast(@import("time_compat").nanoTimestamp());
        const seed = ts ^ (@as(u64, @intCast(@as(u32, @bitCast(node_id)))) *% 2654435761);
        self.prng = std.Random.DefaultPrng.init(seed);
        self.reset();
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "RaftLog append and query" {
    var raft_log = RaftLog.init(testing.allocator);
    defer raft_log.deinit();

    const offset0 = try raft_log.append(1, "entry-0");
    const offset1 = try raft_log.append(1, "entry-1");
    const offset2 = try raft_log.append(2, "entry-2");

    try testing.expectEqual(@as(u64, 0), offset0);
    try testing.expectEqual(@as(u64, 1), offset1);
    try testing.expectEqual(@as(u64, 2), offset2);
    try testing.expectEqual(@as(usize, 3), raft_log.length());
    try testing.expectEqual(@as(i32, 2), raft_log.lastEpoch());

    const entry = raft_log.get(1).?;
    try testing.expectEqualStrings("entry-1", entry.data);
}

test "RaftState election" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    try testing.expectEqual(RaftState.Role.unattached, state.role);
    try testing.expectEqual(@as(usize, 3), state.quorumSize());
    try testing.expectEqual(@as(usize, 2), state.majorityThreshold());

    // Start election
    const result = state.startElection();
    try testing.expectEqual(RaftState.Role.candidate, state.role);
    try testing.expectEqual(@as(i32, 1), result.epoch);
    try testing.expectEqual(@as(?i32, 0), state.voted_for);

    // Become leader (after receiving majority votes)
    state.becomeLeader();
    try testing.expectEqual(RaftState.Role.leader, state.role);
    try testing.expectEqual(@as(?i32, 0), state.leader_id);
}

test "RaftState vote request handling" {
    var state = RaftState.init(testing.allocator, 1, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    // Vote for candidate 0 at epoch 1
    const resp1 = state.handleVoteRequest(0, 1, 0, 0);
    try testing.expect(resp1.vote_granted);
    try testing.expectEqual(@as(?i32, 0), state.voted_for);

    // Reject candidate 2 at same epoch (already voted for 0)
    const resp2 = state.handleVoteRequest(2, 1, 0, 0);
    try testing.expect(!resp2.vote_granted);

    // Accept candidate 2 at higher epoch
    const resp3 = state.handleVoteRequest(2, 2, 0, 0);
    try testing.expect(resp3.vote_granted);
    try testing.expectEqual(@as(i32, 2), state.current_epoch);
}

test "RaftState become follower" {
    var state = RaftState.init(testing.allocator, 1, "test-cluster");
    defer state.deinit();

    state.becomeFollower(5, 0);
    try testing.expectEqual(RaftState.Role.follower, state.role);
    try testing.expectEqual(@as(i32, 5), state.current_epoch);
    try testing.expectEqual(@as(?i32, 0), state.leader_id);
}

test "RaftState leader append" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);

    _ = state.startElection();
    state.becomeLeader();

    const offset = try state.appendEntry("topic-create-record");
    try testing.expectEqual(@as(u64, 0), offset);
    try testing.expectEqual(@as(usize, 1), state.log.length());
}

test "MetadataImage basic" {
    var image = MetadataImage.init(testing.allocator);
    defer image.deinit();

    try image.addBroker(0, "localhost", 9092);
    try image.addBroker(1, "localhost", 9093);

    try testing.expectEqual(@as(usize, 2), image.brokerCount());
    try testing.expectEqual(@as(usize, 0), image.topicCount());
}

test "ElectionTimer" {
    var timer = ElectionTimer.init(100, 200);
    // Timer should not be immediately expired (100-200ms in future)
    try testing.expect(!timer.isExpired());
    timer.reset();
    try testing.expect(!timer.isExpired());
}

test "RaftState commit index update" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    _ = state.startElection();
    state.becomeLeader();

    // Append entries
    _ = try state.appendEntry("entry-0");
    _ = try state.appendEntry("entry-1");
    _ = try state.appendEntry("entry-2");

    // Initially commit_index = 0
    try testing.expectEqual(@as(u64, 0), state.commit_index);

    // Simulate follower 1 acknowledging up to offset 2
    state.handleAppendEntriesResponse(1, .{ .success = true, .epoch = 1, .match_index = 2 });

    // Now: node 0 (leader) match = lastOffset()=2, node 1 match = 2, node 2 match = 0
    // Sorted: [0, 2, 2], median at index 3-2=1 → 2
    // commit_index should advance to 2
    try testing.expectEqual(@as(u64, 2), state.commit_index);
}

test "RaftState commit index needs majority" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    _ = state.startElection();
    state.becomeLeader();

    _ = try state.appendEntry("data");

    // Only node 0 has the entry (self), no followers acked yet
    state.updateCommitIndex();
    // Sorted match indices: [0, 0, 0] (leader=lastOffset()=0, but voters initialized with match=0)
    // With only self matching, commit stays 0
    try testing.expectEqual(@as(u64, 0), state.commit_index);
}

test "RaftState raft log persistence" {
    const tmp_dir = "/tmp/automq-raft-persist-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Write entries
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
        defer state.deinit();

        _ = state.startElection();
        state.becomeLeader();

        _ = try state.appendEntry("entry-A");
        _ = try state.appendEntry("entry-B");
        _ = try state.appendEntry("entry-C");

        try testing.expectEqual(@as(usize, 3), state.log.length());
    }

    // Reload and verify
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
        defer state.deinit();

        const recovered = try state.loadPersistedLog();
        try testing.expectEqual(@as(u64, 3), recovered);
        try testing.expectEqual(@as(usize, 3), state.log.length());
    }

    fs.deleteTreeAbsolute(tmp_dir) catch {};
}

test "RaftState leader steps down on higher epoch" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    _ = state.startElection();
    state.becomeLeader();
    try testing.expectEqual(RaftState.Role.leader, state.role);
    try testing.expectEqual(@as(i32, 1), state.current_epoch);

    // Receive BeginQuorumEpoch from node 1 with higher epoch
    state.becomeFollower(5, 1);
    try testing.expectEqual(RaftState.Role.follower, state.role);
    try testing.expectEqual(@as(i32, 5), state.current_epoch);
    try testing.expectEqual(@as(?i32, 1), state.leader_id);
}

test "RaftState rejects vote for stale epoch" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Move to epoch 5 by becoming follower
    state.becomeFollower(5, 1);

    // Vote request with epoch 3 (stale) should be rejected
    const result = state.handleVoteRequest(1, 3, 0, 0);
    try testing.expect(!result.vote_granted);
}

test "RaftState cannot append when not leader" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);

    // Not leader — should fail
    const result = state.appendEntry("should-fail");
    try testing.expectError(error.NotLeader, result);
}

test "RaftState election resets voted_for" {
    var state = RaftState.init(testing.allocator, 0, "test-cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    // Start election — should vote for self
    const result = state.startElection();
    try testing.expectEqual(@as(?i32, 0), state.voted_for);
    try testing.expectEqual(RaftState.Role.candidate, state.role);
    try testing.expect(result.epoch > 0);
}

// ---------------------------------------------------------------
// Raft Invariant Tests
// ---------------------------------------------------------------

test "Election Safety: at most one leader per epoch" {
    // Two nodes both try to become leader in the same epoch.
    // Only one should succeed because each node can only vote once per epoch.
    var state0 = RaftState.init(testing.allocator, 0, "cluster");
    defer state0.deinit();
    var state1 = RaftState.init(testing.allocator, 1, "cluster");
    defer state1.deinit();

    try state0.addVoter(0);
    try state0.addVoter(1);
    try state1.addVoter(0);
    try state1.addVoter(1);

    // Node 0 starts election at epoch 1
    _ = state0.startElection();
    try testing.expectEqual(@as(i32, 1), state0.current_epoch);
    try testing.expectEqual(@as(?i32, 0), state0.voted_for);

    // Node 1 receives vote request from node 0 — grants vote
    const resp1 = state1.handleVoteRequest(0, 1, 0, 0);
    try testing.expect(resp1.vote_granted);
    try testing.expectEqual(@as(?i32, 0), state1.voted_for);

    // Node 1 also tries to start election at epoch 1
    // But it already voted for node 0 at epoch 1, so if node 0 asks again, it re-grants.
    // Node 1 would need to increment to epoch 2 to vote for itself.
    const resp2 = state1.handleVoteRequest(1, 1, 0, 0);
    // Should be rejected: already voted for 0 at epoch 1
    try testing.expect(!resp2.vote_granted);
}

test "Vote request rejects candidate with stale log" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Give this node some log entries at epoch 3
    state.role = .leader;
    state.current_epoch = 3;
    _ = try state.appendEntry("entry-at-epoch-3");
    state.role = .follower;

    // Candidate at epoch 4 but with empty log (stale) should be rejected
    const resp = state.handleVoteRequest(0, 4, 0, 0);
    try testing.expect(!resp.vote_granted); // Our log is more up-to-date
}

test "Vote request accepts candidate with more up-to-date log" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Our log: one entry at epoch 1
    state.role = .leader;
    state.current_epoch = 1;
    _ = try state.appendEntry("old-entry");
    state.role = .follower;

    // Candidate at epoch 2 with log at epoch 2 offset 5 — more up-to-date
    const resp = state.handleVoteRequest(0, 2, 5, 2);
    try testing.expect(resp.vote_granted);
}

test "Vote request grants re-vote to same candidate" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Vote for candidate 0 at epoch 1
    const resp1 = state.handleVoteRequest(0, 1, 0, 0);
    try testing.expect(resp1.vote_granted);

    // Same candidate, same epoch — should re-grant (idempotent)
    const resp2 = state.handleVoteRequest(0, 1, 0, 0);
    try testing.expect(resp2.vote_granted);
}

test "Vote granting resets election timer" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Force election timer to expire
    state.election_timer.deadline_ms = 0;
    try testing.expect(state.isElectionTimedOut());

    // Granting a vote should reset the timer
    _ = state.handleVoteRequest(0, 1, 0, 0);
    try testing.expect(!state.isElectionTimedOut());
}

test "becomeFollower ignores stale epoch" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Move to epoch 5
    state.becomeFollower(5, 1);
    try testing.expectEqual(@as(i32, 5), state.current_epoch);

    // Try to step down to epoch 3 — should be ignored
    state.becomeFollower(3, 2);
    try testing.expectEqual(@as(i32, 5), state.current_epoch);
    try testing.expectEqual(@as(?i32, 1), state.leader_id); // Still leader 1
}

test "handleAppendEntriesResponse step-down becomes unattached" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    _ = state.startElection();
    state.becomeLeader();
    try testing.expectEqual(RaftState.Role.leader, state.role);

    // Follower responds with higher epoch — leader must step down
    state.handleAppendEntriesResponse(1, .{ .success = false, .epoch = 10, .match_index = 0 });
    try testing.expectEqual(RaftState.Role.unattached, state.role);
    try testing.expectEqual(@as(i32, 10), state.current_epoch);
    try testing.expectEqual(@as(?i32, null), state.leader_id);
}

test "handleAppendEntriesResponse decrements next_index on failure" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    _ = state.startElection();
    state.becomeLeader();

    // Append a few entries so next_index is > 0
    _ = try state.appendEntry("e1");
    _ = try state.appendEntry("e2");

    if (state.voters.getPtr(1)) |v| {
        v.next_index = state.log.nextOffset();
    }

    // Get initial next_index for follower
    const initial_next = if (state.voters.getPtr(1)) |v| v.next_index else 0;
    try testing.expect(initial_next > 0);

    // Follower rejects (log mismatch) — next_index should decrement
    state.handleAppendEntriesResponse(1, .{ .success = false, .epoch = 1, .match_index = 0 });
    const new_next = if (state.voters.getPtr(1)) |v| v.next_index else 0;
    try testing.expectEqual(initial_next - 1, new_next);
}

test "Commit index requires current epoch entry (Raft Figure 8 scenario)" {
    // This tests the critical Raft safety property from Figure 8 of the paper.
    // A leader cannot commit entries from a previous epoch by counting replicas alone.
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    // Epoch 1: node 0 is leader, appends entry
    _ = state.startElection(); // epoch 1
    state.becomeLeader();
    _ = try state.appendEntry("epoch-1-entry"); // offset 0, epoch 1

    // Now simulate: node 0 loses leadership, node 1 wins epoch 2
    // Node 0 comes back as leader in epoch 3 but hasn't yet appended any epoch-3 entries
    state.current_epoch = 3;
    // Reset match indices for fresh leadership
    var it = state.voters.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.match_index = 0;
        entry.value_ptr.next_index = state.log.lastOffset() + 1;
    }

    // Follower 1 acks the epoch-1 entry (offset 0)
    state.handleAppendEntriesResponse(1, .{ .success = true, .epoch = 3, .match_index = 0 });

    // Even though majority (nodes 0 and 1) have offset 0, it's an epoch-1 entry.
    // The leader at epoch 3 must NOT commit it until an epoch-3 entry is replicated.
    try testing.expectEqual(@as(u64, 0), state.commit_index);

    // Now append an epoch-3 entry
    _ = try state.appendEntry("epoch-3-entry"); // offset 1, epoch 3

    // Follower 1 acks up to offset 1
    state.handleAppendEntriesResponse(1, .{ .success = true, .epoch = 3, .match_index = 1 });

    // Now offset 1 (epoch 3) is replicated on majority — commit should advance
    try testing.expectEqual(@as(u64, 1), state.commit_index);
}

test "RaftLog truncateFrom removes conflicting entries" {
    var raft_log = RaftLog.init(testing.allocator);
    defer raft_log.deinit();

    _ = try raft_log.append(1, "entry-0");
    _ = try raft_log.append(1, "entry-1");
    _ = try raft_log.append(2, "entry-2"); // This might conflict with new leader
    _ = try raft_log.append(2, "entry-3");

    try testing.expectEqual(@as(usize, 4), raft_log.length());

    // Truncate from offset 2 (new leader has different entries at offset 2+)
    raft_log.truncateFrom(2);
    try testing.expectEqual(@as(usize, 2), raft_log.length());
    try testing.expectEqual(@as(u64, 1), raft_log.lastOffset());
}

test "RaftLog hasConflict detects epoch mismatch" {
    var raft_log = RaftLog.init(testing.allocator);
    defer raft_log.deinit();

    _ = try raft_log.append(1, "entry-at-epoch-1");
    _ = try raft_log.append(2, "entry-at-epoch-2");

    // Same epoch — no conflict
    try testing.expect(!raft_log.hasConflict(0, 1));
    try testing.expect(!raft_log.hasConflict(1, 2));

    // Different epoch — conflict
    try testing.expect(raft_log.hasConflict(0, 3));
    try testing.expect(raft_log.hasConflict(1, 1));

    // No entry at offset — no conflict
    try testing.expect(!raft_log.hasConflict(5, 1));
}

test "Split vote scenario: both candidates at same epoch" {
    // In a 3-node cluster, two nodes start elections simultaneously.
    // Each votes for itself. The third node can only vote for one.
    var state0 = RaftState.init(testing.allocator, 0, "cluster");
    defer state0.deinit();
    var state1 = RaftState.init(testing.allocator, 1, "cluster");
    defer state1.deinit();
    var state2 = RaftState.init(testing.allocator, 2, "cluster");
    defer state2.deinit();

    for ([_]*RaftState{ &state0, &state1, &state2 }) |s| {
        try s.addVoter(0);
        try s.addVoter(1);
        try s.addVoter(2);
    }

    // Both start elections — both get epoch 1, both vote for themselves
    _ = state0.startElection();
    _ = state1.startElection();

    // Node 2 receives vote request from node 0 first — grants
    const resp0 = state2.handleVoteRequest(0, 1, 0, 0);
    try testing.expect(resp0.vote_granted);

    // Node 2 receives vote request from node 1 at same epoch — MUST reject
    const resp1 = state2.handleVoteRequest(1, 1, 0, 0);
    try testing.expect(!resp1.vote_granted);

    // Result: node 0 has 2 votes (self + node 2), node 1 has 1 vote (self only)
    // Only node 0 should become leader
}

test "Consecutive elections increment epoch" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    // Multiple failed elections should keep incrementing epoch
    const r1 = state.startElection();
    try testing.expectEqual(@as(i32, 1), r1.epoch);

    const r2 = state.startElection();
    try testing.expectEqual(@as(i32, 2), r2.epoch);

    const r3 = state.startElection();
    try testing.expectEqual(@as(i32, 3), r3.epoch);

    // Epoch should be monotonically increasing
    try testing.expectEqual(@as(i32, 3), state.current_epoch);
}

test "Leader cannot be created without majority" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    try state.addVoter(2);

    // With 3 voters, majority = 2
    try testing.expectEqual(@as(usize, 2), state.majorityThreshold());

    // Self-vote gives us 1 vote — not enough
    _ = state.startElection();
    try testing.expectEqual(RaftState.Role.candidate, state.role);

    // Only 1 vote (self) — quorumSize > 1, so single-node auto-promote doesn't apply
    try testing.expect(state.quorumSize() > 1);
}

test "Single-node cluster immediately becomes leader" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try testing.expectEqual(@as(usize, 1), state.quorumSize());
    try testing.expectEqual(@as(usize, 1), state.majorityThreshold());

    _ = state.startElection();
    // In single-node, quorumSize <= 1, so the election loop auto-promotes
    if (state.quorumSize() <= 1) {
        state.becomeLeader();
    }
    try testing.expectEqual(RaftState.Role.leader, state.role);
}

test "RaftState persists and loads epoch and voted_for" {
    const tmp_dir = "/tmp/zmq-raft-meta-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Write epoch and voted_for
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test", tmp_dir);
        defer state.deinit();

        _ = state.startElection(); // epoch=1, voted_for=0
        try testing.expectEqual(@as(i32, 1), state.current_epoch);
        try testing.expectEqual(@as(?i32, 0), state.voted_for);
    }

    // Reload and verify persisted state
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test", tmp_dir);
        defer state.deinit();

        const loaded = state.loadPersistedMeta();
        try testing.expect(loaded);
        try testing.expectEqual(@as(i32, 1), state.current_epoch);
        try testing.expectEqual(@as(?i32, 0), state.voted_for);
    }
}

test "RaftState voted_for persists prevents double-voting after restart" {
    const tmp_dir = "/tmp/zmq-raft-double-vote-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Node 1 votes for candidate 0 at epoch 1
    {
        var state = RaftState.initWithDataDir(testing.allocator, 1, "test", tmp_dir);
        defer state.deinit();

        const resp = state.handleVoteRequest(0, 1, 0, 0);
        try testing.expect(resp.vote_granted);
        try testing.expectEqual(@as(?i32, 0), state.voted_for);
    }

    // Node 1 restarts — should NOT vote for candidate 2 at epoch 1
    {
        var state = RaftState.initWithDataDir(testing.allocator, 1, "test", tmp_dir);
        defer state.deinit();

        _ = state.loadPersistedMeta();
        // voted_for should still be 0 from before crash
        try testing.expectEqual(@as(?i32, 0), state.voted_for);
        try testing.expectEqual(@as(i32, 1), state.current_epoch);

        // Attempt to vote for different candidate at same epoch — MUST be rejected
        const resp = state.handleVoteRequest(2, 1, 0, 0);
        try testing.expect(!resp.vote_granted);
    }
}

test "RaftState handleAppendEntries validates prev_log" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    // Become follower at epoch 1
    state.becomeFollower(1, 0);

    // Append some entries as if leader replicated them
    _ = try state.log.append(1, "entry-0");
    _ = try state.log.append(1, "entry-1");

    // AppendEntries with valid prev_log should succeed
    const resp1 = state.handleAppendEntries(1, 0, 1, 1, &.{}, 0);
    try testing.expect(resp1.success);

    // AppendEntries with wrong prev_log_epoch should fail
    const resp2 = state.handleAppendEntries(1, 0, 1, 99, &.{}, 0);
    try testing.expect(!resp2.success);
}

test "RaftState handleAppendEntries truncates conflicting entries" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    state.becomeFollower(1, 0);

    // Append entries from old leader (epoch 1)
    _ = try state.log.append(1, "old-entry-0");
    _ = try state.log.append(1, "old-entry-1");
    _ = try state.log.append(1, "old-entry-2"); // This will conflict
    try testing.expectEqual(@as(usize, 3), state.log.length());

    // New leader at epoch 2 sends entry at offset 2 with different epoch
    const new_entries = [_]RaftState.AppendEntry{
        .{ .offset = 2, .epoch = 2, .data = "new-entry-2" },
    };
    const resp = state.handleAppendEntries(2, 0, 1, 1, &new_entries, 0);
    try testing.expect(resp.success);

    // Old entry-2 should be truncated and replaced
    try testing.expectEqual(@as(usize, 3), state.log.length());
    const entry2 = state.log.get(2).?;
    try testing.expectEqual(@as(i32, 2), entry2.epoch);
}

test "RaftState handleAppendEntries advances commit_index" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);

    state.becomeFollower(1, 0);
    _ = try state.log.append(1, "entry-0");
    _ = try state.log.append(1, "entry-1");

    try testing.expectEqual(@as(u64, 0), state.commit_index);

    // Leader says commit_index=1
    const resp = state.handleAppendEntries(1, 0, 1, 1, &.{}, 1);
    try testing.expect(resp.success);
    try testing.expectEqual(@as(u64, 1), state.commit_index);
}

test "RaftState handleAppendEntries persists follower entries" {
    const tmp_dir = "/tmp/zmq-raft-follower-append-persist-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    {
        var state = RaftState.initWithDataDir(testing.allocator, 1, "cluster", tmp_dir);
        defer state.deinit();

        const entries = [_]RaftState.AppendEntry{
            .{ .offset = 0, .epoch = 3, .data = "replicated-controller-record" },
        };
        const resp = state.handleAppendEntries(3, 0, 0, 0, &entries, 0);
        try testing.expect(resp.success);
        try testing.expectEqual(@as(usize, 1), state.log.length());
    }

    {
        var state = RaftState.initWithDataDir(testing.allocator, 1, "cluster", tmp_dir);
        defer state.deinit();

        const recovered = try state.loadPersistedLog();
        try testing.expectEqual(@as(u64, 1), recovered);
        try testing.expectEqual(@as(usize, 1), state.log.length());
        try testing.expectEqualStrings("replicated-controller-record", state.log.entries.items[0].data);
    }
}

test "RaftState pre-vote does not change epoch" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();
    try state.addVoter(0);
    try state.addVoter(1);

    const before_epoch = state.current_epoch;
    const result = state.startPreVote();
    // Pre-vote should NOT change current_epoch
    try testing.expectEqual(before_epoch, state.current_epoch);
    // But should return tentative next epoch
    try testing.expectEqual(before_epoch + 1, result.epoch);
}

test "RaftState pre-vote rejected when leader is alive" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();
    try state.addVoter(0);
    try state.addVoter(1);

    // Simulate recent heartbeat (timer not expired)
    state.election_timer.reset();

    // Pre-vote should be rejected (we recently heard from leader)
    const resp = state.handlePreVoteRequest(0, 1, 0, 0);
    try testing.expect(!resp.vote_granted);
}

test "RaftState pre-vote granted when no leader heartbeat" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();
    try state.addVoter(0);
    try state.addVoter(1);

    // Force election timer to expire (no recent heartbeat)
    state.election_timer.deadline_ms = 0;

    const resp = state.handlePreVoteRequest(0, 1, 0, 0);
    try testing.expect(resp.vote_granted);
    // Our state should NOT have changed
    try testing.expectEqual(@as(?i32, null), state.voted_for);
    try testing.expectEqual(@as(i32, 0), state.current_epoch);
}

test "RaftState pre-vote rejected for stale log" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();
    try state.addVoter(0);
    try state.addVoter(1);

    // Give us a log entry
    state.role = .leader;
    state.current_epoch = 3;
    _ = try state.appendEntry("data");
    state.role = .follower;

    // Force timer expired
    state.election_timer.deadline_ms = 0;

    // Candidate with empty log should be rejected
    const resp = state.handlePreVoteRequest(0, 4, 0, 0);
    try testing.expect(!resp.vote_granted);
}

test "ElectionTimer reseed produces different values for different nodes" {
    var timer0 = ElectionTimer.init(1000, 2000);
    var timer1 = ElectionTimer.init(1000, 2000);

    timer0.reseed(0);
    timer1.reseed(1);

    // Deadlines should differ (different seeds) — not guaranteed but very likely
    // We just verify reseed doesn't crash and produces valid deadlines
    try testing.expect(timer0.deadline_ms > 0);
    try testing.expect(timer1.deadline_ms > 0);
}

test "RaftLog truncateBefore removes old entries" {
    var raft_log = RaftLog.init(testing.allocator);
    defer raft_log.deinit();

    _ = try raft_log.append(1, "e0");
    _ = try raft_log.append(1, "e1");
    _ = try raft_log.append(1, "e2");
    _ = try raft_log.append(2, "e3");
    _ = try raft_log.append(2, "e4");

    try testing.expectEqual(@as(usize, 5), raft_log.length());

    // Keep entries with offset >= 3
    raft_log.truncateBefore(3);
    try testing.expectEqual(@as(usize, 2), raft_log.length());
    try testing.expectEqual(@as(u64, 3), raft_log.entries.items[0].offset);
    try testing.expectEqual(@as(u64, 4), raft_log.entries.items[1].offset);
}

test "RaftState takeSnapshot truncates committed entries" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    // Append several entries
    _ = try state.appendEntry("e0");
    _ = try state.appendEntry("e1");
    _ = try state.appendEntry("e2");
    _ = try state.appendEntry("e3");
    _ = try state.appendEntry("e4");

    // Manually advance commit_index (simulating majority ack)
    state.commit_index = 3;

    try testing.expectEqual(@as(usize, 5), state.log.length());

    // Take snapshot at commit_index=3
    state.takeSnapshot();
    try testing.expectEqual(@as(u64, 3), state.last_snapshot_offset);
    // Entries 0,1,2 removed; entries 3,4 kept
    try testing.expectEqual(@as(usize, 2), state.log.length());
}

test "RaftState shouldSnapshot checks log size" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    // Below threshold
    try testing.expect(!state.shouldSnapshot(10));

    // Add enough entries
    var i: usize = 0;
    while (i < 15) : (i += 1) {
        _ = try state.appendEntry("data");
    }

    // Above threshold
    try testing.expect(state.shouldSnapshot(10));
    try testing.expect(!state.shouldSnapshot(20));
}

test "RaftState proposeAddVoter appends config entry" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    // Propose adding voter 1
    const offset = try state.proposeAddVoter(1);
    try testing.expectEqual(@as(u64, 0), offset);
    try testing.expect(state.pending_config_change);

    // Verify it's in the log
    try testing.expectEqual(@as(usize, 1), state.log.length());
    const entry = state.log.get(0).?;
    try testing.expect(RaftState.ConfigChangeEntry.isConfigChange(entry.data));
}

test "RaftState proposeAddVoter rejects duplicate" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    const result = state.proposeAddVoter(0); // Already a voter
    try testing.expectError(error.VoterAlreadyExists, result);
}

test "RaftState proposeAddVoter rejects when config pending" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    _ = try state.proposeAddVoter(1); // First one succeeds
    const result = state.proposeAddVoter(2); // Second rejected
    try testing.expectError(error.ConfigChangePending, result);
}

test "RaftState proposeAddVoterWithMetadata applies endpoint metadata after commit" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    const directory_id = [_]u8{3} ** 16;
    const endpoints = [_]RaftState.VoterEndpointView{.{
        .name = "CONTROLLER",
        .host = "controller-2.example",
        .port = 29093,
    }};

    const offset = try state.proposeAddVoterWithMetadata(2, directory_id, &endpoints);
    try testing.expectEqual(@as(u64, 0), offset);
    try testing.expect(state.pending_config_change);
    try testing.expect(!state.voters.contains(2));

    state.commit_index = offset;
    state.applyCommittedConfigs();

    try testing.expect(!state.pending_config_change);
    const voter = state.voters.get(2).?;
    try testing.expectEqualSlices(u8, &directory_id, &voter.voter_directory_id);
    try testing.expectEqual(@as(usize, 1), voter.endpoints.len);
    try testing.expectEqualStrings("CONTROLLER", voter.endpoints[0].name);
    try testing.expectEqualStrings("controller-2.example", voter.endpoints[0].host);
    try testing.expectEqual(@as(u16, 29093), voter.endpoints[0].port);
}

test "RaftState proposeRemoveVoter works" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    _ = state.startElection();
    state.becomeLeader();

    const offset = try state.proposeRemoveVoter(1);
    try testing.expect(offset >= 0);
    try testing.expect(state.pending_config_change);
}

test "RaftState proposeRemoveVoter rejects last voter" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    const result = state.proposeRemoveVoter(0);
    try testing.expectError(error.CannotRemoveLastVoter, result);
}

test "RaftState applyCommittedConfigs adds voter" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    _ = state.startElection();
    state.becomeLeader();

    // Propose and commit
    _ = try state.proposeAddVoter(1);
    try testing.expectEqual(@as(usize, 1), state.voters.count()); // Not yet applied

    // Simulate commit (manually advance commit_index)
    state.commit_index = 0;
    state.applyCommittedConfigs();

    try testing.expectEqual(@as(usize, 2), state.voters.count());
    try testing.expect(state.voters.contains(1));
    try testing.expect(!state.pending_config_change);
}

test "RaftState applyCommittedConfigs removes voter" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    _ = state.startElection();
    state.becomeLeader();

    _ = try state.proposeRemoveVoter(1);
    state.commit_index = 0;
    state.applyCommittedConfigs();

    try testing.expectEqual(@as(usize, 1), state.voters.count());
    try testing.expect(!state.voters.contains(1));
}

test "RaftState removing self causes resignation" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    _ = state.startElection();
    state.becomeLeader();

    _ = try state.proposeRemoveVoter(0); // Remove self
    state.commit_index = 0;
    state.applyCommittedConfigs();

    try testing.expectEqual(RaftState.Role.resigned, state.role);
}

test "RaftState proposeUpdateVoter applies endpoint metadata after commit" {
    var state = RaftState.init(testing.allocator, 0, "cluster");
    defer state.deinit();

    try state.addVoter(0);
    try state.addVoter(1);
    _ = state.startElection();
    state.becomeLeader();

    const directory_id = [_]u8{1} ** 16;
    const endpoints = [_]RaftState.VoterEndpointView{.{
        .name = "CONTROLLER",
        .host = "controller-1.example",
        .port = 19093,
    }};

    const offset = try state.proposeUpdateVoter(1, directory_id, &endpoints, 1, 2);
    try testing.expectEqual(@as(u64, 0), offset);
    try testing.expect(state.pending_config_change);

    state.commit_index = offset;
    state.applyCommittedConfigs();

    try testing.expect(!state.pending_config_change);
    const voter = state.voters.get(1).?;
    try testing.expectEqualSlices(u8, &directory_id, &voter.voter_directory_id);
    try testing.expectEqual(@as(i16, 1), voter.k_raft_min_supported_version);
    try testing.expectEqual(@as(i16, 2), voter.k_raft_max_supported_version);
    try testing.expectEqual(@as(usize, 1), voter.endpoints.len);
    try testing.expectEqualStrings("CONTROLLER", voter.endpoints[0].name);
    try testing.expectEqualStrings("controller-1.example", voter.endpoints[0].host);
    try testing.expectEqual(@as(u16, 19093), voter.endpoints[0].port);
}

test "RaftState UpdateVoter config entry serializes and deserializes endpoints" {
    const directory_id = [_]u8{2} ** 16;
    const endpoints = [_]RaftState.VoterEndpointView{
        .{ .name = "CONTROLLER", .host = "controller-a", .port = 9093 },
        .{ .name = "SSL", .host = "controller-b", .port = 9094 },
    };

    const data = try RaftState.ConfigChangeEntry.serializeVoterUpdate(testing.allocator, 2, directory_id, &endpoints, 0, 3);
    defer testing.allocator.free(data);

    try testing.expect(RaftState.ConfigChangeEntry.isConfigChange(data));
    var update = (try RaftState.ConfigChangeEntry.deserializeVoterUpdate(testing.allocator, data)).?;
    defer update.deinit(testing.allocator);

    try testing.expectEqual(@as(i32, 2), update.voter_id);
    try testing.expectEqualSlices(u8, &directory_id, &update.voter_directory_id);
    try testing.expectEqual(@as(i16, 0), update.k_raft_min_supported_version);
    try testing.expectEqual(@as(i16, 3), update.k_raft_max_supported_version);
    try testing.expectEqual(@as(usize, 2), update.endpoints.len);
    try testing.expectEqualStrings("CONTROLLER", update.endpoints[0].name);
    try testing.expectEqualStrings("controller-a", update.endpoints[0].host);
    try testing.expectEqual(@as(u16, 9094), update.endpoints[1].port);
}

test "ConfigChangeEntry serialize and deserialize round-trip" {
    const add = RaftState.ConfigChangeEntry{ .change_type = .add_voter, .voter_id = 42 };
    const buf = add.serialize();
    const parsed = RaftState.ConfigChangeEntry.deserialize(&buf).?;
    try testing.expectEqual(RaftState.ConfigChangeType.add_voter, parsed.change_type);
    try testing.expectEqual(@as(i32, 42), parsed.voter_id);

    const remove = RaftState.ConfigChangeEntry{ .change_type = .remove_voter, .voter_id = 7 };
    const buf2 = remove.serialize();
    const parsed2 = RaftState.ConfigChangeEntry.deserialize(&buf2).?;
    try testing.expectEqual(RaftState.ConfigChangeType.remove_voter, parsed2.change_type);
    try testing.expectEqual(@as(i32, 7), parsed2.voter_id);
}

test "ConfigChangeEntry isConfigChange detects correctly" {
    const config = RaftState.ConfigChangeEntry{ .change_type = .add_voter, .voter_id = 1 };
    const buf = config.serialize();
    try testing.expect(RaftState.ConfigChangeEntry.isConfigChange(&buf));

    // Regular data should NOT be detected as config change
    try testing.expect(!RaftState.ConfigChangeEntry.isConfigChange("hello world"));
    try testing.expect(!RaftState.ConfigChangeEntry.isConfigChange(""));
}

// ---------------------------------------------------------------
// Persistence and AppendEntries round-trip tests
// ---------------------------------------------------------------

test "RaftState loadPersistedMeta round-trip" {
    const tmp_dir = "/tmp/zmq-raft-meta-roundtrip-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Phase 1: start election to get epoch=1, voted_for=self, then drop state
    {
        var state = RaftState.initWithDataDir(testing.allocator, 5, "test-cluster", tmp_dir);
        defer state.deinit();
        try state.addVoter(5);
        try state.addVoter(6);

        _ = state.startElection(); // epoch=1, voted_for=5, persisted automatically
        try testing.expectEqual(@as(i32, 1), state.current_epoch);
        try testing.expectEqual(@as(?i32, 5), state.voted_for);
    }

    // Phase 2: fresh state, load from disk, verify epoch and voted_for survived
    {
        var state = RaftState.initWithDataDir(testing.allocator, 5, "test-cluster", tmp_dir);
        defer state.deinit();

        const loaded = state.loadPersistedMeta();
        try testing.expect(loaded);
        try testing.expectEqual(@as(i32, 1), state.current_epoch);
        try testing.expectEqual(@as(?i32, 5), state.voted_for);
    }
}

test "RaftState loadPersistedMeta returns false without file" {
    const tmp_dir = "/tmp/zmq-raft-meta-nofile-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Create the directory but do NOT write a raft.meta file
    fs.makeDirAbsolute(tmp_dir) catch {};

    var state = RaftState.initWithDataDir(testing.allocator, 1, "test-cluster", tmp_dir);
    defer state.deinit();

    const loaded = state.loadPersistedMeta();
    try testing.expect(!loaded);
    // Epoch should remain at default (0)
    try testing.expectEqual(@as(i32, 0), state.current_epoch);
    try testing.expectEqual(@as(?i32, null), state.voted_for);
}

test "RaftState loadPersistedLog replays entries" {
    const tmp_dir = "/tmp/zmq-raft-log-replay-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Phase 1: leader appends 3 entries, persisted to disk automatically
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
        defer state.deinit();
        try state.addVoter(0);

        _ = state.startElection();
        state.becomeLeader();

        _ = try state.appendEntry("alpha");
        _ = try state.appendEntry("bravo");
        _ = try state.appendEntry("charlie");
        try testing.expectEqual(@as(usize, 3), state.log.length());
    }

    // Phase 2: fresh state, loadPersistedLog should recover all 3 entries
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
        defer state.deinit();

        const recovered = try state.loadPersistedLog();
        try testing.expectEqual(@as(u64, 3), recovered);
        try testing.expectEqual(@as(usize, 3), state.log.length());

        // Verify entry content survived the round-trip
        const entry0 = state.log.get(0).?;
        try testing.expectEqualStrings("alpha", entry0.data);
        const entry2 = state.log.get(2).?;
        try testing.expectEqualStrings("charlie", entry2.data);
    }
}

test "RaftState loadPersistedLog handles empty log" {
    const tmp_dir = "/tmp/zmq-raft-log-empty-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Create the directory and an empty raft.log file
    fs.makeDirAbsolute(tmp_dir) catch {};
    {
        const path = tmp_dir ++ "/raft.log";
        const file = try fs.createFileAbsolute(path, .{ .truncate = true });
        file.close();
    }

    var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
    defer state.deinit();

    const recovered = try state.loadPersistedLog();
    try testing.expectEqual(@as(u64, 0), recovered);
    try testing.expectEqual(@as(usize, 0), state.log.length());
}

test "RaftState getAppendEntriesForFollower returns null when not leader" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(1);
    try state.addVoter(2);

    // Node is a follower — should return null
    state.becomeFollower(1, 2);
    try testing.expectEqual(RaftState.Role.follower, state.role);

    const result = state.getAppendEntriesForFollower(2);
    try testing.expect(result == null);
}

test "RaftState getAppendEntriesForFollower returns entries for lagging follower" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(1);
    try state.addVoter(2);

    _ = state.startElection();
    state.becomeLeader();

    // Append 5 entries as leader
    _ = try state.appendEntry("e0");
    _ = try state.appendEntry("e1");
    _ = try state.appendEntry("e2");
    _ = try state.appendEntry("e3");
    _ = try state.appendEntry("e4");

    // After becomeLeader(), voter 2's next_index was initialized to lastOffset()+1
    // at the time of becomeLeader (which was 0+1=1 since log was empty then).
    // But we want to simulate a lagging follower. Manually set next_index to 2.
    if (state.voters.getPtr(2)) |v| {
        v.next_index = 2;
    }

    const result = state.getAppendEntriesForFollower(2);
    try testing.expect(result != null);

    const req = result.?;
    try testing.expectEqual(@as(i32, 1), req.leader_id);
    try testing.expectEqual(state.current_epoch, req.epoch);
    try testing.expectEqual(@as(u64, 2), req.entries_start_index);
    // Entries at offsets 2, 3, 4 should be pending for this follower
    try testing.expectEqual(@as(usize, 3), req.entries_count);
    // prev_log should reference the entry just before next_index (offset 1)
    try testing.expectEqual(@as(u64, 1), req.prev_log_offset);
    try testing.expectEqual(state.current_epoch, req.prev_log_epoch);
    try testing.expectEqual(state.commit_index, req.leader_commit);
}

test "RaftState first entry remains replicable after empty heartbeat" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(1);
    try state.addVoter(2);

    _ = state.startElection();
    state.becomeLeader();

    const empty = state.getAppendEntriesForFollower(2) orelse return error.UnexpectedNull;
    try testing.expectEqual(@as(u64, 0), empty.entries_start_index);
    try testing.expectEqual(@as(usize, 0), empty.entries_count);

    state.handleAppendEntriesResponse(2, .{
        .success = true,
        .epoch = state.current_epoch,
        .match_index = 0,
    });
    try testing.expectEqual(@as(u64, 0), state.voters.get(2).?.next_index);

    _ = try state.appendEntry("first-entry");

    const first = state.getAppendEntriesForFollower(2) orelse return error.UnexpectedNull;
    try testing.expectEqual(@as(u64, 0), first.entries_start_index);
    try testing.expectEqual(@as(usize, 1), first.entries_count);
}

test "RaftState getAppendEntriesForFollower returns null for unknown voter" {
    var state = RaftState.init(testing.allocator, 1, "cluster");
    defer state.deinit();

    try state.addVoter(1);
    try state.addVoter(2);

    _ = state.startElection();
    state.becomeLeader();

    _ = try state.appendEntry("data");

    // Voter 999 is not in the cluster — should return null
    const result = state.getAppendEntriesForFollower(999);
    try testing.expect(result == null);
}

test "RaftState loadSnapshotMeta round-trip" {
    const tmp_dir = "/tmp/zmq-raft-snapshot-roundtrip-test";
    fs.deleteTreeAbsolute(tmp_dir) catch {};
    defer fs.deleteTreeAbsolute(tmp_dir) catch {};

    // Phase 1: leader appends entries, commits, takes snapshot
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
        defer state.deinit();
        try state.addVoter(0);

        _ = state.startElection();
        state.becomeLeader();

        _ = try state.appendEntry("snap-e0");
        _ = try state.appendEntry("snap-e1");
        _ = try state.appendEntry("snap-e2");
        _ = try state.appendEntry("snap-e3");

        // Manually advance commit_index to simulate majority ack
        state.commit_index = 3;
        state.takeSnapshot();

        try testing.expectEqual(@as(u64, 3), state.last_snapshot_offset);
        try testing.expectEqual(@as(i32, 1), state.last_snapshot_epoch);
    }

    // Phase 2: fresh state, loadSnapshotMeta should recover offset and epoch
    {
        var state = RaftState.initWithDataDir(testing.allocator, 0, "test-cluster", tmp_dir);
        defer state.deinit();

        const loaded = state.loadSnapshotMeta();
        try testing.expect(loaded);
        try testing.expectEqual(@as(u64, 3), state.last_snapshot_offset);
        try testing.expectEqual(@as(i32, 1), state.last_snapshot_epoch);
    }
}

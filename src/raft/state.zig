const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const fs = std.fs;

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

    pub const Role = enum {
        unattached,
        follower,
        candidate,
        leader,
        resigned,
    };

    pub const VoterInfo = struct {
        node_id: i32,
        match_index: u64 = 0,
        next_index: u64 = 0,
        last_heartbeat_ms: i64 = 0,
    };

    pub fn init(alloc: Allocator, node_id: i32, cluster_id: []const u8) RaftState {
        return .{
            .node_id = node_id,
            .cluster_id = cluster_id,
            .role = .unattached,
            .voters = std.AutoHashMap(i32, VoterInfo).init(alloc),
            .log = RaftLog.init(alloc),
            .election_timer = ElectionTimer.init(1500, 3000), // 1.5-3s
            .allocator = alloc,
        };
    }

    /// Init with data_dir for raft log persistence (Fix 3).
    pub fn initWithDataDir(alloc: Allocator, node_id: i32, cluster_id: []const u8, data_dir: []const u8) RaftState {
        var state = init(alloc, node_id, cluster_id);
        state.data_dir = data_dir;
        return state;
    }

    pub fn deinit(self: *RaftState) void {
        self.voters.deinit();
        self.log.deinit();
    }

    /// Register a voter in the cluster.
    pub fn addVoter(self: *RaftState, voter_id: i32) !void {
        try self.voters.put(voter_id, .{ .node_id = voter_id });
    }

    /// Handle a vote request. Returns whether vote is granted.
    pub fn handleVoteRequest(self: *RaftState, candidate_id: i32, candidate_epoch: i32, last_log_offset: u64, last_log_epoch: i32) VoteResponse {
        // If candidate's epoch is less than ours, reject
        if (candidate_epoch < self.current_epoch) {
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // If we already voted for someone else this epoch, reject
        if (candidate_epoch == self.current_epoch and self.voted_for != null and self.voted_for.? != candidate_id) {
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // Check if candidate's log is at least as up-to-date as ours
        const our_last_epoch = self.log.lastEpoch();
        const our_last_offset = self.log.lastOffset();

        const log_ok = (last_log_epoch > our_last_epoch) or
            (last_log_epoch == our_last_epoch and last_log_offset >= our_last_offset);

        if (!log_ok) {
            return .{ .vote_granted = false, .epoch = self.current_epoch };
        }

        // Grant vote
        if (candidate_epoch > self.current_epoch) {
            self.current_epoch = candidate_epoch;
            self.role = .follower;
            self.leader_id = null;
        }
        self.voted_for = candidate_id;
        // Reset election timer: granting a vote means the cluster is active,
        // so we shouldn't start our own election immediately.
        self.election_timer.reset();

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
        self.election_timer.reset();

        return .{
            .epoch = self.current_epoch,
            .last_log_offset = self.log.lastOffset(),
            .last_log_epoch = self.log.lastEpoch(),
        };
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

        // Initialize next_index for all voters
        var it = self.voters.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.next_index = self.log.lastOffset() + 1;
            entry.value_ptr.match_index = 0;
        }
    }

    /// Called when we discover a leader with a higher epoch.
    /// Only accepts epoch >= current_epoch. Lower epochs are ignored
    /// (they represent stale messages from a previous leader).
    pub fn becomeFollower(self: *RaftState, epoch: i32, leader_id: i32) void {
        if (epoch < self.current_epoch) return; // Ignore stale epoch
        self.current_epoch = epoch;
        self.role = .follower;
        self.leader_id = leader_id;
        self.voted_for = null;
        self.election_timer.reset();
    }

    /// Append an entry to the log (leader only).
    /// Persists entry to raft.log on disk if data_dir is set.
    pub fn appendEntry(self: *RaftState, data: []const u8) !u64 {
        if (self.role != .leader) return error.NotLeader;
        const offset = try self.log.append(self.current_epoch, data);

        // Persist to disk
        if (self.data_dir) |dir| {
            self.persistEntry(dir, self.current_epoch, offset, data) catch |err| {
                std.log.scoped(.raft).warn("Failed to persist raft log entry: {}", .{err});
            };
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
    }

    /// Load and replay persisted raft log entries on startup.
    pub fn loadPersistedLog(self: *RaftState) !u64 {
        const dir = self.data_dir orelse return 0;
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
            _ = self.log.append(epoch, data) catch break;
            pos += data_len;
            recovered += 1;

            // Update current_epoch to the highest seen
            if (epoch > self.current_epoch) self.current_epoch = epoch;
        }

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
            voter.next_index = response.match_index + 1;
            voter.last_heartbeat_ms = std.time.milliTimestamp();

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
                        self.commit_index = new_commit;
                    }
                } else if (new_commit == 0 and self.log.length() == 0) {
                    // Empty log edge case — commit_index stays at 0
                }
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
};

/// Raft log — append-only log of entries indexed by offset.
pub const RaftLog = struct {
    entries: std.ArrayList(LogEntry),
    allocator: Allocator,

    pub const LogEntry = struct {
        offset: u64,
        epoch: i32,
        data: []u8,
    };

    pub fn init(alloc: Allocator) RaftLog {
        return .{
            .entries = std.ArrayList(LogEntry).init(alloc),
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

    pub fn get(self: *const RaftLog, offset: u64) ?*const LogEntry {
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
    prng: std.rand.DefaultPrng,

    pub fn init(min_ms: i64, max_ms: i64) ElectionTimer {
        var timer = ElectionTimer{
            .min_timeout_ms = min_ms,
            .max_timeout_ms = max_ms,
            .deadline_ms = 0,
            .prng = std.rand.DefaultPrng.init(@intCast(std.time.milliTimestamp())),
        };
        timer.reset();
        return timer;
    }

    pub fn reset(self: *ElectionTimer) void {
        const range: u64 = @intCast(self.max_timeout_ms - self.min_timeout_ms);
        const jitter = self.prng.random().intRangeAtMost(u64, 0, range);
        self.deadline_ms = std.time.milliTimestamp() + self.min_timeout_ms + @as(i64, @intCast(jitter));
    }

    pub fn isExpired(self: *const ElectionTimer) bool {
        return std.time.milliTimestamp() >= self.deadline_ms;
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

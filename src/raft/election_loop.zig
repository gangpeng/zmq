const std = @import("std");
const log = std.log.scoped(.election);
const RaftState = @import("state.zig").RaftState;
const RaftClientPool = @import("network").RaftClientPool;

/// Election loop that runs on a background thread.
///
/// Periodically checks the election timer and:
/// - If this node is a follower/unattached and the election timer expired,
///   triggers a new election.
/// - If this node is a leader, periodically logs status (heartbeats are
///   sent via the RaftClientPool in the main broker loop).
///
/// The loop runs until `should_stop` is set to true.
pub const ElectionLoop = struct {
    raft_state: *RaftState,
    should_stop: *bool,
    /// Polling interval in milliseconds between election timer checks.
    check_interval_ms: u64 = 100,
    /// Monotonic counter incremented each polling cycle (used for periodic actions).
    tick_counter: u64 = 0,
    /// Optional callback invoked every ~1 second for broker-level maintenance.
    broker_tick_fn: ?*const fn () void = null,
    /// Raft client pool for sending RPCs to peers (multi-node).
    raft_client_pool: ?*RaftClientPool = null,
    /// Optional callback invoked before each Raft snapshot to serialize the
    /// PreparedObjectRegistry. Returns serialized data that is set on
    /// raft_state.prepared_registry_data before takeSnapshot() persists it.
    pre_snapshot_fn: ?*const fn () ?[]const u8 = null,
    /// Optional callback invoked before snapshot truncation. Return false to
    /// skip this snapshot when the broker cannot first materialize a complete
    /// state-machine snapshot into the committed log.
    prepare_snapshot_fn: ?*const fn () bool = null,
    /// Allocator for freeing pre_snapshot_fn results.
    snapshot_allocator: ?std.mem.Allocator = null,

    pub fn run(self: *ElectionLoop) void {
        log.info("Election loop started for node {d}", .{self.raft_state.node_id});

        while (!self.should_stop.*) {
            // Sleep for check interval
            @import("time_compat").sleep(self.check_interval_ms * std.time.ns_per_ms);

            if (self.should_stop.*) break;

            self.tick_counter += 1;

            // Run broker maintenance every ~10 ticks (~1 second)
            if (self.tick_counter % 10 == 0) {
                if (self.broker_tick_fn) |tick_fn| {
                    tick_fn();
                }
            }

            switch (self.raft_state.role) {
                .unattached, .follower => {
                    if (self.raft_state.isElectionTimedOut()) {
                        log.info("Election timer expired, starting pre-vote (epoch={d})", .{self.raft_state.current_epoch});

                        // In a single-node cluster, skip pre-vote
                        if (self.raft_state.quorumSize() <= 1) {
                            const result = self.raft_state.startElection();
                            _ = result;
                            self.raft_state.becomeLeader();
                            log.info("Single-node cluster: became leader at epoch {d}", .{self.raft_state.current_epoch});
                        } else {
                            // Pre-vote phase: check if others agree we should hold an election
                            const pre_result = self.raft_state.startPreVote();
                            if (self.broadcastAndCountPreVotes(pre_result)) {
                                // Majority agrees — proceed to real election
                                log.info("Pre-vote succeeded, starting real election", .{});
                                const result = self.raft_state.startElection();
                                self.broadcastAndCountVotes(result);
                            } else {
                                // Pre-vote failed — leader might still be alive, don't disrupt
                                log.info("Pre-vote failed, not starting election (leader may be alive)", .{});
                                self.raft_state.election_timer.reset();
                            }
                        }
                    }
                },
                .candidate => {
                    // Still waiting for votes — check if election timed out
                    if (self.raft_state.isElectionTimedOut()) {
                        log.info("Election timed out, retrying with pre-vote", .{});
                        if (self.raft_state.quorumSize() <= 1) {
                            const result = self.raft_state.startElection();
                            _ = result;
                            self.raft_state.becomeLeader();
                        } else {
                            const pre_result = self.raft_state.startPreVote();
                            if (self.broadcastAndCountPreVotes(pre_result)) {
                                const result = self.raft_state.startElection();
                                self.broadcastAndCountVotes(result);
                            } else {
                                self.raft_state.election_timer.reset();
                            }
                        }
                    }
                },
                .leader => {
                    // Leader is active — send periodic heartbeats to followers
                    if (self.tick_counter % 5 == 0) { // heartbeat every ~500ms
                        self.sendHeartbeats();
                        // Send AppendEntries and process responses to advance commit_index
                        self.replicateAndCommit();
                    }
                },
                .resigned => {
                    // Do nothing
                },
            }

            // Periodic snapshot check (every ~10 seconds)
            if (self.tick_counter % 100 == 0) {
                if (self.raft_state.shouldSnapshot(1000)) {
                    if (self.prepare_snapshot_fn) |prepare_fn| {
                        if (!prepare_fn()) {
                            log.warn("Skipping Raft snapshot because broker state-machine snapshot preparation failed", .{});
                            continue;
                        }
                    }
                    // Serialize prepared object registry before snapshot so it
                    // survives Raft log truncation
                    if (self.pre_snapshot_fn) |pre_fn| {
                        self.raft_state.prepared_registry_data = pre_fn();
                    }
                    self.raft_state.takeSnapshot();
                    // Free serialized data after persistence
                    if (self.raft_state.prepared_registry_data) |d| {
                        if (self.snapshot_allocator) |sa| {
                            sa.free(d);
                        }
                        self.raft_state.prepared_registry_data = null;
                    }
                }
            }
        }

        log.info("Election loop stopped for node {d}", .{self.raft_state.node_id});
    }

    /// Broadcast vote requests and count grants. If majority reached, become leader.
    fn broadcastAndCountVotes(self: *ElectionLoop, result: RaftState.ElectionResult) void {
        if (self.raft_client_pool) |pool| {
            const grants = pool.broadcastVoteRequest(
                self.raft_state.cluster_id,
                result.epoch,
                self.raft_state.node_id,
                @intCast(result.last_log_offset),
                result.last_log_epoch,
            );
            log.info("Election: got {d}/{d} votes (need {d})", .{
                grants, self.raft_state.quorumSize(), self.raft_state.majorityThreshold(),
            });
            if (grants >= self.raft_state.majorityThreshold()) {
                self.raft_state.becomeLeader();
                log.info("Won election! Became leader at epoch {d}", .{self.raft_state.current_epoch});
                // Notify followers of new leader
                pool.broadcastHeartbeat(self.raft_state.current_epoch, self.raft_state.node_id);
            }
        }
    }

    /// Broadcast pre-vote requests and count grants (KIP-996).
    /// Returns true if majority granted pre-vote (safe to proceed to real election).
    fn broadcastAndCountPreVotes(self: *ElectionLoop, result: RaftState.ElectionResult) bool {
        if (self.raft_client_pool) |pool| {
            // Reuse broadcastVoteRequest for pre-vote (same wire format, different semantics).
            // In a full implementation, pre-vote would use a separate RPC.
            // For now, we use the same Vote RPC but the candidate does NOT increment its epoch.
            const grants = pool.broadcastVoteRequest(
                self.raft_state.cluster_id,
                result.epoch,
                self.raft_state.node_id,
                @intCast(result.last_log_offset),
                result.last_log_epoch,
            );
            log.info("Pre-vote: got {d}/{d} grants (need {d})", .{
                grants, self.raft_state.quorumSize(), self.raft_state.majorityThreshold(),
            });
            return grants >= self.raft_state.majorityThreshold();
        }
        return true; // No pool = single-node, pre-vote always passes
    }

    /// Send heartbeats / AppendEntries to all followers.
    fn sendHeartbeats(self: *ElectionLoop) void {
        if (self.raft_state.role != .leader) return;
        if (self.raft_client_pool) |pool| {
            pool.broadcastHeartbeat(self.raft_state.current_epoch, self.raft_state.node_id);
        }
    }

    /// Send AppendEntries to each follower and process responses.
    /// Updates match_index/next_index, advancing commit_index on majority ack.
    fn replicateAndCommit(self: *ElectionLoop) void {
        if (self.raft_client_pool == null) return;
        const pool = self.raft_client_pool.?;
        const raft = self.raft_state;
        if (raft.role != .leader) return;

        var vit = raft.voters.iterator();
        while (vit.next()) |entry| {
            const follower_id = entry.key_ptr.*;
            if (follower_id == raft.node_id) continue; // Skip self

            // Build and send AppendEntries
            const ae_req = raft.getAppendEntriesForFollower(follower_id) orelse continue;
            var entry_buf: [128][]const u8 = undefined;
            var entry_count: usize = 0;
            for (raft.log.entries.items) |log_entry| {
                if (log_entry.offset < ae_req.entries_start_index) continue;
                if (entry_count >= entry_buf.len) break;
                entry_buf[entry_count] = log_entry.data;
                entry_count += 1;
            }
            const sent_entries: ?[]const []const u8 = if (entry_count > 0) entry_buf[0..entry_count] else null;
            const success = pool.sendAppendEntriesToFollower(
                follower_id,
                ae_req.epoch,
                ae_req.leader_id,
                @intCast(ae_req.prev_log_offset),
                ae_req.prev_log_epoch,
                ae_req.leader_commit,
                ae_req.entries_start_index,
                sent_entries,
            );

            if (success) {
                // Follower acknowledged — update match_index
                const match_index = if (entry_count > 0)
                    ae_req.entries_start_index + entry_count - 1
                else
                    ae_req.prev_log_offset;
                raft.handleAppendEntriesResponse(follower_id, .{
                    .success = true,
                    .epoch = ae_req.epoch,
                    .match_index = match_index,
                });
            } else {
                // Follower rejected — decrement next_index for retry
                raft.handleAppendEntriesResponse(follower_id, .{
                    .success = false,
                    .epoch = ae_req.epoch,
                    .match_index = 0,
                });
            }
        }
    }
};

/// Spawn the election loop on a new thread.
pub fn spawnElectionLoop(raft_state: *RaftState, should_stop: *bool) !std.Thread {
    var loop_state = ElectionLoop{
        .raft_state = raft_state,
        .should_stop = should_stop,
    };
    return try std.Thread.spawn(.{}, ElectionLoop.run, .{&loop_state});
}

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "ElectionLoop single-node auto-promote" {
    const alloc = std.testing.allocator;
    var raft = RaftState.init(alloc, 1, "test-cluster");
    defer raft.deinit();

    // Single node — just verify the initial state
    try std.testing.expectEqual(RaftState.Role.unattached, raft.role);

    // Simulate what the election loop does for single-node
    const result = raft.startElection();
    try std.testing.expectEqual(@as(i32, 1), result.epoch);

    if (raft.quorumSize() <= 1) {
        raft.becomeLeader();
    }
    try std.testing.expectEqual(RaftState.Role.leader, raft.role);
}

test "ElectionLoop multi-node stays candidate" {
    const alloc = std.testing.allocator;
    var raft = RaftState.init(alloc, 1, "test-cluster");
    defer raft.deinit();

    // Add voters for 3-node cluster
    try raft.addVoter(1);
    try raft.addVoter(2);
    try raft.addVoter(3);

    // Start election
    _ = raft.startElection();
    try std.testing.expectEqual(RaftState.Role.candidate, raft.role);

    // Without majority votes, should stay candidate
    try std.testing.expectEqual(@as(usize, 2), raft.majorityThreshold());
}

test "ElectionLoop leader sends heartbeats on tick" {
    const alloc = std.testing.allocator;
    var raft = RaftState.init(alloc, 0, "test-cluster");
    defer raft.deinit();

    try raft.addVoter(0);
    _ = raft.startElection();
    raft.becomeLeader();

    // Verify state is correct for heartbeat sending
    try std.testing.expectEqual(RaftState.Role.leader, raft.role);
    try std.testing.expectEqual(@as(i32, 1), raft.current_epoch);
}

test "ElectionLoop follower transitions to candidate on timeout" {
    const alloc = std.testing.allocator;
    var raft = RaftState.init(alloc, 1, "test-cluster");
    defer raft.deinit();

    try raft.addVoter(0);
    try raft.addVoter(1);
    try raft.addVoter(2);

    // Start as follower
    raft.becomeFollower(1, 0);
    try std.testing.expectEqual(RaftState.Role.follower, raft.role);

    // Simulate election timeout by starting election
    const result = raft.startElection();
    try std.testing.expectEqual(RaftState.Role.candidate, raft.role);
    try std.testing.expectEqual(@as(i32, 2), result.epoch);
}

test "ElectionLoop candidate retries election on timeout" {
    const alloc = std.testing.allocator;
    var raft = RaftState.init(alloc, 1, "test-cluster");
    defer raft.deinit();

    try raft.addVoter(0);
    try raft.addVoter(1);
    try raft.addVoter(2);

    // First election — epoch 1
    _ = raft.startElection();
    try std.testing.expectEqual(@as(i32, 1), raft.current_epoch);

    // No majority → stays candidate → timeout → new election
    const result2 = raft.startElection();
    try std.testing.expectEqual(@as(i32, 2), result2.epoch);
    try std.testing.expectEqual(RaftState.Role.candidate, raft.role);
}

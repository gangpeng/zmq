const std = @import("std");
const log = std.log.scoped(.election);
const RaftState = @import("state.zig").RaftState;
const RaftClientPool = @import("../network/raft_client.zig").RaftClientPool;

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
    check_interval_ms: u64 = 100, // Check every 100ms
    tick_counter: u64 = 0,
    broker_tick_fn: ?*const fn () void = null,
    /// Raft client pool for sending RPCs to peers (multi-node).
    raft_client_pool: ?*RaftClientPool = null,

    pub fn run(self: *ElectionLoop) void {
        log.info("Election loop started for node {d}", .{self.raft_state.node_id});

        while (!self.should_stop.*) {
            // Sleep for check interval
            std.time.sleep(self.check_interval_ms * std.time.ns_per_ms);

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
                        log.info("Election timer expired, starting election (epoch={d})", .{self.raft_state.current_epoch});
                        const result = self.raft_state.startElection();
                        log.info("Started election: epoch={d} last_offset={d} last_epoch={d}", .{
                            result.epoch,
                            result.last_log_offset,
                            result.last_log_epoch,
                        });

                        // In a single-node cluster, immediately become leader
                        if (self.raft_state.quorumSize() <= 1) {
                            self.raft_state.becomeLeader();
                            log.info("Single-node cluster: became leader at epoch {d}", .{self.raft_state.current_epoch});
                        } else {
                            // Fix 2: Multi-node — broadcast vote requests and count grants
                            self.broadcastAndCountVotes(result);
                        }
                    }
                },
                .candidate => {
                    // Still waiting for votes — check if election timed out
                    if (self.raft_state.isElectionTimedOut()) {
                        log.info("Election timed out, starting new election", .{});
                        const result = self.raft_state.startElection();
                        if (self.raft_state.quorumSize() <= 1) {
                            self.raft_state.becomeLeader();
                        } else {
                            self.broadcastAndCountVotes(result);
                        }
                    }
                },
                .leader => {
                    // Leader is active — send periodic heartbeats to followers
                    if (self.tick_counter % 5 == 0) { // heartbeat every ~500ms
                        self.sendHeartbeats();
                    }
                },
                .resigned => {
                    // Do nothing
                },
            }
        }

        log.info("Election loop stopped for node {d}", .{self.raft_state.node_id});
    }

    /// Fix 2: Broadcast vote requests and count grants. If majority reached, become leader.
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

    /// Fix 4: Send heartbeats / AppendEntries to all followers.
    fn sendHeartbeats(self: *ElectionLoop) void {
        if (self.raft_state.role != .leader) return;
        if (self.raft_client_pool) |pool| {
            pool.broadcastHeartbeat(self.raft_state.current_epoch, self.raft_state.node_id);
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

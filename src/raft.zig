/// AutoMQ Raft Consensus (KRaft)
///
/// Implements the Raft consensus protocol for cluster metadata management:
/// - RaftState: state machine (Unattached/Candidate/Leader/Follower)
/// - RaftLog: append-only replicated log
/// - MetadataImage: materialized view of cluster metadata
/// - ElectionTimer: randomized election timeouts
/// - ElectionLoop: background thread for election/heartbeat management

pub const state = @import("raft/state.zig");
pub const election_loop = @import("raft/election_loop.zig");

pub const RaftState = state.RaftState;
pub const RaftLog = state.RaftLog;
pub const MetadataImage = state.MetadataImage;
pub const ElectionTimer = state.ElectionTimer;
pub const ElectionLoop = election_loop.ElectionLoop;

test {
    @import("std").testing.refAllDecls(@This());
}

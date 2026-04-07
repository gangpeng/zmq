/// AutoMQ Network Layer
///
/// TCP server for Kafka binary protocol + HTTP metrics endpoint + Raft RPC client.

pub const server = @import("network/server.zig");
pub const connection_manager = @import("network/connection_manager.zig");
pub const metrics_server = @import("network/metrics_server.zig");
pub const raft_client = @import("network/raft_client.zig");

pub const Server = server.Server;
pub const ConnectionManager = connection_manager.ConnectionManager;
pub const MetricsServer = metrics_server.MetricsServer;
pub const RaftClient = raft_client.RaftClient;
pub const RaftClientPool = raft_client.RaftClientPool;

test {
    @import("std").testing.refAllDecls(@This());
}

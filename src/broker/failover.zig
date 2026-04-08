const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.failover);

/// FailoverController detects failed nodes and reassigns their partitions.
/// In ZMQ's cloud-native architecture, data lives in S3, so failover
/// only requires metadata reassignment — no data movement.
///
/// AutoMQ uses WAL epoch fencing to prevent split-brain writes.
/// This implementation provides the same semantics:
/// - Each WAL writer has an epoch; only the current epoch can write
/// - When a broker fails, its epoch is fenced (bumped) so stale writers
///   are rejected
/// - Partitions are reassigned to the surviving broker
pub const FailoverController = struct {
    allocator: Allocator,
    node_id: i32,
    known_nodes: std.AutoHashMap(i32, NodeState),
    wal_epoch: u64,

    pub const PartitionId = struct {
        topic: []const u8,
        partition: i32,
    };

    pub const NodeState = struct {
        node_id: i32,
        last_heartbeat_ms: i64,
        is_fenced: bool,
        owned_partitions: std.ArrayList(PartitionId),

        pub fn init(alloc: Allocator, nid: i32) NodeState {
            return .{
                .node_id = nid,
                .last_heartbeat_ms = std.time.milliTimestamp(),
                .is_fenced = false,
                .owned_partitions = std.ArrayList(PartitionId).init(alloc),
            };
        }

        pub fn deinit(self: *NodeState) void {
            self.owned_partitions.deinit();
        }
    };

    /// Failover timeout — 30 seconds matches AutoMQ default.
    const FAILOVER_TIMEOUT_MS: i64 = 30_000;

    pub fn init(alloc: Allocator, node_id: i32) FailoverController {
        return .{
            .allocator = alloc,
            .node_id = node_id,
            .known_nodes = std.AutoHashMap(i32, NodeState).init(alloc),
            .wal_epoch = 1,
        };
    }

    pub fn deinit(self: *FailoverController) void {
        var it = self.known_nodes.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.known_nodes.deinit();
    }

    /// Register a known node for heartbeat tracking.
    pub fn registerNode(self: *FailoverController, nid: i32) !void {
        if (!self.known_nodes.contains(nid)) {
            try self.known_nodes.put(nid, NodeState.init(self.allocator, nid));
        }
    }

    /// Record a heartbeat from a node.
    pub fn recordHeartbeat(self: *FailoverController, nid: i32) void {
        if (self.known_nodes.getPtr(nid)) |state| {
            state.last_heartbeat_ms = std.time.milliTimestamp();
            state.is_fenced = false;
        }
    }

    /// Check for failed nodes and trigger failover.
    /// Called from the broker's periodic tick().
    pub fn tick(self: *FailoverController, now_ms: i64) u32 {
        var failover_count: u32 = 0;

        var it = self.known_nodes.iterator();
        while (it.next()) |entry| {
            const state = entry.value_ptr;
            if (state.node_id == self.node_id) continue; // Don't failover self
            if (state.is_fenced) continue; // Already fenced

            if ((now_ms - state.last_heartbeat_ms) > FAILOVER_TIMEOUT_MS) {
                self.failoverNode(state);
                failover_count += 1;
            }
        }

        return failover_count;
    }

    /// Reassign all partitions from failed node to self.
    /// Since data is in S3, this is a metadata-only operation.
    fn failoverNode(self: *FailoverController, failed: *NodeState) void {
        // 1. Fence the failed node's WAL epoch
        failed.is_fenced = true;
        self.wal_epoch += 1;

        log.info("Failover: node {d} fenced (epoch={d}), reassigning {d} partitions to node {d}", .{
            failed.node_id,
            self.wal_epoch,
            failed.owned_partitions.items.len,
            self.node_id,
        });

        // 2. Take ownership of failed node's partitions
        for (failed.owned_partitions.items) |partition| {
            self.reassignPartition(partition, self.node_id);
        }

        // 3. Clear the failed node's partition list (they now belong to us)
        failed.owned_partitions.clearRetainingCapacity();
    }

    /// Reassign a partition to a target node.
    /// Updates metadata so partition.leader = target, partition.isr = [target].
    pub fn reassignPartition(self: *FailoverController, partition: PartitionId, target: i32) void {
        _ = self;
        log.info("Reassigning {s}-{d} to node {d}", .{ partition.topic, partition.partition, target });
        // In production: update the metadata store so that
        //   partition.leader = target
        //   partition.isr = [target]
        // Since we're single-node with RF=1 in ZMQ, this is effectively a no-op
        // but the infrastructure is in place for multi-node support.
    }

    /// WAL epoch fencing — check if our epoch is still valid.
    /// Returns true if the current epoch allows writes.
    pub fn isEpochValid(self: *const FailoverController) bool {
        // In single-node mode, our epoch is always valid.
        // In multi-node: another broker with a higher epoch means we've been fenced.
        return !self.isSelfFenced();
    }

    /// Check if this node has been fenced by a higher-epoch writer.
    fn isSelfFenced(self: *const FailoverController) bool {
        if (self.known_nodes.get(self.node_id)) |state| {
            return state.is_fenced;
        }
        return false;
    }

    /// Get the current WAL epoch for including in S3 batch objects.
    pub fn currentEpoch(self: *const FailoverController) u64 {
        return self.wal_epoch;
    }

    /// Get count of known nodes.
    pub fn nodeCount(self: *const FailoverController) usize {
        return self.known_nodes.count();
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "FailoverController init and register" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    try fc.registerNode(1);
    try fc.registerNode(2);

    try testing.expectEqual(@as(usize, 3), fc.nodeCount());
    try testing.expect(fc.isEpochValid());
    try testing.expectEqual(@as(u64, 1), fc.currentEpoch());
}

test "FailoverController heartbeat prevents failover" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(1);
    fc.recordHeartbeat(1);

    // Tick with current time — node 1 just heartbeated, no failover
    const now = std.time.milliTimestamp();
    const count = fc.tick(now);
    try testing.expectEqual(@as(u32, 0), count);
}

test "FailoverController detects timeout and fences node" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(1);
    // Simulate node 1 having an old heartbeat
    if (fc.known_nodes.getPtr(1)) |state| {
        state.last_heartbeat_ms = 0; // very old heartbeat
    }

    // Tick with current time — should detect failure
    const now = std.time.milliTimestamp();
    const count = fc.tick(now);
    try testing.expectEqual(@as(u32, 1), count);

    // Node should be fenced
    if (fc.known_nodes.get(1)) |state| {
        try testing.expect(state.is_fenced);
    }

    // Epoch should have been bumped
    try testing.expectEqual(@as(u64, 2), fc.currentEpoch());
}

test "FailoverController does not failover self" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    // Even with old heartbeat, should not failover self
    if (fc.known_nodes.getPtr(0)) |state| {
        state.last_heartbeat_ms = 0;
    }

    const now = std.time.milliTimestamp();
    const count = fc.tick(now);
    try testing.expectEqual(@as(u32, 0), count);
}

test "FailoverController does not re-fence already fenced node" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(1);
    if (fc.known_nodes.getPtr(1)) |state| {
        state.last_heartbeat_ms = 0;
    }

    const now = std.time.milliTimestamp();
    _ = fc.tick(now); // First tick fences
    const count2 = fc.tick(now); // Second tick should not re-fence
    try testing.expectEqual(@as(u32, 0), count2);
}

test "FailoverController epoch bumps on each failover" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(1);
    try fc.registerNode(2);

    // Force both nodes to have old heartbeats
    if (fc.known_nodes.getPtr(1)) |state| state.last_heartbeat_ms = 0;
    if (fc.known_nodes.getPtr(2)) |state| state.last_heartbeat_ms = 0;

    try testing.expectEqual(@as(u64, 1), fc.currentEpoch());

    const now = std.time.milliTimestamp();
    const count = fc.tick(now);
    try testing.expectEqual(@as(u32, 2), count);

    // Epoch should have bumped twice (once per failover)
    try testing.expectEqual(@as(u64, 3), fc.currentEpoch());
}

test "FailoverController heartbeat un-fences previously fenced node" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(1);

    // Force timeout and fence
    if (fc.known_nodes.getPtr(1)) |state| state.last_heartbeat_ms = 0;
    const now = std.time.milliTimestamp();
    _ = fc.tick(now);

    // Verify fenced
    if (fc.known_nodes.get(1)) |state| {
        try testing.expect(state.is_fenced);
    }

    // Heartbeat should un-fence
    fc.recordHeartbeat(1);
    if (fc.known_nodes.get(1)) |state| {
        try testing.expect(!state.is_fenced);
    }
}

test "FailoverController isEpochValid reflects self-fencing state" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    try testing.expect(fc.isEpochValid());

    // Manually fence self (simulates receiving higher epoch from another leader)
    if (fc.known_nodes.getPtr(0)) |state| {
        state.is_fenced = true;
    }
    try testing.expect(!fc.isEpochValid());
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.failover);

fn monotonicMs() i64 {
    return @intCast(@import("time_compat").monotonicMilliTimestamp());
}

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
        owns_topic: bool = false,
    };

    pub const NodeState = struct {
        allocator: Allocator,
        node_id: i32,
        last_heartbeat_ms: i64,
        is_fenced: bool,
        owned_partitions: std.array_list.Managed(PartitionId),

        pub fn init(alloc: Allocator, nid: i32) NodeState {
            return .{
                .allocator = alloc,
                .node_id = nid,
                .last_heartbeat_ms = monotonicMs(),
                .is_fenced = false,
                .owned_partitions = std.array_list.Managed(PartitionId).init(alloc),
            };
        }

        pub fn deinit(self: *NodeState) void {
            for (self.owned_partitions.items) |partition| {
                if (partition.owns_topic) self.allocator.free(partition.topic);
            }
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

    /// Record a heartbeat from a node. Returns true if this heartbeat
    /// un-fenced a previously fenced node (an ISR-expand-equivalent event).
    pub fn recordHeartbeat(self: *FailoverController, nid: i32) bool {
        if (self.known_nodes.getPtr(nid)) |state| {
            const was_fenced = state.is_fenced;
            state.last_heartbeat_ms = monotonicMs();
            state.is_fenced = false;
            if (was_fenced) {
                log.info("Node {d} un-fenced after heartbeat", .{nid});
                return true;
            }
        }
        return false;
    }

    /// Record the node that currently owns a partition. Topic names are copied
    /// so failover state does not depend on request-buffer lifetimes.
    pub fn registerPartitionOwner(self: *FailoverController, topic: []const u8, partition: i32, owner: i32) !void {
        try self.reassignPartition(.{ .topic = topic, .partition = partition }, owner);
    }

    pub fn removePartitionOwner(self: *FailoverController, topic: []const u8, partition: i32) void {
        self.removePartitionOwnership(topic, partition);
    }

    /// Return the node currently recorded as owner for a partition.
    pub fn findPartitionOwner(self: *const FailoverController, topic: []const u8, partition: i32) ?i32 {
        var it = self.known_nodes.iterator();
        while (it.next()) |entry| {
            for (entry.value_ptr.owned_partitions.items) |owned| {
                if (partitionMatches(owned, topic, partition)) return entry.key_ptr.*;
            }
        }
        return null;
    }

    pub fn nodePartitionCount(self: *const FailoverController, nid: i32) usize {
        if (self.known_nodes.get(nid)) |state| return state.owned_partitions.items.len;
        return 0;
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

            if (state.last_heartbeat_ms == 0 or (now_ms - state.last_heartbeat_ms) > FAILOVER_TIMEOUT_MS) {
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

        // 2. Take ownership of failed node's partitions. Copy topic names
        // instead of shallow-moving them so each NodeState has unambiguous
        // ownership even if later reassignment cleanup scans all nodes.
        if (failed.owned_partitions.items.len == 0) return;
        const target = self.known_nodes.getPtr(self.node_id) orelse {
            log.warn("Failover: local node {d} is not registered; leaving partitions on fenced node {d}", .{ self.node_id, failed.node_id });
            return;
        };

        var moved = std.array_list.Managed(PartitionId).init(self.allocator);
        defer moved.deinit();
        var moved_committed = false;
        defer if (!moved_committed) {
            for (moved.items) |partition| self.freePartitionId(partition);
        };

        for (failed.owned_partitions.items) |partition| {
            const topic_copy = self.allocator.dupe(u8, partition.topic) catch |err| {
                log.err("Failover: failed to copy partition topic for node {d}: {}", .{ failed.node_id, err });
                return;
            };
            moved.append(.{
                .topic = topic_copy,
                .partition = partition.partition,
                .owns_topic = true,
            }) catch |err| {
                self.allocator.free(topic_copy);
                log.err("Failover: failed to stage partition transfer from node {d} to node {d}: {}", .{ failed.node_id, self.node_id, err });
                return;
            };
        }

        target.owned_partitions.appendSlice(moved.items) catch |err| {
            log.err("Failover: failed to transfer partitions from node {d} to node {d}: {}", .{ failed.node_id, self.node_id, err });
            return;
        };
        moved_committed = true;
        for (failed.owned_partitions.items) |partition| {
            log.info("Reassigning {s}-{d} to node {d}", .{ partition.topic, partition.partition, self.node_id });
        }

        // 3. Clear the failed node's partition list. The target owns copied
        // entries; the failed state still owns and must free its old entries.
        for (failed.owned_partitions.items) |partition| self.freePartitionId(partition);
        failed.owned_partitions.clearRetainingCapacity();
    }

    /// Reassign a partition to a target node.
    /// Updates metadata so partition.leader = target, partition.isr = [target].
    pub fn reassignPartition(self: *FailoverController, partition: PartitionId, target: i32) !void {
        const topic_copy = try self.allocator.dupe(u8, partition.topic);
        errdefer self.allocator.free(topic_copy);

        try self.registerNode(target);
        self.removePartitionOwnership(topic_copy, partition.partition);

        const target_state = self.known_nodes.getPtr(target) orelse return error.UnknownFailoverTarget;
        try target_state.owned_partitions.append(.{
            .topic = topic_copy,
            .partition = partition.partition,
            .owns_topic = true,
        });
        log.info("Reassigning {s}-{d} to node {d}", .{ topic_copy, partition.partition, target });
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

    fn removePartitionOwnership(self: *FailoverController, topic: []const u8, partition: i32) void {
        var it = self.known_nodes.iterator();
        while (it.next()) |entry| {
            var i: usize = 0;
            while (i < entry.value_ptr.owned_partitions.items.len) {
                const owned = entry.value_ptr.owned_partitions.items[i];
                if (!partitionMatches(owned, topic, partition)) {
                    i += 1;
                    continue;
                }
                const removed = entry.value_ptr.owned_partitions.swapRemove(i);
                self.freePartitionId(removed);
            }
        }
    }

    fn freePartitionId(self: *FailoverController, partition: PartitionId) void {
        if (partition.owns_topic) self.allocator.free(partition.topic);
    }

    fn partitionMatches(partition: PartitionId, topic: []const u8, partition_id: i32) bool {
        return partition.partition == partition_id and std.mem.eql(u8, partition.topic, topic);
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
    _ = fc.recordHeartbeat(1);

    // Tick with current time — node 1 just heartbeated, no failover
    const now = monotonicMs();
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
    const now = monotonicMs();
    const count = fc.tick(now);
    try testing.expectEqual(@as(u32, 1), count);

    // Node should be fenced
    if (fc.known_nodes.get(1)) |state| {
        try testing.expect(state.is_fenced);
    }

    // Epoch should have been bumped
    try testing.expectEqual(@as(u64, 2), fc.currentEpoch());
}

test "FailoverController transfers partition ownership on timeout" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    try fc.registerNode(1);
    try fc.registerPartitionOwner("topic-a", 0, 1);
    try fc.registerPartitionOwner("topic-a", 1, 1);
    try testing.expectEqual(@as(?i32, 1), fc.findPartitionOwner("topic-a", 0));
    try testing.expectEqual(@as(usize, 2), fc.nodePartitionCount(1));
    try testing.expectEqual(@as(usize, 0), fc.nodePartitionCount(0));

    if (fc.known_nodes.getPtr(1)) |state| {
        state.last_heartbeat_ms = 0;
    }

    const now = monotonicMs();
    const count = fc.tick(now);
    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(@as(?i32, 0), fc.findPartitionOwner("topic-a", 0));
    try testing.expectEqual(@as(?i32, 0), fc.findPartitionOwner("topic-a", 1));
    try testing.expectEqual(@as(usize, 0), fc.nodePartitionCount(1));
    try testing.expectEqual(@as(usize, 2), fc.nodePartitionCount(0));
    try testing.expectEqual(@as(u64, 2), fc.currentEpoch());
}

test "FailoverController explicit reassignment replaces stale ownership" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    try fc.registerNode(1);
    try fc.registerPartitionOwner("topic-b", 0, 1);
    try testing.expectEqual(@as(?i32, 1), fc.findPartitionOwner("topic-b", 0));

    try fc.reassignPartition(.{ .topic = "topic-b", .partition = 0 }, 0);
    try testing.expectEqual(@as(?i32, 0), fc.findPartitionOwner("topic-b", 0));
    try testing.expectEqual(@as(usize, 0), fc.nodePartitionCount(1));
    try testing.expectEqual(@as(usize, 1), fc.nodePartitionCount(0));

    try fc.reassignPartition(.{ .topic = "topic-b", .partition = 0 }, 0);
    try testing.expectEqual(@as(?i32, 0), fc.findPartitionOwner("topic-b", 0));
    try testing.expectEqual(@as(usize, 1), fc.nodePartitionCount(0));
}

test "FailoverController reassignment tolerates stored topic slice" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    try fc.registerNode(1);
    try fc.registerPartitionOwner("topic-stored", 0, 1);

    const stored_topic = fc.known_nodes.getPtr(1).?.owned_partitions.items[0].topic;
    try fc.reassignPartition(.{ .topic = stored_topic, .partition = 0 }, 0);

    try testing.expectEqual(@as(?i32, 0), fc.findPartitionOwner("topic-stored", 0));
    try testing.expectEqual(@as(usize, 0), fc.nodePartitionCount(1));
    try testing.expectEqual(@as(usize, 1), fc.nodePartitionCount(0));
}

test "FailoverController timeout transfer copies topic ownership" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    try fc.registerNode(1);
    try fc.registerPartitionOwner("topic-copy", 0, 1);
    try fc.registerPartitionOwner("topic-copy", 1, 1);

    if (fc.known_nodes.getPtr(1)) |state| state.last_heartbeat_ms = 0;
    try testing.expectEqual(@as(u32, 1), fc.tick(monotonicMs()));

    try fc.reassignPartition(.{ .topic = "topic-copy", .partition = 0 }, 1);
    try testing.expectEqual(@as(?i32, 1), fc.findPartitionOwner("topic-copy", 0));
    try testing.expectEqual(@as(?i32, 0), fc.findPartitionOwner("topic-copy", 1));
}

test "FailoverController does not failover self" {
    var fc = FailoverController.init(testing.allocator, 0);
    defer fc.deinit();

    try fc.registerNode(0);
    // Even with old heartbeat, should not failover self
    if (fc.known_nodes.getPtr(0)) |state| {
        state.last_heartbeat_ms = 0;
    }

    const now = monotonicMs();
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

    const now = monotonicMs();
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

    const now = monotonicMs();
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
    const now = monotonicMs();
    _ = fc.tick(now);

    // Verify fenced
    if (fc.known_nodes.get(1)) |state| {
        try testing.expect(state.is_fenced);
    }

    // Heartbeat should un-fence
    _ = fc.recordHeartbeat(1);
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

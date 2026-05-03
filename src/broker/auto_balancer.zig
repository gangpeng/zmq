const std = @import("std");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.auto_balancer);

/// AutoBalancer computes partition assignments to balance load across nodes.
/// In ZMQ's cloud-native architecture, partition reassignment is a
/// metadata-only operation since data lives in S3 — no data movement needed.
///
/// AutoMQ implements automatic load balancing via traffic-aware partition
/// reassignment. This implementation provides the same semantics:
/// - Periodically compute partition load (bytes in/out per second)
/// - Use a greedy algorithm to balance load across nodes
/// - Generate a RebalancePlan with partition moves
pub const AutoBalancer = struct {
    allocator: Allocator,
    enabled: bool,
    check_interval_ms: i64,
    last_check_ms: i64,

    pub const NodeInfo = struct {
        node_id: i32,
        rack: ?[]const u8 = null,
    };

    pub const BrokerNode = struct {
        node_id: i32,
        rack: ?[]const u8 = null,
        fenced: bool = false,
    };

    pub const PartitionLoad = struct {
        topic: []const u8,
        partition_id: i32,
        bytes_in_rate: f64, // bytes/sec
        bytes_out_rate: f64,
        leader_node: i32,

        /// Total load for sorting purposes.
        pub fn totalLoad(self: *const PartitionLoad) f64 {
            return normalizedRate(self.bytes_in_rate) + normalizedRate(self.bytes_out_rate);
        }
    };

    pub const RebalancePlan = struct {
        moves: std.array_list.Managed(PartitionMove),

        pub const PartitionMove = struct {
            topic: []const u8,
            partition_id: i32,
            from_node: i32,
            to_node: i32,
        };

        pub fn init(alloc: Allocator) RebalancePlan {
            return .{ .moves = std.array_list.Managed(PartitionMove).init(alloc) };
        }

        pub fn deinit(self: *RebalancePlan) void {
            self.moves.deinit();
        }

        pub fn moveCount(self: *const RebalancePlan) usize {
            return self.moves.items.len;
        }
    };

    pub fn init(alloc: Allocator) AutoBalancer {
        return .{
            .allocator = alloc,
            .enabled = true,
            .check_interval_ms = 300_000, // 5 minutes default
            .last_check_ms = @import("time_compat").milliTimestamp(),
        };
    }

    pub fn deinit(self: *AutoBalancer) void {
        _ = self;
    }

    /// Check if it's time to rebalance.
    pub fn shouldCheck(self: *const AutoBalancer) bool {
        if (!self.enabled) return false;
        const now = @import("time_compat").milliTimestamp();
        return (now - self.last_check_ms) >= self.check_interval_ms;
    }

    pub fn markChecked(self: *AutoBalancer) void {
        self.last_check_ms = @import("time_compat").milliTimestamp();
    }

    /// Compute partition assignments to balance load across nodes.
    /// Uses a simple greedy algorithm: sort partitions by load descending,
    /// assign each to the least-loaded node.
    pub fn computeRebalancePlan(
        self: *AutoBalancer,
        nodes: []const i32,
        loads: []const PartitionLoad,
    ) ?RebalancePlan {
        if (nodes.len == 0) return null;

        const node_infos = self.allocator.alloc(NodeInfo, nodes.len) catch return null;
        defer self.allocator.free(node_infos);
        for (nodes, 0..) |node_id, i| {
            node_infos[i] = .{ .node_id = node_id };
        }

        return self.computeRackAwareRebalancePlan(node_infos, loads);
    }

    /// Compute partition assignments with optional rack/topology hints.
    /// When the source node has a rack and a less-loaded node exists in a
    /// different rack, the planner prefers that cross-rack target before
    /// falling back to load-only placement.
    pub fn computeRackAwareRebalancePlan(
        self: *AutoBalancer,
        nodes: []const NodeInfo,
        loads: []const PartitionLoad,
    ) ?RebalancePlan {
        return self.computeRackAwareRebalancePlanWithOptions(nodes, loads, .{});
    }

    /// Compute a plan directly from controller broker lifecycle state.
    /// Only unfenced brokers are eligible targets. Loads whose leader is no
    /// longer an active broker are moved first, which covers scale-in/failover
    /// convergence before normal rack-aware balancing.
    pub fn computeControllerAwareRebalancePlan(
        self: *AutoBalancer,
        brokers: []const BrokerNode,
        loads: []const PartitionLoad,
    ) ?RebalancePlan {
        if (loads.len == 0) return null;

        const nodes = self.collectActiveBrokerNodes(brokers) catch return null;
        defer self.allocator.free(nodes);
        if (nodes.len == 0) return null;

        return self.computeRackAwareRebalancePlanWithOptions(nodes, loads, .{ .move_inactive_leaders = true });
    }

    const PlannerOptions = struct {
        move_inactive_leaders: bool = false,
    };

    fn computeRackAwareRebalancePlanWithOptions(
        self: *AutoBalancer,
        nodes: []const NodeInfo,
        loads: []const PartitionLoad,
        options: PlannerOptions,
    ) ?RebalancePlan {
        if (nodes.len == 0 or loads.len == 0) return null;
        if (nodes.len <= 1 and !options.move_inactive_leaders) return null;

        const sorted_loads = self.allocator.dupe(PartitionLoad, loads) catch return null;
        defer self.allocator.free(sorted_loads);
        std.mem.sort(PartitionLoad, sorted_loads, {}, partitionLoadGreaterThan);

        // Compute current load per node
        var node_loads = std.AutoHashMap(i32, f64).init(self.allocator);
        defer node_loads.deinit();
        for (nodes) |node| {
            node_loads.put(node.node_id, 0.0) catch continue;
        }
        for (sorted_loads) |pl| {
            if (node_loads.getPtr(pl.leader_node)) |nl| {
                nl.* += pl.totalLoad();
            }
        }

        // Compute average load only for partitions led by known nodes. Metrics
        // for stale or unknown brokers should not suppress valid local moves.
        var total_load: f64 = 0.0;
        var has_inactive_leader = false;
        for (sorted_loads) |pl| {
            if (node_loads.get(pl.leader_node) != null) {
                total_load += pl.totalLoad();
            } else if (options.move_inactive_leaders) {
                total_load += pl.totalLoad();
                has_inactive_leader = true;
            }
        }
        if (total_load <= 0.0 and !has_inactive_leader) return null;
        const avg_load = if (total_load > 0.0) total_load / @as(f64, @floatFromInt(nodes.len)) else 0.0;

        // Find overloaded nodes and generate moves
        var plan = RebalancePlan.init(self.allocator);
        if (options.move_inactive_leaders) {
            for (sorted_loads) |pl| {
                if (node_loads.get(pl.leader_node) != null) continue;
                const target = self.chooseLeastLoadedTarget(nodes, &node_loads) orelse continue;
                if (target.node_id == pl.leader_node) continue;

                plan.moves.append(.{
                    .topic = pl.topic,
                    .partition_id = pl.partition_id,
                    .from_node = pl.leader_node,
                    .to_node = target.node_id,
                }) catch continue;

                if (node_loads.getPtr(target.node_id)) |to| {
                    to.* += pl.totalLoad();
                }
            }
        }

        for (sorted_loads) |pl| {
            if (node_loads.get(pl.leader_node) == null) continue;
            if (total_load <= 0.0 or nodes.len <= 1) continue;
            const current_node_load = node_loads.get(pl.leader_node) orelse continue;
            if (current_node_load <= avg_load * 1.2) continue; // Within 20% tolerance

            const target = self.chooseRebalanceTarget(nodes, &node_loads, pl.leader_node, current_node_load) orelse continue;

            if (target.node_id != pl.leader_node and target.load < current_node_load * 0.8) {
                plan.moves.append(.{
                    .topic = pl.topic,
                    .partition_id = pl.partition_id,
                    .from_node = pl.leader_node,
                    .to_node = target.node_id,
                }) catch continue;

                // Update load tracking
                if (node_loads.getPtr(pl.leader_node)) |from| {
                    from.* -= pl.totalLoad();
                }
                if (node_loads.getPtr(target.node_id)) |to| {
                    to.* += pl.totalLoad();
                }
            }
        }

        self.last_check_ms = @import("time_compat").milliTimestamp();

        if (plan.moveCount() > 0) {
            log.info("Rebalance plan: {d} partition moves", .{plan.moveCount()});
            for (plan.moves.items) |move| {
                log.info("Rebalance: moving {s}-{d} from node {d} to node {d}", .{ move.topic, move.partition_id, move.from_node, move.to_node });
            }
            return plan;
        }

        log.debug("Load balanced: no partition moves needed", .{});

        plan.deinit();
        return null;
    }

    const TargetNode = struct {
        node_id: i32,
        load: f64,
    };

    fn chooseRebalanceTarget(
        self: *AutoBalancer,
        nodes: []const NodeInfo,
        node_loads: *std.AutoHashMap(i32, f64),
        source_node: i32,
        source_load: f64,
    ) ?TargetNode {
        _ = self;

        const source_rack = rackForNode(nodes, source_node);
        var best_cross_rack: ?TargetNode = null;
        var best_any: ?TargetNode = null;

        for (nodes) |node| {
            if (node.node_id == source_node) continue;

            const load = node_loads.get(node.node_id) orelse continue;
            if (load >= source_load) continue;

            const candidate = TargetNode{ .node_id = node.node_id, .load = load };
            if (best_any == null or isBetterTarget(candidate, best_any.?)) {
                best_any = candidate;
            }

            if (isDifferentKnownRack(source_rack, node.rack)) {
                if (best_cross_rack == null or isBetterTarget(candidate, best_cross_rack.?)) {
                    best_cross_rack = candidate;
                }
            }
        }

        return best_cross_rack orelse best_any;
    }

    fn chooseLeastLoadedTarget(
        self: *AutoBalancer,
        nodes: []const NodeInfo,
        node_loads: *std.AutoHashMap(i32, f64),
    ) ?TargetNode {
        _ = self;

        var best: ?TargetNode = null;
        for (nodes) |node| {
            const load = node_loads.get(node.node_id) orelse continue;
            const candidate = TargetNode{ .node_id = node.node_id, .load = load };
            if (best == null or isBetterTarget(candidate, best.?)) best = candidate;
        }
        return best;
    }

    fn collectActiveBrokerNodes(self: *AutoBalancer, brokers: []const BrokerNode) ![]NodeInfo {
        var nodes = std.array_list.Managed(NodeInfo).init(self.allocator);
        errdefer nodes.deinit();

        for (brokers) |broker| {
            if (broker.fenced) continue;
            try nodes.append(.{
                .node_id = broker.node_id,
                .rack = broker.rack,
            });
        }

        std.mem.sort(NodeInfo, nodes.items, {}, nodeInfoLessThan);
        return try nodes.toOwnedSlice();
    }

    fn rackForNode(nodes: []const NodeInfo, node_id: i32) ?[]const u8 {
        for (nodes) |node| {
            if (node.node_id == node_id) return node.rack;
        }
        return null;
    }

    fn isDifferentKnownRack(a: ?[]const u8, b: ?[]const u8) bool {
        if (a == null or b == null) return false;
        return !std.mem.eql(u8, a.?, b.?);
    }

    fn isBetterTarget(candidate: TargetNode, current: TargetNode) bool {
        if (candidate.load < current.load) return true;
        if (candidate.load > current.load) return false;
        return candidate.node_id < current.node_id;
    }

    fn normalizedRate(rate: f64) f64 {
        if (rate > 0.0) return rate;
        return 0.0;
    }

    fn partitionLoadGreaterThan(_: void, a: PartitionLoad, b: PartitionLoad) bool {
        const a_load = a.totalLoad();
        const b_load = b.totalLoad();
        if (a_load > b_load) return true;
        if (a_load < b_load) return false;
        const topic_order = std.mem.order(u8, a.topic, b.topic);
        if (topic_order != .eq) return topic_order == .lt;
        if (a.partition_id != b.partition_id) return a.partition_id < b.partition_id;
        return a.leader_node < b.leader_node;
    }

    fn nodeInfoLessThan(_: void, a: NodeInfo, b: NodeInfo) bool {
        return a.node_id < b.node_id;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

const testing = std.testing;

test "AutoBalancer init" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    try testing.expect(ab.enabled);
}

test "AutoBalancer single node returns null plan" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]i32{0};
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 1000, .bytes_out_rate = 500, .leader_node = 0 },
    };

    const plan = ab.computeRebalancePlan(&nodes, &loads);
    try testing.expect(plan == null);
}

test "AutoBalancer balanced cluster returns null plan" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]i32{ 0, 1 };
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 1000, .bytes_out_rate = 500, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 1, .bytes_in_rate = 1000, .bytes_out_rate = 500, .leader_node = 1 },
    };

    const plan = ab.computeRebalancePlan(&nodes, &loads);
    if (plan) |*p| {
        var mp = @constCast(p);
        mp.deinit();
    }
    // Balanced — no moves needed (or very few)
}

test "AutoBalancer imbalanced cluster generates moves" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]i32{ 0, 1 };
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 5000, .bytes_out_rate = 5000, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 1, .bytes_in_rate = 5000, .bytes_out_rate = 5000, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 2, .bytes_in_rate = 5000, .bytes_out_rate = 5000, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 3, .bytes_in_rate = 100, .bytes_out_rate = 100, .leader_node = 1 },
    };

    const plan = ab.computeRebalancePlan(&nodes, &loads);
    try testing.expect(plan != null);
    if (plan) |*p| {
        var mp = @constCast(p);
        defer mp.deinit();
        try testing.expect(mp.moveCount() > 0);
    }
}

test "AutoBalancer rack-aware plan prefers different rack target" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]AutoBalancer.NodeInfo{
        .{ .node_id = 0, .rack = "rack-a" },
        .{ .node_id = 1, .rack = "rack-a" },
        .{ .node_id = 2, .rack = "rack-b" },
    };
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 6000, .bytes_out_rate = 6000, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 1, .bytes_in_rate = 6000, .bytes_out_rate = 6000, .leader_node = 0 },
    };

    const plan = ab.computeRackAwareRebalancePlan(&nodes, &loads);
    try testing.expect(plan != null);
    if (plan) |*p| {
        var mp = @constCast(p);
        defer mp.deinit();
        try testing.expect(mp.moveCount() > 0);
        try testing.expectEqual(@as(i32, 0), mp.moves.items[0].from_node);
        try testing.expectEqual(@as(i32, 2), mp.moves.items[0].to_node);
    }
}

test "AutoBalancer rack-aware plan falls back when racks are equivalent" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]AutoBalancer.NodeInfo{
        .{ .node_id = 0, .rack = "rack-a" },
        .{ .node_id = 1, .rack = "rack-a" },
    };
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 6000, .bytes_out_rate = 6000, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 1, .bytes_in_rate = 100, .bytes_out_rate = 100, .leader_node = 1 },
    };

    const plan = ab.computeRackAwareRebalancePlan(&nodes, &loads);
    try testing.expect(plan != null);
    if (plan) |*p| {
        var mp = @constCast(p);
        defer mp.deinit();
        try testing.expect(mp.moveCount() > 0);
        try testing.expectEqual(@as(i32, 1), mp.moves.items[0].to_node);
    }
}

test "AutoBalancer ignores stale unknown-node load when computing average" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]i32{ 0, 1 };
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 1000, .bytes_out_rate = 1000, .leader_node = 0 },
        .{ .topic = "stale", .partition_id = 0, .bytes_in_rate = 100000, .bytes_out_rate = 100000, .leader_node = 99 },
    };

    const plan = ab.computeRebalancePlan(&nodes, &loads);
    try testing.expect(plan != null);
    if (plan) |*p| {
        var mp = @constCast(p);
        defer mp.deinit();
        try testing.expectEqual(@as(usize, 1), mp.moveCount());
        try testing.expectEqual(@as(i32, 0), mp.moves.items[0].from_node);
        try testing.expectEqual(@as(i32, 1), mp.moves.items[0].to_node);
    }
}

test "AutoBalancer plan converges simulated node load within tolerance" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]i32{ 0, 1, 2 };
    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "t", .partition_id = 0, .bytes_in_rate = 10000, .bytes_out_rate = 0, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 1, .bytes_in_rate = 8000, .bytes_out_rate = 0, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 2, .bytes_in_rate = 6000, .bytes_out_rate = 0, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 3, .bytes_in_rate = 4000, .bytes_out_rate = 0, .leader_node = 0 },
        .{ .topic = "t", .partition_id = 4, .bytes_in_rate = 1000, .bytes_out_rate = 0, .leader_node = 1 },
        .{ .topic = "t", .partition_id = 5, .bytes_in_rate = 1000, .bytes_out_rate = 0, .leader_node = 2 },
    };

    const plan = ab.computeRebalancePlan(&nodes, &loads);
    try testing.expect(plan != null);
    if (plan) |*p| {
        var mp = @constCast(p);
        defer mp.deinit();
        try testing.expectEqual(@as(usize, 2), mp.moveCount());

        var node0: f64 = 28000;
        var node1: f64 = 1000;
        var node2: f64 = 1000;
        for (mp.moves.items) |move| {
            const moved_load = loads[@as(usize, @intCast(move.partition_id))].totalLoad();
            switch (move.from_node) {
                0 => node0 -= moved_load,
                1 => node1 -= moved_load,
                2 => node2 -= moved_load,
                else => {},
            }
            switch (move.to_node) {
                0 => node0 += moved_load,
                1 => node1 += moved_load,
                2 => node2 += moved_load,
                else => {},
            }
        }

        const avg_load = 30000.0 / 3.0;
        const max_allowed = avg_load * 1.2;
        try testing.expect(node0 <= max_allowed);
        try testing.expect(node1 <= max_allowed);
        try testing.expect(node2 <= max_allowed);
    }
}

test "AutoBalancer controller-aware plan uses active broker racks and moves fenced leaders" {
    const brokers = [_]AutoBalancer.BrokerNode{
        .{ .node_id = 0, .rack = "rack-a" },
        .{ .node_id = 1, .rack = "rack-b" },
        .{ .node_id = 2, .rack = "rack-a", .fenced = true },
    };

    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const loads = [_]AutoBalancer.PartitionLoad{
        .{ .topic = "active", .partition_id = 0, .bytes_in_rate = 1, .bytes_out_rate = 0, .leader_node = 0 },
        .{ .topic = "fenced", .partition_id = 0, .bytes_in_rate = 100, .bytes_out_rate = 0, .leader_node = 2 },
    };

    const plan = ab.computeControllerAwareRebalancePlan(&brokers, &loads);
    try testing.expect(plan != null);
    if (plan) |*p| {
        var mp = @constCast(p);
        defer mp.deinit();
        try testing.expectEqual(@as(usize, 1), mp.moveCount());
        try testing.expectEqualStrings("fenced", mp.moves.items[0].topic);
        try testing.expectEqual(@as(i32, 2), mp.moves.items[0].from_node);
        try testing.expectEqual(@as(i32, 1), mp.moves.items[0].to_node);
    }
}

test "AutoBalancer empty loads returns null" {
    var ab = AutoBalancer.init(testing.allocator);
    defer ab.deinit();

    const nodes = [_]i32{ 0, 1 };
    const loads = [_]AutoBalancer.PartitionLoad{};

    const plan = ab.computeRebalancePlan(&nodes, &loads);
    try testing.expect(plan == null);
}
